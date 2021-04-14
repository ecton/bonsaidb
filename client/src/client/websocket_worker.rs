use flume::Receiver;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use pliantdb_core::networking::{Payload, Response};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::{OutstandingRequestMapHandle, PendingRequest};
use crate::Error;

pub async fn reconnecting_client_loop(
    url: Url,
    mut request_receiver: Receiver<PendingRequest>,
) -> Result<(), Error> {
    while let Ok(request) = request_receiver.recv_async().await {
        let (stream, _) = match tokio_tungstenite::connect_async(&url).await {
            Ok(result) => result,
            Err(err) => {
                let _ = request.responder.send(Err(Error::WebSocket(err)));
                continue;
            }
        };

        let (mut sender, receiver) = stream.split();

        let outstanding_requests = OutstandingRequestMapHandle::default();
        {
            let mut outstanding_requests = outstanding_requests.lock().await;
            if let Err(err) = sender
                .send(Message::Binary(bincode::serialize(&request.request)?))
                .await
            {
                let _ = request.responder.send(Err(Error::WebSocket(err)));
                continue;
            }
            outstanding_requests.insert(request.request.id, request.responder);
        }

        if let Err(err) = tokio::try_join!(
            request_sender(&mut request_receiver, sender, outstanding_requests.clone()),
            response_processor(receiver, outstanding_requests)
        ) {
            println!("Error on socket {:?}", err);
        }
    }

    Ok(())
}

async fn request_sender(
    request_receiver: &mut Receiver<PendingRequest>,
    mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    outstanding_requests: OutstandingRequestMapHandle,
) -> Result<(), Error> {
    while let Ok(pending) = request_receiver.recv_async().await {
        {
            let mut outstanding_requests = outstanding_requests.lock().await;
            outstanding_requests.insert(pending.request.id, pending.responder);
        }
        sender
            .send(Message::Binary(bincode::serialize(&pending.request)?))
            .await?;
    }

    Err(Error::Disconnected)
}

async fn response_processor(
    mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outstanding_requests: OutstandingRequestMapHandle,
) -> Result<(), Error> {
    while let Some(message) = receiver.next().await {
        let message = message?;
        match message {
            Message::Binary(response) => {
                let payload = bincode::deserialize::<Payload<Response>>(&response)?;
                let responder = {
                    let mut outstanding_requests = outstanding_requests.lock().await;
                    outstanding_requests
                        .remove(&payload.id)
                        .expect("missing responder")
                };
                let _ = responder.send(Ok(payload.wrapped));
            }
            other => {
                println!("Unexpected websocket message: {:?}", other);
            }
        }
    }

    Ok(())
}