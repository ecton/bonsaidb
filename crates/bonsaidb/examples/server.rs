//! Shows basic usage of a server.

use std::{path::Path, time::Duration};

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        connection::{Connection, ServerConnection},
        schema::Collection,
    },
    server::{Configuration, DefaultPermissions, Server},
};
use rand::{thread_rng, Rng};

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    // ANCHOR: setup
    let server = Server::open(
        Path::new("server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Default::default()
        },
    )
    .await?;
    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();
    server.register_schema::<Shape>().await?;
    server.create_database::<Shape>("my-database", true).await?;
    // ANCHOR_END: setup

    // If websockets are enabled, we'll also listen for websocket traffic. The
    // QUIC-based connection should be overall better to use than WebSockets,
    // but it's much easier to route WebSocket traffic across the internet.
    #[cfg(feature = "websockets")]
    {
        let server = server.clone();
        tokio::spawn(async move {
            server.listen_for_http_on("localhost:8080").await
        });
    }

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // To allow this example to run both websockets and QUIC, we're going to gather the clients
    // into a collection and use join_all to wait until they finish.
    let mut tasks = Vec::new();
    #[cfg(feature = "websockets")]
    {
        // To connect over websockets, use the websocket scheme.
        tasks.push(do_some_database_work(
            Client::new(Url::parse("ws://localhost:8080")?)
                .await?
                .database::<Shape>("my-database")
                .await?,
            "websockets",
        ));
    }

    // To connect over QUIC, use the bonsaidb scheme.
    tasks.push(do_some_database_work(
        Client::<()>::build(Url::parse("bonsaidb://localhost")?)
            .with_certificate(certificate)
            .finish()
            .await?
            .database::<Shape>("my-database")
            .await?,
        "bonsaidb",
    ));

    // Wait for the clients to finish
    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

async fn do_some_database_work<'a, C: Connection>(
    database: C,
    client_name: &str,
) -> anyhow::Result<()> {
    // Insert 50 random shapes
    for _ in 0u32..50 {
        let sides = {
            let mut rng = thread_rng();
            rng.gen_range(3..=10)
        };
        Shape::new(sides).insert_into(&database).await?;
    }

    log::info!("Client {} finished", client_name);

    // Print a summary of all the shapes
    for result in database
        .view::<ShapesByNumberOfSides>()
        .reduce_grouped()
        .await?
    {
        log::info!(
            "Number of entries with {:02} sides: {}",
            result.key,
            result.value
        );
    }

    Ok(())
}
