use std::sync::Arc;

use async_trait::async_trait;
pub use bonsaidb_core::circulate::Relay;
use bonsaidb_core::{
    arc_bytes::OwnedBytes,
    circulate,
    pubsub::{self, database_topic, PubSub},
    Error,
};

#[async_trait]
impl PubSub for super::Database {
    type Subscriber = Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        Ok(Subscriber {
            database_name: self.data.name.to_string(),
            subscriber: self.data.storage.relay().create_subscriber().await,
        })
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .relay()
            .publish(database_topic(&self.data.name, &topic.into()), payload)
            .await?;
        Ok(())
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .relay()
            .publish_to_all(
                topics
                    .iter()
                    .map(|topic| database_topic(&self.data.name, topic))
                    .collect(),
                payload,
            )
            .await?;
        Ok(())
    }

    async fn publish_raw<S: Into<String> + Send, P: Into<OwnedBytes> + Send>(
        &self,
        topic: S,
        payload: P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .relay()
            .publish_raw(database_topic(&self.data.name, &topic.into()), payload)
            .await;
        Ok(())
    }

    async fn publish_raw_to_all<P: Into<OwnedBytes> + Send>(
        &self,
        topics: Vec<String>,
        payload: P,
    ) -> Result<(), bonsaidb_core::Error> {
        let payload = payload.into();
        self.data
            .storage
            .relay()
            .publish_raw_to_all(
                topics
                    .iter()
                    .map(|topic| database_topic(&self.data.name, topic))
                    .collect(),
                payload,
            )
            .await;
        Ok(())
    }
}

/// A subscriber for `PubSub` messages.
pub struct Subscriber {
    database_name: String,
    subscriber: circulate::Subscriber,
}

#[async_trait]
impl pubsub::Subscriber for Subscriber {
    async fn subscribe_to<S: Into<String> + Send>(&self, topic: S) -> Result<(), Error> {
        self.subscriber
            .subscribe_to(database_topic(&self.database_name, &topic.into()))
            .await;
        Ok(())
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), Error> {
        let topic = format!("{}\u{0}{}", self.database_name, topic);
        self.subscriber.unsubscribe_from(&topic).await;
        Ok(())
    }

    fn receiver(&self) -> &'_ flume::Receiver<Arc<circulate::Message>> {
        self.subscriber.receiver()
    }
}
