use std::collections::{HashMap, VecDeque};

use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::Connection,
    document::CollectionDocument,
    keyvalue::{KeyValue, Timestamp},
    pubsub::PubSub,
    schema::SerializedCollection,
    transmog::Format,
};
use tokio::sync::oneshot;

use crate::{
    job::{Job, Progress, Queueable},
    queue::{self, QueueId, QueueName},
    schema::{self, job::PendingJobs, Queue},
    worker::WorkerConfig,
};

#[derive(Clone, Debug)]
pub struct Orchestrator<S = PriorityFifo>
where
    S: Strategy,
{
    sender: flume::Sender<Command<S>>,
}

impl<S> Orchestrator<S>
where
    S: Strategy,
{
    pub fn spawn<Database>(database: Database, strategy: S) -> Self
    where
        Database: Connection + PubSub + KeyValue + 'static,
    {
        let (sender, receiver) = flume::unbounded();
        tokio::task::spawn(Backend::run(receiver, database, strategy));
        Self { sender }
    }

    pub async fn enqueue<Queue: Into<QueueId> + Send, Payload: Queueable>(
        &self,
        queue: Queue,
        job: &Payload,
    ) -> Result<Job<Payload>, queue::Error> {
        let bytes = Payload::format()
            .serialize(job)
            .map_err(|err| bonsaidb_core::Error::Serialization(err.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        self.sender.send(Command::Enqueue {
            queue: queue.into(),
            payload: Bytes::from(bytes),
            result: sender,
        })?;
        let job = receiver.await??;

        Ok(Job::from(job))
    }
}

enum Command<S: Strategy> {
    Enqueue {
        queue: QueueId,
        payload: Bytes,
        result: oneshot::Sender<Result<CollectionDocument<schema::Job>, queue::Error>>,
    },
    RegisterWorker {
        config: S::WorkerConfig,
        result: oneshot::Sender<Result<CollectionDocument<schema::Job>, queue::Error>>,
    },
}

pub struct Backend<Database, S>
where
    Database: Connection + PubSub + KeyValue,
    S: Strategy,
{
    receiver: flume::Receiver<Command<S>>,
    database: Database,
    queues_by_name: HashMap<QueueName, CollectionDocument<Queue>>,
    queues: HashMap<u64, VecDeque<CollectionDocument<schema::Job>>>,
    workers: HashMap<u64, WorkerConfig>,
    strategy: S,
}

impl<Database, S> Backend<Database, S>
where
    Database: Connection + PubSub + KeyValue,
    S: Strategy,
{
    async fn run(
        receiver: flume::Receiver<Command<S>>,
        database: Database,
        strategy: S,
    ) -> Result<(), bonsaidb_core::Error> {
        let mut queues_by_name = HashMap::new();
        let mut queues = HashMap::new();

        for queue in Queue::all(&database).await? {
            queues_by_name.insert(queue.contents.name.clone(), queue);
        }

        for (_, job) in database
            .view::<PendingJobs>()
            .query_with_collection_docs()
            .await?
            .documents
        {
            let queue = queues
                .entry(job.contents.queue_id)
                .or_insert_with(VecDeque::default);
            queue.push_back(job);
        }

        Self {
            receiver,
            database,
            queues_by_name,
            queues,
            workers: HashMap::new(),
            strategy,
        }
        .orchestrate()
        .await
    }

    async fn orchestrate(&mut self) -> Result<(), bonsaidb_core::Error> {
        while let Ok(command) = self.receiver.recv_async().await {
            match command {
                Command::Enqueue {
                    queue,
                    payload,
                    result,
                } => {
                    drop(result.send(self.enqueue(queue, payload).await));
                }
                Command::RegisterWorker { config, result } => {
                    todo!()
                }
            }
        }
        Ok(())
    }

    async fn enqueue(
        &mut self,
        queue: QueueId,
        payload: Bytes,
    ) -> Result<CollectionDocument<schema::Job>, queue::Error> {
        let queue_id = queue.as_id(&self.database).await?;
        let job = schema::Job {
            queue_id,
            payload,
            enqueued_at: Timestamp::now(),
            progress: Progress::default(),
            result: None,
            returned_at: None,
            cancelled_at: None,
        }
        .push_into(&self.database)
        .await?;
        let entries = self.queues.entry(job.contents.queue_id).or_default();
        let insert_at = match entries.binary_search_by(|existing_job| {
            existing_job
                .contents
                .enqueued_at
                .cmp(&job.contents.enqueued_at)
        }) {
            Ok(index) => index,
            Err(index) => index,
        };
        entries.insert(insert_at, job.clone());
        Ok(job)
    }

    pub fn queue(&mut self, queue: u64) -> Option<&mut VecDeque<CollectionDocument<schema::Job>>> {
        self.queues.get_mut(&queue)
    }

    pub fn queue_by_name(
        &mut self,
        queue: &QueueName,
    ) -> Option<&mut VecDeque<CollectionDocument<schema::Job>>> {
        let id = self.queues_by_name.get(queue)?.header.id;
        self.queue(id)
    }
}

pub trait Strategy: Sized + Send + Sync + 'static {
    type WorkerConfig: Send + Sync;

    fn dequeue_for_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        worker: &Self::WorkerConfig,
        backend: &mut Backend<Database, Self>,
    ) -> Option<CollectionDocument<schema::Job>>;
}

pub struct PriorityFifo;

impl Strategy for PriorityFifo {
    type WorkerConfig = WorkerConfig;

    fn dequeue_for_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        worker: &Self::WorkerConfig,
        backend: &mut Backend<Database, Self>,
    ) -> Option<CollectionDocument<schema::Job>> {
        for tier in &worker.tiers {
            if let Some((queue_with_oldest_job, _)) = tier
                .0
                .iter()
                .filter_map(|q| {
                    backend
                        .queue_by_name(q)
                        .and_then(|jobs| jobs.front().map(|j| (q, j.clone())))
                })
                .max_by(|(_, q1_front), (_, q2_front)| {
                    q1_front
                        .contents
                        .enqueued_at
                        .cmp(&q2_front.contents.enqueued_at)
                })
            {
                return backend
                    .queue_by_name(queue_with_oldest_job)
                    .unwrap()
                    .pop_front();
            }
        }

        None
    }
}
