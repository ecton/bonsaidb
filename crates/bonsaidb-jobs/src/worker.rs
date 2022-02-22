use crate::queue::QueueName;

pub struct Worker {}

pub struct WorkerConfig {
    pub tiers: Vec<JobTier>,
}

pub struct JobTier(pub Vec<QueueName>);
