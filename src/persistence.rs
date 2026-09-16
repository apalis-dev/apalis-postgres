use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::{
    backend::persistence::{Persistence, TaskEvent},
    worker::context::WorkerContext,
};
use serde_json::Value;
use sqlx::PgPool;

use crate::{
    PgTask,
    config::Config,
    error::Error,
    queries::{
        self, fetch_next, keep_alive, reenqueue_abandoned, reenqueue_orphaned, register_worker,
    },
    sink::push_tasks,
    timestamp::Timestamp,
};

#[derive(Debug, Clone)]
pub(crate) struct SqlxPersistence {
    pub(crate) pool: PgPool,
    pub(crate) config: Config,
}

impl Persistence for SqlxPersistence {
    type Compact = Vec<u8>;
    type Error = Error;
    type Response = Value;
    async fn register(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        let dead_for = self.config.orphaned_duration().as_secs();
        let queue = self.config.queue.as_ref();
        let count = reenqueue_orphaned(&mut *tx, queue, dead_for).await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        register_worker(&mut *tx, queue, worker, &Timestamp(now), "PgStorage").await?;
        tx.commit().await?;
        if count > 0 {
            tracing::debug!(
                "{count} Re-enqueued orphaned tasks by worker {}",
                worker.name()
            );
        }
        tracing::debug!("Registered Worker: {}", worker.name());
        Ok(())
    }
    async fn heartbeat(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let mut txn = self.pool.begin().await?;
        let queue = self.config.queue.as_ref();
        let dead_for = self.config.orphaned_duration().as_secs();
        keep_alive(&mut *txn, queue, worker).await?;
        let count = reenqueue_orphaned(&mut *txn, queue, dead_for).await?;
        txn.commit().await?;
        if count > 0 {
            tracing::debug!(
                "Re-enqueued {count} orphaned tasks by worker {}",
                worker.name()
            );
        }
        Ok(())
    }
    async fn fetch_next(&mut self, worker: &WorkerContext) -> Result<Vec<PgTask>, Error> {
        let mut tx = self.pool.begin().await?;
        let res = fetch_next(&mut *tx, &self.config, worker).await?;
        tx.commit().await?;
        Ok(res)
    }

    async fn handle_events(
        &mut self,
        messages: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> Result<(), Error> {
        let pool = &self.pool;
        let mut lock_ids = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Lock { task_id, .. } = msg {
                    Some(task_id.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let ack_payloads = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Complete(payload) = msg {
                    Some(payload)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if lock_ids.is_empty() && ack_payloads.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Processing {} messages ({} locks, {} acks)",
            messages.len(),
            lock_ids.len(),
            ack_payloads.len()
        );

        let ack_ids: HashSet<String> = ack_payloads
            .iter()
            .map(|s| s.task_id().to_string())
            .collect();

        lock_ids.retain(|id| !ack_ids.contains(id));

        let mut tx = pool.begin().await?;

        if !ack_payloads.is_empty() {
            queries::handle_results(&mut *tx, &ack_payloads, worker.name()).await?;
        }
        if !lock_ids.is_empty() {
            queries::lock_tasks(&mut *tx, &lock_ids, worker.name()).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<PgTask>,
        worker: &WorkerContext,
    ) -> Result<u64, Error> {
        let config = &self.config;
        let pool = &self.pool;
        let mut txn = pool.begin().await?;
        let task_ids = tasks
            .iter()
            .map(|t| t.task_id().unwrap().to_string())
            .collect::<Vec<_>>();
        let queue = config.queue.as_ref();
        let count = reenqueue_abandoned(&mut *txn, queue, worker.name(), &task_ids).await?;
        if count as usize != tasks.len() {
            return Err(Error::ReenqueueMismatch {
                queued: tasks.len(),
                abandoned: count as usize,
            });
        }
        txn.commit().await?;
        Ok(count)
    }

    async fn push_tasks(&mut self, tasks: Vec<PgTask>) -> Result<(), Self::Error> {
        let queue = self.config.queue.as_ref();
        let mut tx = self.pool.begin().await?;
        push_tasks(&mut *tx, queue, tasks).await?;
        tx.commit().await?;
        Ok(())
    }
}
