use apalis_core::backend::{Backend, ListWorkers, RunningWorker};

use futures::TryFutureExt;

use crate::{PostgresStorage, error::Error, timestamp::Timestamp};

#[derive(Debug)]
pub struct WorkerRow {
    pub id: String,
    pub worker_type: String,
    pub storage_name: String,
    pub layers: Option<String>,
    pub last_seen: Timestamp,
    pub started_at: Option<Timestamp>,
}

impl<Args: Sync> ListWorkers for PostgresStorage<Args>
where
    PostgresStorage<Args>: Backend<Error = Error>,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = self.persistence.config.queue.to_string();

        let pool = self.persistence.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                WorkerRow,
                "queries/backend/list_workers.sql",
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w.started_at.unwrap_or_default().0,
                        last_heartbeat: w.last_seen.0,
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                WorkerRow,
                "queries/backend/list_all_workers.sql",
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w.started_at.unwrap_or_default().0,
                        last_heartbeat: w.last_seen.0,
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }
}
