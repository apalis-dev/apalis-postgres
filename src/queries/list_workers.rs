use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker};
use apalis_sql::context::SqlContext;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use ulid::Ulid;

#[derive(Debug)]
pub struct WorkerRow {
    pub id: String,
    pub worker_type: String,
    pub storage_name: String,
    pub layers: Option<String>,
    pub last_seen: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
}

use crate::{CompactType, PostgresStorage};

impl<Args: Sync, D, F> ListWorkers for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = queue.to_string();
        let pool = self.pool.clone();
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
                        started_at: w.started_at.map(|t| t.timestamp()).unwrap_or_default() as u64,
                        last_heartbeat: w.last_seen.timestamp() as u64,
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
        let pool = self.pool.clone();
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
                        started_at: w.started_at.map(|t| t.timestamp()).unwrap_or_default() as u64,
                        last_heartbeat: w.last_seen.timestamp() as u64,
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
