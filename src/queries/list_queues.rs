use apalis_core::backend::{Backend, ListQueues, QueueInfo};
use serde_json::Value;

use crate::{PostgresStorage, error::Error};

impl<Args> ListQueues for PostgresStorage<Args>
where
    PostgresStorage<Args>: Backend<Error = Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        struct QueueInfoRow {
            pub name: Option<String>,
            pub stats: Option<Value>,
            pub workers: Option<Value>,
            pub activity: Option<Value>,
        }

        async move {
            let queues = sqlx::query_file_as!(QueueInfoRow, "queries/backend/list_queues.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|row| QueueInfo {
                    name: row.name.unwrap_or_default(),
                    stats: serde_json::from_value(row.stats.unwrap()).unwrap_or_default(),
                    workers: serde_json::from_value(row.workers.unwrap()).unwrap_or_default(),
                    activity: serde_json::from_value(row.activity.unwrap()).unwrap_or_default(),
                })
                .collect();
            Ok(queues)
        }
    }
}
