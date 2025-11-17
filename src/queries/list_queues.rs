use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use apalis_sql::context::SqlContext;
use serde_json::Value;
use ulid::Ulid;

use crate::{CompactType, PostgresStorage};

impl<Args, D, F> ListQueues for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.pool.clone();
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
