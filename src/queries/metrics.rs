use apalis_core::backend::{BackendExt, Metrics, Statistic};
use ulid::Ulid;

use crate::{CompactType, PgContext, PostgresStorage};

struct StatisticRow {
    priority: Option<i32>,
    r#type: Option<String>,
    statistic: Option<String>,
    value: Option<f32>,
}

impl<Args, D, F> Metrics for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();

        async move {
            let rec = sqlx::query_file_as!(StatisticRow, "queries/backend/overview.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|r| Statistic {
                    priority: Some(r.priority.unwrap_or_default() as u64),
                    stat_type: apalis_sql::stat_type_from_string(&r.r#type.unwrap_or_default()),
                    title: r.statistic.unwrap_or_default(),
                    value: r.value.unwrap_or_default().to_string(),
                })
                .collect();
            Ok(rec)
        }
    }
    fn fetch_by_queue(
        &self,
        queue_id: &str,
    ) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let queue_id = queue_id.to_string();
        async move {
            let rec = sqlx::query_file_as!(
                StatisticRow,
                "queries/backend/overview_by_queue.sql",
                queue_id
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| Statistic {
                priority: Some(r.priority.unwrap_or_default() as u64),
                stat_type: apalis_sql::stat_type_from_string(&r.r#type.unwrap_or_default()),
                title: r.statistic.unwrap_or_default(),
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
}
