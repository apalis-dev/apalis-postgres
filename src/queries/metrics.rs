use std::str::FromStr;

use apalis_core::backend::{Backend, Metrics, StatType, Statistic};

use crate::{PostgresStorage, error::Error};

struct StatisticRow {
    priority: Option<i32>,
    r#type: Option<String>,
    statistic: Option<String>,
    value: Option<f32>,
}

impl<Args> Metrics for PostgresStorage<Args>
where
    PostgresStorage<Args>: Backend<Error = Error>,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();

        async move {
            let rec = sqlx::query_file_as!(StatisticRow, "queries/backend/overview.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|r| Statistic {
                    priority: Some(r.priority.unwrap_or_default() as u64),
                    stat_type: StatType::from_str(&r.r#type.unwrap_or_default())
                        .unwrap_or_default(),
                    title: r.statistic.unwrap_or_default(),
                    value: r.value.unwrap_or_default().to_string(),
                })
                .collect();
            Ok(rec)
        }
    }
    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let queue_id = self.persistence.config.queue.to_string();
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
                stat_type: StatType::from_str(&r.r#type.unwrap_or_default()).unwrap_or_default(),
                title: r.statistic.unwrap_or_default(),
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
}
