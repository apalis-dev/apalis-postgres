use futures::TryFutureExt;
use sqlx::{Executor, postgres::types::PgInterval};

use crate::error::Error;

/// Reenqueue jobs orphaned by a dead worker
pub async fn reenqueue_orphaned<E>(conn: &mut E, queue: &str, dead_for: u64) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let dead_for = PgInterval {
        months: 0,
        days: 0,
        microseconds: dead_for as i64 * 1_000_000,
    };

    match sqlx::query_file!("queries/backend/reenqueue_orphaned.sql", dead_for, queue,)
        .execute(conn)
        .await
    {
        Ok(res) => {
            if res.rows_affected() > 0 {
                tracing::info!(
                    "Re-enqueued {} orphaned tasks that were being processed by dead workers",
                    res.rows_affected()
                );
            }
            Ok(res.rows_affected())
        }
        Err(e) => {
            tracing::error!("Failed to re-enqueue orphaned tasks: {e}");
            Err(e.into())
        }
    }
}

/// Rescues tasks that could not be executed after a worker shutdown
pub async fn reenqueue_abandoned<E>(
    executor: &mut E,
    queue: &str,
    worker: &str,
    task_ids: &[String],
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let res = sqlx::query_file!(
        "queries/worker/reenqueue_abandoned.sql",
        queue,
        worker,
        task_ids
    )
    .execute(executor)
    .map_ok(|res| res.rows_affected())
    .await?;
    Ok(res)
}
