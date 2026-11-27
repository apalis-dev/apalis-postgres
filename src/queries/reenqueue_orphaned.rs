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

/// Re-enqueues tasks still locked by a previous run of a worker that is registering again
///
/// A registering worker isn't running any tasks yet, so any task locked under its name was
/// abandoned by an earlier run. Call this only after [`register_worker`](super::register_worker)
/// succeeds. This assumes no other live worker uses the same name: `register_worker`'s advisory
/// lock is only taken when the worker row already exists, so it doesn't always reject a
/// duplicate, and this would then re-enqueue the other worker's running tasks.
pub async fn reenqueue_reregistered<E>(
    executor: &mut E,
    queue: &str,
    worker: &str,
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let res = sqlx::query_file!("queries/worker/reenqueue_reregistered.sql", queue, worker)
        .execute(executor)
        .map_ok(|res| res.rows_affected())
        .await?;
    Ok(res)
}
