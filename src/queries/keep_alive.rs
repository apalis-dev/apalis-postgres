use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::error::Error;

/// Heartbeat for denoting liveliness of workers
pub async fn keep_alive<E>(conn: &mut E, queue: &str, worker: &WorkerContext) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let tasks = worker
        .tasks()
        .iter()
        .map(|task| task.task_id().to_string())
        .collect::<Vec<_>>();

    let worker = worker.name();

    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue, &tasks)
        .execute(conn)
        .await?;
    if res.rows_affected() == 0 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}
