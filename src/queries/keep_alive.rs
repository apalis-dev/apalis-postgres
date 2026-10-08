use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::error::Error;

/// Heartbeat for denoting liveliness of workers
pub async fn keep_alive<E>(conn: &mut E, queue: &str, worker: &WorkerContext) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    if cfg!(feature = "task-tracking") {
        keep_alive_tracked(conn, queue, worker).await
    } else {
        keep_alive_untracked(conn, queue, worker).await
    }
}

async fn keep_alive_tracked<E>(
    conn: &mut E,
    queue: &str,
    worker: &WorkerContext,
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    #[cfg(feature = "task-tracking")]
    let tasks = worker
        .tasks()
        .map_err(|_| Error::WorkerOutOfSync)?
        .iter()
        .map(|task| task.task_id().to_owned())
        .collect::<Vec<_>>();

    //We will never reach this point if the feature is not enabled
    #[cfg(not(feature = "task-tracking"))]
    let tasks: Vec<String> = vec![];

    let worker = worker.name();

    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue, &tasks)
        .execute(conn)
        .await?;
    if res.rows_affected() != 1 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}

async fn keep_alive_untracked<E>(
    conn: &mut E,
    queue: &str,
    worker: &WorkerContext,
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let worker = worker.name();

    let res = sqlx::query_file!("queries/backend/keep_alive_untracked.sql", worker, queue)
        .execute(conn)
        .await?;
    if res.rows_affected() != 1 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}
