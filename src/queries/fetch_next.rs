use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::{PgTask, config::Config, error::Error};

/// Fetch the next batch of tasks from the sqlite backend
pub async fn fetch_next<E>(
    conn: &mut E,
    config: &Config,
    worker: &WorkerContext,
) -> Result<Vec<PgTask>, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    use crate::from_row::PgTaskRow;
    let job_type = config.queue.as_ref();
    let buffer_size = config.batch_size as i32;
    let worker = worker.name();

    sqlx::query_file_as!(
        PgTaskRow,
        "queries/task/fetch_next.sql",
        worker,
        job_type,
        buffer_size
    )
    .fetch_all(conn)
    .await?
    .into_iter()
    .map(|r| r.try_into())
    .collect()
}
