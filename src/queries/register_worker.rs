use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::{error::Error, timestamp::Timestamp};

/// Register a worker in the database
///
/// Errors if worker already exists
pub async fn register_worker<E>(
    conn: &mut E,
    queue: &str,
    worker: &WorkerContext,
    last_seen: &Timestamp,
    backend_type: &str,
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let res = sqlx::query_file!(
        "queries/worker/register.sql",
        worker.name(),
        queue,
        backend_type,
        worker.get_service(),
        last_seen
    )
    .execute(conn)
    .await?;
    if res.rows_affected() == 0 {
        return Err(Error::WorkerAlreadyExists(worker.name().to_owned()));
    }
    Ok(())
}
