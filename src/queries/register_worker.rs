use apalis_core::worker::context::WorkerContext;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

pub async fn register(
    pool: PgPool,
    worker_type: String,
    worker: WorkerContext,
    last_seen: DateTime<Utc>,
    backend_type: &str,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query_file!(
        "queries/worker/register.sql",
        worker.name(),
        worker_type,
        backend_type,
        worker.get_service(),
        last_seen as chrono::DateTime<chrono::Utc>,
    )
    .execute(&pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    Ok(())
}
