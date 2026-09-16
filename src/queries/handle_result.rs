use apalis_core::backend::TaskResult;
use sqlx::Executor;

use crate::error::Error;

/// Serialized result payload including the current attempt and status
pub type Payload = TaskResult<serde_json::Value>;

/// Ack multiple tasks, given a worker
pub async fn handle_results<E>(
    executor: &mut E,
    results: &[&Payload],
    worker_id: &str,
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let payload_json = serde_json::to_value(results).map_err(Error::JsonError)?;

    let result = sqlx::query_file!("queries/task/handle_result.sql", payload_json, worker_id)
        .execute(executor)
        .await?;

    Ok(result.rows_affected())
}
