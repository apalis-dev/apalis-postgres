use sqlx::{Error, Executor};

/// Lock multiple tasks, given a worker
pub async fn lock_tasks<E>(conn: &mut E, task_ids: &[String], worker_id: &str) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres>,
{
    let res = sqlx::query_file!("queries/task/lock_by_id.sql", task_ids, worker_id)
        .execute(&mut *conn)
        .await?;

    Ok(res.rows_affected())
}
