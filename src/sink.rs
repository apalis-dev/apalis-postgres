use futures::{FutureExt, Sink, TryFutureExt};
use sqlx::{Executor, postgres::types::PgHstore};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use ulid::Ulid;

use crate::{PgTask, backend::PostgresStorage, error::Error, timestamp::Timestamp};

/// Push a batch of tasks to the database
pub fn push_tasks<E>(
    conn: &mut E,
    queue: &str,
    buffer: Vec<PgTask>,
) -> impl futures::Future<Output = Result<(), Error>> + Send
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Postgres> + Send,
{
    // Build the multi-row INSERT with UNNEST
    let mut ids = Vec::new();
    let mut job_data = Vec::new();
    let mut run_ats = Vec::new();
    let mut priorities = Vec::new();
    let mut max_attempts_vec = Vec::new();
    let mut metadata = Vec::new();
    let mut idempotency_key: Vec<Option<String>> = Vec::new();
    let now = Timestamp::now();
    for task in buffer {
        ids.push(
            task.task_id()
                .map(|id| id.to_string())
                .unwrap_or(Ulid::generate().to_string()),
        );

        run_ats.push(task.run_at().map(|f| f as i64).unwrap_or(now.0 as i64));
        priorities.push(task.priority().map(|f| f as i32).unwrap_or_default());
        max_attempts_vec.push(task.max_attempts().map(|f| f as i32).unwrap_or(25));
        metadata.push(PgHstore(
            task.metadata()
                .clone()
                .into_inner()
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect(),
        ));
        idempotency_key.push(task.idempotency_key().map(|a| a.to_owned()));
        job_data.push(task.args);
    }

    sqlx::query_file!(
        "queries/task/sink.sql",
        &ids,
        &queue,
        &job_data,
        &max_attempts_vec,
        &run_ats,
        &priorities,
        &metadata,
        &idempotency_key as &[Option<String>]
    )
    .execute(conn)
    .map_ok(|_| ())
    .map_err(|e| e.into())
    .boxed()
}

impl<Args> Sink<PgTask> for PostgresStorage<Args>
where
    Args: Unpin + Send + Sync + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().persistence.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: PgTask) -> Result<(), Self::Error> {
        self.project().persistence.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().persistence.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(self.project().persistence, cx)
    }
}
