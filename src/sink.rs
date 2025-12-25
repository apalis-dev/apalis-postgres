use apalis_codec::json::JsonCodec;
use apalis_sql::config::Config;
use chrono::{DateTime, Utc};
use futures::{
    FutureExt, Sink, TryFutureExt,
    future::{BoxFuture, Shared},
};
use sqlx::{Executor, PgPool};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use ulid::Ulid;

use crate::{CompactType, PgTask, PostgresStorage};

type FlushFuture = BoxFuture<'static, Result<(), Arc<sqlx::Error>>>;

#[pin_project::pin_project]
pub struct PgSink<Args, Compact = CompactType, Codec = JsonCodec<CompactType>> {
    pool: PgPool,
    config: Config,
    buffer: Vec<PgTask<Compact>>,
    #[pin]
    flush_future: Option<Shared<FlushFuture>>,
    _marker: std::marker::PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> Clone for PgSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: std::marker::PhantomData,
        }
    }
}

pub fn push_tasks<'a, E>(
    conn: E,
    cfg: Config,
    buffer: Vec<PgTask<CompactType>>,
) -> impl futures::Future<Output = Result<(), sqlx::Error>> + Send + 'a
where
    E: Executor<'a, Database = sqlx::Postgres> + Send + 'a,
{
    let job_type = cfg.queue().to_string();
    // Build the multi-row INSERT with UNNEST
    let mut ids = Vec::new();
    let mut job_data = Vec::new();
    let mut run_ats = Vec::new();
    let mut priorities = Vec::new();
    let mut max_attempts_vec = Vec::new();
    let mut metadata = Vec::new();

    for task in buffer {
        ids.push(
            task.parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or(Ulid::new().to_string()),
        );
        job_data.push(task.args);
        run_ats.push(DateTime::from_timestamp(task.parts.run_at as i64, 0).unwrap_or(Utc::now()));
        priorities.push(task.parts.ctx.priority());
        max_attempts_vec.push(task.parts.ctx.max_attempts());
        metadata.push(serde_json::Value::Object(task.parts.ctx.meta().clone()));
    }

    sqlx::query_file!(
        "queries/task/sink.sql",
        &ids,
        &job_type,
        &job_data,
        &max_attempts_vec,
        &run_ats,
        &priorities,
        &metadata
    )
    .execute(conn)
    .map_ok(|_| ())
    .boxed()
}

impl<Args, Compact, Codec> PgSink<Args, Compact, Codec> {
    pub fn new(pool: &PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            _marker: std::marker::PhantomData,
            flush_future: None,
        }
    }
}

impl<Args, Encode, Fetcher> Sink<PgTask<CompactType>>
    for PostgresStorage<Args, CompactType, Encode, Fetcher>
where
    Args: Unpin + Send + Sync + 'static,
    Fetcher: Unpin,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: PgTask<CompactType>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.get_mut().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // If there's no existing future and buffer is empty, we're done
        if this.sink.flush_future.is_none() && this.sink.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.sink.flush_future.is_none() && !this.sink.buffer.is_empty() {
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let pool = this.sink.pool.clone();
            let fut = async move {
                let mut conn = pool.begin().map_err(Arc::new).await?;
                push_tasks(&mut *conn, config, buffer)
                    .map_err(Arc::new)
                    .await?;
                conn.commit().map_err(Arc::new).await?;
                Ok(())
            };
            this.sink.flush_future = Some(fut.boxed().shared());
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(Arc::<sqlx::Error>::into_inner(e).unwrap()))
                }
                Poll::Pending => {
                    // Future is still pending, put it back and return Pending
                    this.sink.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            // No future and no work to do
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
