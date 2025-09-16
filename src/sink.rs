use std::{
    pin::{self, Pin},
    task::{Context, Poll},
};

use apalis_core::{
    backend::codec::{Codec, json::JsonCodec},
    error::BoxDynError,
};
use chrono::DateTime;
use futures::Sink;
use serde_json::Value;
use sqlx::{PgPool, Postgres};
use ulid::Ulid;

use crate::{PgTask, PostgresStorage, config::Config};

#[pin_project::pin_project]
pub struct PgSink<Args, Compact = Value, Codec = JsonCodec<Value>> {
    pool: PgPool,
    config: Config,
    buffer: Vec<PgTask<Compact>>,
    #[pin]
    flush_future: Option<Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>>,
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

async fn push_tasks(
    pool: PgPool,
    cfg: Config,
    buffer: Vec<PgTask<Value>>,
) -> Result<(), sqlx::Error> {
    let job_type = cfg.namespace();
    // Build the multi-row INSERT with UNNEST
    let mut ids = Vec::new();
    let mut job_data = Vec::new();
    let mut run_ats = Vec::new();
    let mut priorities = Vec::new();
    let mut max_attempts_vec = Vec::new();

    for task in buffer {
        ids.push(
            task.parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or(Ulid::new().to_string()),
        );
        job_data.push(task.args);
        run_ats.push(
            DateTime::from_timestamp(task.parts.run_at as i64, 0)
                .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?,
        );
        priorities.push(*task.parts.ctx.priority());
        max_attempts_vec.push(task.parts.ctx.max_attempts());
    }

    sqlx::query_file!(
        "src/queries/task/sink.sql",
        &ids,
        &job_type,
        &job_data,
        &max_attempts_vec,
        &run_ats,
        &priorities
    )
    .execute(&pool)
    .await?;
    Ok(())
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

impl<Args, Encode, Fetcher> Sink<PgTask<Args>> for PostgresStorage<Args, Value, Encode, Fetcher>
where
    Args: Unpin + Send + Sync + 'static,
    Encode: Codec<Args, Compact = Value> + Unpin,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    Encode::Error: Into<BoxDynError>,
    Fetcher: Unpin,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: PgTask<Args>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.get_mut()
            .sink
            .buffer
            .push(item.try_map(|s| Encode::encode(&s).map_err(|e| sqlx::Error::Encode(e.into())))?);
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
            let pool = this.pool.clone();
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let sink_fut = push_tasks(pool, config, buffer);
            this.sink.flush_future = Some(Box::pin(sink_fut));
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(e))
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
