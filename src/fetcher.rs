use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use apalis_core::{
    backend::codec::{Codec, json::JsonCodec},
    task::Task,
    timer::Delay,
    worker::context::WorkerContext,
};
use futures::{
    Future, FutureExt, StreamExt,
    future::BoxFuture,
    stream::{self, Stream},
};
use pin_project::pin_project;
use serde_json::Value;
use sqlx::{PgPool, Pool, Postgres};
use ulid::Ulid;

use crate::{config::Config, context::PgContext, from_row::TaskRow, PgTask};

async fn fetch_next<Args, D: Codec<Args, Compact = Value>>(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<Args, PgContext, Ulid>>, sqlx::Error>
where
    D::Error: std::error::Error + Send + Sync + 'static,
{
    use futures::TryFutureExt;
    let job_type = config.namespace();
    let buffer_size = config.buffer_size() as i32;

    sqlx::query_file_as!(
        TaskRow,
        "src/queries/task/fetch_next.sql",
        worker.name(),
        job_type,
        buffer_size
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| r.try_into_task::<D, Args>())
    .collect()
}

enum StreamState<Args> {
    Ready,
    Delay(Delay),
    Fetch(BoxFuture<'static, Result<Vec<PgTask<Args>>, sqlx::Error>>),
    Buffered(VecDeque<PgTask<Args>>),
    Empty,
}

#[pin_project]
pub struct PgFetcher<Args, Compact = Value, Decode = JsonCodec<Value>> {
    pool: PgPool,
    config: Config,
    wrk: WorkerContext,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState<Args>,
    current_backoff: Duration,
    last_fetch_time: Option<Instant>,
}

impl<Args, Compact, Decode> Clone for PgFetcher<Args, Compact, Decode> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            current_backoff: self.current_backoff,
            last_fetch_time: self.last_fetch_time,
        }
    }
}

impl<Args: 'static, Decode> PgFetcher<Args, Value, Decode> {
    pub fn new(pool: &Pool<Postgres>, config: &Config, wrk: &WorkerContext) -> Self
    where
        Decode: Codec<Args, Compact = Value> + 'static,
        Decode::Error: std::error::Error + Send + Sync + 'static,
    {
        let initial_backoff = Duration::from_secs(1);
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            current_backoff: initial_backoff,
            last_fetch_time: None,
        }
    }
}

impl<Args, Decode> Stream for PgFetcher<Args, Value, Decode>
where
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = Value> + 'static,
    // Compact: Unpin + Send + 'static,
{
    type Item = Result<Option<PgTask<Args>>, sqlx::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        fetch_next::<Args, Decode>(this.pool.clone(), this.config.clone(), this.wrk.clone());
                    this.state = StreamState::Fetch(stream.boxed());
                }
                StreamState::Delay(ref mut delay) => match Pin::new(delay).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(_) => this.state = StreamState::Ready,
                },

                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(item) => match item {
                        Ok(requests) => {
                            if requests.is_empty() {
                                let next = this.next_backoff(this.current_backoff);
                                this.current_backoff = next;
                                let delay = Delay::new(this.current_backoff);
                                this.state = StreamState::Delay(delay);
                            } else {
                                let mut buffer = VecDeque::new();
                                for request in requests {
                                    buffer.push_back(request);
                                }
                                this.current_backoff = Duration::from_secs(1);
                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            let next = this.next_backoff(this.current_backoff);
                            this.current_backoff = next;
                            this.state = StreamState::Delay(Delay::new(next));
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                },

                StreamState::Buffered(ref mut buffer) => {
                    if let Some(request) = buffer.pop_front() {
                        // Yield the next buffered item
                        if buffer.is_empty() {
                            // Buffer is now empty, transition to ready for next fetch
                            this.state = StreamState::Ready;
                        }
                        return Poll::Ready(Some(Ok(Some(request))));
                    } else {
                        // Buffer is empty, transition to ready
                        this.state = StreamState::Ready;
                    }
                }

                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}

impl<Args, Compact, Decode> PgFetcher<Args, Compact, Decode> {
    fn next_backoff(&self, current: Duration) -> Duration {
        let doubled = current * 2;
        std::cmp::min(doubled, Duration::from_secs(60 * 5))
    }

    pub fn take_pending(&mut self) -> VecDeque<PgTask<Args>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
