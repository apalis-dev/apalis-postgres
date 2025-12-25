use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use apalis_core::{task::Task, timer::Delay, worker::context::WorkerContext};
use apalis_sql::from_row::TaskRow;
use futures::{Future, FutureExt, future::BoxFuture, stream::Stream};
use pin_project::pin_project;

use sqlx::{PgPool, Pool, Postgres};
use ulid::Ulid;

use crate::{CompactType, Config, PgContext, PgTask, from_row::PgTaskRow};

async fn fetch_next(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<CompactType, PgContext, Ulid>>, sqlx::Error> {
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i32;

    sqlx::query_file_as!(
        PgTaskRow,
        "queries/task/fetch_next.sql",
        worker.name(),
        job_type,
        buffer_size
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| {
        let row: TaskRow = r.try_into()?;
        row.try_into_task_compact()
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))
    })
    .collect()
}

enum StreamState<Args> {
    Ready,
    Delay(Delay),
    Fetch(BoxFuture<'static, Result<Vec<PgTask<Args>>, sqlx::Error>>),
    Buffered(VecDeque<PgTask<Args>>),
}

/// Dispatcher for fetching tasks from a PostgreSQL backend via [PgPollFetcher]
#[derive(Clone, Debug)]
pub struct PgFetcher<Args, Compact, Decode> {
    pub _marker: PhantomData<(Args, Compact, Decode)>,
}

#[pin_project]
pub struct PgPollFetcher<Compact> {
    pool: PgPool,
    config: Config,
    wrk: WorkerContext,
    #[pin]
    state: StreamState<Compact>,
    current_backoff: Duration,
    last_fetch_time: Option<Instant>,
}

impl<Compact> Clone for PgPollFetcher<Compact> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            state: StreamState::Ready,
            current_backoff: self.current_backoff,
            last_fetch_time: self.last_fetch_time,
        }
    }
}

impl PgPollFetcher<CompactType> {
    pub fn new(pool: &Pool<Postgres>, config: &Config, wrk: &WorkerContext) -> Self {
        let initial_backoff = Duration::from_secs(1);
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            state: StreamState::Ready,
            current_backoff: initial_backoff,
            last_fetch_time: None,
        }
    }
}

impl Stream for PgPollFetcher<CompactType> {
    type Item = Result<Option<PgTask<CompactType>>, sqlx::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        fetch_next(this.pool.clone(), this.config.clone(), this.wrk.clone());
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
            }
        }
    }
}

impl<Compact> PgPollFetcher<Compact> {
    fn next_backoff(&self, current: Duration) -> Duration {
        let doubled = current * 2;
        std::cmp::min(doubled, Duration::from_secs(60 * 5))
    }

    #[allow(unused)]
    pub fn take_pending(&mut self) -> VecDeque<PgTask<Compact>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
