use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use apalis_core::{
    backend::poll_strategy::{PollContext, PollStrategyExt},
    task::Task,
    worker::context::WorkerContext,
};
use apalis_sql::from_row::TaskRow;
use futures::{
    FutureExt,
    future::BoxFuture,
    stream::{Stream, StreamExt},
};
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
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<PgTask<Args>>, sqlx::Error>>),
    Buffered(VecDeque<PgTask<Args>>),
}

/// Dispatcher for fetching tasks from a PostgreSQL backend via [PgPollFetcher]
#[derive(Clone, Debug)]
pub struct PgFetcher<Compact, Decode> {
    pub _marker: PhantomData<(Compact, Decode)>,
}

#[pin_project]
pub struct PgPollFetcher<Compact> {
    pool: PgPool,
    config: Config,
    wrk: WorkerContext,
    #[pin]
    state: StreamState<Compact>,
    poller: Pin<Box<dyn Stream<Item = ()> + Send>>,
    prev_count: Arc<AtomicUsize>,
}

impl<Compact> Clone for PgPollFetcher<Compact> {
    fn clone(&self) -> Self {
        let prev_count = Arc::new(AtomicUsize::new(1));
        let poll_ctx = PollContext::new(self.wrk.clone(), prev_count.clone());
        let poller = self.config.poll_strategy().clone().build_stream(&poll_ctx);
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            state: StreamState::Ready,
            poller,
            prev_count,
        }
    }
}

impl PgPollFetcher<CompactType> {
    pub fn new(pool: &Pool<Postgres>, config: &Config, wrk: &WorkerContext) -> Self {
        let prev_count = Arc::new(AtomicUsize::new(1));
        let poll_ctx = PollContext::new(wrk.clone(), prev_count.clone());
        let poller = config.poll_strategy().clone().build_stream(&poll_ctx);
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            state: StreamState::Ready,
            poller,
            prev_count,
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
                StreamState::Delay => match this.poller.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(_) => this.state = StreamState::Ready,
                },

                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(item) => match item {
                        Ok(requests) => {
                            if requests.is_empty() {
                                this.prev_count.store(0, Ordering::Relaxed);
                                this.state = StreamState::Delay;
                            } else {
                                this.prev_count.store(requests.len(), Ordering::Relaxed);
                                let mut buffer = VecDeque::new();
                                for request in requests {
                                    buffer.push_back(request);
                                }
                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            this.prev_count.store(0, Ordering::Relaxed);
                            this.state = StreamState::Delay;
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
    #[allow(unused)]
    pub fn take_pending(&mut self) -> VecDeque<PgTask<Compact>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
