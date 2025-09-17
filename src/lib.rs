use std::{
    backtrace::Backtrace,
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    panic,
    pin::{self, Pin},
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{
        Backend, TaskSink, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    error::{BoxDynError, WorkerError},
    layers::Identity,
    task::{Parts, Task, attempt::Attempt, status::Status, task_id::TaskId},
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use chrono::{DateTime, Utc};
use futures::{
    FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt,
    channel::mpsc::{Receiver, Sender},
    future::{BoxFuture, ready},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, postgres::PgListener};
use ulid::Ulid;

use crate::{ack::PgAck, config::Config, context::PgContext, fetcher::PgFetcher, sink::PgSink};

mod ack;
mod config;
mod context;
mod fetcher;
mod from_row;
mod shared;
mod sink;

pub type PgTask<Args> = Task<Args, PgContext, Ulid>;

#[pin_project::pin_project]
pub struct PostgresStorage<
    Args,
    Compact = Value,
    Codec = JsonCodec<Value>,
    Fetcher = PhantomData<PgFetcher<Args, Value, Codec>>,
> {
    _marker: PhantomData<(Args, Compact, Codec)>,
    pool: PgPool,
    config: Config,
    #[pin]
    fetcher: Fetcher,
    #[pin]
    sink: PgSink<Args, Compact, Codec>,
}

impl<Args> PostgresStorage<Args> {
    /// Creates a new PostgresStorage instance.
    pub fn new(pool: PgPool, config: Config) -> Self {
        let sink = PgSink::new(&pool, &config);
        Self {
            _marker: PhantomData,
            pool,
            config,
            fetcher: PhantomData,
            sink,
        }
    }

    pub async fn new_with_notify(
        pool: PgPool,
        config: Config,
    ) -> PostgresStorage<Args, Value, JsonCodec<Value>, PgListener> {
        let sink = PgSink::new(&pool, &config);
        let mut fetcher = PgListener::connect_with(&pool)
            .await
            .expect("Failed to create listener");
        fetcher.listen("apalis::job::insert").await.unwrap();
        PostgresStorage {
            _marker: PhantomData,
            pool,
            config,
            fetcher,
            sink,
        }
    }

    /// Returns a reference to the pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

pub(crate) async fn register(
    pool: PgPool,
    worker_type: String,
    worker: WorkerContext,
    last_seen: i64,
    backend_type: &str
) -> Result<(), sqlx::Error> {
    let last_seen = DateTime::from_timestamp(last_seen, 0).ok_or(sqlx::Error::Io(
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid Timestamp"),
    ))?;
    let res = sqlx::query_file!(
        "src/queries/worker/register.sql",
        worker.name(),
        worker_type,
        backend_type,
        worker.get_service(),
        last_seen
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

impl<Args, Decode> Backend<Args>
    for PostgresStorage<Args, Value, Decode, PhantomData<PgFetcher<Args, Value, Decode>>>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = Value> + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Context = PgContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = PgFetcher<Args, Value, Decode>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<PgAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let fut = register(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
            "PostgresStorage"
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(PgAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        PgFetcher::new(&self.pool, &self.config, worker)
    }
}

impl<Args, Decode> Backend<Args> for PostgresStorage<Args, Value, Decode, PgListener>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = Value> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Context = PgContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = TaskStream<PgTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<PgAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let fut = register(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
            "PostgresStorageWithNotify"
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(PgAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let pool = self.pool.clone();
        let worker_id = worker.name().to_owned();
        let namespace = self.config.namespace().to_owned();
        let lazy_fetcher = self
            .fetcher
            .into_stream()
            .filter_map(move |notification| {
                let namespace = namespace.clone();
                async move {
                    let pg_notification = notification.ok()?;
                    let payload = pg_notification.payload();
                    let ev: InsertEvent = serde_json::from_str(payload).ok()?;

                    if ev.job_type == namespace {
                        return Some(ev.id);
                    }
                    None
                }
            })
            .map(|t| t.to_string())
            .ready_chunks(self.config.buffer_size())
            .then(move |ids| {
                let pool = pool.clone();
                let worker_id = worker_id.clone();
                async move {
                    let mut tx = pool.begin().await?;
                    use crate::from_row::TaskRow;
                    let res: Vec<_> = sqlx::query_file_as!(
                        TaskRow,
                        "src/queries/task/lock_by_id.sql",
                        &ids,
                        &worker_id
                    )
                    .fetch(&mut *tx)
                    .map(|r| Ok(Some(r?.try_into_task::<Decode, Args>()?)))
                    .collect()
                    .await;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(res)
                }
            })
            .flat_map(|vec| match vec {
                Ok(vec) => stream::iter(vec.into_iter().map(|res| match res {
                    Ok(t) => Ok(t),
                    Err(e) => Err(e),
                }))
                .boxed(),
                Err(e) => stream::once(ready(Err(e))).boxed(),
            })
            .boxed();

        let eager_fetcher = StreamExt::boxed(PgFetcher::<Args, Value, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ));
        select(lazy_fetcher, eager_fetcher).boxed()
    }
}

#[derive(Debug, Deserialize)]
pub struct InsertEvent {
    job_type: String,
    id: TaskId,
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::Local;

    use apalis_core::{
        error::BoxDynError,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let mut backend = PostgresStorage::new(
            PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
                .await
                .unwrap(),
            Default::default(),
        );

        let mut items = stream::repeat_with(|| {
            let task = Task::builder(HashMap::default())
                .run_after(Duration::from_secs(1))
                .with_ctx({
                    let mut ctx = PgContext::default();
                    ctx.set_priority(1);
                    ctx
                })
                .build();
            Ok(task)
        })
        .take(1);
        backend.send_all(&mut items).await.unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn notify_worker() {
        let pool = PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
            .await
            .unwrap();
        let config = Config::new("test").set_poll_interval(Duration::from_secs(5));
        let mut backend = PostgresStorage::new_with_notify(pool, config).await;

        let mut items = stream::repeat_with(|| {
            let task = Task::builder(Default::default())
                .with_ctx({
                    let mut ctx = PgContext::default();
                    ctx.set_priority(1);
                    ctx
                })
                .build();
            Ok(task)
        })
        .take(1);
        backend.send_all(&mut items).await.unwrap();

        async fn send_reminder(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }
}
