//! # apalis-postgres
//!
//! Background task processing in rust using `apalis` and `postgres`
//!
//! ## Features
//!
//! - **Reliable job queue** using Postgres as the backend.
//! - **Multiple storage types**: standard polling and `trigger` based storages.
//! - **Custom codecs** for serializing/deserializing job arguments as bytes.
//! - **Heartbeat and orphaned job re-enqueueing** for robust task processing.
//! - **Integration with `apalis` workers and middleware.**
//!
//! ## Storage Types
//!
//! - [`PostgresStorage`]: Standard polling-based storage.
//! - [`PostgresStorageWithListener`]: Event-driven storage using Postgres `NOTIFY` for low-latency job fetching.
//! - [`SharedPostgresStorage`]: Shared storage for multiple job types, uses Postgres `NOTIFY`.
//!
//! The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `PostgresStorage` struct with different configurations.
//!
//! ## Examples
//!
//! ### Basic Worker Example
//!
//! ```rust,no_run
//! # use std::time::Duration;
//! # use apalis_postgres::PostgresStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::event::Event;
//! # use apalis_core::task::Task;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::error::BoxDynError;
//! # use sqlx::PgPool;
//! # use futures::stream;
//! # use apalis_sql::context::SqlContext;
//! # use futures::SinkExt;
//! # use apalis_sql::config::Config;
//! # use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
//!     PostgresStorage::setup(&pool).await.unwrap();
//!     let mut backend = PostgresStorage::new_with_config(&pool, &Config::new("int-queue"));
//!
//!     let mut start = 0;
//!     let mut items = stream::repeat_with(move || {
//!         start += 1;
//!         let task = Task::builder(serde_json::to_vec(&start).unwrap())
//!             .run_after(Duration::from_secs(1))
//!             .with_ctx(SqlContext::new().with_priority(1))
//!             .build();
//!         Ok(task)
//!     })
//!     .take(10);
//!     backend.send_all(&mut items).await.unwrap();
//!
//!     async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-1")
//!         .backend(backend)
//!         .build(send_reminder);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ### `NOTIFY` listener example
//!
//! ```rust,no_run
//! # use std::time::Duration;
//! # use apalis_postgres::PostgresStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::event::Event;
//! # use apalis_core::task::Task;
//! # use sqlx::PgPool;
//! # use apalis_core::backend::poll_strategy::StrategyBuilder;
//! # use apalis_core::backend::poll_strategy::IntervalStrategy;
//! # use apalis_sql::config::Config;
//! # use futures::stream;
//! # use apalis_sql::context::SqlContext;
//! # use apalis_core::error::BoxDynError;
//! # use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
//!     PostgresStorage::setup(&pool).await.unwrap();
//!
//!     let lazy_strategy = StrategyBuilder::new()
//!         .apply(IntervalStrategy::new(Duration::from_secs(5)))
//!         .build();
//!     let config = Config::new("queue")
//!         .with_poll_interval(lazy_strategy)
//!         .set_buffer_size(5);
//!     let backend = PostgresStorage::new_with_notify(&pool, &config);
//!
//!     tokio::spawn({
//!         let pool = pool.clone();
//!         let config = config.clone();
//!         async move {
//!             tokio::time::sleep(Duration::from_secs(2)).await;
//!             let mut start = 0;
//!             let items = stream::repeat_with(move || {
//!                 start += 1;
//!                 Task::builder(serde_json::to_vec(&start).unwrap())
//!                     .run_after(Duration::from_secs(1))
//!                     .with_ctx(SqlContext::new().with_priority(start))
//!                     .build()
//!             })
//!             .take(20)
//!             .collect::<Vec<_>>()
//!             .await;
//!             apalis_postgres::sink::push_tasks(pool, config, items).await.unwrap();
//!         }
//!     });
//!
//!     async fn send_reminder(task: usize) -> Result<(), BoxDynError> {
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-2")
//!         .backend(backend)
//!         .build(send_reminder);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ### Workflow Example
//!
//! ```rust,no_run
//! # use apalis_workflow::WorkFlow;
//! # use apalis_workflow::WorkflowError;
//! # use std::time::Duration;
//! # use apalis_postgres::PostgresStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//! # use apalis_core::worker::event::Event;
//! # use sqlx::PgPool;
//! # use apalis_sql::config::Config;
//! # use apalis_core::backend::WeakTaskSink;
//!
//! #[tokio::main]
//! async fn main() {
//!     let workflow = WorkFlow::new("odd-numbers-workflow")
//!         .then(|a: usize| async move {
//!             Ok::<_, WorkflowError>((0..=a).collect::<Vec<_>>())
//!         })
//!         .filter_map(|x| async move {
//!             if x % 2 != 0 { Some(x) } else { None }
//!         })
//!         .filter_map(|x| async move {
//!             if x % 3 != 0 { Some(x) } else { None }
//!         })
//!         .filter_map(|x| async move {
//!             if x % 5 != 0 { Some(x) } else { None }
//!         })
//!         .delay_for(Duration::from_millis(1000))
//!         .then(|a: Vec<usize>| async move {
//!             println!("Sum: {}", a.iter().sum::<usize>());
//!             Ok::<(), WorkflowError>(())
//!         });
//!
//!     let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
//!     PostgresStorage::setup(&pool).await.unwrap();
//!     let mut backend = PostgresStorage::new_with_config(&pool, &Config::new("workflow-queue"));
//!
//!     backend.push(100usize).await.unwrap();
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(backend)
//!         .on_event(|ctx, ev| {
//!             println!("On Event = {:?}", ev);
//!             if matches!(ev, Event::Error(_)) {
//!                 ctx.stop().unwrap();
//!             }
//!         })
//!         .build(workflow);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Observability
//!
//! You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
//! ![Task](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/task.png)
//!
//! ## License
//!
//! Licensed under either of Apache License, Version 2.0 or MIT license at your option.
//!
//! [`PostgresStorageWithListener`]: crate::PostgresStorage
//! [`SharedPostgresStorage`]: crate::shared::SharedPostgresStorage
use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    layers::Stack,
    task::{Task, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::from_row::TaskRow;
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::ready,
    stream::{self, BoxStream, select},
};
use serde::Deserialize;
pub use sqlx::{PgPool, postgres::PgConnectOptions, postgres::PgListener, postgres::Postgres};
use ulid::Ulid;

use crate::{
    ack::{LockTaskLayer, PgAck},
    config::Config,
    context::PgContext,
    fetcher::{PgFetcher, PgPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::PgSink,
};

mod ack;
pub mod config;
mod fetcher;
mod from_row {
    use chrono::{DateTime, Utc};
    #[derive(Debug)]
    pub struct PgTaskRow {
        pub job: Option<Vec<u8>>,
        pub id: Option<String>,
        pub job_type: Option<String>,
        pub status: Option<String>,
        pub attempts: Option<i32>,
        pub max_attempts: Option<i32>,
        pub run_at: Option<DateTime<Utc>>,
        pub last_result: Option<serde_json::Value>,
        pub lock_at: Option<DateTime<Utc>>,
        pub lock_by: Option<String>,
        pub done_at: Option<DateTime<Utc>>,
        pub priority: Option<i32>,
        pub metadata: Option<serde_json::Value>,
    }
    impl TryInto<apalis_sql::from_row::TaskRow> for PgTaskRow {
        type Error = sqlx::Error;

        fn try_into(self) -> Result<apalis_sql::from_row::TaskRow, Self::Error> {
            Ok(apalis_sql::from_row::TaskRow {
                job: self.job.unwrap_or_default(),
                id: self
                    .id
                    .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?,
                job_type: self
                    .job_type
                    .ok_or_else(|| sqlx::Error::Protocol("Missing job_type".into()))?,
                status: self
                    .status
                    .ok_or_else(|| sqlx::Error::Protocol("Missing status".into()))?,
                attempts: self
                    .attempts
                    .ok_or_else(|| sqlx::Error::Protocol("Missing attempts".into()))?
                    as usize,
                max_attempts: self.max_attempts.map(|v| v as usize),
                run_at: self.run_at,
                last_result: self.last_result,
                lock_at: self.lock_at,
                lock_by: self.lock_by,
                done_at: self.done_at,
                priority: self.priority.map(|v| v as usize),
                metadata: self.metadata,
            })
        }
    }
}
pub mod context {
    pub type PgContext = apalis_sql::context::SqlContext;
}
mod queries;
pub mod shared;
pub mod sink;

pub type PgTask<Args> = Task<Args, PgContext, Ulid>;

pub type CompactType = Vec<u8>;

#[derive(Debug, Clone, Default)]
pub struct PgNotify {
    _private: PhantomData<()>,
}

#[pin_project::pin_project]
pub struct PostgresStorage<
    Args,
    Compact = CompactType,
    Codec = JsonCodec<CompactType>,
    Fetcher = PgFetcher<Args, Compact, Codec>,
> {
    _marker: PhantomData<(Args, Compact, Codec)>,
    pool: PgPool,
    config: Config,
    #[pin]
    fetcher: Fetcher,
    #[pin]
    sink: PgSink<Args, Compact, Codec>,
}

impl<Args, Compact, Codec, Fetcher: Clone> Clone
    for PostgresStorage<Args, Compact, Codec, Fetcher>
{
    fn clone(&self) -> Self {
        Self {
            _marker: PhantomData,
            pool: self.pool.clone(),
            config: self.config.clone(),
            fetcher: self.fetcher.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl PostgresStorage<(), (), ()> {
    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &PgPool) -> Result<(), sqlx::Error> {
        Self::migrations().run(pool).await?;
        Ok(())
    }

    /// Get postgres migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<Args> PostgresStorage<Args> {
    pub fn new(pool: &PgPool) -> Self {
        let config = Config::new(std::any::type_name::<Args>());
        Self::new_with_config(pool, &config)
    }

    /// Creates a new PostgresStorage instance.
    pub fn new_with_config(pool: &PgPool, config: &Config) -> Self {
        let sink = PgSink::new(pool, config);
        Self {
            _marker: PhantomData,
            pool: pool.clone(),
            config: config.clone(),
            fetcher: PgFetcher {
                _marker: PhantomData,
            },
            sink,
        }
    }

    pub fn new_with_notify(
        pool: &PgPool,
        config: &Config,
    ) -> PostgresStorage<Args, CompactType, JsonCodec<CompactType>, PgNotify> {
        let sink = PgSink::new(pool, config);

        PostgresStorage {
            _marker: PhantomData,
            pool: pool.clone(),
            config: config.clone(),
            fetcher: PgNotify::default(),
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

impl<Args, Compact, Codec, Fetcher> PostgresStorage<Args, Compact, Codec, Fetcher> {
    pub fn with_codec<NewCodec>(self) -> PostgresStorage<Args, Compact, NewCodec, Fetcher> {
        PostgresStorage {
            _marker: PhantomData,
            sink: PgSink::new(&self.pool, &self.config),
            pool: self.pool,
            config: self.config,
            fetcher: self.fetcher,
        }
    }
}

impl<Args, Decode> Backend
    for PostgresStorage<Args, CompactType, Decode, PgFetcher<Args, CompactType, Decode>>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type Compact = CompactType;

    type IdType = Ulid;

    type Context = PgContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = TaskStream<PgTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<PgAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(pool, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.pool.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Stack::new(
            LockTaskLayer::new(self.pool.clone()),
            AcknowledgeLayer::new(PgAck::new(self.pool.clone())),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "PostgresStorage",
        )
        .map(|_| Ok(None));
        let register = stream::once(register_worker);
        register
            .chain(PgPollFetcher::<Args, CompactType, Decode>::new(
                &self.pool,
                &self.config,
                worker,
            ))
            .boxed()
    }
}

impl<Args, Decode> Backend for PostgresStorage<Args, CompactType, Decode, PgNotify>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type Compact = CompactType;

    type IdType = Ulid;

    type Context = PgContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = TaskStream<PgTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<PgAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(pool, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.pool.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Stack::new(
            LockTaskLayer::new(self.pool.clone()),
            AcknowledgeLayer::new(PgAck::new(self.pool.clone())),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let pool = self.pool.clone();
        let worker_id = worker.name().to_owned();
        let namespace = self.config.queue().to_string();
        let listener = async move {
            let mut fetcher = PgListener::connect_with(&pool)
                .await
                .expect("Failed to create listener");
            fetcher.listen("apalis::job::insert").await.unwrap();
            fetcher
        };
        let fetcher = stream::once(listener).flat_map(|f| f.into_stream());
        let pool = self.pool.clone();
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "PostgresStorageWithNotify",
        )
        .map(|_| Ok(None));
        let register = stream::once(register_worker);
        let lazy_fetcher = fetcher
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
                    use crate::from_row::PgTaskRow;
                    let res: Vec<_> = sqlx::query_file_as!(
                        PgTaskRow,
                        "queries/task/lock_by_id.sql",
                        &ids,
                        &worker_id
                    )
                    .fetch(&mut *tx)
                    .map(|r| {
                        let row: TaskRow = r?.try_into()?;
                        Ok(Some(
                            row.try_into_task::<Decode, Args, Ulid>()
                                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
                        ))
                    })
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

        let eager_fetcher = StreamExt::boxed(PgPollFetcher::<Args, CompactType, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ));
        register.chain(select(lazy_fetcher, eager_fetcher)).boxed()
    }
}

#[derive(Debug, Deserialize)]
pub struct InsertEvent {
    job_type: String,
    id: TaskId,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, time::Duration};

    use apalis_workflow::{WorkFlow, WorkflowError};

    use apalis_core::{
        error::BoxDynError,
        task::data::Data,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        use apalis_core::backend::TaskSink;
        let pool = PgPool::connect(
            env::var("DATABASE_URL")
                .unwrap_or("postgres://postgres:postgres@localhost/apalis_dev".to_owned())
                .as_str(),
        )
        .await
        .unwrap();
        let mut backend = PostgresStorage::new(&pool);

        let mut items = stream::repeat_with(HashMap::default).take(1);
        backend.push_stream(&mut items).await.unwrap();

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
        use apalis_core::backend::TaskSink;
        let pool = PgPool::connect(
            env::var("DATABASE_URL")
                .unwrap_or("postgres://postgres:postgres@localhost/apalis_dev".to_owned())
                .as_str(),
        )
        .await
        .unwrap();
        let config = Config::new("test");
        let mut backend = PostgresStorage::new_with_notify(&pool, &config);

        let mut items = stream::repeat_with(|| {
            Task::builder(42u32)
                .with_ctx(PgContext::new().with_priority(1))
                .build()
        })
        .take(1);
        backend.push_all(&mut items).await.unwrap();

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

    #[tokio::test]
    async fn test_workflow_complete() {
        use apalis_core::backend::WeakTaskSink;
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = WorkFlow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Preprocessing input: {}",
                    input.text
                ))));
                let processed = input.text.to_lowercase();
                Ok::<_, WorkflowError>(processed)
            })
            // Step 2: Classify text
            .then(|text: String| async move {
                let confidence = 0.85; // pretend model confidence
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_string(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_string(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, WorkflowError>(results)
            })
            // Step 3: Filter out low-confidence predictions
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }

                    // pretend we run a sentiment model
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };
                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_string()),
                    })
                }
            })
            .then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                dbg!(&a);
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let pool = PgPool::connect(
            env::var("DATABASE_URL")
                .unwrap_or("postgres://postgres:postgres@localhost/apalis_dev".to_owned())
                .as_str(),
        )
        .await
        .unwrap();
        let config = Config::new("test");
        let mut backend: PostgresStorage<Vec<u8>> =
            PostgresStorage::new_with_config(&pool, &config);

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        backend.push(input).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .on_event(|ctx, ev| match ev {
                Event::Custom(msg) => {
                    if let Some(m) = msg.downcast_ref::<String>() {
                        println!("Custom Message: {m}");
                    }
                }
                Event::Error(_) => {
                    println!("On Error = {ev:?}");
                    ctx.stop().unwrap();
                }
                _ => {
                    println!("On Event = {ev:?}");
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
