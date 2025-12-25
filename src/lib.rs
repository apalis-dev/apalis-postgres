#![doc = include_str!("../README.md")]
//!
//! [`PostgresStorageWithListener`]: crate::PostgresStorage
//! [`SharedPostgresStorage`]: crate::shared::SharedPostgresStorage
use std::{fmt::Debug, marker::PhantomData};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    features_table,
    layers::Stack,
    task::{Task, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
pub use apalis_sql::{config::Config, from_row::TaskRow};
use futures::{
    StreamExt, TryFutureExt, TryStreamExt,
    future::ready,
    stream::{self, BoxStream, select},
};
use serde::Deserialize;
pub use sqlx::{PgPool, postgres::PgConnectOptions, postgres::PgListener, postgres::Postgres};
use ulid::Ulid;

pub use crate::{
    ack::{LockTaskLayer, PgAck},
    fetcher::{PgFetcher, PgPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::PgSink,
};

mod ack;
mod fetcher;
mod from_row;

pub type PgContext = apalis_sql::context::SqlContext<PgPool>;
mod queries;
pub mod shared;
pub mod sink;

pub type PgTask<Args> = Task<Args, PgContext, Ulid>;

pub type PgTaskId = TaskId<Ulid>;

pub type CompactType = Vec<u8>;

#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_postgres::PostgresStorage;
        #   use sqlx::PgPool;
        #   let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str()).await.unwrap();
        #   PostgresStorage::setup(&pool).await.unwrap();
        #   PostgresStorage::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    MakeShared => supported("Share one connection across multiple workers via [`SharedPostgresStorage`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
///
/// [`SharedPostgresStorage`]: crate::shared::SharedPostgresStorage
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

/// A fetcher that does nothing, used for notify-based storage
#[derive(Debug, Clone, Default)]
pub struct PgNotify {
    _private: PhantomData<()>,
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

    type IdType = Ulid;

    type Context = PgContext;

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
        self.poll_basic(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode> BackendExt
    for PostgresStorage<Args, CompactType, Decode, PgFetcher<Args, CompactType, Decode>>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Compact = CompactType;

    type Codec = Decode;
    type CompactStream = TaskStream<PgTask<CompactType>, Self::Error>;

    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }
    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_basic(worker).boxed()
    }
}

impl<Args, Decode> PostgresStorage<Args, CompactType, Decode, PgFetcher<Args, CompactType, Decode>>
where
    Args: Send + 'static + Unpin,
{
    fn poll_basic(&self, worker: &WorkerContext) -> TaskStream<PgTask<CompactType>, sqlx::Error> {
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "PostgresStorage",
        )
        .map_ok(|_| None);
        let register = stream::once(register_worker);
        register
            .chain(PgPollFetcher::<CompactType>::new(
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

    type IdType = Ulid;

    type Context = PgContext;

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
        self.poll_with_notify(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode> BackendExt for PostgresStorage<Args, CompactType, Decode, PgNotify>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Unpin + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Compact = CompactType;

    type Codec = Decode;
    type CompactStream = TaskStream<PgTask<CompactType>, Self::Error>;

    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_with_notify(worker).boxed()
    }
}

impl<Args, Decode> PostgresStorage<Args, CompactType, Decode, PgNotify> {
    pub fn poll_with_notify(
        &self,
        worker: &WorkerContext,
    ) -> TaskStream<PgTask<CompactType>, sqlx::Error> {
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
        .map_ok(|_| None);
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
                        "queries/task/queue_by_id.sql",
                        &ids,
                        &worker_id
                    )
                    .fetch(&mut *tx)
                    .map(|r| {
                        let row: TaskRow = r?.try_into()?;
                        Ok(Some(
                            row.try_into_task_compact()
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

        let eager_fetcher = StreamExt::boxed(PgPollFetcher::<CompactType>::new(
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
    id: PgTaskId,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        env,
        time::{Duration, Instant},
    };

    use apalis_workflow::Workflow;
    use apalis_workflow::WorkflowSink;

    use apalis_core::{
        backend::poll_strategy::{IntervalStrategy, StrategyBuilder},
        error::BoxDynError,
        task::data::Data,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        use apalis_core::backend::TaskSink;
        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
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
        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let config = Config::new("test").with_poll_interval(
            StrategyBuilder::new()
                .apply(IntervalStrategy::new(Duration::from_secs(6)))
                .build(),
        );
        let backend = PostgresStorage::new_with_notify(&pool, &config);

        let mut b = backend.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut items = stream::repeat_with(|| {
                Task::builder(42u32)
                    .with_ctx(PgContext::new().with_priority(1))
                    .build()
            })
            .take(1);
            b.push_all(&mut items).await.unwrap();
        });

        async fn send_reminder(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
            wrk.stop().unwrap();
            Ok(())
        }

        let instant = Instant::now();
        let worker = WorkerBuilder::new("rango-tango-2")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
        let run_for = instant.elapsed();
        assert!(
            run_for < Duration::from_secs(4),
            "Worker did not use notify mechanism"
        );
    }

    #[tokio::test]
    async fn test_workflow_complete() {
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

        let workflow = Workflow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Preprocessing input: {}",
                    input.text
                ))));
                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            // Step 2: Classify text
            .and_then(|text: String| async move {
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
                Ok::<_, BoxDynError>(results)
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
            .and_then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                dbg!(&a);
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let config = Config::new("test").with_poll_interval(
            StrategyBuilder::new()
                .apply(IntervalStrategy::new(Duration::from_secs(1)))
                .build(),
        );
        let mut backend = PostgresStorage::new_with_notify(&pool, &config);

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        backend.push_start(input).await.unwrap();

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
