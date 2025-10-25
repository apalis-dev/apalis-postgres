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
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    layers::Stack,
    task::{Parts, Task, attempt::Attempt, status::Status, task_id::TaskId},
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use apalis_sql::from_row::TaskRow;
use chrono::{DateTime, Utc};
use futures::{
    FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
    channel::mpsc::{Receiver, Sender},
    future::{BoxFuture, ready},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, postgres::PgListener};
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
mod config;
mod fetcher;
mod from_row {
    use chrono::{DateTime, Utc};

    // pub type TaskRow = apalis_sql::from_row::TaskRow<serde_json::Value>;

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
mod context {
    pub type PgContext = apalis_sql::context::SqlContext;
}
mod queries;
pub mod shared;
mod sink;

pub type PgTask<Args> = Task<Args, PgContext, Ulid>;

pub type CompactType = Vec<u8>;

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

impl<Args> PostgresStorage<Args> {
    /// Creates a new PostgresStorage instance.
    pub fn new(pool: PgPool, config: Config) -> Self {
        let sink = PgSink::new(&pool, &config);
        Self {
            _marker: PhantomData,
            pool,
            config,
            fetcher: PgFetcher {
                _marker: PhantomData,
            },
            sink,
        }
    }

    pub async fn new_with_notify(
        pool: PgPool,
        config: Config,
    ) -> PostgresStorage<Args, CompactType, JsonCodec<CompactType>, PgListener> {
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

impl<Args, Decode> Backend for PostgresStorage<Args, CompactType, Decode, PgListener>
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
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "PostgresStorageWithNotify",
        )
        .map(|_| Ok(None));
        let register = stream::once(register_worker);
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
    use std::{env, time::Duration};

    use apalis_workflow::{WorkFlow, WorkflowError};
    use chrono::Local;

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
        let pool = PgPool::connect(
            env::var("DATABASE_URL")
                .unwrap_or("postgres://postgres:postgres@localhost/apalis_dev".to_owned())
                .as_str(),
        )
        .await
        .unwrap();
        let mut backend = PostgresStorage::new(pool, Default::default());

        let mut items = stream::repeat_with(|| HashMap::default()).take(1);
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
        let mut backend = PostgresStorage::new_with_notify(pool, config).await;

        let mut items = stream::repeat_with(|| {
            let task = Task::builder(42u32)
                .with_ctx(PgContext::new().with_priority(1))
                .build();
            task
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
        let mut backend: PostgresStorage<Vec<u8>> = PostgresStorage::new(pool, config);

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
                        println!("Custom Message: {}", m);
                    }
                }
                Event::Error(_) => {
                    println!("On Error = {:?}", ev);
                    ctx.stop().unwrap();
                }
                _ => {
                    println!("On Event = {:?}", ev);
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
