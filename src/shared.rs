use std::{
    collections::HashMap,
    future::ready,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    CompactType, Config, InsertEvent, PgContext, PgTask, PgTaskId, PostgresStorage,
    ack::{LockTaskLayer, PgAck},
    fetcher::PgPollFetcher,
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
};
use crate::{from_row::PgTaskRow, sink::PgSink};
use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue, shared::MakeShared},
    layers::Stack,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::from_row::TaskRow;
use futures::{
    FutureExt, SinkExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use sqlx::{PgPool, postgres::PgListener};
use ulid::Ulid;

pub struct SharedPostgresStorage<Compact = CompactType, Codec = JsonCodec<CompactType>> {
    pool: PgPool,
    registry: Arc<Mutex<HashMap<String, Sender<PgTaskId>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<(Compact, Codec)>,
}

impl SharedPostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        let registry: Arc<Mutex<HashMap<String, Sender<PgTaskId>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let p = pool.clone();
        let instances = registry.clone();
        Self {
            pool,
            drive: async move {
                let mut listener = PgListener::connect_with(&p).await.unwrap();
                listener.listen("apalis::job::insert").await.unwrap();
                listener
                    .into_stream()
                    .filter_map(|notification| {
                        let instances = instances.clone();
                        async move {
                            let pg_notification = notification.ok()?;
                            let payload = pg_notification.payload();
                            let ev: InsertEvent = serde_json::from_str(payload).ok()?;
                            let instances = instances.lock().await;
                            if instances.get(&ev.job_type).is_some() {
                                return Some(ev);
                            }
                            None
                        }
                    })
                    .for_each(|ev| {
                        let instances = instances.clone();
                        async move {
                            let mut instances = instances.lock().await;
                            let sender = instances.get_mut(&ev.job_type).unwrap();
                            sender.send(ev.id).await.unwrap();
                        }
                    })
                    .await;
            }
            .boxed()
            .shared(),
            registry,
            _marker: PhantomData,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum SharedPostgresError {
    /// Namespace not found
    #[error("namespace already exists: {0}")]
    NamespaceExists(String),

    /// Registry locked
    #[error("registry locked")]
    RegistryLocked,
}

impl<Args, Compact, Codec> MakeShared<Args> for SharedPostgresStorage<Compact, Codec> {
    type Backend = PostgresStorage<Args, Compact, Codec, SharedFetcher>;
    type Config = Config;
    type MakeError = SharedPostgresError;
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        Self::make_shared_with_config(self, Config::new(std::any::type_name::<Args>()))
    }
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size());
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedPostgresError::RegistryLocked)?;
        if r.insert(config.queue().to_string(), tx).is_some() {
            return Err(SharedPostgresError::NamespaceExists(
                config.queue().to_string(),
            ));
        }
        let sink = PgSink::new(&self.pool, &config);
        Ok(PostgresStorage {
            _marker: PhantomData,
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(Mutex::new(rx)),
            },
            pool: self.pool.clone(),
            sink,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SharedFetcher {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<Mutex<Receiver<PgTaskId>>>,
}

impl Stream for SharedFetcher {
    type Item = PgTaskId;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        let mut receiver = this.receiver.try_lock();
        if let Some(ref mut rx) = receiver {
            rx.poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }
}

impl<Args, Decode> Backend for PostgresStorage<Args, CompactType, Decode, SharedFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Unpin + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = TaskStream<PgTask<Args>, Self::Error>;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Context = PgContext;

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
        self.poll_shared(worker)
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

impl<Args, Decode> BackendExt for PostgresStorage<Args, CompactType, Decode, SharedFetcher>
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
        self.poll_shared(worker).boxed()
    }
}

impl<Args, Decode> PostgresStorage<Args, CompactType, Decode, SharedFetcher> {
    fn poll_shared(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<PgTask<CompactType>>, sqlx::Error>> + 'static {
        let pool = self.pool.clone();
        let worker_id = worker.name().to_owned();
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "SharedPostgresStorage",
        )
        .map_ok(|_| None);
        let register = stream::once(register_worker);
        let lazy_fetcher = self
            .fetcher
            .map(|t| t.to_string())
            .ready_chunks(self.config.buffer_size())
            .then(move |ids| {
                let pool = pool.clone();
                let worker_id = worker_id.clone();
                async move {
                    let mut tx = pool.begin().await?;
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
        register.chain(select(lazy_fetcher, eager_fetcher))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{backend::TaskSink, error::BoxDynError, worker::builder::WorkerBuilder};

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let mut store = SharedPostgresStorage::new(pool);

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        map_store
            .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
            .await
            .unwrap();
        int_store.push(99).await.unwrap();

        async fn send_reminder<T>(
            _: T,
            _task_id: PgTaskId,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let int_worker = WorkerBuilder::new("rango-tango-2")
            .backend(int_store)
            .build(send_reminder);
        let map_worker = WorkerBuilder::new("rango-tango-1")
            .backend(map_store)
            .build(send_reminder);
        tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
    }
}
