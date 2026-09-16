//! PostgreSQL backend factory with shared `LISTEN/NOTIFY` polling.
//!
//! [`PostgresStorageFactory`] creates independent PostgreSQL backends while
//! sharing a single PostgreSQL notification listener. Each backend is
//! registered by its queue name and receives task IDs for newly inserted
//! jobs belonging to that queue.
//!
//! This avoids creating a separate [`PgListener`] for every worker and allows
//! multiple queue types to share the same database connection pool.
//!
//! The factory creates [`PostgresStorage`] instances configured with
//! [`StreamStrategy`]. PostgreSQL notifications wake the corresponding
//! backend, which then performs its normal database polling.
//!
//! # Example
//!
//! ```no_run
//! use apalis::prelude::*;
//! use apalis_postgres::factory::PostgresStorageFactory;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect(
//!         &std::env::var("DATABASE_URL")?
//!     ).await?;
//!
//!     let mut factory = PostgresStorageFactory::new(pool);
//!
//!     let mut backend = factory.create()?;
//!
//!     backend.push(42).await?;
//!
//!     let worker = WorkerBuilder::new("numbers")
//!         .backend(backend)
//!         .build(|task: u64| async move {
//!             println!("processing {task}");
//!         });
//!
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! A factory can also create multiple queues. Each queue is independently
//! notified when a matching job is inserted:
//!
//! ```ignore
//! # use apalis::prelude::*;
//! # use apalis_postgres::factory::PostgresStorageFactory;
//! # use sqlx::PgPool;
//! # async fn example(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let mut factory = PostgresStorageFactory::new(pool);
//!
//! let emails = factory.create::<Email>()?;
//! let reports = factory.create::<Report>()?;
//! # Ok(())
//! # }
//! # struct Email;
//! # struct Report;
//! ```
//!
//! [`PostgresStorage`]: crate::PostgresStorage
//! [`PgListener`]: sqlx::postgres::PgListener
//! [`StreamStrategy`]: apalis_core::backend::ext::poll_strategy::StreamStrategy
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{PgTaskId, PostgresStorage, config::Config, pubsub::InsertEvent};
use apalis_core::backend::{
    BackendConfig,
    ext::{
        BackendExt,
        poll_strategy::{PollWith, StreamStrategy},
    },
    factory::BackendFactory,
};

use futures::{
    FutureExt, SinkExt, Stream, StreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
};
use sqlx::{PgPool, postgres::PgListener};

/// A factory for creating PostgreSQL-backed task queues.
///
/// `PostgresStorageFactory` maintains a shared PostgreSQL `LISTEN` connection
/// and routes job insertion notifications to the corresponding queue.
///
/// Each queue created by the factory is identified by its queue name. Multiple
/// backends can therefore share a single notification listener while retaining
/// independent task polling and processing.
///
/// # Example
///
/// ```ignore
/// use apalis_core::backend::factory::BackendFactory;
/// use apalis_postgres::factory::PostgresStorageFactory;
/// use sqlx::PgPool;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = PgPool::connect("postgres://localhost/apalis").await?;
/// let mut factory = PostgresStorageFactory::new(pool);
///
/// let backend = factory.create::<u64>()?;
/// # let _ = backend;
/// # Ok(())
/// # }
/// ```
pub struct PostgresStorageFactory {
    pool: PgPool,
    registry: Arc<Mutex<HashMap<String, Sender<PgTaskId>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
}

impl PostgresStorageFactory {
    /// Creates a new factory backed by the given PostgreSQL connection pool.
    ///
    /// A single PostgreSQL notification listener is shared by all backends
    /// created by this factory.
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
        }
    }
}

/// Errors returned when creating a PostgreSQL backend from a
/// [`PostgresStorageFactory`].
///
/// [`PostgresStorageFactory`]: crate::factory::PostgresStorageFactory
#[derive(Debug, thiserror::Error)]
pub enum PostgresFactoryError {
    /// Namespace not found
    #[error("namespace already exists: {0}")]
    NamespaceExists(String),

    /// Registry locked
    #[error("registry locked")]
    RegistryLocked,
}

impl<Args> BackendFactory<Args> for PostgresStorageFactory {
    type Backend = PollWith<PostgresStorage<Args>, StreamStrategy<SharedFetcher>>;
    type Error = PostgresFactoryError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error>
    where
        <Self::Backend as BackendConfig>::Config: Default,
    {
        self.create_with_config(Config::default().queue(std::any::type_name::<Args>()))
    }
    fn create_with_config(&mut self, config: Config) -> Result<Self::Backend, Self::Error> {
        let mut registry = self
            .registry
            .try_lock()
            .ok_or(PostgresFactoryError::RegistryLocked)?;

        let (tx, rx) = mpsc::channel(config.batch_size * registry.len());
        if registry.insert(config.queue.to_string(), tx).is_some() {
            return Err(PostgresFactoryError::NamespaceExists(
                config.queue.to_string(),
            ));
        }
        Ok(PostgresStorage::new(&self.pool)
            .with_config(config)
            .poll_with_stream(SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(Mutex::new(rx)),
            }))
    }
}

/// A stream of task IDs received from the shared PostgreSQL notification
/// listener.
///
/// `SharedFetcher` keeps the shared notification driver alive while exposing
/// notifications for a specific queue as a [`Stream`].
///
/// The fetcher does not perform database polling itself. Instead, it yields
/// task IDs received through PostgreSQL `LISTEN/NOTIFY`, allowing the backend's
/// polling strategy to use those notifications as a wake-up signal.
///
/// [`Stream`]: futures::Stream
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{
        backend::TaskSink,
        error::BoxDynError,
        worker::{builder::WorkerBuilder, context::WorkerContext},
    };
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let mut store = PostgresStorageFactory::new(pool);

        let mut map_store = store.create().unwrap();

        let mut int_store = store.create().unwrap();

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

        let int_worker = WorkerBuilder::new("rango-tango-3")
            .backend(int_store)
            .build(send_reminder);
        let map_worker = WorkerBuilder::new("rango-tango-4")
            .backend(map_store)
            .build(send_reminder);
        tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
    }
}
