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
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
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
            drive: drive_listener(p, instances).boxed().shared(),
            registry,
        }
    }
}

/// Delay before reconnecting the notification listener after it fails.
const LISTENER_RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Runs the shared notification listener, reconnecting after failures until the pool closes.
///
/// Backends created by the factory are woken only by these notifications, so
/// the listener must outlive transient connection errors such as a database
/// restart.
async fn drive_listener(pool: PgPool, instances: Arc<Mutex<HashMap<String, Sender<PgTaskId>>>>) {
    loop {
        match listen(&pool, &instances).await {
            Err(sqlx::Error::PoolClosed) => return,
            Err(error) => {
                tracing::warn!(?error, "notification listener failed, reconnecting");
            }
            Ok(never) => match never {},
        }
        apalis_core::timer::sleep(LISTENER_RECONNECT_DELAY).await;
    }
}

/// Forwards job insert notifications to the matching backend until the listener fails.
async fn listen(
    pool: &PgPool,
    instances: &Mutex<HashMap<String, Sender<PgTaskId>>>,
) -> Result<Infallible, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("apalis::job::insert").await?;
    loop {
        let notification = listener.recv().await?;
        let Ok(ev) = serde_json::from_str::<InsertEvent>(notification.payload()) else {
            continue;
        };
        if let Some(sender) = instances.lock().await.get_mut(&ev.job_type) {
            // A closed channel only means that backend was dropped; keep serving the others.
            let _ = sender.send(ev.id).await;
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
                poller: Some(self.drive.clone()),
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
    // Cleared once the listener finishes, because polling a completed `Shared` again panics.
    poller: Option<Shared<BoxFuture<'static, ()>>>,
    receiver: Arc<Mutex<Receiver<PgTaskId>>>,
}

impl Stream for SharedFetcher {
    type Item = PgTaskId;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        if let Some(poller) = &mut this.poller
            && poller.poll_unpin(cx).is_ready()
        {
            this.poller = None;
        }

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
        error::{BoxDynError, WorkerError},
        monitor::Monitor,
        worker::{builder::WorkerBuilder, context::WorkerContext},
    };
    use futures::stream;

    use super::*;

    #[test]
    fn fetcher_keeps_polling_after_listener_finishes() {
        let (_tx, rx) = mpsc::channel(1);
        let mut fetcher = SharedFetcher {
            poller: Some(futures::future::ready(()).boxed().shared()),
            receiver: Arc::new(Mutex::new(rx)),
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());

        // The second poll used to panic with "Shared future polled again after completion".
        assert!(fetcher.poll_next_unpin(&mut cx).is_pending());
        assert!(fetcher.poll_next_unpin(&mut cx).is_pending());
        assert!(fetcher.poller.is_none());
    }

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

    /// Application name for the factory's connections, so the test can find and kill them.
    const TEST_APP: &str = "apalis-listener-reconnect-test";

    /// Returns the pid of the factory's `LISTEN` connection once it's connected, ignoring
    /// `previous`.
    async fn wait_for_listener(admin: &PgPool, previous: Option<i32>) -> i32 {
        let find = async {
            loop {
                let pid: Option<i32> = sqlx::query_scalar(
                    "SELECT pid FROM pg_stat_activity \
                     WHERE application_name = $1 AND query LIKE 'LISTEN %' \
                     AND pid IS DISTINCT FROM $2",
                )
                .bind(TEST_APP)
                .bind(previous)
                .fetch_optional(admin)
                .await
                .unwrap();
                if let Some(pid) = pid {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), find)
            .await
            .expect("listener didn't reconnect")
    }

    async fn stop_after_task(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
        wrk.stop().unwrap();
        Ok(())
    }

    /// Workers keep receiving jobs after the shared listener loses its connection and can't
    /// reconnect straight away, as when the database restarts.
    #[tokio::test]
    async fn worker_survives_listener_disconnect() {
        let url = std::env::var("DATABASE_URL").unwrap();
        let admin = PgPool::connect(&url).await.unwrap();

        // While `offline` is set, new connections from the factory's pool fail, so the pool
        // returns `PoolTimedOut`.
        let offline = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let options = url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .unwrap()
            .application_name(TEST_APP);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(500))
            .after_connect({
                let offline = offline.clone();
                move |_, _| {
                    let offline = offline.load(std::sync::atomic::Ordering::SeqCst);
                    Box::pin(async move {
                        if offline {
                            return Err(sqlx::Error::Protocol("database offline".to_owned()));
                        }
                        Ok(())
                    })
                }
            })
            .connect_with(options)
            .await
            .unwrap();

        let mut factory = PostgresStorageFactory::new(pool);
        let mut backend = factory
            .create_with_config(Config::default().queue("listener-disconnect"))
            .unwrap();
        // Restart workers after backend errors, as an application would, but not after panics.
        let monitor = Monitor::new()
            .should_restart(|_, err| matches!(err, WorkerError::BackendError(_)))
            .register({
                let backend = backend.clone();
                move |_| {
                    WorkerBuilder::new("listener-disconnect")
                        .backend(backend.clone())
                        .build(stop_after_task)
                }
            });
        let monitor = tokio::spawn(monitor.run());
        let listener = wait_for_listener(&admin, None).await;

        // Take the database "offline" for the factory: kill its connections and fail reconnects
        // until the listener's reconnect gives up.
        offline.store(true, std::sync::atomic::Ordering::SeqCst);
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
        )
        .bind(TEST_APP)
        .execute(&admin)
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        offline.store(false, std::sync::atomic::Ordering::SeqCst);

        wait_for_listener(&admin, Some(listener)).await;
        backend.push(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), monitor)
            .await
            .expect("worker didn't process the job")
            .unwrap()
            .unwrap();
    }
}
