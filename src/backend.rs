use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        ext::poll_strategy::{PollWith, StreamStrategy},
        finalize::Durable,
        persistence::{Persisted, TaskPersistLayer},
    },
    features_table,
    worker::context::WorkerContext,
};
use serde_json::Value;
use sqlx::PgPool;
use ulid::Ulid;

use crate::{PgTask, config::Config, error::Error, persistence::SqlxPersistence, pubsub::Pubsub};

/// A backend for persisting and consuming jobs behind a postgres database
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_postgres::PostgresStorage;
        #   use sqlx::PgPool;
        #   let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str()).await.unwrap();
        #   PostgresStorage::setup(&pool).await.unwrap();
        #   PostgresStorage::<u32>::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    MakeShared => supported("Share one connection across multiple workers via [`PostgresStorageFactory`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
///
/// [`PostgresStorageFactory`]: crate::factory::PostgresStorageFactory
#[pin_project::pin_project]
pub struct PostgresStorage<Args> {
    #[pin]
    pub(crate) persistence: Persisted<SqlxPersistence>,
    codec: JsonCodec,
    _marker: PhantomData<Args>,
}

impl<Args> Clone for PostgresStorage<Args> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl PostgresStorage<()> {
    /// Runs the PostgreSQL storage migrations.
    ///
    /// ## Fresh databases
    ///
    /// No manual setup is required. Calling `setup()` will create the required
    /// tables and migration history.
    ///
    /// ## Upgrading to `1.0`
    ///
    /// > **⚠️ Important:** Existing databases created by a pre-`1.0` version
    /// > require a **one-time manual migration** before calling `setup()`.
    ///
    /// The `1.0` migration history is no longer relocated automatically by
    /// `setup()`. Follow the **"Upgrading to 1.0"** section in the README to
    /// perform the required transition.
    ///
    /// After the transition has been completed, `setup()` can be used normally
    /// for subsequent migrations.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use apalis_postgres::PostgresStorage;
    /// use sqlx::PgPool;
    ///
    /// # async fn run(pool: PgPool) -> Result<(), apalis_postgres::Error> {
    /// PostgresStorage::<()>::setup(&pool).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Errors
    ///
    /// Returns an error if the migrations cannot be applied to the database.
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &PgPool) -> Result<(), Error> {
        Self::migrations()
            .run(pool)
            .await
            .map_err(sqlx::Error::from)?;
        Ok(())
    }

    /// Get postgres migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<Args> PostgresStorage<Args> {
    /// Creates a new PostgresStorage instance.
    pub fn new(pool: &PgPool) -> Self {
        let config = Config::default().queue(std::any::type_name::<Args>());
        let persistence = Persisted::new(SqlxPersistence {
            config,
            pool: pool.clone(),
        });
        Self {
            _marker: PhantomData,
            codec: JsonCodec::default(),
            persistence,
        }
    }

    /// Mount a standalone [`Pubsub`] which uses its own connection under the hood
    pub fn with_pubsub(self) -> PollWith<Self, StreamStrategy<Pubsub>> {
        let pool = self.pool().clone();
        let config = self.config();
        let namespace = config.queue.to_string();
        PollWith::new(self, StreamStrategy::new(Pubsub::new(pool, namespace)))
    }

    /// Configure a new PostgresStorage instance.
    pub fn with_config(mut self, config: Config) -> Self {
        self.persistence.config = config;
        self
    }

    /// Returns a reference to the pool.
    pub fn pool(&self) -> &PgPool {
        &self.persistence.pool
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &Config {
        &self.persistence.config
    }
}

impl<Args> Backend for PostgresStorage<Args> {
    type Task = PgTask;
    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence
            .poll_ready(cx, worker, self.config().heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<PgTask, Self::Error>>> {
        self.persistence.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence.poll_close(cx, worker)
    }
}

impl<Args> BackendConfig for PostgresStorage<Args> {
    type Args = Args;

    type Kind = Durable;

    type Id = Ulid;

    type Config = Config;

    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.persistence.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        self.persistence
            .layer(JsonCodec::<Value>::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args> WireFormatBackend for PostgresStorage<Args> {
    type Codec = JsonCodec;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}
