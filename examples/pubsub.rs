use std::time::{Duration, Instant};

use apalis::prelude::*;
use apalis_postgres::{Config, *};
use sqlx::pool::PoolOptions;
use tracing::{Instrument, Level, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    use tracing_subscriber::{EnvFilter, fmt};
    let fmt_layer = fmt::layer();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("trace"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let pool = PoolOptions::new()
        .connect_lazy(&std::env::var("DATABASE_URL").unwrap())
        .unwrap();

    PostgresStorage::setup(&pool).await.unwrap();

    let queue = "queue";
    let config = Config::default().queue(queue).batch_size(10);

    let backend = PostgresStorage::new(&pool)
        .with_config(config)
        .with_pubsub()
        .poll_with_interval(Duration::from_secs(10))
        .instrumented(tracing::span!(Level::INFO, "postgres-pubsub"));

    tokio::spawn({
        let pool = pool.clone();

        async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let mut conn = pool.acquire().await.unwrap().detach();
            let mut start = 0;
            while start < 100 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                start += 1;
                let tasks = TaskBuilder::new(serde_json::to_vec(&start).unwrap())
                    .priority(start)
                    .build();

                apalis_postgres::queries::push_tasks(&mut conn, queue, vec![tasks])
                    .await
                    .unwrap();
            }
        }
    });

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        info!("Found Item: {item}");
        if item == 10 {
            wrk.stop()?;
        }
        Ok(())
    }

    let start = Instant::now();
    let worker = WorkerBuilder::new("worker-2")
        .backend(backend)
        .enable_tracing()
        .on_event(|_, e| info!("{:?}", e))
        .build(send_reminder);
    worker
        .run()
        .instrument(tracing::span!(Level::INFO, "worker-2"))
        .await
        .unwrap();

    info!("Elapsed: {:?}", start.elapsed());
}
