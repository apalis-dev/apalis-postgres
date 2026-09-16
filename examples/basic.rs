use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::{Config, *};
use futures::stream::{self, StreamExt};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    let db = std::env::var("DATABASE_URL").unwrap();

    // Configure the pool options and set max connections to 50
    let pool = PgPoolOptions::new().connect(&db).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let config = Config::default()
        .queue("high-priority")
        .batch_size(100)
        .lock_tasks(false)
        .heartbeat_interval(Duration::from_secs(1))
        .missed_heartbeats(10);
    let mut backend = PostgresStorage::new(&pool)
        .with_config(config)
        .with_pubsub();

    // Push some tasks as a stream
    let mut start = 0usize;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = TaskBuilder::new(start)
            .run_after(Duration::from_secs(1))
            .priority(1)
            .max_attempts(5)
            .build();
        task
    })
    .take(10);
    backend.push_all(&mut items).await.unwrap();

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        if item == 10 {
            wrk.stop()?;
        }
        Ok(())
    }

    let worker = WorkerBuilder::new("basic-worker")
        .backend(backend)
        .parallelize(tokio::spawn)
        .build(send_reminder);

    worker.run().await.unwrap();
}
