use std::time::Duration;

use apalis::{layers::retry::RetryPolicy, prelude::*};
use apalis_postgres::*;
use apalis_sql::ext::TaskBuilderExt;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let db = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPool::connect(&db).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new(&pool);

    // Push some tasks as a stream
    let mut start = 0usize;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = Task::builder(start)
            .run_after(Duration::from_secs(1))
            .priority(1)
            .max_attempts(5)
            .build();
        task
    })
    .take(10);
    backend.push_all(&mut items).await.unwrap();

    async fn send_reminder(item: usize, _wrk: WorkerContext) -> Result<(), BoxDynError> {
        if item.is_multiple_of(3) {
            println!("Reminding about item: {} but failing", item);
            return Err("Failed to send reminder".into());
        }
        println!("Reminding about item: {}", item);
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .retry(RetryPolicy::retries(1))
        .build(send_reminder);
    worker.run().await.unwrap();
}
