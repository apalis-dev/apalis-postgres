use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new(&pool);

    let mut start = 0usize;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = Task::builder(start)
            .run_after(Duration::from_secs(1))
            .with_ctx(PgContext::new().with_priority(1))
            .build();
        task
    })
    .take(10);
    backend.push_all(&mut items).await.unwrap();

    async fn send_reminder(_item: usize, _wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
