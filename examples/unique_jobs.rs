use apalis::prelude::*;
use apalis_postgres::PostgresStorage;
use sqlx::PgPool;

#[tokio::main]
async fn main() {
    let dedupe_key = "c76b5ceb-1538-411d-b755-f654e4f53680";

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new(&pool);

    let task_1 = TaskBuilder::new(42)
        .with_idempotency_key(dedupe_key)
        .build();

    let task_2 = TaskBuilder::new(43)
        .with_idempotency_key(dedupe_key)
        .build();

    backend.push_task(task_1).await.unwrap();
    backend.push_task(task_2).await.unwrap();

    async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        apalis_core::timer::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(task, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, e| {
            println!("{e:?}");
        })
        .build(task);
    worker.run().await.unwrap();
}
