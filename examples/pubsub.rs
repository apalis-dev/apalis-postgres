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

    let lazy_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_secs(5)))
        .build();
    let config = Config::new("queue")
        .with_poll_interval(lazy_strategy)
        .set_buffer_size(5);
    let backend = PostgresStorage::new_with_notify(&pool, &config);

    tokio::spawn({
        let pool = pool.clone();
        let config = config.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;
            let items = stream::repeat_with(move || {
                start += 1;
                Task::builder(serde_json::to_vec(&start).unwrap())
                    .run_after(Duration::from_secs(1))
                    .with_ctx(PgContext::new().with_priority(start))
                    .build()
            })
            .take(20)
            .collect::<Vec<_>>()
            .await;
            apalis_postgres::sink::push_tasks(pool, config, items)
                .await
                .unwrap();
        }
    });

    async fn send_reminder(_item: usize, _wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-2")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
