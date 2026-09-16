use std::{collections::HashMap, time::Duration};

use apalis::prelude::*;
use apalis_postgres::{Config, factory::PostgresStorageFactory, *};
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    stream::{self, FuturesUnordered},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    use tracing_subscriber::{EnvFilter, fmt};
    let fmt_layer = fmt::layer();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();

    let config = Config::default()
        .queue("int-store")
        .batch_size(1)
        .heartbeat_interval(Duration::from_secs(1))
        .missed_heartbeats(10);
    let mut store = PostgresStorageFactory::new(pool);

    let mut map_store = store.create().unwrap();

    let mut int_store = store.create_with_config(config).unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    let range = 0..10;
    let mut stream = stream::iter(range).map(|i| i);
    int_store.push_stream(&mut stream).await.unwrap();
    async fn send_reminder<T>(
        _: T,
        _task_id: TaskId,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        Ok(())
    }

    let workers = FuturesUnordered::new();
    for i in 0..5 {
        let worker = WorkerBuilder::new(format!("worker-{}", i))
            .backend(int_store.clone())
            .build(send_reminder);
        workers.push(worker.run().boxed());
    }
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    workers.push(map_worker.run().boxed());

    workers.try_collect::<Vec<_>>().await.unwrap();
}
