# apalis-postgres

Background task processing in rust using `apalis` and `postgres`

## Features

- **Reliable job queue** using Postgres as the backend.
- **Multiple storage types**: standard polling and `trigger` based storages.
- **Custom codecs** for serializing/deserializing job arguments as bytes.
- **Heartbeat and orphaned job re-enqueueing** for robust task processing.
- **Integration with `apalis` workers and middleware.**

## Storage Types

- [`PostgresStorage`]: Standard polling-based storage.
- [`PostgresStorageWithListener`]: Event-driven storage using Postgres `NOTIFY` for low-latency job fetching.
- [`SharedPostgresStorage`]: Shared storage for multiple job types, uses Postgres `NOTIFY`.

The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `PostgresStorage` struct with different configurations.

## Examples

### Basic Worker Example

```rust,no_run
#[tokio::main]
async fn main() {
    let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new(&pool);

    let mut start = 0;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = Task::builder(start)
            .run_after(Duration::from_secs(1))
            .with_ctx(SqlContext::new().with_priority(1))
            .build();
        Ok(task)
    })
    .take(10);
    backend.send_all(&mut items).await.unwrap();

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
```

### `NOTIFY` listener example

```rust,no_run

#[tokio::main]
async fn main() {
    let pool = PostgresPool::connect(env!("DATABASE_URL")).await.unwrap();
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
                    .with_ctx(SqlContext::new().with_priority(start))
                    .build()
            })
            .take(20)
            .collect::<Vec<_>>()
            .await;
            apalis_postgres::sink::push_tasks(pool, config, items).await.unwrap();
        }
    });

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-2")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
```

### Workflow Example

```rust,no_run
#[tokio::main]
async fn main() {
    let workflow = WorkFlow::new("odd-numbers-workflow")
        .then(|a: usize| async move {
            Ok::<_, WorkflowError>((0..=a).collect::<Vec<_>>())
        })
        .filter_map(|x| async move {
            if x % 2 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 3 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 5 != 0 { Some(x) } else { None }
        })
        .delay_for(Duration::from_millis(1000))
        .then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), WorkflowError>(())
        });

    let pool = PostgresPool::connect(env!("DATABASE_URL")).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new_in_queue(&pool, "test-workflow");

    backend.push(100usize).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(workflow);

    worker.run().await.unwrap();
}
```

### Shared Example

This shows an example of multiple backends using the same connection.
This can improve performance if you have many types of jobs.

```rs
#[tokio::main]
async fn main() {
    let pool = PgPool::connect(env!("DATABASE_URL"))
        .await
        .unwrap();
    let mut store = SharedPostgresStorage::new(pool);

    let mut map_store = store.make_shared().unwrap();

    let mut int_store = store.make_shared().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T, I>(
        _: T,
        task_id: TaskId<I>,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        Ok(())
    }

    let int_worker = WorkerBuilder::new("rango-tango-2")
        .backend(int_store)
        .build(send_reminder);
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
}
```

## Observability

You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/task.png)

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
