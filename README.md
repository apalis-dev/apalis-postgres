# apalis-postgres

Background task processing in rust using `apalis` and `postgres`

## Features

- **Reliable job queue** using Postgres as the backend.
- **Multiple storage types**: standard polling and `trigger` based storages.
- **Custom codecs** for serializing/deserializing job arguments as bytes.
- **Heartbeat and orphaned job re-enqueueing** for robust task processing.
- **Integration with `apalis` workers and middleware.**
- **Observability**: Monitor and manage tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).

## Storage Types

- [`PostgresStorage`]: Standard polling-based storage.
- [`PostgresStorageWithListener`]: Event-driven storage using Postgres `NOTIFY` for low-latency job fetching.
- [`SharedPostgresStorage`]: Shared storage for multiple job types, uses Postgres `NOTIFY`.

The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `PostgresStorage` struct with different configurations.

## Examples

### Basic Worker Example

```rust,no_run
use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
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
use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
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
                // Construct compact task
                Task::builder(serde_json::to_vec(&start).unwrap())
                    .with_ctx(PgContext::new().with_priority(start))
                    .build()
            })
            .take(20)
            .collect::<Vec<_>>()
            .await;
            // You can still use backend.push
            // This example shows how to do it with just a pool
            apalis_postgres::sink::push_tasks(&pool, config, items).await.unwrap();
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
use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use apalis_workflow::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let workflow = Workflow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move {
            Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>())
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
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), BoxDynError>(())
        });

    let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new_with_config(&pool, &Config::new("test-workflow"));

    backend.push_start(100usize).await.unwrap();

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

```rust,no_run
use std::{collections::HashMap, time::Duration};

use apalis::prelude::*;
use apalis_postgres::{shared::SharedPostgresStorage, *};
use futures::stream;

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
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
        _task_id: TaskId<I>,
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

Track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## Upgrading to 1.0

1.0 confines everything apalis creates to the `apalis` schema. Two things move out of `public`:

- The sqlx migrations table is now tracked in `apalis._sqlx_migrations` (configured in `sqlx.toml`) instead of `public._sqlx_migrations`. This also keeps apalis's migration history from colliding with your own sqlx migrations on the same database.
- `generate_ulid()` is now `apalis.generate_ulid()` and no longer depends on the `pgcrypto` extension — its random bytes come from core `gen_random_uuid()`. The `public.generate_ulid()` copy is dropped.

### Existing databases: one-time manual step

This applies to **every** way of applying migrations — `PostgresStorage::setup()`, sqlx-cli, copied migration files, or a merged `Migrator`. Run this **once per database, before upgrading**:

```sql
-- Move apalis's existing migration history into the apalis schema.
ALTER TABLE public._sqlx_migrations SET SCHEMA apalis;

-- The first migration gained `IF NOT EXISTS` (so the apalis schema can be
-- created before the tracking table on fresh installs). Re-stamp its checksum
-- so the migrator doesn't reject it as modified:
UPDATE apalis._sqlx_migrations
   SET checksum = decode('d0839c6f57a379769dc27ccd581feb3d2709239c8f138e05271c9e3c760c4517a78a4d8912ab3d63b074b28d15ec74e9', 'hex')
 WHERE version = 20220530084123;
```

Run it **before** upgrading. If you upgrade first without it, the migrator re-runs the first migration against your existing objects and fails with e.g. `function "notify_new_jobs" already exists`. If you've already hit that failure, an empty `apalis._sqlx_migrations` may have been created, which makes the `ALTER TABLE` above fail because the name is taken — drop it first:

```sql
DROP TABLE apalis._sqlx_migrations;
```

then run the two statements above.

If you maintain your **own** `Migrator` (merging in `PostgresStorage::migrations()`), your tracking table stays where it is — skip the `ALTER TABLE` and run only the `UPDATE`, targeting your table name.

Fresh databases need none of this — `sqlx.toml` creates the `apalis` schema and tracking table for you.

### `pgcrypto`

apalis no longer uses `pgcrypto`. An earlier version installed it (usually in `public`); it is left untouched in case something else depends on it. If nothing else needs it, you can remove it:

```sql
DROP EXTENSION pgcrypto;
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
