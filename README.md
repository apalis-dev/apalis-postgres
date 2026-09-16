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
- [`PostgresStorageFactory`]: Shared storage for multiple job types, uses Postgres `NOTIFY`.

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
        let task = TaskBuilder::new(start)
            .run_after(Duration::from_secs(1))
            .priority(2)
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

### Pubsub listener example

Uses `LISTEN/NOTIFY` to subscribe to events. Each worker gets its own listener. To share a listener b
please use `PostgresStorageFactory`

```rust,no_run
use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(env!("DATABASE_URL")).await.unwrap();
    PostgresStorage::setup(&pool).await.unwrap();

    let lazy_strategy = Strategy::new()
        .interval(Duration::from_secs(5));
    let config = Config::default()
        .queue("my-queue")
        .batch_size(5);
    let backend = PostgresStorage::new(&pool)
        .with_config(config)
        .with_pubsub()
        .poll_with_strategy(lazy_strategy);

    tokio::spawn({
        let pool = pool.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;
            let items = stream::repeat_with(move || {
                start += 1;
                // Construct compact task
                TaskBuilder::new(serde_json::to_vec(&start).unwrap())
                    .priority(start)
                    .build()
            })
            .take(20)
            .collect::<Vec<_>>()
            .await;
            let mut tx = pool.begin().await.unwrap();
            apalis_postgres::queries::push_tasks(&mut *tx, "my-queue", items).await.unwrap();
            tx.commit().await.unwrap()
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
    let workflow = SteppedFlow::new("odd-numbers-workflow")
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
    let config = Config::default().queue("workflow");
    let mut backend = PostgresStorage::new(&pool).with_config(config);

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
use apalis_postgres::{factory::PostgresStorageFactory, *};
use futures::stream;

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut factory = PostgresStorageFactory::new(pool);

    let mut map_store = factory.create().unwrap();

    let mut int_store = factory.create().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T>(
        _: T,
        _task_id: TaskId,
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

Starting with `1.0`, `apalis-postgres` keeps everything it creates inside the `apalis` PostgreSQL schema.

This changes two things:

- **Migration history** — SQLx migrations are now tracked in `apalis._sqlx_migrations` instead of `public._sqlx_migrations`. This prevents apalis-postgres's migration history from colliding with migrations belonging to your application.
- **`generate_ulid()`** — The function is now `apalis.generate_ulid()` and no longer requires the `pgcrypto` extension. Its random bytes are generated using PostgreSQL's built-in `gen_random_uuid()`. The old `public.generate_ulid()` function is removed.

> **⚠️ Existing databases require a one-time migration.**
> If your database was created with a pre-`1.0` version of `apalis-postgres`, you must perform the migration below **before running any `1.0` migrations**.

### Existing databases — one-time migration

This applies regardless of how you run migrations: `PostgresStorage::setup()`, `sqlx-cli`, copied migration files, or a custom/merged `Migrator`.

Run this **once per database, before upgrading**:

```sql
-- Move apalis-postgres' migration history into the apalis schema.
ALTER TABLE public._sqlx_migrations SET SCHEMA apalis;

-- The first migration changed to use IF NOT EXISTS so that the apalis
-- schema can be created before the migration table on fresh installs.
-- Re-stamp its checksum to match the 1.0 migration.
UPDATE apalis._sqlx_migrations
   SET checksum = decode(
       'd0839c6f57a379769dc27ccd581feb3d2709239c8f138e05271c9e3c760c4517a78a4d8912ab3d63b074b28d15ec74e9',
       'hex'
   )
 WHERE version = 20220530084123;
```

> **❗ Do this before upgrading.**
> If you upgrade first, the migrator may not find the existing migration history and will attempt to re-run the first migration against objects that already exist, causing errors such as:
>
> ```log
> function "notify_new_jobs" already exists
> ```

#### If you already upgraded and the migration failed

The `apalis._sqlx_migrations` table may have been created (empty) before the migration failed. Remove it first:

```sql
DROP TABLE apalis._sqlx_migrations;
```

Then run the two statements from the [one-time migration](#existing-databases--one-time-migration) above.

#### Custom `Migrator`

If you maintain your own `Migrator` and merge in `PostgresStorage::migrations()`, your migration tracking table stays wherever your existing SQLx configuration puts it.

> **ℹ️ Note:** Do not move your migration table in this case. Skip the `ALTER TABLE` statement and only update the checksum in your existing `_sqlx_migrations` table:


```sql
UPDATE <your_schema>._sqlx_migrations
   SET checksum = decode(
       'd0839c6f57a379769dc27ccd581feb3d2709239c8f138e05271c9e3c760c4517a78a4d8912ab3d63b074b28d15ec74e9',
       'hex'
   )
 WHERE version = 20220530084123;
```

### Fresh databases

✅ No manual migration is required. On a fresh database, the `1.0` migrations automatically create the `apalis` schema and place apalis-postgres's migration history in `apalis._sqlx_migrations`.

### `pgcrypto`

apalis no longer uses `pgcrypto`. An earlier version installed it (usually in `public`); it is left untouched in case something else depends on it. If nothing else needs it, you can remove it:

```sql
DROP EXTENSION pgcrypto;
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
