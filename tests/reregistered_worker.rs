use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis::prelude::*;
use apalis_postgres::{Config, PgPool, PostgresStorage};

/// Returns the status and `lock_by` of the only job in `queue`.
async fn job_state(admin: &PgPool, queue: &str) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, lock_by FROM apalis.jobs WHERE job_type = $1")
        .bind(queue)
        .fetch_one(admin)
        .await
        .unwrap()
}

async fn never_finish(_: u32) -> Result<(), BoxDynError> {
    std::future::pending().await
}

async fn stop_after_task(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
    wrk.stop().unwrap();
    Ok(())
}

#[tokio::test]
async fn reregistered_worker_recovers_previous_jobs() {
    let url = env::var("DATABASE_URL").unwrap();
    let admin = PgPool::connect(&url).await.unwrap();
    PostgresStorage::setup(&admin).await.unwrap();

    // A unique queue and worker name keep runs of this test independent.
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = format!("reregister-{suffix}");
    let name = format!("reregister-worker-{suffix}");
    // Orphaned after 3s without a heartbeat.
    let config = Config::default()
        .queue(&queue)
        .heartbeat_interval(Duration::from_secs(1))
        .missed_heartbeats(3);

    // First run: lock the job, then die mid-job like a killed process. Its own pool stands in
    // for the process's connections, so closing it also releases its advisory lock.
    let first_pool = PgPool::connect(&url).await.unwrap();
    let mut backend = PostgresStorage::new(&first_pool).with_config(config.clone());
    backend.push(1u32).await.unwrap();
    let first_run = tokio::spawn(
        WorkerBuilder::new(name.as_str())
            .backend(backend)
            .build(never_finish)
            .run(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while job_state(&admin, &queue).await != ("Running".to_owned(), Some(name.clone())) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("job wasn't locked by the first run");
    first_run.abort();
    first_pool.close().await;

    // Second run: same name, started well within the 3s orphan timeout.
    let second_pool = PgPool::connect(&url).await.unwrap();
    let backend = PostgresStorage::new(&second_pool).with_config(config);
    let second_run = WorkerBuilder::new(name.as_str())
        .backend(backend)
        .build(stop_after_task)
        .run();

    // The worker only stops if the job is re-enqueued and its handler runs again.
    let result = tokio::time::timeout(Duration::from_secs(15), second_run).await;
    let (status, lock_by) = job_state(&admin, &queue).await;
    assert!(
        result.is_ok(),
        "job was never recovered: status {status}, lock_by {lock_by:?}"
    );
}
