use apalis_core::worker::context::WorkerContext;
use apalis_sql::{SqlDateTime, SqlDateTimeExt};
use futures::{FutureExt, Stream, stream};
use sqlx::PgPool;

use crate::{
    Config,
    queries::{
        reenqueue_orphaned::reenqueue_orphaned, register_worker::register as register_worker,
    },
};

pub async fn keep_alive(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
) -> Result<(), sqlx::Error> {
    let worker = worker.name().to_owned();
    let queue = config.queue().to_string();
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(&pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "WORKER_DOES_NOT_EXIST",
        )));
    }
    Ok(())
}

pub async fn initial_heartbeat(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    reenqueue_orphaned(pool.clone(), config.clone()).await?;
    let last_seen = SqlDateTime::now();
    register_worker(
        pool,
        config.queue().to_string(),
        worker,
        last_seen,
        storage_type,
    )
    .await?;
    Ok(())
}

pub fn keep_alive_stream(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
) -> impl Stream<Item = Result<(), sqlx::Error>> + Send {
    stream::unfold((), move |_| {
        let register = keep_alive(pool.clone(), config.clone(), worker.clone());
        let interval = apalis_core::timer::Delay::new(*config.keep_alive());
        interval.then(move |_| register.map(|res| Some((res, ()))))
    })
}
