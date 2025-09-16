use apalis_core::{
    error::BoxDynError,
    task::{Parts, status::Status},
    worker::ext::ack::Acknowledge,
};
use chrono::Utc;
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use sqlx::PgPool;
use ulid::Ulid;

use crate::context::PgContext;

#[derive(Clone)]
pub struct PgAck {
    pool: PgPool,
}
impl PgAck {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl<Res: Serialize> Acknowledge<Res, PgContext, Ulid> for PgAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<PgContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id.clone();
        let worker_id = parts.ctx.lock_by().clone();

        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));
        let status = calculate_status(parts, res);
        let attempt = parts.attempt.current() as i32;
        let now = Utc::now();
        let pool = self.pool.clone();
        async move {
            let res = sqlx::query_file!(
                "src/queries/task/ack.sql",
                task_id
                    .ok_or(sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                    .to_string(),
                attempt,
                &response.map_err(|e| sqlx::Error::Decode(e.into()))?,
                status.to_string(),
                worker_id.ok_or(sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?
            )
            .execute(&pool)
            .await?;

            if res.rows_affected() == 0 {
                return Err(sqlx::Error::RowNotFound);
            }
            Ok(())
        }
        .boxed()
    }
}

pub fn calculate_status<Res>(
    parts: &Parts<PgContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match &res {
        Ok(_) => Status::Done,
        Err(e) => match &e {
            // Error::Abort(_) => State::Killed,
            e if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            _ => Status::Failed,
        },
    }
}
