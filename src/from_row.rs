use std::str::FromStr;

use apalis_core::{
    backend::codec::Codec,
    task::{attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::{PgTask, context::PgContext};

#[derive(Debug)]
pub(crate) struct TaskRow<Compact = Value> {
    pub(crate) job: Compact,
    pub(crate) id: Option<String>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i32>,
    pub(crate) max_attempts: Option<i32>,
    pub(crate) run_at: Option<DateTime<Utc>>,
    pub(crate) last_error: Option<String>,
    pub(crate) lock_at: Option<DateTime<Utc>>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<DateTime<Utc>>,
    pub(crate) priority: Option<i32>,
    // pub(crate) meta: Option<Map<String, Value>>,
}

impl TaskRow {
    pub fn try_into_task<D: Codec<Args, Compact = Value>, Args>(
        self,
    ) -> Result<PgTask<Args>, sqlx::Error>
    where
        D::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut ctx = PgContext::default();
        ctx.set_lock_by(self.lock_by);
        ctx.set_max_attempts(self.max_attempts.unwrap_or(25));
        ctx.set_done_at(self.done_at.map(|d| d.timestamp()));
        ctx.set_last_result(self.last_error);
        ctx.set_priority(self.priority.unwrap_or(0));
        ctx.set_lock_at(self.lock_at.map(|d| d.timestamp()));
        let args = D::decode(&self.job).map_err(|e| sqlx::Error::Decode(e.into()))?;
        let task = TaskBuilder::new(args)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(
                self.attempts
                    .ok_or(sqlx::Error::ColumnNotFound("attempts".to_owned()))?
                    as usize,
            ))
            .with_status(
                self.status
                    .ok_or(sqlx::Error::ColumnNotFound("status".to_owned()))
                    .and_then(|s| {
                        Status::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .with_task_id(
                self.id
                    .ok_or(sqlx::Error::ColumnNotFound("task_id".to_owned()))
                    .and_then(|s| {
                        TaskId::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .run_at_timestamp(
                self.run_at
                    .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                    .timestamp() as u64,
            );
        Ok(task.build())
    }
}
