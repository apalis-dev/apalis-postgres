use std::{collections::HashMap, str::FromStr};

use apalis_core::task::{
    builder::TaskBuilder,
    metadata::MetadataStore,
    status::Status,
    task_id::{TaskId, TaskIdError::Decode},
};
use sqlx::postgres::types::PgHstore;
use ulid::Ulid;

use crate::{PgTask, error::Error, timestamp::Timestamp};

#[derive(Debug)]
pub struct PgTaskRow {
    pub job: Option<Vec<u8>>,
    pub id: Option<String>,
    pub job_type: Option<String>,
    pub status: Option<String>,
    pub attempts: Option<i32>,
    pub max_attempts: Option<i32>,
    pub run_at: Option<Timestamp>,
    #[allow(unused)]
    pub last_result: Option<serde_json::Value>,
    pub lock_at: Option<Timestamp>,
    pub lock_by: Option<String>,
    pub done_at: Option<Timestamp>,
    pub priority: Option<i32>,
    pub idempotency_key: Option<String>,
    pub metadata: Option<PgHstore>,
}

impl TryInto<PgTask<Vec<u8>>> for PgTaskRow {
    type Error = Error;

    fn try_into(self) -> Result<PgTask<Vec<u8>>, Self::Error> {
        let mut task = TaskBuilder::new(
            self.job
                .ok_or_else(|| sqlx::Error::ColumnNotFound("job".into()))?,
        )
        .task_id({
            let task_id = self
                .id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("task_id".into()))?;
            TaskId::from_ulid(
                Ulid::from_string(&task_id)
                    .map_err(|e| Error::TaskIdError(Decode(e.to_string())))?,
            )
        })
        .queue(
            self.job_type
                .ok_or_else(|| sqlx::Error::ColumnNotFound("job_type".into()))?
                .into(),
        )
        .status(
            Status::from_str(
                &self
                    .status
                    .ok_or_else(|| sqlx::Error::ColumnNotFound("status".into()))?,
            )
            .map_err(Error::StatusError)?,
        )
        .attempt(
            self.attempts
                .ok_or_else(|| sqlx::Error::ColumnNotFound("attempts".into()))?
                as usize,
        )
        .max_attempts(self.max_attempts.map(|v| v as usize).unwrap_or(25))
        .run_at_timestamp(
            self.run_at
                .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                .0,
        )
        .lock_at(self.lock_at.map(|dt| dt.0))
        .done_at(self.done_at.map(|dt| dt.0))
        .lock_by(self.lock_by)
        .priority(self.priority.map(|v| v as usize).unwrap_or_default())
        .with_metadata(
            self.metadata
                .map(|meta| {
                    meta.into_iter()
                        .map(|(k, v)| (k, v.unwrap()))
                        .collect::<HashMap<String, String>>()
                })
                .map(MetadataStore::from_map)
                .unwrap_or_default(),
        );

        if let Some(idempotency_key) = self.idempotency_key {
            task = task.idempotency_key(idempotency_key);
        }

        Ok(task.build())
    }
}
