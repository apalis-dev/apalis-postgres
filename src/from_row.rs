use apalis_sql::{DateTime, TaskRow};

#[derive(Debug)]
pub struct PgTaskRow {
    pub job: Option<Vec<u8>>,
    pub id: Option<String>,
    pub job_type: Option<String>,
    pub status: Option<String>,
    pub attempts: Option<i32>,
    pub max_attempts: Option<i32>,
    pub run_at: Option<DateTime>,
    pub last_result: Option<serde_json::Value>,
    pub lock_at: Option<DateTime>,
    pub lock_by: Option<String>,
    pub done_at: Option<DateTime>,
    pub priority: Option<i32>,
    pub metadata: Option<serde_json::Value>,
}
impl TryInto<TaskRow> for PgTaskRow {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<TaskRow, Self::Error> {
        Ok(TaskRow {
            job: self.job.unwrap_or_default(),
            id: self
                .id
                .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?,
            job_type: self
                .job_type
                .ok_or_else(|| sqlx::Error::Protocol("Missing job_type".into()))?,
            status: self
                .status
                .ok_or_else(|| sqlx::Error::Protocol("Missing status".into()))?,
            attempts: self
                .attempts
                .ok_or_else(|| sqlx::Error::Protocol("Missing attempts".into()))?
                as usize,
            max_attempts: self.max_attempts.map(|v| v as usize),
            run_at: self.run_at,
            last_result: self.last_result,
            lock_at: self.lock_at,
            lock_by: self.lock_by,
            done_at: self.done_at,
            priority: self.priority.map(|v| v as usize),
            metadata: self.metadata,
        })
    }
}
