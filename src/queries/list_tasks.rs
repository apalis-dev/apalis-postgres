use apalis_core::{
    backend::{Backend, Filter, ListAllTasks, ListTasks},
    task::{Task, status::Status},
};

use crate::from_row::PgTaskRow;
use crate::{PgTask, PostgresStorage, error::Error};

impl<Args> ListTasks for PostgresStorage<Args>
where
    PostgresStorage<Args>: Backend<Error = Error>,
    Args: 'static,
{
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<PgTask>, Self::Error>> + Send {
        let queue = self.persistence.config.queue.to_string();
        let pool = self.persistence.pool.clone();
        let limit = filter.limit() as i64;
        let offset = filter.offset() as i64;
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();
        async move {
            let tasks = sqlx::query_file_as!(
                PgTaskRow,
                "queries/backend/list_jobs.sql",
                status,
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}

impl<Args> ListAllTasks for PostgresStorage<Args>
where
    PostgresStorage<Args>: Backend<Error = Error>,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<Task<Self::Compact>>, Self::Error>> + Send {
        let status = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(Status::Pending.to_string());
        let pool = self.persistence.pool.clone();
        let limit = filter.limit() as i64;
        let offset = filter.offset() as i64;
        async move {
            let tasks = sqlx::query_file_as!(
                PgTaskRow,
                "queries/backend/list_all_jobs.sql",
                status,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}
