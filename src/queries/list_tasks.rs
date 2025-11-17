use apalis_core::{
    backend::{BackendExt, Filter, ListAllTasks, ListTasks, codec::Codec},
    task::{Task, status::Status},
};
use apalis_sql::{
    context::SqlContext,
    from_row::{FromRowError, TaskRow},
};
use ulid::Ulid;

use crate::{CompactType, PgTask, PostgresStorage, from_row::PgTaskRow};

impl<Args, D, F> ListTasks<Args> for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn list_tasks(
        &self,
        queue: &str,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<PgTask<Args>>, Self::Error>> + Send {
        let queue = queue.to_string();
        let pool = self.pool.clone();
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
            .map(|r| {
                let row: TaskRow = r.try_into()?;
                row.try_into_task_compact()
                    .and_then(|a| {
                        a.try_map(|t| D::decode(&t))
                            .map_err(|e| FromRowError::DecodeError(e.into()))
                    })
                    .map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}

impl<Args, D, F> ListAllTasks for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send {
        let status = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(Status::Pending.to_string());
        let pool = self.pool.clone();
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
            .map(|r| {
                let row: TaskRow = r.try_into()?;
                row.try_into_task_compact()
                    .map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}
