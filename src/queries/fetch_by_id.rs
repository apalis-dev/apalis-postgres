use apalis_core::{
    backend::{BackendExt, FetchById, codec::Codec},
    task::task_id::TaskId,
};

use apalis_sql::from_row::{FromRowError, TaskRow};
use ulid::Ulid;

use crate::{CompactType, PgContext, PgTask, PostgresStorage, from_row::PgTaskRow};

impl<Args, D, F> FetchById<Args> for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        id: &TaskId<Ulid>,
    ) -> impl Future<Output = Result<Option<PgTask<Args>>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let id = id.to_string();
        async move {
            let task = sqlx::query_file_as!(PgTaskRow, "queries/task/find_by_id.sql", id)
                .fetch_optional(&pool)
                .await?
                .map(|r: PgTaskRow| {
                    let row: TaskRow = r.try_into()?;
                    row.try_into_task_compact()
                        .and_then(|a| {
                            a.try_map(|t| D::decode(&t))
                                .map_err(|e| FromRowError::DecodeError(e.into()))
                        })
                        .map_err(|e| sqlx::Error::Protocol(e.to_string()))
                })
                .transpose()?;
            Ok(task)
        }
    }
}
