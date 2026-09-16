use apalis_core::backend::{Backend, FetchById};

use crate::{PgTask, PgTaskId, PostgresStorage, error::Error, from_row::PgTaskRow};

impl<Args> FetchById for PostgresStorage<Args>
where
    Self: Backend<Error = Error, Task = PgTask>,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        id: &PgTaskId,
    ) -> impl Future<Output = Result<Option<Self::Task>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let id = id.to_string();
        async move {
            let task = sqlx::query_file_as!(PgTaskRow, "queries/task/find_by_id.sql", id)
                .fetch_optional(&pool)
                .await?
                .map(|r: PgTaskRow| r.try_into())
                .transpose()?;
            Ok(task)
        }
    }
}
