use apalis_core::backend::{BackendExt, ConfigExt, queue::Queue};
use apalis_sql::context::SqlContext;
use ulid::Ulid;

pub use apalis_sql::config::*;

use crate::{CompactType, PostgresStorage};

impl<Args: Sync, D, F> ConfigExt for PostgresStorage<Args, CompactType, D, F>
where
    PostgresStorage<Args, CompactType, D, F>:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }
}
