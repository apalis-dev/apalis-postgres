//! Queries needed for polling, updating and exposing tasks.
mod fetch_by_id;
mod fetch_next;
mod handle_result;
mod keep_alive;
mod list_queues;
mod list_tasks;
mod list_workers;
mod lock_task;
mod metrics;
mod reenqueue_orphaned;
mod register_worker;
mod wait_for;

pub use crate::queries::{
    fetch_next::fetch_next, handle_result::Payload as ResultPayload, handle_result::handle_results,
    keep_alive::keep_alive, lock_task::lock_tasks, reenqueue_orphaned::reenqueue_abandoned,
    reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker,
};
pub use crate::sink::push_tasks;
