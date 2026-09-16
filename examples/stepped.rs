use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::{Config, PgPool, PgTaskId, PostgresStorage};
use apalis_workflow::SteppedFlow;

#[tokio::main]
async fn main() {
    let workflow = SteppedFlow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
        .delay_for(Duration::from_secs(5))
        .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
        .delay_for(Duration::from_millis(1000))
        .and_then(
            |a: Vec<usize>, wrk: WorkerContext, task_id: PgTaskId| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                wrk.stop().unwrap();
                println!("Completed Task ID: {}", task_id);
                Ok::<(), BoxDynError>(())
            },
        );

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let config = Config::default().queue("test-workflow");
    let mut backend = PostgresStorage::new(&pool)
        .with_config(config)
        .with_pubsub()
        .poll_with_interval(Duration::from_secs(1));

    backend.push(10usize).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(workflow);

    worker.run().await.unwrap();
}
