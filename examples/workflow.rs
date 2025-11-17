use std::time::Duration;

use apalis::prelude::*;
use apalis_postgres::*;
use apalis_workflow::*;

#[tokio::main]
async fn main() {
    let workflow = Workflow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
        .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
        .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
        .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
        .delay_for(Duration::from_millis(1000))
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), BoxDynError>(())
        });

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new_with_config(&pool, &Config::new("test-workflow"));

    backend.push_start(100usize).await.unwrap();

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
