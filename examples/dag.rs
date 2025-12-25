use apalis::prelude::*;
use apalis_postgres::*;
use apalis_workflow::*;

async fn get_name(user_id: u32) -> Result<String, BoxDynError> {
    Ok(user_id.to_string())
}

async fn get_age(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 20)
}

async fn get_address(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 100)
}

async fn collector(
    (name, age, address): (String, usize, usize),
    wrk: WorkerContext,
) -> Result<usize, BoxDynError> {
    let result = name.parse::<usize>()? + age + address;
    wrk.stop().unwrap();
    Ok(result)
}

#[tokio::main]
async fn main() {
    let dag_flow = DagFlow::new("user-etl-workflow");
    let get_name = dag_flow.node(get_name);
    let get_age = dag_flow.node(get_age);
    let get_address = dag_flow.node(get_address);
    dag_flow
        .node(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    dag_flow.validate().unwrap();

    println!("Executing workflow:\n{}", dag_flow);

    let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    PostgresStorage::setup(&pool).await.unwrap();
    let mut backend = PostgresStorage::new_with_config(&pool, &Config::new("test-workflow"));

    backend.push_start(vec![42u32, 43, 44]).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(dag_flow);

    worker.run().await.unwrap();
}
