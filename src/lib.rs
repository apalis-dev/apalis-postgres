#![doc = include_str!("../README.md")]
//!
//! [`PostgresStorageWithListener`]: crate::PostgresStorage
//! [`PostgresStorageFactory`]: crate::factory::PostgresStorageFactory

use apalis_core::task::{Task, task_id::TaskId};
mod backend;
mod config;
mod error;
pub mod factory;
mod from_row;
mod persistence;
mod pubsub;
pub mod queries;
mod sink;
mod timestamp;

pub use config::Config;
pub use error::Error;
pub use pubsub::{InsertEvent, Pubsub};

/// An alias for [Task], specialized for Postgres.
pub type PgTask<Args = Vec<u8>> = Task<Args>;
/// An alias for [TaskId] using [TaskId::Ulid], specialized for Postgres.
pub type PgTaskId = TaskId;
pub use crate::backend::PostgresStorage;
pub use sqlx::{PgPool, postgres::PgConnectOptions, postgres::PgConnection, postgres::PgListener};

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, time::Duration};

    use apalis_workflow::SteppedFlow;
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;

    use crate::config::Config;
    use apalis::prelude::*;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        use apalis_core::backend::TaskSink;
        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let config = Config::default()
            .queue("sample")
            .batch_size(50)
            .lock_tasks(false);
        let mut backend = PostgresStorage::new(&pool).with_config(config);

        let mut items = stream::repeat_with(HashMap::default).take(1);
        backend.push_stream(&mut items).await.unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }
    #[tokio::test]
    async fn notify_worker() {
        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let config = Config::default()
            .queue("test")
            .persist_results(true)
            .lock_tasks(false)
            .batch_size(20);
        let backend = PostgresStorage::new(&pool)
            .with_config(config)
            .with_pubsub();

        let mut b = backend.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let task = TaskBuilder::new(42u32).priority(1).build();
            b.push_task(task).await.unwrap();
        });

        async fn send_reminder(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
            wrk.stop().unwrap();
            Ok(())
        }

        let ctx = WorkerContext::new("rango-tango-2");
        let worker = WorkerBuilder::new(&ctx)
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
        let run_for = ctx.elapsed();
        assert!(
            run_for < Duration::from_secs(4),
            "Worker did not use notify mechanism"
        );
    }

    #[tokio::test]
    async fn test_workflow_complete() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = SteppedFlow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, worker: WorkerContext| async move {
                worker.emit(format!("Preprocessing input: {}", input.text));
                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            // Step 2: Classify text
            .and_then(|text: String| async move {
                let confidence = 0.85; // pretend model confidence
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_string(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_string(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, BoxDynError>(results)
            })
            // Step 3: Filter out low-confidence predictions
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }

                    // pretend we run a sentiment model
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };
                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_string()),
                    })
                }
            })
            .and_then(|a: Vec<Summary>, worker: WorkerContext| async move {
                dbg!(&a);
                worker.emit(format!("Generated {} summaries", a.len()));
                worker.stop()
            });

        let pool = PgPool::connect(env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap();
        let config = Config::default().queue("test");
        let mut backend = PostgresStorage::new(&pool)
            .with_config(config)
            .with_pubsub();

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        backend.push(input).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .on_event(|ctx, ev| match ev {
                Event::Custom(msg) => {
                    if let Some(m) = msg.downcast_ref::<String>() {
                        println!("Custom Message: {m}");
                    }
                }
                Event::Error(_) => {
                    println!("On Error = {ev:?}");
                    ctx.stop().unwrap();
                }
                _ => {
                    println!("On Event = {ev:?}");
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
