use apalis_core::backend::future::BoxSyncFuture;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use serde::Deserialize;
use sqlx::postgres::{PgListener, PgNotification};
use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use crate::{PgTaskId, error::Error};

/// A standalone listener for `apalis::job::insert`
pub struct Pubsub {
    state: State,
    listener: Option<Mutex<BoxStream<'static, Result<PgNotification, Error>>>>,
    pool: sqlx::PgPool,
    namespace: String,
}

impl Clone for Pubsub {
    fn clone(&self) -> Self {
        Self {
            state: State::Starting,
            listener: None,
            pool: self.pool.clone(),
            namespace: self.namespace.clone(),
        }
    }
}

impl Pubsub {
    pub fn new(pool: sqlx::PgPool, namespace: String) -> Self {
        Self {
            state: State::Starting,
            pool,
            namespace,
            listener: None,
        }
    }
}

enum State {
    Starting,

    Connecting {
        fut: BoxSyncFuture<Result<PgListener, Error>>,
    },

    Listening,
    Closed,
}

/// A new event emitted when a new job is added
#[derive(Debug, Deserialize)]
pub struct InsertEvent {
    pub job_type: String,
    pub id: PgTaskId,
}

impl Stream for Pubsub {
    type Item = Result<PgTaskId, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Starting => {
                    this.state = State::Connecting {
                        fut: {
                            let pool = this.pool.clone();
                            let fut = Box::pin(async move {
                                let mut listener = PgListener::connect_with(&pool).await?;

                                listener.listen("apalis::job::insert").await?;

                                Ok(listener)
                            });
                            BoxSyncFuture::new(fut)
                        },
                    }
                }
                State::Connecting { fut } => {
                    let listener = match fut.poll_unpin(cx) {
                        Poll::Pending => return Poll::Pending,

                        Poll::Ready(Err(err)) => {
                            this.state = State::Closed;
                            return Poll::Ready(Some(Err(err)));
                        }

                        Poll::Ready(Ok(listener)) => listener,
                    };

                    this.listener = Some(Mutex::new(
                        listener.into_stream().map_err(|e| e.into()).boxed(),
                    ));

                    this.state = State::Listening;
                }

                State::Listening => {
                    let listener = this.listener.as_mut().expect("listener initialized");

                    match listener.get_mut().unwrap().as_mut().poll_next(cx) {
                        Poll::Pending => return Poll::Pending,

                        Poll::Ready(None) => {
                            this.state = State::Closed;
                            return Poll::Ready(None);
                        }

                        Poll::Ready(Some(Err(_))) => {
                            continue;
                        }

                        Poll::Ready(Some(Ok(notification))) => {
                            let Ok(ev) =
                                serde_json::from_str::<InsertEvent>(notification.payload())
                            else {
                                continue;
                            };

                            if ev.job_type != this.namespace {
                                continue;
                            }

                            return Poll::Ready(Some(Ok(ev.id)));
                        }
                    }
                }
                State::Closed => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}
