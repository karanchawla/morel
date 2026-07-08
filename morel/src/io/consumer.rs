use std::error::Error;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

use futures_core::Stream as FuturesStream;
use futures_util::stream;
use futures_util::FutureExt;

use crate::core::{Ctx, Input, Operator, Output, Stream, Time};

use super::error::{panic_message, AsyncIoError};
use super::runtime::{make_channel, run_params, AsyncIoConfig, AsyncRunParams};

const OP: &str = "async consumer";

/// Futures stream of `(Time, value)` pairs handed to an async consumer.
pub type AsyncInput<T> = Pin<Box<dyn FuturesStream<Item = (Time, T)> + Send>>;

pub type AsyncConsumerError = Box<dyn Error + Send + Sync>;
type ConsumerFuture = Pin<Box<dyn Future<Output = Result<(), AsyncConsumerError>> + Send>>;
type ConsumerStart<T> = Box<dyn FnOnce(AsyncRunParams, AsyncInput<T>) -> ConsumerFuture + Send>;

enum ConsumerTaskEnd {
    Finished,
    Failed(AsyncConsumerError),
    Panicked(String),
}

struct ConsumeAsyncOp<T: Clone + Send + 'static> {
    input: Input<T>,
    out: Output<()>,
    tx: Option<kanal::Sender<(Time, T)>>,
    async_rx: Option<kanal::AsyncReceiver<(Time, T)>>,
    config: AsyncIoConfig,
    start: Option<ConsumerStart<T>>,
    exhausted: Arc<AtomicBool>,
    done_rx: Option<mpsc::Receiver<ConsumerTaskEnd>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl<T> Operator for ConsumeAsyncOp<T>
where
    T: Clone + Send + 'static,
{
    fn on_start(&mut self, cx: &mut Ctx) {
        let handle = match self.config.runtime.resolve(cx.core, OP) {
            Ok(handle) => handle,
            Err(err) => {
                cx.fail(err);
                return;
            }
        };

        let Some(start) = self.start.take() else {
            return;
        };
        let Some(async_rx) = self.async_rx.take() else {
            return;
        };

        let params = run_params(cx);
        let waker = cx.waker();
        let input = receiver_stream(async_rx, self.exhausted.clone());
        let (done_tx, done_rx) = mpsc::channel();
        self.done_rx = Some(done_rx);

        self.handle = Some(handle.spawn(async move {
            let result = AssertUnwindSafe(async move { start(params, input).await })
                .catch_unwind()
                .await;

            let end = match result {
                Ok(Ok(())) => ConsumerTaskEnd::Finished,
                Ok(Err(err)) => ConsumerTaskEnd::Failed(err),
                Err(payload) => ConsumerTaskEnd::Panicked(panic_message(payload)),
            };

            let _ = done_tx.send(end);
            let _ = waker.wake();
        }));
    }

    fn step(&mut self, cx: &mut Ctx) {
        self.observe_task(cx, false);
        if cx.core.stopping.borrow().is_some() {
            return;
        }

        let Some(tx) = &self.tx else {
            cx.fail(AsyncIoError::Closed { op: OP });
            return;
        };

        if tx.send((cx.now(), self.input.get())).is_err() {
            cx.fail(AsyncIoError::Closed { op: OP });
            return;
        }

        self.out.set(());
    }

    fn on_stop(&mut self, cx: &mut Ctx) {
        self.tx = None;
        self.observe_task(cx, true);
    }
}

impl<T> ConsumeAsyncOp<T>
where
    T: Clone + Send + 'static,
{
    fn observe_task(&mut self, cx: &mut Ctx, wait: bool) {
        let Some(done_rx) = self.done_rx.take() else {
            return;
        };

        let task_end = if wait {
            match done_rx.recv_timeout(self.config.shutdown_timeout) {
                Ok(end) => end,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cx.fail(AsyncIoError::TaskDropped { op: OP });
                    self.handle.take();
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Some(handle) = self.handle.take() {
                        handle.abort();
                    }
                    cx.fail(AsyncIoError::ShutdownTimeout { op: OP });
                    return;
                }
            }
        } else {
            match done_rx.try_recv() {
                Ok(end) => end,
                Err(mpsc::TryRecvError::Empty) => {
                    self.done_rx = Some(done_rx);
                    return;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    cx.fail(AsyncIoError::TaskDropped { op: OP });
                    self.handle.take();
                    return;
                }
            }
        };

        match task_end {
            ConsumerTaskEnd::Finished if self.exhausted.load(Ordering::Acquire) => {}
            ConsumerTaskEnd::Finished => cx.fail(AsyncIoError::Closed { op: OP }),
            ConsumerTaskEnd::Failed(err) => cx.fail(err),
            ConsumerTaskEnd::Panicked(message) => {
                cx.fail(AsyncIoError::TaskPanic { op: OP, message });
            }
        }

        self.handle.take();
    }
}

fn receiver_stream<T: Send + 'static>(
    rx: kanal::AsyncReceiver<(Time, T)>,
    exhausted: Arc<AtomicBool>,
) -> AsyncInput<T> {
    Box::pin(stream::unfold(
        (rx, exhausted),
        |(rx, exhausted)| async move {
            match rx.recv().await {
                Ok(item) => Some((item, (rx, exhausted))),
                Err(_) => {
                    exhausted.store(true, Ordering::Release);
                    None
                }
            }
        },
    ))
}

impl<T> Stream<T>
where
    T: Clone + Send + 'static,
{
    /// Start an async consumer of this stream using default configuration.
    pub fn consume_async<F, Fut, E>(&self, consume: F) -> Stream<()>
    where
        F: FnOnce(AsyncRunParams, AsyncInput<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: fmt::Display + Send + 'static,
    {
        self.consume_async_with(AsyncIoConfig::default(), consume)
    }

    /// Start an async consumer of this stream with explicit configuration.
    pub fn consume_async_with<F, Fut, E>(&self, config: AsyncIoConfig, consume: F) -> Stream<()>
    where
        F: FnOnce(AsyncRunParams, AsyncInput<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: fmt::Display + Send + 'static,
    {
        self.consume_async_boxed_with(config, move |params, input| async move {
            consume(params, input)
                .await
                .map_err(|err| Box::new(AsyncIoError::task(OP, err)) as AsyncConsumerError)
        })
    }

    /// Start an async consumer whose task error is already boxed.
    pub fn consume_async_boxed<F, Fut>(&self, consume: F) -> Stream<()>
    where
        F: FnOnce(AsyncRunParams, AsyncInput<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), AsyncConsumerError>> + Send + 'static,
    {
        self.consume_async_boxed_with(AsyncIoConfig::default(), consume)
    }

    /// Start an async consumer with explicit config and already-boxed task errors.
    pub fn consume_async_boxed_with<F, Fut>(&self, config: AsyncIoConfig, consume: F) -> Stream<()>
    where
        F: FnOnce(AsyncRunParams, AsyncInput<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), AsyncConsumerError>> + Send + 'static,
    {
        let (tx, rx) = make_channel(config.capacity);
        let async_rx = rx.to_async();
        let start: ConsumerStart<T> =
            Box::new(move |params, input| Box::pin(async move { consume(params, input).await }));

        self.wire(|w| ConsumeAsyncOp {
            input: w.on(self),
            out: w.output(),
            tx: Some(tx),
            async_rx: Some(async_rx),
            config,
            start: Some(start),
            exhausted: Arc::new(AtomicBool::new(false)),
            done_rx: None,
            handle: None,
        })
    }
}
