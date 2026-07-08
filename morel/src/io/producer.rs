use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

use futures_core::Stream as FuturesStream;
use futures_util::{FutureExt, StreamExt};
use kanal::ReceiveError;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::channel::OnClose;
use crate::core::{Ctx, Graph, Operator, Output, Stream, Waker};

use super::error::{closed_message, panic_message, AsyncIoError, TaskEnd};
use super::runtime::{make_channel, run_params, AsyncIoConfig, AsyncRunParams};

const OP: &str = "async producer";

enum ProducerPacket<T> {
    Value(T),
    TaskError(String),
    TaskPanic(String),
    Close,
}

#[derive(Clone)]
struct CancelToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelToken {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        loop {
            let mut notified = pin!(self.notify.notified());
            Pin::as_mut(&mut notified).enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// Async handle passed to a live producer task.
pub struct AsyncProducer<T: Send + 'static> {
    tx: kanal::Sender<ProducerPacket<T>>,
    waker: Waker,
    cancel: CancelToken,
}

impl<T: Send + 'static> Clone for AsyncProducer<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            waker: self.waker.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl<T: Send + 'static> AsyncProducer<T> {
    /// Send one value into the graph.
    pub async fn send(&self, value: T) -> Result<(), AsyncIoError> {
        self.send_packet(ProducerPacket::Value(value)).await
    }

    /// True once the graph has requested producer shutdown.
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled() || self.tx.is_closed()
    }

    /// Wait until the graph requests producer shutdown.
    pub async fn closed(&self) {
        self.cancel.cancelled().await;
    }

    async fn send_packet(&self, packet: ProducerPacket<T>) -> Result<(), AsyncIoError> {
        if self.cancel.is_cancelled() {
            return Err(AsyncIoError::Closed { op: OP });
        }

        self.tx
            .as_async()
            .send(packet)
            .await
            .map_err(|_| AsyncIoError::Closed { op: OP })?;
        let _ = self.waker.wake();
        Ok(())
    }
}

struct ProduceAsyncOp<T, F, Fut, E>
where
    T: Send + 'static,
{
    config: AsyncIoConfig,
    on_close: OnClose,
    start: Option<F>,
    tx: kanal::Sender<ProducerPacket<T>>,
    rx: kanal::Receiver<ProducerPacket<T>>,
    out: Output<Vec<T>>,
    scratch: Vec<T>,
    cancel: CancelToken,
    completion_rx: Option<mpsc::Receiver<TaskEnd>>,
    handle: Option<JoinHandle<()>>,
    closed: bool,
    _marker: PhantomData<fn() -> (Fut, E)>,
}

impl<T, F, Fut, E> Operator for ProduceAsyncOp<T, F, Fut, E>
where
    T: Send + 'static,
    F: FnOnce(AsyncRunParams, AsyncProducer<T>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    fn on_start(&mut self, cx: &mut Ctx) {
        if !cx.is_live() {
            cx.fail(AsyncIoError::LiveOnly { op: OP });
            return;
        }

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

        let producer = AsyncProducer {
            tx: self.tx.clone(),
            waker: cx.waker(),
            cancel: self.cancel.clone(),
        };
        let params = run_params(cx);
        let (completion_tx, completion_rx) = mpsc::channel();
        self.completion_rx = Some(completion_rx);

        self.handle = Some(handle.spawn(async move {
            let run_producer = producer.clone();
            let result = AssertUnwindSafe(async move { start(params, run_producer).await })
                .catch_unwind()
                .await;

            let (packet, end) = match result {
                Ok(Ok(())) => (ProducerPacket::Close, TaskEnd::Finished),
                Ok(Err(err)) => {
                    let message = err.to_string();
                    (
                        ProducerPacket::TaskError(message.clone()),
                        TaskEnd::Failed(message),
                    )
                }
                Err(payload) => {
                    let message = panic_message(payload);
                    (
                        ProducerPacket::TaskPanic(message.clone()),
                        TaskEnd::Panicked(message),
                    )
                }
            };

            let _ = producer.send_packet(packet).await;
            let _ = completion_tx.send(end);
        }));

        cx.at(cx.now());
    }

    fn step(&mut self, cx: &mut Ctx) {
        if self.closed {
            return;
        }

        let mut failure = None;

        loop {
            match self.rx.try_recv() {
                Ok(Some(ProducerPacket::Value(value))) => self.scratch.push(value),
                Ok(Some(ProducerPacket::TaskError(message))) => {
                    self.closed = true;
                    failure = Some(AsyncIoError::Task { op: OP, message });
                    break;
                }
                Ok(Some(ProducerPacket::TaskPanic(message))) => {
                    self.closed = true;
                    failure = Some(AsyncIoError::TaskPanic { op: OP, message });
                    break;
                }
                Ok(Some(ProducerPacket::Close)) => {
                    self.closed = true;
                    break;
                }
                Ok(None) => break,
                Err(ReceiveError::SendClosed | ReceiveError::Closed) => {
                    self.closed = true;
                    break;
                }
            }
        }

        if !self.scratch.is_empty() {
            let scratch = &mut self.scratch;
            self.out.update(Vec::new, |burst| {
                burst.clear();
                burst.append(scratch);
            });
        }

        if let Some(err) = failure {
            cx.fail(err);
            return;
        }

        if self.closed && self.on_close == OnClose::Stop {
            cx.stop();
        }
    }

    fn on_stop(&mut self, cx: &mut Ctx) {
        self.cancel.cancel();
        let _ = self.rx.close();

        let Some(completion_rx) = self.completion_rx.take() else {
            return;
        };

        match completion_rx.recv_timeout(self.config.shutdown_timeout) {
            Ok(TaskEnd::Finished) => {}
            Ok(TaskEnd::Failed(message)) if message == closed_message(OP) => {}
            Ok(TaskEnd::Failed(message)) => cx.fail(AsyncIoError::Task { op: OP, message }),
            Ok(TaskEnd::Panicked(message)) => {
                cx.fail(AsyncIoError::TaskPanic { op: OP, message });
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                cx.fail(AsyncIoError::TaskDropped { op: OP });
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(handle) = self.handle.take() {
                    handle.abort();
                }
                cx.fail(AsyncIoError::ShutdownTimeout { op: OP });
            }
        }

        self.handle.take();
    }
}

/// Create a live-only async producer stream using default configuration.
pub fn produce_async<T, F, Fut, E>(g: &Graph, produce: F) -> Stream<Vec<T>>
where
    T: Send + 'static,
    F: FnOnce(AsyncRunParams, AsyncProducer<T>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    produce_async_with(g, AsyncIoConfig::default(), OnClose::Continue, produce)
}

/// Create a live-only async producer stream with explicit configuration.
pub fn produce_async_with<T, F, Fut, E>(
    g: &Graph,
    config: AsyncIoConfig,
    on_close: OnClose,
    produce: F,
) -> Stream<Vec<T>>
where
    T: Send + 'static,
    F: FnOnce(AsyncRunParams, AsyncProducer<T>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    let (tx, rx) = make_channel(config.capacity);
    g.add(move |w| ProduceAsyncOp {
        config,
        on_close,
        start: Some(produce),
        tx,
        rx,
        out: w.output(),
        scratch: Vec::new(),
        cancel: CancelToken::new(),
        completion_rx: None,
        handle: None,
        closed: false,
        _marker: PhantomData,
    })
}

/// Start a live async producer from a fallible futures stream.
///
/// A shutdown that interrupts a send ends the wrapper cleanly through the
/// Closed sentinel; a stream item `Err` fails the run.
pub fn produce_async_stream<T, S, F, Fut, E>(g: &Graph, make_stream: F) -> Stream<Vec<T>>
where
    T: Send + 'static,
    S: FuturesStream<Item = Result<T, E>> + Send + Unpin + 'static,
    F: FnOnce(AsyncRunParams) -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    produce_async_stream_with(g, AsyncIoConfig::default(), OnClose::Continue, make_stream)
}

/// Stream wrapper with explicit buffering, runtime, and close behavior.
pub fn produce_async_stream_with<T, S, F, Fut, E>(
    g: &Graph,
    config: AsyncIoConfig,
    on_close: OnClose,
    make_stream: F,
) -> Stream<Vec<T>>
where
    T: Send + 'static,
    S: FuturesStream<Item = Result<T, E>> + Send + Unpin + 'static,
    F: FnOnce(AsyncRunParams) -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    produce_async_with(g, config, on_close, move |params, producer| async move {
        let mut stream = tokio::select! {
            () = producer.closed() => return Ok::<(), AsyncIoError>(()),
            result = make_stream(params) => {
                result.map_err(|err| AsyncIoError::task("async producer stream", err))?
            }
        };

        loop {
            let item = tokio::select! {
                () = producer.closed() => return Ok::<(), AsyncIoError>(()),
                item = stream.next() => item,
            };
            let Some(item) = item else {
                return Ok::<(), AsyncIoError>(());
            };
            let value = item.map_err(|err| AsyncIoError::task("async producer stream", err))?;
            producer.send(value).await?;
        }
    })
}
