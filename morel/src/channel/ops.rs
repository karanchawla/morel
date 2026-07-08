use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;

use kanal::ReceiveError;

use crate::channel::error::panic_payload_to_string;
use crate::core::graph::{add_node, GraphCore};
use crate::core::{Ctx, Input, Operator, Output, Stream, Time, Waker};

use super::wire::Packet;
use super::{ChannelError, ChannelRx, ChannelTx, OnClose};

pub(crate) type ChildRunResult = Result<(), Box<dyn Error + Send + Sync>>;
pub(crate) type ChildJoin = Rc<RefCell<Option<JoinHandle<ChildRunResult>>>>;
type ReceiverStart = Box<dyn FnOnce(&mut Ctx) -> Result<JoinHandle<ChildRunResult>, ChannelError>>;
type SenderStart = Box<dyn FnOnce(&mut Ctx)>;

pub(crate) fn receiver_stream_on_core<T, P>(
    core: &Rc<GraphCore>,
    mut rx: ChannelRx<T>,
    pace: Option<&Stream<P>>,
    on_close: OnClose,
) -> Stream<Vec<T>>
where
    T: Clone + Send + 'static,
    P: Clone + 'static,
{
    let raw_rx = rx.take_rx();
    let wake = rx.wake.clone();
    let name = rx.name.to_string();

    add_node(core, move |w| {
        let pace = pace.map(|p| w.on(p));
        ReceiverOp {
            rx: Some(raw_rx),
            pace,
            out: w.output(),
            wake: wake.clone(),
            on_close,
            name,
            scratch: Vec::new(),
            queue: VecDeque::new(),
            horizon: None,
            last_time: None,
            extend_after: None,
            closed: false,
            join: None,
            child_join: None,
            start_child: None,
            cancel_child: None,
            drop_rx_before_join: false,
        }
    })
}

pub(crate) fn receiver_stream_with_join<T, P>(
    mut rx: ChannelRx<T>,
    pace: &Stream<P>,
    on_close: OnClose,
    join: ChildJoin,
) -> Stream<Vec<T>>
where
    T: Clone + Send + 'static,
    P: Clone + 'static,
{
    let core = pace.core();
    let raw_rx = rx.take_rx();
    let wake = rx.wake.clone();
    let name = rx.name.to_string();

    add_node(&core, move |w| ReceiverOp {
        rx: Some(raw_rx),
        pace: Some(w.on(pace)),
        out: w.output(),
        wake: wake.clone(),
        on_close,
        name,
        scratch: Vec::new(),
        queue: VecDeque::new(),
        horizon: None,
        last_time: None,
        extend_after: None,
        closed: false,
        join: None,
        child_join: Some(join),
        start_child: None,
        cancel_child: None,
        drop_rx_before_join: false,
    })
}

pub(crate) fn receiver_stream_with_source_start_and_cancel<T, F, C>(
    g: &crate::core::Graph,
    mut rx: ChannelRx<T>,
    on_close: OnClose,
    start: F,
    cancel: C,
) -> Stream<Vec<T>>
where
    T: Clone + Send + 'static,
    F: FnOnce(&mut Ctx) -> Result<JoinHandle<ChildRunResult>, ChannelError> + 'static,
    C: FnOnce() + 'static,
{
    let raw_rx = rx.take_rx();
    let wake = rx.wake.clone();
    let name = rx.name.to_string();

    add_node(&g.core, move |w| ReceiverOp {
        rx: Some(raw_rx),
        pace: None::<Input<()>>,
        out: w.output(),
        wake: wake.clone(),
        on_close,
        name,
        scratch: Vec::new(),
        queue: VecDeque::new(),
        horizon: None,
        last_time: None,
        extend_after: None,
        closed: false,
        join: None,
        child_join: None,
        start_child: Some(Box::new(start)),
        cancel_child: Some(Box::new(cancel)),
        drop_rx_before_join: true,
    })
}

struct ReceiverOp<T: Send + 'static, P> {
    rx: Option<kanal::Receiver<Packet<T>>>,
    pace: Option<Input<P>>,
    out: Output<Vec<T>>,
    wake: Arc<OnceLock<Waker>>,
    on_close: OnClose,
    name: String,
    scratch: Vec<T>,
    queue: VecDeque<(Time, T)>,
    horizon: Option<Time>,
    last_time: Option<Time>,
    extend_after: Option<Time>,
    closed: bool,
    join: Option<JoinHandle<ChildRunResult>>,
    child_join: Option<ChildJoin>,
    start_child: Option<ReceiverStart>,
    cancel_child: Option<Box<dyn FnOnce()>>,
    drop_rx_before_join: bool,
}

impl<T, P> Operator for ReceiverOp<T, P>
where
    T: Clone + Send + 'static,
    P: Clone + 'static,
{
    fn on_start(&mut self, cx: &mut Ctx) {
        let _ = self.wake.set(cx.waker());
        if let Some(start_child) = self.start_child.take() {
            match start_child(cx) {
                Ok(join) => self.join = Some(join),
                Err(err) => cx.fail(err),
            }
        }
        if cx.is_live() || self.pace.is_none() {
            cx.at(cx.now());
        }
    }

    fn step(&mut self, cx: &mut Ctx) {
        if cx.is_live() {
            self.step_live(cx);
        } else {
            self.step_replay(cx);
        }
    }

    fn on_stop(&mut self, cx: &mut Ctx) {
        if let Some(cancel_child) = self.cancel_child.take() {
            cancel_child();
        }

        if self.drop_rx_before_join {
            if let Some(rx) = self.rx.as_ref() {
                let _ = rx.close();
            }
        }

        let join = self
            .child_join
            .as_ref()
            .and_then(|join| join.borrow_mut().take())
            .or_else(|| self.join.take());

        if let Some(join) = join {
            match join.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => cx.fail(err),
                Err(payload) => cx.fail(ChannelError::ChildPanic {
                    channel: self.name.clone(),
                    payload: panic_payload_to_string(payload),
                }),
            }
        }

        self.drain_paced_replay_on_stop(cx);
    }
}

impl<T, P> ReceiverOp<T, P>
where
    T: Clone + Send + 'static,
    P: Clone + 'static,
{
    fn step_live(&mut self, cx: &mut Ctx) {
        if self.closed {
            return;
        }

        loop {
            let Some(rx) = self.rx.as_ref() else {
                self.closed = true;
                break;
            };

            match rx.try_recv() {
                Ok(Some(Packet::Live(value))) => self.scratch.push(value),
                Ok(Some(Packet::Close)) => {
                    self.closed = true;
                    break;
                }
                Ok(Some(Packet::At(..) | Packet::Watermark(..))) => {
                    cx.fail(ChannelError::Protocol {
                        channel: self.name.clone(),
                        message: "replay packet on live channel",
                    });
                    return;
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

        if self.closed && self.on_close == OnClose::Stop {
            cx.stop();
        }
    }

    fn step_replay(&mut self, cx: &mut Ctx) {
        let now = cx.now();
        let result = if self.extend_after.take() == Some(now) {
            self.learn_past(cx, now)
        } else {
            self.learn_until(cx, now)
        };
        if let Err(err) = result {
            cx.fail(err);
            return;
        }

        while let Some((at, _)) = self.queue.front() {
            if *at < now {
                cx.fail(self.protocol_error("replay packet time is behind receiver time"));
                return;
            }
            if *at != now {
                break;
            }
            let (_, value) = self.queue.pop_front().unwrap();
            self.scratch.push(value);
        }

        if !self.scratch.is_empty() {
            let scratch = &mut self.scratch;
            self.out.update(Vec::new, |burst| {
                burst.clear();
                burst.append(scratch);
            });
        }

        if self.closed && self.queue.is_empty() {
            if self.on_close == OnClose::Stop {
                cx.stop();
            }
            return;
        }

        if !self.closed {
            if let Some((at, _)) = self.queue.front() {
                cx.at(*at);
            } else if self.pace.is_none() {
                if let Some(horizon) = self.horizon {
                    if horizon > now {
                        cx.at(horizon);
                    } else {
                        self.extend_after = Some(now);
                        cx.at(now);
                    }
                } else {
                    self.extend_after = Some(now);
                    cx.at(now);
                }
            }
        }
    }

    fn recv_blocking(&self) -> Result<Packet<T>, ChannelError> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(Packet::Close);
        };

        match rx.recv() {
            Ok(packet) => Ok(packet),
            Err(ReceiveError::SendClosed | ReceiveError::Closed) => Ok(Packet::Close),
        }
    }

    fn accept_replay_packet(&mut self, now: Time, packet: Packet<T>) -> Result<(), ChannelError> {
        match packet {
            Packet::At(at, value) => {
                self.check_time(now, at)?;
                self.horizon = Some(at);
                self.last_time = Some(at);
                self.queue.push_back((at, value));
                Ok(())
            }
            Packet::Watermark(at) => {
                self.check_time(now, at)?;
                self.horizon = Some(at);
                self.last_time = Some(at);
                Ok(())
            }
            Packet::Close => {
                self.closed = true;
                Ok(())
            }
            Packet::Live(_) => Err(self.protocol_error("live packet on replay channel")),
        }
    }

    fn check_time(&self, now: Time, at: Time) -> Result<(), ChannelError> {
        if self.last_time.is_some_and(|last| at < last) {
            return Err(self.protocol_error("replay channel time moved backwards"));
        }
        if self.pace.is_some() && self.last_time.is_some_and(|last| at > last && at < now) {
            return Err(
                self.protocol_error("paced replay packet arrived after its lockstep instant")
            );
        }
        if at < now {
            return Err(self.protocol_error("replay packet time is behind receiver time"));
        }
        if self.pace.is_some() && at > now {
            return Err(self.protocol_error("future packet on paced replay channel"));
        }
        Ok(())
    }

    fn learn_until(&mut self, cx: &Ctx, now: Time) -> Result<(), ChannelError> {
        while cx.core.stopping.borrow().is_none()
            && !self.closed
            && self.horizon.is_none_or(|horizon| horizon < now)
        {
            let packet = self.recv_blocking()?;
            self.accept_replay_packet(now, packet)?;
        }
        Ok(())
    }

    fn learn_past(&mut self, cx: &Ctx, now: Time) -> Result<(), ChannelError> {
        while cx.core.stopping.borrow().is_none()
            && !self.closed
            && self.horizon.is_none_or(|horizon| horizon <= now)
        {
            let packet = self.recv_blocking()?;
            self.accept_replay_packet(now, packet)?;
        }
        Ok(())
    }

    fn drain_paced_replay_on_stop(&mut self, cx: &mut Ctx) {
        if cx.is_live() || self.pace.is_none() {
            return;
        }

        loop {
            let Some(rx) = self.rx.as_ref() else {
                return;
            };

            match rx.try_recv() {
                Ok(Some(Packet::At(at, _))) => {
                    let message = if at <= cx.now() {
                        "replay packet time is behind receiver time"
                    } else {
                        "future packet on paced replay channel"
                    };
                    cx.fail(self.protocol_error(message));
                    return;
                }
                Ok(Some(Packet::Watermark(_) | Packet::Close)) => {}
                Ok(Some(Packet::Live(_))) => {
                    cx.fail(self.protocol_error("live packet on replay channel"));
                    return;
                }
                Ok(None) | Err(ReceiveError::SendClosed | ReceiveError::Closed) => return,
            }
        }
    }

    fn protocol_error(&self, message: &'static str) -> ChannelError {
        ChannelError::Protocol {
            channel: self.name.clone(),
            message,
        }
    }
}

struct SenderOp<T: Send + 'static, H> {
    source: Input<T>,
    heartbeat: Option<Input<H>>,
    out: Output<()>,
    tx: Option<ChannelTx<T>>,
    last_sent: Option<Time>,
    start_child: Option<SenderStart>,
}

impl<T, H> Operator for SenderOp<T, H>
where
    T: Clone + Send + 'static,
    H: Clone + 'static,
{
    fn on_start(&mut self, cx: &mut Ctx) {
        if let Some(start_child) = self.start_child.take() {
            start_child(cx);
        }
    }

    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();
        let mut sent = false;

        if self.source.fired() {
            let packet = if cx.is_live() {
                Packet::Live(self.source.get())
            } else {
                Packet::At(now, self.source.get())
            };
            if !self.send_packet(packet, cx) {
                return;
            }
            self.last_sent = Some(now);
            sent = true;
        }

        if self.heartbeat.as_ref().is_some_and(Input::fired) {
            if cx.is_live() {
                if !sent {
                    return;
                }
            } else if !sent {
                if self.last_sent == Some(now) {
                    return;
                }
                if !self.send_packet(Packet::Watermark(now), cx) {
                    return;
                }
                self.last_sent = Some(now);
                sent = true;
            }
        }

        if sent {
            self.out.set(());
        }
    }

    fn on_stop(&mut self, _cx: &mut Ctx) {
        if let Some(tx) = self.tx.take() {
            tx.send_close_best_effort();
        }
    }
}

impl<T, H> SenderOp<T, H>
where
    T: Clone + Send + 'static,
    H: Clone + 'static,
{
    fn send_packet(&self, packet: Packet<T>, cx: &mut Ctx) -> bool {
        let Some(tx) = self.tx.as_ref() else {
            return false;
        };

        if let Err(err) = tx.send_packet(packet) {
            cx.fail(err);
            return false;
        }

        true
    }
}

impl<T> ChannelTx<T>
where
    T: Clone + Send + 'static,
{
    /// Send each value from `source` into this channel.
    ///
    /// In replay mode the packet time is `source`'s graph time. In live mode
    /// the packet is delivered as a live value and wakes the receiver graph.
    pub fn attach(self, source: &Stream<T>) -> Stream<()> {
        source.wire(|w| SenderOp {
            source: w.on(source),
            heartbeat: None::<Input<()>>,
            out: w.output(),
            tx: Some(self),
            last_sent: None,
            start_child: None,
        })
    }

    /// Send values from `source` and replay watermarks from `heartbeat`.
    ///
    /// Use this when a replay receiver must advance through heartbeat instants
    /// where `source` may not emit a value.
    pub fn attach_with_heartbeat<H>(self, source: &Stream<T>, heartbeat: &Stream<H>) -> Stream<()>
    where
        H: Clone + 'static,
    {
        source.wire(|w| SenderOp {
            source: w.on(source),
            heartbeat: Some(w.on(heartbeat)),
            out: w.output(),
            tx: Some(self),
            last_sent: None,
            start_child: None,
        })
    }
}

pub(crate) fn attach_with_child_start<T, F>(
    tx: ChannelTx<T>,
    source: &Stream<T>,
    start: F,
) -> Stream<()>
where
    T: Clone + Send + 'static,
    F: FnOnce(&mut Ctx) + 'static,
{
    source.wire(|w| SenderOp {
        source: w.on(source),
        heartbeat: None::<Input<()>>,
        out: w.output(),
        tx: Some(tx),
        last_sent: None,
        start_child: Some(Box::new(start)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{channel, Capacity};
    use crate::core::{Graph, Live, Replay, Stop, Time};
    use std::time::Duration;

    #[test]
    fn live_receiver_rejects_replay_packet() {
        let g = Graph::new();
        let (tx, rx) = channel::<i64>(Capacity::Unbounded);
        tx.send_packet(Packet::At(Time::EPOCH, 1)).unwrap();
        let _out = rx.into_stream(&g, OnClose::Continue);

        let err = g
            .run(Live::new().stop(Stop::After(Duration::from_millis(10))))
            .unwrap_err();

        assert!(err.to_string().contains("replay packet on live channel"));
    }

    #[test]
    fn replay_receiver_rejects_live_packet() {
        let g = Graph::new();
        let (tx, rx) = channel::<i64>(Capacity::Unbounded);
        tx.send_packet(Packet::Live(1)).unwrap();
        let _out = rx.into_stream(&g, OnClose::Continue);

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert!(err.to_string().contains("live packet on replay channel"));
    }
}
