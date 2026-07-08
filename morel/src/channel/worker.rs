use std::cell::RefCell;
use std::rc::Rc;
use std::thread;

use crate::channel::ops::ChildRunResult;
use crate::channel::{channel, Capacity, ChannelError, OnClose};
use crate::core::{Ctx, Graph, Live, Replay, Stop, Stream, Time};

/// Run `build` in a child graph paced by `input`.
///
/// The parent sends each input value to the child and receives returned values
/// at the same parent time. Replay is strict lockstep: delayed child output, or
/// more than one child output fire for one parent instant, fails the parent run.
pub fn worker<In, Out, F>(input: &Stream<In>, build: F) -> Stream<Vec<Out>>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    F: FnOnce(&Graph, Stream<Vec<In>>) -> Stream<Out> + Send + 'static,
{
    let (ftx, frx) = channel::<In>(Capacity::Bounded(1024));
    let (rtx, rrx) = channel::<Out>(Capacity::Unbounded);
    let join = Rc::new(RefCell::new(None));

    let start_child = {
        let join = join.clone();
        move |cx: &mut Ctx| {
            let is_live = cx.is_live();
            let start = cx.started_at();
            let handle = thread::spawn(move || {
                let child = Graph::new();
                let child_input = frx.into_stream(&child, OnClose::Stop);
                let child_out = build(&child, child_input.clone());
                let _return_sender = rtx.attach_with_heartbeat(&child_out, &child_input);
                let result = if is_live {
                    child.run(Live::new())
                } else {
                    child.run(Replay::from(start))
                };
                map_worker_result(result)
            });
            *join.borrow_mut() = Some(handle);
        }
    };

    let _forward = super::ops::attach_with_child_start(ftx, input, start_child);
    super::ops::receiver_stream_with_join(rrx, input, OnClose::Continue, join)
}

/// Run `build` in a source child graph and stream its output into `g`.
///
/// In live mode, stopping the parent asks the child to stop. In replay mode,
/// the parent run must have a finite horizon so the child can be capped at the
/// same virtual time.
pub fn source_worker<Out, F>(g: &Graph, build: F) -> Stream<Vec<Out>>
where
    Out: Clone + Send + 'static,
    F: FnOnce(&Graph) -> Stream<Out> + Send + 'static,
{
    let (rtx, rrx) = channel::<Out>(Capacity::Unbounded);
    let return_channel = rtx.name.to_string();
    let (stop_tx, stop_rx) = channel::<()>(Capacity::Unbounded);
    super::ops::receiver_stream_with_source_start_and_cancel(
        g,
        rrx,
        OnClose::Continue,
        move |cx| {
            let is_live = cx.is_live();
            let start = cx.started_at();
            let end_at = cx.core.end_at.get();
            if !is_live && end_at == Time::MAX {
                return Err(ChannelError::Protocol {
                    channel: rtx.name.to_string(),
                    message: "source worker replay requires finite parent horizon",
                });
            }
            Ok(thread::spawn(move || {
                let child = Graph::new();
                if is_live {
                    let _stop = stop_rx.into_stream(&child, OnClose::Stop);
                }
                let child_out = build(&child);
                let _return_sender = rtx.attach(&child_out);
                let result = if is_live {
                    if end_at == Time::MAX {
                        child.run(Live::new())
                    } else {
                        child.run(Live::new().stop(Stop::At(end_at)))
                    }
                } else if end_at == Time::MAX {
                    child.run(Replay::from(start))
                } else {
                    child.run(Replay::from(start).stop(Stop::At(end_at)))
                };
                map_source_worker_result(result, &return_channel)
            }))
        },
        move || stop_tx.send_close_best_effort(),
    )
}

fn map_source_worker_result(
    result: Result<crate::core::Summary, crate::core::Error>,
    return_channel: &str,
) -> ChildRunResult {
    match result {
        Ok(_) => Ok(()),
        Err(crate::core::Error::Node { source, .. }) => {
            if let Some(ChannelError::Closed { channel, operation }) =
                source.downcast_ref::<ChannelError>()
            {
                if channel == return_channel && matches!(*operation, "send" | "close") {
                    return Ok(());
                }
            }

            Err(source)
        }
    }
}

fn map_worker_result(result: Result<crate::core::Summary, crate::core::Error>) -> ChildRunResult {
    match result {
        Ok(_) => Ok(()),
        Err(crate::core::Error::Node { source, .. }) => Err(source),
    }
}
