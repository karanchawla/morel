use futures_util::stream;
use morel::{
    produce_async, produce_async_stream, produce_async_stream_with, produce_async_with,
    AsyncIoConfig, AsyncIoError, AsyncProducer, Capacity, Graph, Live, OnClose, Replay, Stop,
    Stream, Time,
};
use std::cell::RefCell;
use std::error::Error as _;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn async_error(err: &morel::Error) -> &AsyncIoError {
    err.source()
        .and_then(|source| source.downcast_ref::<AsyncIoError>())
        .expect("async io error source")
}

fn collect_flat<T: Clone + 'static>(s: &Stream<Vec<T>>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen2 = seen.clone();
    s.sink(move |burst, _| seen2.borrow_mut().extend(burst));
    seen
}

#[test]
fn producer_delivers_values_in_order() {
    let g = Graph::new();
    let out = produce_async(&g, |_params, p: AsyncProducer<i64>| async move {
        for v in 1..=3i64 {
            p.send(v).await?;
        }
        Ok::<(), AsyncIoError>(())
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(100))))
        .unwrap();

    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
}

#[test]
fn producer_receives_run_params() {
    let (tx, rx) = mpsc::channel();
    let g = Graph::new();
    let _out = produce_async(&g, move |params, _p: AsyncProducer<i64>| async move {
        tx.send(params).unwrap();
        Ok::<(), AsyncIoError>(())
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(100))))
        .unwrap();

    let params = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(params.is_live);
    assert_eq!(
        params.end_at,
        Some(params.started_at + Duration::from_millis(100))
    );
}

#[test]
fn producer_replay_fails_without_running_closure() {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let g = Graph::new();
    let _out = produce_async(&g, move |_params, _p: AsyncProducer<i64>| {
        called2.store(true, Ordering::SeqCst);
        async { Ok::<(), AsyncIoError>(()) }
    });

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(
        err.to_string().contains("can only run in live mode"),
        "{err}"
    );
    assert_eq!(
        async_error(&err),
        &AsyncIoError::LiveOnly {
            op: "async producer"
        }
    );
    assert!(!called.load(Ordering::SeqCst));
}

#[test]
fn producer_task_error_fails_run_promptly() {
    let g = Graph::new();
    let _out = produce_async(&g, |_params, _p: AsyncProducer<i64>| async {
        Err::<(), _>("connect refused")
    });

    let started = std::time::Instant::now();
    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_secs(5))))
        .unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(err.to_string().contains("connect refused"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Task {
            op: "async producer",
            message: "connect refused".to_string()
        }
    );
}

#[test]
fn producer_panic_fails_run_with_payload() {
    let g = Graph::new();
    let _out = produce_async(&g, |_params, _p: AsyncProducer<i64>| async {
        panic!("boom in task");
        #[allow(unreachable_code)]
        Ok::<(), AsyncIoError>(())
    });

    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_secs(5))))
        .unwrap_err();

    assert!(err.to_string().contains("boom in task"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::TaskPanic {
            op: "async producer",
            message: "boom in task".to_string()
        }
    );
}

#[test]
fn producer_observes_shutdown_via_closed() {
    let (tx, rx) = mpsc::channel();
    let g = Graph::new();
    let _out = produce_async(&g, move |_params, p: AsyncProducer<i64>| async move {
        p.closed().await;
        tx.send(()).unwrap();
        Ok::<(), AsyncIoError>(())
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(25))))
        .unwrap();

    rx.recv_timeout(Duration::from_secs(1))
        .expect("producer saw cancellation");
}

#[test]
fn busy_producer_shutdown_is_clean() {
    let g = Graph::new();
    let out = produce_async::<i64, _, _, AsyncIoError>(&g, |_params, p| async move {
        let mut n = 0i64;
        loop {
            n += 1;
            p.send(n).await?;
        }
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(50))))
        .unwrap();

    assert!(!seen.borrow().is_empty(), "producer must have been busy");
}

#[test]
fn bounded_capacity_is_lossless_under_backpressure() {
    let config = AsyncIoConfig::default().with_capacity(Capacity::Bounded(1));
    let g = Graph::new();
    let out = produce_async_with(
        &g,
        config,
        OnClose::Continue,
        |_params, p: AsyncProducer<i64>| async move {
            for v in 1..=100i64 {
                p.send(v).await?;
            }
            Ok::<(), AsyncIoError>(())
        },
    );
    let seen = collect_flat(&out);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(300))))
        .unwrap();

    assert_eq!(*seen.borrow(), (1..=100).collect::<Vec<_>>());
}

#[test]
fn producer_ignoring_cancellation_times_out_and_aborts() {
    let config = AsyncIoConfig::default().with_shutdown_timeout(Duration::from_millis(30));
    let g = Graph::new();
    let _out = produce_async_with(
        &g,
        config,
        OnClose::Continue,
        |_params, _p: AsyncProducer<i64>| async move {
            std::future::pending::<()>().await;
            Ok::<(), AsyncIoError>(())
        },
    );

    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_millis(10))))
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("did not stop before shutdown timeout"));
    assert_eq!(
        async_error(&err),
        &AsyncIoError::ShutdownTimeout {
            op: "async producer"
        }
    );
}

#[test]
fn producer_close_can_stop_graph() {
    let g = Graph::new();
    let out = produce_async_with(
        &g,
        AsyncIoConfig::default(),
        OnClose::Stop,
        |_params, p: AsyncProducer<i64>| async move {
            p.send(1).await?;
            Ok::<(), AsyncIoError>(())
        },
    );
    let seen = collect_flat(&out);

    let summary = g.run(Live::new().stop(Stop::Never)).unwrap();

    assert!(summary.steps > 0);
    assert_eq!(*seen.borrow(), vec![1]);
}

#[test]
fn stream_wrapper_delivers_values_in_order() {
    let g = Graph::new();
    let out = produce_async_stream(&g, |_params| async {
        Ok::<_, AsyncIoError>(stream::iter([Ok::<_, AsyncIoError>(10i64), Ok(20), Ok(30)]))
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(100))))
        .unwrap();

    assert_eq!(*seen.borrow(), vec![10, 20, 30]);
}

#[test]
fn stream_wrapper_item_error_fails_run() {
    let g = Graph::new();
    let _out = produce_async_stream(&g, |_params| async {
        Ok::<_, &'static str>(stream::iter([
            Ok::<i64, &'static str>(10),
            Err("bad frame"),
        ]))
    });

    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_secs(5))))
        .unwrap_err();

    assert!(err.to_string().contains("bad frame"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Task {
            op: "async producer",
            message: "async producer stream failed: bad frame".to_string()
        }
    );
}

#[test]
fn stream_wrapper_make_stream_error_fails_run() {
    let g = Graph::new();
    let _out = produce_async_stream(&g, |_params| async {
        Err::<stream::Empty<Result<i64, &'static str>>, _>("bad connect")
    });

    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_secs(5))))
        .unwrap_err();

    assert!(err.to_string().contains("bad connect"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Task {
            op: "async producer",
            message: "async producer stream failed: bad connect".to_string()
        }
    );
}

#[test]
fn stream_wrapper_pending_stream_shutdown_is_clean() {
    let config = AsyncIoConfig::default().with_shutdown_timeout(Duration::from_millis(30));
    let g = Graph::new();
    let _out = produce_async_stream_with(&g, config, OnClose::Continue, |_params| async {
        Ok::<_, AsyncIoError>(stream::pending::<Result<i64, AsyncIoError>>())
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(10))))
        .unwrap();
}

#[test]
fn stream_wrapper_pending_make_stream_shutdown_is_clean() {
    type PendingStream = stream::Pending<Result<i64, AsyncIoError>>;

    let config = AsyncIoConfig::default().with_shutdown_timeout(Duration::from_millis(30));
    let g = Graph::new();
    let _out = produce_async_stream_with(&g, config, OnClose::Continue, |_params| async {
        std::future::pending::<Result<PendingStream, AsyncIoError>>().await
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(10))))
        .unwrap();
}

#[test]
fn stream_wrapper_close_can_stop_graph() {
    let g = Graph::new();
    let _out = produce_async_stream_with(
        &g,
        AsyncIoConfig::default(),
        OnClose::Stop,
        |_params| async { Ok::<_, AsyncIoError>(stream::iter([Ok::<_, AsyncIoError>(1i64)])) },
    );

    let summary = g.run(Live::new().stop(Stop::Never)).unwrap();

    assert!(summary.steps > 0);
}
