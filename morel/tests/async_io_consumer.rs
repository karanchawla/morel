use futures_util::StreamExt;
use morel::{AsyncIoConfig, AsyncIoError, Graph, Live, Replay, Stop, Time};
use std::error::Error as _;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn async_error(err: &morel::Error) -> &AsyncIoError {
    err.source()
        .and_then(|source| source.downcast_ref::<AsyncIoError>())
        .expect("async io error source")
}

#[test]
fn consumer_receives_replay_values_with_virtual_times() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(20), 2), (ms(20), 3)]);
    let _sink = src.consume_async(move |_params, mut input| async move {
        while let Some((time, value)) = input.next().await {
            seen2.lock().unwrap().push((value, time));
        }
        Ok::<(), AsyncIoError>(())
    });

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        vec![(1, ms(10)), (2, ms(20)), (3, ms(20))]
    );
}

#[test]
fn consumer_sees_replay_run_params() {
    let (ptx, prx) = mpsc::channel();
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64)]);
    let _sink = src.consume_async(move |params, mut input| async move {
        ptx.send(params).unwrap();
        while input.next().await.is_some() {}
        Ok::<(), AsyncIoError>(())
    });

    g.run(Replay::from(Time::EPOCH)).unwrap();

    let params = prx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!params.is_live);
    assert_eq!(params.started_at, Time::EPOCH);
    assert_eq!(params.end_at, None);
}

#[test]
fn consumer_receives_live_values() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let g = Graph::new();
    let mut n = 0i64;
    let src = g
        .ticker(Duration::from_millis(10))
        .map(move |()| {
            n += 1;
            n
        })
        .take(3);
    let _sink = src.consume_async(move |_params, mut input| async move {
        while let Some((_time, value)) = input.next().await {
            seen2.lock().unwrap().push(value);
        }
        Ok::<(), AsyncIoError>(())
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(80))))
        .unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn consumer_task_error_fails_run() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64)]);
    let _sink = src.consume_async(|_params, mut input| async move {
        let _ = input.next().await;
        Err::<(), _>("write failed")
    });

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err.to_string().contains("write failed"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Task {
            op: "async consumer",
            message: "write failed".to_string(),
        }
    );
}

#[test]
fn consumer_error_wakes_live_stop_never_graph() {
    let g = Graph::new();
    let src = g.just(1i64);
    let _sink = src.consume_async(|_params, mut input| async move {
        let _ = input.next().await;
        Err::<(), _>("sink failed after first value")
    });

    let started = std::time::Instant::now();
    let err = g.run(Live::new().stop(Stop::Never)).unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        err.to_string().contains("sink failed after first value"),
        "{err}"
    );
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Task {
            op: "async consumer",
            message: "sink failed after first value".to_string(),
        }
    );
}

#[test]
fn consumer_panic_fails_run_with_payload() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64)]);
    let _sink = src.consume_async(|_params, mut input| async move {
        let _ = input.next().await;
        panic!("consumer boom");
        #[allow(unreachable_code)]
        Ok::<(), AsyncIoError>(())
    });

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err.to_string().contains("consumer boom"), "{err}");
    assert_eq!(
        async_error(&err),
        &AsyncIoError::TaskPanic {
            op: "async consumer",
            message: "consumer boom".to_string(),
        }
    );
}

#[test]
fn consumer_early_exit_fails_deterministically() {
    // The task returns Ok after one value while the input is still open.
    // Whether or not the engine's second send races the task's exit, the run
    // must fail: exhaustion tracking makes abandonment deterministic.
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(20), 2)]);
    let _sink = src.consume_async(|_params, mut input| async move {
        let _ = input.next().await;
        Ok::<(), AsyncIoError>(())
    });

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(
        err.to_string().contains("async consumer channel is closed"),
        "{err}"
    );
    assert_eq!(
        async_error(&err),
        &AsyncIoError::Closed {
            op: "async consumer"
        }
    );
}

#[test]
fn consumer_ignoring_shutdown_times_out() {
    let config = AsyncIoConfig::default().with_shutdown_timeout(Duration::from_millis(30));
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64)]);
    let _sink = src.consume_async_with(config, |_params, _input| async move {
        std::future::pending::<()>().await;
        Ok::<(), AsyncIoError>(())
    });

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err
        .to_string()
        .contains("did not stop before shutdown timeout"));
    assert_eq!(
        async_error(&err),
        &AsyncIoError::ShutdownTimeout {
            op: "async consumer"
        }
    );
}
