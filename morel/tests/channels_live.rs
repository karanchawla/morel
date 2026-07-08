use morel::{channel, Capacity, Graph, Live, OnClose, Stop, Stream};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn collect_bursts<T: Clone + 'static>(s: &Stream<Vec<T>>) -> Rc<RefCell<Vec<Vec<T>>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen2 = seen.clone();
    s.sink(move |burst, _| seen2.borrow_mut().push(burst));
    seen
}

#[test]
fn live_round_trip_delivers_values_as_bursts() {
    let g = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    let mut n = 0i64;
    let src = g
        .ticker(Duration::from_millis(5))
        .map(move |()| {
            n += 1;
            n
        })
        .take(3);
    let _sent = tx.attach(&src);
    let out = rx.into_stream(&g, OnClose::Continue);
    let seen = collect_bursts(&out);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(80))))
        .unwrap();

    let flattened: Vec<i64> = seen.borrow().iter().flatten().copied().collect();
    assert_eq!(flattened, vec![1, 2, 3]);
}

#[test]
fn live_wake_coalescing_is_lossless() {
    let g = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    let out = rx.into_stream(&g, OnClose::Continue);
    let seen = collect_bursts(&out);

    let child = thread::spawn(move || {
        let g = Graph::new();
        let mut n = 0i64;
        let src = g
            .ticker(Duration::from_millis(1))
            .map(move |()| {
                n += 1;
                n
            })
            .take(3);
        let _sent = tx.attach(&src);
        g.run(Live::new().stop(Stop::Steps(3))).unwrap();
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(120))))
        .unwrap();
    child.join().unwrap();

    let flattened: Vec<i64> = seen.borrow().iter().flatten().copied().collect();
    assert_eq!(flattened, vec![1, 2, 3]);
}

#[test]
fn live_pre_receiver_buffering_is_drained_on_startup() {
    let parent = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    let child = thread::spawn(move || {
        let g = Graph::new();
        let src = g.just(42i64);
        let _sent = tx.attach(&src);
        g.run(Live::new().stop(Stop::Steps(1))).unwrap();
    });
    child.join().unwrap();

    let out = rx.into_stream(&parent, OnClose::Continue);
    let seen = collect_bursts(&out);

    parent
        .run(Live::new().stop(Stop::After(Duration::from_millis(30))))
        .unwrap();

    assert_eq!(*seen.borrow(), vec![vec![42]]);
}

#[test]
fn live_sender_drop_closes_receiver_without_stopping_when_configured() {
    let g = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    drop(tx);
    let out = rx.into_stream(&g, OnClose::Continue);
    let seen = collect_bursts(&out);
    let ticks = Rc::new(RefCell::new(0usize));
    let ticks2 = ticks.clone();
    g.ticker(Duration::from_millis(5))
        .take(2)
        .sink(move |(), _| *ticks2.borrow_mut() += 1);

    g.run(Live::new().stop(Stop::After(Duration::from_millis(40))))
        .unwrap();

    assert!(seen.borrow().is_empty());
    assert_eq!(*ticks.borrow(), 2);
}

#[test]
fn live_on_close_stop_stops_graph_cleanly() {
    let g = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    drop(tx);
    let _out = rx.into_stream(&g, OnClose::Stop);

    let summary = g
        .run(Live::new().stop(Stop::After(Duration::from_secs(1))))
        .unwrap();

    assert!(summary.steps <= 1);
}

#[test]
fn live_bounded_channel_resumes_after_receiver_drains() {
    let parent = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Bounded(1));
    let (done_tx, done_rx) = mpsc::channel();
    let child = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        let g = Graph::new();
        let mut n = 0i64;
        let src = g
            .ticker(Duration::from_millis(2))
            .map(move |()| {
                n += 1;
                n
            })
            .take(4);
        let _sent = tx.attach(&src);
        g.run(Live::new().stop(Stop::After(Duration::from_millis(80))))
            .unwrap();
        done_tx.send(()).unwrap();
    });
    let out = rx.into_stream(&parent, OnClose::Continue);
    let seen = collect_bursts(&out);

    parent
        .run(Live::new().stop(Stop::After(Duration::from_millis(140))))
        .unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("child sender should finish after receiver drains");
    child.join().unwrap();

    let flattened: Vec<i64> = seen.borrow().iter().flatten().copied().collect();
    assert_eq!(flattened, vec![1, 2, 3, 4]);
}

#[test]
#[should_panic(expected = "bounded channel capacity must be greater than zero")]
fn bounded_zero_capacity_is_rejected() {
    let _ = channel::<i64>(Capacity::Bounded(0));
}
