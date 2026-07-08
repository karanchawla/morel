use morel::core::{init_clock, Ctx, Graph, Live, Operator, Output, Stop, Stream, Waker};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn live_steps_stop_is_exact() {
    for expected in [1u64, 3, 5, 10] {
        let g = Graph::new();
        let ticks = g.ticker(Duration::from_millis(10)).count();
        g.run(Live::new().stop(Stop::Steps(expected))).unwrap();
        assert_eq!(ticks.peek(), Some(expected));
    }
}

#[test]
fn live_ticker_intervals_are_roughly_accurate() {
    init_clock();
    let g = Graph::new();
    let stamps: Arc<std::sync::Mutex<Vec<Instant>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s2 = stamps.clone();
    g.ticker(Duration::from_millis(50))
        .sink(move |(), _| s2.lock().unwrap().push(Instant::now()));
    g.run(Live::new().stop(Stop::Steps(5))).unwrap();

    let ts = stamps.lock().unwrap();
    assert_eq!(ts.len(), 5);
    let total: Duration = ts.windows(2).map(|w| w[1].duration_since(w[0])).sum();
    let avg = total / (ts.len() - 1) as u32;
    assert!(
        avg >= Duration::from_millis(40) && avg <= Duration::from_millis(60),
        "average interval should be ~50ms, got {avg:?}"
    );
}

#[test]
fn live_duration_stop_is_respected() {
    init_clock();
    let g = Graph::new();
    let ticks = g.ticker(Duration::from_millis(20)).count();
    let start = Instant::now();
    g.run(Live::new().stop(Stop::After(Duration::from_millis(100))))
        .unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(90),
        "completed too early: {elapsed:?}"
    );
    assert!(
        elapsed <= Duration::from_millis(200),
        "completed too late: {elapsed:?}"
    );
    let n = ticks.peek().unwrap();
    assert!(
        (5..=6).contains(&n),
        "expected 5-6 ticks for 100ms/20ms, got {n}"
    );
}

struct Wakeable {
    waker_tx: mpsc::Sender<Waker>,
    steps: Arc<AtomicU32>,
    out: Output<()>,
}

impl Operator for Wakeable {
    fn on_start(&mut self, cx: &mut Ctx) {
        let _ = self.waker_tx.send(cx.waker());
    }

    fn step(&mut self, _cx: &mut Ctx) {
        self.steps.fetch_add(1, Ordering::SeqCst);
        self.out.set(());
    }
}

#[test]
fn each_spaced_wake_causes_one_step() {
    let g = Graph::new();
    let (tx, rx) = mpsc::channel();
    let steps = Arc::new(AtomicU32::new(0));
    let s2 = steps.clone();
    let _n: Stream<()> = g.add(|w| Wakeable {
        waker_tx: tx,
        steps: s2,
        out: w.output(),
    });

    let handle = thread::spawn(move || {
        let waker = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        for _ in 0..5 {
            thread::sleep(Duration::from_millis(30));
            waker.wake().expect("wake should succeed");
            thread::sleep(Duration::from_millis(10));
        }
    });

    g.run(Live::new().stop(Stop::After(Duration::from_millis(250))))
        .unwrap();
    handle.join().unwrap();

    assert_eq!(steps.load(Ordering::SeqCst), 5);
}

#[test]
fn empty_live_graph_waits_out_its_duration() {
    let g = Graph::new();
    let _idle = g.just(1);
    let start = Instant::now();
    g.run(Live::new().stop(Stop::After(Duration::from_millis(50))))
        .unwrap();
    assert!(
        start.elapsed() >= Duration::from_millis(45),
        "live run with Stop::After should wait, elapsed {:?}",
        start.elapsed()
    );
}
