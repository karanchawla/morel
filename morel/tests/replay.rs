use morel::core::{Ctx, Graph, Operator, Output, Replay, Stop, Stream, Time};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

/// Test source driven entirely by scheduled replay time.
struct Scheduled {
    values: Vec<(i32, Time)>,
    index: usize,
    out: Output<i32>,
}

impl Operator for Scheduled {
    fn on_start(&mut self, cx: &mut Ctx) {
        if let Some(&(_, at)) = self.values.first() {
            cx.at(at);
        }
    }

    fn step(&mut self, cx: &mut Ctx) {
        let (v, _) = self.values[self.index];
        self.index += 1;
        if let Some(&(_, at)) = self.values.get(self.index) {
            cx.at(at);
        }
        self.out.set(v);
    }
}

fn scheduled(g: &Graph, values: Vec<(i32, u64)>) -> Stream<i32> {
    let values = values
        .into_iter()
        .map(|(v, nanos)| (v, Time::from_nanos(nanos)))
        .collect();
    g.add(move |w| Scheduled {
        values,
        index: 0,
        out: w.output(),
    })
}

fn collect(s: &Stream<i32>) -> Rc<RefCell<Vec<i32>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let s2 = seen.clone();
    s.sink(move |v, _| s2.borrow_mut().push(v));
    seen
}

#[test]
fn time_teleportation() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(1, 100), (2, 200), (3, 1_000_000_000)]);
    let seen = collect(&src);
    g.run(Replay::from(Time::EPOCH)).unwrap();
    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
}

#[test]
fn pipeline_over_scheduled_source() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(10, 100), (20, 200), (30, 300)]);
    let seen = collect(&src.map(|x| x * 2));
    g.run(Replay::from(Time::EPOCH)).unwrap();
    assert_eq!(*seen.borrow(), vec![20, 40, 60]);
}

#[test]
fn stop_steps_limits_run() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)]);
    let seen = collect(&src);
    g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(3)))
        .unwrap();
    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
}

#[test]
fn stop_after_limits_run() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(1, 100), (2, 200), (3, 1_000_000_000)]);
    let seen = collect(&src);
    g.run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(500))))
        .unwrap();
    assert_eq!(*seen.borrow(), vec![1, 2]);
}

#[test]
fn filter_over_scheduled_source() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)]);
    let seen = collect(&src.filter(|x| x % 2 == 0));
    g.run(Replay::from(Time::EPOCH)).unwrap();
    assert_eq!(*seen.borrow(), vec![2, 4]);
}

#[test]
fn scan_accumulates() {
    let g = Graph::new();
    let src = scheduled(&g, vec![(1, 100), (2, 200), (3, 300), (4, 400)]);
    let sum = src.scan(0i32, |acc, x| *acc += x);
    g.run(Replay::from(Time::EPOCH)).unwrap();
    assert_eq!(sum.peek(), Some(10));
}

#[test]
fn empty_source_terminates_immediately() {
    let g = Graph::new();
    let src = scheduled(&g, vec![]);
    let seen = collect(&src);
    let summary = g.run(Replay::from(Time::EPOCH)).unwrap();
    assert!(seen.borrow().is_empty());
    assert_eq!(summary.steps, 0);
}
