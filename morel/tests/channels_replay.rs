use morel::{channel, Capacity, Ctx, Graph, OnClose, Operator, Output, Replay, Stream, Time};
use std::cell::RefCell;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

type TimedBursts = Rc<RefCell<Vec<(Vec<i64>, Time)>>>;

struct Scheduled {
    values: Vec<(i64, Time)>,
    index: usize,
    out: Output<i64>,
}

impl Operator for Scheduled {
    fn on_start(&mut self, cx: &mut Ctx) {
        if let Some((_, at)) = self.values.first() {
            cx.at(*at);
        }
    }

    fn step(&mut self, cx: &mut Ctx) {
        let (value, _) = self.values[self.index];
        self.index += 1;
        if let Some((_, at)) = self.values.get(self.index) {
            cx.at(*at);
        }
        self.out.set(value);
    }
}

fn scheduled(g: &Graph, values: &[(i64, u64)]) -> Stream<i64> {
    let values = values
        .iter()
        .map(|(v, ms)| (*v, Time::EPOCH + Duration::from_millis(*ms)))
        .collect();
    g.add(move |w| Scheduled {
        values,
        index: 0,
        out: w.output(),
    })
}

fn collect_timed(s: &Stream<Vec<i64>>) -> TimedBursts {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen2 = seen.clone();
    s.sink(move |burst, at| seen2.borrow_mut().push((burst, at)));
    seen
}

#[test]
fn replay_channel_delivers_at_sender_virtual_time() {
    let parent = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    let child = thread::spawn(move || {
        let g = Graph::new();
        let src = scheduled(&g, &[(10, 5), (20, 50), (30, 90)]);
        let _sent = tx.attach(&src);
        g.run(Replay::from(Time::EPOCH)).unwrap();
    });
    let out = rx.into_stream(&parent, OnClose::Continue);
    let seen = collect_timed(&out);

    parent.run(Replay::from(Time::EPOCH)).unwrap();
    child.join().unwrap();

    assert_eq!(
        *seen.borrow(),
        vec![
            (vec![10], Time::EPOCH + Duration::from_millis(5)),
            (vec![20], Time::EPOCH + Duration::from_millis(50)),
            (vec![30], Time::EPOCH + Duration::from_millis(90)),
        ]
    );
}

#[test]
fn replay_paced_receiver_treats_same_time_watermark_as_progress() {
    let g = Graph::new();
    let (tx, rx) = channel::<i64>(Capacity::Unbounded);
    let src = scheduled(&g, &[(1, 10), (2, 20), (3, 30), (4, 40)]);
    let evens = src.filter(|x| x % 2 == 0);
    let _sent = tx.attach_with_heartbeat(&evens, &src);
    let out = rx.into_stream_paced(&src, OnClose::Continue);
    let seen = collect_timed(&out);

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(
        *seen.borrow(),
        vec![
            (vec![2], Time::EPOCH + Duration::from_millis(20)),
            (vec![4], Time::EPOCH + Duration::from_millis(40)),
        ]
    );
}
