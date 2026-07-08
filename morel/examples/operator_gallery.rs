//! A compact gallery of Morel's core stateless and stateful stream operators.

use std::cell::RefCell;
use std::rc::Rc;

use morel::{Graph, Replay, Time};

#[derive(Clone, Debug, PartialEq)]
pub struct OperatorGalleryOutput {
    pub even_labels: Vec<(u64, String)>,
    pub parsed_numbers: Vec<(u64, i64)>,
    pub distinct_values: Vec<(u64, i64)>,
    pub taken_values: Vec<(u64, i64)>,
    pub inspected: Vec<(u64, i64)>,
    pub timestamped: Vec<(u64, i64)>,
    pub sink_seen: Vec<(u64, i64)>,
    pub try_sink_seen: Vec<(u64, i64)>,
    pub delta_values: Vec<(u64, i64)>,
    pub reduce_max: Option<i64>,
    pub sum: Option<i64>,
    pub mean: Option<f64>,
    pub count: Option<u64>,
    pub accumulated: Option<Vec<i64>>,
    pub history: Vec<(u64, i64)>,
}

pub fn run() -> Result<OperatorGalleryOutput, morel::Error> {
    let graph = Graph::new();
    let numbers = graph.replay_from_iter(
        [(0, 1i64), (10, 2), (20, 2), (30, 3), (40, 4)]
            .map(|(nanos, value)| (Time::from_nanos(nanos), value)),
    );
    let parse_source = graph.replay_from_iter(
        [(0, "1"), (10, "2"), (20, "3")].map(|(nanos, value)| (Time::from_nanos(nanos), value)),
    );

    let even_labels = numbers
        .filter_map(|value| (value % 2 == 0).then(|| format!("even-{value}")))
        .history();
    let parsed_numbers = parse_source.try_map(|value| value.parse::<i64>()).history();
    let distinct_values = numbers.distinct().history();
    let taken_values = numbers.take(3).history();

    let inspected = Rc::new(RefCell::new(Vec::new()));
    let inspected_for_operator = inspected.clone();
    let _inspected_history = numbers
        .inspect(move |value, time| {
            inspected_for_operator
                .borrow_mut()
                .push((time.as_nanos(), *value));
        })
        .history();

    let timestamped = numbers.timestamp().history();

    let sink_seen = Rc::new(RefCell::new(Vec::new()));
    let sink_seen_for_operator = sink_seen.clone();
    numbers.sink(move |value, time| {
        sink_seen_for_operator
            .borrow_mut()
            .push((time.as_nanos(), value));
    });

    let try_sink_seen = Rc::new(RefCell::new(Vec::new()));
    let try_sink_seen_for_operator = try_sink_seen.clone();
    numbers.try_sink(move |value, time| -> Result<(), std::convert::Infallible> {
        try_sink_seen_for_operator
            .borrow_mut()
            .push((time.as_nanos(), value));
        Ok(())
    });

    let reduce_max = numbers.reduce(i64::max);
    let sum = numbers.sum();
    let delta_values = numbers.delta().history();
    let mean = numbers.mean();
    let count = numbers.count();
    let accumulated = numbers.accumulate();
    let history = numbers.history();

    graph.run(Replay::from(Time::EPOCH))?;

    let inspected = inspected.borrow().clone();
    let sink_seen = sink_seen.borrow().clone();
    let try_sink_seen = try_sink_seen.borrow().clone();

    Ok(OperatorGalleryOutput {
        even_labels: time_history(even_labels.peek().expect("even labels should emit")),
        parsed_numbers: time_history(parsed_numbers.peek().expect("parsed numbers should emit")),
        distinct_values: time_history(distinct_values.peek().expect("distinct values should emit")),
        taken_values: time_history(taken_values.peek().expect("taken values should emit")),
        inspected,
        timestamped: timestamp_history(timestamped.peek().expect("timestamps should emit")),
        sink_seen,
        try_sink_seen,
        delta_values: time_history(delta_values.peek().expect("deltas should emit")),
        reduce_max: reduce_max.peek(),
        sum: sum.peek(),
        mean: mean.peek(),
        count: count.peek(),
        accumulated: accumulated.peek(),
        history: time_history(history.peek().expect("history should emit")),
    })
}

fn time_history<T>(values: Vec<(Time, T)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect()
}

fn timestamp_history(values: Vec<(Time, (Time, i64))>) -> Vec<(u64, i64)> {
    values
        .into_iter()
        .map(|(_history_time, (event_time, value))| (event_time.as_nanos(), value))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("even_labels={:?}", output.even_labels);
    println!("parsed_numbers={:?}", output.parsed_numbers);
    println!(
        "reduce_max={:?} sum={:?} mean={:?} count={:?}",
        output.reduce_max, output.sum, output.mean, output.count
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_numbers() -> Vec<(u64, i64)> {
        vec![(0, 1), (10, 2), (20, 2), (30, 3), (40, 4)]
    }

    #[test]
    fn stateless_operators_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.even_labels,
            vec![
                (10, "even-2".to_string()),
                (20, "even-2".to_string()),
                (40, "even-4".to_string()),
            ]
        );
        assert_eq!(output.parsed_numbers, vec![(0, 1), (10, 2), (20, 3)]);
        assert_eq!(
            output.distinct_values,
            vec![(0, 1), (10, 2), (30, 3), (40, 4)]
        );
        assert_eq!(output.taken_values, vec![(0, 1), (10, 2), (20, 2)]);
        assert_eq!(output.inspected, all_numbers());
        assert_eq!(output.timestamped, all_numbers());
        assert_eq!(output.sink_seen, all_numbers());
        assert_eq!(output.try_sink_seen, all_numbers());
    }

    #[test]
    fn stateful_operators_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.delta_values,
            vec![(10, 1), (20, 0), (30, 1), (40, 1)]
        );
        assert_eq!(output.reduce_max, Some(4));
        assert_eq!(output.sum, Some(12));
        assert_eq!(output.mean, Some(2.4));
        assert_eq!(output.count, Some(5));
        assert_eq!(output.accumulated, Some(vec![1, 2, 2, 3, 4]));
        assert_eq!(output.history, all_numbers());
    }
}
