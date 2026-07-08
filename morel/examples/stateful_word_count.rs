//! Stateful word counting with `scan`, `map`, and deterministic replay history.

use std::collections::BTreeMap;

use morel::{Graph, Replay, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatefulWordCountOutput {
    pub snapshots: Vec<(u64, Vec<(String, u64)>)>,
}

pub fn run() -> Result<StatefulWordCountOutput, morel::Error> {
    let graph = Graph::new();
    let lines = graph.replay_from_iter(
        [
            (0, "morel streams replay cleanly"),
            (10, "streams compose with rust functions"),
            (20, "rust examples should stay deterministic"),
            (30, "morel replay makes tests deterministic"),
        ]
        .map(|(nanos, line)| (Time::from_nanos(nanos), line)),
    );

    let top_words = lines
        .scan(BTreeMap::<String, u64>::new(), |counts, line| {
            for word in words(line) {
                *counts.entry(word).or_default() += 1;
            }
        })
        .map(|counts| top_three(&counts));
    let history = top_words.history();

    graph.run(Replay::from(Time::EPOCH))?;

    Ok(StatefulWordCountOutput {
        snapshots: history
            .peek()
            .expect("word-count history should emit during replay")
            .into_iter()
            .map(|(time, words)| (time.as_nanos(), words))
            .collect(),
    })
}

fn words(line: &str) -> impl Iterator<Item = String> + '_ {
    line.split_whitespace()
        .map(|word| word.trim_matches(|ch: char| ch.is_ascii_punctuation()))
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
}

fn top_three(counts: &BTreeMap<String, u64>) -> Vec<(String, u64)> {
    let mut words: Vec<_> = counts
        .iter()
        .map(|(word, count)| (word.clone(), *count))
        .collect();
    words.sort_by(|(left_word, left_count), (right_word, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_word.cmp(right_word))
    });
    words.truncate(3);
    words
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    for (nanos, words) in output.snapshots {
        println!("{nanos}ns {:?}", words);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_snapshot_ranks_by_count_then_word() {
        assert_eq!(
            run().unwrap().snapshots.last(),
            Some(&(
                30,
                vec![
                    ("deterministic".to_string(), 2),
                    ("morel".to_string(), 2),
                    ("replay".to_string(), 2),
                ],
            ))
        );
    }
}
