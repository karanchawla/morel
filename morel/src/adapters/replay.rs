use std::iter::Peekable;

use crate::core::{Ctx, Graph, Operator, Output, Stream, Time};

pub(crate) type FallibleItems<T> = Box<dyn Iterator<Item = Result<(Time, T), String>>>;

struct ReplayIter<T: Clone + 'static> {
    items: Peekable<FallibleItems<T>>,
    out: Output<T>,
}

impl<T: Clone + 'static> ReplayIter<T> {
    fn schedule_next(&mut self, cx: &mut Ctx) {
        match self.items.peek() {
            Some(Ok((at, _))) if *at < cx.now() => cx.fail(format!(
                "replay source item at {at} is behind the run at {}",
                cx.now()
            )),
            Some(Ok((at, _))) => cx.at(*at),
            Some(Err(message)) => cx.fail(message.clone()),
            None => {}
        }
    }
}

impl<T: Clone + 'static> Operator for ReplayIter<T> {
    fn on_start(&mut self, cx: &mut Ctx) {
        if cx.is_live() {
            cx.fail("replay source used in a live run");
            return;
        }

        self.schedule_next(cx);
    }

    fn step(&mut self, cx: &mut Ctx) {
        let value = match self.items.next() {
            Some(Ok((_at, value))) => value,
            Some(Err(message)) => {
                cx.fail(message);
                return;
            }
            None => return,
        };

        self.schedule_next(cx);
        self.out.set(value);
    }
}

pub(crate) fn replay_from_fallible<T: Clone + 'static>(
    g: &Graph,
    items: FallibleItems<T>,
) -> Stream<T> {
    g.add(|w| ReplayIter {
        items: items.peekable(),
        out: w.output(),
    })
}

impl Graph {
    pub fn replay_from_iter<T, I>(&self, items: I) -> Stream<T>
    where
        T: Clone + 'static,
        I: IntoIterator<Item = (Time, T)>,
        I::IntoIter: 'static,
    {
        replay_from_fallible(
            self,
            Box::new(items.into_iter().map(Ok::<(Time, T), String>)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Graph, Replay, Time};

    #[test]
    fn err_item_fails_the_run_with_its_message() {
        let g = Graph::new();
        let items: FallibleItems<i64> = Box::new(
            vec![
                Ok((Time::from_nanos(10), 1i64)),
                Err("bad row: line 3".to_string()),
            ]
            .into_iter(),
        );
        let src = replay_from_fallible(&g, items);

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert_eq!(src.peek(), Some(1), "valid prefix still emits");
        assert!(err.to_string().contains("bad row: line 3"));
    }
}
