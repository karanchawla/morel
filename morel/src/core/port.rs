use std::cell::{Cell, Ref, RefCell};
use std::rc::Rc;

pub(crate) type SharedValue<T> = Rc<RefCell<Option<T>>>;
pub(crate) type FireFlag = Rc<Cell<bool>>;

/// Write side of a node's output slot.
///
/// Setting the value marks the node as fired for the current engine step.
pub struct Output<T> {
    pub(crate) value: SharedValue<T>,
    pub(crate) fired: FireFlag,
}

impl<T> Output<T> {
    /// Store a new value and fire the output.
    pub fn set(&self, value: T) {
        *self.value.borrow_mut() = Some(value);
        self.fired.set(true);
    }

    /// Mutate the stored value in place, initializing it first if needed.
    pub fn update(&self, init: impl FnOnce() -> T, f: impl FnOnce(&mut T)) {
        f(self.value.borrow_mut().get_or_insert_with(init));
        self.fired.set(true);
    }
}

/// Read side of an upstream output slot.
pub struct Input<T> {
    pub(crate) value: SharedValue<T>,
    pub(crate) fired: FireFlag,
}

impl<T> Input<T> {
    /// Whether the upstream node fired in the current engine step.
    pub fn fired(&self) -> bool {
        self.fired.get()
    }

    /// Whether the upstream has produced at least one value.
    pub fn has_value(&self) -> bool {
        self.value.borrow().is_some()
    }

    /// Borrow the current value without cloning.
    pub fn borrow(&self) -> Option<Ref<'_, T>> {
        Ref::filter_map(self.value.borrow(), Option::as_ref).ok()
    }
}

impl<T: Clone> Input<T> {
    /// Clone the current value.
    ///
    /// Panics if the upstream has not produced yet. Use [`Input::peek`] for
    /// watched inputs or nodes with multiple triggers.
    pub fn get(&self) -> T {
        self.peek().expect(
            "input read before upstream produced a value; use peek() for inputs that may be empty",
        )
    }

    /// Clone the current value, if one exists.
    pub fn peek(&self) -> Option<T> {
        self.value.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    fn pair<T>() -> (Output<T>, Input<T>) {
        let value: SharedValue<T> = Rc::new(RefCell::new(None));
        let fired: FireFlag = Rc::new(Cell::new(false));
        (
            Output {
                value: value.clone(),
                fired: fired.clone(),
            },
            Input { value, fired },
        )
    }

    #[test]
    fn set_marks_fired_and_stores() {
        let (out, input) = pair();
        assert!(!input.has_value());
        assert!(!input.fired());
        out.set(7);
        assert!(input.fired());
        assert_eq!(input.get(), 7);
        assert_eq!(input.peek(), Some(7));
    }

    #[test]
    fn peek_on_empty_is_none() {
        let (_out, input) = pair::<i32>();
        assert_eq!(input.peek(), None);
        assert!(input.borrow().is_none());
    }

    #[test]
    #[should_panic(expected = "input read before upstream produced")]
    fn get_on_empty_panics() {
        let (_out, input) = pair::<i32>();
        input.get();
    }

    #[test]
    fn update_initialises_then_mutates() {
        let (out, input) = pair::<Vec<i32>>();
        out.update(Vec::new, |v| v.push(1));
        out.update(Vec::new, |v| v.push(2));
        assert_eq!(input.get(), vec![1, 2]);
        assert!(input.fired());
    }

    #[test]
    fn borrow_reads_without_clone() {
        let (out, input) = pair();
        out.set(String::from("hello"));
        assert_eq!(input.borrow().unwrap().as_str(), "hello");
    }

    #[test]
    fn fired_flag_is_shared_and_clearable() {
        let (out, input) = pair();
        out.set(1);
        input.fired.set(false);
        assert!(!input.fired());
        assert_eq!(input.peek(), Some(1));
    }
}
