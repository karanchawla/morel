use std::fmt;
use std::ops::{Add, Sub};
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quanta::Clock;

/// Epoch-anchored monotonic clock used by live mode.
///
/// Calibration pays for one `SystemTime` read, then `wall_now` can use
/// `quanta`'s counter reads on the hot path.
struct WallClock {
    clock: Clock,
    anchor_raw: u64,
    anchor_epoch_nanos: u64,
}

impl WallClock {
    fn new() -> Self {
        let clock = Clock::new();
        let anchor_raw_before = clock.now().as_u64();
        let anchor_epoch_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before the unix epoch")
            .as_nanos() as u64;
        let anchor_raw_after = clock.now().as_u64();
        let anchor_raw =
            anchor_raw_before + (anchor_raw_after.saturating_sub(anchor_raw_before) / 2);
        Self {
            clock,
            anchor_raw,
            anchor_epoch_nanos,
        }
    }

    fn now_nanos(&self) -> u64 {
        let elapsed = self.clock.now().as_u64().saturating_sub(self.anchor_raw);
        self.anchor_epoch_nanos.saturating_add(elapsed)
    }
}

static CLOCK: LazyLock<WallClock> = LazyLock::new(WallClock::new);

/// Calibrate the live clock before entering latency-sensitive code.
pub fn init_clock() {
    let _ = &*CLOCK;
}

/// Nanoseconds since the Unix epoch.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(u64);

impl Time {
    pub const EPOCH: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Current live-mode time.
    pub fn wall_now() -> Self {
        Self(CLOCK.now_nanos())
    }

    pub fn saturating_sub(self, d: Duration) -> Self {
        Self(self.0.saturating_sub(duration_nanos(d)))
    }
}

impl Add<Duration> for Time {
    type Output = Time;

    fn add(self, d: Duration) -> Time {
        Time(self.0.saturating_add(duration_nanos(d)))
    }
}

impl Sub<Time> for Time {
    type Output = Duration;

    fn sub(self, earlier: Time) -> Duration {
        Duration::from_nanos(
            self.0
                .checked_sub(earlier.0)
                .expect("cannot subtract a later Time from an earlier Time"),
        )
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{:09}",
            self.0 / 1_000_000_000,
            self.0 % 1_000_000_000
        )
    }
}

fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn nanos_roundtrip() {
        assert_eq!(Time::from_nanos(1_500_000_000).as_nanos(), 1_500_000_000);
        assert_eq!(Time::EPOCH.as_nanos(), 0);
        assert_eq!(Time::MAX.as_nanos(), u64::MAX);
    }

    #[test]
    fn add_duration() {
        let t = Time::from_nanos(1_000);
        assert_eq!((t + Duration::from_nanos(500)).as_nanos(), 1_500);
    }

    #[test]
    fn add_saturates_at_max() {
        assert_eq!(Time::MAX + Duration::from_secs(1), Time::MAX);
    }

    #[test]
    fn sub_gives_duration() {
        let a = Time::from_nanos(2_000);
        let b = Time::from_nanos(500);
        assert_eq!(a - b, Duration::from_nanos(1_500));
    }

    #[test]
    #[should_panic(expected = "cannot subtract a later Time from an earlier Time")]
    fn reversed_subtraction_panics() {
        let earlier = Time::from_nanos(500);
        let later = Time::from_nanos(2_000);
        let _ = earlier - later;
    }

    #[test]
    fn saturating_sub_duration() {
        let t = Time::from_nanos(100);
        assert_eq!(t.saturating_sub(Duration::from_nanos(500)), Time::EPOCH);
        assert_eq!(t.saturating_sub(Duration::from_nanos(40)).as_nanos(), 60);
    }

    #[test]
    fn ordering() {
        assert!(Time::from_nanos(1) < Time::from_nanos(2));
    }

    #[test]
    fn wall_now_is_epoch_anchored() {
        // Guard against returning raw monotonic ticks instead of epoch time.
        let now = Time::wall_now().as_nanos();
        assert!(now > 1_577_836_800_000_000_000);
        assert!(now < 4_102_444_800_000_000_000);
    }

    #[test]
    fn wall_now_is_monotonic() {
        init_clock();
        let mut prev = Time::wall_now();
        for _ in 0..1_000 {
            let next = Time::wall_now();
            assert!(next >= prev);
            prev = next;
        }
    }

    #[test]
    fn display_seconds_and_nanos() {
        assert_eq!(Time::from_nanos(1_500_000_000).to_string(), "1.500000000");
    }
}
