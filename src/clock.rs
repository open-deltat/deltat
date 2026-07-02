//! The determinism seam.
//!
//! Every ambient wall-clock read in the engine flows through [`Clock`]. Production uses
//! [`SystemClock`]; tests and simulations inject a deterministic clock so that a run is
//! reproducible from a seed. This is the ONLY module permitted to call `SystemTime::now()`;
//! `scripts/check-no-ambient-time.sh` enforces that in CI, and a clippy `disallowed-methods`
//! rule bans it everywhere else.
#![allow(clippy::disallowed_methods)]

use crate::model::Ms;

/// A source of the current time, in UTC Unix milliseconds.
///
/// The engine never reads the wall clock directly; it holds an `Arc<dyn Clock>` and asks it.
/// That single indirection is what makes the whole system deterministically testable.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> Ms;
}

/// Production clock: real wall time.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Ms {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is set before the Unix epoch")
            .as_millis() as Ms
    }
}

/// Deterministic clock for tests and simulations: time only moves when you move it.
#[cfg(test)]
pub(crate) struct TestClock(std::sync::atomic::AtomicI64);

#[cfg(test)]
impl TestClock {
    pub(crate) fn new(now: Ms) -> Self {
        Self(std::sync::atomic::AtomicI64::new(now))
    }
    pub(crate) fn advance(&self, by: Ms) {
        self.0.fetch_add(by, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn set(&self, now: Ms) {
        self.0.store(now, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now_ms(&self) -> Ms {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Test-only real-now helper. Production code uses an injected [`Clock`]; this exists solely
/// so existing tests can compute timestamps without reaching for `SystemTime` themselves.
#[cfg(test)]
pub(crate) fn now_ms() -> Ms {
    SystemClock.now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_clock_starts_advances_and_sets() {
        let c = TestClock::new(1_000);
        assert_eq!(c.now_ms(), 1_000);
        c.advance(500);
        assert_eq!(c.now_ms(), 1_500);
        c.set(42);
        assert_eq!(c.now_ms(), 42);
    }

    #[test]
    fn test_clock_is_a_clock_trait_object() {
        // The engine stores `Arc<dyn Clock>`; prove TestClock fits that hole.
        let c: Arc<dyn Clock> = Arc::new(TestClock::new(7));
        assert_eq!(c.now_ms(), 7);
    }

    #[test]
    fn system_clock_is_wired_and_sane() {
        // > 2020-01-01: a smoke test that the real clock is connected.
        assert!(SystemClock.now_ms() > 1_577_836_800_000);
    }
}
