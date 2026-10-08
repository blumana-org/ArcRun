//! Circuit breaker pattern implementation for resilient connection handling.
//!
//! The circuit breaker prevents cascade failures by fast-failing requests when
//! the system is under stress (e.g., database connection pool exhausted).
//!
//! States:
//! - Closed: Normal operation, requests flow through
//! - Open: Failing fast, requests rejected immediately
//! - HalfOpen: Testing if system recovered, allowing one probe request
//!
//! This implementation is fully lock-free using atomic operations.
//! The hot path (Closed state) is a single atomic load with no contention.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Normal operation - requests flow through
    Closed,
    /// Circuit tripped - requests fail immediately
    Open,
    /// Testing recovery - one request allowed through
    HalfOpen,
}

impl State {
    fn from_u8(v: u8) -> Self {
        match v {
            STATE_OPEN => State::Open,
            STATE_HALF_OPEN => State::HalfOpen,
            _ => State::Closed,
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Closed => write!(f, "closed"),
            State::Open => write!(f, "open"),
            State::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// Configuration for the circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before circuit opens
    pub failure_threshold: u32,
    /// Time window for counting failures
    pub failure_window: Duration,
    /// How long to wait before trying half-open
    pub recovery_timeout: Duration,
    /// Number of successes in half-open to close circuit
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            failure_window: Duration::from_secs(10),
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 2,
        }
    }
}

/// Thread-safe, lock-free circuit breaker implementation.
///
/// All operations use atomic compare-and-swap for state transitions,
/// making the hot path (Closed state) a single atomic load.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Current state as AtomicU8 (0=Closed, 1=Open, 2=HalfOpen)
    state: AtomicU8,
    /// Failure count in current window
    failure_count: AtomicU32,
    /// Success count in half-open state
    half_open_successes: AtomicU32,
    /// Whether a probe request is currently in flight in HalfOpen. Ensures only
    /// ONE probe is admitted at a time (all other HalfOpen requests are rejected
    /// like Open) until the probe's verdict is recorded.
    probe_in_flight: AtomicBool,
    /// When the in-flight probe was admitted (epoch millis). A probe whose future
    /// is dropped before recording a verdict (client disconnect mid-acquire) would
    /// otherwise leave `probe_in_flight` stuck true and wedge HalfOpen forever —
    /// after `recovery_timeout` a new probe may take over the slot (see
    /// `try_admit_probe`).
    probe_started_at: AtomicU64,
    /// Timestamp when failure window started (as epoch millis)
    window_start: AtomicU64,
    /// Timestamp when circuit opened (as epoch millis)
    opened_at: AtomicU64,
    /// Reference instant for time calculations
    epoch: Instant,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let epoch = Instant::now();
        Self {
            config,
            state: AtomicU8::new(STATE_CLOSED),
            failure_count: AtomicU32::new(0),
            half_open_successes: AtomicU32::new(0),
            probe_in_flight: AtomicBool::new(false),
            probe_started_at: AtomicU64::new(0),
            window_start: AtomicU64::new(0),
            opened_at: AtomicU64::new(0),
            epoch,
        }
    }

    /// Get current time as milliseconds since epoch
    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Get the current state of the circuit breaker
    pub fn state(&self) -> State {
        State::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Check if a request should be allowed through.
    /// Returns Ok(()) if allowed, Err with state if rejected.
    pub fn check(&self) -> Result<(), State> {
        let current = self.state.load(Ordering::Acquire);

        match current {
            STATE_CLOSED => {
                // Hot path: single atomic load, no contention
                let now = self.now_millis();
                let window_start = self.window_start.load(Ordering::Relaxed);
                let window_ms = self.config.failure_window.as_millis() as u64;

                if now.saturating_sub(window_start) > window_ms {
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.window_start.store(now, Ordering::Relaxed);
                }
                Ok(())
            }
            STATE_OPEN => {
                let now = self.now_millis();
                let opened_at = self.opened_at.load(Ordering::Relaxed);
                let recovery_ms = self.config.recovery_timeout.as_millis() as u64;

                if now.saturating_sub(opened_at) >= recovery_ms {
                    // Try to transition Open -> HalfOpen via CAS
                    if self
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        log::info!("Circuit breaker transitioning from Open to HalfOpen");
                        self.half_open_successes.store(0, Ordering::Relaxed);
                        // Fresh probe slot for this HalfOpen window.
                        self.probe_in_flight.store(false, Ordering::Relaxed);
                        crate::metrics::record_circuit_breaker_state("half_open");
                    }
                    // Now in HalfOpen (whether we or another thread transitioned it):
                    // admit at most ONE probe; reject all other requests as Open.
                    self.try_admit_probe()
                } else {
                    Err(State::Open)
                }
            }
            STATE_HALF_OPEN => self.try_admit_probe(),
            _ => Ok(()),
        }
    }

    /// Admit a single probe in HalfOpen: the first caller to flip `probe_in_flight`
    /// from false to true is allowed through; concurrent/subsequent callers are
    /// rejected as Open until the probe's verdict is recorded.
    ///
    /// **Stale-probe takeover**: a probe that never records a verdict (its future
    /// was dropped between `check()` and the record call — e.g. the client
    /// disconnected while the acquire was pending, exactly the load pattern that
    /// puts the breaker in HalfOpen) must not wedge the slot forever. If the
    /// in-flight probe is older than `recovery_timeout`, the next caller takes the
    /// slot over (CAS on the admission timestamp so exactly one taker wins).
    fn try_admit_probe(&self) -> Result<(), State> {
        let now = self.now_millis();
        if self
            .probe_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.probe_started_at.store(now, Ordering::Relaxed);
            return Ok(());
        }
        // Slot taken — is the holder stale (verdict never recorded)?
        let started = self.probe_started_at.load(Ordering::Relaxed);
        if now.saturating_sub(started) > self.config.recovery_timeout.as_millis() as u64
            && self
                .probe_started_at
                .compare_exchange(started, now, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            log::warn!(
                "Circuit breaker: half-open probe never recorded a verdict for {:?}; \
                 admitting a replacement probe",
                self.config.recovery_timeout
            );
            return Ok(());
        }
        Err(State::Open)
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let current = self.state.load(Ordering::Acquire);

        if current == STATE_HALF_OPEN {
            let successes = self.half_open_successes.fetch_add(1, Ordering::Relaxed) + 1;
            if successes >= self.config.success_threshold {
                // Try to transition HalfOpen -> Closed via CAS
                if self
                    .state
                    .compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_CLOSED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    log::info!(
                        "Circuit breaker closing after {} successful probes",
                        successes
                    );
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.window_start
                        .store(self.now_millis(), Ordering::Relaxed);
                    crate::metrics::record_circuit_breaker_state("closed");
                }
            } else {
                // Not enough probes yet (success_threshold > 1). Release the probe
                // slot so the NEXT request becomes the next probe — probes are
                // admitted strictly one at a time.
                self.probe_in_flight.store(false, Ordering::Relaxed);
            }
        }
        // Closed or Open: nothing to do
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let current = self.state.load(Ordering::Acquire);
        let now = self.now_millis();

        match current {
            STATE_CLOSED => {
                let window_start = self.window_start.load(Ordering::Relaxed);
                let window_ms = self.config.failure_window.as_millis() as u64;

                if now.saturating_sub(window_start) > window_ms {
                    // Window expired, start fresh
                    self.failure_count.store(1, Ordering::Relaxed);
                    self.window_start.store(now, Ordering::Relaxed);
                } else {
                    let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if failures >= self.config.failure_threshold {
                        // Try to transition Closed -> Open via CAS
                        if self
                            .state
                            .compare_exchange(
                                STATE_CLOSED,
                                STATE_OPEN,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            log::warn!(
                                "Circuit breaker opening after {} failures in {}ms window",
                                failures,
                                window_ms
                            );
                            self.opened_at.store(now, Ordering::Relaxed);
                            crate::metrics::record_circuit_breaker_state("open");
                        }
                    }
                }
            }
            // Probe failed, back to open.
            STATE_HALF_OPEN
                if self
                    .state
                    .compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_OPEN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok() =>
            {
                log::warn!("Circuit breaker probe failed, returning to Open state");
                self.opened_at.store(now, Ordering::Relaxed);
                crate::metrics::record_circuit_breaker_state("open");
            }
            _ => {
                // Already open, or another probe already returned it to Open.
            }
        }
    }

    /// Get current failure count (for monitoring)
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn test_allows_requests_when_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_opens_after_threshold_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            failure_window: Duration::from_secs(60),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Record failures below threshold
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), State::Closed);

        // Third failure should trip the breaker
        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
    }

    #[test]
    fn test_rejects_when_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        assert_eq!(cb.state(), State::Open);

        let result = cb.check();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), State::Open);
    }

    #[test]
    fn test_transitions_to_half_open_after_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        assert_eq!(cb.state(), State::Open);

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(15));

        // Check should transition to half-open and allow request
        assert!(cb.check().is_ok());
        assert_eq!(cb.state(), State::HalfOpen);
    }

    #[test]
    fn test_closes_after_successful_probes() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            success_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Trip the breaker
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(5));

        // Transition to half-open
        cb.check().ok();
        assert_eq!(cb.state(), State::HalfOpen);

        // First success
        cb.record_success();
        assert_eq!(cb.state(), State::HalfOpen);

        // Second success should close
        cb.record_success();
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn test_returns_to_open_on_half_open_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Trip and transition to half-open
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(5));
        cb.check().ok();
        assert_eq!(cb.state(), State::HalfOpen);

        // Failure in half-open returns to open
        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
    }

    /// Audit 2, B7 (a): several failures inside the counting window trip the
    /// breaker. Mirrors what `get_conn_with_retry` now produces by recording a
    /// failure per failed acquisition attempt (rather than once after all retries).
    #[test]
    fn test_consecutive_failures_within_window_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            failure_window: Duration::from_secs(10),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Three rapid failures (as if from one request's three retry attempts, or
        // three requests) all land inside the 10s window and open the circuit.
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), State::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
    }

    /// Audit 2, B7 (b): in HalfOpen only ONE probe is admitted. The first request
    /// passes; a second is rejected (as Open) while the probe is unresolved. A
    /// successful probe (threshold 1) closes the circuit.
    #[test]
    fn test_half_open_admits_single_probe_then_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            success_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Trip and wait out the recovery timeout.
        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
        std::thread::sleep(Duration::from_millis(5));

        // First request in HalfOpen is admitted as the probe.
        assert!(cb.check().is_ok());
        assert_eq!(cb.state(), State::HalfOpen);

        // Second request while the probe is unresolved is rejected as Open.
        let second = cb.check();
        assert!(second.is_err());
        assert_eq!(second.unwrap_err(), State::Open);
        // Still HalfOpen — the rejection did not change state.
        assert_eq!(cb.state(), State::HalfOpen);

        // Probe verdict = success -> Closed.
        cb.record_success();
        assert_eq!(cb.state(), State::Closed);
    }

    /// Audit 2, B7 (b): a failed probe returns to Open, and extra HalfOpen requests
    /// were rejected while it was in flight.
    #[test]
    fn test_half_open_single_probe_failure_reopens() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            success_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(5));

        // Probe admitted, extra request rejected.
        assert!(cb.check().is_ok());
        assert!(cb.check().is_err());

        // Probe verdict = failure -> back to Open.
        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
    }

    /// Audit 2, B7 (b): with success_threshold > 1, probes are admitted strictly
    /// one at a time — a partial success releases the slot for the next probe.
    #[test]
    fn test_half_open_probes_are_serial_with_threshold_two() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            success_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(5));

        // Probe #1 admitted; a concurrent request is rejected.
        assert!(cb.check().is_ok());
        assert!(cb.check().is_err());
        assert_eq!(cb.state(), State::HalfOpen);

        // Partial success (1 of 2): stays HalfOpen, releases the slot.
        cb.record_success();
        assert_eq!(cb.state(), State::HalfOpen);

        // Probe #2 is now admitted; another concurrent request is still rejected.
        assert!(cb.check().is_ok());
        assert!(cb.check().is_err());

        // Second success reaches the threshold -> Closed.
        cb.record_success();
        assert_eq!(cb.state(), State::Closed);
    }

    /// Review hardening of B7 (b): a probe whose future is dropped WITHOUT recording
    /// a verdict (client disconnect between `check()` and the record call) must not
    /// wedge HalfOpen forever. After `recovery_timeout`, a new probe takes over the
    /// slot; before that, requests are still rejected (single-probe invariant).
    #[test]
    fn test_half_open_stale_probe_is_taken_over() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(10),
            success_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(15));

        // Probe admitted... and its caller vanishes without a verdict.
        assert!(cb.check().is_ok());
        // Immediately after: still rejected (probe not stale yet).
        assert!(cb.check().is_err());

        // Once the probe is older than recovery_timeout, a replacement is admitted.
        std::thread::sleep(Duration::from_millis(15));
        assert!(
            cb.check().is_ok(),
            "a stale (never-resolved) probe must be taken over, not wedge HalfOpen"
        );
        // And the single-probe invariant holds for the replacement too.
        assert!(cb.check().is_err());

        // The replacement's verdict works as usual.
        cb.record_success();
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn test_failure_window_resets() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            failure_window: Duration::from_millis(10),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // Two failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);

        // Wait for window to expire
        std::thread::sleep(Duration::from_millis(15));

        // Next failure should reset the counter
        cb.record_failure();
        assert_eq!(cb.failure_count(), 1);
        assert_eq!(cb.state(), State::Closed);
    }
}
