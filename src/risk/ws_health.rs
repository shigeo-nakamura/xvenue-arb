//! Per-venue WS staleness watchdog (bot-strategy#244 Group C).
//!
//! Companion to `reference_guard` (cross-checks the *value* of each
//! venue's mid against an independent feed) — this module checks
//! the *liveness* of each venue's book stream. When neither a
//! successful `read_mid` nor a `book_ok=true` snapshot has landed
//! for `ws_stale_emergency_ms`, the runner escalates to
//! `Emergency{WsStale}` and the position machine routes through
//! `Phase::EmergencyFlattening`.
//!
//! Design rationale:
//!
//! - The dex-connector `get_order_book` API does not currently
//!   expose the underlying WS push timestamp — both Extended and
//!   Lighter return a cached snapshot built from the WS feed but
//!   surface only the prices, not the last-update time. So we use
//!   the runner's own observation of "last successful tick" as the
//!   proxy. If the WS goes silent the cached snapshot stops
//!   updating; the bot's `book_ok` flag also flips to false (zero
//!   size on one side) before then in many real outage modes.
//! - First-to-fire wins, dedup not required (per the #244 brief)
//!   — the state machine's `Phase::EmergencyFlattening` is sticky
//!   so additional `Emergency{WsStale}` events while flattening
//!   are no-ops.
//! - Warm-up gating: if a venue has never produced a healthy
//!   sample, `evaluate` returns `Healthy`. The `VenueWarmup` path
//!   in `live.rs` already prevents the loop from acting on partial
//!   data, so a never-ready venue must NOT trip this monitor.
//!
//! What this module does NOT own:
//!
//! - The actual `read_mid` / `book_ok` plumbing (lives in `live.rs`).
//! - Auto-recovery once the venue comes back. The state machine's
//!   `EmergencyComplete` path (Group B) clears the halt; this
//!   monitor stays out of the recovery loop.
//! - Deciding *which* venue caused the halt — the returned
//!   `WsHealthOutcome::Stale(VenueLabel)` is logged but the
//!   downstream emergency-flatten flattens both legs anyway.

use crate::risk::kill_switch::VenueLabel;

/// Per-venue health snapshot for status emission (`status.json`
/// `lt_ws_age_ms` / `ext_ws_age_ms` per #244 Group A schema).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsAge {
    pub ext_age_ms: Option<u64>,
    pub lt_age_ms: Option<u64>,
}

/// Outcome of one tick's evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsHealthOutcome {
    /// At least one venue has not yet produced a healthy book — caller
    /// must not act, but no halt either. Symmetric with the warm-up
    /// path.
    NotReady,
    /// Both venues have recent book updates within threshold.
    Healthy,
    /// One or both venues exceeded `ws_stale_emergency_ms`. The label
    /// reports the first-to-fire venue; if both are stale, Extended
    /// wins arbitrarily (downstream flattens both anyway).
    Stale(VenueLabel),
}

/// Per-venue WS staleness watchdog.
#[derive(Debug)]
pub struct WsHealthMonitor {
    ws_stale_emergency_ms: u64,
    ext_last_book_ms: Option<u64>,
    lt_last_book_ms: Option<u64>,
    /// Sticky once `evaluate` returned `Stale` for a venue, so
    /// status emission can show "still stale" even if a single
    /// late update sneaks through after escalation. Cleared on
    /// `reset_after_recovery` (called by the runner once the state
    /// machine returns to `Phase::Flat`).
    ext_stale_latched: bool,
    lt_stale_latched: bool,
}

impl WsHealthMonitor {
    /// `ws_stale_emergency_ms` of 0 disables the monitor (always
    /// `Healthy` or `NotReady`). Used when YAML opts out.
    pub fn new(ws_stale_emergency_ms: u64) -> Self {
        Self {
            ws_stale_emergency_ms,
            ext_last_book_ms: None,
            lt_last_book_ms: None,
            ext_stale_latched: false,
            lt_stale_latched: false,
        }
    }

    pub fn ws_stale_emergency_ms(&self) -> u64 {
        self.ws_stale_emergency_ms
    }

    /// Called by the runner each tick **after** a successful
    /// `read_mid` that returned `book_ok = true`. Captures the
    /// observation timestamp the live loop already computes.
    ///
    /// `book_ok = false` ticks are deliberately NOT recorded — the
    /// monitor is meant to fire when the venue's book has been
    /// unavailable / one-sided for long enough that we can't trust
    /// it for either entry sizing or exit timing.
    pub fn record_book_update(&mut self, venue: VenueLabel, now_ms: u64) {
        match venue {
            VenueLabel::Extended => {
                self.ext_last_book_ms = Some(now_ms);
                self.ext_stale_latched = false;
            }
            VenueLabel::Lighter => {
                self.lt_last_book_ms = Some(now_ms);
                self.lt_stale_latched = false;
            }
        }
    }

    /// Per-tick evaluation. Returns `NotReady` until both venues have
    /// produced at least one healthy book; once both are warm,
    /// returns `Stale(venue)` if either's age exceeds the threshold.
    pub fn evaluate(&mut self, now_ms: u64) -> WsHealthOutcome {
        if self.ws_stale_emergency_ms == 0 {
            // Operator opt-out — still gate on warm-up so a misuse
            // (calling evaluate before any update) doesn't return
            // misleading Healthy.
            if self.ext_last_book_ms.is_none() || self.lt_last_book_ms.is_none() {
                return WsHealthOutcome::NotReady;
            }
            return WsHealthOutcome::Healthy;
        }

        let (Some(ext_ts), Some(lt_ts)) = (self.ext_last_book_ms, self.lt_last_book_ms) else {
            return WsHealthOutcome::NotReady;
        };

        let ext_age = age_ms(now_ms, ext_ts);
        let lt_age = age_ms(now_ms, lt_ts);

        let ext_stale = ext_age > self.ws_stale_emergency_ms;
        let lt_stale = lt_age > self.ws_stale_emergency_ms;

        if ext_stale {
            self.ext_stale_latched = true;
            return WsHealthOutcome::Stale(VenueLabel::Extended);
        }
        if lt_stale {
            self.lt_stale_latched = true;
            return WsHealthOutcome::Stale(VenueLabel::Lighter);
        }
        WsHealthOutcome::Healthy
    }

    /// Status snapshot for the dashboard (`status.json` per #244 A).
    pub fn ws_age(&self, now_ms: u64) -> WsAge {
        WsAge {
            ext_age_ms: self.ext_last_book_ms.map(|t| age_ms(now_ms, t)),
            lt_age_ms: self.lt_last_book_ms.map(|t| age_ms(now_ms, t)),
        }
    }

    pub fn ext_stale_latched(&self) -> bool {
        self.ext_stale_latched
    }

    pub fn lt_stale_latched(&self) -> bool {
        self.lt_stale_latched
    }

    /// Called by the runner once the state machine completes
    /// emergency-flatten and returns to Flat. Clears both latches so
    /// the next stale event surfaces fresh in status emission.
    pub fn reset_after_recovery(&mut self) {
        self.ext_stale_latched = false;
        self.lt_stale_latched = false;
    }
}

fn age_ms(now_ms: u64, last_ms: u64) -> u64 {
    now_ms.saturating_sub(last_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_monitor_is_not_ready() {
        let mut m = WsHealthMonitor::new(5_000);
        assert_eq!(m.evaluate(1_000), WsHealthOutcome::NotReady);
    }

    #[test]
    fn one_venue_warm_still_not_ready() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        assert_eq!(m.evaluate(1_500), WsHealthOutcome::NotReady);
    }

    #[test]
    fn both_warm_within_threshold_is_healthy() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_100);
        // 4s after both updates — under 5s threshold.
        assert_eq!(m.evaluate(5_000), WsHealthOutcome::Healthy);
    }

    #[test]
    fn extended_stale_returns_stale_extended() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 6_500);
        // ext_age = 7000 - 1000 = 6000 > 5000 → stale.
        // lt_age  = 7000 - 6500 = 500   < 5000 → fresh.
        assert_eq!(
            m.evaluate(7_000),
            WsHealthOutcome::Stale(VenueLabel::Extended)
        );
        assert!(m.ext_stale_latched());
        assert!(!m.lt_stale_latched());
    }

    #[test]
    fn lighter_stale_returns_stale_lighter() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 6_500);
        m.record_book_update(VenueLabel::Lighter, 1_000);
        assert_eq!(
            m.evaluate(7_000),
            WsHealthOutcome::Stale(VenueLabel::Lighter)
        );
        assert!(!m.ext_stale_latched());
        assert!(m.lt_stale_latched());
    }

    #[test]
    fn both_stale_extended_wins() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_500);
        // Both > 5s old at now=10_000.
        assert_eq!(
            m.evaluate(10_000),
            WsHealthOutcome::Stale(VenueLabel::Extended)
        );
        // Latch only flipped for the first-to-fire venue this call;
        // the runner downstream flattens both anyway.
        assert!(m.ext_stale_latched());
        assert!(!m.lt_stale_latched());
    }

    #[test]
    fn boundary_at_threshold_is_healthy() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_000);
        // age = 5000, threshold = 5000 → not strictly greater, healthy.
        assert_eq!(m.evaluate(6_000), WsHealthOutcome::Healthy);
        // age = 5001 → stale.
        assert_eq!(
            m.evaluate(6_001),
            WsHealthOutcome::Stale(VenueLabel::Extended)
        );
    }

    #[test]
    fn fresh_update_clears_latch() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_000);
        // Trip the latch.
        let _ = m.evaluate(7_000);
        assert!(m.ext_stale_latched());
        // New healthy update for Extended clears its latch.
        m.record_book_update(VenueLabel::Extended, 7_500);
        assert!(!m.ext_stale_latched());
    }

    #[test]
    fn recovery_reset_clears_latches() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_000);
        let _ = m.evaluate(20_000);
        assert!(m.ext_stale_latched());
        m.reset_after_recovery();
        assert!(!m.ext_stale_latched());
        assert!(!m.lt_stale_latched());
    }

    #[test]
    fn disabled_monitor_returns_healthy_after_warmup() {
        let mut m = WsHealthMonitor::new(0);
        // Even after a long gap, monitor never fires when threshold = 0.
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 1_000);
        assert_eq!(m.evaluate(1_000_000), WsHealthOutcome::Healthy);
    }

    #[test]
    fn disabled_monitor_still_gates_on_warmup() {
        let mut m = WsHealthMonitor::new(0);
        // No updates yet — caller still sees NotReady so it doesn't
        // act on missing data.
        assert_eq!(m.evaluate(1_000), WsHealthOutcome::NotReady);
    }

    #[test]
    fn ws_age_reports_per_venue_age() {
        let mut m = WsHealthMonitor::new(5_000);
        m.record_book_update(VenueLabel::Extended, 1_000);
        m.record_book_update(VenueLabel::Lighter, 2_500);
        let age = m.ws_age(3_000);
        assert_eq!(age.ext_age_ms, Some(2_000));
        assert_eq!(age.lt_age_ms, Some(500));
    }

    #[test]
    fn ws_age_none_before_warmup() {
        let m = WsHealthMonitor::new(5_000);
        let age = m.ws_age(3_000);
        assert_eq!(age.ext_age_ms, None);
        assert_eq!(age.lt_age_ms, None);
    }

    #[test]
    fn age_ms_saturates_on_clock_skew() {
        // If the runner accidentally feeds a `now_ms` smaller than
        // the recorded ts (clock rewind / unit confusion) we must not
        // panic / underflow. saturating_sub gives 0 — caller will
        // see a "fresh" age, which is the safe direction.
        assert_eq!(age_ms(500, 1_000), 0);
    }
}
