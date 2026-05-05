//! Inventory-skew watchdog (bot-strategy#244 Group C).
//!
//! Companion to `ws_health` (liveness) and `reference_guard` (price
//! sanity) — this module owns *position-level* sanity. Per
//! DESIGN.md §4.4 the two legs target equal USD notional; skew
//! beyond `max_inventory_skew_usd` indicates either a partial-fill
//! that didn't recover or a sizing bug, and must escalate to
//! `Phase::EmergencyFlattening` so the bot doesn't sit on an
//! unhedged residual.
//!
//! Design rationale:
//!
//! - The state machine already exposes
//!   `PositionMachine::inventory_skew_usd(ext_mid, lt_mid)`. This
//!   monitor wraps the threshold + latch logic so the runner stays
//!   readable and the policy lives next to the other risk modules.
//! - 1 Hz cadence in the brief is a hint — the live tick already
//!   runs at `spread_bucket_ms` (5 s default), which is slow enough
//!   that per-tick evaluation is essentially free and matches how
//!   `reference_guard` and `ws_health` are wired. No internal
//!   throttle.
//! - Latch flips on first breach so the runner can avoid emitting
//!   `Emergency{SkewBreach}` every subsequent tick during the same
//!   incident; downstream `Phase::EmergencyFlattening` is sticky
//!   anyway. Cleared by `reset_after_recovery` once the state
//!   machine returns to Flat.

/// Outcome of one tick's skew check.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SkewOutcome {
    /// Threshold is 0 → monitor disabled, or there's no open
    /// position. Caller takes no action.
    Disabled,
    /// Skew within the configured cap.
    Ok { skew_usd: f64 },
    /// Skew exceeds the cap. Caller emits `Event::Emergency
    /// { reason: SkewBreach }` to the position machine.
    Breach { skew_usd: f64, threshold_usd: f64 },
}

#[derive(Debug)]
pub struct SkewMonitor {
    max_inventory_skew_usd: f64,
    breached_latched: bool,
}

impl SkewMonitor {
    /// `max_inventory_skew_usd` of 0 (or a non-finite value) disables
    /// the monitor — `evaluate` always returns `Disabled`.
    pub fn new(max_inventory_skew_usd: f64) -> Self {
        Self {
            max_inventory_skew_usd: sanitize_threshold(max_inventory_skew_usd),
            breached_latched: false,
        }
    }

    pub fn threshold_usd(&self) -> f64 {
        self.max_inventory_skew_usd
    }

    pub fn breached_latched(&self) -> bool {
        self.breached_latched
    }

    /// Per-tick check. `skew_usd` is `PositionMachine::inventory_skew_usd`
    /// converted to `f64`; the f64 path is fine here because the
    /// threshold itself is f64-sourced from YAML and the comparison
    /// only needs ~6 sig figs.
    ///
    /// Returns `Disabled` on no-position (skew == 0.0) so the
    /// runner can short-circuit without checking position state
    /// itself.
    pub fn evaluate(&mut self, skew_usd: f64) -> SkewOutcome {
        if !skew_usd.is_finite() || self.max_inventory_skew_usd <= 0.0 {
            return SkewOutcome::Disabled;
        }
        let skew_abs = skew_usd.abs();
        if skew_abs == 0.0 {
            return SkewOutcome::Disabled;
        }
        if skew_abs > self.max_inventory_skew_usd {
            self.breached_latched = true;
            SkewOutcome::Breach {
                skew_usd: skew_abs,
                threshold_usd: self.max_inventory_skew_usd,
            }
        } else {
            SkewOutcome::Ok { skew_usd: skew_abs }
        }
    }

    /// Called after `Phase::EmergencyFlattening` clears so the
    /// next-cycle latch fires fresh.
    pub fn reset_after_recovery(&mut self) {
        self.breached_latched = false;
    }
}

fn sanitize_threshold(t: f64) -> f64 {
    if t.is_finite() && t > 0.0 {
        t
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_when_threshold_zero() {
        let mut m = SkewMonitor::new(0.0);
        assert_eq!(m.evaluate(100.0), SkewOutcome::Disabled);
        assert!(!m.breached_latched());
    }

    #[test]
    fn disabled_when_threshold_non_finite() {
        let mut m = SkewMonitor::new(f64::NAN);
        assert_eq!(m.evaluate(100.0), SkewOutcome::Disabled);
        assert_eq!(m.threshold_usd(), 0.0);

        let mut m = SkewMonitor::new(f64::INFINITY);
        assert_eq!(m.evaluate(100.0), SkewOutcome::Disabled);

        let mut m = SkewMonitor::new(-1.0);
        assert_eq!(m.evaluate(100.0), SkewOutcome::Disabled);
    }

    #[test]
    fn zero_skew_returns_disabled_short_circuit() {
        // No-position path — runner doesn't have to ask about
        // PositionMachine::summary first.
        let mut m = SkewMonitor::new(50.0);
        assert_eq!(m.evaluate(0.0), SkewOutcome::Disabled);
        assert!(!m.breached_latched());
    }

    #[test]
    fn within_threshold_is_ok() {
        let mut m = SkewMonitor::new(50.0);
        let res = m.evaluate(49.99);
        assert!(matches!(res, SkewOutcome::Ok { .. }));
        if let SkewOutcome::Ok { skew_usd } = res {
            assert!((skew_usd - 49.99).abs() < 1e-9);
        }
        assert!(!m.breached_latched());
    }

    #[test]
    fn at_threshold_is_ok_strict_inequality() {
        // Boundary: skew exactly == threshold is not a breach.
        let mut m = SkewMonitor::new(50.0);
        assert_eq!(m.evaluate(50.0), SkewOutcome::Ok { skew_usd: 50.0 });
        assert!(!m.breached_latched());
    }

    #[test]
    fn over_threshold_breaches_and_latches() {
        let mut m = SkewMonitor::new(50.0);
        let res = m.evaluate(50.01);
        match res {
            SkewOutcome::Breach {
                skew_usd,
                threshold_usd,
            } => {
                assert!((skew_usd - 50.01).abs() < 1e-9);
                assert_eq!(threshold_usd, 50.0);
            }
            other => panic!("expected Breach, got {:?}", other),
        }
        assert!(m.breached_latched());
    }

    #[test]
    fn negative_skew_uses_abs() {
        // PositionMachine::inventory_skew_usd already returns abs
        // value, but the monitor must be defensive in case a future
        // refactor exposes a signed delta.
        let mut m = SkewMonitor::new(50.0);
        let res = m.evaluate(-75.0);
        match res {
            SkewOutcome::Breach { skew_usd, .. } => assert!((skew_usd - 75.0).abs() < 1e-9),
            other => panic!("expected Breach, got {:?}", other),
        }
    }

    #[test]
    fn nan_skew_is_disabled_not_panic() {
        // Pathological input shouldn't panic — clock skew /
        // serialisation glitch shouldn't take down the bot.
        let mut m = SkewMonitor::new(50.0);
        assert_eq!(m.evaluate(f64::NAN), SkewOutcome::Disabled);
        assert!(!m.breached_latched());
    }

    #[test]
    fn recovery_reset_clears_latch() {
        let mut m = SkewMonitor::new(50.0);
        let _ = m.evaluate(100.0);
        assert!(m.breached_latched());
        m.reset_after_recovery();
        assert!(!m.breached_latched());
    }

    #[test]
    fn latch_persists_across_subsequent_ok_ticks() {
        // Once latched, the latch only clears via
        // reset_after_recovery — even if a transient tick reads back
        // within threshold, the operator's incident isn't over until
        // EmergencyFlattening completes.
        let mut m = SkewMonitor::new(50.0);
        let _ = m.evaluate(100.0);
        assert!(m.breached_latched());
        let _ = m.evaluate(10.0);
        assert!(m.breached_latched());
    }
}
