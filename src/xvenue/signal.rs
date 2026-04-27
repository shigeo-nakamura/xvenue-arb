//! Cross-venue spread signal evaluation. DESIGN.md §4 / §5.
//!
//! Pure logic over `SpreadEngine::current_dev_bps()` and the held-position
//! summary. Entry fires when `|dev|` stays past `abs_threshold_bps` for
//! `persistence_sec` continuously (v2 methodology, bot-strategy#166
//! 2026-04-25 refinement). Exit fires on rolling-mean cross, reverse
//! blowout (`force_close_dev_bps`), or `max_hold_sec` timeout. No clock,
//! no I/O — `decide` is fed `(now_ms, dev_bps, is_warm, position)` and
//! returns a `Decision` that the execution layer translates into orders.

#[derive(Debug, Clone)]
pub struct SignalConfig {
    pub abs_threshold_bps: f64,
    pub persistence_sec: u64,
    pub exit_at_mean_cross: bool,
    pub max_hold_sec: u64,
    pub force_close_dev_bps: f64,
    pub min_warmup_samples: usize,
    /// When true (Rust default), `Decision::Enter` only fires on a tick
    /// where `|dev| >= abs_threshold_bps` is still satisfied AND the
    /// breach has lasted `persistence_sec`. When false (Phase 0 v2
    /// Python-compat), the entry fires once persistence has elapsed
    /// regardless of the current dev value — matching the offline sim's
    /// "open at the bar AFTER the confirmation window without checking
    /// dev[entry_idx]" behavior. Used for parity diagnostics with the
    /// Phase 0 simulator (bot-strategy#166).
    pub entry_check_threshold_at_fire: bool,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            abs_threshold_bps: 5.0,
            persistence_sec: 15,
            exit_at_mean_cross: true,
            max_hold_sec: 600,
            force_close_dev_bps: 30.0,
            min_warmup_samples: 60,
            entry_check_threshold_at_fire: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadDirection {
    /// `dev > +threshold`: spread is wider than μ_roll. Short the spread
    /// — sell Extended, buy Lighter.
    Short,
    /// `dev < -threshold`: spread is tighter than μ_roll. Long the spread
    /// — buy Extended, sell Lighter.
    Long,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    MeanCross,
    MaxHold,
    ForceClose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Hold,
    Enter(SpreadDirection),
    Exit(ExitReason),
}

/// Snapshot of the held cross-venue spread leg. Signal logic only needs
/// direction + entry timestamp; full position bookkeeping lives in
/// `state.rs`.
#[derive(Debug, Clone, Copy)]
pub struct PositionSummary {
    pub direction: SpreadDirection,
    pub entry_ts_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct Breach {
    started_ts_ms: u64,
    direction: SpreadDirection,
}

pub struct SignalEngine {
    cfg: SignalConfig,
    breach: Option<Breach>,
}

impl SignalEngine {
    pub fn new(cfg: SignalConfig) -> Self {
        Self { cfg, breach: None }
    }

    pub fn config(&self) -> &SignalConfig {
        &self.cfg
    }

    pub fn decide(
        &mut self,
        now_ts_ms: u64,
        dev_bps: Option<f64>,
        is_warm: bool,
        position: Option<PositionSummary>,
    ) -> Decision {
        let dev = match dev_bps {
            Some(d) => d,
            None => return Decision::Hold,
        };

        if let Some(pos) = position {
            return self.decide_exit(now_ts_ms, dev, pos);
        }

        if !is_warm {
            // Don't carry a pre-warmup breach into trading — μ_roll isn't
            // trustworthy yet, so any threshold cross before warmup is
            // structurally meaningless.
            self.breach = None;
            return Decision::Hold;
        }

        self.decide_entry(now_ts_ms, dev)
    }

    fn decide_entry(&mut self, now_ts_ms: u64, dev: f64) -> Decision {
        // Python-compat path (entry_check_threshold_at_fire = false):
        // fire as soon as an active breach has matured, even if the
        // current bar's dev has already reverted below threshold or
        // crossed sign. Matches the Phase 0 v2 simulator's
        // `entry_idx = i + persistence_buckets` rule which does not
        // re-check dev at the entry bar.
        if !self.cfg.entry_check_threshold_at_fire {
            if let Some(b) = self.breach {
                let elapsed_ms = now_ts_ms.saturating_sub(b.started_ts_ms);
                if elapsed_ms >= self.cfg.persistence_sec.saturating_mul(1_000) {
                    self.breach = None;
                    return Decision::Enter(b.direction);
                }
            }
        }

        let dir = if dev >= self.cfg.abs_threshold_bps {
            SpreadDirection::Short
        } else if dev <= -self.cfg.abs_threshold_bps {
            SpreadDirection::Long
        } else {
            self.breach = None;
            return Decision::Hold;
        };

        let breach = self
            .breach
            .filter(|b| b.direction == dir)
            .unwrap_or(Breach {
                started_ts_ms: now_ts_ms,
                direction: dir,
            });
        self.breach = Some(breach);

        let elapsed_ms = now_ts_ms.saturating_sub(breach.started_ts_ms);
        if elapsed_ms >= self.cfg.persistence_sec.saturating_mul(1_000) {
            // Clear so a flat-position next tick starts the persistence
            // clock fresh; without this we'd re-fire every tick the dev
            // stays past threshold.
            self.breach = None;
            Decision::Enter(dir)
        } else {
            Decision::Hold
        }
    }

    fn decide_exit(&mut self, now_ts_ms: u64, dev: f64, pos: PositionSummary) -> Decision {
        // Held positions don't grow new breaches; reset defensively so
        // re-entry persistence starts cleanly after close.
        self.breach = None;

        let force_close = match pos.direction {
            SpreadDirection::Short => dev >= self.cfg.force_close_dev_bps,
            SpreadDirection::Long => dev <= -self.cfg.force_close_dev_bps,
        };
        if force_close {
            return Decision::Exit(ExitReason::ForceClose);
        }

        if self.cfg.exit_at_mean_cross {
            let crossed = match pos.direction {
                SpreadDirection::Short => dev <= 0.0,
                SpreadDirection::Long => dev >= 0.0,
            };
            if crossed {
                return Decision::Exit(ExitReason::MeanCross);
            }
        }

        let hold_ms = now_ts_ms.saturating_sub(pos.entry_ts_ms);
        if hold_ms >= self.cfg.max_hold_sec.saturating_mul(1_000) {
            return Decision::Exit(ExitReason::MaxHold);
        }

        Decision::Hold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SignalConfig {
        SignalConfig {
            abs_threshold_bps: 5.0,
            persistence_sec: 15,
            exit_at_mean_cross: true,
            max_hold_sec: 600,
            force_close_dev_bps: 30.0,
            min_warmup_samples: 10,
            entry_check_threshold_at_fire: true,
        }
    }

    #[test]
    fn hold_when_dev_missing() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, None, true, None), Decision::Hold);
    }

    #[test]
    fn hold_when_not_warm() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(10.0), false, None), Decision::Hold);
        // Breach must not be retained across warmup boundary.
        assert!(s.breach.is_none());
    }

    #[test]
    fn entry_requires_persistence() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(7.0), true, None), Decision::Hold);
        // 14s elapsed: still under 15s persistence
        assert_eq!(s.decide(15_000, Some(7.0), true, None), Decision::Hold);
    }

    #[test]
    fn entry_fires_after_persistence_short() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(16_000, Some(7.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn entry_fires_after_persistence_long() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(-7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(16_000, Some(-7.0), true, None),
            Decision::Enter(SpreadDirection::Long)
        );
    }

    #[test]
    fn breach_resets_when_dev_drops_below_threshold() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(7.0), true, None), Decision::Hold);
        // 10s in, dev drops below threshold → breach cleared
        assert_eq!(s.decide(11_000, Some(3.0), true, None), Decision::Hold);
        // Dev climbs back: persistence restarts from 12s, not 1s
        assert_eq!(s.decide(12_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(20_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(27_000, Some(7.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn breach_resets_on_sign_flip() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(s.decide(1_000, Some(7.0), true, None), Decision::Hold);
        // Sign flip mid-persistence: must restart clock for the new direction.
        assert_eq!(s.decide(10_000, Some(-7.0), true, None), Decision::Hold);
        // 14s after the flip: not yet 15s
        assert_eq!(s.decide(24_000, Some(-7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(25_000, Some(-7.0), true, None),
            Decision::Enter(SpreadDirection::Long)
        );
    }

    #[test]
    fn entry_does_not_fire_at_exact_threshold_minus_one() {
        let mut s = SignalEngine::new(cfg());
        // Just below threshold: no breach
        assert_eq!(s.decide(1_000, Some(4.99), true, None), Decision::Hold);
        assert_eq!(s.decide(20_000, Some(4.99), true, None), Decision::Hold);
        assert!(s.breach.is_none());
    }

    #[test]
    fn entry_at_exact_threshold() {
        let mut s = SignalEngine::new(cfg());
        // At threshold (>=): breach starts
        assert_eq!(s.decide(1_000, Some(5.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(16_000, Some(5.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn persistence_zero_fires_immediately() {
        let mut c = cfg();
        c.persistence_sec = 0;
        let mut s = SignalEngine::new(c);
        assert_eq!(
            s.decide(1_000, Some(7.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    fn held_short(entry_ts_ms: u64) -> Option<PositionSummary> {
        Some(PositionSummary {
            direction: SpreadDirection::Short,
            entry_ts_ms,
        })
    }

    fn held_long(entry_ts_ms: u64) -> Option<PositionSummary> {
        Some(PositionSummary {
            direction: SpreadDirection::Long,
            entry_ts_ms,
        })
    }

    #[test]
    fn mean_cross_exits_short_at_zero() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(
            s.decide(60_000, Some(0.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::MeanCross)
        );
    }

    #[test]
    fn mean_cross_exits_short_when_negative() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(
            s.decide(60_000, Some(-1.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::MeanCross)
        );
    }

    #[test]
    fn mean_cross_exits_long_at_zero() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(
            s.decide(60_000, Some(0.0), true, held_long(1_000)),
            Decision::Exit(ExitReason::MeanCross)
        );
    }

    #[test]
    fn no_mean_cross_exit_while_still_in_profitable_zone() {
        let mut s = SignalEngine::new(cfg());
        // Short held while dev still above 0 (not yet reverted) — hold
        assert_eq!(
            s.decide(60_000, Some(2.0), true, held_short(1_000)),
            Decision::Hold
        );
        // Long held while dev still below 0 — hold
        assert_eq!(
            s.decide(60_000, Some(-2.0), true, held_long(1_000)),
            Decision::Hold
        );
    }

    #[test]
    fn force_close_short_blowout() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(
            s.decide(60_000, Some(31.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::ForceClose)
        );
    }

    #[test]
    fn force_close_long_blowout() {
        let mut s = SignalEngine::new(cfg());
        assert_eq!(
            s.decide(60_000, Some(-31.0), true, held_long(1_000)),
            Decision::Exit(ExitReason::ForceClose)
        );
    }

    #[test]
    fn max_hold_exits_position() {
        let mut s = SignalEngine::new(cfg());
        // 600s + 1s elapsed, dev still trending the right way (no mean cross)
        assert_eq!(
            s.decide(601_000, Some(7.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::MaxHold)
        );
    }

    #[test]
    fn force_close_priority_over_max_hold() {
        let mut s = SignalEngine::new(cfg());
        // Held > max_hold AND dev past force_close: ForceClose wins (worse case)
        assert_eq!(
            s.decide(700_000, Some(35.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::ForceClose)
        );
    }

    #[test]
    fn mean_cross_priority_over_max_hold() {
        let mut s = SignalEngine::new(cfg());
        // Held > max_hold AND dev crossed 0: prefer the clean MeanCross label
        assert_eq!(
            s.decide(700_000, Some(-1.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::MeanCross)
        );
    }

    #[test]
    fn exit_disabled_when_mean_cross_off() {
        let mut c = cfg();
        c.exit_at_mean_cross = false;
        let mut s = SignalEngine::new(c);
        // Crossed mean but feature disabled → only max_hold or force_close exit
        assert_eq!(
            s.decide(60_000, Some(-1.0), true, held_short(1_000)),
            Decision::Hold
        );
    }

    #[test]
    fn pre_warm_breach_does_not_carry_over() {
        let mut s = SignalEngine::new(cfg());
        // Many ticks above threshold while not warm
        assert_eq!(s.decide(1_000, Some(7.0), false, None), Decision::Hold);
        assert_eq!(s.decide(20_000, Some(7.0), false, None), Decision::Hold);
        assert!(s.breach.is_none());
        // First warm tick: persistence must start now, not back at t=1000
        assert_eq!(s.decide(21_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(35_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(36_000, Some(7.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn re_entry_persistence_starts_fresh_after_exit() {
        let mut s = SignalEngine::new(cfg());
        // Hold exit fires
        assert_eq!(
            s.decide(60_000, Some(0.0), true, held_short(1_000)),
            Decision::Exit(ExitReason::MeanCross)
        );
        // Now flat. Threshold crossed again: must take full persistence_sec
        assert_eq!(s.decide(61_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(75_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(76_000, Some(7.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn python_compat_fires_after_persistence_even_if_dev_reverted() {
        // bot-strategy#166: parity diagnostic. Phase 0 v2 sim opens at
        // `i + persistence_buckets` without re-checking dev there. With
        // entry_check_threshold_at_fire=false we mirror that behavior:
        // breach holds for persistence_sec while dev confirms, and the
        // tick after persistence elapses fires Enter regardless of the
        // current dev.
        let mut c = cfg();
        c.entry_check_threshold_at_fire = false;
        let mut s = SignalEngine::new(c);
        // Confirmation window — dev stays past threshold for the full
        // persistence_sec (bars at 5s cadence).
        assert_eq!(s.decide(0, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(5_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(10_000, Some(7.0), true, None), Decision::Hold);
        // Bar at +15s: persistence elapsed. Even though dev has reverted
        // to +2 (below threshold), Python-compat fires Enter.
        assert_eq!(
            s.decide(15_000, Some(2.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn python_compat_breach_still_resets_on_mid_window_dip() {
        // The "fire regardless of current dev" rule only applies AFTER
        // persistence has fully elapsed under continuous confirmation.
        // A mid-window dip below threshold still resets the breach,
        // matching Phase 0 v2's "all bars in [i, i+persistence_buckets)
        // must confirm" rule.
        let mut c = cfg();
        c.entry_check_threshold_at_fire = false;
        let mut s = SignalEngine::new(c);
        assert_eq!(s.decide(0, Some(7.0), true, None), Decision::Hold);
        // Mid-window dip — breach must reset.
        assert_eq!(s.decide(5_000, Some(2.0), true, None), Decision::Hold);
        // Re-cross at 10s: new breach starts here. 15s elapsed from
        // the original i=0 must NOT be enough to fire.
        assert_eq!(s.decide(10_000, Some(7.0), true, None), Decision::Hold);
        // 15s after the new breach start (= 25s wall-clock) is when
        // the Python-compat rule fires.
        assert_eq!(s.decide(20_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(
            s.decide(25_000, Some(2.0), true, None),
            Decision::Enter(SpreadDirection::Short)
        );
    }

    #[test]
    fn python_compat_does_not_change_strict_default_behavior() {
        // Sanity: with entry_check_threshold_at_fire = true (default),
        // a reverted dev at the persistence boundary still skips entry.
        let mut s = SignalEngine::new(cfg()); // default = strict
        assert_eq!(s.decide(0, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(5_000, Some(7.0), true, None), Decision::Hold);
        assert_eq!(s.decide(10_000, Some(7.0), true, None), Decision::Hold);
        // Bar at +15s with reverted dev: strict mode resets, Holds.
        assert_eq!(s.decide(15_000, Some(2.0), true, None), Decision::Hold);
    }
}
