//! Cross-venue position state machine. DESIGN.md §3.2 / §4.
//!
//! Pure-logic, event-driven. Execution layer ([`trade::execution`]) emits
//! events; this module owns the phase transitions and per-leg fill bookkeeping.
//! Signal layer reads [`PositionMachine::summary`] to gate exit decisions.
//!
//! Only `max_concurrent = 1` is supported. Per DESIGN.md §4.1 entry is
//! serialized (Extended first, Lighter after fill) and §4.2 exit is parallel.
//! Single-leg-filled / WS-stale / skew breaches route through
//! [`Phase::EmergencyFlattening`] (§4.3).
//!
//! ## Phase transitions
//!
//! ```text
//!                              EntrySignal
//!                                  │
//!                                  ▼
//!     ┌─ Flat ────────────► EnteringExtended ───ExtendedFilled──► EnteringLighter
//!     │   ▲                       │                                     │
//!     │   │                       │                                     │
//!     │   │ EmergencyComplete     │ ExtendedFailed                      │ LighterFilled
//!     │   │                       │ (no fills)                          │
//!     │   │                       │                                     ▼
//!     │   │                       └──────────────────────────►        Held
//!     │   │                                                             │
//!     │   │                                                             │ ExitSignal
//!     │   │                                                             │
//!     │   │                                                             ▼
//!     │   │           ExtendedExitFilled / LighterExitFilled         Exiting
//!     │   │                  (until both legs zero)                     │
//!     │   └────────────────────────────────────────────────────────────┘
//!     │
//!     │      ┌──────────────────── Emergency{ws_stale,skew,leg_mismatch,...}
//!     │      │                     LighterFailed
//!     │      ▼                     ExtendedFailed (with prior fills)
//!     └─ EmergencyFlattening ◄───────────────────────────────────────────
//!         │  (consumes ExtendedExitFilled / LighterExitFilled but does
//!         │   NOT auto-flat; requires explicit EmergencyComplete)
//!         │
//!         └─[EmergencyComplete]─► Flat
//!
//!     Operator override: Event::Reset is accepted in any phase and
//!     unconditionally clears `position` and routes to `Flat`. Used after
//!     STUCK file resolution (§4.4).
//! ```
//!
//! ## Invariants ([`PositionMachine::check_invariants`])
//!
//! - `phase == Flat`            ⇒ `position` is `None`
//! - `phase != Flat`            ⇒ `position` is `Some`
//! - `phase == EnteringExtended`⇒ both leg qtys are zero (no fills yet)
//! - `phase == EnteringLighter` ⇒ ext qty > 0, lt qty == 0
//! - `phase == Held`            ⇒ `fully_filled_ts_ms` is `Some`
//! - leg qtys are always `>= 0` (clamped via `max(Decimal::ZERO)` on exit fills)
//!
//! `Exiting` and `EmergencyFlattening` deliberately allow zero-or-positive
//! qtys on either leg so partial / asymmetric exits can drain to zero
//! across multiple events.

use std::fmt;

use rust_decimal::Decimal;

use super::signal::{ExitReason, PositionSummary, SpreadDirection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Flat,
    EnteringExtended,
    EnteringLighter,
    Held,
    Exiting,
    EmergencyFlattening,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmergencyReason {
    WsStale,
    LegMismatchTimeout,
    SkewBreach,
    KillSwitch,
    ReferenceDeviation,
    ExtendedEntryFailed,
    LighterEntryFailed,
    /// Session-DD halt while a position was open (#268 S5-3).
    /// The runner forces a flatten so the open exposure cleans up
    /// rather than waiting for the strategy's natural exit signal
    /// (mean cross / max hold). Distinct from `LegMismatchTimeout`
    /// — that's an exit-side failure; this is a risk-side halt.
    SessionDdHalted,
}

#[derive(Debug, Clone)]
pub enum Event {
    EntrySignal {
        direction: SpreadDirection,
        notional_usd: Decimal,
    },
    /// Entry leg complete on Extended. Per §4.1 the execution layer
    /// aggregates partial post-only / chase / taker-fallback fills and
    /// emits one terminal event with the final qty.
    ExtendedFilled {
        qty: Decimal,
    },
    ExtendedFailed,
    /// Entry leg complete on Lighter (market or aggressive limit, §4.1).
    LighterFilled {
        qty: Decimal,
    },
    LighterFailed,
    ExitSignal {
        reason: ExitReason,
    },
    ExtendedExitFilled {
        qty: Decimal,
    },
    LighterExitFilled {
        qty: Decimal,
    },
    Emergency {
        reason: EmergencyReason,
    },
    EmergencyComplete,
    /// Operator override: clear position and force phase back to Flat.
    /// Used after STUCK file resolution per DESIGN.md §4.4.
    Reset,
}

impl Event {
    fn kind(&self) -> &'static str {
        match self {
            Event::EntrySignal { .. } => "EntrySignal",
            Event::ExtendedFilled { .. } => "ExtendedFilled",
            Event::ExtendedFailed => "ExtendedFailed",
            Event::LighterFilled { .. } => "LighterFilled",
            Event::LighterFailed => "LighterFailed",
            Event::ExitSignal { .. } => "ExitSignal",
            Event::ExtendedExitFilled { .. } => "ExtendedExitFilled",
            Event::LighterExitFilled { .. } => "LighterExitFilled",
            Event::Emergency { .. } => "Emergency",
            Event::EmergencyComplete => "EmergencyComplete",
            Event::Reset => "Reset",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Position {
    pub direction: SpreadDirection,
    pub target_notional_usd: Decimal,
    pub entry_signal_ts_ms: u64,
    /// Set when both legs filled and the machine entered `Held`. Signal
    /// uses this as `entry_ts_ms` so `max_hold_sec` measures delta-neutral
    /// hold, not the entry-flow latency.
    pub fully_filled_ts_ms: Option<u64>,
    /// Net qty open on Extended leg (entry fills − exit fills). Always >= 0.
    pub extended_open_qty: Decimal,
    pub lighter_open_qty: Decimal,
    pub last_exit_reason: Option<ExitReason>,
    pub last_emergency_reason: Option<EmergencyReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionError {
    pub phase: Phase,
    pub event_kind: &'static str,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid event {} in phase {:?}",
            self.event_kind, self.phase
        )
    }
}

impl std::error::Error for TransitionError {}

pub struct PositionMachine {
    phase: Phase,
    phase_entered_ts_ms: u64,
    position: Option<Position>,
}

impl PositionMachine {
    pub fn new() -> Self {
        Self {
            phase: Phase::Flat,
            phase_entered_ts_ms: 0,
            position: None,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn position(&self) -> Option<&Position> {
        self.position.as_ref()
    }

    pub fn time_in_phase_ms(&self, now_ts_ms: u64) -> u64 {
        now_ts_ms.saturating_sub(self.phase_entered_ts_ms)
    }

    /// Snapshot for the signal layer. `Some` only in `Held` — signal must
    /// not evaluate exits during entry / exit / flatten flows.
    pub fn summary(&self) -> Option<PositionSummary> {
        if self.phase != Phase::Held {
            return None;
        }
        let p = self.position.as_ref()?;
        Some(PositionSummary {
            direction: p.direction,
            entry_ts_ms: p.fully_filled_ts_ms?,
        })
    }

    /// Notional skew between the two legs in USD. Per DESIGN.md §4.4 the
    /// legs target equal notional; skew above `max_inventory_skew_usd`
    /// triggers emergency flatten.
    pub fn inventory_skew_usd(&self, ext_mid: Decimal, lt_mid: Decimal) -> Decimal {
        let p = match self.position.as_ref() {
            Some(p) => p,
            None => return Decimal::ZERO,
        };
        let ext_notional = p.extended_open_qty * ext_mid;
        let lt_notional = p.lighter_open_qty * lt_mid;
        (ext_notional - lt_notional).abs()
    }

    pub fn apply(&mut self, now_ts_ms: u64, event: Event) -> Result<(), TransitionError> {
        // Reset is a global escape hatch (operator-driven, §4.4).
        if matches!(event, Event::Reset) {
            self.position = None;
            self.transition_to(Phase::Flat, now_ts_ms);
            return Ok(());
        }

        let kind = event.kind();
        match (self.phase, &event) {
            (
                Phase::Flat,
                Event::EntrySignal {
                    direction,
                    notional_usd,
                },
            ) => {
                self.position = Some(Position {
                    direction: *direction,
                    target_notional_usd: *notional_usd,
                    entry_signal_ts_ms: now_ts_ms,
                    fully_filled_ts_ms: None,
                    extended_open_qty: Decimal::ZERO,
                    lighter_open_qty: Decimal::ZERO,
                    last_exit_reason: None,
                    last_emergency_reason: None,
                });
                self.transition_to(Phase::EnteringExtended, now_ts_ms);
                Ok(())
            }

            (Phase::EnteringExtended, Event::ExtendedFilled { qty }) => {
                if let Some(p) = self.position.as_mut() {
                    p.extended_open_qty += *qty;
                }
                self.transition_to(Phase::EnteringLighter, now_ts_ms);
                Ok(())
            }

            (Phase::EnteringExtended, Event::ExtendedFailed) => {
                let no_exposure = self
                    .position
                    .as_ref()
                    .is_some_and(|p| p.extended_open_qty.is_zero() && p.lighter_open_qty.is_zero());
                if no_exposure {
                    // Nothing filled, nothing to flatten — straight to Flat.
                    self.position = None;
                    self.transition_to(Phase::Flat, now_ts_ms);
                } else {
                    if let Some(p) = self.position.as_mut() {
                        p.last_emergency_reason = Some(EmergencyReason::ExtendedEntryFailed);
                    }
                    self.transition_to(Phase::EmergencyFlattening, now_ts_ms);
                }
                Ok(())
            }

            (Phase::EnteringLighter, Event::LighterFilled { qty }) => {
                if let Some(p) = self.position.as_mut() {
                    p.lighter_open_qty += *qty;
                    p.fully_filled_ts_ms = Some(now_ts_ms);
                }
                self.transition_to(Phase::Held, now_ts_ms);
                Ok(())
            }

            (Phase::EnteringLighter, Event::LighterFailed) => {
                if let Some(p) = self.position.as_mut() {
                    p.last_emergency_reason = Some(EmergencyReason::LighterEntryFailed);
                }
                self.transition_to(Phase::EmergencyFlattening, now_ts_ms);
                Ok(())
            }

            (Phase::Held, Event::ExitSignal { reason }) => {
                if let Some(p) = self.position.as_mut() {
                    p.last_exit_reason = Some(*reason);
                }
                self.transition_to(Phase::Exiting, now_ts_ms);
                Ok(())
            }

            (Phase::Exiting, Event::ExtendedExitFilled { qty })
            | (Phase::EmergencyFlattening, Event::ExtendedExitFilled { qty }) => {
                if let Some(p) = self.position.as_mut() {
                    p.extended_open_qty = (p.extended_open_qty - *qty).max(Decimal::ZERO);
                }
                self.maybe_complete_flat(now_ts_ms);
                Ok(())
            }

            (Phase::Exiting, Event::LighterExitFilled { qty })
            | (Phase::EmergencyFlattening, Event::LighterExitFilled { qty }) => {
                if let Some(p) = self.position.as_mut() {
                    p.lighter_open_qty = (p.lighter_open_qty - *qty).max(Decimal::ZERO);
                }
                self.maybe_complete_flat(now_ts_ms);
                Ok(())
            }

            (Phase::EmergencyFlattening, Event::EmergencyComplete) => {
                self.position = None;
                self.transition_to(Phase::Flat, now_ts_ms);
                Ok(())
            }

            (Phase::EnteringExtended, Event::Emergency { reason })
            | (Phase::EnteringLighter, Event::Emergency { reason })
            | (Phase::Held, Event::Emergency { reason })
            | (Phase::Exiting, Event::Emergency { reason }) => {
                if let Some(p) = self.position.as_mut() {
                    p.last_emergency_reason = Some(*reason);
                }
                self.transition_to(Phase::EmergencyFlattening, now_ts_ms);
                Ok(())
            }

            _ => Err(TransitionError {
                phase: self.phase,
                event_kind: kind,
            }),
        }
    }

    fn transition_to(&mut self, new_phase: Phase, now_ts_ms: u64) {
        self.phase = new_phase;
        self.phase_entered_ts_ms = now_ts_ms;
    }

    /// Debug helper that returns `Err(reason)` if the machine has
    /// drifted away from the invariants documented in the module-
    /// level diagram. Call sites:
    ///   - tests assert `check_invariants().is_ok()` after each step
    ///   - production debug builds can `debug_assert!(...check_invariants()...)`
    ///     post-`apply` to catch any future regression that lets a
    ///     phase / position pair fall out of sync.
    ///
    /// Pure read; never mutates. Returns the *first* invariant
    /// violated — the names match the module doc-comment so a failure
    /// message is grep-able against this file.
    pub fn check_invariants(&self) -> Result<(), &'static str> {
        match self.phase {
            Phase::Flat => {
                if self.position.is_some() {
                    return Err("Flat: position must be None");
                }
            }
            Phase::EnteringExtended => {
                let p = self
                    .position
                    .as_ref()
                    .ok_or("EnteringExtended: position must be Some")?;
                if !p.extended_open_qty.is_zero() {
                    return Err("EnteringExtended: extended_open_qty must be zero");
                }
                if !p.lighter_open_qty.is_zero() {
                    return Err("EnteringExtended: lighter_open_qty must be zero");
                }
            }
            Phase::EnteringLighter => {
                let p = self
                    .position
                    .as_ref()
                    .ok_or("EnteringLighter: position must be Some")?;
                if p.extended_open_qty <= Decimal::ZERO {
                    return Err("EnteringLighter: extended_open_qty must be > 0");
                }
                if !p.lighter_open_qty.is_zero() {
                    return Err("EnteringLighter: lighter_open_qty must be zero");
                }
            }
            Phase::Held => {
                let p = self
                    .position
                    .as_ref()
                    .ok_or("Held: position must be Some")?;
                if p.fully_filled_ts_ms.is_none() {
                    return Err("Held: fully_filled_ts_ms must be Some");
                }
                if p.extended_open_qty <= Decimal::ZERO {
                    return Err("Held: extended_open_qty must be > 0");
                }
                if p.lighter_open_qty <= Decimal::ZERO {
                    return Err("Held: lighter_open_qty must be > 0");
                }
            }
            Phase::Exiting | Phase::EmergencyFlattening => {
                let p = self
                    .position
                    .as_ref()
                    .ok_or("Exiting/EmergencyFlattening: position must be Some")?;
                if p.extended_open_qty < Decimal::ZERO {
                    return Err("Exiting/EmergencyFlattening: extended_open_qty must be >= 0");
                }
                if p.lighter_open_qty < Decimal::ZERO {
                    return Err("Exiting/EmergencyFlattening: lighter_open_qty must be >= 0");
                }
            }
        }
        Ok(())
    }

    fn maybe_complete_flat(&mut self, now_ts_ms: u64) {
        // Auto-flat from Exiting only — EmergencyFlattening waits for an
        // explicit EmergencyComplete so the execution layer can confirm
        // skew / WS health before we declare the position closed.
        if self.phase != Phase::Exiting {
            return;
        }
        let done = self
            .position
            .as_ref()
            .is_some_and(|p| p.extended_open_qty.is_zero() && p.lighter_open_qty.is_zero());
        if done {
            self.position = None;
            self.transition_to(Phase::Flat, now_ts_ms);
        }
    }
}

impl Default for PositionMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn enter_short(m: &mut PositionMachine, ts_ms: u64) {
        m.apply(
            ts_ms,
            Event::EntrySignal {
                direction: SpreadDirection::Short,
                notional_usd: dec!(1000),
            },
        )
        .unwrap();
    }

    fn fill_to_held(m: &mut PositionMachine, ts_ms: u64, qty: Decimal) {
        m.apply(ts_ms, Event::ExtendedFilled { qty }).unwrap();
        m.apply(ts_ms + 200, Event::LighterFilled { qty }).unwrap();
    }

    #[test]
    fn new_machine_is_flat() {
        let m = PositionMachine::new();
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
        assert!(m.summary().is_none());
    }

    #[test]
    fn flat_to_entering_extended() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        assert_eq!(m.phase(), Phase::EnteringExtended);
        let p = m.position().unwrap();
        assert_eq!(p.direction, SpreadDirection::Short);
        assert_eq!(p.target_notional_usd, dec!(1000));
        assert_eq!(p.entry_signal_ts_ms, 1_000);
        assert!(p.fully_filled_ts_ms.is_none());
    }

    #[test]
    fn happy_path_to_held() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        assert_eq!(m.phase(), Phase::EnteringLighter);
        m.apply(1_700, Event::LighterFilled { qty: dec!(0.0128) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Held);
        let p = m.position().unwrap();
        assert_eq!(p.extended_open_qty, dec!(0.0128));
        assert_eq!(p.lighter_open_qty, dec!(0.0128));
        assert_eq!(p.fully_filled_ts_ms, Some(1_700));
    }

    #[test]
    fn summary_only_in_held() {
        let mut m = PositionMachine::new();
        assert!(m.summary().is_none());
        enter_short(&mut m, 1_000);
        assert!(m.summary().is_none()); // EnteringExtended
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        assert!(m.summary().is_none()); // EnteringLighter
        m.apply(1_700, Event::LighterFilled { qty: dec!(0.0128) })
            .unwrap();
        let s = m.summary().unwrap();
        assert_eq!(s.direction, SpreadDirection::Short);
        assert_eq!(s.entry_ts_ms, 1_700);
    }

    #[test]
    fn held_to_exiting_then_flat() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.apply(
            60_000,
            Event::ExitSignal {
                reason: ExitReason::MeanCross,
            },
        )
        .unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        assert_eq!(
            m.position().unwrap().last_exit_reason,
            Some(ExitReason::MeanCross)
        );
        m.apply(60_500, Event::ExtendedExitFilled { qty: dec!(0.0128) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting); // one leg still open
        m.apply(60_600, Event::LighterExitFilled { qty: dec!(0.0128) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn extended_failed_with_no_fills_goes_flat() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(2_000, Event::ExtendedFailed).unwrap();
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn lighter_failed_in_entering_lighter_emergency_flattens() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        m.apply(2_500, Event::LighterFailed).unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
        assert_eq!(
            m.position().unwrap().last_emergency_reason,
            Some(EmergencyReason::LighterEntryFailed)
        );
    }

    #[test]
    fn emergency_in_held_flattens() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.apply(
            5_000,
            Event::Emergency {
                reason: EmergencyReason::WsStale,
            },
        )
        .unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
        assert_eq!(
            m.position().unwrap().last_emergency_reason,
            Some(EmergencyReason::WsStale)
        );
    }

    #[test]
    fn emergency_flatten_does_not_auto_clear_on_partial_fills() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.apply(
            5_000,
            Event::Emergency {
                reason: EmergencyReason::SkewBreach,
            },
        )
        .unwrap();
        m.apply(5_500, Event::ExtendedExitFilled { qty: dec!(0.0128) })
            .unwrap();
        m.apply(5_600, Event::LighterExitFilled { qty: dec!(0.0128) })
            .unwrap();
        // Both legs zeroed but emergency flow waits for explicit confirmation.
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
        m.apply(5_700, Event::EmergencyComplete).unwrap();
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn reset_clears_position_in_any_phase() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.apply(
            5_000,
            Event::Emergency {
                reason: EmergencyReason::KillSwitch,
            },
        )
        .unwrap();
        m.apply(99_999, Event::Reset).unwrap();
        assert_eq!(m.phase(), Phase::Flat);
        assert!(m.position().is_none());
    }

    #[test]
    fn invalid_event_in_flat_returns_error() {
        let mut m = PositionMachine::new();
        let err = m
            .apply(
                1_000,
                Event::ExitSignal {
                    reason: ExitReason::MeanCross,
                },
            )
            .unwrap_err();
        assert_eq!(err.phase, Phase::Flat);
        assert_eq!(err.event_kind, "ExitSignal");
    }

    #[test]
    fn invalid_event_does_not_change_phase() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        let _ = m.apply(2_000, Event::LighterFilled { qty: dec!(1) });
        assert_eq!(m.phase(), Phase::EnteringExtended);
    }

    #[test]
    fn double_extended_filled_in_entering_extended_is_invalid() {
        // Execution layer aggregates partial fills; only one terminal
        // ExtendedFilled per entry is expected.
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        // Re-applying ExtendedFilled in EnteringLighter is invalid.
        let err = m
            .apply(1_600, Event::ExtendedFilled { qty: dec!(0.0001) })
            .unwrap_err();
        assert_eq!(err.event_kind, "ExtendedFilled");
    }

    #[test]
    fn inventory_skew_zero_when_legs_balanced() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        // Same qty, similar mid → near-zero skew (Ext mid 78050, Lt mid 78000)
        let skew = m.inventory_skew_usd(dec!(78050), dec!(78000));
        assert_eq!(skew, dec!(0.0128) * dec!(50));
    }

    #[test]
    fn inventory_skew_when_only_extended_filled() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        // Lighter not filled yet → full Extended notional is the skew.
        let skew = m.inventory_skew_usd(dec!(78000), dec!(78000));
        assert_eq!(skew, dec!(0.0128) * dec!(78000));
    }

    #[test]
    fn time_in_phase_tracks_phase_entry() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        assert_eq!(m.time_in_phase_ms(3_500), 2_500);
        m.apply(4_000, Event::ExtendedFilled { qty: dec!(0.01) })
            .unwrap();
        assert_eq!(m.time_in_phase_ms(4_750), 750);
    }

    #[test]
    fn ws_stale_in_entering_extended_with_no_fills_still_routes_to_emergency() {
        // Distinct from ExtendedFailed: an Emergency carries a specific
        // reason and always flattens, even from EnteringExtended with
        // zero exposure (operator may want the audit trail).
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(
            2_000,
            Event::Emergency {
                reason: EmergencyReason::WsStale,
            },
        )
        .unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
    }

    #[test]
    fn long_direction_round_trip() {
        let mut m = PositionMachine::new();
        m.apply(
            1_000,
            Event::EntrySignal {
                direction: SpreadDirection::Long,
                notional_usd: dec!(500),
            },
        )
        .unwrap();
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.006) })
            .unwrap();
        m.apply(1_700, Event::LighterFilled { qty: dec!(0.006) })
            .unwrap();
        assert_eq!(m.summary().unwrap().direction, SpreadDirection::Long);
    }

    /// Catalogue case 11 (`docs/execution_layer.md` §2): during a
    /// parallel exit one leg fills before the timeout while the other
    /// is still resting; runner-level `leg_mismatch_timeout_ms` fires
    /// and emits `Emergency{LegMismatchTimeout}`. The state machine
    /// must transition `Exiting` → `EmergencyFlattening`, preserve the
    /// un-zeroed leg's qty (so the EmergencyFlattening loop knows
    /// what to flatten), and record `LegMismatchTimeout` as the
    /// reason.
    #[test]
    fn leg_mismatch_timeout_in_exiting_emergency_flattens_with_remaining_leg() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.apply(
            60_000,
            Event::ExitSignal {
                reason: ExitReason::MeanCross,
            },
        )
        .unwrap();
        // Extended exit fills; Lighter exit is still pending.
        m.apply(60_300, Event::ExtendedExitFilled { qty: dec!(0.0128) })
            .unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        // Runner deadline trips → Emergency{LegMismatchTimeout}.
        m.apply(
            63_300,
            Event::Emergency {
                reason: EmergencyReason::LegMismatchTimeout,
            },
        )
        .unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);
        let p = m.position().unwrap();
        assert_eq!(
            p.last_emergency_reason,
            Some(EmergencyReason::LegMismatchTimeout)
        );
        // Extended fully closed; Lighter still open and must be
        // flattened by the emergency loop.
        assert_eq!(p.extended_open_qty, Decimal::ZERO);
        assert_eq!(p.lighter_open_qty, dec!(0.0128));
    }

    /// Catalogue case 13 reinforcement: `EmergencyComplete` is only
    /// valid from `Phase::EmergencyFlattening`. The execution layer
    /// must not emit it from `Held` / `Exiting` / etc. — this is the
    /// guard that lets the state machine treat `EmergencyComplete` as
    /// the explicit "skew + WS confirmed" handshake described in
    /// `maybe_complete_flat`.
    #[test]
    fn emergency_complete_outside_emergency_flattening_is_invalid() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        // From Held: invalid.
        let err = m.apply(2_000, Event::EmergencyComplete).unwrap_err();
        assert_eq!(err.phase, Phase::Held);
        assert_eq!(err.event_kind, "EmergencyComplete");
        // Phase didn't change.
        assert_eq!(m.phase(), Phase::Held);
        // From Exiting: also invalid.
        m.apply(
            3_000,
            Event::ExitSignal {
                reason: ExitReason::MeanCross,
            },
        )
        .unwrap();
        let err = m.apply(3_100, Event::EmergencyComplete).unwrap_err();
        assert_eq!(err.phase, Phase::Exiting);
    }

    /// Walks every documented transition arc and asserts
    /// `check_invariants()` holds at each post-apply rest state. Acts
    /// as a guard against future apply() edits silently dropping a
    /// position or skipping a fully_filled_ts_ms assignment.
    #[test]
    fn check_invariants_holds_across_full_lifecycle() {
        let mut m = PositionMachine::new();
        m.check_invariants().unwrap();

        // Flat → EnteringExtended
        enter_short(&mut m, 1_000);
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::EnteringExtended);

        // EnteringExtended → EnteringLighter
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::EnteringLighter);

        // EnteringLighter → Held
        m.apply(1_700, Event::LighterFilled { qty: dec!(0.0128) })
            .unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::Held);

        // Held → Exiting → Flat (drain both legs)
        m.apply(
            2_000,
            Event::ExitSignal {
                reason: ExitReason::MeanCross,
            },
        )
        .unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::Exiting);
        m.apply(2_100, Event::ExtendedExitFilled { qty: dec!(0.0128) })
            .unwrap();
        m.check_invariants().unwrap();
        m.apply(2_200, Event::LighterExitFilled { qty: dec!(0.0128) })
            .unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::Flat);
    }

    #[test]
    fn check_invariants_passes_through_emergency_flatten() {
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        fill_to_held(&mut m, 1_500, dec!(0.0128));
        m.check_invariants().unwrap();

        // Held → EmergencyFlattening via WS-stale
        m.apply(
            2_000,
            Event::Emergency {
                reason: EmergencyReason::WsStale,
            },
        )
        .unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::EmergencyFlattening);

        // Partial drain — invariants must still hold (lt qty stays >= 0).
        m.apply(2_100, Event::ExtendedExitFilled { qty: dec!(0.0128) })
            .unwrap();
        m.check_invariants().unwrap();

        // EmergencyComplete → Flat (skips the auto-flat that would
        // fire on Exiting; explicit closure of the emergency path).
        m.apply(2_300, Event::EmergencyComplete).unwrap();
        m.check_invariants().unwrap();
        assert_eq!(m.phase(), Phase::Flat);
    }

    #[test]
    fn check_invariants_detects_manually_corrupted_state() {
        // Defensive: if a future edit ever lets a Held position land
        // without `fully_filled_ts_ms`, the invariant catches it.
        // Construct the broken state manually (the public API can't
        // reach it).
        let mut m = PositionMachine::new();
        enter_short(&mut m, 1_000);
        m.apply(1_500, Event::ExtendedFilled { qty: dec!(0.0128) })
            .unwrap();
        m.apply(1_700, Event::LighterFilled { qty: dec!(0.0128) })
            .unwrap();
        // Surgical mutation: clear fully_filled_ts_ms while keeping
        // phase=Held — exactly the regression we want to catch.
        if let Some(p) = m.position.as_mut() {
            p.fully_filled_ts_ms = None;
        }
        let err = m.check_invariants().unwrap_err();
        assert!(err.contains("fully_filled_ts_ms"));
    }
}
