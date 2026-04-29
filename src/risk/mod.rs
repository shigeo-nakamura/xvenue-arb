//! Risk-management gates for xvenue-arb (bot-strategy#244 D-2..7).
//!
//! `manager` owns the persisted [`RiskState`] (daily / session DD,
//! consecutive-loss cooldown) and exposes the gate check the live
//! loop calls before emitting `Decision::Enter`. Mirrors pairtrade's
//! #185 work but slimmed for the single-instance topology of
//! xvenue-arb (one process per symbol).
//!
//! Other risk modules (`reference_guard`, `kill_switch_file_watcher`,
//! `ws_health`, `skew_monitor`) land in subsequent commits per the
//! sprint plan in #244.

pub mod kill_switch;
pub mod manager;
pub mod reference_guard;
