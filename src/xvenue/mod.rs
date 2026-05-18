//! xvenue-arb strategy modules.
//!
//! Cross-venue statistical arb between Lighter and Extended. See
//! `docs/DESIGN.md` for the full design; this tree is a skeleton
//! that will be filled in after Phase 0 data feasibility GO.

pub mod bt;
pub mod bt_grid;
pub mod config;
mod emergency_handlers;
mod entry_dispatch;
mod entry_filter;
mod exit_dispatch;
pub mod live;
pub mod live_exec;
mod live_pnl;
mod live_status;
mod live_tick;
pub(crate) mod s3_mirror;
pub mod signal;
pub mod sizing;
pub mod spread;
pub mod state;
pub mod status;
#[cfg(test)]
pub mod test_helpers;
