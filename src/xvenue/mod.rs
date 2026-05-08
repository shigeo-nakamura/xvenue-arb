//! xvenue-arb strategy modules.
//!
//! Cross-venue statistical arb between Lighter and Extended. See
//! `docs/DESIGN.md` for the full design; this tree is a skeleton
//! that will be filled in after Phase 0 data feasibility GO.

pub mod bt;
pub mod bt_grid;
pub mod config;
pub mod live;
pub mod live_exec;
pub(crate) mod s3_mirror;
pub mod signal;
pub mod sizing;
pub mod spread;
pub mod state;
pub mod status;
#[cfg(test)]
pub mod test_helpers;
