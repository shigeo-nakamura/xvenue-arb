//! Execution layer (bot-strategy#244 Group B).
//!
//! Owns the per-venue order placement / chase / taker-fallback paths
//! plus the venue-side primitives (`VenueOps`) the chase algorithms
//! depend on. Pure logic lives in `types` and the per-venue modules
//! (`extended_maker`, `lighter_fill`); production wires the live
//! [`dex_connector_box::DexConnectorBox`] in via a `VenueOps` impl
//! while tests substitute a deterministic mock.
//!
//! Each module emits one terminal `ExtendedTerminal` /
//! `LighterTerminal` per call, which the runner translates into a
//! `state::Event` for the position machine. See
//! `docs/execution_layer.md` §1 for the module-level decomposition
//! and §2 for the failure-mode catalogue these tests cover.

pub mod dex_connector_box;
pub mod emergency_loop;
pub mod extended_maker;
pub mod lighter_fill;
pub mod live_venue_ops;
pub mod parallel_exit;
pub mod types;
pub mod venue_ops;
