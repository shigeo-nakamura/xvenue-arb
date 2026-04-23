//! Position sizing based on account equity. DESIGN.md §4.4.
//!
//! `notional = (extended_equity + lighter_equity) * trade_size_pct`
//! clamped to `[min_notional_usd, max_notional_usd]`. Both legs use the
//! same notional for delta-neutrality; Lighter's finer tick lets it
//! match the Extended leg exactly.
//!
//! Placeholder until Phase 0 GO.
