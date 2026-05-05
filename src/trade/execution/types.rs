//! Execution-layer terminal events + config (bot-strategy#244 Group B).
//!
//! Each per-venue executor (`extended_maker`, `lighter_fill`) returns
//! one `*Terminal` per call after aggregating partial fills, retries,
//! and any taker fallback. The runner converts the terminal into the
//! corresponding `state::Event` (`ExtendedFilled` / `ExtendedFailed`
//! / `LighterFilled` / `LighterFailed`) so the position machine sees
//! a single transition per leg per cycle — matching the state-machine
//! contract documented in `docs/execution_layer.md` §1.

use rust_decimal::Decimal;

/// Why the execution layer gave up on an order. Mostly informational
/// (logs + status emit); the position machine routes both terminal
/// failures and timeouts to the same `*Failed` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionFailure {
    /// Post-only chase exhausted all retries without filling and
    /// taker fallback was disabled. (Catalogue case 1.)
    PostOnlyExhausted,
    /// Taker fallback also rejected by the venue. (Edge of case 2.)
    TakerRejected,
    /// Venue rejected the place / cancel / poll call (HTTP error,
    /// auth failure, schema mismatch). Distinct from `Timeout` —
    /// the venue actively responded with an error.
    VenueRejected,
    /// Lighter market / aggressive-limit order did not fill within
    /// `lighter_fill_timeout_ms`. (Catalogue case 3.)
    Timeout,
    /// Order was cancelled by the venue before any fill (e.g. price
    /// moved through a post-only quote). Treated as zero-fill.
    Cancelled,
}

/// Terminal outcome of a single Extended entry / exit cycle. The
/// `qty` in `Filled` is the **aggregated** filled qty across all
/// chase rounds + the taker fallback (if any). It can be less than
/// the originally requested qty when chase exhausted but partial
/// fills landed (catalogue case 6).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExtendedTerminal {
    Filled { qty: Decimal },
    Failed { reason: ExecutionFailure },
}

impl ExtendedTerminal {
    pub fn filled_qty(&self) -> Decimal {
        match self {
            ExtendedTerminal::Filled { qty } => *qty,
            ExtendedTerminal::Failed { .. } => Decimal::ZERO,
        }
    }

    pub fn is_filled(&self) -> bool {
        matches!(self, ExtendedTerminal::Filled { qty } if *qty > Decimal::ZERO)
    }
}

/// Terminal outcome of a single Lighter entry / exit cycle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LighterTerminal {
    Filled { qty: Decimal },
    Failed { reason: ExecutionFailure },
}

impl LighterTerminal {
    pub fn filled_qty(&self) -> Decimal {
        match self {
            LighterTerminal::Filled { qty } => *qty,
            LighterTerminal::Failed { .. } => Decimal::ZERO,
        }
    }

    pub fn is_filled(&self) -> bool {
        matches!(self, LighterTerminal::Filled { qty } if *qty > Decimal::ZERO)
    }
}

/// Knobs that show up on every per-venue executor — split out so
/// `ExtendedMakerConfig` and `LighterFillConfig` can embed it instead
/// of carrying parallel copies. Today this is just `poll_interval_ms`;
/// keeping the struct around (rather than a bare field) makes future
/// shared knobs (place-retry budget, etc.) additive without touching
/// every construction site.
#[derive(Debug, Clone)]
pub struct CommonExecutorConfig {
    /// How often `poll_until_terminal_or_deadline` re-polls the venue
    /// for fill status. Lighter defaults tighter (25 ms) since fills
    /// arrive in tens of ms; Extended defaults looser (50 ms) since
    /// chase rounds are bounded by `chase_timeout_ms` already.
    pub poll_interval_ms: u64,
}

/// Knobs for the Extended maker chase loop. Sourced from
/// `XvenueConfig` so YAML drives behavior and unit tests can
/// construct deterministic configs inline.
#[derive(Debug, Clone)]
pub struct ExtendedMakerConfig {
    /// Common executor knobs (poll cadence, etc.). See
    /// [`CommonExecutorConfig`].
    pub common: CommonExecutorConfig,
    /// Number of book ticks to chase a stale post-only price by
    /// before re-cancelling and re-posting. 1 means "if the book
    /// moved, re-post at the new best price"; larger values let the
    /// post-only order trail the book without re-posting on every
    /// jitter. `extended_chase_ticks` in YAML.
    pub chase_ticks: u64,
    /// How many full place-cancel-replace cycles to run before
    /// declaring chase exhausted. Each cycle is bounded by
    /// `chase_timeout_ms`. `extended_chase_retries` in YAML.
    pub chase_retries: u32,
    /// Per-cycle deadline. The order is cancelled after this if it
    /// hasn't fully filled. `extended_chase_timeout_ms` in YAML.
    pub chase_timeout_ms: u64,
    /// When true, after `chase_retries` cycles all fail, place a
    /// taker order for the residual qty. `extended_taker_fallback`
    /// in YAML.
    pub taker_fallback: bool,
    /// `extended_post_only` — when false the maker stage is skipped
    /// and the executor goes straight to taker. Operator escape for
    /// urgent venue-down scenarios.
    pub post_only: bool,
    /// bot-strategy#298: after a taker round terminates with
    /// `filled=0 cancelled=false` (i.e. the venue's WS feed didn't
    /// terminal the order within `chase_timeout_ms`), wait this many
    /// ms and poll the order one more time before declaring Timeout.
    /// Catches WS-lag fills that landed at the venue but propagated
    /// through the connector cache slightly past the chase deadline,
    /// avoiding a one-sided position → EmergencyFlattening on what
    /// was actually a successful fill. 0 disables the grace poll.
    /// `extended_taker_grace_poll_ms` in YAML.
    pub taker_grace_poll_ms: u64,
}

impl ExtendedMakerConfig {
    /// Validates the values. Returns Err with a description on bad
    /// configs so the runner can refuse to start instead of running
    /// with an undefined chase loop.
    pub fn validate(&self) -> Result<(), String> {
        if self.chase_timeout_ms == 0 {
            return Err("extended_chase_timeout_ms must be > 0".into());
        }
        if self.chase_retries == 0 && !self.taker_fallback {
            return Err(
                "extended_chase_retries=0 with taker_fallback=false would never place an order"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Knobs for the Lighter post-only chase loop (bot-strategy#309 step 6).
/// Mirrors [`ExtendedMakerConfig`] for the maker-on-Lighter redesign.
/// Active only when YAML sets `lighter_post_only: true`; the legacy
/// market / aggressive-limit path stays the default until the dex-
/// connector verification gate passes.
#[derive(Debug, Clone)]
pub struct LighterMakerConfig {
    pub common: CommonExecutorConfig,
    /// Number of book ticks to chase a stale post-only price by before
    /// re-cancelling and re-posting. `lighter_chase_ticks` in YAML.
    pub chase_ticks: u64,
    /// How many full place-cancel-replace cycles to run before declaring
    /// chase exhausted. `lighter_chase_retries` in YAML.
    pub chase_retries: u32,
    /// Per-cycle deadline. `lighter_chase_timeout_ms` in YAML.
    pub chase_timeout_ms: u64,
    /// When true, after `chase_retries` cycles all fail, place a taker
    /// order for the residual qty. `lighter_taker_fallback` in YAML.
    pub taker_fallback: bool,
    /// `lighter_post_only` — when false the maker stage is skipped
    /// entirely and execution stays on the legacy `LighterFillLoop` path.
    /// The runner uses this flag to pick which loop to drive; the maker
    /// loop itself only runs when this is true.
    pub post_only: bool,
}

impl LighterMakerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.chase_timeout_ms == 0 {
            return Err("lighter_chase_timeout_ms must be > 0".into());
        }
        if self.post_only && self.chase_retries == 0 && !self.taker_fallback {
            return Err(
                "lighter_chase_retries=0 with taker_fallback=false would never place an order"
                    .into(),
            );
        }
        Ok(())
    }

    /// bot-strategy#288 invariant: the worst-case chase budget plus the
    /// Extended taker deadline must fit inside `leg_mismatch_timeout_ms`
    /// so a slow Lighter chase can't stretch past the cross-venue
    /// recovery window. Returns the worst-case Lighter budget in ms.
    pub fn worst_case_budget_ms(&self) -> u64 {
        let retries = self.chase_retries.max(1) as u64;
        retries.saturating_mul(self.chase_timeout_ms)
    }
}

/// Knobs for the Lighter market / aggressive-limit fill flow.
#[derive(Debug, Clone)]
pub struct LighterFillConfig {
    /// Common executor knobs (poll cadence, etc.). See
    /// [`CommonExecutorConfig`].
    pub common: CommonExecutorConfig,
    /// "market" or "limit". When "limit", the executor places an
    /// aggressive limit at the opposite-side top of book to maximize
    /// fill probability while picking up some price improvement on
    /// quiet markets. `lighter_order_type` in YAML.
    pub order_type: LighterOrderType,
    /// Hard deadline on the fill. Lighter fills should land in
    /// ~50ms in normal conditions; 1000 ms is generous enough to
    /// absorb the occasional reconnect without false-positive
    /// `LighterFailed`. `lighter_fill_timeout_ms` in YAML.
    pub fill_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterOrderType {
    Market,
    AggressiveLimit,
}

impl LighterFillConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.fill_timeout_ms == 0 {
            return Err("lighter_fill_timeout_ms must be > 0".into());
        }
        Ok(())
    }
}

/// Parses a YAML string ("market" / "limit" / "aggressive_limit")
/// into the typed enum. Tolerant of casing for operator UX.
pub fn parse_lighter_order_type(s: &str) -> Result<LighterOrderType, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "market" => Ok(LighterOrderType::Market),
        "limit" | "aggressive_limit" | "aggressive-limit" => Ok(LighterOrderType::AggressiveLimit),
        other => Err(format!(
            "lighter_order_type must be 'market' or 'limit'/'aggressive_limit', got {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn extended_terminal_filled_qty_zero_when_failed() {
        let t = ExtendedTerminal::Failed {
            reason: ExecutionFailure::PostOnlyExhausted,
        };
        assert_eq!(t.filled_qty(), Decimal::ZERO);
        assert!(!t.is_filled());
    }

    #[test]
    fn extended_terminal_filled_returns_qty() {
        let t = ExtendedTerminal::Filled { qty: dec!(0.5) };
        assert_eq!(t.filled_qty(), dec!(0.5));
        assert!(t.is_filled());
    }

    #[test]
    fn extended_terminal_zero_filled_is_not_is_filled() {
        // Defensive: a Filled{0} should never appear from the
        // maker (it would emit Failed instead) but we want
        // `is_filled` to be safe.
        let t = ExtendedTerminal::Filled { qty: Decimal::ZERO };
        assert!(!t.is_filled());
    }

    #[test]
    fn lighter_terminal_filled_qty_handles_failed() {
        let t = LighterTerminal::Failed {
            reason: ExecutionFailure::Timeout,
        };
        assert_eq!(t.filled_qty(), Decimal::ZERO);
        assert!(!t.is_filled());
    }

    #[test]
    fn extended_config_rejects_zero_timeout() {
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 50 },
            chase_ticks: 1,
            chase_retries: 3,
            chase_timeout_ms: 0,
            taker_fallback: true,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn extended_config_rejects_zero_retries_no_fallback() {
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 50 },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 500,
            taker_fallback: false,
            post_only: true,
            taker_grace_poll_ms: 0,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn extended_config_zero_retries_with_fallback_is_ok() {
        // Operator-emergency mode: skip maker, go straight to taker.
        let cfg = ExtendedMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 50 },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 500,
            taker_fallback: true,
            post_only: false,
            taker_grace_poll_ms: 0,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn lighter_maker_config_rejects_zero_timeout() {
        let cfg = LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 3,
            chase_timeout_ms: 0,
            taker_fallback: true,
            post_only: true,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn lighter_maker_config_rejects_zero_retries_no_fallback() {
        let cfg = LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 250,
            taker_fallback: false,
            post_only: true,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn lighter_maker_config_disabled_post_only_is_ok() {
        // post_only=false short-circuits — the loop is unused and
        // validate() should not complain about chase budget.
        let cfg = LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 0,
            chase_timeout_ms: 250,
            taker_fallback: false,
            post_only: false,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn lighter_maker_worst_case_budget_multiplies_retries_and_timeout() {
        let cfg = LighterMakerConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            chase_ticks: 1,
            chase_retries: 4,
            chase_timeout_ms: 250,
            taker_fallback: true,
            post_only: true,
        };
        assert_eq!(cfg.worst_case_budget_ms(), 1_000);
        // chase_retries=0 still costs at least one round.
        let zero = LighterMakerConfig {
            chase_retries: 0,
            ..cfg.clone()
        };
        assert_eq!(zero.worst_case_budget_ms(), 250);
    }

    #[test]
    fn lighter_config_rejects_zero_timeout() {
        let cfg = LighterFillConfig {
            common: CommonExecutorConfig { poll_interval_ms: 25 },
            order_type: LighterOrderType::Market,
            fill_timeout_ms: 0,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn lighter_order_type_parse_variants() {
        assert_eq!(
            parse_lighter_order_type("market").unwrap(),
            LighterOrderType::Market
        );
        assert_eq!(
            parse_lighter_order_type("LIMIT").unwrap(),
            LighterOrderType::AggressiveLimit
        );
        assert_eq!(
            parse_lighter_order_type(" Aggressive_Limit ").unwrap(),
            LighterOrderType::AggressiveLimit
        );
        assert!(parse_lighter_order_type("post_only").is_err());
        assert!(parse_lighter_order_type("").is_err());
    }
}
