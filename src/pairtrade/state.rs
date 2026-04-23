//! Position, pending-order, and related state types extracted from the
//! monolithic pairtrade module. Field visibility is `pub(super)` so that the
//! engine in `mod.rs` can keep accessing them as before; promotion to `pub`
//! is deferred until the engine itself migrates out.

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::time::Instant;

use dex_connector::DexError;
use rust_decimal::Decimal;

use super::config::PairTradeConfig;
use super::kalman::KalmanBeta;
use super::util::mean_std;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PositionDirection {
    LongSpread,
    ShortSpread,
}

#[derive(Debug, Clone)]
pub(super) struct Position {
    pub(super) direction: PositionDirection,
    pub(super) entered_at: Instant,
    /// Replay-aware entry timestamp (seconds). In live mode equals
    /// `chrono::Utc::now().timestamp()` at the moment of entry; in backtest
    /// mode equals the replay's logical timestamp. Used for all
    /// duration-based decisions (force_close, hold-time PnL, etc.) so they
    /// behave identically under replay.
    pub(super) entered_ts: i64,
    pub(super) entry_price_a: Option<Decimal>,
    pub(super) entry_price_b: Option<Decimal>,
    pub(super) entry_size_a: Option<Decimal>,
    pub(super) entry_size_b: Option<Decimal>,
    pub(super) entry_z: Option<f64>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingLeg {
    pub(super) symbol: String,
    pub(super) order_id: String,
    pub(super) exchange_order_id: Option<String>,
    pub(super) target: Decimal,
    pub(super) filled: Decimal,
    pub(super) side: dex_connector::OrderSide,
    /// Limit price posted for this leg, when placed as a limit/post-only
    /// order. `None` for market orders and for reissue paths that do not
    /// carry a limit forward. Used by the post-only fallback instrumentation
    /// ([ORDER_FALLBACK_DETAIL], bot-strategy#165) to compare the posted
    /// price against the book at timeout.
    pub(super) limit_price: Option<Decimal>,
}

#[derive(Debug)]
pub(super) struct PendingOrders {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) direction: PositionDirection,
    pub(super) placed_at: Instant,
    pub(super) hedge_retry_count: u32,
    pub(super) post_only_hybrid: bool,
}

#[derive(Debug)]
pub(super) struct PendingStatus {
    pub(super) open_remaining: usize,
    pub(super) fills: HashMap<String, Decimal>,
    pub(super) open_ids: HashSet<String>,
}

#[derive(Debug)]
pub(super) struct PartialOrderPlacementError {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) source: DexError,
}

impl PartialOrderPlacementError {
    pub(super) fn new(legs: Vec<PendingLeg>, source: DexError) -> Self {
        Self { legs, source }
    }

    pub(super) fn legs(&self) -> &[PendingLeg] {
        &self.legs
    }
}

impl std::fmt::Display for PartialOrderPlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to place all legs: {}", self.source)
    }
}

impl Error for PartialOrderPlacementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub(super) struct PairState {
    pub(super) beta: f64,
    pub(super) z_entry: f64,
    pub(super) spread_history: VecDeque<f64>,
    pub(super) last_spread: Option<f64>,
    pub(super) last_velocity_sigma_per_min: f64,
    pub(super) position: Option<Position>,
    pub(super) last_exit_at: Option<Instant>,
    /// Replay-aware companion to `last_exit_at`. Drives the should_enter
    /// cooldown and unhedged-close cooldown so they fire correctly under
    /// backtest replay.
    pub(super) last_exit_ts: Option<i64>,
    pub(super) beta_short: f64,
    pub(super) beta_long: f64,
    pub(super) half_life_hours: f64,
    pub(super) adf_p_value: f64,
    pub(super) eligible: bool,
    pub(super) last_evaluated: Option<Instant>,
    /// Replay-aware companion to `last_evaluated`. Drives the periodic
    /// pair re-evaluation interval (PAIR_SELECTION_INTERVAL_SECS).
    pub(super) last_evaluated_ts: Option<i64>,
    pub(super) p_value_weighted_score: f64,
    pub(super) beta_gap: f64,
    pub(super) pending_entry: Option<PendingOrders>,
    pub(super) pending_exit: Option<PendingOrders>,
    pub(super) position_guard: bool,
    pub(super) kalman: Option<KalmanBeta>,
    /// Rolling history of the most recent full-window spread std values, one
    /// sample per bar with a valid z-score. Used by the std-collapse guard
    /// (bot-strategy#62) to detect when the z-score denominator has fallen
    /// far below its recent median — a sign that the z-score is no longer a
    /// trustworthy mean-reversion signal.
    pub(super) std_history: VecDeque<f64>,
    /// BT fill-delay: when an exit is decided in dry_run + backtest mode with
    /// `bt_fill_delay_secs > 0`, we defer clearing `position` until the replay
    /// clock has advanced past this timestamp. While set, the bot considers the
    /// position still held (blocking new entries). The PnL is already computed
    /// and stored here so it can be logged when the deferred exit resolves.
    pub(super) bt_deferred_exit: Option<BtDeferredExit>,
}

/// Deferred exit info for BT fill-delay simulation.
#[derive(Debug)]
pub(super) struct BtDeferredExit {
    /// Replay timestamp (seconds) at which the position should be cleared.
    pub(super) resolve_at_ts: i64,
}

impl PairState {
    pub(super) fn new(window: usize, z_entry: f64) -> Self {
        Self {
            beta: 1.0,
            z_entry,
            spread_history: VecDeque::with_capacity(window),
            last_spread: None,
            last_velocity_sigma_per_min: 0.0,
            position: None,
            last_exit_at: None,
            last_exit_ts: None,
            beta_short: 1.0,
            beta_long: 1.0,
            half_life_hours: 0.0,
            adf_p_value: 1.0,
            eligible: false,
            last_evaluated: None,
            last_evaluated_ts: None,
            p_value_weighted_score: 0.0,
            beta_gap: 0.0,
            pending_entry: None,
            pending_exit: None,
            position_guard: false,
            kalman: None,
            std_history: VecDeque::new(),
            bt_deferred_exit: None,
        }
    }

    pub(super) fn push_spread(&mut self, spread: f64, window: usize, config: &PairTradeConfig) {
        if self.spread_history.len() >= window {
            self.spread_history.pop_front();
        }
        self.spread_history.push_back(spread);
        self.last_spread = Some(spread);

        // Record the current full-window std for the std-collapse guard
        // (bot-strategy#62). Skip degenerate or insufficient samples so the
        // rolling median only tracks meaningful std values.
        let std_window = config.default_pair_params.std_collapse_window_bars;
        if std_window > 0 {
            if let Some((_z, std)) = self.z_score() {
                if std > 1e-9 {
                    if self.std_history.len() >= std_window {
                        self.std_history.pop_front();
                    }
                    self.std_history.push_back(std);
                }
            }
        }

        // velocity uses bar-to-bar move (1-minute bars) normalized by std dev
        let k = 1_usize;
        if self.spread_history.len() > k {
            if let (Some(&latest), Some(&past)) = (
                self.spread_history.back(),
                self.spread_history.get(self.spread_history.len() - k - 1),
            ) {
                let delta = latest - past; // per-bar move
                let per_min = delta / ((k as f64 * config.trading_period_secs as f64) / 60.0);
                if let Some((_z, std)) = self.z_score() {
                    if std > 1e-9 {
                        self.last_velocity_sigma_per_min = per_min / std;
                    }
                }
            }
        }
    }

    pub(super) fn z_score(&self) -> Option<(f64, f64)> {
        self.z_score_details().map(|(z, std, _, _)| (z, std))
    }

    pub(super) fn z_score_details(&self) -> Option<(f64, f64, f64, f64)> {
        if self.spread_history.len() < 2 {
            return None;
        }
        let (mean, std) = mean_std(&self.spread_history)?;
        let latest = *self.spread_history.back().unwrap();
        let z = if std < 1e-9 {
            0.0
        } else {
            (latest - mean) / std
        };
        Some((z, std, mean, latest))
    }

    /// Compute z-score using only the last `window` bars of spread_history.
    /// Used by the multi-timeframe confluence filter.
    pub(super) fn z_score_for_window(&self, window: usize) -> Option<f64> {
        let len = self.spread_history.len().min(window);
        if len < 2 {
            return None;
        }
        let start = self.spread_history.len() - len;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for i in start..self.spread_history.len() {
            let v = self.spread_history[i];
            sum += v;
            sum_sq += v * v;
        }
        let n = len as f64;
        let mean = sum / n;
        let var = (sum_sq / n) - mean * mean;
        let std = var.max(0.0).sqrt();
        if std < 1e-9 {
            return None;
        }
        let latest = *self.spread_history.back().unwrap();
        Some((latest - mean) / std)
    }
}
