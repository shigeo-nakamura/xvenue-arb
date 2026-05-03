//! Shared `poll_until_terminal_or_deadline` for the per-venue executor
//! loops in `extended_maker.rs` and `lighter_fill.rs`, plus the
//! `Executor` trait that both per-venue loops implement so callers can
//! hold them behind a uniform interface (test mocks, future
//! third-venue plug-ins).
//!
//! Both per-venue loops follow the same shape: place an order, then
//! poll `VenueOps::poll_fill_status` at a fixed cadence until the
//! venue terminals the order or a wall-clock deadline elapses. The
//! only caller-visible knobs are the per-venue timeout, poll cadence,
//! and log prefix.
//!
//! Behaviour-preserving extraction of the previously-duplicated
//! `LighterFillLoop::poll_until_terminal_or_deadline` and
//! `ExtendedMakerLoop::poll_until_terminal_or_deadline`.

use std::time::Duration;

use async_trait::async_trait;

use rust_decimal::Decimal;
use tokio::time::Instant;

use super::venue_ops::{OrderFillStatus, VenueOps};

/// Common public surface of `LighterFillLoop` and `ExtendedMakerLoop`.
/// Captures "given a per-venue request, return one terminal event"
/// without forcing the caller to know which venue is on the other end
/// — useful for test mocks and future third-venue plug-ins. The
/// per-venue loops keep their idiomatic state-machine internals; the
/// trait just names the shared shape.
#[async_trait]
pub trait Executor {
    type Request: Send;
    type Terminal: Send;

    async fn run(&self, req: Self::Request) -> Self::Terminal;
}

/// Result of one poll round. `terminal_cancelled` is true when the
/// venue terminated the order with `cancelled=true` (taker rejected /
/// post-only crossed); `false` covers both "deadline elapsed without
/// terminal" and "venue terminated with cancelled=false" (i.e. fully
/// filled or flagged-as-fill).
#[derive(Debug, Clone, Copy)]
pub struct PollOutcome {
    pub filled_this_round: Decimal,
    pub terminal_cancelled: bool,
}

/// Poll `ops.poll_fill_status(symbol, order_id)` every
/// `poll_interval_ms` until the venue marks the order terminal or
/// `timeout_ms` elapses since the call entered. Soft errors are
/// logged at warn under `log_prefix` and the loop keeps polling — a
/// venue may recover within the window and surface a fill we'd
/// otherwise miss.
pub async fn poll_until_terminal_or_deadline<V: VenueOps + ?Sized>(
    ops: &V,
    symbol: &str,
    order_id: &str,
    timeout_ms: u64,
    poll_interval_ms: u64,
    log_prefix: &'static str,
) -> PollOutcome {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let poll_dur = Duration::from_millis(poll_interval_ms);
    let mut filled_this_round = Decimal::ZERO;
    loop {
        match ops.poll_fill_status(symbol, order_id).await {
            Ok(OrderFillStatus {
                filled_qty,
                terminal,
                cancelled,
            }) => {
                filled_this_round = filled_qty.max(filled_this_round);
                if terminal {
                    return PollOutcome {
                        filled_this_round,
                        terminal_cancelled: cancelled,
                    };
                }
            }
            Err(e) => {
                log::warn!(
                    "[{}] poll_fill_status order={} err={:?}",
                    log_prefix,
                    order_id,
                    e
                );
            }
        }
        if Instant::now() >= deadline {
            return PollOutcome {
                filled_this_round,
                terminal_cancelled: false,
            };
        }
        tokio::time::sleep(poll_dur).await;
    }
}
