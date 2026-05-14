//! WebSocket transient-event deferral and 24h ws-reset counter, layered
//! alongside [`crate::error_counter`]. Extracted from the original
//! `error_counter.rs` per bot-strategy#383.
//!
//! Two concerns live here:
//!
//! 1. **Defer transient WS events** (bot-strategy#261). A WS reset that
//!    auto-recovers within [`WS_DEFER_WINDOW_SECS`] must not contribute to
//!    the rolling error/warn counters surfaced in `status.json`. We queue
//!    `WebSocket error:` / `WebSocket IO error detail:` / `tick error:
//!    read_mid` / `order book snapshot unavailable` / `waiting for
//!    websocket data` log lines into `pending_ws`. A subsequent
//!    `WebSocket connected successfully` / `WebSocket subscriptions sent
//!    successfully` drains the queue; otherwise the entries age out into
//!    the durable counters via [`flush_expired_pending`].
//!
//! 2. **24h ws-reset counter** (bot-strategy#343). Independent windowed
//!    counter of log lines containing
//!    [`WS_RESET_PHRASE`]. Replaces the dashboard's old `journalctl ... |
//!    awk` SSM probe with a self-reported field in `status.json`.
//!    Counted independently of the defer machinery and independently of
//!    [`crate::error_counter::is_counting_suppressed`] — maintenance
//!    suppression does NOT mask ws_reset volume.

use log::Level;
use std::collections::VecDeque;
use std::sync::Mutex;

use crate::error_counter::Counters;

/// A WS reset that auto-recovers within this window does not contribute to
/// the rolling counts (see bot-strategy#261). Sized for typical Lighter /
/// Extended reconnect cycles (~5–30s observed); 60s gives headroom for slow
/// reconnects without ageing out a real persistent disconnect.
pub(crate) const WS_DEFER_WINDOW_SECS: i64 = 60;

/// 24h window for the ws-reset counter. Threshold for alerting is 10/day
/// per bot-strategy#47.
pub(crate) const WS_RESET_24H_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Substring used to identify a WS reset event in the log stream. The
/// dashboard's old journalctl probe matched the same exact phrase, so the
/// bot-self-reported counter and the journalctl-derived counter are
/// interchangeable.
pub(crate) const WS_RESET_PHRASE: &str = "Connection reset without closing handshake";

#[derive(Debug, Clone)]
pub(crate) struct PendingWsEntry {
    pub ts: i64,
    pub level: Level,
    pub message: String,
}

pub(crate) struct WsDeferState {
    pending_ws: Mutex<VecDeque<PendingWsEntry>>,
    ws_resets_24h: Mutex<VecDeque<i64>>,
}

impl WsDeferState {
    pub(crate) fn new() -> Self {
        Self {
            pending_ws: Mutex::new(VecDeque::new()),
            ws_resets_24h: Mutex::new(VecDeque::new()),
        }
    }
}

/// Match log lines that signal a transient connectivity event whose effect
/// should be suppressed if the bot recovers within [`WS_DEFER_WINDOW_SECS`].
/// Covers (1) the connector ERROR raised by the tungstenite WS layer when
/// the upstream RST-resets, and (2) the WARN downstream of that — the
/// xvenue-arb tick error and the pairtrade orderbook-stale signals — that
/// fire while the reconnect is in progress.
pub(crate) fn is_ws_transient_event(msg: &str) -> bool {
    msg.starts_with("WebSocket error:")
        || msg.starts_with("WebSocket IO error detail:")
        || msg.contains("tick error: read_mid")
        || msg.contains("order book snapshot unavailable")
        || msg.contains("waiting for websocket data")
}

/// Match log lines that signal a successful WS reconnect. Drains pending
/// transient entries logged within the past [`WS_DEFER_WINDOW_SECS`].
pub(crate) fn is_ws_recovery_event(msg: &str) -> bool {
    msg.starts_with("WebSocket connected successfully")
        || msg.contains("WebSocket subscriptions sent successfully")
}

pub(crate) fn defer_entry(state: &WsDeferState, entry: PendingWsEntry) {
    state.pending_ws.lock().unwrap().push_back(entry);
}

/// If `msg` contains [`WS_RESET_PHRASE`], append `ts` to the 24h queue
/// (pruning expired entries first). Independent of maintenance suppression.
pub(crate) fn record_ws_reset_if_match(state: &WsDeferState, ts: i64, msg: &str) {
    if !msg.contains(WS_RESET_PHRASE) {
        return;
    }
    let cutoff = ts - WS_RESET_24H_WINDOW_SECS;
    let mut q = state.ws_resets_24h.lock().unwrap();
    while let Some(&front) = q.front() {
        if front < cutoff {
            q.pop_front();
        } else {
            break;
        }
    }
    q.push_back(ts);
}

/// Drop pending entries whose timestamp is within the defer window of
/// `ts`. Mirrors the original `retain(|e| e.ts < cutoff)` semantics.
pub(crate) fn drain_on_recovery(state: &WsDeferState, ts: i64) {
    let cutoff = ts - WS_DEFER_WINDOW_SECS;
    state.pending_ws.lock().unwrap().retain(|e| e.ts < cutoff);
}

pub(crate) fn ws_reset_24h_count(state: &WsDeferState) -> u64 {
    let now = chrono::Utc::now().timestamp();
    let cutoff = now - WS_RESET_24H_WINDOW_SECS;
    let mut q = state.ws_resets_24h.lock().unwrap();
    while let Some(&front) = q.front() {
        if front < cutoff {
            q.pop_front();
        } else {
            break;
        }
    }
    q.len() as u64
}

/// Move pending WS-defer entries whose recovery window has expired into
/// the durable counters via [`Counters::commit_batch`]. Lock order:
/// pending_ws → recent → last_error/last_warn, matching the original
/// monolithic implementation so the snapshot path remains deadlock-free.
pub(crate) fn flush_expired_pending(state: &WsDeferState, counters: &Counters, now: i64) {
    let cutoff = now - WS_DEFER_WINDOW_SECS;
    let mut pending = state.pending_ws.lock().unwrap();
    let mut to_commit: Vec<PendingWsEntry> = Vec::new();
    while let Some(front) = pending.front() {
        if front.ts <= cutoff {
            to_commit.push(pending.pop_front().unwrap());
        } else {
            break;
        }
    }
    drop(pending);
    if to_commit.is_empty() {
        return;
    }
    counters.commit_batch(to_commit.into_iter().map(|e| (e.ts, e.level, e.message)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_counter::{is_counting_suppressed, set_counting_suppressed, Counters};
    use std::sync::OnceLock;
    use std::sync::{Arc, Mutex as StdMutex};

    fn _serialize() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| StdMutex::new(()));
        match m.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn pending_is_empty(ws: &WsDeferState) -> bool {
        ws.pending_ws.lock().unwrap().is_empty()
    }

    fn ws_reset_count_at(ws: &WsDeferState, now: i64) -> u64 {
        let cutoff = now - WS_RESET_24H_WINDOW_SECS;
        let mut q = ws.ws_resets_24h.lock().unwrap();
        while let Some(&front) = q.front() {
            if front < cutoff {
                q.pop_front();
            } else {
                break;
            }
        }
        q.len() as u64
    }

    /// Run the same sequence the production `ErrorCountingLogger::log()`
    /// runs, against test-owned [`Counters`] + [`WsDeferState`]. Mirrors
    /// the production code path so behavioural assertions here transfer
    /// 1:1 to live behaviour.
    fn fake_log(counters: &Counters, ws: &WsDeferState, ts: i64, level: Level, msg: &str) {
        record_ws_reset_if_match(ws, ts, msg);
        if is_ws_recovery_event(msg) {
            drain_on_recovery(ws, ts);
        }
        flush_expired_pending(ws, counters, ts);
        if level != Level::Error && level != Level::Warn {
            return;
        }
        if is_counting_suppressed() {
            return;
        }
        let truncated = msg.to_string();
        if is_ws_transient_event(&truncated) {
            defer_entry(
                ws,
                PendingWsEntry {
                    ts,
                    level,
                    message: truncated,
                },
            );
        } else {
            counters.commit(ts, level, truncated);
        }
    }

    fn snap_counts(counters: &Counters, ws: &WsDeferState, now: i64) -> (u64, u64) {
        flush_expired_pending(ws, counters, now);
        counters.rolling_counts(now)
    }

    fn make() -> (Arc<Counters>, Arc<WsDeferState>) {
        (Arc::new(Counters::new()), Arc::new(WsDeferState::new()))
    }

    // bot-strategy#261: WS reset that auto-recovers within
    // WS_DEFER_WINDOW_SECS must NOT inflate the rolling counter (#260
    // was the trigger case — single Lighter WS RST produced 2 ERROR + 2
    // WARN, tripping auto-error workflow despite 27s clean reconnect).

    #[test]
    fn ws_reset_with_recovery_within_60s_is_suppressed() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 1_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            &ws,
            t0,
            Level::Error,
            "WebSocket IO error detail: kind=ConnectionReset, error=Connection reset by peer",
        );
        fake_log(&c, &ws, t0 + 18, Level::Warn, "[XVENUE] tick error: read_mid Lighter\n\nCaused by:\n    get_order_book(ETH, 1): Other(\"order book snapshot unavailable (no recent update)\")");
        fake_log(
            &c,
            &ws,
            t0 + 23,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        fake_log(
            &c,
            &ws,
            t0 + 27,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        fake_log(
            &c,
            &ws,
            t0 + 27,
            Level::Info,
            "WebSocket subscriptions sent successfully",
        );
        assert!(
            pending_is_empty(&ws),
            "recovery within window must drain pending WS entries"
        );
        let (e, w) = snap_counts(&c, &ws, t0 + 30);
        assert_eq!(e, 0, "transient WS errors must not commit");
        assert_eq!(w, 0, "transient WS warns must not commit");
    }

    #[test]
    fn ws_reset_without_recovery_commits_after_deadline() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 2_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            &ws,
            t0 + 5,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        let (e0, w0) = snap_counts(&c, &ws, t0 + 30);
        assert_eq!((e0, w0), (0, 0), "pre-deadline must not commit");
        let (e1, w1) = snap_counts(&c, &ws, t0 + WS_DEFER_WINDOW_SECS + 10);
        assert_eq!(e1, 1, "post-deadline ERROR commits");
        assert_eq!(w1, 1, "post-deadline WARN commits");
    }

    #[test]
    fn ws_reset_with_late_recovery_does_not_uncommit() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 3_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer",
        );
        let (e0, _) = snap_counts(&c, &ws, t0 + WS_DEFER_WINDOW_SECS + 1);
        assert_eq!(e0, 1, "post-deadline ERROR commits");
        fake_log(
            &c,
            &ws,
            t0 + 120,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        let (e1, _) = snap_counts(&c, &ws, t0 + 130);
        assert_eq!(e1, 1, "late recovery cannot uncommit");
    }

    // bot-strategy#343: ws_reset_24h_count replaces the dashboard's old
    // journalctl `Connection reset without closing handshake` SSM probe.

    #[test]
    fn ws_reset_24h_counts_matching_substring() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 12_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Warn,
            "orderbook stream error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=orderbook BTC)",
        );
        fake_log(
            &c,
            &ws,
            t0 + 1,
            Level::Warn,
            "public trades stream error: Connection reset without closing handshake",
        );
        fake_log(
            &c,
            &ws,
            t0 + 2,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        assert_eq!(ws_reset_count_at(&ws, t0 + 5), 2);
    }

    #[test]
    fn ws_reset_24h_expires_old_entries() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 13_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Warn,
            "Connection reset without closing handshake (1)",
        );
        fake_log(
            &c,
            &ws,
            t0 + 100,
            Level::Warn,
            "Connection reset without closing handshake (2)",
        );
        let now = t0 + WS_RESET_24H_WINDOW_SECS - 10;
        assert_eq!(ws_reset_count_at(&ws, now), 2);
        let now = t0 + 100 + WS_RESET_24H_WINDOW_SECS + 10;
        assert_eq!(ws_reset_count_at(&ws, now), 0);
    }

    #[test]
    fn ws_reset_24h_counts_independently_of_suppression() {
        let _g = _serialize();
        let (c, ws) = make();
        set_counting_suppressed(true);
        let t0 = 14_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Warn,
            "Connection reset without closing handshake",
        );
        let (e, w) = snap_counts(&c, &ws, t0 + 5);
        assert_eq!((e, w), (0, 0));
        assert_eq!(ws_reset_count_at(&ws, t0 + 5), 1);
        set_counting_suppressed(false);
    }

    #[test]
    fn ws_reset_24h_ignores_non_matching_phrase() {
        let _g = _serialize();
        let (c, ws) = make();
        let t0 = 15_000_000;
        fake_log(
            &c,
            &ws,
            t0,
            Level::Warn,
            "Connection reset by peer (os error 104)",
        );
        fake_log(&c, &ws, t0 + 1, Level::Warn, "WebSocket reset");
        fake_log(
            &c,
            &ws,
            t0 + 2,
            Level::Warn,
            "Connection reset without graceful close",
        );
        assert_eq!(ws_reset_count_at(&ws, t0 + 5), 0);
    }
}
