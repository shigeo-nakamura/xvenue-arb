//! Error / warn counter layered over an inner `log::Log`.
//!
//! Used by the Status Dashboard to surface log-level anomalies that don't
//! cause the service to fail (see bot-strategy#45). Counters are maintained
//! in-process; snapshot via `ErrorCounterHandle::snapshot()` and embed in
//! `status.json`.

use log::{Level, Log, Metadata, Record};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Process-global counter handle, populated by the binary's logger
/// initialization. Library code (e.g. `StatusReporter`) reads it to
/// include an `error_summary` section in `status.json`.
static GLOBAL_HANDLE: OnceLock<ErrorCounterHandle> = OnceLock::new();

/// When true, `ErrorCountingLogger` stops incrementing warn/error counters
/// and stops updating `last_error` / `last_warn`. Set from the live loop
/// whenever Extended is detected as in/upcoming maintenance: the
/// `Maintenance mode` REST rejections, WS reconnect bursts, and stale-book
/// WARNs that fire while the venue is rejecting requests are expected
/// fallout, not actionable signal, and should not inflate `error_summary`
/// in `status.json`. Log emission to the inner logger (journalctl) is
/// unaffected. Mirrors pairtrade `error_counter::SUPPRESS_COUNTING`.
/// bot-strategy#321.
static SUPPRESS_COUNTING: AtomicBool = AtomicBool::new(false);

pub fn install_global(handle: ErrorCounterHandle) {
    let _ = GLOBAL_HANDLE.set(handle);
}

pub fn global() -> Option<&'static ErrorCounterHandle> {
    GLOBAL_HANDLE.get()
}

pub fn set_counting_suppressed(suppressed: bool) {
    SUPPRESS_COUNTING.store(suppressed, Ordering::Relaxed);
}

pub fn is_counting_suppressed() -> bool {
    SUPPRESS_COUNTING.load(Ordering::Relaxed)
}

/// Window (seconds) for the short-term rolling counts published in the
/// status snapshot. Was 300 until bot-strategy#168: GitHub Actions scheduled
/// runs drift 40–70 min under load so a 5-min window let warns age out
/// between polls. 1800 (30 min) absorbs typical drift.
const ROLLING_WINDOW_SECS: i64 = 1800;

/// Defer-window for transient WebSocket reset events. A WS reset that
/// auto-recovers within this window does not contribute to the rolling
/// counts (see bot-strategy#261). Sized for typical Lighter / Extended
/// reconnect cycles (~5–30s observed); 60s gives headroom for slow
/// reconnects without ageing out a real persistent disconnect.
const WS_DEFER_WINDOW_SECS: i64 = 60;

/// 24h window for the ws-reset counter. Replaces the dashboard's
/// `journalctl ... | awk '/Connection reset.../ {c++}'` SSM probe with
/// a self-reported field in `status.json` (bot-strategy#343). Threshold
/// for alerting is 10/day per #47.
const WS_RESET_24H_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Substring used to identify a WS reset event in the log stream. The
/// dashboard's old journalctl probe matched the same exact phrase, so
/// the bot-self-reported counter and the journalctl-derived counter
/// are interchangeable.
const WS_RESET_PHRASE: &str = "Connection reset without closing handshake";

/// Keep the last error message truncated to this many chars so the
/// dashboard can display it without blowing up the JSON payload.
const LAST_ERROR_MAX_CHARS: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSummary {
    pub error_count_30m: u64,
    pub warn_count_30m: u64,
    pub error_count_total: u64,
    pub warn_count_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warn_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warn_message: Option<String>,
}

struct Counters {
    recent: Mutex<VecDeque<(i64, Level)>>,
    last_error: Mutex<Option<(i64, String)>>,
    last_warn: Mutex<Option<(i64, String)>>,
    error_total: AtomicU64,
    warn_total: AtomicU64,
    /// Transient WS-reset events queued for deferred commit. Each entry
    /// stays here until either (a) a recovery log line drains it before
    /// `WS_DEFER_WINDOW_SECS` elapses, or (b) `snapshot()` flushes it into
    /// `recent` once its deadline passes. See bot-strategy#261.
    pending_ws: Mutex<VecDeque<PendingWsEntry>>,
    /// Timestamps (epoch seconds) of WS reset events in the last 24h —
    /// any log line containing `Connection reset without closing
    /// handshake`. Surfaced as `ws_reset_24h_count` in `status.json` so
    /// the dashboard does not need a journalctl SSM probe. See
    /// bot-strategy#343.
    ws_resets_24h: Mutex<VecDeque<i64>>,
}

#[derive(Debug, Clone)]
struct PendingWsEntry {
    ts: i64,
    level: Level,
    message: String,
}

/// Match log lines that signal a transient connectivity event whose effect
/// should be suppressed if the bot recovers within `WS_DEFER_WINDOW_SECS`.
/// Covers (1) the connector ERROR raised by the tungstenite WS layer when
/// the upstream RST-resets, and (2) the WARN downstream of that — the
/// xvenue-arb tick error and the pairtrade orderbook-stale signals — that
/// fire while the reconnect is in progress.
fn is_ws_transient_event(msg: &str) -> bool {
    msg.starts_with("WebSocket error:")
        || msg.starts_with("WebSocket IO error detail:")
        || msg.contains("tick error: read_mid")
        || msg.contains("order book snapshot unavailable")
        || msg.contains("waiting for websocket data")
}

/// Match log lines that signal a successful WS reconnect. Drains pending
/// transient entries logged within the past `WS_DEFER_WINDOW_SECS`.
fn is_ws_recovery_event(msg: &str) -> bool {
    msg.starts_with("WebSocket connected successfully")
        || msg.contains("WebSocket subscriptions sent successfully")
}

#[derive(Clone)]
pub struct ErrorCounterHandle {
    counters: Arc<Counters>,
}

impl ErrorCounterHandle {
    /// Read the 24h ws-reset count without mutating any other counter.
    /// Used by `StatusReporter` to populate `ws_reset_24h_count`
    /// (bot-strategy#343). Prunes expired timestamps as a side effect.
    pub fn ws_reset_24h_count(&self) -> u64 {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - WS_RESET_24H_WINDOW_SECS;
        let mut q = self.counters.ws_resets_24h.lock().unwrap();
        while let Some(&front) = q.front() {
            if front < cutoff {
                q.pop_front();
            } else {
                break;
            }
        }
        q.len() as u64
    }

    pub fn snapshot(&self) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        // Flush any pending WS-defer entries whose recovery window has
        // expired. Lock order: pending_ws → recent → last_error/last_warn,
        // matching the order in `log()` so no deadlock is possible.
        flush_expired_pending_ws(&self.counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let (err_window, warn_window) = {
            let mut recent = self.counters.recent.lock().unwrap();
            while let Some(&(ts, _)) = recent.front() {
                if ts < cutoff {
                    recent.pop_front();
                } else {
                    break;
                }
            }
            let mut e = 0u64;
            let mut w = 0u64;
            for (_, lvl) in recent.iter() {
                match lvl {
                    Level::Error => e += 1,
                    Level::Warn => w += 1,
                    _ => {}
                }
            }
            (e, w)
        };
        let (last_err_ts, last_err_msg) = match self.counters.last_error.lock().unwrap().clone() {
            Some((ts, msg)) => (Some(ts), Some(msg)),
            None => (None, None),
        };
        let (last_warn_ts, last_warn_msg) = match self.counters.last_warn.lock().unwrap().clone() {
            Some((ts, msg)) => (Some(ts), Some(msg)),
            None => (None, None),
        };
        ErrorSummary {
            error_count_30m: err_window,
            warn_count_30m: warn_window,
            error_count_total: self.counters.error_total.load(Ordering::Relaxed),
            warn_count_total: self.counters.warn_total.load(Ordering::Relaxed),
            last_error_ts: last_err_ts,
            last_error_message: last_err_msg,
            last_warn_ts,
            last_warn_message: last_warn_msg,
        }
    }
}

pub struct ErrorCountingLogger {
    counters: Arc<Counters>,
    inner: Box<dyn Log>,
}

impl ErrorCountingLogger {
    pub fn wrap(inner: Box<dyn Log>) -> (Self, ErrorCounterHandle) {
        let counters = Arc::new(Counters {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
            pending_ws: Mutex::new(VecDeque::new()),
            ws_resets_24h: Mutex::new(VecDeque::new()),
        });
        let handle = ErrorCounterHandle {
            counters: Arc::clone(&counters),
        };
        (Self { counters, inner }, handle)
    }
}

/// Move pending WS-defer entries whose recovery window has expired into
/// the durable `recent` queue (and update last_error/last_warn + totals).
/// Called from both `snapshot()` and `log()` so the counts stay current
/// regardless of whether the dashboard is polling.
fn flush_expired_pending_ws(counters: &Counters, now: i64) {
    let cutoff = now - WS_DEFER_WINDOW_SECS;
    let mut pending = counters.pending_ws.lock().unwrap();
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
    let mut recent = counters.recent.lock().unwrap();
    for entry in &to_commit {
        recent.push_back((entry.ts, entry.level));
    }
    drop(recent);
    for entry in to_commit {
        match entry.level {
            Level::Error => {
                counters.error_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_error.lock().unwrap() = Some((entry.ts, entry.message));
            }
            Level::Warn => {
                counters.warn_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_warn.lock().unwrap() = Some((entry.ts, entry.message));
            }
            _ => {}
        }
    }
}

impl Log for ErrorCountingLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let ts = chrono::Utc::now().timestamp();
            let msg = record.args().to_string();
            // 24h ws-reset counter (#343). See pairtrade's
            // `error_counter.rs::log` for the reasoning — we count
            // independently of the pending-WS defer machinery so the
            // dashboard sees the same volume the journalctl probe
            // produced. Suppression (maintenance mode) does NOT mask
            // ws_reset volume.
            if msg.contains(WS_RESET_PHRASE) {
                let cutoff = ts - WS_RESET_24H_WINDOW_SECS;
                let mut q = self.counters.ws_resets_24h.lock().unwrap();
                while let Some(&front) = q.front() {
                    if front < cutoff {
                        q.pop_front();
                    } else {
                        break;
                    }
                }
                q.push_back(ts);
            }
            // Recovery markers fire at INFO; check before the level gate so
            // they can drain pending entries from any preceding WS reset.
            if is_ws_recovery_event(&msg) {
                let cutoff = ts - WS_DEFER_WINDOW_SECS;
                self.counters
                    .pending_ws
                    .lock()
                    .unwrap()
                    .retain(|e| e.ts < cutoff);
            }
            // Always flush any pending entries whose deadline passed before
            // we count anything new — keeps the counter monotone in real
            // time even when snapshot() isn't being called (e.g. dashboard
            // poll lag).
            flush_expired_pending_ws(&self.counters, ts);
            let level = record.level();
            if (level == Level::Error || level == Level::Warn) && !is_counting_suppressed() {
                let truncated = if msg.chars().count() > LAST_ERROR_MAX_CHARS {
                    msg.chars().take(LAST_ERROR_MAX_CHARS).collect::<String>() + "…"
                } else {
                    msg
                };
                if is_ws_transient_event(&truncated) {
                    // Defer: held in pending_ws until either drained by a
                    // recovery marker or expired by flush_expired_pending_ws.
                    self.counters
                        .pending_ws
                        .lock()
                        .unwrap()
                        .push_back(PendingWsEntry {
                            ts,
                            level,
                            message: truncated,
                        });
                } else {
                    self.counters.recent.lock().unwrap().push_back((ts, level));
                    if level == Level::Error {
                        self.counters.error_total.fetch_add(1, Ordering::Relaxed);
                        *self.counters.last_error.lock().unwrap() = Some((ts, truncated));
                    } else {
                        self.counters.warn_total.fetch_add(1, Ordering::Relaxed);
                        *self.counters.last_warn.lock().unwrap() = Some((ts, truncated));
                    }
                }
            }
        }
        self.inner.log(record);
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn make_counters() -> Arc<Counters> {
        Arc::new(Counters {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
            pending_ws: Mutex::new(VecDeque::new()),
            ws_resets_24h: Mutex::new(VecDeque::new()),
        })
    }

    fn fake_log(counters: &Counters, ts: i64, level: Level, msg: &str) {
        if msg.contains(WS_RESET_PHRASE) {
            let cutoff = ts - WS_RESET_24H_WINDOW_SECS;
            let mut q = counters.ws_resets_24h.lock().unwrap();
            while let Some(&front) = q.front() {
                if front < cutoff {
                    q.pop_front();
                } else {
                    break;
                }
            }
            q.push_back(ts);
        }
        if is_ws_recovery_event(msg) {
            let cutoff = ts - WS_DEFER_WINDOW_SECS;
            counters
                .pending_ws
                .lock()
                .unwrap()
                .retain(|e| e.ts < cutoff);
        }
        flush_expired_pending_ws(counters, ts);
        if level != Level::Error && level != Level::Warn {
            return;
        }
        if is_counting_suppressed() {
            return;
        }
        let truncated = msg.to_string();
        if is_ws_transient_event(&truncated) {
            counters
                .pending_ws
                .lock()
                .unwrap()
                .push_back(PendingWsEntry {
                    ts,
                    level,
                    message: truncated,
                });
        } else {
            counters.recent.lock().unwrap().push_back((ts, level));
            if level == Level::Error {
                counters.error_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_error.lock().unwrap() = Some((ts, truncated));
            } else {
                counters.warn_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_warn.lock().unwrap() = Some((ts, truncated));
            }
        }
    }

    fn snap_counts(counters: &Counters, now: i64) -> (u64, u64) {
        flush_expired_pending_ws(counters, now);
        let recent = counters.recent.lock().unwrap();
        let cutoff = now - ROLLING_WINDOW_SECS;
        let mut e = 0u64;
        let mut w = 0u64;
        for &(ts, lvl) in recent.iter() {
            if ts < cutoff {
                continue;
            }
            match lvl {
                Level::Error => e += 1,
                Level::Warn => w += 1,
                _ => {}
            }
        }
        (e, w)
    }

    fn _serialize() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| StdMutex::new(()));
        match m.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    // bot-strategy#261: WS reset that auto-recovers within
    // WS_DEFER_WINDOW_SECS must NOT inflate the rolling counter (#260
    // was the trigger case — single Lighter WS RST produced 2 ERROR + 2
    // WARN, tripping auto-error workflow despite 27s clean reconnect).

    #[test]
    fn ws_reset_with_recovery_within_60s_is_suppressed() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 1_000_000;
        // Simulate the #260 sequence verbatim (~27s window).
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket IO error detail: kind=ConnectionReset, error=Connection reset by peer",
        );
        fake_log(&c, t0 + 18, Level::Warn, "[XVENUE] tick error: read_mid Lighter\n\nCaused by:\n    get_order_book(ETH, 1): Other(\"order book snapshot unavailable (no recent update)\")");
        fake_log(
            &c,
            t0 + 23,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        fake_log(
            &c,
            t0 + 27,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        fake_log(
            &c,
            t0 + 27,
            Level::Info,
            "WebSocket subscriptions sent successfully",
        );
        assert!(
            c.pending_ws.lock().unwrap().is_empty(),
            "recovery within window must drain pending WS entries"
        );
        let (e, w) = snap_counts(&c, t0 + 30);
        assert_eq!(e, 0, "transient WS errors must not commit");
        assert_eq!(w, 0, "transient WS warns must not commit");
    }

    #[test]
    fn ws_reset_without_recovery_commits_after_deadline() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 2_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            t0 + 5,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        let (e0, w0) = snap_counts(&c, t0 + 30);
        assert_eq!((e0, w0), (0, 0), "pre-deadline must not commit");
        let (e1, w1) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 10);
        assert_eq!(e1, 1, "post-deadline ERROR commits");
        assert_eq!(w1, 1, "post-deadline WARN commits");
    }

    #[test]
    fn ws_reset_with_late_recovery_does_not_uncommit() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 3_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer",
        );
        let (e0, _) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 1);
        assert_eq!(e0, 1, "post-deadline ERROR commits");
        fake_log(
            &c,
            t0 + 120,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        let (e1, _) = snap_counts(&c, t0 + 130);
        assert_eq!(e1, 1, "late recovery cannot uncommit");
    }

    #[test]
    fn non_ws_error_commits_immediately() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 4_000_000;
        fake_log(&c, t0, Level::Error, "Some other ERROR unrelated to WS");
        let (e, _) = snap_counts(&c, t0 + 1);
        assert_eq!(e, 1, "non-WS errors must not be deferred");
    }

    /// While `set_counting_suppressed(true)` is in effect (e.g. Extended is
    /// in maintenance), warn/error log lines must not bump the rolling
    /// counters even though they still flow to the inner logger. Mirrors
    /// pairtrade's coverage at `error_counter.rs::tests`. bot-strategy#321.
    #[test]
    fn suppress_counting_blocks_increments() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 5_000_000;
        // Baseline: errors count without the flag.
        fake_log(&c, t0, Level::Error, "Some venue ERROR before maintenance");
        let (e0, _) = snap_counts(&c, t0 + 1);
        assert_eq!(e0, 1);

        set_counting_suppressed(true);
        fake_log(
            &c,
            t0 + 5,
            Level::Error,
            "[UNHEDGED] BTC/ETH close failed err=ServerResponse(\"Maintenance mode\")",
        );
        fake_log(&c, t0 + 6, Level::Warn, "[XVENUE/extmaker] place_taker err=create_order_taker_ioc ETH-USD: Server response error: Maintenance mode");
        let (e1, w1) = snap_counts(&c, t0 + 10);
        assert_eq!(e1, 1, "ERROR must not increment while suppressed");
        assert_eq!(w1, 0, "WARN must not increment while suppressed");

        // Lifting the flag re-arms counting.
        set_counting_suppressed(false);
        fake_log(&c, t0 + 11, Level::Error, "Post-maintenance error");
        let (e2, _) = snap_counts(&c, t0 + 12);
        assert_eq!(e2, 2, "post-flag-clear errors count again");
    }

    // bot-strategy#343: ws_reset_24h_count replaces the dashboard's old
    // journalctl `Connection reset without closing handshake` SSM probe.

    fn ws_reset_count(counters: &Counters, now: i64) -> u64 {
        let cutoff = now - WS_RESET_24H_WINDOW_SECS;
        let mut q = counters.ws_resets_24h.lock().unwrap();
        while let Some(&front) = q.front() {
            if front < cutoff {
                q.pop_front();
            } else {
                break;
            }
        }
        q.len() as u64
    }

    #[test]
    fn ws_reset_24h_counts_matching_substring() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 12_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "orderbook stream error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=orderbook BTC)",
        );
        fake_log(
            &c,
            t0 + 1,
            Level::Warn,
            "public trades stream error: Connection reset without closing handshake",
        );
        fake_log(&c, t0 + 2, Level::Warn, "[XVENUE] tick error: read_mid Lighter");
        assert_eq!(ws_reset_count(&c, t0 + 5), 2);
    }

    #[test]
    fn ws_reset_24h_expires_old_entries() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 13_000_000;
        fake_log(&c, t0, Level::Warn, "Connection reset without closing handshake (1)");
        fake_log(&c, t0 + 100, Level::Warn, "Connection reset without closing handshake (2)");
        let now = t0 + WS_RESET_24H_WINDOW_SECS - 10;
        assert_eq!(ws_reset_count(&c, now), 2);
        let now = t0 + 100 + WS_RESET_24H_WINDOW_SECS + 10;
        assert_eq!(ws_reset_count(&c, now), 0);
    }

    #[test]
    fn ws_reset_24h_counts_independently_of_suppression() {
        let _g = _serialize();
        let c = make_counters();
        set_counting_suppressed(true);
        let t0 = 14_000_000;
        fake_log(&c, t0, Level::Warn, "Connection reset without closing handshake");
        let (e, w) = snap_counts(&c, t0 + 5);
        assert_eq!((e, w), (0, 0));
        assert_eq!(ws_reset_count(&c, t0 + 5), 1);
        set_counting_suppressed(false);
    }

    #[test]
    fn ws_reset_24h_ignores_non_matching_phrase() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 15_000_000;
        fake_log(&c, t0, Level::Warn, "Connection reset by peer (os error 104)");
        fake_log(&c, t0 + 1, Level::Warn, "WebSocket reset");
        fake_log(&c, t0 + 2, Level::Warn, "Connection reset without graceful close");
        assert_eq!(ws_reset_count(&c, t0 + 5), 0);
    }
}
