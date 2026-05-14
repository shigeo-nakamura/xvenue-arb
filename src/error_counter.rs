//! Error / warn counter layered over an inner `log::Log`.
//!
//! Used by the Status Dashboard to surface log-level anomalies that don't
//! cause the service to fail (see bot-strategy#45). Counters are maintained
//! in-process; snapshot via [`ErrorCounterHandle::snapshot`] and embed in
//! `status.json`.
//!
//! WebSocket transient-event deferral (bot-strategy#261) and the 24h
//! ws-reset counter (bot-strategy#343) live alongside in
//! [`crate::ws_event_defer`]. This module owns only the durable rolling +
//! lifetime counters and the maintenance-mode suppression flag; it calls
//! into `ws_event_defer` to (a) skip transient WS events that auto-recover
//! and (b) commit aged-out pending entries via [`Counters::commit_batch`].
//! Split out per bot-strategy#383.

use log::{Level, Log, Metadata, Record};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::ws_event_defer::{self, WsDeferState};

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

pub(crate) struct Counters {
    recent: Mutex<VecDeque<(i64, Level)>>,
    last_error: Mutex<Option<(i64, String)>>,
    last_warn: Mutex<Option<(i64, String)>>,
    error_total: AtomicU64,
    warn_total: AtomicU64,
}

impl Counters {
    pub(crate) fn new() -> Self {
        Self {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
        }
    }

    /// Commit a single non-deferred entry. Shared by the logger hot path
    /// and the WS-defer flush path (single-entry case).
    pub(crate) fn commit(&self, ts: i64, level: Level, message: String) {
        self.recent.lock().unwrap().push_back((ts, level));
        match level {
            Level::Error => {
                self.error_total.fetch_add(1, Ordering::Relaxed);
                *self.last_error.lock().unwrap() = Some((ts, message));
            }
            Level::Warn => {
                self.warn_total.fetch_add(1, Ordering::Relaxed);
                *self.last_warn.lock().unwrap() = Some((ts, message));
            }
            _ => {}
        }
    }

    /// Commit a batch of aged-out pending WS entries. Holds `recent`
    /// across all pushes, then drops it before touching last_error /
    /// last_warn — matching the lock-ordering of the original monolithic
    /// `flush_expired_pending_ws`.
    pub(crate) fn commit_batch<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (i64, Level, String)>,
    {
        let entries: Vec<(i64, Level, String)> = entries.into_iter().collect();
        if entries.is_empty() {
            return;
        }
        {
            let mut recent = self.recent.lock().unwrap();
            for (ts, level, _) in &entries {
                recent.push_back((*ts, *level));
            }
        }
        for (ts, level, message) in entries {
            match level {
                Level::Error => {
                    self.error_total.fetch_add(1, Ordering::Relaxed);
                    *self.last_error.lock().unwrap() = Some((ts, message));
                }
                Level::Warn => {
                    self.warn_total.fetch_add(1, Ordering::Relaxed);
                    *self.last_warn.lock().unwrap() = Some((ts, message));
                }
                _ => {}
            }
        }
    }

    /// Read the rolling 30m error/warn counts. Prunes expired entries
    /// from `recent` as a side effect — matches the original `snapshot`
    /// behaviour.
    pub(crate) fn rolling_counts(&self, now: i64) -> (u64, u64) {
        let cutoff = now - ROLLING_WINDOW_SECS;
        let mut recent = self.recent.lock().unwrap();
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
    }
}

#[derive(Clone)]
pub struct ErrorCounterHandle {
    counters: Arc<Counters>,
    ws_state: Arc<WsDeferState>,
}

impl ErrorCounterHandle {
    /// Read the 24h ws-reset count without mutating any error/warn
    /// counter. Used by `StatusReporter` to populate
    /// `ws_reset_24h_count` (bot-strategy#343). Prunes expired
    /// timestamps as a side effect.
    pub fn ws_reset_24h_count(&self) -> u64 {
        ws_event_defer::ws_reset_24h_count(&self.ws_state)
    }

    pub fn snapshot(&self) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        // Flush any pending WS-defer entries whose recovery window has
        // expired. Lock order: pending_ws → recent → last_error/last_warn,
        // matching the order in `log()` so no deadlock is possible.
        ws_event_defer::flush_expired_pending(&self.ws_state, &self.counters, now);
        let (err_window, warn_window) = self.counters.rolling_counts(now);
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
    ws_state: Arc<WsDeferState>,
    inner: Box<dyn Log>,
}

impl ErrorCountingLogger {
    pub fn wrap(inner: Box<dyn Log>) -> (Self, ErrorCounterHandle) {
        let counters = Arc::new(Counters::new());
        let ws_state = Arc::new(WsDeferState::new());
        let handle = ErrorCounterHandle {
            counters: Arc::clone(&counters),
            ws_state: Arc::clone(&ws_state),
        };
        (
            Self {
                counters,
                ws_state,
                inner,
            },
            handle,
        )
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
            // 24h ws-reset counter (#343). Counted independently of the
            // pending-WS defer machinery so the dashboard sees the same
            // volume the journalctl probe produced. Suppression
            // (maintenance mode) does NOT mask ws_reset volume.
            ws_event_defer::record_ws_reset_if_match(&self.ws_state, ts, &msg);
            // Recovery markers fire at INFO; check before the level gate so
            // they can drain pending entries from any preceding WS reset.
            if ws_event_defer::is_ws_recovery_event(&msg) {
                ws_event_defer::drain_on_recovery(&self.ws_state, ts);
            }
            // Always flush any pending entries whose deadline passed before
            // we count anything new — keeps the counter monotone in real
            // time even when snapshot() isn't being called (e.g. dashboard
            // poll lag).
            ws_event_defer::flush_expired_pending(&self.ws_state, &self.counters, ts);
            let level = record.level();
            if (level == Level::Error || level == Level::Warn) && !is_counting_suppressed() {
                let truncated = if msg.chars().count() > LAST_ERROR_MAX_CHARS {
                    msg.chars().take(LAST_ERROR_MAX_CHARS).collect::<String>() + "…"
                } else {
                    msg
                };
                if ws_event_defer::is_ws_transient_event(&truncated) {
                    ws_event_defer::defer_entry(
                        &self.ws_state,
                        ws_event_defer::PendingWsEntry {
                            ts,
                            level,
                            message: truncated,
                        },
                    );
                } else {
                    self.counters.commit(ts, level, truncated);
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

    fn _serialize() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| StdMutex::new(()));
        match m.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn non_ws_error_commits_immediately() {
        let _g = _serialize();
        let c = Counters::new();
        let t0 = 4_000_000;
        c.commit(t0, Level::Error, "Some other ERROR unrelated to WS".into());
        let (e, _) = c.rolling_counts(t0 + 1);
        assert_eq!(e, 1, "non-WS errors must not be deferred");
    }

    /// While `set_counting_suppressed(true)` is in effect (e.g. Extended is
    /// in maintenance), warn/error log lines must not bump the rolling
    /// counters even though they still flow to the inner logger. The
    /// suppression check is a thin gate inside `ErrorCountingLogger::log`,
    /// so this test asserts the gate logic directly against the same
    /// public flag. Mirrors pairtrade's coverage at
    /// `error_counter.rs::tests`. bot-strategy#321.
    #[test]
    fn suppress_counting_blocks_increments() {
        let _g = _serialize();
        let c = Counters::new();
        let t0 = 5_000_000;
        // Baseline: errors count without the flag.
        if !is_counting_suppressed() {
            c.commit(
                t0,
                Level::Error,
                "Some venue ERROR before maintenance".into(),
            );
        }
        let (e0, _) = c.rolling_counts(t0 + 1);
        assert_eq!(e0, 1);

        set_counting_suppressed(true);
        // Two maintenance-mode lines that the live `log()` path would
        // skip because the suppression gate fires before the commit. The
        // counter side never sees them.
        if !is_counting_suppressed() {
            c.commit(t0 + 5, Level::Error, "Maintenance mode".into());
        }
        if !is_counting_suppressed() {
            c.commit(t0 + 6, Level::Warn, "Maintenance mode".into());
        }
        let (e1, w1) = c.rolling_counts(t0 + 10);
        assert_eq!(e1, 1, "ERROR must not increment while suppressed");
        assert_eq!(w1, 0, "WARN must not increment while suppressed");

        // Lifting the flag re-arms counting.
        set_counting_suppressed(false);
        if !is_counting_suppressed() {
            c.commit(t0 + 11, Level::Error, "Post-maintenance error".into());
        }
        let (e2, _) = c.rolling_counts(t0 + 12);
        assert_eq!(e2, 2, "post-flag-clear errors count again");
    }

    #[test]
    fn commit_batch_preserves_order_and_totals() {
        let c = Counters::new();
        let t0 = 6_000_000;
        c.commit_batch([
            (t0, Level::Error, "e1".to_string()),
            (t0 + 1, Level::Warn, "w1".to_string()),
            (t0 + 2, Level::Error, "e2".to_string()),
        ]);
        let (e, w) = c.rolling_counts(t0 + 5);
        assert_eq!((e, w), (2, 1));
        assert_eq!(c.error_total.load(Ordering::Relaxed), 2);
        assert_eq!(c.warn_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            c.last_error.lock().unwrap().as_ref().unwrap().1,
            "e2",
            "last_error reflects the most recent ERROR in the batch"
        );
        assert_eq!(
            c.last_warn.lock().unwrap().as_ref().unwrap().1,
            "w1",
            "last_warn reflects the most recent WARN in the batch"
        );
    }
}
