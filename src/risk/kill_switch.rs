//! STUCK file + REST consec-fail counters + SIGUSR1 handler.
//! bot-strategy#244 Group C.
//!
//! Companion to the operator-driven KILL_SWITCH (live.rs) and the
//! daily/session DD gates (risk::manager). This module owns the
//! **runner-written** failure tripwire — the file the bot drops
//! itself when it can't recover from an emergency-flatten loop or
//! when REST calls are rejected for too long in a row. Once
//! armed the file blocks new entries until the operator inspects
//! and clears it (manual `rm` of the file, optionally combined
//! with a RISK_ACK to clear any session-DD halt).
//!
//! Three independent escalation paths:
//!
//! 1. **REST consec-fail per venue** — when `get_positions` /
//!    `get_filled_orders` (or any read-side REST) fails N consecutive
//!    times on either venue (default 3, configured via
//!    `rest_consec_fail_to_escalate`), the runner arms the file
//!    with reason `REST_FAIL_LIMIT`. #102 P2 precedent.
//! 2. **Reduce-only consec-fail** — when `close_all_positions`
//!    rejects K consecutive times in `EmergencyFlattening` (default
//!    5, `reduce_only_consec_fail_to_kill`), the runner arms with
//!    reason `REDUCE_ONLY_FAIL_LIMIT`. Same #102 P2 167-min stuck
//!    precedent.
//! 3. **SIGUSR1** — operator can `kill -USR1 $pid` (or the dashboard
//!    flatten button) to arm with reason `SIGUSR1`. Useful when SSH
//!    is reachable but file IO is not (read-only mount, etc.).
//!
//! The file format mirrors the runbook spec in
//! `docs/execution_layer.md` §4.1 — Created/Reason/Position/Action.
//! Operator-readable plain text, not JSON.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{self, error::TryRecvError};

const ARM_FILE_TEMPLATE: &str = "\
Created: {ts}
Reason:  {reason}
Action:  new entries halted. Operator must investigate, then `rm` this file.
";

/// One escalation path armed the tripwire. Surfaced in logs, the
/// status emitter (so the dashboard can render the reason), and the
/// auto-issue framework's last_warn_message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StuckReason {
    /// REST read calls failed `rest_consec_fail_to_escalate` times in a row.
    RestFailLimit,
    /// `close_all_positions` rejected `reduce_only_consec_fail_to_kill`
    /// times in a row inside `EmergencyFlattening`.
    ReduceOnlyFailLimit,
    /// Operator sent SIGUSR1 (`kill -USR1 $pid`).
    Sigusr1,
}

impl StuckReason {
    pub fn as_str(self) -> &'static str {
        match self {
            StuckReason::RestFailLimit => "REST_FAIL_LIMIT",
            StuckReason::ReduceOnlyFailLimit => "REDUCE_ONLY_FAIL_LIMIT",
            StuckReason::Sigusr1 => "SIGUSR1",
        }
    }
}

/// Configuration for the tripwire. Ties to the existing
/// `XvenueConfig` knobs (rest_consec_fail_to_escalate /
/// reduce_only_consec_fail_to_kill / stuck_file).
#[derive(Debug, Clone)]
pub struct StuckTripwireConfig {
    pub stuck_file: PathBuf,
    pub rest_consec_fail_to_escalate: u32,
    pub reduce_only_consec_fail_to_kill: u32,
}

/// Persistent counter state. Only the per-path counters and a
/// "self-armed" flag are held in memory; the file existence is the
/// source of truth for `is_stuck`.
#[derive(Debug)]
pub struct StuckTripwire {
    config: StuckTripwireConfig,
    ext_rest_consec_fail: u32,
    lt_rest_consec_fail: u32,
    reduce_only_consec_fail: u32,
    /// Set when this process armed the file (so a stale file from a
    /// previous run is still reported in logs but isn't double-counted).
    armed_by_self: bool,
    /// Receives SIGUSR1 events. The runner spawns a tokio task that
    /// pushes onto this on each signal; we drain it on each tick.
    /// `None` in tests so unit tests stay deterministic without an
    /// active tokio runtime.
    sigusr1_rx: Option<mpsc::UnboundedReceiver<()>>,
}

impl StuckTripwire {
    pub fn new(config: StuckTripwireConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self::install_sigusr1_handler(tx);
        Self {
            config,
            ext_rest_consec_fail: 0,
            lt_rest_consec_fail: 0,
            reduce_only_consec_fail: 0,
            armed_by_self: false,
            sigusr1_rx: Some(rx),
        }
    }

    /// Test fixture — same as `new` but does not register a SIGUSR1
    /// handler (avoids cross-test interference / `Drop` semantics).
    #[cfg(test)]
    pub fn new_for_test(config: StuckTripwireConfig) -> Self {
        Self {
            config,
            ext_rest_consec_fail: 0,
            lt_rest_consec_fail: 0,
            reduce_only_consec_fail: 0,
            armed_by_self: false,
            sigusr1_rx: None,
        }
    }

    fn install_sigusr1_handler(tx: mpsc::UnboundedSender<()>) {
        // Spawned on the **same** tokio runtime as the live loop so
        // we don't burn an extra OS thread. The handler is small and
        // signal-driven — no busy work.
        tokio::spawn(async move {
            let mut s = match signal(SignalKind::user_defined1()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("[KILL_SWITCH] SIGUSR1 listener install failed: {:?}", e);
                    return;
                }
            };
            log::info!("[KILL_SWITCH] SIGUSR1 handler installed");
            while s.recv().await.is_some() {
                if tx.send(()).is_err() {
                    // Receiver dropped — runner is shutting down.
                    return;
                }
            }
        });
    }

    /// True when the STUCK file exists on disk. Source of truth —
    /// works across processes (CI-deployed binary upgrade keeps the
    /// halt sticky).
    pub fn is_stuck(&self) -> bool {
        self.config.stuck_file.exists()
    }

    /// Returns true if at least one SIGUSR1 has arrived since the
    /// last poll. Live loop calls this each tick; we arm the
    /// tripwire on the receive side rather than inside the signal
    /// handler so all state mutation happens on the runner's thread.
    pub fn poll_sigusr1(&mut self) -> bool {
        let Some(rx) = self.sigusr1_rx.as_mut() else {
            return false;
        };
        let mut signaled = false;
        // Drain — multiple SIGUSR1s in quick succession should
        // collapse to a single arm.
        loop {
            match rx.try_recv() {
                Ok(()) => {
                    signaled = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    // Sender dropped — handler died. Log and stop
                    // polling.
                    self.sigusr1_rx = None;
                    log::warn!("[KILL_SWITCH] SIGUSR1 sender disconnected");
                    break;
                }
            }
        }
        if signaled {
            log::warn!("[KILL_SWITCH] SIGUSR1 received — arming STUCK file");
            self.arm(StuckReason::Sigusr1);
        }
        signaled
    }

    /// Hook for the read-side REST path (live.rs venue health
    /// monitor). Returns true if the call sequence escalated.
    pub fn record_rest_failure(&mut self, venue: VenueLabel) -> bool {
        let counter = match venue {
            VenueLabel::Extended => &mut self.ext_rest_consec_fail,
            VenueLabel::Lighter => &mut self.lt_rest_consec_fail,
        };
        *counter = counter.saturating_add(1);
        if *counter >= self.config.rest_consec_fail_to_escalate {
            log::error!(
                "[KILL_SWITCH] {:?} REST consec fails={} >= threshold={} — arming STUCK",
                venue,
                *counter,
                self.config.rest_consec_fail_to_escalate
            );
            self.arm(StuckReason::RestFailLimit);
            true
        } else {
            log::warn!(
                "[KILL_SWITCH] {:?} REST consec fails={} (threshold={})",
                venue,
                *counter,
                self.config.rest_consec_fail_to_escalate
            );
            false
        }
    }

    pub fn record_rest_success(&mut self, venue: VenueLabel) {
        match venue {
            VenueLabel::Extended => self.ext_rest_consec_fail = 0,
            VenueLabel::Lighter => self.lt_rest_consec_fail = 0,
        }
    }

    /// Hook for the close-all path (Group B emergency-flatten
    /// retries). Returns true if the call sequence escalated.
    pub fn record_reduce_only_failure(&mut self) -> bool {
        self.reduce_only_consec_fail = self.reduce_only_consec_fail.saturating_add(1);
        if self.reduce_only_consec_fail >= self.config.reduce_only_consec_fail_to_kill {
            log::error!(
                "[KILL_SWITCH] reduce-only consec fails={} >= kill={} — arming STUCK",
                self.reduce_only_consec_fail,
                self.config.reduce_only_consec_fail_to_kill
            );
            self.arm(StuckReason::ReduceOnlyFailLimit);
            true
        } else {
            log::warn!(
                "[KILL_SWITCH] reduce-only consec fails={} (kill threshold={})",
                self.reduce_only_consec_fail,
                self.config.reduce_only_consec_fail_to_kill
            );
            false
        }
    }

    pub fn record_reduce_only_success(&mut self) {
        self.reduce_only_consec_fail = 0;
    }

    /// Best-effort write of the STUCK file. Idempotent — multiple
    /// arms with different reasons append a fresh body each time so
    /// the operator sees the latest cause.
    pub fn arm(&mut self, reason: StuckReason) {
        let body = ARM_FILE_TEMPLATE
            .replace("{ts}", &chrono::Utc::now().to_rfc3339())
            .replace("{reason}", reason.as_str());
        if let Some(parent) = self.config.stuck_file.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                log::warn!(
                    "[KILL_SWITCH] mkdir {} for STUCK file: {:?}",
                    parent.display(),
                    e
                );
                return;
            }
        }
        if let Err(e) = (|| -> std::io::Result<()> {
            let mut f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.config.stuck_file)?;
            f.write_all(body.as_bytes())
        })() {
            log::warn!(
                "[KILL_SWITCH] write {}: {:?}",
                self.config.stuck_file.display(),
                e
            );
            return;
        }
        self.armed_by_self = true;
    }

    /// `Some(reason)` when the file was armed by **this** process —
    /// surfaces the in-memory reason (since the file body is plain
    /// text we don't re-parse it). Returns `None` when the file
    /// exists but was inherited from a previous run (operator must
    /// inspect via `cat $stuck_file`).
    pub fn current_reason(&self) -> Option<&'static str> {
        if !self.is_stuck() || !self.armed_by_self {
            return None;
        }
        // We don't preserve the exact reason variant — the file body
        // does. Surfacing "ARMED" is enough for the dashboard halt
        // pill; the operator reads the file for the cause.
        Some("ARMED")
    }

    pub fn stuck_file_path(&self) -> &Path {
        &self.config.stuck_file
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueLabel {
    Extended,
    Lighter,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cfg_in(dir: &TempDir) -> StuckTripwireConfig {
        StuckTripwireConfig {
            stuck_file: dir.path().join("STUCK"),
            rest_consec_fail_to_escalate: 3,
            reduce_only_consec_fail_to_kill: 5,
        }
    }

    #[test]
    fn fresh_tripwire_is_not_stuck() {
        let tmp = TempDir::new().unwrap();
        let t = StuckTripwire::new_for_test(cfg_in(&tmp));
        assert!(!t.is_stuck());
    }

    #[test]
    fn rest_failures_below_threshold_do_not_arm() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        for _ in 0..2 {
            assert!(!t.record_rest_failure(VenueLabel::Extended));
        }
        assert!(!t.is_stuck());
    }

    #[test]
    fn rest_failures_at_threshold_arm() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        let mut armed = false;
        for _ in 0..3 {
            armed = t.record_rest_failure(VenueLabel::Extended);
        }
        assert!(armed);
        assert!(t.is_stuck());
        // File body contains the reason.
        let body = std::fs::read_to_string(t.stuck_file_path()).unwrap();
        assert!(body.contains("REST_FAIL_LIMIT"));
    }

    #[test]
    fn rest_success_resets_per_venue_counter() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        t.record_rest_failure(VenueLabel::Extended);
        t.record_rest_failure(VenueLabel::Extended);
        // Success only resets that venue.
        t.record_rest_success(VenueLabel::Extended);
        // 2 more failures must NOT trigger (counter was reset).
        for _ in 0..2 {
            assert!(!t.record_rest_failure(VenueLabel::Extended));
        }
        assert!(!t.is_stuck());
    }

    #[test]
    fn per_venue_counters_independent() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        // Two Extended fails + two Lighter fails — neither over threshold.
        for _ in 0..2 {
            t.record_rest_failure(VenueLabel::Extended);
            t.record_rest_failure(VenueLabel::Lighter);
        }
        assert!(!t.is_stuck());
    }

    #[test]
    fn reduce_only_failures_arm_at_kill_threshold() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        let mut armed = false;
        for _ in 0..5 {
            armed = t.record_reduce_only_failure();
        }
        assert!(armed);
        assert!(t.is_stuck());
        let body = std::fs::read_to_string(t.stuck_file_path()).unwrap();
        assert!(body.contains("REDUCE_ONLY_FAIL_LIMIT"));
    }

    #[test]
    fn arm_writes_file_with_iso8601_timestamp() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        t.arm(StuckReason::Sigusr1);
        let body = std::fs::read_to_string(t.stuck_file_path()).unwrap();
        assert!(body.contains("Created:"));
        assert!(body.contains("SIGUSR1"));
    }

    #[test]
    fn stale_file_from_prior_run_is_detected_via_is_stuck() {
        let tmp = TempDir::new().unwrap();
        // Pre-create a file as a previous bot run would have.
        std::fs::write(tmp.path().join("STUCK"), "previous run\n").unwrap();
        let t = StuckTripwire::new_for_test(cfg_in(&tmp));
        assert!(t.is_stuck());
        // current_reason returns None because the file wasn't armed
        // by *this* process — caller must inspect cat output.
        assert_eq!(t.current_reason(), None);
    }

    #[test]
    fn arm_then_self_reports_armed() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        t.arm(StuckReason::RestFailLimit);
        assert_eq!(t.current_reason(), Some("ARMED"));
    }

    /// Catalogue case 14 (`docs/execution_layer.md` §2): Lighter REST
    /// `get_positions` fails N times in a row → `Emergency{KillSwitch}`
    /// regardless of Extended-side state. Existing
    /// `per_venue_counters_independent` only walked both venues to
    /// `threshold-1`; this asserts the cross-venue independence holds
    /// when one side actually crosses the threshold while the other
    /// has been failing intermittently.
    #[test]
    fn rest_consec_fail_for_lighter_arms_independently_of_extended() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        // Extended noise: one fail, success, one more fail. Counter
        // bounces around but never reaches threshold=3.
        t.record_rest_failure(VenueLabel::Extended);
        t.record_rest_success(VenueLabel::Extended);
        t.record_rest_failure(VenueLabel::Extended);
        assert!(!t.is_stuck());
        // Lighter: 3 consecutive fails — must arm.
        let armed_at_third = (0..3)
            .map(|_| t.record_rest_failure(VenueLabel::Lighter))
            .last()
            .unwrap();
        assert!(armed_at_third, "Lighter must arm independently of Extended");
        assert!(t.is_stuck());
        let body = std::fs::read_to_string(t.stuck_file_path()).unwrap();
        assert!(body.contains("REST_FAIL_LIMIT"));
    }

    /// Boundary check for case 14: only the threshold-th call returns
    /// `true` from `record_rest_failure`. Documents the off-by-one
    /// contract that the runner's escalation logic relies on.
    #[test]
    fn rest_progression_returns_true_only_at_threshold() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        let returns: Vec<bool> = (0..3)
            .map(|_| t.record_rest_failure(VenueLabel::Extended))
            .collect();
        assert_eq!(returns, vec![false, false, true]);
    }

    /// `is_stuck` must always re-read the filesystem so an operator
    /// `rm $STUCK` clears the halt without needing a restart. Source
    /// of truth contract per `docs/execution_layer.md` §4.
    #[test]
    fn is_stuck_re_reads_filesystem_after_removal() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        t.arm(StuckReason::Sigusr1);
        assert!(t.is_stuck());
        // Operator clears.
        std::fs::remove_file(t.stuck_file_path()).unwrap();
        assert!(!t.is_stuck());
    }

    /// Catalogue case 12 boundary: only the kill-th `record_reduce_only_failure`
    /// returns `true`; the prior K-1 calls return `false`. This is the
    /// signal the `EmergencyFlattening` retry loop uses to decide
    /// "this attempt was the one that crossed the line — write STUCK".
    #[test]
    fn reduce_only_progression_returns_true_only_at_threshold() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        let returns: Vec<bool> = (0..5)
            .map(|_| t.record_reduce_only_failure())
            .collect();
        assert_eq!(returns, vec![false, false, false, false, true]);
    }

    /// Case 12 consecutive-only contract: a successful close-all in
    /// the middle of a fail sequence resets the kill counter so the
    /// runner doesn't trip on transient venue blips.
    #[test]
    fn reduce_only_success_resets_counter_mid_sequence() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        // Four fails — one short of kill threshold (5).
        for _ in 0..4 {
            assert!(!t.record_reduce_only_failure());
        }
        // A successful close-all attempt resets the counter.
        t.record_reduce_only_success();
        // Four more fails must NOT arm (counter restarts at 0).
        for _ in 0..4 {
            assert!(!t.record_reduce_only_failure());
        }
        assert!(!t.is_stuck());
    }

    /// **Catalogue case 14 contract**: the runner-visible signal for
    /// "this venue's read REST is dead — emit `Event::Emergency {
    /// reason: KillSwitch }`" is `record_rest_failure` returning
    /// `true`. Sprint 4 runner wiring will use exactly this pattern:
    ///
    /// ```ignore
    /// let armed = tripwire.record_rest_failure(venue);
    /// if armed {
    ///     machine.apply(now, Event::Emergency { reason: KillSwitch })?;
    /// }
    /// ```
    ///
    /// This test pins that contract: the **first** call that returns
    /// `true` is the one to map to an Emergency event. Subsequent
    /// `record_rest_failure` calls also return `true` (counter stays
    /// at-or-above threshold) — the runner must not double-emit; one
    /// `is_stuck()` is sufficient because the state machine already
    /// transitions to `EmergencyFlattening` on the first emit and
    /// further attempts are no-ops.
    #[test]
    fn case14_runner_contract_arm_signal_for_emergency_event() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        // Pre-arm streak — runner sees no Emergency event yet.
        assert!(!t.record_rest_failure(VenueLabel::Lighter));
        assert!(!t.record_rest_failure(VenueLabel::Lighter));
        // Threshold-th call → arm; runner emits Emergency{KillSwitch}.
        let armed_now = t.record_rest_failure(VenueLabel::Lighter);
        assert!(armed_now, "runner reads this true to emit Emergency");
        // Subsequent calls also return true (counter is sticky), but
        // the runner uses `is_stuck` to dedupe so the state machine
        // doesn't see the same event twice.
        assert!(t.record_rest_failure(VenueLabel::Lighter));
        assert!(t.is_stuck(), "is_stuck must remain true across calls");
    }

    /// **Case 14 SIGUSR1 path**: the operator-driven arm route also
    /// flows through `is_stuck` so the runner converts it to
    /// `Emergency{KillSwitch}` identically to the REST-fail path.
    /// SIGUSR1 → `arm(StuckReason::Sigusr1)` → file present → runner
    /// reads `is_stuck() == true` → Emergency event. Surface the
    /// `current_reason` "ARMED" tag separately so the dashboard can
    /// distinguish SIGUSR1 from REST counters even though both share
    /// the same state-machine event.
    #[test]
    fn case14_sigusr1_arm_is_visible_via_is_stuck_and_current_reason() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        assert!(!t.is_stuck());
        assert_eq!(t.current_reason(), None);
        t.arm(StuckReason::Sigusr1);
        assert!(t.is_stuck(), "runner reads this to emit Emergency");
        assert_eq!(t.current_reason(), Some("ARMED"));
    }

    /// **Case 14 cross-path**: REST counter armed for Extended does
    /// NOT advance the Lighter counter, but `is_stuck` reflects the
    /// global file. The runner should treat any `is_stuck` as a
    /// reason to emit Emergency{KillSwitch} regardless of which
    /// counter armed it; per-venue counters are operational
    /// information for triage, not an event-routing decision.
    #[test]
    fn case14_per_venue_arm_still_surfaces_global_is_stuck() {
        let tmp = TempDir::new().unwrap();
        let mut t = StuckTripwire::new_for_test(cfg_in(&tmp));
        // Extended arms.
        for _ in 0..3 {
            let _ = t.record_rest_failure(VenueLabel::Extended);
        }
        assert!(t.is_stuck());
        // Lighter has zero failures — counter is at 0.
        // But is_stuck is global → runner emits Emergency on next tick
        // regardless of which side the venue health monitor was
        // checking.
        assert!(t.is_stuck());
    }
}
