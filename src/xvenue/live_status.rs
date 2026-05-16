//! Periodic status emission + equity sampling, extracted from `live.rs`
//! per bot-strategy#385 (seam 2 of the 2026-05-13 audit). All entry
//! points are byte-identical wrappers around the original code; the
//! move is cohesion-only.
//!
//! The hot path order — `refresh_equity` → `publish_risk` →
//! `publish_kill_switch` → `write_snapshot_if_due` — is preserved as the
//! body of [`report_status_tick`] so the dashboard sees the same field
//! order it always has.

use rust_decimal::Decimal;

use super::config::XvenueConfig;
use super::live::{
    kill_switch_active, now_unix_secs, wall_clock_ms, LivePaperSummary, Venue, VenueHub,
};
use super::state::{Phase, PositionMachine};
use super::status::{equity_decimal_to_f64, StatusReporter};
use crate::prom;
use crate::risk::kill_switch::StuckTripwire;
use crate::risk::manager::RiskManager;
use crate::risk::ws_health::WsHealthMonitor;

/// Reads equity from both venues and decides whether the sum should
/// be recorded as a sample.
///
/// **All-or-nothing semantics (bot-strategy#360)**: returns
/// `Some(total)` only when *every* queried venue returned
/// `Ok(Some(_))`. Returns `None` in two cases:
/// - All venues failed/missing (typical at boot before WS warmup) —
///   silent, no counter bump.
/// - Some-but-not-all venues reported (e.g. one venue in maintenance)
///   — bumps `equity_samples_skipped_partial` and emits a WARN.
///   Recording a single-venue equity halves the rolling peak and
///   trips a spurious session_dd halt, so we deliberately skip.
pub(super) async fn read_total_equity_for_sample<H: VenueHub + ?Sized>(
    hub: &H,
    summary: &mut LivePaperSummary,
) -> Option<Decimal> {
    let venues = [Venue::Extended, Venue::Lighter];
    let mut total = Decimal::ZERO;
    let mut ok_count: u8 = 0;
    let total_count: u8 = venues.len() as u8;
    let mut missing: Vec<Venue> = Vec::new();
    for v in venues {
        match hub.read_equity_usd(v).await {
            Ok(Some(eq)) => {
                total += eq;
                ok_count += 1;
            }
            Ok(None) => missing.push(v),
            Err(e) => {
                log::warn!("[STATUS] read_equity_usd({:?}) failed: {:?}", v, e);
                missing.push(v);
            }
        }
    }
    if ok_count == 0 {
        return None;
    }
    if ok_count < total_count {
        summary.equity_samples_skipped_partial += 1;
        log::warn!(
            "[STATUS] equity sample skipped: {}/{} venues reported, missing={:?} \
             (would-record total={}; see bot-strategy#360)",
            ok_count,
            total_count,
            missing,
            total
        );
        return None;
    }
    // bot-strategy#382: drop zero-valued readings during the pre-init
    // warm-up. dex-connector's WS-derived balance cache can return
    // Ok(equity=0) before the first account dump lands, and when both
    // venues fall into that state simultaneously the sum is Some(0).
    // Propagating that to `reporter.update_equity(0)` (refresh_equity
    // below) locks `equity_day_start = 0` for the rest of the UTC day,
    // inflating `pnl_today` by the full equity once the real balance
    // arrives. Same root cause as the pairtrade fix at
    // pairtrade@1063983; same shape of gate.
    //
    // Post-init zero IS accepted — a genuinely rekt bot must still
    // surface on dashboards rather than silently pin to its last
    // positive equity.
    if total <= Decimal::ZERO && !summary.equity_initialized {
        log::info!(
            "[STATUS] equity sample skipped: both venues reported 0 during \
             warm-up (pre-init gate, see bot-strategy#382)"
        );
        return None;
    }
    summary.equity_initialized = true;
    Some(total)
}

/// Pulls equity from both venues and threads the sum into the reporter
/// so the dashboard's `pnl_total` / `pnl_today` line tracks the live
/// account. Also hands the equity sample to the risk manager so the
/// session-DD rolling peak (#244 D-4) tracks the same number the
/// dashboard renders. Skip policy lives in
/// [`read_total_equity_for_sample`] (bot-strategy#360).
pub(super) async fn refresh_equity<H: VenueHub + ?Sized>(
    hub: &H,
    reporter: &mut StatusReporter,
    risk_manager: &mut RiskManager,
    summary: &mut LivePaperSummary,
) {
    if let Some(total) = read_total_equity_for_sample(hub, summary).await {
        let eq_f64 = equity_decimal_to_f64(total);
        reporter.update_equity(eq_f64);
        risk_manager.record_equity_sample(eq_f64, now_unix_secs());
    }
}

pub(super) fn publish_risk(risk_manager: &RiskManager, reporter: &mut StatusReporter) {
    reporter.set_daily_risk(risk_manager.daily_snapshot());
    reporter.set_session_risk(risk_manager.session_snapshot());
    reporter.set_circuit_breaker(Some(risk_manager.circuit_breaker_snapshot(now_unix_secs())));
    reporter.set_risk_history(risk_manager.risk_history());
}

/// Refresh `kill_switch_active` on the reporter so the dashboard's
/// `kill_switch_active` field stays current without an SSM probe (#343).
/// Called once per tick from each `write_snapshot_if_due` call site so
/// the reporter sees the same state the live `kill_switch_active()`
/// gate sees inside `gate_decision`.
pub(super) fn publish_kill_switch(cfg: &XvenueConfig, reporter: &mut StatusReporter) {
    reporter.set_kill_switch(kill_switch_active(&cfg.kill_switch_file));
}

/// Periodic [STATUS] log + dashboard snapshot write fired by the
/// `status_ivl` arm in `run_paper_loop`. Pulled out so the loop body
/// reads as a state machine rather than a paragraph of formatting.
/// Behaviour-preserving: identical log line, identical equity refresh /
/// risk publish / snapshot-write order.
#[allow(clippy::too_many_arguments)]
pub(super) async fn report_status_tick<H: VenueHub + ?Sized>(
    cfg: &XvenueConfig,
    hub: &H,
    summary: &mut LivePaperSummary,
    ws_health: &WsHealthMonitor,
    machine: &PositionMachine,
    risk_manager: &mut RiskManager,
    stuck: &StuckTripwire,
    reporter: Option<&mut StatusReporter>,
) {
    let ws_age = ws_health.ws_age(wall_clock_ms());
    let wb_fill_rate = if summary.would_be_maker_attempts > 0 {
        summary.would_be_maker_fills as f64 / summary.would_be_maker_attempts as f64
    } else {
        0.0
    };
    let wb_p_avg = if summary.would_be_maker_attempts > 0 {
        summary.would_be_maker_p_sum / summary.would_be_maker_attempts as f64
    } else {
        0.0
    };
    let wb_exit_fill_rate = if summary.would_be_maker_exit_attempts > 0 {
        summary.would_be_maker_exit_fills as f64 / summary.would_be_maker_exit_attempts as f64
    } else {
        0.0
    };
    let wb_exit_p_avg = if summary.would_be_maker_exit_attempts > 0 {
        summary.would_be_maker_exit_p_sum / summary.would_be_maker_exit_attempts as f64
    } else {
        0.0
    };
    let paper_gross_avg_bps = if summary.paper_net_attempts > 0 {
        summary.paper_gross_bps_sum / summary.paper_net_attempts as f64
    } else {
        0.0
    };
    let paper_net_avg_bps = if summary.paper_net_attempts > 0 {
        summary.paper_net_bps_sum / summary.paper_net_attempts as f64
    } else {
        0.0
    };
    log::info!(
        "[STATUS] ticks={} samples={} hold={} enter_l={} enter_s={} exit={} \
         ks_blocked={} stuck_blocked={} dd_blocked={} sd_blocked={} cb_blocked={} \
         ws_blocked={} depth_blocked={} maint_blocked={} ws_emerg={} skew_emerg={} \
         ws_age_ext={:?} ws_age_lt={:?} \
         ref_supp_ext={} ref_supp_lt={} read_mid_err_ext={} read_mid_err_lt={} \
         eq_skip_partial={} \
         dev_bps={:?} cap_long={:?} cap_short={:?} \
         ext_inside={:?} lt_inside={:?} lt_bid_sz={:?} lt_ask_sz={:?} \
         wb_attempts={} wb_fills={} wb_fill_rate={:.4} wb_p_avg={:.4} \
         wb_exit_attempts={} wb_exit_fills={} wb_exit_fill_rate={:.4} wb_exit_p_avg={:.4} \
         paper_n={} paper_gross_bps_avg={:.2} paper_net_bps_avg={:.2}",
        summary.ticks,
        summary.samples_committed,
        summary.decisions_hold,
        summary.decisions_enter_long,
        summary.decisions_enter_short,
        summary.decisions_exit,
        summary.entries_blocked_by_kill_switch,
        summary.entries_blocked_by_stuck_file,
        summary.entries_blocked_by_daily_dd,
        summary.entries_blocked_by_session_dd,
        summary.entries_blocked_by_circuit_breaker,
        summary.entries_blocked_by_ws_stale,
        summary.entries_blocked_by_book_depth,
        summary.entries_blocked_by_maintenance,
        summary.ws_stale_emergencies_emitted,
        summary.skew_emergencies_emitted,
        ws_age.ext_age_ms,
        ws_age.lt_age_ms,
        summary.ext_book_suppressed_by_ref_guard,
        summary.lt_book_suppressed_by_ref_guard,
        summary.read_mid_err_ext,
        summary.read_mid_err_lt,
        summary.equity_samples_skipped_partial,
        summary.last_dev_bps,
        summary.last_cap_long_bps,
        summary.last_cap_short_bps,
        summary.last_ext_inside_bps,
        summary.last_lt_inside_bps,
        summary.last_lt_bid_size,
        summary.last_lt_ask_size,
        summary.would_be_maker_attempts,
        summary.would_be_maker_fills,
        wb_fill_rate,
        wb_p_avg,
        summary.would_be_maker_exit_attempts,
        summary.would_be_maker_exit_fills,
        wb_exit_fill_rate,
        wb_exit_p_avg,
        summary.paper_net_attempts,
        paper_gross_avg_bps,
        paper_net_avg_bps,
    );
    if let Some(r) = reporter {
        refresh_equity(hub, r, risk_manager, summary).await;
        publish_risk(risk_manager, r);
        publish_kill_switch(cfg, r);
        let now_ts_ms = wall_clock_ms();
        if let Err(e) = r.write_snapshot_if_due(machine, now_ts_ms) {
            log::warn!("[STATUS] snapshot write failed: {:?}", e);
        }
        // bot-strategy#314 Group 5: mirror everything the [STATUS] log
        // line already carries into Prometheus gauges. Done after the
        // snapshot write so the dashboard's status.json and the
        // exporter agree on the same tick.
        publish_prom(cfg, summary, ws_health, machine, risk_manager, stuck, r);
    }
}

/// Mirror the current status-tick state into the in-process Prometheus
/// registry. Called once per status interval from `report_status_tick`,
/// after risk + kill-switch + equity have already been refreshed.
/// Per-tick metrics (e.g. `dev_bps`) are also updated here at the
/// status cadence — a 60s sample is plenty for Grafana panels and
/// avoids touching the hot tick loop.
fn publish_prom(
    cfg: &XvenueConfig,
    summary: &LivePaperSummary,
    ws_health: &WsHealthMonitor,
    machine: &PositionMachine,
    risk_manager: &mut RiskManager,
    stuck: &StuckTripwire,
    reporter: &StatusReporter,
) {
    let agent = cfg.agent_name.as_str();

    // Spread / signal.
    if let Some(v) = summary.last_dev_bps {
        prom::DEV_BPS.with_label_values(&[agent]).set(v);
    }
    if let Some(v) = summary.last_cap_long_bps {
        prom::CAP_LONG_BPS.with_label_values(&[agent]).set(v);
    }
    if let Some(v) = summary.last_cap_short_bps {
        prom::CAP_SHORT_BPS.with_label_values(&[agent]).set(v);
    }
    if let Some(v) = summary.last_ext_inside_bps {
        prom::INSIDE_BPS
            .with_label_values(&[agent, "extended"])
            .set(v);
    }
    if let Some(v) = summary.last_lt_inside_bps {
        prom::INSIDE_BPS
            .with_label_values(&[agent, "lighter"])
            .set(v);
    }
    if let Some(v) = summary.last_lt_bid_size {
        prom::LT_TOUCH_SIZE
            .with_label_values(&[agent, "bid"])
            .set(v);
    }
    if let Some(v) = summary.last_lt_ask_size {
        prom::LT_TOUCH_SIZE
            .with_label_values(&[agent, "ask"])
            .set(v);
    }

    // Position state.
    let phase = machine.phase();
    let (phase_int, has_pos) = match phase {
        Phase::Flat => (0_i64, 0_i64),
        Phase::EnteringExtended => (1, 1),
        Phase::EnteringLighter => (2, 1),
        Phase::Held => (3, 1),
        Phase::Exiting => (4, 1),
        Phase::EmergencyFlattening => (5, 1),
    };
    prom::PHASE_STATE.with_label_values(&[agent]).set(phase_int);
    prom::HAS_POSITION.with_label_values(&[agent]).set(has_pos);
    let age_secs = if matches!(phase, Phase::Flat) {
        0.0
    } else {
        machine.time_in_phase_ms(wall_clock_ms()) as f64 / 1000.0
    };
    prom::POSITION_AGE_SECONDS
        .with_label_values(&[agent])
        .set(age_secs);

    // Decisions + blocks (mirror u64 counters into gauges; use delta()).
    prom::DECISIONS_TOTAL
        .with_label_values(&[agent, "hold"])
        .set(summary.decisions_hold as i64);
    prom::DECISIONS_TOTAL
        .with_label_values(&[agent, "enter_long"])
        .set(summary.decisions_enter_long as i64);
    prom::DECISIONS_TOTAL
        .with_label_values(&[agent, "enter_short"])
        .set(summary.decisions_enter_short as i64);
    prom::DECISIONS_TOTAL
        .with_label_values(&[agent, "exit"])
        .set(summary.decisions_exit as i64);

    for (reason, value) in [
        ("kill_switch", summary.entries_blocked_by_kill_switch),
        ("stuck", summary.entries_blocked_by_stuck_file),
        ("daily_dd", summary.entries_blocked_by_daily_dd),
        ("session_dd", summary.entries_blocked_by_session_dd),
        (
            "circuit_breaker",
            summary.entries_blocked_by_circuit_breaker,
        ),
        ("ws_stale", summary.entries_blocked_by_ws_stale),
        ("book_depth", summary.entries_blocked_by_book_depth),
        ("maintenance", summary.entries_blocked_by_maintenance),
    ] {
        prom::ENTRIES_BLOCKED_TOTAL
            .with_label_values(&[agent, reason])
            .set(value as i64);
    }

    prom::REF_GUARD_SUPPRESSED_TOTAL
        .with_label_values(&[agent, "extended"])
        .set(summary.ext_book_suppressed_by_ref_guard as i64);
    prom::REF_GUARD_SUPPRESSED_TOTAL
        .with_label_values(&[agent, "lighter"])
        .set(summary.lt_book_suppressed_by_ref_guard as i64);
    prom::READ_MID_ERR_TOTAL
        .with_label_values(&[agent, "extended"])
        .set(summary.read_mid_err_ext as i64);
    prom::READ_MID_ERR_TOTAL
        .with_label_values(&[agent, "lighter"])
        .set(summary.read_mid_err_lt as i64);
    prom::EQUITY_SAMPLES_SKIPPED_PARTIAL_TOTAL
        .with_label_values(&[agent])
        .set(summary.equity_samples_skipped_partial as i64);

    // Risk / kill state. `daily_snapshot` and `session_snapshot` return
    // None during pre-equity warm-up; in that window leave the previous
    // value so a transient None doesn't flap the dashboard to 0.
    prom::KILL_SWITCH_ACTIVE.with_label_values(&[agent]).set(
        if kill_switch_active(&cfg.kill_switch_file) {
            1
        } else {
            0
        },
    );
    prom::STUCK_ACTIVE
        .with_label_values(&[agent])
        .set(if stuck.is_stuck() { 1 } else { 0 });

    if let Some(s) = risk_manager.session_snapshot() {
        prom::EQUITY_CURRENT_USD
            .with_label_values(&[agent])
            .set(s.current_equity);
        prom::EQUITY_PEAK_USD
            .with_label_values(&[agent])
            .set(s.peak_equity);
        prom::SESSION_DD_BPS
            .with_label_values(&[agent])
            .set(s.dd_bps);
        prom::SESSION_DD_HALT_ACTIVE
            .with_label_values(&[agent])
            .set(if s.session_halted { 1 } else { 0 });
        prom::EFFECTIVE_MAX_SESSION_LOSS_BPS
            .with_label_values(&[agent])
            .set(s.effective_max_session_loss_bps);
    }
    if let Some(d) = risk_manager.daily_snapshot() {
        prom::DAILY_PNL_BPS
            .with_label_values(&[agent])
            .set(d.daily_pnl_bps);
        prom::DAILY_DD_HALT_ACTIVE
            .with_label_values(&[agent])
            .set(if d.risk_halted { 1 } else { 0 });
        prom::EFFECTIVE_MAX_DAILY_LOSS_BPS
            .with_label_values(&[agent])
            .set(d.effective_max_daily_loss_bps);
    }
    let cb = risk_manager.circuit_breaker_snapshot(now_unix_secs());
    prom::CIRCUIT_BREAKER_ACTIVE
        .with_label_values(&[agent])
        .set(if cb.active { 1 } else { 0 });

    // System health.
    let ws_age = ws_health.ws_age(wall_clock_ms());
    if let Some(ms) = ws_age.ext_age_ms {
        prom::WS_AGE_MS
            .with_label_values(&[agent, "extended"])
            .set(ms as f64);
    }
    if let Some(ms) = ws_age.lt_age_ms {
        prom::WS_AGE_MS
            .with_label_values(&[agent, "lighter"])
            .set(ms as f64);
    }
    if let Ok(meta) = std::fs::metadata(reporter.path()) {
        if let Ok(mtime) = meta.modified() {
            if let Ok(age) = std::time::SystemTime::now().duration_since(mtime) {
                prom::SNAPSHOT_AGE_SECONDS
                    .with_label_values(&[agent])
                    .set(age.as_secs_f64());
            }
        }
    }
}
