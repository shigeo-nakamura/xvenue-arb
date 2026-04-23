#!/usr/bin/env python3
import re
import sys
import argparse
import os
import math
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

RETURN_SCORE_MODES = {"return", "returns", "normalized", "leverage_neutral"}
DEFAULT_SCORE_MODE = "return"
DEFAULT_RETURN_SCALE = 1000.0


def parse_log_line(line):
    """
    Parses a single log line to extract timestamp and trade information.
    Expected log format: "YYYY-MM-DDTHH:MM:SS+ZZZZ [LEVEL] - [ENTRY/EXIT] ..."
    """
    # Regex to capture timestamp and the rest of the message
    log_pattern = re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4})\s+"
        r".*?\[(?P<type>ENTRY|EXIT)\]\s+"
        r"pair=(?P<pair>\S+)\s+"
        r"direction=(?P<direction>\S+)\s+"
        r"size_a=(?P<size_a>\S+)\s+"
        r"price_a=(?P<price_a>\S+)\s+"
        r"size_b=(?P<size_b>\S+)\s+"
        r"price_b=(?P<price_b>\S+)"
        r"(?:\s+.*?\s+ts=(?P<ts>\d+))?"
        r"(?:\s+.*)?$"
    )
    match = log_pattern.search(line)
    if not match:
        return None

    data = match.groupdict()

    # Convert timestamp string to datetime object (fallback when ts is absent).
    ts_str = data["timestamp"]
    data["timestamp"] = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S%z")
    if data.get("ts"):
        data["timestamp"] = datetime.fromtimestamp(
            int(data["ts"]), tz=timezone.utc
        )

    # Convert numeric fields to Decimal for precision
    for key in ["size_a", "price_a", "size_b", "price_b"]:
        data[key] = Decimal(data[key])

    pnl_match = re.search(r"\bpnl=(?P<pnl>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)", line)
    if pnl_match:
        try:
            data["pnl"] = Decimal(pnl_match.group("pnl"))
        except InvalidOperation:
            data["pnl"] = None
    else:
        data["pnl"] = None

    return data


def calculate_pnl(
    log_file, start_time=None, end_time=None, fee_bps=0.0, slippage_bps=0.0
):
    """
    Calculates the total Profit and Loss from a log file, optionally starting from a specific time.
    """
    open_positions = {}
    last_seen_prices = {}
    total_pnl = Decimal("0.0")
    trade_pnls = []
    trade_returns = []
    hold_secs = []
    window_started = start_time is None
    end_reached = False
    fee_bps = max(0.0, fee_bps or 0.0)
    slippage_bps = max(0.0, slippage_bps or 0.0)
    cost_bps = fee_bps + slippage_bps
    cost_ratio = (
        Decimal(str(cost_bps)) / Decimal("10000") if cost_bps > 0.0 else Decimal("0.0")
    )

    def close_position(
        entry, exit_price_a, exit_price_b, exit_ts, exit_pnl=None, use_exit_pnl=False
    ):
        nonlocal total_pnl
        if use_exit_pnl and exit_pnl is not None:
            trade_pnl = exit_pnl
        else:
            if entry["direction"] == "LongSpread":
                pnl_a = (exit_price_a - entry["price_a"]) * entry["size_a"]
                pnl_b = (entry["price_b"] - exit_price_b) * entry["size_b"]
            elif entry["direction"] == "ShortSpread":
                pnl_a = (entry["price_a"] - exit_price_a) * entry["size_a"]
                pnl_b = (exit_price_b - entry["price_b"]) * entry["size_b"]
            else:
                pnl_a = pnl_b = Decimal("0.0")
            trade_pnl = pnl_a + pnl_b

        if cost_ratio > Decimal("0.0"):
            entry_cost = Decimal("0.0")
            if not entry.get("boundary_marked", False):
                entry_cost = (
                    (entry["price_a"] * entry["size_a"])
                    + (entry["price_b"] * entry["size_b"])
                ) * cost_ratio
            exit_cost = (
                (exit_price_a * entry["size_a"]) + (exit_price_b * entry["size_b"])
            ) * cost_ratio
            trade_pnl -= entry_cost + exit_cost

        total_pnl += trade_pnl
        trade_pnls.append(float(trade_pnl))
        notional = abs(entry["price_a"] * entry["size_a"]) + abs(
            entry["price_b"] * entry["size_b"]
        )
        trade_return = 0.0
        if notional > Decimal("0.0"):
            try:
                trade_return = float(trade_pnl / notional)
            except (InvalidOperation, ZeroDivisionError, OverflowError):
                trade_return = 0.0
        trade_returns.append(trade_return)
        if entry.get("timestamp") and exit_ts:
            hold_secs.append(
                max(0.0, (exit_ts - entry["timestamp"]).total_seconds())
            )
        else:
            hold_secs.append(0.0)

    def close_open_positions(boundary_ts=None):
        for pair, entry in list(open_positions.items()):
            prices = last_seen_prices.get(pair)
            if not prices:
                continue
            exit_ts = boundary_ts or prices.get("timestamp") or entry.get("timestamp")
            close_position(
                entry,
                prices["price_a"],
                prices["price_b"],
                exit_ts,
                exit_pnl=None,
                use_exit_pnl=False,
            )
        open_positions.clear()

    try:
        with open(log_file, "r") as f:
            for line in f:
                trade_data = parse_log_line(line)
                if not trade_data:
                    continue

                ts = trade_data["timestamp"]
                pair = trade_data["pair"]

                # Before the window: keep state and last seen prices.
                if start_time and ts < start_time:
                    last_seen_prices[pair] = {
                        "price_a": trade_data["price_a"],
                        "price_b": trade_data["price_b"],
                        "timestamp": ts,
                    }
                    if trade_data["type"] == "ENTRY":
                        trade_data["boundary_marked"] = False
                        open_positions[pair] = trade_data
                    elif trade_data["type"] == "EXIT":
                        if pair in open_positions:
                            open_positions.pop(pair)
                    continue

                if start_time and not window_started:
                    window_started = True
                    for open_pair, entry in open_positions.items():
                        entry["boundary_marked"] = True
                        prices = last_seen_prices.get(open_pair)
                        if prices:
                            entry["price_a"] = prices["price_a"]
                            entry["price_b"] = prices["price_b"]
                        entry["timestamp"] = start_time

                if end_time and window_started and ts > end_time:
                    close_open_positions(end_time)
                    end_reached = True
                    break

                last_seen_prices[pair] = {
                    "price_a": trade_data["price_a"],
                    "price_b": trade_data["price_b"],
                    "timestamp": ts,
                }

                if trade_data["type"] == "ENTRY":
                    trade_data["boundary_marked"] = False
                    open_positions[pair] = trade_data

                elif trade_data["type"] == "EXIT":
                    entry = open_positions.pop(pair, None)
                    if entry:
                        use_exit_pnl = (
                            trade_data.get("pnl") is not None
                            and not entry.get("boundary_marked", False)
                        )
                        close_position(
                            entry,
                            trade_data["price_a"],
                            trade_data["price_b"],
                            ts,
                            exit_pnl=trade_data.get("pnl"),
                            use_exit_pnl=use_exit_pnl,
                        )

        if window_started:
            if end_time and not end_reached:
                close_open_positions(end_time)
            elif not end_time:
                close_open_positions()

    except FileNotFoundError:
        print(f"[log_analyzer] File not found: {log_file}", file=sys.stderr)
        return Decimal("0.0"), [], [], []
    except Exception as exc:
        # On any other error, assume PnL is zero to not halt the optimizer
        print(f"[log_analyzer] Error while parsing {log_file}: {exc}", file=sys.stderr)
        return Decimal("0.0"), [], [], []

    return total_pnl, trade_pnls, trade_returns, hold_secs


def parse_env_int(name):
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def parse_env_float(name):
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def compute_max_drawdown(trade_pnls):
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for pnl in trade_pnls:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def compute_sharpe(trade_pnls):
    n = len(trade_pnls)
    if n < 2:
        return 0.0
    mean = sum(trade_pnls) / n
    variance = sum((p - mean) ** 2 for p in trade_pnls) / (n - 1)
    if variance <= 0:
        return 0.0
    return (mean / math.sqrt(variance)) * math.sqrt(n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze bot logs to calculate PnL.")
    parser.add_argument("log_file", help="Path to the log file.")
    parser.add_argument(
        "--start-timestamp",
        help="ISO 8601 timestamp (e.g., '2023-01-01T12:00:00+0000'). "
        "Only trades after this time will be considered for PnL calculation.",
        default=None,
    )
    parser.add_argument(
        "--end-timestamp",
        help="ISO 8601 timestamp (e.g., '2023-01-01T12:00:00+0000'). "
        "Only trades before this time will be considered for PnL calculation.",
        default=None,
    )

    args = parser.parse_args()

    start_time_obj = None
    if args.start_timestamp:
        try:
            # The timezone format is already handled by %z
            start_time_obj = datetime.strptime(
                args.start_timestamp, "%Y-%m-%dT%H:%M:%S%z"
            )
        except ValueError:
            sys.exit(
                f"Error: Invalid start-timestamp format. Use 'YYYY-MM-DDTHH:MM:SS+ZZZZ'."
            )

    end_time_obj = None
    if args.end_timestamp:
        try:
            end_time_obj = datetime.strptime(
                args.end_timestamp, "%Y-%m-%dT%H:%M:%S%z"
            )
        except ValueError:
            sys.exit(
                "Error: Invalid end-timestamp format. Use 'YYYY-MM-DDTHH:MM:SS+ZZZZ'."
            )

    score_mode = (
        os.getenv("OPTIMIZER_SCORE_MODE", DEFAULT_SCORE_MODE).strip().lower()
    )
    use_return = score_mode in RETURN_SCORE_MODES
    return_scale = parse_env_float("OPTIMIZER_RETURN_SCALE") or DEFAULT_RETURN_SCALE
    if return_scale <= 0.0:
        return_scale = DEFAULT_RETURN_SCALE

    fee_bps = parse_env_float("FEE_BPS") or 0.0
    slippage_bps = parse_env_float("SLIPPAGE_BPS") or 0.0
    final_pnl, trade_pnls, trade_returns, hold_secs = calculate_pnl(
        args.log_file, start_time_obj, end_time_obj, fee_bps, slippage_bps
    )

    if use_return:
        series = [r * return_scale for r in trade_returns]
        score = sum(series)
    else:
        series = trade_pnls
        score = float(final_pnl)

    trade_count = len(series)
    max_drawdown = compute_max_drawdown(series)
    sharpe = compute_sharpe(series)
    avg_hold_secs = sum(hold_secs) / len(hold_secs) if hold_secs else 0.0
    worst_trade = min(series) if series else 0.0
    cvar_pct = parse_env_float("OPTIMIZER_CVAR_PCT") or 0.05
    cvar_penalty = parse_env_float("OPTIMIZER_CVAR_PENALTY") or 0.0
    cvar = 0.0
    if series and cvar_pct > 0.0:
        k = max(1, int(math.ceil(len(series) * cvar_pct)))
        worst = sorted(series)[:k]
        cvar = sum(worst) / len(worst)
        if cvar > 0.0:
            cvar = 0.0

    min_trades = parse_env_int("OPTIMIZER_MIN_TRADES") or 0
    max_dd = parse_env_float("OPTIMIZER_MAX_DRAWDOWN")
    if max_dd is not None and max_dd < 0:
        max_dd = None
    min_sharpe = parse_env_float("OPTIMIZER_MIN_SHARPE")
    max_hold_secs = parse_env_float("OPTIMIZER_MAX_AVG_HOLD_SECS")
    if max_hold_secs is not None and max_hold_secs < 0:
        max_hold_secs = None

    reject_reasons = []
    if min_trades > 0 and trade_count < min_trades:
        reject_reasons.append(f"trades={trade_count}<{min_trades}")
        score = -math.inf
    if max_dd is not None and max_drawdown > max_dd:
        reject_reasons.append(f"drawdown={max_drawdown:.2f}>{max_dd:.2f}")
        score = -math.inf
    if min_sharpe is not None and sharpe < min_sharpe:
        reject_reasons.append(f"sharpe={sharpe:.4f}<{min_sharpe:.4f}")
        score = -math.inf
    if max_hold_secs is not None and avg_hold_secs > max_hold_secs:
        reject_reasons.append(f"avg_hold={avg_hold_secs:.0f}s>{max_hold_secs:.0f}s")
        score = -math.inf
    max_single_loss = parse_env_float("OPTIMIZER_MAX_SINGLE_LOSS")
    if (
        max_single_loss is not None
        and max_single_loss > 0.0
        and series
        and worst_trade < -max_single_loss
    ):
        reject_reasons.append(f"worst_trade={worst_trade:.2f}<-{max_single_loss:.2f}")
        score = -math.inf

    drawdown_penalty = parse_env_float("OPTIMIZER_DRAWDOWN_PENALTY") or 0.0
    hold_penalty = parse_env_float("OPTIMIZER_AVG_HOLD_PENALTY") or 0.0
    sharpe_bonus = parse_env_float("OPTIMIZER_SHARPE_BONUS") or 0.0
    trade_freq_bonus = parse_env_float("OPTIMIZER_TRADE_FREQ_BONUS") or 0.0
    if math.isfinite(score):
        score -= drawdown_penalty * max_drawdown
        score -= hold_penalty * avg_hold_secs
        if cvar < 0.0 and cvar_penalty > 0.0:
            score -= cvar_penalty * abs(cvar)
        score += sharpe_bonus * sharpe
        score += trade_freq_bonus * trade_count

    if math.isfinite(score):
        print(f"{score:.8f}")
    else:
        reason_str = ",".join(reject_reasons) if reject_reasons else "unknown"
        print(f"-inf [{reason_str}]", file=sys.stderr)
        print(str(score).lower())
