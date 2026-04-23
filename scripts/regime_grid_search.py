#!/usr/bin/env python3
"""Grid search for regime filter thresholds (bot-strategy#20).

Uses Bot A live config as baseline, tests regime_vol_max x regime_trend_max
combinations. Also sweeps Kalman Q/R for log-only diagnostics.
"""
import subprocess
import sys
import os
import itertools
import concurrent.futures
import tempfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
BINARY = os.path.join(REPO_ROOT, "target", "release", "debot")
BACKTEST_FILE = os.path.join(REPO_ROOT, "market_data_btceth_365d.bin")
CONFIG = os.path.join(REPO_ROOT, "configs", "pairtrade", "debot-pair-btceth.yaml")
LOG_DIR = "/tmp/regime_grid"

sys.path.insert(0, SCRIPT_DIR)
from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe


def run_one(params: dict, tag: str) -> dict:
    """Run a single backtest with the given env overrides, return metrics."""
    log_file = os.path.join(LOG_DIR, f"{tag}.log")
    env = os.environ.copy()
    env.update({
        "BACKTEST_MODE": "true",
        "BACKTEST_FILE": BACKTEST_FILE,
        "DRY_RUN": "true",
        "ENABLE_DATA_DUMP": "false",
        "RUST_LOG": "warn,debot::pairtrade=info",
        "UNIVERSE_PAIRS": "BTC/ETH",
        "PAIRTRADE_CONFIG_PATH": CONFIG,
    })
    env.update(params)

    try:
        with open(log_file, "w") as f:
            subprocess.run([BINARY], env=env, stdout=f, stderr=subprocess.STDOUT,
                           text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return {"tag": tag, "error": "timeout"}
    except Exception as e:
        return {"tag": tag, "error": str(e)}

    results = {}
    for fee_label, fee_val in [("fee0", 0.0), ("fee5", 5.0)]:
        try:
            pnl, trade_pnls, _, hold_secs = calculate_pnl(log_file, None, None, fee_val, 0.0)
            n = len(trade_pnls)
            wins = sum(1 for p in trade_pnls if p > 0)
            dd = compute_max_drawdown(trade_pnls) if trade_pnls else 0.0
            sh = compute_sharpe(trade_pnls) if trade_pnls else 0.0
            avg_hold = sum(hold_secs) / len(hold_secs) if hold_secs else 0.0
            results[fee_label] = {
                "pnl": float(pnl), "trades": n, "wins": wins,
                "winrate": wins / n * 100 if n > 0 else 0.0,
                "sharpe": sh, "maxdd": dd, "avg_hold": avg_hold,
                "pnl_per_trade": float(pnl) / n if n > 0 else 0.0,
            }
        except Exception as e:
            results[fee_label] = {"error": str(e)}

    return {"tag": tag, "params": params, **results}


def main():
    os.makedirs(LOG_DIR, exist_ok=True)

    # Regime filter grid
    vol_max_values = [0.0, 0.0005, 0.001, 0.002, 0.003, 0.005]
    trend_max_values = [0.0, 0.3, 0.5, 0.8, 1.0]

    jobs = []
    for vm in vol_max_values:
        for tm in trend_max_values:
            tag = f"regime_v{vm}_t{tm}"
            params = {
                "REGIME_VOL_WINDOW": "60",
                "REGIME_VOL_MAX": str(vm),
                "REGIME_TREND_WINDOW": "60",
                "REGIME_TREND_MAX": str(tm),
                "REGIME_REFERENCE_SYMBOL": "BTC",
            }
            jobs.append((params, tag))

    print(f"Running {len(jobs)} regime filter combinations...")
    print()

    # Header
    print(f"{'vol_max':>10} {'trend_max':>10} | {'PnL(0bp)':>10} {'Trades':>7} {'Win%':>6} {'Sharpe':>8} | {'PnL(5bp)':>10} {'Trades':>7} {'Win%':>6} {'Sharpe':>8} {'MaxDD':>8}")
    print("-" * 115)

    results = []
    max_workers = min(os.cpu_count() or 4, 8)
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_one, p, t): (p, t) for p, t in jobs}
        for future in concurrent.futures.as_completed(futures):
            r = future.result()
            results.append(r)
            if "error" in r:
                print(f"  ERROR: {r['tag']}: {r['error']}")
                continue
            p = r["params"]
            f0 = r.get("fee0", {})
            f5 = r.get("fee5", {})
            if "error" in f0 or "error" in f5:
                print(f"  ANALYZER ERROR: {r['tag']}")
                continue
            print(f"{p['REGIME_VOL_MAX']:>10} {p['REGIME_TREND_MAX']:>10} | "
                  f"{f0['pnl']:>10.2f} {f0['trades']:>7} {f0['winrate']:>5.1f}% {f0['sharpe']:>8.3f} | "
                  f"{f5['pnl']:>10.2f} {f5['trades']:>7} {f5['winrate']:>5.1f}% {f5['sharpe']:>8.3f} {f5['maxdd']:>8.2f}")

    # Summary: best configs
    print()
    print("=== Top 5 by PnL (fee=0bps) ===")
    valid = [r for r in results if "fee0" in r and "error" not in r.get("fee0", {})]
    valid.sort(key=lambda x: x["fee0"]["pnl"], reverse=True)
    for i, r in enumerate(valid[:5]):
        p = r["params"]
        f0 = r["fee0"]
        f5 = r["fee5"]
        print(f"  #{i+1}: vol_max={p['REGIME_VOL_MAX']:>6} trend_max={p['REGIME_TREND_MAX']:>4} "
              f"PnL(0bp)=${f0['pnl']:.2f} trades={f0['trades']} win={f0['winrate']:.0f}% "
              f"PnL(5bp)=${f5['pnl']:.2f} sharpe={f0['sharpe']:.3f}")

    print()
    print("=== Top 5 by PnL (fee=5bps) ===")
    valid5 = [r for r in results if "fee5" in r and "error" not in r.get("fee5", {})]
    valid5.sort(key=lambda x: x["fee5"]["pnl"], reverse=True)
    for i, r in enumerate(valid5[:5]):
        p = r["params"]
        f0 = r["fee0"]
        f5 = r["fee5"]
        print(f"  #{i+1}: vol_max={p['REGIME_VOL_MAX']:>6} trend_max={p['REGIME_TREND_MAX']:>4} "
              f"PnL(0bp)=${f0['pnl']:.2f} trades={f0['trades']} win={f0['winrate']:.0f}% "
              f"PnL(5bp)=${f5['pnl']:.2f} sharpe(5bp)={f5['sharpe']:.3f}")


if __name__ == "__main__":
    main()
