#!/usr/bin/env python3
import subprocess
import argparse
import os
import itertools
import sys
import re
import json
import csv
import smtplib
import traceback
import tempfile
import concurrent.futures
import uuid
import shutil
import random
import math
import yaml
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

# --- Configuration ---

RETURN_SCORE_MODES = {"return", "returns", "normalized", "leverage_neutral"}
DEFAULT_SCORE_MODE = "return"

# Step 1: Data Gathering Configuration
# ------------------------------------
# The file where market data will be stored.
# Use an absolute path to avoid CWD changes in runner scripts.
DATA_DUMP_FILE = os.path.abspath(
    os.getenv("DATA_DUMP_FILE", "market_data_30d.jsonl")
)
# Keep the JSONL path for Python-side analysis even after bincode conversion.
DATA_DUMP_JSONL = DATA_DUMP_FILE
# How long to run the bot in live mode to gather data.
# This should be long enough for the backtest period.
DATA_GATHERING_DURATION_SECS = 30 * 24 * 3600

# Step 2: Backtest & Optimization Configuration
# ---------------------------------------------
# A list of pairs to optimize for, using the data from the dump file.
# If None or empty, this will be derived from config (universe_pairs or universe_symbols).
TARGET_PAIRS = None

# Parameters to tune. This grid will be tested against the static dataset.
PARAM_GRID = {
    # Entry/exit thresholds – raised from 1.15 to reduce noise trades.
    # Live analysis showed z>2.0 in 24% of observations; 1.15 enters too shallow.
    "ENTRY_Z_SCORE_BASE": ["1.5", "1.8", "2.0", "2.2"],
    "ENTRY_Z_SCORE_MIN": ["1.0"],  # fixed: always 1.0 in prior results
    "ENTRY_Z_SCORE_MAX": ["2.5"],  # raised to accommodate higher base
    "EXIT_Z_SCORE": ["0.3", "0.5", "0.8"],
    # Stop-loss: 8.0 is effectively infinite, removed.
    "STOP_LOSS_Z_SCORE": ["3.5", "5.0"],
    # Tighter cap to cut large drawdowns (live showed -$0.12 losses).
    "MAX_LOSS_R_MULT": ["1.5", "2.0", "3.0"],
    # Fixed: risk sizing doesn't affect signal quality, only scales returns.
    "RISK_PCT_PER_TRADE": ["0.04"],
    "MAX_LEVERAGE": ["5"],
    # Need at least 1 half-life of hold time for mean reversion to play out.
    "FORCE_CLOSE_TIME_SECS": ["1200", "1800", "2700"],
    # Mean-reversion diagnostics
    # NOTE: Strategy reads these as PAIR_SELECTION_LOOKBACK_HOURS_* (not LOOKBACK_HOURS_*).
    # Prior results show 3-5h short works; 1-2h produced mostly -inf.
    "PAIR_SELECTION_LOOKBACK_HOURS_SHORT": ["1", "2", "3", "4", "5"],
    "PAIR_SELECTION_LOOKBACK_HOURS_LONG": ["4", "6", "8", "12"],
    # ADF threshold: 0.05 too strict (-inf), 0.3 too loose.
    "ADF_P_THRESHOLD": ["0.07", "0.1", "0.15"],
    # Half-life: wider range to test slower mean-reversion regimes.
    "HALF_LIFE_MAX_HOURS": ["0.75", "1.25", "2.0"],
    # Re-evaluation and velocity filters
    "REEVAL_JUMP_Z_MULT": ["1.1"],  # fixed
    "SPREAD_VELOCITY_MAX_SIGMA_PER_MIN": ["0.08", "0.12", "0.15"],
    # Volatility lookback and warm start behavior
    "ENTRY_VOL_LOOKBACK_HOURS": ["6"],  # fixed
    "WARM_START_MODE": ["strict"],  # fixed
    "WARM_START_MIN_BARS": ["60", "120"],
    # Spread trend filter: block entry when spread slope / std exceeds threshold.
    "SPREAD_TREND_MAX_SLOPE_SIGMA": ["0.3", "0.5", "0.8", "1.0"],
    # Beta stability filter: block entry when |beta_s - beta_l| / beta_eff exceeds threshold.
    "BETA_DIVERGENCE_MAX": ["0.10", "0.15", "0.20", "0.30"],
    # Circuit breaker: graduated tiers.
    "CIRCUIT_BREAKER_TIER1_LOSSES": ["3"],
    "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS": ["300", "600"],
    "CIRCUIT_BREAKER_TIER2_LOSSES": ["5"],  # fixed: low impact
    "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS": ["1800", "3600"],
    # Post-only → taker hybrid entry timeout (0=disabled removed).
    "ENTRY_POST_ONLY_TIMEOUT_SECS": ["15", "30"],
}

# Parameters expected to be integers in PairTradeConfig.
INT_PARAM_NAMES = {
    "INTERVAL_SECS",
    "TRADING_PERIOD_SECS",
    "METRICS_WINDOW_LENGTH",
    "FORCE_CLOSE_TIME_SECS",
    "COOLDOWN_SECS",
    "PAIR_SELECTION_LOOKBACK_HOURS_SHORT",
    "PAIR_SELECTION_LOOKBACK_HOURS_LONG",
    "ENTRY_VOL_LOOKBACK_HOURS",
    "SLIPPAGE_BPS",
    "MAX_ACTIVE_PAIRS",
    "MAX_LEVERAGE",
    "WARM_START_MIN_BARS",
    "ORDER_TIMEOUT_SECS",
    "ENTRY_PARTIAL_FILL_MAX_RETRIES",
    "STARTUP_FORCE_CLOSE_ATTEMPTS",
    "STARTUP_FORCE_CLOSE_WAIT_SECS",
    "CIRCUIT_BREAKER_CONSECUTIVE_LOSSES",
    "CIRCUIT_BREAKER_COOLDOWN_SECS",
    "CIRCUIT_BREAKER_TIER1_LOSSES",
    "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS",
    "CIRCUIT_BREAKER_TIER2_LOSSES",
    "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS",
    "ENTRY_POST_ONLY_TIMEOUT_SECS",
}

# In backtesting, we use a portion of the dataset for warmup.
# Keep warmup >= long lookback (max 12h) to avoid premature "insufficient history".
WARMUP_DURATION_SECS = 12 * 3600
# Use only the most recent N days of data for optimization (0 = use all).
OPTIMIZER_DATA_TAIL_DAYS = float(os.getenv("OPTIMIZER_DATA_TAIL_DAYS", "30"))
DEFAULT_TRADING_PERIOD_SECS = int(os.getenv("TRADING_PERIOD_SECS", "60"))
MAX_TAIL_BYTES = 2 * 1024 * 1024
ENABLE_REFINEMENT = os.getenv("OPTIMIZER_ENABLE_REFINEMENT", "1") == "1"
FORCE_CLOSE_TIME_MAX_SECS = 3600
REFINE_PARAM_COUNT = int(os.getenv("OPTIMIZER_REFINE_PARAM_COUNT", "3"))
REFINE_SEED_COUNT = int(os.getenv("OPTIMIZER_REFINE_SEED_COUNT", "5"))
REFINE_MAX_RUNS = int(os.getenv("OPTIMIZER_REFINE_MAX_RUNS", "128"))
COMMON_PARAM_MIN_VAL_SCORE = float(os.getenv("COMMON_PARAM_MIN_VAL_SCORE", "0"))
# "per_pair" updates each pair config; "common" tries shared params then falls back to per-pair.
COMMON_PARAM_MODE = os.getenv("COMMON_PARAM_MODE", "per_pair").strip().lower()
# "best_per_pair" uses per-pair winners; "grid" evaluates the full grid for common selection.
COMMON_PARAM_CANDIDATES = os.getenv(
    "COMMON_PARAM_CANDIDATES", "best_per_pair"
).strip().lower()
BACKTEST_LOG_DIR = os.getenv("BACKTEST_LOG_DIR", "/tmp/debot_backtests")
DEX_NAME = os.getenv("DEX_NAME", "debot").strip().lower()
DEFAULT_OPTIMIZER_LOG_PATH = f"/tmp/{DEX_NAME}.log"
OPTIMIZER_LOG_PATH = os.getenv("OPTIMIZER_LOG_PATH", DEFAULT_OPTIMIZER_LOG_PATH)
OPTIMIZER_SEED_FROM_ENV = os.getenv("OPTIMIZER_SEED_FROM_ENV", "1") == "1"
OPTIMIZER_CONFIG_PATH = os.getenv("OPTIMIZER_CONFIG_PATH")
OPTIMIZER_ENV_PATH = os.getenv("OPTIMIZER_ENV_PATH")
OPTIMIZER_MAX_COMBOS = int(os.getenv("OPTIMIZER_MAX_COMBOS", "384"))
OPTIMIZER_COMBO_SAMPLE_SEED = os.getenv("OPTIMIZER_COMBO_SAMPLE_SEED")
OPTIMIZER_SAMPLING_STRATEGY = os.getenv(
    "OPTIMIZER_SAMPLING_STRATEGY", "balanced"
).strip().lower()
OPTIMIZER_SWEEP_ENABLE = os.getenv("OPTIMIZER_SWEEP_ENABLE", "0") == "1"
OPTIMIZER_SWEEP_WINDOW_DAYS = float(os.getenv("OPTIMIZER_SWEEP_WINDOW_DAYS", "1"))
OPTIMIZER_SWEEP_STEP_DAYS = float(os.getenv("OPTIMIZER_SWEEP_STEP_DAYS", "1"))
OPTIMIZER_SWEEP_MAX_COMBOS = int(os.getenv("OPTIMIZER_SWEEP_MAX_COMBOS", "512"))
OPTIMIZER_SWEEP_TOP_K = int(os.getenv("OPTIMIZER_SWEEP_TOP_K", "10"))
OPTIMIZER_SWEEP_REFINEMENT = os.getenv("OPTIMIZER_SWEEP_REFINEMENT", "0") == "1"
OPTIMIZER_SWEEP_FINAL_MAX = int(os.getenv("OPTIMIZER_SWEEP_FINAL_MAX", "50"))
OPTIMIZER_SWEEP_INCLUDE_TAIL = os.getenv("OPTIMIZER_SWEEP_INCLUDE_TAIL", "1") == "1"
OPTIMIZER_SWEEP_LOG_PATH = os.getenv(
    "OPTIMIZER_SWEEP_LOG_PATH", "/tmp/optimizer_sweep_windows.jsonl"
)
OPTIMIZER_SWEEP_MIN_SCORE = float(os.getenv("OPTIMIZER_SWEEP_MIN_SCORE", "0"))
OPTIMIZER_SWEEP_DIVERSE_K = int(os.getenv("OPTIMIZER_SWEEP_DIVERSE_K", "3"))
OPTIMIZER_SWEEP_DIVERSITY_KEYS_RAW = os.getenv("OPTIMIZER_SWEEP_DIVERSITY_KEYS", "")
OPTIMIZER_SWEEP_DIVERSITY_PRIORITY_KEYS_RAW = os.getenv(
    "OPTIMIZER_SWEEP_DIVERSITY_PRIORITY_KEYS", ""
)
OPTIMIZER_SWEEP_DIVERSITY_WEIGHTS_RAW = os.getenv(
    "OPTIMIZER_SWEEP_DIVERSITY_WEIGHTS", ""
)
OPTIMIZER_SWEEP_DIVERSITY_DISTANCE_RAW = os.getenv(
    "OPTIMIZER_SWEEP_DIVERSITY_DISTANCE", ""
)
OPTIMIZER_SWEEP_MIN_SCORE_STEP = float(
    os.getenv("OPTIMIZER_SWEEP_MIN_SCORE_STEP", "1")
)
OPTIMIZER_SWEEP_MIN_SCORE_FLOOR = float(
    os.getenv("OPTIMIZER_SWEEP_MIN_SCORE_FLOOR", "-10")
)
OPTIMIZER_SWEEP_CSV_PATH = os.getenv(
    "OPTIMIZER_SWEEP_CSV_PATH", "/tmp/optimizer_sweep_windows.csv"
)
VALIDATION_WORKERS = os.getenv("VALIDATION_WORKERS")
OPTIMIZER_WORKERS = os.getenv("OPTIMIZER_WORKERS")
VALIDATION_CANDIDATE_WORKERS = os.getenv("VALIDATION_CANDIDATE_WORKERS")
VALIDATION_CANDIDATE_TOP_K = int(os.getenv("VALIDATION_CANDIDATE_TOP_K", "15"))
VALIDATION_CANDIDATE_DIVERSE_K = int(os.getenv("VALIDATION_CANDIDATE_DIVERSE_K", "15"))
VALIDATION_DIVERSITY_KEYS_RAW = os.getenv("VALIDATION_DIVERSITY_KEYS", "")
DEFAULT_VALIDATION_DIVERSITY_KEYS = (
    "ENTRY_Z_SCORE_BASE",
    "EXIT_Z_SCORE",
    "STOP_LOSS_Z_SCORE",
    "FORCE_CLOSE_TIME_SECS",
    "HALF_LIFE_MAX_HOURS",
    "ADF_P_THRESHOLD",
)
DEFAULT_OPTIMIZER_SCORE_ENV = {
    "OPTIMIZER_SCORE_MODE": DEFAULT_SCORE_MODE,
    "OPTIMIZER_RETURN_SCALE": "1000",
    # Minimum 8 trades to produce a finite score.
    # Target is ~288 trades (30min/trade over 6d), so this is a low floor.
    "OPTIMIZER_MIN_TRADES": "8",
    "OPTIMIZER_MAX_DRAWDOWN": "1000",
    "OPTIMIZER_MIN_SHARPE": "0.0",
    "OPTIMIZER_MAX_AVG_HOLD_SECS": "7200",
    "OPTIMIZER_DRAWDOWN_PENALTY": "0.1",
    "OPTIMIZER_AVG_HOLD_PENALTY": "0.001",
    "OPTIMIZER_SHARPE_BONUS": "5.0",
    "OPTIMIZER_MAX_SINGLE_LOSS": "30",
    "OPTIMIZER_CVAR_PCT": "0.05",
    "OPTIMIZER_CVAR_PENALTY": "1.0",
    # Small bonus per trade to mildly reward higher entry frequency.
    "OPTIMIZER_TRADE_FREQ_BONUS": "0.01",
}

LIBSIGNER_ERROR_MARKER = "libsigner.so"
LIBSIGNER_ERROR_PHRASES = (
    "error while loading shared libraries",
    "cannot open shared object file",
)


class FatalBacktestError(RuntimeError):
    pass


def detect_libsigner_error(log_path):
    try:
        with open(log_path, "r", errors="ignore") as f:
            head = f.read(16384)
    except OSError:
        return False
    if LIBSIGNER_ERROR_MARKER not in head:
        return False
    lower = head.lower()
    return any(phrase in lower for phrase in LIBSIGNER_ERROR_PHRASES)


def resolve_lighter_go_path(_config_path=None):
    env_override = os.getenv("LIGHTER_GO_PATH")
    if env_override:
        return env_override
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # Check repo_root/lib first, then fall back to sibling lighter-go dir.
    lib_path = os.path.join(repo_root, "lib")
    if os.path.isfile(os.path.join(lib_path, "libsigner.so")):
        return lib_path
    sibling_path = os.path.join(repo_root, "..", "lighter-go")
    if os.path.isdir(sibling_path):
        return os.path.abspath(sibling_path)
    return lib_path


def ensure_libsigner_available(config_path):
    config = load_config(config_path)
    dex_name = str(config.get("dex_name") or os.getenv("DEX_NAME", DEX_NAME)).strip().lower()
    if dex_name != "lighter":
        return
    lighter_go_path = resolve_lighter_go_path(config_path)
    libsigner_path = os.path.join(lighter_go_path, "libsigner.so")
    if not os.path.isfile(libsigner_path):
        raise FatalBacktestError(
            f"libsigner.so not found at {libsigner_path}. "
            "Build lighter-go or set LIGHTER_GO_PATH before running optimizer."
        )


def update_config_params(config_path, updates):
    if not updates:
        return False

    if not os.path.exists(config_path):
        print(f"Config update skipped: {config_path} not found.", file=sys.stderr)
        return False
    config = load_config(config_path)

    updated = False
    for key, value in updates.items():
        yaml_key = key.lower()
        config[yaml_key] = coerce_yaml_value(key, value)
        updated = True

    if not updated:
        return False

    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return True


def build_analyzer_env():
    env = os.environ.copy()
    for key, value in DEFAULT_OPTIMIZER_SCORE_ENV.items():
        if key not in env or env[key] == "":
            env[key] = value
    return env


def resolve_score_label():
    mode = (os.getenv("OPTIMIZER_SCORE_MODE") or DEFAULT_SCORE_MODE).strip().lower()
    return "return" if mode in RETURN_SCORE_MODES else "pnl"

STRING_PARAM_NAMES = {
    "WARM_START_MODE",
}


def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def coerce_yaml_value(name, value):
    if name in STRING_PARAM_NAMES:
        return str(value)
    if name in INT_PARAM_NAMES:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def load_env_exports(env_path):
    exports = {}
    visited = set()

    def resolve_source_path(raw_path, base_dir):
        raw_path = raw_path.replace("${SCRIPT_HOME}", base_dir).replace(
            "$SCRIPT_HOME", base_dir
        )
        if not os.path.isabs(raw_path):
            return os.path.abspath(os.path.join(base_dir, raw_path))
        return raw_path

    def parse_file(path):
        path = os.path.abspath(path)
        if path in visited:
            return
        visited.add(path)
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("export ") and "=" in line:
                        key, value = line[len("export ") :].split("=", 1)
                        value = value.strip().strip("'").strip('"')
                        exports[key.strip()] = value
                        continue
                    if line.startswith("source ") or line.startswith(". "):
                        parts = line.split(None, 1)
                        if len(parts) < 2:
                            continue
                        raw_path = parts[1].strip()
                        if " #" in raw_path:
                            raw_path = raw_path.split(" #", 1)[0].strip()
                        if (
                            (raw_path.startswith('"') and raw_path.endswith('"'))
                            or (raw_path.startswith("'") and raw_path.endswith("'"))
                        ):
                            raw_path = raw_path[1:-1]
                        source_path = resolve_source_path(raw_path, os.path.dirname(path))
                        parse_file(source_path)
        except FileNotFoundError:
            return

    if env_path:
        parse_file(env_path)
    return exports


def resolve_config_update_targets(config_path):
    config_path = os.path.abspath(config_path)
    base_dir = os.path.dirname(config_path)
    base_name = os.path.basename(config_path)
    base_stem = os.path.splitext(base_name)[0]
    debot_numbered = re.match(r"^debot\\d+$", base_stem) is not None

    def list_matching(prefix):
        targets = []
        try:
            for name in os.listdir(base_dir):
                if not (name.startswith(prefix) and name.endswith(".yaml")):
                    continue
                full_path = os.path.join(base_dir, name)
                if os.path.isfile(full_path):
                    targets.append(full_path)
        except FileNotFoundError:
            return [config_path]
        targets = sorted(set(targets))
        if config_path not in targets and os.path.exists(config_path):
            targets.insert(0, config_path)
        return targets or [config_path]

    if debot_numbered:
        targets = []
        try:
            for name in os.listdir(base_dir):
                if not (name.endswith(".yaml") and re.match(r"^debot\\d+\\.yaml$", name)):
                    continue
                full_path = os.path.join(base_dir, name)
                if os.path.isfile(full_path):
                    targets.append(full_path)
        except FileNotFoundError:
            return [config_path]
        targets = sorted(set(targets))
        if config_path not in targets and os.path.exists(config_path):
            targets.insert(0, config_path)
        return targets or [config_path]

    if base_name.startswith("debot_lighter"):
        return list_matching("debot_lighter")
    return [config_path]


def describe_config_group(config_paths):
    base_names = [os.path.basename(p) for p in config_paths]
    if base_names and all(re.match(r"^debot\\d+\\.yaml$", name) for name in base_names):
        return "debot*.yaml"
    if base_names and all(name.startswith("debot_lighter") for name in base_names):
        return "debot_lighter*.yaml"
    if len(base_names) == 1:
        return base_names[0]
    return "config files"


def load_target_pairs(config_path):
    config = load_config(config_path)
    pairs_raw = normalize_list(config.get("universe_pairs"))
    if pairs_raw:
        return pairs_raw

    symbols_raw = normalize_list(config.get("universe_symbols"))
    if not symbols_raw:
        return []
    pairs = []
    for i, base in enumerate(symbols_raw):
        for quote in symbols_raw[i + 1 :]:
            pairs.append(f"{base}/{quote}")
    return pairs


def map_config_paths_by_pair(config_paths):
    pair_to_configs = {}
    multi_pair_configs = []
    for path in config_paths:
        pairs = load_target_pairs(path)
        if not pairs:
            continue
        if len(pairs) == 1:
            pair_to_configs.setdefault(pairs[0], []).append(path)
        else:
            multi_pair_configs.append((path, pairs))
    return pair_to_configs, multi_pair_configs


def update_config_pair_overrides(config_path, pair_key, updates):
    """Write per-pair parameter overrides into config's pair_overrides section."""
    if not updates:
        return False
    if not os.path.exists(config_path):
        print(f"Config update skipped: {config_path} not found.", file=sys.stderr)
        return False
    config = load_config(config_path)
    if "pair_overrides" not in config or not isinstance(config["pair_overrides"], dict):
        config["pair_overrides"] = {}
    if pair_key not in config["pair_overrides"] or not isinstance(
        config["pair_overrides"][pair_key], dict
    ):
        config["pair_overrides"][pair_key] = {}
    for key, value in updates.items():
        yaml_key = key.lower()
        config["pair_overrides"][pair_key][yaml_key] = coerce_yaml_value(key, value)
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return True


def update_configs_per_pair(
    config_path,
    overall_results,
    train_start,
    train_end,
    val_start,
    val_end,
):
    config_paths = resolve_config_update_targets(config_path)
    pair_to_configs, multi_pair_configs = map_config_paths_by_pair(config_paths)

    updated_any = False
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Handle multi-pair configs: write per-pair overrides
    for multi_path, multi_pairs in multi_pair_configs:
        for pair in multi_pairs:
            # Register multi-pair config as a valid target for each contained pair
            pair_to_configs.setdefault(pair, [])
            if multi_path not in pair_to_configs[pair]:
                pair_to_configs[pair].append(multi_path)

    for pair, result in overall_results.items():
        params = (
            result.get("val_params")
            if val_start and val_end and result.get("val_params") is not None
            else result.get("params")
        )
        if not params:
            print(f"Skipping {pair}: no params.")
            continue
        configs = pair_to_configs.get(pair)
        if not configs:
            print(f"Skipping {pair}: no matching config file.")
            continue
        score = result.get("val_score") if val_start and val_end else result.get("score")
        if score is None:
            print(f"Skipping {pair}: missing score.")
            continue
        if val_start and val_end and score < 0:
            print(f"Skipping {pair}: validation score {score:.4f} < 0.")
            continue

        updated_paths = []
        for path in configs:
            # Check if this config has multiple pairs
            path_pairs = load_target_pairs(path)
            if len(path_pairs) > 1:
                # Multi-pair config: use pair_overrides
                if update_config_pair_overrides(path, pair, params):
                    updated_paths.append(path)
            else:
                # Single-pair config: update top-level params
                if update_config_params(path, params):
                    updated_paths.append(path)

        if updated_paths:
            updated_any = True
            config_list = ", ".join(os.path.basename(p) for p in updated_paths)
            print(
                f"\nUpdated {config_list} with params from {pair} (score {score:.4f})."
            )
            git_commit_and_push(
                repo_root,
                updated_paths,
                pair,
                score,
                train_start,
                train_end,
                val_start,
                val_end,
            )
        else:
            print(f"Skipping {pair}: unable to update config file(s).")
    return updated_any


def try_parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_number(value):
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    return text


def parse_csv_keys(value, default):
    keys = [key.strip() for key in (value or "").split(",") if key.strip()]
    return tuple(keys) if keys else default


def parse_key_weights(value):
    weights = {}
    if not value:
        return weights
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        key, raw = part.split(":", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            continue
        try:
            weights[key] = float(raw)
        except ValueError:
            continue
    return weights


def parse_key_floats(value):
    values = {}
    if not value:
        return values
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        key, raw = part.split(":", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            continue
        try:
            values[key] = float(raw)
        except ValueError:
            continue
    return values


VALIDATION_DIVERSITY_KEYS = parse_csv_keys(
    VALIDATION_DIVERSITY_KEYS_RAW, DEFAULT_VALIDATION_DIVERSITY_KEYS
)
OPTIMIZER_SWEEP_DIVERSITY_KEYS = parse_csv_keys(
    OPTIMIZER_SWEEP_DIVERSITY_KEYS_RAW, DEFAULT_VALIDATION_DIVERSITY_KEYS
)
OPTIMIZER_SWEEP_DIVERSITY_PRIORITY_KEYS = parse_csv_keys(
    OPTIMIZER_SWEEP_DIVERSITY_PRIORITY_KEYS_RAW, tuple()
)
OPTIMIZER_SWEEP_DIVERSITY_WEIGHTS = parse_key_weights(
    OPTIMIZER_SWEEP_DIVERSITY_WEIGHTS_RAW
)
OPTIMIZER_SWEEP_DIVERSITY_DISTANCE = parse_key_floats(
    OPTIMIZER_SWEEP_DIVERSITY_DISTANCE_RAW
)


def iter_param_combinations(param_grid, param_names=None, shuffle=False, rng=None):
    if param_names is None:
        param_names = list(param_grid.keys())
    param_values = [list(param_grid[name]) for name in param_names]
    if shuffle and rng is not None:
        for values in param_values:
            rng.shuffle(values)
    for combo in itertools.product(*param_values):
        params_to_run = dict(zip(param_names, combo))
        if not _combo_passes_constraints(params_to_run):
            continue
        yield params_to_run


def _combo_passes_constraints(combo):
    """Check param constraint validity without full grid enumeration."""
    if float(combo.get("PAIR_SELECTION_LOOKBACK_HOURS_SHORT", 0)) >= float(
        combo.get("PAIR_SELECTION_LOOKBACK_HOURS_LONG", float("inf"))
    ):
        return False
    entry_base = try_parse_float(combo.get("ENTRY_Z_SCORE_BASE"))
    entry_min = try_parse_float(combo.get("ENTRY_Z_SCORE_MIN"))
    entry_max = try_parse_float(combo.get("ENTRY_Z_SCORE_MAX"))
    if entry_min is not None and entry_max is not None and entry_min > entry_max:
        return False
    if entry_base is not None:
        if entry_min is not None and entry_base < entry_min:
            return False
        if entry_max is not None and entry_base > entry_max:
            return False
    # Graduated circuit breaker: tier2 must be stricter than tier1
    cb_t1_losses = try_parse_float(combo.get("CIRCUIT_BREAKER_TIER1_LOSSES"))
    cb_t2_losses = try_parse_float(combo.get("CIRCUIT_BREAKER_TIER2_LOSSES"))
    cb_t1_cd = try_parse_float(combo.get("CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS"))
    cb_t2_cd = try_parse_float(combo.get("CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS"))
    if cb_t1_losses is not None and cb_t2_losses is not None and cb_t2_losses <= cb_t1_losses:
        return False
    if cb_t1_cd is not None and cb_t2_cd is not None and cb_t2_cd <= cb_t1_cd:
        return False
    return True


def _random_combo(param_grid, param_names, rng):
    """Generate a single random combo by picking one value per param."""
    return {name: rng.choice(param_grid[name]) for name in param_names}


def sample_param_combinations_stream(
    param_grid,
    param_names,
    max_samples,
    seed=None,
    strategy="random",
):
    """Sample max_samples combos directly without enumerating the full grid."""
    rng = random.Random(seed) if seed is not None else random.Random()
    selected = []
    selected_keys = set()
    strategy = (strategy or "random").strip().lower()
    max_rejects = max_samples * 200  # safety bound to avoid infinite loops

    if strategy == "balanced":
        # Target: each value of each param appears roughly equally.
        target = {
            name: max(1, max_samples // max(1, len(param_grid.get(name, []))))
            for name in param_names
        }
        counts = {
            name: {val: 0 for val in param_grid.get(name, [])} for name in param_names
        }

        # Phase 1: balanced selection — pick combos that fill under-represented values.
        rejects = 0
        while len(selected) < max_samples and rejects < max_rejects:
            combo = _random_combo(param_grid, param_names, rng)
            if not _combo_passes_constraints(combo):
                rejects += 1
                continue
            key = tuple(combo[name] for name in param_names)
            if key in selected_keys:
                rejects += 1
                continue
            # Accept if any param value is under its target count.
            should_take = any(
                counts[name].get(combo[name], 0) < target[name]
                for name in param_names
            )
            if not should_take:
                rejects += 1
                continue
            selected.append(combo)
            selected_keys.add(key)
            for name in param_names:
                val = combo[name]
                if val in counts[name]:
                    counts[name][val] += 1
            rejects = 0  # reset on success

        # Phase 2: fill remaining slots with any valid unique combo.
        rejects = 0
        while len(selected) < max_samples and rejects < max_rejects:
            combo = _random_combo(param_grid, param_names, rng)
            if not _combo_passes_constraints(combo):
                rejects += 1
                continue
            key = tuple(combo[name] for name in param_names)
            if key in selected_keys:
                rejects += 1
                continue
            selected.append(combo)
            selected_keys.add(key)
            rejects = 0

        # total is unknown without enumeration; report selected count.
        return selected, len(selected)

    # Random strategy: generate unique valid combos directly.
    rejects = 0
    while len(selected) < max_samples and rejects < max_rejects:
        combo = _random_combo(param_grid, param_names, rng)
        if not _combo_passes_constraints(combo):
            rejects += 1
            continue
        key = tuple(combo[name] for name in param_names)
        if key in selected_keys:
            rejects += 1
            continue
        selected.append(combo)
        selected_keys.add(key)
        rejects = 0
    return selected, len(selected)


def build_param_combinations(
    param_grid, max_combos=None, seed=None, sampling_strategy=None
):
    param_names = list(param_grid.keys())
    if max_combos is None:
        max_combos = OPTIMIZER_MAX_COMBOS
    if sampling_strategy is None:
        sampling_strategy = OPTIMIZER_SAMPLING_STRATEGY
    if seed is None and OPTIMIZER_COMBO_SAMPLE_SEED:
        try:
            seed = int(OPTIMIZER_COMBO_SAMPLE_SEED)
        except ValueError:
            seed = None

    if max_combos > 0:
        combinations, total = sample_param_combinations_stream(
            param_grid,
            param_names,
            max_combos,
            seed=seed,
            strategy=sampling_strategy,
        )
        if total > max_combos:
            print(
                "Grid combos capped: sampled "
                f"{len(combinations)} of {total} "
                f"(seed={seed}, strategy={sampling_strategy})"
            )
        return combinations

    return list(iter_param_combinations(param_grid, param_names))


def sample_param_combinations(
    combinations,
    param_names,
    param_grid,
    max_samples,
    seed=None,
):
    if max_samples <= 0 or max_samples >= len(combinations):
        return combinations
    rng = random.Random(seed) if seed is not None else random
    strategy = OPTIMIZER_SAMPLING_STRATEGY
    if strategy not in ("random", "balanced"):
        print(
            f"Unknown OPTIMIZER_SAMPLING_STRATEGY={strategy}; falling back to random."
        )
        strategy = "random"

    if strategy == "random":
        return rng.sample(combinations, max_samples)

    shuffled = list(combinations)
    rng.shuffle(shuffled)

    target = {
        name: max_samples / max(1, len(param_grid.get(name, [])))
        for name in param_names
    }
    counts = {
        name: {val: 0 for val in param_grid.get(name, [])} for name in param_names
    }
    selected = []
    selected_keys = set()

    for combo in shuffled:
        if len(selected) >= max_samples:
            break
        should_take = False
        for name in param_names:
            val = combo[name]
            if counts[name].get(val, 0) < target[name]:
                should_take = True
                break
        if not should_take:
            continue
        key = tuple(combo[name] for name in param_names)
        if key in selected_keys:
            continue
        selected.append(combo)
        selected_keys.add(key)
        for name in param_names:
            val = combo[name]
            if val in counts[name]:
                counts[name][val] += 1

    while len(selected) < max_samples:
        combo = rng.choice(combinations)
        key = tuple(combo[name] for name in param_names)
        if key in selected_keys:
            continue
        selected.append(combo)
        selected_keys.add(key)

    return selected


def window_days(start, end):
    if start is None or end is None:
        return None
    delta = end - start
    seconds = delta.total_seconds()
    if seconds <= 0:
        return None
    return seconds / 86400.0


def git_commit_and_push(
    repo_root,
    config_paths,
    best_pair,
    best_score,
    train_start,
    train_end,
    val_start,
    val_end,
):
    try:
        is_repo = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
        )
        if is_repo.returncode != 0:
            print("Git commit skipped: not a git repository.", file=sys.stderr)
            return False
    except FileNotFoundError:
        print("Git commit skipped: git not available.", file=sys.stderr)
        return False

    rel_paths = [os.path.relpath(path, repo_root) for path in config_paths]
    status = subprocess.run(
        ["git", "-C", repo_root, "status", "--porcelain", "--", *rel_paths],
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        print("Git status failed; skipping commit.", file=sys.stderr)
        if status.stderr:
            print(status.stderr.strip(), file=sys.stderr)
        return False
    if not status.stdout.strip():
        print(
            f"Git commit skipped: no changes to {describe_config_group(config_paths)}."
        )
        return False

    add = subprocess.run(
        ["git", "-C", repo_root, "add", "--", *rel_paths],
        capture_output=True,
        text=True,
    )
    if add.returncode != 0:
        print("Git add failed; skipping commit.", file=sys.stderr)
        if add.stderr:
            print(add.stderr.strip(), file=sys.stderr)
        return False

    train_days = window_days(train_start, train_end)
    val_days = window_days(val_start, val_end)
    score_label = resolve_score_label()
    metric_label = f"val_{score_label}" if val_start and val_end else f"train_{score_label}"
    details = [f"{best_pair}", f"{metric_label} {best_score:.4f}"]
    if train_days is not None:
        details.append(f"train_days {format_number(train_days)}")
    if val_days is not None:
        details.append(f"val_days {format_number(val_days)}")
    commit_message = (
        f"Update {describe_config_group(config_paths)} via optimizer ({', '.join(details)})"
    )
    commit = subprocess.run(
        ["git", "-C", repo_root, "commit", "-m", commit_message],
        capture_output=True,
        text=True,
    )
    if commit.returncode != 0:
        print("Git commit failed.", file=sys.stderr)
        if commit.stderr:
            print(commit.stderr.strip(), file=sys.stderr)
        return False

    push = subprocess.run(
        ["git", "-C", repo_root, "push"],
        capture_output=True,
        text=True,
    )
    if push.returncode != 0:
        print("Git push failed.", file=sys.stderr)
        if push.stderr:
            print(push.stderr.strip(), file=sys.stderr)
        return False

    print("Git commit and push completed.")
    return True


def make_params_key(params):
    return tuple(sorted(params.items()))


def resolve_optimizer_workers(total_runs):
    cpu_count = os.cpu_count() or 1
    default_workers = max(1, cpu_count - 1)
    workers_raw = (OPTIMIZER_WORKERS or "").strip()
    if workers_raw:
        try:
            worker_count = max(1, int(workers_raw))
        except ValueError:
            worker_count = default_workers
    else:
        worker_count = default_workers
    if total_runs:
        worker_count = min(worker_count, total_runs)
    return worker_count


def resolve_validation_candidate_workers(total_candidates):
    cpu_count = os.cpu_count() or 1
    default_workers = max(1, cpu_count)
    workers_raw = (VALIDATION_CANDIDATE_WORKERS or "").strip()
    if workers_raw:
        try:
            worker_count = max(1, int(workers_raw))
        except ValueError:
            worker_count = default_workers
    else:
        worker_count = default_workers
    if total_candidates:
        worker_count = min(worker_count, total_candidates)
    return worker_count


def make_diversity_signature(params, diversity_keys):
    return tuple((key, params.get(key)) for key in diversity_keys)


def bucket_diversity_value(value, distance):
    if distance is None or distance <= 0:
        return value
    try:
        num = float(value)
    except (TypeError, ValueError):
        return value
    bucket = math.floor(num / distance)
    return (bucket * distance)


def make_diversity_signature_with_distance(params, diversity_keys, distance_map):
    signature = []
    for key in diversity_keys:
        distance = distance_map.get(key)
        raw = params.get(key)
        signature.append((key, bucket_diversity_value(raw, distance)))
    return tuple(signature)


def select_validation_candidates(results, top_k, diverse_k, diversity_keys):
    if not results:
        return []

    sorted_results = sorted(results, key=lambda item: item[1], reverse=True)
    selected = []
    selected_keys = set()
    selected_signatures = set()

    top_k = max(0, int(top_k))
    diverse_k = max(0, int(diverse_k))

    for params, _score in sorted_results:
        if len(selected) >= top_k:
            break
        key = make_params_key(params)
        if key in selected_keys:
            continue
        selected.append(params)
        selected_keys.add(key)
        selected_signatures.add(make_diversity_signature(params, diversity_keys))

    if diverse_k > 0:
        for params, _score in sorted_results:
            if len(selected) >= top_k + diverse_k:
                break
            key = make_params_key(params)
            if key in selected_keys:
                continue
            signature = make_diversity_signature(params, diversity_keys)
            if signature in selected_signatures:
                continue
            selected.append(params)
            selected_keys.add(key)
            selected_signatures.add(signature)

    if not selected and sorted_results:
        selected.append(sorted_results[0][0])

    return selected


def select_top_candidates(results, top_k):
    if not results:
        return []
    top_k = max(0, int(top_k))
    filtered = [
        (params, score)
        for params, score in results
        if score is not None and score != -float("inf")
    ]
    if not filtered:
        return []
    filtered.sort(key=lambda item: item[1], reverse=True)
    selected = []
    seen = set()
    for params, _score in filtered:
        key = make_params_key(params)
        if key in seen:
            continue
        selected.append(params)
        seen.add(key)
        if top_k and len(selected) >= top_k:
            break
    return selected


def select_top_candidates_with_scores(results, top_k):
    if not results:
        return []
    top_k = max(0, int(top_k))
    filtered = [
        (params, score)
        for params, score in results
        if score is not None and score != -float("inf")
    ]
    if not filtered:
        return []
    filtered.sort(key=lambda item: item[1], reverse=True)
    selected = []
    seen = set()
    for params, score in filtered:
        key = make_params_key(params)
        if key in seen:
            continue
        selected.append((params, score))
        seen.add(key)
        if top_k and len(selected) >= top_k:
            break
    return selected


def select_sweep_candidates(
    results,
    top_k,
    diverse_k,
    diversity_keys,
    min_score,
    priority_keys=None,
    weights=None,
    distance_map=None,
):
    if not results:
        return []
    top_k = max(0, int(top_k))
    diverse_k = max(0, int(diverse_k))
    priority_keys = tuple(priority_keys or ())
    weights = weights or {}

    filtered = []
    current_min = min_score
    target_count = top_k + diverse_k
    while True:
        filtered = [
            (params, score)
            for params, score in results
            if score is not None and score != -float("inf") and score >= current_min
        ]
        if filtered or current_min <= OPTIMIZER_SWEEP_MIN_SCORE_FLOOR:
            break
        current_min -= OPTIMIZER_SWEEP_MIN_SCORE_STEP
        if current_min < OPTIMIZER_SWEEP_MIN_SCORE_FLOOR:
            current_min = OPTIMIZER_SWEEP_MIN_SCORE_FLOOR
        if target_count and len(filtered) >= target_count:
            break

    if not filtered:
        return []
    filtered.sort(key=lambda item: item[1], reverse=True)

    selected = []
    selected_keys = set()
    selected_signatures = set()
    seen_values = {key: set() for key in diversity_keys}
    distance_map = distance_map or {}

    for params, score in filtered:
        if len(selected) >= top_k:
            break
        key = make_params_key(params)
        if key in selected_keys:
            continue
        selected.append((params, score))
        selected_keys.add(key)
        selected_signatures.add(
            make_diversity_signature_with_distance(
                params, diversity_keys, distance_map
            )
        )
        for dk in diversity_keys:
            seen_values.setdefault(dk, set()).add(
                bucket_diversity_value(params.get(dk), distance_map.get(dk))
            )

    if diverse_k > 0:
        candidates = []
        for params, score in filtered:
            key = make_params_key(params)
            if key in selected_keys:
                continue
            signature = make_diversity_signature_with_distance(
                params, diversity_keys, distance_map
            )
            if signature in selected_signatures:
                continue
            diversity_score = 0.0
            for dk in diversity_keys:
                candidate_val = bucket_diversity_value(
                    params.get(dk), distance_map.get(dk)
                )
                if candidate_val in seen_values.get(dk, set()):
                    continue
                weight = weights.get(dk)
                if weight is None:
                    weight = 2.0 if dk in priority_keys else 1.0
                diversity_score += weight
            candidates.append((diversity_score, score, params))

        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        for diversity_score, score, params in candidates:
            if len(selected) >= top_k + diverse_k:
                break
            key = make_params_key(params)
            if key in selected_keys:
                continue
            selected.append((params, score))
            selected_keys.add(key)
            selected_signatures.add(
                make_diversity_signature_with_distance(
                    params, diversity_keys, distance_map
                )
            )
            for dk in diversity_keys:
                seen_values.setdefault(dk, set()).add(
                    bucket_diversity_value(params.get(dk), distance_map.get(dk))
                )

    if not selected and filtered:
        selected.append(filtered[0])
    return selected


def log_sweep_window(pair_str, win_start, win_end, candidates):
    if not OPTIMIZER_SWEEP_LOG_PATH:
        return
    try:
        payload = {
            "pair": pair_str,
            "window_start": format_timestamp(win_start),
            "window_end": format_timestamp(win_end),
            "top_k": OPTIMIZER_SWEEP_TOP_K,
            "candidates": [
                {"score": score, "params": params} for params, score in candidates
            ],
        }
        with open(OPTIMIZER_SWEEP_LOG_PATH, "a") as f:
            f.write(json.dumps(payload, ensure_ascii=True))
            f.write("\n")
    except Exception:
        return

    if not OPTIMIZER_SWEEP_CSV_PATH:
        return
    try:
        file_exists = os.path.exists(OPTIMIZER_SWEEP_CSV_PATH)
        with open(OPTIMIZER_SWEEP_CSV_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    ["pair", "window_start", "window_end", "score", "params_json"]
                )
            for params, score in candidates:
                writer.writerow(
                    [
                        pair_str,
                        format_timestamp(win_start),
                        format_timestamp(win_end),
                        score,
                        json.dumps(params, ensure_ascii=True),
                    ]
                )
    except Exception:
        return


def select_best_overall(overall_results):
    best_pair = None
    best_params = None
    best_score = -float("inf")
    for pair, result in overall_results.items():
        score = result.get("val_score")
        params = result.get("val_params") if score is not None else result.get("params")
        if score is None:
            score = result.get("score")
        if params is None or score is None:
            continue
        if score > best_score:
            best_pair = pair
            best_params = params
            best_score = score
    return best_pair, best_params, best_score


def select_best_common_params(
    pairs,
    candidate_params,
    pnl_start_time,
    pnl_end_time,
    min_score,
):
    best_params = None
    best_avg = -float("inf")
    best_worst = None
    total_candidates = len(candidate_params)
    cpu_count = os.cpu_count() or 1
    default_workers = max(1, cpu_count - 1)
    validation_workers = default_workers
    validation_workers_raw = (VALIDATION_WORKERS or "").strip()
    if validation_workers_raw:
        try:
            validation_workers = max(1, int(validation_workers_raw))
        except ValueError:
            validation_workers = default_workers
    if pairs:
        validation_workers = min(validation_workers, len(pairs))
    else:
        validation_workers = 1
    for idx, params in enumerate(candidate_params, start=1):
        print(f"\n  [Common Eval {idx}/{total_candidates}] params={params}")
        scores = []
        worst = float("inf")
        if validation_workers == 1:
            for pair in pairs:
                score = evaluate_params(pair, params, pnl_start_time, pnl_end_time)
                if score is None:
                    scores = None
                    break
                scores.append(score)
                worst = min(worst, score)
        else:
            scores_by_pair = {}
            failed = False
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=validation_workers
            ) as executor:
                future_to_pair = {
                    executor.submit(
                        evaluate_params, pair, params, pnl_start_time, pnl_end_time
                    ): pair
                    for pair in pairs
                }
                for future in concurrent.futures.as_completed(future_to_pair):
                    pair = future_to_pair[future]
                    try:
                        score = future.result()
                    except Exception as e:
                        print(
                            f"  > Common eval failed for {pair}: {e}",
                            file=sys.stderr,
                        )
                        failed = True
                        continue
                    if score is None:
                        failed = True
                        continue
                    scores_by_pair[pair] = score
            if failed or len(scores_by_pair) != len(pairs):
                continue
            for pair in pairs:
                score = scores_by_pair[pair]
                scores.append(score)
                worst = min(worst, score)
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        print(f"  > Common Eval summary: avg={avg:.4f} worst={worst:.4f}")
        if worst < min_score:
            print(f"  > Rejected: worst score {worst:.4f} < min {min_score:.4f}")
            continue
        if avg > best_avg:
            best_avg = avg
            best_params = params
            best_worst = worst
    return best_params, best_avg, best_worst


def send_completion_email(subject, body):
    user = os.getenv("GMAIL_USER")
    to_address = os.getenv("TO_ADDRESS")
    app_password = os.getenv("GMAIL_APP_PASSWORD")
    if not (user and to_address and app_password):
        secrets_env = os.getenv("OPTIMIZER_SECRETS_ENV") or os.getenv("DEBOT_ENV")
        exports = load_env_exports(secrets_env)
        if exports:
            user = user or exports.get("GMAIL_USER")
            to_address = to_address or exports.get("TO_ADDRESS")
            app_password = app_password or exports.get("GMAIL_APP_PASSWORD")
    if not (user and to_address and app_password):
        print(
            "Email notification skipped: missing GMAIL_USER/TO_ADDRESS/GMAIL_APP_PASSWORD.",
            file=sys.stderr,
        )
        return False

    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to_address
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(user, app_password)
            smtp.send_message(msg)
        print("  > Email notification sent.")
        return True
    except Exception as e:
        print(f"Email notification failed: {e}", file=sys.stderr)
        return False


def parse_timestamp_ms(line):
    match = re.search(r'"timestamp"\s*:\s*(\d+)', line)
    if not match:
        return None
    return int(match.group(1))


def read_last_line(path, chunk_size=8192):
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if end == 0:
            return ""
        pos = end
        buffer = b""
        while pos > 0:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size)
            buffer = data + buffer
            if b"\n" in data:
                break
        return buffer.splitlines()[-1].decode("utf-8", errors="ignore")


def estimate_data_bars(data_file):
    try:
        with open(data_file, "r") as f:
            first_line = f.readline()
        first_ts = parse_timestamp_ms(first_line)
        if first_ts is None:
            return None
        last_line = read_last_line(data_file)
        last_ts = parse_timestamp_ms(last_line)
        if last_ts is None or last_ts <= first_ts:
            return None
        duration_secs = (last_ts - first_ts) / 1000.0
        bars = int(duration_secs // DEFAULT_TRADING_PERIOD_SECS)
        return bars
    except Exception:
        return None


def get_data_time_bounds(data_file):
    try:
        with open(data_file, "r") as f:
            first_line = f.readline()
        first_ts = parse_timestamp_ms(first_line)
        if first_ts is None:
            return None
        last_line = read_last_line(data_file)
        last_ts = parse_timestamp_ms(last_line)
        if last_ts is None or last_ts <= first_ts:
            return None
        start_dt = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        return start_dt, end_dt
    except Exception:
        return None


def convert_to_bincode(jsonl_path):
    """Convert JSONL data file to bincode format for faster loading."""
    bin_path = jsonl_path.rsplit(".", 1)[0] + ".bin"
    if os.path.exists(bin_path) and os.path.getmtime(bin_path) >= os.path.getmtime(jsonl_path):
        print(f"Bincode file up-to-date: {bin_path}")
        return bin_path
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    converter = os.path.join(repo_root, "target", "release", "convert-data")
    if not os.path.isfile(converter):
        print(f"convert-data binary not found at {converter}; skipping bincode conversion.")
        return jsonl_path
    try:
        result = subprocess.run(
            [converter, jsonl_path, bin_path],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            print(f"Bincode conversion failed: {result.stderr.strip()}", file=sys.stderr)
            return jsonl_path
        print(f"Converted to bincode: {bin_path}")
        return bin_path
    except Exception as e:
        print(f"Bincode conversion error: {e}", file=sys.stderr)
        return jsonl_path


def snapshot_data_file(source_path):
    if not source_path or not os.path.exists(source_path):
        return None

    snapshot_path = None
    try:
        base = os.path.basename(source_path)
        suffix = os.path.splitext(base)[1] or ".jsonl"
        with tempfile.NamedTemporaryFile(
            prefix="debot_data_snapshot_",
            suffix=suffix,
            dir="/tmp",
            delete=False,
        ) as tmp:
            snapshot_path = tmp.name
        shutil.copy2(source_path, snapshot_path)
        return snapshot_path
    except Exception as e:
        if snapshot_path and os.path.exists(snapshot_path):
            try:
                os.remove(snapshot_path)
            except OSError:
                pass
        print(f"Data snapshot failed: {e}; using original file.", file=sys.stderr)
        return None


def format_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


DECIMAL_INTERP_KEYS = ("price", "funding_rate", "bid_size", "ask_size")


def try_parse_decimal(value):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def decimal_places(value):
    if value is None:
        return None
    text = str(value)
    if "e" in text.lower():
        return None
    if "." in text:
        return len(text.split(".", 1)[1])
    return 0


def format_decimal(value, scale=None):
    try:
        if scale is not None:
            quant = Decimal(1).scaleb(-scale)
            value = value.quantize(quant)
        return format(value, "f")
    except (InvalidOperation, ValueError):
        return format(value, "f")


def interpolate_decimal(prev_val, next_val, fraction):
    prev_dec = try_parse_decimal(prev_val)
    next_dec = try_parse_decimal(next_val)
    if prev_dec is None or next_dec is None:
        return None
    prev_scale = decimal_places(prev_val)
    next_scale = decimal_places(next_val)
    if prev_scale is None and next_scale is None:
        scale = None
    else:
        scale = max(prev_scale or 0, next_scale or 0)
    try:
        interpolated = prev_dec + (next_dec - prev_dec) * fraction
    except (InvalidOperation, ValueError):
        return None
    return format_decimal(interpolated, scale)


def interpolate_prices(prev_prices, next_prices, fraction):
    out = {}
    symbols = set(prev_prices.keys()) | set(next_prices.keys())
    for symbol in symbols:
        prev_sym = prev_prices.get(symbol)
        next_sym = next_prices.get(symbol)
        if prev_sym is None:
            out[symbol] = dict(next_sym)
            continue
        if next_sym is None:
            out[symbol] = dict(prev_sym)
            continue
        merged = dict(prev_sym)
        for key, val in next_sym.items():
            merged.setdefault(key, val)
        for key in DECIMAL_INTERP_KEYS:
            if key in prev_sym and key in next_sym:
                interp = interpolate_decimal(prev_sym[key], next_sym[key], fraction)
                if interp is not None:
                    merged[key] = interp
        out[symbol] = merged
    return out


def preprocess_data_dump_file(data_file):
    if os.getenv("OPTIMIZER_GAP_PREPROCESS", "1") != "1":
        return None
    if not data_file or not os.path.exists(data_file):
        print(
            f"Gap preprocessing skipped: data file not found ({data_file}).",
            file=sys.stderr,
        )
        return None

    try:
        expected_secs = int(
            os.getenv("OPTIMIZER_GAP_EXPECTED_SECS", DEFAULT_TRADING_PERIOD_SECS)
        )
    except ValueError:
        expected_secs = DEFAULT_TRADING_PERIOD_SECS
    if expected_secs <= 0:
        print("Gap preprocessing skipped: invalid expected interval.", file=sys.stderr)
        return None

    try:
        gap_fill_max_secs = int(
            os.getenv(
                "OPTIMIZER_GAP_FILL_MAX_SECS", str(DEFAULT_TRADING_PERIOD_SECS * 2)
            )
        )
    except ValueError:
        gap_fill_max_secs = DEFAULT_TRADING_PERIOD_SECS * 2
    gap_fill_max_secs = max(0, gap_fill_max_secs)
    gap_fill_max_ms = gap_fill_max_secs * 1000
    expected_ms = expected_secs * 1000

    fill_mode = os.getenv("OPTIMIZER_GAP_FILL_MODE", "linear").strip().lower()
    if fill_mode not in ("linear", "forward"):
        fill_mode = "linear"

    output_dir = os.getenv("OPTIMIZER_GAP_OUTPUT_DIR", "/tmp")
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError:
        output_dir = "/tmp"

    segments = []
    stats = {
        "filled_gaps": 0,
        "filled_entries": 0,
        "split_gaps": 0,
        "non_monotonic": 0,
        "parse_errors": 0,
        "lines": 0,
    }

    def open_segment():
        tmp = tempfile.NamedTemporaryFile(
            prefix="debot_data_segment_",
            suffix=".jsonl",
            dir=output_dir,
            delete=False,
            mode="w",
        )
        seg = {"path": tmp.name, "file": tmp, "count": 0, "start_ts": None, "end_ts": None}
        segments.append(seg)
        return seg

    def close_segment(seg):
        try:
            seg["file"].close()
        except Exception:
            pass

    def write_entry(seg, entry):
        seg["file"].write(json.dumps(entry, ensure_ascii=True))
        seg["file"].write("\n")
        seg["count"] += 1
        ts = entry.get("timestamp")
        if ts is not None:
            if seg["start_ts"] is None:
                seg["start_ts"] = ts
            seg["end_ts"] = ts

    prev_entry = None
    prev_ts = None
    seg = None
    try:
        with open(data_file, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                stats["lines"] += 1
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    stats["parse_errors"] += 1
                    continue
                if not isinstance(entry, dict):
                    stats["parse_errors"] += 1
                    continue
                try:
                    ts = int(entry.get("timestamp"))
                except (TypeError, ValueError):
                    stats["parse_errors"] += 1
                    continue
                if seg is None:
                    seg = open_segment()
                if prev_ts is not None:
                    delta = ts - prev_ts
                    if delta <= 0:
                        stats["non_monotonic"] += 1
                        close_segment(seg)
                        seg = open_segment()
                        prev_entry = None
                        prev_ts = None
                    elif expected_ms > 0 and delta <= gap_fill_max_ms:
                        missing = int(delta // expected_ms) - 1
                        if missing > 0 and prev_entry is not None:
                            stats["filled_gaps"] += 1
                            for i in range(1, missing + 1):
                                interp_ts = prev_ts + expected_ms * i
                                if fill_mode == "linear":
                                    fraction = Decimal(i) / Decimal(missing + 1)
                                    prices = interpolate_prices(
                                        prev_entry.get("prices") or {},
                                        entry.get("prices") or {},
                                        fraction,
                                    )
                                else:
                                    prices = dict(prev_entry.get("prices") or {})
                                filler = {"timestamp": interp_ts, "prices": prices}
                                write_entry(seg, filler)
                                stats["filled_entries"] += 1
                    elif delta > gap_fill_max_ms:
                        stats["split_gaps"] += 1
                        close_segment(seg)
                        seg = open_segment()
                        prev_entry = None
                        prev_ts = None
                entry_copy = dict(entry)
                entry_copy["timestamp"] = ts
                if not isinstance(entry_copy.get("prices"), dict):
                    entry_copy["prices"] = {}
                write_entry(seg, entry_copy)
                prev_entry = entry_copy
                prev_ts = ts
    finally:
        if seg is not None:
            close_segment(seg)

    if not segments:
        print("Gap preprocessing produced no usable segments.", file=sys.stderr)
        return None

    for seg_info in segments:
        if seg_info["start_ts"] is None or seg_info["end_ts"] is None:
            seg_info["duration_ms"] = 0
        else:
            seg_info["duration_ms"] = max(0, seg_info["end_ts"] - seg_info["start_ts"])

    best_segment = max(segments, key=lambda s: (s["duration_ms"], s["count"]))
    selected_path = best_segment["path"]

    print(
        "Gap preprocessing summary: "
        f"segments={len(segments)} filled_gaps={stats['filled_gaps']} "
        f"filled_entries={stats['filled_entries']} split_gaps={stats['split_gaps']} "
        f"non_monotonic={stats['non_monotonic']} parse_errors={stats['parse_errors']} "
        f"output_dir={output_dir}"
    )
    if len(segments) > 1:
        for seg_info in segments:
            duration_secs = seg_info["duration_ms"] / 1000.0
            print(
                f"  segment={seg_info['path']} bars={seg_info['count']} "
                f"duration_secs={duration_secs:.1f}"
            )
        print(f"Selected segment for optimization: {selected_path}")

    return selected_path


def build_walk_forward_windows(data_start, data_end):
    # Truncate to the most recent OPTIMIZER_DATA_TAIL_DAYS if set.
    if OPTIMIZER_DATA_TAIL_DAYS > 0:
        tail_start = data_end - timedelta(days=OPTIMIZER_DATA_TAIL_DAYS)
        if tail_start > data_start:
            data_start = tail_start
    mid = data_start + (data_end - data_start) / 2
    train_start = data_start + timedelta(seconds=WARMUP_DURATION_SECS)
    train_end = mid
    val_start = mid
    val_end = data_end
    if train_start >= train_end:
        print(
            "Warning: dataset shorter than warmup; using full window without validation.",
            file=sys.stderr,
        )
        train_start = data_start
        train_end = data_end
        val_start = None
        val_end = None
    return train_start, train_end, val_start, val_end


def build_sweep_windows(train_start, train_end):
    window_days = OPTIMIZER_SWEEP_WINDOW_DAYS
    step_days = OPTIMIZER_SWEEP_STEP_DAYS
    if window_days <= 0:
        return [(train_start, train_end)]
    if step_days <= 0:
        step_days = window_days

    window_secs = window_days * 86400.0
    step_secs = step_days * 86400.0
    total_secs = (train_end - train_start).total_seconds()
    if total_secs <= 0 or total_secs < window_secs:
        return [(train_start, train_end)]

    windows = []
    cursor = train_start
    while cursor + timedelta(seconds=window_secs) <= train_end:
        end = cursor + timedelta(seconds=window_secs)
        windows.append((cursor, end))
        cursor = cursor + timedelta(seconds=step_secs)
    if OPTIMIZER_SWEEP_INCLUDE_TAIL and windows:
        last_end = windows[-1][1]
        if last_end < train_end:
            tail_start = train_end - timedelta(seconds=window_secs)
            if tail_start < train_start:
                tail_start = train_start
            tail_window = (tail_start, train_end)
            if tail_window not in windows:
                windows.append(tail_window)
    if not windows:
        windows.append((train_start, train_end))
    return windows


def score_param_importance(results, param_grid):
    param_scores = {}
    for params, score in results:
        if score == -float("inf"):
            continue
        for key, value in params.items():
            param_scores.setdefault(key, {}).setdefault(value, []).append(score)

    importance = []
    for key, value_scores in param_scores.items():
        if len(value_scores) < 2:
            continue
        avg_scores = {
            val: sum(scores) / len(scores) for val, scores in value_scores.items()
        }
        score_range = max(avg_scores.values()) - min(avg_scores.values())
        best_value = max(avg_scores.items(), key=lambda item: item[1])[0]
        importance.append((key, score_range, best_value))

    importance.sort(key=lambda item: item[1], reverse=True)
    return importance


def build_refined_values(param_name, base_value, grid_values):
    max_val = FORCE_CLOSE_TIME_MAX_SECS if param_name == "FORCE_CLOSE_TIME_SECS" else None
    is_int_param = param_name in INT_PARAM_NAMES

    def round_half_up(val):
        if val >= 0:
            return int(math.floor(val + 0.5))
        return int(math.ceil(val - 0.5))

    def normalize_value(val):
        if is_int_param:
            return round_half_up(val)
        return val

    def format_value(val):
        if is_int_param:
            return str(int(val))
        return format_number(val)

    def within_bounds(val):
        if val <= 0:
            return False
        if max_val is not None and val > max_val:
            return False
        return True

    def base_within_bounds():
        if not is_int_param and max_val is None:
            return [base_value]
        base_num = try_parse_float(base_value)
        if base_num is None:
            return []
        base_num = normalize_value(base_num)
        if not within_bounds(base_num):
            return []
        return [format_value(base_num)]

    numeric_grid = []
    for value in grid_values:
        parsed = try_parse_float(value)
        if parsed is None:
            continue
        parsed = normalize_value(parsed)
        if not within_bounds(parsed):
            continue
        numeric_grid.append(parsed)
    if not numeric_grid:
        return base_within_bounds()

    numeric_grid = sorted(set(numeric_grid))
    if len(numeric_grid) < 2:
        return base_within_bounds()

    diffs = [b - a for a, b in zip(numeric_grid, numeric_grid[1:]) if b > a]
    step = min(diffs) if diffs else 0.0
    if step <= 0:
        return base_within_bounds()

    base_num = try_parse_float(base_value)
    if base_num is None:
        return base_within_bounds()
    base_num = normalize_value(base_num)
    if not within_bounds(base_num):
        return base_within_bounds()

    if is_int_param:
        candidates = [base_num - step, base_num, base_num + step]
    else:
        candidates = [base_num - step / 2, base_num, base_num + step / 2]
    refined = []
    for val in candidates:
        if not within_bounds(val):
            continue
        refined.append(format_value(val))
    return sorted(set(refined), key=lambda v: float(v))


def resolve_config_path():
    if OPTIMIZER_CONFIG_PATH:
        return os.path.abspath(OPTIMIZER_CONFIG_PATH)
    if OPTIMIZER_ENV_PATH:
        return os.path.abspath(OPTIMIZER_ENV_PATH)
    if os.getenv("PAIRTRADE_CONFIG_PATH"):
        return os.path.abspath(os.getenv("PAIRTRADE_CONFIG_PATH"))
    dex_name = os.getenv("DEX_NAME", "").strip().lower()
    basename = "debot00.yaml"
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(repo_root, "configs", "pairtrade", basename)


def seed_param_grid_from_config(param_grid, config_path, pair_key=None):
    config = load_config(config_path)
    if not config:
        return param_grid

    # If pair_key is given, merge pair_overrides on top of global config values
    effective_config = dict(config)
    if pair_key:
        overrides = config.get("pair_overrides", {})
        if isinstance(overrides, dict):
            pair_ovr = overrides.get(pair_key, {})
            if isinstance(pair_ovr, dict):
                effective_config.update(pair_ovr)

    seeded = {}
    for name, values in param_grid.items():
        new_values = list(values)
        config_val = effective_config.get(name.lower())
        if config_val is None:
            seeded[name] = new_values
            continue
        candidates = build_refined_values(name, str(config_val), new_values)
        for val in candidates:
            if val not in new_values:
                new_values.append(val)
        seeded[name] = new_values
    return seeded


def build_refined_param_sets(
    results,
    param_grid,
    param_names,
    max_runs,
):
    importance = score_param_importance(results, param_grid)
    if not importance:
        return []
    selected_params = [item[0] for item in importance[:REFINE_PARAM_COUNT]]

    top_results = sorted(results, key=lambda item: item[1], reverse=True)[
        :REFINE_SEED_COUNT
    ]

    refined_sets = []
    seen = {tuple(sorted(params.items())) for params, _score in results}
    for params, _score in top_results:
        value_lists = []
        for name in param_names:
            base_value = params[name]
            if name in selected_params:
                refined_values = build_refined_values(
                    name, base_value, param_grid[name]
                )
                value_lists.append(refined_values)
            else:
                value_lists.append([base_value])

        for combo in itertools.product(*value_lists):
            candidate = dict(zip(param_names, combo))
            key = tuple(sorted(candidate.items()))
            if key in seen:
                continue
            seen.add(key)
            refined_sets.append(candidate)
            if max_runs and len(refined_sets) >= max_runs:
                return refined_sets

    return refined_sets


def estimate_progress_from_log(backtest_log_file, expected_bars):
    try:
        with open(backtest_log_file, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(size - MAX_TAIL_BYTES, 0))
            tail = f.read().decode("utf-8", errors="ignore")
        pattern = re.compile(r"insufficient history .*?\(([^:]+):(\d+),")
        last_match = None
        for match in pattern.finditer(tail):
            last_match = match
        if not last_match:
            return None
        bars_seen = int(last_match.group(2))
        if not expected_bars:
            return f"bars_seen={bars_seen}"
        pct = min(100.0, (bars_seen / expected_bars) * 100.0)
        return f"{pct:.1f}% ({bars_seen}/{expected_bars} bars)"
    except Exception:
        return None


# Keep backtest logs only when explicitly requested.
KEEP_BACKTEST_LOG = os.getenv("CLEAN_BACKTEST_LOG", "1") != "1"


def gather_data(target_pairs):
    """
    Runs the bot in live mode to gather market data if the dump file doesn't exist.
    """
    if os.path.exists(DATA_DUMP_FILE):
        print(f"Data file '{DATA_DUMP_FILE}' already exists. Skipping data gathering.")
        return

    print(
        f"--- Starting Data Gathering for {DATA_GATHERING_DURATION_SECS / 3600:.1f} hours ---"
    )

    env = os.environ.copy()
    env.update(
        {
            "DRY_RUN": "true",
            "ENABLE_DATA_DUMP": "true",
            "DATA_DUMP_FILE": DATA_DUMP_FILE,
            "DISABLE_HISTORY_PERSIST": "1",
            "RUST_LOG": "info,debot=info",
            "BOT_EXECUTABLE": os.path.abspath(
                os.path.join(os.path.dirname(__file__), "run_pairtrade.sh")
            ),
            **(
                {"UNIVERSE_SYMBOLS": os.environ["UNIVERSE_SYMBOLS"], "UNIVERSE_PAIRS": ""}
                if os.environ.get("UNIVERSE_SYMBOLS")
                else {"UNIVERSE_PAIRS": os.environ.get("UNIVERSE_PAIRS", "") or ",".join(target_pairs)}
            ),
            "RESTART_GUARD_DIR": BACKTEST_LOG_DIR,
            "RESTART_GUARD_KEY": "optimizer_data_gathering",
        }
    )

    bot_process = None
    try:
        with open("data_gathering.log", "w") as log_file:
            bot_process = subprocess.Popen(
                [env["BOT_EXECUTABLE"]],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            print(f"  > Data gathering process started (PID: {bot_process.pid})...")
            bot_process.wait(timeout=DATA_GATHERING_DURATION_SECS)
    except subprocess.TimeoutExpired:
        print(f"  > Data gathering time limit reached. Terminating process.")
    except Exception as e:
        print(f"  > An error occurred during data gathering: {e}", file=sys.stderr)
    finally:
        if bot_process and bot_process.poll() is None:
            bot_process.terminate()
            try:
                bot_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                bot_process.kill()
    print("--- Data Gathering Finished ---")


def ensure_backtest_log_dir():
    os.makedirs(BACKTEST_LOG_DIR, exist_ok=True)


def cleanup_restart_counters():
    ensure_backtest_log_dir()
    prefix = "debot_restart_counter_optimizer_"
    for name in os.listdir(BACKTEST_LOG_DIR):
        if not name.startswith(prefix):
            continue
        path = os.path.join(BACKTEST_LOG_DIR, name)
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass


def get_latest_log_path(pair_str, suffix=None):
    safe_pair = pair_str.replace("/", "_")
    name = f"debot_backtest_{safe_pair}"
    if suffix:
        name = f"{name}_{suffix}"
    return os.path.join(BACKTEST_LOG_DIR, f"{name}.log")


def run_backtest(
    params, pair_str, backtest_log_file, pnl_start_time=None, pnl_end_time=None
):
    """
    Runs a single backtest using the collected data file.
    """
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    binary_path = os.path.join(repo_root, "target", "release", "debot")
    lighter_go_path = resolve_lighter_go_path()
    ld_path = lighter_go_path + ":" + os.environ.get("LD_LIBRARY_PATH", "")

    env = os.environ.copy()
    env.update(
        {
            "BACKTEST_MODE": "true",
            "BACKTEST_FILE": DATA_DUMP_FILE,
            "DRY_RUN": "true",
            "RUST_LOG": "warn,debot::pairtrade=info",
            "LD_LIBRARY_PATH": ld_path,
            "UNIVERSE_PAIRS": pair_str,
            "RESTART_GUARD_DIR": BACKTEST_LOG_DIR,
            "RESTART_GUARD_KEY": f"optimizer_{pair_str.replace('/', '_')}_{uuid.uuid4().hex}",
        }
    )
    env.update(params)

    try:
        with open(backtest_log_file, "w") as log_file:
            subprocess.run(
                [binary_path],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3600,  # allow enough time for full backtest over 7d dataset
            )
    except subprocess.TimeoutExpired as e:
        expected_bars = estimate_data_bars(DATA_DUMP_JSONL)
        progress = estimate_progress_from_log(backtest_log_file, expected_bars)
        if progress:
            print(
                f"      > Backtest timed out after {e.timeout}s (progress {progress}).",
                file=sys.stderr,
            )
        else:
            print(
                f"      > Backtest timed out after {e.timeout}s.",
                file=sys.stderr,
            )
        return -float("inf"), "timeout"
    except Exception as e:
        return -float("inf"), f"backtest_error: {e}"

    if detect_libsigner_error(backtest_log_file):
        raise FatalBacktestError(
            f"Backtest failed due to missing libsigner.so "
            f"(log: {backtest_log_file})."
        )

    try:
        analyzer_path = os.path.join(os.path.dirname(__file__), "log_analyzer.py")

        if pnl_start_time is None:
            bounds = get_data_time_bounds(DATA_DUMP_JSONL)
            if bounds is None:
                raise ValueError("Cannot determine data time bounds.")
            pnl_start_time = bounds[0] + timedelta(seconds=WARMUP_DURATION_SECS)

        analyzer_cmd = [sys.executable, analyzer_path, backtest_log_file]
        if pnl_start_time is not None:
            analyzer_cmd.extend(["--start-timestamp", format_timestamp(pnl_start_time)])
        if pnl_end_time is not None:
            analyzer_cmd.extend(["--end-timestamp", format_timestamp(pnl_end_time)])

        analyzer_env = build_analyzer_env()
        result = subprocess.run(
            analyzer_cmd, capture_output=True, text=True, env=analyzer_env
        )
        if result.returncode != 0:
            reason = f"analyzer_exit={result.returncode}"
            if result.stderr:
                reason += f" {result.stderr.strip()}"
            return -float("inf"), reason
        score = float(result.stdout.strip())
        reject_reason = ""
        if score == -float("inf") and result.stderr:
            reject_reason = result.stderr.strip()
        return score, reject_reason
    except Exception as e:
        return -float("inf"), f"analyzer_error: {e}"


def run_backtest_for_params(
    pair_str,
    params_to_run,
    pnl_start_time,
    pnl_end_time,
    latest_log_path,
    run_index,
    total_runs,
):
    backtest_log_file = None
    try:
        print(
            f"  [Running {run_index}/{total_runs} for {pair_str}] params={params_to_run}"
        )
        ensure_backtest_log_dir()
        with tempfile.NamedTemporaryFile(
            dir=BACKTEST_LOG_DIR,
            prefix=f"debot_backtest_{pair_str.replace('/', '_')}_",
            suffix=".log",
            delete=False,
        ) as tmp:
            backtest_log_file = tmp.name

        score, reject_reason = run_backtest(
            params_to_run,
            pair_str,
            backtest_log_file,
            pnl_start_time,
            pnl_end_time,
        )
        shutil.copyfile(backtest_log_file, latest_log_path)
        return params_to_run, score, reject_reason
    finally:
        if (
            backtest_log_file
            and os.path.exists(backtest_log_file)
            and not KEEP_BACKTEST_LOG
        ):
            os.remove(backtest_log_file)
        elif backtest_log_file and os.path.exists(backtest_log_file):
            print(f"  > Kept backtest log at {backtest_log_file}")


def run_backtest_batch(
    pair_str, param_batch, pnl_start_time, pnl_end_time, batch_index
):
    """
    Run multiple backtests in a single process using batch mode.
    Returns list of (params, score, reject_reason) tuples.
    """
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    binary_path = os.path.join(repo_root, "target", "release", "debot")
    lighter_go_path = resolve_lighter_go_path()
    ld_path = lighter_go_path + ":" + os.environ.get("LD_LIBRARY_PATH", "")
    analyzer_path = os.path.join(os.path.dirname(__file__), "log_analyzer.py")

    # Write param sets to a temp JSONL file.
    batch_dir = os.path.join(BACKTEST_LOG_DIR, f"batch_{batch_index}")
    os.makedirs(batch_dir, exist_ok=True)
    params_file = os.path.join(batch_dir, "params.jsonl")
    with open(params_file, "w") as f:
        for params in param_batch:
            f.write(json.dumps(params) + "\n")

    env = os.environ.copy()
    env.update(
        {
            "BACKTEST_MODE": "true",
            "BACKTEST_FILE": DATA_DUMP_FILE,
            "DRY_RUN": "true",
            "RUST_LOG": "warn,debot::pairtrade=info",
            "LD_LIBRARY_PATH": ld_path,
            "UNIVERSE_PAIRS": pair_str,
            "BATCH_PARAMS_FILE": params_file,
            "BATCH_LOG_DIR": batch_dir,
            "RESTART_GUARD_DIR": BACKTEST_LOG_DIR,
            "RESTART_GUARD_KEY": f"optimizer_batch_{batch_index}_{uuid.uuid4().hex}",
        }
    )

    try:
        result = subprocess.run(
            [binary_path],
            env=env,
            capture_output=True,
            text=True,
            timeout=3600 * 2,
        )
    except subprocess.TimeoutExpired:
        return [(p, -float("inf"), "batch_timeout") for p in param_batch]
    except Exception as e:
        return [(p, -float("inf"), f"batch_error: {e}") for p in param_batch]

    # Parse batch output (JSONL on stdout): {"index": N, "log_file": "...", ...}
    batch_outputs = {}
    for line in (result.stdout or "").strip().split("\n"):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            batch_outputs[entry["index"]] = entry
        except (json.JSONDecodeError, KeyError):
            continue

    # Score each run's log file.
    results = []
    analyzer_env = build_analyzer_env()
    for idx, params in enumerate(param_batch):
        entry = batch_outputs.get(idx)
        if not entry or "error" in entry:
            error_msg = entry.get("error", "no_output") if entry else "no_output"
            results.append((params, -float("inf"), f"batch_run_error: {error_msg}"))
            continue

        log_file = entry.get("log_file", "")
        if not log_file or not os.path.exists(log_file):
            results.append((params, -float("inf"), "no_log_file"))
            continue

        # Run analyzer on this log file.
        analyzer_cmd = [sys.executable, analyzer_path, log_file]
        if pnl_start_time is not None:
            analyzer_cmd.extend(["--start-timestamp", format_timestamp(pnl_start_time)])
        if pnl_end_time is not None:
            analyzer_cmd.extend(["--end-timestamp", format_timestamp(pnl_end_time)])

        try:
            a_result = subprocess.run(
                analyzer_cmd, capture_output=True, text=True, env=analyzer_env
            )
            if a_result.returncode != 0:
                results.append((params, -float("inf"), f"analyzer_exit={a_result.returncode}"))
                continue
            score = float(a_result.stdout.strip())
            reject_reason = ""
            if score == -float("inf") and a_result.stderr:
                reject_reason = a_result.stderr.strip()
            results.append((params, score, reject_reason))
        except Exception as e:
            results.append((params, -float("inf"), f"analyzer_error: {e}"))

        # Clean up log file unless keeping.
        if not KEEP_BACKTEST_LOG and os.path.exists(log_file):
            try:
                os.remove(log_file)
            except OSError:
                pass

    # Clean up batch dir.
    try:
        os.remove(params_file)
        if not KEEP_BACKTEST_LOG:
            shutil.rmtree(batch_dir, ignore_errors=True)
    except OSError:
        pass

    return results


def optimize_for_pair(
    pair_str,
    pnl_start_time,
    pnl_end_time,
    param_grid,
    max_combos=None,
    sampling_strategy=None,
    enable_refinement=None,
):
    """
    Runs the optimization grid search for a single pair using the backtest data.
    """
    print(f"\n{'='*20} Optimizing for Pair: {pair_str} {'='*20}")
    log_file_path = OPTIMIZER_LOG_PATH

    runnable_params = build_param_combinations(
        param_grid,
        max_combos=max_combos,
        sampling_strategy=sampling_strategy,
    )

    best_score = -float("inf")
    best_params = None
    stage1_results = []
    stage2_results = []

    total_runs = len(runnable_params)
    print(f"Total backtests to run: {total_runs}")
    with open(log_file_path, "a") as opt_log:
        opt_log.write(
            f"== Pair {pair_str} == total_runs={total_runs} "
            f"train_start={format_timestamp(pnl_start_time)} "
            f"train_end={format_timestamp(pnl_end_time)}\n"
        )

    for i, params_to_run in enumerate(runnable_params):
        print(f"  [Queued {i+1}/{total_runs} for {pair_str}] params={params_to_run}")

    worker_count = resolve_optimizer_workers(len(runnable_params))
    completed = 0
    latest_log_path = get_latest_log_path(pair_str)

    # Use batch mode: group params into batches, each batch runs in a single
    # process that loads data once.
    batch_size = int(os.getenv("OPTIMIZER_BATCH_SIZE", "0"))
    use_batch = batch_size > 0 and os.path.isfile(
        os.path.join(
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            "target", "release", "debot",
        )
    )

    if use_batch:
        ensure_backtest_log_dir()
        batches = [
            runnable_params[i : i + batch_size]
            for i in range(0, len(runnable_params), batch_size)
        ]
        print(
            f"  Batch mode: {len(batches)} batches of ~{batch_size} "
            f"({worker_count} workers)"
        )
        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_to_batch = {
                executor.submit(
                    run_backtest_batch,
                    pair_str,
                    batch,
                    pnl_start_time,
                    pnl_end_time,
                    batch_idx,
                ): batch
                for batch_idx, batch in enumerate(batches)
            }
            for future in concurrent.futures.as_completed(future_to_batch):
                try:
                    batch_results = future.result()
                except Exception as e:
                    batch = future_to_batch[future]
                    print(f"      > Batch failed: {e}", file=sys.stderr)
                    batch_results = [
                        (p, -float("inf"), f"batch_error: {e}") for p in batch
                    ]

                for params_to_run, score, reject_reason in batch_results:
                    stage1_results.append((params_to_run, score))
                    completed += 1
                    reason_suffix = f" reason={reject_reason}" if reject_reason else ""
                    print(
                        f"  [Completed {completed}/{total_runs} for {pair_str}] "
                        f"score={score:.4f}{reason_suffix} params={params_to_run}"
                    )
                    if score > best_score:
                        best_score = score
                        best_params = params_to_run
                        print(
                            f"  *** New best score for {pair_str}: {best_score:.4f} "
                            f"with params: {best_params} ***"
                        )
                        with open(log_file_path, "a") as opt_log:
                            opt_log.write(
                                f"[{pair_str}] new_best score={best_score:.4f} "
                                f"params={best_params}\n"
                            )
    else:
        # Fallback: original per-run mode.
        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_to_params = {
                executor.submit(
                    run_backtest_for_params,
                    pair_str,
                    params,
                    pnl_start_time,
                    pnl_end_time,
                    latest_log_path,
                    idx + 1,
                    len(runnable_params),
                ): params
                for idx, params in enumerate(runnable_params)
            }
            for future in concurrent.futures.as_completed(future_to_params):
                params_to_run = future_to_params[future]
                reject_reason = ""
                try:
                    _, score, reject_reason = future.result()
                except FatalBacktestError as e:
                    print(f"      > Fatal backtest error: {e}", file=sys.stderr)
                    raise
                except Exception as e:
                    print(
                        f"      > Backtest failed for params {params_to_run}: {e}",
                        file=sys.stderr,
                    )
                    score = -float("inf")
                    reject_reason = f"backtest_error: {e}"

                stage1_results.append((params_to_run, score))
                completed += 1
                reason_suffix = f" reason={reject_reason}" if reject_reason else ""
                print(
                    f"  [Completed {completed}/{total_runs} for {pair_str}] "
                    f"score={score:.4f}{reason_suffix} params={params_to_run}"
                )
                if score > best_score:
                    best_score = score
                    best_params = params_to_run
                    print(
                        f"  *** New best score for {pair_str}: {best_score:.4f} "
                        f"with params: {best_params} ***"
                    )
                    with open(log_file_path, "a") as opt_log:
                        opt_log.write(
                            f"[{pair_str}] new_best score={best_score:.4f} "
                            f"params={best_params}\n"
                        )

    stage2_best_params = None
    stage2_best_score = -float("inf")
    if (ENABLE_REFINEMENT if enable_refinement is None else enable_refinement) and stage1_results:
        max_refine_runs = REFINE_MAX_RUNS or max(1, total_runs // 3)
        refined_params = build_refined_param_sets(
            stage1_results,
            param_grid,
            list(param_grid.keys()),
            max_refine_runs,
        )
        if refined_params:
            print(
                f"\n  [Stage 2] Refining {len(refined_params)} candidates for {pair_str}"
            )
            with open(log_file_path, "a") as opt_log:
                opt_log.write(f"[{pair_str}] stage2_runs={len(refined_params)}\n")

            stage2_completed = 0
            stage2_workers = resolve_optimizer_workers(len(refined_params))
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=stage2_workers
            ) as executor:
                future_to_params = {
                    executor.submit(
                        run_backtest_for_params,
                        pair_str,
                        params,
                        pnl_start_time,
                        pnl_end_time,
                        latest_log_path,
                        idx + 1,
                        len(refined_params),
                    ): params
                    for idx, params in enumerate(refined_params)
                }
                for future in concurrent.futures.as_completed(future_to_params):
                    params_to_run = future_to_params[future]
                    try:
                        _, score = future.result()
                    except FatalBacktestError as e:
                        print(f"      > Fatal backtest error: {e}", file=sys.stderr)
                        raise
                    except Exception as e:
                        print(
                            f"      > Backtest failed for params {params_to_run}: {e}",
                            file=sys.stderr,
                        )
                        score = -float("inf")

                    stage2_results.append((params_to_run, score))
                    stage2_completed += 1
                    print(
                        f"  [Completed {stage2_completed}/{len(refined_params)} for {pair_str}] "
                        f"score={score:.4f} params={params_to_run}"
                    )
                    if score > stage2_best_score:
                        stage2_best_score = score
                        stage2_best_params = params_to_run
                        print(
                            f"  *** Stage 2 best for {pair_str}: {stage2_best_score:.4f} with params: {stage2_best_params} ***"
                        )
                        with open(log_file_path, "a") as opt_log:
                            opt_log.write(
                                f"[{pair_str}] stage2_best score={stage2_best_score:.4f} params={stage2_best_params}\n"
                            )

    if stage2_best_params and stage2_best_score > best_score:
        best_params = stage2_best_params
        best_score = stage2_best_score

    candidate_results = stage2_results if stage2_results else stage1_results
    return best_params, best_score, candidate_results


def evaluate_params(pair_str, params, pnl_start_time, pnl_end_time, label=None):
    if not params:
        return None
    backtest_log_file = None
    latest_log_path = get_latest_log_path(pair_str, "val")
    try:
        ensure_backtest_log_dir()
        with tempfile.NamedTemporaryFile(
            dir=BACKTEST_LOG_DIR,
            prefix=f"debot_backtest_{pair_str.replace('/', '_')}_val_",
            suffix=".log",
            delete=False,
        ) as tmp:
            backtest_log_file = tmp.name
        label_text = f" {label}" if label else ""
        print(f"  > Validation backtest{label_text} for {pair_str} with params.")
        score, _reject_reason = run_backtest(
            params, pair_str, backtest_log_file, pnl_start_time, pnl_end_time
        )
        shutil.copyfile(backtest_log_file, latest_log_path)
        return score
    finally:
        if (
            backtest_log_file
            and os.path.exists(backtest_log_file)
            and not KEEP_BACKTEST_LOG
        ):
            os.remove(backtest_log_file)
        elif backtest_log_file and os.path.exists(backtest_log_file):
            print(f"  > Kept backtest log at {backtest_log_file}")


def evaluate_candidate_params(pair_str, candidates, pnl_start_time, pnl_end_time):
    if not candidates:
        return None, None, []

    best_params = None
    best_score = -float("inf")
    results = []
    total = len(candidates)
    candidate_workers = resolve_validation_candidate_workers(total)
    if candidate_workers > 1:
        print(
            f"  > Parallelizing validation candidates for {pair_str} "
            f"(workers={candidate_workers})."
        )
        results = [None] * total
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=candidate_workers
        ) as executor:
            future_to_index = {}
            for idx, params in enumerate(candidates, start=1):
                future = executor.submit(
                    evaluate_params,
                    pair_str,
                    params,
                    pnl_start_time,
                    pnl_end_time,
                    label=f"(candidate {idx}/{total})",
                )
                future_to_index[future] = idx
            for future in concurrent.futures.as_completed(future_to_index):
                idx = future_to_index[future]
                params = candidates[idx - 1]
                score = future.result()
                results[idx - 1] = {"params": params, "score": score}
                if score is not None and score > best_score:
                    best_score = score
                    best_params = params
    else:
        for idx, params in enumerate(candidates, start=1):
            score = evaluate_params(
                pair_str,
                params,
                pnl_start_time,
                pnl_end_time,
                label=f"(candidate {idx}/{total})",
            )
            results.append({"params": params, "score": score})
            if score is not None and score > best_score:
                best_score = score
                best_params = params
    return best_params, best_score, results


def parse_args():
    parser = argparse.ArgumentParser(
        description="Gather market data and/or run optimizer backtests."
    )
    parser.add_argument(
        "--mode",
        choices=["all", "gather", "optimize"],
        default=os.getenv("OPTIMIZER_MODE", "all"),
        help="all: gather data then optimize; gather: data only; optimize: backtest only",
    )
    return parser.parse_args()


def main():
    """
    Main entry point: Gather data and/or run optimization for all target pairs.
    """
    global DATA_DUMP_FILE
    args = parse_args()

    if os.path.exists(OPTIMIZER_LOG_PATH):
        os.remove(OPTIMIZER_LOG_PATH)
    log_file = open(OPTIMIZER_LOG_PATH, "a", buffering=1)
    sys.stdout = log_file
    sys.stderr = log_file

    start_time = datetime.now()
    data_dump_source = DATA_DUMP_FILE
    data_dump_snapshot = None
    data_dump_preprocessed = None

    exc_info = None
    overall_results = {}
    try:
        config_path = resolve_config_path()
        os.environ["PAIRTRADE_CONFIG_PATH"] = config_path
        if not os.path.exists(config_path):
            print(f"Config not found at {config_path}.", file=sys.stderr)
            return
        base_param_grid = PARAM_GRID
        seeded_base_grid = base_param_grid
        if OPTIMIZER_SEED_FROM_ENV:
            seeded_base_grid = seed_param_grid_from_config(base_param_grid, config_path)
            print(f"Seeded PARAM_GRID with values from {config_path}.")
        target_pairs = TARGET_PAIRS or load_target_pairs(config_path)
        if args.mode in ("all", "optimize"):
            ensure_libsigner_available(config_path)
        if not target_pairs:
            print(
                "No target pairs found; set universe_pairs or universe_symbols in the config.",
                file=sys.stderr,
            )
            return
        pair_to_configs = None
        if COMMON_PARAM_MODE != "common":
            config_paths = resolve_config_update_targets(config_path)
            pair_to_configs, _multi = map_config_paths_by_pair(config_paths)

        cleanup_restart_counters()
        if args.mode in ("all", "gather"):
            gather_data(target_pairs)
        if args.mode == "gather":
            print("Mode=gather: skipping optimization.")
            return

        if args.mode in ("all", "optimize"):
            data_dump_snapshot = snapshot_data_file(DATA_DUMP_FILE)
            if data_dump_snapshot:
                print(
                    f"Using snapshot data file: {data_dump_snapshot} (source {data_dump_source})."
                )
                DATA_DUMP_FILE = data_dump_snapshot
            data_dump_preprocessed = preprocess_data_dump_file(DATA_DUMP_FILE)
            if data_dump_preprocessed:
                print(f"Using preprocessed data file: {data_dump_preprocessed}")
                DATA_DUMP_FILE = data_dump_preprocessed
            DATA_DUMP_JSONL = DATA_DUMP_FILE

        # Convert JSONL to bincode for faster loading in backtests.
        if os.path.exists(DATA_DUMP_FILE) and not DATA_DUMP_FILE.endswith(".bin"):
            bincode_path = convert_to_bincode(DATA_DUMP_FILE)
            if bincode_path != DATA_DUMP_FILE:
                DATA_DUMP_FILE = bincode_path

        if os.path.exists(DATA_DUMP_JSONL):
            bounds = get_data_time_bounds(DATA_DUMP_JSONL)
            if bounds is None:
                print(
                    "Could not determine data time bounds. Halting optimization.",
                    file=sys.stderr,
                )
                return

            data_duration_days = (bounds[1] - bounds[0]).total_seconds() / 86400.0
            min_data_days = float(os.getenv("OPTIMIZER_MIN_DATA_DAYS", "7"))
            if data_duration_days < min_data_days:
                print(
                    f"Insufficient data: {data_duration_days:.1f} days "
                    f"(need {min_data_days:.0f}). Skipping optimization."
                )
                return

            train_start, train_end, val_start, val_end = build_walk_forward_windows(
                bounds[0], bounds[1]
            )
            print(
                f"Train window: {format_timestamp(train_start)} to {format_timestamp(train_end)}"
            )
            if val_start and val_end:
                print(
                    f"Validation window: {format_timestamp(val_start)} to {format_timestamp(val_end)}"
                )
            for pair in target_pairs:
                pair_param_grid = seeded_base_grid
                if OPTIMIZER_SEED_FROM_ENV and COMMON_PARAM_MODE != "common":
                    pair_configs = (
                        pair_to_configs.get(pair) if pair_to_configs else None
                    )
                    if pair_configs:
                        pair_config_path = pair_configs[0]
                        pair_param_grid = seed_param_grid_from_config(
                            base_param_grid, pair_config_path, pair_key=pair
                        )
                        print(
                            f"Seeded PARAM_GRID for {pair} with values from {pair_config_path}."
                        )
                if OPTIMIZER_SWEEP_ENABLE:
                    sweep_windows = build_sweep_windows(train_start, train_end)
                    print(
                        f"\nSweep mode enabled: windows={len(sweep_windows)} "
                        f"window_days={OPTIMIZER_SWEEP_WINDOW_DAYS} "
                        f"step_days={OPTIMIZER_SWEEP_STEP_DAYS} "
                        f"max_combos={OPTIMIZER_SWEEP_MAX_COMBOS} "
                        f"top_k={OPTIMIZER_SWEEP_TOP_K} "
                        f"diverse_k={OPTIMIZER_SWEEP_DIVERSE_K} "
                        f"min_score={OPTIMIZER_SWEEP_MIN_SCORE} "
                        f"final_max={OPTIMIZER_SWEEP_FINAL_MAX} "
                        f"include_tail={OPTIMIZER_SWEEP_INCLUDE_TAIL} "
                        f"refine={OPTIMIZER_SWEEP_REFINEMENT} "
                        f"min_step={OPTIMIZER_SWEEP_MIN_SCORE_STEP} "
                        f"min_floor={OPTIMIZER_SWEEP_MIN_SCORE_FLOOR}"
                    )
                    sweep_candidates = []
                    for idx, (win_start, win_end) in enumerate(
                        sweep_windows, start=1
                    ):
                        print(
                            f"\nSweep window {idx}/{len(sweep_windows)} "
                            f"{format_timestamp(win_start)} to {format_timestamp(win_end)}"
                        )
                        _best_params, _best_score, window_results = optimize_for_pair(
                            pair,
                            win_start,
                            win_end,
                            pair_param_grid,
                            max_combos=OPTIMIZER_SWEEP_MAX_COMBOS,
                            sampling_strategy=OPTIMIZER_SAMPLING_STRATEGY,
                            enable_refinement=OPTIMIZER_SWEEP_REFINEMENT,
                        )
                        window_candidates = select_sweep_candidates(
                            window_results,
                            OPTIMIZER_SWEEP_TOP_K,
                            OPTIMIZER_SWEEP_DIVERSE_K,
                            OPTIMIZER_SWEEP_DIVERSITY_KEYS,
                            OPTIMIZER_SWEEP_MIN_SCORE,
                            priority_keys=OPTIMIZER_SWEEP_DIVERSITY_PRIORITY_KEYS,
                            weights=OPTIMIZER_SWEEP_DIVERSITY_WEIGHTS,
                            distance_map=OPTIMIZER_SWEEP_DIVERSITY_DISTANCE,
                        )
                        log_sweep_window(pair, win_start, win_end, window_candidates)
                        sweep_candidates.extend(window_candidates)

                    deduped_map = {}
                    for params, score in sweep_candidates:
                        key = make_params_key(params)
                        best = deduped_map.get(key)
                        if best is None or score > best[1]:
                            deduped_map[key] = (params, score)
                    deduped = list(deduped_map.values())
                    deduped.sort(key=lambda item: item[1], reverse=True)
                    if OPTIMIZER_SWEEP_FINAL_MAX > 0:
                        deduped = deduped[:OPTIMIZER_SWEEP_FINAL_MAX]

                    if deduped:
                        print(
                            f"\nSweep candidates collected: {len(deduped)}; "
                            "re-evaluating on full train window."
                        )
                        deduped_params = [params for params, _score in deduped]
                        best_params, best_score, val_results = evaluate_candidate_params(
                            pair, deduped_params, train_start, train_end
                        )
                        candidate_results = [
                            (item["params"], item.get("score", -float("inf")))
                            for item in (val_results or [])
                        ]
                    else:
                        print(
                            "\nSweep produced no candidates; falling back to full-grid optimization.",
                            file=sys.stderr,
                        )
                        best_params, best_score, candidate_results = optimize_for_pair(
                            pair, train_start, train_end, pair_param_grid
                        )
                else:
                    best_params, best_score, candidate_results = optimize_for_pair(
                        pair, train_start, train_end, pair_param_grid
                    )
                overall_results[pair] = {
                    "params": best_params,
                    "score": best_score,
                    "candidate_results": candidate_results,
                }

            if val_start and val_end:
                validation_pairs = []
                for pair, result in overall_results.items():
                    candidates = select_validation_candidates(
                        result.get("candidate_results") or [],
                        VALIDATION_CANDIDATE_TOP_K,
                        VALIDATION_CANDIDATE_DIVERSE_K,
                        VALIDATION_DIVERSITY_KEYS,
                    )
                    if not candidates:
                        continue
                    result["validation_candidates"] = candidates
                    validation_pairs.append((pair, candidates))
                if validation_pairs:
                    cpu_count = os.cpu_count() or 1
                    default_workers = max(1, cpu_count - 1)
                    validation_workers = default_workers
                    validation_workers_raw = (VALIDATION_WORKERS or "").strip()
                    if validation_workers_raw:
                        try:
                            validation_workers = max(1, int(validation_workers_raw))
                        except ValueError:
                            validation_workers = default_workers
                    validation_workers = min(validation_workers, len(validation_pairs))
                    print(
                        f"\nRunning validation backtests for {len(validation_pairs)} pairs "
                        f"(workers={validation_workers})."
                    )
                    if validation_workers == 1:
                        for pair, candidates in validation_pairs:
                            val_params, val_score, val_results = evaluate_candidate_params(
                                pair, candidates, val_start, val_end
                            )
                            overall_results[pair]["val_params"] = val_params
                            overall_results[pair]["val_score"] = val_score
                            overall_results[pair]["val_results"] = val_results
                    else:
                        with concurrent.futures.ThreadPoolExecutor(
                            max_workers=validation_workers
                        ) as executor:
                            future_to_pair = {
                                executor.submit(
                                    evaluate_candidate_params,
                                    pair,
                                    candidates,
                                    val_start,
                                    val_end,
                                ): pair
                                for pair, candidates in validation_pairs
                            }
                            for future in concurrent.futures.as_completed(
                                future_to_pair
                            ):
                                pair = future_to_pair[future]
                                try:
                                    val_params, val_score, val_results = future.result()
                                except FatalBacktestError as e:
                                    print(
                                        f"  > Fatal validation backtest error for {pair}: {e}",
                                        file=sys.stderr,
                                    )
                                    raise
                                except Exception as e:
                                    print(
                                        f"  > Validation failed for {pair}: {e}",
                                        file=sys.stderr,
                                    )
                                    val_params = None
                                    val_score = -float("inf")
                                    val_results = []
                                overall_results[pair]["val_params"] = val_params
                                overall_results[pair]["val_score"] = val_score
                                overall_results[pair]["val_results"] = val_results
        else:
            print(
                "Could not find data dump file. Halting optimization.", file=sys.stderr
            )
            return

        end_time = datetime.now()
        print(f"\n{'='*20} Overall Optimization Summary {'='*20}")
        print(f"Total execution time: {end_time - start_time}")
        score_label = resolve_score_label()
        for pair, result in overall_results.items():
            print(f"\nResult for {pair}:")
            if result["params"]:
                print(f"  Train Best Score ({score_label}): {result['score']:.4f}")
                print(f"  Train Best Parameters: {result['params']}")
                if result.get("val_score") is not None:
                    print(f"  Validation Score ({score_label}): {result['val_score']:.4f}")
                    if result.get("val_params") is not None:
                        print(f"  Validation Parameters: {result['val_params']}")
            else:
                print("  No successful runs.")

        best_pair = None
        best_params = None
        best_score = None
        per_pair_updated = False
        if COMMON_PARAM_MODE != "common":
            print(
                f"\nCOMMON_PARAM_MODE={COMMON_PARAM_MODE}; skipping common-params selection."
            )
            per_pair_updated = update_configs_per_pair(
                config_path,
                overall_results,
                train_start,
                train_end,
                val_start,
                val_end,
            )
        elif val_start and val_end:
            candidate_params = []
            if COMMON_PARAM_CANDIDATES == "grid":
                candidate_params = build_param_combinations(seeded_base_grid)
                print(
                    f"\nUsing full grid for common-params candidates: {len(candidate_params)}"
                )
            else:
                seen = set()
                for result in overall_results.values():
                    params = result.get("val_params") or result.get("params")
                    if not params:
                        continue
                    key = tuple(sorted(params.items()))
                    if key in seen:
                        continue
                    seen.add(key)
                    candidate_params.append(params)

            if candidate_params:
                print(
                    "\nEvaluating common parameters across all pairs (validation only)."
                )
                best_params, best_score, best_worst = select_best_common_params(
                    target_pairs,
                    candidate_params,
                    val_start,
                    val_end,
                    COMMON_PARAM_MIN_VAL_SCORE,
                )
                if best_params:
                    best_pair = "common-params"
                    print(
                        f"\nSelected common params: avg={best_score:.4f} worst={best_worst:.4f}"
                    )
                else:
                    print(
                        f"\nNo common params met min score {COMMON_PARAM_MIN_VAL_SCORE:.4f}; "
                        "falling back to per-pair updates.",
                        file=sys.stderr,
                    )
                    per_pair_updated = update_configs_per_pair(
                        config_path,
                        overall_results,
                        train_start,
                        train_end,
                        val_start,
                        val_end,
                    )
            else:
                print(
                    "\nNo common-params candidates found; falling back to per-pair updates.",
                    file=sys.stderr,
                )
                per_pair_updated = update_configs_per_pair(
                    config_path,
                    overall_results,
                    train_start,
                    train_end,
                    val_start,
                    val_end,
                )
        else:
            best_pair, best_params, best_score = select_best_overall(overall_results)

        if per_pair_updated:
            return

        if best_params:
            if val_start and val_end and best_score is not None and best_score < 0:
                print(
                    f"\nValidation score {best_score:.4f} < 0; skipping debot config update.",
                    file=sys.stderr,
                )
                return

            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

            # Update debot config (dex-specific)
            config_paths = resolve_config_update_targets(config_path)
            updated_paths = [
                path for path in config_paths if update_config_params(path, best_params)
            ]
            if updated_paths:
                config_list = ", ".join(os.path.basename(p) for p in updated_paths)
                print(
                    f"\nUpdated {config_list} with best params from {best_pair} (score {best_score:.4f})."
                )
                git_commit_and_push(
                    repo_root,
                    updated_paths,
                    best_pair,
                    best_score,
                    train_start,
                    train_end,
                    val_start,
                    val_end,
                )
            else:
                print(
                    f"\nSkipping debot config update; unable to update {config_path}.",
                    file=sys.stderr,
                )
    except Exception:
        exc_info = traceback.format_exc()
        raise
    finally:
        end_time = datetime.now()
        status = "SUCCESS" if exc_info is None else "FAILED"
        subject = f"[debot] optimizer.py finished: {status}"
        duration = end_time - start_time
        body_lines = [
            f"Status: {status}",
            f"Start: {start_time.isoformat()}",
            f"End: {end_time.isoformat()}",
            f"Duration: {duration}",
            f"Data file (source): {os.path.abspath(data_dump_source)}",
            f"Data file (used): {os.path.abspath(DATA_DUMP_FILE)}",
        ]
        if data_dump_snapshot:
            body_lines.append(
                f"Data file (snapshot): {os.path.abspath(data_dump_snapshot)}"
            )
        if data_dump_preprocessed:
            body_lines.append(
                f"Data file (preprocessed): {os.path.abspath(data_dump_preprocessed)}"
            )
        if exc_info:
            body_lines.append("")
            body_lines.append("Exception:")
            body_lines.append(exc_info)
        send_completion_email(subject, "\n".join(body_lines))
        if data_dump_snapshot and os.path.exists(data_dump_snapshot):
            try:
                os.remove(data_dump_snapshot)
            except OSError:
                pass


if __name__ == "__main__":

    import re

    main()
