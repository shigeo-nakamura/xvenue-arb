#!/usr/bin/env python3
"""
Pair trade candidate data collector.

Collects order book data for pair trade candidate symbols on Lighter DEX
via WebSocket. Writes JSONL snapshots at a fixed interval.

Usage:
    python3 pair_data_collector.py

Environment:
    PAIR_INTERVAL_SECS   Snapshot interval in seconds (default: 10)
    PAIR_DATA_DIR        Output directory (default: /opt/slow-mm/scripts)
    PAIR_WS_HOST         WebSocket host (default: mainnet.zklighter.elliot.ai)

Output: pair_data.jsonl (multi-symbol JSONL, one line per snapshot)

See: https://github.com/shigeo-nakamura/bot-strategy/issues/8
"""
import json
import os
import sys
import time
import signal
import logging
from datetime import datetime, timezone

from websockets.sync.client import connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configuration
INTERVAL_SECS = int(os.getenv("PAIR_INTERVAL_SECS", "10"))
DATA_DIR = os.getenv("PAIR_DATA_DIR", "/opt/slow-mm/scripts")
WS_HOST = os.getenv("PAIR_WS_HOST", "mainnet.zklighter.elliot.ai")
WS_URL = f"wss://{WS_HOST}/stream"

# Pair trade candidate symbols: (symbol_name, market_id)
# Crypto
#   BTC(1), ETH(0), SOL(2) -- already collected by market_data_collector
# Equity (crypto-related)
#   MSTR(122), COIN(109), HOOD(108)
# Equity (tech)
#   NVDA(110), TSLA(112), AAPL(113), AMZN(114), MSFT(115), GOOGL(116), META(117), INTC(137), AMD(138)
# ETF/Index
#   SPY(128), QQQ(129), DIA(152), IWM(153)
# Commodity
#   XAU(92), XAG(93), WTI(145)
# FX
#   EURUSD(96)

SYMBOLS = [
    # Crypto (core)
    ("BTC", 1),
    ("ETH", 0),
    ("SOL", 2),
    # Equity - crypto-related
    ("MSTR", 122),
    ("COIN", 109),
    ("HOOD", 108),
    # Equity - tech majors
    ("NVDA", 110),
    ("TSLA", 112),
    ("AAPL", 113),
    ("MSFT", 115),
    ("META", 117),
    ("AMD", 138),
    # ETF/Index
    ("SPY", 128),
    ("QQQ", 129),
    # Commodity
    ("XAU", 92),
]

OUTPUT_FILE = "pair_data.jsonl"

shutdown = False


def signal_handler(sig, frame):
    global shutdown
    logger.info("Shutdown signal received")
    shutdown = True


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


class OrderBookState:
    """Maintains order book state from WebSocket updates."""

    def __init__(self, symbol, market_id):
        self.symbol = symbol
        self.market_id = market_id
        self.bids = []
        self.asks = []
        self.last_update = 0

    def on_snapshot(self, order_book):
        self.bids = order_book.get("bids", [])
        self.asks = order_book.get("asks", [])
        self.last_update = time.time()

    def on_update(self, order_book):
        self._apply_delta(order_book.get("bids", []), self.bids)
        self._apply_delta(order_book.get("asks", []), self.asks)
        self.last_update = time.time()

    def _apply_delta(self, updates, book):
        for upd in updates:
            found = False
            for existing in book:
                if existing["price"] == upd["price"]:
                    found = True
                    if float(upd["size"]) == 0:
                        book.remove(existing)
                    else:
                        existing["size"] = upd["size"]
                    break
            if not found and float(upd["size"]) > 0:
                book.append(upd)

    def best_bid(self):
        if not self.bids:
            return 0.0, 0.0
        best = max(self.bids, key=lambda x: float(x["price"]))
        return float(best["price"]), float(best["size"])

    def best_ask(self):
        if not self.asks:
            return 0.0, 0.0
        best = min(self.asks, key=lambda x: float(x["price"]))
        return float(best["price"]), float(best["size"])

    def snapshot_dict(self):
        bid_price, bid_size = self.best_bid()
        ask_price, ask_size = self.best_ask()
        if bid_price <= 0 or ask_price <= 0:
            return None
        mid = (bid_price + ask_price) / 2.0
        spread_bps = (ask_price - bid_price) / mid * 10000
        return {
            "price": f"{mid:.6f}",
            "bid_price": f"{bid_price}",
            "ask_price": f"{ask_price}",
            "bid_size": f"{bid_size}",
            "ask_size": f"{ask_size}",
            "spread_bps": round(spread_bps, 2),
        }


def write_snapshot(states, data_dir):
    ts_ms = int(time.time() * 1000)
    prices = {}
    for state in states.values():
        snap = state.snapshot_dict()
        if snap:
            prices[state.symbol] = snap

    if len(prices) < 2:
        return

    record = {"timestamp": ts_ms, "prices": prices}
    fpath = os.path.join(data_dir, OUTPUT_FILE)
    try:
        with open(fpath, "a") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
    except Exception as e:
        logger.error(f"Failed to write {fpath}: {e}")


def run_collector():
    global shutdown

    os.makedirs(DATA_DIR, exist_ok=True)

    states = {}
    market_id_to_symbol = {}
    for symbol, market_id in SYMBOLS:
        states[market_id] = OrderBookState(symbol, market_id)
        market_id_to_symbol[market_id] = symbol

    fpath = os.path.join(DATA_DIR, OUTPUT_FILE)
    logger.info(f"Pair data collector started")
    logger.info(f"  Interval: {INTERVAL_SECS}s")
    logger.info(f"  Output: {fpath}")
    logger.info(f"  Symbols ({len(SYMBOLS)}): {', '.join(f'{s}({m})' for s, m in SYMBOLS)}")

    last_snapshot_time = 0

    while not shutdown:
        try:
            ws = connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            )
            logger.info("WebSocket connected")

            for message in ws:
                if shutdown:
                    break

                msg = json.loads(message)
                msg_type = msg.get("type", "")

                if msg_type == "connected":
                    for symbol, market_id in SYMBOLS:
                        sub = json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"})
                        ws.send(sub)
                        logger.info(f"Subscribed to order_book/{market_id} ({symbol})")

                elif msg_type == "subscribed/order_book":
                    channel = msg.get("channel", "")
                    market_id = int(channel.split(":")[1]) if ":" in channel else 0
                    if market_id in states:
                        states[market_id].on_snapshot(msg.get("order_book", {}))
                        logger.info(f"Got snapshot for {market_id_to_symbol.get(market_id, '?')}")

                elif msg_type == "update/order_book":
                    channel = msg.get("channel", "")
                    market_id = int(channel.split(":")[1]) if ":" in channel else 0
                    if market_id in states:
                        states[market_id].on_update(msg.get("order_book", {}))

                now = time.time()
                if now - last_snapshot_time >= INTERVAL_SECS:
                    write_snapshot(states, DATA_DIR)
                    last_snapshot_time = now

        except Exception as e:
            if shutdown:
                break
            logger.error(f"WebSocket error: {e}, reconnecting in 5s...")
            time.sleep(5)

    logger.info("Collector stopped")


if __name__ == "__main__":
    run_collector()
