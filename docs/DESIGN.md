# xvenue-arb 設計メモ

Cross-venue statistical arbitrage between Lighter and Extended (BTC-USD / ETH-USD perp). 対応 issue: [bot-strategy#166](https://github.com/shigeo-nakamura/bot-strategy/issues/166)。

このドキュメントは Phase 0 (データ feasibility) 着手前の **設計ドラフト** であり、Phase 0 GO 後に確定させる。

## 1. 戦略サマリ

両 venue の同一 perp 銘柄 (まず BTC-USD、余力があれば ETH-USD を追加) の価格を spread = `(P_ext - P_lt) / P_lt * 10000` [bps] で観測し、rolling z-score が閾値超過した瞬間に両脚同時建て、mean revert で決済する delta-neutral 戦略。

```
z = (spread - μ_roll) / σ_roll
z >  entry_z  →  Extended SHORT + Lighter LONG
z < -entry_z  →  Extended LONG  + Lighter SHORT
|z| < exit_z  →  両脚クローズ
```

ネット方向エクスポージャ ≈ 0。理論的には spread の mean-reversion のみに賭ける。

## 2. Venue 制約の非対称性

| | Lighter | Extended |
|---|---|---|
| Maker fee | 0 bp | 0 bp (理論上) |
| Taker fee | 0 bp | 2.5 bp |
| Tick 粒度 (BTC) | 0.1 | 1.0 |
| Latency (Tokyo) | ~300 ms | ~9 ms |
| 推奨 order type | maker/taker 自由 | **maker limit 原則、taker は emergency のみ** |

この非対称性が execution 設計の中心になる。単純な同時 market 成行は Extended 側で 2.5 bp × 2 脚 = 5 bp の往復コストを毎回払うことになり、理論 edge を食いつぶす可能性が高い。

## 3. アーキテクチャ

### 3.1 プロセス構成

**単一プロセス / 単一 Tokyo インスタンス (`debot-tokyo`)** で 2 venue を同時制御。理由:
- Lighter は地理を問わず ~300ms 固定 ∴ Tokyo 配置でも不利なし
- Extended は Tokyo ~9ms で決定的に有利
- 単一プロセスなら signal 集約・inventory 整合が local reasoning で済む (cross-region coordination 不要)

### 3.2 モジュール構成 (提案)

```
src/
├── main.rs              # 2 connector init、event loop
├── config.rs            # yaml 読み込み
├── xvenue/              # xvenue-arb 戦略ロジック (pairtrade/ を置き換え)
│   ├── mod.rs
│   ├── spread.rs        # 価格差・z-score 計算 (rolling μ/σ)
│   ├── signal.rs        # entry/exit/force-close 判定
│   ├── sizing.rs        # venue 別 notional、maker 片脚化考慮
│   ├── state.rs         # 両脚 position、pending order、inventory skew
│   └── status.rs        # status.json 出力
├── trade/
│   └── execution/
│       ├── extended_maker.rs  # post-only limit + chase + taker fallback
│       └── lighter_fill.rs    # market or aggressive limit
├── ports/
│   ├── replay_dex.rs    # 2 venue 同時 replay (BT 用、拡張要)
│   └── live_dual.rs     # live 用 2-connector ハブ (新規)
├── risk/
│   └── kill_switch.rs   # single-leg filled / stale venue / skew 検知
└── error_counter.rs, email_client.rs 等 (pairtrade から流用)
```

`src/pairtrade/` は scaffold 元の名残として初期コミットに残すが、戦略実装は `src/xvenue/` 配下に新規作成する。最終的に `pairtrade/` ツリーは削除。

### 3.3 2 DexConnector の同居

dex-connector 側の `DexConnector` trait は既に venue 非依存 (`Box<dyn DexConnector>`)。`LighterConnector` と `ExtendedConnector` を両方 `Box` 化して保持するだけ。trait 変更不要。

```rust
struct VenueHub {
    lighter: Arc<dyn DexConnector>,
    extended: Arc<dyn DexConnector>,
}
```

両 venue の WS feed は独立タスクで回し、market state を `tokio::sync::watch` か `Mutex<MarketState>` に集約。

## 4. Execution 戦略

### 4.1 Entry (通常時)

1. signal 発火 ← spread z-score が閾値超過
2. **Extended 側を先に post-only limit で打つ** (better 価格 = 成行相当の 1 tick inside)
3. N 秒以内に fill しなければ:
   - a. キャンセル → 1 tick aggressive に再掲 (chase)
   - b. 上限 M 回 chase しても未約定なら **taker fallback** (2.5 bp 許容)
4. Extended fill を確認 **してから** Lighter 側を発注
   - Lighter は 0 bp なので market 成行で即時約定狙い
   - もし market でも 1s 以内に fill しない異常事態 → Extended 脚を即クローズ (single-leg filled リスク回避)

この serialized leg 方式は **inventory skew の発生を制御しやすい**が、spread 消滅までの時間との競争になる。Phase 0 の half-life 測定で許容レイテンシ上限を見積もる。

### 4.2 Exit (通常時)

1. |z| < exit_z で exit signal
2. 両 venue 同時に reduce-only order 発行
   - Lighter: market で即時
   - Extended: post-only limit → 短 chase → 必要なら taker
3. 両脚 flat 確認で 1 サイクル完了

### 4.3 Emergency flatten

以下の条件で **両脚即時 taker clean-up**:
- WebSocket stale > 5s (片 venue でも)
- 片脚約定後、反対脚が fill 期限 (例: 3s) 内に約定せず
- inventory skew > 許容値 (e.g. |Δnotional| > $50)
- global kill signal (SIGUSR1 / dashboard トリガ)

Extended 側 taker 手数料 (2.5 bp) はこの時だけは許容する。

### 4.4 Position sizing

- entry_notional (USD) を config で定義、両 venue で揃える (delta-neutral のため)
- Tick 粒度差で size が完全一致しないため、Extended 側を基準に Lighter size を算出 (Lighter 0.1 tick は十分細かい)
- max concurrent position = 1 ペアのみから開始 (Phase 1 BT で複数同時可否を検討)

## 5. Signal & statistics

### 5.1 Spread 計算

```
spread_bps = (P_ext_mid - P_lt_mid) / P_lt_mid * 10_000
```

mid = best_bid と best_ask の平均。両 venue の最新 quote を **1 秒 bucket** に align (bucket 中の最終値)。

### 5.2 Rolling μ/σ

窓長は Phase 0 の autocorr 分析で決める。初期値として以下から探索:
- rolling_window = [5 min, 30 min, 2 h, 24 h]
- 短すぎ → σ が signal 自身を追随、z が出ない
- 長すぎ → regime shift に追随せず false signal

### 5.3 Funding rate 補正

Extended / Lighter で funding が 1h cadence で独立発生。保有中に funding bar を跨ぐと PnL にバイアスが乗るため、保有時間 > 15 min の見込みなら funding 予測を signal に織り込む余地あり (Phase 1 で評価)。

## 6. 設定スキーマ (draft)

`configs/xvenue-arb/debot-xvenue-arb-btc.yaml`:

```yaml
strategy:
  symbol_ext: BTC-USD          # Extended 側シンボル
  symbol_lt:  BTC-USD          # Lighter 側シンボル
  entry_z: 1.5
  exit_z:  0.3
  force_close_z: 3.0           # 逆方向暴走カットオフ
  rolling_window_sec: 1800
  spread_bucket_ms: 1000

sizing:
  entry_notional_usd: 100
  max_concurrent: 1

execution:
  extended:
    order_type: limit          # limit | taker
    chase_ticks: 1
    chase_retries: 3
    chase_timeout_ms: 500
  lighter:
    order_type: market         # market | limit
    fill_timeout_ms: 1000

risk:
  emergency_flatten_on_ws_stale_ms: 5000
  max_inventory_skew_usd: 50
  leg_mismatch_timeout_ms: 3000
  kill_switch_file: /tmp/xvenue-arb.kill

venues:
  extended:
    account_id: <env: EXTENDED_ACCOUNT_ID>
    # ... credentials via env
  lighter:
    account_id: <env: LIGHTER_ACCOUNT_ID>
    # ...
```

## 7. Status & 観測

`status.json` を pairtrade 互換に拡張:
- 従来: `targets[].status.*` (error_summary, position, last_signal)
- 追加: `venue_state` array (per-venue WS health, last_fill_ts, recent_taker_fills)
- 追加: `spread_series` (直近 N 分の z-score snapshot — dashboard chart 用)

dashboard (`debot-dashboard`) 側は xvenue-arb 識別 → 2 venue 表示。pill に `ext:OK/lt:OK` のように片 venue 毎の health を出す。

## 8. デプロイ

### 8.1 Tokyo (`debot-tokyo` / ARM)

1. **Go toolchain 導入**: `sudo dnf install -y golang` (Amazon Linux 2023) or upstream `go1.21+` for ARM64
2. **lighter-go 取得 & ビルド**:
   ```
   git clone https://github.com/elliottech/lighter-go.git ~/lighter-go
   cd ~/lighter-go && go build -buildmode=c-shared -o libsigner.so ./sharedlib
   ```
3. `LD_LIBRARY_PATH` に `~/lighter-go` を追加
4. systemd unit `debot-xvenue-arb.service` で binary 起動

### 8.2 CI

GitHub Actions runner (`ubuntu-latest` x86) 上でも同じ Go build を仕込む。pairtrade の CI が既に Frankfurt で lighter-sdk ビルドしているなら設定流用可。Tokyo 実機への SSM deploy step を追加。

Phase 0 GO までは CI / deploy workflow は `.disabled` 拡張子で止めておく (`.github/workflows/*.yml.disabled`)。

## 9. BT データ & Phase 0

### 9.1 データソース

- Lighter: `debot:/opt/debot/market_data_btceth_*.jsonl` (既存、Frankfurt)
- Extended: Tokyo 実機 `debot-tokyo:/opt/debot/market_data_btceth_extended_*.jsonl` (#123 で運用中、~2026-04-29 に 7 日分が揃う)

### 9.2 Phase 0 スクリプト (`scripts/phase0_spread_analysis.py`)

- 両ダンプを読み、`ts` を秒バケットに align
- spread 時系列、分布、ACF、half-life (Ornstein-Uhlenbeck fit) を出力
- GO 判定: ±1.5σ 超過頻度 × 期待 mean-reverted PnL > 2.5 bps (往復コスト lower bound)

Phase 0 の結論が NG の場合は **実装着手せず issue をクローズ** (Risk セクション「Spread 定常化」の現実化)。

### 9.3 Phase 1 BT engine

既存 `ReplayConnector` (pairtrade) は単一 venue 前提。2 venue 同時 replay 対応のため:
- `ReplayConnector` を 2 インスタンス立てて同一タイムラインに沿って `tick()` を呼ぶ
- 両 venue の quote をローカル clock で align するドライバを書く

既存コードを大きく変えずに済む公算あり。Phase 1 着手時に詳細化。

## 10. Open questions (Phase 0 GO 時に確定させる)

1. **rolling_window 最適値** — Phase 0 の ACF 結果から決定
2. **Extended maker 化の成立率** — live paper で測定。BTC Extended は tick 1.0 なので post-only 成立しやすいはずだが未確認
3. **Funding 織り込みの要否** — Phase 1 BT で on/off 比較
4. **複数 symbol 同時運用** — BTC で安定したら ETH を後から追加 (capital 余力次第)
5. **order coordination の粒度** — Extended 先 → Lighter 後の serialized 方式が serve the purpose なのか、両 venue 並列発注 (inventory skew 許容) の方が edge 捕捉率高いのか

## 11. 参考

- `bot-strategy#166` — 親 issue、Phase 0-4 の全体像
- `bot-strategy#102` — Extended dual-sided MM (直交戦略、capital 共有)
- `bot-strategy#123` — Extended Tokyo deploy (data 依存)
- `bot-strategy#46` — BT data source 一覧
- `~/bot/pairtrade/` — scaffold 元、pairtrade.rs / ports/replay_dex.rs が直接の参考
- `~/bot/dex-connector/README_LIGHTER.md` — Go/libsigner セットアップ手順
