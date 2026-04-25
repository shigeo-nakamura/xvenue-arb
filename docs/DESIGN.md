# xvenue-arb 設計メモ

Cross-venue statistical arbitrage between Lighter and Extended. 対応 issue: [bot-strategy#166](https://github.com/shigeo-nakamura/bot-strategy/issues/166)。

このドキュメントは Phase 0 (データ feasibility) 着手前の **設計ドラフト** であり、Phase 0 GO 後に確定させる。

## 0. 確定事項 (2026-04-23 レビューで合意)

- **初期 symbol**: ~~BTC-USD のみ。ETH は Phase 3 probe で安定したら追加検討~~ → **2026-04-25 改定**: **ETH を Phase 0 / 1 の primary 対象とし、BTC は secondary**。3-day overlap dump で ETH の persistence-filtered trade rate は BTC の 2.4× / 平均 capture は 2.3× (issue #166 comment 4318185827 + 4318159352)
- **両脚発注方式**: **hybrid** — entry は serialized (Extended post-only → fill 後に Lighter)、exit は parallel (両 venue 同時に reduce-only)。Entry の single-leg-filled リスクが exit より金額大きいため
- **Extended アカウント**: 新規サブアカウント (既存 `debot-pair-btceth-extended` との分離のため。position / funding の相殺を避ける)
- **Capital sizing**: **account equity の %** で指定 (`trade_size_pct: 0.05` = equity の 5%)。固定 USD でなく equity 追従にすることで PnL 蓄積に比例して size 増、資本効率を保つ
- **`src/pairtrade/` 撤去**: scaffold の initial commit (`b7cf98b`) で git 履歴に残るため、次コミットで即削除。以降は `src/xvenue/` に一本化
- **2026-04-25 追加 — エッジの真の出所**: cross-venue mid 差ではなく **inside-spread asymmetry**。Lighter standard は BTC mean inside 8.7 bps / ETH mean 10.4 bps と Extended (両方 1 tick = 0.13 / 0.43 bps) より遥かに広い。passive MM が Extended で機能しない構造的理由 (#102 の 6 reasons) はそのまま arb-taker 戦略にとっての追い風になる
- **2026-04-25 追加 — signal 方法論**: z-score でなく `|spread - rolling_mean| ≥ ABS_THRESHOLD_BPS` の絶対 bps 閾値 + persistence filter (15-30s)。bucket-scale ノイズを除外する。詳細は `phase0.md` v2 GO 基準

## 1. 戦略サマリ

両 venue の同一 perp 銘柄 (Phase 0/1 は ETH-USD primary、BTC-USD secondary) の価格を spread = `(P_ext - P_lt) / P_lt * 10000` [bps] で観測し、`|spread - μ_roll| ≥ ABS_THRESHOLD_BPS` が `persistence_sec` 秒以上持続した瞬間に両脚建て、rolling mean revert で決済する delta-neutral 戦略。

```
dev = spread - μ_roll
dev >  +abs_threshold で persist 確認  →  Extended SHORT + Lighter LONG
dev <  -abs_threshold で persist 確認  →  Extended LONG  + Lighter SHORT
dev ≈ 0 (mean を跨いだら)              →  両脚クローズ
```

`μ_roll` は 30 分窓のローリング平均で、cross-venue の構造的 funding bias (BTC ~+1.9 bps / ETH ~+3.1 bps、Extended が常時上方) を吸収する。z-score 方式 (旧 v1) はゼロ復帰を仮定するためこの bias 側で過剰発火していた。詳細は `phase0.md` の v2 methodology。

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

### 4.2 Exit (通常時) — parallel

Entry は serialized だが exit は **両 venue 並列発注** (決定事項 §0)。早く flat を取る方が残存リスクを減らせる。

1. |z| < exit_z で exit signal
2. 両 venue 同時に reduce-only order 発行
   - Lighter: market で即時
   - Extended: post-only limit → 短 chase → 必要なら taker
3. 両脚 flat 確認で 1 サイクル完了

parallel exit で片脚先 fill → 反対脚未 fill が起きた場合は emergency flatten (§4.3) に落ちて taker clean-up。

### 4.3 Emergency flatten

以下の条件で **両脚即時 taker clean-up**:
- WebSocket stale > 5s (片 venue でも)
- 片脚約定後、反対脚が fill 期限 (例: 3s) 内に約定せず
- inventory skew > 許容値 (e.g. |Δnotional| > $50)
- global kill signal (SIGUSR1 / dashboard トリガ)
- **Binance 1m reference との偏差 > 30 bps (片 venue で 3 bucket 連続)** — stale-quote pattern。`scripts/phase0/fetch_reference.sh` のリファレンスを live 化して使う。2026-04-21 22:33-22:55 UTC に Lighter で 23 分間 stuck quote が発生し +2182 bps の phantom signal を生んだケースを防ぐ

Extended 側 taker 手数料 (2.5 bp) はこの時だけは許容する。

### 4.4 残コネクタ stuck の escalation (#102 P2 流用)

slow-mm が 2026-04-24 に 167 分間 stuck した事象 (#102 P2) と同型のリスクが xvenue-arb には複数 venue 分存在する。slow-mm に入れた 3 段 escalation をそのまま流用:

1. **REST 失敗カウンタ**: venue ごとに `get_positions` / `get_filled_orders` の連続失敗を数え、3 回連続で escalate
2. **Reduce-only retry counter**: `close_all_positions` 連続失敗を数え、5 回連続で kill switch
3. **Kill switch on file**: `/var/run/xvenue-arb/STUCK` を best-effort で書き出し、新規 entry を停止 (operator 介入が必要なシグナル)
4. **30s 間隔 retry**: TakerFlattening 状態で `inventory != 0` のとき次 tick 連打しない (slow-mm の 167 分 stuck の真因の修正)

Lighter の `connect_timeout(5s)+timeout(15s)` は元から、Extended は v4.2.81 (#102 P2) で同パターンを獲得済み。両 venue とも request-level に bound されている。

### 4.4 Position sizing

- **account equity の %** で指定 (`trade_size_pct`、決定事項 §0)。固定 USD でなく equity 追従
- entry 時に `notional = equity_usd * trade_size_pct` を両 venue で揃える (delta-neutral)
- Tick 粒度差で size が完全一致しないため、Extended 側を基準に Lighter size を算出 (Lighter 0.1 tick は十分細かい)
- equity 取得は各 venue の `DexConnector::get_account` 相当。両 venue で separate に管理するのでなく、**全体 equity は Extended + Lighter の合計残高**として扱う
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

`configs/xvenue-arb/debot-xvenue-arb-eth.yaml` (primary、ETH first per 2026-04-25 改定):

```yaml
strategy:
  symbol_ext: ETH-USD          # Extended 側シンボル
  symbol_lt:  ETH-USD          # Lighter 側シンボル
  abs_threshold_bps: 5.0       # |spread - μ_roll| 閾値 (v2 methodology)
  persistence_sec: 15          # signal 確認窓
  exit_at_mean_cross: true     # μ_roll を跨いだ瞬間に exit
  max_hold_sec: 600            # max hold (funding cycle 突入を回避)
  force_close_dev_bps: 30      # 逆方向暴走カットオフ
  rolling_window_sec: 1800
  spread_bucket_ms: 1000
  # 旧 z-score 互換 (legacy diagnostics 用、live signal は使わない)
  entry_z_legacy: 1.5
  exit_z_legacy: 0.3

sizing:
  trade_size_pct: 0.05         # 全 equity (Ext + Lt 合計) の 5%
  min_notional_usd: 20         # 下限 (dust order 防止)
  max_notional_usd: 5000       # 上限 (equity 急増時の暴走防止)
  max_concurrent: 1

execution:
  # entry = serialized (Extended 先 → Lighter 後)、exit = parallel
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
  kill_switch_file: /var/run/xvenue-arb/STUCK
  # Reference cross-check (stale-quote guard、§4.3)
  reference_max_dev_bps: 30
  reference_consec_buckets_for_halt: 3
  # Funding cycle filter
  no_entry_window_before_funding_sec: 1800
  no_entry_window_after_funding_sec: 600
  # #102 P2 escalation
  rest_consec_fail_to_escalate: 3
  reduce_only_consec_fail_to_kill: 5

venues:
  extended:
    # 新規サブアカウント (既存 debot-pair-btceth-extended と分離)
    account_id: <env: EXTENDED_XVENUE_ACCOUNT_ID>
    # ... credentials via env (EXTENDED_XVENUE_* プレフィックスで分離)
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

### 8.1 Tokyo (`debot-tokyo` / ARM64 / Amazon Linux 2023)

サーバー上の Go toolchain は **不要**。libsigner.so は CI 側で arm64 クロスコンパイルして S3 経由で配布する方式 (pairtrade が既に採用済み)。

実際 `/opt/debot/lib/libsigner.so` は pairtrade CI の arm64 ビルドステップで既にデプロイ済み (11.8 MB、ARM aarch64 ELF、依存はすべて標準 libc / libresolv で解決)。xvenue-arb はこの資産をそのまま流用できる。

ランタイム側の要件は:
- `/opt/debot/lib/libsigner.so` (既に存在)
- systemd unit `debot-xvenue-arb.service` 新規追加
- 起動スクリプトで `LD_LIBRARY_PATH=/opt/debot/lib` を export (pairtrade の `debot-pair-btceth.sh` と同じパターン)

### 8.2 CI (arm64 クロスビルド)

pairtrade の `.github/workflows/ci.yml` Tokyo job をベースに以下を改変:
- **libsigner.so arm64 ビルド**: pairtrade の Docker `--platform linux/arm64` + `dnf install golang gcc` + `CGO_ENABLED=1 GOARCH=arm64 go build -buildmode=c-shared` ステップをそのまま流用
- **cargo build**: `--no-default-features --features extended-sdk` から **default features (= lighter-sdk + extended-sdk)** に変更
- **S3 prefix**: `debot-extended/` から `debot-xvenue-arb/` に変更
- **SSM deploy target**: `debot-pair-btceth-extended.service` から `debot-xvenue-arb.service` に変更

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

## 10. Open questions

§0 で確定した Q4-Q8 以外で残っているもの:

1. **rolling_window 最適値** — Phase 0 の ACF 結果から決定 (仮 1800 秒)
2. **Extended maker 化の成立率** — live paper (Phase 2) で測定。BTC Extended は tick 1.0 なので post-only 成立しやすいはずだが未確認
3. **Funding 織り込みの要否** — Phase 1 BT で on/off 比較 (仮 config flag `funding_adjustment: false` default)

## 11. 参考

- `bot-strategy#166` — 親 issue、Phase 0-4 の全体像
- `bot-strategy#102` — Extended dual-sided MM (直交戦略、capital 共有)
- `bot-strategy#123` — Extended Tokyo deploy (data 依存)
- `bot-strategy#46` — BT data source 一覧
- `~/bot/pairtrade/` — scaffold 元、pairtrade.rs / ports/replay_dex.rs が直接の参考
- `~/bot/dex-connector/README_LIGHTER.md` — Go/libsigner セットアップ手順
