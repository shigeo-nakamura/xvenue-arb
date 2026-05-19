//! Per-tick market-data dump writer for xvenue-arb (bot-strategy#455).
//!
//! Writes one JSONL row per evaluation tick to a UTC-date-rotated file.
//! Mirrors the pairtrade dump pattern at `src/pairtrade/data_dump.rs` so
//! BT replay can consume xvenue-arb's own dumps with the same
//! `DualReplay` reader the pairtrade BT uses.
//!
//! File naming: `{base}_YYYYMMDD.jsonl`. Rotation happens at UTC
//! midnight; the writer transparently switches to the next-day file on
//! the first `write_line` after midnight. Disabling the writer keeps
//! the field as `None` and `run_one_tick` skips the entire block.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};

pub(super) struct RotatingDumpWriter {
    /// Base path without extension, e.g. `/opt/debot/market_data_xvenue_ext`.
    base: PathBuf,
    /// Extension, e.g. `.jsonl`.
    ext: String,
    writer: BufWriter<File>,
    current_date: NaiveDate,
}

impl RotatingDumpWriter {
    /// Open (or create) the file for today's UTC date in append mode.
    pub(super) fn new(configured_path: &str) -> std::io::Result<Self> {
        let path = Path::new(configured_path);
        let ext = path
            .extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default();
        let base = path.with_extension("");
        let today = Utc::now().date_naive();
        let file = Self::open_file(&base, &ext, today)?;
        Ok(Self {
            base,
            ext,
            writer: BufWriter::new(file),
            current_date: today,
        })
    }

    /// Write one JSONL line, rotating the file if the UTC date has changed.
    pub(super) fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        let today = Utc::now().date_naive();
        if today != self.current_date {
            self.writer.flush()?;
            let file = Self::open_file(&self.base, &self.ext, today)?;
            self.writer = BufWriter::new(file);
            self.current_date = today;
            log::info!(
                "[XVENUE/dump] rotated to {}",
                Self::file_path(&self.base, &self.ext, today).display()
            );
        }
        writeln!(self.writer, "{}", line)
    }

    fn file_path(base: &Path, ext: &str, date: NaiveDate) -> PathBuf {
        let date_str = date.format("%Y%m%d").to_string();
        let filename = format!(
            "{}_{}{}",
            base.file_name()
                .map(|f| f.to_string_lossy().into_owned())
                .unwrap_or_default(),
            date_str,
            ext
        );
        base.with_file_name(filename)
    }

    fn open_file(base: &Path, ext: &str, date: NaiveDate) -> std::io::Result<File> {
        let path = Self::file_path(base, ext, date);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)
    }
}

impl std::fmt::Debug for RotatingDumpWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotatingDumpWriter")
            .field("base", &self.base)
            .field("current_date", &self.current_date)
            .finish()
    }
}

/// Render one venue's per-tick row as the JSONL shape `DualReplay`
/// expects: `{"timestamp": <ms>, "prices": {"<symbol>": { bid_price,
/// ask_price, bid_size, ask_size, price, exchange_ts }}}`.
///
/// Wall-clock `timestamp` is the BT's per-row clock (mirrors the
/// pairtrade convention). `exchange_ts` is the venue's own per-snap ts
/// so the replay can detect stale-WS rows the same way the pairtrade
/// BT does. `funding_rate` / `min_order` / `min_tick` are omitted; the
/// replay reader treats them as optional defaults and the BT does not
/// read them.
pub(super) fn render_dump_row(
    symbol: &str,
    timestamp_ms: i64,
    exchange_ts_ms: u64,
    mid: rust_decimal::Decimal,
    bid_price: rust_decimal::Decimal,
    ask_price: rust_decimal::Decimal,
    bid_size: rust_decimal::Decimal,
    ask_size: rust_decimal::Decimal,
) -> String {
    format!(
        r#"{{"timestamp":{ts},"prices":{{"{sym}":{{"price":"{p}","funding_rate":"0","bid_price":"{b}","ask_price":"{a}","bid_size":"{bs}","ask_size":"{as_}","exchange_ts":{ets}}}}}}}"#,
        ts = timestamp_ms,
        sym = symbol,
        p = mid,
        b = bid_price,
        a = ask_price,
        bs = bid_size,
        as_ = ask_size,
        ets = exchange_ts_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn file_path_format_matches_pairtrade_convention() {
        let base = Path::new("/opt/debot/market_data_xvenue_ext");
        let ext = ".jsonl";
        let date = NaiveDate::from_ymd_opt(2026, 5, 19).unwrap();
        let path = RotatingDumpWriter::file_path(base, ext, date);
        assert_eq!(
            path.to_string_lossy(),
            "/opt/debot/market_data_xvenue_ext_20260519.jsonl"
        );
    }

    #[test]
    fn writes_to_dated_file() {
        let dir = TempDir::new().unwrap();
        let base_path = dir.path().join("dump.jsonl");
        let mut writer = RotatingDumpWriter::new(base_path.to_str().unwrap()).unwrap();
        writer.write_line(r#"{"test": 1}"#).unwrap();
        writer.writer.flush().unwrap();

        let today = Utc::now().date_naive().format("%Y%m%d").to_string();
        let expected_file = dir.path().join(format!("dump_{}.jsonl", today));
        assert!(expected_file.exists(), "dated file should exist");

        let mut content = String::new();
        File::open(&expected_file)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert!(content.contains(r#"{"test": 1}"#));
    }

    #[test]
    fn render_dump_row_matches_dualreplay_schema() {
        let line = render_dump_row(
            "ETH",
            1779148800002,
            1779148799837,
            dec!(2128.55),
            dec!(2128.5),
            dec!(2128.6),
            dec!(67.179),
            dec!(68.019),
        );
        // Sanity: must parse as valid JSON and round-trip the symbol
        // through the `prices` map.
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["timestamp"], 1779148800002_i64);
        let p = &v["prices"]["ETH"];
        assert_eq!(p["bid_price"], "2128.5");
        assert_eq!(p["ask_price"], "2128.6");
        assert_eq!(p["bid_size"], "67.179");
        assert_eq!(p["ask_size"], "68.019");
        assert_eq!(p["price"], "2128.55");
        assert_eq!(p["exchange_ts"], 1779148799837_i64);
    }
}
