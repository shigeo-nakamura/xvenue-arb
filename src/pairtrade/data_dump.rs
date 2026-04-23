//! Date-rotating data dump writer.
//!
//! Writes JSONL data to files named `{base}_YYYYMMDD.jsonl`, automatically
//! rotating to a new file at UTC midnight. Replaces the previous approach
//! of writing to a single file rotated by logrotate with `copytruncate`,
//! which caused data loss during the copy window.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};

pub(super) struct RotatingDumpWriter {
    /// Base path without extension, e.g. `/opt/debot/market_data_btceth`
    base: PathBuf,
    /// Extension, e.g. `.jsonl`
    ext: String,
    writer: BufWriter<File>,
    current_date: NaiveDate,
}

impl RotatingDumpWriter {
    /// Create a new rotating writer. Opens (or creates) the file for today's
    /// date in append mode.
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

    /// Write a line, rotating the file if the UTC date has changed.
    pub(super) fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        let today = Utc::now().date_naive();
        if today != self.current_date {
            // Flush the old file before switching
            self.writer.flush()?;
            let file = Self::open_file(&self.base, &self.ext, today)?;
            self.writer = BufWriter::new(file);
            self.current_date = today;
            log::info!(
                "[DataDump] Rotated to {}",
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
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn file_path_format() {
        let base = Path::new("/opt/debot/market_data_btceth");
        let ext = ".jsonl";
        let date = NaiveDate::from_ymd_opt(2026, 4, 12).unwrap();
        let path = RotatingDumpWriter::file_path(base, ext, date);
        assert_eq!(
            path.to_string_lossy(),
            "/opt/debot/market_data_btceth_20260412.jsonl"
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
}
