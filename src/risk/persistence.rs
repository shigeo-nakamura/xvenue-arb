//! On-disk persistence for [`RiskState`] (#244 D-2).
//!
//! Wraps the raw `RiskState` in a versioned envelope (`RiskStateSnapshot`)
//! so future schema changes can land additive `#[serde(default)]` fields
//! without rejecting older state files. The current writer always stamps
//! `RISK_STATE_VERSION = 1`; the reader accepts whatever version came
//! off disk and lets serde apply field-level defaults on missing keys.
//!
//! Atomic write contract: writes go to `.<file>.tmp.<pid>` first then
//! `rename(2)` over the target so a crash mid-write never leaves a
//! truncated `risk_state.json` for the next boot to read.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::manager::RiskState;

const RISK_STATE_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug)]
struct RiskStateSnapshot {
    #[serde(rename = "_v")]
    version: u32,
    state: RiskState,
}

/// Atomically write `state` to `path`. Logs and continues on failure —
/// risk persistence is best-effort because losing the file just degrades
/// to "halt-state lost across restart", which the operator already
/// recovers via RISK_ACK.
pub(super) fn persist_state(path: &Path, state: &RiskState) {
    let snapshot = RiskStateSnapshot {
        version: RISK_STATE_VERSION,
        state: state.clone(),
    };
    let Ok(json) = serde_json::to_string(&snapshot) else {
        log::warn!("[RISK_STATE] serialize failed");
        return;
    };
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Err(e) = fs::create_dir_all(dir) {
        log::warn!("[RISK_STATE] mkdir {}: {:?}", dir.display(), e);
        return;
    }
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "risk_state.json".to_string());
    let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
    if let Err(e) = fs::write(&tmp, json) {
        log::warn!("[RISK_STATE] tmp write {}: {:?}", tmp.display(), e);
        return;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        log::warn!(
            "[RISK_STATE] rename {} → {}: {:?}",
            tmp.display(),
            path.display(),
            e
        );
        let _ = fs::remove_file(&tmp);
    }
}

/// Read back a previously-persisted `RiskState`. Returns
/// `RiskState::default()` on missing file, read error, or parse error
/// — those are recoverable conditions (operator clears RISK_ACK to
/// re-arm), and crashing on boot would lose the live process. Failures
/// are logged so the boot trace makes the divergence visible.
pub(super) fn load_state(path: &Path) -> RiskState {
    let raw = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return RiskState::default(),
        Err(e) => {
            log::warn!("[RISK_STATE] read {}: {:?}", path.display(), e);
            return RiskState::default();
        }
    };
    match serde_json::from_str::<RiskStateSnapshot>(&raw) {
        Ok(s) => s.state,
        Err(e) => {
            log::warn!("[RISK_STATE] parse {}: {:?}", path.display(), e);
            RiskState::default()
        }
    }
}
