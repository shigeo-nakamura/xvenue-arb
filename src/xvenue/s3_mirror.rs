//! Optional S3 mirror for the status writer (bot-strategy#343).
//!
//! When `STATUS_S3_BUCKET` and `STATUS_S3_KEY_PREFIX` are both set, the
//! status writer fires a fire-and-forget `PutObject` for each local
//! `status.json` rewrite. Failures are logged at WARN and do not block
//! the local write — matches the durability model of the existing
//! `equity_history.jsonl` append path.
//!
//! The S3 client is constructed once per process behind a `OnceCell`
//! and shared across all per-instance reporters. Region is forced to
//! `eu-central-1` because the `debot-dashboard` bucket is single-region
//! there; Tokyo bots cross-region write the same way #255's
//! `archive_bt_replay_events.sh` does.

use std::env;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use once_cell::sync::OnceCell;
use tokio::runtime::Handle;
use tokio::sync::OnceCell as AsyncOnceCell;

/// Bucket region. Hard-coded to `eu-central-1` because the
/// `debot-dashboard` bucket is single-region there. If we ever
/// regionalize, promote this to `STATUS_S3_REGION`.
const BUCKET_REGION: &str = "eu-central-1";

/// Process-global handle. Initialized on first call to `from_env`,
/// stays None when the env vars are absent so production deployments
/// without the mirror feature pay no cost.
static GLOBAL: OnceCell<Option<Arc<S3Mirror>>> = OnceCell::new();

pub(crate) struct S3Mirror {
    bucket: String,
    /// Trailing-slash-free prefix, e.g. `debot/status/frankfurt`.
    key_prefix: String,
    /// Built lazily on first put so callers in non-tokio contexts (unit
    /// tests, BT) don't pay for the credential resolution.
    client: AsyncOnceCell<Client>,
}

impl std::fmt::Debug for S3Mirror {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Mirror")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix)
            .field("client_init", &self.client.initialized())
            .finish()
    }
}

impl S3Mirror {
    /// Read `STATUS_S3_BUCKET` / `STATUS_S3_KEY_PREFIX`. When either is
    /// unset / empty, returns `None` and subsequent `put_async` calls
    /// are no-ops.
    pub(crate) fn from_env() -> Option<Arc<Self>> {
        GLOBAL
            .get_or_init(|| {
                let mirror = Self::build_from_env_unlocked()?;
                log::info!(
                    "[STATUS_S3] mirror enabled bucket={} prefix={} region={}",
                    mirror.bucket,
                    mirror.key_prefix,
                    BUCKET_REGION
                );
                Some(Arc::new(mirror))
            })
            .clone()
    }

    /// Same env-parsing logic as `from_env`, but bypasses the process
    /// global. Test-only — production callers must always go through
    /// `from_env` so every status reporter shares one S3 client.
    fn build_from_env_unlocked() -> Option<Self> {
        let bucket = env::var("STATUS_S3_BUCKET")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())?;
        let prefix = env::var("STATUS_S3_KEY_PREFIX")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())?;
        Some(Self {
            bucket,
            key_prefix: prefix,
            client: AsyncOnceCell::new(),
        })
    }

    /// Fire-and-forget put with `Content-Type: application/json`.
    /// Spawns a tokio task on the current runtime and returns
    /// immediately. When no runtime is current (e.g. unit tests calling
    /// `write_snapshot` directly) the call becomes a no-op so non-tokio
    /// paths keep working.
    ///
    /// `file_name` is appended to `key_prefix` with a `/` separator —
    /// it must NOT include a leading slash.
    pub(crate) fn put_async(self: &Arc<Self>, file_name: &str, body: Vec<u8>) {
        self.put_async_with_content_type(file_name, body, "application/json");
    }

    /// Like `put_async`, but with an explicit `Content-Type`. Used by
    /// sibling-file mirrors (`equity_history.jsonl`, `backtest_alert.json`)
    /// — see bot-strategy#343 Phase 3.
    pub(crate) fn put_async_with_content_type(
        self: &Arc<Self>,
        file_name: &str,
        body: Vec<u8>,
        content_type: &'static str,
    ) {
        let handle = match Handle::try_current() {
            Ok(h) => h,
            Err(_) => return,
        };
        let me = Arc::clone(self);
        let file_name = file_name.to_string();
        handle.spawn(async move {
            let key = format!("{}/{}", me.key_prefix, file_name);
            let client = me.client().await;
            let resp = client
                .put_object()
                .bucket(&me.bucket)
                .key(&key)
                .cache_control("max-age=2")
                .content_type(content_type)
                .body(ByteStream::from(body))
                .send()
                .await;
            match resp {
                Ok(_) => log::debug!("[STATUS_S3] put ok key={}", key),
                Err(err) => log::warn!("[STATUS_S3] put failed key={} err={:?}", key, err),
            }
        });
    }

    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let cfg = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(BUCKET_REGION))
                    .load()
                    .await;
                Client::new(&cfg)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env var access in tests must serialize: `from_env_unlocked` reads
    /// process env vars, and parallel test execution can otherwise see
    /// each other's mutations. Recover from poisoning so a panic in one
    /// test does not cascade-fail the rest.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        match ENV_LOCK.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    #[test]
    fn from_env_unset_returns_none() {
        let _g = lock_env();
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
        assert!(S3Mirror::build_from_env_unlocked().is_none());
    }

    #[test]
    fn from_env_partial_returns_none() {
        let _g = lock_env();
        env::set_var("STATUS_S3_BUCKET", "debot-dashboard");
        env::remove_var("STATUS_S3_KEY_PREFIX");
        assert!(S3Mirror::build_from_env_unlocked().is_none());
        env::remove_var("STATUS_S3_BUCKET");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/frankfurt");
        assert!(S3Mirror::build_from_env_unlocked().is_none());
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }

    #[test]
    fn from_env_strips_trailing_slash_in_prefix() {
        let _g = lock_env();
        env::set_var("STATUS_S3_BUCKET", "debot-dashboard");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/frankfurt/");
        let mirror = S3Mirror::build_from_env_unlocked().expect("present");
        assert_eq!(mirror.bucket, "debot-dashboard");
        assert_eq!(mirror.key_prefix, "debot/status/frankfurt");
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }

    #[test]
    fn from_env_treats_whitespace_as_unset() {
        let _g = lock_env();
        env::set_var("STATUS_S3_BUCKET", "   ");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/frankfurt");
        assert!(S3Mirror::build_from_env_unlocked().is_none());
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }

    #[test]
    fn put_async_without_runtime_is_noop() {
        // Calling put_async outside a tokio runtime must not panic; it
        // logs and returns. This protects unit-test paths and any
        // legacy synchronous caller.
        let mirror = Arc::new(S3Mirror {
            bucket: "test".into(),
            key_prefix: "p".into(),
            client: AsyncOnceCell::new(),
        });
        mirror.put_async("foo.json", b"{}".to_vec());
    }
}
