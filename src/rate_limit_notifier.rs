use crate::email_client::EmailClient;
use once_cell::sync::Lazy;
use std::fs;
use std::path::PathBuf;

static RATE_LIMIT_NOTIFIER: Lazy<RateLimitNotifier> = Lazy::new(RateLimitNotifier::new);

/// Host-shared dedup file for the Lighter WAF cooldown email. Sibling of
/// `/tmp/lighter_waf_cooldown` (the cooldown deadline file written by
/// dex-connector). Multiple bot processes on the same host check this so that
/// only the first observer per engagement event sends the alert email. See
/// bot-strategy#35.
const WAF_EMAIL_DEDUP_FILE: &str = "/tmp/lighter_waf_cooldown_emailed";

pub fn notify_rate_limit(context: &str, detail: &str) {
    RATE_LIMIT_NOTIFIER.notify(context, detail);
}

/// Send a one-shot email when the Lighter WAF cooldown engages, deduped across
/// bot processes by the host-shared dedup file. Safe to call from every
/// `report_rate_limit` site — only the first observer per engagement event
/// (across all bot processes on the host) will actually send.
pub fn notify_lighter_waf_cooldown(until_unix: i64, context: &str) {
    if let Some(prev) = read_last_emailed_until() {
        if until_unix <= prev {
            log::debug!(
                "[RateLimit] WAF cooldown email already sent for until_unix>={}, suppressing",
                prev
            );
            return;
        }
    }
    write_last_emailed_until(until_unix);
    RATE_LIMIT_NOTIFIER.notify_waf_cooldown(until_unix, context);
}

fn dedup_path() -> PathBuf {
    PathBuf::from(WAF_EMAIL_DEDUP_FILE)
}

fn read_last_emailed_until() -> Option<i64> {
    fs::read_to_string(dedup_path())
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
}

fn write_last_emailed_until(until_unix: i64) {
    let path = dedup_path();
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    if fs::write(&tmp, until_unix.to_string()).is_ok() {
        let _ = fs::rename(&tmp, &path);
    }
}

struct RateLimitNotifier {
    token_name: String,
}

impl RateLimitNotifier {
    fn new() -> Self {
        let token_name = std::env::var("SYMBOLS")
            .or_else(|_| std::env::var("SYMBOL"))
            .unwrap_or_default();
        Self { token_name }
    }

    fn notify(&self, context: &str, detail: &str) {
        let subject = if self.token_name.is_empty() {
            format!("[RateLimit] {}", context)
        } else {
            format!("[{}] Rate limit - {}", self.token_name, context)
        };
        let body = format!(
            "HTTP 429 Too Many Requests detected while {}.\nDetail: {}",
            context, detail
        );

        EmailClient::new().send(&subject, &body);
        log::info!(
            "📧 [RateLimit] Email notification sent for '{}' (detail: {})",
            context,
            detail
        );
    }

    fn notify_waf_cooldown(&self, until_unix: i64, context: &str) {
        let subject = if self.token_name.is_empty() {
            format!("[RateLimit] Lighter WAF cooldown engaged ({})", context)
        } else {
            format!(
                "[{}] Lighter WAF cooldown engaged ({})",
                self.token_name, context
            )
        };
        let body = format!(
            "Lighter WAF rate-limit / CAPTCHA detected on this host while {}.\n\
             All Lighter REST calls on this host are now failing fast until \
             unix={} (host-shared cooldown via /tmp/lighter_waf_cooldown).\n\
             \n\
             Trading bots will skip strategy ticks until the cooldown expires.\n\
             See bot-strategy#35 for context. If this fires repeatedly, the \
             root cause is likely a WS reconnect cascade or insufficient \
             stagger between bots on the same IP.",
            context, until_unix
        );

        EmailClient::new().send(&subject, &body);
        log::info!(
            "📧 [RateLimit] Lighter WAF cooldown email sent (until_unix={}, context={})",
            until_unix,
            context
        );
    }
}
