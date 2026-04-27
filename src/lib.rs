// src/lib.rs
pub mod ports {
    pub mod live_dual;
    pub mod replay_dex;
}
pub mod config;
pub mod email_client;
pub mod error_counter;
pub mod rate_limit_notifier;
pub mod trade;
pub mod xvenue;
