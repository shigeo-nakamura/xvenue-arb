use async_trait::async_trait;
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
    FilledOrdersResponse, HyperliquidConnector, LastTradesResponse, OpenOrdersResponse,
    OrderBookSnapshot, OrderSide, TickerResponse, TpSl, TriggerOrderStyle,
};
#[cfg(feature = "lighter-sdk")]
use dex_connector::{create_lighter_connector, LighterConnector, LighterConnectorConfig};
#[cfg(feature = "extended-sdk")]
use dex_connector::create_extended_connector;

use rust_decimal::Decimal;

#[cfg(feature = "lighter-sdk")]
use crate::config::get_lighter_config_from_env;
#[cfg(feature = "extended-sdk")]
use crate::config::get_extended_config_from_env;
use crate::config::{get_hyperliquid_config_from_env, RunMode};
use crate::rate_limit_notifier::{notify_lighter_waf_cooldown, notify_rate_limit};
use lazy_static::lazy_static;
use std::env;

lazy_static! {
    static ref FILLED_PROBABILITY_IN_EMULATION: Decimal = {
        match env::var("FILLED_PROBABILITY_IN_EMULATION") {
            Ok(val) => val.parse::<Decimal>().unwrap_or(Decimal::new(1, 0)),
            Err(_) => Decimal::new(1, 0),
        }
    };
}

pub struct DexConnectorBox {
    pub inner: Box<dyn DexConnector>,
}

impl DexConnectorBox {
    fn report_rate_limit(&self, operation: &str, detail: &str, err: &DexError) {
        // New structured form of the Lighter WAF cooldown (HTTP 405 +
        // x-amzn-waf-action: captcha or HTTP 429). Send a single deduped email
        // per engagement event across all bot processes on this host. See
        // bot-strategy#35.
        if let DexError::RateLimited { until_unix } = err {
            let context = format!("{} ({})", operation, detail);
            notify_lighter_waf_cooldown(*until_unix, &context);
            return;
        }
        let err_text = err.to_string();
        if err_text.contains("429") || err_text.contains("Too Many Requests") {
            let context = format!("{} ({})", operation, detail);
            notify_rate_limit(&context, &err_text);
        }
    }

    // instance_id is only read from the lighter-sdk arm; extended-sdk-only
    // builds (Tokyo, bot-strategy#123) don't consume it.
    #[cfg_attr(not(feature = "lighter-sdk"), allow(unused_variables))]
    pub async fn create(
        dex_name: &str,
        rest_endpoint: &str,
        web_socket_endpoint: &str,
        dry_run: bool,
        agent_name: Option<String>,
        token_list: &[String],
        // Optional instance id for the multi-strategy single-process
        // architecture (shigeo-nakamura/bot-strategy#25). When `Some`, the
        // Lighter env loader prefers credentials suffixed with this id so
        // each strategy variant can target its own sub-account. `None`
        // preserves single-instance behavior.
        instance_id: Option<&str>,
    ) -> Result<Self, DexError> {
        match dex_name {
            "hyperliquid" => {
                let run_mode = if dry_run {
                    RunMode::Dry
                } else {
                    RunMode::RealTrade
                };
                let hyperliquid_config = match get_hyperliquid_config_from_env(run_mode).await {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(DexError::Other(e.to_string()));
                    }
                };

                let token_list_refs: Vec<&str> = token_list.iter().map(|s| s.as_str()).collect();
                let connector = HyperliquidConnector::new(
                    rest_endpoint,
                    web_socket_endpoint,
                    &hyperliquid_config.private_key,
                    &hyperliquid_config.evm_wallet_address,
                    hyperliquid_config.vault_address,
                    !dry_run,
                    agent_name,
                    &token_list_refs,
                )
                .await?;

                Ok(DexConnectorBox {
                    inner: Box::new(connector),
                })
            }
            #[cfg(feature = "lighter-sdk")]
            "lighter" => {
                let lighter_config = match get_lighter_config_from_env(instance_id).await {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(DexError::Other(e.to_string()));
                    }
                };

                let mut account_index = lighter_config.account_index;

                // Auto-discover account_index if not set (0 = not configured)
                if account_index == 0 {
                    let wallet_address =
                        lighter_config.wallet_address.as_deref().ok_or_else(|| {
                            DexError::Other(
                                "LIGHTER_ACCOUNT_INDEX not set and LIGHTER_WALLET_ADDRESS not set. \
                                 Set one of them to enable account discovery."
                                    .to_string(),
                            )
                        })?;
                    log::info!(
                        "LIGHTER_ACCOUNT_INDEX not set, discovering for api_key_index={}...",
                        lighter_config.api_key_index
                    );
                    let tmp_config = LighterConnectorConfig {
                        api_key_public: lighter_config.api_key.clone(),
                        api_key_index: lighter_config.api_key_index,
                        api_private_key_hex: lighter_config.private_key.clone(),
                        evm_wallet_private_key: lighter_config.evm_wallet_private_key.clone(),
                        account_index: 0,
                        base_url: lighter_config.base_url.clone(),
                        websocket_url: lighter_config.websocket_url.clone(),
                        tracked_symbols: vec![],
                        ob_stale_secs: None,
                    };
                    let tmp_connector = LighterConnector::new(tmp_config)?;
                    account_index = tmp_connector
                        .discover_account_index(wallet_address)
                        .await?;
                }

                let connector_config = LighterConnectorConfig {
                    api_key_public: lighter_config.api_key,
                    api_key_index: lighter_config.api_key_index,
                    api_private_key_hex: lighter_config.private_key,
                    evm_wallet_private_key: lighter_config.evm_wallet_private_key,
                    account_index,
                    base_url: lighter_config.base_url,
                    websocket_url: lighter_config.websocket_url,
                    tracked_symbols: token_list.to_vec(),
                    ob_stale_secs: None, // use default
                };

                if dry_run {
                    let connector = LighterConnector::new(connector_config)?;
                    Ok(DexConnectorBox {
                        inner: Box::new(connector),
                    })
                } else {
                    let connector = create_lighter_connector(connector_config)?;
                    Ok(DexConnectorBox { inner: connector })
                }
            }
            #[cfg(feature = "extended-sdk")]
            "extended" => {
                let extended_config = get_extended_config_from_env()
                    .await
                    .map_err(|e| DexError::Other(e.to_string()))?;

                let connector = create_extended_connector(
                    extended_config.api_key,
                    extended_config.public_key,
                    extended_config.private_key,
                    extended_config.vault,
                    extended_config.base_url,
                    extended_config.websocket_url,
                    token_list.to_vec(),
                )
                .await?;

                Ok(DexConnectorBox { inner: connector })
            }
            _ => Err(DexError::Other("Unsupported dex".to_owned())),
        }
    }
}

#[async_trait]
impl DexConnector for DexConnectorBox {
    async fn start(&self) -> Result<(), DexError> {
        let result = self.inner.start().await;
        if let Err(ref err) = result {
            self.report_rate_limit("start", "connector", err);
        }
        result
    }

    async fn stop(&self) -> Result<(), DexError> {
        let result = self.inner.stop().await;
        if let Err(ref err) = result {
            self.report_rate_limit("stop", "connector", err);
        }
        result
    }

    async fn restart(&self, max_retries: i32) -> Result<(), DexError> {
        let result = self.inner.restart(max_retries).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "restart",
                &format!("connector | retries={}", max_retries),
                err,
            );
        }
        result
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let result = self.inner.set_leverage(symbol, leverage).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "set_leverage",
                &format!("{} | leverage={}", symbol, leverage),
                err,
            );
        }
        result
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let result = self.inner.get_ticker(symbol, test_price).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_ticker", symbol, err);
        }
        result
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let result = self.inner.get_filled_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_filled_orders", symbol, err);
        }
        result
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let result = self.inner.get_canceled_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_canceled_orders", symbol, err);
        }
        result
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        let result = self.inner.get_open_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_open_orders", symbol, err);
        }
        result
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let detail = symbol.unwrap_or("ALL");
        let result = self.inner.get_balance(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_balance", detail, err);
        }
        result
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        let result = self.inner.get_last_trades(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_last_trades", symbol, err);
        }
        result
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let result = self.inner.get_order_book(symbol, depth).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_order_book", symbol, err);
        }
        result
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let result = self.inner.clear_filled_order(symbol, trade_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "clear_filled_order",
                &format!("{} | trade_id={}", symbol, trade_id),
                err,
            );
        }
        result
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let result = self.inner.clear_all_filled_orders().await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_all_filled_orders", "all", err);
        }
        result
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let result = self.inner.clear_canceled_order(symbol, order_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "clear_canceled_order",
                &format!("{} | order_id={}", symbol, order_id),
                err,
            );
        }
        result
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        let result = self.inner.clear_all_canceled_orders().await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_all_canceled_orders", "all", err);
        }
        result
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .create_order(symbol, size, side, price, spread, reduce_only, expiry_secs)
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "create_order",
                &format!("{} | side={:?} size={}", symbol, side, size),
                err,
            );
        }
        result
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .create_advanced_trigger_order(
                symbol,
                size,
                side,
                trigger_px,
                limit_px,
                order_style,
                slippage_bps,
                tpsl,
                reduce_only,
                expiry_secs,
            )
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "create_advanced_trigger_order",
                &format!(
                    "{} | side={:?} size={} trigger_px={}",
                    symbol, side, size, trigger_px
                ),
                err,
            );
        }
        result
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let result = self.inner.cancel_order(symbol, order_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "cancel_order",
                &format!("{} | order_id={}", symbol, order_id),
                err,
            );
        }
        result
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let detail = symbol.as_deref().unwrap_or("ALL").to_string();
        let result = self.inner.cancel_all_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("cancel_all_orders", &detail, err);
        }
        result
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let order_count = order_ids.len();
        let detail = format!(
            "{} | orders={}",
            symbol.as_deref().unwrap_or("ALL"),
            order_count
        );
        let result = self.inner.cancel_orders(symbol, order_ids).await;
        if let Err(ref err) = result {
            self.report_rate_limit("cancel_orders", &detail, err);
        }
        result
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let detail = symbol.as_deref().unwrap_or("ALL").to_string();
        let result = self.inner.close_all_positions(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("close_all_positions", &detail, err);
        }
        result
    }

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError> {
        let result = self.inner.clear_last_trades(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_last_trades", symbol, err);
        }
        result
    }

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool {
        self.inner.is_upcoming_maintenance(hours_ahead).await
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        let result = self.inner.sign_evm_65b(message).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "sign_evm_65b",
                &format!("message_len={}", message.len()),
                err,
            );
        }
        result
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        let result = self.inner.sign_evm_65b_with_eip191(message).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "sign_evm_65b_with_eip191",
                &format!("message_len={}", message.len()),
                err,
            );
        }
        result
    }

    async fn get_combined_balance(
        &self,
    ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
        let result = self.inner.get_combined_balance().await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_combined_balance", "all", err);
        }
        result
    }

    async fn get_positions(&self) -> Result<Vec<dex_connector::PositionSnapshot>, DexError> {
        self.inner.get_positions().await
    }
}
