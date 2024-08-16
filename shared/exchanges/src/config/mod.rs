use crate::{
    enums::TraderExchangeId,
    structs::{ApiCredentials, ApiEndpoints, ExchangeConfig},
};
use common::constants::{API_KEY_ENV_SUFFIX, API_SECRET_ENV_SUFFIX};
// use dotenv::dotenv;
use std::{collections::HashMap, env::var, sync::LazyLock};

pub static WS_RECONNECT_INTERVAL_IN_SECS: u64 = 2;

pub static TRADER_EXCHANGES_CONFIG_MAP: LazyLock<HashMap<TraderExchangeId, ExchangeConfig>> =
    LazyLock::new(|| {
        // dotenv().ok();
        let mut configs = HashMap::new();
        {
            let env = var("ENV_NAME")
                .expect("ENV_NAME env var to be provided")
                .to_uppercase();
            let http_url: String;
            let ws_url: String;
            let exchange_title = "Bybit".to_uppercase();
            let api_key_env_var = format!("{}_{}_{}", exchange_title, API_KEY_ENV_SUFFIX, env);
            let api_key = var(&api_key_env_var)
                .expect(&format!("{} env var to be provided", api_key_env_var));
            let api_secret_env_var =
                format!("{}_{}_{}", exchange_title, API_SECRET_ENV_SUFFIX, env);
            let api_secret = var(&api_secret_env_var)
                .expect(&format!("{} env var to be provided", api_secret_env_var));

            if env == "PROD".to_string() {
                http_url = var("BYBIT_HTTP_BASE_URL_PROD")
                    .expect("BYBIT_HTTP_BASE_URL_PROD env var to be provided");
                ws_url = var("BYBIT_WS_BASE_URL_PROD")
                    .expect("BYBIT_WS_BASE_URL_PROD env var to be provided");
            } else {
                http_url = var("BYBIT_HTTP_BASE_URL_DEV")
                    .expect("BYBIT_HTTP_BASE_URL_DEV env var to be provided");
                ws_url = var("BYBIT_WS_BASE_URL_DEV")
                    .expect("BYBIT_WS_BASE_URL_DEV env var to be provided");
            }

            let config = ExchangeConfig {
                credentials: ApiCredentials {
                    key: Box::leak(api_key.into_boxed_str()),
                    secret: Box::leak(api_secret.into_boxed_str()),
                },
                endpoints: ApiEndpoints {
                    ws: Box::leak(ws_url.into_boxed_str()),
                    http: Box::leak(http_url.into_boxed_str()),
                },
            };

            configs.insert(TraderExchangeId::Bybit, config);
        }

        configs
    });
