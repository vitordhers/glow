use crate::structs::{ApiCredentials, ApiEndpoints, ExchangeConfig, ExchangeContext};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use common::{
    constants::{API_KEY_ENV_SUFFIX, API_SECRET_ENV_SUFFIX},
    r#static::SYMBOLS_MAP,
    structs::Contract,
};
use dotenv::dotenv;
use std::{collections::HashMap, env::var, sync::LazyLock};

pub static WS_RECONNECT_INTERVAL_IN_SECS: u64 = 2;

pub static EXCHANGES_CONFIGS: LazyLock<HashMap<&'static str, ExchangeConfig>> =
    LazyLock::new(|| {
        dotenv().ok();
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

            configs.insert("Bybit", config);
        }

        configs
    });

pub static EXCHANGES_CONTEXTS: LazyLock<HashMap<&'static str, ExchangeContext>> =
    LazyLock::new(|| {
        let mut exchanges_contexts = HashMap::new();

        {
            let btcusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2020, 3, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                0.000009,
                100.0,
                1190.0,
                0.001,
                None,
                SYMBOLS_MAP.get("BTCUSDT").unwrap().name,
                0.1,
            );

            let ethusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2021, 3, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                0.000031,
                50.0,
                7240.0,
                0.01,
                None,
                SYMBOLS_MAP.get("ETHUSDT").unwrap().name,
                0.01,
            );

            let solusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2021, 10, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                0.000048,
                50.0,
                196310.0,
                0.1,
                None,
                SYMBOLS_MAP.get("SOLUSDT").unwrap().name,
                0.001,
            );

            let arbusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                -0.0288,
                50.0,
                79770.0,
                0.1,
                None,
                SYMBOLS_MAP.get("ARBUSDT").unwrap().name,
                0.01,
            );

            let linkusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2020, 10, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                0.000048,
                50.0,
                196310.0,
                0.1,
                None,
                SYMBOLS_MAP.get("LINKUSDT").unwrap().name,
                0.001,
            );

            let mut contracts = HashMap::new();
            contracts.insert(btcusdt_contract.symbol, btcusdt_contract);
            contracts.insert(ethusdt_contract.symbol, ethusdt_contract);
            contracts.insert(solusdt_contract.symbol, solusdt_contract);
            contracts.insert(arbusdt_contract.symbol, arbusdt_contract);
            contracts.insert(linkusdt_contract.symbol, linkusdt_contract);

            let context = ExchangeContext {
                taker_fee: 0.0055,
                maker_fee: 0.002,
                contracts,
            };

            exchanges_contexts.insert("Bybit", context);
        }

        exchanges_contexts
    });
