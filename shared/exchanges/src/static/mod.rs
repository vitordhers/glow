use crate::{enums::TraderExchangeId, structs::ExchangeContext};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use common::{r#static::SYMBOLS_MAP, structs::Contract};
use std::{collections::HashMap, sync::LazyLock};

pub static TRADER_EXCHANGES_CONTEXT_MAP: LazyLock<HashMap<TraderExchangeId, ExchangeContext>> =
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
                (119.0, 1190.0),
                0.001,
                None,
                SYMBOLS_MAP.get("BTCUSDT").unwrap(),
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
                (724.0, 7240.0),
                0.01,
                None,
                SYMBOLS_MAP.get("ETHUSDT").unwrap(),
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
                (11740.0, 79770.0),
                0.1,
                None,
                SYMBOLS_MAP.get("SOLUSDT").unwrap(),
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
                (283360.0, 1799900.0),
                0.1,
                None,
                SYMBOLS_MAP.get("ARBUSDT").unwrap(),
                0.0001,
            );

            let linkusdt_contract = Contract::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2020, 10, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Duration::hours(8),
                0.000048,
                50.0,
                (31450.0, 196310.0),
                0.1,
                None,
                SYMBOLS_MAP.get("LINKUSDT").unwrap(),
                0.001,
            );

            let mut contracts = HashMap::new();
            contracts.insert(btcusdt_contract.symbol.id, btcusdt_contract);
            contracts.insert(ethusdt_contract.symbol.id, ethusdt_contract);
            contracts.insert(solusdt_contract.symbol.id, solusdt_contract);
            contracts.insert(arbusdt_contract.symbol.id, arbusdt_contract);
            contracts.insert(linkusdt_contract.symbol.id, linkusdt_contract);

            let context = ExchangeContext {
                taker_fee: 0.0055,
                maker_fee: 0.002,
                contracts,
            };

            exchanges_contexts.insert(TraderExchangeId::Bybit, context);
        }

        exchanges_contexts
    });
