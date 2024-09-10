use chrono::Duration;
use cli::{change_benchmark_datetimes, change_symbols_pair, select_from_list};
use common::{
    enums::{balance::Balance, order_action::OrderAction},
    functions::current_datetime,
    structs::{BehaviorSubject, Execution, SymbolsPair, Trade, TradingSettings},
};
use core::data_feed::DataFeed;
use dialoguer::console::Term;
use dotenv::dotenv;
use exchanges::enums::{
    DataProviderExchangeId, DataProviderExchangeWrapper, TraderExchangeId, TraderExchangeWrapper,
};

use std::env;
use std::sync::{Arc, Mutex};
use strategy::{Strategy, StrategyId};
use tokio::{join, spawn, task::JoinHandle};

#[tokio::main]
async fn main() {
    // todo!()
        dotenv().ok();
        let max_rows = "40".to_string();
        env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

        // piping schema: https://app.clickup.com/9013233975/v/wb/8ckp29q-433
        let default_symbols_pair = SymbolsPair::default();
        let selected_symbols_pair = BehaviorSubject::new(default_symbols_pair);

        let default_trading_settings = TradingSettings::load_or_default();
        let current_trading_settings = BehaviorSubject::new(default_trading_settings.clone());

        handle_symbols_pair_selection(&selected_symbols_pair, &current_trading_settings);

        let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
        let trader_last_ws_error_ts: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(None));

        let update_balance_listener: BehaviorSubject<Option<Balance>> = BehaviorSubject::new(None);
        let update_executions_listener: BehaviorSubject<Vec<Execution>> = BehaviorSubject::new(vec![]);
        let update_order_listener: BehaviorSubject<Option<OrderAction>> = BehaviorSubject::new(None);

        let default_trader_exchange_id = TraderExchangeId::default();
        let selected_trader_exchange_id = BehaviorSubject::new(default_trader_exchange_id);
        let default_trader_exchange = TraderExchangeWrapper::new(
            default_trader_exchange_id,
            &current_trade_listener,
            &trader_last_ws_error_ts,
            &default_trading_settings,
            &update_balance_listener,
            &update_executions_listener,
            &update_order_listener,
        );

        let current_trader_exchange = BehaviorSubject::new(default_trader_exchange);

        handle_trader_exchange_selection(
            &current_trading_settings,
            &current_trader_exchange,
            &selected_trader_exchange_id,
            &current_trade_listener,
            &trader_last_ws_error_ts,
            &update_balance_listener,
            &update_executions_listener,
            &update_order_listener,
        );

        let default_strategy_id = StrategyId::default();
        let selected_strategy_id = BehaviorSubject::new(default_strategy_id);
        let default_strategy = Strategy::default();
        let current_strategy = BehaviorSubject::new(default_strategy.clone());

        handle_strategy_selection(
            &current_strategy,
            &current_trading_settings,
            &selected_strategy_id,
        );

        let data_provider_last_ws_error_ts: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(None));
        let trading_data_update_listener = BehaviorSubject::new(TradingDataUpdate::default());
        let default_data_provider_exchange_id = DataProviderExchangeId::default();
        let selected_data_provider_exchange_id =
            BehaviorSubject::new(default_data_provider_exchange_id);
        let default_data_provider_exchange = DataProviderExchangeWrapper::new(
            default_data_provider_exchange_id,
            default_trading_settings.granularity.get_chrono_duration(),
            &data_provider_last_ws_error_ts,
            default_strategy.get_minimum_klines_for_calculation(),
            default_symbols_pair,
            &trading_data_update_listener,
        );

        let current_data_provider_exchange = BehaviorSubject::new(default_data_provider_exchange);

        handle_data_provider_selection(
            &current_trading_settings,
            &current_strategy,
            &trading_data_update_listener,
            &selected_data_provider_exchange_id,
            &data_provider_last_ws_error_ts,
            &current_data_provider_exchange,
        );

        // let mut current_strategy = get_default_strategy();
        let mut end_datetime = current_datetime();
        let mut start_datetime = end_datetime - Duration::days(1);

        let term = Term::stdout();
        loop {
            term.clear_screen().unwrap();

            term.write_line("Glow Backtesting Suite - v0.02.").unwrap();
            term.write_line(
                r#"Gloria Patri, et Filio, et Spiritui Sancto.
            Sicut erat in principio, et nunc et semper, et in saecula saeculorum.
            Amen 🙏"#,
            )
            .unwrap();

            term.write_line(&format!(
                "Current Start Time: {:?} \nCurrent End Time: {:?}\nCurrent Settings {:?}",
                start_datetime,
                end_datetime,
                current_trading_settings.value()
            ))
            .unwrap();

            let options = vec![
                "📆 Select Benchmark Datetimes",
                "🪙  Change Trading Coins",
                "📥 Select Data Provider Exchange",
                "📤 Select Trader Exchange",
                "🧐 Change Strategy",
                "🎛  Change Trading Settings",
                "▶️ Run Benchmark",
            ];

            let default_index = options.len() + 1;
            let selection = select_from_list("Select an option", &options, Some(default_index));
            match selection {
                0 => {
                    let current_strategy = current_strategy.value();
                    let current_trade_exchange = current_trader_exchange.value();
                    let result = change_benchmark_datetimes(
                        start_datetime,
                        end_datetime,
                        &current_trade_exchange,
                        current_strategy.get_minimum_klines_for_calculation(),
                    );
                    if result.is_none() {
                        continue;
                    }
                    let (updated_start_datetime, updated_end_datetime) = result.unwrap();
                    start_datetime = updated_start_datetime;
                    end_datetime = updated_end_datetime;
                }
                1 => {
                    let current_symbols_pair = selected_symbols_pair.value();
                    let updated_symbols_pair = change_symbols_pair(current_symbols_pair);
                    if updated_symbols_pair.is_none() {
                        continue;
                    }
                    let updated_symbols_pair = updated_symbols_pair.unwrap();
                    selected_symbols_pair.next(updated_symbols_pair);
                }
                2 => {
                    // CHANGE PROVIDER EXCHANGE
                }
                3 => {
                    // CHANGE TRADER EXCHANGE
                }
                4 => {
                    // CHANGE STRATEGY
                }
                5 => {
                    // CHANGE TRADING SETTINGS
                }
                6 => {
                    // RUN BENCHMARK
                    let benchmark_datetimes = (Some(start_datetime), Some(end_datetime));
                    let data_provider_exchange = current_data_provider_exchange.value();
                    let strategy = current_strategy.value();

                    let data_feed = DataFeed::new(
                        benchmark_datetimes,
                        &current_trade_listener,
                        &data_provider_last_ws_error_ts,
                        data_provider_exchange,
                        true,
                        &strategy,
                        &trading_data_update_listener,
                        &current_trader_exchange,
                        &update_balance_listener,
                        &update_executions_listener,
                        &update_order_listener,
                    );
                }
                _ => {
                    println!("Invalid option");
                    continue;
                }
            }
        }
    }

    fn handle_symbols_pair_selection(
        selected_symbols_pair: &BehaviorSubject<SymbolsPair>,
        current_trading_settings: &BehaviorSubject<TradingSettings>,
    ) -> JoinHandle<()> {
        let selected_symbols_pair = selected_symbols_pair.clone();
        let current_trading_settings = current_trading_settings.clone();
        spawn(async move {
            let mut selection_subscription = selected_symbols_pair.subscribe();
            while let Some(updated_symbols_pair) = selection_subscription.next().await {
                let updated_trading_settings = current_trading_settings
                    .value()
                    .patch_symbols_pair(updated_symbols_pair);
                current_trading_settings.next(updated_trading_settings);
            }
        })
    }

    fn handle_trader_exchange_selection(
        current_trading_settings: &BehaviorSubject<TradingSettings>,
        current_trader_exchange: &BehaviorSubject<TraderExchangeWrapper>,
        selected_trader_exchange_id: &BehaviorSubject<TraderExchangeId>,
        current_trade_listener: &BehaviorSubject<Option<Trade>>,
        trader_last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    ) -> JoinHandle<()> {
        let current_trading_settings = current_trading_settings.clone();
        let current_trader_exchange = current_trader_exchange.clone();
        let selected_trader_exchange_id = selected_trader_exchange_id.clone();
        let current_trade_listener = current_trade_listener.clone();
        let trader_last_ws_error_ts = trader_last_ws_error_ts.clone();
        let update_balance_listener = update_balance_listener.clone();
        let update_executions_listener = update_executions_listener.clone();
        let update_order_listener = update_order_listener.clone();

        spawn(async move {
            let selection_subscription = &mut selected_trader_exchange_id.subscribe();
            let mut settings_subscription = current_trading_settings.subscribe();

            while let (Some(selected_exchange_id), Some(trading_settings)) =
                join!(selection_subscription.next(), settings_subscription.next())
            {
                let updated_trader_exchange = TraderExchangeWrapper::new(
                    selected_exchange_id,
                    &current_trade_listener,
                    &trader_last_ws_error_ts,
                    &trading_settings,
                    &update_balance_listener,
                    &update_executions_listener,
                    &update_order_listener,
                );
                current_trader_exchange.next(updated_trader_exchange);
            }
        })
    }

    fn handle_strategy_selection(
        current_strategy: &BehaviorSubject<Strategy>,
        current_trading_settings: &BehaviorSubject<TradingSettings>,
        selected_strategy_id: &BehaviorSubject<StrategyId>,
    ) -> JoinHandle<()> {
        let current_strategy = current_strategy.clone();
        let current_trading_settings = current_trading_settings.clone();
        let selected_strategy_id = selected_strategy_id.clone();

        spawn(async move {
            let selection_subscription = &mut selected_strategy_id.subscribe();
            let mut trading_settings_subscription = current_trading_settings.subscribe();

            while let (Some(selected_strategy_id), Some(trading_settings)) = join!(
                selection_subscription.next(),
                trading_settings_subscription.next()
            ) {
                let current_symbol_pair = trading_settings.symbols_pair;
                let updated_strategy = Strategy::new(selected_strategy_id, current_symbol_pair);
                current_strategy.next(updated_strategy);
            }
        })
    }

    fn handle_data_provider_selection(
        current_trading_settings: &BehaviorSubject<TradingSettings>,
        current_strategy: &BehaviorSubject<Strategy>,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,
        selected_data_provider_exchange_id: &BehaviorSubject<DataProviderExchangeId>,
        data_provider_last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        current_data_provider_exchange: &BehaviorSubject<DataProviderExchangeWrapper>,
    ) -> JoinHandle<()> {
        let current_trading_settings = current_trading_settings.clone();
        let current_strategy = current_strategy.clone();
        let trading_data_update_listener = trading_data_update_listener.clone();
        let selected_data_provider_exchange_id = selected_data_provider_exchange_id.clone();
        let data_provider_last_ws_error_ts = data_provider_last_ws_error_ts.clone();
        let current_data_provider_exchange = current_data_provider_exchange.clone();

        spawn(async move {
            let mut selection_subscription = selected_data_provider_exchange_id.subscribe();
            let mut settings_subscription = current_trading_settings.subscribe();
            let mut strategy_subscription = current_strategy.subscribe();

            while let (
                Some(selected_exchange),
                Some(TradingSettings {
                    granularity,
                    symbols_pair,
                    ..
                }),
                Some(strategy),
            ) = join!(
                selection_subscription.next(),
                settings_subscription.next(),
                strategy_subscription.next()
            ) {
                let updated_data_provider_exchange = DataProviderExchangeWrapper::new(
                    selected_exchange,
                    granularity.get_chrono_duration(),
                    &data_provider_last_ws_error_ts,
                    strategy.get_minimum_klines_for_calculation(),
                    symbols_pair,
                    &trading_data_update_listener,
                );
                current_data_provider_exchange.next(updated_data_provider_exchange);
            }
        })
}
