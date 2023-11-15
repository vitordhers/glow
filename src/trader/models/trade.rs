use crate::trader::{
    enums::{
        modifiers::price_level::{PriceLevel, TrailingStopLoss},
        order_stage::OrderStage,
        order_status::OrderStatus,
        order_type::OrderType,
        side::Side,
        time_in_force::TimeInForce,
        trade_status::TradeStatus,
    },
    errors::{CustomError, Error},
    functions::closest_multiple_below,
    traits::exchange::Exchange,
};
use chrono::Utc;
use tokio::sync::watch::Ref;

use super::{execution::Execution, order::Order};

#[derive(Debug, Clone, Default)]
pub struct Trade {
    /// defined as `{traded_symbol}_{timestamp}
    pub id: String,
    pub open_order: Order,
    pub close_order: Option<Order>,
}

impl Trade {
    pub fn new(open_order: Order, close_order: Option<Order>) -> Self {
        let mut iter = open_order.id.splitn(3, '_');

        let traded_symbol = iter.next().unwrap_or("");
        let open_order_timestamp = iter.next().unwrap_or("");
        let id = format!("{}_{}", traded_symbol, open_order_timestamp).to_string();
        if id.len() == 1 {
            panic!(
                "trade id incorrectly generated. Open order {:?}",
                open_order
            );
        }
        Trade {
            id,
            open_order,
            close_order,
        }
    }

    fn get_leverage_factor(&self) -> f64 {
        self.open_order.leverage_factor
    }

    pub fn get_current_order(&self) -> Order {
        if self.close_order.is_some() {
            self.close_order.clone().unwrap()
        } else {
            self.open_order.clone()
        }
    }

    pub fn get_current_position(&self) -> i32 {
        self.get_current_order().side.into()
    }

    pub fn status(&self) -> TradeStatus {
        if let Some(close_order) = &self.close_order {
            match close_order.status {
                OrderStatus::Closed
                | OrderStatus::StoppedBR
                | OrderStatus::StoppedSL
                | OrderStatus::StoppedTP
                | OrderStatus::StoppedTSL => TradeStatus::Closed,
                OrderStatus::PartiallyClosed => TradeStatus::PartiallyClosed,
                OrderStatus::StandBy => TradeStatus::CloseOrderStandBy,
                OrderStatus::Cancelled | OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                    panic!(
                        "This shouldn't be happening, close order has status {:?}",
                        close_order.status
                    );
                }
            }
        } else {
            match self.open_order.status {
                OrderStatus::Filled => TradeStatus::PendingCloseOrder,
                OrderStatus::PartiallyFilled => TradeStatus::PartiallyOpen,
                OrderStatus::StandBy => TradeStatus::New,
                OrderStatus::Cancelled => TradeStatus::Cancelled,
                OrderStatus::PartiallyClosed
                | OrderStatus::Closed
                | OrderStatus::StoppedBR
                | OrderStatus::StoppedSL
                | OrderStatus::StoppedTP
                | OrderStatus::StoppedTSL => {
                    panic!(
                        "This shouldn't be happening, open order has PartiallyClosed status {:?}",
                        self
                    );
                }
            }
        }
    }

    pub fn calculate_initial_margin(&self) -> f64 {
        let open_order_executed_value = self.open_order.get_executed_order_value();
        let leverage_factor = self.get_leverage_factor();
        if leverage_factor != 0.0 {
            open_order_executed_value / leverage_factor
        } else {
            0.0
        }
    }

    pub fn new_close_order(&self, order_type: OrderType, est_price: f64) -> Result<Order, Error> {
        let id = format!("{}_{}", self.id, OrderStage::Close.to_string());
        let time_in_force;
        time_in_force = if order_type == OrderType::Market {
            TimeInForce::IOC
        } else {
            TimeInForce::GTC
        };
        let timestamp = Utc::now().timestamp() * 1000;

        let executions = vec![];

        let avg_price = if order_type == OrderType::Limit {
            Some(est_price)
        } else {
            None
        };

        let close_side = self.open_order.side.get_opposite_side()?;

        let order = Order::new(
            format!("pending_order_uuid_{}", timestamp),
            id,
            self.open_order.symbol.clone(),
            OrderStatus::StandBy,
            order_type,
            close_side,
            time_in_force,
            self.open_order.units,
            self.open_order.leverage_factor,
            None,
            None,
            avg_price,
            executions,
            0.0,
            0.0,
            true,
            false,
            timestamp,
            timestamp,
        );

        Ok(order)
    }

    pub fn calculate_current_pnl_and_returns(
        &self,
        end_timestamp: i64,
        current_price: f64,
    ) -> (f64, f64) {
        let realized_pnl =
            self.get_interval_profit_and_loss(self.open_order.created_at, end_timestamp);
        let (unrealized_pnl, _) = self.calculate_unrealized_pnl_and_returns(current_price);

        let initial_margin = self.calculate_initial_margin();

        let returns = if initial_margin != 0.0 {
            (realized_pnl + unrealized_pnl) / initial_margin
        } else {
            0.0
        };

        ((realized_pnl + unrealized_pnl), returns)
    }

    pub fn calculate_pnl_and_returns(&self) -> (f64, f64) {
        let closed_order = &self
            .close_order
            .clone()
            .expect("calculate_pnl_and_returns close order unwrap");

        let end_timestamp = closed_order.updated_at;
        let realized_pnl =
            self.get_interval_profit_and_loss(self.open_order.created_at, end_timestamp);

        let initial_margin = self.calculate_initial_margin();

        let returns = if initial_margin != 0.0 {
            realized_pnl / initial_margin
        } else {
            0.0
        };

        (realized_pnl, returns)
    }

    pub fn calculate_unrealized_pnl_and_returns(&self, current_price: f64) -> (f64, f64) {
        // short: Unrealized P&L = (Average Entry Price - Current Mark Price) × Position Size, ROI = [(Entry Price − Mark Price) × Position Size/ Initial Margin] × 100%
        // long: Unrealized P&L = (Current Mark Price - Average Entry Price) × Position Size, ROI = [(Mark Price − Entry Price) × Position Size/ Initial Margin] × 100%
        let avg_entry_price = self.open_order.get_executed_avg_price();
        let executed_qty = self.open_order.get_executed_quantity();

        let closed_qty = if let Some(close_order) = &self.close_order {
            close_order.get_closed_quanitity()
        } else {
            0.0
        };

        let current_position_size = executed_qty - closed_qty;
        let bankruptcy_price = self.open_order.get_bankruptcy_price().unwrap_or_default();
        let provisional_close_fee =
            current_position_size * bankruptcy_price * self.open_order.taker_fee_rate;

        // TODO: CHECK THIS =>  here, we don't subtract fees since their effects result in having less units
        let unrealized_pnl = if self.open_order.side == Side::Sell {
            (avg_entry_price - current_price) * current_position_size - provisional_close_fee
        } else if self.open_order.side == Side::Buy {
            (current_price - avg_entry_price) * current_position_size - provisional_close_fee
        } else {
            panic!("calculate_unrealized_profit -> open order position is different from -1 or 1");
        };

        let initial_margin = self.calculate_initial_margin();

        let unrealized_returns = if initial_margin != 0.0 {
            unrealized_pnl / initial_margin
        } else {
            0.0
        };
        (unrealized_pnl, unrealized_returns)
    }

    fn get_interval_close_executions(
        &self,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Vec<Execution> {
        if let Some(close_order) = &self.close_order {
            close_order.get_executions_between_interval(start_timestamp, end_timestamp)
        } else {
            Vec::new()
        }
    }

    fn get_interval_executions(&self, start_timestamp: i64, end_timestamp: i64) -> Vec<Execution> {
        let mut executions = self
            .open_order
            .get_executions_between_interval(start_timestamp, end_timestamp);
        let close_executions = self.get_interval_close_executions(start_timestamp, end_timestamp);
        executions.extend(close_executions);

        executions
    }

    pub fn get_interval_units(&self, end_timestamp: i64) -> f64 {
        let open_units = self
            .open_order
            .get_executions_between_interval(self.open_order.created_at, end_timestamp)
            .iter()
            .fold(0.0, |acc, execution| acc + execution.qty);
        let closed_units = self
            .get_interval_close_executions(self.open_order.created_at, end_timestamp)
            .iter()
            .fold(0.0, |acc, execution| acc + execution.closed_qty);

        open_units - closed_units
    }

    pub fn get_interval_profit_and_loss(&self, start_timestamp: i64, end_timestamp: i64) -> f64 {
        let avg_entry_price = self.open_order.get_executed_avg_price();
        let entry_side = self.open_order.side;
        let close_interval_executions =
            self.get_interval_close_executions(start_timestamp, end_timestamp);
        let result = close_interval_executions
            .iter()
            .fold(0.0, |acc, execution| {
                if entry_side == Side::Sell {
                    acc + ((avg_entry_price - execution.price) * execution.closed_qty)
                } else if entry_side == Side::Buy {
                    acc + ((execution.price - avg_entry_price) * execution.closed_qty)
                } else {
                    panic!(
                        "get_interval_profit_and_loss -> entry_position is different from -1 or 1"
                    );
                }
            });
        let total_fees = self.get_executed_fees_between_interval(start_timestamp, end_timestamp);
        result - total_fees
    }

    pub fn get_executed_fees(&self) -> f64 {
        let mut total_executed_fees = 0.0;
        if self.close_order.is_some() {
            let close_order = self.close_order.clone().unwrap();
            let close_fees = close_order.get_executed_order_fee();
            total_executed_fees += close_fees;
        }

        let open_fees = self.open_order.get_executed_order_fee();
        total_executed_fees += open_fees;
        total_executed_fees
    }

    pub fn get_executed_fees_between_interval(
        &self,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> f64 {
        let executions = self.get_interval_executions(start_timestamp, end_timestamp);
        executions
            .iter()
            .fold(0.0, |acc, execution| acc + execution.fee)
    }

    pub fn updated_at(&self) -> i64 {
        let mut updated_at = self.open_order.updated_at;
        if let Some(close_order) = &self.close_order {
            if close_order.updated_at > updated_at {
                updated_at = close_order.updated_at;
            }
        }
        updated_at
    }

    pub fn get_active_order_uuid(&self) -> Option<String> {
        let status = self.status();
        match status {
            TradeStatus::Cancelled
            | TradeStatus::New
            | TradeStatus::PartiallyOpen
            | TradeStatus::PendingCloseOrder => Some(self.open_order.uuid.clone()),
            TradeStatus::CloseOrderStandBy => None,
            TradeStatus::PartiallyClosed | TradeStatus::Closed => {
                let close_order = self.close_order.clone().unwrap();
                Some(close_order.uuid)
            }
        }
    }

    pub fn get_active_order_id(&self) -> Option<String> {
        let status = self.status();
        match status {
            TradeStatus::Cancelled
            | TradeStatus::New
            | TradeStatus::PartiallyOpen
            | TradeStatus::PendingCloseOrder => Some(self.open_order.id.clone()),
            TradeStatus::CloseOrderStandBy => None,
            TradeStatus::PartiallyClosed | TradeStatus::Closed => {
                let close_order = self.close_order.clone().unwrap();
                Some(close_order.id)
            }
        }
    }

    pub fn update_trade(&self, order: Order) -> Result<Trade, Error> {
        let mut updated_trade = self.clone();

        if order.is_close {
            match &self.close_order {
                Some(close_order) => {
                    // otherwise, check if it's related to close order
                    // by comparing their ids
                    if close_order.id == order.id {
                        // if it's not cancelation, update close order
                        let updated_order = close_order.update(
                            order.created_at,
                            order.updated_at,
                            order.status,
                            order.units,
                            order.avg_price,
                            order.stop_loss_price,
                            order.take_profit_price,
                            order.executions,
                        );
                        updated_trade.close_order = Some(updated_order);
                        Ok(updated_trade)
                    } else {
                        // this shouldn't be happening, log and return current order
                        let error = format!(
                            r#"update_trade -> Latest order not related to current trade close order, that exists already
                            Updated Order {:?},
                            Trade {:?}"#,
                            &order, &self
                        );
                        let error = CustomError { message: error };
                        return Err(Error::CustomError(error));
                    }
                }
                None => {
                    let mut updated_order = order.clone();
                    updated_order = updated_order.push_executions_if_new(order.executions);
                    updated_trade.close_order = Some(updated_order);
                    Ok(updated_trade)
                }
            }
        } else {
            // check if it's related to open order
            if self.open_order.id == order.id {
                // order is not cancel, so we should update order and trade
                let updated_order = self.open_order.update(
                    order.created_at,
                    order.updated_at,
                    order.status,
                    order.units,
                    order.avg_price,
                    order.stop_loss_price,
                    order.take_profit_price,
                    order.executions,
                );

                updated_trade.open_order = updated_order;
                Ok(updated_trade)
            } else {
                // this shouldn't be happening, log and return current order
                let error = format!(
                    r#"get_order_update_handle -> Latest order not related to current trade close order,
                    Updated Order {:?},
                    Trade {:?}"#,
                    &order, &self
                );
                let error = CustomError { message: error };
                return Err(Error::CustomError(error));
            }
        }
    }

    pub fn update_executions(&self, executions: Vec<Execution>) -> Result<Option<Trade>, Error> {
        let mut updated_trade = self.clone();
        let mut was_updated = false;

        if self.close_order.is_some() {
            let mut close_order = self.close_order.clone().unwrap();
            let close_order_uuid = close_order.uuid.clone();
            if close_order_uuid != "".to_string() {
                let close_order_executions = executions
                    .clone()
                    .into_iter()
                    .filter(|exec| exec.order_uuid == close_order_uuid)
                    .collect::<Vec<_>>();
                if close_order_executions.len() > 0 {
                    was_updated = true;
                    close_order = close_order.push_executions_if_new(close_order_executions);
                    updated_trade = updated_trade.update_trade(close_order)?;
                }
            } else {
                println!("update_executions -> close_order has empty uuid");
            }
        }

        let mut open_order = updated_trade.open_order.clone();
        let open_order_uuid = updated_trade.open_order.uuid.clone();

        if open_order_uuid != "".to_string() {
            let open_order_executions = executions
                .clone()
                .into_iter()
                .filter(|exec| exec.order_uuid == open_order_uuid)
                .collect::<Vec<_>>();

            if open_order_executions.len() > 0 {
                was_updated = true;
                open_order = open_order.push_executions_if_new(open_order_executions);
                updated_trade = updated_trade.update_trade(open_order)?;
            }
        }

        if was_updated {
            Ok(Some(updated_trade))
        } else {
            println!(
                r#"update_executions ->
             trade {:?}
             received 
             {:?} executions
             but wasn't updated!"#,
                updated_trade, &executions
            );
            Ok(None)
        }
    }

    pub fn check_price_level_modifiers(
        &self,
        exchange_ref: &Ref<'_, Box<dyn Exchange + Send + Sync>>,
        current_timestamp: i64,
        close_price: f64,
        stop_loss: Option<&PriceLevel>,
        take_profit: Option<&PriceLevel>,
        trailing_stop_loss: Option<&PriceLevel>,
        current_peak_returns: f64,
    ) -> Result<Option<Trade>, Error> {
        if self.open_order.leverage_factor > 1.0 {
            let bankruptcy_price = self
                .open_order
                .get_bankruptcy_price()
                .expect("check_price_level_modifiers -> get_bankruptcy_price unwrap");

            if self.open_order.side == Side::Sell && bankruptcy_price <= close_price
                || self.open_order.side == Side::Buy && bankruptcy_price >= close_price
            {
                let close_order = exchange_ref.create_benchmark_close_order(
                    current_timestamp,
                    &self.id,
                    close_price,
                    self.open_order.clone(),
                    OrderStatus::StoppedBR,
                )?;

                let closed_trade = self.update_trade(close_order)?;
                return Ok(Some(closed_trade));
            }
        }
        if stop_loss.is_some() || take_profit.is_some() || trailing_stop_loss.is_some() {
            let (_, returns) = self.calculate_unrealized_pnl_and_returns(close_price);
            if let Some(stop_loss) = stop_loss {
                let stop_loss_percentage = stop_loss.get_percentage();
                if returns < 0.0 && stop_loss_percentage <= returns.abs() {
                    println!(
                        "Stop loss took effect: returns = {}, stop loss percentage = {}",
                        returns, stop_loss_percentage
                    );
                    let close_order = exchange_ref.create_benchmark_close_order(
                        current_timestamp,
                        &self.id,
                        close_price,
                        self.open_order.clone(),
                        OrderStatus::StoppedSL,
                    )?;

                    let closed_trade = self.update_trade(close_order)?;
                    return Ok(Some(closed_trade));
                }
            }

            if let Some(take_profit) = take_profit {
                let take_profit_percentage = take_profit.get_percentage();
                if returns > 0.0 && take_profit_percentage <= returns.abs() {
                    println!(
                        "Take profit took effect: returns = {}, take profit percentage = {}",
                        returns, take_profit_percentage
                    );
                    let close_order = exchange_ref.create_benchmark_close_order(
                        current_timestamp,
                        &self.id,
                        close_price,
                        self.open_order.clone(),
                        OrderStatus::StoppedTP,
                    )?;

                    let closed_trade = self.update_trade(close_order)?;
                    return Ok(Some(closed_trade));
                }
            }

            if let Some(trailing_stop_loss) = trailing_stop_loss {
                match trailing_stop_loss {
                    PriceLevel::TrailingStopLoss(tsl) => match tsl {
                        TrailingStopLoss::Percent(percentage, start_percentage) => {
                            if start_percentage < &current_peak_returns {
                                let acceptable_returns = percentage * current_peak_returns;
                                let acceptable_returns = acceptable_returns.max(*start_percentage);

                                if returns <= acceptable_returns {
                                    let close_order = exchange_ref.create_benchmark_close_order(
                                        current_timestamp,
                                        &self.id,
                                        close_price,
                                        self.open_order.clone(),
                                        OrderStatus::StoppedTSL,
                                    )?;

                                    let closed_trade = self.update_trade(close_order)?;
                                    return Ok(Some(closed_trade));
                                }
                            }
                        }
                        TrailingStopLoss::Stepped(percentage, start_percentage) => {
                            if start_percentage < &current_peak_returns {
                                let acceptable_returns =
                                    closest_multiple_below(*percentage, current_peak_returns);
                                let acceptable_returns = acceptable_returns.max(*start_percentage);
                                // println!(
                                //     "@@@ start_percentage {} current returns {}, acceptable_returns {}",
                                //     start_percentage, returns, acceptable_returns
                                // );
                                if returns <= acceptable_returns {
                                    let close_order = exchange_ref.create_benchmark_close_order(
                                        current_timestamp,
                                        &self.id,
                                        close_price,
                                        self.open_order.clone(),
                                        OrderStatus::StoppedTP,
                                    )?;

                                    let closed_trade = self.update_trade(close_order)?;
                                    return Ok(Some(closed_trade));
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
        }

        Ok(None)
    }
}

impl From<Order> for Trade {
    fn from(order: Order) -> Self {
        Trade::new(order.clone(), None)
    }
}
