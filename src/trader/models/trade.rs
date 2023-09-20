use chrono::Utc;

use crate::trader::{
    enums::{
        order_stage::OrderStage, order_status::OrderStatus, order_type::OrderType,
        time_in_force::TimeInForce, trade_status::TradeStatus,
    },
    errors::{CustomError, Error},
};

use super::{execution::Execution, order::Order};

#[derive(Debug, Clone)]
pub struct Trade {
    // defined as `{traded_symbol}_{timestamp}
    pub id: String,
    pub symbol: String,
    pub leverage: f64,
    pub open_order: Order,
    pub close_order: Option<Order>,
}

impl Trade {
    pub fn new(
        symbol: &String,
        leverage: f64,
        open_order: Order,
        close_order: Option<Order>,
    ) -> Self {
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
            symbol: symbol.to_string(),
            leverage,
            open_order,
            close_order,
        }
    }

    pub fn status(&self) -> TradeStatus {
        if let Some(close_order) = &self.close_order {
            match close_order.status {
                OrderStatus::Closed => TradeStatus::Closed,
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
                OrderStatus::PartiallyClosed | OrderStatus::Closed => {
                    panic!(
                        "This shouldn't be happening, open order has PartiallyClosed status {:?}",
                        self
                    );
                }
            }
        }
    }

    pub fn new_close_order(&self, order_type: OrderType, est_price: f64) -> Order {
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

        Order {
            uuid: "".to_string(),
            id,
            symbol: self.open_order.symbol.clone(),
            is_close: true,
            is_stop: false,
            status: OrderStatus::StandBy,
            order_type,
            units: self.open_order.units,
            avg_price,
            position: self.open_order.position * -1,
            executions,
            stop_loss_price: None,
            take_profit_price: None,
            time_in_force,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    pub fn calculate_current_pnl_and_returns(
        &self,
        end_timestamp: i64,
        current_price: f64,
    ) -> (f64, f64) {
        let realized_pnl =
            self.get_interval_profit_and_loss(self.open_order.created_at, end_timestamp);
        let (unrealized_pnl, _) = self.calculate_unrealized_pnl_and_returns(current_price);

        let open_order_executed_value = self.open_order.get_executed_order_value();
        let executed_initial_margin = open_order_executed_value / self.leverage;

        // println!("@@@ realized_pnl = {}, unrealized_pnl = {}, open_order_executed_value = {}, executed_initial_margin = {}, current_price = {}",realized_pnl, unrealized_pnl, open_order_executed_value,  executed_initial_margin, current_price);
        let returns = if executed_initial_margin != 0.0 {
            (realized_pnl + unrealized_pnl) / executed_initial_margin
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

        let open_order_executed_value = self.open_order.get_executed_order_value();
        let executed_initial_margin = open_order_executed_value / self.leverage;

        let returns = if executed_initial_margin != 0.0 {
            realized_pnl / executed_initial_margin
        } else {
            0.0
        };

        (realized_pnl, returns)
    }

    pub fn calculate_unrealized_pnl_and_returns(&self, current_price: f64) -> (f64, f64) {
        // short: Unrealized P&L = (Average Entry Price - Current Mark Price) × Position Size, ROI = [(Entry Price − Mark Price) × Position Size/ Initial Margin] × 100%
        // long: Unrealized P&L = (Current Mark Price - Average Entry Price) × Position Size, ROI = [(Mark Price − Entry Price) × Position Size/ Initial Margin] × 100%
        let open_order_executed_value = self.open_order.get_executed_order_value();
        let avg_entry_price = self.open_order.get_executed_avg_price();
        let executed_qty = self.open_order.get_executed_quantity();

        let closed_qty = if let Some(close_order) = &self.close_order {
            close_order.get_closed_quanitity()
        } else {
            0.0
        };

        let current_position_size = executed_qty - closed_qty;

        // here, we don't subtract fees since their effects result in having less units
        let pnl = if self.open_order.position == -1 {
            (avg_entry_price - current_price) * current_position_size
        } else if self.open_order.position == 1 {
            (current_price - avg_entry_price) * current_position_size
        } else {
            panic!("calculate_unrealized_profit -> open order position is different from -1 or 1");
        };

        let executed_initial_margin = if self.leverage != 0.0 {
            open_order_executed_value / self.leverage
        } else {
            println!("current leverage is zero");
            open_order_executed_value
        };

        let returns = if executed_initial_margin != 0.0 {
            pnl / executed_initial_margin
        } else {
            0.0
        };
        (pnl, returns)
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
        let entry_position = self.open_order.position;
        let close_interval_executions =
            self.get_interval_close_executions(start_timestamp, end_timestamp);
        let result = close_interval_executions
            .iter()
            .fold(0.0, |acc, execution| {
                if entry_position == -1 {
                    acc + ((avg_entry_price - execution.price) * execution.closed_qty)
                } else if entry_position == 1 {
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
}
