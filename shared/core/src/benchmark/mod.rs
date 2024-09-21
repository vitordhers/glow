use common::enums::side::Side;

pub struct BenchmarkOrder {
    pub side: Side,
    pub prices: (f32, Option<f32>, Option<f32>, Option<f32>),
    pub units: f32,
    pub leverage_factor: f32,
    pub initial_margin: f32,
    pub fees: (f32, f32),
}

pub struct PriceLock(f32);
pub enum LockType {
    StopLoss,
    TakeProfit,
}

impl LockType {
    fn get_price_mod(&self, side: Side, pct: f32) -> f32 {
        match (self, side) {
            (Self::StopLoss, Side::Buy) | (Self::TakeProfit, Side::Sell) => -1.0 * pct,
            (Self::TakeProfit, Side::Buy) | (Self::StopLoss, Side::Sell) => 1.0 * pct,
            (_, _) => panic!("invalid side"),
        }
    }
}

impl BenchmarkOrder {
    pub fn new(
        side: Side,
        price: f32,
        units: f32,
        fee_rate: f32,
        decimals: i32,
        tick_size: f32,
        order_cost: f32,
        leverage_factor: f32,
        price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
    ) -> Self {
        let initial_margin = order_cost / leverage_factor;
        let mut bankruptcy_price = None;
        if leverage_factor != 1.0 {
            bankruptcy_price = Some(round_down_nth_decimal(
                price * (leverage_factor + if side == Side::Buy { -1.0 } else { 1.0 })
                    / leverage_factor,
                decimals,
            ))
        }
        let mut stop_loss_price = None;
        if let Some(lock) = price_locks.0 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::StopLoss.get_price_mod(side, pct);
            let mut price = price * position_mod / leverage_factor;
            let price_remainder = calculate_remainder(price, tick_size);
            price = round_down_nth_decimal(price - price_remainder, decimals);
            stop_loss_price = Some(price);
        }
        let mut take_profit_price = None;
        if let Some(lock) = price_locks.1 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::TakeProfit.get_price_mod(side, pct);
            let mut price = price * position_mod / leverage_factor;
            let price_remainder = calculate_remainder(price, tick_size);
            price = round_down_nth_decimal(price - price_remainder, decimals);
            take_profit_price = Some(price);
        }
        let initial_fee = units * fee_rate * price;

        Self {
            side,
            prices: (price, bankruptcy_price, stop_loss_price, take_profit_price),
            units,
            leverage_factor,
            initial_margin,
            fees: (initial_fee, 0.0)
        }
    }

    pub fn calculate_pnl_and_returns(&self) -> (f64, f64) {
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
}

pub enum BenchmarkOrderError {
    ZeroUnits,
    UnitsLessThanMinimum,
    UnitsMoreThanMax,
}

pub fn calculate_remainder(dividend: f32, divisor: f32) -> f32 {
    dividend.rem_euclid(divisor)
}

pub fn count_decimal_places(value: f32) -> i32 {
    let decimal_str = value.to_string();

    if let Some(dot_idx) = decimal_str.find('.') {
        (decimal_str.len() - dot_idx - 1) as i32
    } else {
        0
    }
}

pub fn round_down_nth_decimal(num: f32, n: i32) -> f32 {
    let multiplier = 10.0_f32.powi(n);
    (num * multiplier).floor() / multiplier
}

pub fn new_benchmark_order(
    side: Side,
    price: f32,
    taker_fee_rate: f32, // usually taker fee
    order_fee_rate: f32,
    order_sizes: (f32, f32), // (min,max)
    leverage_factor: f32,
    tick_size: f32,
    price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
    expenditure: f32,
) -> Result<(BenchmarkOrder, f32), BenchmarkOrderError> {
    let price_lock_modifier = if side == Side::Sell {
        taker_fee_rate
    } else if side == Side::Buy {
        -taker_fee_rate
    } else {
        panic!("Invalid side for opening benchmark order");
    };
    let mut units = expenditure * leverage_factor
        / (price * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + price_lock_modifier)));
    let fract_units = calculate_remainder(units, order_sizes.0);
    let decimals = count_decimal_places(order_sizes.0);
    units = round_down_nth_decimal(units - fract_units, decimals);
    if units == 0.0 {
        return Err(BenchmarkOrderError::ZeroUnits);
    }
    if units < order_sizes.0 {
        return Err(BenchmarkOrderError::UnitsLessThanMinimum);
    }
    if units > order_sizes.1 {
        return Err(BenchmarkOrderError::UnitsMoreThanMax);
    }
    let balance_remainder = fract_units * price / leverage_factor;
    let order_cost = expenditure - balance_remainder;
    let order = BenchmarkOrder::new(
        side,
        price,
        units,
        order_fee_rate,
        decimals,
        tick_size,
        order_cost,
        leverage_factor,
        price_locks,
    );
    Ok((order, balance_remainder))
}
