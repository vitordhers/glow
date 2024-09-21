use common::enums::{modifiers::price_level::PriceLevel, side::Side};
pub mod functions;

pub struct BenchmarkTrade {
    pub side: Side,
    pub prices: (f32, Option<f32>, Option<f32>, Option<f32>),
    pub units: f32,
    pub leverage_factor: f32,
    pub initial_margin: f32,
    pub open_fee: f32,
}

pub struct PriceLock(f32);

impl From<PriceLevel> for PriceLock {
    fn from(value: PriceLevel) -> Self {
        match value {
            PriceLevel::StopLoss(factor) | PriceLevel::TakeProfit(factor) => {
                PriceLock(factor as f32)
            }
        }
    }
}
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

impl BenchmarkTrade {
    pub fn new(
        side: Side,
        price: f32,
        units: f32,
        fee_rate: f32,
        decimals: i32,
        order_cost: f32,
        leverage_factor: f32,
        price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
    ) -> Self {
        let initial_margin = order_cost / leverage_factor;
        let mut bankruptcy_price = None;
        if leverage_factor != 1.0 {
            bankruptcy_price = Some(round_down_nth_decimal(
                price * (leverage_factor + if side == Side::Sell { 1.0 } else { -1.0 })
                    / leverage_factor,
                decimals,
            ))
        }
        let mut stop_loss_price = None;
        if let Some(lock) = price_locks.0 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::StopLoss.get_price_mod(side, pct);
            let price = round_down_nth_decimal(price * position_mod / leverage_factor, decimals);
            stop_loss_price = Some(price);
        }
        let mut take_profit_price = None;
        if let Some(lock) = price_locks.1 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::TakeProfit.get_price_mod(side, pct);
            let price = round_down_nth_decimal(price * position_mod / leverage_factor, decimals);
            take_profit_price = Some(price);
        }
        let open_fee = units * fee_rate * price;

        Self {
            side,
            prices: (price, bankruptcy_price, stop_loss_price, take_profit_price),
            units,
            leverage_factor,
            initial_margin,
            open_fee,
        }
    }

    pub fn get_pnl_returns_and_fees(&self, price: f32, close_fee_rate: f32) -> (f32, f32, f32) {
        // short: Unrealized P&L = (Average Entry Price - Current Mark Price) × Position Size, ROI = [(Entry Price − Mark Price) × Position Size/ Initial Margin] × 100%
        // long: Unrealized P&L = (Current Mark Price - Average Entry Price) × Position Size, ROI = [(Mark Price − Entry Price) × Position Size/ Initial Margin] × 100%
        // TODO: CHECK THIS =>  here, we don't subtract fees since their effects result in having less units
        let close_fee = self.units * price * close_fee_rate;
        let price = if self.side == Side::Sell {
            self.prices.0 - price
        } else {
            price - self.prices.0
        };
        // TODO: (self.prices.0 - price) * self.units - (close_fee + self.open_fee) make sure close_fee is not double-counted
        let pnl = price * self.units - close_fee;
        let roi = if self.initial_margin != 0.0 {
            pnl / self.initial_margin
        } else {
            0.0
        };
        (pnl, roi, close_fee)
    }
}

pub enum BenchmarkTradeError {
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

pub fn new_benchmark_trade(
    side: Side,
    price: f32,
    taker_fee_rate: f32, // usually taker fee
    open_order_fee_rate: f32,
    order_sizes: (f32, f32), // (min,max)
    leverage_factor: f32,
    tick_size: f32,
    price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
    expenditure: f32,
) -> Result<(BenchmarkTrade, f32), BenchmarkTradeError> {
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
        return Err(BenchmarkTradeError::ZeroUnits);
    }
    if units < order_sizes.0 {
        return Err(BenchmarkTradeError::UnitsLessThanMinimum);
    }
    if units > order_sizes.1 {
        return Err(BenchmarkTradeError::UnitsMoreThanMax);
    }
    let balance_remainder = fract_units * price / leverage_factor;
    let order_cost = expenditure - balance_remainder;
    let order = BenchmarkTrade::new(
        side,
        price,
        units,
        open_order_fee_rate,
        decimals,
        order_cost,
        leverage_factor,
        price_locks,
    );
    Ok((order, balance_remainder))
}
