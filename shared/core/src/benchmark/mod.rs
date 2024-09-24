use common::enums::{modifiers::price_level::PriceLevel, side::Side};
pub mod functions;

pub struct BenchmarkTrade {
    pub initial_margin: f32,
    pub leverage_factor: f32,
    pub open_fee: f32,
    pub prices: (f32, Option<f32>, Option<f32>, Option<f32>), // (price, bankruptcy_price, stop_loss_price, take_profit_price)
    pub side: Side,
    pub symbol_decimals: i32,
    pub units: f32,
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
        initial_margin: f32,
        leverage_factor: f32,
        open_order_fee_rate: f32,
        price: f32,
        price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
        side: Side,
        symbol_decimals: i32,
        units: f32,
    ) -> Self {
        let mut bankruptcy_price = None;
        if leverage_factor != 1.0 {
            bankruptcy_price = Some(round_down_nth_decimal(
                price * (leverage_factor + if side == Side::Sell { 1.0 } else { -1.0 })
                    / leverage_factor,
                symbol_decimals,
            ))
        }
        let mut stop_loss_price = None;
        if let Some(lock) = price_locks.0 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::StopLoss.get_price_mod(side, pct);
            let sl_price =
                round_nth_decimal(price * position_mod / leverage_factor, symbol_decimals);
            stop_loss_price = Some(sl_price);
        }
        let mut take_profit_price = None;
        if let Some(lock) = price_locks.1 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::TakeProfit.get_price_mod(side, pct);
            let tp_price =
                round_nth_decimal(price * position_mod / leverage_factor, symbol_decimals);
            take_profit_price = Some(tp_price);
        }
        let open_fee = round_nth_decimal(units * open_order_fee_rate * price, symbol_decimals);

        Self {
            initial_margin,
            leverage_factor,
            open_fee,
            prices: (price, bankruptcy_price, stop_loss_price, take_profit_price),
            side,
            symbol_decimals,
            units,
        }
    }

    pub fn get_pnl_returns_and_fees(&self, price: f32, close_fee_rate: f32) -> (f32, f32, f32) {
        // short: Unrealized P&L = (Average Entry Price - Current Mark Price) × Position Size, ROI = [(Entry Price − Mark Price) × Position Size/ Initial Margin] × 100%
        // long: Unrealized P&L = (Current Mark Price - Average Entry Price) × Position Size, ROI = [(Mark Price − Entry Price) × Position Size/ Initial Margin] × 100%
        // TODO: CHECK THIS =>  here, we don't subtract fees since their effects result in having less units
        let close_fee =
            round_nth_decimal(self.units * price * close_fee_rate, self.symbol_decimals);
        let price = if self.side == Side::Sell {
            self.prices.0 - price
        } else {
            price - self.prices.0
        };
        // TODO: (self.prices.0 - price) * self.units - (close_fee + self.open_fee) make sure close_fee is not double-counted
        let pnl = round_nth_decimal(price * self.units - close_fee, self.symbol_decimals);
        let roi = if self.initial_margin != 0.0 {
            pnl / self.initial_margin
        } else {
            0.0
        };
        (pnl, roi, close_fee)
    }

    pub fn get_threshold_prices(&self) -> (Option<f32>, Option<f32>) {
        match self.side {
            Side::Sell => (self.prices.3, self.prices.2.or_else(|| self.prices.1)),
            Side::Buy => (self.prices.2.or_else(|| self.prices.1), self.prices.3),
            Side::None => unreachable!(),
        }
    }
}

#[derive(Debug)]
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

pub fn round_down_nth_decimal(n: f32, decimals: i32) -> f32 {
    let multiplier = 10.0_f32.powi(decimals);
    (n * multiplier).floor() / multiplier
}

pub fn round_nth_decimal(n: f32, decimals: i32) -> f32 {
    let multiplier = 10.0_f32.powi(decimals);
    (n * multiplier).round() / multiplier
}

pub fn new_benchmark_trade(
    side: Side,
    price: f32,
    taker_fee_rate: f32, // usually taker fee
    symbol_decimals: i32,
    open_order_fee_rate: f32,
    order_sizes: (f32, f32), // (min,max)
    leverage_factor: f32,
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
    let units = round_down_nth_decimal(
        expenditure * leverage_factor
            / (price * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + price_lock_modifier))),
        symbol_decimals,
    );
    if units == 0.0 {
        return Err(BenchmarkTradeError::ZeroUnits);
    }
    if units < order_sizes.0 {
        return Err(BenchmarkTradeError::UnitsLessThanMinimum);
    }
    if units > order_sizes.1 {
        return Err(BenchmarkTradeError::UnitsMoreThanMax);
    }
    // 100.0043 USDT
    // 1.9117 USDT
    // 98,0926
    // 98.1017 USDT

    let order_value = round_nth_decimal(units * price, symbol_decimals);
    let initial_margin = round_nth_decimal(order_value / leverage_factor, symbol_decimals);
    let balance_remainder = round_down_nth_decimal(expenditure - initial_margin, symbol_decimals);
    let order = BenchmarkTrade::new(
        initial_margin,
        leverage_factor,
        open_order_fee_rate,
        price,
        price_locks,
        side,
        symbol_decimals,
        units,
    );
    Ok((order, balance_remainder))
}
