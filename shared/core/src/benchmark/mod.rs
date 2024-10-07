use common::enums::{modifiers::price_level::PriceLevel, side::Side};
pub mod functions;

#[derive(Clone, Copy, Debug)]
pub struct BenchmarkTrade {
    pub initial_margin: f32,
    pub leverage_factor: f32,
    pub open_fee: f32,
    pub prices: (f32, Option<f32>, Option<f32>, Option<f32>), // (price, bankruptcy_price, stop_loss_price, take_profit_price)
    pub side: Side,
    pub symbol_decimals: i32,
    pub tick_decimals: i32,
    pub units: f32,
}

#[derive(Clone, Copy)]
pub struct PriceLock(pub f32);

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
        tick_decimals: i32,
    ) -> Self {
        let mut bankruptcy_price = None;
        if leverage_factor != 1.0 {
            bankruptcy_price = Some(round_down_nth_decimal(
                price * (leverage_factor + if side == Side::Sell { 1.0 } else { -1.0 })
                    / leverage_factor,
                tick_decimals,
            ))
        }
        let mut stop_loss_price = None;
        if let Some(lock) = price_locks.0 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::StopLoss.get_price_mod(side, pct);
            let sl_price = round_nth_decimal(price * position_mod / leverage_factor, tick_decimals);
            stop_loss_price = Some(sl_price);
        }
        let mut take_profit_price = None;
        if let Some(lock) = price_locks.1 {
            let pct = lock.0;
            let position_mod = leverage_factor + LockType::TakeProfit.get_price_mod(side, pct);
            let tp_price = round_nth_decimal(price * position_mod / leverage_factor, tick_decimals);
            take_profit_price = Some(tp_price);
        }
        let open_fee = round_nth_decimal(units * open_order_fee_rate * price, tick_decimals);
        Self {
            initial_margin,
            leverage_factor,
            open_fee,
            prices: (price, bankruptcy_price, stop_loss_price, take_profit_price),
            side,
            symbol_decimals,
            units,
            tick_decimals,
        }
    }

    pub fn get_pnl_returns_and_fees(
        &self,
        price: f32,
        close_order_fee_rate: f32,
    ) -> (f32, f32, f32) {
        // short: Unrealized P&L = (Average Entry Price - Current Mark Price) × Position Size, ROI = [(Entry Price − Mark Price) × Position Size/ Initial Margin] × 100%
        // long: Unrealized P&L = (Current Mark Price - Average Entry Price) × Position Size, ROI = [(Mark Price − Entry Price) × Position Size/ Initial Margin] × 100%
        // TODO: CHECK THIS =>  here, we don't subtract fees since their effects result in having less units
        let close_fee = round_nth_decimal(
            self.units * price * close_order_fee_rate,
            self.symbol_decimals,
        );
        let price = if self.side == Side::Sell {
            self.prices.0 - price
        } else {
            price - self.prices.0
        };
        // TODO: (self.prices.0 - price) * self.units - (close_fee + self.open_fee) make sure close_fee is not double-counted
        let pnl = round_nth_decimal(
            price * self.units - (self.open_fee + close_fee),
            self.symbol_decimals,
        );
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BenchmarkTradeError {
    UnitsLessThanMinSize {
        min_expenditure: f32,
    },
    UnitsMoreThanMaxSize {
        max_expenditure: f32,
        expenditure: f32,
    },
    ValueLessThanNotionalMin {
        min_expenditure: f32,
    },
    ZeroUnits,
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

#[derive(Clone, Copy)]
pub struct NewBenchmarkTradeParams {
    pub allocation_pct: f32,
    pub current_balance: f32,
    pub leverage_factor: f32,
    pub minimum_notional_value: Option<f32>,
    pub open_order_fee_rate: f32,
    pub order_sizes: (f32, f32), // (min,max)
    pub price: f32,
    pub price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
    pub side: Side,
    pub symbol_decimals: i32,
    pub taker_fee_rate: f32, // usually taker fee
    pub tick_decimals: i32,
}

impl NewBenchmarkTradeParams {
    pub fn new(
        allocation_pct: f32,
        current_balance: f32,
        leverage_factor: f32,
        minimum_notional_value: Option<f32>,
        open_order_fee_rate: f32,
        order_sizes: (f32, f32), // (min,max)
        price: f32,
        price_locks: (Option<PriceLock>, Option<PriceLock>), // (stop_loss, take_profit)
        side: Side,
        symbol_decimals: i32,
        taker_fee_rate: f32,
        tick_decimals: i32,
    ) -> Self {
        Self {
            allocation_pct,
            current_balance,
            leverage_factor,
            minimum_notional_value,
            open_order_fee_rate,
            order_sizes,
            price,
            price_locks,
            side,
            symbol_decimals,
            taker_fee_rate,
            tick_decimals,
        }
    }
}

pub fn new_benchmark_trade(
    params: NewBenchmarkTradeParams,
) -> Result<BenchmarkTrade, BenchmarkTradeError> {
    let NewBenchmarkTradeParams {
        allocation_pct,
        current_balance,
        leverage_factor,
        minimum_notional_value,
        open_order_fee_rate,
        order_sizes,
        price,
        price_locks,
        side,
        symbol_decimals,
        taker_fee_rate,
        tick_decimals,
    } = params;
    let price_lock_modifier = if side == Side::Sell {
        taker_fee_rate
    } else if side == Side::Buy {
        -taker_fee_rate
    } else {
        unreachable!();
    };
    let expenditure =
        round_down_nth_decimal(allocation_pct * current_balance / 100_f32, tick_decimals);
    let units = round_down_nth_decimal(
        expenditure * leverage_factor
            / (price * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + price_lock_modifier))),
        symbol_decimals,
    );

    if units == 0.0 {
        return Err(BenchmarkTradeError::ZeroUnits);
    }
    if units < order_sizes.0 {
        let min_units = order_sizes.0;
        let min_expenditure = round_down_nth_decimal(
            min_units
                * (price
                    * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + price_lock_modifier)))
                / leverage_factor,
            tick_decimals,
        );
        return Err(BenchmarkTradeError::UnitsLessThanMinSize { min_expenditure });
    }
    if units > order_sizes.1 {
        let max_units = order_sizes.1;
        let max_expenditure = round_down_nth_decimal(
            max_units
                * (price
                    * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + price_lock_modifier)))
                / leverage_factor,
            tick_decimals,
        );
        return Err(BenchmarkTradeError::UnitsMoreThanMaxSize {
            max_expenditure,
            expenditure,
        });
    }

    let order_value = round_nth_decimal(units * price, tick_decimals);
    if let Some(minimum_notional_value) = minimum_notional_value {
        if order_value < minimum_notional_value {
            return Err(BenchmarkTradeError::ValueLessThanNotionalMin {
                min_expenditure: minimum_notional_value,
            });
        }
    }
    let initial_margin = round_nth_decimal(order_value / leverage_factor, tick_decimals);
    // let balance_remainder = round_down_nth_decimal(expenditure - initial_margin, tick_decimals);

    let trade = BenchmarkTrade::new(
        initial_margin,
        leverage_factor,
        open_order_fee_rate,
        price,
        price_locks,
        side,
        symbol_decimals,
        units,
        tick_decimals,
    );
    Ok(trade)
}
