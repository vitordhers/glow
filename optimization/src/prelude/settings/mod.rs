use common::enums::order_type::OrderType;
use glow_error::GlowError;
use itertools::iproduct;
use std::{collections::HashSet, ops::RangeInclusive, panic::catch_unwind};

#[derive(Debug, Clone, Copy)]
pub enum PriceLevelType {
    StopLoss,
    TrailingStopLoss,
    TakeProfit,
}

pub struct PriceLevelGenerator {
    pub price_level_type: PriceLevelType,
    pub min: u8,
    pub step: u8,
    pub max: u8,
    pub range: HashSet<u8>,
}

const MAX_STOP_LOSS: u8 = 50;
const MAX_TAKE_PROFIT: u8 = 50;
const MAX_TRAILING_STOP_LOSS: u8 = 50;

fn get_max(price_level_type: PriceLevelType) -> u8 {
    match price_level_type {
        PriceLevelType::StopLoss => MAX_STOP_LOSS,
        PriceLevelType::TrailingStopLoss => MAX_TRAILING_STOP_LOSS,
        PriceLevelType::TakeProfit => MAX_TAKE_PROFIT,
    }
}

impl PriceLevelGenerator {
    fn calculate_range(
        price_level_type: PriceLevelType,
        min: u8,
        step: u8,
        max: u8,
    ) -> HashSet<u8> {
        assert!(step != 0 && max != 0, "min, step and max can't be zero");
        let type_max = get_max(price_level_type);
        assert!(
            min < type_max && step < type_max,
            "min and step can't be greater than or equal {}",
            type_max
        );
        assert!(max <= type_max, "max can't be greater than {}", type_max);
        assert!(min < max, "min must be less than max");
        assert!(step < max, "step must be less than max");
        assert!(
            min + step <= max,
            "min + step must be less than or equal max"
        );
        (min..=max).step_by(step as usize).collect()
    }

    pub fn new(price_level_type: PriceLevelType, min: u8, step: u8, max: u8) -> Self {
        let range = Self::calculate_range(price_level_type, min, step, max);
        Self {
            price_level_type,
            min,
            step,
            max,
            range,
        }
    }

    pub fn patch_params(
        &mut self,
        min: Option<u8>,
        step: Option<u8>,
        max: Option<u8>,
    ) -> Result<HashSet<u8>, GlowError> {
        self.min = min.unwrap_or(self.min);
        self.step = step.unwrap_or(self.step);
        self.max = max.unwrap_or(self.max);
        match catch_unwind(|| {
            Self::calculate_range(self.price_level_type, self.min, self.step, self.max)
        }) {
            Ok(added_range) => {
                let mut newly_added = HashSet::new();
                for value in added_range {
                    if self.range.insert(value) {
                        newly_added.insert(value);
                    }
                }
                Ok(newly_added)
            }
            Err(_) => Err(GlowError::new(
                "catch_unwind_error".to_string(),
                "patch params error".to_string(),
            )),
        }
    }
}

pub struct PriceLevelModifiersGenerator {
    pub stop_loss: PriceLevelGenerator,          // 1% to 50% -> 51x
    pub trailing_stop_loss: PriceLevelGenerator, // 1% to 50% -> 51x
    pub take_profit: PriceLevelGenerator,        // 1% to 50% -> 51x
}

#[derive(Debug, Clone, Copy)]
pub struct PriceLevelModifiers {
    pub stop_loss: Option<f32>,          // less likely to bankrupt as goes up
    pub trailing_stop_loss: Option<f32>, // ambiguous likely to bankrut as goes up
    pub take_profit: Option<f32>,        // ambiguous likely to bankrupt as goes up
}

impl PriceLevelModifiers {
    pub fn new(
        stop_loss: Option<f32>,
        trailing_stop_loss: Option<f32>,
        take_profit: Option<f32>,
    ) -> Self {
        Self {
            stop_loss,
            trailing_stop_loss,
            take_profit,
        }
    }
}
// stream settings is a must
pub struct TradingSettingsGenerator {
    pub order_type: (OrderType, OrderType),
    pub allocation_percentage: RangeInclusive<u8>,
    pub leverage: RangeInclusive<u8>,
    pub price_level_modifiers: PriceLevelModifiersGenerator,
    pub estimate_product: Option<usize>,
}

impl TradingSettingsGenerator {
    pub fn new_full_combinations() -> Self {
        Self {
            order_type: (OrderType::Market, OrderType::Market),
            allocation_percentage: 1..=100,
            leverage: 0..=50,
            price_level_modifiers: PriceLevelModifiersGenerator {
                take_profit: PriceLevelGenerator::new(PriceLevelType::TakeProfit, 0, 1, 50),
                trailing_stop_loss: PriceLevelGenerator::new(
                    PriceLevelType::TrailingStopLoss,
                    0,
                    1,
                    50,
                ),
                stop_loss: PriceLevelGenerator::new(PriceLevelType::StopLoss, 0, 1, 50),
            },
            estimate_product: Some(344_632_500),
        }
    }

    pub fn stream_combinations(&self) -> impl Iterator<Item = TradingSettings> + Clone {
        // let allocations = self.allocation_percentage.clone();
        // let leverages = self.leverage.clone();
        // let take_profits: Vec<u8> = self
        //     .price_level_modifiers
        //     .take_profit
        //     .range
        //     .clone()
        //     .into_iter()
        //     .collect();
        // let trailing_stop_losses: Vec<u8> = self
        //     .price_level_modifiers
        //     .trailing_stop_loss
        //     .range
        //     .clone()
        //     .into_iter()
        //     .collect();
        // let stop_losses: Vec<u8> = self
        //     .price_level_modifiers
        //     .stop_loss
        //     .range
        //     .clone()
        //     .into_iter()
        //     .collect();
        iproduct!(
            [
                1_u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
                44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
                65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85,
                86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
            ],
            [
                0_u8, 1_u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                43, 44, 45, 46, 47, 48, 49, 50
            ],
            [
                0_u8, 1_u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                43, 44, 45, 46, 47, 48, 49, 50
            ],
            [
                0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                43, 44, 45, 46, 47, 48, 49
            ],
            [
                0_u8, 1_u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                43, 44, 45, 46, 47, 48, 49, 50
            ],
        )
        // iproduct!(
        //     allocations,
        //     leverages,
        //     stop_losses,
        //     trailing_stop_losses,
        //     take_profits,
        // )
        .filter(|&(_, _, _, tsl, tp)| tsl <= tp)
        .map(|(allocation, leverage, sl, tsl, tp)| {
            TradingSettings::new(
                (allocation as f32) / 100_f32,
                1_f32 + (leverage as f32 / 100_f32),
                OrderType::Market,
                PriceLevelModifiers::new(
                    (sl != 0).then(|| sl as f32 / 100.0),
                    (tsl != 0).then(|| tsl as f32 / 100.0),
                    (tp != 0).then(|| tp as f32 / 100.0),
                ),
            )
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TradingSettings {
    pub allocation_percentage: f32, // min 1, max 100 -> 100x // more likely to bankrupt as goes up
    pub leverage: f32,              // 51x // more likely to bankrupt as goes up
    pub order_type: (OrderType, OrderType), // open, close
    pub price_level_modifiers: PriceLevelModifiers,
}

impl TradingSettings {
    pub fn new(
        allocation_percentage: f32,
        leverage: f32,
        order_type: OrderType,
        price_level_modifiers: PriceLevelModifiers,
    ) -> Self {
        Self {
            allocation_percentage,
            leverage,
            order_type: (order_type, order_type),
            price_level_modifiers,
        }
    }
}

#[test]
fn test_settings_product() {
    let gen = TradingSettingsGenerator::new_full_combinations();
    let iter = gen.stream_combinations();
    let mut counter = 0;
    for _settings in iter {
        counter += 1;
        // if counter % 1_000_000 == 0 {
        //     println!("SETTINGS {:?}", settings);
        // }
    }

    println!("TOTAL COMBINATIONS {}", counter);
}

#[test]
fn test_settings_product_par1() {
    use std::thread::spawn;

    let gen = TradingSettingsGenerator::new_full_combinations();
    let iter = gen.stream_combinations();
    let threads = rayon::current_num_threads();
    let estimate = gen.estimate_product.unwrap() / threads;
    // let full: Vec<TradingSettings> = iter.collect();
    println!("rayon threads {}", threads);
    let mut handles = vec![];
    for i in 1..=threads {
        let prev = (i - 1) * estimate;
        let curr = i * estimate;
        let current_iter = iter.clone();
        let handle = spawn(move || {
            let slice: Vec<_> = current_iter.skip(prev).take(estimate).collect();
            println!("PREV {}, CURR {}", prev, curr);

            let mut result = 0;
            for _value in slice {
                result += 1;
            }
            result
        });
        handles.push(handle);
    }
    let mut total = 0;
    for handle in handles {
        let value = handle.join().unwrap();
        total += value;
    }

    println!("@@@ THIS IS TOTAL: {}", total);
    // use rayon::{prelude::*, ThreadPoolBuilder};
    // ThreadPoolBuilder::new()
    //     .num_threads(8) // set this to your desired number of threads
    //     .build_global()
    //     .unwrap();
    //
    // let gen = TradingSettingsGenerator::new_full_combinations();
    // let iter = gen.stream_combinations();
    //
    // // Collect counts per thread without atomic
    // let total: usize = iter
    //     .par_bridge()
    //     .map(|_settings| {
    //         // simulate some CPU work here if needed
    //         1usize // count 1 per item
    //     })
    //     .sum();
    //
    // println!("TOTAL COMBINATIONS {}", total);
}
