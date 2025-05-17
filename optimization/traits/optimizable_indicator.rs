use crate::trader::indicators::{ExponentialMovingAverageIndicator, StochasticThresholdIndicator};
use crate::trader::traits::indicator::Indicator;
use crate::trader::{errors::Error, indicators::StochasticIndicator};
use itertools::Itertools;
use polars::prelude::{col, Expr, LazyFrame};

pub trait OptimizableIndicator {
    fn get_select_clauses(&self, arg: OptimizableIndicatorArg) -> Vec<Expr>;
    fn set_combined_indicator_columns(
        &self,
        arg: OptimizableIndicatorArg,
        lf: LazyFrame,
    ) -> Result<LazyFrame, Error>;
}

pub enum OptimizableIndicatorWrapper {
    StochasticIndicator(StochasticIndicator),
    StochasticThresholdIndicator(StochasticThresholdIndicator),
    ExponentialMovingAverageIndicator(ExponentialMovingAverageIndicator),
}

impl OptimizableIndicator for OptimizableIndicatorWrapper {
    fn get_select_clauses(&self, arg: OptimizableIndicatorArg) -> Vec<Expr> {
        match &self {
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.get_select_clauses(arg)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.get_select_clauses(arg)
            }
            Self::ExponentialMovingAverageIndicator(exponential_moving_average_indicator) => {
                exponential_moving_average_indicator.get_select_clauses(arg)
            }
        }
    }

    fn set_combined_indicator_columns(
        &self,
        arg: OptimizableIndicatorArg,
        lf: LazyFrame,
    ) -> Result<LazyFrame, Error> {
        match &self {
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.set_combined_indicator_columns(arg, lf)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.set_combined_indicator_columns(arg, lf)
            }
            Self::ExponentialMovingAverageIndicator(exponential_moving_average_indicator) => {
                exponential_moving_average_indicator.set_combined_indicator_columns(arg, lf)
            }
        }
    }
}

impl OptimizableIndicator for StochasticIndicator {
    fn get_select_clauses(&self, param: OptimizableIndicatorArg) -> Vec<Expr> {
        match param {
            OptimizableIndicatorArg::StochasticIndicatorArg {
                windows,
                anchor_symbol,
            } => {
                let mut columns = Vec::new();
                for window in windows {
                    let suffix = format!("{}_{}", anchor_symbol, window);

                    let k_column = &format!("K%_{}", suffix);
                    let d_column = &format!("D%_{}", suffix);
                    columns.push(col(k_column));
                    columns.push(col(d_column));
                }
                columns
            }
            _ => {
                vec![]
            }
        }
    }
    fn set_combined_indicator_columns(
        &self,
        arg: OptimizableIndicatorArg,
        lf: LazyFrame,
    ) -> Result<LazyFrame, Error> {
        match arg {
            OptimizableIndicatorArg::StochasticIndicatorArg {
                windows,
                anchor_symbol,
            } => {
                let range = 3..=30;

                let indicator = StochasticIndicator {
                    name: "StochasticIndicator".to_string(),
                    windows: range.collect_vec(),
                    anchor_symbol,
                };

                indicator.set_indicator_columns(lf)
            }
            _ => Ok(lf),
        }
    }
}

impl OptimizableIndicator for ExponentialMovingAverageIndicator {
    fn get_select_clauses(&self, param: OptimizableIndicatorArg) -> Vec<Expr> {
        match param {
            OptimizableIndicatorArg::ExponentialMovingAverageIndicator {
                anchor_symbol,
                long_span,
                short_span,
            } => {
                let indicator = ExponentialMovingAverageIndicator {
                    name: "".to_string(),
                    anchor_symbol,
                    short_span,
                    long_span,
                };
                let columns_names = indicator.get_indicator_columns();
                columns_names
                    .into_iter()
                    .map(|col_name| col(&col_name))
                    .collect()
            }
            _ => vec![],
        }
    }

    fn set_combined_indicator_columns(
        &self,
        param: OptimizableIndicatorArg,
        lf: LazyFrame,
    ) -> Result<LazyFrame, Error> {
        match param {
            OptimizableIndicatorArg::ExponentialMovingAverageIndicator {
                anchor_symbol,
                long_span,
                short_span,
            } => {
                let mut lf = lf.clone();
                let mut combinations = (5..=50).step_by(5).combinations(2); // 990

                // while let Some(comb) = combinations.next() {
                //     let short_span = comb[0];
                //     let long_span = comb[1];
                //     let indicator = ExponentialMovingAverageIndicator {
                //         name: "".to_string(),
                //         anchor_symbol: anchor_symbol.clone(),
                //         trend_col_prefix: trend_col_prefix.clone(),
                //         short_span,
                //         long_span,
                //     };
                //     let indicator_lf = indicator.set_indicator_columns(lf)?;
                //     lf = lf.left_join(lf, "start_time", "start_time");
                // }

                Ok(lf)
            }
            _ => Ok(lf),
        }
    }
}

impl OptimizableIndicator for StochasticThresholdIndicator {
    fn get_select_clauses(&self, param: OptimizableIndicatorArg) -> Vec<Expr> {
        match param {
            OptimizableIndicatorArg::StochasticThresholdIndicator {
                lower_threshold,
                upper_threshold,
                anchor_symbol,
                trend_col_prefix,
                short_span,
                long_span,
            } => {
                let indicator = StochasticThresholdIndicator {
                    name: "".to_string(),
                    lower_threshold,
                    upper_threshold,
                    anchor_symbol,
                    trend_col_prefix,
                    short_span, // 3~
                    long_span,  // ~50
                };

                let columns_names = indicator.get_indicator_columns();
                columns_names
                    .into_iter()
                    .map(|col_name| col(&col_name))
                    .collect()
            }
            _ => vec![],
        }
    }

    fn set_combined_indicator_columns(
        &self,
        param: OptimizableIndicatorArg,
        lf: LazyFrame,
    ) -> Result<LazyFrame, Error> {
        match param {
            OptimizableIndicatorArg::StochasticThresholdIndicator {
                upper_threshold,
                lower_threshold,
                anchor_symbol,
                trend_col_prefix,
                short_span,
                long_span,
            } => {
                let mut lf = lf.clone();

                // let mut combinations = (15..=85).step_by(5).combinations(2);
                // while let Some(comb) = combinations.next() {
                //     let lower_threshold = comb[0];
                //     let upper_threshold = comb[1];

                //     let indicator = StochasticThresholdIndicator {
                //         name: "".to_string(),
                //         lower_threshold,
                //         upper_threshold,
                //         trend_col,
                //     };
                // }

                Ok(lf)
            }
            _ => Ok(lf),
        }
    }
}

pub enum OptimizableIndicatorArg {
    StochasticIndicatorArg {
        windows: Vec<u32>,
        anchor_symbol: String,
    },
    StochasticThresholdIndicator {
        upper_threshold: i32,
        lower_threshold: i32,
        anchor_symbol: String,
        trend_col_prefix: String,
        short_span: usize,
        long_span: usize,
    },
    ExponentialMovingAverageIndicator {
        anchor_symbol: String,
        long_span: usize,
        short_span: usize,
    },
}
