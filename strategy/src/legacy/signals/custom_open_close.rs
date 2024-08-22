use super::{
    enums::signal_category::SignalCategory, errors::Error, functions::get_symbol_close_col,
    traits::signal::Signal,
};
use polars::prelude::*;

#[derive(Clone)]
pub struct MultipleStochasticWithThresholdShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub cloud_length: u32,
}

#[derive(Clone)]
pub struct MultipleStochasticWithThresholdLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub cloud_length: u32,
}

#[derive(Clone)]
pub struct MultipleStochasticWithThresholdCloseLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub cloud_length: u32,
}

#[derive(Clone)]
pub struct MultipleStochasticWithThresholdCloseShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub cloud_length: u32,
}

impl Signal for MultipleStochasticWithThresholdShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";
        let trends_col = "trends";

        // let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(
                        col(trends_col)
                            .eq(lit("1234"))
                            .and(
                                col(close_slow_diff)
                                    .gt_eq(lit(0.025))
                                    .or(col(close_fast_diff).gt_eq(lit(0.02))),
                            )
                            .or(col(trends_col).eq(lit("1324")).and(
                                col(close_slow_diff)
                                    .gt_eq(lit(0.01))
                                    .or(col(close_fast_diff).gt_eq(lit(0.01))),
                            ))
                            .or(col(trends_col)
                                .eq(lit("1432"))
                                .and(col(close_slow_diff).gt_eq(lit(0.01))))
                            .or(col(trends_col)
                                .eq(lit("1423"))
                                .and(col(close_slow_diff).gt_eq(lit(0.01))))
                            // .or(
                            //     col(trends_col).eq(lit("3421")).and(col(close_slow_diff).gt_eq(lit(0.015)))
                            // )
                            .or(col(trends_col)
                                .eq(lit("4321"))
                                .and(col(close_slow_diff).gt_eq(lit(0.005))))
                            .or(col(trends_col).eq(lit("4312")).and(
                                col(close_slow_diff)
                                    .gt_eq(lit(0.015))
                                    .or(col(close_fast_diff).gt_eq(lit(0.01))),
                            )),
                    ),
            )
            .then(col(trends_col))
            .otherwise(lit(""))
            .alias(signal_col),
        );
        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = data.clone().lazy();
        new_lf = self.set_signal_column(&new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = data.clone();
        let signal = self.signal_category();
        let column = signal.get_column();
        let series = new_df.column(column)?;
        let _ = result_df.replace(&column, series.to_owned());

        Ok(result_df)
    }
}

impl Signal for MultipleStochasticWithThresholdLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoLong
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);
        // let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";
        let trends_col = "trends";

        // let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
        //     GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(
                        col(trends_col)
                            .eq(lit("1234"))
                            .and(col(close_slow_diff).lt_eq(lit(-0.005)))
                            .or(col(trends_col)
                                .eq(lit("1243"))
                                .and(col(close_slow_diff).lt_eq(lit(-0.0025))))
                            .or(col(trends_col)
                                .eq(lit("4321"))
                                .and(col(close_slow_diff).lt_eq(lit(-0.015))))
                            .or(col(trends_col)
                                .eq(lit("4312"))
                                .and(col(close_slow_diff).lt_eq(lit(-0.005))))
                            .or(col(trends_col)
                                .eq(lit("4132"))
                                .and(col(close_slow_diff).lt_eq(lit(-0.01)))),
                    ),
            )
            .then(col(trends_col))
            .otherwise(lit(""))
            .alias(signal_col),
        );

        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = data.clone().lazy();
        new_lf = self.set_signal_column(&new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = data.clone();
        let signal = self.signal_category();
        let column = signal.get_column();
        let series = new_df.column(column)?;
        let _ = result_df.replace(&column, series.to_owned());

        Ok(result_df)
    }
}

impl Signal for MultipleStochasticWithThresholdCloseLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseLong
    }
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let signal_col_clone = signal_col.to_string();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        // let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            col(close_slow_diff)
                .apply_many(
                    move |series: &mut [Series]| {
                        let close_slow_diff_series: &Series = &series[0];
                        let close_fast_diff_series: &Series = &series[1];
                        let cloud_top_series: &Series = &series[2];
                        let close_series: &Series = &series[3];

                        let close_slow_vec = close_slow_diff_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let close_fast_vec = close_fast_diff_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let cloud_top_vec = cloud_top_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let closes_vec = close_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let mut close_long_signals_vec: Vec<Option<String>> = vec![];

                        for (index, _) in close_slow_vec.iter().enumerate() {
                            let mut value = "".to_string();

                            if index == 0 {
                                close_long_signals_vec.push(Some(value));
                                continue;
                            }

                            let current_close_slow = close_slow_vec[index];
                            let current_close_fast = close_fast_vec[index];

                            let previous_cloud_top = cloud_top_vec[index - 1];
                            let current_cloud_top = cloud_top_vec[index];
                            let previous_close = closes_vec[index - 1];
                            let current_close = closes_vec[index];

                            if current_close_slow.is_none()
                                || current_close_fast.is_none()
                                || previous_cloud_top.is_none()
                                || current_cloud_top.is_none()
                                || previous_close.is_none()
                                || current_close.is_none()
                            {
                                close_long_signals_vec.push(Some(value));
                                continue;
                            }

                            let close_slow = current_close_slow.unwrap();
                            let close_fast = current_close_fast.unwrap();
                            let prev_cloud_top = previous_cloud_top.unwrap();
                            let cloud_top = current_cloud_top.unwrap();
                            let prev_close = previous_close.unwrap();
                            let close = current_close.unwrap();

                            if cloud_top == close || prev_cloud_top != prev_close {
                                close_long_signals_vec.push(Some(value));
                                continue;
                            }

                            if close_slow >= 0.01 || close_fast >= 0.005 {
                                value = value + "1234_";
                            }

                            if close_slow >= 0.01 || close_fast >= 0.005 {
                                value = value + "1243_";
                            }

                            if close_slow >= 0.025 || close_fast >= 0.02 {
                                value = value + "4321_";
                            }

                            if close_slow >= 0.01 || close_fast >= 0.01 {
                                value = value + "4312_";
                            }

                            if close_slow >= 0.01 || close_fast >= 0.01 {
                                value = value + "4132_";
                            }

                            if !value.is_empty() {
                                // Remove the last character
                                value.truncate(value.len() - 1);
                            }

                            close_long_signals_vec.push(Some(value));
                        }

                        let series = Series::new(&signal_col_clone, close_long_signals_vec);
                        Ok(Some(series))
                    },
                    &[col(close_fast_diff), col(cloud_top_col), col(close_col)],
                    returns_output,
                )
                .alias(signal_col),
        );

        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = data.clone().lazy();
        new_lf = self.set_signal_column(&new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = data.clone();
        let signal = self.signal_category();
        let column = signal.get_column();
        let series = new_df.column(column)?;
        let _ = result_df.replace(&column, series.to_owned());

        Ok(result_df)
    }
}

impl Signal for MultipleStochasticWithThresholdCloseShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let signal_col_clone = signal_col.to_string().clone();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);
        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            col(close_slow_diff)
                .apply_many(
                    move |series: &mut [Series]| {
                        let close_slow_diff_series: &Series = &series[0];
                        let close_fast_diff_series: &Series = &series[1];
                        let cloud_base_series: &Series = &series[2];
                        let close_series: &Series = &series[3];

                        let close_slow_vec = close_slow_diff_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let close_fast_vec = close_fast_diff_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let cloud_base_vec = cloud_base_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let closes_vec = close_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let mut close_short_signals_vec: Vec<Option<String>> = vec![];

                        for (index, _) in close_slow_vec.iter().enumerate() {
                            let mut value = "".to_string();

                            if index == 0 {
                                close_short_signals_vec.push(Some(value));
                                continue;
                            }

                            let current_close_slow = close_slow_vec[index];
                            let current_close_fast = close_fast_vec[index];

                            let previous_cloud_base = cloud_base_vec[index - 1];
                            let current_cloud_base = cloud_base_vec[index];
                            let previous_close = closes_vec[index - 1];
                            let current_close = closes_vec[index];

                            if current_close_slow.is_none()
                                || current_close_fast.is_none()
                                || previous_cloud_base.is_none()
                                || current_cloud_base.is_none()
                                || previous_close.is_none()
                                || current_close.is_none()
                            {
                                close_short_signals_vec.push(Some(value));
                                continue;
                            }
                            let close_slow = current_close_slow.unwrap();
                            let close_fast = current_close_fast.unwrap();
                            let prev_cloud_base = previous_cloud_base.unwrap();
                            let cloud_base = current_cloud_base.unwrap();
                            let prev_close = previous_close.unwrap();
                            let close = current_close.unwrap();

                            if cloud_base == close || prev_cloud_base != prev_close {
                                close_short_signals_vec.push(Some(value));
                                continue;
                            }

                            if close_slow < 0.01 || close_fast <= -0.005 {
                                value = value + "1234_";
                            }

                            if close_slow < -0.01 {
                                value = value + "1324_";
                            }

                            if close_slow <= -0.005 {
                                value = value + "1432_";
                            }

                            if close_fast <= -0.005 {
                                value = value + "1423_";
                            }

                            if close_slow <= -0.005 || close_fast < -0.005 {
                                value = value + "4321_";
                            }

                            if close_slow <= -0.015 || close_fast < -0.01 {
                                value = value + "4312_";
                            }

                            if !value.is_empty() {
                                // Remove the last character
                                value.truncate(value.len() - 1);
                            }

                            close_short_signals_vec.push(Some(value));
                        }

                        let series = Series::new(&signal_col_clone, close_short_signals_vec);
                        Ok(Some(series))
                    },
                    &[col(close_fast_diff), col(cloud_base_col), col(close_col)],
                    returns_output,
                )
                .alias(signal_col),
        );
        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = data.clone().lazy();
        new_lf = self.set_signal_column(&new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = data.clone();
        let signal = self.signal_category();
        let column = signal.get_column();
        let series = new_df.column(column)?;
        let _ = result_df.replace(&column, series.to_owned());

        Ok(result_df)
    }
}

#[derive(Clone)]
pub enum SignalWrapper {
    MultipleStochasticWithThresholdShortSignal(MultipleStochasticWithThresholdShortSignal),
    MultipleStochasticWithThresholdCloseShortSignal(
        MultipleStochasticWithThresholdCloseShortSignal,
    ),
    MultipleStochasticWithThresholdLongSignal(MultipleStochasticWithThresholdLongSignal),
    MultipleStochasticWithThresholdCloseLongSignal(MultipleStochasticWithThresholdCloseLongSignal),
}

impl Signal for SignalWrapper {
    fn signal_category(&self) -> SignalCategory {
        match self {
            Self::MultipleStochasticWithThresholdShortSignal(
                multiple_stochastic_with_threshold_short_signal,
            ) => multiple_stochastic_with_threshold_short_signal.signal_category(),
            Self::MultipleStochasticWithThresholdCloseShortSignal(
                multiple_stochastic_with_threshold_short_close_signal,
            ) => multiple_stochastic_with_threshold_short_close_signal.signal_category(),
            Self::MultipleStochasticWithThresholdLongSignal(
                multiple_stochastic_with_threshold_long_signal,
            ) => multiple_stochastic_with_threshold_long_signal.signal_category(),
            Self::MultipleStochasticWithThresholdCloseLongSignal(
                multiple_stochastic_with_threshold_long_close_signal,
            ) => multiple_stochastic_with_threshold_long_close_signal.signal_category(),
        }
    }
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        match self {
            Self::MultipleStochasticWithThresholdShortSignal(
                multiple_stochastic_with_threshold_short_signal,
            ) => multiple_stochastic_with_threshold_short_signal.set_signal_column(lf),
            Self::MultipleStochasticWithThresholdCloseShortSignal(
                multiple_stochastic_with_threshold_short_close_signal,
            ) => multiple_stochastic_with_threshold_short_close_signal.set_signal_column(lf),
            Self::MultipleStochasticWithThresholdLongSignal(
                multiple_stochastic_with_threshold_long_signal,
            ) => multiple_stochastic_with_threshold_long_signal.set_signal_column(lf),
            Self::MultipleStochasticWithThresholdCloseLongSignal(
                multiple_stochastic_with_threshold_long_close_signal,
            ) => multiple_stochastic_with_threshold_long_close_signal.set_signal_column(lf),
        }
    }
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        match self {
            Self::MultipleStochasticWithThresholdShortSignal(
                multiple_stochastic_with_threshold_short_signal,
            ) => multiple_stochastic_with_threshold_short_signal.update_signal_column(data),
            Self::MultipleStochasticWithThresholdCloseShortSignal(
                multiple_stochastic_with_threshold_short_close_signal,
            ) => multiple_stochastic_with_threshold_short_close_signal.update_signal_column(data),
            Self::MultipleStochasticWithThresholdLongSignal(
                multiple_stochastic_with_threshold_long_signal,
            ) => multiple_stochastic_with_threshold_long_signal.update_signal_column(data),
            Self::MultipleStochasticWithThresholdCloseLongSignal(
                multiple_stochastic_with_threshold_long_close_signal,
            ) => multiple_stochastic_with_threshold_long_close_signal.update_signal_column(data),
        }
    }
}

impl From<MultipleStochasticWithThresholdShortSignal> for SignalWrapper {
    fn from(value: MultipleStochasticWithThresholdShortSignal) -> Self {
        Self::MultipleStochasticWithThresholdShortSignal(value)
    }
}

impl From<MultipleStochasticWithThresholdCloseShortSignal> for SignalWrapper {
    fn from(value: MultipleStochasticWithThresholdCloseShortSignal) -> Self {
        Self::MultipleStochasticWithThresholdCloseShortSignal(value)
    }
}

impl From<MultipleStochasticWithThresholdLongSignal> for SignalWrapper {
    fn from(value: MultipleStochasticWithThresholdLongSignal) -> Self {
        Self::MultipleStochasticWithThresholdLongSignal(value)
    }
}

impl From<MultipleStochasticWithThresholdCloseLongSignal> for SignalWrapper {
    fn from(value: MultipleStochasticWithThresholdCloseLongSignal) -> Self {
        Self::MultipleStochasticWithThresholdCloseLongSignal(value)
    }
}
