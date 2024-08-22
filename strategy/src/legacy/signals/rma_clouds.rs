use common::{
    enums::signal_category::SignalCategory, functions::get_symbol_close_col, traits::signal::Signal,
};
use glow_error::GlowError;
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

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
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
                    .and(col(trends_col).shift(1).eq(lit("1234")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.02)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1324")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.02)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1423")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.015)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.005))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("2314")))
                    .and(
                        col(close_slow_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_slow_diff).shift(1).gt(lit(0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_fast_diff).shift(1).gt(lit(0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("3241")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.01)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.005))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("4321")))
                    .and(
                        col(close_slow_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_slow_diff).shift(1).gt(lit(0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .gt_eq(lit(-0.01))
                            .and(col(close_fast_diff).shift(1).lt(lit(0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4321")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.02)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.005))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4123")))
                    .and(col(close_slow_diff).shift(1).lt_eq(lit(-0.02)))
                    .and(col(close_fast_diff).shift(1).lt_eq(lit(-0.0985))),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(signal_col),
        );
        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError> {
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

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);
        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);

        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";
        let trends_col = "trends";

        // let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
        //     GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1234")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.01)))
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_fast_diff).shift(1).lt(lit(0.0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("1243")))
                    .and(
                        col(close_slow_diff)
                            .gt_eq(lit(-0.005))
                            .and(col(close_slow_diff).shift(1).lt(lit(0.0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .gt_eq(lit(-0.01))
                            .and(col(close_fast_diff).shift(1).lt(lit(-0.005))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1423")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .gt_eq(lit(-0.01))
                            .and(col(close_slow_diff).shift(1).lt(lit(0.0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_fast_diff).shift(1).lt(lit(0.0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("3421")))
                    .and(
                        col(close_slow_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_slow_diff).shift(1).gt(lit(0.0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_fast_diff).shift(1).gt(lit(0.0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("3241")))
                    .and(
                        col(close_slow_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_slow_diff).shift(1).gt(lit(0.0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .lt_eq(lit(0.005))
                            .and(col(close_fast_diff).shift(1).gt(lit(0.0))),
                    ),
            )
            .then(lit(1))
            // .when(
            //     col(cloud_top_col).eq(col(close_col))
            //     .and(col(trends_col).eq(lit("2314")))
            //     .and(col(close_slow_diff).lt_eq(lit(0.005)).and(col(close_slow_diff).gt(lit(0))))
            //     .and(col(close_fast_diff).lt_eq(lit(0.005)).and(col(close_fast_diff).gt(lit(0))))
            // )
            // .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("4321")))
                    .and(col(close_slow_diff).lt_eq(lit(-0.02)))
                    .and(col(close_fast_diff).lt_eq(lit(-0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_top_col)
                    .eq(col(close_col))
                    .and(col(trends_col).eq(lit("4312")))
                    .and(col(close_slow_diff).lt_eq(lit(0.005)))
                    .and(col(close_fast_diff).lt_eq(lit(0.005))),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(signal_col),
        );

        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError> {
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
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let signal_col_clone = signal_col.to_string();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";
        let trends_col = "trends";

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1234")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.018)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.007))),
            )
            .then(lit(1)) // good for invert
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("2134")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .lt_eq(lit(0.01))
                            .and(col(close_slow_diff).shift(1).gt_eq(lit(0.005))),
                    )
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(0.0))
                            .and(col(close_fast_diff).shift(1).lt_eq(lit(0.005))),
                    ),
            )
            .then(lit(1))
            // .when(
            //     col(cloud_top_col).neq(col(close_col)).and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
            //     .and(col(trends_col).eq(lit("2314")))
            //     .and(col(close_slow_diff).gt_eq(lit(-0.005)).and(col(close_slow_diff).lt_eq(lit(0.005))))
            //     .and(col(close_fast_diff).gt_eq(lit(0.01)))
            // )
            // .then(lit(1)) // good for invert
            .when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("3241")))
                    .and(col(close_slow_diff).shift(1).gt_eq(lit(0.02)))
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.015))),
            )
            .then(lit(1)) // good for invert
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4132")))
                    .and(
                        col(close_slow_diff)
                            .gt_eq(lit(0.0))
                            .and(col(close_slow_diff).shift(1).lt_eq(lit(0.005))),
                    )
                    .and(
                        col(close_fast_diff)
                            .gt_eq(lit(-0.01))
                            .and(col(close_fast_diff).shift(1).lt_eq(lit(-0.005))),
                    ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(signal_col),
        );

        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError> {
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

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut signal_lf = lf.clone();

        let signal = self.signal_category();
        let signal_col = signal.get_column();
        let signal_col_clone = signal_col.to_string().clone();
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        let close_fast_diff = "close_fast_diff";
        let close_slow_diff = "close_slow_diff";
        let trends_col = "trends";

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Utf8);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("1234")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_slow_diff).shift(1).lt(lit(0.0))),
                    )
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("2134")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_slow_diff).shift(1).lt_eq(lit(0.005))),
                    )
                    .and(col(close_fast_diff).shift(1).gt_eq(lit(0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4321")))
                    .and(col(close_slow_diff).shift(1).lt(lit(-0.02)))
                    .and(col(close_fast_diff).shift(1).lt_eq(lit(-0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4312")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .gt_eq(lit(-0.01))
                            .and(col(close_slow_diff).shift(1).lt_eq(lit(-0.005))),
                    )
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_fast_diff).shift(1).lt_eq(lit(0.0))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4231")))
                    .and(col(close_slow_diff).shift(1).lt(lit(-0.02)))
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(-0.01))
                            .and(col(close_fast_diff).shift(1).lt_eq(lit(-0.005))),
                    ),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4123")))
                    .and(col(close_slow_diff).shift(1).lt(lit(-0.02)))
                    .and(col(close_fast_diff).shift(1).lt_eq(lit(-0.01))),
            )
            .then(lit(1))
            .when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(trends_col).shift(1).eq(lit("4132")))
                    .and(
                        col(close_slow_diff)
                            .shift(1)
                            .gt_eq(lit(-0.01))
                            .and(col(close_slow_diff).shift(1).lt_eq(lit(0.0))),
                    )
                    .and(
                        col(close_fast_diff)
                            .shift(1)
                            .gt_eq(lit(-0.005))
                            .and(col(close_fast_diff).shift(1).lt_eq(lit(0.0))),
                    ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(signal_col),
        );
        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError> {
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
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
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
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError> {
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
