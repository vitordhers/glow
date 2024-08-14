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

        // let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(cloud_top_col).shift(2).eq(col(close_col).shift(2)))
                    .and(
                        col("trend").eq(lit("strong_bull")), // .or(col("trend_status").eq(lit("residual_bear"))),
                    ),
            )
            .then(lit(1))
            .otherwise(lit(0))
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

        // let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(cloud_base_col).shift(2).eq(col(close_col).shift(2)))
                    .and(
                        col("trend").eq(lit("strong_bear")), // .or(col("trend_status").eq(lit("residual_bull"))),
                    ),
            )
            .then(lit(1))
            .otherwise(lit(0))
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
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        // let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_top_col)
                    .neq(col(close_col))
                    .and(col(cloud_top_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(cloud_top_col).shift(2).eq(col(close_col).shift(2))), // .and(
                                                                                   //     col("trend_status")
                                                                                   //         .eq(lit("consistent_bear"))
                                                                                   //         .or(col("trend_status").eq(lit("reversal_bear")))
                                                                                   //         .or(col("trend_status").eq(lit("residual_bear"))),
                                                                                   // ),
            )
            .then(lit(1))
            .otherwise(lit(0))
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
        let select_columns = vec![col("start_time"), col(signal_col)];

        let close_col = &get_symbol_close_col(&self.anchor_symbol);

        // let cloud_top_col = &format!("{}_{}_cloud_top", &self.anchor_symbol, &self.cloud_length);
        let cloud_base_col = &format!("{}_{}_cloud_base", &self.anchor_symbol, &self.cloud_length);

        signal_lf = signal_lf.with_column(
            when(
                col(cloud_base_col)
                    .neq(col(close_col))
                    .and(col(cloud_base_col).shift(1).eq(col(close_col).shift(1)))
                    .and(col(cloud_base_col).shift(2).eq(col(close_col).shift(2))), // .and(
                                                                                    //     col("trend_status")
                                                                                    //         .eq(lit("consistent_bull"))
                                                                                    //         .or(col("trend_status").eq(lit("reversal_bull")))
                                                                                    //         .or(col("trend_status").eq(lit("residual_bull"))),
                                                                                    // ),
            )
            .then(lit(1))
            .otherwise(lit(0))
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
