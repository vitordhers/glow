use polars::prelude::*;

use crate::trader::{
    enums::signal_category::SignalCategory, errors::Error, traits::signal::Signal,
};

#[derive(Clone)]
pub struct MultipleStochasticWithThresholdShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
}

impl Signal for MultipleStochasticWithThresholdShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let column = signal_category.get_column();

        let suffix = format!("{}_{}", self.anchor_symbol, 5);
        let stochastic_k_col = &format!("K%_{}", suffix);
        // let stochastic_d_col = &format!("D%_{}", suffix);
        let stc_col = &format!("STC_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).gt(lit(75)).alias("open_short_1"),
                col(stochastic_k_col)
                    .lt(col(stochastic_k_col).shift(1))
                    .alias("open_short_2"),
                col(stc_col).lt(col(stc_col).shift(1)).alias("open_short_3"),
            ])
            .with_column(
                when(
                    col("open_short_1")
                        .and(col("open_short_2"))
                        .and(col("open_short_3")),
                )
                .then(1)
                .otherwise(0)
                .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col(column),
            col("open_short_1"),
            col("open_short_2"),
            col("open_short_3"),
        ]);

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
pub struct MultipleStochasticWithThresholdLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
}

impl Signal for MultipleStochasticWithThresholdLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoLong
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let column = signal_category.get_column();

        let suffix = format!("{}_{}", self.anchor_symbol, 5);
        let stochastic_k_col = &format!("K%_{}", suffix);
        let stc_col = &format!("STC_{}", suffix);
        // let stochastic_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).lt(25).alias("open_long_1"),
                col(stochastic_k_col)
                    .gt(col(stochastic_k_col).shift(1))
                    .alias("open_long_2"),
                col(stc_col).gt(col(stc_col).shift(1)).alias("open_long_3"),
            ])
            .with_column(
                when(
                    col("open_long_1")
                        .and(col("open_long_2"))
                        .and("open_long_3"),
                )
                .then(1)
                .otherwise(0)
                .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col(column),
            col("open_long_1"),
            col("open_long_2"),
            col("open_long_3"),
        ]);

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
pub struct MultipleStochasticWithThresholdCloseShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub close_window_index: usize,
}

impl Signal for MultipleStochasticWithThresholdCloseShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let column = signal_category.get_column();

        let suffix = format!("{}_{}", self.anchor_symbol, 5);
        let stochastic_k_col = &format!("K%_{}", suffix);
        // let stochastic_d_col = &format!("D%_{}", suffix);
        let stc_col = &format!("STC_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).lt(25).alias("close_short_1"),
                col(stochastic_k_col)
                    .gt(col(stochastic_k_col).shift(1))
                    .alias("close_short_2"),
                col(stc_col)
                    .gt(col(stc_col).shift(1))
                    .alias("close_short_3"),
            ])
            .with_column(
                when(
                    col("close_short_1")
                        .and(col("close_short_2"))
                        .and("close_short_3"),
                )
                .then(1)
                .otherwise(0)
                .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col("close_short_1"),
            col("close_short_2"),
            col("close_short_3"),
            col(column),
        ]);

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
pub struct MultipleStochasticWithThresholdCloseLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub close_window_index: usize,
}

impl Signal for MultipleStochasticWithThresholdCloseLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseLong
    }
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let column = signal_category.get_column();

        let suffix = format!("{}_{}", self.anchor_symbol, 5);
        let stochastic_k_col = &format!("K%_{}", suffix);
        // let stochastic_d_col = &format!("D%_{}", suffix);
        let stc_col = &format!("STC_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).gt(lit(75)).alias("close_long_1"),
                col(stochastic_k_col)
                    .lt(col(stochastic_k_col).shift(1))
                    .alias("close_long_2"),
                col(stc_col).lt(col(stc_col).shift(1)).alias("close_long_3"),
            ])
            .with_column(
                when(
                    col("close_long_1")
                        .and(col("close_long_2"))
                        .and(col("close_long_3")),
                )
                .then(1)
                .otherwise(0)
                .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col("close_long_1"),
            col("close_long_2"),
            col("close_long_3"),
            col(column),
        ]);

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
