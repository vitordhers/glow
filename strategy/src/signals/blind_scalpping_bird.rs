use super::{enums::signal_category::SignalCategory, errors::Error, traits::signal::Signal};
use polars::prelude::*;

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
        let stochastic_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).gt(75).alias("open_short_1"),
                col(stochastic_k_col)
                    .lt(col(stochastic_k_col).shift(1))
                    .alias("open_short_3"),
            ])
            .with_column(
                when(col("open_short_1").and(col("open_short_3")))
                    .then(1)
                    .otherwise(0)
                    .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col(column),
            col("open_short_1"),
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

    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        Box::new(Self {
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
        })
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
        let stochastic_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).lt(25).alias("open_long_1"),
                col(stochastic_k_col)
                    .gt(col(stochastic_k_col).shift(1))
                    .alias("open_long_3"),
            ])
            .with_column(
                when(col("open_long_1").and(col("open_long_3")))
                    .then(1)
                    .otherwise(0)
                    .alias(column),
            );
        signal_lf = signal_lf.select([
            col("start_time"),
            col(column),
            col("open_long_1"),
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

    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        Box::new(Self {
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
        })
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
        let stochastic_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf.with_column(
            when(
                col(stochastic_k_col)
                    .lt(25)
                    .and(col(stochastic_k_col).lt_eq(col(stochastic_d_col)))
                    .and(col(stochastic_k_col).gt(col(stochastic_k_col).shift(1))),
            )
            .then(1)
            .otherwise(0)
            .alias(column),
        );
        signal_lf = signal_lf.select([col("start_time"), col(column)]);

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

    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        Box::new(Self {
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
            close_window_index: self.close_window_index.clone(),
        })
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
        let stochastic_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_columns([
                col(stochastic_k_col).gt(75).alias("close_long_1"),
                col(stochastic_k_col)
                    .gt_eq(col(stochastic_d_col))
                    .alias("close_long_2"),
                col(stochastic_k_col)
                    .lt(col(stochastic_k_col).shift(1))
                    .alias("close_long_3"),
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

    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        Box::new(Self {
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
            close_window_index: self.close_window_index.clone(),
        })
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
    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        match self {
            Self::MultipleStochasticWithThresholdShortSignal(
                multiple_stochastic_with_threshold_short_signal,
            ) => multiple_stochastic_with_threshold_short_signal.clone_box(),
            Self::MultipleStochasticWithThresholdCloseShortSignal(
                multiple_stochastic_with_threshold_short_close_signal,
            ) => multiple_stochastic_with_threshold_short_close_signal.clone_box(),
            Self::MultipleStochasticWithThresholdLongSignal(
                multiple_stochastic_with_threshold_long_signal,
            ) => multiple_stochastic_with_threshold_long_signal.clone_box(),
            Self::MultipleStochasticWithThresholdCloseLongSignal(
                multiple_stochastic_with_threshold_long_close_signal,
            ) => multiple_stochastic_with_threshold_long_close_signal.clone_box(),
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
