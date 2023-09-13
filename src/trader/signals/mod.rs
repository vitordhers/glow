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
        let mut col_exprs: Vec<Expr> = vec![];
        let mut iter = self.windows.iter().enumerate();

        let short_threshold_col = "short_threshold";

        while let Some((index, window)) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let windowed_threshold_col = format!("short_{}", index);

            col_exprs.push(col(&windowed_threshold_col).eq(lit(1)));

            signal_lf = signal_lf.with_column(
                when(
                    (col(&stochastic_k_col).gt(col(short_threshold_col)))
                        .and(col(&stochastic_d_col).gt(col(&stochastic_k_col))),
                )
                .then(1)
                .otherwise(0)
                .alias(&windowed_threshold_col),
            );
        }

        let and_cols_expr: Expr = col_exprs
            .iter()
            .fold(None, |prev: Option<Expr>, curr: &Expr| {
                if let Some(e) = prev {
                    return Some(e.and(curr.clone()));
                }
                Some(curr.clone())
            })
            .unwrap();
        signal_lf = signal_lf.with_column(when(and_cols_expr).then(1).otherwise(0).alias("short"));
        signal_lf = signal_lf.select([col("start_time"), col("short")]);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut stochastic_clauses = vec![];
        let last_index = data.height() - 1;

        let mut iter = self.windows.iter();
        while let Some(window) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);
            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let k_column_ca = data
                .column(&stochastic_k_col)?
                .f64()?
                .into_iter()
                .collect::<Vec<_>>();
            let d_column_ca = data
                .column(&stochastic_d_col)?
                .f64()?
                .into_iter()
                .collect::<Vec<_>>();
            let short_threshold_ca = data
                .column("short_threshold")?
                .i32()?
                .into_iter()
                .collect::<Vec<_>>();

            let k_percent = k_column_ca[last_index].unwrap();
            let d_percent = d_column_ca[last_index].unwrap();
            let short_threshold = short_threshold_ca[last_index].unwrap() as f64;

            let clause = if k_percent > short_threshold && d_percent > k_percent {
                1
            } else {
                0
            };
            stochastic_clauses.push(clause);
        }
        let all_clauses_true = stochastic_clauses.iter().all(|&clause| clause == 1);
        let short_signal = if all_clauses_true { 1 } else { 0 };
        let signal_cat = self.signal_category();
        let signal_col = signal_cat.get_column();
        let mut shorts = data
            .column(signal_col)?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        shorts[last_index] = Some(short_signal);
        let mut df = data.clone();
        df.replace(signal_col, Series::new(signal_col, shorts))?;

        Ok(df)
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

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);

        let close_k_col = &format!("K%_{}", suffix);
        let close_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_column(
                when(
                    col(close_k_col)
                        .gt(col(close_d_col))
                        .and(col(close_k_col).shift(1).lt(col(close_d_col).shift(1))),
                )
                .then(1)
                .otherwise(0)
                .alias("short_close"),
            )
            .select([col("start_time"), col("short_close")]);

        // col(&short_close_k_col)
        //             .gt(lit(self.lower_threshold)) // lit(self.lower_threshold) ? col("short_threshold") TODO: REVIEW this
        //             .and(col(&short_close_k_col).gt(col(&short_close_d_col)))
        //             .and(
        //                 col(&short_close_k_col)
        //                     .shift(1)
        //                     .lt(col(&short_close_d_col).shift(1)),
        //             ),

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let last_index = data.height() - 1;
        let penultimate_index = if last_index == 0 { 0 } else { last_index - 1 };

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);
        let stochastic_k_col = format!("K%_{}", suffix);
        let stochastic_d_col = format!("D%_{}", suffix);
        let short_close_k_column_ca = data
            .column(&stochastic_k_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>();
        let short_close_d_column_ca = data
            .column(&stochastic_d_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>();
        // let short_threshold_ca = data
        //     .column("short_threshold")?
        //     .i32()?
        //     .into_iter()
        //     .collect::<Vec<_>>();
        let k_percent = short_close_k_column_ca[last_index].unwrap();
        let k_percent_shifted = short_close_k_column_ca[penultimate_index].unwrap();
        let d_percent = short_close_d_column_ca[last_index].unwrap();
        let d_percent_shifted = short_close_d_column_ca[penultimate_index].unwrap();
        // let short_threshold = short_threshold_ca[last_index].unwrap() as f64;

        let close_clause = if k_percent > d_percent && k_percent_shifted < d_percent_shifted {
            true
        } else {
            false
        };

        let short_close_signal = if close_clause { 1 } else { 0 };
        let signal_cat = self.signal_category();
        let signal_col = signal_cat.get_column();
        let mut shorts_closes = data
            .column(signal_col)?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        shorts_closes[last_index] = Some(short_close_signal);
        let mut df = data.clone();
        df.replace(signal_col, Series::new(signal_col, shorts_closes))?;

        Ok(df)
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
        let mut col_exprs: Vec<Expr> = vec![];
        let mut iter = self.windows.iter().enumerate();
        let select_columns = vec![col("start_time"), col("long")];
        let long_threshold_col = "long_threshold";
        while let Some((index, window)) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let windowed_threshold_col = format!("long_{}", index);
            col_exprs.push(col(&windowed_threshold_col).eq(lit(1)));

            signal_lf = signal_lf.with_column(
                when(
                    (col(&stochastic_k_col).lt(col(long_threshold_col)))
                        .and(col(&stochastic_d_col).lt(col(&stochastic_k_col))),
                )
                .then(1)
                .otherwise(0)
                .alias(&windowed_threshold_col),
            );
        }

        let and_cols_expr: Expr = col_exprs
            .iter()
            .fold(None, |prev: Option<Expr>, curr: &Expr| {
                if let Some(e) = prev {
                    return Some(e.and(curr.clone()));
                }
                Some(curr.clone())
            })
            .unwrap();
        signal_lf = signal_lf.with_column(when(and_cols_expr).then(1).otherwise(0).alias("long"));
        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let mut stochastic_clauses = vec![];
        let last_index = data.height() - 1;

        let mut iter = self.windows.iter();
        while let Some(window) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);
            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let k_column_ca = data
                .column(&stochastic_k_col)?
                .f64()?
                .into_iter()
                .collect::<Vec<_>>();
            let d_column_ca = data
                .column(&stochastic_d_col)?
                .f64()?
                .into_iter()
                .collect::<Vec<_>>();
            let long_threshold_ca = data
                .column("long_threshold")?
                .i32()?
                .into_iter()
                .collect::<Vec<_>>();
            let k_percent = k_column_ca[last_index].unwrap();
            let d_percent = d_column_ca[last_index].unwrap();
            let long_threshold = long_threshold_ca[last_index].unwrap() as f64;

            let clause = if k_percent < long_threshold && d_percent < k_percent {
                1
            } else {
                0
            };
            stochastic_clauses.push(clause);
        }
        let all_clauses_true = stochastic_clauses.iter().all(|&clause| clause == 1);
        let long_signal = if all_clauses_true { 1 } else { 0 };
        let signal_cat = self.signal_category();
        let signal_col = signal_cat.get_column();
        let mut longs = data
            .column(signal_col)?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        longs[last_index] = Some(long_signal);
        let mut df = data.clone();
        df.replace(signal_col, Series::new(signal_col, longs))?;

        Ok(df)
    }

    fn clone_box(&self) -> Box<dyn Signal + Send + Sync> {
        Box::new(Self {
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
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

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);

        let close_k_col = &format!("K%_{}", suffix);
        let close_d_col = &format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_column(
                when(
                    col(close_k_col)
                        .lt(col(close_d_col))
                        .and(col(close_k_col).shift(1).gt(col(close_d_col).shift(1))),
                )
                .then(1)
                .otherwise(0)
                .alias("long_close"),
            )
            .select([col("start_time"), col("long_close")]);

        // col(&long_close_k_col)
        // .lt(lit(self.upper_threshold)) // lit(self.upper_threshold) ? col("long_threshold")
        // .and(col(&long_close_k_col).lt(col(&long_close_d_col)))
        // .and(
        //     col(&long_close_k_col)
        //         .shift(1)
        //         .gt(col(&long_close_d_col).shift(1)),
        // ),

        Ok(signal_lf)
    }

    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        let last_index = data.height() - 1;
        let penultimate_index = if last_index == 0 { 0 } else { last_index - 1 };

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);
        let stochastic_k_col = format!("K%_{}", suffix);
        let stochastic_d_col = format!("D%_{}", suffix);
        let long_close_k_column_ca = data
            .column(&stochastic_k_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>();
        let long_close_d_column_ca = data
            .column(&stochastic_d_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>();
        // let long_threshold_ca = data
        //     .column("long_threshold")?
        //     .i32()?
        //     .into_iter()
        //     .collect::<Vec<_>>();
        let k_percent = long_close_k_column_ca[last_index].unwrap();
        let k_percent_shifted = long_close_k_column_ca[penultimate_index].unwrap();
        let d_percent = long_close_d_column_ca[last_index].unwrap();
        let d_percent_shifted = long_close_d_column_ca[penultimate_index].unwrap();
        // let long_threshold = long_threshold_ca[last_index].unwrap() as f64;

        let close_clause = if k_percent < d_percent && k_percent_shifted > d_percent_shifted {
            true
        } else {
            false
        };

        let long_close_signal = if close_clause { 1 } else { 0 };
        let signal_cat = self.signal_category();
        let signal_col = signal_cat.get_column();
        let mut longs_closes = data
            .column(signal_col)?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        longs_closes[last_index] = Some(long_close_signal);
        let mut df = data.clone();
        df.replace(signal_col, Series::new(signal_col, longs_closes))?;

        Ok(df)
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
