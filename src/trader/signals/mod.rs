use super::errors::Error;
use super::models::signal::{SignalCategory, Signer};
use polars::prelude::*;

pub struct MultipleStochasticWithThresholdShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
}

impl Signer for MultipleStochasticWithThresholdShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoShort
    }
    fn compute_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();
        let mut col_exprs: Vec<Expr> = vec![];
        let mut iter = self.windows.iter().enumerate();
        while let Some((index, window)) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let short_threshold_col = "short_threshold";
            let windowed_threshold_col = format!("short_{}", index);
            col_exprs.push(col(&windowed_threshold_col));

            signal_lf = signal_lf.with_column(
                when(
                    (col(&stochastic_k_col).gt(col(short_threshold_col)))
                        .and(col(&stochastic_d_col).gt(col(&stochastic_k_col))),
                )
                .then(true)
                .otherwise(false)
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
        signal_lf = signal_lf.with_column(
            when(and_cols_expr)
                .then(true)
                .otherwise(false)
                .alias("short"),
        );

        signal_lf = signal_lf.select([col("start_time"), col("short")]);

        Ok(signal_lf)
    }

    fn clone_box(&self) -> Box<dyn Signer + Send + Sync> {
        Box::new(Self {
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
        })
    }
}

pub struct MultipleStochasticWithThresholdLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
}

impl Signer for MultipleStochasticWithThresholdLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoLong
    }
    fn compute_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();
        let mut col_exprs: Vec<Expr> = vec![];
        let mut iter = self.windows.iter().enumerate();
        let select_columns = vec![col("start_time"), col("long")];
        while let Some((index, window)) = iter.next() {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let stochastic_k_col = format!("K%_{}", suffix);
            let stochastic_d_col = format!("D%_{}", suffix);
            let long_threshold_col = "long_threshold";
            let windowed_threshold_col = format!("long_{}", index);
            col_exprs.push(col(&windowed_threshold_col));

            signal_lf = signal_lf.with_column(
                when(
                    (col(&stochastic_k_col).lt(col(long_threshold_col)))
                        .and(col(&stochastic_d_col).lt(col(&stochastic_k_col))),
                )
                .then(true)
                .otherwise(false)
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

        signal_lf = signal_lf.with_column(
            when(and_cols_expr)
                .then(true)
                .otherwise(false)
                .alias("long"),
        );

        signal_lf = signal_lf.select(select_columns);

        Ok(signal_lf)
    }

    fn clone_box(&self) -> Box<dyn Signer + Send + Sync> {
        Box::new(Self {
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
        })
    }
}

pub struct MultipleStochasticWithThresholdCloseLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub close_window_index: usize,
}

impl Signer for MultipleStochasticWithThresholdCloseLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseLong
    }
    fn compute_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);

        let long_close_k_col = format!("K%_{}", suffix);
        let long_close_d_col = format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_column(
                when(
                    col(&long_close_k_col)
                        .lt(lit(self.upper_threshold)) // lit(self.upper_threshold) ? col("long_threshold")
                        .and(col(&long_close_k_col).lt(col(&long_close_d_col)))
                        .and(
                            col(&long_close_k_col)
                                .shift(1)
                                .gt(col(&long_close_d_col).shift(1)),
                        ),
                )
                .then(true)
                .otherwise(false)
                .alias("long_close"),
            )
            .select([col("start_time"), col("long_close")]);
        Ok(signal_lf)
    }

    fn clone_box(&self) -> Box<dyn Signer + Send + Sync> {
        Box::new(Self {
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
            close_window_index: self.close_window_index.clone(),
        })
    }
}

pub struct MultipleStochasticWithThresholdCloseShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub close_window_index: usize,
}

impl Signer for MultipleStochasticWithThresholdCloseShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseShort
    }
    fn compute_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let closing_window = self.windows[self.close_window_index];
        let suffix = format!("{}_{}", self.anchor_symbol, closing_window);

        let short_close_k_col = format!("K%_{}", suffix);
        let short_close_d_col = format!("D%_{}", suffix);

        signal_lf = signal_lf
            .with_column(
                when(
                    col(&short_close_k_col)
                        .gt(lit(self.lower_threshold)) // lit(self.lower_threshold) ? col("short_threshold")
                        .and(col(&short_close_k_col).gt(col(&short_close_d_col)))
                        .and(
                            col(&short_close_k_col)
                                .shift(1)
                                .lt(col(&short_close_d_col).shift(1)),
                        ),
                )
                .then(true)
                .otherwise(false)
                .alias("short_close"),
            )
            .select([col("start_time"), col("short_close")]);
        Ok(signal_lf)
    }

    fn clone_box(&self) -> Box<dyn Signer + Send + Sync> {
        Box::new(Self {
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            windows: self.windows.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
            close_window_index: self.close_window_index.clone(),
        })
    }
}
