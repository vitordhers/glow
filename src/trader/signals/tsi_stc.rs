use crate::trader::{
    enums::signal_category::SignalCategory, errors::Error, traits::signal::Signal,
};
use polars::prelude::*;

#[derive(Clone)]
pub struct TSISTCShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub period: i64,
}

impl Signal for TSISTCShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let signal_col = signal_category.get_column();

        // let nulls_clause = null_condition_fold(self.period, &tsi_col, &stc_col);
        let suffix = format!("{}_{}", self.anchor_symbol, 5);

        let tsi_col = &format!("TSI_{}", suffix);
        let tsi_open_col_alias_prefix = &format!("{}_tsi", signal_col);
        let tsi_clauses =
            previous_condition_fold(self.period, &tsi_col, false, &tsi_open_col_alias_prefix);

        println!("@@@ TSI CLAUSES {:?}", tsi_clauses);

        signal_lf = signal_lf.with_columns(tsi_clauses);

        let mut all_tsi_clauses = sum_condition_fold(self.period, &tsi_open_col_alias_prefix);
        // all_tsi_clauses = all_tsi_clauses.and(col(tsi_col).gt(0));

        println!("@@@ ALL TSI CLAUSES {:?}", all_tsi_clauses);

        signal_lf = signal_lf.with_column(
            when(all_tsi_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("short_tsi"),
        );

        let stc_col = &format!("STC_{}", suffix);
        let stc_open_col_alias_prefix = &format!("{}_stc", signal_col);
        let stc_clauses =
            previous_condition_fold(self.period, &stc_col, false, stc_open_col_alias_prefix);

        println!("@@@ STC CLAUSES {:?}", stc_clauses);

        signal_lf = signal_lf.with_columns(stc_clauses);
        let all_stc_clauses = sum_condition_fold(self.period, stc_open_col_alias_prefix);

        println!("@@@ ALL STC CLAUSES {:?}", all_stc_clauses);

        signal_lf = signal_lf.with_column(
            when(all_stc_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("short_stc"),
        );

        signal_lf = signal_lf.with_column(
            when(col("short_tsi").and(col("short_stc")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias(signal_col),
        );

        signal_lf = signal_lf.select([
            col("start_time"),
            col("short_tsi"),
            col("short_stc"),
            col(signal_col),
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
pub struct TSISTCLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub period: i64,
}

impl Signal for TSISTCLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::GoLong
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let signal_col = signal_category.get_column();

        // let nulls_clause = null_condition_fold(self.period, &tsi_col, &stc_col);
        let suffix = format!("{}_{}", self.anchor_symbol, 5);

        let tsi_col = &format!("TSI_{}", suffix);
        let tsi_open_col_alias_prefix = &format!("{}_tsi", signal_col);
        let tsi_clauses =
            previous_condition_fold(self.period, &tsi_col, true, tsi_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(tsi_clauses);

        let mut all_tsi_clauses = sum_condition_fold(self.period, tsi_open_col_alias_prefix);
        // all_tsi_clauses = all_tsi_clauses.and(col(tsi_col).lt(0));

        signal_lf = signal_lf.with_column(
            when(all_tsi_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("long_tsi"),
        );

        let stc_col = &format!("STC_{}", suffix);
        let stc_open_col_alias_prefix = &format!("{}_stc", signal_col);
        let stc_clauses =
            previous_condition_fold(self.period, &stc_col, true, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(stc_clauses);
        let all_stc_clauses = sum_condition_fold(self.period, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_column(
            when(all_stc_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("long_stc"),
        );

        signal_lf = signal_lf.with_column(
            when(col("long_tsi").and(col("long_stc")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias(signal_col),
        );

        signal_lf = signal_lf.select([
            col("start_time"),
            col("long_tsi"),
            col("long_stc"),
            col(signal_col),
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
pub struct TSISTCCloseLongSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub period: i64,
}

impl Signal for TSISTCCloseLongSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseLong
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let signal_col = signal_category.get_column();

        // let nulls_clause = null_condition_fold(self.period, &tsi_col, &stc_col);
        let suffix = format!("{}_{}", self.anchor_symbol, 5);

        let tsi_col = &format!("TSI_{}", suffix);
        let tsi_open_col_alias_prefix = &format!("{}_tsi", signal_col);
        let tsi_clauses =
            previous_condition_fold(self.period, &tsi_col, false, tsi_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(tsi_clauses);

        let all_tsi_clauses = sum_condition_fold(self.period, tsi_open_col_alias_prefix);

        signal_lf = signal_lf.with_column(
            when(all_tsi_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("close_long_tsi"),
        );

        let stc_col = &format!("STC_{}", suffix);
        let stc_open_col_alias_prefix = &format!("{}_stc", signal_col);
        let stc_clauses =
            previous_condition_fold(self.period, &stc_col, false, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(stc_clauses);
        let all_stc_clauses = sum_condition_fold(self.period, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_column(
            when(all_stc_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("close_long_stc"),
        );

        signal_lf = signal_lf.with_column(
            when(col("close_long_tsi").and(col("close_long_stc")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias(signal_col),
        );

        signal_lf = signal_lf.select([
            col("start_time"),
            col("close_long_tsi"),
            col("close_long_stc"),
            col(signal_col),
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
pub struct TSISTCCloseShortSignal {
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub period: i64,
}

impl Signal for TSISTCCloseShortSignal {
    fn signal_category(&self) -> SignalCategory {
        SignalCategory::CloseShort
    }

    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut signal_lf = lf.clone();

        let signal_category = self.signal_category();
        let signal_col = signal_category.get_column();

        // let nulls_clause = null_condition_fold(self.period, &tsi_col, &stc_col);
        let suffix = format!("{}_{}", self.anchor_symbol, 5);

        let tsi_col = &format!("TSI_{}", suffix);
        let tsi_open_col_alias_prefix = &format!("{}_tsi", signal_col);
        let tsi_clauses =
            previous_condition_fold(self.period, &tsi_col, true, &tsi_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(tsi_clauses);

        let all_tsi_clauses = sum_condition_fold(self.period, tsi_open_col_alias_prefix);

        signal_lf = signal_lf.with_column(
            when(all_tsi_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("close_short_tsi"),
        );

        let stc_col = &format!("STC_{}", suffix);
        let stc_open_col_alias_prefix = &format!("{}_stc", signal_col);
        let stc_clauses =
            previous_condition_fold(self.period, &stc_col, true, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_columns(stc_clauses);
        let all_stc_clauses = sum_condition_fold(self.period, stc_open_col_alias_prefix);

        signal_lf = signal_lf.with_column(
            when(all_stc_clauses)
                .then(lit(true))
                .otherwise(lit(false))
                .alias("close_short_stc"),
        );

        signal_lf = signal_lf.with_column(
            when(col("close_short_tsi").and(col("close_short_stc")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias(signal_col),
        );

        signal_lf = signal_lf.select([
            col("start_time"),
            col("close_short_tsi"),
            col("close_short_stc"),
            col(signal_col),
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

// fn null_condition_fold(period: i64, tsi_col: &str, stc_col: &str) -> Expr {
//     let period_range = 1..=period;

//     period_range.into_iter().fold(
//         col(tsi_col).is_null().or(col(tsi_col).is_null()),
//         |prev, curr| {
//             prev.and(
//                 col(tsi_col)
//                     .shift(curr)
//                     .is_null()
//                     .or(col(tsi_col).shift(curr).is_null()),
//             )
//         },
//     )
// }

fn previous_condition_fold(
    period: i64,
    column: &str,
    bigger: bool,
    alias_prefix: &str,
) -> Vec<Expr> {
    let period_range = 1..=period;

    period_range
        .into_iter()
        .map(|curr| {
            let col_alias = &format!("{}_{}", alias_prefix, curr);
            if bigger {
                when(col(column).is_null().or(col(column).shift(curr).is_null()))
                    .then(lit(false))
                    .otherwise(col(column).gt(col(column).shift(curr)))
                    .alias(col_alias)
            } else {
                when(col(column).is_null().or(col(column).shift(curr).is_null()))
                    .then(lit(false))
                    .otherwise(col(column).lt(col(column).shift(curr)))
                    .alias(col_alias)
            }
        })
        .collect()
}

fn sum_condition_fold(period: i64, alias_prefix: &str) -> Expr {
    let period_range = 1..=period;

    period_range.into_iter().fold(lit(true), |prev, curr| {
        let col_alias = &format!("{}_{}", alias_prefix, curr);

        prev.and(col(col_alias))
    })
}
