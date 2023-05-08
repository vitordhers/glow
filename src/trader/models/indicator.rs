use crate::trader::{
    constants::{NANOS_IN_SECOND, SECONDS_IN_MIN},
    errors::Error,
    functions::{get_symbol_ohlc_cols, get_symbol_window_ohlc_cols},
};
use polars::prelude::*;
use std::collections::HashMap;

pub struct Indicator {
    pub name: String,
    pub window_in_mins: u32,
    pub signal: Vec<IndicatorType>,
    pub column_generator: HashMap<
        String,
        Box<dyn Fn(&Self, &String, &String, &LazyFrame) -> Result<LazyFrame, Error>>,
    >, // name -> generator expression
}
#[allow(dead_code)]
pub enum IndicatorType {
    GoLong,
    GoShort,
    RevertLong,
    RevertShort,
    CloseLong,
    CloseShort,
    ClosePosition,
    RevertPosition,
}

impl Indicator {
    pub fn new(
        name: String,
        window_in_mins: u32,
        signal: Vec<IndicatorType>,
        column_generator: HashMap<
            String,
            Box<dyn Fn(&Self, &String, &String, &LazyFrame) -> Result<LazyFrame, Error>>,
        >,
    ) -> Self {
        Self {
            name,
            window_in_mins,
            signal,
            column_generator,
        }
    }

    pub fn get_resampled_window_data(
        &self,
        anchor_symbol: &String,
        tick_data: &LazyFrame,
    ) -> Result<LazyFrame, Error> {
        // first, calculate windows necessary to feed indicators
        let mut agg_expressions: Vec<Expr> = Vec::new();
        // fetch col names
        let (open_col, high_col, close_col, low_col) = get_symbol_ohlc_cols(anchor_symbol);
        // then aliases
        let (open_col_alias, high_col_alias, close_col_alias, low_col_alias) =
            get_symbol_window_ohlc_cols(anchor_symbol, &self.window_in_mins.to_string());
        // create expressions
        let open = col(&open_col).drop_nulls().first().alias(&open_col_alias);
        let high = col(&high_col).max().alias(&high_col_alias);
        let close = col(&close_col).drop_nulls().last().alias(&close_col_alias);
        let low = col(&low_col).min().alias(&low_col_alias);
        //append them
        agg_expressions.push(open);
        agg_expressions.push(high);
        agg_expressions.push(close);
        agg_expressions.push(low);

        let window_in_nanos = self.window_in_mins as i64 * SECONDS_IN_MIN * NANOS_IN_SECOND;

        let resampled_data = tick_data
            .clone()
            .sort(
                "start_time",
                SortOptions {
                    descending: false,
                    nulls_last: false,
                    multithreaded: true,
                },
            )
            .groupby_dynamic(
                vec![],
                DynamicGroupOptions {
                    start_by: StartBy::DataPoint,
                    index_column: "start_time".into(),
                    every: Duration::new(window_in_nanos),
                    period: Duration::new(window_in_nanos),
                    offset: Duration::new(0),
                    truncate: true,
                    include_boundaries: false,
                    closed_window: ClosedWindow::Left,
                },
            )
            .agg(agg_expressions);
        Ok(resampled_data)
    }

    pub fn set_columns_data(
        &self,
        anchor_symbol: &String,
        resampled_tick_data: &LazyFrame,
    ) -> Result<LazyFrame, Error> {
        let mut resampled_lfs = vec![resampled_tick_data.clone()];
        for (column, function) in &self.column_generator {
            let resampled_lf = function(&self, column, anchor_symbol, resampled_tick_data)?;
            // is this really necessary? Just append k and d results from function to resampled_tick_data (viable?)
            let resampled_lf = resample_lf_to_mins(&resampled_lf, 1)?;
            println!(
                "NEWLY RESAMPLED DF {:?}",
                resampled_lf.clone().sort(["start_time"], false)
            );
            resampled_lfs.push(resampled_lf.lazy());
        }
        let resampled_tick_data = diag_concat_lf(resampled_lfs, true, true)?
            .sort(
                "start_time",
                SortOptions {
                    descending: false,
                    nulls_last: false,
                    multithreaded: true,
                },
            )
            .groupby_dynamic(
                vec![],
                DynamicGroupOptions {
                    start_by: StartBy::DataPoint,
                    index_column: "start_time".into(),
                    every: Duration::new(NANOS_IN_SECOND * SECONDS_IN_MIN),
                    period: Duration::new(NANOS_IN_SECOND * SECONDS_IN_MIN),
                    offset: Duration::new(0),
                    truncate: true,
                    include_boundaries: false,
                    closed_window: ClosedWindow::Right,
                },
            )
            .agg([col("*")]);
        println!("set columns data 4");

        println!(
            "PRE RETURN SET COLUMNS DATA {:?}",
            resampled_tick_data.clone().collect()?
        );

        Ok(resampled_tick_data)
    }
}

pub fn resample_lf_to_mins(lf: &LazyFrame, mins: i64) -> Result<DataFrame, Error> {
    println!(
        "COLUMNS NAMES {:?}",
        lf.clone().collect()?.get_column_names()
    );
    let period = mins * NANOS_IN_SECOND * SECONDS_IN_MIN;
    Ok(lf
        .clone()
        .collect()?
        .upsample(
            vec![] as Vec<&str>,
            "start_time",
            Duration::new(period),
            Duration::new(0), // "-2m" yields 1440 records
        )?
        .fill_null(FillNullStrategy::Forward(None))?)
    // let gb = lf.clone()
    //     // .sort(
    //     //     "date",
    //     //     SortOptions {
    //     //         descending: false,
    //     //         nulls_last: false,
    //     //         multithreaded: true,
    //     //     },
    //     // )
    //     .groupby_dynamic(
    //         vec![col("start_time")],
    //         DynamicGroupOptions {
    //             start_by: StartBy::DataPoint,
    //             index_column: "start_time".into(),
    //             every: Duration::parse("1m"),
    //             period: Duration::parse("1m"),
    //             offset: Duration::parse("0s"),
    //             truncate: true,
    //             include_boundaries: false,
    //             closed_window: ClosedWindow::Right,
    //         },
    //     );

    //     Ok(gb.agg([col("*")]))
}
