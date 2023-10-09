use polars::prelude::*;

use crate::trader::{errors::Error, traits::indicator::Indicator};

#[derive(Clone)]
pub struct StochasticThresholdIndicator {
    pub name: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub trend_col: String,
}

impl Indicator for StochasticThresholdIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();
        let long_threshold_col_title = "long_threshold".to_string();
        let long_threshold_col_dtype = DataType::Int32;

        let short_threshold_col_title = "short_threshold".to_string();
        let short_threshold_col_dtype = DataType::Int32;

        columns_names.push((long_threshold_col_title, long_threshold_col_dtype));
        columns_names.push((short_threshold_col_title, short_threshold_col_dtype));
        columns_names
    }
    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let trend_col = &self.trend_col;
        let long_threshold_col = "long_threshold";
        let short_threshold_col = "short_threshold";
        let upper_threshold = self.upper_threshold;
        let lower_threshold = self.lower_threshold;
        let lf = lf.with_columns([
            when(col(trend_col).eq(1))
                .then(lit(upper_threshold))
                .otherwise(lit(lower_threshold))
                .alias(&short_threshold_col),
            when(col(trend_col).eq(1))
                .then(lit(lower_threshold))
                .otherwise(lit(upper_threshold))
                .alias(long_threshold_col),
        ]);
        let lf = lf.select([
            col("start_time"),
            col(long_threshold_col),
            col(short_threshold_col),
        ]);
        Ok(lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = df.clone().lazy();
        new_lf = self.set_indicator_columns(new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = df.clone();

        for (column, _) in self.get_indicator_columns() {
            let series = new_df.column(&column)?;
            let _ = result_df.replace(&column, series.to_owned());
        }

        Ok(result_df)
    }

    fn get_data_offset(&self) -> u32 {
        0
    }
}
