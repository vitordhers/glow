use super::super::errors::Error;
use super::{indicator::Indicator, performance::Performance};
use polars::prelude::*;

pub struct Strategy {
    pub name: String,
    pub indicators: Vec<Indicator>,
    pub performance: Option<Performance>,
    pub data: Option<LazyFrame>,
}

impl Strategy {
    pub fn new(name: String, indicators: Vec<Indicator>) -> Strategy {
        Strategy {
            name,
            indicators,
            performance: None,
            data: None,
        }
    }

    pub fn calculate_initial_positions(
        &mut self,
        tick_data: &LazyFrame,
        symbols: &[String; 2],
    ) -> Result<(), Error> {
        let lf = self.calculate_initial_indicators(&symbols[0], tick_data)?;
        self.data = Some(lf);
        Ok(())
    }

    fn calculate_initial_indicators(
        &self,
        anchor_symbol: &String,
        tick_data: &LazyFrame,
    ) -> Result<LazyFrame, Error> {
        let mut resampled_lfs: Vec<LazyFrame> = vec![tick_data.clone()];

        for indicator in &self.indicators {
            let resampled_window_data =
                indicator.get_resampled_window_data(anchor_symbol, tick_data)?;

            let resampled_data = indicator.set_columns_data(anchor_symbol, &resampled_window_data);
        }

        let non_forwarded_concatenated_lf = diag_concat_lf(resampled_lfs, true, true).unwrap();
        let df = non_forwarded_concatenated_lf
            .collect()?
            .fill_null(FillNullStrategy::Forward(None))?;
        println!("DATAFRAME \n {}", df);
        Ok(df.lazy())
    }

    #[allow(dead_code)]
    pub fn update_positions(&mut self, _tick_data: &LazyFrame) -> Result<(), Error> {
        Ok(())
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy {
            name: "Default Strategy".to_string(),
            indicators: vec![],
            performance: None,
            data: None,
        }
    }
}
