use crate::trader::enums::signal_category::SignalCategory;

use super::super::errors::Error;
use polars::prelude::*;

pub trait Signal {
    fn signal_category(&self) -> SignalCategory;
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error>;
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error>;
    fn clone_box(&self) -> Box<dyn Signal + Send + Sync>;
}

impl Clone for Box<dyn Signal + Send + Sync> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
