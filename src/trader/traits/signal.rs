use crate::trader::enums::signal_category::SignalCategory;

use super::super::errors::Error;
use polars::prelude::*;

pub trait Signal {
    fn signal_category(&self) -> SignalCategory;
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error>;
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error>;
}
