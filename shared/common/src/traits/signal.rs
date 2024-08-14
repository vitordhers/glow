use crate::enums::signal_category::SignalCategory;
use glow_error::GlowError;
use polars::prelude::*;

pub trait Signal {
    fn signal_category(&self) -> SignalCategory;
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError>;
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError>;
}
