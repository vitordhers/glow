use crate::{enums::signal_category::SignalCategory, structs::SymbolsPair};
use glow_error::GlowError;
use polars::prelude::*;

// TODO: deprecate this
pub trait Signal: Sized {
    type Wrapper;
    fn signal_category(&self) -> SignalCategory;
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError>;
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, GlowError>;
    fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) -> Result<Self::Wrapper, GlowError>;
}
