use polars_lazy::prelude::LazyFrame;

use crate::trader::{enums::signal_category::SignalCategory, errors::Error};

pub trait OptimizableSignal {
    fn signal_category(&self) -> SignalCategory;
    fn set_combined_signal_columns(&self, lf: &LazyFrame) -> Result<LazyFrame, Error>;
}
