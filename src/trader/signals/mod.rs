use super::{enums::signal_category::SignalCategory, errors::Error, traits::signal::Signal};
use polars::prelude::*;
pub mod multiple_stochastic_with_threshold;
pub mod tsi_stc;
use tsi_stc::{
    TSISTCCloseLongSignal, TSISTCCloseShortSignal, TSISTCLongSignal, TSISTCShortSignal,
};

#[derive(Clone)]
pub enum SignalWrapper {
    // MultipleStochasticWithThresholdShortSignal(MultipleStochasticWithThresholdShortSignal),
    // MultipleStochasticWithThresholdCloseShortSignal(
    //     MultipleStochasticWithThresholdCloseShortSignal,
    // ),
    // MultipleStochasticWithThresholdLongSignal(MultipleStochasticWithThresholdLongSignal),
    // MultipleStochasticWithThresholdCloseLongSignal(MultipleStochasticWithThresholdCloseLongSignal),
    TSISTCShortSignal(TSISTCShortSignal),
    TSISTCLongSignal(TSISTCLongSignal),
    TSISTCCloseShortSignal(TSISTCCloseShortSignal),
    TSISTCCloseLongSignal(TSISTCCloseLongSignal),
}

impl Signal for SignalWrapper {
    fn signal_category(&self) -> SignalCategory {
        match self {
            // Self::MultipleStochasticWithThresholdShortSignal(
            //     multiple_stochastic_with_threshold_short_signal,
            // ) => multiple_stochastic_with_threshold_short_signal.signal_category(),
            // Self::MultipleStochasticWithThresholdCloseShortSignal(
            //     multiple_stochastic_with_threshold_short_close_signal,
            // ) => multiple_stochastic_with_threshold_short_close_signal.signal_category(),
            // Self::MultipleStochasticWithThresholdLongSignal(
            //     multiple_stochastic_with_threshold_long_signal,
            // ) => multiple_stochastic_with_threshold_long_signal.signal_category(),
            // Self::MultipleStochasticWithThresholdCloseLongSignal(
            //     multiple_stochastic_with_threshold_long_close_signal,
            // ) => multiple_stochastic_with_threshold_long_close_signal.signal_category(),
            Self::TSISTCShortSignal(signal) => signal.signal_category(),
            Self::TSISTCLongSignal(signal) => signal.signal_category(),
            Self::TSISTCCloseShortSignal(signal) => signal.signal_category(),
            Self::TSISTCCloseLongSignal(signal) => signal.signal_category(),
        }
    }
    fn set_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error> {
        match self {
            // Self::MultipleStochasticWithThresholdShortSignal(
            //     multiple_stochastic_with_threshold_short_signal,
            // ) => multiple_stochastic_with_threshold_short_signal.set_signal_column(lf),
            // Self::MultipleStochasticWithThresholdCloseShortSignal(
            //     multiple_stochastic_with_threshold_short_close_signal,
            // ) => multiple_stochastic_with_threshold_short_close_signal.set_signal_column(lf),
            // Self::MultipleStochasticWithThresholdLongSignal(
            //     multiple_stochastic_with_threshold_long_signal,
            // ) => multiple_stochastic_with_threshold_long_signal.set_signal_column(lf),
            // Self::MultipleStochasticWithThresholdCloseLongSignal(
            //     multiple_stochastic_with_threshold_long_close_signal,
            // ) => multiple_stochastic_with_threshold_long_close_signal.set_signal_column(lf),
            Self::TSISTCShortSignal(signal) => signal.set_signal_column(lf),
            Self::TSISTCLongSignal(signal) => signal.set_signal_column(lf),
            Self::TSISTCCloseShortSignal(signal) => signal.set_signal_column(lf),
            Self::TSISTCCloseLongSignal(signal) => signal.set_signal_column(lf),
        }
    }
    fn update_signal_column(&self, data: &DataFrame) -> Result<DataFrame, Error> {
        match self {
            // Self::MultipleStochasticWithThresholdShortSignal(
            //     multiple_stochastic_with_threshold_short_signal,
            // ) => multiple_stochastic_with_threshold_short_signal.update_signal_column(data),
            // Self::MultipleStochasticWithThresholdCloseShortSignal(
            //     multiple_stochastic_with_threshold_short_close_signal,
            // ) => multiple_stochastic_with_threshold_short_close_signal.update_signal_column(data),
            // Self::MultipleStochasticWithThresholdLongSignal(
            //     multiple_stochastic_with_threshold_long_signal,
            // ) => multiple_stochastic_with_threshold_long_signal.update_signal_column(data),
            // Self::MultipleStochasticWithThresholdCloseLongSignal(
            //     multiple_stochastic_with_threshold_long_close_signal,
            // ) => multiple_stochastic_with_threshold_long_close_signal.update_signal_column(data),
            Self::TSISTCShortSignal(signal) => signal.update_signal_column(data),
            Self::TSISTCLongSignal(signal) => signal.update_signal_column(data),
            Self::TSISTCCloseShortSignal(signal) => signal.update_signal_column(data),
            Self::TSISTCCloseLongSignal(signal) => signal.update_signal_column(data),
        }
    }
}

impl From<TSISTCShortSignal> for SignalWrapper {
    fn from(value: TSISTCShortSignal) -> Self {
        Self::TSISTCShortSignal(value)
    }
}

impl From<TSISTCLongSignal> for SignalWrapper {
    fn from(value: TSISTCLongSignal) -> Self {
        Self::TSISTCLongSignal(value)
    }
}

impl From<TSISTCCloseShortSignal> for SignalWrapper {
    fn from(value: TSISTCCloseShortSignal) -> Self {
        Self::TSISTCCloseShortSignal(value)
    }
}

impl From<TSISTCCloseLongSignal> for SignalWrapper {
    fn from(value: TSISTCCloseLongSignal) -> Self {
        Self::TSISTCCloseLongSignal(value)
    }
}
