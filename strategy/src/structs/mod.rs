use crate::{
    indicators::IndicatorWrapper, preindicators::PreIndicatorWrapper, signals::SignalWrapper,
};
use common::{structs::SymbolsPair, traits::indicator::Indicator};
use glow_error::GlowError;

#[derive(Clone, Debug)]
pub struct Strategy {
    pub name: &'static str,
    pub preindicators: Vec<PreIndicatorWrapper>,
    pub indicators: Vec<IndicatorWrapper>,
    pub signals: Vec<SignalWrapper>,
}

impl Strategy {
    pub fn new(
        name: &'static str,
        preindicators: Vec<PreIndicatorWrapper>,
        indicators: Vec<IndicatorWrapper>,
        signals: Vec<SignalWrapper>,
    ) -> Self {
        Self {
            name,
            preindicators,
            indicators,
            signals,
        }
    }

    pub fn get_minimum_klines_for_benchmarking(&self) -> u32 {
        let mut minimum_klines_for_benchmarking_candidates = vec![0];
        self.preindicators.iter().for_each(|p| {
            minimum_klines_for_benchmarking_candidates.push(p.get_minimum_klines_for_benchmarking())
        });
        self.indicators.iter().for_each(|p| {
            minimum_klines_for_benchmarking_candidates.push(p.get_minimum_klines_for_benchmarking())
        });
        *minimum_klines_for_benchmarking_candidates
            .iter()
            .max()
            .unwrap_or(&0)
    }

    pub fn patch_symbols_pair(&self, symbols_pair: SymbolsPair) -> Result<Self, GlowError> {
        let mut updated_strategy = self.clone();

        let updated_preindicators = self
            .preindicators
            .clone()
            .into_iter()
            .map(|pi| {
                pi.patch_symbols_pair(symbols_pair).expect(&format!(
                    "Preindicator {} patch to be valid for {:?}",
                    pi.name(),
                    symbols_pair
                ))
            })
            .collect::<Vec<PreIndicatorWrapper>>();

        let updated_indicators = self
            .indicators
            .clone()
            .into_iter()
            .map(|i| {
                i.patch_symbols_pair(symbols_pair).expect(&format!(
                    "Preindicator {} patch to be valid for {:?}",
                    i.name(),
                    symbols_pair
                ))
            })
            .collect::<Vec<IndicatorWrapper>>();

        Ok(updated_strategy)
    }
}
