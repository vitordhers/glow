use common::traits::indicator::Indicator;
use indicators::IndicatorWrapper;
use preindicators::PreIndicatorWrapper;
use signals::SignalWrapper;
pub mod functions;
pub mod indicators;
pub mod preindicators;
pub mod signals;
pub mod r#static;

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
}

impl Default for Strategy {
    fn default() -> Self {
        
    }
}
