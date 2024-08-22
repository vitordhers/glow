#[derive(Clone, Debug)]
pub struct Strategy {
    pub name: &'static str,
    pub preindicators: Vec<PreIndicatorWrapper>,
    pub indicators: Vec<IndicatorWrapper>,
    pub signals: Vec<SignalWrapper>,
}

impl<T: Indicator + Clone> TestNew<T> {
    pub fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) {
        let updated_preindicators = self
            .preindicators
            .clone()
            .into_iter()
            .map(|pi| {
                pi.patch_symbols_pair(updated_symbols_pair).expect(&format!(
                    "Preindicator {} patch to be valid for {:?}",
                    pi.name(),
                    updated_symbols_pair
                ))
            })
            .collect::<Vec<_>>();
    }
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

    pub fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) -> Result<Self, GlowError> {
        let updated_preindicators = self
            .preindicators
            .clone()
            .into_iter()
            .map(|pi| {
                pi.patch_symbols_pair(updated_symbols_pair).expect(&format!(
                    "Preindicator {} patch to be valid for {:?}",
                    pi.name(),
                    updated_symbols_pair
                ))
            })
            .collect::<Vec<PreIndicatorWrapper>>();

        let updated_indicators = self
            .indicators
            .clone()
            .into_iter()
            .map(|i| {
                i.patch_symbols_pair(updated_symbols_pair).expect(&format!(
                    "Preindicator {} patch to be valid for {:?}",
                    i.name(),
                    updated_symbols_pair
                ))
            })
            .collect::<Vec<IndicatorWrapper>>();

        let updated_signals = self
            .signals
            .clone()
            .into_iter()
            .map(|s| {
                s.patch_symbols_pair(updated_symbols_pair).expect(&format!(
                    "Signal {:?} patch to be valid for {:?}",
                    s.signal_category(),
                    updated_symbols_pair
                ))
            })
            .collect::<Vec<SignalWrapper>>();

        let updated_strategy = Strategy::new(
            self.name,
            updated_preindicators,
            updated_indicators,
            updated_signals,
        );
        Ok(updated_strategy)
    }
}
