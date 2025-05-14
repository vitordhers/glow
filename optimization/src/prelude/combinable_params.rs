use glow_error::GlowError;
use polars::prelude::{DataType, LazyFrame, NamedFrom, Series};

pub trait FeatureGenerator {
    fn get_name(&self) -> &str;
    fn get_param(&self) -> CombinableParam;
    fn compute(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError>;
}

pub trait OptimizableStrategy<T> {
    fn get_name(&self) -> &str;
    fn get_indicators(&self) -> Vec<Box<dyn FeatureGenerator>>;
    fn get_opening_indicators(&self) -> Vec<Box<dyn FeatureGenerator>>;
    fn get_closing_indicators(&self) -> Vec<Box<dyn FeatureGenerator>>;

    fn get_params(&self) -> Vec<CombinableParam> {
        self.get_indicators()
            .iter()
            .map(|i| i.get_param())
            .collect()
    }
    fn compute_opening_indicators(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut result_lf = lf.clone();
        for indicator_generator in self.get_opening_indicators() {
            result_lf = indicator_generator.compute(&result_lf)?;
        }
        Ok(result_lf)
    }
    fn compute_closing_indicators(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut result_lf = lf.clone();
        for indicator_generator in self.get_closing_indicators() {
            result_lf = indicator_generator.compute(&result_lf)?;
        }
        Ok(result_lf)
    }
    fn get_cartesian_product_len(&self) -> u32 {
        self.get_params().iter().fold(0u32, |prev, p| {
            let current_range_size: u32 = p.range_size().into();
            if prev == 0 {
                return current_range_size;
            }
            prev * current_range_size
        })
    }
}

#[derive(Debug, Clone)]
pub enum CombinableParam {
    Uint8(OptimizableParam<u8>),
}

impl CombinableParam {
    pub fn range_size(&self) -> u16 {
        match self {
            Self::Uint8(p) => p.range_size(),
        }
    }

    pub fn get_range_as_series(&self) -> Series {
        match self {
            Self::Uint8(p) => p.get_range_as_series(),
        }
    }

    pub fn get_dtype(&self) -> &DataType {
        match self {
            Self::Uint8(p) => &p.dtype,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimizableParam<T> {
    pub name: &'static str,
    pub dtype: DataType, // native base dtype
    pub range: Vec<T>,
}

impl<T> OptimizableParam<T> {
    pub fn range_size(&self) -> u16 {
        let len = self.range.len();
        match u16::try_from(len) {
            Ok(v) => v,
            Err(_) => panic!("{} param can't be cast into u16!", self.name),
        }
    }
}

impl OptimizableParam<u8> {
    pub fn new(name: &'static str, min: u8, step: u8, max: u8) -> Self {
        assert!(min < max, "min {} must be less than max {}", min, max);
        assert!(step == 0, "step cannot be 0");
        assert!(step < max, "step {} must be less than max {}", step, max);
        assert!(
            min + step <= max,
            "diff between min {} and max {} must be less than step {}",
            min,
            max,
            step
        );

        Self {
            name,
            range: (min..=max).step_by(step.into()).collect(),
            dtype: DataType::UInt8,
        }
    }

    pub fn get_range_as_series(&self) -> Series {
        Series::new(self.name.into(), self.range.clone())
    }
}
