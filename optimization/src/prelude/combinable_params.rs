use polars::prelude::*;

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
        assert!(step != 0, "step cannot be 0");
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

    pub fn get_least_amount_of_rows(&self) -> Option<usize> {
        let last_param = self.range.last().expect("range to have last member");
        let amount: usize = (last_param - 1).into();
        Some(amount)
    }
}
