use common::structs::Symbol;
use glow_error::GlowError;
use polars::prelude::*;

use super::strategy::FeaturesCacheStrategy;

pub enum OpenSignal {
    OpenShort,
    OpenLong,
}

pub enum CloseSignal {
    CloseShort,
    CloseLong,
    Close,
}

pub enum Signal {
    Open(OpenSignal),
    Close(CloseSignal),
}

pub trait PositionGenerator: Sync + Send {
    fn get_position_name(&self) -> &str;
    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<(&str, u32)>,
        features_cache: &FeaturesCacheStrategy,
    ) -> Result<LazyFrame, GlowError>;
}

#[test]

fn test_iteration() {
    use itertools::*;
    use rayon::prelude::*;
    use std::collections::HashMap;

    let vec1 = (6..=30).collect::<Vec<u32>>(); // 25
    let mut vec1_params = HashMap::new();
    vec1.iter().for_each(|v| {
        vec1_params.insert(v, v + 1);
    });
    let vec2 = (51..=100).collect::<Vec<u32>>(); // 50
    let mut vec2_params = HashMap::new();
    vec2.iter().for_each(|v| {
        vec2_params.insert(v, v + 1);
    });

    let iter: Vec<_> = iproduct!(vec1, vec2).collect();

    iter.par_iter().for_each(|value| {
        println!("VALUE {:?}", &value);
    });
}
