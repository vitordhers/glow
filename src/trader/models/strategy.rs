use chrono::NaiveDate;
use polars::prelude::*;

use super::{indicator::Indicator, performance::Performance};

pub struct Strategy {
    pub name: String,
    pub indicators: Vec<Indicator>,
    pub performance: Option<Performance>,
    pub data: LazyFrame,
}

impl Strategy {
    pub fn new(name: String, indicators: Vec<Indicator>, data: LazyFrame) -> Strategy {
        Strategy {
            name,
            indicators,
            performance: None,
            data,
        }
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy {
            name: "Default Strategy".to_string(),
            indicators: vec![],
            performance: None,
            data: df!("position" => &[] as &[i64], "date" => &[] as &[NaiveDate])
                .unwrap()
                .lazy(),
        }
    }
}
