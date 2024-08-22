// use chrono::Duration;
// use glow_error::GlowError;
// use polars::prelude::TimeUnit;
use std::fmt::Display;
use glow_error::GlowError;
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ParamId {
    SlowSpan,
    FastSpan,
}

#[derive(Clone, Debug)]
pub enum Param {
    Boolean(bool, BoolParamConfig),
    UInt8(u8, NumberParamConfig<u8>),
    UInt16(u16, NumberParamConfig<u16>),
    UInt32(u32, NumberParamConfig<u32>),
    UInt64(u64, NumberParamConfig<u64>),
    Int8(i8, NumberParamConfig<i8>),
    Int16(i16, NumberParamConfig<i16>),
    Int32(i32, NumberParamConfig<i32>),
    Int64(i64, NumberParamConfig<i64>),
    Float32(f32, NumberParamConfig<f32>),
    Float64(f64, NumberParamConfig<f64>),
    String(String, StringParamConfig),
    // Array(Box<[Param]>, usize),
    // List(Box<Vec<Param>>),
    // /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    // /// in days (32 bits).
    // Date(NumberParam<u32>),
    // /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    // /// in the given timeunit (64 bits).
    // /// 64-bit integer representing difference between times in milliseconds or nanoseconds
    // Datetime(TimeUnit, NumberParam<i64>),
    // // 64-bit integer representing difference between times in milliseconds or nanoseconds
    // Duration(TimeUnit, NumberParam<Duration>),
    // /// A 64-bit time representing the elapsed time since midnight in nanoseconds
    // Time(NumberParam<i64>),
    // Null,
}

impl Param {
    pub fn validate(&self, value: &Self) -> Result<(), GlowError> {
        match (self, value) {
            (Param::Boolean(_, _), Param::Boolean(_, _)) => Ok(()),
            (Param::String(_, _), Param::String(_, _)) => Ok(()),
            (Param::UInt8(_, config), Param::UInt8(value, _)) => config.validate(value),
            (Param::UInt16(_, config), Param::UInt16(value, _)) => config.validate(value),
            (Param::UInt32(_, config), Param::UInt32(value, _)) => config.validate(value),
            (Param::UInt64(_, config), Param::UInt64(value, _)) => config.validate(value),
            (Param::Int8(_, config), Param::Int8(value, _)) => config.validate(value),
            (Param::Int16(_, config), Param::Int16(value, _)) => config.validate(value),
            (Param::Int32(_, config), Param::Int32(value, _)) => config.validate(value),
            (Param::Int64(_, config), Param::Int64(value, _)) => config.validate(value),
            (Param::Float32(_, config), Param::Float32(value, _)) => config.validate(value),
            (Param::Float64(_, config), Param::Float64(value, _)) => config.validate(value),
            (_, value) => Err(GlowError::new(
                format!("Invalid param validation"),
                format!("Incompatible param types {:?} and {:?}", self, value),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BoolParamConfig {
    pub default: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct NumberParamConfig<T: Clone + Copy> {
    pub default: T,
    pub min: Option<T>,
    pub max: Option<T>,
}

impl<T: Clone + Copy + PartialOrd + PartialEq + Display> NumberParamConfig<T> {
    pub fn new(default: T, min: Option<T>, max: Option<T>) -> Self {
        Self { default, min, max }
    }

    pub fn validate(&self, value: &T) -> Result<(), GlowError> {
        if self.min.is_some() {
            let min = self.min.unwrap();
            if value < &min {
                return Err(GlowError::new(
                    format!("Invalid param"),
                    format!("value {} is less than param minimum {}", value, min),
                ));
            }
        }
        if self.max.is_some() {
            let max = self.min.unwrap();
            if value > &max {
                return Err(GlowError::new(
                    format!("Invalid param"),
                    format!("value {} is more than param maximum {}", value, max),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StringParamConfig {
    pub default: String,
}

impl StringParamConfig {}
