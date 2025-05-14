use num::{Float, Signed, ToPrimitive, Unsigned};
use std::{any::type_name, fmt::Display};

#[derive(Clone, Copy, Debug)]
pub enum NumericParam {
    Uint(UintParam<u8>),
    Uint16(UintParam<u16>),
    Uint32(UintParam<u32>),
    Int(IntParam<i8>),
    Int16(IntParam<i16>),
    Int32(IntParam<i32>),
    Float(FloatParam<f32>),
    Float64(FloatParam<f64>),
}

pub trait CombinableParam<T: ToPrimitive> {
    type Inner: ToPrimitive;
    fn get_amplitude(&self) -> Self::Inner;
    fn total_steps(&self) -> Self::Inner;
    fn get_range(&self) -> Vec<Self::Inner>;
}

impl CombinableParam<u8> for NumericParam {
    type Inner = u8;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into u8", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into u8", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Uint(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into u8", invalid),
        }
    }
}

impl CombinableParam<i8> for NumericParam {
    type Inner = i8;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into i8", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into i8", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Int(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into i8", invalid),
        }
    }
}

impl CombinableParam<u16> for NumericParam {
    type Inner = u16;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.get_amplitude().into(),
            Self::Uint16(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into u16", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.total_steps().into(),
            Self::Uint16(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into u16", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Uint(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint16(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into u16", invalid),
        }
    }
}

impl CombinableParam<i16> for NumericParam {
    type Inner = i16;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.get_amplitude().into(),
            Self::Int16(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into i16", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.total_steps().into(),
            Self::Int16(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into i16", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Int(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int16(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into i16", invalid),
        }
    }
}

impl CombinableParam<u32> for NumericParam {
    type Inner = u32;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.get_amplitude().into(),
            Self::Uint16(param) => param.get_amplitude().into(),
            Self::Uint32(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into u32", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.total_steps().into(),
            Self::Uint16(param) => param.total_steps().into(),
            Self::Uint32(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into u32", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Uint(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint32(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into u32", invalid),
        }
    }
}

impl CombinableParam<i32> for NumericParam {
    type Inner = i32;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.get_amplitude().into(),
            Self::Int16(param) => param.get_amplitude().into(),
            Self::Int32(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Int(param) => param.total_steps().into(),
            Self::Int16(param) => param.total_steps().into(),
            Self::Int32(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Int(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int32(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }
}

impl CombinableParam<f32> for NumericParam {
    type Inner = f32;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.get_amplitude().into(),
            Self::Uint16(param) => param.get_amplitude().into(),
            Self::Int(param) => param.get_amplitude().into(),
            Self::Int16(param) => param.get_amplitude().into(),
            Self::Float(param) => param.get_amplitude(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.total_steps().into(),
            Self::Uint16(param) => param.total_steps().into(),
            Self::Int(param) => param.total_steps().into(),
            Self::Int16(param) => param.total_steps().into(),
            Self::Float(param) => param.total_steps(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Uint(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Float(param) => param.get_range(),
            invalid => panic!("cannot convert {:?} into i32", invalid),
        }
    }
}

impl CombinableParam<f64> for NumericParam {
    type Inner = f64;

    fn get_amplitude(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.get_amplitude().into(),
            Self::Uint16(param) => param.get_amplitude().into(),
            Self::Uint32(param) => param.get_amplitude().into(),
            Self::Int(param) => param.get_amplitude().into(),
            Self::Int16(param) => param.get_amplitude().into(),
            Self::Int32(param) => param.get_amplitude().into(),
            Self::Float(param) => param.get_amplitude().into(),
            Self::Float64(param) => param.get_amplitude(),
        }
    }

    fn total_steps(&self) -> Self::Inner {
        match self {
            Self::Uint(param) => param.total_steps().into(),
            Self::Uint16(param) => param.total_steps().into(),
            Self::Uint32(param) => param.total_steps().into(),
            Self::Int(param) => param.total_steps().into(),
            Self::Int16(param) => param.total_steps().into(),
            Self::Int32(param) => param.total_steps().into(),
            Self::Float(param) => param.total_steps().into(),
            Self::Float64(param) => param.total_steps(),
        }
    }

    fn get_range(&self) -> Vec<Self::Inner> {
        match self {
            Self::Uint(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Uint32(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int16(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Int32(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Float(param) => param
                .get_range()
                .iter()
                .map(|p| (*p).into())
                .collect::<Vec<Self::Inner>>(),
            Self::Float64(param) => param.get_range(),
        }
    }
}

impl NumericParam {
    pub fn new<T>(name: &'static str, min: T, step: T, max: T) -> Self
    where
        T: ToPrimitive,
    {
        match type_name::<T>() {
            "u8" => {
                let min = min.to_u8().expect("min to implement Into<u8>");
                let step = step.to_u8().expect("step to implement Into<u8>");
                let max = max.to_u8().expect("max to implement Into<u8>");
                NumericParam::Uint(UintParam::new(name, min, step, max))
            }
            "u16" => {
                let min = min.to_u16().expect("min to implement Into<u16>");
                let step = step.to_u16().expect("step to implement Into<u16>");
                let max = max.to_u16().expect("max to implement Into<u16>");
                NumericParam::Uint16(UintParam::new(name, min, step, max))
            }
            "u32" => {
                let min = min.to_u32().expect("min to implement Into<u32>");
                let step = step.to_u32().expect("step to implement Into<u32>");
                let max = max.to_u32().expect("max to implement Into<u32>");
                NumericParam::Uint32(UintParam::new(name, min, step, max))
            }
            "i8" => {
                let min = min.to_i8().expect("min to implement Into<i8>");
                let step = step.to_i8().expect("step to implement Into<i8>");
                let max = max.to_i8().expect("max to implement Into<i8>");
                NumericParam::Int(IntParam::new(name, min, step, max))
            }
            "i16" => {
                let min = min.to_i16().expect("min to implement Into<i16>");
                let step = step.to_i16().expect("step to implement Into<i16>");
                let max = max.to_i16().expect("max to implement Into<i16>");
                NumericParam::Int16(IntParam::new(name, min, step, max))
            }
            "i32" => {
                let min = min.to_i32().expect("min to implement Into<i32>");
                let step = step.to_i32().expect("step to implement Into<i32>");
                let max = max.to_i32().expect("max to implement Into<i32>");
                NumericParam::Int32(IntParam::new(name, min, step, max))
            }
            "f32" => {
                let min = min.to_f32().expect("min to implement Into<f32>");
                let step = step.to_f32().expect("step to implement Into<f32>");
                let max = max.to_f32().expect("max to implement Into<f32>");
                NumericParam::Float(FloatParam::new(name, min, step, max))
            }
            "f64" => {
                let min = min.to_f64().expect("min to implement Into<f64>");
                let step = step.to_f64().expect("step to implement Into<f64>");
                let max = max.to_f64().expect("max to implement Into<f64>");
                NumericParam::Float64(FloatParam::new(name, min, step, max))
            }
            found_type => {
                panic!("invalid found type {:?}", found_type);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UintParam<T>
where
    T: Unsigned + PartialOrd + Copy,
{
    pub name: &'static str,
    pub step: T,
    pub min: T,
    pub max: T,
}

#[derive(Clone, Copy, Debug)]
pub struct IntParam<T>
where
    T: Signed + PartialOrd + Copy,
{
    pub name: &'static str,
    pub step: T,
    pub min: T,
    pub max: T,
}

#[derive(Clone, Copy, Debug)]
pub struct FloatParam<T>
where
    T: Float + PartialOrd + Copy,
{
    pub name: &'static str,
    pub step: T,
    pub min: T,
    pub max: T,
}

impl<T: Unsigned + PartialOrd + Display + Copy> UintParam<T> {
    pub fn new(name: &'static str, min: T, step: T, max: T) -> Self {
        assert!(min < max, "min {} must be less than max {}", min, max);
        assert!(!step.is_zero(), "step cannot be 0");
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
            min,
            step,
            max,
        }
    }
}

impl<T: Signed + PartialOrd + Display + Copy> IntParam<T> {
    pub fn new(name: &'static str, min: T, step: T, max: T) -> Self {
        assert!(min < max, "min {} must be less than max {}", min, max);
        assert!(!step.is_zero(), "step cannot be 0");
        assert!(!step.is_negative(), "step cannot be negative");
        assert!(
            min + step <= max,
            "diff between min {} and max {} must be less than step {}",
            min,
            max,
            step
        );
        Self {
            name,
            min,
            step,
            max,
        }
    }
}

impl<T: Float + PartialOrd + Display + Copy> FloatParam<T> {
    pub fn new(name: &'static str, min: T, step: T, max: T) -> Self {
        assert!(min < max, "min {} must be less than max {}", min, max);
        assert!(!step.is_zero(), "step cannot be 0");
        assert!(!step.is_nan(), "step cannot be NaN");
        assert!(!step.is_sign_negative(), "step cannot be negative");
        assert!(
            min + step <= max,
            "diff between min {} and max {} must be less than step {}",
            min,
            max,
            step
        );
        Self {
            name,
            min,
            step,
            max,
        }
    }
}

pub trait UintCombinableParam<T> {
    fn get_amplitude(&self) -> T;
    fn total_steps(&self) -> T;
    fn get_range(&self) -> Vec<T>;
}

pub trait IntCombinableParam<T> {
    fn get_amplitude(&self) -> T;
    fn total_steps(&self) -> T;
    fn get_range(&self) -> Vec<T>;
}

pub trait FloatCombinableParam<T> {
    fn get_amplitude(&self) -> T;
    fn total_steps(&self) -> T;
    fn get_range(&self) -> Vec<T>;
}

impl<T> UintCombinableParam<T> for UintParam<T>
where
    T: Unsigned + Copy + PartialOrd + ToPrimitive,
{
    fn get_amplitude(&self) -> T {
        self.max - self.min
    }

    fn total_steps(&self) -> T {
        (self.max - self.min) / self.step
    }

    fn get_range(&self) -> Vec<T> {
        let mut range = Vec::new();
        let mut current = self.min;
        while current <= self.max {
            range.push(current);
            current = current + self.step;
        }
        range
    }
}

impl<T> IntCombinableParam<T> for IntParam<T>
where
    T: Signed + Copy + PartialOrd + ToPrimitive,
{
    fn get_amplitude(&self) -> T {
        (self.max - self.min).abs()
    }

    fn total_steps(&self) -> T {
        (self.max - self.min / self.step).abs()
    }

    fn get_range(&self) -> Vec<T> {
        let mut range = Vec::new();
        let mut current = self.min;
        while current <= self.max {
            range.push(current);
            current = current + self.step;
        }
        range
    }
}

impl<T> FloatCombinableParam<T> for FloatParam<T>
where
    T: Float + Copy + PartialOrd + ToPrimitive,
{
    fn get_amplitude(&self) -> T {
        self.max - self.min
    }

    fn total_steps(&self) -> T {
        (self.max - self.min).abs() / self.step.abs()
    }

    fn get_range(&self) -> Vec<T> {
        let mut range = Vec::new();
        let mut current = self.min;
        let step = self.step;
        while current <= self.max {
            range.push(current);
            current = current + step;
        }
        range
    }
}

// #[test]
// fn test() {
//     let param = UintParam::new(1u8, 1u8, 10u8);
//     let amplitude = param.get_amplitude();
//     let total_steps = param.total_steps();
//     let range = param.get_range();
//
//     println!(
//         "amplitude {}, total steps {}, range {:?}",
//         amplitude, total_steps, range
//     );
//     let param = IntParam::new(-12i16, 2i16, 0i16);
//     let amplitude = param.get_amplitude();
//     let total_steps = param.total_steps();
//     let range = param.get_range();
//     println!(
//         "amplitude {}, total steps {}, range {:?}",
//         amplitude, total_steps, range
//     );
//     let param = FloatParam::new(-0.5f32, 0.5f32, 5f32);
//     let amplitude = param.get_amplitude();
//     let total_steps = param.total_steps();
//     let range = param.get_range();
//     println!(
//         "amplitude {}, total steps {}, range {:?}",
//         amplitude, total_steps, range
//     );
// }
