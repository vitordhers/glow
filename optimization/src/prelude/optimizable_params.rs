pub trait OptimizableParam {
    fn min() -> u8 {
        u8::MIN
    }

    fn max() -> u8 {
        u8::MAX
    }
}
