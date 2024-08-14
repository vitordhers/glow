#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, PartialOrd, Clone, Copy)]
#[repr(u8)]
pub enum LogLevel {
    Nothing = 0,
    Results = 1,
    Trades = 2,
    All = 3,
}
