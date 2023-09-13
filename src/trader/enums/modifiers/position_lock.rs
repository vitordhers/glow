#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
#[repr(u8)]
pub enum PositionLock {
    Nil = 0,
    Fee = 1,  // absolute value of revenue > transaction fee
    Loss = 2, // least percentage for the trade to close
}
