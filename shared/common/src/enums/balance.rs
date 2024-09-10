use crate::functions::current_timestamp;

#[derive(Debug, Clone, Copy)]
pub struct Balance {
    pub timestamp: i64,
    pub available_to_withdraw: f64,
    pub wallet_balance: f64,
}

impl Balance {
    pub fn new(timestamp: i64, available_to_withdraw: f64, wallet_balance: f64) -> Self {
        Balance {
            timestamp,
            available_to_withdraw,
            wallet_balance,
        }
    }
}

impl Default for Balance {
    fn default() -> Self {
        Self {
            timestamp: current_timestamp(),
            available_to_withdraw: 0.0,
            wallet_balance: 0.0,
        }
    }
}
