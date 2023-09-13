use super::balance::Balance;
use crate::trader::models::{execution::Execution, order::Order};

#[derive(Debug, Clone)]
pub enum ProcesserAction {
    Nil,
    Auth { success: bool },
    Execution { executions: Vec<Execution> },
    UpdateOrder { updated_order: Order },
    StopOrder { stop_order: Order },
    CancelOrder { cancelled_order: Order },
    Balance { balance: Balance },
}
