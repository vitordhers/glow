use crate::structs::{Execution, Order};

use super::balance::Balance;

// TODO: deprecate this
#[derive(Debug, Clone)]
pub enum ProcesserAction {
    None,
    Auth { success: bool },
    Execution { executions: Vec<Execution> },
    UpdateOrder { updated_order: Order },
    StopOrder { stop_order: Order },
    CancelOrder { cancelled_order: Order },
    Balance { balance: Balance },
}
