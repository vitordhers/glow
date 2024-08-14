use crate::structs::Order;

#[derive(Clone, Debug)]
pub enum OrderAction {
    Update(Order),
    Cancel(Order),
    Stop(Order),
}
