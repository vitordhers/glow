use crate::trader::models::order::Order;

#[derive(Clone, Debug)]
pub enum OrderAction {
    Update(Order),
    Cancel(Order),
    Stop(Order),
}
