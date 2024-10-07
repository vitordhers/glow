use crate::structs::Order;

#[derive(Clone, Debug, Default)]
pub enum OrderAction {
    #[default]
    None,
    Update(Order),
    Cancel(Order),
    Stop(Order),
}
