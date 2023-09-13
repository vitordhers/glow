#[derive(Clone, Debug, PartialEq, Eq, Copy, Default)]
pub enum OrderStatus {
    #[default]
    StandBy,
    PartiallyFilled,
    Filled,
    PartiallyClosed,
    Closed,
    Cancelled,
}
