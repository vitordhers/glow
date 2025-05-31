use super::side::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Short,
    Long,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalSide {
    None,
    Side(Direction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SignalType {
    #[default]
    KeepPosition,
    Open(Direction),
    Close(SignalSide),
    Revert(SignalSide),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EventType {
    #[default]
    None,
    LeverageBankruptcy,
    StopLoss,
    TrailingStopLoss,
    TakeProfit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Signal(SignalType),
    Event(EventType),
}

impl Default for Action {
    fn default() -> Self {
        Self::Signal(SignalType::default())
    }
}

impl From<Direction> for Side {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Short => Side::Sell,
            Direction::Long => Side::Buy,
        }
    }
}

impl Action {
    pub fn as_str(&self) -> &'static str {
        match self {
            Action::Signal(signal_type) => match signal_type {
                SignalType::KeepPosition => "",
                SignalType::Open(direction) => match direction {
                    Direction::Short => "short",
                    Direction::Long => "long",
                },
                SignalType::Close(signal_side) => match signal_side {
                    SignalSide::None => "close",
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => "short_close",
                        Direction::Long => "long_close",
                    },
                },
                SignalType::Revert(signal_side) => match signal_side {
                    SignalSide::None => "revert",
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => "short_revert",
                        Direction::Long => "long_revert",
                    },
                },
            },
            Action::Event(event_type) => match event_type {
                EventType::None => "no_event",
                EventType::LeverageBankruptcy => "leverage_bankruptcy",
                EventType::StopLoss => "stop_loss",
                EventType::TrailingStopLoss => "trailing_stop_loss",
                EventType::TakeProfit => "take_profit",
            },
        }
    }
}
