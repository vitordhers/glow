use std::ops::Not;

use super::side::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Short,
    Long,
}

impl Not for Direction {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Short => Self::Long,
            Self::Long => Self::Short,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalSide {
    Both,
    Side(Direction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    Open(Direction),
    Close(SignalSide),
    Revert(SignalSide),
}

impl SignalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open(direction) => match direction {
                Direction::Short => "short",
                Direction::Long => "long",
            },
            Self::Close(signal_side) => match signal_side {
                SignalSide::Both => "close",
                SignalSide::Side(direction) => match direction {
                    Direction::Short => "short_close",
                    Direction::Long => "long_close",
                },
            },
            Self::Revert(signal_side) => match signal_side {
                SignalSide::Both => "revert",
                SignalSide::Side(direction) => match direction {
                    Direction::Short => "short_revert",
                    Direction::Long => "long_revert",
                },
            },
        }
    }
}

impl TryFrom<SignalType> for u8 {
    type Error = ();

    fn try_from(value: SignalType) -> Result<Self, Self::Error> {
        match value {
            SignalType::Open(direction) => match direction {
                Direction::Short => Ok(1),
                Direction::Long => Ok(2),
            },
            SignalType::Close(signal_side) => match signal_side {
                SignalSide::Both => Ok(13),
                SignalSide::Side(direction) => match direction {
                    Direction::Short => Ok(11),
                    Direction::Long => Ok(12),
                },
            },
            SignalType::Revert(signal_side) => match signal_side {
                SignalSide::Both => Ok(23),
                SignalSide::Side(direction) => match direction {
                    Direction::Short => Ok(21),
                    Direction::Long => Ok(22),
                },
            },
        }
    }
}

impl TryFrom<u8> for SignalType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Open(Direction::Short)),
            2 => Ok(Self::Open(Direction::Long)),
            11 => Ok(Self::Close(SignalSide::Side(Direction::Short))),
            12 => Ok(Self::Close(SignalSide::Side(Direction::Long))),
            13 => Ok(Self::Close(SignalSide::Both)),
            21 => Ok(Self::Revert(SignalSide::Side(Direction::Short))),
            22 => Ok(Self::Revert(SignalSide::Side(Direction::Long))),
            23 => Ok(Self::Revert(SignalSide::Both)),
            _ => Err("Invalid number for SignalType"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    LeverageBankruptcy,
    StopLoss,
    TrailingStopLoss,
    TakeProfit,
}

impl TryFrom<u8> for EventType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            252 => Ok(Self::TakeProfit),
            253 => Ok(Self::TrailingStopLoss),
            254 => Ok(Self::StopLoss),
            255 => Ok(Self::LeverageBankruptcy),
            _ => Err("Invalid number for EventType"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Action {
    #[default]
    KeepPosition,
    Signal(SignalType),
    Event(EventType),
}

impl From<Action> for u8 {
    fn from(value: Action) -> Self {
        match value {
            Action::KeepPosition => 0,
            Action::Signal(signal_type) => signal_type.try_into().unwrap_or(0),
            Action::Event(event_type) => match event_type {
                EventType::LeverageBankruptcy => 255,
                EventType::StopLoss => 254,
                EventType::TrailingStopLoss => 253,
                EventType::TakeProfit => 252,
            },
        }
    }
}

impl From<Option<u8>> for Action {
    fn from(value: Option<u8>) -> Self {
        match value {
            Some(value) => match value {
                0 => Action::KeepPosition,
                1 | 2 | 3 | 11 | 12 | 13 | 21 | 22 | 23 => {
                    let result: Result<SignalType, &'static str> = value.try_into();
                    if result.is_err() {
                        Action::KeepPosition
                    } else {
                        let signal = result.unwrap();
                        Action::Signal(signal)
                    }
                }
                252 | 253 | 254 | 255 => {
                    let result: Result<EventType, &'static str> = value.try_into();
                    if result.is_err() {
                        Action::KeepPosition
                    } else {
                        let event = result.unwrap();
                        Action::Event(event)
                    }
                }
                _ => Action::KeepPosition,
            },
            None => Action::KeepPosition,
        }
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
            Action::KeepPosition => "",
            Action::Signal(signal_type) => signal_type.as_str(),
            Action::Event(event_type) => match event_type {
                EventType::LeverageBankruptcy => "leverage_bankruptcy",
                EventType::StopLoss => "stop_loss",
                EventType::TrailingStopLoss => "trailing_stop_loss",
                EventType::TakeProfit => "take_profit",
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TradingState {
    #[default]
    Neutral,
    Short,
    Long,
}

impl Not for TradingState {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            TradingState::Neutral => TradingState::Neutral,
            TradingState::Short => TradingState::Long,
            TradingState::Long => TradingState::Short,
        }
    }
}

impl TradingState {
    pub fn on_signals_and_event(
        prev_state: TradingState,
        signals: Option<Vec<u8>>,
        event: Option<u8>,
    ) -> TradingState {
        match (signals, event) {
            (None, None) => prev_state,
            (None, Some(_)) => TradingState::Neutral,
            (Some(_), Some(_)) => TradingState::Neutral,
            (Some(signals), None) => match (prev_state, signals.contains(&2)) {
                (TradingState::Neutral, true) => TradingState::Long,
                (TradingState::Neutral, _) => prev_state,
                (TradingState::Short | TradingState::Long, SignalType::Open(_)) => prev_state,
                (TradingState::Short, SignalType::Close(signal_side)) => match signal_side {
                    SignalSide::Both => TradingState::Neutral,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => TradingState::Neutral,
                        Direction::Long => prev_state,
                    },
                },
                (TradingState::Short, SignalType::Revert(signal_side)) => match signal_side {
                    SignalSide::Both => !prev_state,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => TradingState::Long,
                        Direction::Long => prev_state,
                    },
                },
                (TradingState::Long, SignalType::Close(signal_side)) => match signal_side {
                    SignalSide::Both => TradingState::Neutral,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => prev_state,
                        Direction::Long => TradingState::Neutral,
                    },
                },
                (TradingState::Long, SignalType::Revert(signal_side)) => match signal_side {
                    SignalSide::Both => !prev_state,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => prev_state,
                        Direction::Long => TradingState::Short,
                    },
                },
                (TradingState::Short, true) => todo!(),
                (TradingState::Short, false) => todo!(),
                (TradingState::Long, true) => todo!(),
                (TradingState::Long, false) => todo!(),
            },
        }
    }
    pub fn on_signal_and_event(
        prev_state: TradingState,
        signal: Option<SignalType>,
        event: Option<EventType>,
    ) -> TradingState {
        match (signal, event) {
            (None, None) => prev_state,
            (None, Some(_)) => TradingState::Neutral,
            (Some(_), Some(_)) => TradingState::Neutral,
            (Some(signal_type), None) => match (prev_state, signal_type) {
                (TradingState::Neutral, SignalType::Open(direction)) => match direction {
                    Direction::Short => TradingState::Short,
                    Direction::Long => TradingState::Long,
                },
                (TradingState::Neutral, _) => prev_state,
                (TradingState::Short | TradingState::Long, SignalType::Open(_)) => prev_state,
                (TradingState::Short, SignalType::Close(signal_side)) => match signal_side {
                    SignalSide::Both => TradingState::Neutral,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => TradingState::Neutral,
                        Direction::Long => prev_state,
                    },
                },
                (TradingState::Short, SignalType::Revert(signal_side)) => match signal_side {
                    SignalSide::Both => !prev_state,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => TradingState::Long,
                        Direction::Long => prev_state,
                    },
                },
                (TradingState::Long, SignalType::Close(signal_side)) => match signal_side {
                    SignalSide::Both => TradingState::Neutral,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => prev_state,
                        Direction::Long => TradingState::Neutral,
                    },
                },
                (TradingState::Long, SignalType::Revert(signal_side)) => match signal_side {
                    SignalSide::Both => !prev_state,
                    SignalSide::Side(direction) => match direction {
                        Direction::Short => prev_state,
                        Direction::Long => TradingState::Short,
                    },
                },
            },
        }
    }

    pub fn on_action(prev_state: TradingState, action: Action) -> TradingState {
        match prev_state {
            TradingState::Neutral => match action {
                Action::Signal(signal_type) => {
                    if let SignalType::Open(direction) = signal_type {
                        match direction {
                            Direction::Short => TradingState::Short,
                            Direction::Long => TradingState::Long,
                        }
                    } else {
                        prev_state
                    }
                }
                _ => TradingState::Neutral,
            },
            TradingState::Short => match action {
                Action::KeepPosition => prev_state,
                Action::Signal(signal_type) => match signal_type {
                    SignalType::Open(_) => prev_state,
                    SignalType::Close(signal_side) => match signal_side {
                        SignalSide::Both => TradingState::Neutral,
                        SignalSide::Side(direction) => match direction {
                            Direction::Short => TradingState::Neutral,
                            Direction::Long => prev_state,
                        },
                    },
                    SignalType::Revert(signal_side) => match signal_side {
                        SignalSide::Both => TradingState::Long,
                        SignalSide::Side(direction) => match direction {
                            Direction::Short => TradingState::Long,
                            Direction::Long => prev_state,
                        },
                    },
                },
                Action::Event(_) => TradingState::Neutral,
            },
            TradingState::Long => match action {
                Action::KeepPosition => prev_state,
                Action::Signal(signal_type) => match signal_type {
                    SignalType::Open(_) => prev_state,
                    SignalType::Close(signal_side) => match signal_side {
                        SignalSide::Both => TradingState::Neutral,
                        SignalSide::Side(direction) => match direction {
                            Direction::Short => prev_state,
                            Direction::Long => TradingState::Neutral,
                        },
                    },
                    SignalType::Revert(signal_side) => match signal_side {
                        SignalSide::Both => TradingState::Short,
                        SignalSide::Side(direction) => match direction {
                            Direction::Short => prev_state,
                            Direction::Long => TradingState::Short,
                        },
                    },
                },
                Action::Event(_) => TradingState::Neutral,
            },
        }
    }
}
