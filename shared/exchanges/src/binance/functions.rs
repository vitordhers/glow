use super::dtos::ws::incoming::TickMessage;
use common::structs::TickData;

pub fn from_tick_to_tick_data(
    tick: TickMessage,
    symbols: &(&'static str, &'static str),
) -> TickData {
    TickData {
        symbol: if tick.symbol == symbols.0 {
            symbols.0
        } else {
            symbols.1
        },
        start_time: tick.data.start_time,
        open: tick.data.open,
        high: tick.data.high,
        close: tick.data.close,
        low: tick.data.low,
    }
}
