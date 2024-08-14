use chrono::NaiveDateTime;
use std::fmt;
use std::io::{stdout, Write};

#[derive(Debug, Clone, PartialEq)]
pub struct TickData {
    pub symbol: &'static str,
    pub start_time: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub close: f64,
    pub low: f64,
}

impl TickData {
    pub fn new_from_string(
        symbol: &'static str,
        start_time: NaiveDateTime,
        open: f64,
        high: f64,
        close: f64,
        low: f64,
    ) -> Self {
        Self {
            symbol,
            start_time,
            open,
            high,
            close,
            low,
        }
    }
}

pub struct LogKlines(pub Vec<TickData>);

impl fmt::Display for LogKlines {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut binding = self.0.clone();
        binding.sort_by(|a, b| a.symbol.cmp(&b.symbol));

        let mut handle = stdout().lock();

        for (index, item) in binding.iter().enumerate() {
            if index > 0 {
                writeln!(
                    handle,
                    "                    | ✅ {} open: {}, high: {}, low: {}, close: {}",
                    item.symbol, item.open, item.high, item.low, item.close
                )
                .unwrap();
            } else {
                writeln!(
                    handle,
                    "{:?} | ✅ {} open: {}, high: {}, low: {}, close: {}",
                    item.start_time, item.symbol, item.open, item.high, item.low, item.close
                )
                .unwrap();
            }
        }

        // Move the cursor up two lines and clear each line
        // write!(handle, "\x1B[F\x1B[K\x1B[F\x1B[K").unwrap();

        // handle.flush().unwrap(); // Flush stdout to remove both lines

        Ok(())
    }
}
