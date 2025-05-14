use crate::{
    enums::symbol_id::SymbolId,
    r#static::{get_default_symbol, SYMBOLS_MAP},
};
use polars::prelude::{DataType, Schema, TimeUnit};
use serde::{
    de::{Deserializer, Error, IgnoredAny, MapAccess, Visitor},
    ser::{Serialize, SerializeStruct, Serializer},
    {Deserialize, Serialize as DerivedSerialize},
};
use std::fmt::{Debug, Formatter, Result as FormatterResult};

#[derive(DerivedSerialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Symbol {
    pub id: SymbolId,
    pub name: &'static str,
    pub open: &'static str,
    pub high: &'static str,
    pub low: &'static str,
    pub close: &'static str,
}

impl Symbol {
    pub fn get_open_col(&self) -> &'static str {
        self.open
    }
    pub fn get_high_col(&self) -> &'static str {
        self.high
    }
    pub fn get_low_col(&self) -> &'static str {
        self.low
    }
    pub fn get_close_col(&self) -> &'static str {
        self.close
    }

    pub fn get_ohlc_cols(&self) -> (&'static str, &'static str, &'static str, &'static str) {
        (self.open, self.high, self.low, self.close)
    }

    pub fn derive_symbol_tick_data_schema(&self) -> Schema {
        let mut schema = Schema::default();
        let _ = schema.insert_at_index(
            0,
            "start_time".into(),
            DataType::Datetime(TimeUnit::Milliseconds, None),
        );
        let (open, high, low, close) = self.get_ohlc_cols();
        let _ = schema.insert_at_index(1, open.into(), DataType::Float64);
        let _ = schema.insert_at_index(2, high.into(), DataType::Float64);
        let _ = schema.insert_at_index(3, low.into(), DataType::Float64);
        let _ = schema.insert_at_index(4, close.into(), DataType::Float64);
        schema
    }
}

#[derive(Clone, Copy)]
pub struct SymbolsPair {
    pub base: &'static Symbol,
    pub quote: &'static Symbol,
}

impl SymbolsPair {
    pub fn new(quote_symbol_id: &SymbolId, base_symbol_id: &SymbolId) -> Self {
        assert!(
            quote_symbol_id != base_symbol_id,
            "Base and Quote ids must be different!"
        );
        let quote = quote_symbol_id.get_symbol_str();
        let quote = SYMBOLS_MAP
            .get(quote)
            .unwrap_or_else(|| panic!("Quote symbol {} to exist at SYMBOLS_MAP", quote));
        let base = base_symbol_id.get_symbol_str();
        let base = SYMBOLS_MAP
            .get(base)
            .unwrap_or_else(|| panic!("Base symbol {} to exist at SYMBOLS_MAP", base));
        Self { base, quote }
    }

    pub fn get_tuple(&self) -> (&'static str, &'static str) {
        (self.quote.name, self.base.name)
    }

    pub fn get_unique_symbols(&self) -> [&'static Symbol; 2] {
        [self.base, self.quote]
    }
}

impl Default for SymbolsPair {
    fn default() -> Self {
        let default_symbol = get_default_symbol();

        Self {
            quote: default_symbol,
            base: default_symbol,
        }
    }
}

impl Serialize for SymbolsPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SymbolsPair", 2)?;
        state.serialize_field("quote", &self.quote.name)?;
        state.serialize_field("base", &self.base.name)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SymbolsPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SymbolsPairVisitor;

        impl<'de> Visitor<'de> for SymbolsPairVisitor {
            type Value = SymbolsPair;

            fn expecting(&self, formatter: &mut Formatter) -> FormatterResult {
                formatter.write_str("a struct representing a pair of symbols")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut quote: Option<String> = None;
                let mut base: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "quote" => {
                            quote = Some(map.next_value()?);
                        }
                        "base" => {
                            base = Some(map.next_value()?);
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let quote = quote.ok_or_else(|| Error::missing_field("quote"))?;
                let base = base.ok_or_else(|| Error::missing_field("base"))?;

                let quote = SYMBOLS_MAP
                    .get(quote.as_str())
                    .ok_or_else(|| Error::custom("Invalid quote symbol"))?;
                let base = SYMBOLS_MAP
                    .get(base.as_str())
                    .ok_or_else(|| Error::custom("Invalid base symbol"))?;

                Ok(SymbolsPair { base, quote })
            }
        }

        deserializer.deserialize_struct("SymbolsPair", &["quote", "base"], SymbolsPairVisitor)
    }
}

impl Debug for SymbolsPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(
            f,
            "⚓ Quote Symbol {}, 📈 Base Symbol {}",
            self.quote.name, self.base.name
        )
    }
}
