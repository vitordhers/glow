use crate::r#static::{SYMBOLS_LIST, SYMBOLS_MAP};
use serde::{
    de::{Deserializer, Error, IgnoredAny, MapAccess, Visitor},
    ser::{Serialize, SerializeStruct, Serializer},
    {Deserialize, Serialize as DerivedSerialize},
};
use std::fmt;

#[derive(DerivedSerialize, Deserialize, Clone, Copy, Debug)]
pub struct Symbol {
    pub name: &'static str,
    pub open: &'static str,
    pub high: &'static str,
    pub low: &'static str,
    pub close: &'static str,
}

impl Symbol {
    pub fn new(name: &'static str) -> Self {
        let open = format!("{}_open", name);
        let high = format!("{}_high", name);
        let low = format!("{}_low", name);
        let close = format!("{}_close", name);

        Self {
            name,
            open: Box::leak(open.into_boxed_str()),
            high: Box::leak(high.into_boxed_str()),
            low: Box::leak(low.into_boxed_str()),
            close: Box::leak(close.into_boxed_str()),
        }
    }

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
}

impl Default for Symbol {
    fn default() -> Self {
        let name = SYMBOLS_LIST.get(0).unwrap();
        let open = format!("{}_open", name);
        let high = format!("{}_high", name);
        let low = format!("{}_low", name);
        let close = format!("{}_close", name);

        Self {
            name,
            open: Box::leak(open.into_boxed_str()),
            high: Box::leak(high.into_boxed_str()),
            low: Box::leak(low.into_boxed_str()),
            close: Box::leak(close.into_boxed_str()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SymbolsPair {
    pub anchor: &'static Symbol,
    pub traded: &'static Symbol,
}

impl SymbolsPair {
    pub fn new(anchor_symbol: &str, traded_symbol: &str) -> Self {
        let anchor = SYMBOLS_MAP.get(anchor_symbol).expect(&format!(
            "Anchor symbol {} to exist at SYMBOLS_MAP",
            anchor_symbol
        ));
        let traded = SYMBOLS_MAP.get(traded_symbol).expect(&format!(
            "Traded symbol {} to exist at SYMBOLS_MAP",
            traded_symbol
        ));
        Self { anchor, traded }
    }
}

impl Default for SymbolsPair {
    fn default() -> Self {
        let default_symbol = Symbol::default();
        let default_symbol = SYMBOLS_MAP.get(default_symbol.name).expect(&format!(
            "Default symbol {} to exist at SYMBOLS_MAP",
            default_symbol.name
        ));

        Self {
            anchor: &default_symbol,
            traded: &default_symbol,
        }
    }
}

impl Serialize for SymbolsPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SymbolsPair", 2)?;
        state.serialize_field("anchor", &self.anchor.name)?;
        state.serialize_field("traded", &self.traded.name)?;
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

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a struct representing a pair of symbols")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut anchor: Option<String> = None;
                let mut traded: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "anchor" => {
                            anchor = Some(map.next_value()?);
                        }
                        "traded" => {
                            traded = Some(map.next_value()?);
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let anchor_str = anchor.ok_or_else(|| Error::missing_field("anchor"))?;
                let traded_str = traded.ok_or_else(|| Error::missing_field("traded"))?;

                let anchor_ref = SYMBOLS_MAP
                    .get(anchor_str.as_str())
                    .ok_or_else(|| Error::custom("Invalid anchor symbol"))?;
                let traded_ref = SYMBOLS_MAP
                    .get(traded_str.as_str())
                    .ok_or_else(|| Error::custom("Invalid traded symbol"))?;

                Ok(SymbolsPair {
                    anchor: anchor_ref,
                    traded: traded_ref,
                })
            }
        }

        deserializer.deserialize_struct("SymbolsPair", &["anchor", "traded"], SymbolsPairVisitor)
    }
}
