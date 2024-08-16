use crate::{
    enums::symbol_id::SymbolId,
    r#static::{get_default_symbol, SYMBOLS_MAP},
};
use serde::{
    de::{Deserializer, Error, IgnoredAny, MapAccess, Visitor},
    ser::{Serialize, SerializeStruct, Serializer},
    {Deserialize, Serialize as DerivedSerialize},
};
use std::{collections::HashSet, fmt};

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
    // pub fn new(name: &'static str) -> Self {
    //     let open = format!("{}_open", name);
    //     let high = format!("{}_high", name);
    //     let low = format!("{}_low", name);
    //     let close = format!("{}_close", name);

    //     Self {
    //         name,
    //         open: Box::leak(open.into_boxed_str()),
    //         high: Box::leak(high.into_boxed_str()),
    //         low: Box::leak(low.into_boxed_str()),
    //         close: Box::leak(close.into_boxed_str()),
    //     }
    // }

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

#[derive(Clone, Copy, Debug)]
pub struct SymbolsPair {
    pub anchor: &'static Symbol,
    pub traded: &'static Symbol,
}

impl SymbolsPair {
    pub fn new(anchor_symbol_id: &SymbolId, traded_symbol_id: &SymbolId) -> Self {
        let anchor_symbol_str = anchor_symbol_id.get_symbol_str();
        let anchor = SYMBOLS_MAP.get(anchor_symbol_str).expect(&format!(
            "Anchor symbol {} to exist at SYMBOLS_MAP",
            anchor_symbol_str
        ));
        let traded_symbol_str = traded_symbol_id.get_symbol_str();
        let traded = SYMBOLS_MAP.get(traded_symbol_str).expect(&format!(
            "Traded symbol {} to exist at SYMBOLS_MAP",
            traded_symbol_str
        ));
        Self { anchor, traded }
    }

    pub fn get_tuple(&self) -> (&'static str, &'static str) {
        (&self.anchor.name, &self.traded.name)
    }

    pub fn get_unique_symbols(&self) -> Vec<&'static Symbol> {
        let mut symbols = HashSet::new();
        symbols.insert(self.anchor);
        symbols.insert(self.traded);
        symbols.into_iter().collect()
    }
}

impl Default for SymbolsPair {
    fn default() -> Self {
        let default_symbol = get_default_symbol();

        Self {
            anchor: default_symbol,
            traded: default_symbol,
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
