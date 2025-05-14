// use polars::prelude::{DataType, Schema, TimeUnit};
// use serde::{
//     de::{Deserializer, Error, IgnoredAny, MapAccess, Visitor},
//     ser::{Serialize as CustomSerialize, SerializeStruct, Serializer},
//     {Deserialize, Serialize},
// };
//
// #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
// pub struct Currency<'a> {
//     symbol: &'a str,
//     name: &'a str,
// }
//
// #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
// pub struct Market<'a> {
//     pub base: &'a Currency<'a>,
//     pub quote: &'a Currency<'a>,
// }
//
// pub struct MarketColumns<'a> {
//     pub open: &'a str,
//     pub high: &'a str,
//     pub low: &'a str,
//     pub close: &'a str,
// }
//
// impl<'a> MarketColumns<'a> {
//     fn new(open: &'a str, high: &'a str, low: &'a str, close: &'a str) -> Self {
//         Self {
//             open,
//             high,
//             low,
//             close,
//         }
//     }
// }
//
// #[derive(Default, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
// pub enum Separator {
//     #[default]
//     #[serde(rename = "")]
//     None,
//     #[serde(rename = "-")]
//     Dash,
//     #[serde(rename = "_")]
//     Underscore,
//     #[serde(rename = ".")]
//     Dot,
// }
//
// impl<'a> Market<'a> {
//     pub fn into_tick_data_schema(&self, time_unit: Option<TimeUnit>) -> Schema {
//         let mut schema = Schema::default();
//         let _ = schema.insert_at_index(
//             0,
//             "start_time".into(),
//             DataType::Datetime(time_unit.unwrap_or(TimeUnit::Milliseconds), None),
//         );
//         let columns: MarketColumns = self.clone().into();
//         let MarketColumns {
//             open,
//             high,
//             low,
//             close,
//         } = columns;
//         let _ = schema.insert_at_index(1, open.into(), DataType::Float64);
//         let _ = schema.insert_at_index(2, high.into(), DataType::Float64);
//         let _ = schema.insert_at_index(3, low.into(), DataType::Float64);
//         let _ = schema.insert_at_index(4, close.into(), DataType::Float64);
//         schema
//     }
// }
//
// impl<'a> From<Market<'a>> for MarketColumns<'a> {
//     fn from(value: Market<'a>) -> Self {
//         MarketColumns::new(value.open, value.high, value.low, value.close)
//     }
// }
//
// impl<'a> From<Market<'a>> for Schema {
//     fn from(value: Market<'a>) -> Self {
//         value.into_tick_data_schema(None)
//     }
// }

// #[derive(Clone, Copy)]
// pub struct SymbolsPair {
//     pub base: &'static Symbol,
//     pub quote: &'static Symbol,
// }
//
// impl SymbolsPair {
//     pub fn new(quote_symbol_id: &SymbolId, base_symbol_id: &SymbolId) -> Self {
//         assert!(
//             quote_symbol_id != base_symbol_id,
//             "Base and Quote ids must be different!"
//         );
//         let quote = quote_symbol_id.get_symbol_str();
//         let quote = SYMBOLS_MAP
//             .get(quote)
//             .unwrap_or_else(|| panic!("Quote symbol {} to exist at SYMBOLS_MAP", quote));
//         let base = base_symbol_id.get_symbol_str();
//         let base = SYMBOLS_MAP
//             .get(base)
//             .unwrap_or_else(|| panic!("Base symbol {} to exist at SYMBOLS_MAP", base));
//         Self { base, quote }
//     }
//
//     pub fn get_tuple(&self) -> (&'static str, &'static str) {
//         (self.quote.name, self.base.name)
//     }
//
//     pub fn get_unique_symbols(&self) -> (&'static Symbol, &'static Symbol) {
//         (self.base, self.quote)
//     }
// }
//
// impl Default for SymbolsPair {
//     fn default() -> Self {
//         let default_symbol = get_default_symbol();
//
//         Self {
//             quote: default_symbol,
//             base: default_symbol,
//         }
//     }
// }
//
// impl Serialize for SymbolsPair {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let mut state = serializer.serialize_struct("SymbolsPair", 2)?;
//         state.serialize_field("quote", &self.quote.name)?;
//         state.serialize_field("base", &self.base.name)?;
//         state.end()
//     }
// }
//
// impl<'de> Deserialize<'de> for SymbolsPair {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         struct SymbolsPairVisitor;
//
//         impl<'de> Visitor<'de> for SymbolsPairVisitor {
//             type Value = SymbolsPair;
//
//             fn expecting(&self, formatter: &mut Formatter) -> FormatterResult {
//                 formatter.write_str("a struct representing a pair of symbols")
//             }
//
//             fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
//             where
//                 V: MapAccess<'de>,
//             {
//                 let mut quote: Option<String> = None;
//                 let mut base: Option<String> = None;
//
//                 while let Some(key) = map.next_key::<String>()? {
//                     match key.as_str() {
//                         "quote" => {
//                             quote = Some(map.next_value()?);
//                         }
//                         "base" => {
//                             base = Some(map.next_value()?);
//                         }
//                         _ => {
//                             let _: IgnoredAny = map.next_value()?;
//                         }
//                     }
//                 }
//
//                 let quote = quote.ok_or_else(|| Error::missing_field("quote"))?;
//                 let base = base.ok_or_else(|| Error::missing_field("base"))?;
//
//                 let quote = SYMBOLS_MAP
//                     .get(quote.as_str())
//                     .ok_or_else(|| Error::custom("Invalid quote symbol"))?;
//                 let base = SYMBOLS_MAP
//                     .get(base.as_str())
//                     .ok_or_else(|| Error::custom("Invalid base symbol"))?;
//
//                 Ok(SymbolsPair { base, quote })
//             }
//         }
//
//         deserializer.deserialize_struct("SymbolsPair", &["quote", "base"], SymbolsPairVisitor)
//     }
// }
//
// impl Debug for SymbolsPair {
//     fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
//         write!(
//             f,
//             "⚓ Quote Symbol {}, 📈 Base Symbol {}",
//             self.quote.name, self.base.name
//         )
//     }
// }
