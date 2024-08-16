use std::collections::HashMap;

use common::{enums::symbol_id::SymbolId, structs::Contract};

#[derive(Debug, Clone, Copy)]
pub struct ApiCredentials {
    pub key: &'static str,
    pub secret: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct ApiEndpoints {
    pub ws: &'static str,
    pub http: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct ExchangeConfig {
    pub credentials: ApiCredentials,
    pub endpoints: ApiEndpoints,
}

#[derive(Debug, Clone)]
pub struct ExchangeContext {
    pub taker_fee: f64,
    pub maker_fee: f64,
    pub contracts: HashMap<SymbolId, Contract>,
}
