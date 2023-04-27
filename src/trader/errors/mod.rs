use polars::prelude::PolarsError;
use reqwest::Error as ReqwestError;
use serde_json::Error as SerdeError;
use std::{env::VarError, num::ParseFloatError};
use tokio_tungstenite::tungstenite::Error as TugsteniteError;
use url::ParseError as UrlParseError;

#[derive(Debug)]
pub enum Error {
    VarError(VarError),
    UrlParseError(UrlParseError),
    TugsteniteError(TugsteniteError),
    SerdeError(SerdeError),
    ParseFloatError(ParseFloatError),
    PolarsError(PolarsError),
    ReqwestError(ReqwestError),
}

impl From<VarError> for Error {
    fn from(var_error: VarError) -> Self {
        println!("Var Error = {:?}", var_error.to_string());
        Error::VarError(var_error)
    }
}

impl From<UrlParseError> for Error {
    fn from(parse_error: UrlParseError) -> Self {
        println!("Parse Error = {:?}", parse_error.to_string());
        Error::UrlParseError(parse_error)
    }
}

impl From<TugsteniteError> for Error {
    fn from(tungstenite_error: TugsteniteError) -> Self {
        println!("Tungstenite Error = {:?}", tungstenite_error.to_string());
        Error::TugsteniteError(tungstenite_error)
    }
}

impl From<SerdeError> for Error {
    fn from(serde_error: SerdeError) -> Self {
        println!("Serde Error = {:?}", serde_error.to_string());
        Error::SerdeError(serde_error)
    }
}

impl From<ParseFloatError> for Error {
    fn from(parse_float_error: ParseFloatError) -> Self {
        println!("Parse Float Error = {:?}", parse_float_error.to_string());
        Error::ParseFloatError(parse_float_error)
    }
}

impl From<PolarsError> for Error {
    fn from(polars_error: PolarsError) -> Self {
        println!("Polars Error = {:?}", polars_error.to_string());
        Error::PolarsError(polars_error)
    }
}

impl From<ReqwestError> for Error {
    fn from(reqwest_error: ReqwestError) -> Self {
        println!("Reqwest Error = {:?}", reqwest_error.to_string());
        Error::ReqwestError(reqwest_error)
    }
}
