use polars::prelude::PolarsError;
use reqwest::Error as ReqwestError;
use serde_json::Error as SerdeError;
use serde_urlencoded::ser::Error as UrlEncodedError;
use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;
use std::num::ParseIntError;
use std::string::FromUtf8Error;
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
    CustomError(CustomError),
    UrlEncodedError(UrlEncodedError),
    FromUtf8Error(FromUtf8Error),
    ParseIntError(ParseIntError),
    IoError(IoError),
}
#[derive(Debug)]
pub struct CustomError {
    pub message: String,
}

impl CustomError {
    pub fn new(message: String) -> Self {
        CustomError { message }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl StdError for CustomError {}
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
        println!("Reqwest Error = {}", reqwest_error.to_string());
        Error::ReqwestError(reqwest_error)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(from_utf8_error: FromUtf8Error) -> Self {
        println!("From UTF-8 Error = {:?}", from_utf8_error.to_string());
        Error::FromUtf8Error(from_utf8_error)
    }
}

impl From<UrlEncodedError> for Error {
    fn from(url_encoded_error: UrlEncodedError) -> Self {
        println!("Url Encoded Error = {:?}", url_encoded_error.to_string());
        Error::UrlEncodedError(url_encoded_error)
    }
}

impl From<CustomError> for Error {
    fn from(custom_error: CustomError) -> Self {
        println!("Reqwest Error = {:?}", custom_error.to_string());
        Error::CustomError(custom_error)
    }
}

impl From<ParseIntError> for Error {
    fn from(parse_int_error: ParseIntError) -> Self {
        println!("Parse Int Error = {:?}", parse_int_error.to_string());
        Error::ParseIntError(parse_int_error)
    }
}

impl From<IoError> for Error {
    fn from(io_error: IoError) -> Self {
        println!("Io Error = {:?}", io_error.to_string());
        Error::IoError(io_error)
    }
}
