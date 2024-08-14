use polars::prelude::PolarsError;
use reqwest::Error as ReqwestError;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_urlencoded::ser::Error as UrlEncodedError;
use std::{
    env::VarError,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    num::{ParseFloatError, ParseIntError},
    string::FromUtf8Error,
};
use tokio_tungstenite::tungstenite::Error as TugsteniteError;
use url::ParseError as UrlParseError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlowError {
    pub title: String,
    pub description: String,
}

impl GlowError {
    pub fn new(title: String, description: String) -> Self {
        Self { title, description }
    }

    pub fn new_unsuccessful_response(description: String) -> Self {
        Self {
            title: String::from("Unsuccessful Response"),
            description,
        }
    }

    pub fn new_assert_error<T: Display>(assertion: T) -> Self {
        Self {
            title: String::from("Assert Error"),
            description: format!("{} not valid!", assertion),
        }
    }
}

impl Display for GlowError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:/n/n{}", self.title, self.description)
    }
}

impl Error for GlowError {}

impl From<VarError> for GlowError {
    fn from(error: VarError) -> Self {
        Self::new(String::from("Var Error"), error.to_string())
    }
}

impl From<UrlParseError> for GlowError {
    fn from(error: UrlParseError) -> Self {
        Self::new(String::from("Url Parse Error"), error.to_string())
    }
}

impl From<TugsteniteError> for GlowError {
    fn from(error: TugsteniteError) -> Self {
        Self::new(String::from("Tungstenite Error"), error.to_string())
    }
}

impl From<SerdeError> for GlowError {
    fn from(error: SerdeError) -> Self {
        Self::new(String::from("Serde Error"), error.to_string())
    }
}

impl From<ParseFloatError> for GlowError {
    fn from(error: ParseFloatError) -> Self {
        Self::new(String::from("Parse Float Error"), error.to_string())
    }
}

impl From<PolarsError> for GlowError {
    fn from(error: PolarsError) -> Self {
        Self::new(String::from("Polars Error"), error.to_string())
    }
}

impl From<ReqwestError> for GlowError {
    fn from(error: ReqwestError) -> Self {
        Self::new(String::from("Reqwest Error"), error.to_string())
    }
}

impl From<FromUtf8Error> for GlowError {
    fn from(error: FromUtf8Error) -> Self {
        Self::new(String::from("From UTF-8 Error"), error.to_string())
    }
}

impl From<UrlEncodedError> for GlowError {
    fn from(error: UrlEncodedError) -> Self {
        Self::new(String::from("Url Encoded Error"), error.to_string())
    }
}

impl From<ParseIntError> for GlowError {
    fn from(error: ParseIntError) -> Self {
        Self::new(String::from("Parse Int Error"), error.to_string())
    }
}

impl From<IoError> for GlowError {
    fn from(error: IoError) -> Self {
        Self::new(String::from("I/O Error"), error.to_string())
    }
}

#[macro_export]
macro_rules! assert_or_error {
    ($cond:expr) => {
        if !$cond {
            return Err(GlowError::new_assert_error(stringify!($cond)));
        }
    };
}

#[macro_export]
macro_rules! assert_or_none {
    ($cond:expr) => {
        if !$cond {
            return None;
        }
    };
}

#[test]
fn test_macro() {
    fn return_result() -> Result<(), GlowError> {
        assert_or_error!(2 < 1);
        Ok(())
    }

    let result = return_result();
    println!("RESULT = {:?}", result);
}
