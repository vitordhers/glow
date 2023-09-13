use crate::trader::errors::Error;

pub fn calculate_span_alpha(span: f64) -> Result<f64, Error> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}
