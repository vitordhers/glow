// use std::fmt;
// use thiserror::Error as ThisError;
//
// #[derive(Debug, ThisError)]
// pub enum OptimizationError {
//     #[error("Invalid param type")]
//     InvalidParamType(String),
// }
//
// impl fmt::Display for OptimizationError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "Optimization failed: {}", self.details)
//     }
// }
//
// impl std::error::Error for OptimizationError {}
