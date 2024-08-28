use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter, Result as DebugResult};

#[derive(Serialize, Deserialize, Clone)]
pub enum Leverage {
    #[serde(rename = "iso")]
    Isolated(i32),
    #[serde(rename = "cross")]
    Cross(i32),
}

impl Default for Leverage {
    fn default() -> Self {
        Self::Isolated(1)
    }
}

impl Leverage {
    pub fn get_factor(&self) -> f64 {
        match self {
            Leverage::Isolated(factor) => *factor as f64,
            Leverage::Cross(factor) => *factor as f64,
        }
    }
}

impl Debug for Leverage {
    fn fmt(&self, f: &mut Formatter<'_>) -> DebugResult {
        let str = match self {
            Self::Isolated(factor) => &format!("⭕️ Isolated {}x", factor),
            Self::Cross(factor) => &format!("❌ Cross {}x", factor),
        };
        write!(f, "{}", str)
    }
}
