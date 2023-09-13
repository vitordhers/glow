#[derive(Debug, Clone)]
pub enum Leverage {
    Isolated(i32),
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
