#[allow(dead_code)]
#[derive(Clone)]
pub struct Performance {
    success_rate: Option<u8>,
}

#[allow(dead_code)]
impl Performance {
    pub fn new(success_rate: u8) -> Self {
        Self {
            success_rate: Some(success_rate),
        }
    }
}

impl Default for Performance {
    fn default() -> Self {
        Self { success_rate: None }
    }
}
