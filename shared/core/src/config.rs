use chrono::{DateTime, Utc};
use common::functions::current_datetime;
use exchanges::enums::{DataProviderExchangeId, TraderExchangeId};
use glow_error::GlowError;
use serde::{Deserialize, Serialize};
use serde_json::{from_reader, to_writer};
use std::{
    env::args,
    fs::File,
    io::{BufReader, Result as IoResult},
};
use strategy::StrategyId;

#[derive(Clone, Serialize, Deserialize)]
pub struct BenchmarkSettings {
    pub datetimes: (Option<DateTime<Utc>>, Option<DateTime<Utc>>),
    pub strategy_id: StrategyId,
    pub data_provider_id: DataProviderExchangeId,
    pub trader_exchange_id: TraderExchangeId,
}

impl BenchmarkSettings {
    fn get_config_file_path() -> Result<String, GlowError> {
        let args: Vec<String> = args().collect();

        match args.first() {
            Some(member) => {
                let member = member.split("/").last().unwrap();
                Ok(format!("config/{}/benchmark_settings.json", member))
            }
            _ => Err(GlowError::new(
                "Invalid -p flag".to_owned(),
                "Invalid -p flag".to_owned(),
            )),
        }
    }

    pub fn load_or_default() -> Self {
        let file_result = File::open(Self::get_config_file_path().unwrap_or_default());
        if file_result.is_err() {
            return Self::default();
        }
        let file = file_result.unwrap();
        let reader = BufReader::new(file);
        from_reader(reader).unwrap_or_default()
    }

    pub fn save_config(&self) -> IoResult<()> {
        let file = File::create(Self::get_config_file_path().unwrap_or_default())?;
        to_writer(file, self)?;
        Ok(())
    }
}

impl Default for BenchmarkSettings {
    fn default() -> Self {
        Self {
            datetimes: (None::<DateTime<Utc>>, Some(current_datetime())),
            strategy_id: StrategyId::default(),
            data_provider_id: DataProviderExchangeId::default(),
            trader_exchange_id: TraderExchangeId::default(),
        }
    }
}
