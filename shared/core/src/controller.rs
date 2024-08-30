use crate::{data_feed::DataFeed, trader::Trader};

use super::performance::Performance;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct Controller {
    data_feed: Arc<Mutex<DataFeed>>,
    performance: Arc<Mutex<Performance>>,
    trader: Arc<Mutex<Trader>>,
}

impl Controller {
    pub fn new(
        data_feed: &Arc<Mutex<DataFeed>>,
        performance: &Arc<Mutex<Performance>>,
        trader: Arc<Mutex<Trader>>,
    ) -> Self {
        Self {
            data_feed: data_feed.clone(),
            performance: performance.clone(),
            trader: trader.clone(),
        }
    }

    pub async fn init(&self) {
        // init core modules
    }
}
