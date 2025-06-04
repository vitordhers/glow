use glow_error::{poison, GlowError};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

#[derive(Clone, Debug)]
pub struct MaxReadsCache {
    pub feature_series: Series,
    pub cache: Arc<RwLock<HashMap<u32, Arc<Series>>>>,
    pub reads_count: Arc<Mutex<HashMap<u32, u32>>>,
    pub max_reads_before_eviction: u32,
}

impl MaxReadsCache {
    pub fn new(feature_series: &Series, max_reads_before_eviction: u32) -> Self {
        Self {
            feature_series: feature_series.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            reads_count: Arc::new(Mutex::new(HashMap::new())),
            max_reads_before_eviction,
        }
    }

    pub fn has_key(&self, key: &u32) -> bool {
        let cache = self.cache.read().unwrap();
        cache.contains_key(key)
    }

    pub fn get(&self, key: &u32) -> Option<Arc<Series>> {
        let cache = self.cache.read().unwrap();
        let series = cache.get(key);
        series?;
        let mut reads_counts_map = self.reads_count.lock().unwrap();
        let reads = reads_counts_map.get(key).copied().unwrap_or(0);
        if reads >= self.max_reads_before_eviction {
            let mut cache = self.cache.write().unwrap();
            cache.remove(key);
            reads_counts_map.remove(key);
        } else {
            reads_counts_map.insert(*key, reads + 1);
        }
        series.cloned()
    }

    pub fn insert(&self, key: u32, series: &Arc<Series>) -> Result<(), GlowError> {
        let mut cache = self.cache.write().map_err(poison)?;
        cache.insert(key, series.clone());
        let mut counts_map = self.reads_count.lock().map_err(poison)?;
        counts_map.insert(key, 1);
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub enum FeaturesCache {
    #[default]
    None,
    List(Series),
    Eager(HashMap<u32, Series>),
    LazyMaxReads(MaxReadsCache),
}
