use polars::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, RwLock};

// pub struct FifoCache {
//     map: HashMap<String, Series>,
//     order: VecDeque<String>,
//     capacity: usize,
// }
//
// impl FifoCache {
//     fn new(capacity: usize) -> Self {
//         Self {
//             map: HashMap::new(),
//             order: VecDeque::new(),
//             capacity,
//         }
//     }
//
//     fn insert(&mut self, key: String, value: Series) {
//         if !self.map.contains_key(&key) {
//             if self.order.len() == self.capacity {
//                 // Evict the oldest
//                 if let Some(old_key) = self.order.pop_front() {
//                     self.map.remove(&old_key);
//                 }
//             }
//             self.order.push_back(key.clone());
//         }
//         self.map.insert(key, value);
//     }
//
//     fn get(&self, key: &str) -> Option<&Series> {
//         self.map.get(key)
//     }
// }

#[derive(Clone, Debug)]
pub struct MaxReadsCache {
    pub cache: Arc<RwLock<HashMap<u32, Arc<Series>>>>,
    pub reads_count: Arc<Mutex<HashMap<u32, u32>>>,
    pub max_reads_before_eviction: u32,
}

impl MaxReadsCache {
    pub fn new(max_reads_before_eviction: u32) -> Self {
        Self {
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

    pub fn insert<'a>(&self, key: u32, series: &'a Series) -> &'a Series {
        let mut cache = self.cache.write().unwrap();
        cache.insert(key, Arc::new(series.clone()));
        let mut counts_map = self.reads_count.lock().unwrap();
        counts_map.insert(key, 1);
        series
    }
}
