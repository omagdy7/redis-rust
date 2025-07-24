use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: String,
    pub expires_at: Option<u64>, // Unix timestamp in milliseconds
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            now > expiry
        } else {
            false
        }
    }
}

pub type Cache = HashMap<String, CacheEntry>;
