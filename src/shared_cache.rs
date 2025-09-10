use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: Frame,
    pub expires_at: Option<u64>, // Unix timestamp in milliseconds
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_millis() as u64,
                Err(_) => return false, // If we can't get time, assume not expired
            };
            now > expiry
        } else {
            false
        }
    }
}

pub type Cache = HashMap<String, CacheEntry>;
