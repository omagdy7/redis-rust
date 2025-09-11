use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use tracing::error;

use crate::shared_cache::Cache;
use crate::types::SharedMut;

// responsible for periodically removing expired keys from database
pub fn spawn_cleanup_task(cache: SharedMut<Cache>) {
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await; // Wait for the next tick (10 seconds)

            let mut cache = cache_clone.lock().await;
            let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_millis() as u64,
                Err(e) => {
                    error!("System time error: {}", e);
                    continue;
                }
            };

            // Remove expired keys
            cache.retain(|_, entry| entry.expires_at.map_or(true, |expiry| now <= expiry));
        }
    });
}
