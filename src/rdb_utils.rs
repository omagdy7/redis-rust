use base64::{Engine as _, engine::general_purpose};
use tokio::io::AsyncWriteExt;
use tracing::error;

use crate::frame::Frame;
use crate::rdb::{RDBFile, RedisValue};
use crate::server::RedisServer;
use crate::shared_cache::CacheEntry;

pub async fn send_empty_rdb<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let hardcoded_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    let bytes = general_purpose::STANDARD.decode(hardcoded_rdb)?;

    let mut response = format!("${}\r\n", bytes.len()).into_bytes();
    response.extend_from_slice(&bytes);

    // Write the binary RDB data
    let _ = writer.write_all(&response).await;

    Ok(())
}

pub async fn load_rdb(server: &RedisServer) {
    // Load RDB file if dir and dbfilename are provided
    if let (Some(dir), Some(dbfilename)) = (server.dir().clone(), server.dbfilename().clone()) {
        if let Ok(rdb_file) = RDBFile::read(dir, dbfilename) {
            if let Some(rdb) = rdb_file {
                let mut cache = server.cache().lock().await;
                if let Some(db) = rdb.databases.get(&0) {
                    let hash_table = &db.hash_table;

                    for (key, db_entry) in hash_table.iter() {
                        let value = match &db_entry.value {
                            RedisValue::String(data) => Frame::RedisString(data.clone()),
                            RedisValue::Integer(data) => Frame::Integer(*data),
                            RedisValue::List(items) => Frame::RedisList(items.clone()),
                            RedisValue::Set(items) => Frame::RedisSet(items.clone()),
                            RedisValue::Hash(map) => Frame::RedisHash(map.clone()),
                        };
                        let expires_at = if let Some(key_expiry) = &db_entry.expiry {
                            Some(key_expiry.timestamp)
                        } else {
                            None
                        };
                        let cache_entry = CacheEntry { value, expires_at };
                        match String::from_utf8(key.to_vec()) {
                            Ok(key_str) => {
                                cache.insert(key_str, cache_entry);
                            }
                            Err(e) => {
                                error!("Failed to decode key: {}", e);
                                continue;
                            }
                        }
                    }
                } else {
                    error!("No database with index 0 found in RDB file");
                }
            }
        }
    }
}
