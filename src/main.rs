#![allow(unused_imports)]
use core::time;
use std::{
    collections::HashMap,
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use codecrafters_redis::{
    rdb::{KeyExpiry, ParseError, RDBFile, RedisValue},
    shared_cache::*,
};
use codecrafters_redis::{resp_commands::RedisCommands, Config};
use codecrafters_redis::{
    resp_parser::{parse, RespType},
    SharedConfig,
};

fn spawn_cleanup_thread(cache: SharedCache) {
    let cache_clone = cache.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(10)); // Check every 10 seconds

            let mut cache = cache_clone.lock().unwrap();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Remove expired keys
            cache.retain(|_, entry| entry.expires_at.map_or(true, |expiry| now <= expiry));
        }
    });
}

fn handle_client(mut stream: TcpStream, cache: SharedCache, config: SharedConfig) {
    let mut buffer = [0; 512];

    loop {
        let _ = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let parsed_resp = parse(&buffer).unwrap();
        let response = RedisCommands::from(parsed_resp.0).execute(cache.clone(), config.clone());

        // write respose back to the client
        stream.write(&response).unwrap();
    }
}

fn main() -> std::io::Result<()> {
    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));
    let mut config: SharedConfig = Arc::new(None);
    let mut port = "6379".to_string();

    match Config::new() {
        Ok(conf) => {
            if let Some(conf) = conf {
                let mut cache = cache.lock().unwrap();
                let dir = conf.dir.clone().unwrap_or("".to_string());
                let dbfilename = conf.dbfilename.clone().unwrap_or("".to_string());
                let redis_server = conf.server.clone();
                port = redis_server.port.clone();
                if let Ok(rdb_file) = RDBFile::read(dir, dbfilename) {
                    if let Some(rdb) = rdb_file {
                        let hash_table = &rdb.databases.get(&0).unwrap().hash_table;

                        for (key, db_entry) in hash_table.iter() {
                            let value = match &db_entry.value {
                                RedisValue::String(data) => {
                                    String::from_utf8(data.clone()).unwrap()
                                }
                                RedisValue::Integer(data) => data.to_string(),
                                _ => {
                                    unreachable!()
                                }
                            };
                            let expires_at = if let Some(key_expiry) = &db_entry.expiry {
                                Some(key_expiry.timestamp)
                            } else {
                                None
                            };
                            let cache_entry = CacheEntry { value, expires_at };
                            cache.insert(String::from_utf8(key.clone()).unwrap(), cache_entry);
                        }
                    }
                }
                config = Arc::new(Some(conf));
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    spawn_cleanup_thread(cache.clone());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cache_clone = cache.clone();
                let config_clone = Arc::clone(&config);
                thread::spawn(|| {
                    handle_client(stream, cache_clone, config_clone);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
