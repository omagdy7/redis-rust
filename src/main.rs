#![allow(unused_imports)]
use core::time;
use std::{
    collections::HashMap,
    env,
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use codecrafters_redis::{
    rdb::{KeyExpiry, ParseError, RDBFile, RedisValue},
    resp_bytes,
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

use base64::{engine::general_purpose, Engine as _};

fn write_rdb_to_stream<W: Write>(writer: &mut W) -> Result<(), Box<dyn std::error::Error>> {
    let hardcoded_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    let bytes = general_purpose::STANDARD.decode(hardcoded_rdb)?;

    let mut response = format!("${}\r\n", bytes.len()).into_bytes();
    response.extend_from_slice(&bytes);

    // Write the binary RDB data
    writer.write_all(&response)?;

    Ok(())
}

// TODO: This should return a Result to handle the plethora of different errors
fn handle_client(mut stream: TcpStream, cache: SharedCache, config: SharedConfig) {
    let mut buffer = [0; 512];

    loop {
        let _ = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let request = parse(&buffer).unwrap();
        let response =
            RedisCommands::from(request.0.clone()).execute(cache.clone(), config.clone());

        let mut request_command = "".to_string();

        // FIXME: Find a solution for this mess!!
        match &request.0 {
            RespType::Array(arr) => {
                if let RespType::BulkString(s) = arr[0].clone() {
                    request_command = String::from_utf8(s).unwrap();
                }
            }
            _ => {}
        }

        // if this true immediately write and send back rdb file after response
        // HACK: This just feels wrong I feel this shouldn't be handled here and should be handled
        // in the exexute command
        if request_command.starts_with("PSYNC") {
            stream.write(&response).unwrap();
            let _ = write_rdb_to_stream(&mut stream);
        } else {
            // write respose back to the client
            stream.write(&response).unwrap();
        }
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
