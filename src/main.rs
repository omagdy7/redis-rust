#![allow(unused_imports)]
use std::{
    collections::HashMap,
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use codecrafters_redis::shared_cache::*;
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
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));
    let mut config: SharedConfig = Arc::new(None);

    spawn_cleanup_thread(cache.clone());

    match Config::new() {
        Ok(conf) => {
            if let Some(conf) = conf {
                config = Arc::new(Some(conf));
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

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
