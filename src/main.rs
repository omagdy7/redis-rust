#![allow(unused_imports)]
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

mod resp_commands;
mod resp_parser;

use resp_commands::RedisCommands;
use resp_parser::{parse, RespType};

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

pub type SharedCache = Arc<Mutex<HashMap<String, CacheEntry>>>;

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

fn handle_client(mut stream: TcpStream, cache: SharedCache) {
    let mut buffer = [0; 512];

    loop {
        let _ = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let parsed_resp = parse(&buffer).unwrap();
        let response = RedisCommands::from(parsed_resp.0).execute(cache.clone());

        // write respose back to the client
        stream.write(&response).unwrap();
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));

    spawn_cleanup_thread(cache.clone());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cache_clone = cache.clone();
                thread::spawn(|| {
                    handle_client(stream, cache_clone);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
