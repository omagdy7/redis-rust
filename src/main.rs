#![allow(unused_imports)]
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

mod resp_commands;
mod resp_parser;

use resp_commands::RedisCommands;
use resp_parser::{parse, RespType};

pub type SharedCache = Arc<Mutex<HashMap<String, String>>>;

fn handle_client(mut stream: TcpStream, cache: SharedCache) {
    let mut buffer = [0; 512];
    loop {
        let bytes_read = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let parsed_resp = parse(&buffer).unwrap();
        let response = RedisCommands::from(parsed_resp.0).execute(cache.clone());

        // Hardcode PONG response for now
        stream.write(&response).unwrap();

        // Echo the message back
        // if let Err(_) = stream.write_all(&buffer[..bytes_read]) {
        //     return; // writing failed
        // }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let cache: SharedCache = Arc::new(Mutex::new(HashMap::new()));

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
