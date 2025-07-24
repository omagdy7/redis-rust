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
    server::SharedMut,
    shared_cache::*,
};
use codecrafters_redis::{resp_commands::RedisCommand, server::RedisServer};
use codecrafters_redis::{
    resp_parser::{parse, RespType},
    server::SlaveServer,
};

fn spawn_cleanup_thread(cache: SharedMut<Cache>) {
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
fn handle_client(mut stream: TcpStream, server: Arc<Mutex<RedisServer>>) {
    let mut buffer = [0; 512];

    loop {
        let _ = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let request = parse(&buffer).unwrap();

        let mut server = server.lock().unwrap();

        // Big State vars
        let cache = server.cache().clone();
        let server_state = server.get_server_state().clone();
        let config = server.config();
        let brodcaster = server.as_broadcaster();

        let response = RedisCommand::from(request.0.clone()).execute(
            cache.clone(),
            config,
            server_state,
            brodcaster,
        );

        let mut request_command = "".to_string();

        // FIXME: Find a solution for this mess!! (Design better API)
        match &request.0 {
            RespType::Array(arr) => {
                if let RespType::BulkString(s) = arr[0].clone() {
                    request_command = String::from_utf8(s).unwrap();
                }
            }
            _ => {}
        }

        // Store the persistent connection
        // let shared_stream = Arc::new(Mutex::new(
        //     stream.try_clone().expect("What could go wrong? :)"),
        // ));

        // if this true immediately write and send back rdb file after response
        // HACK: This just feels wrong I feel this shouldn't be handled here and should be handled
        // in the exexute command
        if request_command.starts_with("PSYNC") {
            stream.write(&response).unwrap();
            let replica_addr = stream
                .peer_addr()
                .expect("This shouldn't fail right? right?? :)");

            server.add_replica(
                replica_addr,
                Arc::new(Mutex::new(
                    stream.try_clone().expect("What could go wrong? :)"),
                )),
            );

            let _ = write_rdb_to_stream(&mut stream);
            // handshake completed and I should add the server sending me the handshake to my replicas
        } else {
            // write respose back to the client
            stream.write(&response).unwrap();
        }
    }
}

fn main() -> std::io::Result<()> {
    let server = match RedisServer::new() {
        Ok(Some(server)) => server,
        Ok(None) => RedisServer::master(), // Default to master if no args
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Load RDB file if dir and dbfilename are provided
    if let (Some(dir), Some(dbfilename)) = (server.dir().clone(), server.dbfilename().clone()) {
        if let Ok(rdb_file) = RDBFile::read(dir, dbfilename) {
            if let Some(rdb) = rdb_file {
                let mut cache = server.cache().lock().unwrap();
                let hash_table = &rdb.databases.get(&0).unwrap().hash_table;

                for (key, db_entry) in hash_table.iter() {
                    let value = match &db_entry.value {
                        RedisValue::String(data) => String::from_utf8(data.clone()).unwrap(),
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
    }

    let port = server.port().to_string();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    spawn_cleanup_thread(server.cache().clone());

    let server = Arc::new(Mutex::new(server));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let server_clone = Arc::clone(&server);
                // TODO: Use tokio instead of multi threads to handle multiple clients
                thread::spawn(|| {
                    handle_client(stream, server_clone);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
