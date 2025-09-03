#![allow(unused_imports)]
use core::time;
use std::{
    collections::HashMap,
    env,
    fmt::format,
    io::{Read, Write},
    net::SocketAddr,
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use codecrafters_redis::{
    rdb::{KeyExpiry, ParseError, RDBFile, RedisValue},
    resp_bytes,
    server::{RedisNode, SharedMut},
    shared_cache::*,
};
use codecrafters_redis::{resp_commands::RedisCommand, server::RedisServer};
use codecrafters_redis::{
    resp_parser::{RespType, parse},
    server::SlaveServer,
};
use tokio::io::{AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::Mutex;
use tokio::{io::AsyncReadExt, net::TcpStream};
use tokio::{io::AsyncWriteExt, spawn};

// responsible for periodically removing expired keys from database
fn spawn_cleanup_task(cache: SharedMut<Cache>) {
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await; // Wait for the next tick (10 seconds)

            let mut cache = cache_clone.lock().await;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Remove expired keys
            cache.retain(|_, entry| entry.expires_at.map_or(true, |expiry| now <= expiry));
        }
    });
}

use base64::{Engine as _, engine::general_purpose};

fn write_rdb_to_stream<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let hardcoded_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    let bytes = general_purpose::STANDARD.decode(hardcoded_rdb)?;

    let mut response = format!("${}\r\n", bytes.len()).into_bytes();
    response.extend_from_slice(&bytes);

    // Write the binary RDB data
    let _ = writer.write_all(&response);

    Ok(())
}

// TODO: This should return a Result to handle the plethora of different errors
async fn handle_client<W: AsyncWrite + Send + Unpin + 'static>(
    mut reader: ReadHalf<TcpStream>, // Takes ownership of the reader
    writer: SharedMut<W>,            // Takes the shared writer
    socket_addr: SocketAddr,
    server: SharedMut<RedisServer<W>>,
) -> Result<()> {
    let mut buffer = [0; 1024];

    loop {
        let n = match reader.read(&mut buffer).await {
            Ok(0) => return Ok(()), // connection closed
            Ok(n) => n,
            Err(e) => return Err(e).context("error occurred while reading from stream"),
        };

        let (request, left_bytes) = parse(&buffer[..n]).context("failed to parse request")?;
        assert!(left_bytes.is_empty());

        let server_instance = server.lock().await;

        // Big State vars
        let cache = server_instance.cache().clone();
        let server_state = server_instance.get_server_state().await.clone();
        let sender = server_instance.get_replication_msg_sender().await;
        let config = server_instance.config();

        let command = RedisCommand::from(request.clone());
        // Pass the writer to the execute function
        let _response = command
            .execute(
                writer.clone(),
                socket_addr,
                sender,
                cache,
                config,
                server_state,
            )
            .await;
    }
}

async fn load_rdb<W: AsyncWrite + Send + Unpin + 'static>(server: &RedisServer<W>) {
    // Load RDB file if dir and dbfilename are provided
    if let (Some(dir), Some(dbfilename)) = (server.dir().clone(), server.dbfilename().clone()) {
        if let Ok(rdb_file) = RDBFile::read(dir, dbfilename) {
            if let Some(rdb) = rdb_file {
                let mut cache = server.cache().lock().await;
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
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let server: RedisServer<WriteHalf<TcpStream>> = match RedisServer::new().await {
        Ok(Some(server)) => server,
        Ok(None) => RedisServer::new_master(), // Default to master if no args
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    println!("Server created and its role is {}", server.role());

    load_rdb(&server).await;
    let port = server.port().to_string();
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    spawn_cleanup_task(server.cache().clone());
    let server = Arc::new(Mutex::new(server));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let server_clone = Arc::clone(&server);
                let socket_addr = stream.peer_addr().unwrap();

                // Split the stream into a reader and a writer
                let (reader, writer) = tokio::io::split(stream);
                let shared_writer = Arc::new(Mutex::new(writer));

                tokio::spawn(async move {
                    // Pass the reader and the shared writer to the handler
                    if let Err(e) =
                        handle_client(reader, shared_writer, socket_addr, server_clone).await
                    {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}
