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
    server::{ReplicationMsg, ServerRole, ServerState, ServerStateTrait, SharedMut},
    shared_cache::*,
};
use codecrafters_redis::{resp_commands::RedisCommand, server::RedisServer};
use codecrafters_redis::{
    resp_parser::{RespType, parse},
    server::SlaveServer,
};
use tokio::sync::Mutex;
use tokio::{io::AsyncReadExt, net::TcpStream};
use tokio::{io::AsyncWriteExt, spawn};
use tokio::{
    io::{AsyncWrite, ReadHalf, WriteHalf},
    sync::Notify,
};

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

async fn send_empty_rdb<W: AsyncWriteExt + Unpin>(
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

// TODO: This should return a Result to handle the plethora of different errors
async fn handle_client<W: AsyncWrite + Send + Unpin + 'static>(
    mut reader: ReadHalf<TcpStream>,
    writer: SharedMut<W>,
    socket_addr: SocketAddr,
    server: SharedMut<RedisServer<W>>,
    role: &str,
    acks_map: Option<SharedMut<HashMap<SocketAddr, usize>>>,
    ack_notifier: Option<Arc<Notify>>,
) -> Result<()> {
    println!("Starting handle_client for {}", socket_addr);

    println!("Attempting to lock server to get role");

    let mut buffer = [0; 1024];

    loop {
        println!("Waiting to read from client {}", socket_addr);
        let n = match reader.read(&mut buffer).await {
            Ok(0) => {
                println!("Client {} disconnected", socket_addr);
                return Ok(()); // connection closed
            }
            Ok(n) => {
                println!("Read {} bytes from client {}", n, socket_addr);
                n
            }
            Err(e) => {
                println!("Error while reading from client {}: {}", socket_addr, e);
                return Err(e).context("error occurred while reading from stream");
            }
        };

        let (request, left_bytes) = parse(&buffer[..n]).context("failed to parse request")?;
        println!(
            "Parsed request: {:?}, leftover bytes: {}",
            request,
            left_bytes.len()
        );

        // Sanity check
        assert!(left_bytes.is_empty());

        let command = RedisCommand::from(request.clone());
        println!("Converted request to RedisCommand: {:?}", command);

        // Special handling for PSYNC ---
        if let RedisCommand::Psync(_) = command {
            println!("Handling PSYNC command for client {}", socket_addr);

            let (server_state, sender) = {
                println!("Attempting to lock server for PSYNC state and sender");
                let server_instance = server.lock().await;
                println!("Server lock acquired for PSYNC");
                (
                    server_instance.get_server_state_owned(),
                    server_instance.get_replication_msg_sender().await,
                )
            };

            if let ServerState::MasterState(master) = server_state {
                println!("Server is Master, fulfilling PSYNC handshake");

                // Fulfill the master side of the handshake
                println!("Attempting to lock master state for replid");

                let master_state_guard = master.lock().await;
                let replid = master_state_guard.replid();

                println!("Master state lock acquired, replid={}", replid);

                let response_str = format!("FULLRESYNC {} 0", replid);
                let full_resync_response = resp_bytes!(response_str);

                // Add this connection to the list of replicas
                println!("Adding client {} as replica", socket_addr);
                sender
                    .unwrap() // Safe to unwrap; we are in the Master role
                    .send(ReplicationMsg::AddReplica(socket_addr, writer.clone()))
                    .await
                    .context("Failed to add replica")?;
                println!("Replica {} added successfully", socket_addr);
                let server_guard = server.lock().await;

                println!("adding acks of master's replica: {}", socket_addr);
                if let RedisServer::Master(master) = &*server_guard {
                    master.acks.lock().await.insert(socket_addr, 0);
                }

                // Lock the writer to send the multi-part response
                println!("Attempting to lock writer for FULLRESYNC response");
                let mut writer_guard = writer.lock().await;
                println!("Writer lock acquired for FULLRESYNC response");
                writer_guard.write_all(&full_resync_response).await?;
                println!("Sent FULLRESYNC response to {}", socket_addr);

                let _ = send_empty_rdb(&mut *writer_guard).await.unwrap();
                println!("Sent empty RDB to {}", socket_addr);

                writer_guard.flush().await?;
                println!("Flushed writer after FULLRESYNC to {}", socket_addr);
            } else {
                // A slave should not receive a PSYNC command from a client
                println!("Server is Slave, rejecting PSYNC command");
                let response = resp_bytes!(error "ERR PSYNC not supported on slave");
                writer.lock().await.write_all(&response).await?;
                println!("Sent PSYNC error response to {}", socket_addr);
            }

            // Skip the generic command handling below and wait for the next command
            continue;
        } else {
            // Generic command handling for all other commands ---
            println!(
                "Handling generic command {:?} from {}",
                command, socket_addr
            );

            // Special handling for REPLCONF ACK if master
            if role == "master" {
                if let RedisCommand::ReplConf((ref op1, ref op2)) = command {
                    println!("Received REPLCONF command: op1={}, op2={}", op1, op2);
                    if op1.to_uppercase() == "ACK" {
                        println!("Handling REPLCONF ACK from {}", socket_addr);
                        if let Ok(offset) = op2.parse::<usize>() {
                            // ---- START REFACTORED CODE ----
                            // We use the passed-in acks_map and notifier, avoiding the main server lock.
                            if let (Some(acks), Some(notifier)) = (&acks_map, &ack_notifier) {
                                println!("Attempting to acquire acks_guard mutex directly");
                                let mut acks_guard = acks.lock().await;
                                println!("Successfully acquired acks_guard mutex");

                                if let Some(o) = acks_guard.get_mut(&socket_addr) {
                                    println!(
                                        "Updating ack of {:?} of offset {} to offset {}",
                                        socket_addr, o, offset
                                    );
                                    *o = offset;
                                }
                                // Drop the lock explicitly before notifying to be clean
                                drop(acks_guard);
                                // Notify any waiting WAIT commands
                                notifier.notify_waiters();
                                println!("Notified waiters after ACK update.");
                            }
                            // ---- END REFACTORED CODE ----
                            else {
                                println!(
                                    "Error: Received ACK but acks_map/notifier not available."
                                );
                            }
                        } else {
                            println!("Failed to parse ACK offset from {}", op2);
                        }
                        // No response needed for ACK
                        continue; // Continue to the next loop iteration
                    }
                }
            }

            let response = {
                println!("Attempting to lock server for execute()");
                let server_instance = server.lock().await;
                println!("Server lock acquired for execute()");
                server_instance.execute(command).await
            };

            println!("Command executed, response size: {}", response.len());

            if !response.is_empty() {
                println!("Attempting to lock writer to send response");
                let mut writer_guard = writer.lock().await;
                println!("Writer lock acquired to send response");
                writer_guard.write_all(&response).await?;
                println!("Response sent to {}", socket_addr);
            } else {
                println!("Empty response, nothing sent to {}", socket_addr);
            }
        }
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
                        RedisValue::String(data) => String::from_utf8(data.to_vec()).unwrap(),
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
                    cache.insert(String::from_utf8(key.to_vec()).unwrap(), cache_entry);
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

    let role = {
        if let RedisServer::Master(_) = server {
            "master"
        } else {
            "slave"
        }
    };

    spawn_cleanup_task(server.cache().clone());
    let server = Arc::new(Mutex::new(server));

    let (acks_for_handler, notifier_for_handler) = {
        let server_guard = server.lock().await;
        if let RedisServer::Master(master) = &*server_guard {
            (Some(master.acks.clone()), Some(master.ack_notifier.clone()))
        } else {
            (None, None)
        }
    };

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let server_clone = Arc::clone(&server);
                let acks_for_handler_clone = match acks_for_handler {
                    Some(ref inner) => Some(Arc::clone(inner)),
                    None => None,
                };
                let notifier_for_handler_clone = match notifier_for_handler {
                    Some(ref inner) => Some(Arc::clone(inner)),
                    None => None,
                };
                let socket_addr = stream.peer_addr().unwrap();

                // Split the stream into a reader and a writer
                let (reader, writer) = tokio::io::split(stream);
                let shared_writer = Arc::new(Mutex::new(writer));

                tokio::spawn(async move {
                    // Pass the reader and the shared writer to the handler
                    if let Err(e) = handle_client(
                        reader,
                        shared_writer,
                        socket_addr,
                        server_clone,
                        role,
                        acks_for_handler_clone,
                        notifier_for_handler_clone,
                    )
                    .await
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
