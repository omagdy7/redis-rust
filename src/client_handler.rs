use anyhow::{Context, Result};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf},
    net::TcpStream,
    sync::Notify,
    time::{Duration, timeout},
};
use tracing::{error, info};

use crate::error::RespError;
use crate::frame::Frame;
use crate::parser::parse;
use crate::server::RedisServer;
use crate::shared_cache::Cache;
use crate::stream::{StreamEntry, StreamId};
use crate::types::{
    BlockedClient, BlockingQueue, BoxedAsyncWrite, NotificationManager, NotifierType,
    ReplicationMsg, ServerState, SharedMut, resolve_stream_id,
};
use crate::{commands::RedisCommand, stream::XaddStreamId};
use bytes::Bytes;

use crate::rdb_utils::send_empty_rdb;

pub async fn handle_client(
    mut reader: ReadHalf<TcpStream>,
    writer: SharedMut<BoxedAsyncWrite>,
    socket_addr: SocketAddr,
    server: SharedMut<RedisServer>,
    role: &str,
    cache: SharedMut<Cache>,
    notification_manager: Option<NotificationManager>,
    acks_map: Option<SharedMut<HashMap<SocketAddr, usize>>>,
    blocking_queue: Option<SharedMut<BlockingQueue>>,
) -> Result<()> {
    info!("Starting handle_client for {}", socket_addr);

    let mut buffer = [0; 1024];

    loop {
        info!("Waiting to read from client {}", socket_addr);
        let n = match reader.read(&mut buffer).await {
            Ok(0) => {
                info!("Client {} disconnected", socket_addr);
                return Ok(()); // connection closed
            }
            Ok(n) => {
                info!("Read {} bytes from client {}", n, socket_addr);
                n
            }
            Err(e) => {
                info!("Error while reading from client {}: {}", socket_addr, e);
                return Err(e).context("error occurred while reading from stream");
            }
        };

        let (request, left_bytes) = parse(&buffer[..n]).context("failed to parse request")?;
        info!(
            "Parsed request: {:?}, leftover bytes: {}",
            request,
            left_bytes.len()
        );

        // Sanity check
        assert!(left_bytes.is_empty());

        let command = RedisCommand::from(request.clone());
        info!("Converted request to RedisCommand: {:?}", command);

        match command {
            RedisCommand::Psync(_) => {
                if let Err(e) = handle_psync(&server, &writer, socket_addr).await {
                    error!("Error handling PSYNC: {}", e);
                    return Err(e);
                }
                continue; // Skip to next loop iteration
            }
            RedisCommand::Xadd {
                key,
                parsed_id,
                fields,
            } => {
                if let Err(e) = handle_xadd(
                    &cache,
                    &writer,
                    key,
                    parsed_id,
                    fields,
                    notification_manager.as_ref(),
                )
                .await
                {
                    error!("Error handling XADD: {}", e);
                    return Err(e);
                }
            }
            RedisCommand::Blpop { key, time_sec } => {
                if let Err(e) = handle_blpop(
                    &cache,
                    &writer,
                    socket_addr,
                    key,
                    time_sec,
                    notification_manager.as_ref(),
                    blocking_queue.as_ref(),
                )
                .await
                {
                    error!("Error handling BLPOP: {}", e);
                    return Err(e);
                }
            }
            _ => {
                if let Err(e) = handle_generic_command(
                    &server,
                    &writer,
                    socket_addr,
                    role,
                    acks_map.as_ref(),
                    notification_manager.as_ref(),
                    command,
                )
                .await
                {
                    error!("Error handling generic command: {}", e);
                    return Err(e);
                }
            }
        }
    }
}

// helper function for PSYNC handling
async fn handle_psync(
    server: &SharedMut<RedisServer>,
    writer: &SharedMut<BoxedAsyncWrite>,
    socket_addr: SocketAddr,
) -> Result<()> {
    info!("Handling PSYNC command for client {}", socket_addr);

    let (server_state, sender) = {
        info!("Attempting to lock server for PSYNC state and sender");
        let server_instance = server.lock().await;
        info!("Server lock acquired for PSYNC");
        (
            server_instance.get_server_state_owned(),
            server_instance.get_replication_msg_sender().await,
        )
    };

    if let ServerState::MasterState(master) = server_state {
        info!("Server is Master, fulfilling PSYNC handshake");

        // Fulfill the master side of the handshake
        info!("Attempting to lock master state for replid");

        let master_state_guard = master.lock().await;
        let replid = master_state_guard.replid();

        info!("Master state lock acquired, replid={}", replid);

        let response_str = format!("FULLRESYNC {} 0", replid);
        let full_resync_response = frame_bytes!(response_str);

        // Add this connection to the list of replicas
        info!("Adding client {} as replica", socket_addr);
        if let Some(sender) = sender {
            sender
                .send(ReplicationMsg::AddReplica(socket_addr, writer.clone()))
                .await
                .context("Failed to add replica")?;
        } else {
            return Err(anyhow::anyhow!(
                "Replication sender not available in master mode"
            ));
        }
        info!("Replica {} added successfully", socket_addr);
        let server_guard = server.lock().await;

        info!("adding acks of master's replica: {}", socket_addr);
        if let RedisServer::Master(master) = &*server_guard {
            master.acks.lock().await.insert(socket_addr, 0);
        }

        // Lock the writer to send the multi-part response
        info!("Attempting to lock writer for FULLRESYNC response");
        let mut writer_guard = writer.lock().await;
        info!("Writer lock acquired for FULLRESYNC response");
        writer_guard.write_all(&full_resync_response).await?;
        info!("Sent FULLRESYNC response to {}", socket_addr);

        if let Err(e) = send_empty_rdb(&mut *writer_guard).await {
            error!("Failed to send empty RDB: {}", e);
        }
        info!("Sent empty RDB to {}", socket_addr);

        writer_guard.flush().await?;
        info!("Flushed writer after FULLRESYNC to {}", socket_addr);
    } else {
        // A slave should not receive a PSYNC command from a client
        info!("Server is Slave, rejecting PSYNC command");
        let response = RespError::OperationNotSupported.to_resp();
        writer.lock().await.write_all(&response).await?;
        info!("Sent PSYNC error response to {}", socket_addr);
    }

    Ok(())
}

// helper function for XADD handling
async fn handle_xadd(
    cache: &SharedMut<Cache>,
    writer: &SharedMut<BoxedAsyncWrite>,
    key: String,
    parsed_id: XaddStreamId,
    fields: HashMap<String, String>,
    notification_manager: Option<&NotificationManager>,
) -> Result<()> {
    info!("Received XADD command for key: {}", key);
    info!("Attempting to lock cache for XADD");
    let mut cache = cache.lock().await;
    info!("Cache lock acquired for XADD");

    // Get the last stream entry to determine the next ID
    let last_id = if let Some(entry) = cache.get(&key) {
        if let Frame::Stream(ref vec) = entry.value {
            vec.last().map(|e| e.id)
        } else {
            None
        }
    } else {
        None
    };

    let stream_id = match resolve_stream_id(parsed_id, last_id) {
        Ok(id) => id,
        Err(e) => {
            let response = frame_bytes!(error e);
            writer.lock().await.write_all(&response).await?;
            // HACK: I feel this could cause weird issues in the future
            return Ok(());
        }
    };

    let mut response = frame_bytes!(bulk stream_id.to_string());

    let stream_entry = StreamEntry::new(stream_id, fields);

    if let Some(entry) = cache.get_mut(&key) {
        if let Frame::Stream(ref mut vec) = entry.value {
            if stream_id == (StreamId { ms_time: 0, seq: 0 }) {
                response = RespError::StreamIdTooSmall.to_resp();
            } else if stream_id <= vec.last().unwrap().id {
                response = RespError::StreamIdNotGreater.to_resp();
            } else {
                vec.push(stream_entry);
                if let Some(nm) = notification_manager {
                    nm.notify(NotifierType::XAdd).await;
                }
            }
        } else {
            entry.value = Frame::Stream(vec![stream_entry]);
        }
    } else {
        cache.insert(
            key.clone(),
            crate::shared_cache::CacheEntry {
                value: Frame::Stream(vec![stream_entry]),
                expires_at: None,
            },
        );
    }

    info!("Inserted/key {}", key);

    drop(cache);

    writer.lock().await.write_all(&response).await?;
    writer.lock().await.flush().await?;

    Ok(())
}

// helper function for BLPOP handling
async fn handle_blpop(
    cache: &SharedMut<Cache>,
    writer: &SharedMut<BoxedAsyncWrite>,
    socket_addr: SocketAddr,
    key: String,
    time_sec: f64,
    notification_manager: Option<&NotificationManager>,
    blocking_queue: Option<&SharedMut<BlockingQueue>>,
) -> Result<()> {
    info!(
        "Received BLPOP command for key: {} with timeout: {}",
        key, time_sec
    );

    loop {
        // Try to pop from the list (with write lock to make it atomic)
        {
            let mut cache_write = cache.lock().await;
            if let Some(entry) = cache_write.get_mut(&key) {
                if let Frame::List(arr) = &mut entry.value {
                    if !arr.is_empty() {
                        let popped_element = arr.remove(0);
                        if arr.is_empty() {
                            // Remove the key if list is now empty
                            cache_write.remove(&key);
                        }

                        // Return the result
                        let response = frame_bytes!(list => [frame!(bulk key), popped_element]);
                        writer.lock().await.write_all(&response).await?;
                        writer.lock().await.flush().await?;
                        return Ok(());
                    }
                }
            }
        }

        // List is empty, wait for notification
        if let Some(bq) = blocking_queue {
            info!(
                "BLPOP waiting for notification for key: {} with timeout: {}",
                key, time_sec
            );

            let blocked_client = BlockedClient::new(socket_addr, writer.clone());
            let client_notifier = blocked_client.notifier.clone();

            // Add to blocking queue
            {
                let mut queue = bq.lock().await;
                queue.enqueue(key.clone(), blocked_client);
            }

            // Wait for notification
            let wait_result = if time_sec == 0.0 {
                // Indefinite wait
                client_notifier.notified().await;
                Ok(())
            } else {
                // Wait with timeout
                timeout(
                    Duration::from_secs_f64(time_sec),
                    client_notifier.notified(),
                )
                .await
            };

            match wait_result {
                Ok(_) => {
                    // Notification received, check again
                    info!(
                        "BLPOP woke up by notification, checking again for key: {}",
                        key
                    );
                }
                Err(_) => {
                    // Timeout occurred, remove from queue and return null array
                    info!(
                        "BLPOP timeout occurred for key: {}, returning null array",
                        key
                    );
                    {
                        let mut queue_guard = bq.lock().await;
                        queue_guard.remove_client(socket_addr);
                    }
                    let response = frame_bytes!(null_list);
                    writer.lock().await.write_all(&response).await?;
                    writer.lock().await.flush().await?;
                    return Ok(());
                }
            }
        } else {
            // No blocking queue available, return null array
            let response = frame_bytes!(null_list);
            writer.lock().await.write_all(&response).await?;
            writer.lock().await.flush().await?;
            return Ok(());
        }
    }
}

// helper function for REPLCONF ACK handling
async fn handle_replconf_ack(
    socket_addr: SocketAddr,
    op2: &str,
    acks_map: Option<&SharedMut<HashMap<SocketAddr, usize>>>,
    notification_manager: Option<&NotificationManager>,
) -> Result<()> {
    info!("Handling REPLCONF ACK from {}", socket_addr);
    if let Ok(offset) = op2.parse::<usize>() {
        // We use the passed-in acks_map and notification_manager, avoiding the main server lock.
        if let (Some(acks), Some(nm)) = (acks_map, notification_manager) {
            info!("Attempting to acquire acks_guard mutex directly");
            let mut acks_guard = acks.lock().await;
            info!("Successfully acquired acks_guard mutex");

            if let Some(o) = acks_guard.get_mut(&socket_addr) {
                info!(
                    "Updating ack of {:?} of offset {} to offset {}",
                    socket_addr, o, offset
                );
                *o = offset;
            }

            // Drop the lock explicitly before notifying to be clean
            drop(acks_guard);
            // Notify any waiting WAIT commands
            nm.notify(NotifierType::Ack).await;

            info!("Notified waiters after ACK update.");
        } else {
            info!("Error: Received ACK but acks_map/notification_manager not available.");
        }
    } else {
        info!("Failed to parse ACK offset from {}", op2);
    }
    Ok(())
}

// helper function for generic command handling
async fn handle_generic_command(
    server: &SharedMut<RedisServer>,
    writer: &SharedMut<BoxedAsyncWrite>,
    socket_addr: SocketAddr,
    role: &str,
    acks_map: Option<&SharedMut<HashMap<SocketAddr, usize>>>,
    notification_manager: Option<&NotificationManager>,
    command: RedisCommand,
) -> Result<()> {
    info!(
        "Handling generic command {:?} from {}",
        command, socket_addr
    );

    // Special handling for REPLCONF ACK if master
    if role == "master" {
        if let RedisCommand::ReplConf((ref op1, ref op2)) = command {
            info!("Received REPLCONF command: op1={}, op2={}", op1, op2);
            if op1.to_uppercase() == "ACK" {
                handle_replconf_ack(socket_addr, op2, acks_map, notification_manager).await?;
                // No response needed for ACK
                return Ok(()); // Continue to the next loop iteration
            }
        }
    }

    let response = {
        info!("Attempting to lock server for execute()");
        let server_instance = server.lock().await;
        info!("Server lock acquired for execute()");
        match server_instance.execute(command, socket_addr).await {
            Ok(bytes) => bytes,
            Err(error) => error.to_resp(),
        }
    };

    info!("Command executed, response size: {}", response.len());

    if !response.is_empty() {
        info!("Attempting to lock writer to send response");
        let mut writer_guard = writer.lock().await;
        info!("Writer lock acquired to send response");
        writer_guard.write_all(&response).await?;
        writer_guard.flush().await?;
        info!("Response sent to {}", socket_addr);
    } else {
        info!("Empty response, nothing sent to {}", socket_addr);
    }

    Ok(())
}
