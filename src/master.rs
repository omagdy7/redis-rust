use bytes::Bytes;
use regex::Regex;
use std::{
    collections::{HashMap, hash_map::Entry},
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, mpsc::Sender},
    time::Duration,
};
use tracing::{error, info};

use crate::shared_cache::{Cache, CacheEntry};
use crate::stream::{StreamEntry, StreamId, XReadStreamId, XrangeStreamdId};
use crate::types::*;
use crate::{
    commands::{ExpiryOption, RedisCommand, SetCondition},
    transaction::Transaction,
};
use crate::{error::RespError, transaction};
use crate::{frame::Frame, transaction::TxState};
use crate::types::{NotificationManager, NotifierType};

#[derive(Debug, Clone)]
pub struct MasterServer {
    pub config: Shared<ServerConfig>,
    pub cache: SharedMut<Cache>,
    pub connection_socket: SocketAddr,
    pub replication_msg_sender: Sender<ReplicationMsg>, // channel to send all that concerns replicaion nodes
    pub state: SharedMut<MasterState>,
    pub acks: SharedMut<HashMap<SocketAddr, usize>>,
    pub notification_manager: NotificationManager,
    pub blocking_queue: SharedMut<BlockingQueue>,
    pub transactions: SharedMut<HashMap<SocketAddr, Transaction>>,
}

impl MasterServer {
    pub fn new() -> Self {
        let config = Arc::new(ServerConfig {
            dir: None,
            dbfilename: None,
            port: "6379".to_string(),
        });

        let state = Arc::new(Mutex::new(MasterState {
            role: ServerRole::Master,
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            current_offset: 0,
        }));

        let connection_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 420);

        let cache = Arc::new(Mutex::new(HashMap::new()));
        let notification_manager = NotificationManager::new();
        let blocking_queue = Arc::new(Mutex::new(BlockingQueue::new()));
        let acks = Arc::new(Mutex::new(HashMap::new()));
        let transactions = Arc::new(Mutex::new(HashMap::new()));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicationMsg>(256);

        tokio::spawn(async move {
            let mut replicas: HashMap<SocketAddr, SharedMut<BoxedAsyncWrite>> = HashMap::new();

            let mut reported_offsets = HashMap::<SocketAddr, usize>::new();

            while let Some(msg) = rx.recv().await {
                match msg {
                    ReplicationMsg::Broadcast(cmd) => {
                        for (_addr, writer) in replicas.iter_mut() {
                            let mut writer_guard = writer.lock().await;
                            if let Err(e) = writer_guard.write_all(&cmd).await {
                                error!("Failed to write to replica: {}", e);
                                // Optionally, handle the error by removing the replica
                                continue;
                            }
                            // Flush the buffer to ensure the command is sent immediately
                            if let Err(e) = writer_guard.flush().await {
                                error!("Failed to flush to replica: {}", e);
                            }
                            info!(
                                "Replication handler wrote {} bytes command successfully",
                                cmd.len()
                            );
                        }
                    }
                    ReplicationMsg::AddReplica(addr, stream_writer) => {
                        info!("Adding new replica: {}", addr);
                        replicas.insert(addr, stream_writer);
                        reported_offsets.insert(addr, 0);
                    }
                    ReplicationMsg::RemoveReplica(addr) => {
                        info!("Removing new replica: {}", addr);
                        replicas.remove(&addr);

                        reported_offsets.remove(&addr);
                    }
                }
            }
        });

        Self {
            config,
            replication_msg_sender: tx,
            state,
            cache,
            connection_socket,
            acks,
            notification_manager,
            blocking_queue,
            transactions,
        }
    }

    pub fn role(&self) -> &str {
        "master"
    }
    pub fn is_master(&self) -> bool {
        true
    }

    pub fn is_slave(&self) -> bool {
        false
    }

    pub fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    pub fn cache(&self) -> &SharedMut<Cache> {
        &self.cache
    }
    pub fn get_server_state(&self) -> &SharedMut<MasterState> {
        &self.state
    }

    pub fn get_server_state_mut(&mut self) -> &mut SharedMut<MasterState> {
        &mut self.state
    }

    #[allow(dead_code)]
    fn port(&self) -> &str {
        &self.config.port
    }

    pub async fn get_repl_offset(&self) -> usize {
        self.state.lock().await.current_offset
    }

    pub async fn increment_repl_offset(&self, amount: usize) {
        self.state.lock().await.current_offset += amount;
    }

    pub async fn get_replid(&self) -> String {
        self.state.lock().await.replid.clone()
    }
}

impl MasterServer {
    async fn check_and_queue_transaction(
        &self,
        command: &RedisCommand,
        connection_socket: SocketAddr,
    ) -> bool {
        info!("Attempting to lock transactions");
        let mut transaction_guard = self.transactions.lock().await;
        info!("Transactions lock acquired");

        info!("Checking transactions of socket: {}", connection_socket);

        match transaction_guard.entry(connection_socket.clone()) {
            Entry::Occupied(mut entry) => {
                let transaction = entry.get_mut();
                if transaction.state() == transaction::TxState::Queuing {
                    info!("Transaction map has an entry and it's state is Queuing");
                    info!("Adding command {:?} to queue", command);
                    transaction.queue(command.clone());
                    return true;
                }
            }
            Entry::Vacant(_) => {
                info!("Transaction map is vacant");
            }
        }
        false
    }
}

impl CommandHandler<BoxedAsyncWrite> for MasterServer {
    fn execute(
        &self,
        command: RedisCommand,
        connection_socket: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RespError>> + Send + '_>> {
        Box::pin(async move {
            use RedisCommand as RC;

            match command {
                RC::Ping => {
                    info!("Received PING command");
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    Ok(frame_bytes!("PONG"))
                }
                RC::Echo(ref echo_string) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received ECHO command: {}", echo_string);
                    Ok(frame_bytes!(echo_string))
                }
                RC::Get(ref key) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received GET command for key: {}", key);
                    info!("Attempting to lock cache for GET");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired for GET");

                    match cache.get(key.as_str()).cloned() {
                        Some(entry) if !entry.is_expired() => {
                            info!("Key {} found and not expired", key);
                            Ok(entry.value.to_resp())
                        }
                        Some(_) => {
                            info!("Key {} expired, removing", key);
                            cache.remove(key.as_str()); // Clean up expired key
                            Ok(frame_bytes!(null))
                        }
                        None => {
                            info!("Key {} not found", key);
                            Ok(frame_bytes!(null))
                        }
                    }
                }
                RC::Type(ref key) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received TYPE command for key: {}", key);
                    info!("Attempting to lock cache for GET");
                    let cache = self.cache.lock().await;
                    info!("Cache lock acquired for GET");

                    match cache.get(key.as_str()).cloned() {
                        Some(entry) if !entry.is_expired() => {
                            info!("Key {} found and not expired", key);
                            let type_str = match &entry.value {
                                Frame::SimpleString(_) | Frame::BulkString(_) => "string",
                                Frame::Integer(_) => "string", // Redis treats integers as strings
                                Frame::List(_) => "list",
                                Frame::Set(_) => "set",
                                Frame::Map(_) => "hash",
                                Frame::Stream(_) => "stream",
                                _ => "string", // Default to string for other types
                            };
                            Ok(frame_bytes!(type_str))
                        }
                        Some(_) => {
                            info!("Key {} expired, removing", key);
                            Ok(frame_bytes!(null))
                        }
                        None => {
                            info!("Key {} not found", key);
                            Ok(frame_bytes!("none"))
                        }
                    }
                }
                RC::Set(ref set_cmd) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received SET command for key: {}", set_cmd.key);
                    info!("Attempting to lock cache for SET");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired for SET");

                    // Check conditions (NX/XX)
                    let key_exists = cache.contains_key(set_cmd.key.as_str());
                    info!("Key exists? {}", key_exists);

                    if (matches!(set_cmd.condition, Some(SetCondition::NotExists)) && key_exists)
                        || (matches!(set_cmd.condition, Some(SetCondition::Exists)) && !key_exists)
                    {
                        info!("SET condition not met for key {}", set_cmd.key);
                        return Ok(frame_bytes!(null));
                    }

                    let get_value = if set_cmd.get_old_value {
                        let old = cache.get(set_cmd.key.as_str()).map(|v| v.value.clone());
                        info!("Returning old value for key {}: {:?}", set_cmd.key, old);
                        old
                    } else {
                        None
                    };

                    // Calculate expiry
                    let expires_at = if let Some(ExpiryOption::KeepTtl) = set_cmd.expiry {
                        let ttl = cache.get(set_cmd.key.as_str()).and_then(|e| e.expires_at);
                        info!("Keeping TTL for key {}: {:?}", set_cmd.key, ttl);
                        ttl
                    } else {
                        let ttl = set_cmd.calculate_expiry_time();
                        info!("Calculated new TTL for key {}: {:?}", set_cmd.key, ttl);
                        ttl
                    };

                    let frame_value = set_cmd.value.clone();
                    let broadcast_cmd = frame_bytes!(list => [
                        frame!(bulk "SET"),
                        frame!(bulk set_cmd.key.clone()),
                        frame_value.clone()
                    ]);

                    // Set the value
                    cache.insert(
                        set_cmd.key.clone(),
                        CacheEntry {
                            value: frame_value,
                            expires_at,
                        },
                    );

                    info!("Inserted/updated key {}", set_cmd.key);
                    drop(cache);
                    info!("Released cache lock for SET");

                    let broadcast_cmd_len = broadcast_cmd.len();
                    info!("Broadcasting SET command, len={}", broadcast_cmd_len);

                    info!("Attempting to lock state for SET offset update");
                    {
                        let mut state = self.state.lock().await;
                        info!("State lock acquired for SET offset update");
                        state.current_offset += broadcast_cmd_len;
                        info!("Updated current_offset: {}", state.current_offset);
                    }
                    info!("Released state lock after SET");

                    let _ = self
                        .replication_msg_sender
                        .send(ReplicationMsg::Broadcast(broadcast_cmd))
                        .await;
                    info!("Sent broadcast to replicas");

                    if !set_cmd.get_old_value {
                        Ok(frame_bytes!("OK"))
                    } else {
                        match get_value {
                            Some(val) => Ok(val.to_resp()),
                            None => Ok(frame_bytes!(null)),
                        }
                    }
                }
                RC::Incr { ref key } => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received INCR command for key: {}", key);
                    info!("Attempting to lock cache for Incr");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired for Incr");

                    let key_exists = cache.contains_key(key.as_str());
                    info!("Key exists? {}", key_exists);

                    let mut response = frame_bytes!(int 1);

                    if let Some(entry) = cache.get_mut(key.as_str()) {
                        match &entry.value {
                            Frame::BulkString(bytes) => {
                                // Try to parse as integer
                                if let Some(mut i) = std::str::from_utf8(bytes)
                                    .ok()
                                    .and_then(|s| s.parse::<i64>().ok())
                                {
                                    i += 1;
                                    info!("updated key {} from {} to {}", key, i - 1, i);

                                    // Store back as BulkString again (Redis semantics)
                                    entry.value = Frame::BulkString(Bytes::from(i.to_string()));

                                    // Reply as Integer
                                    response = frame_bytes!(int i);
                                } else {
                                    info!(
                                        "Tried to update key {} but it couldn't be parsed as an integer",
                                        key
                                    );
                                    response = frame_bytes!(error "ERR value is not an integer or out of range");
                                }
                            }
                            _ => {
                                // If somehow stored as another Frame type, treat as error
                                response = frame_bytes!(error "ERR value is not an integer or out of range");
                            }
                        }
                    } else {
                        cache.insert(
                            key.to_string(),
                            CacheEntry {
                                value: Frame::BulkString(Bytes::from("1")),
                                expires_at: None,
                            },
                        );
                        info!("Inserted key {} with 1", key);
                    }
                    drop(cache);
                    info!("Released cache lock for Incr");

                    Ok(response)
                }
                RC::Xadd {
                    key: _,
                    parsed_id: _,
                    fields: _,
                } => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received Xadd command (not handled here)");
                    // PSYNC is handled specially in `handle_client`, this is a fallback.
                    Err(RespError::InvalidStreamOperation)
                }
                RC::XRange {
                    ref key,
                    ref start,
                    ref end,
                } => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received XRange command for key: {}", key);
                    info!("Attempting to lock cache for XRANGE");
                    let cache = self.cache.lock().await;
                    info!("Cache lock acquired for XRANGE");

                    if let Some(entry) = cache.get(key.as_str()) {
                        if let Frame::Stream(ref vec) = entry.value {
                            info!(
                                "In XRANGE execution and for key {} and the Stream is : {:?}",
                                key, vec
                            );
                            let filtered_streams = match (start, end) {
                                (
                                    XrangeStreamdId::Literal(start),
                                    XrangeStreamdId::Literal(end),
                                ) => vec
                                    .iter()
                                    .filter(|stream| stream.id >= *start && stream.id <= *end)
                                    .collect::<Vec<&StreamEntry>>(),
                                (
                                    XrangeStreamdId::AutoSequence { ms_time: start },
                                    XrangeStreamdId::AutoSequence { ms_time: end },
                                ) => vec
                                    .iter()
                                    .filter(|stream| {
                                        stream.id.ms_time >= *start && stream.id.ms_time <= *end
                                    })
                                    .collect::<Vec<&StreamEntry>>(),
                                (
                                    XrangeStreamdId::AutoStart,
                                    XrangeStreamdId::AutoSequence { ms_time: end },
                                ) => vec
                                    .iter()
                                    .filter(|stream| stream.id.ms_time <= *end)
                                    .collect::<Vec<&StreamEntry>>(),
                                (XrangeStreamdId::AutoStart, XrangeStreamdId::Literal(end)) => vec
                                    .iter()
                                    .filter(|stream| stream.id <= *end)
                                    .collect::<Vec<&StreamEntry>>(),
                                (
                                    XrangeStreamdId::AutoSequence { ms_time: start },
                                    XrangeStreamdId::AutoEnd,
                                ) => vec
                                    .iter()
                                    .filter(|stream| stream.id.ms_time >= *start)
                                    .collect::<Vec<&StreamEntry>>(),
                                (XrangeStreamdId::Literal(start), XrangeStreamdId::AutoEnd) => vec
                                    .iter()
                                    .filter(|stream| stream.id >= *start)
                                    .collect::<Vec<&StreamEntry>>(),
                                (_, _) => unreachable!("This is logically impossible"),
                            };

                            let mut filtered_frames = vec![];
                            for stream in filtered_streams {
                                let id_str = stream.id.to_string();
                                filtered_frames.push(vec![frame!(bulk id_str)]);
                                let fields = stream
                                                                        .fields
                                                                        .iter()
                                                                        .map(|(key, value)| frame!(list => [frame!(bulk key.clone()), frame!(bulk value.clone())]))
                                                                        .collect::<Vec<Frame>>();
                                filtered_frames
                                    .last_mut()
                                    .unwrap()
                                    .extend_from_slice(&fields);
                            }
                            let filtered_frames: Vec<Frame> = filtered_frames
                                .into_iter()
                                .map(|x| frame!(list => x))
                                .collect();
                            return Ok(frame_bytes!(list => filtered_frames));
                        }
                    }

                    Ok(frame_bytes!(error "ERR"))
                }
                RC::XRead {
                    ref block_param,
                    ref keys,
                    ref stream_ids,
                } => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received XRead command for keys: {:?}", keys);

                    info!("Attempting to lock cache for XREAD");
                    let cache = self.cache.lock().await;
                    info!("Cache lock acquired for XREAD");

                    // Calculate filter_ids before waiting
                    let mut filter_ids = Vec::new();
                    for (key, stream_id) in keys.iter().zip(stream_ids.iter()) {
                        if let Some(entry) = cache.get(key) {
                            if let Frame::Stream(ref vec) = entry.value {
                                let filter_id = match stream_id {
                                    XReadStreamId::Literal(id) => *id,
                                    XReadStreamId::Latest => {
                                        // Find the maximum ID in the stream
                                        vec.iter()
                                            .max_by_key(|e| e.id)
                                            .map(|e| e.id)
                                            .unwrap_or(StreamId { ms_time: 0, seq: 0 })
                                    }
                                };
                                filter_ids.push(filter_id);
                            } else {
                                filter_ids.push(StreamId { ms_time: 0, seq: 0 });
                            }
                        } else {
                            filter_ids.push(StreamId { ms_time: 0, seq: 0 });
                        }
                    }

                    // Drop the lock before waiting
                    drop(cache);

                    if let Some(timeout_ms) = block_param {
                        if *timeout_ms == 0 {
                            info!("Blocking XREAD indefinitely until we recieve a notification");
                            self.notification_manager.wait_for(NotifierType::XAdd).await;
                            info!("XREAD unblocked by notification")
                        } else {
                            info!("Blocking XREAD with timeout: {}ms", timeout_ms);
                            // Wait for notification or timeout
                            let timeout_duration = Duration::from_millis(*timeout_ms);
                            let xadd_notifier = self.notification_manager.get_notifier(NotifierType::XAdd).await;
                            let timeout_result = tokio::time::timeout(
                                timeout_duration,
                                xadd_notifier.notified(),
                            )
                            .await;

                            match timeout_result {
                                Ok(_) => info!("XREAD unblocked by notification"),
                                Err(_) => {
                                    info!("XREAD timed out after {}ms", timeout_ms);
                                    // Return null on timeout with no new entries
                                    return Ok(frame_bytes!(null_list));
                                }
                            }
                        }
                    }

                    // Re-lock cache after waiting
                    let cache = self.cache.lock().await;

                    let mut stream_responses = Vec::new();

                    // Iterate through each key-stream_id pair
                    for ((key, _stream_id), filter_id) in
                        keys.iter().zip(stream_ids.iter()).zip(filter_ids.iter())
                    {
                        if let Some(entry) = cache.get(key) {
                            if let Frame::Stream(ref vec) = entry.value {
                                info!(
                                    "In XREAD execution and for key {} and the Stream is : {:?}",
                                    key, vec
                                );
                                let filtered_streams = vec
                                    .iter()
                                    .filter(|stream| stream.id > *filter_id)
                                    .collect::<Vec<&StreamEntry>>();

                                // Only include this stream in response if it has entries
                                if !filtered_streams.is_empty() {
                                    let mut entries = Vec::new();
                                    for stream in filtered_streams {
                                        let id_str = stream.id.to_string();
                                        // Build fields: ["temperature", "65"]
                                        let fields = stream
                                            .fields
                                            .iter()
                                            .flat_map(|(k, v)| {
                                                vec![frame!(bulk k.clone()), frame!(bulk v.clone())]
                                            })
                                            .collect::<Vec<Frame>>();
                                        // Each entry: ["0-1", ["temperature", "65"]]
                                        let entry = frame!(list => [frame!(bulk id_str), frame!(list => fields)]);
                                        entries.push(entry);
                                    }
                                    // Wrap into: ["grape", [entries...]]
                                    let stream_resp = frame!(list => [frame!(bulk key.clone()), frame!(list => entries)]);
                                    stream_responses.push(stream_resp);
                                }
                            }
                        }
                    }

                    // If no streams have new entries, return null
                    if stream_responses.is_empty() {
                        return Ok(frame_bytes!(null));
                    }

                    // Final response is an array of streams
                    Ok(frame_bytes!(list => stream_responses))
                }
                RC::ConfigGet(ref s) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received CONFIG GET for key: {}", s);
                    match s.as_str() {
                        "dir" => Ok(frame_bytes!(list => [
                            frame!(bulk s),
                            frame!(bulk self.config.dir.as_deref().unwrap_or(""))
                        ])),
                        "dbfilename" => Ok(frame_bytes!(list => [
                            frame!(bulk s),
                            frame!(bulk self.config.dbfilename.as_deref().unwrap_or(""))
                        ])),
                        _ => Ok(frame_bytes!(list => [])),
                    }
                }
                RC::Keys(ref query) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received KEYS command with pattern: {}", query);
                    let query = query.replace('*', ".*");
                    info!("Translated query regex: {}", query);

                    info!("Attempting to lock cache for KEYS");
                    let cache = self.cache.lock().await;
                    info!("Cache lock acquired for KEYS");

                    let regex = match Regex::new(&query) {
                        Ok(regex) => regex,
                        Err(_) => return Err(RespError::InvalidArgument),
                    };
                    let matching_keys: Vec<Frame> = cache
                        .keys()
                        .filter(|key| regex.is_match(key))
                        .map(|key| Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())))
                        .collect();
                    info!("Matched {} keys", matching_keys.len());

                    Ok(Frame::List(matching_keys).to_resp())
                }
                RC::Info(ref _sub_command) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received INFO command");
                    info!("Attempting to lock state for INFO");
                    let state = self.state.lock().await;
                    info!("State lock acquired for INFO");

                    let info_response = format!(
                        "# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                        state.replid, state.current_offset,
                    );
                    info!("Generated INFO response");

                    Ok(frame_bytes!(bulk info_response))
                }
                RC::ReplConf(ref _args) => {
                    if self
                        .check_and_queue_transaction(&command, connection_socket)
                        .await
                    {
                        return Ok(frame_bytes!("QUEUED"));
                    }
                    info!("Received REPLCONF command");
                    // Master receives ACKs, but doesn't send a response to them.
                    // Other REPLCONFs are part of handshake.
                    Ok(frame_bytes!("OK"))
                }
                RC::Psync(_) => {
                    info!("Received PSYNC command (not handled here)");
                    // PSYNC is handled specially in `handle_client`, this is a fallback.
                    Err(RespError::InvalidStreamOperation)
                }
                RC::Rpush { key, elements } => {
                    info!(
                        "Recieved RPUSH command with key: {}, and elements: {:?}",
                        key, elements
                    );
                    info!("Attempting to acquire cache lock");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired");

                    // Get or insert an empty array
                    let entry = cache.entry(key.clone()).or_insert_with(|| CacheEntry {
                        value: frame!(list => []),
                        expires_at: None,
                    });

                     match &mut entry.value {
                         Frame::List(arr) => {
                             // Convert Vec<String> into Frames and append
                              arr.extend(elements.into_iter().map(|s| frame!(bulk s)));

                              let new_len = arr.len();

                              // Notify any waiting BLPOP commands
                              drop(cache); // Release lock before notifying
                              info!("Notifying waiting BLPOP clients for key: {}", key);
                               if let Some(blocked_client) = self.blocking_queue.lock().await.dequeue(&key) {
                                   blocked_client.notifier.notify_waiters();
                               }

                              return Ok(frame_bytes!(int new_len));
                         }
                         _ => {
                             // WRONGTYPE error if key exists but isn’t a list
                             return Err(RespError::WrongType);
                         }
                     }
                }
                RC::Lpush { key, elements } => {
                    info!(
                        "Recieved LPUSH command with key: {}, and elements: {:?}",
                        key, elements
                    );
                    info!("Attempting to acquire cache lock");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired");

                    // Get or insert an empty array
                    let entry = cache.entry(key.clone()).or_insert_with(|| CacheEntry {
                        value: frame!(list => []),
                        expires_at: None,
                    });

                     match &mut entry.value {
                         Frame::List(arr) => {
                             // Convert Vec<String> into BulkString frames
                             let mut new_frames: Vec<Frame> =
                                 elements.into_iter().map(|s| frame!(bulk s)).rev().collect();

                              // Prepend by inserting at the front
                              new_frames.append(arr); // moves arr to the back of new_frames
                              *arr = new_frames;

                              let new_len = arr.len();

                              // Notify any waiting BLPOP commands
                              drop(cache); // Release lock before notifying
                              info!("Notifying waiting BLPOP clients for key: {}", key);
                               if let Some(blocked_client) = self.blocking_queue.lock().await.dequeue(&key) {
                                   blocked_client.notifier.notify_waiters();
                               }

                              return Ok(frame_bytes!(int new_len));
                         }
                         _ => {
                             // WRONGTYPE error if key exists but isn’t a list
                             return Err(RespError::WrongType);
                         }
                     }
                }
                RC::Lrange {
                    key,
                    start_idx,
                    end_idx,
                } => {
                    info!(
                        "Received Lrange command with key: {} start_idx: {}, end_idx: {}",
                        key, start_idx, end_idx
                    );
                    let cache = self.cache.lock().await;
                    let start_idx = start_idx as isize;
                    let end_idx = end_idx as isize;

                    match cache.get(&key) {
                        Some(entry) => match &entry.value {
                            Frame::List(arr) => {
                                let len = arr.len() as isize;

                                // Normalize negative indices
                                let mut start = if start_idx < 0 {
                                    len + start_idx
                                } else {
                                    start_idx
                                };
                                let mut end = if end_idx < 0 { len + end_idx } else { end_idx };

                                // Clamp to valid range
                                if start < 0 {
                                    start = 0;
                                }
                                if end < 0 {
                                    end = 0;
                                }
                                if start >= len {
                                    return Ok(frame_bytes!(list => Vec::<Frame>::new()));
                                }
                                if end >= len {
                                    end = len - 1;
                                }

                                // If range is invalid (start > end) → empty array
                                if start > end {
                                    return Ok(frame_bytes!(list => vec![]));
                                }

                                // Slice is inclusive of end
                                let slice = &arr[start as usize..=end as usize];
                                let response = slice.to_vec();

                                Ok(frame_bytes!(list => response))
                            }
                            _ => Err(RespError::WrongType),
                        },
                        None => Ok(frame_bytes!(list => vec![])), // non-existent key → empty array
                    }
                }
                RC::Llen { key } => {
                    info!("Recieved Llen command with key: {}", key);
                    info!("Attempting to acquire cache lock");
                    let mut cache = self.cache.lock().await;
                    info!("Cache lock acquired");
                    match cache.entry(key) {
                        Entry::Occupied(occupied_entry) => {
                            let frame = &occupied_entry.get().value;
                            match frame {
                                Frame::List(arr) => Ok(frame_bytes!(int arr.len())),
                                _ => Err(RespError::WrongType),
                            }
                        }
                        Entry::Vacant(_) => Ok(frame_bytes!(int 0)),
                    }
                }
                RC::Lpop {
                    key,
                    number_of_items,
                } => {
                    info!(
                        "Received LPOP command with key: {}, and number_of_items: {:?}",
                        key, number_of_items
                    );
                    let mut cache = self.cache.lock().await;

                    match cache.entry(key) {
                        Entry::Occupied(mut occupied_entry) => {
                            let frame = &mut occupied_entry.get_mut().value;
                            match frame {
                                Frame::List(arr) => {
                                    let count = number_of_items.unwrap_or(1) as usize;
                                    let len = arr.len();

                                    if len == 0 {
                                        // Empty list behaves like non-existent
                                        occupied_entry.remove();
                                        return Ok(frame_bytes!(null));
                                    }

                                    if count == 1 {
                                        // Single element → BulkString
                                        let value = arr.remove(0);
                                        if arr.is_empty() {
                                            occupied_entry.remove(); // delete key if list now empty
                                        }
                                        return Ok(value.to_resp());
                                    } else {
                                        // Multiple elements → Array
                                        let actual = count.min(len);
                                        let drained: Vec<_> = arr.drain(..actual).collect();
                                        if arr.is_empty() {
                                            occupied_entry.remove(); // delete key if list now empty
                                        }
                                        return Ok(frame_bytes!(list => drained));
                                    }
                                }
                                _ => Err(RespError::WrongType),
                            }
                        }
                        Entry::Vacant(_) => Ok(frame_bytes!(null)),
                    }
                }
                RC::Blpop { key: _, time_sec: _ } => {
                    // BLPOP is handled specially in client_handler.rs for blocking behavior
                    // This should not be reached in normal operation
                    Ok(frame_bytes!(null_list))
                }
                RC::Multi => {
                    info!("Received Multi command");
                    info!("Starting transaction");
                    let transactions = self.transactions.as_ref();
                    let mut transactions_guard = transactions.lock().await;
                    transactions_guard.insert(connection_socket, Transaction::multi());
                    Ok(frame_bytes!("OK"))
                }
                RC::Discard => {
                    info!("Received Discard command");
                    let transactions = self.transactions.as_ref();
                    let mut transactions_guard = transactions.lock().await;
                    let mut response = Err(RespError::DiscardWithoutMulti);
                    match transactions_guard.entry(connection_socket) {
                        Entry::Occupied(mut entry) => {
                            // Key does exist meaning MULTI has at least executed once
                            let transaction = entry.get_mut();
                            match transaction.state() {
                                transaction::TxState::Idle => {
                                    unreachable!()
                                }
                                transaction::TxState::Queuing => {
                                    response = Ok(frame_bytes!("OK"));
                                    if !transaction.is_empty_queue() {
                                        transaction.discard();
                                        transactions_guard.remove(&connection_socket);
                                    }
                                }
                                transaction::TxState::Discarded => unreachable!(),
                            }
                        }
                        Entry::Vacant(_) => {
                            // Key doesn't exist meaning there is no transaction started and I should
                            // return the default response which is the DiscardWithoutMulti error
                        }
                    }
                    response
                }
                RC::Exec => {
                    info!("Received EXEC command");
                    let transactions = self.transactions.as_ref();
                    let mut transactions_guard = transactions.lock().await;
                    let mut response = Err(RespError::ExecWithoutMulti);
                    match transactions_guard.entry(connection_socket) {
                        Entry::Occupied(mut entry) => {
                            // Key does exist meaning MULTI has at least executed once
                            let transaction = entry.get_mut();
                            match transaction.state() {
                                transaction::TxState::Idle => {}
                                transaction::TxState::Queuing => {
                                    if transaction.is_empty_queue() {
                                        info!("State is queuing and commands queue is empty");
                                        response = Ok(frame_bytes!(list => vec![]));
                                        drop(transactions_guard);
                                    } else {
                                        // execute the commands
                                        info!("State is queuing and commands isn't empty");
                                        let commands = transaction.commands.clone();
                                        info!("commands.len() = {}", commands.len());
                                        let mut arr = Vec::new();
                                        info!("The commands to be executed are : {:?}", commands);
                                        transaction.state = TxState::Idle;
                                        drop(transactions_guard);
                                        for (i, cmd) in commands.iter().enumerate() {
                                            info!("i: {i}, Executing command {cmd:?} in Exec");
                                            match self.execute(cmd.clone(), connection_socket).await
                                            {
                                                Ok(bytes) => arr.push(bytes),
                                                Err(error) => arr.push(error.to_resp()),
                                            }
                                        }

                                        let arr_clone = arr.clone();
                                        let flatten_arr = arr_clone
                                            .into_iter()
                                            .filter(|bytes| Frame::from_bytes(&bytes).is_ok())
                                            .map(|bytes| Frame::from_bytes(&bytes).unwrap().0)
                                            .collect();

                                        response = Ok(frame_bytes!(list => flatten_arr))
                                    }
                                }
                                transaction::TxState::Discarded => todo!(),
                            }
                        }
                        Entry::Vacant(_) => {
                            drop(transactions_guard);
                            // Key doesn't exist meaning there is no transaction started and I should
                            // return the default response which is the ExecWithoutMulti error
                        }
                    }

                    let mut transactions_guard = transactions.lock().await;
                    info!("Clearning transaction after EXEC");
                    transactions_guard.remove(&connection_socket);
                    response
                }
                RC::Wait((no_replicas, time_in_ms)) => {
                    info!(
                        "Received WAIT command: replicas={}, timeout={}ms",
                        no_replicas, time_in_ms
                    );

                    let num_needed = match no_replicas.parse::<usize>() {
                        Ok(n) => {
                            info!("Parsed replicas needed: {}", n);
                            n
                        }
                        Err(_) => {
                            info!("Failed to parse replicas number");
                            return Err(RespError::InvalidArgument);
                        }
                    };

                    let timeout = Duration::from_millis(match time_in_ms.parse::<u64>() {
                        Ok(t) => {
                            info!("Parsed timeout: {}ms", t);
                            t
                        }
                        Err(_) => {
                            info!("Failed to parse timeout");
                            return Err(RespError::InvalidArgument);
                        }
                    });

                    info!("Attempting to lock state for WAIT");
                    let required_offset = self.state.lock().await.current_offset;
                    info!(
                        "State lock acquired for WAIT. Required offset={}",
                        required_offset
                    );

                    // Function to ask the actor for the current count
                    let get_current_ack_count = || async {
                        info!("Requesting current ack count...");
                        let acks_guard = self.acks.lock().await;
                        let count = acks_guard
                            .values()
                            .inspect(|&&off| {
                                info!("offset: {}, required_offset: {}", off, required_offset)
                            })
                            .filter(|&&off| off >= required_offset)
                            .count();
                        info!("Ack count received: {}", count);
                        count
                    };

                    if required_offset == 0 {
                        info!("Offset=0, returning replica count directly");
                        let num_replicas = get_current_ack_count().await;
                        info!("Number of replicas: {}", num_replicas);
                        return Ok(frame_bytes!(int num_replicas as u64));
                    }

                    // Check if the condition is already met before waiting.
                    let initial_count = get_current_ack_count().await;
                    if initial_count >= num_needed {
                        info!(
                            "Condition already met ({} >= {})",
                            initial_count, num_needed
                        );
                        return Ok(frame_bytes!(int initial_count as u64));
                    }

                    info!("Broadcasting GETACK to replicas");
                    let getack_cmd = frame_bytes!(list => [
                        frame!(bulk "REPLCONF"),
                        frame!(bulk "GETACK"),
                        frame!(bulk "*")
                    ]);

                    let _ = self
                        .replication_msg_sender
                        .send(ReplicationMsg::Broadcast(getack_cmd))
                        .await;

                    // This is our main waiting future.
                    let wait_fut = async {
                        loop {
                            info!("WAIT: waiting for notification...");
                            self.notification_manager.wait_for(NotifierType::Ack).await;
                            info!("WAIT: notified!");

                            let current_count = get_current_ack_count().await;
                            info!(
                                "WAIT: current_count={}, needed={}",
                                current_count, num_needed
                            );
                            if current_count >= num_needed {
                                info!("WAIT condition met!");
                                return current_count;
                            }
                        }
                    };

                    let final_count = if timeout.as_millis() == 0 {
                        info!("WAIT timeout=0, returning immediately");
                        get_current_ack_count().await
                    } else {
                        match tokio::time::timeout(timeout, wait_fut).await {
                            Ok(count) => {
                                info!("WAIT completed successfully with count={}", count);
                                count
                            }
                            Err(_) => {
                                info!("WAIT timed out");
                                get_current_ack_count().await
                            }
                        }
                    };

                    info!("WAIT returning final_count={}", final_count);
                    Ok(frame_bytes!(int final_count as u64))
                }
                RC::Subscribe { .. } | RC::Unsubscribe { .. } | RC::Publish { .. } => todo!(),
                RC::Invalid => {
                    info!("Received INVALID command");
                    Err(RespError::InvalidCommandSyntax)
                }
            }
        })
    }
}
