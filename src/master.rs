use crate::frame::Frame;
use crate::commands::{ExpiryOption, RedisCommand, SetCondition};
use crate::shared_cache::{Cache, CacheEntry};
use crate::stream::{StreamEntry, StreamId, XReadStreamId, XrangeStreamdId};
use crate::types::*;
use bytes::Bytes;
use regex::Regex;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify, mpsc::Sender};
use tokio::time::Duration;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct MasterServer {
    pub config: Shared<ServerConfig>,
    pub cache: SharedMut<Cache>,
    pub replication_msg_sender: Sender<ReplicationMsg>, // channel to send all that concerns replicaion nodes
    pub state: SharedMut<MasterState>,
    pub acks: SharedMut<HashMap<SocketAddr, usize>>,
    pub ack_notifier: Arc<Notify>,
    pub xadd_notifier: Arc<Notify>,
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

        let cache = Arc::new(Mutex::new(HashMap::new()));
        let ack_notifier = Arc::new(Notify::new());
        let xadd_notifier = Arc::new(Notify::new());
        let acks = Arc::new(Mutex::new(HashMap::new()));

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
            acks,
            ack_notifier,
            xadd_notifier,
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

impl CommandHandler<BoxedAsyncWrite> for MasterServer {
    async fn execute(&self, command: RedisCommand) -> Vec<u8> {
        use RedisCommand as RC;

        match command {
            RC::Ping => {
                info!("Received PING command");
                resp_bytes!("PONG")
            }
            RC::Echo(echo_string) => {
                info!("Received ECHO command: {}", echo_string);
                resp_bytes!(echo_string)
            }
            RC::Get(key) => {
                info!("Received GET command for key: {}", key);
                info!("Attempting to lock cache for GET");
                let mut cache = self.cache.lock().await;
                info!("Cache lock acquired for GET");

                match cache.get(&key).cloned() {
                    Some(entry) if !entry.is_expired() => {
                        info!("Key {} found and not expired", key);
                        entry.value.to_resp_bytes()
                    }
                    Some(_) => {
                        info!("Key {} expired, removing", key);
                        cache.remove(&key); // Clean up expired key
                        resp_bytes!(null)
                    }
                    None => {
                        info!("Key {} not found", key);
                        resp_bytes!(null)
                    }
                }
            }
            RC::Type(key) => {
                info!("Received TYPE command for key: {}", key);
                info!("Attempting to lock cache for GET");
                let cache = self.cache.lock().await;
                info!("Cache lock acquired for GET");

                match cache.get(&key).cloned() {
                    Some(entry) if !entry.is_expired() => {
                        info!("Key {} found and not expired", key);
                        let type_str = match &entry.value {
                            Frame::SimpleString(_) | Frame::BulkString(_) => "string",
                            Frame::Integer(_) => "string", // Redis treats integers as strings
                            Frame::Array(_) => "list",
                            Frame::Set(_) => "set",
                            Frame::Map(_) => "hash",
                            Frame::Stream(_) => "stream",
                            _ => "string", // Default to string for other types
                        };
                        resp_bytes!(type_str)
                    }
                    Some(_) => {
                        info!("Key {} expired, removing", key);
                        resp_bytes!(null)
                    }
                    None => {
                        info!("Key {} not found", key);
                        resp_bytes!("none")
                    }
                }
            }
            RC::Set(command) => {
                info!("Received SET command for key: {}", command.key);
                info!("Attempting to lock cache for SET");
                let mut cache = self.cache.lock().await;
                info!("Cache lock acquired for SET");

                // Check conditions (NX/XX)
                let key_exists = cache.contains_key(&command.key);
                info!("Key exists? {}", key_exists);

                if (matches!(command.condition, Some(SetCondition::NotExists)) && key_exists)
                    || (matches!(command.condition, Some(SetCondition::Exists)) && !key_exists)
                {
                    info!("SET condition not met for key {}", command.key);
                    return resp_bytes!(null);
                }

                let get_value = if command.get_old_value {
                    let old = cache.get(&command.key).map(|v| v.value.clone());
                    info!("Returning old value for key {}: {:?}", command.key, old);
                    old
                } else {
                    None
                };

                // Calculate expiry
                let expires_at = if let Some(ExpiryOption::KeepTtl) = command.expiry {
                    let ttl = cache.get(&command.key).and_then(|e| e.expires_at);
                    info!("Keeping TTL for key {}: {:?}", command.key, ttl);
                    ttl
                } else {
                    let ttl = command.calculate_expiry_time();
                    info!("Calculated new TTL for key {}: {:?}", command.key, ttl);
                    ttl
                };

                // Set the value
                cache.insert(
                    command.key.clone(),
                    CacheEntry {
                        value: Frame::BulkString(command.value.clone().into()),
                        expires_at,
                    },
                );
                info!("Inserted/updated key {}", command.key);

                drop(cache);
                info!("Released cache lock for SET");

                // Broadcast message to replicas
                let broadcast_cmd = resp_bytes!(array => [
                    resp!(bulk "SET"),
                    resp!(bulk command.key),
                    resp!(bulk command.value)
                ]);

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

                if !command.get_old_value {
                    resp_bytes!("OK")
                } else {
                    match get_value {
                        Some(val) => val.to_resp_bytes(),
                        None => resp_bytes!(null),
                    }
                }
            }
            RC::Xadd {
                key: _,
                parsed_id: _,
                fields: _,
            } => {
                info!("Received Xadd command (not handled here)");
                // PSYNC is handled specially in `handle_client`, this is a fallback.
                resp_bytes!(error "ERR PSYNC logic error")
            }
            RC::XRange { key, start, end } => {
                info!("Received XRange command for key: {}", key);
                info!("Attempting to lock cache for XRANGE");
                let cache = self.cache.lock().await;
                info!("Cache lock acquired for XRANGE");

                if let Some(entry) = cache.get(&key) {
                    if let Frame::Stream(ref vec) = entry.value {
                        info!(
                            "In XRANGE execution and for key {} and the Stream is : {:?}",
                            key, vec
                        );
                        let filtered_streams = match (start, end) {
                            (XrangeStreamdId::Literal(start), XrangeStreamdId::Literal(end)) => vec
                                .iter()
                                .filter(|stream| stream.id >= start && stream.id <= end)
                                .collect::<Vec<&StreamEntry>>(),
                            (
                                XrangeStreamdId::AutoSequence { ms_time: start },
                                XrangeStreamdId::AutoSequence { ms_time: end },
                            ) => vec
                                .iter()
                                .filter(|stream| {
                                    stream.id.ms_time >= start && stream.id.ms_time <= end
                                })
                                .collect::<Vec<&StreamEntry>>(),
                            (
                                XrangeStreamdId::AutoStart,
                                XrangeStreamdId::AutoSequence { ms_time: end },
                            ) => vec
                                .iter()
                                .filter(|stream| stream.id.ms_time <= end)
                                .collect::<Vec<&StreamEntry>>(),
                            (XrangeStreamdId::AutoStart, XrangeStreamdId::Literal(end)) => vec
                                .iter()
                                .filter(|stream| stream.id <= end)
                                .collect::<Vec<&StreamEntry>>(),
                            (
                                XrangeStreamdId::AutoSequence { ms_time: start },
                                XrangeStreamdId::AutoEnd,
                            ) => vec
                                .iter()
                                .filter(|stream| stream.id.ms_time >= start)
                                .collect::<Vec<&StreamEntry>>(),
                            (XrangeStreamdId::Literal(start), XrangeStreamdId::AutoEnd) => vec
                                .iter()
                                .filter(|stream| stream.id >= start)
                                .collect::<Vec<&StreamEntry>>(),
                            (_, _) => unreachable!("This is logically impossible"),
                        };

                        let mut filtered_frames = vec![];
                        for stream in filtered_streams {
                            let id_str = stream.id.to_string();
                            filtered_frames.push(vec![resp!(bulk id_str)]);
                            let fields = stream
                                        .fields
                                        .iter()
                                        .map(|(key, value)| resp!(array => [resp!(bulk key.clone()), resp!(bulk value.clone())]))
                                        .collect::<Vec<Frame>>();
                            filtered_frames
                                .last_mut()
                                .unwrap()
                                .extend_from_slice(&fields);
                        }
                        let filtered_frames: Vec<Frame> = filtered_frames
                            .into_iter()
                            .map(|x| resp!(array => x))
                            .collect();
                        return resp_bytes!(array => filtered_frames);
                    }
                }

                resp_bytes!(error "ERR")
            }

            RC::XRead {
                block_param,
                keys,
                stream_ids,
            } => {
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
                    if timeout_ms == 0 {
                        info!("Blocking XREAD indefinitely until we recieve a notification");
                        self.xadd_notifier.notified().await;
                        info!("XREAD unblocked by notification")
                    } else {
                        info!("Blocking XREAD with timeout: {}ms", timeout_ms);
                        // Wait for notification or timeout
                        let timeout_duration = Duration::from_millis(timeout_ms);
                        let timeout_result =
                            tokio::time::timeout(timeout_duration, self.xadd_notifier.notified())
                                .await;

                        match timeout_result {
                            Ok(_) => info!("XREAD unblocked by notification"),
                            Err(_) => {
                                info!("XREAD timed out after {}ms", timeout_ms);
                                // Return null on timeout with no new entries
                                return resp_bytes!(null_array);
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
                                            vec![resp!(bulk k.clone()), resp!(bulk v.clone())]
                                        })
                                        .collect::<Vec<Frame>>();
                                    // Each entry: ["0-1", ["temperature", "65"]]
                                    let entry = resp!(array => [resp!(bulk id_str), resp!(array => fields)]);
                                    entries.push(entry);
                                }
                                // Wrap into: ["grape", [entries...]]
                                let stream_resp = resp!(array => [resp!(bulk key.clone()), resp!(array => entries)]);
                                stream_responses.push(stream_resp);
                            }
                        }
                    }
                }

                // If no streams have new entries, return null
                if stream_responses.is_empty() {
                    return resp_bytes!(null);
                }

                // Final response is an array of streams
                resp_bytes!(array => stream_responses)
            }
            RC::ConfigGet(s) => {
                info!("Received CONFIG GET for key: {}", s);
                match s.as_str() {
                    "dir" => resp_bytes!(array => [
                        resp!(bulk s),
                        resp!(bulk self.config.dir.as_deref().unwrap_or(""))
                    ]),
                    "dbfilename" => resp_bytes!(array => [
                        resp!(bulk s),
                        resp!(bulk self.config.dbfilename.as_deref().unwrap_or(""))
                    ]),
                    _ => resp_bytes!(array => []),
                }
            }
            RC::Keys(query) => {
                info!("Received KEYS command with pattern: {}", query);
                let query = query.replace('*', ".*");
                info!("Translated query regex: {}", query);

                info!("Attempting to lock cache for KEYS");
                let cache = self.cache.lock().await;
                info!("Cache lock acquired for KEYS");

                let regex = match Regex::new(&query) {
                    Ok(regex) => regex,
                    Err(_) => return resp_bytes!(error "ERR invalid regex pattern"),
                };
                let matching_keys: Vec<Frame> = cache
                    .keys()
                    .filter(|key| regex.is_match(key))
                    .map(|key| Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())))
                    .collect();
                info!("Matched {} keys", matching_keys.len());

                Frame::Array(matching_keys).to_resp_bytes()
            }
            RC::Info(_sub_command) => {
                info!("Received INFO command");
                info!("Attempting to lock state for INFO");
                let state = self.state.lock().await;
                info!("State lock acquired for INFO");

                let info_response = format!(
                    "# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                    state.replid, state.current_offset,
                );
                info!("Generated INFO response");

                resp_bytes!(bulk info_response)
            }
            RC::ReplConf(_) => {
                info!("Received REPLCONF command");
                // Master receives ACKs, but doesn't send a response to them.
                // Other REPLCONF are part of handshake.
                resp_bytes!("OK")
            }
            RC::Psync(_) => {
                info!("Received PSYNC command (not handled here)");
                // PSYNC is handled specially in `handle_client`, this is a fallback.
                resp_bytes!(error "ERR PSYNC logic error")
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
                        return resp_bytes!(error "ERR invalid number of replicas");
                    }
                };

                let timeout = Duration::from_millis(match time_in_ms.parse::<u64>() {
                    Ok(t) => {
                        info!("Parsed timeout: {}ms", t);
                        t
                    }
                    Err(_) => {
                        info!("Failed to parse timeout");
                        return resp_bytes!(error "ERR invalid timeout");
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
                    return resp_bytes!(int num_replicas as u64);
                }

                // Check if the condition is already met before waiting.
                let initial_count = get_current_ack_count().await;
                if initial_count >= num_needed {
                    info!(
                        "Condition already met ({} >= {})",
                        initial_count, num_needed
                    );
                    return resp_bytes!(int initial_count as u64);
                }

                info!("Broadcasting GETACK to replicas");
                let getack_cmd = resp_bytes!(array => [
                    resp!(bulk "REPLCONF"),
                    resp!(bulk "GETACK"),
                    resp!(bulk "*")
                ]);
                let _ = self
                    .replication_msg_sender
                    .send(ReplicationMsg::Broadcast(getack_cmd))
                    .await;

                // This is our main waiting future.
                let wait_fut = async {
                    loop {
                        info!("WAIT: waiting for notification...");
                        self.ack_notifier.notified().await;
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
                resp_bytes!(int final_count as u64)
            }
            RC::Invalid => {
                info!("Received INVALID command");
                resp_bytes!(error "ERR Invalid Command")
            }
        }
    }
}
