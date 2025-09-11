use crate::frame::Frame;
use crate::rdb::{ExpiryUnit, FromBytes, RDBFile, RedisValue};
use crate::commands::RedisCommand;
use crate::parser::parse;
use crate::shared_cache::{Cache, CacheEntry};
use crate::types::*;
use bytes::Bytes;
use regex::Regex;
use std::{collections::HashMap, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct SlaveServer {
    pub config: Shared<ServerConfig>,
    pub state: SharedMut<SlaveState>,
    pub cache: SharedMut<Cache>,
}

pub trait SlaveRole {
    fn connect(&self) -> impl Future<Output = Result<TcpStream, std::io::Error>>;
    fn handshake(&mut self) -> impl Future<Output = Result<(), String>>;
    fn start_replication_handler(self, rest: Vec<u8>) -> impl Future<Output = Result<(), String>>;
}

impl SlaveRole for SlaveServer {
    async fn connect(&self) -> Result<TcpStream, std::io::Error> {
        let state = self.state.lock().await;
        let master_address = format!("{}:{}", state.master_host, state.master_port);
        TcpStream::connect(master_address).await
    }

    async fn handshake(&mut self) -> Result<(), String> {
        match self.connect().await {
            Ok(mut stream) => {
                let mut buffer = [0; 1024];

                let mut send_command = async |command: &[u8], read: bool| -> Result<(), String> {
                    stream
                        .write_all(command)
                        .await
                        .map_err(|e| format!("Failed to send: {}", e))?;

                    if read {
                        match stream.read(&mut buffer).await {
                            Ok(0) | Err(_) => return Ok(()), // connection closed or error
                            Ok(_) => Ok(()),
                        }
                    } else {
                        Ok(())
                    }
                };

                // Step1: PING
                send_command(&resp_bytes!(array => [resp!(bulk "PING")]), true).await?;

                let port = self.config.port.clone();
                // Step2: REPLCONF listening-port <PORT>
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "REPLCONF"),
                        resp!(bulk "listening-port"),
                        resp!(bulk port)
                    ]),
                    true,
                )
                .await?;

                // Step3: REPLCONF capa psync2
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "REPLCONF"),
                        resp!(bulk "capa"),
                        resp!(bulk "psync2")
                    ]),
                    true,
                )
                .await?;

                // Step 4: PSYNC <REPL_ID> <REPL_OFFSSET>
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "PSYNC"),
                        resp!(bulk "?"),
                        resp!(bulk "-1")
                    ]),
                    false,
                )
                .await?;

                // Step 5: Read FULLRESYNC response
                let bytes_read = stream
                    .read(&mut buffer)
                    .await
                    .map_err(|e| format!("Failed to read FULLRESYNC: {}", e))?;
                let (parsed, mut rest) = parse(&buffer[..bytes_read])
                    .map_err(|e| format!("Failed to parse FULLRESYNC: {:?}", e))?;
                match parsed {
                    Frame::SimpleString(s) if s.starts_with("FULLRESYNC") => {
                        // Expected response
                    }
                    _ => return Err("Invalid FULLRESYNC response".to_string()),
                }

                info!("rest: {}", bytes_to_ascii(rest));

                info!("FULLRESYNC response bytes read: {}", bytes_read);

                // So there is an interesting behaviour where the FULLRESYNC + RDB and if you are
                // really lucky the REPLCONF would all get sent in one TCP segment so I shouldn't
                // assume I would get nice segments refelecting each command
                if !rest.is_empty() {
                    match RDBFile::from_bytes(rest) {
                        Ok((rdb_file, bytes_consumed)) => {
                            // Sync the rdb_file with the slave's cache
                            if let Err(e) = self.sync_rdb_to_cache(&rdb_file).await {
                                error!("Failed to sync RDB to cache: {}", e);
                                return Err(format!("RDB sync error: {}", e));
                            }
                            rest = &rest[bytes_consumed..];
                            info!("rdb bytes: {}", bytes_consumed);
                            info!("remaining bytes after rdb: {}", rest.len());
                        }
                        Err(e) => {
                            error!("Failed to parse RDB file: {}", e);
                            return Err(format!("RDB parsing error: {}", e));
                        }
                    }
                }

                // Store the persistent connection
                {
                    self.state.lock().await.connection = Some(Arc::new(Mutex::new(stream)));
                }
                self.clone()
                    .start_replication_handler(rest.to_vec())
                    .await?;

                Ok(())
            }
            Err(e) => Err(format!("Master node doesn't exist: {}", e)),
        }
    }

    async fn start_replication_handler(self, initial_data: Vec<u8>) -> Result<(), String> {
        info!("In start replication handler");
        let state = self.state.clone();
        let server_clone = self.clone(); // Clone self to pass to execute

        // Spawn the background listener thread
        tokio::spawn(async move {
            let result = async {
                info!("Inside the tokio replication handler task");

                // This is our persistent buffer. It starts with the leftover data from the handshake.
                let mut processing_buffer = initial_data;
                let mut temp_read_buffer = [0u8; 1024]; // Temporary buffer for network reads

                loop {
                    'processing: loop {
                        if processing_buffer.is_empty() {
                            break 'processing; // Nothing to do, break to read from network.
                        }

                        // Attempt 1: Try to parse an RDB file.
                        // The RDB file is sent as a RESP Bulk String, so it will start with '$'.
                        if let Ok((rdb_file, bytes_consumed)) =
                            RDBFile::from_bytes(&processing_buffer)
                        {
                            info!("Parsed and consumed RDB file of size {}.", bytes_consumed);
                            // Sync the rdb_file with the slave's cache
                            if let Err(e) = server_clone.sync_rdb_to_cache(&rdb_file).await {
                                error!("Failed to sync RDB to cache: {}", e);
                            }

                            processing_buffer.drain(..bytes_consumed);

                            // After consuming the RDB file, there might be more commands
                            // already in the buffer, so we `continue` the processing loop.
                            continue 'processing;
                        }

                        if let Ok((resp, leftover)) = parse(&processing_buffer) {
                            // A command was successfully parsed.
                            let command_size = processing_buffer.len() - leftover.len();
                            info!("Successfully parsed a command of size {}", command_size);
                            info!("Command from master: {:?}", resp);

                            let command = RedisCommand::from(resp);

                            // Some commands from master (like REPLCONF) require an acknowledgement.
                            let needs_reply = matches!(command, RedisCommand::ReplConf(..));

                            let response = <SlaveServer as CommandHandler<TcpStream>>::execute(
                                &server_clone,
                                command,
                            )
                            .await;

                            if needs_reply && !response.is_empty() {
                                info!("Slave responding to master: {}", bytes_to_ascii(&response));
                                let master_connection = {
                                    let state_guard = state.lock().await;
                                    state_guard.connection.clone()
                                };

                                if let Some(conn_arc) = master_connection {
                                    let mut stream_guard = conn_arc.lock().await;
                                    if let Err(e) = stream_guard.write_all(&response).await {
                                        return Err(e.to_string());
                                    }
                                }
                            }

                            // Update the slave's tracked offset
                            {
                                let mut state_guard = state.lock().await;
                                state_guard.master_repl_offset += command_size;
                                info!("Slave offset is now: {}", state_guard.master_repl_offset);
                            }

                            // Remove the processed command from the front of the buffer.
                            processing_buffer.drain(..command_size);
                            continue 'processing;
                        }

                        // If we reach here, neither parser could complete.
                        // We have incomplete data. Break the inner loop to read more.
                        info!("Incomplete data in buffer, waiting for more from master.");
                        break 'processing;
                    }

                    // Now, we read more data from the master. This only happens when the
                    // processing_buffer is either empty or contains an incomplete command.
                    let master_connection = {
                        let state_guard = state.lock().await;
                        state_guard.connection.clone()
                    };

                    let bytes_read = if let Some(stream_arc) = master_connection {
                        let mut stream_guard = stream_arc.lock().await;
                        match stream_guard.read(&mut temp_read_buffer).await {
                            Ok(0) => {
                                return Ok(());
                            }
                            Ok(n) => n,
                            Err(e) => {
                                return Err(e.to_string());
                            }
                        }
                    } else {
                        return Err("No connection to master found.".to_string());
                    };

                    info!("Read {} new bytes from master.", bytes_read);

                    // Append the newly read data to our persistent buffer.
                    processing_buffer.extend_from_slice(&temp_read_buffer[..bytes_read]);
                }
            }
            .await;
            if let Err(e) = result {
                error!("Replication handler error: {}", e);
            }
        });
        return Ok(());
    }
}

impl<W: AsyncWrite + Send + Unpin + 'static> CommandHandler<W> for SlaveServer {
    async fn execute(&self, command: RedisCommand) -> Vec<u8> {
        use RedisCommand as RC;
        match command {
            RC::Ping => resp_bytes!("PONG"),
            RC::Echo(echo_string) => resp_bytes!(echo_string),
            RC::Get(key) => {
                let mut cache = self.cache.lock().await;
                match cache.get(&key).cloned() {
                    Some(entry) if !entry.is_expired() => entry.value.to_resp_bytes(),
                    Some(_) => {
                        cache.remove(&key); // Clean up expired key
                        resp_bytes!(null)
                    }
                    None => resp_bytes!(null),
                }
            }
            RC::Type(_key) => {
                todo!()
            }
            RC::ConfigGet(s) => match s.as_str() {
                "dir" => {
                    resp_bytes!(array => [resp!(bulk s), resp!(bulk self.config.dir.as_deref().unwrap_or(""))])
                }
                "dbfilename" => {
                    resp_bytes!(array => [resp!(bulk self.config.dbfilename.as_deref().unwrap_or(""))])
                }
                _ => resp_bytes!(array => []),
            },
            RC::Keys(query) => {
                let query = query.replace('*', ".*");
                let cache = self.cache.lock().await;
                let regex = match Regex::new(&query) {
                    Ok(regex) => regex,
                    Err(_) => return resp_bytes!(error "ERR invalid regex pattern"),
                };
                let matching_keys: Vec<Frame> = cache
                    .keys()
                    .filter(|key| regex.is_match(key))
                    .map(|key| Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())))
                    .collect();
                Frame::Array(matching_keys).to_resp_bytes()
            }
            RC::Info(_sub_command) => {
                // Slaves respond with their role
                let state = self.state.lock().await;
                let info_response = format!(
                    "# Replication\r\nrole:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                    state.master_replid, state.master_repl_offset,
                );
                resp_bytes!(bulk info_response)
            }
            RC::Set(command) => {
                // For a slave, write commands are read-only by default. This could be configurable.
                // If this SET command came from the master, it would be applied.
                // The current setup applies it regardless, which is fine for now.
                let mut cache = self.cache.lock().await;
                let expires_at = command.calculate_expiry_time();
                cache.insert(
                    command.key,
                    CacheEntry {
                        value: Frame::BulkString(command.value.into()),
                        expires_at,
                    },
                );
                // Slaves do not propagate writes and typically respond with OK.
                resp_bytes!("OK")
            }
            RC::Xadd {
                key: _,
                parsed_id: _,
                fields: _,
            } => {
                todo!("Implement XADD for slaves")
            }

            RC::XRange {
                key: _,
                start: _,
                end: _,
            } => {
                todo!("Implement XRange for slaves")
            }
            RC::XRead {
                block_param: _,
                keys: _,
                stream_ids: _,
            } => {
                todo!("Implement XRead for slaves")
            }
            RC::ReplConf((op1, op2)) => {
                if op1.to_uppercase() == "GETACK" && op2 == "*" {
                    let state = self.state.lock().await;
                    resp_bytes!(array => [
                        resp!(bulk "REPLCONF"),
                        resp!(bulk "ACK"),
                        resp!(bulk state.master_repl_offset.to_string())
                    ])
                } else {
                    resp_bytes!("OK") // For other REPLCONFs during handshake
                }
            }
            RC::Psync(_) => resp_bytes!(error "ERR PSYNC not supported on slave"),
            RC::Wait(_) => resp_bytes!(error "ERR WAIT cannot be used with replica."),
            RC::Invalid => resp_bytes!(error "ERR Invalid Command"),
        }
    }
}

impl SlaveServer {
    pub fn new(port: String, master_host: String, master_port: String) -> Self {
        let config = Arc::new(ServerConfig {
            dir: None,
            dbfilename: None,
            port,
        });

        let state = Arc::new(Mutex::new(SlaveState {
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            master_host,
            master_port,
            role: ServerRole::Slave,
            connection: None,
        }));

        let cache = Arc::new(Mutex::new(HashMap::new()));

        Self {
            config,
            state,
            cache,
        }
    }

    pub fn role(&self) -> &str {
        "slave"
    }
    pub fn is_master(&self) -> bool {
        false
    }

    pub fn is_slave(&self) -> bool {
        true
    }

    pub fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    pub fn cache(&self) -> &SharedMut<Cache> {
        &self.cache
    }

    pub async fn increment_repl_offset(&mut self, amount: usize) {
        self.state.lock().await.master_repl_offset += amount;
    }

    pub fn get_server_state(&self) -> &SharedMut<SlaveState> {
        &self.state
    }

    pub fn get_server_state_mut(&mut self) -> &mut SharedMut<SlaveState> {
        &mut self.state
    }

    /// Sync the RDB file data with the slave's cache
    async fn sync_rdb_to_cache(&self, rdb_file: &RDBFile) -> Result<(), String> {
        let mut cache = self.cache.lock().await;

        // Clear existing cache before syncing
        cache.clear();

        // Iterate through all databases in the RDB file
        for (_db_index, database) in &rdb_file.databases {
            for (key_bytes, entry) in &database.hash_table {
                // Convert key from Bytes to String
                let key = match std::str::from_utf8(key_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => continue, // Skip keys that aren't valid UTF-8
                };

                // Convert RedisValue to Frame for cache storage
                let value = match &entry.value {
                    RedisValue::String(bytes) => Frame::RedisString(bytes.clone()),
                    RedisValue::Integer(i) => Frame::Integer(*i),
                    RedisValue::List(items) => Frame::RedisList(items.clone()),
                    RedisValue::Set(items) => Frame::RedisSet(items.clone()),
                    RedisValue::Hash(map) => Frame::RedisHash(map.clone()),
                };

                // Convert expiry time to milliseconds
                let expires_at = match &entry.expiry {
                    Some(expiry) => {
                        let timestamp_ms = match expiry.unit {
                            ExpiryUnit::Seconds => expiry.timestamp * 1000,
                            ExpiryUnit::Milliseconds => expiry.timestamp,
                        };
                        Some(timestamp_ms)
                    }
                    None => None,
                };

                // Insert into cache
                cache.insert(key, CacheEntry { value, expires_at });
            }
        }

        info!(
            "Successfully synced {} databases from RDB to cache",
            rdb_file.databases.len()
        );
        Ok(())
    }
}
