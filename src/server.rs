use crate::frame::Frame;
use crate::frame::{ParsedStreamId, StreamEntry, StreamId};
use crate::rdb::{ExpiryUnit, FromBytes, RDBFile, RedisValue};
use crate::resp_commands::{ExpiryOption, RedisCommand, SetCondition};
use crate::resp_parser::parse;
use crate::shared_cache::{Cache, CacheEntry};
use bytes::Bytes;
use regex::Regex;
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, mpsc::Sender};
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

// function for debugging purposes
pub fn bytes_to_ascii(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            b'\0' => out.push_str("\\0"),
            0x20..=0x7e => out.push(b as char), // printable ASCII
            _ => out.push_str(&format!("\\x{:02x}", b)), // hex escape
        }
    }
    out
}

pub type SharedMut<T> = Arc<Mutex<T>>;
pub type Shared<T> = Arc<T>;
pub type BoxedAsyncWrite = Box<dyn AsyncWrite + Unpin + Send>;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Slave,
}

#[derive(Debug)]
pub enum ServerState {
    MasterState(SharedMut<MasterState>),
    SlaveState(SharedMut<SlaveState>),
}

#[derive(Debug)]
pub struct MasterState {
    pub role: ServerRole,
    pub replid: String,
    pub current_offset: usize,
}

// Slave-specific state
#[derive(Debug)]
pub struct SlaveState {
    pub master_replid: String,
    pub master_repl_offset: usize,
    pub master_host: String,
    pub master_port: String,
    pub role: ServerRole,
    pub connection: Option<Arc<Mutex<TcpStream>>>,
}

#[derive(Debug)]
pub struct ReplicaConnection {
    pub port: String,
    pub connection: Arc<Mutex<TcpStream>>,
}

#[derive(Debug)]
pub enum ReplicationMsg<W: AsyncWrite + Send + Unpin + 'static> {
    Broadcast(Vec<u8>),
    AddReplica(SocketAddr, SharedMut<W>),
    RemoveReplica(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct MasterServer<W: AsyncWrite + Send + Unpin + 'static> {
    config: Shared<ServerConfig>,
    cache: SharedMut<Cache>,
    replication_msg_sender: Sender<ReplicationMsg<W>>, // channel to send all that concerns replicaion nodes
    state: SharedMut<MasterState>,
    pub acks: SharedMut<HashMap<SocketAddr, usize>>,
    pub ack_notifier: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub struct SlaveServer {
    config: Shared<ServerConfig>,
    state: SharedMut<SlaveState>,
    cache: SharedMut<Cache>,
}

#[derive(Debug, Clone)]
pub enum RedisServer<W: AsyncWrite + Send + Unpin + 'static> {
    Master(MasterServer<W>),
    Slave(SlaveServer),
}

pub trait CommandHandler<W: AsyncWrite + Send + Unpin + 'static> {
    async fn execute(&self, command: RedisCommand) -> Vec<u8>;
}

trait SlaveRole {
    async fn connect(&self) -> Result<TcpStream, std::io::Error>;
    async fn handshake(&mut self) -> Result<(), String>;
    async fn start_replication_handler(self, rest: Vec<u8>) -> Result<(), String>;
}

impl<W: AsyncWrite + Send + Unpin + 'static> RedisServer<W> {
    pub async fn new() -> Result<Option<RedisServer<W>>, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() == 1 {
            return Ok(None);
        }

        let mut redis_server = RedisServer::new_master();
        let mut dir = None;
        let mut dbfilename = None;

        let mut i = 1; // Skip program name
        while i < args.len() {
            match args[i].as_str() {
                "--dir" => {
                    if i + 1 >= args.len() {
                        return Err("--dir requires a value".to_string());
                    }
                    dir = Some(args[i + 1].clone());
                    i += 2;
                }
                "--dbfilename" => {
                    if i + 1 >= args.len() {
                        return Err("--dbfilename requires a value".to_string());
                    }
                    dbfilename = Some(args[i + 1].clone());
                    i += 2;
                }
                "--port" => {
                    if i + 1 >= args.len() {
                        return Err("--port requires a value".to_string());
                    }
                    redis_server.set_port(args[i + 1].clone());
                    i += 2;
                }
                "--replicaof" => {
                    if i + 1 >= args.len() {
                        return Err("--replicaof requires a value".to_string());
                    }

                    let info = args[i + 1].clone();
                    let (master_host, master_port) = info.split_once(' ').ok_or_else(|| {
                        "Invalid --replicaof format. Expected 'host port'".to_string()
                    })?;

                    // Get current port or use default
                    let current_port = redis_server.port().to_string();

                    // Create new slave server
                    redis_server = RedisServer::new_slave(
                        current_port,
                        master_host.to_string(),
                        master_port.to_string(),
                    );

                    // Perform handshake
                    if let RedisServer::Slave(ref mut slave) = redis_server {
                        slave.handshake().await?;
                    }

                    i += 2;
                }
                _ => {
                    return Err(format!("Unknown argument: {}", args[i]));
                }
            }
        }

        // Set dir and dbfilename after server is finalized
        redis_server.set_dir(dir);
        redis_server.set_dbfilename(dbfilename);

        Ok(Some(redis_server))
    }

    pub async fn execute(&self, command: RedisCommand) -> Vec<u8> {
        match self {
            RedisServer::Master(master) => master.execute(command).await,
            RedisServer::Slave(slave) => {
                <SlaveServer as CommandHandler<W>>::execute(slave, command).await
            }
        }
    }

    pub fn get_server_state_owned(&self) -> ServerState {
        match self {
            RedisServer::Master(master) => {
                ServerState::MasterState(master.get_server_state().clone())
            }
            RedisServer::Slave(slave) => ServerState::SlaveState(slave.get_server_state().clone()),
        }
    }

    pub async fn get_replication_msg_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::Sender<ReplicationMsg<W>>> {
        match self {
            RedisServer::Master(master) => Some(master.replication_msg_sender.clone()),
            RedisServer::Slave(_) => None,
        }
    }

    pub fn role(&self) -> &str {
        match self {
            Self::Master(m) => m.role(),
            Self::Slave(s) => s.role(),
        }
    }

    pub fn is_master(&self) -> bool {
        match self {
            Self::Master(m) => m.is_master(),
            Self::Slave(s) => s.is_master(),
        }
    }

    pub fn is_slave(&self) -> bool {
        match self {
            Self::Master(m) => m.is_slave(),
            Self::Slave(s) => s.is_slave(),
        }
    }

    pub fn config(&self) -> Shared<ServerConfig> {
        match self {
            Self::Master(m) => m.config(),
            Self::Slave(s) => s.config(),
        }
    }

    pub fn cache(&self) -> &SharedMut<Cache> {
        match self {
            Self::Master(m) => m.cache(),
            Self::Slave(s) => s.cache(),
        }
    }

    pub fn new_master() -> Self {
        let master = RedisServer::Master(MasterServer::new());
        master
    }

    pub fn new_slave(port: String, master_host: String, master_port: String) -> Self {
        RedisServer::Slave(SlaveServer::new(port, master_host, master_port))
    }

    // Helper methods to access common fields regardless of variant
    pub fn port(&self) -> &str {
        match self {
            RedisServer::Master(master) => &master.config.port,
            RedisServer::Slave(slave) => &slave.config.port,
        }
    }

    pub fn set_port(&mut self, port: String) {
        match self {
            RedisServer::Master(master) => {
                // Create new config with updated port
                let new_config = Arc::new(ServerConfig {
                    port,
                    dir: master.config.dir.clone(),
                    dbfilename: master.config.dbfilename.clone(),
                });
                master.config = new_config;
            }
            RedisServer::Slave(slave) => {
                let new_config = Arc::new(ServerConfig {
                    port,
                    dir: slave.config.dir.clone(),
                    dbfilename: slave.config.dbfilename.clone(),
                });
                slave.config = new_config;
            }
        }
    }

    pub fn dir(&self) -> &Option<String> {
        match self {
            RedisServer::Master(master) => &master.config.dir,
            RedisServer::Slave(slave) => &slave.config.dir,
        }
    }

    pub fn set_dir(&mut self, dir: Option<String>) {
        match self {
            RedisServer::Master(master) => {
                let new_config = Arc::new(ServerConfig {
                    dir,
                    port: master.config.port.clone(),
                    dbfilename: master.config.dbfilename.clone(),
                });
                master.config = new_config;
            }
            RedisServer::Slave(slave) => {
                let new_config = Arc::new(ServerConfig {
                    dir,
                    port: slave.config.port.clone(),
                    dbfilename: slave.config.dbfilename.clone(),
                });
                slave.config = new_config;
            }
        }
    }

    pub fn dbfilename(&self) -> &Option<String> {
        match self {
            RedisServer::Master(master) => &master.config.dbfilename,
            RedisServer::Slave(slave) => &slave.config.dbfilename,
        }
    }

    pub fn set_dbfilename(&mut self, dbfilename: Option<String>) {
        match self {
            RedisServer::Master(master) => {
                let new_config = Arc::new(ServerConfig {
                    dbfilename,
                    port: master.config.port.clone(),
                    dir: master.config.dir.clone(),
                });
                master.config = new_config;
            }
            RedisServer::Slave(slave) => {
                let new_config = Arc::new(ServerConfig {
                    dbfilename,
                    port: slave.config.port.clone(),
                    dir: slave.config.dir.clone(),
                });
                slave.config = new_config;
            }
        }
    }

    pub fn set_cache(&mut self, cache: &SharedMut<Cache>) {
        match self {
            RedisServer::Master(master) => master.cache = cache.clone(),
            RedisServer::Slave(slave) => slave.cache = cache.clone(),
        }
    }

    pub async fn repl_offset(&self) -> usize {
        match self {
            RedisServer::Master(master) => master.state.lock().await.current_offset,
            RedisServer::Slave(slave) => slave.state.lock().await.master_repl_offset,
        }
    }

    pub async fn set_repl_offset(&mut self, repl_offset: usize) {
        match self {
            RedisServer::Master(master) => {
                master.state.lock().await.current_offset = repl_offset;
            }
            RedisServer::Slave(slave) => slave.state.lock().await.master_repl_offset = repl_offset,
        }
    }

    pub async fn repl_offset_increment(&mut self, amount: usize) {
        match self {
            RedisServer::Master(master) => master.state.lock().await.current_offset += amount,
            RedisServer::Slave(slave) => slave.state.lock().await.master_repl_offset += amount,
        }
    }
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

impl MasterState {
    pub fn role(&self) -> &ServerRole {
        &self.role
    }

    pub fn replid(&self) -> &str {
        &self.replid
    }

    pub fn current_offset(&self) -> usize {
        self.current_offset
    }

    pub fn set_role(&mut self, role: ServerRole) {
        self.role = role;
    }

    pub fn set_replid<S: Into<String>>(&mut self, replid: S) {
        self.replid = replid.into();
    }

    pub fn set_current_offset(&mut self, offset: usize) {
        self.current_offset = offset;
    }

    pub fn increment_offset(&mut self, delta: usize) {
        self.current_offset += delta;
    }
}

impl SlaveState {
    pub fn master_replid(&self) -> &str {
        &self.master_replid
    }

    pub fn master_repl_offset(&self) -> usize {
        self.master_repl_offset
    }

    pub fn master_host(&self) -> &str {
        &self.master_host
    }

    pub fn master_port(&self) -> &str {
        &self.master_port
    }

    pub fn role(&self) -> &ServerRole {
        &self.role
    }

    pub fn connection(&self) -> Option<&Arc<Mutex<TcpStream>>> {
        self.connection.as_ref()
    }

    pub fn set_master_replid<S: Into<String>>(&mut self, replid: S) {
        self.master_replid = replid.into();
    }

    pub fn set_master_repl_offset(&mut self, offset: usize) {
        self.master_repl_offset = offset;
    }

    pub fn increment_master_repl_offset(&mut self, delta: usize) {
        self.master_repl_offset += delta;
    }

    pub fn set_master_host<S: Into<String>>(&mut self, host: S) {
        self.master_host = host.into();
    }

    pub fn set_master_port<S: Into<String>>(&mut self, port: S) {
        self.master_port = port.into();
    }

    pub fn set_role(&mut self, role: ServerRole) {
        self.role = role;
    }

    pub fn set_connection(&mut self, conn: Option<Arc<Mutex<TcpStream>>>) {
        self.connection = conn;
    }

    pub fn clear_connection(&mut self) {
        self.connection = None;
    }
}

pub trait ServerStateTrait {
    fn is_master(&self) -> bool;

    fn is_slave(&self) -> bool;

    fn as_master(&self) -> Option<&SharedMut<MasterState>>;

    fn as_master_mut(&mut self) -> Option<&mut SharedMut<MasterState>>;

    fn as_slave(&self) -> Option<&SharedMut<SlaveState>>;

    fn as_slave_mut(&mut self) -> Option<&mut SharedMut<SlaveState>>;
}

impl ServerStateTrait for ServerState {
    fn is_master(&self) -> bool {
        matches!(self, ServerState::MasterState(_))
    }

    fn is_slave(&self) -> bool {
        matches!(self, ServerState::SlaveState(_))
    }

    fn as_master(&self) -> Option<&SharedMut<MasterState>> {
        match self {
            ServerState::MasterState(state) => Some(state),
            _ => None,
        }
    }

    fn as_master_mut(&mut self) -> Option<&mut SharedMut<MasterState>> {
        match self {
            ServerState::MasterState(state) => Some(state),
            _ => None,
        }
    }

    fn as_slave(&self) -> Option<&SharedMut<SlaveState>> {
        match self {
            ServerState::SlaveState(state) => Some(state),
            _ => None,
        }
    }

    fn as_slave_mut(&mut self) -> Option<&mut SharedMut<SlaveState>> {
        match self {
            ServerState::SlaveState(state) => Some(state),
            _ => None,
        }
    }
}

impl<W: AsyncWrite + Send + Unpin> MasterServer<W> {
    fn new() -> Self {
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
        let acks = Arc::new(Mutex::new(HashMap::new()));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicationMsg<W>>(256);

        tokio::spawn(async move {
            let mut replicas: HashMap<SocketAddr, SharedMut<W>> = HashMap::new();

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
        }
    }

    fn role(&self) -> &str {
        "master"
    }
    fn is_master(&self) -> bool {
        true
    }

    fn is_slave(&self) -> bool {
        false
    }

    fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    fn cache(&self) -> &SharedMut<Cache> {
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

impl<W: AsyncWrite + Send + Unpin + 'static> CommandHandler<W> for MasterServer<W> {
    async fn execute(&self, command: RedisCommand) -> Vec<u8> {
        use RedisCommand as RC;

        fn resolve_stream_id(
            parsed_id: ParsedStreamId,
            last_id: Option<StreamId>,
        ) -> Result<StreamId, String> {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            match parsed_id {
                ParsedStreamId::Literal(id) => Ok(id),
                ParsedStreamId::AutoSequence { ms_time } => {
                    if let Some(last) = last_id {
                        if last.ms_time == ms_time {
                            Ok(StreamId {
                                ms_time,
                                seq: last.seq + 1,
                            })
                        } else {
                            // For different timestamp, start from 0, but ensure it's not 0-0
                            let seq = if ms_time == 0 { 1 } else { 0 };
                            Ok(StreamId { ms_time, seq })
                        }
                    } else {
                        // No previous entries, ensure it's not 0-0
                        let seq = if ms_time == 0 { 1 } else { 0 };
                        Ok(StreamId { ms_time, seq })
                    }
                }
                ParsedStreamId::Auto => {
                    if let Some(last) = last_id {
                        if last.ms_time == now {
                            Ok(StreamId {
                                ms_time: now,
                                seq: last.seq + 1,
                            })
                        } else {
                            // For different timestamp, start from 0, but ensure it's not 0-0
                            let seq = if now == 0 { 1 } else { 0 };
                            Ok(StreamId { ms_time: now, seq })
                        }
                    } else {
                        // No previous entries, ensure it's not 0-0
                        let seq = if now == 0 { 1 } else { 0 };
                        Ok(StreamId { ms_time: now, seq })
                    }
                }
            }
        }

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
                key,
                parsed_id,
                fields,
            } => {
                info!("Received XADD command for key: {}", key);
                info!("Attempting to lock cache for SET");
                let mut cache = self.cache.lock().await;
                info!("Cache lock acquired for SET");

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
                    Err(e) => return resp_bytes!(error e),
                };

                let stream_entry = StreamEntry::new(stream_id, fields);

                if let Some(entry) = cache.get_mut(&key) {
                    if let Frame::Stream(ref mut vec) = entry.value {
                        if stream_id == (StreamId { ms_time: 0, seq: 0 }) {
                            return resp_bytes!(error "ERR The ID specified in XADD must be greater than 0-0");
                        }
                        if stream_id <= vec.last().unwrap().id {
                            return resp_bytes!(error "ERR The ID specified in XADD is equal or smaller than the target stream top item");
                        } else {
                            vec.push(stream_entry);
                        }
                    } else {
                        entry.value = Frame::Stream(vec![stream_entry]);
                    }
                } else {
                    cache.insert(
                        key.clone(),
                        CacheEntry {
                            value: Frame::Stream(vec![stream_entry]),
                            expires_at: None,
                        },
                    );
                }

                info!("Inserted/key {}", key);

                drop(cache);

                resp_bytes!(bulk stream_id.to_string())
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
            RC::Type(key) => {
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
                key,
                parsed_id,
                fields,
            } => {
                todo!("Implement XADD for slaves")
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
    fn new(port: String, master_host: String, master_port: String) -> Self {
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

    fn role(&self) -> &str {
        "slave"
    }
    fn is_master(&self) -> bool {
        false
    }

    fn is_slave(&self) -> bool {
        true
    }

    fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    fn cache(&self) -> &SharedMut<Cache> {
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
