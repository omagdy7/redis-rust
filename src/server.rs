use crate::rdb::{FromBytes, RDBFile};
use crate::resp_commands::RedisCommand;
use crate::resp_parser::{RespType, parse};
use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

use crate::shared_cache::Cache;

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

pub trait RedisNode: Send + Sync {
    fn role(&self) -> &str;
    fn is_master(&self) -> bool;
    fn is_slave(&self) -> bool;
    fn config(&self) -> Shared<ServerConfig>;
    fn cache(&self) -> &SharedMut<Cache>;
}

trait SlaveRole: RedisNode {
    async fn connect(&self) -> Result<TcpStream, std::io::Error>;
    async fn handshake(&mut self) -> Result<(), String>;
    // TODO: This should return a Result
    async fn start_replication_handler(self, rest: &mut &[u8]);
}

impl<W: AsyncWrite + Send + Unpin + 'static> RedisNode for MasterServer<W> {
    fn role(&self) -> &str {
        "master"
    }

    fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    fn cache(&self) -> &SharedMut<Cache> {
        &self.cache
    }

    fn is_master(&self) -> bool {
        true
    }

    fn is_slave(&self) -> bool {
        false
    }
}

impl RedisNode for SlaveServer {
    fn config(&self) -> Shared<ServerConfig> {
        self.config.clone()
    }

    fn cache(&self) -> &SharedMut<Cache> {
        &self.cache
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
}

impl<W: AsyncWrite + Send + Unpin + 'static> RedisNode for RedisServer<W> {
    fn role(&self) -> &str {
        match self {
            Self::Master(m) => m.role(),
            Self::Slave(s) => s.role(),
        }
    }

    fn is_master(&self) -> bool {
        match self {
            Self::Master(m) => m.is_master(),
            Self::Slave(s) => s.is_master(),
        }
    }

    fn is_slave(&self) -> bool {
        match self {
            Self::Master(m) => m.is_slave(),
            Self::Slave(s) => s.is_slave(),
        }
    }

    fn config(&self) -> Shared<ServerConfig> {
        match self {
            Self::Master(m) => m.config(),
            Self::Slave(s) => s.config(),
        }
    }

    fn cache(&self) -> &SharedMut<Cache> {
        match self {
            Self::Master(m) => m.cache(),
            Self::Slave(s) => s.cache(),
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
                    RespType::SimpleString(s) if s.starts_with("FULLRESYNC") => {
                        // Expected response
                    }
                    _ => return Err("Invalid FULLRESYNC response".to_string()),
                }

                println!("rest: {}", bytes_to_ascii(rest));

                println!("FULLRESYNC response bytes read: {}", bytes_read);

                // So there is an interesting behaviour where the FULLRESYNC + RDB and if you are
                // really lucky the REPLCONF would all get sent in one TCP segment so I shouldn't
                // assume I would get nice segments refelecting each command
                if !rest.is_empty() {
                    // TODO: Sync the rdb_file with the slave's cache
                    // TODO: Find a way to propagate the error up the stack by using anyhow or something
                    let (rdb_file, bytes_consumed) = RDBFile::from_bytes(rest).unwrap();
                    rest = &rest[bytes_consumed..];
                    println!("rdb bytes: {}", bytes_consumed);
                    println!("remaining bytes after rdb: {}", rest.len());
                }

                // Store the persistent connection
                self.state.lock().await.connection = Some(Arc::new(Mutex::new(stream)));
                self.clone().start_replication_handler(&mut rest).await;

                Ok(())
            }
            Err(e) => Err(format!("Master node doesn't exist: {}", e)),
        }
    }

    // TODO: This should return a Result
    async fn start_replication_handler(self, rest: &mut &[u8]) {
        let state = self.state.clone();
        let server_state = self.get_server_state().await;
        let cache = self.cache.clone();
        let config = self.config.clone();

        // The 'rest' variable might contain data from the handshake.
        // We need to move it into the spawned task so it can be processed first.
        let mut initial_data = rest.to_vec();

        // Spawn the background listener thread
        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];

            // This is the main loop for listening to the master.
            loop {
                // First, process any leftover data from the handshake.
                let mut all_data = initial_data.clone();
                initial_data.clear(); // Clear it so we don't process it again

                // Get a clone of the connection Arc without holding the state lock
                // across the await point
                let master_connection = {
                    let state_guard = state.lock().await;
                    state_guard.connection.clone()
                };

                // Now, read new data from the master stream. The state lock is NOT held here.
                let bytes_read = if let Some(stream_arc) = master_connection {
                    let mut stream_guard = stream_arc.lock().await;
                    match stream_guard.read(&mut buffer).await {
                        Ok(0) => {
                            eprintln!("Master disconnected.");
                            break; // Exit the loop if connection is closed.
                        }
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Error reading from master: {}", e);
                            break; // Exit on error.
                        }
                    }
                } else {
                    // This should not happen if the handshake was successful.
                    eprintln!("No connection to master found.");
                    break;
                };

                // Combine leftover data with newly read data.
                all_data.extend_from_slice(&buffer[..bytes_read]);
                let mut remaining_bytes = all_data.as_slice();

                println!(
                    "remaining_bytes before loop: {}",
                    bytes_to_ascii(remaining_bytes)
                );

                // try to parse a possible rdb file
                match RDBFile::from_bytes(remaining_bytes) {
                    Ok((rdb_file, bytes_consumed)) => {
                        remaining_bytes = &remaining_bytes[bytes_consumed..];
                        // proceed normally
                    }
                    Err(_) => {
                        // do nothing it was probably consumed elsewhere
                    }
                }

                // Process all complete commands in the buffer.
                while !remaining_bytes.is_empty() {
                    match parse(remaining_bytes) {
                        Ok((resp, leftover)) => {
                            let command_size = remaining_bytes.len() - leftover.len();

                            let command = RedisCommand::from(resp);

                            println!(
                                "remaining_bytes in loop: {}",
                                bytes_to_ascii(remaining_bytes)
                            );
                            println!("command: {:?}", command);

                            let needs_reply = matches!(command, RedisCommand::ReplConf(..));

                            let response = command
                                .execute::<TcpStream>(
                                    None,
                                    cache.clone(),
                                    config.clone(),
                                    server_state.clone(),
                                )
                                .await;

                            println!(
                                "Response after executing command is {}",
                                bytes_to_ascii(&response)
                            );

                            if needs_reply && !response.is_empty() {
                                let master_connection = {
                                    let state_guard = state.lock().await;
                                    state_guard.connection.clone()
                                }
                                .unwrap();

                                println!(
                                    "Slave responding to master: {}",
                                    bytes_to_ascii(&response)
                                );
                                let mut stream_guard = master_connection.lock().await;
                                if let Err(e) = stream_guard.write_all(&response).await {
                                    eprintln!("Failed to write response to master: {}", e);
                                    // If we can't write, the connection is likely broken.
                                    break; // Break the inner `while` loop
                                }
                            }

                            // After the command is processed, update the offset.
                            let mut state_guard = state.lock().await;
                            state_guard.master_repl_offset += command_size;

                            // Continue parsing with the rest of the buffer.
                            remaining_bytes = leftover;
                        }
                        Err(e) => {
                            // A real parsing error.
                            eprintln!("Failed to parse command from master: {:?}", e);
                            // We might have corrupt data, so we break to avoid an infinite loop.
                            break;
                        }
                    }
                }
            }
        });
    }
}

// TODO: add functions to access member variables instead of accessing them directly
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: String,
}

// Server state that commands might need
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerState {
    pub role: ServerRole,
    pub repl_id: String,
    pub repl_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Slave,
}

// Helper methods to extract server state
impl<W: AsyncWrite + Send + Unpin + 'static> MasterServer<W> {
    pub async fn get_server_state(&self) -> ServerState {
        ServerState {
            role: ServerRole::Master,
            repl_id: self.get_replid().await,
            repl_offset: self.get_repl_offset().await,
        }
    }
}

impl SlaveServer {
    pub async fn get_server_state(&self) -> ServerState {
        let state = self.state.lock().await;
        ServerState {
            role: ServerRole::Slave,
            repl_id: state.master_replid.clone(),
            repl_offset: state.master_repl_offset,
        }
    }
}

impl<W: AsyncWrite + Send + Unpin + 'static> RedisServer<W> {
    pub async fn get_server_state(&self) -> ServerState {
        match self {
            RedisServer::Master(master) => master.get_server_state().await,
            RedisServer::Slave(slave) => slave.get_server_state().await,
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
}

#[derive(Debug)]
pub struct MasterState {
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
    pub connection: Option<Arc<Mutex<TcpStream>>>,
}

#[derive(Debug)]
pub struct ReplicaConnection {
    pub port: String,
    pub connection: Arc<Mutex<TcpStream>>,
}

pub type SharedMut<T> = Arc<Mutex<T>>;
pub type Shared<T> = Arc<T>;

pub type BoxedAsyncWrite = Box<dyn AsyncWrite + Unpin + Send>;

#[derive(Debug, Clone)]
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
}

impl<W: AsyncWrite + Send + Unpin> MasterServer<W> {
    fn new() -> Self {
        let config = Arc::new(ServerConfig {
            dir: None,
            dbfilename: None,
            port: "6379".to_string(),
        });

        let state = Arc::new(Mutex::new(MasterState {
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            current_offset: 0,
        }));

        let cache = Arc::new(Mutex::new(HashMap::new()));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicationMsg<W>>(256);

        tokio::spawn(async move {
            let mut replicas: HashMap<SocketAddr, SharedMut<W>> = HashMap::new();

            while let Some(msg) = rx.recv().await {
                match msg {
                    ReplicationMsg::Broadcast(cmd) => {
                        for writer in replicas.values_mut() {
                            let mut writer_guard = writer.lock().await;
                            if let Err(e) = writer_guard.write_all(&cmd).await {
                                eprintln!("Failed to write to replica: {}", e);
                                // Optionally, handle the error by removing the replica
                                continue;
                            }
                            // Flush the buffer to ensure the command is sent immediately
                            if let Err(e) = writer_guard.flush().await {
                                eprintln!("Failed to flush to replica: {}", e);
                            }
                            println!(
                                "Replication handler wrote {} bytes command successfully",
                                cmd.len()
                            );
                        }
                    }
                    ReplicationMsg::AddReplica(addr, stream_writer) => {
                        println!("Adding new replica: {}", addr);
                        replicas.insert(addr, stream_writer);
                    }
                    ReplicationMsg::RemoveReplica(addr) => {
                        replicas.remove(&addr);
                    }
                }
            }
        });

        Self {
            config,
            replication_msg_sender: tx,
            state,
            cache,
        }
    }

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

#[derive(Debug, Clone)]
pub struct SlaveServer {
    config: Shared<ServerConfig>,
    state: SharedMut<SlaveState>,
    cache: SharedMut<Cache>,
}

fn read_rdb_from_stream<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut buffer = [0u8; 1024];

    // Read until we get the length prefix ($<length>\r\n)
    let mut length_bytes = Vec::new();

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|e| format!("Failed to read: {}", e))?;
        if bytes_read == 0 {
            return Err("Connection closed while reading RDB length".to_string());
        }

        length_bytes.extend_from_slice(&buffer[..bytes_read]);

        if length_bytes.len() >= 2 && &length_bytes[length_bytes.len() - 2..] == b"\r\n" {
            break;
        }
    }

    // Parse the length prefix ($<length>\r\n)
    let (resp, remaining) =
        parse(&length_bytes).map_err(|e| format!("Failed to parse RDB length: {:?}", e))?;
    let length = match resp {
        RespType::BulkString(_) => {
            let len_str = String::from_utf8_lossy(&length_bytes[1..length_bytes.len() - 2]);
            len_str
                .parse::<usize>()
                .map_err(|e| format!("Invalid RDB length: {}", e))?
        }
        _ => return Err("Expected bulk string for RDB length".to_string()),
    };

    // Read the exact number of bytes for the RDB file
    let mut rdb_bytes = vec![0u8; length];
    let mut total_read = remaining.len();
    rdb_bytes[..remaining.len()].copy_from_slice(remaining);

    while total_read < length {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|e| format!("Failed to read RDB: {}", e))?;
        if bytes_read == 0 {
            return Err("Connection closed while reading RDB file".to_string());
        }
        let end = (total_read + bytes_read).min(length);
        rdb_bytes[total_read..end].copy_from_slice(&buffer[..(end - total_read)]);
        total_read += bytes_read;
    }

    Ok(rdb_bytes)
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
            connection: None,
        }));

        let cache = Arc::new(Mutex::new(HashMap::new()));

        Self {
            config,
            state,
            cache,
        }
    }

    pub async fn increment_repl_offset(&mut self, amount: usize) {
        self.state.lock().await.master_repl_offset += amount;
    }
}

#[derive(Debug, Clone)]
pub enum RedisServer<W: AsyncWrite + Send + Unpin + 'static> {
    Master(MasterServer<W>),
    Slave(SlaveServer),
}

impl<W: AsyncWrite + Send + Unpin + 'static> RedisServer<W> {
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

                    // TODO: Find a better name for this variable info
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
}
