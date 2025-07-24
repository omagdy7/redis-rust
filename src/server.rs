use crate::rdb::{FromBytes, RDBFile};
use crate::resp_commands::RedisCommand;
use crate::resp_parser::{parse, RespType};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::{env, thread};

use crate::shared_cache::Cache;

// TODO: add functions to access member variables instead of accessing them directly

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: String,
}

// Server state that commands might need
#[derive(Debug, Clone)]
pub struct ServerState {
    pub role: ServerRole,
    pub repl_id: String,
    pub repl_offset: usize,
}

#[derive(Debug, Clone)]
pub enum ServerRole {
    Master,
    Slave,
}

// Trait for broadcasting - only masters can do this
pub trait CanBroadcast: Send {
    fn broadcast_command_to_replicas(&mut self, command: &[u8]);
}

// Implementation for Master
impl CanBroadcast for MasterServer {
    fn broadcast_command_to_replicas(&mut self, command: &[u8]) {
        self.broadcast_command(command);
    }
}

// Helper methods to extract server state
impl MasterServer {
    pub fn get_server_state(&self) -> ServerState {
        ServerState {
            role: ServerRole::Master,
            repl_id: self.get_replid(),
            repl_offset: self.get_repl_offset(),
        }
    }
}

impl SlaveServer {
    pub fn get_server_state(&self) -> ServerState {
        let state = self.state.lock().unwrap();
        ServerState {
            role: ServerRole::Slave,
            repl_id: state.master_replid.clone(),
            repl_offset: state.master_repl_offset,
        }
    }
}

impl RedisServer {
    pub fn get_server_state(&self) -> ServerState {
        match self {
            RedisServer::Master(master) => master.get_server_state(),
            RedisServer::Slave(slave) => slave.get_server_state(),
        }
    }

    pub fn as_broadcaster(&mut self) -> Option<SharedMut<&mut dyn CanBroadcast>> {
        match self {
            RedisServer::Master(master) => {
                Some(Arc::new(Mutex::new(master as &mut dyn CanBroadcast)))
            }
            RedisServer::Slave(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct MasterState {
    pub replid: String,
    pub current_offset: usize,
    pub replicas: Vec<ReplicaConnection>,
}

// Slave-specific state
#[derive(Debug)]
pub struct SlaveState {
    pub master_replid: String,
    pub master_repl_offset: usize,
    pub master_host: String,
    pub master_port: String,
    pub connection: Option<TcpStream>,
}

#[derive(Debug)]
pub struct ReplicaConnection {
    pub port: String,
    pub connection: Arc<Mutex<TcpStream>>,
}

pub type SharedMut<T> = Arc<Mutex<T>>;
pub type Shared<T> = Arc<T>;

#[derive(Debug, Clone)]
pub struct MasterServer {
    config: Shared<ServerConfig>,
    state: SharedMut<MasterState>,
    cache: SharedMut<Cache>,
}

impl MasterServer {
    fn new() -> Self {
        let config = Arc::new(ServerConfig {
            dir: None,
            dbfilename: None,
            port: "6379".to_string(),
        });

        let state = Arc::new(Mutex::new(MasterState {
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            current_offset: 0,
            replicas: vec![],
        }));

        let cache = Arc::new(Mutex::new(HashMap::new()));

        Self {
            config,
            state,
            cache,
        }
    }

    fn port(&self) -> &str {
        &self.config.port
    }

    pub fn broadcast_command(&mut self, command: &[u8]) {
        let mut state = self.state.lock().unwrap();

        state.replicas.retain(|replica| {
            let mut conn = replica.connection.lock().unwrap();
            if let Err(e) = conn.write_all(command) {
                eprintln!("Failed to send to replica {}: {}", replica.port, e);
                false // Drop dead connections
            } else {
                true
            }
        })
    }

    pub fn add_replica(&self, replica_addr: SocketAddr, connection: Arc<Mutex<TcpStream>>) {
        let replica = ReplicaConnection {
            port: replica_addr.port().to_string(),
            connection,
        };

        self.state.lock().unwrap().replicas.push(replica);
    }

    pub fn get_repl_offset(&self) -> usize {
        self.state.lock().unwrap().current_offset
    }

    pub fn increment_repl_offset(&self, amount: usize) {
        self.state.lock().unwrap().current_offset += amount;
    }

    pub fn get_replid(&self) -> String {
        self.state.lock().unwrap().replid.clone()
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

    pub fn increment_repl_offset(&mut self, amount: usize) {
        self.state.lock().unwrap().master_repl_offset += amount;
    }

    fn connect(&self) -> Result<TcpStream, std::io::Error> {
        let state = self.state.lock().unwrap();
        let master_address = format!("{}:{}", state.master_host, state.master_port);
        TcpStream::connect(master_address)
    }

    fn handshake(&mut self) -> Result<(), String> {
        match self.connect() {
            Ok(mut stream) => {
                let mut buffer = [0; 1024];

                let mut send_command = |command: &[u8], read: bool| -> Result<(), String> {
                    stream
                        .write_all(command)
                        .map_err(|e| format!("Failed to send: {}", e))?;

                    if read {
                        match stream.read(&mut buffer) {
                            Ok(0) | Err(_) => return Ok(()), // connection closed or error
                            Ok(_) => {
                                println!("Recieved some bytes here!");
                                Ok(())
                            }
                        }
                    } else {
                        Ok(())
                    }
                };

                // Step1: PING
                send_command(&resp_bytes!(array => [resp!(bulk "PING")]), true)?;

                let port = self.config.port.clone();
                // Step2: REPLCONF listening-port <PORT>
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "REPLCONF"),
                        resp!(bulk "listening-port"),
                        resp!(bulk port)
                    ]),
                    true,
                )?;

                // Step3: REPLCONF capa psync2
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "REPLCONF"),
                        resp!(bulk "capa"),
                        resp!(bulk "psync2")
                    ]),
                    true,
                )?;

                // Step 4: PSYNC <REPL_ID> <REPL_OFFSSET>
                send_command(
                    &resp_bytes!(array => [
                        resp!(bulk "PSYNC"),
                        resp!(bulk "?"),
                        resp!(bulk "-1")
                    ]),
                    false,
                )?;

                // Step 5: Read FULLRESYNC response
                let bytes_read = stream
                    .read(&mut buffer)
                    .map_err(|e| format!("Failed to read FULLRESYNC: {}", e))?;
                let (parsed, mut rest) = parse(&buffer[..bytes_read])
                    .map_err(|e| format!("Failed to parse FULLRESYNC: {:?}", e))?;
                match parsed {
                    RespType::SimpleString(s) if s.starts_with("FULLRESYNC") => {
                        // Expected response
                    }
                    _ => return Err("Invalid FULLRESYNC response".to_string()),
                }

                println!("rest: {:?}", rest);

                println!("FULLRESYNC response bytes read: {}", bytes_read);

                // So there is an interesting behaviour where the FULLRESYNC + RDB and if you are
                // really lucky the REPLCONF would all get sent in one TCP segment so I should
                // assume I would get nice segments refelecting each command
                if !rest.is_empty() {
                    // TODO: Sync the rdb_file with the slave's cache
                    // TODO: Find a way to propagate the error up the stack by using anyhow or something
                    let (rdb_file, bytes_consumed) = RDBFile::from_bytes(rest).unwrap();
                    rest = &rest[bytes_consumed..];
                    println!("rdb bytes: {}", bytes_consumed);
                    println!("remaining btyes after rdb: {}", rest.len());
                }

                // Store the persistent connection
                self.state.lock().unwrap().connection = Some(stream);
                self.start_replication_listener(&mut rest);

                Ok(())
            }
            Err(e) => Err(format!("Master node doesn't exist: {}", e)),
        }
    }

    // TODO: This should return a Result
    fn start_replication_listener<'a>(&'a self, rest: &mut &[u8]) {
        let state = self.state.clone();
        let cache = self.cache.clone();
        let config = self.config.clone();
        let server_state = self.get_server_state();
        let broadcaster = None::<Arc<Mutex<&mut dyn CanBroadcast>>>;

        // if it's not empty then there is probably a REPLCONF command sent and I should handle it
        // before reading anymore bytes
        if !rest.is_empty() {
            // TODO: Sync the rdb_file with the slave's cache
            // TODO: Find a way to propagate the error up the stack by using anyhow or something
            if rest[0] == '$' as u8 {
                // this means that the rdb segment got in here some how so I have to deal with it here
                let (rdb_file, bytes_consumed) = RDBFile::from_bytes(rest).unwrap();
                *rest = &rest[bytes_consumed..];

                println!("rdb bytes: {}", bytes_consumed);
                println!("remaining btyes after rdb: {}", rest.len());
                if rest.len() > 0 {
                    match parse(rest) {
                        Ok((resp, leftover)) => {
                            dbg!(&resp, leftover);

                            // Update replication offset
                            let command_size = resp.to_resp_bytes().len();
                            let mut state_guard = state.lock().unwrap();

                            let command = RedisCommand::from(resp);

                            let response = command.execute(
                                cache.clone(),
                                config.clone(),
                                server_state.clone(),
                                broadcaster.clone(),
                            );

                            if let Some(ref mut stream) = state_guard.connection {
                                let _ = stream.write_all(&response);
                                let _ = stream.flush();
                            }

                            state_guard.master_repl_offset += command_size;
                        }
                        Err(_) => {}
                    }
                }
            } else {
                match parse(rest) {
                    Ok((resp, leftover)) => {
                        dbg!(&resp, leftover);

                        // Update replication offset
                        let command_size = resp.to_resp_bytes().len();
                        let mut state_guard = state.lock().unwrap();

                        let command = RedisCommand::from(resp);

                        let response = command.execute(
                            cache.clone(),
                            config.clone(),
                            server_state.clone(),
                            broadcaster.clone(),
                        );

                        if let Some(ref mut stream) = state_guard.connection {
                            let _ = stream.write_all(&response);
                            let _ = stream.flush();
                        }

                        state_guard.master_repl_offset += command_size;
                    }
                    Err(_) => {}
                }
            }
        }

        // Spawn the background listener thread
        thread::spawn(move || {
            let mut buffer = [0u8; 1024];
            loop {
                let bytes_read = {
                    let mut state_guard = state.lock().unwrap();
                    if let Some(ref mut stream) = state_guard.connection {
                        match stream.read(&mut buffer) {
                            Ok(0) => {
                                println!("Master disconnected");
                                break;
                            }
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("Error reading from master: {}", e);
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                };

                println!("After handshake: {}", bytes_read);

                // Parse and execute all commands in the buffer
                let mut remaining_bytes = &buffer[..bytes_read];

                println!("remaining_bytes: {:?}", &remaining_bytes);

                // TODO: Sync the rdb_file with the slave's cache
                // TODO: Find a way to propagate the error up the stack by using anyhow or something
                if remaining_bytes.len() > 0 && remaining_bytes[0] == '$' as u8 {
                    // this means that the rdb segment got in here some how so I have to deal with it here
                    let (rdb_file, bytes_consumed) = RDBFile::from_bytes(remaining_bytes).unwrap();
                    println!("rdb bytes: {}", bytes_consumed);
                    remaining_bytes = &remaining_bytes[bytes_consumed..];
                    println!(
                        "remaining btyes length after rdb: {}",
                        remaining_bytes.len()
                    );
                    println!("remaining btyes after rdb: {:?}", remaining_bytes);
                }

                while !remaining_bytes.is_empty() {
                    match parse(remaining_bytes) {
                        Ok((resp, leftover)) => {
                            dbg!(&resp, leftover);

                            // Update replication offset
                            let command_size = resp.to_resp_bytes().len();
                            let mut state_guard = state.lock().unwrap();

                            let command = RedisCommand::from(resp);

                            let response = command.execute(
                                cache.clone(),
                                config.clone(),
                                server_state.clone(),
                                broadcaster.clone(),
                            );

                            if let Some(ref mut stream) = state_guard.connection {
                                let _ = stream.write_all(&response);
                                let _ = stream.flush();
                            }

                            state_guard.master_repl_offset += command_size;

                            remaining_bytes = leftover
                        }
                        Err(_) => {
                            // If parsing fails, break out of the loop
                            break;
                        }
                    }
                }
            }
        });
    }
}

#[derive(Debug, Clone)]
pub enum RedisServer {
    Master(MasterServer),
    Slave(SlaveServer),
}

impl RedisServer {
    pub fn master() -> Self {
        RedisServer::Master(MasterServer::new())
    }

    pub fn slave(port: String, master_host: String, master_port: String) -> Self {
        RedisServer::Slave(SlaveServer::new(port, master_host, master_port))
    }

    // Helper methods to access common fields regardless of variant
    pub fn port(&self) -> &str {
        match self {
            RedisServer::Master(master) => &master.config.port,
            RedisServer::Slave(slave) => &slave.config.port,
        }
    }

    pub fn config(&self) -> Arc<ServerConfig> {
        match self {
            RedisServer::Master(master) => master.config.clone(),
            RedisServer::Slave(slave) => slave.config.clone(),
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

    pub fn cache(&self) -> &SharedMut<Cache> {
        match self {
            RedisServer::Master(master) => &master.cache,
            RedisServer::Slave(slave) => &slave.cache,
        }
    }

    pub fn repl_offset(&self) -> usize {
        match self {
            RedisServer::Master(master) => master.state.lock().unwrap().current_offset,
            RedisServer::Slave(slave) => slave.state.lock().unwrap().master_repl_offset,
        }
    }

    pub fn set_cache(&mut self, cache: &SharedMut<Cache>) {
        match self {
            RedisServer::Master(master) => master.cache = cache.clone(),
            RedisServer::Slave(slave) => slave.cache = cache.clone(),
        }
    }

    pub fn set_repl_offset(&mut self, repl_offset: usize) {
        match self {
            RedisServer::Master(master) => {
                master.state.lock().unwrap().current_offset = repl_offset;
            }
            RedisServer::Slave(slave) => {
                slave.state.lock().unwrap().master_repl_offset = repl_offset
            }
        }
    }

    pub fn repl_offset_increment(&mut self, amount: usize) {
        match self {
            RedisServer::Master(master) => master.state.lock().unwrap().current_offset += amount,
            RedisServer::Slave(slave) => slave.state.lock().unwrap().master_repl_offset += amount,
        }
    }

    pub fn role(&self) -> &str {
        match self {
            RedisServer::Master(_) => "master",
            RedisServer::Slave(_) => "slave",
        }
    }

    pub fn add_replica(&mut self, replica_adr: SocketAddr, connection: Arc<Mutex<TcpStream>>) {
        match self {
            // TODO: Should probably add host to MasterServer and SlaveServer as member field
            RedisServer::Master(master) => {
                master.add_replica(replica_adr, connection);
            }
            RedisServer::Slave(_) => {
                unreachable!("Slaves don't have replicas")
            }
        }
    }

    pub fn broadcast_command(&mut self, command: &[u8]) {
        if let RedisServer::Master(master) = self {
            master.broadcast_command(command);
        }
    }

    pub fn is_master(&self) -> bool {
        matches!(self, RedisServer::Master(_))
    }

    pub fn is_slave(&self) -> bool {
        matches!(self, RedisServer::Slave(_))
    }
}

impl RedisServer {
    pub fn new() -> Result<Option<RedisServer>, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() == 1 {
            return Ok(None);
        }

        let mut redis_server = RedisServer::master();
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
                    redis_server = RedisServer::slave(
                        current_port,
                        master_host.to_string(),
                        master_port.to_string(),
                    );

                    // Perform handshake
                    if let RedisServer::Slave(mut slave) = redis_server.clone() {
                        slave.handshake()?;
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
