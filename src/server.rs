use crate::resp_commands::RedisCommands;
use crate::resp_parser::{parse, RespType};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::{env, thread};

use crate::shared_cache::SharedCache;

#[derive(Debug, Clone)]
pub struct MasterServer {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub replid: Option<String>,
    pub current_offset: Option<String>,
    pub port: String,
    pub cache: SharedCache,
    replicas: Vec<SlaveServer>,
}

impl MasterServer {
    fn new() -> Self {
        Self {
            dir: None,
            dbfilename: None,
            port: "6379".to_string(),
            replid: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
            current_offset: Some("0".to_string()),
            cache: Arc::new(Mutex::new(HashMap::new())),
            replicas: vec![],
        }
    }

    fn port(&self) -> &str {
        &self.port
    }

    pub fn broadcast_command(&mut self, command: &[u8]) {
        println!("Hello from brodcast");
        self.replicas.retain(|replica| {
            if let Some(conn) = &replica.connection {
                let mut conn = conn.lock().unwrap();
                if let Err(e) = conn.write_all(command) {
                    eprintln!("Failed to send to replica {}: {}", replica.port, e);
                    false // Drop dead connections
                } else {
                    true
                }
            } else {
                false
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct SlaveServer {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: String,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<String>,
    pub master_host: String,
    pub master_port: String,
    pub connection: Option<Arc<Mutex<TcpStream>>>,
    pub cache: SharedCache,
}

impl SlaveServer {
    fn new(
        port: String,
        master_host: String,
        master_port: String,
        connection: Option<Arc<Mutex<TcpStream>>>,
    ) -> Self {
        Self {
            dir: None,
            dbfilename: None,
            port,
            master_replid: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
            master_repl_offset: Some("0".to_string()),
            master_host,
            master_port,
            connection,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn connect(&self) -> Result<TcpStream, std::io::Error> {
        let master_address = format!("{}:{}", self.master_host, self.master_port);
        return TcpStream::connect(master_address);
    }

    fn handshake(&mut self) -> Result<(), String> {
        match self.connect() {
            Ok(mut stream) => {
                let mut buffer = [0; 512];

                let mut send_command = |command: &[u8]| -> Result<(), String> {
                    stream
                        .write_all(command)
                        .map_err(|e| format!("Failed to send: {}", e))?;

                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => return Ok(()), // connection closed or error
                        Ok(_) => {
                            println!("Recieved some bytes here!");
                            Ok(())
                        }
                    }
                };

                // PING
                send_command(&resp_bytes!(array => [resp!(bulk "PING")]))?;

                // REPLCONF listening-port <PORT>
                send_command(&resp_bytes!(array => [
                    resp!(bulk "REPLCONF"),
                    resp!(bulk "listening-port"),
                    resp!(bulk self.port.clone())
                ]))?;

                // REPLCONF capa psync2
                send_command(&resp_bytes!(array => [
                    resp!(bulk "REPLCONF"),
                    resp!(bulk "capa"),
                    resp!(bulk "psync2")
                ]))?;

                // PSYNC <REPL_ID> <REPL_OFFSSET>
                send_command(&resp_bytes!(array => [
                    resp!(bulk "PSYNC"),
                    resp!(bulk "?"),
                    resp!(bulk "-1")
                ]))?;

                // Store the persistent connection
                let shared_stream = Arc::new(Mutex::new(stream));
                self.connection = Some(shared_stream.clone());

                // Spawn the background listener thread
                let cache_clone = self.cache.clone();
                thread::spawn(move || {
                    let mut buffer = [0u8; 1024];
                    loop {
                        let bytes_read = {
                            let mut stream = shared_stream.lock().unwrap();
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
                        }; // stream lock is dropped here
                           // Parse and execute all commands in the buffer
                        let mut remaining_bytes = &buffer[..bytes_read];

                        while !remaining_bytes.is_empty() {
                            match parse(remaining_bytes) {
                                Ok((parsed_command, leftover)) => {
                                    // Create a temporary slave server for command execution
                                    let temp_slave = RedisServer::Slave(SlaveServer::new(
                                        "0".to_string(),         // dummy port
                                        "localhost".to_string(), // dummy host
                                        "6379".to_string(),      // dummy master port
                                        None, // no connection needed for execution
                                    ));

                                    // Set the cache to our actual cache
                                    let mut temp_slave = temp_slave;
                                    temp_slave.set_cache(&cache_clone);
                                    let server_arc = Arc::new(Mutex::new(temp_slave));

                                    let _ = RedisCommands::from(parsed_command.clone())
                                        .execute(server_arc);

                                    // Update remaining bytes for next iteration
                                    remaining_bytes = leftover;
                                }
                                Err(_) => {
                                    // If parsing fails, break out of the loop
                                    break;
                                }
                            }
                        }
                    }
                });
                Ok(())
            }
            Err(e) => Err(format!("Master node doesn't exist: {}", e)),
        }
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
        RedisServer::Slave(SlaveServer::new(port, master_host, master_port, None))
    }

    // Helper methods to access common fields regardless of variant
    pub fn port(&self) -> &str {
        match self {
            RedisServer::Master(master) => &master.port,
            RedisServer::Slave(slave) => &slave.port,
        }
    }

    pub fn set_port(&mut self, port: String) {
        match self {
            RedisServer::Master(master) => master.port = port,
            RedisServer::Slave(slave) => slave.port = port,
        }
    }

    pub fn dir(&self) -> &Option<String> {
        match self {
            RedisServer::Master(master) => &master.dir,
            RedisServer::Slave(slave) => &slave.dir,
        }
    }

    pub fn set_dir(&mut self, dir: Option<String>) {
        match self {
            RedisServer::Master(master) => master.dir = dir,
            RedisServer::Slave(slave) => slave.dir = dir,
        }
    }

    pub fn dbfilename(&self) -> &Option<String> {
        match self {
            RedisServer::Master(master) => &master.dbfilename,
            RedisServer::Slave(slave) => &slave.dbfilename,
        }
    }

    pub fn set_dbfilename(&mut self, dbfilename: Option<String>) {
        match self {
            RedisServer::Master(master) => master.dbfilename = dbfilename,
            RedisServer::Slave(slave) => slave.dbfilename = dbfilename,
        }
    }

    pub fn cache(&self) -> &SharedCache {
        match self {
            RedisServer::Master(master) => &master.cache,
            RedisServer::Slave(slave) => &slave.cache,
        }
    }

    pub fn set_cache(&mut self, cache: &SharedCache) {
        match self {
            RedisServer::Master(master) => master.cache = cache.clone(),
            RedisServer::Slave(slave) => slave.cache = cache.clone(),
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
            RedisServer::Master(master) => master.replicas.push(SlaveServer::new(
                replica_adr.port().to_string(),
                "localhost".to_owned(),
                master.port().to_owned(),
                Some(connection),
            )),
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
