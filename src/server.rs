use std::collections::HashMap;
use std::net::SocketAddr;
use std::{env, sync::Arc};

use crate::commands::RedisCommand;
use crate::error::RespError;
use crate::master::MasterServer;
use crate::shared_cache::Cache;
use crate::slave::{SlaveRole, SlaveServer};
use crate::transaction::Transaction;
use crate::types::*;

#[derive(Debug, Clone)]
pub enum RedisServer {
    Master(MasterServer),
    Slave(SlaveServer),
}

impl RedisServer {
    pub async fn new() -> Result<Option<RedisServer>, String> {
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

    pub async fn execute(
        &self,
        command: RedisCommand,
        connection_socket: SocketAddr,
    ) -> Result<Vec<u8>, RespError> {
        match self {
            RedisServer::Master(master) => {
                <MasterServer as CommandHandler<BoxedAsyncWrite>>::execute(
                    master,
                    command,
                    connection_socket,
                )
                .await
            }
            RedisServer::Slave(slave) => {
                <SlaveServer as CommandHandler<tokio::net::TcpStream>>::execute(
                    slave,
                    command,
                    connection_socket,
                )
                .await
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
    ) -> Option<tokio::sync::mpsc::Sender<ReplicationMsg>> {
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

    pub fn transaction(&self) -> Option<&SharedMut<HashMap<SocketAddr, Transaction>>> {
        match self {
            Self::Master(m) => Some(&m.transactions),
            Self::Slave(_) => None,
        }
    }

    pub fn set_connection_socket(&mut self, socket: SocketAddr) {
        if let Self::Master(m) = self {
            m.connection_socket = socket;
        }
    }

    pub fn cache(&self) -> &SharedMut<Cache> {
        match self {
            Self::Master(m) => m.cache(),
            Self::Slave(s) => s.cache(),
        }
    }

    pub fn new_master() -> Self {
        RedisServer::Master(MasterServer::new())
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
