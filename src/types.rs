use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::{future::Future, net::SocketAddr, sync::Arc};

use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};

use crate::commands::RedisCommand;
use crate::error::RespError;
use crate::stream::{StreamId, XaddStreamId};

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

pub enum ReplicationMsg {
    Broadcast(Vec<u8>),
    AddReplica(SocketAddr, SharedMut<BoxedAsyncWrite>),
    RemoveReplica(SocketAddr),
}

pub enum PubSubMsg {
    Publish { channel: String, message: String, sender: tokio::sync::oneshot::Sender<usize> },
    AddSubscriber(SocketAddr, String),
    AddWriter(SocketAddr, SharedMut<BoxedAsyncWrite>),
    RemoveSubscriber(SocketAddr, String),
    RemoveWriter(SocketAddr),
}

/// Represents the mode of the current_client
#[derive(Debug, Clone, Copy)]
pub enum ClientMode {
    Normal,
    Transaction,
    Subscribe,
}

/// Types of notifiers used in the system
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NotifierType {
    Ack,                             // For replication acknowledgments (WAIT command)
    XAdd,                            // For stream XADD operations (XREAD blocking)
    List(String),                    // For list operations (BLPOP blocking) - key-specific
    SubscribtionMsgRecieved(String), // For list operations (BLPOP blocking) - key-specific
}

/// Centralized manager for all notifiers in the system
#[derive(Debug, Clone)]
pub struct NotificationManager {
    notifiers: SharedMut<HashMap<NotifierType, Arc<Notify>>>,
}

impl NotificationManager {
    pub fn new() -> Self {
        let mut notifiers = HashMap::new();
        notifiers.insert(NotifierType::Ack, Arc::new(Notify::new()));
        notifiers.insert(NotifierType::XAdd, Arc::new(Notify::new()));
        // Note: List notifiers are created on-demand for specific keys

        Self {
            notifiers: Arc::new(Mutex::new(notifiers)),
        }
    }

    /// Get a notifier by type
    pub async fn get_notifier(&self, notifier_type: NotifierType) -> Arc<Notify> {
        let notifiers = self.notifiers.lock().await;
        notifiers.get(&notifier_type).unwrap().clone()
    }

    /// Notify all waiters for a specific notifier type
    pub async fn notify(&self, notifier_type: NotifierType) {
        let notifier = self.get_notifier(notifier_type).await;
        notifier.notify_waiters();
    }

    /// Wait for notification on a specific notifier type
    pub async fn wait_for(&self, notifier_type: NotifierType) {
        let notifier = self.get_notifier(notifier_type).await;
        notifier.notified().await;
    }

    /// Add a new notifier type dynamically
    pub async fn add_notifier(&self, notifier_type: NotifierType) {
        let mut notifiers = self.notifiers.lock().await;
        notifiers
            .entry(notifier_type)
            .or_insert_with(|| Arc::new(Notify::new()));
    }

    /// Remove a notifier type
    pub async fn remove_notifier(&self, notifier_type: NotifierType) {
        let mut notifiers = self.notifiers.lock().await;
        notifiers.remove(&notifier_type);
    }

    /// Check if a notifier type exists
    pub async fn has_notifier(&self, notifier_type: NotifierType) -> bool {
        let notifiers = self.notifiers.lock().await;
        notifiers.contains_key(&notifier_type)
    }

    /// Get or create a list notifier for a specific key
    pub async fn get_list_notifier(&self, key: &str) -> Arc<Notify> {
        let notifier_type = NotifierType::List(key.to_string());
        let mut notifiers = self.notifiers.lock().await;

        notifiers
            .entry(notifier_type)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Notify all waiters for a specific list key
    pub async fn notify_list(&self, key: &str) {
        let notifier = self.get_list_notifier(key).await;
        notifier.notify_waiters();
    }
}

impl Default for NotificationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a client waiting for a BLPOP operation
pub struct BlockedClient {
    pub socket_addr: SocketAddr,
    pub writer: SharedMut<BoxedAsyncWrite>,
    pub notifier: Arc<Notify>,
}

impl std::fmt::Debug for BlockedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockedClient")
            .field("socket_addr", &self.socket_addr)
            .field("writer", &"<BoxedAsyncWrite>")
            .field("notifier", &self.notifier)
            .finish()
    }
}

impl BlockedClient {
    pub fn new(socket_addr: SocketAddr, writer: SharedMut<BoxedAsyncWrite>) -> Self {
        Self {
            socket_addr,
            writer,
            notifier: Arc::new(Notify::new()),
        }
    }
}

/// Blocking queue for managing clients waiting for BLPOP operations
#[derive(Debug)]
pub struct BlockingQueue {
    /// Map of key -> queue of waiting clients (FIFO order)
    pub queues: std::collections::HashMap<String, VecDeque<BlockedClient>>,
}

impl BlockingQueue {
    pub fn new() -> Self {
        Self {
            queues: std::collections::HashMap::new(),
        }
    }

    /// Add a client to the waiting queue for a specific key
    pub fn enqueue(&mut self, key: String, client: BlockedClient) {
        self.queues
            .entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(client);
    }

    /// Remove and return the first client waiting for a specific key
    pub fn dequeue(&mut self, key: &str) -> Option<BlockedClient> {
        self.queues.get_mut(key).and_then(|queue| queue.pop_front())
    }

    /// Check if there are any clients waiting for a specific key
    pub fn has_waiting_clients(&self, key: &str) -> bool {
        self.queues
            .get(key)
            .map_or(false, |queue| !queue.is_empty())
    }

    /// Get the number of clients waiting for a specific key
    pub fn waiting_count(&self, key: &str) -> usize {
        self.queues.get(key).map_or(0, |queue| queue.len())
    }

    /// Remove a specific client from all queues (useful for cleanup when client disconnects)
    pub fn remove_client(&mut self, socket_addr: SocketAddr) {
        for queue in self.queues.values_mut() {
            queue.retain(|client| client.socket_addr != socket_addr);
        }
        // Clean up empty queues
        self.queues.retain(|_, queue| !queue.is_empty());
    }
}

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

pub trait CommandHandler<W: AsyncWrite + Send + Unpin + 'static> {
    fn execute(
        &self,
        command: RedisCommand,
        connection_socket: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RespError>> + Send + '_>>;
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

// helper function to resolve stream id
pub fn resolve_stream_id(
    parsed_id: XaddStreamId,
    last_id: Option<StreamId>,
) -> Result<StreamId, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    match parsed_id {
        XaddStreamId::Literal(id) => Ok(id),
        XaddStreamId::AutoSequence { ms_time } => {
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
        XaddStreamId::Auto => {
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
