/// Custom error type for RDB parsing operations
#[derive(Debug, thiserror::Error)]
pub enum RdbError {
    #[error("Invalid magic number in RDB file")]
    InvalidMagicNumber,

    #[error("Invalid version in RDB file: {version}")]
    InvalidVersion { version: String },

    #[error("Unexpected end of file while parsing RDB")]
    UnexpectedEof,

    #[error("Invalid metadata in RDB file")]
    InvalidMetadata,

    #[error("Invalid length encoding: {length}")]
    InvalidLength { length: usize },

    #[error("Array conversion failed: {message}")]
    ArrayConversionError { message: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),

    #[error("Value type conversion failed: {value}")]
    ValueTypeConversion { value: u8 },
}

// Alias for backward compatibility
pub type ParseError = RdbError;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseStreamIdError {
    #[error("Missing part in stream ID")]
    MissingPart,
    #[error("Invalid number in stream ID")]
    InvalidNumber,
    #[error("Invalid auto sequence in stream ID")]
    InvalidAuto,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RespError {
    // Protocol errors
    #[error("ERR Protocol error")]
    InvalidProtocol,
    #[error("ERR Invalid data type")]
    InvalidDataType,
    #[error("ERR Invalid length")]
    InvalidLength,
    #[error("ERR Unexpected end of input")]
    UnexpectedEnd,
    #[error("ERR Invalid integer")]
    InvalidInteger,
    #[error("ERR Invalid bulk string")]
    InvalidBulkString,
    #[error("ERR Invalid array")]
    InvalidArray,
    #[error("ERR Malformed command")]
    MalformedCommand,

    // Command errors
    #[error("ERR unknown command")]
    UnknownCommand,
    #[error("ERR wrong number of arguments")]
    WrongNumberOfArguments,
    #[error("ERR syntax error")]
    InvalidCommandSyntax,

    // Data type errors
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR invalid key")]
    InvalidKey,
    #[error("ERR invalid value")]
    InvalidValue,
    #[error("ERR invalid index")]
    InvalidIndex,
    #[error("ERR index out of range")]
    IndexOutOfRange,

    // Memory and resource errors
    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    OutOfMemory,
    #[error("ERR max number of clients reached")]
    MaxClientsReached,
    #[error("ERR max number of databases reached")]
    MaxDatabasesReached,

    // Authentication and authorization
    #[error("NOAUTH Authentication required")]
    NoAuth,
    #[error("ERR invalid password")]
    InvalidPassword,
    #[error("NOPERM this user has no permissions to run this command")]
    NoPermission,

    // Database errors
    #[error("ERR invalid database")]
    InvalidDatabase,
    #[error("ERR database not found")]
    DatabaseNotFound,
    #[error("ERR key not found")]
    KeyNotFound,
    #[error("ERR key already exists")]
    KeyExists,

    // Transaction errors
    #[error("ERR MULTI calls can not be nested")]
    MultiNotAllowed,
    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("ERR DISCARD without MULTI")]
    DiscardWithoutMulti,
    #[error("ERR WATCH inside MULTI is not allowed")]
    WatchInMulti,

    // Replication errors
    #[error("ERR master is down")]
    MasterDown,
    #[error("ERR slave not connected")]
    SlaveNotConnected,
    #[error("ERR replication error")]
    ReplicationError,

    // Scripting errors
    #[error("ERR script error")]
    ScriptError,
    #[error("ERR script killed")]
    ScriptKilled,
    #[error("ERR script timeout")]
    ScriptTimeout,
    #[error("NOSCRIPT No matching script")]
    NoScript,

    // Pub/Sub errors
    #[error("ERR invalid channel")]
    InvalidChannel,
    #[error("ERR not subscribed")]
    NotSubscribed,

    // Persistence errors
    #[error("ERR Background save already in progress")]
    BackgroundSaveInProgress,
    #[error("ERR Background save error")]
    BackgroundSaveError,

    // Stream errors
    #[error("ERR Invalid stream ID format")]
    InvalidStreamId,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    StreamIdTooSmall,
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    StreamIdNotGreater,
    #[error("ERR Invalid stream entry ID")]
    InvalidStreamEntryId,
    #[error("ERR Stream not found")]
    StreamNotFound,
    #[error("ERR Invalid stream operation")]
    InvalidStreamOperation,

    // Generic errors
    #[error("ERR internal error")]
    InternalError,
    #[error("ERR timeout")]
    Timeout,
    #[error("ERR connection lost")]
    ConnectionLost,
    #[error("ERR invalid argument")]
    InvalidArgument,
    #[error("ERR operation not supported")]
    OperationNotSupported,
    #[error("READONLY You can't write against a read only replica")]
    Readonly,
    #[error("LOADING Redis is loading the dataset in memory")]
    Loading,
    #[error("BUSY Redis is busy running a script")]
    Busy,

    // Custom error with message
    #[error("ERR {0}")]
    Custom(String),
}

impl RespError {
    /// Convert RespError to RESP protocol bytes for network transmission
    pub fn to_resp(&self) -> Vec<u8> {
        use RespError::*;
        match self {
            // Protocol errors
            InvalidProtocol => b"-ERR Protocol error\r\n".to_vec(),
            InvalidDataType => b"-ERR Invalid data type\r\n".to_vec(),
            InvalidLength => b"-ERR Invalid length\r\n".to_vec(),
            UnexpectedEnd => b"-ERR Unexpected end of input\r\n".to_vec(),
            InvalidInteger => b"-ERR Invalid integer\r\n".to_vec(),
            InvalidBulkString => b"-ERR Invalid bulk string\r\n".to_vec(),
            InvalidArray => b"-ERR Invalid array\r\n".to_vec(),
            MalformedCommand => b"-ERR Malformed command\r\n".to_vec(),

            // Command errors
            UnknownCommand => b"-ERR unknown command\r\n".to_vec(),
            WrongNumberOfArguments => b"-ERR wrong number of arguments\r\n".to_vec(),
            InvalidCommandSyntax => b"-ERR syntax error\r\n".to_vec(),

            // Data type errors
            WrongType => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
            InvalidKey => b"-ERR invalid key\r\n".to_vec(),
            InvalidValue => b"-ERR invalid value\r\n".to_vec(),
            InvalidIndex => b"-ERR invalid index\r\n".to_vec(),
            IndexOutOfRange => b"-ERR index out of range\r\n".to_vec(),

            // Memory and resource errors
            OutOfMemory => b"-OOM command not allowed when used memory > 'maxmemory'\r\n".to_vec(),
            MaxClientsReached => b"-ERR max number of clients reached\r\n".to_vec(),
            MaxDatabasesReached => b"-ERR max number of databases reached\r\n".to_vec(),

            // Authentication and authorization
            NoAuth => b"-NOAUTH Authentication required\r\n".to_vec(),
            InvalidPassword => b"-ERR invalid password\r\n".to_vec(),
            NoPermission => b"-NOPERM this user has no permissions to run this command\r\n".to_vec(),

            // Database errors
            InvalidDatabase => b"-ERR invalid database\r\n".to_vec(),
            DatabaseNotFound => b"-ERR database not found\r\n".to_vec(),
            KeyNotFound => b"-ERR key not found\r\n".to_vec(),
            KeyExists => b"-ERR key already exists\r\n".to_vec(),

            // Transaction errors
            MultiNotAllowed => b"-ERR MULTI calls can not be nested\r\n".to_vec(),
            ExecWithoutMulti => b"-ERR EXEC without MULTI\r\n".to_vec(),
            DiscardWithoutMulti => b"-ERR DISCARD without MULTI\r\n".to_vec(),
            WatchInMulti => b"-ERR WATCH inside MULTI is not allowed\r\n".to_vec(),

            // Replication errors
            MasterDown => b"-ERR master is down\r\n".to_vec(),
            SlaveNotConnected => b"-ERR slave not connected\r\n".to_vec(),
            ReplicationError => b"-ERR replication error\r\n".to_vec(),

            // Scripting errors
            ScriptError => b"-ERR script error\r\n".to_vec(),
            ScriptKilled => b"-ERR script killed\r\n".to_vec(),
            ScriptTimeout => b"-ERR script timeout\r\n".to_vec(),
            NoScript => b"-NOSCRIPT No matching script\r\n".to_vec(),

            // Pub/Sub errors
            InvalidChannel => b"-ERR invalid channel\r\n".to_vec(),
            NotSubscribed => b"-ERR not subscribed\r\n".to_vec(),

            // Persistence errors
            BackgroundSaveInProgress => b"-ERR Background save already in progress\r\n".to_vec(),
            BackgroundSaveError => b"-ERR Background save error\r\n".to_vec(),

            // Stream errors
            InvalidStreamId => b"-ERR Invalid stream ID format\r\n".to_vec(),
            StreamIdTooSmall => b"-ERR The ID specified in XADD must be greater than 0-0\r\n".to_vec(),
            StreamIdNotGreater => b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".to_vec(),
            InvalidStreamEntryId => b"-ERR Invalid stream entry ID\r\n".to_vec(),
            StreamNotFound => b"-ERR Stream not found\r\n".to_vec(),
            InvalidStreamOperation => b"-ERR Invalid stream operation\r\n".to_vec(),

            // Generic errors
            InternalError => b"-ERR internal error\r\n".to_vec(),
            Timeout => b"-ERR timeout\r\n".to_vec(),
            ConnectionLost => b"-ERR connection lost\r\n".to_vec(),
            InvalidArgument => b"-ERR invalid argument\r\n".to_vec(),
            OperationNotSupported => b"-ERR operation not supported\r\n".to_vec(),
            Readonly => b"-READONLY You can't write against a read only replica\r\n".to_vec(),
            Loading => b"-LOADING Redis is loading the dataset in memory\r\n".to_vec(),
            Busy => b"-BUSY Redis is busy running a script\r\n".to_vec(),

            // Custom error with message
            Custom(msg) => format!("-ERR {}\r\n", msg).into_bytes(),
        }
    }
}
