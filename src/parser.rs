#![allow(unused)]
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    fmt::format,
    io::Read,
    isize,
};

use crate::frame::Frame;
use bytes::Bytes;

//  TODO: [ ] Find a better way to convert from Frame to bytes and vice versa

pub const SIMPLE_STRING: u8 = b'+';
pub const SIMPLE_ERROR: u8 = b'-';
pub const INTEGER: u8 = b':';
pub const BULK_STRING: u8 = b'$';
pub const ARRAY: u8 = b'*';
pub const NULL: u8 = b'_';
pub const BOOLEAN: u8 = b'#';
pub const DOUBLES: u8 = b',';
pub const BIG_NUMBERS: u8 = b'(';
pub const BULK_ERRORS: u8 = b'!';
pub const VERBATIM_STRINGS: u8 = b'=';
pub const MAPS: u8 = b'%';
pub const ATTRIBUTES: u8 = b'|';
pub const SETS: u8 = b'~';
pub const PUSHES: u8 = b'>';

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespError {
    // Protocol errors
    InvalidProtocol,
    InvalidDataType,
    InvalidLength,
    UnexpectedEnd,
    InvalidInteger,
    InvalidBulkString,
    InvalidArray,
    MalformedCommand,

    // Command errors
    UnknownCommand,
    WrongNumberOfArguments,
    InvalidCommandSyntax,

    // Data type errors
    WrongType,
    InvalidKey,
    InvalidValue,
    InvalidIndex,
    IndexOutOfRange,

    // Memory and resource errors
    OutOfMemory,
    MaxClientsReached,
    MaxDatabasesReached,

    // Authentication and authorization
    NoAuth,
    InvalidPassword,
    NoPermission,

    // Database errors
    InvalidDatabase,
    DatabaseNotFound,
    KeyNotFound,
    KeyExists,

    // Transaction errors
    MultiNotAllowed,
    ExecWithoutMulti,
    DiscardWithoutMulti,
    WatchInMulti,

    // Replication errors
    MasterDown,
    SlaveNotConnected,
    ReplicationError,

    // Scripting errors
    ScriptError,
    ScriptKilled,
    ScriptTimeout,
    NoScript,

    // Pub/Sub errors
    InvalidChannel,
    NotSubscribed,

    // Persistence errors
    BackgroundSaveInProgress,
    BackgroundSaveError,

    // Generic errors
    InternalError,
    Timeout,
    ConnectionLost,
    InvalidArgument,
    OperationNotSupported,
    Readonly,
    Loading,
    Busy,

    // Custom error with message
    Custom(String),
}

impl RespError {
    pub fn message(&self) -> Cow<'static, str> {
        match self {
            // Protocol errors
            RespError::InvalidProtocol => Cow::Borrowed("ERR Protocol error"),
            RespError::InvalidDataType => Cow::Borrowed("ERR Invalid data type"),
            RespError::InvalidLength => Cow::Borrowed("ERR Invalid length"),
            RespError::UnexpectedEnd => Cow::Borrowed("ERR Unexpected end of input"),
            RespError::InvalidInteger => Cow::Borrowed("ERR Invalid integer"),
            RespError::InvalidBulkString => Cow::Borrowed("ERR Invalid bulk string"),
            RespError::InvalidArray => Cow::Borrowed("ERR Invalid array"),
            RespError::MalformedCommand => Cow::Borrowed("ERR Malformed command"),

            // Command errors
            RespError::UnknownCommand => Cow::Borrowed("ERR unknown command"),
            RespError::WrongNumberOfArguments => Cow::Borrowed("ERR wrong number of arguments"),
            RespError::InvalidCommandSyntax => Cow::Borrowed("ERR syntax error"),

            // Data type errors
            RespError::WrongType => {
                Cow::Borrowed("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            RespError::InvalidKey => Cow::Borrowed("ERR invalid key"),
            RespError::InvalidValue => Cow::Borrowed("ERR invalid value"),
            RespError::InvalidIndex => Cow::Borrowed("ERR invalid index"),
            RespError::IndexOutOfRange => Cow::Borrowed("ERR index out of range"),

            // Memory and resource errors
            RespError::OutOfMemory => {
                Cow::Borrowed("OOM command not allowed when used memory > 'maxmemory'")
            }
            RespError::MaxClientsReached => Cow::Borrowed("ERR max number of clients reached"),
            RespError::MaxDatabasesReached => Cow::Borrowed("ERR max number of databases reached"),

            // Authentication and authorization
            RespError::NoAuth => Cow::Borrowed("NOAUTH Authentication required"),
            RespError::InvalidPassword => Cow::Borrowed("ERR invalid password"),
            RespError::NoPermission => {
                Cow::Borrowed("NOPERM this user has no permissions to run this command")
            }

            // Database errors
            RespError::InvalidDatabase => Cow::Borrowed("ERR invalid database"),
            RespError::DatabaseNotFound => Cow::Borrowed("ERR database not found"),
            RespError::KeyNotFound => Cow::Borrowed("ERR key not found"),
            RespError::KeyExists => Cow::Borrowed("ERR key already exists"),

            // Transaction errors
            RespError::MultiNotAllowed => Cow::Borrowed("ERR MULTI calls can not be nested"),
            RespError::ExecWithoutMulti => Cow::Borrowed("ERR EXEC without MULTI"),
            RespError::DiscardWithoutMulti => Cow::Borrowed("ERR DISCARD without MULTI"),
            RespError::WatchInMulti => Cow::Borrowed("ERR WATCH inside MULTI is not allowed"),

            // Replication errors
            RespError::MasterDown => Cow::Borrowed("ERR master is down"),
            RespError::SlaveNotConnected => Cow::Borrowed("ERR slave not connected"),
            RespError::ReplicationError => Cow::Borrowed("ERR replication error"),

            // Scripting errors
            RespError::ScriptError => Cow::Borrowed("ERR script error"),
            RespError::ScriptKilled => Cow::Borrowed("ERR script killed"),
            RespError::ScriptTimeout => Cow::Borrowed("ERR script timeout"),
            RespError::NoScript => Cow::Borrowed("NOSCRIPT No matching script"),

            // Pub/Sub errors
            RespError::InvalidChannel => Cow::Borrowed("ERR invalid channel"),
            RespError::NotSubscribed => Cow::Borrowed("ERR not subscribed"),

            // Persistence errors
            RespError::BackgroundSaveInProgress => {
                Cow::Borrowed("ERR Background save already in progress")
            }
            RespError::BackgroundSaveError => Cow::Borrowed("ERR Background save error"),

            // Generic errors
            RespError::InternalError => Cow::Borrowed("ERR internal error"),
            RespError::Timeout => Cow::Borrowed("ERR timeout"),
            RespError::ConnectionLost => Cow::Borrowed("ERR connection lost"),
            RespError::InvalidArgument => Cow::Borrowed("ERR invalid argument"),
            RespError::OperationNotSupported => Cow::Borrowed("ERR operation not supported"),
            RespError::Readonly => {
                Cow::Borrowed("READONLY You can't write against a read only replica")
            }
            RespError::Loading => Cow::Borrowed("LOADING Redis is loading the dataset in memory"),
            RespError::Busy => Cow::Borrowed("BUSY Redis is busy running a script"),

            // Custom error
            RespError::Custom(msg) => Cow::Owned(format!("ERR {msg}")),
        }
    }
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl Error for RespError {}

pub fn parse_simple_strings(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != SIMPLE_STRING {
                return Err(RespError::WrongType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
                return Err(RespError::InvalidValue);
            }

            let consumed = Frame::SimpleString(String::from_utf8_lossy(consumed).to_string());
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_simple_errors(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != SIMPLE_ERROR {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
                return Err(RespError::InvalidValue);
            }

            let consumed = Frame::SimpleError(String::from_utf8_lossy(consumed).to_string());
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_integers(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != INTEGER {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            let parsed_int = String::from_utf8_lossy(consumed)
                .parse::<i64>()
                .map_err(|_| RespError::InvalidValue)?;
            let consumed = Frame::Integer(parsed_int);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes[0] {
        SIMPLE_STRING => {
            let (parsed, remain) = parse_simple_strings(bytes)?;
            Ok((parsed, remain))
        }
        SIMPLE_ERROR => {
            let (parsed, remain) = parse_simple_errors(bytes)?;
            Ok((parsed, remain))
        }
        BULK_STRING => {
            let (parsed, remain) = parse_bulk_strings(bytes)?;
            Ok((parsed, remain))
        }
        ARRAY => {
            let (parsed, remain) = parse_array(bytes)?;
            Ok((parsed, remain))
        }
        INTEGER => {
            let (parsed, remain) = parse_integers(bytes)?;
            Ok((parsed, remain))
        }
        DOUBLES => {
            let (parsed, remain) = parse_doubles(bytes)?;
            Ok((parsed, remain))
        }
        BOOLEAN => {
            let (parsed, remain) = parse_boolean(bytes)?;
            Ok((parsed, remain))
        }
        NULL => {
            let (parsed, remain) = parse_nulls(bytes)?;
            Ok((parsed, remain))
        }
        MAPS => {
            let (parsed, remain) = parse_maps(bytes)?;
            Ok((parsed, remain))
        }

        _ => Err(RespError::InvalidDataType),
    }
}

pub fn parse_array(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != ARRAY {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, mut remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            let length = String::from_utf8_lossy(consumed)
                .parse::<u64>()
                .map_err(|_| RespError::InvalidValue)?;

            let mut array: Vec<Frame> = Vec::with_capacity(length as usize);

            for _ in 0..length {
                if !remained.is_empty() {
                    let (parsed, rest) = parse(remained)?;
                    remained = rest;
                    array.push(parsed);
                }
            }

            if array.len() != length as usize {
                return Err(RespError::UnexpectedEnd);
            }

            let consumed = Frame::Array(array);

            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_bulk_strings(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != BULK_STRING {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            let length = String::from_utf8_lossy(consumed)
                .parse::<isize>()
                .map_err(|_| RespError::InvalidValue)?;

            if length == -1 {
                return Ok((Frame::Null, remained));
            }

            if length < 0 {
                return Err(RespError::InvalidValue);
            }

            if length as usize > remained.len() {
                return Err(RespError::UnexpectedEnd);
            }

            let bulk_string = Bytes::copy_from_slice(&remained[..length as usize]);
            let remaining_after_string = &remained[length as usize..];

            if !remaining_after_string.starts_with(b"\r\n") {
                return Err(RespError::UnexpectedEnd);
            }

            Ok((Frame::BulkString(bulk_string), &remaining_after_string[2..]))
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_nulls(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != NULL {
                return Err(RespError::WrongType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
                return Err(RespError::InvalidValue);
            }

            let consumed = Frame::Null;
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_boolean(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != BOOLEAN {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            let mut val = false;
            if consumed.len() == 1 {
                match consumed.first() {
                    Some(b't') => val = true,
                    Some(b'f') => val = false,
                    Some(_) => return Err(RespError::InvalidValue),
                    None => return Err(RespError::UnexpectedEnd),
                }
            } else {
                return Err(RespError::UnexpectedEnd);
            }

            let consumed = Frame::Boolean(val);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_doubles(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != DOUBLES {
                return Err(RespError::InvalidDataType);
            }

            let (consumed, remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            let parsed_double = String::from_utf8_lossy(consumed)
                .parse::<f64>()
                .map_err(|_| RespError::InvalidValue)?;
            let consumed = Frame::Double(parsed_double);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_maps(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    match bytes {
        [first, rest @ ..] => {
            if *first != MAPS {
                return Err(RespError::InvalidDataType);
            }

            // this would consume the <digit>\r\n
            let (consumed, mut remained) = rest
                .windows(2)
                .position(|window| window == b"\r\n")
                .map(|pos| (&rest[..pos], &rest[pos + 2..]))
                .ok_or(RespError::UnexpectedEnd)?;

            // should equal the digit
            let length = String::from_utf8_lossy(consumed)
                .parse::<u64>()
                .map_err(|_| RespError::InvalidValue)?;

            let mut map: HashMap<String, Frame> = HashMap::new();

            let mut key_set: HashSet<String> = HashSet::new();

            // I mean this is pretty unredable but it is what it is :/
            // The redundant !remained.is_empty() is because the parse function should handle the
            // empty bytes but that would mean I refactor the parse to return and (Option<Frame>, &[u8])
            // Which is kind of a lot of work now so this works for now I should probably do this for arrray parsing I think
            for _ in 0..length {
                if !remained.is_empty() {
                    if !remained.is_empty() {
                        let (key, rest) = parse(remained)?;
                        key_set.insert(key.as_string().unwrap_or_default().to_string());
                        remained = rest;
                        if !remained.is_empty() {
                            let (value, rest) = parse(remained)?;
                            remained = rest;
                            map.insert(key.as_string().unwrap_or_default().to_string(), value);
                        }
                    }
                }
            }

            // I need this because if the user sent the same key it should override and the check
            // for unexpected end fails because it would expect the length of the map that was intended by length variable
            if map.len() != key_set.len() {
                return Err(RespError::UnexpectedEnd);
            }

            let consumed = Frame::Map(map);

            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse_big_numbers() {
    todo!()
}

pub fn parse_sets() {
    todo!()
}

pub fn parse_verbatim_string() {
    todo!()
}

pub fn parse_bulk_errors() {
    todo!()
}

pub fn parse_attributes() {
    todo!()
}

pub fn parse_pushes() {
    todo!()
}
