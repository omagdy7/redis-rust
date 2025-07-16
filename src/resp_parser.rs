#![allow(unused)]
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::format,
    io::Read,
    isize,
};

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

fn parse_simple_strings(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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

            let consumed = RespType::SimpleString(String::from_utf8_lossy(consumed).to_string());
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_simple_errors(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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

            let consumed = RespType::SimpleError(String::from_utf8_lossy(consumed).to_string());
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_integers(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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
                .parse::<u64>()
                .map_err(|_| RespError::InvalidValue)?;
            let consumed = RespType::Integer(parsed_int);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

pub fn parse(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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

        _ => Err(RespError::InvalidDataType),
    }
}

fn parse_array(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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

            let mut array: Vec<RespType> = Vec::with_capacity(length as usize);

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

            let consumed = RespType::Array(array);

            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_bulk_strings(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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
                return Ok((RespType::Null(), remained));
            }

            if length < 0 {
                return Err(RespError::InvalidValue);
            }

            if length as usize > remained.len() {
                return Err(RespError::UnexpectedEnd);
            }

            let mut bulk_string: Vec<u8> = Vec::with_capacity(length as usize);

            for i in 0..length {
                bulk_string.push(remained[i as usize]);
            }

            let consumed = RespType::BulkString(bulk_string);

            if !(&remained[length as usize..]).starts_with(b"\r\n") {
                return Err(RespError::UnexpectedEnd);
            }
            return Ok((consumed, &remained[length as usize + 2..]));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_nulls(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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

            let consumed = RespType::Null();
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_boolean(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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
                match consumed.first().unwrap() {
                    b't' => val = true,
                    b'f' => val = false,
                    _ => return Err(RespError::InvalidValue),
                }
            } else {
                return Err(RespError::UnexpectedEnd);
            }

            let consumed = RespType::Boolean(val);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_doubles(bytes: &[u8]) -> Result<(RespType, &[u8]), RespError> {
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
            let consumed = RespType::Doubles(parsed_double);
            return Ok((consumed, remained));
        }
        [] => Err(RespError::Custom(String::from("Empty data"))),
    }
}

fn parse_big_numbers() {
    todo!()
}

fn parse_sets() {
    todo!()
}

fn parse_maps() {
    todo!()
}

fn parse_verbatim_string() {
    todo!()
}

fn parse_bulk_errors() {
    todo!()
}

fn parse_attributes() {
    todo!()
}

fn parse_pushes() {
    todo!()
}

#[derive(Debug, Clone)]
pub enum RespType {
    SimpleString(String),              // +
    SimpleError(String),               // -
    Integer(u64),                      // :
    BulkString(Vec<u8>),               // $
    Array(Vec<RespType>),              // *
    Null(),                            // _
    Boolean(bool),                     // #
    Doubles(f64),                      // ,
    BigNumbers(String),                // (
    BulkErrors(Vec<RespType>),         // !
    VerbatimStrings(Vec<RespType>),    // =
    Maps(HashMap<RespType, RespType>), // %
    Attributes(Vec<RespType>),         // |
    Sets(HashSet<RespType>),           // ~
    Pushes(Vec<RespType>),             // >
}

impl RespType {
    pub fn to_resp_bytes(&self) -> Vec<u8> {
        match self {
            RespType::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespType::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
            RespType::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespType::BulkString(bytes) => {
                let len = bytes.len();
                let s = String::from_utf8_lossy(bytes);
                format!("${}\r\n{}\r\n", len, s).into_bytes()
            }
            RespType::Array(arr) => {
                let len = arr.len();
                let elements = arr
                    .iter()
                    .map(|e| e.to_resp_bytes())
                    .collect::<Vec<Vec<u8>>>();
                // TODO: Implement proper Display for elements because this will definitely not
                // work
                format!("*{:?}\r\n{:?}", len, elements).into_bytes()
            }
            RespType::Null() => b"_\r\n".into(),
            RespType::Boolean(b) => format!("#{}\r\n", if *b { "t" } else { "f" }).into_bytes(),
            RespType::Doubles(d) => format!(",{}\r\n", d).into_bytes(),
            RespType::BigNumbers(n) => format!("({}\r\n", n).into_bytes(),
            RespType::BulkErrors(errors) => {
                todo!()
            }
            RespType::VerbatimStrings(strings) => {
                todo!()
            }
            RespType::Maps(map) => {
                todo!()
            }
            RespType::Attributes(attrs) => {
                todo!()
            }
            RespType::Sets(set) => {
                todo!()
            }
            RespType::Pushes(pushes) => {
                todo!()
            }
        }
    }
}

impl PartialEq for RespType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RespType::SimpleString(a), RespType::SimpleString(b)) => a == b,
            (RespType::SimpleError(a), RespType::SimpleError(b)) => a == b,
            (RespType::Integer(a), RespType::Integer(b)) => a == b,
            (RespType::BulkString(a), RespType::BulkString(b)) => a == b,
            (RespType::Array(a), RespType::Array(b)) => a == b,
            (RespType::Null(), RespType::Null()) => true,
            (RespType::Boolean(a), RespType::Boolean(b)) => a == b,
            (RespType::Doubles(a), RespType::Doubles(b)) => a == b,
            (RespType::BigNumbers(a), RespType::BigNumbers(b)) => a == b,
            (RespType::BulkErrors(a), RespType::BulkErrors(b)) => a == b,
            (RespType::VerbatimStrings(a), RespType::VerbatimStrings(b)) => a == b,
            // (RespType::Maps(a), RespType::Maps(b)) => a == b,
            (RespType::Attributes(a), RespType::Attributes(b)) => a == b,
            // (RespType::Sets(a), RespType::Sets(b)) => a == b,
            (RespType::Pushes(a), RespType::Pushes(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialEq<&str> for RespType {
    fn eq(&self, other: &&str) -> bool {
        match self {
            RespType::SimpleString(s) => s == other,
            RespType::SimpleError(s) => s == other,
            RespType::BigNumbers(s) => s == other,
            RespType::BulkString(bytes) => {
                for (b1, b2) in bytes.iter().zip(other.as_bytes().iter()) {
                    if b1 != b2 {
                        return false;
                    }
                }
                return true;
            }
            _ => false,
        }
    }
}

impl PartialEq<str> for RespType {
    fn eq(&self, other: &str) -> bool {
        match self {
            RespType::SimpleString(s) => s == other,
            RespType::SimpleError(s) => s == other,
            RespType::BigNumbers(s) => s == other,
            RespType::BulkString(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    *s == *other
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl PartialEq<String> for RespType {
    fn eq(&self, other: &String) -> bool {
        match self {
            RespType::SimpleString(s) => s == other,
            RespType::SimpleError(s) => s == other,
            RespType::BigNumbers(s) => s == other,
            RespType::BulkString(bytes) => {
                for (b1, b2) in bytes.iter().zip(other.as_bytes().iter()) {
                    if b1 != b2 {
                        return false;
                    }
                }
                return true;
            }
            _ => false,
        }
    }
}

impl PartialEq<u64> for RespType {
    fn eq(&self, other: &u64) -> bool {
        match self {
            RespType::Integer(i) => i == other,
            _ => false,
        }
    }
}

impl PartialEq<bool> for RespType {
    fn eq(&self, other: &bool) -> bool {
        match self {
            RespType::Boolean(b) => b == other,
            _ => false,
        }
    }
}

impl PartialEq<f64> for RespType {
    fn eq(&self, other: &f64) -> bool {
        match self {
            RespType::Doubles(d) => d == other,
            _ => false,
        }
    }
}

// Test module
#[cfg(test)]
mod tests {
    use super::*;

    mod simple_strings_tests {
        use super::*;

        #[test]
        fn test_valid_simple_strings() {
            // Basic valid cases
            assert_eq!(parse_simple_strings(b"+OK\r\n").unwrap().0, "OK");
            assert_eq!(parse_simple_strings(b"+PONG\r\n").unwrap().0, "PONG");
            assert_eq!(
                parse_simple_strings(b"+Hello World\r\n").unwrap().0,
                "Hello World"
            );

            // Empty string
            assert_eq!(parse_simple_strings(b"+\r\n").unwrap().0, "");

            // String with spaces and special characters (but no \r or \n)
            assert_eq!(
                parse_simple_strings(b"+Hello, World! 123\r\n").unwrap().0,
                "Hello, World! 123"
            );

            // String with various ASCII characters
            assert_eq!(
                parse_simple_strings(b"+!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \r\n")
                    .unwrap()
                    .0,
                "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ "
            );

            // Unicode characters (should work with UTF-8)
            assert_eq!(
                parse_simple_strings(b"+\xc3\xa9\xc3\xa1\xc3\xb1\r\n")
                    .unwrap()
                    .0,
                "éáñ"
            );
        }

        #[test]
        fn test_invalid_prefix() {
            // Missing '+' prefix
            assert_eq!(
                parse_simple_strings(b"OK\r\n").err().unwrap().message(),
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );

            // Wrong prefix
            assert_eq!(
                parse_simple_strings(b"-Error\r\n").err().unwrap().message(),
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );
            assert_eq!(
                parse_simple_strings(b":123\r\n").err().unwrap().message(),
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );
            assert_eq!(
                parse_simple_strings(b"$5\r\nhello\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );
        }

        #[test]
        fn test_missing_crlf_terminator() {
            // No CRLF at all
            assert_eq!(
                parse_simple_strings(b"+OK").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \r
            assert_eq!(
                parse_simple_strings(b"+OK\r").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \n
            assert_eq!(
                parse_simple_strings(b"+OK\n").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Wrong order (\n\r instead of \r\n)
            assert_eq!(
                parse_simple_strings(b"+OK\n\r").err().unwrap().message(),
                "ERR Unexpected end of input"
            );
        }

        #[test]
        fn test_invalid_characters_in_content() {
            // Contains \r in content
            assert_eq!(
                parse_simple_strings(b"+Hello\rWorld\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR invalid value"
            );

            // Contains \n in content
            assert_eq!(
                parse_simple_strings(b"+Hello\nWorld\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR invalid value"
            );
        }

        #[test]
        fn test_empty_input() {
            assert_eq!(
                parse_simple_strings(b"").err().unwrap().message(),
                "ERR Empty data"
            );
        }

        #[test]
        fn test_with_trailing_data() {
            // RESP simple string with extra data after CRLF (should be ignored)
            assert_eq!(parse_simple_strings(b"+OK\r\nextra_data").unwrap().0, "OK");
            assert_eq!(
                parse_simple_strings(b"+PONG\r\n+another_string\r\n")
                    .unwrap()
                    .0,
                "PONG"
            );
        }

        #[test]
        fn test_real_world_redis_responses() {
            // Common Redis simple string responses
            assert_eq!(parse_simple_strings(b"+OK\r\n").unwrap().0, "OK");
            assert_eq!(parse_simple_strings(b"+PONG\r\n").unwrap().0, "PONG");
            assert_eq!(parse_simple_strings(b"+QUEUED\r\n").unwrap().0, "QUEUED");

            // Redis status responses
            assert_eq!(
                parse_simple_strings(b"+Background saving started\r\n")
                    .unwrap()
                    .0,
                "Background saving started"
            );
            assert_eq!(
                parse_simple_strings(b"+Background saving successfully finished\r\n")
                    .unwrap()
                    .0,
                "Background saving successfully finished"
            );
        }

        #[test]
        fn test_edge_cases() {
            // Just the prefix and CRLF
            assert_eq!(parse_simple_strings(b"+\r\n").unwrap().0, "");

            // Long string
            let long_string = "a".repeat(1000);
            let mut input = b"+".to_vec();
            input.extend_from_slice(long_string.as_bytes());
            input.extend_from_slice(b"\r\n");
            assert_eq!(parse_simple_strings(&input).unwrap().0, long_string);

            // String with only spaces
            assert_eq!(parse_simple_strings(b"+   \r\n").unwrap().0, "   ");

            // String with tabs and other whitespace
            assert_eq!(parse_simple_strings(b"+\t  \t\r\n").unwrap().0, "\t  \t");
        }

        #[test]
        fn test_binary_safety_within_limits() {
            // Non-UTF8 bytes (but no \r or \n)
            let mut input = b"+".to_vec();
            input.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8
            input.extend_from_slice(b"\r\n");

            // Should handle invalid UTF-8 gracefully with replacement characters
            if let RespType::SimpleString(data) = parse_simple_strings(&input).unwrap().0 {
                assert!(!data.is_empty()); // Should contain replacement characters
            }
        }
    }

    mod simple_errors_tests {
        use super::*;

        #[test]
        fn test_valid_simple_errors() {
            // Basic valid cases
            assert_eq!(
                parse_simple_errors(b"-ERR unknown command\r\n").unwrap().0,
                "ERR unknown command"
            );
            assert_eq!(
                parse_simple_errors(b"-WRONGTYPE\r\n").unwrap().0,
                "WRONGTYPE"
            );
            assert_eq!(
                parse_simple_errors(b"-ERR syntax error\r\n").unwrap().0,
                "ERR syntax error"
            );

            // Empty error string
            assert_eq!(parse_simple_errors(b"-\r\n").unwrap().0, "");

            // Error with spaces and special characters (but no \r or \n)
            assert_eq!(
                parse_simple_errors(b"-ERR invalid key: 'test123'\r\n")
                    .unwrap()
                    .0,
                "ERR invalid key: 'test123'"
            );

            // Error with various ASCII characters
            assert_eq!(
                parse_simple_errors(b"-ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \r\n")
                    .unwrap()
                    .0,
                "ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ "
            );

            // Unicode characters in error message
            assert_eq!(
                parse_simple_errors(b"-ERR \xc3\xa9\xc3\xa1\xc3\xb1\r\n")
                    .unwrap()
                    .0,
                "ERR éáñ"
            );

            // Common Redis error patterns
            assert_eq!(
                parse_simple_errors(b"-NOAUTH Authentication required\r\n")
                    .unwrap()
                    .0,
                "NOAUTH Authentication required"
            );
            assert_eq!(
                parse_simple_errors(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                )
                .unwrap()
                .0,
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );
        }

        #[test]
        fn test_invalid_simple_errors() {
            // Wrong data type marker
            assert_eq!(
                parse_simple_errors(b"+OK\r\n").err().unwrap().message(),
                "ERR Invalid data type"
            );

            // Contains \r in content
            assert_eq!(
                parse_simple_errors(b"-ERR invalid\r character\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR invalid value"
            );

            // Contains \n in content
            assert_eq!(
                parse_simple_errors(b"-ERR invalid\n character\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR invalid value"
            );

            // Missing \r\n terminator
            assert_eq!(
                parse_simple_errors(b"-ERR no terminator")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Only \r without \n
            assert_eq!(
                parse_simple_errors(b"-ERR only carriage return\r")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Only \n without \r
            assert_eq!(
                parse_simple_errors(b"-ERR only newline\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Empty input
            assert_eq!(
                parse_simple_errors(b"").err().unwrap().message(),
                "ERR Empty data"
            );

            // Just the marker without content
            assert_eq!(
                parse_simple_errors(b"-").err().unwrap().message(),
                "ERR Unexpected end of input"
            );
        }

        #[test]
        fn test_simple_error_remaining_bytes() {
            // Test that remaining bytes are correctly returned
            let (error, remaining) = parse_simple_errors(b"-ERR test\r\nnext data").unwrap();
            assert_eq!(error, "ERR test");
            assert_eq!(remaining, b"next data");

            // Test with multiple commands
            let (error, remaining) = parse_simple_errors(b"-WRONGTYPE\r\n+OK\r\n").unwrap();
            assert_eq!(error, "WRONGTYPE");
            assert_eq!(remaining, b"+OK\r\n");

            // Test with no remaining data
            let (error, remaining) = parse_simple_errors(b"-ERR final\r\n").unwrap();
            assert_eq!(error, "ERR final");
            assert_eq!(remaining, b"");
        }
    }

    mod integeres_tests {
        use super::*;

        #[test]
        fn test_valid_integers() {
            // Basic valid cases
            assert_eq!(parse_integers(b":0\r\n").unwrap().0, 0u64);
            assert_eq!(parse_integers(b":1\r\n").unwrap().0, 1u64);
            assert_eq!(parse_integers(b":42\r\n").unwrap().0, 42u64);
            assert_eq!(parse_integers(b":1000\r\n").unwrap().0, 1000u64);

            assert_eq!(parse_integers(b":+42\r\n").unwrap().0, 42u64);

            // Large numbers
            assert_eq!(
                parse_integers(b":9223372036854775807\r\n").unwrap().0,
                9223372036854775807u64
            );
            assert_eq!(
                parse_integers(b":18446744073709551615\r\n").unwrap().0,
                18446744073709551615u64
            ); // u64::MAX

            // Edge cases
            assert_eq!(parse_integers(b":123456789\r\n").unwrap().0, 123456789u64);

            // Numbers with leading zeros (should still parse correctly)
            assert_eq!(parse_integers(b":0000042\r\n").unwrap().0, 42u64);
            assert_eq!(parse_integers(b":00000\r\n").unwrap().0, 0u64);
        }

        #[test]
        fn test_invalid_integers() {
            // Wrong data type marker
            assert_eq!(
                parse_integers(b"+42\r\n").err().unwrap().message(),
                "ERR Invalid data type"
            );

            // Negative numbers (not valid for u64)
            assert_eq!(
                parse_integers(b":-42\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Non-numeric content
            assert_eq!(
                parse_integers(b":abc\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Mixed numeric and non-numeric
            assert_eq!(
                parse_integers(b":42abc\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Empty integer
            assert_eq!(
                parse_integers(b":\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Contains \r in content
            assert_eq!(
                parse_integers(b":42\r23\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Contains \n in content
            assert_eq!(
                parse_integers(b":42\n23\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Missing \r\n terminator
            assert_eq!(
                parse_integers(b":42").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \r without \n
            assert_eq!(
                parse_integers(b":42\r").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \n without \r
            assert_eq!(
                parse_integers(b":42\n").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Empty input
            assert_eq!(
                parse_integers(b"").err().unwrap().message(),
                "ERR Empty data"
            );

            // Just the marker without content
            assert_eq!(
                parse_integers(b":").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Number too large for u64
            assert_eq!(
                parse_integers(b":18446744073709551616\r\n") // u64::MAX + 1
                    .err()
                    .unwrap()
                    .message(),
                "ERR invalid value"
            );

            // Floating point numbers
            assert_eq!(
                parse_integers(b":42.5\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Scientific notation
            assert_eq!(
                parse_integers(b":1e5\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Hexadecimal numbers
            assert_eq!(
                parse_integers(b":0x42\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Whitespace
            assert_eq!(
                parse_integers(b": 42\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            assert_eq!(
                parse_integers(b":42 \r\n").err().unwrap().message(),
                "ERR invalid value"
            );
        }

        #[test]
        fn test_integer_remaining_bytes() {
            // Test that remaining bytes are correctly returned
            let (integer, remaining) = parse_integers(b":42\r\nnext data").unwrap();
            assert_eq!(integer, 42u64);
            assert_eq!(remaining, b"next data");

            // Test with multiple commands
            let (integer, remaining) = parse_integers(b":1337\r\n+OK\r\n").unwrap();
            assert_eq!(integer, 1337u64);
            assert_eq!(remaining, b"+OK\r\n");

            // Test with no remaining data
            let (integer, remaining) = parse_integers(b":999\r\n").unwrap();
            assert_eq!(integer, 999u64);
            assert_eq!(remaining, b"");

            // Test with zero and remaining data
            let (integer, remaining) = parse_integers(b":0\r\n-ERR test\r\n").unwrap();
            assert_eq!(integer, 0u64);
            assert_eq!(remaining, b"-ERR test\r\n");
        }
    }

    #[cfg(test)]
    mod bulk_string_tests {
        use super::*;

        #[test]
        fn test_valid_bulk_strings() {
            // basic valid cases
            assert_eq!(parse_bulk_strings(b"$2\r\nok\r\n").unwrap().0, "ok");
            assert_eq!(parse_bulk_strings(b"$4\r\npong\r\n").unwrap().0, "pong");
            assert_eq!(
                parse_bulk_strings(b"$11\r\nhello world\r\n").unwrap().0,
                "hello world"
            );

            // empty string
            assert_eq!(parse_bulk_strings(b"$0\r\n\r\n").unwrap().0, "");

            // string with special characters (including \r and \n - allowed in bulk strings)
            assert_eq!(
                parse_bulk_strings(b"$13\r\nhello\r\nworld!\r\n").unwrap().0,
                "hello\r\nworld!"
            );

            // string with various ascii characters
            assert_eq!(
                parse_bulk_strings(b"$30\r\n!@#$%^&*()_+-={}[]|\\:;\"'<>?,./\r\n")
                    .unwrap()
                    .0,
                "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./"
            );

            // large string
            let large_content = "x".repeat(1000);
            let large_bulk = format!("$1000\r\n{}\r\n", large_content);
            if let RespType::BulkString(bulk) = parse_bulk_strings(large_bulk.as_bytes()).unwrap().0
            {
            }

            assert_eq!(
                parse_bulk_strings(large_bulk.as_bytes()).unwrap().0,
                large_content
            );

            // string with only whitespace
            assert_eq!(parse_bulk_strings(b"$3\r\n   \r\n").unwrap().0, "   ");

            // string with tabs and newlines
            assert_eq!(
                parse_bulk_strings(b"$7\r\nhe\tllo\n\r\n").unwrap().0,
                "he\tllo\n"
            );
        }

        #[test]
        fn test_null_bulk_string() {
            // Null bulk string
            let (result, remaining) = parse_bulk_strings(b"$-1\r\n").unwrap();
            assert_eq!(result, RespType::Null());
            assert_eq!(remaining, b"");

            // Null bulk string with remaining data
            let (result, remaining) = parse_bulk_strings(b"$-1\r\n+OK\r\n").unwrap();
            assert_eq!(result, RespType::Null());
            assert_eq!(remaining, b"+OK\r\n");
        }

        #[test]
        fn test_invalid_bulk_strings() {
            // Wrong data type marker
            assert_eq!(
                parse_bulk_strings(b"+OK\r\n").err().unwrap().message(),
                "ERR Invalid data type"
            );

            // Invalid length format
            assert_eq!(
                parse_bulk_strings(b"$abc\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Negative length (other than -1)
            assert_eq!(
                parse_bulk_strings(b"$-5\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Missing length
            assert_eq!(
                parse_bulk_strings(b"$\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Missing first \r\n after length
            assert_eq!(
                parse_bulk_strings(b"$5hello\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Content shorter than declared length
            assert_eq!(
                parse_bulk_strings(b"$5\r\nhi\r\n").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Content longer than declared length (missing final \r\n)
            assert_eq!(
                parse_bulk_strings(b"$2\r\nhello\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Missing final \r\n
            assert_eq!(
                parse_bulk_strings(b"$5\r\nhello").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \r without \n at the end
            assert_eq!(
                parse_bulk_strings(b"$5\r\nhello\r")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Only \n without \r at the end
            assert_eq!(
                parse_bulk_strings(b"$5\r\nhello\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Empty input
            assert_eq!(
                parse_bulk_strings(b"").err().unwrap().message(),
                "ERR Empty data"
            );

            // Just the marker
            assert_eq!(
                parse_bulk_strings(b"$").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Length too large for available data
            assert_eq!(
                parse_bulk_strings(b"$100\r\nshort\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Zero length but with content
            assert_eq!(
                parse_bulk_strings(b"$0\r\nhello\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );
        }

        #[test]
        fn test_bulk_string_remaining_bytes() {
            // Test that remaining bytes are correctly returned
            let (string, remaining) = parse_bulk_strings(b"$5\r\nhello\r\nnext data").unwrap();
            assert_eq!(string, "hello");
            assert_eq!(remaining, b"next data");

            // Test with multiple commands
            let (string, remaining) = parse_bulk_strings(b"$4\r\ntest\r\n:42\r\n").unwrap();
            assert_eq!(string, "test");
            assert_eq!(remaining, b":42\r\n");

            // Test with no remaining data
            let (string, remaining) = parse_bulk_strings(b"$3\r\nend\r\n").unwrap();
            assert_eq!(string, "end");
            assert_eq!(remaining, b"");

            // Test null string with remaining data
            let (result, remaining) = parse_bulk_strings(b"$-1\r\n+PONG\r\n").unwrap();
            assert_eq!(result, RespType::Null());
            assert_eq!(remaining, b"+PONG\r\n");
        }

        #[test]
        fn test_bulk_string_edge_cases() {
            // String that contains the exact sequence that would end it
            assert_eq!(
                parse_bulk_strings(b"$8\r\ntest\r\n\r\n\r\n").unwrap().0,
                "test\r\n"
            );

            // String with only \r\n
            assert_eq!(parse_bulk_strings(b"$2\r\n\r\n\r\n").unwrap().0, "\r\n");

            // String that starts with numbers
            assert_eq!(parse_bulk_strings(b"$5\r\n12345\r\n").unwrap().0, "12345");

            // String with control characters
            assert_eq!(
                parse_bulk_strings(b"$5\r\n\x01\x02\x03\x04\x05\r\n")
                    .unwrap()
                    .0,
                "\x01\x02\x03\x04\x05"
            );

            // Maximum length value (within reason)
            let content = "a".repeat(65535);
            let bulk = format!("$65535\r\n{}\r\n", content);
            assert_eq!(parse_bulk_strings(bulk.as_bytes()).unwrap().0, content);
        }
    }

    mod array_tests {
        use super::*;

        #[test]
        fn test_valid_arrays() {
            // Simple array with strings
            let arr = vec![
                RespType::BulkString("hello".into()),
                RespType::BulkString("world".into()),
            ];
            assert_eq!(
                parse_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
                    .unwrap()
                    .0,
                RespType::Array(arr)
            );

            // Array with mixed types
            let arr = vec![
                RespType::Integer(42),
                RespType::SimpleString("OK".to_string()),
                RespType::Null(),
            ];
            assert_eq!(
                parse_array(b"*3\r\n:42\r\n+OK\r\n_\r\n").unwrap().0,
                RespType::Array(arr)
            );

            // Nested array
            let arr = vec![
                RespType::Array(vec![
                    RespType::BulkString("nested".into()),
                    RespType::Integer(123),
                ]),
                RespType::SimpleError("ERR test".to_string()),
            ];
            assert_eq!(
                parse_array(b"*2\r\n*2\r\n$6\r\nnested\r\n:123\r\n-ERR test\r\n")
                    .unwrap()
                    .0,
                RespType::Array(arr)
            );

            // Empty array
            let arr = vec![];
            assert_eq!(parse_array(b"*0\r\n").unwrap().0, RespType::Array(arr));
        }

        #[test]
        fn test_invalid_arrays() {
            // Wrong data type marker
            assert_eq!(
                parse_array(b"+2\r\n$5\r\nhello\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Invalid data type"
            );

            // Missing \r\n terminator
            assert_eq!(
                parse_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Invalid length
            assert_eq!(
                parse_array(b"*-1\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Non-numeric length
            assert_eq!(
                parse_array(b"*abc\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Incomplete array elements
            assert_eq!(
                parse_array(b"*2\r\n$5\r\nhello\r\n")
                    .err()
                    .unwrap()
                    .message(),
                "ERR Unexpected end of input"
            );

            // Empty input
            assert_eq!(parse_array(b"").err().unwrap().message(), "ERR Empty data");

            // Just the marker
            assert_eq!(
                parse_array(b"*").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Invalid element type
            assert_eq!(
                parse_array(b"*1\r\n@invalid\r\n").err().unwrap().message(),
                "ERR Invalid data type"
            );
        }

        #[test]
        fn test_array_remaining_bytes() {
            // Test with remaining data
            let arr = vec![RespType::BulkString("test".into()), RespType::Integer(99)];
            let (value, remaining) = parse_array(b"*2\r\n$4\r\ntest\r\n:99\r\n+OK\r\n").unwrap();
            assert_eq!(value, RespType::Array(arr));
            assert_eq!(remaining, b"+OK\r\n");

            // Test with no remaining data
            let arr = vec![RespType::SimpleString("PONG".to_string())];
            let (value, remaining) = parse_array(b"*1\r\n+PONG\r\n").unwrap();
            assert_eq!(value, RespType::Array(arr));
            assert_eq!(remaining, b"");

            // Test with multiple commands
            let arr = vec![RespType::Null()];
            let (value, remaining) = parse_array(b"*1\r\n_\r\n*0\r\n").unwrap();
            assert_eq!(value, RespType::Array(arr));
            assert_eq!(remaining, b"*0\r\n");

            // Test with empty array and remaining data
            let arr = vec![];
            let (value, remaining) = parse_array(b"*0\r\n-ERR test\r\n").unwrap();
            assert_eq!(value, RespType::Array(arr));
            assert_eq!(remaining, b"-ERR test\r\n");
        }
    }

    mod boolean_tests {
        use super::*;

        #[test]
        fn test_valid_booleans() {
            // Basic true value
            assert_eq!(parse_boolean(b"#t\r\n").unwrap().0, RespType::Boolean(true));

            // Basic false value
            assert_eq!(
                parse_boolean(b"#f\r\n").unwrap().0,
                RespType::Boolean(false)
            );
        }

        #[test]
        fn test_invalid_booleans() {
            // Wrong data type marker
            assert_eq!(
                parse_boolean(b":t\r\n").err().unwrap().message(),
                "ERR Invalid data type"
            );

            // Invalid boolean value
            assert_eq!(
                parse_boolean(b"#x\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Missing \r\n terminator
            assert_eq!(
                parse_boolean(b"#t").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Only \r without \n
            assert_eq!(
                parse_boolean(b"#t\r").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Empty input
            assert_eq!(
                parse_boolean(b"").err().unwrap().message(),
                "ERR Empty data"
            );

            // Just the marker
            assert_eq!(
                parse_boolean(b"#").err().unwrap().message(),
                "ERR Unexpected end of input"
            );

            // Case sensitivity
            assert_eq!(
                parse_boolean(b"#T\r\n").err().unwrap().message(),
                "ERR invalid value"
            );

            // Extra content
            assert_eq!(
                parse_boolean(b"#ttrue\r\n").err().unwrap().message(),
                "ERR Unexpected end of input"
            );
        }

        #[test]
        fn test_boolean_remaining_bytes() {
            // Test with remaining data
            let (value, remaining) = parse_boolean(b"#t\r\n+OK\r\n").unwrap();
            assert_eq!(value, RespType::Boolean(true));
            assert_eq!(remaining, b"+OK\r\n");

            // Test with no remaining data
            let (value, remaining) = parse_boolean(b"#f\r\n").unwrap();
            assert_eq!(value, RespType::Boolean(false));
            assert_eq!(remaining, b"");

            // Test with multiple commands
            let (value, remaining) = parse_boolean(b"#t\r\n:42\r\n").unwrap();
            assert_eq!(value, RespType::Boolean(true));
            assert_eq!(remaining, b":42\r\n");

            // Test with false and remaining data
            let (value, remaining) = parse_boolean(b"#f\r\n-ERR test\r\n").unwrap();
            assert_eq!(value, RespType::Boolean(false));
            assert_eq!(remaining, b"-ERR test\r\n");
        }
    }
}
