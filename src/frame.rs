use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use crate::rdb;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub ms_time: u64,
    pub seq: u64,
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.ms_time.cmp(&other.ms_time) {
            std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms_time, self.seq)
    }
}

#[derive(Debug, Clone)]
pub enum ParseStreamIdError {
    MissingPart,
    InvalidNumber,
    InvalidAuto,
}

#[derive(Debug, Clone)]
pub enum XaddStreamId {
    Literal(StreamId),
    AutoSequence { ms_time: u64 },
    Auto,
}

#[derive(Debug, Clone)]
pub enum XrangeStreamdId {
    Literal(StreamId),
    AutoSequence { ms_time: u64 },
    AutoStart,
    AutoEnd,
}

impl FromStr for StreamId {
    type Err = ParseStreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, '-');
        let ms_time = parts
            .next()
            .ok_or(ParseStreamIdError::MissingPart)?
            .parse::<u64>()
            .map_err(|_| ParseStreamIdError::InvalidNumber)?;
        let seq = parts
            .next()
            .ok_or(ParseStreamIdError::MissingPart)?
            .parse::<u64>()
            .map_err(|_| ParseStreamIdError::InvalidNumber)?;
        Ok(StreamId { ms_time, seq })
    }
}

impl FromStr for XaddStreamId {
    type Err = ParseStreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            return Ok(XaddStreamId::Auto);
        }

        let mut parts = s.splitn(2, '-');
        let ms_part = parts.next().ok_or(ParseStreamIdError::MissingPart)?;
        let seq_part = parts.next().ok_or(ParseStreamIdError::MissingPart)?;

        match (ms_part, seq_part) {
            (ms_str, "*") => {
                let ms_time = ms_str
                    .parse::<u64>()
                    .map_err(|_| ParseStreamIdError::InvalidNumber)?;
                Ok(XaddStreamId::AutoSequence { ms_time })
            }
            (ms_str, seq_str) if seq_str != "*" => {
                let ms_time = ms_str
                    .parse::<u64>()
                    .map_err(|_| ParseStreamIdError::InvalidNumber)?;
                let seq = seq_str
                    .parse::<u64>()
                    .map_err(|_| ParseStreamIdError::InvalidNumber)?;
                Ok(XaddStreamId::Literal(StreamId { ms_time, seq }))
            }
            _ => Err(ParseStreamIdError::InvalidAuto),
        }
    }
}

impl FromStr for XrangeStreamdId {
    type Err = ParseStreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "-" {
            return Ok(XrangeStreamdId::AutoStart);
        }

        if s == "+" {
            return Ok(XrangeStreamdId::AutoEnd);
        }

        // Try to parse as a single integer first if it succedes it means it's something like this:
        // 1526985054069 not like this 1526985054069-1
        if let Ok(ms_time) = s.parse::<u64>() {
            return Ok(XrangeStreamdId::AutoSequence { ms_time });
        }

        let mut parts = s.splitn(2, '-');
        let ms_part = parts.next().ok_or(ParseStreamIdError::MissingPart)?;
        let seq_part = parts.next().ok_or(ParseStreamIdError::MissingPart)?;

        let ms_time = ms_part
            .parse::<u64>()
            .map_err(|_| ParseStreamIdError::InvalidNumber)?;
        let seq = seq_part
            .parse::<u64>()
            .map_err(|_| ParseStreamIdError::InvalidNumber)?;
        Ok(XrangeStreamdId::Literal(StreamId { ms_time, seq }))
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(id: StreamId, fields: HashMap<String, String>) -> Self {
        Self { id, fields }
    }

    pub fn into_vec(self) -> Vec<HashMap<String, String>> {
        let mut vec = Vec::new();
        vec.push(self.fields);
        vec
    }
}

#[derive(Debug, Clone)]
pub enum Frame {
    // RESP Protocol Types
    /// Simple String (+)
    SimpleString(String),
    /// Simple Error (-)
    SimpleError(String),
    /// Integer (:)
    Integer(i64),
    /// Bulk String ($)
    BulkString(Bytes),
    /// Array (*)
    Array(Vec<Frame>),
    /// Null (_)
    Null,
    /// Null (_)
    NullArray,
    /// Boolean (#)
    Boolean(bool),
    /// Double (,)
    Double(f64),
    /// Big Number (()
    BigNumber(String),
    /// Bulk Error (!)
    BulkError(Vec<Frame>),
    /// Verbatim String (=)
    VerbatimString(Vec<Frame>),
    /// Map (%)
    Map(HashMap<String, Frame>),
    /// Stream
    Stream(Vec<StreamEntry>),
    /// Attribute (|)
    Attribute(Vec<Frame>),
    /// Set (~)
    Set(HashSet<String>),
    /// Push (>)
    Push(Vec<Frame>),

    // Redis Data Types (from RDB)
    /// Redis String
    RedisString(Bytes),
    /// Redis List
    RedisList(Vec<Bytes>),
    /// Redis Set
    RedisSet(HashSet<Bytes>),
    /// Redis Hash
    RedisHash(HashMap<Bytes, Bytes>),
}

impl Frame {
    /// Convert Frame to RESP bytes for network transmission
    pub fn to_resp_bytes(&self) -> Vec<u8> {
        match self {
            Frame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::BulkString(bytes) => {
                let len = bytes.len();
                let s = String::from_utf8_lossy(bytes.as_ref());
                format!("${}\r\n{}\r\n", len, s).into_bytes()
            }
            Frame::Array(arr) => {
                let len = arr.len();
                let mut result = format!("*{}\r\n", len).into_bytes();
                for element in arr {
                    result.extend(element.to_resp_bytes());
                }
                result
            }
            Frame::Null => b"$-1\r\n".to_vec(),
            Frame::NullArray => b"*-1\r\n".to_vec(),
            Frame::Boolean(b) => format!("#{}\r\n", if *b { "t" } else { "f" }).into_bytes(),
            Frame::Double(d) => format!(",{}\r\n", d).into_bytes(),
            Frame::BigNumber(n) => format!("({}\r\n", n).into_bytes(),
            Frame::BulkError(errors) => {
                // For now, just return the first error as a simple error
                if let Some(Frame::SimpleError(err)) = errors.first() {
                    format!("-{}\r\n", err).into_bytes()
                } else {
                    b"-ERR Bulk error\r\n".to_vec()
                }
            }
            Frame::VerbatimString(strings) => {
                // For now, just return the first string
                if let Some(Frame::BulkString(bytes)) = strings.first() {
                    let len = bytes.len();
                    let s = String::from_utf8_lossy(bytes.as_ref());
                    format!("${}\r\n{}\r\n", len, s).into_bytes()
                } else {
                    b"$0\r\n\r\n".to_vec()
                }
            }
            Frame::Map(map) => {
                let len = map.len();
                let mut result = format!("%{}\r\n", len).into_bytes();
                for (key, value) in map {
                    result.extend(
                        Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())).to_resp_bytes(),
                    );
                    result.extend(value.to_resp_bytes());
                }
                result
            }
            Frame::Stream(stream) => {
                let len = stream.len();
                let mut result = format!("*{}\r\n", len).into_bytes();

                for entry in stream.iter() {
                    let id = entry.id;
                    // Each stream entry is an array of two elements: [ id, [ field, value, ... ] ]
                    // Start the entry array with 2 elements
                    result.extend(format!("*2\r\n").into_bytes());

                    // 1) id as bulk string
                    let id_s = id.to_string();
                    let id_bytes = id_s.as_bytes();
                    result.extend(format!("${}\r\n", id_bytes.len()).into_bytes());
                    result.extend(id_bytes);
                    result.extend(b"\r\n");

                    // 2) fields as an array of alternating field/value bulk strings
                    let fields_count = entry.fields.len();
                    result.extend(format!("*{}\r\n", fields_count * 2).into_bytes());
                    for (field, value) in entry.fields.iter() {
                        result.extend(format!("${}\r\n", field.as_bytes().len()).into_bytes());
                        result.extend(field.as_bytes());
                        result.extend(b"\r\n");

                        result.extend(format!("${}\r\n", value.as_bytes().len()).into_bytes());
                        result.extend(value.as_bytes());
                        result.extend(b"\r\n");
                    }
                }

                result
            }
            Frame::Attribute(attrs) => {
                let len = attrs.len();
                let mut result = format!("|{}\r\n", len).into_bytes();
                for attr in attrs {
                    result.extend(attr.to_resp_bytes());
                }
                result
            }
            Frame::Set(set) => {
                let len = set.len();
                let mut result = format!("~{}\r\n", len).into_bytes();
                for item in set {
                    result.extend(
                        Frame::BulkString(Bytes::copy_from_slice(item.as_bytes())).to_resp_bytes(),
                    );
                }
                result
            }
            Frame::Push(items) => {
                let len = items.len();
                let mut result = format!(">{}\r\n", len).into_bytes();
                for item in items {
                    result.extend(item.to_resp_bytes());
                }
                result
            }
            // Redis data types - convert to appropriate RESP representation
            Frame::RedisString(bytes) => {
                let len = bytes.len();
                let s = String::from_utf8_lossy(bytes.as_ref());
                format!("${}\r\n{}\r\n", len, s).into_bytes()
            }
            Frame::RedisList(items) => {
                let len = items.len();
                let mut result = format!("*{}\r\n", len).into_bytes();
                for item in items {
                    let len = item.len();
                    let s = String::from_utf8_lossy(item.as_ref());
                    result.extend(format!("${}\r\n{}\r\n", len, s).into_bytes());
                }
                result
            }
            Frame::RedisSet(items) => {
                let len = items.len();
                let mut result = format!("~{}\r\n", len).into_bytes();
                for item in items {
                    let len = item.len();
                    let s = String::from_utf8_lossy(item.as_ref());
                    result.extend(format!("${}\r\n{}\r\n", len, s).into_bytes());
                }
                result
            }
            Frame::RedisHash(map) => {
                let len = map.len();
                let mut result = format!("%{}\r\n", len).into_bytes();
                for (key, value) in map {
                    let key_len = key.len();
                    let key_s = String::from_utf8_lossy(key.as_ref());
                    result.extend(format!("${}\r\n{}\r\n", key_len, key_s).into_bytes());

                    let value_len = value.len();
                    let value_s = String::from_utf8_lossy(value.as_ref());
                    result.extend(format!("${}\r\n{}\r\n", value_len, value_s).into_bytes());
                }
                result
            }
        }
    }

    /// Get the string representation of the frame for debugging
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Frame::SimpleString(s) => Some(s),
            Frame::BulkString(bytes) => std::str::from_utf8(bytes.as_ref()).ok(),
            Frame::RedisString(bytes) => std::str::from_utf8(bytes.as_ref()).ok(),
            _ => None,
        }
    }

    /// Get the integer value if this is an integer frame
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Frame::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get the boolean value if this is a boolean frame
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Frame::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get the array elements if this is an array frame
    pub fn as_array(&self) -> Option<&[Frame]> {
        match self {
            Frame::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Check if this frame represents null
    pub fn is_null(&self) -> bool {
        matches!(self, Frame::Null)
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::SimpleString(s) => write!(f, "{}", s),
            Frame::SimpleError(s) => write!(f, "{}", s),
            Frame::Integer(i) => write!(f, "{}", i),
            Frame::BulkString(bytes) => match std::str::from_utf8(bytes.as_ref()) {
                Ok(s) => write!(f, "{}", s),
                Err(_) => write!(f, "<binary data>"),
            },
            Frame::Array(arr) => {
                write!(f, "[")?;
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Frame::Stream(stream) => {
                write!(f, "Stream[")?;
                for (i, entry) in stream.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} => {:?}", entry.id, entry.fields)?;
                }
                write!(f, "]")
            }
            Frame::Null => write!(f, "(null)"),
            Frame::NullArray => write!(f, "(null_array)"),
            Frame::Boolean(b) => write!(f, "{}", b),
            Frame::Double(d) => write!(f, "{}", d),
            Frame::BigNumber(n) => write!(f, "{}", n),
            Frame::BulkError(errors) => write!(f, "BulkError({} errors)", errors.len()),
            Frame::VerbatimString(strings) => write!(f, "VerbatimString({} parts)", strings.len()),
            Frame::Map(map) => write!(f, "Map({} entries)", map.len()),
            Frame::Attribute(attrs) => write!(f, "Attribute({} attrs)", attrs.len()),
            Frame::Set(set) => write!(f, "Set({} items)", set.len()),
            Frame::Push(items) => write!(f, "Push({} items)", items.len()),
            Frame::RedisString(bytes) => match std::str::from_utf8(bytes.as_ref()) {
                Ok(s) => write!(f, "{}", s),
                Err(_) => write!(f, "<binary data>"),
            },
            Frame::RedisList(items) => write!(f, "List({} items)", items.len()),
            Frame::RedisSet(items) => write!(f, "RedisSet({} items)", items.len()),
            Frame::RedisHash(map) => write!(f, "Hash({} entries)", map.len()),
        }
    }
}

// Convenience constructors
impl Frame {
    pub fn simple_string(s: impl Into<String>) -> Self {
        Frame::SimpleString(s.into())
    }

    pub fn simple_error(s: impl Into<String>) -> Self {
        Frame::SimpleError(s.into())
    }

    pub fn integer(i: i64) -> Self {
        Frame::Integer(i)
    }

    pub fn bulk_string(bytes: impl Into<Bytes>) -> Self {
        Frame::BulkString(bytes.into())
    }

    pub fn array(items: Vec<Frame>) -> Self {
        Frame::Array(items)
    }

    pub fn boolean(b: bool) -> Self {
        Frame::Boolean(b)
    }

    pub fn double(d: f64) -> Self {
        Frame::Double(d)
    }

    pub fn null() -> Self {
        Frame::Null
    }

    pub fn redis_string(bytes: impl Into<Bytes>) -> Self {
        Frame::RedisString(bytes.into())
    }

    pub fn redis_list(items: Vec<Bytes>) -> Self {
        Frame::RedisList(items)
    }

    pub fn redis_set(items: HashSet<Bytes>) -> Self {
        Frame::RedisSet(items)
    }

    pub fn redis_hash(map: HashMap<Bytes, Bytes>) -> Self {
        Frame::RedisHash(map)
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Frame::SimpleString(a), Frame::SimpleString(b)) => a == b,
            (Frame::SimpleError(a), Frame::SimpleError(b)) => a == b,
            (Frame::Integer(a), Frame::Integer(b)) => a == b,
            (Frame::BulkString(a), Frame::BulkString(b)) => a == b,
            (Frame::Array(a), Frame::Array(b)) => a == b,
            (Frame::Null, Frame::Null) => true,
            (Frame::Boolean(a), Frame::Boolean(b)) => a == b,
            (Frame::Double(a), Frame::Double(b)) => a == b, // Note: This uses == which is fine for f64
            (Frame::BigNumber(a), Frame::BigNumber(b)) => a == b,
            (Frame::BulkError(a), Frame::BulkError(b)) => a == b,
            (Frame::VerbatimString(a), Frame::VerbatimString(b)) => a == b,
            (Frame::Map(a), Frame::Map(b)) => a == b,
            (Frame::Attribute(a), Frame::Attribute(b)) => a == b,
            (Frame::Set(a), Frame::Set(b)) => a == b,
            (Frame::Push(a), Frame::Push(b)) => a == b,
            (Frame::RedisString(a), Frame::RedisString(b)) => a == b,
            (Frame::RedisList(a), Frame::RedisList(b)) => a == b,
            (Frame::RedisSet(a), Frame::RedisSet(b)) => a == b,
            (Frame::RedisHash(a), Frame::RedisHash(b)) => a == b,
            _ => false,
        }
    }
}

impl From<rdb::RedisValue> for Frame {
    fn from(value: rdb::RedisValue) -> Self {
        match value {
            rdb::RedisValue::String(bytes) => Frame::RedisString(bytes),
            rdb::RedisValue::Integer(i) => Frame::Integer(i),
            rdb::RedisValue::List(items) => Frame::RedisList(items),
            rdb::RedisValue::Set(items) => Frame::RedisSet(items),
            rdb::RedisValue::Hash(map) => Frame::RedisHash(map),
        }
    }
}

impl TryFrom<Frame> for rdb::RedisValue {
    type Error = &'static str;

    fn try_from(value: Frame) -> Result<Self, Self::Error> {
        match value {
            Frame::RedisString(bytes) => Ok(rdb::RedisValue::String(bytes)),
            Frame::Integer(i) => Ok(rdb::RedisValue::Integer(i)),
            Frame::RedisList(items) => Ok(rdb::RedisValue::List(items)),
            Frame::RedisSet(items) => Ok(rdb::RedisValue::Set(items)),
            Frame::RedisHash(map) => Ok(rdb::RedisValue::Hash(map)),
            _ => Err("Cannot convert non-Redis data type Frame to RedisValue"),
        }
    }
}
