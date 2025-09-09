// Helpful resource
// https://rdb.fnordig.de/file_format.html#zipmap-encoding
#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io, isize,
    path::Path,
};

use bytes::Bytes;
use thiserror::Error;

use crate::resp_commands::ExpiryOption;

/// Represents any possible value that a key can hold in Redis.
///
/// Note: All "string" elements are represented as `Bytes` because
/// Redis strings are binary-safe and not guaranteed to be valid UTF-8.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisValue {
    /// The STRING type.
    String(Bytes),

    /// The Integer type.
    Integer(i64),

    /// The LIST type. An ordered collection of strings.
    List(Vec<Bytes>),

    /// The SET type. An unordered collection of unique strings.
    Set(HashSet<Bytes>),

    /// The HASH type. A collection of field-value pairs.
    Hash(HashMap<Bytes, Bytes>),
}

impl RedisValue {
    /// Convert RedisValue to bytes using Redis string encoding format
    pub fn to_bytes(&self) -> Result<Vec<u8>, RdbError> {
        match self {
            RedisValue::String(data) => encode_string(data.as_ref()),
            RedisValue::Integer(value) => encode_integer(*value),
            RedisValue::List(items) => {
                let mut result = encode_length(items.len())?;
                // For lists, we'd typically encode each item separately
                // This is a simplified version that just encodes the count
                for item in items {
                    result.extend(encode_string(item.as_ref())?);
                }
                Ok(result)
            }
            RedisValue::Set(items) => {
                let mut result = encode_length(items.len())?;
                for item in items {
                    result.extend(encode_string(item.as_ref())?);
                }
                Ok(result)
            }
            RedisValue::Hash(map) => {
                let mut result = encode_length(map.len())?;
                for (key, value) in map {
                    result.extend(encode_string(key.as_ref())?);
                    result.extend(encode_string(value.as_ref())?);
                }
                Ok(result)
            }
        }
    }
}

/// Encode a string using Redis length encoding + raw bytes
fn encode_string(data: &[u8]) -> Result<Vec<u8>, RdbError> {
    let mut result = encode_length(data.len())?;
    result.extend_from_slice(data);
    Ok(result)
}

/// Encode an integer using Redis special encoding if possible, otherwise as string
fn encode_integer(value: i64) -> Result<Vec<u8>, RdbError> {
    // Try to use special integer encodings for efficiency
    if value >= i8::MIN as i64 && value <= i8::MAX as i64 {
        // 8-bit integer encoding: 0b11000000 (0xC0) followed by 1 byte
        Ok(vec![0xC0, value as u8])
    } else if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
        // 16-bit integer encoding: 0b11000001 (0xC1) followed by 2 bytes
        let bytes = (value as i16).to_be_bytes();
        Ok(vec![0xC1, bytes[0], bytes[1]])
    } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
        // 32-bit integer encoding: 0b11000010 (0xC2) followed by 4 bytes
        let bytes = (value as i32).to_be_bytes();
        Ok(vec![0xC2, bytes[0], bytes[1], bytes[2], bytes[3]])
    } else {
        // For very large integers, encode as string
        let string_repr = value.to_string();
        encode_string(string_repr.as_bytes())
    }
}

/// Encode length using Redis length encoding format
fn encode_length(len: usize) -> Result<Vec<u8>, RdbError> {
    if len < 64 {
        // 6-bit length (0-63): 0b00xxxxxx
        Ok(vec![len as u8])
    } else if len < 16384 {
        // 14-bit length: 0b01xxxxxx xxxxxxxx
        let first_byte = 0x40 | ((len >> 8) as u8);
        let second_byte = (len & 0xFF) as u8;
        Ok(vec![first_byte, second_byte])
    } else if len <= u32::MAX as usize {
        // 32-bit length: 0b10000000 followed by 4 bytes big-endian
        let bytes = (len as u32).to_be_bytes();
        Ok(vec![0x80, bytes[0], bytes[1], bytes[2], bytes[3]])
    } else {
        Err(RdbError::InvalidLength { length: len })
    }
}

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

// Custom parsing trait that returns bytes consumed
pub trait FromBytes: Sized {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), ParseError>;
}

#[derive(Debug, Copy, Clone)]
pub enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    SortedSet = 3,
    Hash = 4,
    Zipmap = 9,
    Ziplist = 10,
    Intset = 11,
    SortedSetInZiplist = 12,
    HashmapInZiplist = 13,
    ListInQuicklist = 14,
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::String),
            1 => Ok(ValueType::List),
            2 => Ok(ValueType::Set),
            3 => Ok(ValueType::SortedSet),
            4 => Ok(ValueType::Hash),
            9 => Ok(ValueType::Zipmap),
            10 => Ok(ValueType::Ziplist),
            11 => Ok(ValueType::Intset),
            12 => Ok(ValueType::SortedSetInZiplist),
            13 => Ok(ValueType::HashmapInZiplist),
            14 => Ok(ValueType::ListInQuicklist),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct KeyExpiry {
    pub timestamp: u64,
    pub unit: ExpiryUnit,
}

#[derive(Debug)]
pub enum ExpiryUnit {
    Seconds,
    Milliseconds,
}

#[derive(Debug)]
pub struct DatabaseEntry {
    pub expiry: Option<KeyExpiry>,
    pub value_type: ValueType,
    pub value: RedisValue,
}

type LengthEncoding = usize;
type BytesConsumed = usize;

/// Parses a Redis length-encoded integer.
fn parse_length(bytes: &[u8]) -> Result<(usize, usize), RdbError> {
    let (first_byte, mut rest) = bytes.split_at_checked(1).ok_or(RdbError::UnexpectedEof)?;
    let mut consumed = 1;

    match first_byte[0] >> 6 {
        0b00 => {
            // 6-bit length
            // 0x3F = 0011 1111
            let len = (first_byte[0] & 0x3F) as usize;
            Ok((len, consumed))
        }
        0b01 => {
            // 14-bit length
            let (second_byte, _) = rest.split_at_checked(1).ok_or(RdbError::UnexpectedEof)?;
            consumed += 1;

            // We need to get the first last 6 bits(most right bits) of the first byte and then the
            // whole next byte which can be done by a trivial so we mask out the first 6 bits and
            // then shift them left by a byte and oring the second byte (6 + 8) = our 14-bit length
            let len = (((first_byte[0] & 0x3F) as usize) << 8) | (second_byte[0] as usize);
            Ok((len, consumed))
        }
        0b10 => {
            // 32-bit length from next 4 bytes
            let (len_bytes, _) = rest.split_at_checked(4).ok_or(RdbError::UnexpectedEof)?;
            consumed += 4;

            // pretty straight forward just ignore the first byte and interpret the next 4 bytes as a u32
            let len = u32::from_be_bytes(
                len_bytes.try_into().map_err(|_| RdbError::ArrayConversionError {
                    message: "Failed to convert slice to array for u32".to_string()
                })?
            ) as usize;
            Ok((len, consumed))
        }
        0b11 => {
            // Special format, not a length
            Err(RdbError::InvalidLength { length: first_byte[0] as usize })
        }
        _ => unreachable!(),
    }
}

fn parse_special_length(
    special_type: u8,
    bytes: &[u8],
    bytes_consumed: &mut usize,
) -> Result<(RedisValue, usize), RdbError> {
    match special_type {
        0 => {
            let (int_bytes, _) = bytes.split_at_checked(1).ok_or(RdbError::UnexpectedEof)?;
            *bytes_consumed += 1;
            Ok((RedisValue::Integer(int_bytes[0] as i64), *bytes_consumed))
        }
        1 => {
            let (int_bytes, _) = bytes.split_at_checked(2).ok_or(RdbError::UnexpectedEof)?;
            *bytes_consumed += 2;
            let value = i16::from_be_bytes([int_bytes[0], int_bytes[1]]) as i64;
            Ok((RedisValue::Integer(value), *bytes_consumed))
        }
        2 => {
            let (int_bytes, _) = bytes.split_at_checked(4).ok_or(RdbError::UnexpectedEof)?;
            *bytes_consumed += 4;
            let value =
                i32::from_be_bytes([int_bytes[0], int_bytes[1], int_bytes[2], int_bytes[3]]) as i64;
            Ok((RedisValue::Integer(value), *bytes_consumed))
        }
        _ => Err(RdbError::InvalidLength { length: special_type as usize }),
    }
}

impl FromBytes for RedisValue {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), ParseError> {
        let (length_bytes, rest) = bytes.split_at_checked(1).ok_or(ParseError::UnexpectedEof)?;
        let mut bytes_consumed = 1;

        if length_bytes[0] >> 6 == 0b11 {
            // Special encoding
            let special_type = length_bytes[0] & 0x3F;
            return parse_special_length(special_type, rest, &mut bytes_consumed);
        } else {
            // It's a string, use our new helper
            let (len, len_consumed) = parse_length(bytes)?;
            let (string_bytes, _) = bytes[len_consumed..]
                .split_at_checked(len)
                .ok_or(ParseError::UnexpectedEof)?;

            let total_consumed = len_consumed + len;
            Ok((RedisValue::String(Bytes::copy_from_slice(string_bytes)), total_consumed))
        }
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct RDBHeader {
    pub magic_number: [u8; 5],
    pub version: [u8; 4],
}

impl FromBytes for RDBHeader {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), RdbError> {
        let mut rdb_header = RDBHeader::default();

        // first 5 bytes should match the magic number
        let (magic_number, rest) = bytes.split_at_checked(5).ok_or(RdbError::UnexpectedEof)?;

        if magic_number != b"REDIS" {
            return Err(RdbError::InvalidMagicNumber);
        }
        rdb_header.magic_number = *b"REDIS";

        // The following 4 bytes should match the version number
        let (version, _) = rest.split_at_checked(4).ok_or(RdbError::UnexpectedEof)?;

        let version_str = std::str::from_utf8(version)?;
        if ("0001" <= version_str) && (version_str <= "0011") {
            rdb_header.version = version.try_into().map_err(|_| RdbError::ArrayConversionError {
                message: "Failed to convert version bytes to array".to_string()
            })?;
        } else {
            return Err(RdbError::InvalidVersion { version: version_str.to_string() });
        }

        Ok((rdb_header, 9))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RDBMetaData {
    pub metadata: HashMap<Bytes, Bytes>,
}

impl FromBytes for RDBMetaData {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), RdbError> {
        let mut metadata = HashMap::new();
        let mut remaining = bytes;
        let mut total_consumed = 0;

        // Keep parsing AUX entries until we hit a non-AUX byte
        loop {
            let (aux_byte, rest) = remaining
                .split_at_checked(1)
                .ok_or(ParseError::UnexpectedEof)?;

            if aux_byte[0] != RDBFile::AUX {
                // Hit a non-AUX byte, we're done with metadata
                break;
            }

            remaining = rest;
            total_consumed += 1;

            // Parse key string
            let (key, key_consumed) = RedisValue::from_bytes(remaining)?;
            remaining = &remaining[key_consumed..];
            total_consumed += key_consumed;

            // Parse value string
            let (value, value_consumed) = RedisValue::from_bytes(remaining)?;
            remaining = &remaining[value_consumed..];
            total_consumed += value_consumed;

            let key_data = match key {
                RedisValue::String(data) => data,
                RedisValue::Integer(data) => Bytes::copy_from_slice(data.to_string().as_bytes()),
                _ => return Err(RdbError::InvalidMetadata),
            };

            let value_data = match value {
                RedisValue::String(data) => data,
                RedisValue::Integer(data) => Bytes::copy_from_slice(data.to_string().as_bytes()),
                _ => return Err(RdbError::InvalidMetadata),
            };

            metadata.insert(key_data, value_data);
        }

        Ok((RDBMetaData { metadata }, total_consumed))
    }
}

#[derive(Debug, Default)]
pub struct HashTableSizeInfo {
    pub hash_table_size: usize,
    pub expired_hash_table_size: usize,
}

pub type DatabaseIndex = usize;

#[derive(Debug)]
pub struct RDBDatabase {
    pub database_index: DatabaseIndex,
    pub size_hints: HashTableSizeInfo,
    pub hash_table: HashMap<Bytes, DatabaseEntry>,
}

fn parse_db_key_value(
    remaining: &mut &[u8],
    total_consumed: &mut usize,
    expiry: Option<KeyExpiry>,
    value_type: ValueType,
    hash_table: &mut HashMap<Bytes, DatabaseEntry>,
) -> Result<(), RdbError> {
    // Parse key string
    let (key, key_consumed) = RedisValue::from_bytes(remaining)?;
    *remaining = &remaining[key_consumed..];
    *total_consumed += key_consumed;

    // Parse value string
    let (value, value_consumed) = RedisValue::from_bytes(remaining)?;
    *remaining = &remaining[value_consumed..];
    *total_consumed += value_consumed;

    let database_entry = DatabaseEntry {
        expiry: expiry,
        value_type: ValueType::String,
        value: value,
    };

    let key_data = match key {
        RedisValue::String(data) => data,
        RedisValue::Integer(data) => Bytes::copy_from_slice(data.to_string().as_bytes()),
        _ => return Err(RdbError::InvalidMetadata),
    };

    hash_table.insert(key_data, database_entry);

    Ok(())
}

impl FromBytes for RDBDatabase {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), RdbError> {
        let mut hash_table: HashMap<Bytes, DatabaseEntry> = HashMap::new();
        let mut remaining = bytes;
        let mut size_hints = HashTableSizeInfo::default();
        let mut total_consumed = 0;
        let mut database_index = 0;

        // Keep parsing db entries until we hit a EOF byte
        loop {
            let (next_byte, rest) = remaining
                .split_at_checked(1)
                .ok_or(ParseError::UnexpectedEof)?;

            if next_byte[0] == RDBFile::EOF {
                break;
            }

            remaining = rest;
            total_consumed += 1;

            match next_byte[0] {
                RDBFile::SELECT_DB => {
                    let (index, consumed) = parse_length(remaining)?;
                    total_consumed += consumed;
                    remaining = &remaining[consumed..];

                    database_index = index;
                }
                RDBFile::RESIZE_DB => {
                    let (len, consumed) = parse_length(remaining)?;
                    total_consumed += consumed;
                    remaining = &remaining[consumed..];

                    size_hints.hash_table_size = len;

                    let (len, consumed) = parse_length(remaining)?;
                    total_consumed += consumed;
                    remaining = &remaining[consumed..];

                    size_hints.expired_hash_table_size = len;
                }
                RDBFile::EXPIRE_TIME_MS => {
                    let (timestamp_bytes, rest) = remaining
                        .split_at_checked(8)
                        .ok_or(RdbError::UnexpectedEof)?;

                    remaining = rest;
                    total_consumed += 8;

                    let timestamp = u64::from_le_bytes(
                        timestamp_bytes[0..8]
                            .try_into()
                            .map_err(|_| RdbError::ArrayConversionError {
                                message: "Failed to convert timestamp bytes to array".to_string()
                            })?,
                    );

                    let (value_type_byte, rest) = remaining
                        .split_at_checked(1)
                        .ok_or(RdbError::UnexpectedEof)?;

                    remaining = rest;
                    total_consumed += 1;

                    let expiry = Some(KeyExpiry {
                        timestamp,
                        unit: ExpiryUnit::Milliseconds,
                    });

                    match ValueType::try_from(value_type_byte[0]).map_err(|_| RdbError::ValueTypeConversion { value: value_type_byte[0] })? {
                        ValueType::String => {
                            parse_db_key_value(
                                &mut remaining,
                                &mut total_consumed,
                                expiry,
                                ValueType::String,
                                &mut hash_table,
                            )?;
                        }
                        _ => unreachable!(),
                    }
                }
                RDBFile::EXPIRE_TIME => {
                    let (timestamp_bytes, rest) = remaining
                        .split_at_checked(4)
                        .ok_or(RdbError::UnexpectedEof)?;

                    remaining = rest;
                    total_consumed += 4;

                    let timestamp = u32::from_le_bytes(
                        timestamp_bytes[0..4]
                            .try_into()
                            .map_err(|_| RdbError::ArrayConversionError {
                                message: "Failed to convert timestamp bytes to array".to_string()
                            })?,
                    ) as u64;

                    let (value_type_byte, rest) = remaining
                        .split_at_checked(1)
                        .ok_or(RdbError::UnexpectedEof)?;

                    remaining = rest;
                    total_consumed += 1;

                    let expiry = Some(KeyExpiry {
                        timestamp,
                        unit: ExpiryUnit::Seconds,
                    });

                    match ValueType::try_from(value_type_byte[0]).map_err(|_| RdbError::ValueTypeConversion { value: value_type_byte[0] })? {
                        ValueType::String => {
                            parse_db_key_value(
                                &mut remaining,
                                &mut total_consumed,
                                expiry,
                                ValueType::String,
                                &mut hash_table,
                            )?;
                        }
                        _ => unreachable!(),
                    }
                }
                n @ 0..15 => match ValueType::try_from(n as u8).map_err(|_| RdbError::ValueTypeConversion { value: n as u8 })? {
                    ValueType::String => {
                        parse_db_key_value(
                            &mut remaining,
                            &mut total_consumed,
                            None,
                            ValueType::String,
                            &mut hash_table,
                        )?;
                    }
                    _ => unreachable!(),
                },
                _ => break,
            }
        }

        Ok((
            RDBDatabase {
                database_index,
                size_hints,
                hash_table,
            },
            total_consumed,
        ))
    }
}

#[derive(Debug)]
pub struct RDBFile {
    pub header: RDBHeader,
    pub metadata: Option<RDBMetaData>,
    pub databases: HashMap<DatabaseIndex, RDBDatabase>,
    pub checksum: u64,
}

impl FromBytes for RDBFile {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), RdbError> {
        let mut remaining = bytes;
        let mut total_consumed = 0;
        let mut databases = HashMap::new();

        // special case when rdb file is sent by the master to replicas in the following format
        // $<length>/r/n<bytes_of_length_length>
        if bytes[0] == '$' as u8 {
            // consume up to the CRLF
            let pos = bytes
                .windows(2)
                .position(|window| window == b"\r\n")
                .ok_or(ParseError::UnexpectedEof)?;
            remaining = &bytes[pos + 2..];
            total_consumed += pos + 2; // include CRLF
        }

        // 1. Parse the RDB header ("REDIS" + version)
        let (header, consumed) = RDBHeader::from_bytes(remaining)?;
        total_consumed += consumed;
        remaining = &remaining[consumed..];

        // 2. Parse metadata (any AUX key-value fields)
        // Your RDBMetaData::from_bytes implementation correctly handles this by
        // consuming all sequential AUX fields until it hits another opcode.
        let (metadata, consumed) = RDBMetaData::from_bytes(remaining)?;
        total_consumed += consumed;
        remaining = &remaining[consumed..];

        // 3. Parse database sections
        // The provided RDBDatabase::from_bytes is designed to parse the entire
        // key-value section, including multiple DB selectors, until the final EOF.
        let (database_section, consumed) = RDBDatabase::from_bytes(remaining)?;
        total_consumed += consumed;
        remaining = &remaining[consumed..];
        databases.insert(database_section.database_index, database_section);

        // 4. Parse the final EOF marker and checksum
        // The RDBDatabase parser stops when it sees the EOF marker but doesn't consume it.
        // We must consume it here.
        let (eof_byte, rest) = remaining
            .split_at_checked(1)
            .ok_or(RdbError::UnexpectedEof)?;
        if eof_byte[0] != Self::EOF {
            return Err(RdbError::InvalidMetadata); // Expected EOF marker
        }
        total_consumed += 1;
        remaining = rest;

        // The final 8 bytes of a valid RDB file are the checksum.
        if remaining.len() >= 8 {
            let (checksum_bytes, _) = remaining
                .split_at_checked(8)
                .ok_or(RdbError::UnexpectedEof)?;
            let checksum = u64::from_le_bytes(
                checksum_bytes.try_into().map_err(|_| RdbError::ArrayConversionError {
                    message: "Failed to convert checksum bytes to array".to_string()
                })?
            );
            total_consumed += 8;

            let rdb_file = RDBFile {
                header,
                metadata: Some(metadata),
                databases,
                checksum,
            };
            Ok((rdb_file, total_consumed))
        } else {
            Err(ParseError::UnexpectedEof)
        }
    }
}

impl RDBFile {
    pub fn read(dir: String, dbfilename: String) -> Result<Option<Self>, RdbError> {
        let dir = Path::new(&dir);
        let file_path = dir.join(dbfilename);

        // Read file to bytes
        if file_path.exists() {
            let bytes = fs::read(&file_path)?;
            let (rdb_file, consumed) = RDBFile::from_bytes(&bytes)?;

            // sanity check
            assert!(bytes.len() == consumed);

            Ok(Some(rdb_file))
        } else {
            File::create(file_path)?;
            Ok(None)
        }
    }
}

impl RDBFile {
    /// The file starts off with the magic string “REDIS”. This is a quick sanity check to know we are dealing with a redis rdb file.
    pub const MAGIC_NUMBER: [u8; 5] = *b"REDIS";

    /// End of the RDB File
    pub const EOF: u8 = 0xFF;

    /// Database Selector
    pub const SELECT_DB: u8 = 0xFE;

    /// Expire time in seconds
    pub const EXPIRE_TIME: u8 = 0xFD;

    /// Expire time in milliseconds
    pub const EXPIRE_TIME_MS: u8 = 0xFC;

    /// Hash table sizes for the main keyspace and expires
    pub const RESIZE_DB: u8 = 0xFB;

    /// Auxiliray fields. Arbitrary key-value settings
    pub const AUX: u8 = 0xFA;
}
