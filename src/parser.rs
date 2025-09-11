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

use crate::error::RespError;
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



// Helper function to find CRLF and split bytes
fn find_crlf(bytes: &[u8]) -> Result<(&[u8], &[u8]), RespError> {
    bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|pos| (&bytes[..pos], &bytes[pos + 2..]))
        .ok_or(RespError::UnexpectedEnd)
}

// Helper function to check the first byte
fn check_first_byte(bytes: &[u8], expected: u8, error: RespError) -> Result<&[u8], RespError> {
    match bytes {
        [first, rest @ ..] if *first == expected => Ok(rest),
        [] => Err(RespError::Custom(String::from("Empty data"))),
        _ => Err(error),
    }
}

pub fn parse_simple_strings(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, SIMPLE_STRING, RespError::WrongType)?;
    let (consumed, remained) = find_crlf(rest)?;

    if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
        return Err(RespError::InvalidValue);
    }

    let frame = Frame::SimpleString(String::from_utf8_lossy(consumed).to_string());
    Ok((frame, remained))
}

pub fn parse_simple_errors(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, SIMPLE_ERROR, RespError::InvalidDataType)?;
    let (consumed, remained) = find_crlf(rest)?;

    if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
        return Err(RespError::InvalidValue);
    }

    let frame = Frame::SimpleError(String::from_utf8_lossy(consumed).to_string());
    Ok((frame, remained))
}

pub fn parse_integers(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, INTEGER, RespError::InvalidDataType)?;
    let (consumed, remained) = find_crlf(rest)?;

    let parsed_int = String::from_utf8_lossy(consumed)
        .parse::<i64>()
        .map_err(|_| RespError::InvalidValue)?;
    let frame = Frame::Integer(parsed_int);
    Ok((frame, remained))
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
    let rest = check_first_byte(bytes, ARRAY, RespError::InvalidDataType)?;
    let (consumed, mut remained) = find_crlf(rest)?;

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

    Ok((Frame::Array(array), remained))
}

pub fn parse_bulk_strings(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, BULK_STRING, RespError::InvalidDataType)?;
    let (consumed, remained) = find_crlf(rest)?;

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

pub fn parse_nulls(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, NULL, RespError::WrongType)?;
    let (consumed, remained) = find_crlf(rest)?;

    if consumed.iter().any(|&byte| byte == b'\r' || byte == b'\n') {
        return Err(RespError::InvalidValue);
    }

    Ok((Frame::Null, remained))
}

pub fn parse_boolean(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, BOOLEAN, RespError::InvalidDataType)?;
    let (consumed, remained) = find_crlf(rest)?;

    let val = if consumed.len() == 1 {
        match consumed.first() {
            Some(b't') => true,
            Some(b'f') => false,
            _ => return Err(RespError::InvalidValue),
        }
    } else {
        return Err(RespError::UnexpectedEnd);
    };

    Ok((Frame::Boolean(val), remained))
}

pub fn parse_doubles(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, DOUBLES, RespError::InvalidDataType)?;
    let (consumed, remained) = find_crlf(rest)?;

    let parsed_double = String::from_utf8_lossy(consumed)
        .parse::<f64>()
        .map_err(|_| RespError::InvalidValue)?;
    let frame = Frame::Double(parsed_double);
    Ok((frame, remained))
}

pub fn parse_maps(bytes: &[u8]) -> Result<(Frame, &[u8]), RespError> {
    let rest = check_first_byte(bytes, MAPS, RespError::InvalidDataType)?;

    // this would consume the <digit>\r\n
    let (consumed, mut remained) = find_crlf(rest)?;

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

    // I need this because if the user sent the same key it should override and the check
    // for unexpected end fails because it would expect the length of the map that was intended by length variable
    if map.len() != key_set.len() {
        return Err(RespError::UnexpectedEnd);
    }

    Ok((Frame::Map(map), remained))
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
