use bytes::Bytes;
use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
};

use crate::{rdb, stream::StreamEntry};

/// Wrapper for f64 that implements Ord for use in BTreeMap
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Score(pub f64);

impl Eq for Score {}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct GeoPosition {
    longitude: Score,
    latitude: Score,
}

fn spread_int32_to_int64(v: u32) -> u64 {
    // Ensure only lower 32 bits are non-zero.
    let v: u64 = v as u64 & 0xFFFFFFFF;

    // Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    let v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    let v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
    let v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
    let v = (v | (v << 2)) & 0x3333333333333333;
    let v = (v | (v << 1)) & 0x5555555555555555;

    return v as u64;
}
fn compact_int64_to_int32(v: u64) -> u32 {
    // Keep only the bits in even positions
    let v = v & 0x5555555555555555;

    // Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
    // After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32

    // Where w1, w2,..w31 are the digits from longitude if we're compacting latitude, or digits from latitude if we're compacting longitude
    // So, we mask them out and only keep the relevant bits that we wish to compact

    // ------
    // Reverse the spreading process by shifting and masking
    let v = (v | (v >> 1)) & 0x3333333333333333;
    let v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    let v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    let v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    let v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    // Before compacting: 0   v1  ...   0   v16  ... 0  v31  0  v32
    // After compacting: v1  v2  ...  v31  v32
    // -----

    return v as u32;
}
fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> (f64, f64) {
    const MIN_LATITUDE: f64 = -85.05112878;
    const MAX_LATITUDE: f64 = 85.05112878;
    const MIN_LONGITUDE: f64 = -180.0;
    const MAX_LONGITUDE: f64 = 180.0;

    const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
    const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2_f64;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2_f64;
    return (longitude, latitude);
}

fn interleave(x: u32, y: u32) -> u64 {
    // First, the values are spread from 32-bit to 64-bit integers.
    // This is done by inserting 32 zero bits in-between.
    //
    // Before spread: x1  x2  ...  x31  x32
    // After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
    let x = spread_int32_to_int64(x);
    let y = spread_int32_to_int64(y);

    // The y value is then shifted 1 bit to the left.
    // Before shift: 0   y1   0   y2 ... 0   y31   0   y32
    // After shift:  y1   0   y2 ... 0   y31   0   y32   0
    let y_shifted = y << 1;

    // Next, x and y_shifted are combined using a bitwise OR.
    //
    // Before bitwise OR (x): 0   x1   0   x2   ...  0   x31    0   x32
    // Before bitwise OR (y): y1  0    y2  0    ...  y31  0    y32   0
    // After bitwise OR     : y1  x2   y2  x2   ...  y31  x31  y32  x32
    return x | y_shifted;
}

impl GeoPosition {
    pub fn new(longitude: f64, latitude: f64) -> Self {
        Self {
            longitude: Score(longitude),
            latitude: Score(latitude),
        }
    }

    pub fn validate(&self) -> bool {
        const MIN_LATITUDE: f64 = -85.05112878;
        const MAX_LATITUDE: f64 = 85.05112878;
        const MIN_LONGITUDE: f64 = -180.0;
        const MAX_LONGITUDE: f64 = 180.0;

        self.longitude >= Score(MIN_LONGITUDE)
            && self.longitude <= Score(MAX_LONGITUDE)
            && self.latitude >= Score(MIN_LATITUDE)
            && self.latitude <= Score(MAX_LATITUDE)
    }

    pub fn calculate_score(&self) -> u64 {
        // Normalization
        const MIN_LATITUDE: f64 = -85.05112878;
        const MAX_LATITUDE: f64 = 85.05112878;
        const MIN_LONGITUDE: f64 = -180.0;
        const MAX_LONGITUDE: f64 = 180.0;

        const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
        const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

        let normalized_latitude =
            2_f64.powi(26) * (self.latitude.0 - MIN_LATITUDE) / LATITUDE_RANGE;
        let normalized_longitude =
            2_f64.powi(26) * (self.longitude.0 - MIN_LONGITUDE) / LONGITUDE_RANGE;

        // Truncation
        let normalized_latitude = normalized_latitude as u32;
        let normalized_longitude = normalized_longitude as u32;

        // Interleaving
        let score = interleave(normalized_latitude, normalized_longitude);
        score
    }

    pub fn decode_score(score: u64) -> (f64, f64) {
        // Step 1: Separating the Interleaved Bits

        // Extract longitude bits (they were shifted left by 1 during encoding)
        let y = score >> 1;
        // Extract latitude bits (they were in the original positions)
        let x = score;

        // Step 2: Compacting 64-bit integer to 32-bit integers

        // Compact both latitude and longitude back to 32-bit integers
        let grid_latitude_number = compact_int64_to_int32(x);
        let grid_longitude_number = compact_int64_to_int32(y);

        // Step 3: Converting Back to Geographic Coordinates
        convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
    }

    pub fn dist(&self, destination: GeoPosition) -> f64 {
        const R: f64 = 6372797.560856;

        let lat1 = self.latitude.0.to_radians();
        let lat2 = destination.latitude.0.to_radians();
        let d_lat = lat2 - lat1;
        let d_lon = (destination.longitude.0 - self.longitude.0).to_radians();

        let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
        let c = 2.0 * a.sqrt().asin();
        R * c
    }
}

impl Eq for GeoPosition {}

impl Ord for GeoPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.longitude.cmp(&other.longitude) {
            Ordering::Equal => self.latitude.cmp(&other.latitude),
            ord => ord,
        }
    }
}

/// Efficient sorted set implementation with fast member lookup
#[derive(Debug, Clone)]
pub struct SortedSet {
    /// Ordered set for ranking: (score, member) - sorted by score then lexicographically by member
    ordered: BTreeSet<(Score, String)>,
    /// Fast lookup: member -> score
    member_map: HashMap<String, Score>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            ordered: BTreeSet::new(),
            member_map: HashMap::new(),
        }
    }

    /// Add or update a member with score. Returns true if new member added, false if updated.
    pub fn insert(&mut self, score: f64, member: String) -> bool {
        let ordered_score = Score(score);
        let is_new = if let Some(old_score) = self.member_map.get(&member) {
            // Remove old entry
            self.ordered.remove(&(old_score.clone(), member.clone()));
            false
        } else {
            true
        };

        self.ordered.insert((ordered_score, member.clone()));
        self.member_map.insert(member, ordered_score);

        is_new
    }

    /// Remove a member. Returns true if removed.
    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.member_map.remove(member) {
            self.ordered.remove(&(score, member.to_string()));
            true
        } else {
            false
        }
    }

    /// Get score of a member
    pub fn score(&self, member: &str) -> Option<f64> {
        self.member_map.get(member).map(|s| s.0)
    }

    /// Get rank (0-based index) of a member
    pub fn rank(&self, member: &str) -> Option<usize> {
        self.member_map.get(member).map(|score| {
            self.ordered
                .range(..(score.clone(), member.to_string()))
                .count()
        })
    }

    /// Get cardinality
    pub fn len(&self) -> usize {
        self.ordered.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }

    /// Get members in range [start, end] inclusive, with optional scores
    pub fn range(&self, start: isize, end: isize, with_scores: bool) -> Vec<Frame> {
        let len = self.ordered.len() as isize;
        let mut start_idx = if start < 0 { len + start } else { start };
        let mut end_idx = if end < 0 { len + end } else { end };

        if start_idx < 0 {
            start_idx = 0;
        }
        if end_idx >= len {
            end_idx = len - 1;
        }
        if start_idx > end_idx {
            return vec![];
        }

        self.ordered
            .iter()
            .enumerate()
            .filter(|(i, _)| *i >= start_idx as usize && *i <= end_idx as usize)
            .flat_map(|(_, (score, member))| {
                if with_scores {
                    vec![
                        Frame::BulkString(Bytes::copy_from_slice(member.as_bytes())),
                        Frame::BulkString(Bytes::copy_from_slice(score.0.to_string().as_bytes())),
                    ]
                } else {
                    vec![Frame::BulkString(Bytes::copy_from_slice(member.as_bytes()))]
                }
            })
            .collect()
    }

    /// Search for members within radius from center point
    pub fn geo_search(
        &self,
        center_lon: f64,
        center_lat: f64,
        radius: f64,
        unit: &str,
    ) -> Vec<String> {
        let center = GeoPosition::new(center_lon, center_lat);
        let radius_m = match unit {
            "m" => radius,
            "km" => radius * 1000.0,
            "ft" => radius * 0.3048,
            "mi" => radius * 1609.344,
            _ => radius, // default m
        };
        self.member_map
            .iter()
            .filter_map(|(member, score)| {
                let (lon, lat) = GeoPosition::decode_score(score.0 as u64);
                let pos = GeoPosition::new(lon, lat);
                let dist = center.dist(pos);
                if dist <= radius_m {
                    Some(member.clone())
                } else {
                    None
                }
            })
            .collect()
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
    List(Vec<Frame>),
    /// Null (_)
    Null,
    /// Null (_)
    NullList,
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
    /// Set (~)
    Set(HashSet<String>),
    /// Sorted Set (z)
    SortedSet(SortedSet),
    /// Stream
    Stream(Vec<StreamEntry>),
    /// Attribute (|)
    Attribute(Vec<Frame>),
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

/// Find CRLF position in a byte slice
fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

impl Frame {
    /// Convert Frame to RESP bytes for network transmission
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            Frame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::BulkString(bytes) => {
                let len = bytes.len();
                let s = String::from_utf8_lossy(bytes.as_ref());
                format!("${}\r\n{}\r\n", len, s).into_bytes()
            }
            Frame::List(arr) => {
                let len = arr.len();
                let mut result = format!("*{}\r\n", len).into_bytes();
                for element in arr {
                    result.extend(element.to_resp());
                }
                result
            }
            Frame::Null => b"$-1\r\n".to_vec(),
            Frame::NullList => b"*-1\r\n".to_vec(),
            Frame::Boolean(b) => format!("#{}\r\n", if *b { "t" } else { "f" }).into_bytes(),
            Frame::Double(d) => format!(",{}\r\n", d).into_bytes(),
            Frame::BigNumber(n) => format!("({}\r\n", n).into_bytes(),
            Frame::BulkError(errors) => {
                if let Some(Frame::SimpleError(err)) = errors.first() {
                    format!("-{}\r\n", err).into_bytes()
                } else {
                    b"-ERR Bulk error\r\n".to_vec()
                }
            }
            Frame::VerbatimString(strings) => {
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
                        Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())).to_resp(),
                    );
                    result.extend(value.to_resp());
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
                    result.extend(attr.to_resp());
                }
                result
            }
            Frame::Set(set) => {
                let len = set.len();
                let mut result = format!("~{}\r\n", len).into_bytes();
                for item in set {
                    result.extend(
                        Frame::BulkString(Bytes::copy_from_slice(item.as_bytes())).to_resp(),
                    );
                }
                result
            }
            Frame::SortedSet(_) => b"-ERR SortedSet cannot be serialized\r\n".to_vec(),
            Frame::Push(items) => {
                let len = items.len();
                let mut result = format!(">{}\r\n", len).into_bytes();
                for item in items {
                    result.extend(item.to_resp());
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

    pub fn from_bytes(data: &[u8]) -> Result<(Frame, usize), String> {
        if data.is_empty() {
            return Err("Empty input".to_string());
        }

        match data[0] {
            b'+' => {
                // Simple String
                if let Some(pos) = find_crlf(&data[1..]) {
                    let s = String::from_utf8_lossy(&data[1..pos + 1]).to_string();
                    Ok((Frame::SimpleString(s), pos + 3)) // + and CRLF
                } else {
                    Err("Malformed SimpleString".to_string())
                }
            }
            b'-' => {
                // Simple Error
                if let Some(pos) = find_crlf(&data[1..]) {
                    let s = String::from_utf8_lossy(&data[1..pos + 1]).to_string();
                    Ok((Frame::SimpleError(s), pos + 3))
                } else {
                    Err("Malformed SimpleError".to_string())
                }
            }
            b':' => {
                // Integer
                if let Some(pos) = find_crlf(&data[1..]) {
                    let num_str = &data[1..pos + 1];
                    let i: i64 = String::from_utf8_lossy(num_str)
                        .parse()
                        .map_err(|_| "Invalid integer")?;
                    Ok((Frame::Integer(i), pos + 3))
                } else {
                    Err("Malformed Integer".to_string())
                }
            }
            b'$' => {
                // Bulk String
                if let Some(pos) = find_crlf(&data[1..]) {
                    let len_str = &data[1..pos + 1];
                    let len: isize = String::from_utf8_lossy(len_str)
                        .parse()
                        .map_err(|_| "Invalid bulk length")?;
                    let consumed = pos + 3;
                    if len == -1 {
                        Ok((Frame::Null, consumed))
                    } else {
                        let len = len as usize;
                        if data.len() < consumed + len + 2 {
                            return Err("Truncated bulk string".to_string());
                        }
                        let bulk = &data[consumed..consumed + len];
                        Ok((
                            Frame::BulkString(Bytes::copy_from_slice(bulk)),
                            consumed + len + 2,
                        ))
                    }
                } else {
                    Err("Malformed BulkString".to_string())
                }
            }
            b'*' => {
                // Array
                if let Some(pos) = find_crlf(&data[1..]) {
                    let len_str = &data[1..pos + 1];
                    let len: isize = String::from_utf8_lossy(len_str)
                        .parse()
                        .map_err(|_| "Invalid array length")?;
                    let mut consumed = pos + 3;
                    if len == -1 {
                        return Ok((Frame::NullList, consumed));
                    }
                    let len = len as usize;
                    let mut items = Vec::with_capacity(len);
                    for _ in 0..len {
                        let (frame, used) = Frame::from_bytes(&data[consumed..])?;
                        items.push(frame);
                        consumed += used;
                    }
                    Ok((Frame::List(items), consumed))
                } else {
                    Err("Malformed Array".to_string())
                }
            }
            b'#' => {
                // Boolean
                if data.len() < 3 {
                    return Err("Truncated boolean".to_string());
                }
                match data[1] {
                    b't' => Ok((Frame::Boolean(true), 3)),
                    b'f' => Ok((Frame::Boolean(false), 3)),
                    _ => Err("Invalid boolean".to_string()),
                }
            }
            _ => Err("Unsupported or unknown RESP type".to_string()),
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
            Frame::List(arr) => Some(arr),
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
            Frame::List(arr) => {
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
            Frame::NullList => write!(f, "(null_array)"),
            Frame::Boolean(b) => write!(f, "{}", b),
            Frame::Double(d) => write!(f, "{}", d),
            Frame::BigNumber(n) => write!(f, "{}", n),
            Frame::BulkError(errors) => write!(f, "BulkError({} errors)", errors.len()),
            Frame::VerbatimString(strings) => write!(f, "VerbatimString({} parts)", strings.len()),
            Frame::Map(map) => write!(f, "Map({} entries)", map.len()),
            Frame::Attribute(attrs) => write!(f, "Attribute({} attrs)", attrs.len()),
            Frame::Set(set) => write!(f, "Set({} items)", set.len()),
            Frame::SortedSet(s) => write!(f, "SortedSet({} entries)", s.len()),
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
        Frame::List(items)
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
            (Frame::List(a), Frame::List(b)) => a == b,
            (Frame::Null, Frame::Null) => true,
            (Frame::Boolean(a), Frame::Boolean(b)) => a == b,
            (Frame::Double(a), Frame::Double(b)) => a == b, // Note: This uses == which is fine for f64
            (Frame::BigNumber(a), Frame::BigNumber(b)) => a == b,
            (Frame::BulkError(a), Frame::BulkError(b)) => a == b,
            (Frame::VerbatimString(a), Frame::VerbatimString(b)) => a == b,
            (Frame::Map(a), Frame::Map(b)) => a == b,
            (Frame::Stream(_), Frame::Stream(_)) => false, // TODO: implement proper comparison
            (Frame::Attribute(a), Frame::Attribute(b)) => a == b,
            (Frame::Set(a), Frame::Set(b)) => a == b,
            (Frame::SortedSet(a), Frame::SortedSet(b)) => a.ordered == b.ordered,
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
