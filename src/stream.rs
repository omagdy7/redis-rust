use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

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

#[derive(Debug, Clone)]
pub enum XReadStreamId {
    Literal(StreamId),
    Latest,
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

impl FromStr for XReadStreamId {
    type Err = ParseStreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "$" {
            return Ok(XReadStreamId::Latest);
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
        Ok(XReadStreamId::Literal(StreamId { ms_time, seq }))
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
