use crate::frame::{Frame, StreamId, XaddStreamId, XrangeStreamdId};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub enum SetCondition {
    /// NX - only set if key doesn't exists
    NotExists,
    /// XX - only set if key already exists
    Exists,
}

#[derive(Debug, Clone)]
pub enum ExpiryOption {
    /// EX seconds - expire in N seconds
    Seconds(u64),
    /// PX milliseconds - expire in N milliseconds  
    Milliseconds(u64),
    /// EXAT timestamp-seconds - expire at Unix timestamp (seconds)
    ExpiresAtSeconds(u64),
    /// PXAT timestamp-milliseconds - expire at Unix timestamp (milliseconds)
    ExpiresAtMilliseconds(u64),
    /// KEEPTTL - retain existing TTL
    KeepTtl,
}

/// Link: https://redis.io/docs/latest/commands/set/
/// Syntax:
/// -------
/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
///  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
///
/// Options:
/// --------
/// EX seconds -- Set the specified expire time, in seconds (a positive integer).
/// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
/// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
/// PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
/// NX -- Only set the key if it does not already exist.
/// XX -- Only set the key if it already exists.
/// KEEPTTL -- Retain the time to live associated with the key.
/// GET -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
#[derive(Debug, Clone)]
pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub condition: Option<SetCondition>,
    pub expiry: Option<ExpiryOption>,
    pub get_old_value: bool,
}

impl SetCommand {
    pub fn new(key: String, value: String) -> Self {
        Self {
            key,
            value,
            condition: None,
            expiry: None,
            get_old_value: false,
        }
    }

    pub fn with_condition(mut self, condition: Option<SetCondition>) -> Self {
        self.condition = condition;
        self
    }

    pub fn with_expiry(mut self, expiry: Option<ExpiryOption>) -> Self {
        self.expiry = expiry;
        self
    }

    pub fn with_get(mut self, value: bool) -> Self {
        self.get_old_value = value;
        self
    }

    /// Calculate the absolute expiry time in milliseconds since Unix epoch
    pub fn calculate_expiry_time(&self) -> Option<u64> {
        match &self.expiry {
            Some(ExpiryOption::Seconds(secs)) => {
                match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(duration) => {
                        let now = duration.as_millis() as u64;
                        Some(now + (secs * 1000))
                    }
                    Err(_) => None,
                }
            }
            Some(ExpiryOption::Milliseconds(ms)) => {
                match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(duration) => {
                        let now = duration.as_millis() as u64;
                        Some(now + ms)
                    }
                    Err(_) => None,
                }
            }
            Some(ExpiryOption::ExpiresAtSeconds(timestamp)) => Some(timestamp * 1000),
            Some(ExpiryOption::ExpiresAtMilliseconds(timestamp)) => Some(*timestamp),
            Some(ExpiryOption::KeepTtl) => None, // Keep existing TTL
            None => None,
        }
    }
}

// Helper function to extract string from BulkString
fn extract_string(resp: &Frame) -> Option<String> {
    match resp {
        Frame::BulkString(bytes) => str::from_utf8(bytes).ok().map(|s| s.to_owned()),
        _ => None,
    }
}

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Get(String),
    Set(SetCommand),
    Type(String),
    ConfigGet(String),
    Keys(String),
    Info(String),
    ReplConf((String, String)),
    Psync((String, String)),
    Wait((String, String)),
    Xadd {
        key: String,
        parsed_id: XaddStreamId,
        fields: HashMap<String, String>,
    },
    XRange {
        key: String,
        start: XrangeStreamdId,
        end: XrangeStreamdId,
    },
    XRead {
        keys: Vec<String>,
        stream_ids: Vec<StreamId>,
    },
    Invalid,
}

// Parser for SET command options
struct SetOptionParser {
    command: SetCommand,
}

impl SetOptionParser {
    fn new(key: String, value: String) -> Self {
        Self {
            command: SetCommand::new(key, value),
        }
    }

    fn parse_option(&mut self, option: &str, next_arg: Option<&str>) -> Result<bool, &'static str> {
        match option.to_ascii_uppercase().as_str() {
            "GET" => {
                self.command = self.command.clone().with_get(true);
                Ok(false) // doesn't consume next argument
            }
            "NX" => {
                self.command = self
                    .command
                    .clone()
                    .with_condition(Some(SetCondition::NotExists));
                Ok(false)
            }
            "XX" => {
                self.command = self
                    .command
                    .clone()
                    .with_condition(Some(SetCondition::Exists));
                Ok(false)
            }
            "KEEPTTL" => {
                self.command = self
                    .command
                    .clone()
                    .with_expiry(Some(ExpiryOption::KeepTtl));
                Ok(false)
            }
            "EX" => {
                let seconds = next_arg
                    .ok_or("EX requires a value")?
                    .parse::<u64>()
                    .map_err(|_| "Invalid EX value")?;
                self.command = self
                    .command
                    .clone()
                    .with_expiry(Some(ExpiryOption::Seconds(seconds)));
                Ok(true) // consumes next argument
            }
            "PX" => {
                let ms = next_arg
                    .ok_or("PX requires a value")?
                    .parse::<u64>()
                    .map_err(|_| "Invalid PX value")?;
                self.command = self
                    .command
                    .clone()
                    .with_expiry(Some(ExpiryOption::Milliseconds(ms)));
                Ok(true)
            }
            "EXAT" => {
                let timestamp = next_arg
                    .ok_or("EXAT requires a value")?
                    .parse::<u64>()
                    .map_err(|_| "Invalid EXAT value")?;
                self.command = self
                    .command
                    .clone()
                    .with_expiry(Some(ExpiryOption::ExpiresAtSeconds(timestamp)));
                Ok(true)
            }
            "PXAT" => {
                let timestamp = next_arg
                    .ok_or("PXAT requires a value")?
                    .parse::<u64>()
                    .map_err(|_| "Invalid PXAT value")?;
                self.command = self
                    .command
                    .clone()
                    .with_expiry(Some(ExpiryOption::ExpiresAtMilliseconds(timestamp)));
                Ok(true)
            }
            _ => Err("Unknown SET option"),
        }
    }

    fn parse_options(mut self, options: &[String]) -> Result<SetCommand, &'static str> {
        let mut i = 0;
        while i < options.len() {
            let option = &options[i];
            let next_arg = options.get(i + 1).map(|s| s.as_str());

            let consumes_next = self.parse_option(option, next_arg)?;
            i += if consumes_next { 2 } else { 1 };
        }
        Ok(self.command)
    }
}

impl From<Frame> for RedisCommand {
    fn from(value: Frame) -> Self {
        // Any command is a Frame::Array so we make sure it is
        let Frame::Array(command) = value else {
            return Self::Invalid;
        };

        let mut args = command.iter().filter_map(extract_string);

        let Some(cmd_name) = args.next() else {
            return Self::Invalid;
        };

        match cmd_name.to_ascii_uppercase().as_str() {
            "PING" => {
                if args.next().is_none() {
                    Self::Ping
                } else {
                    Self::Invalid
                }
            }
            "ECHO" => match (args.next(), args.next()) {
                (Some(echo_string), None) => Self::Echo(echo_string),
                _ => Self::Invalid,
            },
            "GET" => match (args.next(), args.next()) {
                (Some(key), None) => Self::Get(key),
                _ => Self::Invalid,
            },
            "SET" => {
                let Some(key) = args.next() else {
                    return Self::Invalid;
                };
                let Some(value) = args.next() else {
                    return Self::Invalid;
                };

                let options: Vec<String> = args.collect();

                if options.is_empty() {
                    Self::Set(SetCommand::new(key, value))
                } else {
                    let parser = SetOptionParser::new(key, value);
                    match parser.parse_options(&options) {
                        Ok(set_command) => Self::Set(set_command),
                        Err(_) => Self::Invalid,
                    }
                }
            }
            "XADD" => {
                let Some(key) = args.next() else {
                    return Self::Invalid;
                };
                let Some(stream_id) = args.next() else {
                    return Self::Invalid;
                };

                let parsed_id: XaddStreamId = stream_id.parse().unwrap();
                let mut fields: HashMap<String, String> = HashMap::new();
                while let (Some(field_key), Some(value)) = (args.next(), args.next()) {
                    fields.insert(field_key, value);
                }
                Self::Xadd {
                    key: key,
                    parsed_id,
                    fields,
                }
            }
            "XRANGE" => {
                let Some(key) = args.next() else {
                    return Self::Invalid;
                };
                let Some(start) = args.next() else {
                    return Self::Invalid;
                };
                let Some(end) = args.next() else {
                    return Self::Invalid;
                };

                let start_id: XrangeStreamdId = start.parse().unwrap();
                let end_id: XrangeStreamdId = end.parse().unwrap();

                Self::XRange {
                    key: key,
                    start: start_id,
                    end: end_id,
                }
            }
            "XREAD" => {
                // to consume the 'streams' literal
                let Some(_) = args.next() else {
                    return Self::Invalid;
                };

                let mut args: Vec<String> = args.collect();

                let i = args.partition_point(|element| element.parse::<StreamId>().is_err());
                let stream_ids: Vec<StreamId> = args
                    .split_off(i)
                    .iter()
                    .map(|s_id| s_id.parse().unwrap())
                    .collect();

                Self::XRead {
                    keys: args,
                    stream_ids,
                }
            }
            "TYPE" => match args.next() {
                Some(key) => Self::Type(key),
                _ => Self::Invalid,
            },
            "KEYS" => {
                let Some(query) = args.next() else {
                    return Self::Invalid;
                };
                Self::Keys(query)
            }
            "CONFIG" => {
                let Some(sub_command) = args.next() else {
                    return Self::Invalid;
                };
                let Some(key) = args.next() else {
                    return Self::Invalid;
                };
                if &sub_command.to_uppercase() == &"GET" {
                    return Self::ConfigGet(key);
                }
                Self::Invalid
            }
            "INFO" => {
                let Some(sub_command) = args.next() else {
                    return Self::Invalid;
                };
                if &sub_command.to_uppercase() == &"REPLICATION" {
                    return Self::Info(sub_command);
                }
                Self::Invalid
            }
            "REPLCONF" => {
                let Some(op1) = args.next() else {
                    return Self::Invalid;
                };
                let Some(op2) = args.next() else {
                    return Self::Invalid;
                };
                Self::ReplConf((op1, op2))
            }
            "WAIT" => {
                let Some(op1) = args.next() else {
                    return Self::Invalid;
                };
                let Some(op2) = args.next() else {
                    return Self::Invalid;
                };
                Self::Wait((op1, op2))
            }
            "PSYNC" => {
                let Some(repl_id) = args.next() else {
                    return Self::Invalid;
                };
                let Some(repl_offset) = args.next() else {
                    return Self::Invalid;
                };
                Self::Psync((repl_id, repl_offset))
            }
            _ => Self::Invalid,
        }
    }
}
