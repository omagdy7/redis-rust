use crate::{resp_parser::*, shared_cache::*};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                Some(now + (secs * 1000))
            }
            Some(ExpiryOption::Milliseconds(ms)) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                Some(now + ms)
            }
            Some(ExpiryOption::ExpiresAtSeconds(timestamp)) => Some(timestamp * 1000),
            Some(ExpiryOption::ExpiresAtMilliseconds(timestamp)) => Some(*timestamp),
            Some(ExpiryOption::KeepTtl) => None, // Handled specially
            None => None,
        }
    }
}

// Helper function to extract string from BulkString
fn extract_string(resp: &RespType) -> Option<String> {
    match resp {
        RespType::BulkString(bytes) => str::from_utf8(bytes).ok().map(|s| s.to_owned()),
        _ => None,
    }
}

// Helper function to parse u64 from BulkString
fn parse_u64(resp: &RespType) -> Option<u64> {
    extract_string(resp)?.parse().ok()
}

pub enum RedisCommands {
    PING,
    ECHO(String),
    GET(String),
    SET(SetCommand),
    Invalid,
}

impl RedisCommands {
    pub fn execute(self, cache: SharedCache) -> Vec<u8> {
        match self {
            RedisCommands::PING => resp!("PONG"),
            RedisCommands::ECHO(echo_string) => resp!(echo_string),
            RedisCommands::GET(key) => {
                let mut cache = cache.lock().unwrap();
                match cache.get(&key).cloned() {
                    Some(entry) => {
                        if entry.is_expired() {
                            cache.remove(&key); // Clean up expired key
                            resp!(null)
                        } else {
                            resp!(bulk entry.value)
                        }
                    }
                    None => resp!(null),
                }
            }
            RedisCommands::SET(command) => {
                let mut cache = cache.lock().unwrap();

                // Check conditions (NX/XX)
                let key_exists = cache.contains_key(&command.key);

                match command.condition {
                    Some(SetCondition::NotExists) if key_exists => {
                        return resp!(null); // Key exists, NX failed
                    }
                    Some(SetCondition::Exists) if !key_exists => {
                        return resp!(null); // Key doesn't exist, XX failed
                    }
                    _ => {} // No condition or condition met
                }

                let mut get_value: Option<String> = None;

                // Handle GET option
                if command.get_old_value {
                    match cache.get(&command.key) {
                        Some(val) => get_value = Some(val.value.clone()),
                        None => {}
                    }
                } else {
                }

                // Calculate expiry
                let expires_at = if let Some(ExpiryOption::KeepTtl) = command.expiry {
                    // Keep existing TTL
                    cache.get(&command.key).and_then(|e| e.expires_at)
                } else {
                    command.calculate_expiry_time()
                };

                // Set the value
                cache.insert(
                    command.key.clone(),
                    CacheEntry {
                        value: command.value.clone(),
                        expires_at,
                    },
                );

                if !command.get_old_value {
                    return resp!("OK");
                }

                match get_value {
                    Some(val) => return resp!(bulk val),
                    None => return resp!(null),
                }
            }
            RedisCommands::Invalid => todo!(),
        }
    }
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

impl From<RespType> for RedisCommands {
    fn from(value: RespType) -> Self {
        // Alternative approach using a more functional style with iterators
        let RespType::Array(command) = value else {
            return Self::Invalid;
        };

        let mut args = command.iter().filter_map(extract_string);

        let Some(cmd_name) = args.next() else {
            return Self::Invalid;
        };

        match cmd_name.to_ascii_uppercase().as_str() {
            "PING" => {
                if args.next().is_none() {
                    Self::PING
                } else {
                    Self::Invalid
                }
            }
            "ECHO" => match (args.next(), args.next()) {
                (Some(echo_string), None) => Self::ECHO(echo_string),
                _ => Self::Invalid,
            },
            "GET" => match (args.next(), args.next()) {
                (Some(key), None) => Self::GET(key),
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
                    Self::SET(SetCommand::new(key, value))
                } else {
                    let parser = SetOptionParser::new(key, value);
                    match parser.parse_options(&options) {
                        Ok(set_command) => Self::SET(set_command),
                        Err(_) => Self::Invalid,
                    }
                }
            }
            _ => Self::Invalid,
        }
    }
}

#[cfg(test)]
mod tests {}
