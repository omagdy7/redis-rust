use crate::{resp_parser::*, SharedCache};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[macro_export]
macro_rules! resp {
    // Null: resp!(null)
    (null) => {
        $crate::RespType::Null().to_resp_bytes()
    };

    // Simple String: resp!("PONG") or resp!(simple "PONG")
    (simple $s:expr) => {
        $crate::RespType::SimpleString($s.to_string()).to_resp_bytes()
    };
    ($s:expr) => {
        $crate::RespType::SimpleString($s.to_string()).to_resp_bytes()
    };

    // Simple Error: resp!(error "ERR message")
    (error $s:expr) => {
        $crate::RespType::SimpleError($s.to_string()).to_resp_bytes()
    };

    // Integer: resp!(int 123)
    (int $i:expr) => {
        $crate::RespType::Integer($i).to_resp_bytes()
    };

    // Bulk String: resp!(bulk "hello") or resp!(bulk vec![104, 101, 108, 108, 111])
    (bulk $s:expr) => {
        $crate::RespType::BulkString($s.into()).to_resp_bytes()
    };

    // Array: resp!(array [resp!("one"), resp!(int 2)])
    (array [$($elem:expr),*]) => {
        $crate::RespType::Array(vec![$($elem),*]).to_resp_bytes()
    };

    // Boolean: resp!(bool true)
    (bool $b:expr) => {
        $crate::RespType::Boolean($b).to_resp_bytes()
    };

    // Double: resp!(double 3.14)
    (double $d:expr) => {
        $crate::RespType::Doubles($d).to_resp_bytes()
    };

    // Big Number: resp!(bignumber "123456789")
    (bignumber $n:expr) => {
        $crate::RespType::BigNumbers($n.to_string()).to_resp_bytes()
    };

    // Bulk Error: resp!(bulkerror [resp!("err1"), resp!("err2")])
    (bulkerror [$($elem:expr),*]) => {
        $crate::RespType::BulkErrors(vec![$($elem),*]).to_resp_bytes()
    };

    // Verbatim String: resp!(verbatim [resp!("txt"), resp!("example")])
    (verbatim [$($elem:expr),*]) => {
        $crate::RespType::VerbatimStrings(vec![$($elem),*]).to_resp_bytes()
    };

    // Map: resp!(map {resp!("key") => resp!("value")})
    (map {$($key:expr => $value:expr),*}) => {
        $crate::RespType::Maps({
            let mut map = HashMap::new();
            $(map.insert($key, $value);)*
            map
        }).to_resp_bytes()
    };

    // Attributes: resp!(attributes [resp!("key"), resp!("value")])
    (attributes [$($elem:expr),*]) => {
        $crate::RespType::Attributes(vec![$($elem),*]).to_resp_bytes()
    };

    // Set: resp!(set [resp!("one"), resp!("two")])
    (set [$($elem:expr),*]) => {
        $crate::RespType::Sets({
            let mut set = HashSet::new();
            $(set.insert($elem);)*
            set
        }).to_resp_bytes()
    };

    // Push: resp!(push [resp!("event"), resp!("data")])
    (push [$($elem:expr),*]) => {
        $crate::RespType::Pushes(vec![$($elem),*]).to_resp_bytes()
    };
}

enum SetCondition {
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

pub struct SetCommand {
    key: String,
    value: String,
    condition: Option<SetCondition>,
    expiry: Option<ExpiryOption>,
    get_old_value: bool,
}

#[derive(Debug, Clone)]
pub enum SetResult {
    /// Key was set successfully
    Ok,
    /// Key was set and old value returned (when GET option used)
    OkWithOldValue(String),
    /// Operation aborted due to condition (NX/XX conflict)
    Aborted,
    /// GET option used but key didn't exist
    AbortedNoOldValue,
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

    pub fn with_condition(mut self, condition: SetCondition) -> Self {
        self.condition = Some(condition);
        self
    }

    pub fn with_expiry(mut self, expiry: ExpiryOption) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn with_get(mut self) -> Self {
        self.get_old_value = true;
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
                let cache = cache.lock().unwrap();
                match cache.get(&key).cloned() {
                    Some(val) => resp!(val),
                    None => resp!(null),
                }
            }
            RedisCommands::SET(command) => {
                let mut cache = cache.lock().unwrap();
                cache.insert(command.key.clone(), command.value.clone());
                resp!("OK")
            }
            RedisCommands::Invalid => todo!(),
        }
    }
}

impl From<RespType> for RedisCommands {
    fn from(value: RespType) -> Self {
        match value {
            RespType::Array(command) => {
                let length = command.len();
                match length {
                    // Probably PING
                    1 => {
                        if let RespType::BulkString(command_name) = command[0].clone() {
                            if command_name == b"PING" {
                                return Self::PING;
                            } else {
                                // TODO: Handle the case where it's another command with
                                // insufficient arugments
                                return Self::Invalid;
                            }
                        }
                        return Self::Invalid;
                    }
                    // Probably GET or ECHO
                    2 => {
                        if let (RespType::BulkString(command_name), RespType::BulkString(key)) =
                            (command[0].clone(), command[1].clone())
                        {
                            if command_name == b"GET" {
                                return Self::GET(str::from_utf8(&key).unwrap().to_owned());
                            } else if command_name == b"ECHO" {
                                return Self::ECHO(str::from_utf8(&key).unwrap().to_owned());
                            } else {
                                // TODO: Handle the case where it's another command with
                                // insufficient arugments
                                return Self::Invalid;
                            }
                        }
                        return Self::Invalid;
                    }
                    // Probably SET wit key and value
                    3 => {
                        if let (
                            RespType::BulkString(command_name),
                            RespType::BulkString(key),
                            RespType::BulkString(value),
                        ) = (command[0].clone(), command[1].clone(), command[2].clone())
                        {
                            if command_name == b"SET" {
                                let set_command = SetCommand::new(
                                    str::from_utf8(&key).unwrap().to_owned(),
                                    str::from_utf8(&value).unwrap().to_owned(),
                                );
                                return Self::SET(set_command);
                            } else {
                                // TODO: Handle the case where it's another command with
                                // insufficient arugments
                                return Self::Invalid;
                            }
                        }
                        return Self::Invalid;
                    }
                    // Probably SET wit key and value and [NX | XX]
                    4 => {
                        todo!()
                    }
                    // Probably SET wit key and value and [NX | XX] and possibly [GET]
                    5 => {
                        todo!()
                    }
                    // Probably SET wit key and value and [NX | XX] and possibly [GET] and that
                    // other plethora of expiry options
                    6 => {
                        todo!()
                    }
                    7 => {
                        todo!()
                    }
                    _ => {
                        todo!()
                    }
                }
            }
            _ => todo!(),
        }
    }
}
