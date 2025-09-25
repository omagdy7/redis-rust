use crate::frame::Frame;
use crate::stream::{XReadStreamId, XaddStreamId, XrangeStreamdId};
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
    pub value: Frame,
    pub condition: Option<SetCondition>,
    pub expiry: Option<ExpiryOption>,
    pub get_old_value: bool,
}

impl SetCommand {
    pub fn new(key: String, value: Frame) -> Self {
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

// TODO: Refactor this to use enum struct variants with more descreptive names
#[derive(Debug, Clone)]
pub enum RedisCommand {
    /// Generic commands
    Ping,
    Echo(String),
    Get(String),
    Set(SetCommand),
    Type(String),
    ConfigGet(String),
    Keys(String),
    Info(String),
    Incr {
        key: String,
    },
    /// Replication commands
    ReplConf((String, String)),
    Psync((String, String)),
    Wait((String, String)),
    // Transaction commands
    Multi,
    Exec,
    Discard,
    // List commands
    Rpush {
        key: String,
        elements: Vec<String>,
    },
    Lpush {
        key: String,
        elements: Vec<String>,
    },
    Lrange {
        key: String,
        start_idx: i64,
        end_idx: i64,
    },
    Llen {
        key: String,
    },
    Lpop {
        key: String,
        number_of_items: Option<u64>,
    },
    Blpop {
        key: String,
        time_sec: f64,
    },
    // Sorted set commands
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
    Zrange {
        key: String,
        start: Option<i64>,
        end: Option<i64>,
        member: Option<String>,
    },
    Zrank {
        key: String,
        member: String,
    },
    Zscore {
        key: String,
        member: String,
    },
    Zcard {
        key: String,
    },
    Zrem {
        key: String,
        member: String,
    },
    // Sorted set commands
    Geoadd {
        key: String,
        longitude: f64,
        latitude: f64,
        member: String,
    },
    Geopos {
        key: String,
        locations: Vec<String>,
    },
    // Stream commands
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
        block_param: Option<u64>, // time in ms
        keys: Vec<String>,
        stream_ids: Vec<XReadStreamId>,
    },
    // Subscribe commands
    Subscribe {
        channel: String,
    },
    Unsubscribe {
        channel: String,
    },
    Publish {
        channel: String,
        msg: String,
    },
    Invalid,
}

impl RedisCommand {
    /// Helper function to extract a string from a Frame::BulkString
    fn extract_string(frame: &Frame) -> Option<String> {
        match frame {
            Frame::BulkString(bytes) => std::str::from_utf8(bytes).ok().map(|s| s.to_string()),
            _ => None,
        }
    }

    /// Helper function to extract a u64 from a Frame (BulkString or Integer)
    fn extract_i64(frame: &Frame) -> Option<i64> {
        match frame {
            Frame::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse::<i64>().ok(),
            Frame::Integer(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Helper function to extract a u64 from a Frame (BulkString or Integer)
    fn extract_u64(frame: &Frame) -> Option<u64> {
        match frame {
            Frame::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse::<u64>().ok(),
            Frame::Integer(i) => Some(*i as u64),
            _ => None,
        }
    }

    /// Helper function to extract a f64 from a Frame (BulkString or Double)
    fn extract_f64(frame: &Frame) -> Option<f64> {
        match frame {
            Frame::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse::<f64>().ok(),
            Frame::Double(i) => Some(*i),
            _ => None,
        }
    }

    /// Helper function to parse N string arguments
    fn parse_args<'a, const N: usize>(
        args: &mut impl Iterator<Item = &'a Frame>,
    ) -> Option<[String; N]> {
        let mut result = [(); N].map(|_| None::<String>);
        for i in 0..N {
            let frame = Self::require_next_arg(args)?;
            let s = Self::extract_string(frame)?;
            result[i] = Some(s);
        }
        let mut arr = std::array::from_fn(|_| String::new());
        for (i, opt) in result.into_iter().enumerate() {
            arr[i] = opt?;
        }
        Some(arr)
    }

    /// Helper function to safely get the next argument from an iterator
    fn require_next_arg<'a, I>(iter: &mut I) -> Option<I::Item>
    where
        I: Iterator<Item = &'a Frame>,
    {
        iter.next()
    }

    fn parse_set_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let key_frame = Self::require_next_arg(&mut args);
        let value_frame = Self::require_next_arg(&mut args);

        let (Some(key_frame), Some(value_frame)) = (key_frame, value_frame) else {
            return Self::Invalid;
        };

        let Some(key) = Self::extract_string(key_frame) else {
            return Self::Invalid;
        };

        let options: Vec<&Frame> = args.collect();

        if options.is_empty() {
            Self::Set(SetCommand::new(key, (*value_frame).clone()))
        } else {
            let parser = SetOptionParser::new(key, (*value_frame).clone());
            parser
                .parse_options(&options)
                .map(Self::Set)
                .unwrap_or(Self::Invalid)
        }
    }

    fn parse_xadd_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, stream_id_str]) = Self::parse_args::<2>(&mut args) {
            let Ok(parsed_id) = stream_id_str.parse::<XaddStreamId>() else {
                return Self::Invalid;
            };

            let mut fields = HashMap::new();
            while let (Some(field_key_frame), Some(value_frame)) = (args.next(), args.next()) {
                let Some(field_key) = Self::extract_string(field_key_frame) else {
                    return Self::Invalid;
                };

                let Some(value) = Self::extract_string(value_frame) else {
                    return Self::Invalid;
                };

                fields.insert(field_key, value);
            }

            Self::Xadd {
                key,
                parsed_id,
                fields,
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_xrange_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, start_str, end_str]) = Self::parse_args::<3>(&mut args) {
            let Ok(start_id) = start_str.parse::<XrangeStreamdId>() else {
                return Self::Invalid;
            };
            let Ok(end_id) = end_str.parse::<XrangeStreamdId>() else {
                return Self::Invalid;
            };

            Self::XRange {
                key,
                start: start_id,
                end: end_id,
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_xread_command<'a>(args: impl Iterator<Item = &'a Frame>) -> Self {
        let mut args = args.peekable();

        // Handle optional BLOCK argument
        let block_param = if args
            .peek()
            .map(|&f| match f {
                Frame::BulkString(bytes) => std::str::from_utf8(&bytes)
                    .map(|s| s.eq_ignore_ascii_case("block"))
                    .unwrap_or(false),
                _ => false,
            })
            .unwrap_or(false)
        {
            args.next(); // consume "block"
            match args.next() {
                Some(frame) => Self::extract_u64(frame),
                _ => return Self::Invalid,
            }
        } else {
            None
        };

        // Consume the 'streams' literal
        if args.next().is_none() {
            return Self::Invalid;
        }

        let mut args: Vec<&Frame> = args.collect();
        let i = args.partition_point(|element| match element {
            Frame::BulkString(bytes) => match std::str::from_utf8(&bytes) {
                Ok(s) => s.parse::<XReadStreamId>().is_err(),
                Err(_) => true,
            },
            _ => true,
        });

        let stream_frames = args.split_off(i);
        let mut stream_ids = Vec::new();
        for frame in &stream_frames {
            let Some(stream_id_str) = Self::extract_string(frame) else {
                return Self::Invalid;
            };
            let Ok(id) = stream_id_str.parse::<XReadStreamId>() else {
                return Self::Invalid;
            };
            stream_ids.push(id);
        }

        let mut keys = Vec::new();
        for frame in &args {
            let Some(key) = Self::extract_string(frame) else {
                return Self::Invalid;
            };
            keys.push(key);
        }

        Self::XRead {
            block_param,
            keys,
            stream_ids,
        }
    }

    fn parse_config_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([sub_command, key]) = Self::parse_args::<2>(&mut args) {
            if sub_command.eq_ignore_ascii_case("GET") {
                Self::ConfigGet(key)
            } else {
                Self::Invalid
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_info_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([sub_command]) = Self::parse_args::<1>(&mut args) {
            if sub_command.eq_ignore_ascii_case("REPLICATION") {
                Self::Info(sub_command)
            } else {
                Self::Invalid
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_ping_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if args.next().is_none() {
            Self::Ping
        } else {
            Self::Invalid
        }
    }

    fn parse_echo_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([message]) = Self::parse_args::<1>(&mut args) {
            if args.next().is_some() {
                Self::Invalid
            } else {
                Self::Echo(message)
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_get_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key]) = Self::parse_args::<1>(&mut args) {
            if args.next().is_some() {
                Self::Invalid
            } else {
                Self::Get(key)
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_type_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key]) = Self::parse_args::<1>(&mut args) {
            Self::Type(key)
        } else {
            Self::Invalid
        }
    }

    fn parse_keys_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([pattern]) = Self::parse_args::<1>(&mut args) {
            Self::Keys(pattern)
        } else {
            Self::Invalid
        }
    }

    fn parse_replconf_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([directive, value]) = Self::parse_args::<2>(&mut args) {
            Self::ReplConf((directive, value))
        } else {
            Self::Invalid
        }
    }

    fn parse_wait_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([num_replicas, timeout]) = Self::parse_args::<2>(&mut args) {
            Self::Wait((num_replicas, timeout))
        } else {
            Self::Invalid
        }
    }

    fn parse_llen_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key]) = Self::parse_args::<1>(&mut args) {
            Self::Llen { key }
        } else {
            Self::Invalid
        }
    }

    fn parse_rpush_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let op1_frame = Self::require_next_arg(&mut args);
        let Some(op1_frame) = op1_frame else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(op1_frame) else {
            return Self::Invalid;
        };

        let mut elements = Vec::new();

        while let Some(element) = Self::require_next_arg(&mut args) {
            if let Some(element) = Self::extract_string(element) {
                elements.push(element);
            };
        }

        Self::Rpush { key, elements }
    }

    fn parse_lpush_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let op1_frame = Self::require_next_arg(&mut args);
        let Some(op1_frame) = op1_frame else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(op1_frame) else {
            return Self::Invalid;
        };

        let mut elements = Vec::new();

        while let Some(element) = Self::require_next_arg(&mut args) {
            if let Some(element) = Self::extract_string(element) {
                elements.push(element);
            };
        }

        Self::Lpush { key, elements }
    }

    fn parse_lpop_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let op1_frame = Self::require_next_arg(&mut args);
        let Some(op1_frame) = op1_frame else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(op1_frame) else {
            return Self::Invalid;
        };

        let mut number_of_items = None;

        if let Some(no_items_frame) = Self::require_next_arg(&mut args)
            && let Some(no_items) = Self::extract_u64(no_items_frame)
        {
            number_of_items = Some(no_items);
        };

        Self::Lpop {
            key,
            number_of_items,
        }
    }

    fn parse_lrange_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let key = Self::require_next_arg(&mut args);
        let start_idx = Self::require_next_arg(&mut args);
        let end_idx = Self::require_next_arg(&mut args);
        let (Some(key), Some(start_idx), Some(end_idx)) = (key, start_idx, end_idx) else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(key) else {
            return Self::Invalid;
        };
        let Some(start_idx) = Self::extract_i64(start_idx) else {
            return Self::Invalid;
        };
        let Some(end_idx) = Self::extract_i64(end_idx) else {
            return Self::Invalid;
        };
        Self::Lrange {
            key,
            start_idx,
            end_idx,
        }
    }

    fn parse_blpop_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let op1_frame = Self::require_next_arg(&mut args);
        let op2_frame = Self::require_next_arg(&mut args);
        let Some(op1_frame) = op1_frame else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(op1_frame) else {
            return Self::Invalid;
        };

        let Some(op2_frame) = op2_frame else {
            return Self::Invalid;
        };
        let Some(time_sec) = Self::extract_f64(op2_frame) else {
            return Self::Invalid;
        };

        Self::Blpop { key, time_sec }
    }

    fn parse_multi_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if args.next().is_none() {
            Self::Multi
        } else {
            Self::Invalid
        }
    }

    fn parse_exec_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if args.next().is_none() {
            Self::Exec
        } else {
            Self::Invalid
        }
    }

    fn parse_discard_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if args.next().is_none() {
            Self::Discard
        } else {
            Self::Invalid
        }
    }

    fn parse_psync_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([repl_id, repl_offset]) = Self::parse_args::<2>(&mut args) {
            Self::Psync((repl_id, repl_offset))
        } else {
            Self::Invalid
        }
    }

    fn parse_incr_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key]) = Self::parse_args::<1>(&mut args) {
            Self::Incr { key }
        } else {
            Self::Invalid
        }
    }

    fn parse_subscribe_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([channel]) = Self::parse_args::<1>(&mut args) {
            Self::Subscribe { channel }
        } else {
            Self::Invalid
        }
    }

    fn parse_unsubscribe_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([channel]) = Self::parse_args::<1>(&mut args) {
            Self::Unsubscribe { channel }
        } else {
            Self::Invalid
        }
    }

    fn parse_publish_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([channel, msg]) = Self::parse_args::<2>(&mut args) {
            Self::Publish { channel, msg }
        } else {
            Self::Invalid
        }
    }

    fn parse_zadd_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, score_str, member]) = Self::parse_args::<3>(&mut args) {
            let Ok(score) = score_str.parse::<f64>() else {
                return Self::Invalid;
            };
            Self::Zadd { key, score, member }
        } else {
            Self::Invalid
        }
    }

    fn parse_zrange_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let key_frame = Self::require_next_arg(&mut args);
        let Some(key_frame) = key_frame else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(key_frame) else {
            return Self::Invalid;
        };

        let arg1_frame = Self::require_next_arg(&mut args);
        let arg2_frame = args.next();

        let (start, end, member) = if let Some(arg1_frame) = arg1_frame {
            if let Some(arg2_frame) = arg2_frame {
                // Two args: start and end
                let Some(start) = Self::extract_i64(arg1_frame) else {
                    return Self::Invalid;
                };
                let Some(end) = Self::extract_i64(arg2_frame) else {
                    return Self::Invalid;
                };
                (Some(start), Some(end), None)
            } else {
                // One arg: member
                let Some(member) = Self::extract_string(arg1_frame) else {
                    return Self::Invalid;
                };
                (None, None, Some(member))
            }
        } else {
            // No args: invalid
            return Self::Invalid;
        };

        Self::Zrange {
            key,
            start,
            end,
            member,
        }
    }

    fn parse_zrank_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, member]) = Self::parse_args::<2>(&mut args) {
            Self::Zrank { key, member }
        } else {
            Self::Invalid
        }
    }

    fn parse_zscore_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, member]) = Self::parse_args::<2>(&mut args) {
            Self::Zscore { key, member }
        } else {
            Self::Invalid
        }
    }

    fn parse_zcard_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key]) = Self::parse_args::<1>(&mut args) {
            Self::Zcard { key }
        } else {
            Self::Invalid
        }
    }

    fn parse_zrem_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, member]) = Self::parse_args::<2>(&mut args) {
            Self::Zrem { key, member }
        } else {
            Self::Invalid
        }
    }

    fn parse_geoadd_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        if let Some([key, longitude, latitude, member]) = Self::parse_args::<4>(&mut args) {
            let Ok(longitude) = longitude.parse::<f64>() else {
                return Self::Invalid;
            };
            let Ok(latitude) = latitude.parse::<f64>() else {
                return Self::Invalid;
            };
            Self::Geoadd {
                key,
                longitude,
                latitude,
                member,
            }
        } else {
            Self::Invalid
        }
    }

    fn parse_geopos_command<'a>(mut args: impl Iterator<Item = &'a Frame>) -> Self {
        let key = Self::require_next_arg(&mut args);
        let Some(op1_frame) = key else {
            return Self::Invalid;
        };
        let Some(key) = Self::extract_string(op1_frame) else {
            return Self::Invalid;
        };

        let mut locations = Vec::new();

        while let Some(location) = Self::require_next_arg(&mut args) {
            if let Some(location) = Self::extract_string(location) {
                locations.push(location);
            };
        }

        Self::Geopos {
            key,
            locations: locations,
        }
    }

    /// Unified command parser that handles all Redis commands
    fn parse_command<'a>(cmd_name: &str, args: impl Iterator<Item = &'a Frame>) -> Self {
        match cmd_name {
            // Generic commands
            "PING" => Self::parse_ping_command(args),
            "ECHO" => Self::parse_echo_command(args),
            "GET" => Self::parse_get_command(args),
            "TYPE" => Self::parse_type_command(args),
            "KEYS" => Self::parse_keys_command(args),
            "INCR" => Self::parse_incr_command(args),
            "SET" => Self::parse_set_command(args),
            "CONFIG" => Self::parse_config_command(args),
            "INFO" => Self::parse_info_command(args),
            // Replication commands
            "REPLCONF" => Self::parse_replconf_command(args),
            "WAIT" => Self::parse_wait_command(args),
            "PSYNC" => Self::parse_psync_command(args),
            // List commadns
            "RPUSH" => Self::parse_rpush_command(args),
            "LPUSH" => Self::parse_lpush_command(args),
            "LLEN" => Self::parse_llen_command(args),
            "LPOP" => Self::parse_lpop_command(args),
            "LRANGE" => Self::parse_lrange_command(args),
            "BLPOP" => Self::parse_blpop_command(args),
            // Transaction commands
            "MULTI" => Self::parse_multi_command(args),
            "EXEC" => Self::parse_exec_command(args),
            "DISCARD" => Self::parse_discard_command(args),
            // Stream commands
            "XADD" => Self::parse_xadd_command(args),
            "XRANGE" => Self::parse_xrange_command(args),
            "XREAD" => Self::parse_xread_command(args),
            // Subscribe commands
            "SUBSCRIBE" => Self::parse_subscribe_command(args),
            "UNSUBSCRIBE" => Self::parse_unsubscribe_command(args),
            "PUBLISH" => Self::parse_publish_command(args),
            // Sorted set commands
            "ZADD" => Self::parse_zadd_command(args),
            "ZRANGE" => Self::parse_zrange_command(args),
            "ZRANK" => Self::parse_zrank_command(args),
            "ZSCORE" => Self::parse_zscore_command(args),
            "ZCARD" => Self::parse_zcard_command(args),
            "ZREM" => Self::parse_zrem_command(args),
            // Geospatial commands
            "GEOADD" => Self::parse_geoadd_command(args),
            "GEOPOS" => Self::parse_geopos_command(args),
            _ => Self::Invalid,
        }
    }
}

// Parser for SET command options
struct SetOptionParser {
    command: SetCommand,
}

impl SetOptionParser {
    fn new(key: String, value: Frame) -> Self {
        Self {
            command: SetCommand::new(key, value),
        }
    }

    fn parse_option(
        &mut self,
        option_frame: &Frame,
        next_arg_frame: Option<&Frame>,
    ) -> Result<bool, &'static str> {
        let option = match option_frame {
            Frame::BulkString(bytes) => match std::str::from_utf8(&bytes) {
                Ok(s) => s.to_ascii_uppercase(),
                Err(_) => return Err("Invalid option encoding"),
            },
            _ => return Err("Option must be a string"),
        };

        let next_arg_str = match next_arg_frame {
            Some(Frame::BulkString(bytes)) => match std::str::from_utf8(&bytes) {
                Ok(s) => Some(s.to_string()),
                Err(_) => return Err("Invalid argument encoding"),
            },
            Some(Frame::Integer(i)) => Some(i.to_string()),
            Some(_) => return Err("Argument must be a string or integer"),
            None => None,
        };

        let next_arg = next_arg_str.as_deref();

        match option.as_str() {
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

    fn parse_options(mut self, options: &[&Frame]) -> Result<SetCommand, &'static str> {
        let mut i = 0;
        while i < options.len() {
            let option = &options[i];
            let next_arg = options.get(i + 1).copied();

            let consumes_next = self.parse_option(option, next_arg)?;
            i += if consumes_next { 2 } else { 1 };
        }
        Ok(self.command)
    }
}

impl From<Frame> for RedisCommand {
    fn from(value: Frame) -> Self {
        let Frame::List(command) = value else {
            return Self::Invalid;
        };

        let mut args = command.iter();

        let Some(cmd_frame) = args.next() else {
            return Self::Invalid;
        };

        let cmd_name = match cmd_frame {
            Frame::BulkString(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => s.to_ascii_uppercase(),
                Err(_) => return Self::Invalid,
            },
            _ => return Self::Invalid,
        };

        Self::parse_command(&cmd_name, args)
    }
}
