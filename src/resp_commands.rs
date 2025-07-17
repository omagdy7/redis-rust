use crate::{macros::*, resp_parser::*, CacheEntry, SharedCache};
use std::collections::{HashMap, HashSet};
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
    key: String,
    value: String,
    condition: Option<SetCondition>,
    expiry: Option<ExpiryOption>,
    get_old_value: bool,
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
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    // Test Helpers & Mocks

    /// Creates a new, empty shared cache for each test.
    fn new_cache() -> SharedCache {
        Arc::new(Mutex::new(HashMap::new()))
    }

    /// Builds a `RespType::Array` from string slices to simplify parser tests.
    fn build_command_from_str_slice(args: &[&str]) -> RespType {
        let resp_args = args
            .iter()
            .map(|s| RespType::BulkString(s.to_string().into_bytes()))
            .collect();
        RespType::Array(resp_args)
    }

    /// A helper to get a value directly from the cache for assertions.
    fn get_from_cache(cache: &SharedCache, key: &str) -> Option<CacheEntry> {
        cache.lock().unwrap().get(key).cloned()
    }

    /// Tests for the `RedisCommands::from(RespType)` parser logic.
    mod command_parser_tests {
        use super::*;

        #[test]
        fn test_parse_ping() {
            let cmd = build_command_from_str_slice(&["PING"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::PING));
        }

        #[test]
        fn test_parse_ping_case_insensitive() {
            let cmd = build_command_from_str_slice(&["pInG"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::PING));
        }

        #[test]
        fn test_parse_ping_with_extra_args_is_invalid() {
            let cmd = build_command_from_str_slice(&["PING", "extra"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }

        #[test]
        fn test_parse_echo() {
            let cmd = build_command_from_str_slice(&["ECHO", "hello world"]);
            match RedisCommands::from(cmd) {
                RedisCommands::ECHO(s) => assert_eq!(s, "hello world"),
                _ => panic!("Expected ECHO command"),
            }
        }

        #[test]
        fn test_parse_echo_no_args_is_invalid() {
            let cmd = build_command_from_str_slice(&["ECHO"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }

        #[test]
        fn test_parse_get() {
            let cmd = build_command_from_str_slice(&["GET", "mykey"]);
            match RedisCommands::from(cmd) {
                RedisCommands::GET(k) => assert_eq!(k, "mykey"),
                _ => panic!("Expected GET command"),
            }
        }

        #[test]
        fn test_parse_simple_set() {
            let cmd = build_command_from_str_slice(&["SET", "mykey", "myvalue"]);
            match RedisCommands::from(cmd) {
                RedisCommands::SET(c) => {
                    assert_eq!(c.key, "mykey");
                    assert_eq!(c.value, "myvalue");
                    assert!(c.condition.is_none() && c.expiry.is_none() && !c.get_old_value);
                }
                _ => panic!("Expected SET command"),
            }
        }

        #[test]
        fn test_parse_set_with_all_options() {
            let cmd = build_command_from_str_slice(&["SET", "k", "v", "NX", "PX", "5000", "GET"]);
            match RedisCommands::from(cmd) {
                RedisCommands::SET(c) => {
                    assert!(matches!(c.condition, Some(SetCondition::NotExists)));
                    assert!(matches!(c.expiry, Some(ExpiryOption::Milliseconds(5000))));
                    assert!(c.get_old_value);
                }
                _ => panic!("Expected SET command"),
            }
        }

        #[test]
        fn test_parse_set_options_case_insensitive() {
            let cmd = build_command_from_str_slice(&["set", "k", "v", "nx", "px", "100"]);
            match RedisCommands::from(cmd) {
                RedisCommands::SET(c) => {
                    assert!(matches!(c.condition, Some(SetCondition::NotExists)));
                    assert!(matches!(c.expiry, Some(ExpiryOption::Milliseconds(100))));
                }
                _ => panic!("Expected SET command"),
            }
        }

        #[test]
        fn test_parse_set_invalid_option_value() {
            let cmd = build_command_from_str_slice(&["SET", "k", "v", "EX", "not-a-number"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }

        #[test]
        fn test_parse_set_option_missing_value() {
            let cmd = build_command_from_str_slice(&["SET", "k", "v", "PX"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }

        #[test]
        fn test_parse_unknown_command() {
            let cmd = build_command_from_str_slice(&["UNKNOWN", "foo"]);
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }

        #[test]
        fn test_parse_not_an_array_is_invalid() {
            let cmd = RespType::SimpleString("SET k v".into());
            assert!(matches!(RedisCommands::from(cmd), RedisCommands::Invalid));
        }
    }

    /// Tests for the command execution logic in `RedisCommands::execute`.
    mod command_execution_tests {
        use super::*;

        /// Helper to parse and execute a command against a cache.
        fn run_command(cache: &SharedCache, args: &[&str]) -> Vec<u8> {
            let command = RedisCommands::from(build_command_from_str_slice(args));
            command.execute(Arc::clone(cache))
        }

        #[test]
        fn test_execute_ping() {
            let cache = new_cache();
            let result = run_command(&cache, &["PING"]);
            assert_eq!(result, b"+PONG\r\n");
        }

        #[test]
        fn test_execute_echo() {
            let cache = new_cache();
            // Note: the provided code has a bug, it returns a Simple String, not a Bulk String.
            // A correct implementation would return `resp!(bulk "hello")`.
            let result = run_command(&cache, &["ECHO", "hello"]);
            assert_eq!(result, b"+hello\r\n");
        }

        #[test]
        fn test_execute_get_non_existent() {
            let cache = new_cache();
            let result = run_command(&cache, &["GET", "mykey"]);
            assert_eq!(result, b"$-1\r\n"); // Null Bulk String
        }

        #[test]
        fn test_execute_set_and_get() {
            let cache = new_cache();
            let set_result = run_command(&cache, &["SET", "mykey", "myvalue"]);
            assert_eq!(set_result, b"+OK\r\n");

            let get_result = run_command(&cache, &["GET", "mykey"]);
            assert_eq!(get_result, b"$7\r\nmyvalue\r\n");
        }

        #[test]
        fn test_execute_set_nx() {
            let cache = new_cache();
            // Should succeed when key doesn't exist
            assert_eq!(run_command(&cache, &["SET", "k", "v1", "NX"]), b"+OK\r\n");
            assert_eq!(get_from_cache(&cache, "k").unwrap().value, "v1");

            // Should fail when key exists
            assert_eq!(run_command(&cache, &["SET", "k", "v2", "NX"]), b"$-1\r\n");
            assert_eq!(get_from_cache(&cache, "k").unwrap().value, "v1"); // Value is unchanged
        }

        #[test]
        fn test_execute_set_xx() {
            let cache = new_cache();
            // Should fail when key doesn't exist
            assert_eq!(run_command(&cache, &["SET", "k", "v1", "XX"]), b"$-1\r\n");
            assert!(get_from_cache(&cache, "k").is_none());

            // Pre-populate and should succeed
            run_command(&cache, &["SET", "k", "v1"]);
            assert_eq!(run_command(&cache, &["SET", "k", "v2", "XX"]), b"+OK\r\n");
            assert_eq!(get_from_cache(&cache, "k").unwrap().value, "v2");
        }

        #[test]
        fn test_execute_set_with_get_option() {
            let cache = new_cache();
            run_command(&cache, &["SET", "mykey", "old"]);

            // Note: This test will fail with the provided code, which incorrectly returns
            // a Simple String `+old\r\n`. The test correctly expects a Bulk String.
            let result = run_command(&cache, &["SET", "mykey", "new", "GET"]);
            assert_eq!(result, b"$3\r\nold\r\n");
            assert_eq!(get_from_cache(&cache, "mykey").unwrap().value, "new");
        }

        #[test]
        fn test_execute_set_get_on_non_existent() {
            let cache = new_cache();
            // Note: This test will fail with the provided code, which incorrectly
            // returns `+OK\r\n`. The spec requires a nil reply.
            let result = run_command(&cache, &["SET", "mykey", "new", "GET"]);
            assert_eq!(result, b"$-1\r\n");
            assert!(get_from_cache(&cache, "mykey").is_some());
        }

        #[test]
        fn test_expiry_with_px_and_cleanup() {
            let cache = new_cache();
            run_command(&cache, &["SET", "mykey", "val", "PX", "50"]);

            assert!(get_from_cache(&cache, "mykey").is_some());
            thread::sleep(Duration::from_millis(60));

            // GET on an expired key should return nil and trigger cleanup
            assert_eq!(run_command(&cache, &["GET", "mykey"]), b"$-1\r\n");
            assert!(get_from_cache(&cache, "mykey").is_none());
        }

        #[test]
        fn test_keepttl() {
            let cache = new_cache();
            run_command(&cache, &["SET", "mykey", "v1", "EX", "2"]);
            let expiry1 = get_from_cache(&cache, "mykey").unwrap().expires_at;

            thread::sleep(Duration::from_millis(100));
            run_command(&cache, &["SET", "mykey", "v2", "KEEPTTL"]);

            let entry2 = get_from_cache(&cache, "mykey").unwrap();
            assert_eq!(entry2.value, "v2"); // Value is updated
            assert_eq!(entry2.expires_at, expiry1); // TTL is retained
        }
    }

    /// Unit tests for the `SetCommand` helper methods.
    mod set_command_tests {
        use super::*;

        #[test]
        fn test_calculate_expiry_seconds() {
            let cmd = SetCommand::new("k".into(), "v".into())
                .with_expiry(Some(ExpiryOption::Seconds(10)));
            let expiry = cmd.calculate_expiry_time().unwrap();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let delta = expiry.saturating_sub(now);
            // Allow a small delta for execution time variance
            assert!((9990..=10010).contains(&delta), "Delta was {}", delta);
        }

        #[test]
        fn test_calculate_expiry_at_seconds() {
            let ts_secs = 1893456000; // 2030-01-01 00:00:00 UTC
            let cmd = SetCommand::new("k".into(), "v".into())
                .with_expiry(Some(ExpiryOption::ExpiresAtSeconds(ts_secs)));
            let expiry = cmd.calculate_expiry_time().unwrap();
            assert_eq!(expiry, ts_secs * 1000);
        }

        #[test]
        fn test_calculate_expiry_at_milliseconds() {
            let ts_ms = 1893456000123;
            let cmd = SetCommand::new("k".into(), "v".into())
                .with_expiry(Some(ExpiryOption::ExpiresAtMilliseconds(ts_ms)));
            let expiry = cmd.calculate_expiry_time().unwrap();
            assert_eq!(expiry, ts_ms);
        }

        #[test]
        fn test_calculate_expiry_for_none_and_keepttl() {
            let cmd_keepttl =
                SetCommand::new("k".into(), "v".into()).with_expiry(Some(ExpiryOption::KeepTtl));
            assert!(cmd_keepttl.calculate_expiry_time().is_none());

            let cmd_none = SetCommand::new("k".into(), "v".into()).with_expiry(None);
            assert!(cmd_none.calculate_expiry_time().is_none());
        }
    }
}
