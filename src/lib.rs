use std::{env, sync::Arc};

#[macro_use]
pub mod macros;
pub mod rdb;
pub mod resp_commands;
pub mod resp_parser;
pub mod shared_cache;

// TODO: Model this in a better way there could be an enum where a Slave Server is distinguised
// from master servers in 2 different structs
#[derive(Debug, Clone, Default)]
pub struct RedisServer {
    pub role: String,
    pub port: String,
    pub master_host: String,
    pub master_port: String,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<String>,
}

impl RedisServer {
    fn new() -> Self {
        Self {
            role: "master".to_string(),
            port: "6379".to_string(),
            master_host: "".to_string(),
            master_port: "".to_string(),
            // HACK: Hardcoded for now
            master_replid: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
            master_repl_offset: Some("0".to_string()),
        }
    }
}

#[derive(Debug, Default)]
pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub server: RedisServer,
}

pub type SharedConfig = Arc<Option<Config>>;

impl Config {
    pub fn new() -> Result<Option<Config>, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() == 1 {
            return Ok(None);
        }

        let mut dir = None;
        let mut dbfilename = None;
        let mut redis_server = RedisServer::new();

        let mut i = 1; // Skip program name
        while i < args.len() {
            match args[i].as_str() {
                "--dir" => {
                    if i + 1 >= args.len() {
                        return Err("--dir requires a value".to_string());
                    }
                    dir = Some(args[i + 1].clone());
                    i += 2;
                }
                "--dbfilename" => {
                    if i + 1 >= args.len() {
                        return Err("--dbfilename requires a value".to_string());
                    }
                    dbfilename = Some(args[i + 1].clone());
                    i += 2;
                }
                "--port" => {
                    if i + 1 >= args.len() {
                        return Err("--port requires a value".to_string());
                    }
                    redis_server.port = args[i + 1].clone();
                    i += 2;
                }
                "--replicaof" => {
                    if i + 1 >= args.len() {
                        return Err("--replicaof requires a value".to_string());
                    }

                    // TODO: Find a better name for this variable
                    let info = args[i + 1].clone();

                    let (master_host, master_port) = info
                        .strip_prefix('"')
                        .and_then(|x| x.strip_suffix('"'))
                        .and_then(|x| x.split_once(' '))
                        .unwrap_or(("", ""));

                    redis_server.role = "slave".to_string();

                    // slaves don't have master attributes!!
                    redis_server.master_replid = None;
                    redis_server.master_repl_offset = None;

                    redis_server.master_host = master_host.to_string();
                    redis_server.master_port = master_port.to_string();
                    i += 2;
                }
                _ => {
                    return Err(format!("Unknown argument: {}", args[i]));
                }
            }
        }

        Ok(Some(Config {
            dir,
            dbfilename,
            server: redis_server,
        }))
    }
}
