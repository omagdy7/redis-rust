use std::{env, sync::Arc};

#[macro_use]
pub mod macros;
pub mod rdb;
pub mod resp_commands;
pub mod resp_parser;
pub mod shared_cache;

#[derive(Debug, Default)]
pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
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
                _ => {
                    return Err(format!("Unknown argument: {}", args[i]));
                }
            }
        }

        Ok(Some(Config { dir, dbfilename }))
    }
}
