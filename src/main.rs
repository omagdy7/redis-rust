#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

mod resp_commands;
mod resp_parser;

use resp_commands::RespCommands;
use resp_parser::parse;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    loop {
        let bytes_read = match stream.read(&mut buffer) {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return, // error occurred
        };

        let parsed_resp = parse(&buffer).unwrap();
        let response = RespCommands::from(parsed_resp.0).execute();

        // Hardcode PONG response for now
        stream.write(&response).unwrap();

        // Echo the message back
        // if let Err(_) = stream.write_all(&buffer[..bytes_read]) {
        //     return; // writing failed
        // }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    handle_client(stream);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
