#![allow(unused_imports)]
use anyhow::Result;
use codecrafters_redis::{
    cleanup::spawn_cleanup_task,
    client_handler::handle_client,
    rdb_utils::load_rdb,
    server::RedisServer,
    shared_cache::*,
    transaction,
    types::{BoxedAsyncWrite, SharedMut},
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, ReadHalf},
    net::TcpStream,
    sync::{Mutex, Notify},
};
use tracing::{error, info};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    let server = match RedisServer::new().await {
        Ok(Some(server)) => server,
        Ok(None) => RedisServer::new_master(), // Default to master if no args
        Err(e) => {
            error!("Error: {}", e);
            std::process::exit(1);
        }
    };

    info!("Server created and its role is {}", server.role());

    load_rdb(&server).await;
    let port = server.port().to_string();
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    let role = {
        if let RedisServer::Master(_) = server {
            "master"
        } else {
            "slave"
        }
    };

    spawn_cleanup_task(server.cache().clone());
    let server = Arc::new(Mutex::new(server));

    let (acks_for_handler, notification_manager_handler, blocking_queue_handler) = {
        let server_guard = server.lock().await;
        if let RedisServer::Master(master) = &*server_guard {
            (
                Some(master.acks.clone()),
                Some(master.notification_manager.clone()),
                Some(master.blocking_queue.clone()),
            )
        } else {
            (None, None, None)
        }
    };

    let cache = server.lock().await.cache().clone();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let server_clone = Arc::clone(&server);
                let acks_for_handler_clone = match acks_for_handler {
                    Some(ref inner) => Some(Arc::clone(inner)),
                    None => None,
                };

                let notification_manager_handler_clone = match notification_manager_handler {
                    Some(ref inner) => Some(inner.clone()),
                    None => None,
                };

                let blocking_queue_handler_clone = match blocking_queue_handler {
                    Some(ref inner) => Some(Arc::clone(inner)),
                    None => None,
                };

                let cache_clone = cache.clone();
                let socket_addr = match stream.peer_addr() {
                    Ok(addr) => addr,
                    Err(e) => {
                        error!("Failed to get peer address: {}", e);
                        continue;
                    }
                };

                // Split the stream into a reader and a writer
                let (reader, writer) = tokio::io::split(stream);
                let shared_writer: SharedMut<BoxedAsyncWrite> =
                    Arc::new(Mutex::new(Box::new(writer)));

                tokio::spawn(async move {
                    // Pass the reader and the shared writer to the handler
                    if let Err(e) = handle_client(
                        reader,
                        shared_writer,
                        socket_addr,
                        server_clone,
                        role,
                        cache_clone,
                        notification_manager_handler_clone,
                        acks_for_handler_clone,
                        blocking_queue_handler_clone,
                    )
                    .await
                    {
                        error!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }
}
