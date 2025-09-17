use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::sleep;

const BIN_NAME: &str = "codecrafters-redis";
const TEST_HOST: &str = "127.0.0.1";

async fn get_available_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct TestRedisServer {
    process: tokio::process::Child,
    port: u16,
}

impl TestRedisServer {
    async fn new(args: &[&str]) -> Self {
        let port = get_available_port().await;
        let port_str = port.to_string();

        let mut final_args = vec!["--port", &port_str];
        final_args.extend_from_slice(args);

        let process = Command::new(format!("./target/debug/{}", BIN_NAME))
            .args(&final_args)
            .spawn()
            .expect("Failed to spawn redis server process");

        // Give the server a moment to start up
        sleep(Duration::from_millis(500)).await;

        Self { process, port }
    }

    async fn new_master() -> Self {
        Self::new(&[]).await
    }

    async fn new_slave(master_port: u16) -> Self {
        let master_addr = format!("{} {}", TEST_HOST, master_port);
        Self::new(&["--replicaof", &master_addr]).await
    }

    fn port(&self) -> u16 {
        self.port
    }

    async fn connect(&self) -> TcpStream {
        let addr = format!("{}:{}", TEST_HOST, self.port);
        TcpStream::connect(&addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to server at {}: {}", addr, e))
    }
}

impl Drop for TestRedisServer {
    fn drop(&mut self) {
        // Best effort to kill the process
        let _ = self.process.start_kill();
    }
}

async fn send_command(stream: &mut TcpStream, cmd: &str) -> String {
    let mut buffer = [0; 1024];
    stream
        .write_all(cmd.as_bytes())
        .await
        .expect("Failed to write to stream");
    let n = stream
        .read(&mut buffer)
        .await
        .expect("Failed to read from stream");
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

#[tokio::test]
async fn test_ping_command() {
    let server = TestRedisServer::new_master().await;
    let mut client = server.connect().await;
    let response = send_command(&mut client, "*1\r\n$4\r\nPING\r\n").await;
    assert_eq!(response, "+PONG\r\n");
}

#[tokio::test]
async fn test_set_and_get_commands() {
    let server = TestRedisServer::new_master().await;
    let mut client = server.connect().await;

    let set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    let set_response = send_command(&mut client, set_cmd).await;
    assert_eq!(set_response, "+OK\r\n");

    let get_cmd = "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
    let get_response = send_command(&mut client, get_cmd).await;
    assert_eq!(get_response, "$7\r\nmyvalue\r\n");
}

// #[tokio::test]
// async fn test_replication() {
//     // 1. Spawn a master server.
//     let master = TestRedisServer::new_master().await;
//     let master_port = master.port();
//
//     // 2. Spawn a slave server connected to the master.
//     let slave = TestRedisServer::new_slave(master_port).await;
//
//     // 3. Connect a client to the MASTER and set a key.
//     let mut master_client = master.connect().await;
//
//     // Send a PING to make sure the server is ready
//     let ping_response = send_command(&mut master_client, "*1\r\n$4\r\nPING\r\n").await;
//     assert_eq!(ping_response, "+PONG\r\n");
//
//     let set_cmd = "*3\r\n$3\r\nSET\r\n$5\r\nrepl\r\n$4\r\ndata\r\n";
//     let set_response = send_command(&mut master_client, set_cmd).await;
//     assert_eq!(set_response, "+OK\r\n");
//
//     // 4. Use the WAIT command on the master to ensure the slave has processed the SET command.
//     // Wait for 1 replica to acknowledge, with a 500ms timeout.
//     let wait_cmd = "*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n";
//     let wait_response = send_command(&mut master_client, wait_cmd).await;
//     assert_eq!(wait_response, ":1\r\n"); // Expect 1 replica to have acknowledged.
//
//     // 5. Connect a client to the SLAVE and get the key.
//     let mut slave_client = slave.connect().await;
//     let get_cmd = "*2\r\n$3\r\nGET\r\n$5\r\nrepl\r\n";
//     let get_response = send_command(&mut slave_client, get_cmd).await;
//
//     // 6. Assert that the slave returns the value set on the master.
//     assert_eq!(get_response, "$4\r\ndata\r\n");
// }
