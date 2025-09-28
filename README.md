# tiny-redis

`tiny-redis` is a lightweight, asynchronous Redis server implementation written from scratch in Rust. It is designed to explore the core concepts of Redis, including its network protocol (RESP), master-slave replication, data persistence (RDB), and common data structures.

The project leverages the power of [Tokio](https://tokio.rs/) for high-performance, non-blocking I/O, allowing it to handle multiple concurrent client connections efficiently.

## Core Features Implemented

-   **Asynchronous Architecture:** Built entirely on `async/await` and the Tokio runtime, ensuring high throughput and concurrency with minimal overhead.
-   **Master-Slave Replication:** Full support for master-slave replication, including the `PSYNC` handshake, command propagation from master to replicas, and the `WAIT` command for checking replication consistency.
-   **RDB Persistence:** Capable of parsing Redis's RDB file format on startup to load a persisted dataset into memory.
-   **RESP Protocol Handling:** A robust parser for the Redis Serialization Protocol (RESP), capable of handling simple strings, bulk strings, integers, arrays, and errors.
-   **Concurrent State Management:** Thread-safe access to the central key-value store and server state using `Arc<Mutex<T>>` for safe sharing across concurrent tasks.
-   **Key Expiry & Cleanup:** A dedicated background task periodically cleans up expired keys from the cache, mimicking Redis's own active expiration strategy.
-   **Blocking Operations:** Correctly implements blocking commands like `BLPOP` and `XREAD BLOCK` using Tokio's notification mechanisms (`tokio::sync::Notify`) to efficiently suspend and resume client tasks without blocking threads.
-   **Publish/Subscribe:** A complete Pub/Sub implementation that allows clients to subscribe to channels and receive messages published by other clients.
-   **Transactions:** Supports `MULTI`, `EXEC`, and `DISCARD` for atomic execution of a block of commands.

## Commands Implemented

The server implements a wide range of Redis commands across several categories:

| Command | Category | Description |
|---|---|---|
| `PING` | Generic | Returns "PONG", used to test connection. |
| `ECHO` | Generic | Returns the given string. |
| `GET` | Generic | Get the value of a key. |
| `SET` | Generic | Set the value of a key, with support for `NX`, `XX`, `EX`, `PX`, `GET`. |
| `TYPE` | Generic | Returns the string representation of the type of the value stored at key. |
| `KEYS` | Generic | Find all keys matching the given pattern. |
| `INCR` | Generic | Increments the integer value of a key by one. |
| `CONFIG GET` | Generic | Get configuration parameters. |
| `INFO` | Generic | Get information and statistics about the server (specifically replication). |
| `RPUSH` / `LPUSH` | List | Append/prepend one or multiple values to a list. |
| `LRANGE` | List | Get a range of elements from a list. |
| `LLEN` | List | Get the length of a list. |
| `LPOP` | List | Remove and get the first elements in a list. |
| `BLPOP` | List | Remove and get the first element in a list, or block until one is available. |
| `XADD` | Stream | Appends a new entry to a stream. |
| `XRANGE` | Stream | Return a range of entries in a stream. |
| `XREAD` | Stream | Read entries from one or more streams, optionally blocking. |
| `PSYNC` | Replication | Used by replicas to initiate synchronization with a master. |
| `REPLCONF` | Replication | Used to configure a replica or receive acknowledgements (`ACK`). |
| `WAIT` | Replication | Wait for a specified number of replicas to acknowledge a write. |
| `MULTI` | Transaction | Marks the start of a transaction block. |
| `EXEC` | Transaction | Executes all commands queued in a transaction. |
| `DISCARD` | Transaction | Flushes all commands queued in a transaction. |
| `SUBSCRIBE` | Pub/Sub | Subscribes the client to the given channels. |
| `UNSUBSCRIBE` | Pub/Sub | Unsubscribes the client from the given channels. |
| `PUBLISH` | Pub/Sub | Posts a message to a channel. |
| `ZADD` | Sorted Set | Adds a member with a score to a sorted set. |
| `ZRANGE` | Sorted Set | Returns a range of members from a sorted set. |
| `ZRANK` | Sorted Set | Returns the rank of a member in a sorted set. |
| `ZSCORE` | Sorted Set | Returns the score of a member in a sorted set. |
| `ZCARD` | Sorted Set | Returns the cardinality of a sorted set. |
| `ZREM` | Sorted Set | Removes a member from a sorted set. |
| `GEOADD` | Geospatial | Adds a geospatial item (longitude, latitude, name). |
| `GEOPOS` | Geospatial | Returns the longitude and latitude of members. |
| `GEODIST` | Geospatial | Returns the distance between two members. |
| `GEOSEARCH` | Geospatial | Queries a sorted set of geospatial items by radius. |

## Architectural Choices

-   **Tokio for Asynchronous I/O:** The entire server is built on the Tokio runtime. The main thread runs an accept loop, and each new TCP connection is spawned as a separate, lightweight green thread (a `tokio::task`). This allows the server to handle thousands of concurrent connections on a small number of OS threads.

-   **Shared State with `Arc<Mutex<T>>`:** The central cache, server configuration, and replication state are wrapped in `Arc<Mutex<T>>`. This standard Rust pattern enables safe, shared access to data from the many concurrent client-handling tasks. Locks are held for the shortest duration possible to maximize throughput.

-   **Enum-based Server Role (`RedisServer::Master`/`Slave`):** The server's role is determined at startup and encapsulated in an enum. This leverages Rust's type system to provide compile-time guarantees about which state and logic is available, avoiding runtime errors. Both variants implement a common `CommandHandler` trait, allowing for clean, polymorphic command execution.

-   **Command Pattern with `RedisCommand` Enum:** Incoming RESP commands are parsed into a structured `RedisCommand` enum. This decouples the network parsing logic from the command execution logic. The `execute` function uses a single `match` statement to delegate to the appropriate handler, making the code clean, readable, and easy to extend with new commands.

-   **Actor-like Tasks for Background Processes:** Long-running, isolated tasks are modeled as actors. For example:
    -   A **Replication Task** listens on an MPSC channel for `ReplicationMsg`s (like `Broadcast`) and propagates commands to all connected replicas.
    -   A **Pub/Sub Task** listens on a channel for `PubSubMsg`s and broadcasts messages to all clients subscribed to a given channel.
    -   A **Cleanup Task** runs on a timer (`tokio::time::interval`) to periodically scan and remove expired keys.
    This design isolates concerns and prevents any single part of the system from blocking others.

-   **Centralized Notification for Blocking Commands:** A `NotificationManager` provides a unified system for handling blocking operations. Instead of ad-hoc signaling, tasks waiting on `BLPOP`, `XREAD`, or `WAIT` can asynchronously wait on a `tokio::sync::Notify` object. When a relevant event occurs (e.g., an `LPUSH` or an `ACK` from a replica), the corresponding notifier is triggered, waking up only the relevant waiting tasks. This is a highly efficient, scalable approach to managing blocking semantics.
