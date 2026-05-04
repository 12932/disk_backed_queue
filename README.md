# disk_backed_queue

A robust, crash-resistant queue implementation for Rust that persists all data to disk using SQLite. Provides an mpsc-like channel API while ensuring messages survive application restarts and system failures.

## Features

- **Durable Storage**: Messages are fsynced to SQLite before `send` returns, so anything in the queue survives `SIGKILL`, process crashes, and power loss
- **Crash-Safe Recovery**: Built on SQLite WAL mode; on next open, partial writes are rolled back via WAL recovery
- **At-Least-Once to `recv`**: `recv` reads and deletes inside a single transaction — if the process dies before commit, the message stays in the queue
- **MPSC-like API**: Familiar channel-based interface for async Rust
- **Dead Letter Queue (DLQ)**: Items that fail to deserialize are moved to a separate `.dlq.db` file. If the DLQ write itself fails, the corrupt item stays in the main queue (no silent data loss)
- **Batch Operations**: Bulk send/receive amortize fsync cost (~50x throughput vs. single ops on typical SSDs)
- **Backpressure**: Optional `max_size` blocks senders when full; `send_batch` rejects oversized batches up front
- **Atomic Capacity Check**: Concurrent senders cannot exceed `max_size` (count + insert run in one `BEGIN IMMEDIATE` transaction)
- **Binary Serialization**: Compact bincode encoding

> **What it doesn't do:** there is no ack/visibility-timeout mechanism. Once `recv` returns a message, it has been deleted from the queue. If your consumer crashes *while processing* it, that message is gone. For end-to-end at-least-once, make your processing idempotent or layer an in-flight table on top.

## Use Cases

Designed for single-machine workloads where messages must survive process termination or power loss while sitting in the queue:

- Buffering writes during downstream outages (database, HTTP, etc.)
- Job queues that survive restarts
- Local event spool before forwarding to a network service
- Audit/event logs where the producer must not lose data on crash

Not designed for: multi-machine fan-out, networked clients, or workloads requiring ack-after-process semantics. For those, reach for Kafka/Redis Streams/RabbitMQ.

## Installation

```toml
[dependencies]
disk_backed_queue = "*"
tokio = { version = "*", features = ["rt-multi-thread", "macros"] }  # For async runtime
serde = { version = "*", features = ["derive"] }  # For message serialization
```

## Quick Start

```rust
use disk_backed_queue::disk_backed_channel;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct MyMessage {
    id: u64,
    content: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a disk-backed channel
    let (tx, mut rx) = disk_backed_channel::<MyMessage, _>(
        "my_queue.db",
        "messages".to_string(),
        None  // No size limit (or Some(100_000) for max 100k messages)
    ).await?;

    // Send messages
    tx.send(MyMessage {
        id: 1,
        content: "Hello, world!".to_string(),
    }).await?;

    // Receive messages
    if let Some(msg) = rx.recv().await? {
        println!("Received: {:?}", msg);
    }

    Ok(())
}
```

## Batch Operations

For high-throughput scenarios, use batch operations for massive performance gains:

```rust
// Send batch (~50x faster than individual sends)
let messages: Vec<MyMessage> = (0..1000)
    .map(|i| MyMessage { id: i, content: format!("Message {}", i) })
    .collect();

tx.send_batch(messages).await?;

// Receive batch
let received = rx.recv_batch(100).await?;  // Get up to 100 messages
println!("Received {} messages", received.len());
```

**Performance:** Batch operations are dramatically faster than single operations. Run `cargo run --example throughput_demo --release` to benchmark on your hardware.

## Architecture

### Storage

- **Main Queue**: SQLite database with BLOB storage for serialized messages
- **Dead Letter Queue**: Separate `.dlq.db` file for corrupted/undeserializable messages
- **WAL Mode**: Write-Ahead Logging for better concurrency and crash safety
- **Atomic Transactions**: All operations use SQLite transactions for ACID guarantees

### Queue Structure

```
my_queue.db               # Main queue database
├── messages table
│   ├── id (PRIMARY KEY)
│   ├── data (BLOB)       # Bincode-serialized message
│   └── created_at (INT)  # Unix timestamp

my_queue.dlq.db           # Dead letter queue
└── messages_dlq table
    ├── id (PRIMARY KEY)
    ├── original_id (INT)
    ├── data (BLOB)
    ├── error_message (TEXT)
    ├── created_at (INT)
    └── moved_at (INT)
```

## Dead Letter Queue (DLQ)

When messages fail to deserialize (schema changes, corruption, etc.), they're automatically moved to the DLQ instead of blocking the queue.

**Failure mode:** if writing to the DLQ itself fails (disk full, permissions, etc.), the corrupt row is **left in the main queue** and `recv` returns `DlqWriteFailed` instead of `Deserialization`. This avoids silent data loss but means subsequent `recv` calls will hit the same row until the DLQ is fixed. `recv_batch` will skip such rows when other items are available, surfacing `DlqWriteFailed` only when no valid items came back.

### Inspecting the DLQ

```bash
# Open the DLQ database
sqlite3 my_queue.dlq.db

# View corrupted messages
SELECT * FROM messages_dlq ORDER BY moved_at DESC;

# Count corrupted messages
SELECT COUNT(*) FROM messages_dlq;

# View error patterns
SELECT error_message, COUNT(*) as count
FROM messages_dlq
GROUP BY error_message
ORDER BY count DESC;
```

### Common DLQ Scenarios

1. **Schema Changes**: Your Rust struct changed but old messages still in queue
2. **Corrupted Data**: Disk corruption or incomplete writes
3. **Version Mismatches**: Different versions of your app using same queue

### Recovery Strategies

```bash
# Export corrupted messages for analysis
sqlite3 my_queue.dlq.db "SELECT data FROM messages_dlq" > corrupted.bin

# Clear old DLQ entries after investigation
sqlite3 my_queue.dlq.db "DELETE FROM messages_dlq WHERE moved_at < strftime('%s', 'now', '-7 days')"

# Clear entire DLQ
sqlite3 my_queue.dlq.db "DELETE FROM messages_dlq"
```

## API Reference

### Channel Creation

```rust
pub async fn disk_backed_channel<T, P: AsRef<Path>>(
    db_path: P,
    table_name: String,
    max_size: Option<usize>,
) -> Result<(DiskBackedSender<T>, DiskBackedReceiver<T>)>
```

### Sender Methods

```rust
// Send single message
async fn send(&self, item: T) -> Result<()>

// Send batch (much faster)
async fn send_batch(&self, items: Vec<T>) -> Result<()>

// Blocking send (for sync contexts)
fn blocking_send(&self, item: T) -> Result<()>
```

### Receiver Methods

```rust
// Receive single message (returns None if empty)
async fn recv(&mut self) -> Result<Option<T>>

// Receive batch (returns empty Vec if no messages)
async fn recv_batch(&mut self, limit: usize) -> Result<Vec<T>>

// Queue status
async fn len(&self) -> Result<usize>
async fn is_empty(&self) -> Result<bool>
```

### Direct Queue Access

For advanced use cases, you can use `DiskBackedQueue` directly:

```rust
use disk_backed_queue::DiskBackedQueue;

let queue = DiskBackedQueue::new("queue.db", "table".to_string(), None).await?;

// All sender/receiver methods available
queue.send(message).await?;
queue.clear().await?;  // Clear all messages
```

## Configuration

### Queue Size Limits

```rust
// Unlimited queue (default)
let (tx, rx) = disk_backed_channel("queue.db", "messages".to_string(), None).await?;

// Limited queue (blocks senders when full)
let (tx, rx) = disk_backed_channel("queue.db", "messages".to_string(), Some(100_000)).await?;
```

When the queue reaches max size, `send()` blocks with exponential backoff until space becomes available. `send_batch()` returns `QueueFull(max)` immediately if the batch is larger than `max_size` (otherwise it waits for room for the whole batch). The capacity check and insert run inside a single `BEGIN IMMEDIATE` transaction, so the queue never exceeds `max_size` even with many concurrent senders.

### Durability Levels

Control the trade-off between performance and data safety:

```rust
use disk_backed_queue::{disk_backed_channel_with_durability, DurabilityLevel};

// Maximum safety (default) - survives power loss
let (tx, rx) = disk_backed_channel_with_durability(
    "queue.db",
    "messages".to_string(),
    None,
    DurabilityLevel::Full,
).await?;

// Balanced performance and safety
let (tx, rx) = disk_backed_channel_with_durability(
    "queue.db",
    "messages".to_string(),
    None,
    DurabilityLevel::Normal,
).await?;

// Maximum performance - no power loss protection
let (tx, rx) = disk_backed_channel_with_durability(
    "queue.db",
    "messages".to_string(),
    None,
    DurabilityLevel::Off,
).await?;
```

| Durability Level | Performance Impact | Power Loss Safety | SQLite Mode | Use Case |
|-----------------|-------------------|-------------------|-------------|----------|
| `Full` (default) | Baseline | ✅ Maximum | `SYNCHRONOUS=FULL` | Critical data, financial transactions |
| `Normal` |  (untested) | ⚠️ Good | `SYNCHRONOUS=NORMAL` | Balanced performance, high-throughput caching |
| `Off` |  (untested) | ❌ None | `SYNCHRONOUS=OFF` | Cache/ephemeral data only |
| `Extra` |  (untested) | ✅✅ Paranoid | `SYNCHRONOUS=EXTRA` | Regulatory compliance |

**Note:** Batch operations have similar throughput across all durability levels since fsync cost is amortized. Run the throughput demo to measure actual performance differences on your system.


### Table Name Validation

Table names must:
- Be non-empty and ≤ 128 characters
- Contain only alphanumeric characters and underscores

## Error Handling

```rust
use disk_backed_queue::DiskQueueError;

match rx.recv().await {
    Ok(Some(msg)) => {
        // Process message
    },
    Ok(None) => {
        // Queue is empty
    },
    Err(DiskQueueError::Deserialization(e)) => {
        // Message was corrupted; row has been moved to DLQ.
        eprintln!("Corrupted message moved to DLQ: {}", e);
        // Continue — queue is not blocked.
    },
    Err(DiskQueueError::DlqWriteFailed(e)) => {
        // Corrupt row could not be moved to DLQ; it stays in the main queue.
        // Investigate the DLQ database before retrying, or you'll loop here.
        eprintln!("DLQ write failed: {}", e);
    },
    Err(e) => {
        // Other errors (database, I/O, etc.)
        eprintln!("Queue error: {}", e);
    }
}
```

### Error Types

- `Database(rusqlite::Error)` — SQLite database errors
- `Serialization(bincode::error::EncodeError)` — Failed to serialize an outgoing message
- `Deserialization(bincode::error::DecodeError)` — Stored bytes did not decode; the row has been moved to the DLQ before this is returned
- `DlqWriteFailed(String)` — A corrupt row was found but the DLQ write failed; the row is left in the main queue so it can be retried (no silent data loss)
- `LockPoisoned(String)` — An internal mutex was poisoned by a panic in another worker. Always indicates a bug
- `InvalidTableName(String)` — Table name was empty, longer than 128 chars, or contained non-`[A-Za-z0-9_]`
- `TaskJoin(String)` — `tokio::spawn_blocking` join failure
- `QueueClosed` — Reserved for future use
- `UnexpectedRowCount(String)` — Insert/delete affected an unexpected number of rows; indicates DB inconsistency
- `QueueFull(usize)` — Returned by `send_batch` when the batch is larger than `max_size`. (Plain `send` waits rather than returning this.)

## Performance

### Benchmarks

Performance varies significantly based on your hardware (SSD vs HDD), operating system, and filesystem.

**Key insight:** single operations are bottlenecked by fsync (each `send` triggers one). Batch operations amortize one fsync across the whole batch, so throughput rises roughly with batch size up to ~50× on typical SSDs.

**To measure on your hardware:**

```bash
cargo run --example throughput_demo --release
```

This will show you actual throughput for single and batch operations with different durability levels on your specific system.

### Optimization Tips

1. **Use batch operations**: accumulate messages and use `send_batch()` / `recv_batch()` whenever you can — this is the single biggest lever.
2. **Tune batch size**: 100–1000 is a reasonable starting range; pick by measurement.
3. **Smaller messages**: smaller payloads = less fsync data = better throughput.
4. **Lower durability for non-critical data**: `DurabilityLevel::Normal` (still safe in WAL mode for most crashes) is meaningfully faster than `Full`.

### Concurrency model — what to expect

All queue operations are serialized through a single in-process `Mutex<rusqlite::Connection>`. Multiple senders are *safe* and won't lose data, but they don't run *in parallel* — they queue on the mutex. WAL mode helps multi-*process* access to the same `.db` file, not multi-thread within one process. If you need throughput beyond what a single connection can do, batch your operations.

## Testing

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Run throughput demo
cargo run --example throughput_demo --release
```

## Thread Safety

- **Sender**: `Clone + Send + Sync`. Safe to clone and share across tasks/threads.
- **Receiver**: `Send`. `recv` and `recv_batch` take `&mut self` (mpsc-style), so you'll typically own the receiver from one task. If you really need multiple consumers, use `DiskBackedQueue` directly via an `Arc` — `recv` on the queue takes `&self`.
- **Internal**: all SQLite work runs inside `tokio::task::spawn_blocking` so the async executor is never blocked on I/O.

## Crash Safety Semantics (what really happens on `kill -9`)

- The library installs **no signal handlers** and has no graceful-shutdown path. It doesn't need one — SQLite WAL recovery handles consistency on next open.
- A `SIGKILL` mid-`send` rolls back the transaction; the message is *not* persisted (caller would re-send on retry).
- A `SIGKILL` mid-`recv` rolls back the transaction; the message *stays* in the queue.
- A `SIGKILL` mid-`fsync` is safe: the WAL frame either landed (durable) or didn't (truncated on recovery); no torn writes.
- The narrow window where a message *can* be lost is between SQLite committing the `recv` deletion and your `await` site receiving the value — at this point the row is gone but the consumer hasn't seen it. This is the same ack-gap as a clean process crash; the fix is application-level (idempotent processing, or a separate in-flight table).


## Requirements

- **Rust:** Edition 2024 (1.86+)
- **Tokio:** Async runtime required
- **SQLite:** Bundled via rusqlite (no external dependencies)

## License

MIT

## Contributing

Contributions welcome! Areas for improvement:

- [ ] Async-friendly DLQ recovery API
- [ ] Metrics/observability hooks
- [ ] Pluggable serialization backends
- [ ] Queue compaction/vacuuming
- [ ] Multi-receiver support
