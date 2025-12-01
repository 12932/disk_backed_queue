# disk_backed_queue

A robust, crash-resistant queue implementation for Rust that persists all data to disk using SQLite. Provides an mpsc-like channel API while ensuring messages survive application restarts and system failures.

## Features

- **Zero Message Loss**: All messages are persisted to SQLite before acknowledgment
- **Crash Recovery**: Messages survive application restarts and system crashes
- **MPSC-like API**: Familiar channel-based interface compatible with async Rust
- **Transaction-Based**: Atomic operations with SQLite transactions for reliability
- **Dead Letter Queue (DLQ)**: Corrupted messages automatically moved to separate database
- **Batch Operations**: High-performance bulk send/receive (460x faster than single operations)
- **Backpressure Support**: Optional queue size limits prevent unbounded growth
- **Concurrent Access**: Multiple senders supported via WAL mode and proper locking
- **Binary Serialization**: Efficient bincode encoding for minimal storage overhead

## Use Cases

Perfect for scenarios where message loss is unacceptable:

- Message queuing during database outages
- Event sourcing and audit logs
- Job queues that survive restarts
- Data pipelines requiring guaranteed delivery
- Reliable inter-process communication

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
// Send batch (460x faster than individual sends)
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

When the queue reaches max size, `send()` will block with exponential backoff until space becomes available.

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
        // Message was corrupted and moved to DLQ
        eprintln!("Corrupted message moved to DLQ: {}", e);
        // Continue processing - queue is not blocked
    },
    Err(e) => {
        // Other errors (database, I/O, etc.)
        eprintln!("Queue error: {}", e);
    }
}
```

### Error Types

- `Database(rusqlite::Error)` - SQLite database errors
- `Serialization(bincode::error::EncodeError)` - Failed to serialize message
- `Deserialization(bincode::error::DecodeError)` - Corrupted message (moved to DLQ)
- `InvalidTableName(String)` - Invalid table name provided
- `TaskJoin(String)` - Internal async task error
- `QueueClosed` - Queue was closed
- `UnexpectedRowCount(String)` - Database consistency error
- `QueueFull(usize)` - Queue reached size limit (only with max_size)

## Performance

### Benchmarks

Performance varies significantly based on your hardware (SSD vs HDD), operating system, and filesystem.

**Key Insight:** Single operations are limited by fsync overhead. Batch operations use a single transaction, achieving 50-500x speedup depending on your system.

**To measure on your hardware:**

```bash
cargo run --example throughput_demo --release
```

This will show you actual throughput for single and batch operations with different durability levels on your specific system.

### Optimization Tips

1. **Use Batch Operations**: For high throughput, accumulate messages and use `send_batch()`
2. **Tune Batch Size**: Test different batch sizes (100-1000) for your workload
3. **WAL Mode**: Enabled by default for better concurrency
4. **Message Size**: Smaller messages = better throughput
5. **Concurrent Senders**: Multiple senders work well due to WAL mode

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

- **Sender**: `Clone` + `Send` + `Sync` - Safe to clone and share across threads
- **Receiver**: `Send` but not `Sync` - Use from a single async task
- **Internal**: All SQLite operations use `spawn_blocking` to avoid blocking async executor


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
