# disk_backed_queue

A robust, crash-resistant queue implementation for Rust that persists all data to disk using SQLite. Provides an mpsc-like channel API while ensuring messages survive application restarts and system failures.

## Features

- **Durable Enqueue**: `send` returns only after the message is committed. With the default `DurabilityLevel::Full`, a committed message survives `SIGKILL`, process crashes, OS crashes, and power loss (see [Durability Levels](#durability-levels) for the others)
- **Crash-Safe Recovery**: Built on SQLite WAL mode; on next open, uncommitted writes are rolled back via WAL recovery
- **Cancellation-Safe Receive**: a message is removed only once the receiving future has taken it. Dropping a `recv` (in `select!`, behind a timeout, or by aborting the task) leaves the message queued
- **At-Least-Once to `recv`**: the deletion commits right after the message is handed to the caller; if the process dies in that window, the message is delivered again. Receivers on any connection wait for that commit, so they never see the message twice; a read-only query on another connection may still see it for about one fsync
- **MPSC-like API**: Familiar channel-based interface for async Rust
- **Dead Letter Queue (DLQ)**: Items that fail to decode are moved to the `__disk_queue_dlq` table in the same database file, in the same transaction that removes them from the queue. If that move fails, the corrupt item stays in the main queue (no silent data loss)
- **Batch Operations**: Bulk send/receive share one transaction and one fsync (typically tens of times the throughput of single operations; measure on your hardware)
- **Backpressure**: Optional `max_size` makes senders wait asynchronously while the queue is full; `send_batch` rejects oversized batches up front
- **Atomic Capacity Check**: Concurrent senders cannot exceed `max_size` (count + insert run in one `BEGIN IMMEDIATE` transaction)
- **Binary Serialization**: Compact postcard (varint) encoding; a stored payload must decode exactly, with no trailing bytes

> **What it doesn't do:** there is no ack/visibility-timeout mechanism. Once `recv` returns a message, it is removed from the queue. If your consumer crashes *while processing* it, that message is gone. Idempotent processing protects you against the duplicates that at-least-once delivery can produce; it cannot bring back a message lost during processing. For that you need acknowledgments, e.g. an in-flight table layered on top.

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
// Send batch (one transaction and fsync for the whole batch)
let messages: Vec<MyMessage> = (0..1000)
    .map(|i| MyMessage { id: i, content: format!("Message {}", i) })
    .collect();

tx.send_batch(messages).await?;

// Receive batch
let received = rx.recv_batch(100).await?;  // Get up to 100 messages
println!("Received {} messages", received.len());
```

`recv_batch` skips (and dead-letters) corrupt rows and keeps scanning, so an empty batch means the queue holds no deliverable message, not merely that the first rows were corrupt.

**Performance:** Batch operations are dramatically faster than single operations. Run `cargo run --example throughput_demo --release` to benchmark on your hardware.

## Architecture

### Storage

- **Main Queue**: SQLite database with BLOB storage for serialized messages
- **Dead Letter Queue**: The `__disk_queue_dlq` table in the same database file, shared by all queues in that file
- **WAL Mode**: Write-Ahead Logging for better concurrency and crash safety
- **Atomic Transactions**: All operations use SQLite transactions for ACID guarantees; read-then-modify operations use `BEGIN IMMEDIATE`, so independent connections wait for each other instead of failing on stale snapshots
- **FIFO by insertion**: messages are ordered by their `AUTOINCREMENT` id, never by wall-clock time

### Queue Structure

```
my_queue.db               # One database file
├── messages table        # One table per queue
│   ├── id (PRIMARY KEY)  # Insertion order (FIFO)
│   ├── data (BLOB)       # Postcard-serialized message
│   └── created_at (INT)  # Unix timestamp (informational)
└── __disk_queue_dlq      # Dead letters for every queue in this file
    ├── id (PRIMARY KEY)
    ├── queue_name (TEXT)
    ├── original_id (INT)
    ├── data (BLOB)
    ├── error_message (TEXT)
    ├── created_at (INT)  # When the message was originally enqueued
    └── moved_at (INT)
```

## Dead Letter Queue (DLQ)

When messages fail to deserialize (schema changes, corruption, etc.), they're automatically moved to the DLQ instead of blocking the queue.

The move is a single transaction: the dead letter is written and the message removed from the queue together, or not at all. A retried move therefore cannot produce duplicate dead letters.

Decoding is strict: a payload must be consumed completely. Bytes left over after a complete message (appended garbage, or a payload written by a version of your type with extra trailing fields) send the message to the DLQ instead of being silently ignored. Note that postcard is not self-describing, so some incompatible type changes still decode without error; version your message types if you change them while messages are queued. A `Deserialize` implementation that panics is treated the same way: the message is dead-lettered and the queue keeps working.

**Failure mode:** if writing to the DLQ itself fails, the corrupt row is **left in the main queue** and `recv` returns `DlqWriteFailed` instead of `Deserialization`. This avoids silent data loss but means subsequent `recv` calls will hit the same row until the problem is fixed. `recv_batch` also returns `DlqWriteFailed` in that case, and the whole batch, valid messages included, stays queued.

**Upgrading:** earlier versions kept dead letters in a separate `<name>.dlq.db` file. That file is no longer read or written; it is left untouched (a warning is logged when it exists) so you can inspect or archive it.

### Inspecting the DLQ

```bash
# Open the queue database
sqlite3 my_queue.db

# View corrupted messages of one queue
SELECT * FROM __disk_queue_dlq WHERE queue_name = 'messages' ORDER BY moved_at DESC;

# Count corrupted messages per queue
SELECT queue_name, COUNT(*) FROM __disk_queue_dlq GROUP BY queue_name;

# View error patterns
SELECT error_message, COUNT(*) as count
FROM __disk_queue_dlq
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
sqlite3 my_queue.db "SELECT data FROM __disk_queue_dlq WHERE queue_name = 'messages'" > corrupted.bin

# Clear old DLQ entries after investigation
sqlite3 my_queue.db "DELETE FROM __disk_queue_dlq WHERE moved_at < strftime('%s', 'now', '-7 days')"

# Clear the DLQ of one queue
sqlite3 my_queue.db "DELETE FROM __disk_queue_dlq WHERE queue_name = 'messages'"
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

Construction runs its SQLite setup on Tokio's blocking pool, so it does not stall the executor while waiting for a lock held by another connection. If you stop waiting (e.g. a timeout), setup still finishes in the background; it is idempotent, so constructing again is safe.

### Sender Methods

```rust
// Send single message
async fn send(&self, item: T) -> Result<()>

// Send batch (much faster)
async fn send_batch(&self, items: Vec<T>) -> Result<()>

// Blocking send (for sync contexts)
fn blocking_send(&self, item: T) -> Result<()>
```

`blocking_send` writes directly on the calling thread outside a runtime, and inside `block_in_place` on a multi-thread runtime. On a current-thread runtime it returns `UnsupportedRuntime`: blocking the runtime's only thread would stall it, and Tokio does not reveal whether the caller is that thread, so the call is refused even from `spawn_blocking` threads. Use `send(..).await` from async code, or `tokio::runtime::Handle::current().block_on(tx.send(msg))` from a blocking thread.

### Receiver Methods

```rust
// Receive single message (returns None if empty)
async fn recv(&mut self) -> Result<Option<T>>

// Receive batch (returns empty Vec if no deliverable messages)
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

// Limited queue (senders wait when full)
let (tx, rx) = disk_backed_channel("queue.db", "messages".to_string(), Some(100_000)).await?;
```

When the queue reaches max size, `send()` waits asynchronously until space becomes available. Waiting does not occupy a Tokio blocking thread, so a full queue can always be drained, and dropping a waiting `send` stops it. Senders are woken as soon as `recv`, `recv_batch`, `clear`, or a dead-letter move on the same queue instance frees space; space freed by another process or an independently opened queue is noticed within 100 ms, by a small helper thread that runs only while senders are waiting. Waiting does not use Tokio's timer, so it also works on runtimes built without `enable_time()`.

The capacity check costs the same at any queue length: it uses the span between the oldest and newest message id instead of counting rows. Every queue operation keeps ids contiguous, so this is exact; if you delete rows out of order by hand (e.g. with `sqlite3`), the gaps keep counting toward `max_size` until the messages around them are received. That can make senders wait early, but never lets the queue exceed `max_size`. (`len()` still counts rows exactly, so it slows down as the queue grows.)

`send_batch()` returns `QueueFull(max)` immediately if the batch is larger than `max_size` (otherwise it waits for room for the whole batch). `max_size` must be at least 1; `Some(0)` is rejected with `InvalidConfiguration`. The capacity check and insert run inside a single `BEGIN IMMEDIATE` transaction, so the queue never exceeds `max_size` even with many concurrent senders.

**Cancelling a send:** dropping a `send` while it waits for capacity means nothing is enqueued. Dropping it while the insert itself is running leaves the outcome unknown: the insert may still commit. A retry after such a cancellation can enqueue a duplicate.

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

// Faster; power loss can roll back the most recent commits
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

The queue always runs SQLite in WAL mode. Assuming storage that honours fsync:

| Durability Level | Fsync | Application crash | OS crash / power loss | SQLite Mode |
|-----------------|-------|-------------------|-----------------------|-------------|
| `Full` (default) | Every commit | ✅ No loss | ✅ No loss | `SYNCHRONOUS=FULL` |
| `Normal` | WAL checkpoints only | ✅ No loss | ⚠️ Consistent, but recent commits can be rolled back | `SYNCHRONOUS=NORMAL` |
| `Off` | Never | ✅ No loss | ❌ Recent commits can be lost; database can be corrupted | `SYNCHRONOUS=OFF` |
| `Extra` | Same as `Full` | ✅ No loss | ✅ No loss (no stronger than `Full` in WAL mode) | `SYNCHRONOUS=EXTRA` |

**Note:** Batch operations have similar throughput across all durability levels since fsync cost is amortized. Run the throughput demo to measure actual performance on your system.


### Table Name Validation

Table names must:
- Be non-empty and at most 128 bytes (UTF-8)
- Contain only Unicode letters, digits, and underscores
- Not start with `sqlite_` or `__disk_queue` (case-insensitive), which are reserved for SQLite and for this library

Names are quoted in all SQL, so SQL keywords (`table`, `order`) and names starting with a digit are fine.

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

`DiskQueueError` is `#[non_exhaustive]`; keep a catch-all arm when matching it.

- `Database(rusqlite::Error)` — SQLite database errors
- `Serialization(postcard::Error)` — Failed to serialize an outgoing message
- `Deserialization(postcard::Error)` — Stored bytes did not decode exactly (or the `Deserialize` impl panicked); the row has been moved to the DLQ before this is returned
- `DlqWriteFailed(String)` — A corrupt row was found but the DLQ write failed; the row is left in the main queue so it can be retried (no silent data loss)
- `LockPoisoned(String)` — An internal mutex was poisoned by a panic in another worker. Always indicates a bug
- `InvalidTableName(String)` — Table name was empty, longer than 128 bytes, contained characters other than letters, digits and `_`, or used a reserved prefix
- `InvalidConfiguration(String)` — Another constructor argument was invalid, e.g. `max_size` of `Some(0)`
- `UnsupportedRuntime(String)` — `blocking_send` was called on a current-thread Tokio runtime (see [Sender Methods](#sender-methods) for alternatives)
- `TaskJoin(String)` — `tokio::spawn_blocking` join failure
- `QueueClosed` — Reserved for future use
- `UnexpectedRowCount(String)` — Insert/delete affected an unexpected number of rows; indicates DB inconsistency
- `QueueFull(usize)` — Returned by `send_batch` when the batch is larger than `max_size`. (Plain `send` waits rather than returning this.)

## Performance

### Benchmarks

Performance varies significantly based on your hardware (SSD vs HDD), operating system, and filesystem.

**Key insight:** single operations are bottlenecked by fsync (each `send` triggers one). Batch operations amortize one fsync across the whole batch, so throughput rises roughly with batch size, often by tens of times on typical SSDs.

**To measure on your hardware:**

```bash
cargo run --example throughput_demo --release
```

This will show you actual throughput for single and batch operations on your specific system. The demo and the benchmarks put their databases in the system temp directory; if that is RAM-backed (e.g. `/tmp` on tmpfs), fsync is free and the numbers overstate disk throughput. Point `TMPDIR` at a real disk to measure durable writes.

### Optimization Tips

1. **Use batch operations**: accumulate messages and use `send_batch()` / `recv_batch()` whenever you can — this is the single biggest lever.
2. **Tune batch size**: 100–1000 is a reasonable starting range; pick by measurement.
3. **Smaller messages**: smaller payloads = less fsync data = better throughput.
4. **Lower durability for non-critical data**: `DurabilityLevel::Normal` is meaningfully faster than `Full`, at the cost of possibly losing the most recent commits on power loss.

### Concurrency model — what to expect

All queue operations are serialized through a single in-process `Mutex<rusqlite::Connection>`. Multiple senders are *safe* and won't lose data, but they don't run *in parallel* — they queue on the mutex. WAL mode helps multi-*process* access to the same `.db` file, not multi-thread within one process. If you need throughput beyond what a single connection can do, batch your operations.

Several processes (or independently opened queues) may share one queue file: every operation that reads and then modifies the queue takes the SQLite write lock up front, and other connections wait for it instead of failing. Receiving from an empty queue only reads, so idle pollers don't hold the lock. If another connection keeps the lock past the 5-second busy timeout, the operation is retried twice before `Database(DatabaseBusy)` is returned.

## Testing

```bash
# Run tests
cargo test

# Run benchmarks (or `-- --test` for a quick smoke test)
cargo bench

# Run throughput demo
cargo run --example throughput_demo --release
```

## Thread Safety

- **Sender**: `Clone + Send + Sync`. Safe to clone and share across tasks/threads.
- **Receiver**: `Send`. `recv` and `recv_batch` take `&mut self` (mpsc-style), so you'll typically own the receiver from one task. If you really need multiple consumers, use `DiskBackedQueue` directly via an `Arc` — `recv` on the queue takes `&self`.
- **Internal**: all SQLite work, including construction, runs inside `tokio::task::spawn_blocking`, so the async executor is never blocked on I/O. Each blocking job is short and finite; waiting for capacity happens asynchronously.

## Crash Safety Semantics (what really happens on `kill -9`)

- The library installs **no signal handlers** and has no graceful-shutdown path. It doesn't need one — SQLite WAL recovery handles consistency on next open.
- A `SIGKILL` mid-`send` before the commit rolls the transaction back; the message is *not* persisted. A `SIGKILL` after the commit but before `send` returned leaves it persisted even though the caller never saw success, so a blind retry can duplicate it.
- A `SIGKILL` mid-`recv` before the deletion commits leaves the message in the queue, to be delivered again — even if `recv` had already returned it. This is the at-least-once window. The same applies to exiting without shutting the runtime down (`std::process::exit`, `Runtime::shutdown_background`) right after `recv` returns; a normal runtime shutdown waits for the commit.
- A `SIGKILL` mid-`fsync` is safe: the WAL frame either landed (durable) or didn't (truncated on recovery); no torn writes.
- A message *is* lost if the consumer dies after the deletion committed but before it finished processing the message. That is inherent to receive-then-delete without acknowledgments; idempotent processing does not prevent it.
- These guarantees hold for `DurabilityLevel::Full` (and `Extra`). With `Normal`, an OS crash or power loss can additionally roll back recent commits; with `Off`, it can lose data or corrupt the database.


## Requirements

- **Rust:** 1.88 or newer (edition 2024)
- **Tokio:** Async runtime required
- **SQLite:** Bundled via rusqlite (no external dependencies)

## License

MIT

## Contributing

Contributions welcome! Areas for improvement:

- [ ] Acknowledgment-based receive (claim, ack, lease expiry)
- [ ] Producer idempotency keys for safe retries
- [ ] Versioned message envelopes
- [ ] Async-friendly DLQ recovery API
- [ ] Metrics/observability hooks
- [ ] Pluggable serialization backends
- [ ] Queue compaction/vacuuming
- [ ] Multi-receiver support
