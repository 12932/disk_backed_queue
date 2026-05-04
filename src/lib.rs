//! # disk-backed-queue
//!
//! A robust, crash-resistant queue implementation that persists all data to disk using SQLite.
//! Provides an mpsc-like channel API while ensuring messages survive application restarts and
//! system failures.
//!
//! ## Features
//!
//! - **Zero Message Loss**: All messages are persisted to SQLite before acknowledgment
//! - **Crash Recovery**: Messages survive application restarts and system crashes
//! - **MPSC-like API**: Familiar channel-based interface compatible with async Rust
//! - **Dead Letter Queue**: Corrupted messages automatically moved to separate database
//! - **Batch Operations**: High-performance bulk send/receive (~50x faster than single operations)
//! - **Backpressure Support**: Optional queue size limits prevent unbounded growth
//!
//! ## Quick Start
//!
//! ```no_run
//! use disk_backed_queue::disk_backed_channel;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Debug)]
//! struct MyMessage {
//!     id: u64,
//!     content: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a disk-backed channel
//!     let (tx, mut rx) = disk_backed_channel::<MyMessage, _>(
//!         "my_queue.db",
//!         "messages".to_string(),
//!         None  // No size limit
//!     ).await?;
//!
//!     // Send messages
//!     tx.send(MyMessage {
//!         id: 1,
//!         content: "Hello, world!".to_string(),
//!     }).await?;
//!
//!     // Receive messages
//!     if let Some(msg) = rx.recv().await? {
//!         println!("Received: {:?}", msg);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Use Cases
//!
//! Perfect for scenarios where message loss is unacceptable:
//!
//! - Message queuing during database outages
//! - Event sourcing and audit logs
//! - Job queues that survive restarts
//! - Data pipelines requiring guaranteed delivery
//!
//! ## Performance
//!
//! Batch operations are approximately **50x faster** than single operations due to
//! reduced fsync overhead. For high-throughput scenarios, use [`DiskBackedSender::send_batch`]
//! and [`DiskBackedReceiver::recv_batch`].
//!
//! Run `cargo run --example throughput_demo --release` to see performance on your system.

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, instrument, warn};

#[derive(Debug)]
pub enum DiskQueueError {
    Database(rusqlite::Error),
    Serialization(postcard::Error),
    Deserialization(postcard::Error),
    InvalidTableName(String),
    TaskJoin(String),
    /// A worker thread panicked and poisoned an internal lock. Always indicates a bug.
    LockPoisoned(String),
    /// A corrupt item could not be moved to the dead letter queue. The original item is left
    /// in the main queue (so it can be retried) and the underlying error is reported here.
    DlqWriteFailed(String),
    QueueClosed,
    UnexpectedRowCount(String),
    QueueFull(usize),
}

impl std::fmt::Display for DiskQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "Database error: {e}"),
            Self::Serialization(e) => write!(f, "Serialization error: {e}"),
            Self::Deserialization(e) => write!(f, "Deserialization error: {e}"),
            Self::InvalidTableName(s) => write!(f, "Invalid table name: {s}"),
            Self::TaskJoin(s) => write!(f, "Internal task error: {s}"),
            Self::LockPoisoned(s) => write!(f, "Internal lock poisoned: {s}"),
            Self::DlqWriteFailed(s) => write!(f, "Failed to write to dead letter queue: {s}"),
            Self::QueueClosed => f.write_str("Queue is closed"),
            Self::UnexpectedRowCount(s) => write!(f, "Unexpected row count: {s}"),
            Self::QueueFull(n) => write!(f, "Queue is full (max size: {n})"),
        }
    }
}

impl std::error::Error for DiskQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::Serialization(e) | Self::Deserialization(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for DiskQueueError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e)
    }
}

pub type Result<T> = std::result::Result<T, DiskQueueError>;

/// Lock the SQLite mutex, mapping a poisoned lock to [`DiskQueueError::LockPoisoned`].
///
/// Mutex poisoning means a previous holder panicked while holding the lock; the database
/// state is intact (SQLite is transactional) but the panic itself is a bug worth surfacing.
fn lock_conn(
    db: &Mutex<rusqlite::Connection>,
) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>> {
    db.lock().map_err(|e| {
        error!("Internal lock poisoned: {}", e);
        DiskQueueError::LockPoisoned(e.to_string())
    })
}

/// Durability configuration for SQLite synchronous mode
///
/// This controls the trade-off between durability and performance.
#[derive(Debug, Clone, Copy, Default)]
pub enum DurabilityLevel {
    /// OFF (0) - No fsync, fastest but data loss on power failure
    /// **WARNING**: Only use for temporary/cache data where loss is acceptable
    Off,

    /// NORMAL (1) - Fsync only at critical moments (WAL checkpoints)
    /// Safe in WAL mode for most crashes, but power loss during checkpoint can corrupt
    /// Good balance of performance and safety for WAL mode
    Normal,

    /// FULL (2) - Fsync after every commit (DEFAULT)
    /// Maximum durability - survives power loss at any moment
    /// Recommended for critical data where message loss is unacceptable
    #[default]
    Full,

    /// EXTRA (3) - Even more paranoid than FULL
    /// Additional fsyncs for maximum durability
    /// Rarely needed, significant performance impact
    Extra,
}

impl DurabilityLevel {
    fn as_str(&self) -> &'static str {
        match self {
            DurabilityLevel::Off => "OFF",
            DurabilityLevel::Normal => "NORMAL",
            DurabilityLevel::Full => "FULL",
            DurabilityLevel::Extra => "EXTRA",
        }
    }
}

/// Cached SQL query strings for the queue operations.
///
/// Pre-formatted SQL queries are cached to avoid repeated string formatting
/// on every operation. The table name is validated and interpolated once at
/// queue creation time, ensuring SQL injection safety.
#[derive(Debug)]
struct CachedQueries {
    insert_sql: String,
    select_sql: String,
    delete_sql: String,
    count_sql: String,
    clear_sql: String,
}

/// A disk-backed queue that persists all messages to SQLite.
///
/// This is the core queue implementation. For most use cases, prefer using
/// [`disk_backed_channel`] which provides a more ergonomic sender/receiver API.
///
/// All operations are async and use `spawn_blocking` internally to avoid blocking
/// the async executor.
///
/// # Type Parameters
///
/// * `T` - The message type. Must implement `Serialize + Deserialize + Send + Sync + 'static`.
///
/// # Example
///
/// ```no_run
/// use disk_backed_queue::DiskBackedQueue;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Message {
///     data: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let queue = DiskBackedQueue::new(
///         "queue.db",
///         "messages".to_string(),
///         None,
///     ).await?;
///
///     queue.send(Message { data: "hello".to_string() }).await?;
///     let msg = queue.recv().await?;
///
///     Ok(())
/// }
/// ```
pub struct DiskBackedQueue<T> {
    db: Arc<Mutex<rusqlite::Connection>>,
    dlq_db: Arc<Mutex<rusqlite::Connection>>,
    queries: CachedQueries,
    dlq_insert_sql: String,
    table_name: String,
    max_size: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for DiskBackedQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskBackedQueue")
            .field("table_name", &self.table_name)
            .field("max_size", &self.max_size)
            .field("queries", &self.queries)
            .finish_non_exhaustive()
    }
}

impl<T> DiskBackedQueue<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    /// Create a new disk-backed queue with default durability (FULL)
    ///
    /// This is the recommended constructor for most use cases.
    #[instrument(skip_all, fields(db_path = %db_path.as_ref().display(), table_name = %table_name))]
    pub async fn new<P: AsRef<Path>>(
        db_path: P,
        table_name: String,
        max_size: Option<usize>,
    ) -> Result<Self> {
        Self::with_durability(db_path, table_name, max_size, DurabilityLevel::default()).await
    }

    /// Create a new disk-backed queue with custom durability level
    ///
    /// # Durability Levels
    ///
    /// - `DurabilityLevel::Full` (default) - Maximum safety, fsync after every commit
    /// - `DurabilityLevel::Normal` - Good balance in WAL mode, fsync at checkpoints
    /// - `DurabilityLevel::Off` - Fastest, but data loss on power failure (not recommended)
    /// - `DurabilityLevel::Extra` - Paranoid mode, additional fsyncs (rarely needed)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // For critical data (default)
    /// let queue = DiskBackedQueue::with_durability(
    ///     "critical.db",
    ///     "messages".to_string(),
    ///     None,
    ///     DurabilityLevel::Full,
    /// ).await?;
    ///
    /// // For high-performance temporary data
    /// let queue = DiskBackedQueue::with_durability(
    ///     "cache.db",
    ///     "temp".to_string(),
    ///     None,
    ///     DurabilityLevel::Normal,
    /// ).await?;
    /// ```
    #[instrument(skip_all, fields(db_path = %db_path.as_ref().display(), table_name = %table_name, durability = ?durability))]
    pub async fn with_durability<P: AsRef<Path>>(
        db_path: P,
        table_name: String,
        max_size: Option<usize>,
        durability: DurabilityLevel,
    ) -> Result<Self> {
        // Validate table name to prevent SQL injection
        if table_name.is_empty() || table_name.len() > 128 {
            return Err(DiskQueueError::InvalidTableName(table_name));
        }
        if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(DiskQueueError::InvalidTableName(table_name));
        }

        let conn = rusqlite::Connection::open(&db_path).map_err(|e| {
            error!(
                "Failed to open SQLite database at {}: {}",
                db_path.as_ref().display(),
                e
            );
            e
        })?;

        // Enable WAL mode for better concurrency and crash safety
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| {
                error!("Failed to enable WAL mode: {}", e);
                e
            })?;

        // Set synchronous mode for durability guarantees
        conn.pragma_update(None, "synchronous", durability.as_str())
            .map_err(|e| {
                error!(
                    "Failed to set synchronous mode to {}: {}",
                    durability.as_str(),
                    e
                );
                e
            })?;

        debug!(
            "Configured SQLite with WAL mode and synchronous={}",
            durability.as_str()
        );

        // Enable foreign key constraints
        conn.pragma_update(None, "foreign_keys", true)
            .map_err(|e| {
                error!("Failed to enable foreign keys: {}", e);
                e
            })?;

        // Set busy timeout to handle concurrent access
        conn.busy_timeout(Duration::from_secs(5)).map_err(|e| {
            error!("Failed to set busy timeout: {}", e);
            e
        })?;

        // Create table if it doesn't exist
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )"
        );
        conn.execute(&create_table_sql, []).map_err(|e| {
            error!("Failed to create table {}: {}", table_name, e);
            e
        })?;

        // Create index on created_at for efficient ordering
        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name} (created_at, id)"
        );
        conn.execute(&create_index_sql, []).map_err(|e| {
            error!("Failed to create index for table {}: {}", table_name, e);
            e
        })?;

        debug!(
            "Successfully initialized disk-backed queue table: {}",
            table_name
        );

        // Open dead letter queue database
        let dlq_path = db_path.as_ref().with_extension("dlq.db");
        let dlq_conn = rusqlite::Connection::open(&dlq_path).map_err(|e| {
            error!(
                "Failed to open DLQ database at {}: {}",
                dlq_path.display(),
                e
            );
            e
        })?;

        // Configure DLQ database with same durability settings
        dlq_conn
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| {
                error!("Failed to enable WAL mode for DLQ: {}", e);
                e
            })?;

        dlq_conn
            .pragma_update(None, "synchronous", durability.as_str())
            .map_err(|e| {
                error!(
                    "Failed to set synchronous mode for DLQ to {}: {}",
                    durability.as_str(),
                    e
                );
                e
            })?;

        dlq_conn.busy_timeout(Duration::from_secs(5)).map_err(|e| {
            error!("Failed to set busy timeout for DLQ: {}", e);
            e
        })?;

        // Create DLQ table with error information
        let dlq_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name}_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                data BLOB NOT NULL,
                error_message TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                moved_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )"
        );
        dlq_conn.execute(&dlq_table_sql, []).map_err(|e| {
            error!("Failed to create DLQ table {}_dlq: {}", table_name, e);
            e
        })?;

        debug!(
            "Successfully initialized dead letter queue for table: {}",
            table_name
        );

        let queries = CachedQueries {
            insert_sql: format!("INSERT INTO {table_name} (data) VALUES (?)"),
            select_sql: format!(
                "SELECT id, data FROM {table_name} ORDER BY created_at ASC, id ASC LIMIT 1"
            ),
            delete_sql: format!("DELETE FROM {table_name} WHERE id = ?"),
            count_sql: format!("SELECT COUNT(*) FROM {table_name}"),
            clear_sql: format!("DELETE FROM {table_name}"),
        };

        let dlq_insert_sql = format!(
            "INSERT INTO {table_name}_dlq (original_id, data, error_message) VALUES (?, ?, ?)"
        );

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            dlq_db: Arc::new(Mutex::new(dlq_conn)),
            queries,
            dlq_insert_sql,
            table_name,
            max_size,
            _phantom: PhantomData,
        })
    }

    /// Send a single message to the queue.
    ///
    /// The message is serialized using postcard and persisted to SQLite before this
    /// method returns. If the queue has a `max_size` configured and the queue is full,
    /// this method will block with exponential backoff until space becomes available.
    ///
    /// # Performance
    ///
    /// Single sends are limited by fsync overhead. For high throughput, use
    /// [`send_batch`](Self::send_batch) instead, which is approximately 50x faster.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// queue.send(Message { data: "hello".to_string() }).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name))]
    pub async fn send(&self, item: T) -> Result<()> {
        let serialized = postcard::to_allocvec(&item).map_err(|e| {
            error!("Failed to serialize item for queue: {}", e);
            DiskQueueError::Serialization(e)
        })?;

        let db = self.db.clone();
        let insert_sql = self.queries.insert_sql.clone();
        let count_sql = self.queries.count_sql.clone();
        let max_size = self.max_size;
        let table_name = self.table_name.clone();

        tokio::task::spawn_blocking(move || {
            // The capacity check and insert run in a single BEGIN IMMEDIATE transaction so
            // concurrent senders cannot both observe `count == max-1` and both insert.
            let mut backoff = Duration::from_millis(10);
            loop {
                let mut conn = lock_conn(&db)?;
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(|e| {
                        error!(
                            "Failed to start transaction for table {}: {}",
                            table_name, e
                        );
                        DiskQueueError::Database(e)
                    })?;

                if let Some(max) = max_size {
                    let count: i64 = tx
                        .query_row(&count_sql, [], |row| row.get(0))
                        .map_err(DiskQueueError::Database)?;

                    if usize::try_from(count).unwrap_or(usize::MAX) >= max {
                        // Queue is full: roll back the transaction, release the lock, and wait.
                        drop(tx);
                        drop(conn);
                        warn!(
                            table_name = %table_name,
                            current_size = count,
                            max_size = max,
                            "Queue is full, waiting for space..."
                        );
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_secs(1));
                        continue;
                    }
                }

                let rows_affected = tx.execute(&insert_sql, [&serialized]).map_err(|e| {
                    error!(
                        "Failed to insert item into queue table {}: {}",
                        table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

                if rows_affected != 1 {
                    error!("Expected to insert 1 row, but inserted {}", rows_affected);
                    return Err(DiskQueueError::UnexpectedRowCount(format!(
                        "Insert affected {rows_affected} rows instead of 1"
                    )));
                }

                tx.commit().map_err(|e| {
                    error!(
                        "Failed to commit insert for table {}: {}",
                        table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

                debug!("Successfully enqueued item to disk queue");
                return Ok(());
            }
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Send multiple messages in a single transaction for much better throughput.
    ///
    /// All messages in the batch are inserted atomically - either all succeed or all fail.
    /// This is significantly faster than individual sends (approximately 50x speedup) because
    /// it uses a single SQLite transaction and fsync.
    ///
    /// # Performance
    ///
    /// Batch operations amortize the fsync overhead across all messages in the batch.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// let messages: Vec<Message> = (0..100)
    ///     .map(|i| Message { data: format!("msg_{}", i) })
    ///     .collect();
    ///
    /// queue.send_batch(messages).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name, batch_size = items.len()))]
    pub async fn send_batch(&self, items: Vec<T>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // Reject batches larger than max_size up front so we don't spin forever waiting
        // for space that can never exist.
        if let Some(max) = self.max_size
            && items.len() > max
        {
            return Err(DiskQueueError::QueueFull(max));
        }

        let mut serialized_items = Vec::with_capacity(items.len());

        // Serialize all items first (outside spawn_blocking for error handling)
        for item in items {
            let serialized = postcard::to_allocvec(&item).map_err(|e| {
                error!("Failed to serialize item for batch queue: {}", e);
                DiskQueueError::Serialization(e)
            })?;
            serialized_items.push(serialized);
        }

        let db = self.db.clone();
        let insert_sql = self.queries.insert_sql.clone();
        let count_sql = self.queries.count_sql.clone();
        let max_size = self.max_size;
        let table_name = self.table_name.clone();
        let batch_size = serialized_items.len();

        tokio::task::spawn_blocking(move || {
            // Capacity check and inserts run in the same BEGIN IMMEDIATE transaction so the
            // queue cannot exceed `max_size` even with concurrent senders.
            let mut backoff = Duration::from_millis(10);
            loop {
                let mut conn = lock_conn(&db)?;
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(|e| {
                        error!(
                            "Failed to start transaction for batch insert on table {}: {}",
                            table_name, e
                        );
                        DiskQueueError::Database(e)
                    })?;

                if let Some(max) = max_size {
                    let count: i64 = tx
                        .query_row(&count_sql, [], |row| row.get(0))
                        .map_err(DiskQueueError::Database)?;

                    if usize::try_from(count).unwrap_or(usize::MAX) + batch_size > max {
                        drop(tx);
                        drop(conn);
                        warn!(
                            table_name = %table_name,
                            current_size = count,
                            max_size = max,
                            batch_size = batch_size,
                            "Queue is full, waiting for space for batch..."
                        );
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_secs(1));
                        continue;
                    }
                }

                {
                    let mut stmt = tx.prepare_cached(&insert_sql).map_err(|e| {
                        error!(
                            "Failed to prepare INSERT for table {}: {}",
                            table_name, e
                        );
                        DiskQueueError::Database(e)
                    })?;
                    for serialized in &serialized_items {
                        stmt.execute([serialized]).map_err(|e| {
                            error!(
                                "Failed to insert item into queue table {} during batch: {}",
                                table_name, e
                            );
                            DiskQueueError::Database(e)
                        })?;
                    }
                }

                tx.commit().map_err(|e| {
                    error!(
                        "Failed to commit batch transaction for table {}: {}",
                        table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

                debug!(
                    "Successfully enqueued {} items to disk queue in batch",
                    batch_size
                );
                return Ok(());
            }
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Receive a single message from the queue.
    ///
    /// Returns the oldest message in FIFO order and removes it from the queue atomically.
    /// If the queue is empty, returns `Ok(None)` immediately (non-blocking).
    ///
    /// # Dead Letter Queue
    ///
    /// If a message fails to deserialize (e.g., due to schema changes or corruption),
    /// it is automatically moved to the dead letter queue database (`.dlq.db` file)
    /// and a `Deserialization` error is returned. The queue is not blocked - subsequent
    /// calls will process the next message.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize, Debug)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// match queue.recv().await? {
    ///     Some(msg) => println!("Received: {:?}", msg),
    ///     None => println!("Queue is empty"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name))]
    pub async fn recv(&self) -> Result<Option<T>> {
        let db = self.db.clone();
        let dlq_db = self.dlq_db.clone();
        let select_sql = self.queries.select_sql.clone();
        let delete_sql = self.queries.delete_sql.clone();
        let dlq_insert_sql = self.dlq_insert_sql.clone();
        let table_name = self.table_name.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = lock_conn(&db)?;

            // Start a transaction for atomic read-delete
            let tx = conn.transaction().map_err(|e| {
                error!(
                    "Failed to start transaction for table {}: {}",
                    table_name, e
                );
                DiskQueueError::Database(e)
            })?;

            // Read the oldest item
            let result: Option<(i64, Vec<u8>)> = tx
                .query_row(&select_sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()
                .map_err(|e| {
                    error!(
                        "Failed to execute SELECT query on table {}: {}",
                        table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

            if let Some((id, data)) = result {
                // Try to deserialize BEFORE deleting
                let item: T = match postcard::from_bytes::<T>(&data) {
                    Ok(item) => item,
                    Err(e) => {
                        // Deserialization failed - move to dead letter queue.
                        // We require the DLQ insert to succeed before deleting from the main
                        // queue, otherwise a DLQ failure would silently drop the message.
                        error!(
                            "Failed to deserialize item from queue (id {}): {}. Moving to DLQ.",
                            id, e
                        );

                        let dlq_result = match dlq_db.lock() {
                            Ok(dlq_conn) => dlq_conn
                                .execute(
                                    &dlq_insert_sql,
                                    rusqlite::params![id, &data, e.to_string()],
                                )
                                .map_err(|dlq_err| dlq_err.to_string()),
                            Err(poison_err) => Err(format!("DLQ mutex poisoned: {poison_err}")),
                        };

                        if let Err(dlq_msg) = dlq_result {
                            // Leave the corrupt row in the main queue (transaction rolls back
                            // on drop) so the operator can intervene without losing data.
                            error!(
                                "Failed to write corrupt item {} from table {} to DLQ: {}. Item left in main queue.",
                                id, table_name, dlq_msg
                            );
                            return Err(DiskQueueError::DlqWriteFailed(dlq_msg));
                        }

                        // DLQ insert succeeded; safe to delete the corrupt row from main.
                        tx.execute(&delete_sql, [&id]).map_err(|err| {
                            error!(
                                "Failed to delete item {} from table {}: {}",
                                id, table_name, err
                            );
                            DiskQueueError::Database(err)
                        })?;

                        tx.commit().map_err(|err| {
                            error!(
                                "Failed to commit transaction after DLQ move for table {}: {}",
                                table_name, err
                            );
                            DiskQueueError::Database(err)
                        })?;

                        // Return deserialization error to signal corruption
                        return Err(DiskQueueError::Deserialization(e));
                    }
                };

                // Delete the item from the queue
                let rows_deleted = tx.execute(&delete_sql, [&id]).map_err(|e| {
                    error!(
                        "Failed to delete item {} from table {}: {}",
                        id, table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

                if rows_deleted != 1 {
                    error!(
                        "Expected to delete 1 row, but deleted {} rows for id {}",
                        rows_deleted, id
                    );
                    return Err(DiskQueueError::UnexpectedRowCount(format!(
                        "Delete affected {rows_deleted} rows instead of 1 for id {id}"
                    )));
                }

                // Commit the transaction - if this fails, message stays in queue
                tx.commit().map_err(|e| {
                    error!(
                        "Failed to commit transaction for table {}: {}",
                        table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;

                debug!("Successfully dequeued item from disk queue");
                Ok(Some(item))
            } else {
                // No items in queue
                Ok(None)
            }
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Receive multiple messages in a single transaction for much better throughput.
    ///
    /// Returns up to `limit` messages from the queue in FIFO order. May return fewer if
    /// there aren't enough items. Returns an empty `Vec` if the queue is empty.
    ///
    /// All messages in the batch are removed atomically - either all succeed or all fail.
    ///
    /// # Dead Letter Queue
    ///
    /// If any message fails to deserialize, it is moved to the DLQ and skipped.
    /// The batch will contain only successfully deserialized messages. The transaction
    /// still commits successfully.
    ///
    /// # Performance
    ///
    /// Batch operations are approximately 50x faster than individual receives due to
    /// reduced transaction overhead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// // Receive up to 100 messages at once
    /// let messages = queue.recv_batch(100).await?;
    /// println!("Received {} messages", messages.len());
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name, limit = limit))]
    pub async fn recv_batch(&self, limit: usize) -> Result<Vec<T>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let db = self.db.clone();
        let dlq_db = self.dlq_db.clone();
        let table_name = self.table_name.clone();
        let dlq_insert_sql = self.dlq_insert_sql.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = lock_conn(&db)?;

            // Start a transaction for atomic batch read-delete
            let tx = conn.transaction().map_err(|e| {
                error!("Failed to start transaction for table {}: {}", table_name, e);
                DiskQueueError::Database(e)
            })?;

            // Read up to `limit` oldest items.
            // Safety: `table_name` is validated at construction time to contain only
            // alphanumeric characters and underscores, preventing SQL injection.
            let select_batch_sql = format!(
                "SELECT id, data FROM {table_name} ORDER BY created_at ASC, id ASC LIMIT ?"
            );

            let mut stmt = tx.prepare_cached(&select_batch_sql).map_err(|e| {
                error!("Failed to prepare SELECT statement for table {}: {}", table_name, e);
                DiskQueueError::Database(e)
            })?;

            let limit_param = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows = stmt
                .query_map([limit_param], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|e| {
                    error!("Failed to execute SELECT query on table {}: {}", table_name, e);
                    DiskQueueError::Database(e)
                })?;

            let mut items = Vec::new();
            let mut ids_to_delete = Vec::new();
            let mut dlq_failures: usize = 0;

            for row_result in rows {
                let (id, data) = row_result.map_err(|e| {
                    error!("Failed to read row from table {}: {}", table_name, e);
                    DiskQueueError::Database(e)
                })?;

                // Try to deserialize
                match postcard::from_bytes::<T>(&data) {
                    Ok(item) => {
                        items.push(item);
                        ids_to_delete.push(id);
                    }
                    Err(e) => {
                        // Deserialization failed - move to dead letter queue.
                        // Only mark for deletion if the DLQ insert succeeds, otherwise the
                        // corrupt row stays in the main queue (no silent data loss).
                        error!(
                            "Failed to deserialize item from queue (id {}): {}. Moving to DLQ.",
                            id, e
                        );

                        let dlq_ok = match dlq_db.lock() {
                            Ok(dlq_conn) => match dlq_conn.execute(
                                &dlq_insert_sql,
                                rusqlite::params![id, &data, e.to_string()],
                            ) {
                                Ok(_) => true,
                                Err(dlq_err) => {
                                    error!(
                                        "Failed to insert corrupt item {} into DLQ for table {}: {}. Leaving item in main queue.",
                                        id, table_name, dlq_err
                                    );
                                    false
                                }
                            },
                            Err(poison_err) => {
                                error!(
                                    "DLQ mutex poisoned while handling corrupt item {}: {}. Leaving item in main queue.",
                                    id, poison_err
                                );
                                false
                            }
                        };

                        if dlq_ok {
                            ids_to_delete.push(id);
                        } else {
                            dlq_failures += 1;
                        }
                    }
                }
            }

            drop(stmt); // Release the statement before executing deletes

            // Delete all processed items
            if !ids_to_delete.is_empty() {
                // SQLite limit for host parameters is typically 999 or higher, but 900 is safe
                const BATCH_DELETE_LIMIT: usize = 900;

                for chunk in ids_to_delete.chunks(BATCH_DELETE_LIMIT) {
                    // Safety: `table_name` is validated at construction time to contain only
                    // alphanumeric characters and underscores, preventing SQL injection.
                    // The placeholders (?) are properly parameterized below.
                    let delete_batch_sql = format!(
                        "DELETE FROM {} WHERE id IN ({})",
                        table_name,
                        chunk
                            .iter()
                            .map(|_| "?")
                            .collect::<Vec<_>>()
                            .join(",")
                    );

                    let params: Vec<&dyn rusqlite::ToSql> = chunk
                        .iter()
                        .map(|id| id as &dyn rusqlite::ToSql)
                        .collect();

                    let rows_deleted = tx
                        .execute(&delete_batch_sql, params.as_slice())
                        .map_err(|e| {
                            error!("Failed to delete items from table {}: {}", table_name, e);
                            DiskQueueError::Database(e)
                        })?;

                    if rows_deleted != chunk.len() {
                        error!(
                            "Expected to delete {} rows, but deleted {} rows",
                            chunk.len(),
                            rows_deleted
                        );
                        return Err(DiskQueueError::UnexpectedRowCount(format!(
                            "Delete affected {rows_deleted} rows instead of {}",
                            chunk.len()
                        )));
                    }
                }
            }

            // Commit the transaction
            tx.commit().map_err(|e| {
                error!("Failed to commit batch transaction for table {}: {}", table_name, e);
                DiskQueueError::Database(e)
            })?;

            // If every item we read failed to land in the DLQ and we have nothing valid to
            // return, surface an error so the caller is not stuck silently spinning. If we
            // recovered at least some items, return them and let the caller observe the DLQ
            // failures via logs/metrics on the next call.
            if dlq_failures > 0 && items.is_empty() {
                return Err(DiskQueueError::DlqWriteFailed(format!(
                    "{dlq_failures} corrupt item(s) could not be moved to DLQ"
                )));
            }

            if dlq_failures > 0 {
                warn!(
                    table_name = %table_name,
                    dlq_failures = dlq_failures,
                    "Some corrupt items could not be moved to DLQ; they remain in the main queue"
                );
            }

            debug!("Successfully dequeued {} items from disk queue in batch", items.len());
            Ok(items)
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Returns the number of messages currently in the queue.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// let count = queue.len().await?;
    /// println!("Queue has {} messages", count);
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name))]
    pub async fn len(&self) -> Result<usize> {
        let db = self.db.clone();
        let count_sql = self.queries.count_sql.clone();

        tokio::task::spawn_blocking(move || {
            let conn = lock_conn(&db)?;
            let count: i64 = conn
                .query_row(&count_sql, [], |row| row.get(0))
                .map_err(DiskQueueError::Database)?;
            Ok(usize::try_from(count).unwrap_or(usize::MAX))
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Returns `true` if the queue contains no messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// if queue.is_empty().await? {
    ///     println!("No messages to process");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name))]
    pub async fn is_empty(&self) -> Result<bool> {
        let db = self.db.clone();
        // Cheap O(1) check that stops at the first row instead of doing a full COUNT(*).
        // Safety: `table_name` is validated at construction time.
        let exists_sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
            self.table_name
        );

        tokio::task::spawn_blocking(move || {
            let conn = lock_conn(&db)?;
            let exists: i64 = conn
                .query_row(&exists_sql, [], |row| row.get(0))
                .map_err(DiskQueueError::Database)?;
            Ok(exists == 0)
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }

    /// Removes all messages from the queue.
    ///
    /// This operation is atomic - either all messages are deleted or none are.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::DiskBackedQueue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let queue = DiskBackedQueue::<Message>::new("queue.db", "messages".to_string(), None).await?;
    /// queue.clear().await?;
    /// assert!(queue.is_empty().await?);
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip_all, fields(table_name = %self.table_name))]
    pub async fn clear(&self) -> Result<()> {
        let db = self.db.clone();
        let clear_sql = self.queries.clear_sql.clone();

        tokio::task::spawn_blocking(move || {
            let conn = lock_conn(&db)?;
            conn.execute(&clear_sql, [])
                .map_err(DiskQueueError::Database)?;
            debug!("Cleared disk queue");
            Ok(())
        })
        .await
        .map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?
    }
}

/// The sending half of a disk-backed channel.
///
/// Messages sent through this sender are persisted to SQLite. The sender can be
/// cloned and shared across multiple tasks or threads safely.
///
/// Created via [`disk_backed_channel`] or [`disk_backed_channel_with_durability`].
///
/// # Example
///
/// ```no_run
/// # use disk_backed_queue::disk_backed_channel;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct Message { data: String }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (tx, mut rx) = disk_backed_channel::<Message, _>(
///     "queue.db",
///     "messages".to_string(),
///     None,
/// ).await?;
///
/// // Sender can be cloned and moved to other tasks
/// let tx2 = tx.clone();
/// tokio::spawn(async move {
///     tx2.send(Message { data: "from another task".to_string() }).await
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DiskBackedSender<T> {
    queue: std::sync::Arc<DiskBackedQueue<T>>,
}

impl<T> DiskBackedSender<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    /// Send a single message to the queue.
    ///
    /// See [`DiskBackedQueue::send`] for details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// tx.send(Message { data: "hello".to_string() }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, item: T) -> Result<()> {
        self.queue.send(item).await
    }

    /// Send multiple messages in a single transaction for much better throughput.
    ///
    /// See [`DiskBackedQueue::send_batch`] for details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// let messages = vec![
    ///     Message { data: "msg1".to_string() },
    ///     Message { data: "msg2".to_string() },
    /// ];
    /// tx.send_batch(messages).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_batch(&self, items: Vec<T>) -> Result<()> {
        self.queue.send_batch(items).await
    }

    /// Blocking send for use in synchronous contexts.
    ///
    /// This method blocks the current thread until the message is persisted.
    /// If called from within a Tokio runtime, it uses `block_in_place` to avoid
    /// blocking the async executor. If called outside a runtime, it creates a
    /// temporary runtime.
    ///
    /// # Performance
    ///
    /// This should be used sparingly as it blocks a thread. Prefer async `send`
    /// when possible.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// // In a synchronous context
    /// std::thread::spawn(move || {
    ///     tx.blocking_send(Message { data: "from sync".to_string() })
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_send(&self, item: T) -> Result<()> {
        // `tokio::task::block_in_place` only works on the multi-thread runtime; calling it
        // from a current-thread runtime panics. We probe the runtime flavor and fall back
        // to a temporary runtime when block_in_place is unavailable.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(self.send(item)))
            }
            _ => {
                let runtime = tokio::runtime::Runtime::new().map_err(|e| {
                    error!("Failed to create Tokio runtime: {}", e);
                    DiskQueueError::TaskJoin(format!("Runtime creation failed: {e}"))
                })?;
                runtime.block_on(self.send(item))
            }
        }
    }
}

impl<T> Clone for DiskBackedSender<T> {
    /// Clone the sender.
    ///
    /// This is a cheap operation that only clones an `Arc` pointer. All cloned
    /// senders share the same underlying queue and can be safely used from
    /// multiple tasks or threads.
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

/// The receiving half of a disk-backed channel.
///
/// Messages are received from SQLite in FIFO order. Unlike the sender, the receiver
/// cannot be cloned (similar to `tokio::sync::mpsc::Receiver`).
///
/// Created via [`disk_backed_channel`] or [`disk_backed_channel_with_durability`].
///
/// # Example
///
/// ```no_run
/// # use disk_backed_queue::disk_backed_channel;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Debug)]
/// # struct Message { data: String }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (tx, mut rx) = disk_backed_channel::<Message, _>(
///     "queue.db",
///     "messages".to_string(),
///     None,
/// ).await?;
///
/// // Receive messages
/// while let Some(msg) = rx.recv().await? {
///     println!("Received: {:?}", msg);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DiskBackedReceiver<T> {
    queue: std::sync::Arc<DiskBackedQueue<T>>,
}

impl<T> DiskBackedReceiver<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    /// Receive a single message from the queue.
    ///
    /// Returns `Ok(None)` if the queue is empty (non-blocking).
    /// See [`DiskBackedQueue::recv`] for details about dead letter queue handling.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// match rx.recv().await? {
    ///     Some(msg) => println!("Got message"),
    ///     None => println!("Queue is empty"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<Option<T>> {
        self.queue.recv().await
    }

    /// Receive multiple messages in a single transaction for much better throughput.
    ///
    /// Returns up to `limit` messages from the queue. May return fewer if there aren't
    /// enough items. Returns an empty `Vec` if the queue is empty.
    ///
    /// See [`DiskBackedQueue::recv_batch`] for details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// // Receive up to 100 messages at once
    /// let messages = rx.recv_batch(100).await?;
    /// for msg in messages {
    ///     // Process message
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv_batch(&mut self, limit: usize) -> Result<Vec<T>> {
        self.queue.recv_batch(limit).await
    }

    /// Returns the number of messages currently in the queue.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// let count = rx.len().await?;
    /// println!("Queue has {} messages", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn len(&self) -> Result<usize> {
        self.queue.len().await
    }

    /// Returns `true` if the queue contains no messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disk_backed_queue::disk_backed_channel;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Message { data: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (tx, mut rx) = disk_backed_channel::<Message, _>("queue.db", "messages".to_string(), None).await?;
    /// if rx.is_empty().await? {
    ///     println!("No messages to process");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_empty(&self) -> Result<bool> {
        self.queue.is_empty().await
    }
}

/// Create a disk-backed channel with default durability (FULL).
///
/// This is the recommended way to create a disk-backed queue for most use cases.
/// It provides maximum data safety by fsyncing after every commit, ensuring messages
/// survive power loss.
///
/// # Arguments
///
/// * `db_path` - Path to the SQLite database file. Will be created if it doesn't exist.
/// * `table_name` - Name of the table to store messages. Must contain only alphanumeric
///   characters and underscores, and be between 1-128 characters.
/// * `max_size` - Optional maximum queue size. When set, senders will block when the
///   queue is full. Use `None` for unbounded queues.
///
/// # Returns
///
/// A tuple of `(sender, receiver)` where the sender can be cloned and shared across
/// tasks, while the receiver is single-threaded.
///
/// # Example
///
/// ```no_run
/// use disk_backed_queue::disk_backed_channel;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct Message {
///     id: u64,
///     content: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let (tx, mut rx) = disk_backed_channel::<Message, _>(
///         "my_queue.db",
///         "messages".to_string(),
///         Some(10_000),  // Max 10,000 messages
///     ).await?;
///
///     // Send from multiple tasks
///     let tx2 = tx.clone();
///     tokio::spawn(async move {
///         tx2.send(Message { id: 1, content: "hello".to_string() }).await
///     });
///
///     // Receive from single task
///     while let Some(msg) = rx.recv().await? {
///         println!("Received: {:?}", msg);
///     }
///
///     Ok(())
/// }
/// ```
pub async fn disk_backed_channel<T, P: AsRef<Path>>(
    db_path: P,
    table_name: String,
    max_size: Option<usize>,
) -> Result<(DiskBackedSender<T>, DiskBackedReceiver<T>)>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    disk_backed_channel_with_durability(db_path, table_name, max_size, DurabilityLevel::default())
        .await
}

/// Create a disk-backed channel with custom durability level.
///
/// This allows you to trade off between performance and data safety by choosing
/// different SQLite synchronous modes.
///
/// # Durability Levels
///
/// * [`DurabilityLevel::Full`] (default) - Maximum safety, survives power loss
/// * [`DurabilityLevel::Normal`] - Balanced performance, safe in most crashes
/// * [`DurabilityLevel::Off`] - Fastest, but data loss on power failure
/// * [`DurabilityLevel::Extra`] - Paranoid mode, additional fsyncs
///
/// # Example
///
/// ```no_run
/// use disk_backed_queue::{disk_backed_channel_with_durability, DurabilityLevel};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Message { data: String }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // For critical data (default)
///     let (tx, rx) = disk_backed_channel_with_durability::<Message, _>(
///         "critical.db",
///         "messages".to_string(),
///         None,
///         DurabilityLevel::Full,
///     ).await?;
///
///     // For high-performance temporary cache
///     let (tx_fast, rx_fast) = disk_backed_channel_with_durability::<Message, _>(
///         "cache.db",
///         "temp".to_string(),
///         None,
///         DurabilityLevel::Normal,
///     ).await?;
///
///     Ok(())
/// }
/// ```
pub async fn disk_backed_channel_with_durability<T, P: AsRef<Path>>(
    db_path: P,
    table_name: String,
    max_size: Option<usize>,
    durability: DurabilityLevel,
) -> Result<(DiskBackedSender<T>, DiskBackedReceiver<T>)>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    let queue = DiskBackedQueue::with_durability(db_path, table_name, max_size, durability).await?;
    let queue_arc = std::sync::Arc::new(queue);

    let sender = DiskBackedSender {
        queue: queue_arc.clone(),
    };

    let receiver = DiskBackedReceiver { queue: queue_arc };

    Ok((sender, receiver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::NamedTempFile;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestMessage {
        id: u64,
        content: String,
    }

    #[tokio::test]
    async fn test_disk_backed_queue_basic() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "test_queue".to_string(), None)
            .await
            .unwrap();

        // Test empty queue
        assert!(queue.is_empty().await.unwrap());
        assert_eq!(queue.len().await.unwrap(), 0);
        assert!(queue.recv().await.unwrap().is_none());

        // Send some messages
        let msg1 = TestMessage {
            id: 1,
            content: "Hello".to_string(),
        };
        let msg2 = TestMessage {
            id: 2,
            content: "World".to_string(),
        };

        queue.send(msg1.clone()).await.unwrap();
        queue.send(msg2.clone()).await.unwrap();

        // Test queue state
        assert!(!queue.is_empty().await.unwrap());
        assert_eq!(queue.len().await.unwrap(), 2);

        // Receive messages in FIFO order
        let received1 = queue.recv().await.unwrap().unwrap();
        assert_eq!(received1, msg1);
        assert_eq!(queue.len().await.unwrap(), 1);

        let received2 = queue.recv().await.unwrap().unwrap();
        assert_eq!(received2, msg2);
        assert_eq!(queue.len().await.unwrap(), 0);

        // Queue should be empty now
        assert!(queue.is_empty().await.unwrap());
        assert!(queue.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_disk_backed_channel() {
        let temp_file = NamedTempFile::new().unwrap();
        let (sender, mut receiver) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "test_channel".to_string(),
            None,
        )
        .await
        .unwrap();

        let msg = TestMessage {
            id: 42,
            content: "Channel test".to_string(),
        };

        sender.send(msg.clone()).await.unwrap();
        let received = receiver.recv().await.unwrap().unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path().to_path_buf();

        let msg = TestMessage {
            id: 99,
            content: "Persistent message".to_string(),
        };

        // Create queue, send message, and drop it
        {
            let queue = DiskBackedQueue::new(&temp_path, "persistence_test".to_string(), None)
                .await
                .unwrap();
            queue.send(msg.clone()).await.unwrap();
        }

        // Create new queue instance and verify message is still there
        {
            let queue: DiskBackedQueue<TestMessage> =
                DiskBackedQueue::new(&temp_path, "persistence_test".to_string(), None)
                    .await
                    .unwrap();
            assert_eq!(queue.len().await.unwrap(), 1);
            let received = queue.recv().await.unwrap().unwrap();
            assert_eq!(received, msg);
        }
    }

    #[tokio::test]
    async fn test_error_handling_database_corruption() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "test_queue".to_string(), None)
            .await
            .unwrap();

        // Send a valid message
        let msg = TestMessage {
            id: 1,
            content: "Test".to_string(),
        };
        queue.send(msg).await.unwrap();

        // Manually corrupt the database by writing invalid binary data
        {
            let db = queue.db.lock().unwrap();
            let garbage_data: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0xFC]; // Invalid postcard
            db.execute(
                "UPDATE test_queue SET data = ? WHERE id = 1",
                [&garbage_data],
            )
            .unwrap();
        }

        // Try to receive - should return deserialization error but message moved to DLQ
        let result = queue.recv().await;
        assert!(result.is_err(), "Expected error, got: {:?}", result);
        assert!(
            matches!(result, Err(DiskQueueError::Deserialization(_))),
            "Expected Deserialization error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = std::sync::Arc::new(
            DiskBackedQueue::new(temp_file.path(), "concurrent_test".to_string(), None)
                .await
                .unwrap(),
        );

        // Spawn multiple senders
        let mut send_handles = vec![];
        for i in 0..10 {
            let queue_clone = queue.clone();
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    let msg = TestMessage {
                        id: i * 10 + j,
                        content: format!("Message from sender {i}, iteration {j}"),
                    };
                    queue_clone.send(msg).await.unwrap();
                }
            });
            send_handles.push(handle);
        }

        // Spawn multiple receivers
        let mut recv_handles = vec![];
        let received_messages = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

        for _ in 0..5 {
            let queue_clone = queue.clone();
            let messages_clone = received_messages.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match queue_clone.recv().await {
                        Ok(Some(msg)) => {
                            messages_clone.lock().await.push(msg);
                        }
                        Ok(None) => {
                            // No more messages, small delay before checking again
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        Err(_) => break,
                    }

                    // Stop when we've received all messages
                    if messages_clone.lock().await.len() >= 100 {
                        break;
                    }
                }
            });
            recv_handles.push(handle);
        }

        // Wait for all senders to complete
        for handle in send_handles {
            handle.await.unwrap();
        }

        // Wait for all messages to be received with timeout
        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);

        loop {
            let count = received_messages.lock().await.len();
            if count >= 100 {
                break;
            }

            if start.elapsed() > timeout {
                panic!(
                    "Timeout: Only received {} messages after {:?}",
                    count, timeout
                );
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Check that all messages were received
        let messages = received_messages.lock().await;
        assert_eq!(messages.len(), 100);

        // Clean up receivers
        for handle in recv_handles {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn test_empty_queue_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue: DiskBackedQueue<TestMessage> =
            DiskBackedQueue::new(temp_file.path(), "empty_test".to_string(), None)
                .await
                .unwrap();

        // Multiple recv calls on empty queue should all return None
        for _ in 0..5 {
            assert!(queue.recv().await.unwrap().is_none());
        }

        assert!(queue.is_empty().await.unwrap());
        assert_eq!(queue.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_large_messages() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "large_test".to_string(), None)
            .await
            .unwrap();

        // Create a large message (1MB of content)
        let large_content = "x".repeat(1024 * 1024);
        let large_msg = TestMessage {
            id: 42,
            content: large_content.clone(),
        };

        // Send and receive large message
        queue.send(large_msg.clone()).await.unwrap();
        let received = queue.recv().await.unwrap().unwrap();

        assert_eq!(received.id, 42);
        assert_eq!(received.content.len(), 1024 * 1024);
        assert_eq!(received.content, large_content);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "fifo_test".to_string(), None)
            .await
            .unwrap();

        // Send messages in order
        let messages: Vec<TestMessage> = (0..100)
            .map(|i| TestMessage {
                id: i,
                content: format!("Message {i}"),
            })
            .collect();

        for msg in &messages {
            queue.send(msg.clone()).await.unwrap();
        }

        // Receive messages and verify FIFO order
        for expected_msg in &messages {
            let received = queue.recv().await.unwrap().unwrap();
            assert_eq!(received, *expected_msg);
        }

        // Queue should be empty now
        assert!(queue.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_invalid_table_name() {
        let temp_file = NamedTempFile::new().unwrap();

        // Try to create queue with invalid table name
        let _result = DiskBackedQueue::<TestMessage>::new(
            temp_file.path(),
            "invalid-table-name-with-dashes".to_string(),
            None,
        )
        .await;

        // This should still work because SQLite is quite permissive
        // but let's test a truly invalid name
        let result2 = DiskBackedQueue::<TestMessage>::new(
            temp_file.path(),
            "".to_string(), // Empty table name should cause issues
            None,
        )
        .await;

        // Empty table name should cause an error
        assert!(result2.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blocking_send() {
        let temp_file = NamedTempFile::new().unwrap();
        let (sender, mut receiver) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "blocking_test".to_string(),
            None,
        )
        .await
        .unwrap();

        let msg = TestMessage {
            id: 123,
            content: "Blocking test".to_string(),
        };

        // Test blocking send
        sender.blocking_send(msg.clone()).unwrap();

        // Receive and verify
        let received = receiver.recv().await.unwrap().unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn test_database_file_permissions() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path().to_path_buf();

        // Create queue first
        let _queue = DiskBackedQueue::<TestMessage>::new(&temp_path, "perm_test".to_string(), None)
            .await
            .unwrap();

        // Check that database file was created
        assert!(temp_path.exists());

        // Basic metadata check (permissions test is platform-specific)
        let metadata = std::fs::metadata(&temp_path).unwrap();
        assert!(metadata.is_file());
        assert!(metadata.len() > 0); // Should have some content (SQLite header)
    }

    #[tokio::test]
    async fn test_dlq_file_created() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        disk_backed_channel::<TestMessage, _>(temp_path, "test".to_string(), None)
            .await
            .unwrap();

        // Check DLQ file exists
        let dlq_path = temp_path.with_extension("dlq.db");
        assert!(dlq_path.exists());
    }

    #[tokio::test]
    async fn test_transaction_rollback_on_corruption() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "test_queue".to_string(), None)
            .await
            .unwrap();

        // Send a valid message
        let msg = TestMessage {
            id: 1,
            content: "Valid".to_string(),
        };
        queue.send(msg).await.unwrap();

        // Manually corrupt the database by writing invalid binary data
        {
            let db = queue.db.lock().unwrap();
            let garbage_data: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0xFC]; // Invalid postcard
            db.execute(
                "UPDATE test_queue SET data = ? WHERE id = 1",
                [&garbage_data],
            )
            .unwrap();
        }

        // Try to receive - should return deserialization error
        let result = queue.recv().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DiskQueueError::Deserialization(_))));

        // Verify message was moved to DLQ
        let dlq_path = temp_file.path().with_extension("dlq.db");
        let dlq_conn = rusqlite::Connection::open(&dlq_path).unwrap();
        let count: i64 = dlq_conn
            .query_row("SELECT COUNT(*) FROM test_queue_dlq", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // Verify main queue is empty
        assert_eq!(queue.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_max_size_blocks() {
        let temp_file = NamedTempFile::new().unwrap();
        let (tx, mut rx) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "test".to_string(),
            Some(2), // Max 2 messages
        )
        .await
        .unwrap();

        let msg1 = TestMessage {
            id: 1,
            content: "Message 1".to_string(),
        };
        let msg2 = TestMessage {
            id: 2,
            content: "Message 2".to_string(),
        };
        let msg3 = TestMessage {
            id: 3,
            content: "Message 3".to_string(),
        };

        // Send 2 messages (should succeed immediately)
        tx.send(msg1.clone()).await.unwrap();
        tx.send(msg2.clone()).await.unwrap();

        // Send 3rd message in background (should block until space available)
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move { tx_clone.send(msg3).await });

        // Give it time to attempt send (should be blocked)
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // Receive one message to make space
        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(received, msg1);

        // Now 3rd send should complete
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(handle.is_finished());
        handle.await.unwrap().unwrap();

        // Verify queue has 2 messages
        assert_eq!(rx.len().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_send_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "batch_test".to_string(), None)
            .await
            .unwrap();

        // Create a batch of messages
        let messages: Vec<TestMessage> = (0..100)
            .map(|i| TestMessage {
                id: i,
                content: format!("Batch message {}", i),
            })
            .collect();

        // Send batch
        queue.send_batch(messages.clone()).await.unwrap();

        // Verify count
        assert_eq!(queue.len().await.unwrap(), 100);

        // Receive and verify all messages
        for expected in &messages {
            let received = queue.recv().await.unwrap().unwrap();
            assert_eq!(received, *expected);
        }

        assert!(queue.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_recv_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "batch_recv_test".to_string(), None)
            .await
            .unwrap();

        // Send 100 individual messages
        for i in 0..100 {
            let msg = TestMessage {
                id: i,
                content: format!("Message {}", i),
            };
            queue.send(msg).await.unwrap();
        }

        // Receive in batches of 25
        let batch1 = queue.recv_batch(25).await.unwrap();
        assert_eq!(batch1.len(), 25);
        assert_eq!(batch1[0].id, 0);
        assert_eq!(batch1[24].id, 24);

        let batch2 = queue.recv_batch(25).await.unwrap();
        assert_eq!(batch2.len(), 25);
        assert_eq!(batch2[0].id, 25);
        assert_eq!(batch2[24].id, 49);

        // Receive remaining 50 items (but only request 100)
        let batch3 = queue.recv_batch(100).await.unwrap();
        assert_eq!(batch3.len(), 50); // Should only get 50
        assert_eq!(batch3[0].id, 50);
        assert_eq!(batch3[49].id, 99);

        // Queue should be empty
        assert!(queue.is_empty().await.unwrap());
        let empty_batch = queue.recv_batch(10).await.unwrap();
        assert!(empty_batch.is_empty());
    }

    #[tokio::test]
    async fn test_batch_with_channel_api() {
        let temp_file = NamedTempFile::new().unwrap();
        let (tx, mut rx) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "batch_channel_test".to_string(),
            None,
        )
        .await
        .unwrap();

        // Send batch via sender
        let messages: Vec<TestMessage> = (0..50)
            .map(|i| TestMessage {
                id: i,
                content: format!("Batch {}", i),
            })
            .collect();

        tx.send_batch(messages.clone()).await.unwrap();

        // Receive batch via receiver
        let received = rx.recv_batch(50).await.unwrap();
        assert_eq!(received.len(), 50);
        assert_eq!(received, messages);
    }

    #[tokio::test]
    async fn test_batch_performance_comparison() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let queue_single = DiskBackedQueue::new(temp_file1.path(), "single".to_string(), None)
            .await
            .unwrap();
        let queue_batch = DiskBackedQueue::new(temp_file2.path(), "batch".to_string(), None)
            .await
            .unwrap();

        let message_count = 1000;
        let messages: Vec<TestMessage> = (0..message_count)
            .map(|i| TestMessage {
                id: i,
                content: format!("Perf test {}", i),
            })
            .collect();

        // Single send
        let start = std::time::Instant::now();
        for msg in messages.clone() {
            queue_single.send(msg).await.unwrap();
        }
        let single_duration = start.elapsed();

        // Batch send
        let start = std::time::Instant::now();
        queue_batch.send_batch(messages.clone()).await.unwrap();
        let batch_duration = start.elapsed();

        println!(
            "Single send: {:?}, Batch send: {:?}, Speedup: {:.2}x",
            single_duration,
            batch_duration,
            single_duration.as_secs_f64() / batch_duration.as_secs_f64()
        );

        // Batch should be significantly faster (at least 5x)
        assert!(batch_duration < single_duration / 5);
    }

    #[tokio::test]
    async fn test_batch_with_corrupted_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "batch_corrupt_test".to_string(), None)
            .await
            .unwrap();

        // Send 10 valid messages
        for i in 0..10 {
            let msg = TestMessage {
                id: i,
                content: format!("Valid {}", i),
            };
            queue.send(msg).await.unwrap();
        }

        // Corrupt message 5
        {
            let db = queue.db.lock().unwrap();
            let garbage_data: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0xFC];
            db.execute(
                "UPDATE batch_corrupt_test SET data = ? WHERE id = 6",
                [&garbage_data],
            )
            .unwrap();
        }

        // Batch receive should skip corrupted message and move it to DLQ
        let batch = queue.recv_batch(10).await.unwrap();
        assert_eq!(batch.len(), 9); // Should get 9 valid messages (10 - 1 corrupted)

        // Verify DLQ has 1 entry
        let dlq_path = temp_file.path().with_extension("dlq.db");
        let dlq_conn = rusqlite::Connection::open(&dlq_path).unwrap();
        let dlq_count: i64 = dlq_conn
            .query_row("SELECT COUNT(*) FROM batch_corrupt_test_dlq", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(dlq_count, 1);
    }

    /// Concurrent senders racing into a bounded queue must never exceed `max_size`.
    ///
    /// Before the BEGIN IMMEDIATE fix, two senders that both observed `count == max-1`
    /// could both insert and push the queue to `max+1`. With the atomic count+insert,
    /// `len()` should never exceed `max` from any observation point.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_max_size_never_exceeded_under_contention() {
        const MAX: usize = 10;
        const SENDERS: usize = 50;
        const PER_SENDER: usize = 5;

        let temp_file = NamedTempFile::new().unwrap();
        // Use the queue directly so both producer and consumer can share it via Arc.
        let queue = std::sync::Arc::new(
            DiskBackedQueue::<TestMessage>::new(
                temp_file.path(),
                "race_test".to_string(),
                Some(MAX),
            )
            .await
            .unwrap(),
        );

        // Spawn a sampler that watches `len()` while senders contend. If the bound is
        // ever violated, the assertion fires here.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler = {
            let queue = queue.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let len = queue.len().await.unwrap();
                    assert!(
                        len <= MAX,
                        "Queue length {len} exceeded max_size {MAX} — race fix regressed"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                }
            })
        };

        // Spawn senders that race to fill the queue. Senders block when full and unblock
        // as a separate consumer task drains items.
        let mut send_handles = Vec::new();
        for sender_id in 0..SENDERS {
            let queue = queue.clone();
            send_handles.push(tokio::spawn(async move {
                for j in 0..PER_SENDER {
                    queue
                        .send(TestMessage {
                            id: (sender_id * PER_SENDER + j) as u64,
                            content: String::new(),
                        })
                        .await
                        .unwrap();
                }
            }));
        }

        // Drain in the background so senders eventually unblock.
        let drainer = {
            let queue = queue.clone();
            tokio::spawn(async move {
                let total = SENDERS * PER_SENDER;
                let mut received = 0;
                while received < total {
                    if queue.recv().await.unwrap().is_some() {
                        received += 1;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                }
            })
        };

        for h in send_handles {
            h.await.unwrap();
        }
        drainer.await.unwrap();

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        sampler.await.unwrap();

        assert!(queue.is_empty().await.unwrap());
    }

    /// `blocking_send` from a current-thread runtime must not panic.
    ///
    /// `tokio::task::block_in_place` panics on the current-thread runtime, so the
    /// implementation falls back to a temporary runtime in that case.
    #[tokio::test] // default flavor is current_thread
    async fn test_blocking_send_on_current_thread_runtime() {
        let temp_file = NamedTempFile::new().unwrap();
        let (tx, mut rx) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "blocking_ct_test".to_string(),
            None,
        )
        .await
        .unwrap();

        let msg = TestMessage {
            id: 7,
            content: "from current-thread".to_string(),
        };

        // Run blocking_send on a separate OS thread so we don't block the current-thread
        // runtime that's hosting the test (which would deadlock the recv below).
        let tx_clone = tx.clone();
        let msg_clone = msg.clone();
        let handle =
            std::thread::spawn(move || tx_clone.blocking_send(msg_clone));
        // Hand back to the runtime so the spawned thread can make progress on its own
        // temporary runtime without contention with this current_thread executor.
        tokio::task::yield_now().await;
        handle.join().expect("thread panicked").unwrap();

        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(received, msg);
    }

    /// `send_batch` rejects batches larger than `max_size` immediately rather than
    /// blocking forever waiting for space that can never exist.
    #[tokio::test]
    async fn test_send_batch_rejects_oversized_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue =
            DiskBackedQueue::new(temp_file.path(), "oversize_test".to_string(), Some(5))
                .await
                .unwrap();

        let oversized: Vec<TestMessage> = (0..10)
            .map(|i| TestMessage {
                id: i,
                content: String::new(),
            })
            .collect();

        let result = queue.send_batch(oversized).await;
        assert!(
            matches!(result, Err(DiskQueueError::QueueFull(5))),
            "Expected QueueFull(5), got: {result:?}"
        );

        // Queue should still be empty and usable.
        assert!(queue.is_empty().await.unwrap());
        let small_batch = vec![
            TestMessage {
                id: 0,
                content: String::new(),
            };
            3
        ];
        queue.send_batch(small_batch).await.unwrap();
        assert_eq!(queue.len().await.unwrap(), 3);
    }

    /// `clear()` removes all messages, leaves the queue functional for further sends/recvs,
    /// and is a no-op when the queue is already empty.
    #[tokio::test]
    async fn test_clear_empties_queue_and_remains_usable() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue =
            DiskBackedQueue::new(temp_file.path(), "clear_test".to_string(), None)
                .await
                .unwrap();

        // Clear on an empty queue should be a no-op.
        queue.clear().await.unwrap();
        assert!(queue.is_empty().await.unwrap());

        // Populate, clear, verify empty.
        for i in 0..25 {
            queue
                .send(TestMessage {
                    id: i,
                    content: format!("msg {i}"),
                })
                .await
                .unwrap();
        }
        assert_eq!(queue.len().await.unwrap(), 25);

        queue.clear().await.unwrap();
        assert!(queue.is_empty().await.unwrap());
        assert_eq!(queue.len().await.unwrap(), 0);
        assert!(queue.recv().await.unwrap().is_none());

        // Queue must remain usable after clear.
        let after = TestMessage {
            id: 999,
            content: "after-clear".to_string(),
        };
        queue.send(after.clone()).await.unwrap();
        assert_eq!(queue.recv().await.unwrap().unwrap(), after);
    }
}
