//! # disk-backed-queue
//!
//! A robust, crash-resistant queue implementation that persists all data to disk using SQLite.
//! Provides an mpsc-like channel API while ensuring messages survive application restarts and
//! system failures.
//!
//! ## Features
//!
//! - **Durable Enqueue**: `send` returns only after the message is committed to SQLite at the
//!   configured [`DurabilityLevel`]
//! - **Cancellation-Safe Receive**: a message is removed only once the receiving future has taken
//!   it; a receive that is cancelled (`select!`, timeouts, task abort) leaves it queued
//! - **Crash Recovery**: Messages survive application restarts and system crashes
//! - **MPSC-like API**: Familiar channel-based interface compatible with async Rust
//! - **Dead Letter Queue**: Messages that fail to decode are atomically moved to a dead-letter
//!   table ([`DLQ_TABLE_NAME`]) in the same database file
//! - **Batch Operations**: Bulk send/receive share one transaction and one fsync
//! - **Backpressure Support**: Optional queue size limits prevent unbounded growth
//!
//! ## Delivery Semantics
//!
//! Receiving is destructive: there is no acknowledgment step, so a consumer that crashes while
//! processing a received message loses it. Up to that point delivery is at-least-once: the
//! deletion commits just after `recv` hands the message over, so a crash (or a storage error)
//! in that window means the message is delivered again. Making processing idempotent handles
//! such duplicates; it cannot recover a message lost during processing.
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
//! Suited to single-machine workloads where messages must survive process termination or power
//! loss while they sit in the queue:
//!
//! - Message queuing during database outages
//! - Event sourcing and audit logs
//! - Job queues that survive restarts
//! - Local spools in front of a network service
//!
//! ## Performance
//!
//! Single operations are bound by one fsync per commit. Batch operations share one transaction,
//! so they are typically tens of times faster. For high-throughput scenarios, use
//! [`DiskBackedSender::send_batch`] and [`DiskBackedReceiver::recv_batch`].
//!
//! Run `cargo run --example throughput_demo --release` to see performance on your system.

use rusqlite::{OptionalExtension, Transaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tracing::{debug, error, instrument, warn};

/// Name of the internal table that stores dead letters for every queue in a database file.
///
/// Rows record the source queue (`queue_name`), the message's `original_id`, its raw `data`,
/// the decoding `error_message`, the original `created_at`, and the `moved_at` time.
pub const DLQ_TABLE_NAME: &str = "__disk_queue_dlq";

/// Table-name prefixes reserved for SQLite and for this library's internal tables (compared
/// case-insensitively, as SQLite does).
const RESERVED_TABLE_PREFIXES: [&str; 2] = ["sqlite_", "__disk_queue"];

/// Maximum table-name length, in bytes of UTF-8.
const MAX_TABLE_NAME_BYTES: usize = 128;

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times an operation is retried after SQLite reports the database busy, which
/// means another connection held the write lock for the whole [`BUSY_TIMEOUT`].
const BUSY_RETRIES: usize = 2;

/// While senders wait on a full bounded queue, they are woken this often to re-check capacity,
/// so they notice space freed by another process or an independently opened queue on the same
/// file. Space freed through the same queue instance wakes them immediately.
const CAPACITY_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long a receive transaction waits for the receiving future to take its messages before
/// rolling back. It only expires when the future is kept alive but not polled, and is kept well
/// below [`BUSY_TIMEOUT`] so other connections waiting for the lock are not failed meanwhile.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
#[non_exhaustive]
pub enum DiskQueueError {
    Database(rusqlite::Error),
    Serialization(postcard::Error),
    Deserialization(postcard::Error),
    InvalidTableName(String),
    /// A constructor argument other than the table name is invalid, e.g. a `max_size` of zero.
    InvalidConfiguration(String),
    /// The method cannot run in the calling context, e.g. `blocking_send` on a current-thread
    /// Tokio runtime.
    UnsupportedRuntime(String),
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
            Self::InvalidConfiguration(s) => write!(f, "Invalid configuration: {s}"),
            Self::UnsupportedRuntime(s) => write!(f, "Unsupported runtime context: {s}"),
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

fn join_error(e: tokio::task::JoinError) -> DiskQueueError {
    DiskQueueError::TaskJoin(e.to_string())
}

/// Decides whether to retry an operation that failed with `e`: true for the first
/// [`BUSY_RETRIES`] times SQLite reports the database busy. SQLite's busy handler has already
/// waited for the lock by then, so the retry needs no further delay. Busy errors only occur
/// before anything was written, so retrying is safe.
fn should_retry_busy(e: &DiskQueueError, retries: &mut usize, table_name: &str) -> bool {
    let busy = matches!(
        e,
        DiskQueueError::Database(rusqlite::Error::SqliteFailure(failure, _))
            if failure.code == rusqlite::ErrorCode::DatabaseBusy
    );
    if !busy || *retries >= BUSY_RETRIES {
        return false;
    }
    *retries += 1;
    warn!(
        "Database for table {} is busy: {}. Retrying ({}/{})",
        table_name, e, retries, BUSY_RETRIES
    );
    true
}

/// Wakes the thread that is blocked in [`wait_on_thread`].
struct ThreadWaker(std::thread::Thread);

impl std::task::Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Blocks the calling thread until `future` completes. Only for futures that are woken from
/// other threads without a Tokio runtime driving them, such as [`Notify`] waits.
fn wait_on_thread<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

/// Durability configuration for SQLite synchronous mode
///
/// This controls the trade-off between durability and performance. The queue always uses WAL
/// mode; the descriptions below are for WAL mode on storage that honours fsync. At every level,
/// an application crash loses no committed message.
#[derive(Debug, Clone, Copy, Default)]
pub enum DurabilityLevel {
    /// OFF (0) - Never fsyncs. An OS crash or power loss can lose committed messages and can
    /// corrupt the database.
    /// **WARNING**: Only use for temporary/cache data where loss is acceptable
    Off,

    /// NORMAL (1) - Fsyncs only during WAL checkpoints. The database stays consistent, but an OS
    /// crash or power loss can roll back the most recent commits.
    Normal,

    /// FULL (2) - Fsyncs the WAL on every commit (DEFAULT)
    /// Committed messages survive OS crashes and power loss
    /// Recommended for critical data where message loss is unacceptable
    #[default]
    Full,

    /// EXTRA (3) - Same durability as FULL in WAL mode. Its extra directory fsync only applies
    /// to rollback-journal mode, which this queue does not use.
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

/// Accepts names of 1-128 bytes made of Unicode letters, digits and underscores, outside the
/// reserved prefixes.
fn validate_table_name(table_name: &str) -> Result<()> {
    let lowercase = table_name.to_ascii_lowercase();
    let valid = !table_name.is_empty()
        && table_name.len() <= MAX_TABLE_NAME_BYTES
        && table_name.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| lowercase.starts_with(prefix));
    if valid {
        Ok(())
    } else {
        Err(DiskQueueError::InvalidTableName(table_name.to_string()))
    }
}

/// Quotes an SQL identifier so keywords (`table`) and digit-leading names (`1queue`) are valid.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(item).map_err(|e| {
        error!("Failed to serialize item for queue: {}", e);
        DiskQueueError::Serialization(e)
    })
}

/// Decodes a stored payload, requiring every byte to be consumed: a payload with trailing bytes
/// was corrupted or written by a different message type, so it is dead-lettered instead of
/// being silently truncated. On failure, also returns the description recorded in the DLQ.
///
/// A panicking `Deserialize` implementation counts as a decoding failure too, so the payload is
/// dead-lettered instead of the panic poisoning the connection lock for every later operation.
fn decode<T: DeserializeOwned>(data: &[u8]) -> std::result::Result<T, (postcard::Error, String)> {
    let decoded = match std::panic::catch_unwind(|| postcard::take_from_bytes::<T>(data)) {
        Ok(decoded) => decoded,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("non-string panic payload");
            return Err((
                postcard::Error::SerdeDeCustom,
                format!("Deserialize implementation panicked: {message}"),
            ));
        }
    };
    match decoded {
        Ok((item, [])) => Ok(item),
        Ok((_, rest)) => Err((
            postcard::Error::DeserializeBadEncoding,
            format!(
                "{} unexpected trailing byte(s) after a complete message",
                rest.len()
            ),
        )),
        Err(e) => {
            let description = e.to_string();
            Err((e, description))
        }
    }
}

/// Coordinates a receive transaction, running on a blocking thread, with the future that asked
/// for the messages.
///
/// The transaction deletes the messages but only commits once the future has accepted them.
/// If the future is dropped first, or is not polled within [`HANDOFF_TIMEOUT`], the transaction
/// rolls back and the messages stay queued. A started blocking task cannot be aborted, so this is
/// what keeps a cancelled `recv` from deleting messages nobody received.
#[derive(Default)]
struct Handoff {
    state: Mutex<HandoffState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum HandoffState {
    #[default]
    Pending,
    Accepted,
    Abandoned,
}

enum Acceptance {
    Accepted,
    Cancelled,
    TimedOut,
}

impl Handoff {
    /// Moves a pending handoff to `next`; returns the resulting state.
    fn resolve(&self, next: HandoffState) -> HandoffState {
        // The state is a plain enum, so a poisoned lock cannot hold a broken value.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if *state == HandoffState::Pending {
            *state = next;
            self.changed.notify_all();
        }
        *state
    }

    /// Called by the receiving future; returns `true` if the messages are now its own.
    fn accept(&self) -> bool {
        self.resolve(HandoffState::Accepted) == HandoffState::Accepted
    }

    /// Called by the receive transaction after handing the messages over.
    fn wait_for_acceptance(&self, timeout: Duration) -> Acceptance {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| *state == HandoffState::Pending)
            .unwrap_or_else(PoisonError::into_inner);
        match *state {
            HandoffState::Accepted => Acceptance::Accepted,
            HandoffState::Abandoned => Acceptance::Cancelled,
            HandoffState::Pending => {
                *state = HandoffState::Abandoned;
                Acceptance::TimedOut
            }
        }
    }
}

/// Abandons the handoff if the receiving future is dropped before accepting.
struct AbandonOnDrop(Arc<Handoff>);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        self.0.resolve(HandoffState::Abandoned);
    }
}

type Reply<V> = oneshot::Sender<Result<V>>;

/// Cached SQL query strings for the queue operations.
///
/// Pre-formatted SQL queries are cached to avoid repeated string formatting
/// on every operation. The table name is validated and quoted once at
/// queue creation time, ensuring SQL injection safety.
#[derive(Debug)]
struct CachedQueries {
    insert: String,
    select_oldest: String,
    select_after: String,
    delete_one: String,
    delete_through: String,
    count: String,
    id_span: String,
    exists: String,
    clear: String,
    dead_letter: String,
}

impl CachedQueries {
    fn new(table_name: &str) -> Self {
        let table = quote_identifier(table_name);
        let dlq = quote_identifier(DLQ_TABLE_NAME);
        Self {
            insert: format!("INSERT INTO {table} (data) VALUES (?1)"),
            // FIFO follows the AUTOINCREMENT id, never `created_at`: wall-clock time can go
            // backwards.
            select_oldest: format!("SELECT id, data FROM {table} ORDER BY id LIMIT 1"),
            select_after: format!(
                "SELECT id, data FROM {table} WHERE id > ?1 ORDER BY id LIMIT ?2"
            ),
            delete_one: format!("DELETE FROM {table} WHERE id = ?1"),
            delete_through: format!("DELETE FROM {table} WHERE id <= ?1"),
            count: format!("SELECT COUNT(*) FROM {table}"),
            // An upper bound on the row count, answered from the primary key in O(log n)
            // where COUNT(*) reads every row. It is exact while ids are contiguous, which every
            // queue operation preserves: inserts append ids and removals take the oldest rows.
            // Rows deleted out of order outside the library make it over-count, which can only
            // apply backpressure early, never let the queue exceed `max_size`. The separate
            // subqueries keep SQLite's MIN/MAX optimization, which a combined aggregate loses.
            id_span: format!(
                "SELECT COALESCE((SELECT MAX(id) FROM {table}) - (SELECT MIN(id) FROM {table}) + 1, 0)"
            ),
            exists: format!("SELECT EXISTS(SELECT 1 FROM {table})"),
            clear: format!("DELETE FROM {table}"),
            dead_letter: format!(
                "INSERT INTO {dlq} (queue_name, original_id, data, error_message, created_at) \
                 SELECT ?1, id, data, ?2, created_at FROM {table} WHERE id = ?3"
            ),
        }
    }
}

enum InsertAttempt {
    Committed,
    Full { count: usize },
}

enum Oldest<T> {
    Empty,
    Item(T),
    DeadLettered(postcard::Error),
}

/// Queue state shared between a queue handle and the blocking tasks it spawns.
struct Inner {
    db: Mutex<rusqlite::Connection>,
    queries: CachedQueries,
    table_name: String,
    max_size: Option<usize>,
    /// Wakes senders waiting for capacity once this queue frees space.
    space_freed: Notify,
    /// Number of senders waiting for capacity; see [`Inner::ensure_capacity_poller`].
    capacity_waiters: AtomicUsize,
    capacity_poller_running: AtomicBool,
}

/// Counts a sender as waiting for capacity for as long as it is alive.
struct CapacityWaiter(Arc<Inner>);

impl CapacityWaiter {
    fn new(inner: &Arc<Inner>, count: usize, batch_size: usize) -> Self {
        warn!(
            table_name = %inner.table_name,
            current_size = count,
            max_size = ?inner.max_size,
            batch_size = batch_size,
            "Queue is full, waiting for space..."
        );
        inner.capacity_waiters.fetch_add(1, Ordering::SeqCst);
        inner.ensure_capacity_poller();
        Self(Arc::clone(inner))
    }
}

impl Drop for CapacityWaiter {
    fn drop(&mut self) {
        self.0.capacity_waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Inner {
    /// Opens the database and creates the queue and DLQ tables. Every step is idempotent, so an
    /// initialization whose caller stopped waiting can simply be run again.
    fn open(
        path: &Path,
        table_name: String,
        max_size: Option<usize>,
        durability: DurabilityLevel,
    ) -> Result<Self> {
        let conn = rusqlite::Connection::open(path).map_err(|e| {
            error!(
                "Failed to open SQLite database at {}: {}",
                path.display(),
                e
            );
            e
        })?;

        // Set busy timeout to handle concurrent access, before anything else can need a lock
        conn.busy_timeout(BUSY_TIMEOUT).map_err(|e| {
            error!("Failed to set busy timeout: {}", e);
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

        let table = quote_identifier(&table_name);
        // Earlier versions ordered messages through this index; ordering now uses the primary
        // key, so the index would only slow down inserts.
        let legacy_index = quote_identifier(&format!("idx_{table_name}_created_at"));
        let dlq = quote_identifier(DLQ_TABLE_NAME);
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            DROP INDEX IF EXISTS {legacy_index};
            CREATE TABLE IF NOT EXISTS {dlq} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                original_id INTEGER NOT NULL,
                data BLOB NOT NULL,
                error_message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                moved_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            );"
        ))
        .map_err(|e| {
            error!("Failed to create tables for queue {}: {}", table_name, e);
            e
        })?;

        debug!(
            "Successfully initialized disk-backed queue table: {}",
            table_name
        );

        // Earlier versions kept dead letters in a separate `<stem>.dlq.db` file, which is no
        // longer read or written.
        let legacy_dlq = path.with_extension("dlq.db");
        if legacy_dlq.exists() {
            warn!(
                "Found legacy dead letter database {}. Dead letters are now stored in the {} table of {}; the legacy file is left untouched.",
                legacy_dlq.display(),
                DLQ_TABLE_NAME,
                path.display()
            );
        }

        Ok(Self {
            db: Mutex::new(conn),
            queries: CachedQueries::new(&table_name),
            table_name,
            max_size,
            space_freed: Notify::new(),
            capacity_waiters: AtomicUsize::new(0),
            capacity_poller_running: AtomicBool::new(false),
        })
    }

    /// Starts the capacity poller unless it is running. While any sender waits for capacity, it
    /// wakes all waiters every [`CAPACITY_POLL_INTERVAL`] so they notice space freed through
    /// other connections, which cannot notify `space_freed`. It is a plain thread rather than a
    /// Tokio timer, so waiting works on runtimes without the time driver, and outside any.
    fn ensure_capacity_poller(self: &Arc<Self>) {
        if self.capacity_poller_running.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name("disk-queue-capacity-poller".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(CAPACITY_POLL_INTERVAL);
                    let Some(inner) = inner.upgrade() else {
                        return;
                    };
                    if inner.capacity_waiters.load(Ordering::SeqCst) == 0 {
                        inner.capacity_poller_running.store(false, Ordering::SeqCst);
                        // A sender that started waiting meanwhile may have seen this poller as
                        // running. Keep going for it, unless it already started a new poller.
                        if inner.capacity_waiters.load(Ordering::SeqCst) == 0
                            || inner.capacity_poller_running.swap(true, Ordering::SeqCst)
                        {
                            return;
                        }
                    }
                    inner.space_freed.notify_waiters();
                }
            });
        if let Err(e) = spawned {
            self.capacity_poller_running.store(false, Ordering::SeqCst);
            error!(
                "Failed to start the capacity poller for table {}: {}. Waiting senders will only notice space freed through this queue.",
                self.table_name, e
            );
        }
    }

    /// Starts a transaction that holds the database write lock from the start, so later
    /// reads cannot be invalidated by other connections' writes before this one modifies the
    /// queue.
    fn begin<'c>(&self, conn: &'c mut rusqlite::Connection) -> Result<Transaction<'c>> {
        conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| {
                error!(
                    "Failed to start transaction for table {}: {}",
                    self.table_name, e
                );
                DiskQueueError::Database(e)
            })
    }

    fn count(conn: &rusqlite::Connection, count_sql: &str) -> Result<usize> {
        let count: i64 = conn
            .prepare_cached(count_sql)?
            .query_row([], |row| row.get(0))?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    /// Makes one attempt to enqueue all `payloads` atomically. Returns `Full` instead of waiting
    /// when there is not enough room, so a blocking worker is never held indefinitely.
    fn try_insert(&self, payloads: &[Vec<u8>]) -> Result<InsertAttempt> {
        let mut conn = lock_conn(&self.db)?;
        // The capacity check and insert share one BEGIN IMMEDIATE transaction so concurrent
        // senders cannot both observe `count == max-1` and both insert.
        let tx = self.begin(&mut conn)?;

        if let Some(max) = self.max_size {
            let count = Self::count(&tx, &self.queries.id_span)?;
            if count.saturating_add(payloads.len()) > max {
                return Ok(InsertAttempt::Full { count });
            }
        }

        {
            let mut insert = tx.prepare_cached(&self.queries.insert)?;
            for payload in payloads {
                let rows_affected = insert.execute([payload]).map_err(|e| {
                    error!(
                        "Failed to insert item into queue table {}: {}",
                        self.table_name, e
                    );
                    DiskQueueError::Database(e)
                })?;
                if rows_affected != 1 {
                    error!("Expected to insert 1 row, but inserted {}", rows_affected);
                    return Err(DiskQueueError::UnexpectedRowCount(format!(
                        "Insert affected {rows_affected} rows instead of 1"
                    )));
                }
            }
        }

        tx.commit().map_err(|e| {
            error!(
                "Failed to commit insert for table {}: {}",
                self.table_name, e
            );
            DiskQueueError::Database(e)
        })?;
        Ok(InsertAttempt::Committed)
    }

    /// Enqueues on the calling thread, blocking it while the queue is full. The synchronous
    /// counterpart of `DiskBackedQueue::enqueue`, for callers that are allowed to block; it
    /// needs no runtime and no thread from Tokio's blocking pool.
    fn insert_blocking(self: &Arc<Self>, payloads: &[Vec<u8>]) -> Result<()> {
        let mut waiter = None;
        let mut busy_retries = 0;
        loop {
            // Register for a wakeup before checking capacity, so space freed between the check
            // and the wait below is not missed.
            let mut space_freed = std::pin::pin!(self.space_freed.notified());
            space_freed.as_mut().enable();

            match self.try_insert(payloads) {
                Ok(InsertAttempt::Committed) => return Ok(()),
                Ok(InsertAttempt::Full { count }) => {
                    waiter.get_or_insert_with(|| CapacityWaiter::new(self, count, payloads.len()));
                    wait_on_thread(space_freed);
                }
                Err(e) if should_retry_busy(&e, &mut busy_retries, &self.table_name) => {}
                Err(e) => return Err(e),
            }
        }
    }

    fn notify_space_freed(&self) {
        if self.max_size.is_some() {
            self.space_freed.notify_waiters();
        }
    }

    /// Commits a transaction that removed rows, then wakes senders waiting for capacity.
    fn commit_removal(&self, tx: Transaction<'_>) -> Result<()> {
        tx.commit().map_err(|e| {
            error!(
                "Failed to commit transaction for table {}: {}",
                self.table_name, e
            );
            DiskQueueError::Database(e)
        })?;
        self.notify_space_freed();
        Ok(())
    }

    fn delete(tx: &Transaction<'_>, delete_sql: &str, id: i64, expected: usize) -> Result<()> {
        let rows_deleted = tx.prepare_cached(delete_sql)?.execute([id])?;
        if rows_deleted != expected {
            error!(
                "Expected to delete {} rows, but deleted {} rows (id {})",
                expected, rows_deleted, id
            );
            return Err(DiskQueueError::UnexpectedRowCount(format!(
                "Delete affected {rows_deleted} rows instead of {expected} (id {id})"
            )));
        }
        Ok(())
    }

    /// Moves the row `id`, which failed to decode, to the dead letter table. Runs inside the
    /// caller's transaction, so the copy and the removal commit or roll back together.
    fn dead_letter(&self, tx: &Transaction<'_>, id: i64, reason: &str) -> Result<()> {
        error!(
            "Failed to deserialize item {} from table {}: {}. Moving to DLQ.",
            id, self.table_name, reason
        );
        let copied = tx
            .prepare_cached(&self.queries.dead_letter)
            .and_then(|mut stmt| stmt.execute(rusqlite::params![self.table_name, reason, id]))
            .map_err(|e| {
                error!(
                    "Failed to write corrupt item {} from table {} to DLQ: {}. Item left in main queue.",
                    id, self.table_name, e
                );
                DiskQueueError::DlqWriteFailed(e.to_string())
            })?;
        if copied != 1 {
            return Err(DiskQueueError::DlqWriteFailed(format!(
                "DLQ insert for item {id} affected {copied} rows instead of 1"
            )));
        }
        Self::delete(tx, &self.queries.delete_one, id, 1)
    }

    fn take_oldest<T: DeserializeOwned>(&self, tx: &Transaction<'_>) -> Result<Oldest<T>> {
        let row: Option<(i64, Vec<u8>)> = tx
            .prepare_cached(&self.queries.select_oldest)?
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(|e| {
                error!(
                    "Failed to execute SELECT query on table {}: {}",
                    self.table_name, e
                );
                DiskQueueError::Database(e)
            })?;
        let Some((id, data)) = row else {
            return Ok(Oldest::Empty);
        };

        match decode::<T>(&data) {
            Ok(item) => {
                Self::delete(tx, &self.queries.delete_one, id, 1)?;
                Ok(Oldest::Item(item))
            }
            Err((e, reason)) => {
                self.dead_letter(tx, id, &reason)?;
                Ok(Oldest::DeadLettered(e))
            }
        }
    }

    /// Takes up to `limit` decodable messages in FIFO order, dead-lettering undecodable rows on
    /// the way. Keeps scanning past dead-lettered rows, so an empty result means no deliverable
    /// message is left. Returns the messages and the number of rows dead-lettered.
    fn take_batch<T: DeserializeOwned>(
        &self,
        tx: &Transaction<'_>,
        limit: usize,
    ) -> Result<(Vec<T>, usize)> {
        let mut items = Vec::new();
        let mut dead_lettered = 0;
        let mut last_id = i64::MIN;
        let mut select = tx.prepare_cached(&self.queries.select_after)?;

        while items.len() < limit {
            let wanted = limit - items.len();
            let rows = select
                .query_map(
                    rusqlite::params![last_id, i64::try_from(wanted).unwrap_or(i64::MAX)],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| {
                    error!("Failed to read rows from table {}: {}", self.table_name, e);
                    DiskQueueError::Database(e)
                })?;
            let exhausted = rows.len() < wanted;

            for (id, data) in rows {
                last_id = id;
                match decode::<T>(&data) {
                    Ok(item) => items.push(item),
                    Err((_, reason)) => {
                        self.dead_letter(tx, id, &reason)?;
                        dead_lettered += 1;
                    }
                }
            }
            if exhausted {
                break;
            }
        }
        drop(select);

        if !items.is_empty() {
            // Every row up to `last_id` was either taken or already dead-lettered, and the
            // IMMEDIATE transaction keeps other writers out, so this removes exactly the
            // taken rows.
            Self::delete(tx, &self.queries.delete_through, last_id, items.len())?;
        }
        Ok((items, dead_lettered))
    }

    fn is_empty(conn: &rusqlite::Connection, exists_sql: &str) -> Result<bool> {
        // Cheap O(1) check that stops at the first row instead of doing a full COUNT(*).
        let exists: i64 = conn
            .prepare_cached(exists_sql)?
            .query_row([], |row| row.get(0))?;
        Ok(exists == 0)
    }

    /// Runs `job` inside a receive transaction on the current (blocking) thread, replying with
    /// any error that prevents the transaction from starting. Replies `empty()` straight away if
    /// the queue is empty.
    fn in_receive_tx<V>(
        &self,
        reply: Reply<V>,
        empty: impl FnOnce() -> V,
        job: impl FnOnce(Transaction<'_>, Reply<V>),
    ) {
        let mut conn = match lock_conn(&self.db) {
            Ok(conn) => conn,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        // Consumers often poll an empty queue. Answering that with a plain read spares them the
        // write lock, which other connections would otherwise have to queue behind.
        match Self::is_empty(&conn, &self.queries.exists) {
            Ok(true) => {
                let _ = reply.send(Ok(empty()));
                return;
            }
            Ok(false) => {}
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        }
        match self.begin(&mut conn) {
            Ok(tx) => job(tx, reply),
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Hands `value` to the receiving future and commits the deletions only once the future
    /// accepts it (see [`Handoff`]).
    fn commit_on_handoff<V>(
        &self,
        tx: Transaction<'_>,
        value: V,
        handoff: &Handoff,
        reply: Reply<V>,
    ) {
        if reply.send(Ok(value)).is_err() {
            debug!(
                "Receive on table {} was cancelled before delivery; rolled back",
                self.table_name
            );
            return;
        }
        match handoff.wait_for_acceptance(HANDOFF_TIMEOUT) {
            Acceptance::Accepted => {
                if let Err(e) = self.commit_removal(tx) {
                    error!(
                        "Failed to commit receive for table {}: {}. The delivered messages remain queued and will be delivered again.",
                        self.table_name, e
                    );
                }
            }
            Acceptance::Cancelled => debug!(
                "Receive on table {} was cancelled before delivery; rolled back",
                self.table_name
            ),
            Acceptance::TimedOut => warn!(
                "Receiver for table {} was not polled within {:?}; rolled back so the messages stay queued",
                self.table_name, HANDOFF_TIMEOUT
            ),
        }
    }

    fn recv_one<T: DeserializeOwned>(&self, handoff: &Handoff, reply: Reply<Option<T>>) {
        self.in_receive_tx(
            reply,
            || None,
            |tx, reply| {
                let result = match self.take_oldest::<T>(&tx) {
                    Ok(Oldest::Item(item)) => {
                        return self.commit_on_handoff(tx, Some(item), handoff, reply);
                    }
                    Ok(Oldest::Empty) => Ok(None),
                    // The DLQ move commits whether or not anyone is still waiting for the error.
                    Ok(Oldest::DeadLettered(e)) => self
                        .commit_removal(tx)
                        .and(Err(DiskQueueError::Deserialization(e))),
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            },
        );
    }

    fn recv_many<T: DeserializeOwned>(
        &self,
        limit: usize,
        handoff: &Handoff,
        reply: Reply<Vec<T>>,
    ) {
        self.in_receive_tx(reply, Vec::new, |tx, reply| {
            let result = match self.take_batch::<T>(&tx, limit) {
                Ok((items, _)) if !items.is_empty() => {
                    return self.commit_on_handoff(tx, items, handoff, reply);
                }
                Ok((_, 0)) => Ok(Vec::new()),
                Ok(_) => self.commit_removal(tx).map(|()| Vec::new()),
                Err(e) => Err(e),
            };
            let _ = reply.send(result);
        });
    }
}

/// A disk-backed queue that persists all messages to SQLite.
///
/// This is the core queue implementation. For most use cases, prefer using
/// [`disk_backed_channel`] which provides a more ergonomic sender/receiver API.
///
/// All operations, including construction, run their SQLite work in `spawn_blocking` so the
/// async executor is never blocked. Waiting for capacity happens asynchronously.
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
    inner: Arc<Inner>,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for DiskBackedQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskBackedQueue")
            .field("table_name", &self.inner.table_name)
            .field("max_size", &self.inner.max_size)
            .field("queries", &self.inner.queries)
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
    /// `table_name` must be 1-128 bytes of Unicode letters, digits and underscores, and must
    /// not start with the reserved prefixes `sqlite_` or `__disk_queue` (case-insensitive).
    /// `max_size` must be `None` (unbounded) or at least 1.
    ///
    /// If this future is dropped while initialization is running, the initialization still
    /// finishes in the background. It is idempotent, so calling the constructor again is safe.
    ///
    /// # Durability Levels
    ///
    /// - `DurabilityLevel::Full` (default) - Fsync on every commit; survives power loss
    /// - `DurabilityLevel::Normal` - Fsync at checkpoints; recent commits can be lost on power loss
    /// - `DurabilityLevel::Off` - No fsync; power loss can lose data or corrupt the database
    /// - `DurabilityLevel::Extra` - Same as `Full` in WAL mode
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
        validate_table_name(&table_name)?;
        if max_size == Some(0) {
            return Err(DiskQueueError::InvalidConfiguration(
                "max_size must be at least 1 (use None for an unbounded queue)".to_string(),
            ));
        }

        let path = db_path.as_ref().to_path_buf();
        let inner = tokio::task::spawn_blocking(move || {
            Inner::open(&path, table_name, max_size, durability)
        })
        .await
        .map_err(join_error)??;

        Ok(Self {
            inner: Arc::new(inner),
            _phantom: PhantomData,
        })
    }

    /// Send a single message to the queue.
    ///
    /// The message is serialized using postcard and committed to SQLite before this method
    /// returns. If the queue has a `max_size` configured and the queue is full, this method
    /// waits asynchronously until space becomes available.
    ///
    /// # Cancellation
    ///
    /// Dropping the future while it waits for capacity stops it; nothing is enqueued later.
    /// Dropping it while an insert is already running leaves the outcome unknown: the insert
    /// may still commit. Retrying after such a cancellation can therefore enqueue a duplicate.
    ///
    /// # Performance
    ///
    /// Single sends are limited by fsync overhead. For high throughput, use
    /// [`send_batch`](Self::send_batch) instead.
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name))]
    pub async fn send(&self, item: T) -> Result<()> {
        self.enqueue(vec![serialize(&item)?]).await?;
        debug!("Successfully enqueued item to disk queue");
        Ok(())
    }

    /// Send multiple messages in a single transaction for much better throughput.
    ///
    /// All messages in the batch are inserted atomically - either all succeed or all fail.
    /// This is significantly faster than individual sends because it uses a single SQLite
    /// transaction and fsync.
    ///
    /// A batch larger than `max_size` is rejected with [`DiskQueueError::QueueFull`]; otherwise
    /// this waits, like [`send`](Self::send), until there is room for the whole batch.
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name, batch_size = items.len()))]
    pub async fn send_batch(&self, items: Vec<T>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // Reject batches larger than max_size up front so we don't wait forever for space
        // that can never exist.
        if let Some(max) = self.inner.max_size
            && items.len() > max
        {
            return Err(DiskQueueError::QueueFull(max));
        }

        let payloads = items.iter().map(serialize).collect::<Result<Vec<_>>>()?;
        let batch_size = payloads.len();
        self.enqueue(payloads).await?;
        debug!(
            "Successfully enqueued {} items to disk queue in batch",
            batch_size
        );
        Ok(())
    }

    /// Inserts `payloads` atomically, waiting asynchronously while the queue lacks room. Each
    /// insert attempt is a short, finite blocking job, so waiting senders never tie up the
    /// blocking pool that receivers need in order to free space.
    async fn enqueue(&self, payloads: Vec<Vec<u8>>) -> Result<()> {
        let payloads = Arc::new(payloads);
        let mut waiter = None;
        let mut busy_retries = 0;
        loop {
            // Register for a wakeup before checking capacity, so space freed between the check
            // and the wait below is not missed.
            let space_freed = self.inner.space_freed.notified();
            tokio::pin!(space_freed);
            space_freed.as_mut().enable();

            let inner = Arc::clone(&self.inner);
            let attempt_payloads = Arc::clone(&payloads);
            let attempt = tokio::task::spawn_blocking(move || inner.try_insert(&attempt_payloads))
                .await
                .map_err(join_error)?;
            match attempt {
                Ok(InsertAttempt::Committed) => return Ok(()),
                Ok(InsertAttempt::Full { count }) => {
                    waiter.get_or_insert_with(|| {
                        CapacityWaiter::new(&self.inner, count, payloads.len())
                    });
                    // Woken when this queue frees space, or by the capacity poller.
                    space_freed.await;
                }
                Err(e) if should_retry_busy(&e, &mut busy_retries, &self.inner.table_name) => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Receive a single message from the queue.
    ///
    /// Returns the oldest message in FIFO order and removes it from the queue atomically.
    /// If the queue is empty, returns `Ok(None)` immediately (non-blocking).
    ///
    /// # Cancellation and delivery
    ///
    /// This is cancellation-safe: if the future is dropped before it returns, the message stays
    /// in the queue. The deletion commits right after the message is handed over, so a crash
    /// or storage failure at that moment means the message is delivered again (at-least-once).
    /// There is no acknowledgment: once the deletion has committed, the message is gone.
    ///
    /// Shutting the runtime down normally waits for that commit. Exiting without doing so
    /// (`std::process::exit`, `Runtime::shutdown_background`) right after this returns can leave
    /// the message queued, to be delivered again after a restart.
    ///
    /// Later operations on this queue, and receives on any connection, wait for that commit.
    /// Only a read-only query on another connection (e.g. `len` on an independently opened
    /// queue, or the `sqlite3` CLI) can still see the message, for about one fsync.
    ///
    /// # Dead Letter Queue
    ///
    /// If the oldest message fails to deserialize (e.g., due to schema changes, corruption, or
    /// trailing bytes), it is moved to the dead letter table ([`DLQ_TABLE_NAME`]) in the same
    /// database and a `Deserialization` error is returned. The queue is not blocked -
    /// subsequent calls will process the next message.
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name))]
    pub async fn recv(&self) -> Result<Option<T>> {
        let item = self
            .receive(|inner: &Inner, handoff: &Handoff, reply| inner.recv_one::<T>(handoff, reply))
            .await?;
        if item.is_some() {
            debug!("Successfully dequeued item from disk queue");
        }
        Ok(item)
    }

    /// Receive multiple messages in a single transaction for much better throughput.
    ///
    /// Returns up to `limit` messages from the queue in FIFO order. May return fewer if
    /// there aren't enough items. An empty `Vec` means the queue holds no deliverable message.
    ///
    /// All messages in the batch are removed atomically - either all succeed or all fail. The
    /// cancellation and delivery behavior is the same as for [`recv`](Self::recv).
    ///
    /// # Dead Letter Queue
    ///
    /// Messages that fail to deserialize are moved to the DLQ and skipped, and scanning
    /// continues past them until `limit` valid messages are found or the queue is exhausted.
    /// If a corrupt message cannot be moved to the DLQ, this returns `DlqWriteFailed` and the
    /// whole batch, valid messages included, stays queued.
    ///
    /// # Performance
    ///
    /// Batch operations are much faster than individual receives due to reduced
    /// transaction overhead.
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name, limit = limit))]
    pub async fn recv_batch(&self, limit: usize) -> Result<Vec<T>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let items = self
            .receive(move |inner: &Inner, handoff: &Handoff, reply| {
                inner.recv_many::<T>(limit, handoff, reply)
            })
            .await?;
        debug!(
            "Successfully dequeued {} items from disk queue in batch",
            items.len()
        );
        Ok(items)
    }

    /// Runs a receive `job` on a blocking thread and takes delivery of its result through a
    /// [`Handoff`]. If the job gave up waiting for this future and rolled back, it is retried.
    async fn receive<V, F>(&self, job: F) -> Result<V>
    where
        V: Send + 'static,
        F: Fn(&Inner, &Handoff, Reply<V>) + Clone + Send + 'static,
    {
        let mut busy_retries = 0;
        loop {
            let handoff = Arc::new(Handoff::default());
            let _abandon_on_drop = AbandonOnDrop(Arc::clone(&handoff));
            let (reply, response) = oneshot::channel();
            let inner = Arc::clone(&self.inner);
            let job_handoff = Arc::clone(&handoff);
            let job = job.clone();
            // The join handle is deliberately not awaited: the job commits only after `accept`
            // below, and awaiting that commit would add a cancellation point after the
            // messages are already ours.
            drop(tokio::task::spawn_blocking(move || {
                job(&inner, &job_handoff, reply)
            }));

            let value = match response.await.map_err(|_| {
                DiskQueueError::TaskJoin("receive task ended without a result".to_string())
            })? {
                Ok(value) => value,
                Err(e) if should_retry_busy(&e, &mut busy_retries, &self.inner.table_name) => {
                    continue;
                }
                Err(e) => return Err(e),
            };
            if handoff.accept() {
                return Ok(value);
            }
            debug!(
                table_name = %self.inner.table_name,
                "Receive was rolled back before this receiver accepted it; retrying"
            );
        }
    }

    /// Returns the number of messages currently in the queue.
    ///
    /// This counts every row, so it takes time proportional to the queue length (tens of
    /// milliseconds at 100,000 messages). Prefer [`is_empty`](Self::is_empty) when that is
    /// all you need.
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name))]
    pub async fn len(&self) -> Result<usize> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let conn = lock_conn(&inner.db)?;
            Inner::count(&conn, &inner.queries.count)
        })
        .await
        .map_err(join_error)?
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name))]
    pub async fn is_empty(&self) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let conn = lock_conn(&inner.db)?;
            Inner::is_empty(&conn, &inner.queries.exists)
        })
        .await
        .map_err(join_error)?
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
    #[instrument(skip_all, fields(table_name = %self.inner.table_name))]
    pub async fn clear(&self) -> Result<()> {
        let mut busy_retries = 0;
        loop {
            let inner = Arc::clone(&self.inner);
            let result = tokio::task::spawn_blocking(move || {
                let conn = lock_conn(&inner.db)?;
                conn.prepare_cached(&inner.queries.clear)?.execute([])?;
                inner.notify_space_freed();
                debug!("Cleared disk queue");
                Ok(())
            })
            .await
            .map_err(join_error)?;
            match result {
                Err(e) if should_retry_busy(&e, &mut busy_retries, &self.inner.table_name) => {}
                result => return result,
            }
        }
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
    /// This method blocks the current thread until the message is persisted, including while
    /// waiting for capacity in a full bounded queue.
    ///
    /// | Calling context | Behavior |
    /// | --- | --- |
    /// | Outside any Tokio runtime | Writes directly on the calling thread |
    /// | Inside a multi-thread runtime | Writes on the calling thread inside `block_in_place`, so other tasks keep running |
    /// | Inside a current-thread runtime | Returns [`DiskQueueError::UnsupportedRuntime`] |
    ///
    /// On a current-thread runtime, blocking the executor thread would stall the runtime, and
    /// with it any receiver that could free space. Tokio does not reveal whether the caller is
    /// that thread, so the call is refused from every thread that sees the runtime, including
    /// `spawn_blocking` threads. From async code use `send(..).await`; from a blocking thread,
    /// `tokio::runtime::Handle::current().block_on(sender.send(..))`.
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
        let insert = || {
            let payload = serialize(&item)?;
            self.queue
                .inner
                .insert_blocking(std::slice::from_ref(&payload))
        };
        match tokio::runtime::Handle::try_current() {
            Err(_) => insert(),
            // `block_in_place` only works on the multi-thread runtime; calling it from a
            // current-thread runtime panics. The insert runs on this thread rather than through
            // `send`, which would need another thread from the blocking pool.
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(insert)
            }
            Ok(_) => Err(DiskQueueError::UnsupportedRuntime(
                "blocking_send cannot block a current-thread Tokio runtime; use `send(..).await`, or `Handle::block_on(sender.send(..))` from a blocking thread"
                    .to_string(),
            )),
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
    /// See [`DiskBackedQueue::recv`] for details about cancellation, delivery, and dead letter
    /// queue handling.
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
    /// enough items. Returns an empty `Vec` if the queue holds no deliverable message.
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
/// * `table_name` - Name of the table to store messages. Must be 1-128 bytes of Unicode
///   letters, digits and underscores, not starting with `sqlite_` or `__disk_queue`.
/// * `max_size` - Optional maximum queue size (at least 1). When set, senders wait while the
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
/// * [`DurabilityLevel::Full`] (default) - Survives power loss
/// * [`DurabilityLevel::Normal`] - Faster; power loss can roll back the latest commits
/// * [`DurabilityLevel::Off`] - Fastest; power loss can lose data or corrupt the database
/// * [`DurabilityLevel::Extra`] - Same as `Full` in WAL mode
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
    use std::future::Future;
    use std::time::Instant;
    use tempfile::{NamedTempFile, TempDir};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestMessage {
        id: u64,
        content: String,
    }

    fn message(id: u64) -> TestMessage {
        TestMessage {
            id,
            content: format!("Message {id}"),
        }
    }

    /// Runs `sql` on the queue's own connection.
    fn exec_sql<T>(queue: &DiskBackedQueue<T>, sql: &str) {
        queue.inner.db.lock().unwrap().execute_batch(sql).unwrap();
    }

    /// Rewrites the stored payload of row `id`.
    fn edit_payload<T>(queue: &DiskBackedQueue<T>, id: i64, edit: impl FnOnce(&mut Vec<u8>)) {
        let conn = queue.inner.db.lock().unwrap();
        let table = quote_identifier(&queue.inner.table_name);
        let mut data: Vec<u8> = conn
            .query_row(
                &format!("SELECT data FROM {table} WHERE id = ?1"),
                [id],
                |row| row.get(0),
            )
            .unwrap();
        edit(&mut data);
        conn.execute(
            &format!("UPDATE {table} SET data = ?1 WHERE id = ?2"),
            rusqlite::params![data, id],
        )
        .unwrap();
    }

    /// Overwrites the stored payload of row `id` with bytes that are not valid postcard.
    fn corrupt_row<T>(queue: &DiskBackedQueue<T>, id: i64) {
        edit_payload(queue, id, |data| *data = vec![0xFF, 0xFE, 0xFD, 0xFC]);
    }

    /// Counts dead letters through a separate connection. Takes the write lock, like a
    /// receiver would, so it waits for receives whose deletion has not committed yet.
    fn dlq_count(db_path: &Path, queue_name: &str) -> i64 {
        let mut conn = rusqlite::Connection::open(db_path).unwrap();
        conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap()
            .query_row(
                &format!("SELECT COUNT(*) FROM {DLQ_TABLE_NAME} WHERE queue_name = ?1"),
                [queue_name],
                |row| row.get(0),
            )
            .unwrap()
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
        corrupt_row(&queue, 1);

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

    /// FIFO order follows insertion, even if the wall clock went backwards between sends.
    #[tokio::test]
    async fn test_fifo_survives_clock_rollback() {
        let temp_file = NamedTempFile::new().unwrap();
        {
            let queue = DiskBackedQueue::new(temp_file.path(), "clock_test".to_string(), None)
                .await
                .unwrap();
            for id in 1..=4 {
                queue.send(message(id)).await.unwrap();
            }
            // Give each newer message an older timestamp, as after clock corrections.
            exec_sql(&queue, "UPDATE clock_test SET created_at = 1000000 - id");
            assert_eq!(queue.recv().await.unwrap(), Some(message(1)));
        }

        // The order also holds after reopening.
        let queue = DiskBackedQueue::new(temp_file.path(), "clock_test".to_string(), None)
            .await
            .unwrap();
        assert_eq!(
            queue.recv_batch(2).await.unwrap(),
            vec![message(2), message(3)]
        );
        assert_eq!(queue.recv().await.unwrap(), Some(message(4)));
    }

    #[tokio::test]
    async fn test_table_name_validation() {
        let temp_file = NamedTempFile::new().unwrap();
        let too_long = "a".repeat(129);
        let too_many_bytes = "é".repeat(65); // 65 characters, 130 bytes
        for name in [
            "",
            "has-dash",
            "has space",
            "quote\"d",
            "semi;colon",
            "sqlite_master",
            "SQLITE_custom",
            "__disk_queue_dlq",
            "__DISK_QUEUE_other",
            too_long.as_str(),
            too_many_bytes.as_str(),
        ] {
            let result =
                DiskBackedQueue::<TestMessage>::new(temp_file.path(), name.to_string(), None).await;
            assert!(
                matches!(result, Err(DiskQueueError::InvalidTableName(_))),
                "{name:?}: {result:?}"
            );
        }
    }

    /// Names that are SQL keywords, start with a digit, or use non-ASCII letters work for
    /// every operation, including dead-lettering.
    #[tokio::test]
    async fn test_table_names_are_quoted() {
        let temp_file = NamedTempFile::new().unwrap();
        let longest = "a".repeat(128);
        for name in [
            "table",
            "select",
            "123messages",
            "Ünïcödé_队列",
            "orders_dlq",
            longest.as_str(),
        ] {
            let queue = DiskBackedQueue::new(temp_file.path(), name.to_string(), Some(10))
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            queue
                .send_batch(vec![message(1), message(2)])
                .await
                .unwrap();
            assert_eq!(queue.len().await.unwrap(), 2, "{name}");
            assert_eq!(queue.recv().await.unwrap(), Some(message(1)), "{name}");

            corrupt_row(&queue, 2);
            assert!(
                matches!(queue.recv().await, Err(DiskQueueError::Deserialization(_))),
                "{name}"
            );
            assert!(queue.is_empty().await.unwrap(), "{name}");
            assert_eq!(dlq_count(temp_file.path(), name), 1, "{name}");

            queue.send(message(3)).await.unwrap();
            queue.clear().await.unwrap();
            assert!(queue.recv_batch(10).await.unwrap().is_empty(), "{name}");
        }
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
    async fn test_dlq_table_lives_in_main_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("queue.db");

        disk_backed_channel::<TestMessage, _>(&path, "test".to_string(), None)
            .await
            .unwrap();

        // The DLQ table exists in the main database, and no separate DLQ file is created.
        assert_eq!(dlq_count(&path, "test"), 0);
        assert!(!path.with_extension("dlq.db").exists());
    }

    /// Databases with the same stem but different extensions keep separate dead letters.
    #[tokio::test]
    async fn test_dead_letters_stay_with_their_database() {
        let dir = TempDir::new().unwrap();
        let paths = [dir.path().join("queue.db"), dir.path().join("queue.sqlite")];
        for path in &paths {
            let queue = DiskBackedQueue::new(path, "messages".to_string(), None)
                .await
                .unwrap();
            queue.send(message(1)).await.unwrap();
            corrupt_row(&queue, 1);
            assert!(matches!(
                queue.recv().await,
                Err(DiskQueueError::Deserialization(_))
            ));
        }
        for path in &paths {
            assert_eq!(dlq_count(path, "messages"), 1, "{}", path.display());
        }
        assert!(!dir.path().join("queue.dlq.db").exists());
    }

    #[tokio::test]
    async fn test_legacy_dlq_file_left_untouched() {
        let dir = TempDir::new().unwrap();
        let legacy = dir.path().join("queue.dlq.db");
        std::fs::write(&legacy, b"legacy dead letters").unwrap();

        let _queue = DiskBackedQueue::<TestMessage>::new(
            dir.path().join("queue.db"),
            "messages".to_string(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&legacy).unwrap(), b"legacy dead letters");
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
        exec_sql(&queue, "UPDATE test_queue SET created_at = 1234");
        corrupt_row(&queue, 1);

        // Try to receive - should return deserialization error
        let result = queue.recv().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DiskQueueError::Deserialization(_))));

        // Verify message was moved to DLQ with its payload and metadata
        let dlq_conn = rusqlite::Connection::open(temp_file.path()).unwrap();
        let (queue_name, original_id, data, error_message, created_at): (
            String,
            i64,
            Vec<u8>,
            String,
            i64,
        ) = dlq_conn
            .query_row(
                &format!(
                    "SELECT queue_name, original_id, data, error_message, created_at FROM {DLQ_TABLE_NAME}"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(queue_name, "test_queue");
        assert_eq!(original_id, 1);
        assert_eq!(data, vec![0xFF, 0xFE, 0xFD, 0xFC]);
        assert!(!error_message.is_empty());
        assert_eq!(created_at, 1234);

        // Verify main queue is empty
        assert_eq!(queue.len().await.unwrap(), 0);
    }

    /// A dead-letter transfer whose removal from the queue fails must roll back entirely, so
    /// retrying cannot produce duplicate dead letters.
    #[tokio::test]
    async fn test_failed_dead_letter_transfer_rolls_back() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "atomic_dlq".to_string(), None)
            .await
            .unwrap();
        queue.send(message(1)).await.unwrap();
        corrupt_row(&queue, 1);
        exec_sql(
            &queue,
            "CREATE TRIGGER fail_delete BEFORE DELETE ON atomic_dlq
             BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END;",
        );

        for _ in 0..2 {
            let result = queue.recv().await;
            assert!(
                matches!(result, Err(DiskQueueError::Database(_))),
                "{result:?}"
            );
        }
        assert_eq!(dlq_count(temp_file.path(), "atomic_dlq"), 0);
        assert_eq!(queue.len().await.unwrap(), 1);

        exec_sql(&queue, "DROP TRIGGER fail_delete");
        assert!(matches!(
            queue.recv().await,
            Err(DiskQueueError::Deserialization(_))
        ));
        assert_eq!(dlq_count(temp_file.path(), "atomic_dlq"), 1);
        assert!(queue.is_empty().await.unwrap());
    }

    /// A corrupt row that cannot be dead-lettered stays queued, and so does any batch it is in.
    #[tokio::test]
    async fn test_dlq_write_failure_preserves_rows() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "dlq_failure".to_string(), None)
            .await
            .unwrap();
        queue
            .send_batch(vec![message(1), message(2)])
            .await
            .unwrap();
        corrupt_row(&queue, 1);
        exec_sql(
            &queue,
            &format!(
                "CREATE TRIGGER fail_dlq BEFORE INSERT ON {DLQ_TABLE_NAME}
                 BEGIN SELECT RAISE(ABORT, 'injected DLQ failure'); END;"
            ),
        );

        assert!(matches!(
            queue.recv().await,
            Err(DiskQueueError::DlqWriteFailed(_))
        ));
        assert!(matches!(
            queue.recv_batch(10).await,
            Err(DiskQueueError::DlqWriteFailed(_))
        ));
        assert_eq!(queue.len().await.unwrap(), 2);

        exec_sql(&queue, "DROP TRIGGER fail_dlq");
        assert_eq!(queue.recv_batch(10).await.unwrap(), vec![message(2)]);
        assert_eq!(dlq_count(temp_file.path(), "dlq_failure"), 1);
    }

    #[tokio::test]
    async fn test_trailing_bytes_are_dead_lettered() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::<u64>::new(temp_file.path(), "trailing".to_string(), None)
            .await
            .unwrap();
        queue.send(42).await.unwrap();
        edit_payload(&queue, 1, |data| data.push(0xFF));

        assert!(matches!(
            queue.recv().await,
            Err(DiskQueueError::Deserialization(_))
        ));
        let reason: String = rusqlite::Connection::open(temp_file.path())
            .unwrap()
            .query_row(
                &format!("SELECT error_message FROM {DLQ_TABLE_NAME}"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(reason.contains("trailing"), "{reason}");
        assert!(queue.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_truncated_payload_is_dead_lettered() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "truncated".to_string(), None)
            .await
            .unwrap();
        queue.send(message(1)).await.unwrap();
        edit_payload(&queue, 1, |data| {
            data.pop();
        });

        assert!(matches!(
            queue.recv().await,
            Err(DiskQueueError::Deserialization(_))
        ));
        assert_eq!(dlq_count(temp_file.path(), "truncated"), 1);
        assert!(queue.is_empty().await.unwrap());
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
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("waiting sender did not complete after space was freed")
            .unwrap()
            .unwrap();

        // Verify queue has 2 messages
        assert_eq!(rx.len().await.unwrap(), 2);
    }

    /// The capacity check counts the id span. A row deleted out of order outside the library
    /// leaves a gap that is still counted until the rows around it are received: backpressure
    /// can come early, but the queue never exceeds `max_size`.
    #[tokio::test]
    async fn test_capacity_check_counts_id_gaps_conservatively() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = Arc::new(
            DiskBackedQueue::new(temp_file.path(), "gap_test".to_string(), Some(3))
                .await
                .unwrap(),
        );
        queue
            .send_batch(vec![message(1), message(2), message(3)])
            .await
            .unwrap();
        exec_sql(&queue, "DELETE FROM gap_test WHERE id = 2");
        assert_eq!(queue.len().await.unwrap(), 2);

        let waiting_sender = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.send(message(4)).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting_sender.is_finished(),
            "the gap should still count as used"
        );

        assert_eq!(queue.recv().await.unwrap(), Some(message(1)));
        tokio::time::timeout(Duration::from_secs(5), waiting_sender)
            .await
            .expect("sender should proceed once the gap is behind the oldest message")
            .unwrap()
            .unwrap();
        assert_eq!(
            queue.recv_batch(10).await.unwrap(),
            vec![message(3), message(4)]
        );
    }

    #[tokio::test]
    async fn test_zero_capacity_rejected_before_touching_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("zero.db");

        let result = DiskBackedQueue::<TestMessage>::new(&path, "zero".to_string(), Some(0)).await;

        assert!(
            matches!(result, Err(DiskQueueError::InvalidConfiguration(_))),
            "{result:?}"
        );
        assert!(!path.exists());
    }

    /// Senders waiting for capacity must not occupy the blocking pool that the receiver needs.
    #[test]
    fn test_full_queue_drains_with_one_blocking_thread() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let outcome: std::result::Result<(), String> = runtime.block_on(async {
            let queue = Arc::new(
                DiskBackedQueue::new(temp_file.path(), "one_blocking".to_string(), Some(1))
                    .await
                    .map_err(|e| e.to_string())?,
            );
            queue.send(message(1)).await.map_err(|e| e.to_string())?;
            let waiting_sender = tokio::spawn({
                let queue = Arc::clone(&queue);
                async move { queue.send(message(2)).await }
            });
            // Let the second sender find the queue full and start waiting.
            tokio::time::sleep(Duration::from_millis(100)).await;

            let first = tokio::time::timeout(Duration::from_secs(5), queue.recv())
                .await
                .map_err(|_| "receiver starved by a waiting sender".to_string())?
                .map_err(|e| e.to_string())?;
            if first != Some(message(1)) {
                return Err(format!("unexpected first message: {first:?}"));
            }
            tokio::time::timeout(Duration::from_secs(5), waiting_sender)
                .await
                .map_err(|_| "waiting sender never completed".to_string())?
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            let second = queue.recv().await.map_err(|e| e.to_string())?;
            if second != Some(message(2)) {
                return Err(format!("unexpected second message: {second:?}"));
            }
            Ok(())
        });
        // Don't wait indefinitely for blocking work if this regressed into a deadlock.
        runtime.shutdown_timeout(Duration::from_secs(1));
        outcome.unwrap();
    }

    /// A sender cancelled while waiting for capacity must neither enqueue later nor keep the
    /// runtime from shutting down.
    #[test]
    fn test_cancelled_waiting_send_stops() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let queue = runtime.block_on(async {
            let queue = DiskBackedQueue::new(temp_file.path(), "cancel_send".to_string(), Some(1))
                .await
                .unwrap();
            queue.send(message(1)).await.unwrap();
            let attempt =
                tokio::time::timeout(Duration::from_millis(100), queue.send(message(2))).await;
            assert!(
                attempt.is_err(),
                "send into a full queue should still be waiting"
            );
            queue
        });

        let shutdown_started = Instant::now();
        runtime.shutdown_timeout(Duration::from_secs(5));
        assert!(
            shutdown_started.elapsed() < Duration::from_secs(5),
            "a cancelled sender kept a blocking worker alive"
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            assert_eq!(queue.recv().await.unwrap(), Some(message(1)));
            // Longer than the capacity poll interval: a surviving waiter would have retried.
            tokio::time::sleep(CAPACITY_POLL_INTERVAL * 3).await;
            assert!(queue.is_empty().await.unwrap());
        });
    }

    #[tokio::test]
    async fn test_clear_unblocks_waiting_sender() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = Arc::new(
            DiskBackedQueue::new(temp_file.path(), "clear_wakes".to_string(), Some(1))
                .await
                .unwrap(),
        );
        queue.send(message(1)).await.unwrap();
        let waiting_sender = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.send(message(2)).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting_sender.is_finished());

        queue.clear().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), waiting_sender)
            .await
            .expect("waiting sender did not complete after clear")
            .unwrap()
            .unwrap();
        assert_eq!(queue.recv().await.unwrap(), Some(message(2)));
    }

    /// Cancelling a receive whose blocking work has already started must not lose the message.
    #[test]
    fn test_cancelled_recv_keeps_message() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let queue = runtime.block_on(async {
            let queue = DiskBackedQueue::new(temp_file.path(), "cancel_recv".to_string(), None)
                .await
                .unwrap();
            queue.send(message(1)).await.unwrap();
            queue
        });

        // Hold the connection so the receive is still in progress when its future is dropped.
        let conn = queue.inner.db.lock().unwrap();
        let attempt = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(50), queue.recv()).await
        });
        assert!(
            attempt.is_err(),
            "receive should still have been in progress"
        );
        drop(conn);
        // Shutting down waits for the abandoned receive to run to completion.
        drop(runtime);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(runtime.block_on(queue.recv()).unwrap(), Some(message(1)));
    }

    #[test]
    fn test_cancelled_recv_batch_keeps_messages() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let queue = runtime.block_on(async {
            let queue =
                DiskBackedQueue::new(temp_file.path(), "cancel_recv_batch".to_string(), None)
                    .await
                    .unwrap();
            queue
                .send_batch(vec![message(1), message(2), message(3)])
                .await
                .unwrap();
            queue
        });

        let conn = queue.inner.db.lock().unwrap();
        let attempt = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(50), queue.recv_batch(10)).await
        });
        assert!(
            attempt.is_err(),
            "receive should still have been in progress"
        );
        drop(conn);
        drop(runtime);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            runtime.block_on(queue.recv_batch(10)).unwrap(),
            vec![message(1), message(2), message(3)]
        );
    }

    /// A receive future that is kept alive but not polled must not hold the database lock
    /// forever; when it is polled again it still delivers the message exactly once.
    #[tokio::test]
    async fn test_unpolled_recv_releases_lock_and_recovers() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "stalled_recv".to_string(), None)
            .await
            .unwrap();
        queue.send(message(1)).await.unwrap();

        let mut stalled = Box::pin(queue.recv());
        {
            // Hold the connection during the first poll so the receive cannot finish in it.
            let _conn = queue.inner.db.lock().unwrap();
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(stalled.as_mut().poll(&mut cx).is_pending());
        }
        // Let the receive reach the handoff before competing with it for the connection.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // This needs the connection held by the stalled receive, so it can only complete once
        // the receive gives up and rolls back.
        assert_eq!(queue.len().await.unwrap(), 1);

        assert_eq!(stalled.await.unwrap(), Some(message(1)));
        assert!(queue.is_empty().await.unwrap());
    }

    /// Independently opened queues on one file (separate connections, as with separate
    /// processes) must not fail with busy or stale-snapshot errors, nor duplicate messages.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_independent_connections_share_a_queue() {
        const TOTAL: u64 = 200;
        let temp_file = NamedTempFile::new().unwrap();
        let open =
            || DiskBackedQueue::<TestMessage>::new(temp_file.path(), "shared".to_string(), None);
        let producer = open().await.unwrap();
        let single = open().await.unwrap();
        let batch = open().await.unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let done = {
            let received = Arc::clone(&received);
            move || received.lock().unwrap().len() as u64 >= TOTAL
        };

        let producing = tokio::spawn(async move {
            for id in 0..TOTAL {
                producer.send(message(id)).await.unwrap();
            }
        });
        let single_consumer = tokio::spawn({
            let received = Arc::clone(&received);
            let done = done.clone();
            async move {
                while !done() {
                    match single.recv().await.unwrap() {
                        Some(msg) => received.lock().unwrap().push(msg.id),
                        None => tokio::time::sleep(Duration::from_millis(1)).await,
                    }
                }
            }
        });
        let batch_consumer = tokio::spawn({
            let received = Arc::clone(&received);
            async move {
                while !done() {
                    let msgs = batch.recv_batch(7).await.unwrap();
                    if msgs.is_empty() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    received
                        .lock()
                        .unwrap()
                        .extend(msgs.into_iter().map(|msg| msg.id));
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            producing.await.unwrap();
            single_consumer.await.unwrap();
            batch_consumer.await.unwrap();
        })
        .await
        .expect("consumers did not finish");

        let mut ids = received.lock().unwrap().clone();
        ids.sort_unstable();
        assert_eq!(ids, (0..TOTAL).collect::<Vec<_>>());
    }

    /// Construction must not block the executor while SQLite waits for a lock, and must be
    /// safe to repeat after a caller stopped waiting.
    #[tokio::test] // current-thread: a blocking constructor would also stall the timeout
    async fn test_constructor_does_not_block_executor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("locked.db");
        let locker = rusqlite::Connection::open(&path).unwrap();
        locker
            .execute_batch("CREATE TABLE hold (x); BEGIN EXCLUSIVE; INSERT INTO hold VALUES (1);")
            .unwrap();

        let started = Instant::now();
        let attempt = tokio::time::timeout(
            Duration::from_millis(50),
            DiskBackedQueue::<TestMessage>::new(&path, "locked".to_string(), None),
        )
        .await;
        assert!(
            attempt.is_err(),
            "construction should still be waiting for the lock"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "construction blocked the executor for {:?}",
            started.elapsed()
        );

        locker.execute_batch("COMMIT").unwrap();
        drop(locker);

        let queue = DiskBackedQueue::new(&path, "locked".to_string(), None)
            .await
            .unwrap();
        queue.send(message(1)).await.unwrap();
        assert_eq!(queue.recv().await.unwrap(), Some(message(1)));
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

    /// A batch insert that fails part-way leaves none of its messages queued.
    #[tokio::test]
    async fn test_failed_batch_insert_rolls_back() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "batch_rollback".to_string(), None)
            .await
            .unwrap();
        exec_sql(
            &queue,
            "CREATE TRIGGER fail_third BEFORE INSERT ON batch_rollback
             WHEN (SELECT COUNT(*) FROM batch_rollback) >= 2
             BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END;",
        );

        let result = queue.send_batch((1..=5).map(message).collect()).await;

        assert!(
            matches!(result, Err(DiskQueueError::Database(_))),
            "{result:?}"
        );
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
    async fn test_recv_batch_larger_than_parameter_limit() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "large_batch".to_string(), None)
            .await
            .unwrap();
        let messages: Vec<TestMessage> = (0..1200).map(message).collect();
        queue.send_batch(messages.clone()).await.unwrap();

        assert_eq!(queue.recv_batch(1200).await.unwrap(), messages);
        assert!(queue.is_empty().await.unwrap());
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
        corrupt_row(&queue, 6);

        // Batch receive should skip corrupted message and move it to DLQ
        let batch = queue.recv_batch(10).await.unwrap();
        assert_eq!(batch.len(), 9); // Should get 9 valid messages (10 - 1 corrupted)

        // Verify DLQ has 1 entry
        assert_eq!(dlq_count(temp_file.path(), "batch_corrupt_test"), 1);
    }

    /// A batch keeps scanning past dead-lettered rows, so an all-corrupt prefix cannot hide
    /// valid messages behind an empty result.
    #[tokio::test]
    async fn test_recv_batch_scans_past_corrupt_prefix() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "corrupt_prefix".to_string(), None)
            .await
            .unwrap();
        queue
            .send_batch(vec![message(1), message(2), message(3)])
            .await
            .unwrap();
        corrupt_row(&queue, 1);
        corrupt_row(&queue, 2);

        assert_eq!(queue.recv_batch(2).await.unwrap(), vec![message(3)]);
        assert_eq!(dlq_count(temp_file.path(), "corrupt_prefix"), 2);
        assert!(queue.is_empty().await.unwrap());

        // With only corrupt rows left, they are dead-lettered and the batch is empty.
        queue.send(message(4)).await.unwrap();
        corrupt_row(&queue, 4);
        assert!(queue.recv_batch(5).await.unwrap().is_empty());
        assert_eq!(dlq_count(temp_file.path(), "corrupt_prefix"), 3);
        assert!(queue.is_empty().await.unwrap());
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

    /// `blocking_send` on a current-thread runtime returns an error instead of panicking:
    /// blocking would stall the only executor thread.
    #[tokio::test] // default flavor is current_thread
    async fn test_blocking_send_on_current_thread_runtime_is_rejected() {
        let temp_file = NamedTempFile::new().unwrap();
        let (tx, rx) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "blocking_ct_test".to_string(),
            None,
        )
        .await
        .unwrap();

        let result = tx.blocking_send(message(7));

        assert!(
            matches!(result, Err(DiskQueueError::UnsupportedRuntime(_))),
            "{result:?}"
        );
        assert!(rx.is_empty().await.unwrap());
    }

    /// Outside any runtime, `blocking_send` writes on the calling thread and waits for
    /// capacity.
    #[test]
    fn test_blocking_send_outside_runtime() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = runtime
            .block_on(disk_backed_channel::<TestMessage, _>(
                temp_file.path(),
                "blocking_sync_test".to_string(),
                Some(1),
            ))
            .unwrap();

        tx.blocking_send(message(1)).unwrap();
        let waiting_sender = std::thread::spawn({
            let tx = tx.clone();
            move || tx.blocking_send(message(2))
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !waiting_sender.is_finished(),
            "send into a full queue should wait"
        );

        assert_eq!(runtime.block_on(rx.recv()).unwrap(), Some(message(1)));
        waiting_sender
            .join()
            .expect("sender thread panicked")
            .unwrap();
        assert_eq!(runtime.block_on(rx.recv()).unwrap(), Some(message(2)));
    }

    /// Waiting for capacity must not depend on Tokio's timer, which a runtime may not enable.
    #[test]
    fn test_full_queue_without_tokio_timer() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let queue = Arc::new(
                DiskBackedQueue::new(temp_file.path(), "no_timer".to_string(), Some(1))
                    .await
                    .unwrap(),
            );
            queue.send(message(1)).await.unwrap();
            let waiting_sender = tokio::spawn({
                let queue = Arc::clone(&queue);
                async move { queue.send(message(2)).await }
            });
            // No timer available: let the sender start waiting by sleeping on the blocking pool.
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(100)))
                .await
                .unwrap();
            assert!(!waiting_sender.is_finished());

            assert_eq!(queue.recv().await.unwrap(), Some(message(1)));
            waiting_sender.await.unwrap().unwrap();
            assert_eq!(queue.recv().await.unwrap(), Some(message(2)));
        });
    }

    /// Space freed through another connection cannot notify this queue's waiters directly;
    /// the capacity poller must still wake them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_space_freed_by_another_connection_wakes_sender() {
        let temp_file = NamedTempFile::new().unwrap();
        let open = || DiskBackedQueue::new(temp_file.path(), "cross_conn".to_string(), Some(1));
        let producer = Arc::new(open().await.unwrap());
        let consumer = open().await.unwrap();
        producer.send(message(1)).await.unwrap();

        let waiting_sender = tokio::spawn({
            let producer = Arc::clone(&producer);
            async move { producer.send(message(2)).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting_sender.is_finished());

        assert_eq!(consumer.recv().await.unwrap(), Some(message(1)));
        tokio::time::timeout(Duration::from_secs(5), waiting_sender)
            .await
            .expect("sender was not woken after another connection freed space")
            .unwrap()
            .unwrap();
        assert_eq!(consumer.recv().await.unwrap(), Some(message(2)));
    }

    /// Receiving from an empty queue must not need the write lock, so it does not wait behind
    /// another connection's write transaction.
    #[tokio::test]
    async fn test_empty_recv_does_not_wait_for_write_lock() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue =
            DiskBackedQueue::<TestMessage>::new(temp_file.path(), "empty_poll".to_string(), None)
                .await
                .unwrap();
        let writer = rusqlite::Connection::open(temp_file.path()).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();

        let started = Instant::now();
        assert_eq!(queue.recv().await.unwrap(), None);
        assert!(queue.recv_batch(10).await.unwrap().is_empty());
        assert!(
            started.elapsed() < BUSY_TIMEOUT / 2,
            "empty receives waited {:?} for the write lock",
            started.elapsed()
        );
        writer.execute_batch("ROLLBACK").unwrap();
    }

    /// A message type whose `Deserialize` impl panics on some values.
    #[derive(Debug, PartialEq)]
    struct Fragile(u32);

    impl Serialize for Fragile {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.0.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for Fragile {
        fn deserialize<D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Self, D::Error> {
            let value = u32::deserialize(deserializer)?;
            assert_ne!(value, 13, "unlucky value");
            Ok(Self(value))
        }
    }

    /// A panicking `Deserialize` impl dead-letters the message instead of poisoning the queue.
    #[tokio::test]
    async fn test_panicking_deserializer_is_dead_lettered() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "fragile".to_string(), None)
            .await
            .unwrap();
        queue
            .send_batch(vec![Fragile(13), Fragile(14), Fragile(13), Fragile(15)])
            .await
            .unwrap();

        assert!(matches!(
            queue.recv().await,
            Err(DiskQueueError::Deserialization(_))
        ));
        assert_eq!(queue.recv().await.unwrap(), Some(Fragile(14)));
        assert_eq!(queue.recv_batch(10).await.unwrap(), vec![Fragile(15)]);
        assert_eq!(dlq_count(temp_file.path(), "fragile"), 2);

        // The queue is still usable afterwards.
        queue.send(Fragile(16)).await.unwrap();
        assert_eq!(queue.recv().await.unwrap(), Some(Fragile(16)));
    }

    /// `blocking_send` from the blocking pool must not need a second blocking thread.
    #[test]
    fn test_blocking_send_from_blocking_pool_with_one_thread() {
        let temp_file = NamedTempFile::new().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let outcome: std::result::Result<(), String> = runtime.block_on(async {
            let (tx, mut rx) = disk_backed_channel::<TestMessage, _>(
                temp_file.path(),
                "blocking_pool".to_string(),
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
            let send = tokio::task::spawn_blocking(move || tx.blocking_send(message(1)));
            tokio::time::timeout(Duration::from_secs(5), send)
                .await
                .map_err(|_| "blocking_send hung on the blocking pool".to_string())?
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            match rx.recv().await.map_err(|e| e.to_string())? {
                Some(msg) if msg == message(1) => Ok(()),
                other => Err(format!("unexpected message: {other:?}")),
            }
        });
        // Don't wait indefinitely for blocking work if this regressed into a hang.
        runtime.shutdown_timeout(Duration::from_secs(1));
        outcome.unwrap();
    }

    /// The documented alternative to `blocking_send` on a current-thread runtime works from
    /// a blocking thread.
    #[tokio::test] // default flavor is current_thread
    async fn test_handle_block_on_send_from_blocking_thread_on_current_thread_runtime() {
        let temp_file = NamedTempFile::new().unwrap();
        let (tx, mut rx) = disk_backed_channel::<TestMessage, _>(
            temp_file.path(),
            "blocking_ct_workaround".to_string(),
            Some(1),
        )
        .await
        .unwrap();
        tokio::task::spawn_blocking(move || {
            assert!(matches!(
                tx.blocking_send(message(1)),
                Err(DiskQueueError::UnsupportedRuntime(_))
            ));
            tokio::runtime::Handle::current().block_on(tx.send(message(1)))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rx.recv().await.unwrap(), Some(message(1)));
    }

    /// `send_batch` rejects batches larger than `max_size` immediately rather than
    /// blocking forever waiting for space that can never exist.
    #[tokio::test]
    async fn test_send_batch_rejects_oversized_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let queue = DiskBackedQueue::new(temp_file.path(), "oversize_test".to_string(), Some(5))
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
        let queue = DiskBackedQueue::new(temp_file.path(), "clear_test".to_string(), None)
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
