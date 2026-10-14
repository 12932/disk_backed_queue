# Code review findings and remediation plan

## Scope and status

This document records all problems identified during the project review and all proposed solutions discussed afterward. It is a review and implementation plan, not a claim that the fixes have already been implemented.

- **Reviewed version:** `disk_backed_queue` 0.1.3.
- **Reviewed commit:** `5cf4c59` (`v0.1.3`).
- **Reviewed files:** `src/lib.rs`, `Cargo.toml`, `Cargo.lock`, `README.md`, `benches/queue_benchmark.rs`, `examples/throughput_demo.rs`, and the existing unit tests.
- **Test environment:** Windows, `rustc` 1.99.0, Cargo 1.99.0, and cargo-nextest 0.9.129.
- **Implementation status:** see [Implementation status](#implementation-status). The problem and solution sections below are the original review record.
- **References:** file and line references refer to the reviewed commit and will move as fixes are implemented.

The normal send/receive and persistence paths work. The highest-priority defects are message loss when receives are cancelled and loss of progress when full senders occupy Tokio's blocking workers. Other findings concern runtime usage, independent SQLite connections, ordering, identifier handling, dead-letter isolation, serialization, documentation, and test reliability.

Some findings are reproducible implementation defects; others are robustness gaps, misleading documentation, or missing tests. In particular, duplicate dead letters, strict input consumption, and the meaning of an empty batch require explicit API contracts rather than assumptions.

## Implementation status

Implemented in the working tree after `9c17360`. Scope decision: fix the defects **without** changing the `recv() -> T` API (the S01 claim/ack redesign is deferred), and store dead letters in the main database (S08 preferred design). Verified on Linux with rustc 1.99.0 and 1.88.0.

| ID | Status | What was done |
| --- | --- | --- |
| P01 | Fixed without S01 | A receive transaction deletes the rows but commits only after the receiving future has accepted them (`Handoff`). A dropped future, or one left unpolled for 1 s, rolls back; the future retries if polled again. Delivery to the caller is at-least-once. Claim/ack, leases and fencing are deferred. |
| P02 | Fixed | Each insert attempt is one finite `BEGIN IMMEDIATE` job. Waiting for capacity is async: `Notify` is registered before the check. A helper thread, running only while senders wait, wakes them every 100 ms to notice space freed by other connections; Tokio's timer is not needed. |
| P03 | Fixed | Dropping a waiting send stops further attempts and does not hold up shutdown. An insert already running may still commit (documented). Idempotency keys are deferred. |
| P04 | Fixed | `Some(0)` is rejected with the new `InvalidConfiguration` error before any file is touched. |
| P05 | Fixed | Outside a runtime: synchronous write on the calling thread (no temporary runtime). Multi-thread runtime: the same synchronous write inside `block_in_place`, which needs no thread from the blocking pool. Current-thread runtime: new `UnsupportedRuntime` error, including on `spawn_blocking` threads, because Tokio cannot tell them apart from the executor thread; `Handle::block_on(send(..))` is documented as the alternative. |
| P06 | Fixed | Every read-then-modify transaction uses `BEGIN IMMEDIATE`. Receiving from an empty queue answers from a plain read without taking the write lock. A busy error (lock held past the 5 s busy timeout) is retried twice, each time as a new blocking job. |
| P07 | Fixed | Initialization runs in `spawn_blocking`, and every step is idempotent. |
| P08 | Fixed | Ordering uses `id`. The legacy `created_at` index is dropped on open. |
| P09 | Fixed | All identifiers are quoted. The `sqlite_` and `__disk_queue` prefixes are reserved (case-insensitive). The limit is documented as 128 UTF-8 bytes, and Unicode letters and digits are still accepted. |
| P10, P12 | Fixed | Dead letters live in `__disk_queue_dlq` (with `queue_name` and the original `created_at`) in the main database. They are moved in the same transaction that deletes the row, with row-count checks. Legacy `.dlq.db` files are neither read nor imported; they are left untouched and a warning is logged. |
| P11 | Fixed | Benchmarks use `iter_custom` with untimed setup and teardown and a `TempDir`. Recv benchmarks were added. |
| P13 | Fixed | `recv_batch` keyset-scans past dead-lettered rows, so an empty batch means no deliverable message remains. A DLQ write failure now fails the whole batch; previously the bad row was skipped. |
| P14 | Partly fixed | Decoding must consume every byte; trailing or truncated payloads go to the DLQ. Message-format versioning is deferred. |
| P15 | Fixed | `rust-version = "1.88"`. The library build and tests were verified with rustc 1.88.0, and 1.87 is rejected by Cargo. No CI exists in the repo, so no MSRV job was added. |
| P16, P17 | Fixed | Rustdoc and README rewritten: WAL durability levels, at-least-once receive, cancellation, and producer-outcome gaps. |
| P18 | Fixed | The 5× speedup test was removed. |
| P19 | Fixed | `cargo fmt --all` applied. |
| P20 | Fixed | Regression tests were added for every reproduced defect and positive invariant: 50 library tests, up from 23. |

Behavior changes for users, which warrant a 0.2.0 release: `DiskQueueError` gains two variants and becomes `#[non_exhaustive]`; dead letters move into the main database; `Some(0)` and reserved table names are rejected; `blocking_send` returns an error on a current-thread runtime instead of panicking; payloads with trailing bytes are dead-lettered; `recv_batch` fails on a DLQ write failure. In addition, a read-only query on *another* connection can see a received message for about one fsync after `recv` returns, until the deletion commits.

Follow-up beyond the review, same working tree:
- **Bounded-queue capacity check.** It now uses the id span (`MAX(id) - MIN(id) + 1`, O(log n)) instead of `COUNT(*)`, which reads every row. Bounded single sends at 100,000 queued messages went from 14 to ~760 msg/s on NVMe; they were 118 msg/s at 10,000 queued. The span is an upper bound on the count, so `max_size` still holds; gaps from rows deleted out of order outside the library cause early backpressure.
- **Dependencies.** Updated to the latest releases: tokio 1.53.3, serde 1.0.229, and rusqlite 0.40.2, which bundles SQLite 3.53.2. All other direct dependencies were already current, and every resolved version supports Rust 1.88.

- **Second, adversarial review of this implementation.** Fixed: busy errors with several connections under load (see P06); a panic on full bounded queues when the runtime has no timer; a `blocking_send` hang inside `spawn_blocking` with one blocking thread; a panicking `Deserialize` impl poisoning the queue permanently (such messages are now dead-lettered); a flaky fixed sleep in `test_max_size_blocks`. Documented rather than changed: `recv` returning before its deletion commits, which is the at-least-once design (`std::process::exit` right after `recv` can cause redelivery), and `blocking_send` refusing current-thread runtimes even on blocking threads.

Still open: S01 claim/ack, S02 producer idempotency keys, S11 message-format versioning, importing legacy DLQ files, CI (Windows/Linux, MSRV job), `cargo audit` and nextest (neither installed here; `cargo test` was used).

Performance on Linux NVMe, `DurabilityLevel::Full`, 1 KiB messages, steady state: single send ~806 → ~850 msg/s, single recv ~811 → ~852 msg/s, batch send (100) ~23.3k → ~26.3k msg/s, batch recv (100) ~32.1k → ~37.4k msg/s. The gains come from dropping the `created_at` index and from overlapping the receive commit with the caller. On a brand-new database, single sends are about 7% slower for the first ~500 commits, because the WAL takes longer to reach its first checkpoint now that each commit writes fewer pages.

## Contents

0. [Implementation status](#implementation-status)
1. [All identified problems](#all-identified-problems)
2. [All proposed solutions](#all-proposed-solutions)
3. [Validation performed](#validation-performed)
4. [Regression and acceptance checklist](#regression-and-acceptance-checklist)
5. [Recommended implementation order](#recommended-implementation-order)

## All identified problems

### Problem register

| ID | Priority/type | Problem | Proposed solution |
| --- | --- | --- | --- |
| P01 | High | Cancelled receives can delete messages without delivering them | S01 |
| P02 | High | Full senders can exhaust the blocking pool and deadlock the queue | S02 |
| P03 | High | Cancelled sends continue running; waiting workers can prevent shutdown | S02 |
| P04 | Medium | Zero capacity is accepted even though sends can never make progress | S02 |
| P05 | Medium | `blocking_send` panics inside a current-thread runtime | S03, S14 |
| P06 | Medium | Deferred receive transactions fail under independent-connection writes | S04 |
| P07 | Medium | Async initialization performs blocking SQLite work on the executor | S05 |
| P08 | Medium | Wall-clock rollback breaks FIFO ordering | S06 |
| P09 | Medium | Allowed table names are used as invalid unquoted SQL identifiers | S07 |
| P10 | Medium | Different main database filenames can share and mix dead letters | S08 |
| P11 | Medium | All Criterion benchmark cases panic due to nested runtime execution | S09 |
| P12 | Robustness/retry consistency | DLQ transfers are neither atomic across files nor idempotent | S08 |
| P13 | API/consumer correctness | An empty batch can leave valid messages queued | S10 |
| P14 | Data integrity | Trailing serialized bytes and some schema mismatches are silently accepted | S11 |
| P15 | Compatibility/documentation | Advertised Rust 1.86 support conflicts with let-chain syntax | S12 |
| P16 | Documentation | WAL durability levels are described incorrectly | S13 |
| P17 | Documentation/API contract | Delivery claims and suggested remedies overstate what destructive receive provides | S01, S02, S13 |
| P18 | Test reliability | A unit test requires an environment-dependent performance improvement | S09, S14 |
| P19 | Maintenance | The source does not pass the formatting check | S14 |
| P20 | Test coverage | Existing tests miss important failure and concurrency paths | S14 |

### P01. Cancelled receives can silently consume messages

**Locations:** `src/lib.rs:741`, `src/lib.rs:840`, `src/lib.rs:904`, `src/lib.rs:1040`.

`recv` and `recv_batch` execute selection, deserialization, deletion, and commit inside `tokio::task::spawn_blocking`. Dropping the async receive future drops its join handle, but does not stop a blocking task that has already started.

The blocking task can subsequently commit the deletion and produce a value that no caller receives. This is possible during normal use of `tokio::select!`, receive timeouts, and task cancellation during shutdown; a process crash is not required.

**Reproduction:** pause deserialization inside the blocking worker, cancel the receiving task, then release the worker. The queue becomes empty even though the caller never received the message. This was reproduced for both single and batch receives.

**Impact:** silent message loss before delivery to the caller. SQLite transaction consistency does not protect the async handoff of the committed result.

### P02. Full senders can deadlock the queue

**Locations:** `src/lib.rs:500-534`, `src/lib.rs:628-662`, especially the sleeps at lines 531 and 659.

The entire capacity-waiting loop runs inside a blocking worker. Each sender waiting for capacity holds a Tokio blocking thread and repeatedly uses `std::thread::sleep`.

If enough waiting senders occupy the blocking pool, `recv`, `len`, and `clear` cannot execute because they also need blocking workers. The consumer cannot free the capacity needed by the senders, creating a circular wait even though the database mutex is released between attempts.

**Reproduction:** configure a runtime with `max_blocking_threads(1)`, fill a capacity-one queue, start another sender, and attempt to receive. The receive times out. External SQL deletion was needed to free capacity and allow the reproduction to shut down cleanly.

**Impact:** no forward progress. The same failure is possible with the default, larger blocking pool at greater concurrency.

### P03. Cancelled sends continue and can prevent shutdown

**Locations:** the same blocking loops as P02.

Cancelling an async sender does not terminate its already-started blocking worker. The worker still owns its serialized message and database handle, continues polling for capacity, and may enqueue later.

**Reproduction:** fill the queue, start a waiting send, cancel the sender task, consume the existing message, and wait. The cancelled sender's message appears in the queue afterward.

If capacity never becomes available, the worker never exits. Normal Tokio runtime shutdown waits for started blocking tasks, so shutdown can wait indefinitely. Dropping the sender or receiver does not stop a worker that owns its own database `Arc`.

**Impact:** unexpected later writes, retained resources, and shutdown hangs. There is also a separate producer acknowledgment gap: once an insert transaction has begun, cancellation can leave its final outcome uncertain.

### P04. Zero capacity permits a permanent wait

**Locations:** configuration handling in `src/lib.rs:295-307` and the capacity comparison in `src/lib.rs:516-533`.

`Some(0)` is accepted. Every queue size, including an empty queue, satisfies `count >= 0`, so a single-message send waits forever for impossible capacity.

`send_batch` already rejects nonempty batches larger than the configured capacity, making zero-capacity behavior inconsistent between the two send methods.

**Evidence:** construction with `Some(0)` succeeds; inspection of the send condition proves that no insertion can pass that condition. An infinite sender was deliberately not left running in the probe.

### P05. `blocking_send` panics in a current-thread runtime

**Locations:** `src/lib.rs:1297-1308`; misleading test coverage at `src/lib.rs:2333-2361`.

For a current-thread Tokio runtime, the implementation creates a temporary runtime and calls `runtime.block_on` while already executing inside a Tokio runtime. Tokio rejects this nested execution.

**Reproduction:** invoke `blocking_send` directly inside a default `#[tokio::test]`. It panics with the nested-runtime error.

The existing current-thread test calls `blocking_send` on a fresh OS thread. That thread has no active Tokio runtime, so the test exercises the outside-runtime path rather than the path named by the test.

**Impact:** a supposedly handled calling context causes a panic instead of a documented result or error.

### P06. Independent connections invalidate receive transactions

**Locations:** `src/lib.rs:745`, `src/lib.rs:908`.

Receives use SQLite's default deferred transaction. They first read a snapshot, deserialize a row, and then attempt to upgrade to a write transaction for deletion.

Another connection can commit a write after the SELECT. The reader can then fail to upgrade its stale WAL snapshot with `SQLITE_BUSY_SNAPSHOT`. The configured busy timeout cannot repair a stale snapshot. Two competing read-to-write upgrades can also fail immediately with ordinary `DatabaseBusy`.

**Reproductions:**

- Two independently opened queues receiving from the same file produce a `DatabaseBusy` error in one receiver.
- A separate producer connection committing between the SELECT and DELETE produces extended SQLite error 517, `SQLITE_BUSY_SNAPSHOT`.

**Impact:** ordinary independent-connection activity can make receives fail without benefiting from the expected lock-wait behavior. The existing tests share one queue and mutex and cannot expose this.

### P07. Async initialization blocks the executor

**Locations:** `src/lib.rs:309-427`; conflicting descriptions at `src/lib.rs:198` and `README.md:367`.

`with_durability` opens connections, configures PRAGMAs, creates tables and indexes, and opens/configures the DLQ synchronously inside its async function. Initialization is not offloaded to `spawn_blocking`.

**Reproduction:** hold a SQLite writer lock briefly and wrap construction in a 50 ms async timeout. Construction blocks the current-thread executor for approximately 460 ms and returns successfully instead of allowing the timeout to run promptly.

**Impact:** delayed timers, unrelated tasks, and startup responsiveness under lock contention or slow storage. Merely declaring a function async does not make its SQLite work non-blocking.

### P08. Wall-clock ordering violates FIFO

**Locations:** `src/lib.rs:440`, `src/lib.rs:917`.

Rows are ordered by `created_at` before the monotonic insertion ID. The timestamp comes from wall-clock time, which can move backward due to a clock correction, manual adjustment, or a restart with a different clock value.

A newer message with an earlier timestamp can therefore be selected before an older message.

**Reproduction:** simulate backward timestamps on two sequentially inserted rows. Message 2 is received before message 1. The system clock itself was not modified.

**Impact:** the advertised FIFO property depends on wall-clock behavior even though SQLite already provides an appropriate sequence identifier.

### P09. Validated names are invalid unquoted identifiers

**Locations:** `src/lib.rs:301-307`, `src/lib.rs:356`, `src/lib.rs:437-452`; the direct-queue README example uses `"table"`.

Validation allows names such as `table`, `select`, and `123messages`. These names are interpolated without identifier quoting, so keywords or digit-leading names can produce SQL syntax errors during initialization.

**Reproduction:** all three names pass character validation and then fail in `CREATE TABLE` with database syntax errors. The README's `"table"` example is affected.

Related policy inconsistencies:

- `is_alphanumeric` accepts Unicode, whereas the error documentation describes an ASCII-style restriction.
- `String::len()` limits UTF-8 bytes, whereas the prose describes a number of characters.

**Impact:** documented input fails, errors are classified as database failures instead of validation failures, and the naming policy is unclear.

### P10. DLQ filenames are not unique to their main databases

**Location:** `src/lib.rs:382`.

Using `with_extension("dlq.db")` discards the main database extension:

```text
queue.db     -> queue.dlq.db
queue.sqlite -> queue.dlq.db
```

When the queues use the same table name, both write to the same dead-letter table. `original_id` does not identify which main database produced a row.

**Reproduction:** two independent main database files with the same stem and table name place their corrupt rows in one DLQ table.

**Impact:** mixed data, ambiguous source attribution, and unsafe operator assumptions during recovery or cleanup.

### P11. Criterion benchmarks panic

**Locations:** `benches/queue_benchmark.rs:19-29`, `benches/queue_benchmark.rs:46-56`.

Criterion invokes the `iter_with_setup` closure inside its async runner. The closure calls `rt.block_on`, attempting to enter a Tokio runtime from inside runtime-driven execution.

**Reproduction:** all four benchmark cases panic under `cargo nextest run --all-targets`. The release benchmark smoke test also panics.

**Impact:** the benchmark suite cannot measure performance or act as a useful smoke test.

### P12. DLQ transfers are not atomic or idempotent

**Locations:** `src/lib.rs:779-807`, `src/lib.rs:961-985`.

The DLQ insert commits on an independent connection/database before deletion commits in the main queue. If the deletion or commit fails, the dead letter exists while the original remains queued. Retrying writes another dead letter. A crash between the two commits has the same consistency window.

**Reproduction:** inject a main-table deletion failure using a trigger, call receive twice, and inspect both databases. The original row remains in the main queue and two DLQ entries exist for it.

**Impact:** duplicate dead letters and misleading move/retry semantics. This is primarily a retry-consistency problem; it is not evidence that the default `FULL` mode loses the corrupt payload in this scenario.

The DLQ insert paths also need explicit affected-row validation before treating a transfer as successful.

### P13. Empty batches do not necessarily mean the queue is drained

**Locations:** `src/lib.rs:917-930`, `src/lib.rs:946-990`.

The batch limit counts examined database rows, not successfully deserialized messages. If the first limited set consists entirely of corrupt rows that are moved successfully to the DLQ, the method returns an empty vector even if valid messages remain behind them.

**Reproduction:** insert two corrupt rows followed by one valid row and call `recv_batch(2)`. The result is empty while the valid row remains queued.

**Impact:** common drain-until-empty loops can stop prematurely. This behavior needs either a stronger method contract or an explicit progress/availability result.

### P14. Decoding silently ignores trailing bytes

**Locations:** `src/lib.rs:767`, `src/lib.rs:946`.

`postcard::from_bytes` returns a decoded value without verifying that all input bytes were consumed. A valid prefix with appended bytes is accepted, and the entire stored row is then deleted.

**Reproduction:** serialize `42u64`, append `0xff`, store the bytes, and receive. The result is `Ok(Some(42))`, not a deserialization error or dead letter.

Some schema changes, such as removing a trailing field, can likewise silently discard bytes. Other incompatible changes can decode to plausible values while consuming every byte, so complete consumption alone is not a complete schema-compatibility check.

**Impact:** reduced corruption detection and silent acceptance of some mismatched message formats. The stored format has no explicit message-schema version to distinguish these cases.

### P15. Advertised MSRV does not match syntax

**Locations:** `README.md:380`, `src/lib.rs:604-606`, and `Cargo.toml`.

The README advertises Rust 1.86+, but the source uses let-chains, stabilized in Rust 1.88. The manifest does not declare `rust-version`.

**Impact:** users following the documented minimum can encounter a compilation failure instead of a clear manifest-level compatibility requirement. Dependency compatibility at the eventual declared minimum also needs verification.

### P16. Durability descriptions are inaccurate

**Locations:** `src/lib.rs:151-164`, `README.md:266-270`.

The descriptions suggest that WAL `NORMAL` can corrupt the database during a checkpoint and that `EXTRA` provides stronger guarantees than `FULL` with substantial additional cost.

For SQLite WAL mode under the usual filesystem and synchronization assumptions:

- `NORMAL` preserves database consistency but can lose recent committed transactions after a power loss or OS crash.
- `EXTRA` provides the same durability as `FULL`; its additional rollback-journal directory synchronization does not add a WAL-mode guarantee.

Broad statements about every successful send being fsynced also need to be scoped to the selected durability level, not applied unconditionally to `NORMAL` or `OFF`.

**Impact:** incorrect guidance when users choose performance/durability trade-offs.

### P17. Delivery documentation overstates guarantees and remedies

**Locations:** crate-level feature claims in `src/lib.rs`; `README.md:9`, `README.md:17`, `README.md:369-375`.

The current receive deletes before processing and before the async result has necessarily reached the caller. The README already acknowledges the absence of processing acknowledgments, but also uses at-least-once wording and suggests idempotent processing as a remedy for the deletion/processing gap.

Idempotency handles duplicate processing. It does not reconstruct a message deleted before processing or before delivery to the caller. Durable acknowledgment or an in-flight mechanism is required for that recovery.

Producer outcomes can also be ambiguous between commit and observation of success. A send interrupted after its commit need not be absent from the database, so crash descriptions must distinguish uncommitted work from committed-but-unobserved work.

**Impact:** users can mistake durable storage of queued rows for end-to-end reliable delivery or assume retries cannot create duplicates.

Destructive receive is an existing documented design limitation; it should be distinguished from the separate P01 cancellation defect and from SQLite's consistency guarantees.

### P18. Performance is asserted as unit-test correctness

**Location:** `src/lib.rs:2160-2199`.

The performance comparison test requires batch sends to be at least five times faster than individual sends. This depends on storage, filesystem behavior, scheduling, load, and other environment characteristics.

**Evidence:** the test passed during the review, but the source-level requirement is environment-dependent rather than a correctness invariant.

**Impact:** potential flaky failures unrelated to message correctness. Performance comparisons belong in controlled benchmarks rather than mandatory unit-test pass criteria.

### P19. Formatting validation fails

**Location:** `src/lib.rs`, as reported by `cargo fmt --all -- --check`.

Rustfmt reports differences around transaction logging, query formatting, and newer tests.

**Impact:** a failed formatting check and inconsistent source style, not a runtime defect.

### P20. Important failure paths lack effective regression coverage

**Locations:** the existing unit tests beginning at `src/lib.rs:1592`.

The 23 library tests passed despite the defects above. Coverage does not adequately exercise:

- Cancellation after blocking receive work has started.
- A full sender with a deliberately constrained blocking pool.
- Cancelled waiting sends and their shutdown consequences.
- Zero-capacity validation.
- `blocking_send` directly on the current-thread executor.
- Independent database connections and read-to-write upgrade races.
- Initialization under a writer lock with an async timeout.
- FIFO under backward timestamps.
- SQL keywords and digit-leading table names.
- DLQ filename collisions and failed-transfer retries.
- An all-corrupt prefix followed by valid batch data.
- Trailing bytes and message-format compatibility.

The concurrency tests primarily share an in-process mutex and therefore do not demonstrate the independent-connection behavior promised by the WAL discussion. The current-thread blocking-send test exercises a different calling context, as described in P05.

**Impact:** a green test suite does not currently provide confidence in cancellation, failure recovery, or independent-connection behavior.

## All proposed solutions

### Architectural direction

The recommended design has two central rules:

1. **Do not delete a deliverable payload until the consumer explicitly acknowledges it.**
2. **Every blocking job is finite; wait for queue capacity asynchronously.**

A same-file DLQ then makes quarantine transactions atomic. The remaining fixes address transaction behavior, initialization, ordering, identifiers, serialization, benchmarking, compatibility, and documentation.

Code examples below illustrate the proposed design. They are not implementations already present in the project.

### S01. Introduce recoverable claim/ack delivery

**Addresses:** P01 and P17.

Replace destructive receive as the reliable default with delivery receipts, for example:

```rust
if let Some(delivery) = rx.recv().await? {
    process(&delivery.message).await?;
    delivery.ack().await?;
}
```

The durable lifecycle becomes:

```text
ready -> claimed -> acknowledged/deleted
            |
            v
       lease expires
            |
            v
          ready
```

Required behavior:

- Persist claim state and lease expiration in SQLite; do not remove the payload during receive.
- A cancelled claim task may finish claiming, but the stored payload remains recoverable.
- A process crash or abandoned receipt is recovered through lease expiry, without requiring successful async cleanup in `Drop`.
- Delete only on explicit acknowledgment.
- Include a claim-generation/fencing token in the receipt. A stale receipt must not delete a row claimed by a newer consumer.
- Make retries of acknowledgments safe and distinguish stale claims from completed acknowledgments.
- Support explicit negative acknowledgment/release and lease renewal for long-running work.
- Add `ack_batch` to retain batching performance; define whether an acknowledgment batch is atomic.
- Count ready and claimed/unacknowledged rows toward `max_size`; merely claiming a row does not free capacity.
- Expose or document the difference between total unacknowledged size and currently available messages.
- Document that at-least-once processing can produce duplicates; applications still need idempotency where duplicate side effects are unacceptable.

Simply checking a cancellation flag before commit is not a complete fix: cancellation can race with commit and with the async result handoff. Aborting a started blocking task's join handle does not stop that task.

**Compatibility:** this changes the reliable receive contract. Prefer a deliberate breaking release, such as 0.2. If compatibility is required, introduce a clearly named claim/receipt API and deprecate or explicitly label the old destructive `recv() -> T` convenience API. Keeping that legacy behavior must not be presented as fixing durable delivery through cancellation or consumer failure.

### S02. Use finite enqueue attempts and async backpressure

**Addresses:** P02, P03, P04, and the producer-outcome part of P17.

Serialize each message or batch once, then loop asynchronously around a single transaction attempt:

```rust
loop {
    match try_insert_in_blocking_task(&serialized).await? {
        InsertAttempt::Committed => return Ok(()),
        InsertAttempt::Full => wait_for_capacity().await,
    }
}
```

The synchronous helper must:

1. Lock the connection.
2. Start one `BEGIN IMMEDIATE` transaction.
3. Check capacity and insert, or return `Full` without inserting.
4. Validate affected-row counts where an exact count is required.
5. Commit or roll back, release the lock, and exit.

It must not sleep or repeatedly poll while occupying a blocking worker.

The async layer should:

- Use `tokio::sync::Notify` for queue instances sharing state.
- Register the notification before checking capacity to prevent missed wakeups.
- Notify after successful acknowledgments, clears, and quarantine operations that free main-queue capacity.
- Retain a bounded async polling interval as a fallback for changes from another process or an independently opened queue, which cannot trigger the local notification.
- Use `tokio::time::sleep`/async notification outside blocking jobs.
- Stop issuing attempts when the waiting future is cancelled.
- Reject `Some(0)` during configuration validation, before database creation.
- Preserve immediate rejection of batches larger than the maximum capacity.

**Cancellation qualification:** a short insert transaction that has already started can still commit after the caller cancels. Document that its outcome may be unknown; do not claim that moving the wait loop makes every in-progress write abortable.

Provide an optional `send_with_id` or idempotency-key API for safe retries:

- The caller reuses the same stable key after an uncertain result.
- Store and enforce uniqueness per logical queue.
- A retry with the same key and payload resolves to the same logical enqueue.
- Reject a conflicting payload for an already-used key instead of silently treating it as success.

This removes immortal capacity waiters while giving callers a way to reconcile the remaining producer acknowledgment gap.

### S03. Define safe synchronous-send calling contexts

**Addresses:** P05.

Do not create a nested Tokio runtime. Share a synchronous persistence helper with the async implementation and use an explicit context policy:

| Context | Proposed behavior |
| --- | --- |
| Outside Tokio | Call the synchronous persistence helper directly; no temporary runtime is needed |
| Inside a Tokio multi-thread runtime | Invoke the synchronous helper through `block_in_place` |
| Inside a Tokio current-thread runtime | Return an explicit unsupported-context error and direct the caller to async `send` |

For truly synchronous callers, any blocking capacity wait belongs on their explicitly blocking thread, not inside an indefinitely running `spawn_blocking` job. Document blocking behavior and consider timeout/cancellation options for synchronous waits.

Tests must call `blocking_send` directly in each relevant context. Keep the OS-thread test as outside-runtime coverage, not as a substitute for current-thread coverage.

### S04. Reserve the writer before read/modify operations

**Addresses:** P06.

Use immediate transactions for operations that read and then modify queue state:

```rust
let tx = conn.transaction_with_behavior(
    rusqlite::TransactionBehavior::Immediate,
)?;
```

Apply this to:

- Single and batch claims/receives.
- Capacity checks plus enqueues.
- DLQ transfers.
- Acknowledgment operations that validate state before changing it.

Acquire the writer reservation before reading the message snapshot, rather than trying to upgrade a deferred snapshot later.

For remaining ordinary lock contention:

- Keep individual blocking attempts finite.
- Retry with bounded backoff, optionally jitter, outside the blocking worker.
- Honor a configured deadline or retry budget where applicable.
- If a snapshot failure occurs, retry the entire transaction, not just the DELETE.
- Do not silently discard errors or continue using a transaction that storage errors have invalidated.

Add deterministic independent-connection tests, including a producer write between the old implementation's SELECT and DELETE.

### S05. Offload all SQLite initialization

**Addresses:** P07.

Convert the path to an owned `PathBuf` and move the complete initialization sequence into one blocking task:

```rust
let path = db_path.as_ref().to_path_buf();

let queue = tokio::task::spawn_blocking(move || {
    initialize_queue(path, table_name, max_size, durability)
})
.await
.map_err(|e| DiskQueueError::TaskJoin(e.to_string()))?;
```

The synchronous initializer should open connections, configure PRAGMAs, validate/migrate schema, and construct the queue. Pure input validation should occur before performing file work where practical.

Initialization must be idempotent because an async timeout can stop waiting while the finite blocking initialization task continues. A responsive timeout does not imply that every underlying file operation was aborted or that no database was created.

Update the documentation to distinguish async offloading from cancellation of an already-started synchronous operation.

### S06. Order deliverable messages by insertion ID

**Addresses:** P08.

Use:

```sql
SELECT id, data
FROM "messages"
ORDER BY id ASC
LIMIT ?;
```

For claim-based delivery, add the availability condition while retaining ID ordering.

- Keep `created_at` for auditing/metadata, not queue ordering.
- Remove the timestamp-ordering index unless another documented feature needs it.
- Define FIFO as selection/claim order among available rows; concurrent processing and redelivery need not complete in that order.
- Test backward timestamps, identical timestamps, and ordering across reopen.

### S07. Quote identifiers consistently and clarify validation

**Addresses:** P09.

Validate names once and quote every generated identifier in table/index creation and all cached INSERT, SELECT, DELETE, COUNT, EXISTS, clear, and DLQ statements.

A general-purpose quoting helper escapes embedded quotes by doubling them:

```rust
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
```

Validation remains a separate step. With the current policy excluding quote characters, quoting validated names is simpler, but all statements must still use the same helper/result.

Also:

- Reject SQLite's reserved `sqlite_` namespace.
- Reserve an internal namespace for library metadata and DLQ tables so user queue names cannot collide with them.
- Choose and document one character policy; preserve Unicode support if that remains intended.
- Make the length limit explicitly a UTF-8 byte limit or change validation to count characters, matching the documentation.
- Return validation errors for prohibited internal names rather than surprising database syntax errors.
- Test keywords, digit-leading names, supported Unicode, empty names, invalid characters, and length boundaries.

Do not fix only CREATE TABLE while leaving cached queries unquoted.

### S08. Use same-database atomic DLQ storage

**Addresses:** P10 and P12.

**Preferred design:** store dead letters in an internal table in the main SQLite database, for example `__disk_queue_dlq`, using the same connection and transaction as the main row change.

An illustrative transfer is:

```sql
BEGIN IMMEDIATE;

INSERT INTO "__disk_queue_dlq" (...)
VALUES (...);

DELETE FROM "messages"
WHERE id = ?;

COMMIT;
```

Requirements:

- Preserve the original payload, original ID, original enqueue timestamp, error details, and quarantine timestamp.
- Include a stable logical queue identity persisted in metadata.
- Validate the required insert/delete row counts.
- Roll back the whole transfer on failure; preserve the main-queue row.
- For a batch affected by a storage/transaction failure, return an error rather than continuing with an invalid transaction or returning values whose state is uncertain.
- Use a uniqueness constraint on stable queue/item identity for idempotency.
- Do not rely on `original_id` alone across independent databases or a recreated logical queue whose IDs restart.
- Notify capacity waiters only after a successful committed transfer.

With both tables in one transaction, a crash cannot leave a committed DLQ copy with an uncommitted main deletion from that transfer.

**Alternative if separate files remain supported:** append a suffix to the complete filename or allow an explicit path:

```text
queue.db     -> queue.db.dlq.db
queue.sqlite -> queue.sqlite.dlq.db
```

Add stable source identity and idempotent writes. This fixes path collisions and repeated copies but does not make a cross-file WAL transfer crash-atomic. Do not assume that using `ATTACH` supplies that guarantee.

**Migration requirements:**

1. Version the internal schema and assign/persist logical queue identities.
2. Import legacy dead letters through restart-safe migration steps.
3. Track imports so interruption/retry does not append the same legacy row again.
4. Preserve payloads and existing duplicates as legacy evidence when their original identity cannot be established safely.
5. Do not silently assign a shared legacy DLQ to one main database when multiple database files could have contributed to it; require an explicit mapping or retain unknown provenance.
6. Do not delete legacy files until migration has been verified and deletion is explicitly authorized.
7. Update inspection/recovery examples and tests to inspect the new DLQ table, not merely expect a separate file.

### S09. Repair benchmark runtime usage and measurement boundaries

**Addresses:** P11 and P18.

Remove `rt.block_on` from closures executed by Criterion's async runner.

Possible approaches:

- Create the runtime, temporary directory, and queue before entering `b.to_async(&rt)`.
- Use an async `iter_custom` routine and await setup/reset outside the interval being measured.

In either case:

- Keep temporary storage alive for the whole benchmark.
- Prefer `TempDir` so the database and its WAL/SHM/DLQ sidecars are cleaned up together.
- Reset/drain queue state outside measured intervals when necessary.
- Avoid unintentionally timing connection destruction or checkpointing; include those costs only in an explicitly labeled benchmark.
- Define what is measured: enqueue, claim, acknowledgment, or complete send/process/ack flow after the API changes.
- Keep message sizes, batch sizes, and throughput labels consistent with the measured operation.
- Run both the normal benchmark and a smoke-test mode.

Remove the required five-times speedup assertion from the unit test. Keep correctness assertions in tests and performance comparisons in Criterion or explicitly controlled performance runs. Do not advertise a fixed speedup as a universal hardware guarantee.

### S10. Scan batches by valid messages, not just initial rows

**Addresses:** P13.

Continue scanning until either:

- `limit` valid deliverable messages have been collected, or
- no further deliverable rows remain in the transaction.

Quarantine corrupt rows and continue past them. Use ID-based/keyset pagination rather than OFFSET:

```sql
WHERE id > ?
ORDER BY id ASC
LIMIT ?;
```

Keep the last scanned ID and the remaining valid-message budget. Do not revisit failed rows indefinitely or skip rows because earlier deletions changed OFFSET positions.

A large corrupt prefix can otherwise make a transaction run for too long. If a scan/work budget is introduced, return explicit progress/exhaustion information rather than an empty vector that masquerades as a drained queue.

With claim/ack delivery, document that an empty batch means no messages are currently available for delivery, not that there are no claimed/unacknowledged rows or that producers can never enqueue more messages.

### S11. Require complete decoding and version message formats

**Addresses:** P14.

Use a shared decoder for single and batch paths:

```rust
let (item, remainder) = postcard::take_from_bytes::<T>(&data)
    .map_err(DiskQueueError::Deserialization)?;
```

Require `remainder.is_empty()`. Otherwise, classify the payload as invalid, preserve the original bytes, and transfer the row to the DLQ with an informative error.

Also:

- Store an explicit message-format/schema version in each record or a versioned envelope.
- Keep that version distinct from the library's internal SQLite schema version.
- Define supported decoders/migrations for old formats.
- Mark legacy payloads explicitly; do not assume their schema can be inferred reliably from postcard bytes.
- Preserve/quarantine unsupported versions rather than decoding them under an unrelated current type or discarding them.
- Test appended bytes, truncated input, incompatible versions, removed fields, and supported migrations.

Full byte consumption improves validation but does not detect every semantically incompatible schema change. Explicit format versioning addresses that separate limitation.

### S12. Make the minimum Rust version accurate

**Addresses:** P15.

Preferred approach:

```toml
[package]
rust-version = "1.88"
```

Update the README accordingly and verify the library, supported targets, and dependencies on that compiler. If dependencies require a higher minimum, declare the actual verified minimum instead.

Alternative: remove the let-chain syntax and verify the entire supported build on Rust 1.86 before continuing to advertise that version.

Add an MSRV CI job; do not infer compatibility solely from edition 2024 or a successful build on a much newer compiler.

### S13. Correct durability and delivery documentation

**Addresses:** P16 and P17, plus the clarified contracts from S01/S02/S07/S10/S11.

Document three separate concepts:

1. **Durable enqueue:** a successful send has committed according to the selected SQLite durability mode.
2. **Recoverable claim:** receipt-based delivery keeps the payload persisted until acknowledgment, with lease-based redelivery.
3. **Acknowledged processing:** application processing completes before acknowledgment; duplicate side effects still require application idempotency or a stronger transactional integration.

Required corrections:

- Do not equate queued-row durability with end-to-end delivery or exactly-once processing.
- Explain the limitations of any retained destructive receive API and explicitly state its cancellation behavior.
- Explain the commit/result acknowledgment gaps for both producers and consumers.
- Explain that cancelled/incomplete sends can have uncertain outcomes and that retries without stable keys can duplicate messages.
- State that idempotency handles duplicates, not missing payloads already deleted before processing.
- Scope fsync/power-loss claims to the configured durability level and SQLite/filesystem assumptions.
- Describe WAL `NORMAL` as consistent but not guaranteed durable across power loss/OS crash for recent commits.
- Describe WAL `EXTRA` as equivalent to `FULL` for durability rather than a stronger regulatory-compliance mode.
- Update method signatures, examples, queue size semantics, receipt lifetimes, lease expiry/renewal, batch results, table-name policy, and format-version handling.
- Update DLQ inspection and recovery commands for the chosen same-file or explicit-path layout.

SQLite WAL recovery remains responsible for database consistency. Signal handlers are not required to recover an unacknowledged persisted payload under the proposed design.

### S14. Strengthen tests, formatting, and CI

**Addresses:** P05, P18, P19, P20, and regression protection for every runtime change.

Promote the exploratory reproduction cases into permanent tests. Use deterministic barriers, controlled clocks, and explicit coordination where possible, rather than depending on arbitrary sleeps or machine speed.

Run supported combinations of:

- Current-thread and multi-thread Tokio runtimes.
- A deliberately small blocking pool, including one worker.
- Shared queue instances and independently opened SQLite connections.
- Windows and Linux.
- The declared MSRV and current stable Rust.

Keep positive tests for already-working behavior: rollback of partially executed batch inserts, deletion spanning more than 900 rows, and preservation of corrupt rows when quarantine fails.

Run formatting as an actual fix:

```bash
cargo fmt --all
```

Suggested CI checks, after the benchmark runtime fix:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo nextest run --all-targets
cargo test --doc
cargo bench --bench queue_benchmark -- --test
cargo audit
```

Doctests use Cargo's doctest runner because nextest does not execute them. Install/provision `cargo-audit` in CI before invoking it; its absence during this review is a verification limitation, not a detected dependency vulnerability.

Do not resolve the reproduction tests merely by weakening assertions to accept silent message loss. Where the receive API changes, rewrite the tests around durable claims, acknowledgment, fencing, and lease recovery while preserving the intended reliability property.

## Validation performed

These results describe the reviewed code, before any remediation. They are not results for an implementation of the proposed design.

| Check | Result |
| --- | --- |
| `cargo nextest run --lib` | 23 passed |
| `cargo test --doc` | 20 passed, 1 ignored |
| `cargo clippy --all-targets --all-features -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Failed with formatting differences |
| `cargo nextest run --all-targets` | 23 library tests passed; all four benchmark cases failed |
| `cargo bench --bench queue_benchmark -- --test` | Release benchmark panicked due to nested runtime execution |
| `cargo run --example throughput_demo --release` | Passed |
| Additional isolated probes | 18 cases: 15 failing edge-case expectations, 3 passing positive checks |

The 15 failing probe expectations are not 15 distinct root causes. Several cover different paths of one problem, and some express the stronger contracts proposed for batch exhaustion, DLQ idempotency, or strict decoding.

### Exploratory probe inventory

| Probe | Observed result | Related problem |
| --- | --- | --- |
| Cancelled single receive retains message | Failed: row count became zero without delivery | P01 |
| Cancelled batch receive retains messages | Failed: row count became zero without delivery | P01 |
| Full queue does not starve receiver | Failed: receiver timed out with one blocking worker | P02 |
| Cancelled full send does not enqueue later | Failed: cancelled message appeared later | P03 |
| Zero capacity is rejected | Failed: constructor accepted it | P04 |
| Blocking send inside current-thread runtime | Failed: runtime panic | P05 |
| Independent receivers avoid upgrade errors | Failed: one returned `DatabaseBusy` | P06 |
| Concurrent writer does not invalidate receive snapshot | Failed: extended SQLite error 517 | P06 |
| Constructor respects async timeout | Failed: approximately 460 ms of executor blocking defeated a 50 ms timeout | P07 |
| FIFO survives clock rollback | Failed: message 2 preceded message 1 | P08 |
| Documented table names are accepted | Failed: keywords/digit-leading names produced syntax errors | P09 |
| Different database extensions have isolated DLQs | Failed: both contributed to one DLQ table | P10 |
| Retry after failed deletion does not duplicate dead letter | Failed: two DLQ entries for one retained main row | P12 |
| Empty batch means no valid messages remain | Failed: a valid message remained queued | P13 |
| Trailing serialized bytes are rejected | Failed: appended bytes were ignored | P14 |
| DLQ failure preserves corrupt row | Passed for single and batch paths | Positive invariant |
| Failed batch insert rolls back earlier items | Passed | Positive invariant |
| Batch deletion handles more than 900 rows | Passed with 1,200 messages | Positive invariant |

The exploratory probe crate was created under `target/review-probes/`, which is ignored by Git. It is not included in this documentation commit and may be removed by cleaning the build directory. In the original review working tree, it can be run with:

```bash
cargo nextest run --manifest-path target/review-probes/Cargo.toml --target-dir target --no-fail-fast
```

Permanent versions belong in the project's normal regression test suite, not under `target/`.

### Verification limits

- Testing was performed on Windows, not on every supported operating system.
- Independent connections were tested; a full multi-process crash-injection matrix was not performed.
- Actual power-cut testing was not performed.
- The failed-deletion DLQ scenario used fault injection, not a physical disk failure.
- Backward timestamps were simulated without changing the machine clock.
- `cargo-audit` was not installed, so dependency vulnerability auditing was not performed.
- A successful throughput example does not establish a universal performance multiplier or prove failure-path safety.

## Regression and acceptance checklist

Checked items were implemented and verified locally (see [Implementation status](#implementation-status)); unchecked items remain open.

### Delivery, capacity, and shutdown

- [x] Cancelling single/batch claims cannot delete an unacknowledged payload. *(Implemented for `recv`/`recv_batch` without a claim API: a cancelled receive cannot delete an undelivered payload.)*
- [ ] Abandoned claims and process restarts permit redelivery after lease expiry.
- [ ] A stale receipt cannot acknowledge/delete a newer claim.
- [ ] Lease renewal, release, and acknowledgment retries have documented behavior.
- [ ] Batch acknowledgment has tested atomicity and affected-row validation.
- [ ] Capacity counts ready plus claimed/unacknowledged rows.
- [x] A full queue remains drainable with one blocking worker.
- [x] Cancelling a capacity waiter stops future attempts; no immortal worker remains.
- [x] Runtime shutdown is not held indefinitely by async capacity waiters.
- [x] Zero capacity and oversized batches produce deliberate errors.
- [ ] Producer retry keys reconcile committed-but-unobserved enqueues and reject conflicting payloads.

### SQLite, names, and message integrity

- [x] Independent receivers and producers do not encounter deferred snapshot-upgrade failures in the supported access pattern.
- [x] Remaining busy errors use finite attempts and bounded async retry policies. *(Busy errors are retried twice; each attempt is a new finite blocking job, bounded by the 5 s busy timeout.)*
- [x] Initialization does not block executor timers and is safe to retry after cancellation.
- [x] FIFO is based on insertion IDs, including backward/equal timestamps and reopen.
- [x] All generated SQL identifiers use one quoting policy.
- [x] Keyword, digit-leading, Unicode, invalid, and reserved names have tested behavior.
- [x] Logical queue identities and DLQ storage cannot collide across independent queues.
- [x] Quarantine insert/delete either commit together or preserve the original row.
- [x] Failed/retried transfers do not create new duplicate dead letters.
- [x] Legacy shared DLQs are not silently misattributed during migration. *(Legacy files are not imported at all.)*
- [x] An all-corrupt prefix does not hide valid batch messages behind an ambiguous empty result.
- [ ] Trailing/truncated bytes and unsupported message versions preserve the original payload in a recoverable form. *(Partly: trailing/truncated bytes are dead-lettered; format versions are not implemented.)*
- [x] Already-working batch rollback, large deletion chunks, and DLQ-failure preservation remain covered.

### API, documentation, and tooling

- [x] `blocking_send` has tested outside-runtime, multi-thread, and current-thread behavior without a nested-runtime panic.
- [x] Receive API compatibility and the proposed breaking release/migration strategy are explicit. *(The receive API is unchanged; other breaking changes are listed above.)*
- [x] The declared MSRV builds the supported project/dependency configuration.
- [x] Durability documentation correctly describes WAL `NORMAL`, `FULL`, and `EXTRA`.
- [x] Delivery documentation distinguishes persistence, claim recovery, acknowledgment, and duplicate processing.
- [x] Examples and DLQ recovery commands match the implemented schema and API.
- [x] Unit tests no longer require a fixed performance multiplier.
- [x] Benchmarks run without nested runtimes and label measurement boundaries correctly.
- [ ] Formatting, Clippy, nextest, doctests, benchmark smoke tests, and dependency auditing pass in CI. *(Locally: fmt, clippy, `cargo test --all-targets`, doctests and the bench smoke test pass; no CI exists, and nextest and `cargo audit` were not available.)*
- [ ] Windows/Linux and MSRV/stable coverage are established.

## Recommended implementation order

1. **Decide the delivery contract and compatibility boundary.** Adopt receipts/leases/acknowledgments as the reliable API; document any retained destructive legacy API.
2. **Design the internal schema and migration.** Persist logical queue identities, claim state, format versions, and same-file DLQ storage. Preserve legacy data and ambiguous provenance.
3. **Make blocking operations finite.** Replace the blocking capacity loops with async waiting, reject zero capacity, and define producer idempotency keys and synchronous calling contexts.
4. **Fix transaction and initialization behavior.** Use immediate read/modify transactions, bounded retries, and offloaded idempotent initialization.
5. **Correct ordering, identifiers, decoding, and batching.** Use insertion IDs, quote consistently, require full decoding, version formats, and distinguish batch progress from exhaustion.
6. **Update tests, benchmarks, examples, and documentation together.** Promote regression cases, remove speed-dependent unit-test assertions, repair Criterion setup, format the source, and establish CI checks.
7. **Verify and release deliberately.** Run the acceptance checklist, migration/recovery tests, supported platform/compiler matrix, and dependency audit before claiming stronger reliability guarantees.

The key distinction is that SQLite protects committed database state, while the library must separately ensure reliable handoff, recoverable delivery, and forward progress. Durable storage alone is not a substitute for acknowledgment-based processing or cancellation-safe coordination.
