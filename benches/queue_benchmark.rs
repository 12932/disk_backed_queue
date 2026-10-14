//! Throughput of the queue operations.
//!
//! Each routine opens a fresh queue in its own temporary directory and times only the queue
//! operations; setup, prefilling, and dropping the queue (connection close, WAL checkpoint, file
//! removal) are excluded. Temporary files follow `TMPDIR`: if that is a RAM-backed filesystem
//! such as tmpfs, fsync is free and the results do not reflect disk durability costs.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use disk_backed_queue::DiskBackedQueue;
use std::time::Instant;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 100;

/// Opens an empty queue. The returned directory owns the database and its WAL/SHM files.
async fn fresh_queue(table_name: &str) -> (TempDir, DiskBackedQueue<Vec<u8>>) {
    let dir = TempDir::new().unwrap();
    let queue = DiskBackedQueue::new(dir.path().join("bench.db"), table_name.to_string(), None)
        .await
        .unwrap();
    (dir, queue)
}

async fn prefill(queue: &DiskBackedQueue<Vec<u8>>, msg: &[u8], count: usize) {
    let mut remaining = count;
    while remaining > 0 {
        let chunk = remaining.min(1000);
        queue.send_batch(vec![msg.to_vec(); chunk]).await.unwrap();
        remaining -= chunk;
    }
}

fn benchmark_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    // Define message sizes to benchmark
    let msg_sizes = [64, 1024]; // 64 bytes, 1KB

    for size in msg_sizes {
        let msg = vec![0u8; size];

        // One message per committed transaction
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("send", size), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (_dir, queue) = fresh_queue("bench_send").await;
                let start = Instant::now();
                for _ in 0..iters {
                    queue.send(msg.clone()).await.unwrap();
                }
                start.elapsed()
            });
        });

        group.bench_with_input(BenchmarkId::new("recv", size), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (_dir, queue) = fresh_queue("bench_recv").await;
                prefill(&queue, msg, iters as usize).await;
                let start = Instant::now();
                for _ in 0..iters {
                    queue.recv().await.unwrap().unwrap();
                }
                start.elapsed()
            });
        });

        // One batch of BATCH_SIZE messages per committed transaction. Building each batch
        // (cloning the payloads) is timed, but is negligible next to the commit.
        group.throughput(Throughput::Bytes((size * BATCH_SIZE) as u64));
        group.bench_with_input(BenchmarkId::new("send_batch_100", size), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (_dir, queue) = fresh_queue("bench_send_batch").await;
                let start = Instant::now();
                for _ in 0..iters {
                    queue
                        .send_batch(vec![msg.clone(); BATCH_SIZE])
                        .await
                        .unwrap();
                }
                start.elapsed()
            });
        });

        group.bench_with_input(BenchmarkId::new("recv_batch_100", size), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (_dir, queue) = fresh_queue("bench_recv_batch").await;
                prefill(&queue, msg, iters as usize * BATCH_SIZE).await;
                let start = Instant::now();
                for _ in 0..iters {
                    assert_eq!(
                        queue.recv_batch(BATCH_SIZE).await.unwrap().len(),
                        BATCH_SIZE
                    );
                }
                start.elapsed()
            });
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_throughput);
criterion_main!(benches);
