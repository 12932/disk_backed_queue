// Quick throughput demonstration comparing single vs batch operations
//
// This example demonstrates the performance difference between single message
// operations and batch operations using 1KiB messages. It's designed for quick
// spot-checks, not comprehensive benchmarking.

use disk_backed_queue::disk_backed_channel;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tempfile::NamedTempFile;

#[derive(Serialize, Deserialize, Clone)]
struct Message1KB {
    id: u64,
    // Create ~1KiB payload (using Vec since arrays > 32 have issues with serde)
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

impl Message1KB {
    fn new(id: u64) -> Self {
        Self {
            id,
            data: vec![0x42; 1008], // ~1KiB total with id
        }
    }
}

// Helper module for efficient byte serialization
mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<u8>::deserialize(deserializer)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Disk-Backed Queue Throughput Demo ===");
    println!("Message size: ~1KiB");
    println!();

    // Test parameters
    const SINGLE_COUNT: usize = 500;
    const BATCH_COUNT: usize = 5000;
    const BATCH_SIZE: usize = 100;

    // Create temporary database files
    let single_db = NamedTempFile::new()?;
    let batch_db = NamedTempFile::new()?;

    // ========== SINGLE MESSAGE TEST ==========
    println!("--- Single Message Operations ---");
    let (tx, mut rx) =
        disk_backed_channel::<Message1KB, _>(single_db.path(), "single_test".to_string(), None)
            .await?;

    // Measure single send operations
    let messages: Vec<Message1KB> = (0..SINGLE_COUNT)
        .map(|i| Message1KB::new(i as u64))
        .collect();

    let start = Instant::now();
    for msg in &messages {
        tx.send(msg.clone()).await?;
    }
    let send_duration = start.elapsed();
    let send_throughput = SINGLE_COUNT as f64 / send_duration.as_secs_f64();

    println!(
        "  Send {} messages: {:?} ({:.0} msg/sec)",
        SINGLE_COUNT, send_duration, send_throughput
    );

    // Measure single recv operations
    let start = Instant::now();
    for _ in 0..SINGLE_COUNT {
        rx.recv().await?.expect("Queue should not be empty");
    }
    let recv_duration = start.elapsed();
    let recv_throughput = SINGLE_COUNT as f64 / recv_duration.as_secs_f64();

    println!(
        "  Recv {} messages: {:?} ({:.0} msg/sec)",
        SINGLE_COUNT, recv_duration, recv_throughput
    );

    let single_total = send_duration + recv_duration;
    let single_total_throughput = (SINGLE_COUNT * 2) as f64 / single_total.as_secs_f64();
    println!(
        "  Total time: {:?} ({:.0} ops/sec)",
        single_total, single_total_throughput
    );
    println!();

    // ========== BATCH OPERATIONS TEST ==========
    println!(
        "--- Batch Operations ({} messages per batch) ---",
        BATCH_SIZE
    );
    let (tx, mut rx) =
        disk_backed_channel::<Message1KB, _>(batch_db.path(), "batch_test".to_string(), None)
            .await?;

    // Prepare batches
    let num_batches = BATCH_COUNT / BATCH_SIZE;
    let mut batches: Vec<Vec<Message1KB>> = Vec::with_capacity(num_batches);
    for batch_idx in 0..num_batches {
        let batch: Vec<Message1KB> = (0..BATCH_SIZE)
            .map(|i| Message1KB::new((batch_idx * BATCH_SIZE + i) as u64))
            .collect();
        batches.push(batch);
    }

    // Measure batch send operations
    let start = Instant::now();
    for batch in &batches {
        tx.send_batch(batch.clone()).await?;
    }
    let batch_send_duration = start.elapsed();
    let batch_send_throughput = BATCH_COUNT as f64 / batch_send_duration.as_secs_f64();

    println!(
        "  Send {} messages ({} batches): {:?} ({:.0} msg/sec)",
        BATCH_COUNT, num_batches, batch_send_duration, batch_send_throughput
    );

    // Measure batch recv operations
    let start = Instant::now();
    let mut total_received = 0;
    while total_received < BATCH_COUNT {
        let batch = rx.recv_batch(BATCH_SIZE).await?;
        total_received += batch.len();
    }
    let batch_recv_duration = start.elapsed();
    let batch_recv_throughput = BATCH_COUNT as f64 / batch_recv_duration.as_secs_f64();

    println!(
        "  Recv {} messages ({} batches): {:?} ({:.0} msg/sec)",
        BATCH_COUNT, num_batches, batch_recv_duration, batch_recv_throughput
    );

    let batch_total = batch_send_duration + batch_recv_duration;
    let batch_total_throughput = (BATCH_COUNT * 2) as f64 / batch_total.as_secs_f64();
    println!(
        "  Total time: {:?} ({:.0} ops/sec)",
        batch_total, batch_total_throughput
    );
    println!();

    // ========== COMPARISON ==========
    println!("--- Performance Comparison ---");
    let send_speedup = batch_send_throughput / send_throughput;
    let recv_speedup = batch_recv_throughput / recv_throughput;
    let total_speedup = batch_total_throughput / single_total_throughput;

    println!("  Batch send speedup: {:.1}x faster", send_speedup);
    println!("  Batch recv speedup: {:.1}x faster", recv_speedup);
    println!("  Overall speedup: {:.1}x faster", total_speedup);
    println!();

    // Data transfer rates
    let single_data_rate =
        (SINGLE_COUNT * 1024 * 2) as f64 / single_total.as_secs_f64() / 1024.0 / 1024.0;
    let batch_data_rate =
        (BATCH_COUNT * 1024 * 2) as f64 / batch_total.as_secs_f64() / 1024.0 / 1024.0;

    println!("--- Data Transfer Rates ---");
    println!("  Single operations: {:.2} MiB/sec", single_data_rate);
    println!("  Batch operations: {:.2} MiB/sec", batch_data_rate);
    println!();

    println!("=== Demo Complete ===");

    Ok(())
}
