use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use disk_backed_queue::DiskBackedQueue;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

fn benchmark_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    // Define message sizes to benchmark
    let msg_sizes = [64, 1024]; // 64 bytes, 1KB

    for size in msg_sizes {
        let msg = vec![0u8; size];

        // Benchmark individual send
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("send", size), &size, |b, &_size| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let tmp = NamedTempFile::new().unwrap();
                    let path = tmp.path().to_owned();
                    let queue = rt
                        .block_on(DiskBackedQueue::<Vec<u8>>::new(
                            &path,
                            "bench_send".to_string(),
                            None,
                        ))
                        .unwrap();
                    (queue, tmp, msg.clone())
                },
                |(queue, _tmp, msg)| async move {
                    queue.send(msg).await.unwrap();
                },
            );
        });

        // Benchmark batch send (batch size 100)
        let batch_size = 100;
        let total_bytes = (size * batch_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(
            BenchmarkId::new("send_batch_100", size),
            &size,
            |b, &_size| {
                b.to_async(&rt).iter_with_setup(
                    || {
                        let tmp = NamedTempFile::new().unwrap();
                        let path = tmp.path().to_owned();
                        let queue = rt
                            .block_on(DiskBackedQueue::<Vec<u8>>::new(
                                &path,
                                "bench_batch".to_string(),
                                None,
                            ))
                            .unwrap();
                        let batch = vec![msg.clone(); batch_size];
                        (queue, tmp, batch)
                    },
                    |(queue, _tmp, batch)| async move {
                        queue.send_batch(batch).await.unwrap();
                    },
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_throughput);
criterion_main!(benches);
