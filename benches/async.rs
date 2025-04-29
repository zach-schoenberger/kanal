use criterion::*;
use std::{thread::available_parallelism, time::Duration};

const BENCH_MSG_COUNT: usize = 1 << 20;

fn check_value(value: usize) {
    if value == 0 {
        println!("Value should not be zero");
    }
}

macro_rules! run_bench {
    ($b:expr, $kanal:expr, $writers:expr, $readers:expr) => {{
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(usize::from(available_parallelism().unwrap()))
            .enable_all()
            .build()
            .unwrap();
        $b.iter(|| {
            let (tx, rx) = $kanal;
            let mut handles = Vec::with_capacity($readers + $writers);
            for _ in 0..$readers {
                let rx = rx.clone();
                handles.push(rt.spawn(async move {
                    loop {
                        match black_box(rx.recv_async().await) {
                            Ok(value) => check_value(value),
                            Err(_) => break,
                        }
                    }
                }));
            }
            drop(rx);

            for _ in 0..$writers {
                let tx = tx.clone();
                handles.push(rt.spawn(async move {
                    for i in 0..BENCH_MSG_COUNT / $writers {
                        tx.send_async(i + 1).await.unwrap();
                    }
                }));
            }
            drop(tx);

            for handle in handles {
                rt.block_on(handle).unwrap();
            }
        });
    }};
}

fn mpmc(c: &mut Criterion) {
    let mut g = c.benchmark_group("async::mpmc");
    g.throughput(Throughput::Elements(BENCH_MSG_COUNT as u64));
    g.sample_size(10).warm_up_time(Duration::from_secs(1));
    let core_count = usize::from(available_parallelism().unwrap());

    g.bench_function("b0", |b| {
        run_bench!(b, kanal::bounded::<usize>(0), core_count, core_count);
    });
    g.bench_function("b0_contended", |b| {
        run_bench!(
            b,
            kanal::bounded::<usize>(0),
            core_count * 64,
            core_count * 64
        );
    });
    g.bench_function("b1", |b| {
        run_bench!(b, kanal::bounded::<usize>(1), core_count, core_count);
    });
    g.bench_function("bn", |b| {
        run_bench!(b, kanal::unbounded(), core_count, core_count);
    });
    g.finish();
}

fn mpsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("async::mpsc");
    g.throughput(Throughput::Elements(BENCH_MSG_COUNT as u64));
    g.sample_size(10).warm_up_time(Duration::from_secs(1));
    let core_count = usize::from(available_parallelism().unwrap());

    g.bench_function("b0", |b| {
        run_bench!(b, kanal::bounded::<usize>(0), core_count, 1);
    });
    g.bench_function("b0_contended", |b| {
        run_bench!(b, kanal::bounded::<usize>(0), core_count * 64, 1);
    });
    g.bench_function("b1", |b| {
        run_bench!(b, kanal::bounded::<usize>(1), core_count, 1);
    });
    g.bench_function("bn", |b| {
        run_bench!(b, kanal::unbounded(), core_count, 1);
    });
    g.finish();
}

fn spsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("async::spsc");
    g.throughput(Throughput::Elements(BENCH_MSG_COUNT as u64));
    g.sample_size(10).warm_up_time(Duration::from_secs(1));

    g.bench_function("b0", |b| {
        run_bench!(b, kanal::bounded::<usize>(0), 1, 1);
    });
    g.bench_function("b1", |b| {
        run_bench!(b, kanal::bounded::<usize>(1), 1, 1);
    });
    g.finish();
}
criterion_group!(async_bench, mpmc, mpsc, spsc);
criterion_main!(async_bench);
