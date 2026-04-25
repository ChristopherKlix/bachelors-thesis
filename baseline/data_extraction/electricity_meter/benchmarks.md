# Data Extraction - Electricity Meter

## Yearly Runtime Scaling of Daily ETL Pipeline (Single Device)

Incremental ETL Benchmark: Runtime Evolution Across a Year

This benchmark evaluates the runtime behavior of a daily ETL pipeline for a single device over the course of a full year (2025). For each day, the pipeline performs three steps:

**Extract:** fetches that day’s data from a REST API \
**Transform:** processes the data (e.g., converting meter readings to interval energy values, handling gaps via interpolation) \
**Load:** updates a yearly CSV file

The CSV acts as a cumulative store: it is initialized on the first day with a full-year index and progressively filled as new daily data arrives. Each run reads the existing CSV, updates the relevant portion, and writes the full file back to disk.

The benchmark iterates from `2025-01-01` to `2025-12-31`, measuring execution time per run for each stage (extract, transform, load). As the year progresses, the CSV grows from a few kilobytes to several megabytes, allowing observation of how increasing dataset size impacts runtime.

**Metric:** Time in seconds \
**Method Signature:** `Benchmark.run_device_for_full_year`

| Day        |  Extract | Transform |    Load   |   Total   |
| ---------- | -------- | --------- | --------- | --------- |
| 2025-01-01 | 0.144293 |  0.022313 |  0.910918 |  1.077524 |
| ...        |          |           |           |           |
| 2025-12-31 | 0.144279 |  0.012584 | 12.347187 | 12.504050 |

### Expectation

**Extract:** remains constant — small payload, dominated by network latency \
**Transform:** remains mostly constant — fixed daily data size \
**Load:** increases over time — full file read + write scales with CSV size

Overall runtime is expected to be increasingly dominated by the load step, with total execution time growing approximately linearly with file size. The primary bottleneck should be file I/O rather than computation or network latency.

## Scaling Behavior of Daily ETL Pipeline Across Devices

ETL Throughput Scaling with Increasing Device Count

This benchmark evaluates how the daily ETL pipeline scales with an increasing number of devices. For each run, the pipeline processes a single day of data per device, including extract (REST API), transform (data processing), and load (CSV update).

The number of devices is gradually increased (1, 2, 5, 10, 20, …, 2000) to observe how total runtime grows under different execution models:

1. **Sequential execution:** all devices are processed one after another in a single process and thread.
1. **Threaded execution:** a `ThreadPoolExecutor` is used to parallelize device processing, primarily to reduce idle time caused by I/O.
1. **Hybrid execution:** a `ProcessPoolExecutor` with 4 processes (matching CPU cores), where each process handles a subset of devices and internally uses a thread pool as in (2).

The benchmark measures total runtime per configuration and device count, allowing comparison of scaling behavior and parallelization efficiency.

**Metric:** Time in seconds \
**Method Signatures:** \
`Benchmark.benchmark_sequential_etl`, \
`Benchmark.benchmark_threaded_etl`, \
`Benchmark.benchmark_hybrid_etl`

| Device Count    | Sequential   | Threaded   | Hybrid      |
| --------------- | ------------ | ---------- | ----------- |
|               1 | based on avg |  29.862294 |   31.919517 |
|               2 | O(n) time    |  51.905274 |   28.905051 |
|               5 |              | 130.089864 |   68.194092 |
|              10 |              | 266.716428 |  124.840977 |
|              20 |              |            |  227.455077 |
|              50 |              |            |  632.797041 |
|             100 |              |            | 1515.811929 |
|             200 |              |            |             |
|             500 |              |            |             |
|            1000 |              |            |             |
|            2000 |              |            |             |
| time complexity | O(n)         |          - | O(log n)    |

### Expectations

1. **Sequential execution:** runtime scales linearly with the number of devices. No overlap of I/O → worst performance.
1. **Threaded execution:**
noticeable improvement vs. sequential (≈20–40%)
gains come from overlapping I/O (API + file access)
diminishing returns at higher device counts due to contention (disk, network, GIL overhead negligible here)
1. **Hybrid execution (4 processes + threads):**
best overall performance
processes reduce contention and better utilize CPU during transform + I/O orchestration
near-linear scaling up to moderate device counts, then tapering

**Overall:**

- threading improves latency hiding
- multiprocessing improves throughput and isolation
- bottlenecks will shift to disk I/O and API limits at higher scales
