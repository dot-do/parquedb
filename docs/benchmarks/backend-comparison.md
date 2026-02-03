# Backend Performance Benchmarks

Compare the performance of different table formats on Cloudflare R2:
- **Native Parquet** - Direct hyparquet read/write
- **Apache Iceberg** - With metadata, manifests, snapshots
- **Delta Lake** - With transaction log

## Quick Start

### Run Benchmark on Deployed Worker

```bash
# Full comparison (all backends, all operations)
curl "https://parquedb.workers.do/benchmark/backends"

# Specific backend
curl "https://parquedb.workers.do/benchmark/backends?backend=iceberg"

# Multiple backends
curl "https://parquedb.workers.do/benchmark/backends?backend=native,iceberg"

# Specific operations only
curl "https://parquedb.workers.do/benchmark/backends?operations=write,query"

# Custom dataset size and iterations
curl "https://parquedb.workers.do/benchmark/backends?size=1000&iterations=5"

# Keep data after benchmark (for inspection)
curl "https://parquedb.workers.do/benchmark/backends?cleanup=false"
```

### Query Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `backend` | `native`, `iceberg`, `delta`, `all` | `all` | Which backends to test |
| `operations` | `write`, `read`, `query`, `time-travel` | `write,read,query` | Which operations to test |
| `size` | number | `100` | Number of entities to test with |
| `iterations` | number | `3` | Iterations per operation |
| `cleanup` | `true`, `false` | `true` | Delete test data after benchmark |

## Example Response

```json
{
  "config": {
    "backends": ["native", "iceberg", "delta"],
    "operations": ["write", "read", "query"],
    "size": 100,
    "iterations": 3
  },
  "results": [
    {
      "backend": "native",
      "operations": [
        {
          "operation": "write",
          "latency": { "p50": 45.2, "p95": 52.1, "p99": 55.0, "mean": 46.8 },
          "bytesWritten": 28456,
          "rowsAffected": 100
        },
        {
          "operation": "read",
          "latency": { "p50": 12.3, "p95": 15.2, "p99": 16.1, "mean": 13.1 },
          "bytesRead": 28456,
          "rowsAffected": 100
        },
        {
          "operation": "query",
          "latency": { "p50": 8.5, "p95": 10.2, "p99": 11.0, "mean": 9.1 },
          "bytesRead": 15234,
          "rowsAffected": 12,
          "metadata": { "filter": "category = 'electronics'" }
        }
      ],
      "totalTimeMs": 245,
      "storageBytes": 28456
    }
  ],
  "comparison": {
    "fastestWrite": { "backend": "native", "p50Ms": 45.2 },
    "fastestRead": { "backend": "native", "p50Ms": 12.3 },
    "fastestQuery": { "backend": "delta", "p50Ms": 7.8 },
    "smallestStorage": { "backend": "native", "bytes": 28456 },
    "recommendation": "Use 'native' for your workload"
  },
  "metadata": {
    "timestamp": "2024-01-15T10:30:00.000Z",
    "colo": "SJC",
    "durationMs": 1250
  }
}
```

## Accessing Benchmark Data

Benchmark data is stored on the CDN bucket and accessible via:

```
https://cdn.workers.do/parquedb-benchmarks/{backend}/...
```

### Storage Layout

```
parquedb-benchmarks/
├── native/
│   └── data.parquet
├── iceberg/
│   ├── metadata/
│   │   └── v1.metadata.json
│   ├── manifest/
│   │   └── manifest-{id}.json
│   ├── manifest-list-{id}.json
│   └── data/
│       └── part-{id}.parquet
└── delta/
    ├── _delta_log/
    │   ├── 00000000000000000000.json
    │   └── ...
    └── part-{id}.parquet
```

## Local Benchmarks

Run benchmarks locally without deploying:

```bash
# All backends
npx tsx tests/benchmarks/run-backend-benchmark.ts

# Specific backend
npx tsx tests/benchmarks/run-backend-benchmark.ts --backend=iceberg

# Custom size
npx tsx tests/benchmarks/run-backend-benchmark.ts --sizes=100,1000,10000

# Output as markdown
npx tsx tests/benchmarks/run-backend-benchmark.ts --output=markdown
```

## Understanding the Results

### Write Performance
- **Native**: Single Parquet file write
- **Iceberg**: Data file + manifest + manifest list + metadata (4 writes)
- **Delta**: Data file + commit log entry (2 writes)

### Read Performance
- **Native**: Single file read
- **Iceberg**: Metadata → manifest list → manifest → data file (4 reads)
- **Delta**: Commit log scan → data file (2+ reads)

### Query Performance
All backends use the same Parquet query engine with predicate pushdown. The difference is in metadata overhead.

### Time Travel Support
- **Native**: Not supported
- **Iceberg**: Snapshot-based, query any point in time
- **Delta**: Version-based, query any commit version

## Recommendations

| Use Case | Recommended Backend |
|----------|---------------------|
| Simple analytics, no updates | Native Parquet |
| ACID transactions, time travel | Iceberg |
| Streaming updates, CDC | Delta Lake |
| Maximum read performance | Native Parquet |
| Schema evolution needed | Iceberg or Delta |
| Interop with Spark/Snowflake | Iceberg |
