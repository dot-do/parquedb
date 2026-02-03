# E2E Benchmarks on Deployed Workers

This guide describes how to run end-to-end performance benchmarks against deployed ParqueDB workers, measuring real-world latency, throughput, and backend performance.

## Architecture Overview

```
+------------------+     HTTPS      +--------------------+
|  Local Client    | ------------> |  Deployed Worker   |
|  (scripts/...)   |               |  (Cloudflare)      |
+------------------+               +--------------------+
                                           |
                                   +-------+-------+
                                   |               |
                              +----v----+    +-----v-----+
                              | R2      |    | Durable   |
                              | Storage |    | Objects   |
                              +---------+    +-----------+
```

### Components

1. **Benchmark Worker** (`src/worker/benchmark-e2e.ts`): Deployed worker endpoints that execute benchmarks and return metrics
2. **Local Runner** (`scripts/run-e2e-benchmark.ts`): Node.js script that calls the worker and collects results
3. **CI Integration** (`.github/workflows/e2e-benchmark.yml`): Automated daily benchmarks with alerting

## Endpoints

The benchmark worker exposes these endpoints under the `/benchmark/e2e/` prefix:

### `GET /benchmark/e2e/health`
Health check endpoint to verify R2 and Cache connectivity.

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2025-01-15T10:30:00Z",
  "checks": {
    "r2": { "status": "ok", "latencyMs": 15 },
    "cache": { "status": "ok", "latencyMs": 2 }
  },
  "metadata": {
    "colo": "SJC"
  }
}
```

### `GET /benchmark/e2e/cold-start`
Forces a cold start measurement by creating a new isolate.

**Response:**
```json
{
  "coldStartMs": 45,
  "workerInitMs": 12,
  "cacheInitMs": 8,
  "firstQueryMs": 25,
  "metadata": {
    "colo": "SJC",
    "timestamp": "2025-01-15T10:30:00Z"
  }
}
```

### `GET /benchmark/e2e/crud/:operation`
Benchmarks CRUD operations. Valid operations: `create`, `read`, `update`, `delete`, `batch-create`, `batch-read`.

**Query Parameters:**
- `iterations`: Number of iterations (default: 10)
- `batchSize`: For batch operations (default: 100)
- `warmup`: Warmup iterations (default: 2)

**Response:**
```json
{
  "operation": "create",
  "iterations": 10,
  "latencyMs": {
    "p50": 15,
    "p95": 25,
    "p99": 32,
    "avg": 17,
    "min": 12,
    "max": 35
  },
  "throughput": {
    "opsPerSec": 58.8,
    "totalTimeMs": 170
  }
}
```

### `GET /benchmark/e2e/query/:type`
Benchmarks query patterns. Valid types: `simple-filter`, `range-filter`, `compound-filter`, `full-text-search`, `pagination`, `aggregation`.

**Query Parameters:**
- `dataset`: Dataset to query (default: `imdb`)
- `iterations`: Number of iterations (default: 5)
- `limit`: Result limit for queries (default: 100)

**Response:**
```json
{
  "queryType": "compound-filter",
  "dataset": "imdb",
  "iterations": 5,
  "latencyMs": {
    "p50": 45,
    "p95": 78,
    "p99": 92,
    "avg": 52
  },
  "queryStats": {
    "rowsScanned": 1500,
    "rowsReturned": 42,
    "rowGroupsSkipped": 8,
    "rowGroupsScanned": 2
  }
}
```

### `GET /benchmark/e2e/backend/:type`
Backend-specific benchmarks. Valid types: `iceberg`, `delta`, `native-parquet`.

**Query Parameters:**
- `operation`: `write`, `read`, `time-travel` (default: all)
- `iterations`: Number of iterations (default: 3)
- `dataSize`: Rows to generate (default: 1000)

**Response:**
```json
{
  "backend": "iceberg",
  "operations": {
    "write": {
      "latencyMs": { "p50": 120, "p95": 180 },
      "bytesWritten": 45000,
      "rowsWritten": 1000
    },
    "read": {
      "latencyMs": { "p50": 35, "p95": 55 },
      "bytesRead": 12000,
      "rowsRead": 1000
    },
    "time-travel": {
      "latencyMs": { "p50": 42, "p95": 68 },
      "snapshotsAvailable": 5
    }
  }
}
```

## Deploying Benchmark Workers

### Prerequisites

1. Wrangler CLI installed and authenticated
2. R2 bucket configured with benchmark datasets
3. Environment variables set in `wrangler.toml`

### Deploy to Staging

```bash
# Deploy benchmark worker
wrangler deploy src/worker/benchmark-e2e.ts --env staging

# Verify deployment
curl https://parquedb-staging.workers.dev/benchmark/e2e/health
```

### Deploy to Production

```bash
wrangler deploy src/worker/benchmark-e2e.ts --env production
```

## Running Benchmarks Externally

### Basic Usage

```bash
# Run all benchmarks against production
bun scripts/run-e2e-benchmark.ts --url https://parquedb.workers.do

# Run specific benchmark types
bun scripts/run-e2e-benchmark.ts --benchmarks cold-start,crud

# Compare backends
bun scripts/run-e2e-benchmark.ts --backend iceberg --output json > iceberg-results.json
bun scripts/run-e2e-benchmark.ts --backend delta --output json > delta-results.json
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--url` | Worker URL | `https://parquedb.workers.do` |
| `--benchmarks` | Comma-separated benchmark types | all |
| `--backend` | Backend to test: `iceberg`, `delta`, `native` | all |
| `--iterations` | Iterations per test | 5 |
| `--warmup` | Warmup iterations | 2 |
| `--output` | Output format: `table`, `json`, `markdown` | table |
| `--verbose` | Show detailed results | false |
| `--concurrency` | Concurrent requests for throughput tests | 1 |

### Output Formats

**Table (default):**
```
E2E Benchmark Results
=====================
Benchmark           P50     P95     Throughput
-----------         ---     ---     ----------
cold-start          45ms    62ms    -
crud/create         15ms    25ms    58.8 ops/s
crud/read           8ms     12ms    125.0 ops/s
query/simple        32ms    48ms    31.2 ops/s
```

**JSON:**
```json
{
  "metadata": {
    "url": "https://parquedb.workers.do",
    "timestamp": "2025-01-15T10:30:00Z",
    "duration": 45000
  },
  "benchmarks": {
    "cold-start": {...},
    "crud": {...},
    "query": {...},
    "backend": {...}
  },
  "summary": {
    "avgLatencyMs": 28,
    "p95LatencyMs": 52,
    "totalOpsPerSec": 215
  }
}
```

## Metrics to Collect

### Cold Start Metrics
- **Worker initialization time**: Time to load Worker code
- **Cache initialization**: Time to open Cache API
- **First query latency**: End-to-end latency for first request

### Warm Request Metrics
- **P50/P95/P99 latency**: Distribution of response times
- **Throughput (ops/sec)**: Sustained request rate
- **Cache hit rate**: Percentage of requests served from cache

### Backend-Specific Metrics

**All Backends:**
- Read latency (p50, p95)
- Write latency (p50, p95)
- Bytes read/written
- Row groups scanned

**Iceberg:**
- Snapshot creation time
- Time-travel query overhead
- Manifest file parsing time

**Delta:**
- Transaction log parsing time
- Checkpoint reading time
- Vacuum operation time

**Native Parquet:**
- Row group skip rate
- Predicate pushdown effectiveness
- Column pruning savings

## Comparing Backends

Run comparative benchmarks:

```bash
# Generate comparison report
bun scripts/run-e2e-benchmark.ts \
  --benchmarks backend \
  --compare-backends \
  --output markdown > backend-comparison.md
```

**Example Output:**
```markdown
## Backend Comparison

| Metric | Iceberg | Delta | Native |
|--------|---------|-------|--------|
| Write P50 | 120ms | 95ms | 45ms |
| Write P95 | 180ms | 140ms | 75ms |
| Read P50 | 35ms | 38ms | 28ms |
| Time-travel | Yes | Yes | No |
| ACID | Yes | Yes | No |
```

### When to Use Each Backend

- **Native Parquet**: Best for read-heavy workloads with simple queries
- **Iceberg**: Best for time-travel and schema evolution needs
- **Delta**: Best for frequent updates with transaction support

## CI Integration

### GitHub Actions Workflow

The workflow runs daily and on manual trigger:

```yaml
name: E2E Benchmark
on:
  schedule:
    - cron: '0 6 * * *'
  workflow_dispatch:
    inputs:
      url:
        description: 'Worker URL'
        default: 'https://parquedb.workers.do'
```

### Alerting

Configure alerts for performance regressions:

```yaml
- name: Check for regression
  run: |
    bun scripts/run-e2e-benchmark.ts \
      --output json \
      --compare-baseline baseline.json \
      --threshold 20 || exit 1
```

### Storing Results

Results are stored as artifacts and can be published to GitHub Pages:

```yaml
- name: Store benchmark results
  uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: 'customSmallerIsBetter'
    output-file-path: benchmark-results.json
```

## Troubleshooting

### High Cold Start Times
- Check Worker bundle size
- Verify no heavy initialization code
- Consider lazy loading dependencies

### Inconsistent Results
- Increase iteration count
- Add more warmup iterations
- Check for network variance

### Backend Timeouts
- Reduce data sizes for writes
- Check R2 bucket region
- Verify sufficient CPU allocation

## Related Documentation

- [Benchmark Infrastructure](../architecture/benchmarks.md)
- [Backend Architecture](../architecture/backends.md)
- [Performance Tuning](./performance-tuning.md)
