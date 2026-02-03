# ParqueDB E2E Benchmark Suite

End-to-end performance benchmarks for deployed ParqueDB Cloudflare Workers.

## Overview

This benchmark suite measures real-world performance characteristics of a deployed ParqueDB worker, including:

- **Cold Start Latency**: Time for the first request after isolate teardown
- **Warm Request Latency**: Response times for subsequent requests
- **Cache Performance**: Edge cache hit vs miss latency comparison

## Quick Start

```bash
# Run with defaults against parque.do
npx tsx benchmarks/run.ts

# Run with JSON output
npx tsx benchmarks/run.ts --output results.json

# Quick test (fewer iterations)
npx tsx benchmarks/run.ts -c 2 -w 10 --cold-delay 30
```

## Usage

```bash
npx tsx benchmarks/run.ts [options]
```

### Options

| Option | Alias | Default | Description |
|--------|-------|---------|-------------|
| `--url` | `-u` | `https://parque.do` | Base URL of the deployed worker |
| `--cold-iterations` | `-c` | `5` | Number of cold start measurements |
| `--cold-delay` | | `65` | Seconds to wait between cold start tests |
| `--warm-iterations` | `-w` | `20` | Number of warm request measurements |
| `--output` | `-o` | | Output file path for JSON results |
| `--verbose` | `-v` | | Enable verbose logging |
| `--help` | `-h` | | Show help message |

### Examples

```bash
# Full benchmark against production
npx tsx benchmarks/run.ts --url https://parque.do -o prod-results.json

# Test against staging with verbose output
npx tsx benchmarks/run.ts -u https://staging.parque.do -v

# Quick sanity check
npx tsx benchmarks/run.ts -c 1 -w 5 --cold-delay 10

# Comprehensive benchmark
npx tsx benchmarks/run.ts -c 10 -w 100 --verbose -o comprehensive.json
```

## Output Format

Results are output as JSON with the following structure:

```typescript
interface BenchmarkReport {
  meta: {
    startTime: string      // ISO timestamp
    endTime: string        // ISO timestamp
    durationMs: number     // Total benchmark duration
    baseUrl: string        // Target URL
    config: { ... }        // Benchmark configuration
  }
  coldStart: {
    measurements: [{
      totalMs: number      // Client-measured total latency
      serverMs: number     // Server-reported latency
      metadata: {
        colo: string       // Cloudflare colo code
        timestamp: string
        isolateId?: string
      }
    }]
    stats: LatencyStats    // Percentiles for total
    serverStats: LatencyStats
  }
  warmRequests: {
    health: {
      endpoint: string
      measurements: [{
        totalMs: number
        serverMs: number
        cacheHit: boolean
        statusCode: number
      }]
      stats: LatencyStats
      cacheHitRate: number
    }
    crud: {
      create: { ... }
      read: { ... }
    }
  }
  cachePerformance?: {
    missLatency: LatencyStats
    hitLatency: LatencyStats
    improvementPercent: number
  }
  errors: [{
    phase: string
    endpoint: string
    error: string
    timestamp: string
  }]
}
```

### LatencyStats

```typescript
interface LatencyStats {
  p50: number   // 50th percentile (median)
  p95: number   // 95th percentile
  p99: number   // 99th percentile
  avg: number   // Mean
  min: number   // Minimum
  max: number   // Maximum
  stdDev: number // Standard deviation
}
```

## Understanding Cold Start Measurements

Cloudflare Workers isolates are typically evicted after 30-60 seconds of inactivity. To measure true cold starts:

1. We wait `--cold-delay` seconds (default: 65s) between measurements
2. Use cache-busting query parameters
3. Set `Cache-Control: no-store` headers

**Note**: Cold start measurements take significant time due to the required delays. For a 5-iteration test with 65-second delays, expect ~5 minutes just for cold start benchmarks.

## Programmatic Usage

You can import and run benchmarks programmatically:

```typescript
import { runBenchmarks, type BenchmarkConfig } from './benchmarks'

const config: Partial<BenchmarkConfig> = {
  baseUrl: 'https://parque.do',
  coldStartIterations: 3,
  warmIterations: 50,
  verbose: true,
}

const report = await runBenchmarks(config)
console.log(`Cold start p50: ${report.coldStart.stats.p50}ms`)
```

## Worker Endpoints

The benchmarks call these worker endpoints:

| Endpoint | Purpose |
|----------|---------|
| `/benchmark/e2e/health` | Health check with R2/Cache status |
| `/benchmark/e2e/cold-start` | Cold start measurement |
| `/benchmark/e2e/crud/create` | CRUD create benchmark |
| `/benchmark/e2e/crud/read` | CRUD read benchmark |
| `/health` | Standard health check (for cache testing) |

## Performance Targets

Reference targets for ParqueDB workers:

| Metric | Target (p50) | Target (p99) |
|--------|--------------|--------------|
| Cold Start | < 50ms | < 100ms |
| Warm Health | < 10ms | < 50ms |
| Warm CRUD Read | < 20ms | < 100ms |
| Cache Hit | < 5ms | < 20ms |

## Troubleshooting

### "Cannot connect to..." Error

Verify the worker is deployed and accessible:
```bash
curl https://parque.do/benchmark/e2e/health
```

### High Cold Start Variance

Cold start times can vary significantly based on:
- Cloudflare colo (data center)
- Time of day / load
- Worker bundle size
- R2 proximity

Run multiple iterations and focus on p50/p95 rather than individual measurements.

### No Cache Hits Detected

Edge caching may be disabled or the response may not be cacheable. Check:
- `Cache-Control` headers on responses
- `CF-Cache-Status` header values
- Cloudflare zone cache settings
