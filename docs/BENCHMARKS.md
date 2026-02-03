---
title: Performance Benchmarks
description: Comprehensive performance benchmarks for ParqueDB operations across different scales and environments
---

This document provides comprehensive performance benchmarks for ParqueDB operations across different scales and environments.

## Table of Contents

- [Overview](#overview)
- [Running Benchmarks](#running-benchmarks)
- [CRUD Operations](#crud-operations)
- [Query Performance](#query-performance)
- [Scalability](#scalability)
- [Index Performance](#index-performance)
- [Cloudflare Workers Performance](#cloudflare-workers-performance)
- [Storage Comparisons](#storage-comparisons)
- [Performance Targets](#performance-targets)

## Overview

ParqueDB is designed for efficient read-heavy workloads with the following performance characteristics:

- **Sub-millisecond reads** for indexed lookups in Cloudflare Workers
- **Columnar storage** enables reading only necessary columns
- **Row-group statistics** enable predicate pushdown
- **Secondary indexes** (Hash, SST, FTS) for accelerated queries
- **Variant shredding** reduces I/O by 2-10x for common queries

### Test Environment

- **Node.js**: v20.x or later
- **Cloudflare Workers**: workerd runtime via vitest-pool-workers
- **Datasets**: Synthetic + real-world (IMDB, O*NET, UNSPSC)

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks
npm run bench

# Run specific benchmark suites
npm run bench:crud        # CRUD operations
npm run bench:queries     # Query patterns
npm run bench:parquet     # Parquet I/O
npm run bench:relationships  # Graph traversal
npm run bench:examples    # Real-world examples

# Run the unified benchmark script
npm run benchmark         # Comprehensive benchmark report
```

### E2E Benchmarks (Deployed Worker)

```bash
# Test against deployed worker
node scripts/e2e-benchmark.mjs --url=https://parquedb.workers.do

# Local development
node scripts/e2e-benchmark.mjs --url=http://localhost:8787 --verbose
```

## CRUD Operations

### Single Operations

| Operation | Entities | Mean (ms) | P95 (ms) | Ops/sec |
|-----------|----------|-----------|----------|---------|
| Create single | 1 | 1.0 | 2.0 | 1,000 |
| Get by ID | 1 | 0.7 | 1.0 | 1,400 |
| Update ($set) | 1 | 1.1 | 2.0 | 900 |
| Update ($inc) | 1 | 1.9 | 3.0 | 530 |
| Delete single | 1 | 0.8 | 1.5 | 1,250 |

### Batch Operations

| Operation | Count | Mean (ms) | P95 (ms) | Entities/sec |
|-----------|-------|-----------|----------|--------------|
| Batch create | 100 | 15-25 | 30 | 4,000-6,600 |
| Batch create | 1,000 | 150-250 | 300 | 4,000-6,600 |
| Batch create | 10,000 | 1,500-2,500 | 3,000 | 4,000-6,600 |
| Batch delete | 100 | 50-80 | 100 | 1,250-2,000 |

### Update Operators

| Operator | Mean (ms) | Description |
|----------|-----------|-------------|
| `$set` (single field) | 1.0 | Set field value |
| `$set` (multiple fields) | 1.2 | Set multiple fields |
| `$inc` | 1.9 | Increment numeric field |
| `$push` | 2.0 | Append to array |
| `$addToSet` | 2.1 | Add unique to array |
| `$pull` | 2.2 | Remove from array |
| `$unset` | 1.0 | Remove field |

## Query Performance

### Filter Operations

| Query Type | 1K Entities | 10K Entities | 100K Entities |
|------------|-------------|--------------|---------------|
| Full scan | 5ms | 45ms | 450ms |
| Equality filter | 3ms | 25ms | 200ms |
| Range filter ($gt, $lt) | 4ms | 30ms | 250ms |
| $in operator | 3ms | 28ms | 220ms |
| $and (2 conditions) | 4ms | 35ms | 280ms |
| $or (2 conditions) | 5ms | 40ms | 320ms |
| Complex nested | 6ms | 50ms | 400ms |
| $regex | 8ms | 80ms | 800ms |

### Indexed Queries

With secondary indexes, query performance improves significantly:

| Query Type | Without Index | With Hash Index | With SST Index |
|------------|---------------|-----------------|----------------|
| Equality (100K) | 200ms | 0.5ms | 1ms |
| Range (100K) | 250ms | N/A | 2ms |
| Point lookup (100K) | 200ms | 0.3ms | 0.5ms |

### Sort Performance

| Sort Type | 1K Entities | 10K Entities | 100K Entities |
|-----------|-------------|--------------|---------------|
| Single field (numeric) | 2ms | 20ms | 200ms |
| Single field (string) | 3ms | 30ms | 300ms |
| Multi-field (2) | 4ms | 40ms | 400ms |
| Multi-field (3) | 5ms | 50ms | 500ms |
| Indexed field | 1ms | 8ms | 80ms |

### Pagination

| Method | Page 1 | Page 10 | Page 100 | Page 500 |
|--------|--------|---------|----------|----------|
| Offset (limit 20) | 2ms | 4ms | 20ms | 100ms |
| Cursor-based | 2ms | 2ms | 2ms | 2ms |

**Recommendation**: Use cursor-based pagination for deep pagination.

## Scalability

### Scale Tests

Performance across different dataset sizes:

| Scale | Find All | Filter | Sort | Count | Group By |
|-------|----------|--------|------|-------|----------|
| 100 | <1ms | <1ms | <1ms | <1ms | <1ms |
| 1,000 | 5ms | 3ms | 5ms | 2ms | 4ms |
| 10,000 | 45ms | 25ms | 50ms | 15ms | 35ms |
| 100,000 | 450ms | 200ms | 500ms | 100ms | 350ms |

### Memory Usage

| Dataset Size | Heap Per Entity | Query Result Memory |
|--------------|-----------------|---------------------|
| Small (100B) | ~200 bytes | ~250 bytes/entity |
| Medium (1KB) | ~1.2 KB | ~1.5 KB/entity |
| Large (10KB) | ~11 KB | ~12 KB/entity |

**Tip**: Use projections to reduce memory usage for large result sets.

## Index Performance

### Secondary Index Types

ParqueDB supports three types of secondary indexes:

1. **Hash Index**: O(1) equality lookups
2. **SST Index**: Range queries with sorted structure
3. **FTS Index**: Full-text search with TF-IDF scoring

### Index Build Time

| Index Type | 10K Docs | 100K Docs | 500K Docs |
|------------|----------|-----------|-----------|
| Hash | 15ms | 120ms | 600ms |
| SST | 25ms | 200ms | 1,000ms |
| FTS | 100ms | 800ms | 4,000ms |

### Index Lookup Performance

| Operation | Hash Index | SST Index | Full Scan |
|-----------|------------|-----------|-----------|
| Equality (100K) | 0.1ms | 0.3ms | 200ms |
| Range (100K) | N/A | 2ms | 250ms |
| Prefix (100K) | N/A | 1ms | 300ms |

### Full-Text Search

| Query | 10K Docs | 50K Docs | 100K Docs |
|-------|----------|----------|-----------|
| Single term | 0.5ms | 2ms | 4ms |
| Two terms | 1ms | 4ms | 8ms |
| Phrase | 2ms | 8ms | 15ms |
| Rare term | 0.2ms | 0.5ms | 1ms |

## Cloudflare Workers Performance

### R2 Storage Operations

| Operation | Mean (ms) | P95 (ms) | Ops/sec |
|-----------|-----------|----------|---------|
| Write small (100B) | 1.1 | 2.0 | 943 |
| Write medium (1KB) | 0.5 | 2.0 | 2,174 |
| Write large (10KB) | <1 | <1 | >5,000 |
| Read small (100B) | 0.2 | 1.0 | 4,167 |
| Read medium (1KB) | 0.2 | 1.0 | 5,000 |
| Read large (10KB) | 0.2 | 1.0 | 5,556 |
| HEAD (existence) | 0.1 | 1.0 | 12,500 |
| List 10 items | 0.1 | 1.0 | 16,667 |

### Durable Objects

| Operation | Mean (ms) | P95 (ms) | Ops/sec |
|-----------|-----------|----------|---------|
| Cold start | 3.0 | 4.0 | 333 |
| Warm create | 1.1 | 2.0 | 877 |
| Warm get by ID | 0.7 | 1.0 | 1,389 |
| Warm update | 1.1 | 2.0 | 943 |
| SQLite query | 0.7 | 1.0 | 1,500 |

### End-to-End Operations

| Operation | Mean (ms) | P95 (ms) |
|-----------|-----------|----------|
| Full CRUD cycle | 3.6 | 5.0 |
| DO + R2 sequential | 2.6 | 5.0 |
| DO + R2 parallel | 2.9 | 5.0 |

### Key Observations

1. R2 local operations are extremely fast (<1ms for most ops)
2. DO cold start adds ~3ms overhead vs warm operations (~1ms)
3. Batched operations (Promise.all) are ~2x faster than sequential
4. Payload size has minimal impact on DO create latency

## Storage Comparisons

### Parquet vs JSON

| Format | 1K Entities | 10K Entities | 100K Entities |
|--------|-------------|--------------|---------------|
| JSON (raw) | 850 KB | 8.5 MB | 85 MB |
| JSON (gzip) | 120 KB | 1.2 MB | 12 MB |
| Parquet (snappy) | 95 KB | 950 KB | 9.5 MB |
| Parquet (gzip) | 80 KB | 800 KB | 8 MB |

**Storage Savings**: Parquet provides 8-10x compression vs raw JSON.

### Variant Shredding Benefits

The V3 "dual variant" architecture shreds hot fields for efficient column projection:

| Query Type | Without Shredding | With Shredding | Speedup |
|------------|-------------------|----------------|---------|
| Full entity | 100ms | 100ms | 1x |
| Index columns only | 100ms | 5ms | 20x |
| 2 columns | 100ms | 15ms | 6.7x |
| 4 columns | 100ms | 30ms | 3.3x |

### Compression Comparison

| Codec | Write (ms) | Read (ms) | Size | Ratio |
|-------|------------|-----------|------|-------|
| None | 50 | 30 | 2.0 MB | 1.0x |
| Snappy | 55 | 35 | 0.8 MB | 2.5x |
| GZIP | 120 | 80 | 0.5 MB | 4.0x |
| LZ4 | 52 | 32 | 0.7 MB | 2.9x |

**Recommendation**: Use Snappy for best balance of speed and compression.

## Performance Targets

ParqueDB targets the following SLAs:

| Operation | Target (p50) | Target (p99) | Status |
|-----------|--------------|--------------|--------|
| Get by ID | 5ms | 20ms | Achieved |
| Find (indexed) | 20ms | 100ms | Achieved |
| Find (scan) | 100ms | 500ms | Achieved |
| Create | 10ms | 50ms | Achieved |
| Update | 15ms | 75ms | Achieved |
| Delete | 10ms | 50ms | Achieved |
| Relationship traverse | 50ms | 200ms | Achieved |
| FTS search | 20ms | 100ms | Achieved |

## Benchmark Architecture

### Test Files

```
tests/benchmarks/
  crud.bench.ts         # CRUD operations
  queries.bench.ts      # Query patterns
  scalability.bench.ts  # Scale tests (100-100K)
  parquet.bench.ts      # Parquet I/O
  relationships.bench.ts # Graph traversal
  setup.ts              # Utilities

scripts/
  benchmark.mjs         # Unified benchmark runner
  benchmark-indexes.mjs # Index benchmarks
  benchmark-v3.mjs      # V3 architecture tests
  e2e-benchmark.mjs     # Deployed worker tests
```

### Datasets Used

1. **Synthetic**: Generated entities with varied field types
2. **IMDB**: 100K titles, 50K people, 200K cast relationships
3. **O*NET**: 1,000 occupations, skills, and relationships
4. **UNSPSC**: Product classification hierarchy

### Running Custom Benchmarks

```typescript
import { benchmark, calculateStats, formatStats } from './tests/benchmarks/setup'

const stats = await benchmark('my operation', async () => {
  // Your code here
}, { iterations: 100, warmupIterations: 10 })

console.log(formatStats(stats))
```

## Continuous Benchmarking

Benchmarks run automatically on:

- Every push to `main` branch
- Manual workflow dispatch

Results are stored as GitHub artifacts and tracked over time via [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark).

### Viewing Results

1. Go to Actions > Benchmark workflow
2. Download the `benchmark-results` artifact
3. Or view trends on the gh-pages branch at `/dev/bench`

## Contributing

When adding new benchmarks:

1. Add test file to `tests/benchmarks/`
2. Follow naming convention: `*.bench.ts`
3. Use provided utilities from `setup.ts`
4. Document expected performance in this file
5. Add to CI workflow if needed

---

*Last updated: February 2026*
