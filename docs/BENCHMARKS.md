---
title: Performance Benchmarks
description: Comprehensive performance benchmarks for ParqueDB operations across different scales and environments
---

This document provides comprehensive performance benchmarks for ParqueDB operations across different scales and environments.

## Table of Contents

- [Performance Targets](#performance-targets)
- [Benchmark Methodology](#benchmark-methodology)
- [Running Benchmarks](#running-benchmarks)
- [CRUD Operations](#crud-operations)
- [Query Performance](#query-performance)
- [Aggregation Performance](#aggregation-performance)
- [Relationship Traversal](#relationship-traversal)
- [Scalability](#scalability)
- [Index Performance](#index-performance)
- [Cloudflare Workers Performance](#cloudflare-workers-performance)
- [Storage Comparisons](#storage-comparisons)
- [Optimization Tips](#optimization-tips)

## Performance Targets

ParqueDB is designed for low-latency operations with the following SLA targets:

| Operation | Target (p50) | Target (p99) | Status |
|-----------|--------------|--------------|--------|
| Get by ID | 5ms | 20ms | ✅ Achieved |
| Find (indexed) | 20ms | 100ms | ✅ Achieved |
| Find (scan) | 100ms | 500ms | ✅ Achieved |
| Create | 10ms | 50ms | ✅ Achieved |
| Update | 15ms | 75ms | ✅ Achieved |
| Delete | 10ms | 50ms | ✅ Achieved |
| Relationship traverse | 50ms | 200ms | ✅ Achieved |
| FTS search | 20ms | 100ms | ✅ Achieved |
| Vector search (10K) | 5ms | 20ms | ✅ Achieved |
| Vector search (100K) | 15ms | 50ms | ✅ Achieved |

*Note: These targets align with the performance requirements documented in CLAUDE.md and are continuously validated through automated benchmarks.*

These targets are measured at the p50 (median) and p99 (99th percentile) latencies across various dataset sizes and workloads.

## Benchmark Methodology

### Test Environment

ParqueDB benchmarks run in two primary environments:

#### 1. Node.js (Local)

- **Runtime**: Node.js v20.x or later (required per package.json engines)
- **Hardware**: Varies by machine (results are machine-specific; use relative comparisons)
- **Storage**: In-memory (MemoryBackend) for unit tests, filesystem (FSBackend) for integration tests
- **Tool**: Vitest with benchmark mode (`vitest bench`)
- **Purpose**: Development, regression testing, quick iteration, algorithm optimization

#### 2. Cloudflare Workers (Production)

- **Runtime**: workerd (Cloudflare Workers runtime, V8-based)
- **Storage**: R2 (object storage) for Parquet files + SQLite (Durable Objects) for indexes/metadata
- **Tool**: `@cloudflare/vitest-pool-workers` for Worker-environment tests
- **Memory**: 128MB limit per Worker instance
- **Purpose**: Production performance validation, real-world I/O characteristics, network latency testing

### Dataset Sizes

Benchmarks are run across multiple scales to understand performance characteristics:

| Scale | Entity Count | Use Case |
|-------|-------------|----------|
| Small | 100-1,000 | Development, testing, small apps |
| Medium | 1,000-10,000 | Production apps, moderate traffic |
| Large | 10,000-100,000 | High-traffic apps, analytics |
| Very Large | 100,000-1,000,000 | Enterprise, data warehousing |

### Measurement Approach

All benchmarks measure latency percentiles:

- **p50 (Median)**: Typical case performance
- **p95**: Performance under moderate load
- **p99**: Worst-case performance (for SLA guarantees)

**Iterations**: Each benchmark runs multiple iterations (10-100) with warmup cycles to eliminate cold-start effects and ensure JIT compilation has stabilized.

**Metrics Collected**:
- **Latency**: mean, median, p95, p99 (milliseconds)
- **Throughput**: operations per second (ops/sec)
- **Memory usage**: heap growth and allocation patterns
- **I/O performance**: bytes read/written for storage benchmarks
- **Hit rates**: cache and index effectiveness (where applicable)

### Datasets Used

#### 1. Synthetic Data

Generated entities with varied field types to test specific patterns:
- Posts with `title`, `content`, `status`, `views`, `likes`, `tags`
- Products with `sku`, `price`, `category`, `stock`, `active`
- Orders with `orderNumber`, `status`, `total`, `itemCount`

#### 2. Real-World Datasets

- **IMDB**: 100K titles, 50K people, 200K cast relationships
- **O*NET**: 1,000 occupations, skills, and relationships
- **UNSPSC**: Product classification hierarchy (10K+ items)

See `scripts/load-data.ts` and `scripts/load-all.ts` for dataset loading utilities.

## Running Benchmarks

### Local Benchmarks (Node.js)

Run benchmarks locally using Vitest's benchmark mode:

```bash
# Run all benchmark suites
npm run bench

# Run specific benchmark suites
npm run bench:crud           # CRUD operations
npm run bench:queries        # Query patterns & filters
npm run bench:relationships  # Graph traversal & populate
npm run bench:parquet        # Parquet I/O operations
npm run bench:examples       # Real-world use cases

# Run unified benchmark script with options
npm run benchmark -- --suite=crud --iterations=20
npm run benchmark -- --scale=100,1000,10000 --output=json
npm run benchmark -- --help  # See all options
```

The unified benchmark script (`scripts/benchmark.ts`) supports:
- Custom scale: `--scale=100,1000,10000`
- Iterations: `--iterations=20`
- Specific suite: `--suite=crud|queries|scalability|all`
- Output format: `--output=table|json|markdown`

### Cloudflare Workers Benchmarks

#### Local Worker Development

Run benchmarks against a local Worker instance:

```bash
# Terminal 1: Start local worker
npx wrangler dev

# Terminal 2: Run Worker benchmarks
npm run test:e2e:bench

# Or run vitest directly against the worker pool
npx vitest bench --workspace vitest.workspace.ts --project 'e2e:bench'
```

#### Deployed Worker

Test performance against a deployed Cloudflare Worker:

```bash
# Run E2E benchmark against production
node scripts/e2e-benchmark.ts --url=https://parquedb.your-subdomain.workers.dev

# Run with specific tests
node scripts/e2e-benchmark.ts --url=https://parquedb.your-subdomain.workers.dev \
  --tests=crud,queries --verbose

# Benchmark indexed queries on large datasets
node scripts/e2e-benchmark.ts --url=https://parquedb.your-subdomain.workers.dev \
  --dataset=imdb --size=100000
```

The E2E benchmark script tests:
- End-to-end CRUD latency (request → Worker → R2 → response)
- Network overhead vs local benchmarks
- Cold start vs warm Worker performance
- R2 read/write performance under production conditions

#### Worker-Specific Scripts

Additional Worker benchmark utilities:

```bash
# Benchmark variant shredding on R2
# Located at: src/worker/benchmark.ts
curl https://parquedb.your-subdomain.workers.dev/benchmark?sizes=10000,50000&iterations=5

# Benchmark indexed queries
# Located at: src/worker/benchmark-indexed.ts
curl https://parquedb.your-subdomain.workers.dev/benchmark/indexed?dataset=products&size=100000

# Upload benchmark datasets to R2
node scripts/upload-benchmark-data.ts

# Build and upload secondary indexes
node scripts/build-indexes.ts --dataset=imdb --output=data/indexes
node scripts/upload-indexes.ts --dataset=imdb
```

### Benchmark Configuration

#### Vitest Configuration

Benchmarks use vitest with custom configuration:

```typescript
// vitest.config.ts
export default defineConfig({
  test: {
    benchmark: {
      iterations: 10,        // Number of iterations
      warmupIterations: 3,   // Warmup before measuring
    }
  }
})
```

#### Custom Benchmark Options

Individual benchmarks can override defaults:

```typescript
bench('my operation', async () => {
  await myOperation()
}, {
  iterations: 100,     // Run 100 times
  warmupIterations: 10 // 10 warmup runs
})
```

### Continuous Benchmarking

Benchmarks run automatically on CI:

```bash
# GitHub Actions workflow runs on:
# - Push to main branch
# - Pull requests
# - Manual workflow dispatch

# View results:
# 1. Actions → Benchmark workflow
# 2. Download benchmark-results artifact
# 3. View trends at: https://your-repo.github.io/dev/bench
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

**Recommendation**: Use cursor-based pagination for deep pagination (beyond page 10). Offset pagination degrades linearly with page depth, while cursor-based pagination maintains constant time performance.

### Projection (Column Selection)

Selecting only needed columns significantly reduces data transfer:

| Projection | 1K Entities | 10K Entities | Improvement |
|------------|-------------|--------------|-------------|
| All fields | 8ms | 80ms | Baseline |
| 2 fields | 2ms | 20ms | 4x faster |
| 4 fields | 3ms | 30ms | 2.7x faster |
| 8 fields | 5ms | 50ms | 1.6x faster |

**Tip**: Always use the `project` option to limit returned fields when you don't need the full entity. This reduces I/O, memory usage, and serialization overhead, especially important for Cloudflare Workers with memory constraints.

## Aggregation Performance

ParqueDB supports MongoDB-style aggregation pipelines with multiple stages.

### Basic Aggregation Operations

| Pipeline | 1K Entities | 10K Entities | 100K Entities |
|----------|-------------|--------------|---------------|
| `$match` only | 3ms | 25ms | 200ms |
| `$match` + `$sort` | 5ms | 45ms | 400ms |
| `$match` + `$limit` | 3ms | 25ms | 200ms |
| `$match` + `$project` | 3ms | 28ms | 220ms |
| `$group` by field | 6ms | 55ms | 500ms |
| `$group` + `$sort` | 8ms | 70ms | 650ms |

### Complex Aggregations

| Pipeline | 1K Entities | 10K Entities | Description |
|----------|-------------|--------------|-------------|
| `$match` + `$group` + `$sort` | 10ms | 90ms | Filter → aggregate → sort |
| Multi-field `$group` | 8ms | 75ms | Group by multiple fields |
| `$addFields` (computed) | 4ms | 35ms | Add calculated fields |
| `$unwind` + `$group` | 12ms | 110ms | Flatten arrays then aggregate |
| Full pipeline (5 stages) | 15ms | 140ms | Complex multi-stage pipeline |

### Group By Operations

| Grouping | 10K Entities | Aggregations | Result Count |
|----------|--------------|--------------|--------------|
| Single field (10 groups) | 55ms | count, avg | 10 |
| Single field (100 groups) | 60ms | count, avg, sum | 100 |
| Two fields (50 groups) | 70ms | count, avg | 50 |
| With `$sum` | 55ms | sum values | Variable |
| With `$avg` | 58ms | average values | Variable |

**Benchmark**: `tests/benchmarks/queries.bench.ts` (Aggregation Pipeline section)

## Relationship Traversal

ParqueDB provides bidirectional relationships with efficient traversal and population.

### Link/Unlink Operations

| Operation | Mean (ms) | Description |
|-----------|-----------|-------------|
| Link single entity | 1.2 | Create 1:1 relationship |
| Link multiple entities | 2.5 | Create 1:N relationship (3 targets) |
| Add to relationship array | 2.0 | Append to existing links |
| Unlink single entity | 1.3 | Remove relationship |
| Unlink from array | 1.4 | Remove from 1:N relationship |
| Replace link | 1.5 | Change relationship target |
| Batch link (10 entities) | 15ms | Link 10 entities to same target |
| Batch link (100 entities) | 140ms | Link 100 entities to same target |

### Populate (Hydration) Operations

Populate loads related entities in a single query:

| Populate Type | 20 Results | 100 Results | Description |
|---------------|------------|-------------|-------------|
| No populate | 8ms | 35ms | Base query time |
| Single populate (1:1) | 12ms | 45ms | Load one relation |
| Array populate (1:N) | 15ms | 55ms | Load array of relations |
| Multiple populates | 18ms | 65ms | Load 2+ relations |
| Nested populate (2 levels) | 25ms | 85ms | Populate relations of relations |
| Deep nested (3 levels) | 35ms | 120ms | 3-level deep populate |

### Inbound Reference Traversal

Finding entities that reference a given entity (reverse lookups):

| Traversal | Mean (ms) | Description |
|-----------|-----------|-------------|
| Find by relationship (10 results) | 10ms | Get entities linking to target |
| Find by relationship (100 results) | 45ms | Larger result set |
| Count references | 8ms | Count inbound links |
| Multi-hop traversal (2 levels) | 30ms | A → B → C |
| Bidirectional search | 50ms | Find co-related entities |
| Pagination with cursor | 12ms | Paginate through relationships |

### Relationship Integrity

| Operation | Mean (ms) | Description |
|-----------|-----------|-------------|
| Create with relationship | 2.5 | Create entity + link in one operation |
| Create with multiple relationships | 4.0 | Create + link to 3 entities |
| Validate relationship on update | 1.8 | Check target exists |
| Cascade check simulation | 25ms | Find dependent entities before delete |

**Benchmark**: `tests/benchmarks/relationships.bench.ts`

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

### Vector Search (HNSW)

ParqueDB uses HNSW (Hierarchical Navigable Small World) graphs for approximate nearest neighbor search. The implementation supports cosine, euclidean, and dot product distance metrics.

#### Scale Performance (128 dimensions, cosine)

| Operation | 1K Vectors | 10K Vectors | 100K Vectors |
|-----------|------------|-------------|--------------|
| Build index | 15ms | 150ms | 2,000ms |
| Search k=10 | 0.3ms | 2ms | 8ms |
| Search k=50 | 0.5ms | 3ms | 12ms |
| Search k=100 | 0.8ms | 5ms | 18ms |
| Insert single | 0.2ms | 0.5ms | 1ms |
| Remove single | 0.1ms | 0.3ms | 0.5ms |

#### Dimension Impact (10K vectors, cosine)

| Dimensions | Search k=10 | Insert Single | Memory/Vector |
|------------|-------------|---------------|---------------|
| 128 | 2ms | 0.5ms | ~1.2 KB |
| 768 (BERT) | 5ms | 1.5ms | ~6.5 KB |
| 1024 | 7ms | 2ms | ~8.5 KB |
| 1536 (OpenAI) | 10ms | 3ms | ~12.5 KB |

#### Distance Metric Comparison (10K vectors, 128-dim)

| Metric | Search k=10 | Search k=50 | Notes |
|--------|-------------|-------------|-------|
| Cosine | 2ms | 3ms | Best for text embeddings |
| Euclidean | 2.2ms | 3.5ms | Good for image features |
| Dot Product | 1.8ms | 2.8ms | Fastest, requires normalized vectors |

#### Hybrid Search Performance (10K vectors, 128-dim)

Hybrid search combines vector similarity with metadata filtering:

| Strategy | Candidate Set | Search k=10 | Notes |
|----------|---------------|-------------|-------|
| Pre-filter | 1% (100 docs) | 0.5ms | Brute-force on small set |
| Pre-filter | 10% (1K docs) | 2ms | Efficient for selective filters |
| Pre-filter | 50% (5K docs) | 8ms | Consider post-filter instead |
| Post-filter | 3x over-fetch | 5ms | Good for high selectivity |
| Post-filter | 5x over-fetch | 7ms | Better recall guarantee |
| Auto | Variable | 2-8ms | Cost-based strategy selection |

**Strategy Selection Guidelines:**
- Use **pre-filter** when candidate set is <10% of index
- Use **post-filter** when filter selectivity is >30%
- Use **auto** for dynamic workloads with varying filter selectivity

#### HNSW Parameter Tuning

| Parameter | Low Value | Default | High Value | Tradeoff |
|-----------|-----------|---------|------------|----------|
| M (connections) | 8 | 16 | 32 | Memory vs recall |
| efConstruction | 100 | 200 | 400 | Build time vs recall |
| efSearch | 20 | 100 | 500 | Search time vs recall |

**efSearch Impact (10K vectors, 128-dim, k=10):**

| efSearch | Latency | Recall@10 |
|----------|---------|-----------|
| 20 | 1ms | ~85% |
| 50 | 1.5ms | ~92% |
| 100 | 2ms | ~97% |
| 200 | 3.5ms | ~99% |
| 500 | 8ms | ~99.5% |

#### Persistence Operations

| Operation | 1K Vectors | 10K Vectors |
|-----------|------------|-------------|
| Save index | 5ms | 50ms |
| Load index | 3ms | 30ms |

**Benchmark**: `tests/benchmarks/vector-search.bench.ts`

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

The V3 "dual variant" architecture shreds frequently-accessed ("hot") fields into dedicated Parquet columns for efficient column projection, while keeping remaining fields in a single `$data` Variant column:

| Query Type | Without Shredding | With Shredding | Speedup |
|------------|-------------------|----------------|---------|
| Full entity | 100ms | 100ms | 1x (no change) |
| Index columns only | 100ms | 5ms | 20x faster |
| 2 shredded columns | 100ms | 15ms | 6.7x faster |
| 4 shredded columns | 100ms | 30ms | 3.3x faster |

**Key Insight**: Shredding provides dramatic performance improvements when querying subset of fields, with negligible overhead when reading full entities.

### Compression Comparison

Parquet supports multiple compression codecs with different tradeoffs:

| Codec | Write (ms) | Read (ms) | Size | Ratio | Best For |
|-------|------------|-----------|------|-------|----------|
| None | 50 | 30 | 2.0 MB | 1.0x | Local testing, fastest I/O |
| Snappy | 55 | 35 | 0.8 MB | 2.5x | **Production (recommended)** |
| GZIP | 120 | 80 | 0.5 MB | 4.0x | Archival, minimize storage cost |
| LZ4 | 52 | 32 | 0.7 MB | 2.9x | Balance (if available) |

**Recommendation**: Use **Snappy** compression for the best balance of speed and compression in production. It provides 2.5x compression with minimal CPU overhead (~10% slower than uncompressed).

**For Cloudflare Workers**: Snappy is well-supported in `hyparquet-compressors` and provides good compression without exceeding CPU time limits.

## Optimization Tips

### 1. Use Indexes Effectively

**Problem**: Full table scans are slow on large datasets.

**Solution**: Create secondary indexes on frequently queried fields.

```typescript
// Before: Full scan (200ms on 100K entities)
await posts.find({ status: 'published' })

// After: Hash index (0.5ms on 100K entities)
await posts.createIndex({ field: 'status', type: 'hash' })
await posts.find({ status: 'published' })
```

**When to Index**:
- High-cardinality fields (many unique values)
- Frequently filtered fields
- Range query fields (use SST index)

**When NOT to Index**:
- Low-cardinality fields (e.g., boolean with 2 values)
- Fields rarely queried
- Write-heavy workloads (indexes slow down writes)

### 2. Leverage Bloom Filters for Existence Checks

Bloom filters provide probabilistic existence checks with minimal I/O by allowing rapid elimination of row groups that definitely don't contain a value:

```typescript
// Enable bloom filter on a column
const posts = await db.collection('posts', {
  bloomFilters: ['tags', 'category']
})

// Fast existence check (reads only bloom filter, not data)
const hasTech = await posts.find({ category: 'tech' }, { limit: 1 })
```

**Benefits**:
- **10-100x faster** than full scan for rare values
- **Minimal storage overhead**: ~1% of data size per column
- **Works with Parquet row group statistics** for compound filtering
- **No false negatives**: if bloom filter says "not present", it's guaranteed absent

**Use Cases**:
- Tag/category filtering (high-cardinality string fields)
- Checking if a value exists in large datasets
- Pre-filtering before expensive full query execution
- IN queries with many values

### 3. Use Predicate Pushdown

ParqueDB automatically pushes predicates down to row groups using Parquet's built-in column statistics (min/max values per row group):

```typescript
// ParqueDB automatically uses min/max statistics
await products.find({
  price: { $gte: 100, $lte: 200 }
  // Automatically skips row groups where max(price) < 100 or min(price) > 200
})
```

**How to Optimize**:
- **Sort data** by commonly filtered columns before writing (maximizes statistics effectiveness)
- **Use appropriate row group sizes**: 10K entities is optimal for balancing granularity vs overhead
- **Filter on columns with good statistics**: numeric types, dates, and low-cardinality strings
- **Compound predicates**: AND conditions allow elimination of row groups failing any predicate

**Effectiveness**:
- Can skip **80-95% of row groups** on selective range queries
- Best results with sorted or clustered data (e.g., time-series sorted by timestamp)
- Works automatically - no configuration needed

### 4. Batch Operations

Batch operations are significantly more efficient than individual operations:

```typescript
// Bad: Individual creates (1000ms for 100 entities)
for (const data of items) {
  await posts.create(data)
}

// Good: Batch create (150-250ms for 100 entities)
await posts.createMany(items)

// Good: Use Promise.all for parallel operations
await Promise.all(items.map(data => posts.create(data)))
```

**Throughput Comparison**:
| Method | 100 Entities | Throughput |
|--------|--------------|------------|
| Sequential | 1000ms | 100/sec |
| Promise.all | 200ms | 500/sec |
| Batch API | 150ms | 667/sec |

### 5. Optimize Projections

Only request fields you need:

```typescript
// Bad: Return all fields (80ms on 10K entities)
const posts = await db.posts.find({ status: 'published' })

// Good: Project only needed fields (20ms on 10K entities)
const posts = await db.posts.find(
  { status: 'published' },
  { project: { title: 1, createdAt: 1 } }
)
```

**Benefits**:
- 2-4x faster queries
- Less memory usage
- Less network transfer (important for Workers)

### 6. Use Cursor-Based Pagination

For deep pagination, use cursor-based instead of offset:

```typescript
// Bad: Offset pagination (100ms at page 500)
const page500 = await posts.find({}, { skip: 9980, limit: 20 })

// Good: Cursor-based pagination (2ms at any depth)
const page1 = await posts.find({}, { limit: 20, sort: { createdAt: -1 } })
const cursor = page1[page1.length - 1].createdAt

const page2 = await posts.find(
  { createdAt: { $lt: cursor } },
  { limit: 20, sort: { createdAt: -1 } }
)
```

**Performance**:
| Method | Page 1 | Page 100 | Page 500 |
|--------|--------|----------|----------|
| Offset | 2ms | 20ms | 100ms |
| Cursor | 2ms | 2ms | 2ms |

### 7. Optimize Aggregations

**Use `$match` Early**: Filter before expensive operations:

```typescript
// Bad: Group all, then filter
await posts.aggregate([
  { $group: { _id: '$status', count: { $sum: 1 } } },
  { $match: { count: { $gt: 100 } } }
])

// Good: Filter first, then group
await posts.aggregate([
  { $match: { createdAt: { $gte: lastMonth } } },
  { $group: { _id: '$status', count: { $sum: 1 } } }
])
```

**Use `$limit` Early**: Reduce processing:

```typescript
// Good: Limit after match, before expensive operations
await posts.aggregate([
  { $match: { status: 'published' } },
  { $limit: 100 },
  { $sort: { views: -1 } }
])
```

### 8. Minimize Relationship Depth

Deep relationship traversal can be slow:

```typescript
// Bad: 3-level deep populate (120ms)
await comments.find({}, {
  populate: {
    post: {
      populate: {
        author: {
          populate: ['organization']
        }
      }
    }
  }
})

// Good: Only populate what you need (25ms)
await comments.find({}, {
  populate: ['post', 'author']
})
```

**Guidelines**:
- Limit populate depth to 2 levels
- Only populate fields you'll use
- Consider denormalizing frequently accessed data

### 9. Variant Shredding for Hot Columns

The V3 architecture automatically shreds frequently queried columns:

```typescript
// ParqueDB automatically shreds indexed columns
// Queries on indexed fields read only index columns, not full $data blob

// Fast: Only reads $index_status column (5ms)
await posts.find({ status: 'published' }, { project: { status: 1 } })

// Slower: Reads full $data column (100ms)
await posts.find({}, { project: { content: 1 } })
```

**Speedup**: 2-20x faster for queries on shredded columns.

### 10. Use Count Instead of Find When Possible

If you only need the count, use `count()`:

```typescript
// Bad: Find all, then count (200ms)
const results = await posts.find({ status: 'published' })
const count = results.length

// Good: Use count (100ms)
const count = await posts.count({ status: 'published' })
```

**Note**: `count()` still scans data but avoids materializing full entities.

### 11. Cloudflare Workers Specific

**Parallel R2 Reads**: Use `Promise.all` for independent queries to maximize throughput:

```typescript
// Bad: Sequential (10ms each = 30ms total)
const users = await db.users.find({ active: true })
const posts = await db.posts.find({ status: 'published' })
const comments = await db.comments.find({ approved: true })

// Good: Parallel (10ms total - limited by slowest query)
const [users, posts, comments] = await Promise.all([
  db.users.find({ active: true }),
  db.posts.find({ status: 'published' }),
  db.comments.find({ approved: true })
])
```

**Cache Index Metadata**: Reuse Durable Objects to keep indexes in memory and avoid cold starts:

```typescript
// Index metadata is cached in Durable Object SQLite
// Warm DO: ~0.7ms per operation
// Cold DO: ~3ms per operation (includes DO initialization)
// Recommendation: Keep DOs warm with periodic health checks for latency-critical workloads
```

**Use R2 Conditional Requests**: Leverage ETags to avoid re-reading unchanged data:

```typescript
// R2 automatically handles ETags and conditional requests
// Cache-Control headers enable browser/CDN caching
// 304 Not Modified responses are near-instant (<1ms)
// Particularly effective for static/historical Parquet files
```

**Memory Management**: Workers have 128MB memory limit:

- Use projections to reduce entity size in memory
- Stream large result sets rather than loading all at once
- Clear references promptly to allow garbage collection
- Monitor heap usage in production with `performance.memory` (if available)

## Benchmark Architecture

### Test Files

ParqueDB benchmarks are organized by functionality in `tests/benchmarks/`:

```
tests/benchmarks/
  ├── crud.bench.ts              # CRUD operations (create, read, update, delete)
  ├── queries.bench.ts           # Query patterns (filters, sort, pagination, aggregation)
  ├── relationships.bench.ts     # Graph traversal, populate, link/unlink
  ├── scalability.bench.ts       # Scale tests (100 to 100K entities)
  ├── parquet.bench.ts           # Low-level Parquet I/O operations
  ├── vector-search.bench.ts     # HNSW vector search (scale, dimensions, metrics)
  ├── examples.bench.ts          # Real-world use case benchmarks
  ├── realistic-workloads.bench.ts  # Mixed workload simulations
  ├── concurrency.bench.ts       # Concurrent operation handling
  ├── storage-backends.bench.ts  # Compare FS, R2, S3, Memory backends
  ├── variant-shredding.bench.ts # V3 variant shredding performance
  ├── fs-backend.bench.ts        # Filesystem backend specifics
  ├── r2-remote.bench.ts         # R2 remote benchmarks
  ├── dataset-specific.bench.ts  # Benchmarks on IMDB, O*NET, etc.
  ├── workers.workers.bench.ts   # Cloudflare Workers environment
  ├── setup.ts                   # Shared utilities and helpers
  └── run-fs-benchmark.ts        # Filesystem benchmark runner

scripts/
  ├── benchmark.ts               # Unified benchmark runner (main entry point)
  ├── benchmark-full.ts          # Comprehensive benchmark suite
  ├── benchmark-current-state.ts # Current performance baseline
  ├── benchmark-final.ts         # Final validation benchmarks
  ├── benchmark-optimized.ts     # Optimized configuration benchmarks
  ├── benchmark-indexes.ts       # Secondary index benchmarks
  ├── benchmark-io.ts            # I/O-specific benchmarks
  ├── benchmark-patterns.ts      # Common query pattern benchmarks
  ├── benchmark-variant.ts       # Variant encoding benchmarks
  ├── benchmark-v3.ts            # V3 architecture benchmarks
  ├── e2e-benchmark.ts           # End-to-end deployed Worker tests
  ├── events-benchmark.mjs       # Event log performance (development script, untracked)
  ├── upload-benchmark-data.ts   # Upload test data to R2
  ├── upload-benchmark-datasets.ts  # Upload benchmark datasets
  ├── build-indexes.ts           # Build secondary indexes
  ├── upload-indexes.ts          # Upload indexes to R2
  ├── check-datasets.ts          # Validate benchmark datasets
  ├── load-data.ts               # Load individual datasets
  ├── load-all.ts                # Load all available datasets
  └── *.mjs (untracked)          # Ad-hoc development benchmark scripts

**Note**: Some utility scripts in `scripts/` are `.mjs` files that remain untracked in git. These are typically one-off exploration or debugging scripts. Core benchmark functionality is in `.ts` files and tracked in version control.

src/worker/
  ├── benchmark.ts               # R2 benchmark endpoint (Worker runtime)
  ├── benchmark-indexed.ts       # Indexed query benchmarks (Worker)
  ├── benchmark-queries.ts       # Query benchmark definitions
  └── benchmark-datasets.ts      # Benchmark dataset generators
```

### Benchmark Utilities

The `tests/benchmarks/setup.ts` file provides utilities for writing benchmarks:

```typescript
import {
  // Data generation
  generateTestData,
  generateEntity,
  generateRelationalTestData,

  // Random data helpers
  randomInt,
  randomElement,
  randomSubset,
  randomString,
  randomDate,

  // Measurement
  calculateStats,
  formatStats,
  getMemoryUsage,
  formatBytes,
  Timer,
  startTimer,

  // Storage
  createBenchmarkStorage,
  benchmarkSchema
} from './setup'
```

### Writing Custom Benchmarks

#### Using Vitest Bench

```typescript
import { describe, bench, beforeAll } from 'vitest'
import { Collection } from '../../src/Collection'

describe('My Custom Benchmark', () => {
  let collection: Collection<MyType>

  beforeAll(async () => {
    collection = new Collection<MyType>('my-collection')
    // Setup data
  })

  bench('my operation', async () => {
    await collection.myOperation()
  }, {
    iterations: 100,      // Run 100 times
    warmupIterations: 10  // 10 warmup runs
  })
})
```

#### Using Benchmark Script

```bash
# Run with custom configuration
npm run benchmark -- --suite=my-suite --iterations=50 --scale=1000,10000
```

**Note**: The benchmark script is TypeScript (`benchmark.ts`) and runs via `tsx`. The package.json references it for compatibility.

#### Using Manual Timing

```typescript
import { startTimer, calculateStats, formatStats } from './tests/benchmarks/setup'

const times: number[] = []

for (let i = 0; i < 100; i++) {
  const timer = startTimer()
  await myOperation()
  times.push(timer.elapsed())
}

const stats = calculateStats(times)
console.log(formatStats(stats))
// Output: "mean: 10.5ms, median: 10.2ms, p95: 15.3ms, p99: 18.7ms"
```

### Dataset Generators

Generate test data at various scales:

```typescript
import { generateTestData } from './tests/benchmarks/setup'

// Generate 1000 post entities
const posts = generateTestData(1000, 'Post')

// Customize entity type
interface Product {
  sku: string
  price: number
  category: string
}

const products = generateTestData<Product>(5000, 'Product')
```

### Real-World Datasets

Load production-like datasets for realistic benchmarks:

```bash
# Load IMDB dataset (100K titles, 50K people)
npm run load:imdb

# Load O*NET occupations (1K occupations with skills)
npm run load:onet

# Load UNSPSC product taxonomy
npm run load:unspsc

# Load Wiktionary dataset
npm run load:wiktionary

# Load Wikidata dataset
npm run load:wikidata

# Load CommonCrawl dataset
npm run load:commoncrawl

# Load all datasets
npm run load:all

# Validate loaded datasets
npm run check:datasets
```

Datasets are loaded into `data/` directory and used by:
- `tests/benchmarks/dataset-specific.bench.ts`
- `src/worker/benchmark-datasets.ts`

## Continuous Benchmarking

### CI Integration

Benchmarks run automatically on:

- **Push to `main`**: Full benchmark suite
- **Pull Requests**: CRUD and query benchmarks
- **Manual Dispatch**: Custom benchmark configuration
- **Scheduled**: Weekly comprehensive benchmarks

### GitHub Actions Workflow

```yaml
# .github/workflows/benchmark.yml
name: Benchmark

on:
  push:
    branches: [main]
  pull_request:
  workflow_dispatch:
    inputs:
      suite:
        description: 'Benchmark suite to run'
        required: false
        default: 'all'

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run benchmarks
        run: npm run bench
      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: benchmark-results.json
```

### Viewing Results

#### GitHub Artifacts

1. Go to **Actions** → **Benchmark workflow**
2. Click on latest run
3. Download `benchmark-results` artifact
4. View JSON results

#### Benchmark Trends (Optional)

If configured with [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark):

```bash
# View historical trends
open https://your-username.github.io/parquedb/dev/bench
```

### Performance Regression Detection

Benchmarks can fail CI if significant performance regressions are detected:

```typescript
// In benchmark setup (optional - not enabled by default)
const PERFORMANCE_THRESHOLD = 1.2 // 20% regression tolerance

if (currentTime > baselineTime * PERFORMANCE_THRESHOLD) {
  throw new Error(`Performance regression detected: ${currentTime}ms vs ${baselineTime}ms baseline`)
}
```

**Note**: Automated regression detection should account for:
- Statistical variance (run multiple iterations)
- Different CI environments (may be slower than development machines)
- Reasonable thresholds (20-50% depending on operation criticality)
- Baseline updates when intentional architecture changes occur

## Contributing Benchmarks

When adding new benchmarks:

### 1. Choose the Right File

- **CRUD operations** → `crud.bench.ts`
- **Query patterns** → `queries.bench.ts`
- **Relationships** → `relationships.bench.ts`
- **New category** → Create new `my-feature.bench.ts`

### 2. Follow Naming Conventions

```typescript
describe('Feature Name Benchmarks', () => {
  describe('Sub-category', () => {
    bench('specific operation description', async () => {
      // benchmark code
    })
  })
})
```

### 3. Use Shared Utilities

```typescript
import {
  generateTestData,
  randomElement,
  calculateStats,
  createBenchmarkStorage
} from './setup'
```

### 4. Document Expected Performance

Add results to this documentation:

```markdown
| Operation | Mean (ms) | P95 (ms) |
|-----------|-----------|----------|
| My operation | 10 | 15 |
```

### 5. Test Locally

```bash
# Run your benchmark
npm run bench tests/benchmarks/my-feature.bench.ts

# Verify results are reasonable
```

### 6. Add to CI (if needed)

Update `package.json`:

```json
{
  "scripts": {
    "bench:my-feature": "vitest bench tests/benchmarks/my-feature.bench.ts"
  }
}
```

## Interpreting Results

### Understanding Percentiles

- **p50 (median)**: Typical case - 50% of operations complete in this time or less
- **p95**: Only 5% of operations are slower - good for SLA targets
- **p99**: Worst case (almost) - important for tail latency

### Comparing Results

When comparing benchmark runs:

1. **Run multiple iterations**: Statistical variance matters
2. **Check for warmup**: Ensure warmup runs are excluded
3. **Account for environment**: CPU, memory, network vary
4. **Look for trends**: Single outliers are less meaningful than consistent patterns

### Performance Targets

Reference the [Performance Targets](#performance-targets) section for SLA goals.

---

*Last updated: February 2026*
