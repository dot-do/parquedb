# Workers Memory Optimization for Large Queries

## Overview

Cloudflare Workers have constrained resources:
- **Memory**: 128MB limit
- **CPU time**: 30 seconds (Standard), 50ms (Free tier)
- **Subrequest limit**: 1000 subrequests per invocation

Large `find()` queries that return 10K+ entities or perform complex aggregations can hit these limits. This document covers:
1. Current optimization strategies already implemented
2. Recommended patterns for query design
3. Future improvements under consideration

## Current Optimizations

### 1. Predicate Pushdown with Row Group Statistics

ParqueDB leverages Parquet's row group statistics (min/max values) to skip entire row groups that cannot contain matching data.

```typescript
// Example: Find users with age >= 30
const results = await db.collection('users').find({ age: { $gte: 30 } })

// If a row group has max(age) = 25, it's skipped entirely
// Stats show: { rowGroupsScanned: 3, rowGroupsSkipped: 7 }
```

**Memory impact**: Instead of loading all 10 row groups, only 3 are loaded.

### 2. Column Projection Pushdown

Only columns needed for the query are loaded from Parquet files:

```typescript
// Only loads $id, name, and email columns
const results = await db.collection('users').find(
  { status: 'active' },
  { project: { name: 1, email: 1 } }
)
```

**Memory impact**: For wide entities (50+ fields), this can reduce memory by 90%+.

### 3. Bloom Filters for ID Lookups

Fast O(1) negative lookups before scanning Parquet:

```typescript
// Bloom filter check happens first
const user = await db.collection('users').get('user/123')
// If bloom filter says "definitely not present", returns null immediately
```

**Memory impact**: Bloom filters are typically 128KB, far smaller than the data files.

### 4. In-Memory Caching with Size Limits

The `QueryExecutor` caches parsed Parquet data in memory, bounded by `MAX_CACHE_SIZE` (2MB):

```typescript
// From src/constants.ts
export const MAX_CACHE_SIZE = 2 * 1024 * 1024  // 2MB
```

Subsequent queries to the same collection reuse cached data without additional I/O.

### 5. Concurrent Row Group Processing

Row groups are processed in parallel with bounded concurrency:

```typescript
// From src/constants.ts
export const DEFAULT_CONCURRENCY = 4

// In QueryExecutor: processes 4 row groups simultaneously
```

**Memory impact**: Limits peak memory usage while maintaining performance.

### 6. Vector Index Memory Budgets

Vector indexes are constrained to prevent OOM:

```typescript
// From src/constants.ts
export const DEFAULT_VECTOR_INDEX_MAX_BYTES = 64 * 1024 * 1024  // 64MB
export const DEFAULT_VECTOR_INDEX_MAX_NODES = 100000  // ~10MB for 128-dim
```

### 7. Query Limits

Hard limits prevent malicious or accidental unbounded queries:

```typescript
// From src/constants.ts
export const MAX_QUERY_LIMIT = 1000     // Max results per query
export const MAX_QUERY_OFFSET = 100000  // Max skip value
```

## Recommended Query Patterns

### Use Cursor-Based Pagination

Instead of loading all results, paginate through data:

```typescript
// BAD: Loads all matching entities
const allUsers = await db.Users.find({ status: 'active' })

// GOOD: Paginate with cursors
let cursor: string | undefined
do {
  const page = await db.Users.find(
    { status: 'active' },
    { limit: 100, cursor }
  )
  processPage(page.items)
  cursor = page.nextCursor
} while (cursor)
```

### Project Only Needed Fields

Reduce memory footprint by selecting only required fields:

```typescript
// BAD: Loads entire entity including large description/content fields
const posts = await db.Posts.find({ published: true })

// GOOD: Only load what's needed for the list view
const posts = await db.Posts.find(
  { published: true },
  { project: { title: 1, author: 1, createdAt: 1 } }
)
```

### Use Shredded Columns for Filtering

For frequently-filtered fields, use column shredding (`$index_*` columns) which enables predicate pushdown:

```typescript
// Shredded column enables row group skipping
const posts = await db.Posts.find({
  $index_status: 'published',
  $index_category: 'tech'
})
```

See [Variant Shredding Guide](../guides/variant-shredding.md) for setup.

### Batch Large Operations

Split large operations into batches:

```typescript
// BAD: Process all at once
const allDocs = await db.collection('docs').find({})
for (const doc of allDocs) { /* process */ }

// GOOD: Process in batches
let cursor: string | undefined
let processed = 0
do {
  const batch = await db.collection('docs').find({}, { limit: 100, cursor })
  await Promise.all(batch.items.map(doc => process(doc)))
  processed += batch.items.length
  cursor = batch.nextCursor
} while (cursor)
```

### Use Secondary Indexes for High-Selectivity Queries

Full-text search and hash indexes narrow the candidate set before loading data:

```typescript
// FTS index reduces candidates to relevant documents first
const results = await db.Posts.find({
  $text: { $search: 'machine learning' }
}, { limit: 10 })
```

See [Secondary Indexes](./secondary-indexes.md) for index types and creation.

## Architecture: Read/Write Separation

ParqueDB's dual-path architecture helps manage memory:

```
+------------------------------------------------------------------+
|                     Worker Request                                |
+------------------------------------------------------------------+
         |                                    |
         v                                    v
   +-------------+                    +----------------+
   |   READS     |                    |    WRITES      |
   |  (Stateless)|                    | (Durable Obj)  |
   +-------------+                    +----------------+
         |                                    |
         v                                    v
   QueryExecutor                        ParqueDBDO
         |                                    |
         v                                    v
   R2 Parquet Files                     SQLite Buffer
   (predicate pushdown)                 (bounded size)
```

**Reads** go directly to R2 with predicate pushdown - no DO memory overhead.
**Writes** buffer in SQLite within the DO, with bounded event buffers.

## Memory Budget Planning

For a typical Worker (128MB limit):

| Component | Budget | Notes |
|-----------|--------|-------|
| V8 overhead | ~10MB | Runtime baseline |
| Code + dependencies | ~20MB | ParqueDB + hyparquet |
| Query executor cache | 2MB | `MAX_CACHE_SIZE` |
| Index cache | 50MB | `DEFAULT_INDEX_CACHE_MAX_BYTES` |
| Vector index | 64MB | `DEFAULT_VECTOR_INDEX_MAX_BYTES` |
| **Headroom** | ~32MB | For query processing |

Adjust constants in `src/constants.ts` for your workload.

## Future Improvements

### 1. Streaming Parquet Parsing

**Status**: Under consideration

Current approach loads entire row groups into memory. Future optimization:

```typescript
// Future API (not yet implemented)
const stream = await db.Posts.findStream({ status: 'active' })
for await (const entity of stream) {
  // Process one entity at a time
  // Memory stays constant regardless of result size
}
```

Implementation would use `hyparquet`'s row-by-row iteration rather than bulk reads.

### 2. Storage-Level Cursor Pagination

**Status**: Under consideration

Currently, cursors work at the post-processing stage. Storage-level cursors would:
- Seek directly to cursor position in Parquet files
- Skip loading entities before the cursor
- Enable efficient deep pagination (skip 1M+ rows)

### 3. Aggregation Offloading

**Status**: Planned

For complex aggregations that exceed Worker limits:

1. **D1 Offloading**: Push aggregations to D1 (Cloudflare's SQL database)
2. **Queued Processing**: Use Queues + Durable Objects for multi-pass aggregations
3. **Scheduled Workers**: Run heavy analytics in scheduled (30s CPU) workers

### 4. Adaptive Row Group Sizing

**Status**: Under consideration

Dynamically adjust row group size based on entity size:
- Small entities (< 1KB): Larger row groups (100K rows)
- Large entities (> 100KB): Smaller row groups (1K rows)

### 5. Memory-Aware Query Planning

**Status**: Future

Query planner that estimates memory usage before execution:

```typescript
// Future API (not yet implemented)
const plan = await db.Posts.find({ status: 'active' }).explain()
// plan.estimatedMemoryBytes: 45_000_000
// plan.recommendation: 'Add pagination with limit: 500'
```

## Monitoring Memory Usage

### In Development

Use `wrangler tail` to monitor memory:

```bash
wrangler tail --format=pretty
```

### In Production

Add custom metrics:

```typescript
// Track memory in handlers
const memBefore = performance.memory?.usedJSHeapSize ?? 0
const result = await db.Posts.find({ status: 'active' }, { limit: 100 })
const memAfter = performance.memory?.usedJSHeapSize ?? 0

console.log({
  memoryDelta: memAfter - memBefore,
  rowsReturned: result.items.length,
  rowGroupsScanned: result.stats?.rowGroupsScanned,
})
```

### Query Statistics

Use the `stats` object returned by `find()`:

```typescript
const result = await db.Posts.find({ status: 'active' })
console.log(result.stats)
// {
//   rowGroupsScanned: 5,
//   rowGroupsSkipped: 15,
//   rowsScanned: 50000,
//   rowsReturned: 1000,
//   executionTimeMs: 45,
//   cacheHit: false
// }
```

## Related Documentation

- [Entity Storage Architecture](./entity-storage.md) - Dual storage model
- [Secondary Indexes](./secondary-indexes.md) - Index types for query optimization
- [Variant Shredding Guide](../guides/variant-shredding.md) - Column shredding setup
- [Constants Reference](../../src/constants.ts) - Tunable limits

## Configuration Reference

Key constants for memory tuning (in `src/constants.ts`):

| Constant | Default | Description |
|----------|---------|-------------|
| `MAX_QUERY_LIMIT` | 1000 | Maximum results per query |
| `MAX_QUERY_OFFSET` | 100000 | Maximum skip value |
| `DEFAULT_CONCURRENCY` | 4 | Parallel row group processing |
| `MAX_CACHE_SIZE` | 2MB | Query executor cache limit |
| `DEFAULT_INDEX_CACHE_MAX_BYTES` | 50MB | Secondary index cache |
| `DEFAULT_VECTOR_INDEX_MAX_BYTES` | 64MB | Vector index memory budget |
| `DEFAULT_PAGE_SIZE` | 100 | Default pagination size |
| `MAX_BATCH_SIZE` | 1000 | Maximum bulk operation size |
