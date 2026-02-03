---
title: Consistency Model
description: ParqueDB's read/write consistency guarantees including CQRS architecture, strong consistency for writes via Durable Objects, eventual consistency for reads with caching, and best practices for applications.
---

**Design Document: Read/Write Consistency Guarantees**

This document explains ParqueDB's consistency model, including when reads are strongly consistent vs eventually consistent, how the CQRS architecture affects behavior, and best practices for applications.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Write Path: Strong Consistency](#write-path-strong-consistency)
3. [Read Path: Eventual Consistency with Caching](#read-path-eventual-consistency-with-caching)
4. [CQRS Architecture](#cqrs-architecture)
5. [Read-After-Write Guarantees](#read-after-write-guarantees)
6. [Stale Data Handling](#stale-data-handling)
7. [Cache Invalidation](#cache-invalidation)
8. [Consistency Levels by Operation](#consistency-levels-by-operation)
9. [Best Practices](#best-practices)
10. [Configuration Options](#configuration-options)

---

## Architecture Overview

ParqueDB uses a **CQRS (Command Query Responsibility Segregation)** architecture with physically separated read and write paths:

```
                    +-------------------------------------------------------------+
                    |                    ParqueDB Worker                           |
                    |                                                              |
   Application      |   +------------------------------------------------------+   |
        |           |   |              ParqueDBWorker (RPC)                     |   |
        |           |   |                                                       |   |
        v           |   |   +-------------+       +----------------------+      |   |
  +----------+      |   |   |   READS     |       |     WRITES           |      |   |
  |  Request |------+---+-->|  (Direct)   |       |  (via DO Stub)       |      |   |
  +----------+      |   |   |             |       |                      |      |   |
                    |   |   +------+------+       +--------+-------------+      |   |
                    |   |          |                        |                   |   |
                    |   +----------+------------------------+-------------------+   |
                    |              |                        |                       |
                    |              v                        v                       |
                    |   +-----------------+      +----------------------+           |
                    |   |   ReadPath      |      |    ParqueDBDO        |           |
                    |   |                 |      |  (Durable Object)    |           |
                    |   |  +-----------+  |      |                      |           |
                    |   |  |   Cache   |  |      |  +----------------+  |           |
                    |   |  |   API     |  |      |  |    SQLite      |  |           |
                    |   |  +-----+-----+  |      |  |  (Transient)   |  |           |
                    |   |       |         |      |  +-------+--------+  |           |
                    |   |       v         |      |          |           |           |
                    |   |  +---------+    |      |          v           |           |
                    |   |  |    R2   |<---+------+    Event Log +       |           |
                    |   |  |  Bucket |    |      |    Flush to R2       |           |
                    |   |  +---------+    |      |                      |           |
                    |   +-----------------+      +----------------------+           |
                    |                                                              |
                    +-------------------------------------------------------------+
```

### Key Components

| Component | Role | Consistency |
|-----------|------|-------------|
| **ParqueDBWorker** | RPC entrypoint, routes reads/writes | N/A |
| **ReadPath** | Cached R2 reader | Eventually consistent |
| **QueryExecutor** | Parquet query engine | Eventually consistent |
| **ParqueDBDO** | Durable Object for writes | Strongly consistent |
| **R2 Bucket** | Parquet file storage | Eventually consistent |
| **Cache API** | Edge caching layer | Eventually consistent |
| **DO SQLite** | Transient metadata store | Strongly consistent (within DO) |

---

## Write Path: Strong Consistency

All write operations (create, update, delete, link, unlink) are routed through a **Durable Object** which provides strong consistency guarantees:

### How Writes Work

1. **Worker receives write request**
2. **Worker routes to Durable Object** via RPC stub
3. **DO performs write atomically** using SQLite transactions
4. **DO appends event to event log** for CDC and time-travel
5. **DO schedules flush** to persist events to R2 as Parquet files
6. **Worker invalidates cache** for affected paths

```typescript
// Write flow in ParqueDBWorker
async create<T>(ns: string, data: Partial<T>, options: CreateOptions = {}): Promise<T> {
  // 1. Route to Durable Object
  const doId = this.env.PARQUEDB.idFromName(ns)
  const stub = this.env.PARQUEDB.get(doId)
  const result = await stub.create(ns, data, options)

  // 2. Invalidate cache after write succeeds
  await this.invalidateCacheForNamespace(ns)

  return result
}
```

### Durable Object Guarantees

The Durable Object provides:

- **Single-writer semantics**: Only one DO instance per namespace ID exists globally
- **Transactional writes**: SQLite operations are ACID-compliant
- **Ordered operations**: All writes to a namespace are serialized
- **Optimistic concurrency**: `expectedVersion` support for conflict detection

```typescript
// In ParqueDBDO - optimistic concurrency control
if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
  throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
}
```

### Write Consistency Summary

| Guarantee | Provided |
|-----------|----------|
| Atomicity | Yes (SQLite transactions) |
| Serialization | Yes (single-writer DO) |
| Durability | Yes (after R2 flush) |
| Immediate visibility to other requests | Yes (within same DO) |

---

## Read Path: Eventual Consistency with Caching

Read operations bypass the Durable Object for performance and go directly to R2 with caching:

### How Reads Work

1. **Worker receives read request** (find, get, count, exists)
2. **QueryExecutor checks in-memory cache** (parsed Parquet data)
3. **If miss, ReadPath checks Cache API** (edge cache)
4. **If miss, ReadPath fetches from R2** (object storage)
5. **Data is parsed and returned** to caller

```typescript
// Read flow - bypasses DO entirely
async find<T>(ns: string, filter: Filter, options: FindOptions<T> = {}): Promise<FindResult<T>> {
  await this.ensureInitialized()
  return this.queryExecutor.find<T>(ns, filter, options)  // Direct to R2
}
```

### Caching Layers

ParqueDB has multiple caching layers, each with different TTLs:

```typescript
// Default cache configuration
export const DEFAULT_CACHE_CONFIG: CacheConfig = {
  dataTtl: 60,      // 1 minute for Parquet data files
  metadataTtl: 300, // 5 minutes for metadata/schema
  bloomTtl: 600,    // 10 minutes for bloom filters
  staleWhileRevalidate: true,
}
```

#### Layer 1: In-Memory Data Cache (QueryExecutor)

```typescript
// Cached parsed Parquet data - fastest, but per-isolate
private dataCache = new Map<string, unknown[]>()

// Check cache before reading Parquet
const cached = this.dataCache.get(path)
if (cached) {
  stats.cacheHit = true
  // Return cached data immediately
}
```

#### Layer 2: Edge Cache (Cache API)

```typescript
// Cloudflare Cache API - shared across edge
const cached = await this.cache.match(cacheKey)
if (cached) {
  // Serve from edge cache
  return new Uint8Array(await cached.arrayBuffer())
}
```

#### Layer 3: R2 Object Storage

```typescript
// Fall back to R2 if not cached
const obj = await this.bucket.get(path)
if (!obj) throw new NotFoundError(path)

// Cache for future reads
await this.cacheResponse(cacheKey, data, obj.etag, ttl)
```

### Read Consistency Summary

| Scenario | Consistency Level |
|----------|-------------------|
| Cache hit | Eventually consistent (up to TTL stale) |
| Cache miss | Read-after-R2-write consistent |
| After explicit invalidation | Strongly consistent |

---

## CQRS Architecture

ParqueDB implements CQRS with physical separation of reads and writes:

### Command Side (Writes)

- All mutations go through Durable Objects
- Single-writer per namespace guarantees ordering
- Writes are immediately visible within the DO
- Events logged for CDC and time-travel

### Query Side (Reads)

- Reads go directly to R2 with caching
- High throughput via distributed Workers
- Eventual consistency with configurable staleness
- No coordination overhead with write path

### Why CQRS?

1. **Scalability**: Reads scale independently via edge caching
2. **Performance**: No write-path coordination for reads
3. **Cost efficiency**: DO charges per operation; reads avoid this
4. **Latency**: Edge caching provides sub-millisecond reads

### CQRS Trade-offs

| Benefit | Trade-off |
|---------|-----------|
| High read throughput | Eventual consistency for reads |
| Low read latency | Stale data possible |
| Cost efficient | Complexity of cache invalidation |
| Write ordering | Single-point-of-write bottleneck (~30 req/s per DO, see [DO Write Bottleneck](./do-write-bottleneck.md)) |

---

## Read-After-Write Guarantees

ParqueDB provides **read-after-write consistency** in specific scenarios:

### Guaranteed Read-After-Write

1. **Reads through the Durable Object**:
   ```typescript
   // In ParqueDBDO - always sees latest data
   async get(ns: string, id: string): Promise<Entity | null> {
     const rows = [...this.sql.exec<StoredEntity>(
       'SELECT * FROM entities WHERE ns = ? AND id = ?',
       ns, id
     )]
     return rows[0] ? this.toEntity(rows[0]) : null
   }
   ```

2. **After explicit cache invalidation**:
   ```typescript
   // After write, cache is invalidated
   await this.invalidateCacheForNamespace(ns)
   // Next read will fetch fresh data from R2
   ```

### Not Guaranteed

1. **Cached reads immediately after write**:
   - Writes invalidate cache, but cache may be repopulated before R2 is updated
   - Other Workers may still serve cached data

2. **Cross-region reads**:
   - R2 replication lag can cause staleness
   - Edge caches in different regions may have different data

### Achieving Stronger Consistency

For cases requiring read-after-write consistency:

```typescript
// Option 1: Skip cache on critical reads
const result = await readPath.readParquet(path, { skipCache: true })

// Option 2: Read through DO (higher latency)
const entity = await doStub.get(ns, id)  // Always fresh

// Option 3: Wait for flush + invalidation
await doStub.flushToParquet()
await worker.invalidateCache(ns)
const fresh = await worker.get(ns, id)
```

---

## Stale Data Handling

### How Stale Data Occurs

1. **Cache TTL**: Data remains cached until TTL expires
2. **R2 propagation**: Writes to R2 may take milliseconds to propagate
3. **Flush delay**: DO events are batched before flushing to Parquet
4. **Replication lag**: R2 multi-region replication is not instant

### Detecting Stale Data

ParqueDB provides metadata to detect staleness:

```typescript
// Cache stats show hit/miss information
const stats = await worker.getCacheStats()
// {
//   hits: 150,
//   misses: 23,
//   hitRatio: 0.87,
//   cachedBytes: 4521984,
//   fetchedBytes: 892416
// }

// Query stats indicate cache usage
const result = await worker.find(ns, filter)
// result.stats.cacheHit indicates if cache was used
```

### Stale-While-Revalidate

ParqueDB supports stale-while-revalidate for improved latency:

```typescript
// When enabled, serves stale data while fetching fresh
if (this.config.staleWhileRevalidate && this.shouldRevalidate(cached)) {
  // Return cached data immediately
  this.revalidateInBackground(path, cacheKey, ttl)
}
```

This means:
- Reads always return quickly (from cache)
- Background refresh keeps cache relatively fresh
- Maximum staleness = 2x TTL

---

## Cache Invalidation

### Automatic Invalidation

After every write operation, the Worker invalidates relevant caches:

```typescript
// After write operations
private async invalidateCacheForNamespace(ns: string): Promise<void> {
  // Invalidate data file
  await this.readPath.invalidate([`data/${ns}/data.parquet`])

  // Invalidate query executor cache
  this.queryExecutor.invalidateCache(ns)

  // Invalidate bloom filter
  await this.readPath.invalidate([`indexes/bloom/${ns}.bloom`])
}
```

### Manual Invalidation

Applications can trigger manual invalidation:

```typescript
// Invalidate specific namespace
await worker.invalidateCache('posts')

// Or via ReadPath directly
await readPath.invalidate(['data/posts/data.parquet'])
await readPath.invalidateNamespace('posts')
```

### Invalidation Limitations

1. **Edge cache propagation**: Cache API invalidation is not instant globally
2. **In-memory caches**: Each Worker isolate has its own cache
3. **CDN bucket**: If using CDN bucket, requires separate invalidation

---

## Consistency Levels by Operation

| Operation | Consistency | Notes |
|-----------|-------------|-------|
| `create()` | Strong | Via DO, immediately visible in DO |
| `update()` | Strong | Via DO with optimistic locking |
| `delete()` | Strong | Via DO, supports soft/hard delete |
| `link()`/`unlink()` | Strong | Via DO |
| `get()` | Eventual | Cached reads, use DO for strong |
| `find()` | Eventual | Cached reads |
| `count()` | Eventual | May count stale data |
| `exists()` | Eventual | May return false for new data |
| `related()` | Eventual | Graph traversal on cached data |

### Operations Through DO (Strong Consistency)

```typescript
// These always see latest data
const entity = await doStub.get(ns, id)
const rels = await doStub.getRelationships(ns, id, predicate)
```

### Operations Through Worker (Eventual Consistency)

```typescript
// These use caching and may be stale
const entities = await worker.find(ns, filter)
const exists = await worker.exists(ns, filter)
```

---

## Best Practices

### 1. Design for Eventual Consistency

```typescript
// Good: Tolerate eventual consistency in reads
const posts = await worker.find('posts', { status: 'published' })
// Display posts knowing they may be slightly stale

// Bad: Expecting immediate visibility after write
await worker.create('posts', { title: 'New Post' })
const count = await worker.count('posts') // May not include new post
```

### 2. Use DO Reads When Freshness is Critical

```typescript
// For critical operations, read through DO
const doId = env.PARQUEDB.idFromName('accounts')
const stub = env.PARQUEDB.get(doId)
const account = await stub.get('accounts', accountId) // Always fresh
```

### 3. Implement Optimistic Concurrency

```typescript
// Use version checking for concurrent updates
const entity = await worker.get('posts', postId)
try {
  await worker.update('posts', postId, update, {
    expectedVersion: entity.version
  })
} catch (e) {
  if (e.message.includes('Version mismatch')) {
    // Handle conflict - retry with fresh data
  }
}
```

### 4. Choose Appropriate Cache Configuration

```typescript
// For read-heavy workloads
const config = READ_HEAVY_CACHE_CONFIG // Longer TTLs

// For write-heavy workloads
const config = WRITE_HEAVY_CACHE_CONFIG // Shorter TTLs

// For development/debugging
const config = NO_CACHE_CONFIG // No caching
```

### 5. Use Explicit Invalidation When Needed

```typescript
// After bulk operations or external updates
await worker.invalidateCache('posts')

// Force fresh read
const posts = await readPath.readParquet('data/posts/data.parquet', {
  skipCache: true
})
```

### 6. Handle Stale Data Gracefully

```typescript
// Display timestamps to users
const posts = await worker.find('posts', {})
// Show "Updated X minutes ago" based on updatedAt

// Use If-Modified-Since patterns
const lastKnown = localStorage.getItem('lastSync')
const result = await worker.find('posts', {
  updatedAt: { $gt: new Date(lastKnown) }
})
```

### 7. Consider Read Replicas for High Traffic

```typescript
// Use CDN bucket for read-heavy paths
const queryExecutor = new QueryExecutor(
  readPath,
  env.BUCKET,      // Primary (writes)
  env.CDN_BUCKET,  // CDN bucket (reads)
  env.CDN_R2_DEV_URL // Edge caching
)
```

---

## Configuration Options

### Cache Configuration

```typescript
interface CacheConfig {
  /** TTL for Parquet data files (seconds) */
  dataTtl: number

  /** TTL for metadata/schema (seconds) */
  metadataTtl: number

  /** TTL for bloom filters (seconds) */
  bloomTtl: number

  /** Use stale-while-revalidate for improved latency */
  staleWhileRevalidate: boolean

  /** Maximum size to cache (bytes, 0 = no limit) */
  maxCacheSize?: number
}
```

### Preset Configurations

| Configuration | Data TTL | Metadata TTL | Bloom TTL | Use Case |
|--------------|----------|--------------|-----------|----------|
| `DEFAULT_CACHE_CONFIG` | 60s | 300s | 600s | Balanced workloads |
| `READ_HEAVY_CACHE_CONFIG` | 300s | 900s | 1800s | Read-heavy, tolerant of staleness |
| `WRITE_HEAVY_CACHE_CONFIG` | 15s | 60s | 120s | Frequent updates |
| `NO_CACHE_CONFIG` | 0 | 0 | 0 | Development/debugging |

### Environment Variables

```toml
# wrangler.toml
[vars]
CACHE_DATA_TTL = "60"
CACHE_METADATA_TTL = "300"
CACHE_BLOOM_TTL = "600"
CACHE_STALE_WHILE_REVALIDATE = "true"
```

---

## Summary

ParqueDB's consistency model prioritizes:

1. **Strong consistency for writes** via Durable Objects
2. **High-performance reads** via R2 with multi-layer caching
3. **Configurable staleness** based on workload requirements
4. **Explicit cache control** when freshness is critical

Understanding these trade-offs enables building applications that leverage ParqueDB's strengths while working within its consistency guarantees.

### Quick Reference

| Need | Solution |
|------|----------|
| Always-fresh reads | Read through DO |
| High-throughput reads | Use cached reads (eventual) |
| Read-after-write | Invalidate cache, then read |
| Conflict detection | Use `expectedVersion` |
| Reduce staleness | Lower cache TTLs |
| Maximize performance | Higher cache TTLs |
