# Cache Invalidation Strategy

This document describes ParqueDB's cache invalidation strategy, including TTL configuration, staleness guarantees, and cache bypass options.

---

## Table of Contents

1. [Overview](#overview)
2. [TTL Configuration](#ttl-configuration)
3. [Staleness SLAs](#staleness-slas)
4. [Cache Bypass Options](#cache-bypass-options)
5. [Invalidation Mechanisms](#invalidation-mechanisms)
6. [Related Documentation](#related-documentation)

---

## Overview

ParqueDB uses a multi-layer caching architecture to optimize read performance in its CQRS (Command Query Responsibility Segregation) design:

1. **In-Memory Cache** (QueryExecutor): Parsed Parquet data, per-isolate
2. **Edge Cache** (Cloudflare Cache API): Shared across edge locations
3. **R2 Object Storage**: Source of truth for persisted data

When Durable Objects write data, cached reads may become stale. ParqueDB provides TTL-based expiration and explicit invalidation to manage cache coherence.

---

## TTL Configuration

### Default TTL Values

ParqueDB uses different TTL values for different resource types based on their update frequency:

| Resource Type | Default TTL | Description |
|--------------|-------------|-------------|
| **Data** (Parquet files) | 60 seconds | Entity data files, frequently updated |
| **Metadata** (Schema) | 300 seconds (5 min) | Parquet schema and file metadata |
| **Bloom Filters** | 600 seconds (10 min) | ID lookup filters, stable after creation |
| **Indexes** | 600 seconds (10 min) | Secondary indexes, same as bloom filters |

### TTL Per Resource Type

Each resource type has its own TTL to balance freshness with performance:

```typescript
// Default cache configuration
export const DEFAULT_CACHE_CONFIG: CacheConfig = {
  dataTtl: 60,      // Parquet data files
  metadataTtl: 300, // Schema and metadata
  bloomTtl: 600,    // Bloom filter indexes
  staleWhileRevalidate: true,
}
```

**Data files** have the shortest TTL because they change most frequently with entity creates, updates, and deletes.

**Metadata and schema** files change less frequently (only on schema migrations or structural changes), so they have a longer TTL.

**Bloom filters and indexes** are the most stable - they're rebuilt during compaction but otherwise remain unchanged, allowing the longest TTL.

### How to Configure TTL

#### Option 1: Environment Variables

Configure TTL via environment variables in your `wrangler.toml`:

```toml
[vars]
CACHE_DATA_TTL = "60"
CACHE_METADATA_TTL = "300"
CACHE_BLOOM_TTL = "600"
CACHE_STALE_WHILE_REVALIDATE = "true"
```

#### Option 2: Programmatic Configuration

Use the `CacheStrategy` class with a custom configuration:

```typescript
import { CacheStrategy } from 'parquedb/worker'

// Custom configuration
const cacheStrategy = new CacheStrategy({
  dataTtl: 30,           // More aggressive for write-heavy workloads
  metadataTtl: 120,
  bloomTtl: 300,
  staleWhileRevalidate: true,
})
```

#### Option 3: Preset Configurations

ParqueDB provides preset configurations for common workloads:

```typescript
import {
  DEFAULT_CACHE_CONFIG,      // Balanced (60s/300s/600s)
  READ_HEAVY_CACHE_CONFIG,   // Longer TTLs (300s/900s/1800s)
  WRITE_HEAVY_CACHE_CONFIG,  // Shorter TTLs (15s/60s/120s)
  NO_CACHE_CONFIG,           // No caching (0/0/0)
} from 'parquedb/worker'
```

| Configuration | Data TTL | Metadata TTL | Bloom TTL | SWR | Use Case |
|--------------|----------|--------------|-----------|-----|----------|
| `DEFAULT_CACHE_CONFIG` | 60s | 300s | 600s | Yes | Balanced workloads |
| `READ_HEAVY_CACHE_CONFIG` | 300s | 900s | 1800s | Yes | Read-heavy, tolerant of staleness |
| `WRITE_HEAVY_CACHE_CONFIG` | 15s | 60s | 120s | No | Frequent updates, fresher reads |
| `NO_CACHE_CONFIG` | 0 | 0 | 0 | No | Development/debugging |

> **Note**: SWR = stale-while-revalidate. When disabled (as in `WRITE_HEAVY_CACHE_CONFIG`), cache misses will block until fresh data is fetched. This provides stricter freshness at the cost of higher latency on cache misses.

---

## Staleness SLAs

### Staleness Bounds

ParqueDB provides the following staleness guarantees:

| Scenario | Maximum Staleness |
|----------|-------------------|
| Default configuration | 2 minutes (60s TTL + 60s stale-while-revalidate) |
| Read-heavy configuration | 10 minutes (300s + 300s) |
| Write-heavy configuration | 15 seconds (stale-while-revalidate disabled) |
| After explicit invalidation | 0 (strongly consistent) |

### Maximum Staleness Guarantee

With `staleWhileRevalidate` enabled (default), the maximum staleness is **2x the TTL**:

```
Maximum Staleness = TTL + stale-while-revalidate window
```

For the default data TTL of 60 seconds:
- **Normal case**: Data is fresh (0-60 seconds old)
- **Stale-while-revalidate**: Data may be 60-120 seconds old while background refresh occurs
- **After 120 seconds**: Cache miss forces fresh fetch

### Read-After-Write Consistency Expectations

ParqueDB does **NOT** guarantee immediate read-after-write consistency for cached reads. After a write:

1. The Durable Object (DO) has the latest data immediately
2. R2 is updated during the flush cycle
3. Edge caches may serve stale data until TTL expires or explicit invalidation

To achieve read-after-write consistency:

```typescript
// Option 1: Read through the Durable Object (always fresh)
const entity = await doStub.get(ns, id)

// Option 2: Skip the cache (forces R2 fetch)
const data = await readPath.readParquet(path, { skipCache: true })

// Option 3: Explicitly invalidate before reading
await readPath.invalidateNamespace(ns)
const entities = await queryExecutor.find(ns, filter)
```

### Why Pub/Sub Was Not Chosen

ParqueDB uses TTL-based cache invalidation instead of a pub/sub notification system for several reasons:

1. **Cost**: Pub/sub systems (like Cloudflare Queues or external message brokers) add per-message costs that scale with write volume. TTL-based invalidation has zero marginal cost.

2. **Complexity**: Pub/sub requires additional infrastructure, subscription management, and handling of message delivery failures. TTL-based caching is simpler and more predictable.

3. **Edge Distribution**: Cloudflare's Cache API operates per-colo (data center). Global cache invalidation via pub/sub would require broadcasting to all edge locations, adding latency and complexity.

4. **Acceptable Staleness**: Most ParqueDB workloads tolerate eventual consistency. The TTL-based approach provides bounded staleness without the overhead of real-time synchronization.

5. **Version-Based Alternative**: ParqueDB supports version-based cache keys as an alternative to pub/sub. When a namespace version increments, old cache entries become unreachable without explicit invalidation.

For applications requiring stronger consistency, use the cache bypass options documented below.

---

## Cache Bypass Options

### How to Bypass Cache

#### Option 1: `skipCache` Parameter

The `ReadPath` class accepts a `skipCache` option to force an R2 fetch:

```typescript
// Bypass cache for a single read
const data = await readPath.readParquet('data/posts/data.parquet', {
  skipCache: true
})
```

#### Option 2: Read Through Durable Object

For critical reads, route through the Durable Object which always has the latest data:

```typescript
// Get DO stub
const doId = env.PARQUEDB.idFromName(ns)
const stub = env.PARQUEDB.get(doId)

// Read directly from DO (bypasses all caches)
const entity = await stub.get(ns, entityId)
```

#### Option 3: Explicit Cache Invalidation

Invalidate the cache before reading to ensure fresh data:

```typescript
// Invalidate specific paths
await readPath.invalidate(['data/posts/data.parquet'])

// Or invalidate entire namespace
await readPath.invalidateNamespace('posts')

// Subsequent reads will fetch from R2
const entities = await queryExecutor.find('posts', filter)
```

#### Option 4: Custom TTL Override

Override the TTL for specific reads:

```typescript
// Use shorter TTL for this read
const data = await readPath.readParquet(path, {
  ttl: 5  // 5 second TTL
})
```

### When to Use Cache Bypass

Use cache bypass in these scenarios:

| Scenario | Recommended Approach |
|----------|---------------------|
| **Critical financial data** | Read through DO or `skipCache: true` |
| **Immediately after writes** | Explicit invalidation + read |
| **User-facing "refresh" action** | `skipCache: true` |
| **Real-time collaboration** | Read through DO |
| **Debugging cache issues** | `NO_CACHE_CONFIG` or `skipCache: true` |
| **Bulk data migration** | Invalidate namespace after completion |

**Caution**: Overusing cache bypass negates the performance benefits of caching. Reserve it for use cases that truly require fresh data.

---

## Invalidation Mechanisms

### Automatic Invalidation After Writes

ParqueDB automatically invalidates caches after write operations:

```typescript
// In ParqueDBDO after writes
await invalidateAfterWrite(cache, ns, operation)

// This invalidates:
// - data/{ns}/data.parquet (entity data)
// - indexes/bloom/{ns}.bloom (bloom filter)
// - rels/forward/{ns}.parquet (forward relationships)
// - rels/reverse/{ns}.parquet (reverse relationships)
```

### Version-Based Cache Keys

For scenarios requiring more precise invalidation, use version-based cache keys:

```typescript
const invalidator = new CacheInvalidator(cache)

// Get versioned cache key
const cacheKey = invalidator.getVersionedCacheKey(path, ns)
// Returns: "data/posts/data.parquet?v=42"

// After write, bump version
invalidator.bumpVersion(ns)
// Old cache entries with ?v=42 are now stale
// New reads use ?v=43
```

### QueryExecutor Cache Clearing

The QueryExecutor maintains an in-memory cache that needs separate invalidation:

```typescript
// Clear specific namespace
queryExecutor.invalidateCache('posts')

// Clear all cached data
queryExecutor.clearCache()
```

---

## Edge Cases and Limitations

### Per-Colo Cache Invalidation

Cloudflare's Cache API operates **per-colo** (data center). This means:

- Cache invalidation only affects the current colo
- Other edge locations continue serving cached data until TTL expiration
- For global invalidation, consider version-based cache keys or longer TTLs with explicit bypass

**Recommendation**: For globally-consistent invalidation, use version-based cache keys. When the namespace version increments, all colos naturally fetch fresh data with the new cache key.

### In-Memory Cache Isolation

Each Worker isolate has its own in-memory cache (QueryExecutor):

- Invalidating one isolate's cache doesn't affect others
- Heavy traffic may spawn multiple isolates serving stale data
- Consider using shorter TTLs for data requiring fresher reads

### Relationship Invalidation

When relationships change (link/unlink operations), both forward and reverse indexes are invalidated:

```typescript
// Relationship change invalidates:
// - rels/forward/{fromNs}.parquet
// - rels/reverse/{toNs}.parquet
```

Entity data files are NOT invalidated on relationship-only changes, reducing unnecessary cache churn.

### Entity-Level vs Namespace-Level Invalidation

Currently, entity-level invalidation triggers full namespace invalidation:

```typescript
// This invalidates the entire namespace, not just the entity
await invalidator.invalidateEntity('posts', 'post-123')
```

This is because ParqueDB stores entities in namespace-level Parquet files. Future optimizations may support row-group-level invalidation for more surgical cache updates.

---

## Related Documentation

- [CONSISTENCY.md](./CONSISTENCY.md) - Read/write consistency guarantees and CQRS architecture
- [STORAGE_UNIFICATION.md](./STORAGE_UNIFICATION.md) - Storage backend architecture
- [do-write-bottleneck.md](./do-write-bottleneck.md) - Durable Object write scaling
- [namespace-sharded-architecture.md](./namespace-sharded-architecture.md) - Multi-tenant sharding

### Source Code References

- `src/worker/CacheInvalidation.ts` - Cache invalidation implementation
- `src/worker/CacheStrategy.ts` - Cache configuration and TTL management
- `src/worker/ReadPath.ts` - Cached R2 reader with `skipCache` support
- `src/constants.ts` - TTL constant definitions
