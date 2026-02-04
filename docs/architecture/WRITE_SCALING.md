---
title: Write Scaling
description: Understanding and mitigating the Durable Object write bottleneck in ParqueDB deployments on Cloudflare Workers.
---

# Write Scaling

This document explains the Durable Object write bottleneck in ParqueDB and provides guidance on when and how to scale writes beyond the single-DO limit.

---

## Table of Contents

1. [Why the Bottleneck Exists](#why-the-bottleneck-exists)
2. [Current Throughput Limits](#current-throughput-limits)
3. [Mitigation Strategies](#mitigation-strategies)
4. [When This Matters](#when-this-matters)
5. [Recommended Patterns](#recommended-patterns)
6. [Related Documentation](#related-documentation)

---

## Why the Bottleneck Exists

ParqueDB uses Cloudflare Durable Objects (DOs) with a **single-writer pattern** for write operations. This architectural choice provides strong consistency guarantees but creates an inherent throughput ceiling.

### The Single-Writer Pattern

```
┌─────────────────────────────────────────────────────────┐
│                     Durable Object                      │
│                                                         │
│   Request 1 ──┐                                         │
│   Request 2 ──┼──► [Queue] ──► [Process Serially] ──►  │
│   Request 3 ──┤         ↓            ↓                 │
│   Request 4 ──┘    One at a time   Results             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Key characteristics:**

1. **Serial execution**: All requests to a DO are processed one at a time
2. **Global ordering**: Writes are globally ordered within a namespace
3. **No concurrent modifications**: Eliminates race conditions and conflicts
4. **Automatic queuing**: Cloudflare queues requests when the DO is busy

### Why ParqueDB Uses This Pattern

| Benefit | Description |
|---------|-------------|
| **Strong consistency** | All reads within the DO see the latest writes |
| **No conflicts** | No need for conflict resolution or merge strategies |
| **Ordered event log** | CDC events are strictly ordered for time-travel |
| **Relationship integrity** | Atomic multi-entity updates for graph operations |
| **Simple programming model** | No distributed locks or eventual consistency handling |

---

## Current Throughput Limits

### Baseline Performance

| Operation Type | Throughput | Notes |
|---------------|------------|-------|
| Simple reads | 50-100 req/s | SQLite lookups |
| Simple writes | 30-40 req/s | Single entity CRUD |
| Complex writes | 10-20 req/s | Multi-entity, relationships |
| **Practical sustained** | **~30 req/s** | Mixed workload |

### What Determines the Limit

Each write operation involves:

```
┌─────────────────────────────────────────────────────┐
│ Single Write Operation (~25-40ms total)             │
├─────────────────────────────────────────────────────┤
│ Network overhead         │  5-10ms                  │
│ SQLite transaction       │ 10-20ms                  │
│ Event log append         │  5-10ms                  │
│ R2 flush (async)         │ Non-blocking             │
└─────────────────────────────────────────────────────┘

Maximum throughput = 1000ms / 30ms = ~33 ops/sec
```

### Behavior Under Load

| Load Level | Behavior |
|------------|----------|
| < 30 req/s | Stable, low latency (p99 < 100ms) |
| 30-50 req/s | Queue builds, increasing latency (p99 100-500ms) |
| > 50 req/s | Queue grows unbounded, timeouts occur |

---

## Mitigation Strategies

### 1. Write Batching

Combine multiple writes into a single DO request to amortize overhead.

```typescript
// Instead of individual writes
for (const item of items) {
  await db.Orders.create(item)  // 30 separate requests
}

// Batch writes in a single request
await db.Orders.createMany(items)  // 1 request, processed atomically
```

**Benefits:**
- Reduces network round-trips
- Single SQLite transaction for all writes
- Single event log entry for the batch

**When to use:**
- Bulk imports
- Batch processing jobs
- Multi-item form submissions

### 2. Queue-Based Writes

Decouple write acceptance from processing using Cloudflare Queues.

```typescript
// Worker accepts writes immediately
export default {
  async fetch(request, env) {
    const data = await request.json()

    // Queue the write (returns immediately)
    await env.WRITE_QUEUE.send({
      operation: 'create',
      namespace: 'orders',
      data
    })

    return new Response('Accepted', { status: 202 })
  }
}

// Queue consumer processes at sustainable rate
export class WriteConsumer {
  async queue(batch, env) {
    const stub = env.PARQUEDB.get(env.PARQUEDB.idFromName('default'))

    // Process batch (respects DO throughput)
    for (const message of batch.messages) {
      await stub.create(message.body.namespace, message.body.data)
      message.ack()
    }
  }
}
```

**Benefits:**
- Absorbs traffic spikes
- Guarantees delivery
- Natural backpressure

**When to use:**
- High burst traffic
- Non-latency-sensitive writes
- Event ingestion

### 3. Sharding by Namespace

Route different entity types to separate DOs.

```
┌─────────────────────────────────────────────────────┐
│                    Worker                            │
│                                                      │
│   /users/*  ──────► DO(users)   ──► 30 req/s        │
│   /orders/* ──────► DO(orders)  ──► 30 req/s        │
│   /events/* ──────► DO(events)  ──► 30 req/s        │
│                                                      │
│   Total capacity: 90 req/s                          │
└─────────────────────────────────────────────────────┘
```

```typescript
// Router selects DO based on entity type
function getDO(env: Env, namespace: string): DurableObjectStub {
  const doId = env.PARQUEDB.idFromName(namespace)
  return env.PARQUEDB.get(doId)
}

// Each namespace gets its own DO
const usersDO = getDO(env, 'users')
const ordersDO = getDO(env, 'orders')
```

**Benefits:**
- Linear scaling with number of types
- Natural data isolation
- Simple routing logic

**When to use:**
- Multiple distinct entity types
- Types have independent access patterns
- Cross-type transactions are rare

### 4. Time-Bucket Partitioning

Route writes to time-bucketed DOs for append-only data.

```
Time: 14:00-14:59 ──► DO(events:h14) ──► 30 req/s
Time: 15:00-15:59 ──► DO(events:h15) ──► 30 req/s
Time: 16:00-16:59 ──► DO(events:h16) ──► 30 req/s
```

```typescript
function getTimeBucketDO(env: Env, namespace: string): DurableObjectStub {
  const hour = Math.floor(Date.now() / (60 * 60 * 1000))
  const bucketKey = `${namespace}:h:${hour}`
  return env.PARQUEDB.get(env.PARQUEDB.idFromName(bucketKey))
}
```

**Benefits:**
- Old buckets become read-only (no contention)
- Natural data lifecycle management
- Time-range queries map to specific buckets

**When to use:**
- Time-series data (events, logs, metrics)
- Append-only workloads
- Data has natural time-based access patterns

### 5. Hash-Based Sharding

Distribute writes uniformly across multiple DOs using consistent hashing.

```typescript
function getShardDO(env: Env, entityId: string, shardCount = 4): DurableObjectStub {
  const hash = fnv1a(entityId)
  const shard = hash % shardCount
  return env.PARQUEDB.get(env.PARQUEDB.idFromName(`shard:${shard}`))
}
```

**Benefits:**
- Maximum write throughput (shards x 30 req/s)
- Uniform load distribution
- No hot spots

**Trade-offs:**
- Queries must fan out to all shards
- Cross-shard transactions require saga pattern
- Resharding is complex

**When to use:**
- Maximum throughput requirements
- Point queries (by ID) are primary access pattern
- Can accept fan-out for range queries

---

## When This Matters

### Low-Write Scenarios (No Action Needed)

Most applications do not need write scaling:

| Scenario | Estimated Writes | Within Limit? |
|----------|-----------------|---------------|
| Blog/CMS | 1-10 writes/min | Yes |
| SaaS dashboard | 10-100 writes/min | Yes |
| E-commerce store | 100-500 writes/hour | Yes |
| Internal tools | < 1000 writes/hour | Yes |

**Rule of thumb:** If your sustained write rate is under 20 req/s, single-DO is sufficient.

### High-Write Scenarios (Consider Mitigation)

| Scenario | Estimated Writes | Strategy |
|----------|-----------------|----------|
| Event tracking | 50-200 writes/s | Queue + Time-bucket |
| Real-time analytics | 100+ writes/s | Queue + Hash sharding |
| IoT data ingestion | 1000+ writes/s | Queue + Hash sharding |
| High-traffic e-commerce | 50-100 orders/s | Queue + Namespace sharding |

### Warning Signs

Monitor these metrics to detect when scaling is needed:

| Metric | Warning | Critical |
|--------|---------|----------|
| p99 write latency | > 500ms | > 2000ms |
| Write requests/sec | > 20/s sustained | > 40/s sustained |
| Error rate | > 1% | > 5% |
| Queue depth (if visible) | > 10 | > 50 |

---

## Recommended Patterns

### Pattern 1: Start Simple, Scale When Needed

```typescript
// Start with single DO per namespace
const db = new ParqueDB({ namespace: 'default' })

// Monitor metrics, add sharding when needed
```

### Pattern 2: Queue for Spiky Workloads

```typescript
// wrangler.toml
[[queues.producers]]
queue = "writes"
binding = "WRITE_QUEUE"

[[queues.consumers]]
queue = "writes"
max_batch_size = 10
max_batch_timeout = 1
```

### Pattern 3: Namespace Isolation for Multi-Tenant

```typescript
// Each tenant gets own namespace/DO
function getTenantDB(env: Env, tenantId: string) {
  const doId = env.PARQUEDB.idFromName(`tenant:${tenantId}`)
  return env.PARQUEDB.get(doId)
}
```

### Pattern 4: Read from R2, Write to DO

```typescript
// Writes go through DO for consistency
await db.Posts.create({ title: 'Hello' })

// Reads can bypass DO and query R2 directly
const posts = await queryR2Parquet(env.BUCKET, 'data/posts/data.parquet', filter)
```

This pattern provides:
- Write consistency through DO
- Unlimited read scalability through R2
- Eventual consistency for reads (configurable cache TTL)

---

## Related Documentation

- [Durable Object Write Bottleneck](./do-write-bottleneck.md) - Detailed technical deep-dive
- [Consistency Model](./CONSISTENCY.md) - Read/write consistency guarantees
- [Entity Storage Architecture](./entity-storage.md) - DO + R2 dual storage
- [Namespace-Sharded Architecture](./namespace-sharded-architecture.md) - Data file organization
- [Configuration Guide](../deployment/configuration.md) - Environment setup

---

## Summary

| Strategy | Complexity | Throughput Gain | Best For |
|----------|-----------|-----------------|----------|
| Write batching | Low | 2-10x per batch | Bulk operations |
| Queue-based | Medium | Absorbs spikes | Spiky traffic |
| Namespace sharding | Low | N x 30 req/s | Multiple entity types |
| Time-bucket | Medium | Unlimited (over time) | Time-series data |
| Hash sharding | High | N x 30 req/s | Maximum throughput |

**Key takeaways:**

1. The ~30 req/s limit is a deliberate trade-off for consistency
2. Most applications do not need write scaling
3. Start simple, add complexity only when metrics indicate need
4. Choose strategy based on data access patterns
5. Queues are the most versatile solution for traffic spikes

---

*Architecture Design Document - ParqueDB Write Scaling Guide*
