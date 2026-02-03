---
title: Durable Object Write Bottleneck
description: Understanding the single-writer bottleneck in Cloudflare Durable Objects, why ParqueDB uses this architecture for consistency, and mitigation strategies through sharding.
---

# Durable Object Write Bottleneck

This document explains the inherent write throughput limitations of Cloudflare Durable Objects, why ParqueDB chooses this architecture despite the bottleneck, and strategies for scaling writes beyond the single-DO limit.

---

## Table of Contents

1. [The Bottleneck Explained](#the-bottleneck-explained)
2. [Why Single-Writer Architecture](#why-single-writer-architecture)
3. [Mitigation Strategies](#mitigation-strategies)
   - [Type-Based Sharding](#type-based-sharding)
   - [Time-Based Sharding](#time-based-sharding)
   - [Hash-Based Sharding](#hash-based-sharding)
4. [Implementation Guide](#implementation-guide)
5. [Trade-offs Comparison](#trade-offs-comparison)
6. [When to Shard](#when-to-shard)
7. [Related Documentation](#related-documentation)

---

## The Bottleneck Explained

### The ~30 req/sec Limit

Cloudflare Durable Objects process requests **serially** within a single instance. While requests are queued and processed efficiently, the single-threaded execution model creates a practical throughput ceiling:

```
Theoretical maximum: ~50-100 requests/second (simple operations)
Practical sustained: ~20-40 requests/second (typical CRUD operations)
Complex operations:  ~10-20 requests/second (with SQLite transactions)
```

For ParqueDB with SQLite-backed entity storage, the realistic sustained write throughput is approximately **30 requests per second per Durable Object**.

### Why This Limit Exists

```
           Request Queue                    Durable Object
┌─────────────────────────────────────────┐
│                                         │
│  Request 1 ─────┐                       │
│  Request 2 ─────┼───► [Queue] ─────────►│  Processing  │────► Response 1
│  Request 3 ─────┤                       │  (serial)    │────► Response 2
│  Request 4 ─────┘                       │              │────► Response 3
│  ...                                    │              │────► ...
│                                         │
└─────────────────────────────────────────┘

Average operation time: 20-50ms
Maximum throughput: 1000ms / 30ms = ~33 ops/sec
```

**Key factors affecting throughput:**

| Factor | Impact | Typical Duration |
|--------|--------|------------------|
| Network latency | Request/response overhead | 5-10ms |
| SQLite operation | Read/write transaction | 10-30ms |
| Event logging | CDC event append | 5-10ms |
| R2 flush (async) | Background, doesn't block | N/A |

### What Happens Under Load

When write requests exceed the DO's capacity:

1. **Queue buildup**: Requests queue at the DO
2. **Increased latency**: p99 latency grows with queue depth
3. **Timeout risk**: Requests may timeout if queue grows too large
4. **Backpressure**: Eventually clients see errors

```
Load < 30 req/s:   ✓ Stable, low latency
Load = 30-50 req/s: ⚠ Increasing latency, manageable
Load > 50 req/s:   ✗ Queue grows unbounded, timeouts
```

---

## Why Single-Writer Architecture

Despite the throughput limitation, ParqueDB uses a single-writer Durable Object architecture for critical consistency guarantees:

### 1. Strong Consistency Within Namespace

All writes to a namespace are serialized, eliminating:
- **Race conditions**: No concurrent modifications to same entity
- **Lost updates**: No write-write conflicts
- **Dirty reads**: Reads within DO always see latest state

```typescript
// Without single-writer: Race condition
// Thread 1: read(balance: 100) → write(balance: 90)
// Thread 2: read(balance: 100) → write(balance: 80)
// Result: balance = 80 (Thread 1's write is lost)

// With single-writer: Serialized
// Request 1: read(balance: 100) → write(balance: 90)
// Request 2: read(balance: 90) → write(balance: 70)
// Result: balance = 70 (correct)
```

### 2. Ordered Event Log

The CDC event log maintains strict ordering:
- **Time-travel**: Point-in-time reconstruction is deterministic
- **Replication**: Events can be replayed in order
- **Audit trail**: Causal ordering preserved

### 3. Relationship Integrity

Graph operations require atomic multi-entity updates:
- Creating bidirectional edges
- Cascade deletes
- Referential integrity checks

### 4. Simpler Application Logic

Applications don't need to handle:
- Distributed locks
- Conflict resolution
- Eventual consistency edge cases

### Trade-off Summary

| Benefit | Cost |
|---------|------|
| Strong consistency | Limited write throughput |
| Simple programming model | Single point of scaling |
| Ordered events | Geographic latency (DO location) |
| No conflicts | Cannot horizontally scale single namespace |

---

## Mitigation Strategies

When write throughput exceeds the single-DO limit, ParqueDB supports several sharding strategies to distribute writes across multiple Durable Objects.

### Type-Based Sharding

Distribute writes by entity type, routing each type to a dedicated DO.

#### How It Works

```
                            ┌─────────────────────────────────┐
                            │         ParqueDBWorker          │
                            │                                 │
  POST /users/create ──────►│  ┌─────────────────────────┐   │
                            │  │   Type-Based Router     │   │
  POST /orders/create ─────►│  │                         │   │
                            │  │  users  → DO(ns:users)  │   │
  POST /products/create ───►│  │  orders → DO(ns:orders) │   │
                            │  │  products → DO(ns:prod) │   │
                            │  └───────────┬─────────────┘   │
                            │              │                  │
                            └──────────────┼──────────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
           ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
           │ DO(ns:users) │      │ DO(ns:orders)│      │ DO(ns:prod)  │
           │ ~30 writes/s │      │ ~30 writes/s │      │ ~30 writes/s │
           └──────────────┘      └──────────────┘      └──────────────┘

Total write capacity: 3 types × 30 req/s = ~90 req/s
```

#### Implementation

```typescript
// Configuration
interface TypeShardConfig {
  strategy: 'type'
  // Each entity type gets its own namespace (and DO)
  typeToNamespace: Record<string, string>
}

// Example configuration
const shardConfig: TypeShardConfig = {
  strategy: 'type',
  typeToNamespace: {
    User: 'users',
    Order: 'orders',
    Product: 'products',
    OrderItem: 'orders',  // Co-locate related types
    Review: 'products',
  }
}

// Router implementation
class TypeShardedRouter {
  private config: TypeShardConfig

  getDOForWrite(entityType: string): DurableObjectStub {
    const ns = this.config.typeToNamespace[entityType] || 'default'
    const doId = this.env.PARQUEDB.idFromName(ns)
    return this.env.PARQUEDB.get(doId)
  }

  async create<T>(type: string, data: CreateInput<T>): Promise<Entity<T>> {
    const stub = this.getDOForWrite(type)
    return stub.create(this.config.typeToNamespace[type], data)
  }
}
```

#### Storage Layout

```
bucket/
├── data/
│   ├── users/              # User entities only
│   │   └── data.parquet
│   ├── orders/             # Order + OrderItem entities
│   │   └── data.parquet
│   └── products/           # Product + Review entities
│       └── data.parquet
└── events/
    ├── users/
    │   └── current.parquet
    ├── orders/
    │   └── current.parquet
    └── products/
        └── current.parquet
```

#### Best For

- **Distinct entity types** with different access patterns
- **Natural grouping** of related types (Order + OrderItem)
- **Predictable load distribution** across types
- **Cross-type queries are rare**

---

### Time-Based Sharding

Distribute writes by time period, routing to time-bucketed DOs.

#### How It Works

```
                            ┌─────────────────────────────────┐
                            │         ParqueDBWorker          │
                            │                                 │
  Any write @ 14:32 ───────►│  ┌─────────────────────────┐   │
                            │  │   Time-Based Router     │   │
  Any write @ 14:45 ───────►│  │                         │   │
                            │  │  14:00-14:59 → DO(h:14) │   │
  Any write @ 15:02 ───────►│  │  15:00-15:59 → DO(h:15) │   │
                            │  │  16:00-16:59 → DO(h:16) │   │
                            │  └───────────┬─────────────┘   │
                            │              │                  │
                            └──────────────┼──────────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
           ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
           │  DO(h:14)    │      │  DO(h:15)    │      │  DO(h:16)    │
           │ 14:00-14:59  │      │ 15:00-15:59  │      │ 16:00-16:59  │
           └──────────────┘      └──────────────┘      └──────────────┘

At any instant: only 1 DO active for writes
Over time: old DOs become read-only, new DOs handle writes
```

#### Implementation

```typescript
// Configuration
interface TimeShardConfig {
  strategy: 'time'
  bucketDuration: 'hour' | 'day' | 'week'
  namespace: string  // Base namespace
}

// Example configuration
const shardConfig: TimeShardConfig = {
  strategy: 'time',
  bucketDuration: 'hour',
  namespace: 'events'
}

// Router implementation
class TimeShardedRouter {
  private config: TimeShardConfig

  private getBucketKey(timestamp: Date): string {
    const { bucketDuration, namespace } = this.config
    const ts = timestamp.getTime()

    switch (bucketDuration) {
      case 'hour':
        const hour = Math.floor(ts / (60 * 60 * 1000))
        return `${namespace}:h:${hour}`
      case 'day':
        const day = Math.floor(ts / (24 * 60 * 60 * 1000))
        return `${namespace}:d:${day}`
      case 'week':
        const week = Math.floor(ts / (7 * 24 * 60 * 60 * 1000))
        return `${namespace}:w:${week}`
    }
  }

  getDOForWrite(timestamp: Date = new Date()): DurableObjectStub {
    const bucketKey = this.getBucketKey(timestamp)
    const doId = this.env.PARQUEDB.idFromName(bucketKey)
    return this.env.PARQUEDB.get(doId)
  }

  async create<T>(ns: string, data: CreateInput<T>): Promise<Entity<T>> {
    const stub = this.getDOForWrite()
    return stub.create(ns, data)
  }

  // Query spans multiple time buckets
  async find<T>(ns: string, filter: Filter, timeRange: TimeRange): Promise<Entity<T>[]> {
    const buckets = this.getBucketsInRange(timeRange)
    const results = await Promise.all(
      buckets.map(bucket => {
        const stub = this.env.PARQUEDB.get(
          this.env.PARQUEDB.idFromName(bucket)
        )
        return stub.find(ns, filter)
      })
    )
    return this.mergeResults(results, filter.sort)
  }
}
```

#### Storage Layout

```
bucket/
├── data/
│   └── events/
│       ├── _shards/
│       │   ├── period=2024-01-15-14/   # Hour bucket
│       │   │   └── data.parquet
│       │   ├── period=2024-01-15-15/
│       │   │   └── data.parquet
│       │   └── period=2024-01-15-16/
│       │       └── data.parquet
│       └── data.parquet                 # Compacted historical data
└── events/
    └── events/
        ├── h-2024011514.parquet
        ├── h-2024011515.parquet
        └── h-2024011516.parquet
```

#### Best For

- **Time-series data** (events, logs, metrics)
- **Append-mostly workloads** (no updates to old data)
- **Time-bounded queries** (dashboards, reports)
- **Natural data lifecycle** (archive old buckets)

---

### Hash-Based Sharding

Distribute writes by entity ID hash, spreading load uniformly across DOs.

#### How It Works

```
                            ┌─────────────────────────────────┐
                            │         ParqueDBWorker          │
                            │                                 │
  create(id: "abc123") ────►│  ┌─────────────────────────┐   │
                            │  │   Hash-Based Router     │   │
  create(id: "def456") ────►│  │                         │   │
                            │  │  hash("abc") % 4 = 0    │   │
  create(id: "ghi789") ────►│  │  hash("def") % 4 = 2    │   │
                            │  │  hash("ghi") % 4 = 1    │   │
                            │  └───────────┬─────────────┘   │
                            │              │                  │
                            └──────────────┼──────────────────┘
                                           │
            ┌──────────────┬───────────────┼───────────────┬──────────────┐
            │              │               │               │              │
            ▼              ▼               ▼               ▼              │
     ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐           │
     │ DO(s:0)  │   │ DO(s:1)  │   │ DO(s:2)  │   │ DO(s:3)  │           │
     │ ~30 w/s  │   │ ~30 w/s  │   │ ~30 w/s  │   │ ~30 w/s  │           │
     └──────────┘   └──────────┘   └──────────┘   └──────────┘

Total write capacity: 4 shards × 30 req/s = ~120 req/s
```

#### Implementation

```typescript
// Configuration
interface HashShardConfig {
  strategy: 'hash'
  namespace: string
  shardCount: number  // Power of 2 recommended
}

// Example configuration
const shardConfig: HashShardConfig = {
  strategy: 'hash',
  namespace: 'users',
  shardCount: 4
}

// Router implementation
class HashShardedRouter {
  private config: HashShardConfig

  private hashString(str: string): number {
    // Simple FNV-1a hash
    let hash = 2166136261
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i)
      hash = Math.imul(hash, 16777619)
    }
    return hash >>> 0  // Convert to unsigned
  }

  private getShardIndex(entityId: string): number {
    const hash = this.hashString(entityId)
    return hash % this.config.shardCount
  }

  getDOForEntity(entityId: string): DurableObjectStub {
    const shardIndex = this.getShardIndex(entityId)
    const shardKey = `${this.config.namespace}:s:${shardIndex}`
    const doId = this.env.PARQUEDB.idFromName(shardKey)
    return this.env.PARQUEDB.get(doId)
  }

  async create<T>(ns: string, data: CreateInput<T>): Promise<Entity<T>> {
    // Generate ID first to determine shard
    const entityId = generateULID()
    const stub = this.getDOForEntity(entityId)
    return stub.create(ns, { ...data, $id: entityId })
  }

  async update<T>(ns: string, id: string, update: Update): Promise<Entity<T>> {
    // Route to correct shard based on existing ID
    const stub = this.getDOForEntity(id)
    return stub.update(ns, id, update)
  }

  async get<T>(ns: string, id: string): Promise<Entity<T> | null> {
    const stub = this.getDOForEntity(id)
    return stub.get(ns, id)
  }

  // Queries must fan out to all shards
  async find<T>(ns: string, filter: Filter): Promise<Entity<T>[]> {
    const shardPromises = Array.from(
      { length: this.config.shardCount },
      (_, i) => {
        const shardKey = `${this.config.namespace}:s:${i}`
        const stub = this.env.PARQUEDB.get(
          this.env.PARQUEDB.idFromName(shardKey)
        )
        return stub.find(ns, filter)
      }
    )
    const results = await Promise.all(shardPromises)
    return this.mergeResults(results, filter.sort, filter.limit)
  }
}
```

#### Storage Layout

```
bucket/
├── data/
│   └── users/
│       └── _shards/
│           ├── shard=0/
│           │   └── data.parquet
│           ├── shard=1/
│           │   └── data.parquet
│           ├── shard=2/
│           │   └── data.parquet
│           └── shard=3/
│               └── data.parquet
└── events/
    └── users/
        ├── s0-current.parquet
        ├── s1-current.parquet
        ├── s2-current.parquet
        └── s3-current.parquet
```

#### Best For

- **Uniform access patterns** (no hot keys)
- **Point queries by ID** (get, update, delete)
- **Maximum write throughput** needed
- **Scan queries acceptable** or via R2 directly

---

## Implementation Guide

### Step 1: Choose Strategy

| Question | If Yes | If No |
|----------|--------|-------|
| Do entity types have distinct workloads? | Type-based | Continue |
| Is data time-series or append-only? | Time-based | Continue |
| Do you need maximum write throughput? | Hash-based | Single DO |

### Step 2: Configure Sharding

```typescript
// In wrangler.toml or environment config
interface ShardingConfig {
  enabled: boolean
  strategy: 'type' | 'time' | 'hash'

  // Type-based options
  typeMapping?: Record<string, string>

  // Time-based options
  bucketDuration?: 'hour' | 'day' | 'week'

  // Hash-based options
  shardCount?: number
}

// Example: Type-based sharding
export const SHARDING_CONFIG: ShardingConfig = {
  enabled: true,
  strategy: 'type',
  typeMapping: {
    User: 'users',
    Session: 'sessions',
    Order: 'orders',
    OrderItem: 'orders',
    Product: 'products',
    Review: 'products',
  }
}
```

### Step 3: Update Worker Configuration

```typescript
// ParqueDBWorker with sharding
export class ParqueDBWorker extends RpcTarget {
  private router: ShardRouter

  constructor(env: Env, ctx: ExecutionContext) {
    super()
    this.router = createShardRouter(env, SHARDING_CONFIG)
  }

  async create<T>(ns: string, data: CreateInput<T>): Promise<Entity<T>> {
    const stub = this.router.getDOForCreate(ns, data)
    const result = await stub.create(ns, data)
    await this.invalidateCache(ns)
    return result
  }
}
```

### Step 4: Update Queries for Multi-Shard

```typescript
// QueryExecutor for sharded data
class ShardedQueryExecutor {
  async find<T>(ns: string, filter: Filter, options: FindOptions): Promise<Entity<T>[]> {
    // Option A: Query R2 directly (recommended for scans)
    const shardPaths = await this.listShardPaths(ns)
    const results = await Promise.all(
      shardPaths.map(path => this.queryParquet(path, filter))
    )
    return this.mergeAndSort(results, options)

    // Option B: Query through DOs (for strong consistency)
    // const shards = this.router.getAllShards(ns)
    // return this.fanOutQuery(shards, filter, options)
  }
}
```

---

## Trade-offs Comparison

| Aspect | Type-Based | Time-Based | Hash-Based |
|--------|-----------|------------|------------|
| **Max throughput** | types × 30/s | 30/s (current bucket) | shards × 30/s |
| **Query complexity** | Type-scoped = simple | Time-ranged = moderate | All queries = fan-out |
| **Cross-entity txns** | Within type only | Within bucket only | Within shard only |
| **Hot spots** | Popular types | Current bucket | None (uniform) |
| **Storage efficiency** | Good (type clustering) | Good (time clustering) | Moderate (scattered) |
| **Implementation** | Simple | Moderate | Complex |
| **Scaling** | Add types | Automatic (time) | Increase shard count |
| **Rebalancing** | N/A | N/A | Requires resharding |

### Cross-Shard Operations

| Strategy | Cross-Shard Writes | Cross-Shard Reads |
|----------|-------------------|-------------------|
| Type-based | Saga pattern | Join types at read time |
| Time-based | N/A (current bucket only) | Merge across buckets |
| Hash-based | Saga pattern | Fan-out to all shards |

### Consistency Trade-offs

| Strategy | Within Shard | Cross-Shard |
|----------|-------------|-------------|
| Type-based | Strong | Eventually consistent |
| Time-based | Strong | Eventually consistent |
| Hash-based | Strong | Eventually consistent |

---

## When to Shard

### Monitoring Indicators

Enable these metrics to detect when sharding is needed:

```typescript
// Metrics to monitor
interface DOMetrics {
  // Request latency
  p50LatencyMs: number
  p95LatencyMs: number
  p99LatencyMs: number

  // Throughput
  requestsPerSecond: number

  // Queue depth (indicates backpressure)
  queueDepth: number
  queueWaitMs: number

  // Errors
  timeoutRate: number
  errorRate: number
}
```

### Sharding Thresholds

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| p99 latency | > 500ms | > 2000ms | Enable sharding |
| Requests/sec | > 20/s | > 40/s | Enable sharding |
| Queue depth | > 10 | > 50 | Enable sharding |
| Timeout rate | > 1% | > 5% | Enable sharding immediately |

### Decision Matrix

| Current Load | Expected Growth | Recommendation |
|-------------|-----------------|----------------|
| < 20 req/s | Stable | Single DO |
| < 20 req/s | 2-3x growth | Plan sharding strategy |
| 20-40 req/s | Any | Implement sharding |
| > 40 req/s | Any | Urgent: implement sharding |

---

## Related Documentation

- [Consistency Model](./consistency.md) - Read/write consistency guarantees
- [Entity Storage Architecture](./entity-storage.md) - Dual storage (DO + R2)
- [Namespace-Sharded Architecture](./namespace-sharded-architecture.md) - Data file sharding
- [DO WAL Rewrite](./DO_WAL_REWRITE.md) - Cost optimization proposal
- [Compaction Capacity Planning](../guides/compaction-capacity-planning.md) - Write throughput scaling

---

## Summary

ParqueDB uses a single-writer Durable Object architecture for strong consistency guarantees. The inherent ~30 req/sec bottleneck is a deliberate trade-off for simplicity and correctness.

When this limit becomes constraining:

1. **Type-based sharding**: Best for distinct entity types
2. **Time-based sharding**: Best for time-series/event data
3. **Hash-based sharding**: Best for maximum throughput with uniform access

Each strategy preserves strong consistency within shards while accepting eventual consistency for cross-shard operations.

**Key takeaways:**
- Monitor latency and throughput metrics
- Plan sharding strategy before hitting limits
- Choose strategy based on data access patterns
- Cross-shard transactions require saga patterns

---

*Architecture Design Document - ParqueDB Durable Object Write Scaling*
