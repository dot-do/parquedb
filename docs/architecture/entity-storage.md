---
title: Entity Storage Architecture
description: Unified event-sourced storage architecture for ParqueDB across Node.js and Cloudflare Workers environments.
---

## Overview

ParqueDB uses an **event-sourced architecture** as the unified storage model across all environments. This eliminates the previous divergence between Node.js (globalEntityStore) and Workers (SQLite) implementations.

### Key Principle: Events as Source of Truth

```
┌─────────────────────────────────────────────────────────────────┐
│                    Event-Sourced Core                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │               EventSourcedBackend                        │   │
│  │  ┌─────────────────┐  ┌─────────────────┐               │   │
│  │  │   Event Log     │  │   Snapshots     │               │   │
│  │  │  (source of     │  │  (performance   │               │   │
│  │  │   truth)        │  │   checkpoints)  │               │   │
│  │  └────────┬────────┘  └────────┬────────┘               │   │
│  │           │                    │                         │   │
│  │           └──────────┬─────────┘                         │   │
│  │                      │                                   │   │
│  │           ┌──────────▼──────────┐                        │   │
│  │           │  Entity Reconstruction │                     │   │
│  │           │  (snapshot + replay)   │                     │   │
│  │           └──────────┬──────────┘                        │   │
│  │                      │                                   │   │
│  │           ┌──────────▼──────────┐                        │   │
│  │           │   Entity Cache      │                        │   │
│  │           │   (derived state)   │                        │   │
│  │           └─────────────────────┘                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                     │
│                           ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              StorageBackend (pluggable)                  │   │
│  │  MemoryBackend │ FsBackend │ R2Backend │ DOSqliteBackend │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

This unified model provides:

1. **Consistency**: Same behavior in Node.js tests and Workers production
2. **Simplicity**: Single codebase for storage logic
3. **Time-travel**: Built-in history from event log
4. **Auditability**: Complete change tracking

## Unified Architecture

### EventSourcedBackend

The `EventSourcedBackend` class (`src/storage/EventSourcedBackend.ts`) provides a unified interface:

```typescript
import { EventSourcedBackend, withEventSourcing } from '@parquedb/storage'

// Wrap any StorageBackend with event-sourcing semantics
const storage = withEventSourcing(new MemoryBackend(), {
  autoSnapshotThreshold: 100,
  maxCachedEntities: 1000,
})

// All operations go through the event log
await storage.appendEvent(event)
const entity = await storage.reconstructEntity('users', 'abc123')
```

### How It Works

1. **Writes**: All mutations append events to the event log
2. **Reads**: Entity state is reconstructed from last snapshot + replayed events
3. **Snapshots**: Periodic checkpoints for fast reconstruction
4. **Caching**: Derived entity state is cached with TTL

## Legacy Architecture (Deprecated)

The previous dual architecture is deprecated but still documented for reference:

### Node.js / Testing Environment (Legacy)

```
+-----------------------------------------------------+
|                  ParqueDB.ts                        |
|                                                     |
|  +----------------------------------------------+   |
|  |         globalEntityStore (WeakMap)          |   |
|  |  +---------------------------------------+   |   |
|  |  |   Map<StorageBackend, Map<$id, Entity>> | |   |
|  |  +---------------------------------------+   |   |
|  +----------------------------------------------+   |
|                        |                            |
|                        v                            |
|  +----------------------------------------------+   |
|  |            StorageBackend                    |   |
|  |  (FsBackend, MemoryBackend, R2Backend, etc.) |   |
|  +----------------------------------------------+   |
+-----------------------------------------------------+
```

### Cloudflare Workers Environment (Legacy)

## Current Architecture

### Node.js / Testing Environment

```
+-----------------------------------------------------+
|                  ParqueDB.ts                        |
|                                                     |
|  +----------------------------------------------+   |
|  |         globalEntityStore (WeakMap)          |   |
|  |  +---------------------------------------+   |   |
|  |  |   Map<StorageBackend, Map<$id, Entity>> | |   |
|  |  +---------------------------------------+   |   |
|  +----------------------------------------------+   |
|                        |                            |
|                        v                            |
|  +----------------------------------------------+   |
|  |            StorageBackend                    |   |
|  |  (FsBackend, MemoryBackend, R2Backend, etc.) |   |
|  +----------------------------------------------+   |
+-----------------------------------------------------+
```

**How it works:**
- Entities are stored in-memory in `globalEntityStore`
- Events are buffered in `globalEventStore`
- Periodic flush writes events to Parquet files via StorageBackend
- Entity reconstruction on read uses in-memory state + event replay

**Use cases:**
- Unit tests with MemoryBackend
- Local development with FsBackend
- CLI tools
- Server-side Node.js applications

### Cloudflare Workers Environment

```
+-----------------------------------------------------------------+
|                     ParqueDBWorker.ts                           |
|                                                                 |
|  +---------------------+       +----------------------------+   |
|  |   READS (fast)      |       |   WRITES (consistent)      |   |
|  |                     |       |                            |   |
|  |  QueryExecutor      |       |  ParqueDBDO (Durable Obj)  |   |
|  |       |             |       |       |                    |   |
|  |       v             |       |       v                    |   |
|  |   ReadPath          |       |   SQLite (SqlStorage)      |   |
|  |       |             |       |       |                    |   |
|  |       v             |       |       v                    |   |
|  |  Cache API + R2     |       |  Flush to R2 (Parquet)     |   |
|  +---------------------+       +----------------------------+   |
+-----------------------------------------------------------------+
```

**How it works:**
- READS: Go directly to R2 via `QueryExecutor`/`ReadPath` with Cache API
- WRITES: Routed through `ParqueDBDO` Durable Object for consistency
- `ParqueDBDO` stores entities in SQLite (`entities` table)
- Events are logged to SQLite and periodically flushed to R2 as Parquet

**Why SQLite in DO?**
- Single-writer consistency (no race conditions)
- Transactional updates
- Fast lookups by ID
- Relationship indexes

**Why R2 for reads?**
- Distributed edge caching
- No DO round-trip latency for reads
- Parquet columnar format for efficient scans
- CDN-friendly immutable files

## Source of Truth

| Environment | Writes Source of Truth | Reads Source of Truth |
|------------|------------------------|----------------------|
| Node.js/Testing | `globalEntityStore` (in-memory) | `globalEntityStore` + event replay |
| Workers | `ParqueDBDO` SQLite | R2 Parquet files |

## Why Two Implementations?

1. **Different runtime constraints**:
   - Node.js has persistent process memory
   - Workers are stateless; need Durable Objects for state

2. **Different performance profiles**:
   - Node.js: In-memory is fastest
   - Workers: R2 with edge caching is fastest for reads

3. **Consistency requirements**:
   - Workers need single-writer pattern via DO
   - Node.js can use simpler in-process locking

## Consistency Guarantees

### Node.js
- Strong consistency within a single process
- Eventual consistency across processes (via storage backend)
- Event log provides time-travel and audit trail

### Workers
- Strong consistency for writes (single DO per namespace)
- Eventual consistency for reads (R2 updates after DO flush)
- Cache invalidation on writes for faster convergence

## Migration Path

### Current State (v1.0)

**Unified Event-Sourced Core (Implemented)**:
- `EventSourcedBackend` wraps any StorageBackend with event-sourcing semantics
- Events are the single source of truth
- Entity state is derived by replaying events from snapshots
- Same behavior across Node.js and Workers environments

### Migration Steps

1. **New Code**: Use `EventSourcedBackend` directly
2. **Existing Node.js Code**: Wrap existing StorageBackend with `withEventSourcing()`
3. **Existing Workers Code**: ParqueDBDO already uses event sourcing internally

```typescript
// Before (legacy)
const db = new ParqueDB({ storage: new MemoryBackend() })

// After (unified)
import { withEventSourcing, MemoryBackend } from '@parquedb/storage'
const storage = withEventSourcing(new MemoryBackend())
const db = new ParqueDB({ storage })
```

## Recommendations

### For All Environments

1. **Use EventSourcedBackend**: Wrap your storage with `withEventSourcing()` for consistent behavior
2. **Configure Snapshots**: Set `autoSnapshotThreshold` based on your read/write ratio
3. **Monitor Event Log Size**: Implement retention policies for event archival

### For Node.js Applications

```typescript
import { FsBackend, withEventSourcing } from '@parquedb/storage'

const storage = withEventSourcing(new FsBackend('/data'), {
  autoSnapshotThreshold: 50,  // Snapshot every 50 events
  maxCachedEntities: 5000,    // Cache up to 5000 entities
})
```

### For Cloudflare Workers

```typescript
// ParqueDBDO already uses event sourcing internally
// For reads, continue using QueryExecutor with R2
// The unified model ensures consistency between environments
```

### For Tests

```typescript
import { MemoryBackend, withEventSourcing } from '@parquedb/storage'

const storage = withEventSourcing(new MemoryBackend(), {
  autoSnapshotThreshold: 10,  // More frequent snapshots for testing
})
```

## Related Documentation

- [Consistency Model](./consistency.md) - Consistency guarantees
- [DO Write Bottleneck](./do-write-bottleneck.md) - Single-writer scaling strategies
- [Graph-First Architecture](./graph-first-architecture.md) - Relationship storage
- [Secondary Indexes](./secondary-indexes.md) - Index architecture
