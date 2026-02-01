# Entity Storage Architecture

## Overview

ParqueDB has two distinct storage implementations depending on the runtime environment:

1. **Node.js/Testing**: `ParqueDB.ts` with in-memory `globalEntityStore`
2. **Cloudflare Workers**: `ParqueDBDO.ts` with SQLite + `QueryExecutor` with R2

This document explains why both exist, when to use each, and future consolidation plans.

## Current Architecture

### Node.js / Testing Environment

```
┌─────────────────────────────────────────────────────┐
│                  ParqueDB.ts                        │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │         globalEntityStore (WeakMap)           │  │
│  │  ┌─────────────────────────────────────────┐ │  │
│  │  │   Map<StorageBackend, Map<$id, Entity>> │ │  │
│  │  └─────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────┘  │
│                        │                            │
│                        ▼                            │
│  ┌──────────────────────────────────────────────┐  │
│  │            StorageBackend                     │  │
│  │  (FsBackend, MemoryBackend, R2Backend, etc.)  │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
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
┌─────────────────────────────────────────────────────────────────┐
│                     ParqueDBWorker.ts                           │
│                                                                 │
│  ┌─────────────────────┐       ┌────────────────────────────┐  │
│  │   READS (fast)      │       │   WRITES (consistent)      │  │
│  │                     │       │                            │  │
│  │  QueryExecutor      │       │  ParqueDBDO (Durable Obj)  │  │
│  │       │             │       │       │                    │  │
│  │       ▼             │       │       ▼                    │  │
│  │   ReadPath          │       │   SQLite (SqlStorage)      │  │
│  │       │             │       │       │                    │  │
│  │       ▼             │       │       ▼                    │  │
│  │  Cache API + R2     │       │  Flush to R2 (Parquet)     │  │
│  └─────────────────────┘       └────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
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

### Current State (v0.x)
- Two separate implementations
- `globalEntityStore` for Node.js
- SQLite for Workers

### Future State (v1.0)
Options under consideration:

**Option A: Unified Storage Abstraction**
- ParqueDB.ts delegates all storage to backend
- StorageBackend gets transactional write methods
- In-memory caching layer on top of storage

**Option B: Event-Sourced Core**
- Both environments use event log as source of truth
- Materialized views (in-memory, SQLite, or Parquet)
- Snapshots for fast reconstruction

**Option C: Storage-Specific Optimizations**
- Keep current architecture
- Better document the differences
- Provide migration tools between environments

## Recommendations

### For New Code

1. **Node.js applications**: Use `ParqueDB` class directly
2. **Workers**: Use `ParqueDBWorker` (reads) + `ParqueDBDO` (writes)
3. **Tests**: Use `MemoryBackend` with `ParqueDB`

### For Existing Code

1. Don't mix Node.js and Workers code paths
2. Be aware that Workers reads come from R2, not SQLite
3. Allow time for R2 propagation after writes

## Related Documentation

- [CONSISTENCY.md](./CONSISTENCY.md) - Consistency guarantees
- [GRAPH_FIRST_ARCHITECTURE.md](./GRAPH_FIRST_ARCHITECTURE.md) - Relationship storage
- [SECONDARY_INDEXES.md](./SECONDARY_INDEXES.md) - Index architecture
