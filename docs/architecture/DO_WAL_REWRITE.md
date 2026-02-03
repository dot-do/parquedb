---
title: DO WAL Rewrite - Cost Optimization
description: Proposal to rewrite ParqueDBDO for cost optimization by using SQLite as WAL-only (events buffer) rather than full entity store, with bulk operations bypassing SQLite entirely.
---

# DO WAL Rewrite - Cost Optimization

**Status**: Proposal
**Author**: Claude Code
**Date**: 2026-02-03

## Executive Summary

The current `ParqueDBDO` implementation stores each entity as a separate SQLite row, causing cost explosion on Cloudflare Workers. This document proposes a rewrite that uses SQLite exclusively as a Write-Ahead Log (WAL) for events, with bulk operations streaming directly to R2.

**Key insight**: DO SQLite is 4.5x more expensive than R2 writes per operation. For batches of 5+ entities, writing directly to R2 is cheaper than routing through SQLite.

## Current Architecture

```
                              ParqueDBDO (Current)
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    SQLite (SqlStorage)                           │  │
│  │                                                                  │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │  │
│  │  │    entities     │  │  relationships  │  │     events      │  │  │
│  │  │                 │  │                 │  │                 │  │  │
│  │  │ 1 row/entity    │  │ 1 row/edge     │  │ 1 row/event     │  │  │
│  │  │ (COST ISSUE!)   │  │                 │  │ (flushed flag)  │  │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘  │  │
│  │                                                                  │  │
│  │  ┌─────────────────┐                                             │  │
│  │  │   checkpoints   │                                             │  │
│  │  └─────────────────┘                                             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                │                                       │
│                                ▼                                       │
│                        Periodic Flush                                  │
│                                │                                       │
└────────────────────────────────┼───────────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │     R2 (Parquet)       │
                    │  events/archive/*.pq   │
                    └────────────────────────┘
```

### Cost Analysis - Current

| Operation | SQLite Rows | DO Cost | R2 Cost | Notes |
|-----------|-------------|---------|---------|-------|
| Create 1 entity | 2 (entity + event) | $$ | - | Write entity + log event |
| Create 100 entities | 200 | $$$$ | - | 100 entities + 100 events |
| Bulk import 10K | 20,000 | $$$$$$$ | - | Kills the cost budget |

**Problem**: Each entity write = 1 SQLite row. Events = 1 row per event. At 4.5x DO premium, this explodes costs.

### Issues Identified

1. **Line 340-344**: Each `create()` inserts entity into `entities` table
   ```typescript
   this.sql.exec(
     `INSERT INTO entities (ns, id, type, name, ...) VALUES (?, ?, ...)`,
     ns, id, $type, name, ...
   )
   ```

2. **Line 808-822**: Each event = separate row in `events` table
   ```typescript
   this.sql.exec(
     `INSERT INTO events (id, ts, target, op, ...) VALUES (?, ?, ...)`,
     event.id, ...
   )
   ```

3. **Line 500-505**: Updates also write per-entity rows
   ```typescript
   this.sql.exec(
     `UPDATE entities SET ... WHERE ns = ? AND id = ?`,
     ...
   )
   ```

4. **No bulk bypass**: There's no path for bulk operations to skip SQLite and write directly to R2.

5. **Flush strategy inadequate**: Current flush (line 840-888) flushes events to R2 but entities remain in SQLite forever.

## Proposed Architecture

```
                           ParqueDBDO (Proposed)
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Write Path Decision (threshold = 5 entities)                           │
│                                                                         │
│  ┌─────────────────────────────────────┐                                │
│  │   Single/Small Batch (< 5)          │                                │
│  │   ──────────────────────────        │                                │
│  │         │                           │                                │
│  │         ▼                           │                                │
│  │  ┌─────────────────────────┐        │     ┌───────────────────────┐  │
│  │  │   SQLite WAL            │        │     │   Bulk (>= 5)         │  │
│  │  │   (events_wal table)    │        │     │   ──────────────      │  │
│  │  │                         │        │     │         │             │  │
│  │  │   ┌───────────────────┐ │        │     │         ▼             │  │
│  │  │   │ Row Group Batch   │ │        │     │   Stream directly     │  │
│  │  │   │ (BLOB, ~1000 evts)│ │        │     │   to R2 Parquet       │  │
│  │  │   └───────────────────┘ │        │     │         │             │  │
│  │  │                         │        │     │         ▼             │  │
│  │  │   ┌───────────────────┐ │        │     │   Return entity IDs   │  │
│  │  │   │ Row Group Batch   │ │        │     │                       │  │
│  │  │   │ (BLOB, ~1000 evts)│ │        │     └───────────────────────┘  │
│  │  │   └───────────────────┘ │        │                                │
│  │  └─────────────────────────┘        │                                │
│  │              │                      │                                │
│  │              ▼                      │                                │
│  │        Alarm/Threshold              │                                │
│  │              │                      │                                │
│  └──────────────┼──────────────────────┘                                │
│                 │                                                       │
│                 ▼                                                       │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                        R2 (Parquet)                               │   │
│  │                                                                   │   │
│  │  data/{ns}/data.parquet          (entities - full dataset)       │   │
│  │  data/{ns}/pending/*.parquet     (unflushed row groups)          │   │
│  │  events/current.parquet          (CDC log)                       │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Changes

1. **SQLite = WAL only**: No `entities` or `relationships` tables. Only event batches stored as blobs.
2. **Bulk bypass**: 5+ entities stream directly to R2 as pending Parquet row groups.
3. **Row groups as blobs**: Events batched into ~1000 event blobs, not individual rows.
4. **Reads merge**: QueryExecutor merges `data.parquet` + `pending/*.parquet` + unflushed WAL.

## New SQLite Schema

```sql
-- REMOVED: entities, relationships tables (cost explosion)

-- Keep: events_wal (but batched as blobs)
CREATE TABLE IF NOT EXISTS events_wal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  -- Row group batch (serialized EventBatch)
  batch BLOB NOT NULL,           -- JSON or MessagePack encoded events

  -- Batch metadata (for efficient queries)
  min_ts INTEGER NOT NULL,       -- Earliest event timestamp
  max_ts INTEGER NOT NULL,       -- Latest event timestamp
  event_count INTEGER NOT NULL,  -- Number of events in batch

  -- Target info (for read filtering)
  namespaces TEXT NOT NULL,      -- JSON array of affected namespaces

  -- Flush tracking
  flushed INTEGER DEFAULT 0,     -- 0=pending, 1=flushed to R2
  r2_path TEXT,                  -- Path if flushed to pending/

  created_at TEXT DEFAULT (datetime('now'))
);

-- Index for unflushed batches
CREATE INDEX IF NOT EXISTS idx_events_wal_unflushed
ON events_wal (flushed, min_ts);

-- Index for namespace filtering
CREATE INDEX IF NOT EXISTS idx_events_wal_namespaces
ON events_wal (namespaces);

-- Keep: flush checkpoints
CREATE TABLE IF NOT EXISTS checkpoints (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  batch_ids TEXT NOT NULL,        -- JSON array of WAL batch IDs included
  r2_path TEXT NOT NULL,          -- Final Parquet path
  row_count INTEGER NOT NULL
);

-- NEW: pending row groups (for tracking unflushed R2 writes)
CREATE TABLE IF NOT EXISTS pending_row_groups (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  r2_path TEXT NOT NULL,          -- data/{ns}/pending/{id}.parquet
  row_count INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  merged INTEGER DEFAULT 0         -- 1 = merged into data.parquet
);

CREATE INDEX IF NOT EXISTS idx_pending_ns
ON pending_row_groups (namespace, merged);
```

### Cost Comparison

| Operation | Current (rows) | Proposed (rows) | Savings |
|-----------|---------------|-----------------|---------|
| Create 1 entity | 2 | 0-1* | 50-100% |
| Create 100 entities | 200 | 1 | 99.5% |
| Bulk import 10K | 20,000 | 10 | 99.95% |

*Single entities batched; may be 0 if coalesced with other events

## Write Path Details

### Path 1: Single/Small Batch (< 5 entities)

```
Client ──► DO.create() ──► Buffer event in memory
                            │
                            ▼
                         BatchCoalescer
                            │
                         (accumulate until batch threshold OR timeout)
                            │
                            ▼
                         Write batch blob to events_wal
                         (single row, ~1000 events)
                            │
                            ▼
                         Return immediately
                            │
                            ▼
                         Alarm triggers flush to R2
```

**Cost**: 1 SQLite row per ~1000 events (vs 1000 rows current)

### Path 2: Bulk Operations (>= 5 entities)

```
Client ──► DO.createMany(entities) ──► Check count >= 5
                                         │
                                         ▼
                                    ┌────┴────┐
                                    │         │
                             Generate ULIDs   Stream to R2
                             for all entities  pending Parquet
                                    │         │
                                    └────┬────┘
                                         │
                                         ▼
                                    Record in pending_row_groups
                                    (1 SQLite row for entire batch)
                                         │
                                         ▼
                                    Log bulk event to WAL
                                    (1 event, not N)
                                         │
                                         ▼
                                    Return entity IDs
```

**Cost**: 2 SQLite rows total (metadata + event) for ANY batch size

### Path 3: Flush (Alarm-triggered)

```
Alarm ──► Read unflushed batches from events_wal
           │
           ▼
        Deserialize event batches
           │
           ▼
        Group by namespace
           │
           ▼
        For each namespace:
           ├──► Read existing data/{ns}/data.parquet
           │
           ├──► Merge with pending row groups
           │
           ├──► Apply events (replay)
           │
           ├──► Write new data.parquet
           │
           └──► Mark batches flushed, delete pending

        Log checkpoint
```

## Read Path Details

```
QueryExecutor.find(ns, filter)
     │
     ▼
  Read data/{ns}/data.parquet (main data)
     │
     ▼
  Read data/{ns}/pending/*.parquet (unflushed bulk writes)
     │
     ▼
  Query DO for unflushed WAL events affecting ns
     │
     ▼
  Merge all sources:
     • data.parquet rows
     • pending/*.parquet rows
     • Replayed events from WAL
     │
     ▼
  Apply filter, sort, pagination
     │
     ▼
  Return results
```

### Read Merge Strategy

```typescript
async find(ns: string, filter: Filter): Promise<Entity[]> {
  // 1. Read committed data
  const committed = await this.readParquet(`data/${ns}/data.parquet`)

  // 2. Read pending row groups
  const pending = await this.listPendingRowGroups(ns)
  const pendingRows = await Promise.all(
    pending.map(p => this.readParquet(p.r2_path))
  )

  // 3. Get unflushed events from DO
  const unflushedEvents = await this.doStub.getUnflushedEvents(ns)

  // 4. Build entity map (latest version wins)
  const entityMap = new Map<string, Entity>()

  // Add committed (oldest)
  for (const row of committed) {
    entityMap.set(row.$id, row)
  }

  // Apply pending (newer)
  for (const rows of pendingRows) {
    for (const row of rows) {
      entityMap.set(row.$id, row)
    }
  }

  // Replay events (newest)
  for (const event of unflushedEvents) {
    this.applyEvent(entityMap, event)
  }

  // 5. Filter and return
  return [...entityMap.values()].filter(e => matchesFilter(e, filter))
}
```

## Bulk Operations API

```typescript
interface ParqueDBDO {
  // Existing (unchanged signature, new implementation)
  create(ns: string, data: CreateInput): Promise<Entity>
  update(ns: string, id: string, update: UpdateInput): Promise<Entity>
  delete(ns: string, id: string): Promise<boolean>

  // New bulk operations
  createMany(ns: string, data: CreateInput[]): Promise<Entity[]>
  updateMany(ns: string, filter: Filter, update: UpdateInput): Promise<number>
  deleteMany(ns: string, filter: Filter): Promise<number>

  // For read merging
  getUnflushedEvents(ns: string): Promise<Event[]>
  getPendingRowGroups(ns: string): Promise<PendingRowGroup[]>
}
```

### Threshold Logic

```typescript
const BULK_THRESHOLD = 5

async createMany(ns: string, data: CreateInput[]): Promise<Entity[]> {
  if (data.length < BULK_THRESHOLD) {
    // Small batch: use WAL path
    return Promise.all(data.map(d => this.create(ns, d)))
  }

  // Large batch: stream to R2
  return this.bulkCreateToR2(ns, data)
}

private async bulkCreateToR2(ns: string, data: CreateInput[]): Promise<Entity[]> {
  const entities = data.map(d => this.toEntity(ns, d))

  // Generate Parquet row group
  const parquet = await this.generateParquet(entities)

  // Write to pending location
  const id = generateULID()
  const path = `data/${ns}/pending/${id}.parquet`
  await this.env.BUCKET.put(path, parquet)

  // Track in SQLite (1 row!)
  this.sql.exec(
    `INSERT INTO pending_row_groups (id, namespace, r2_path, row_count, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    id, ns, path, entities.length, new Date().toISOString()
  )

  // Log single bulk event
  await this.appendBulkEvent('CREATE', ns, entities.map(e => e.$id))

  return entities
}
```

## Migration Plan

### Phase 1: Add WAL batching (non-breaking)
- Keep existing tables
- Add `events_wal` table alongside `events`
- Add `pending_row_groups` table
- Implement batched event writer
- DO continues to work as before, just more efficiently

### Phase 2: Implement bulk bypass
- Add `createMany()`, `updateMany()`, `deleteMany()` to DO
- Implement R2 streaming for bulk ops
- Update QueryExecutor to merge pending row groups

### Phase 3: Remove entity storage
- Stop writing to `entities` table
- Update reads to use merged path
- Add migration script to flush `entities` to R2
- Drop `entities` and `relationships` tables

### Phase 4: Optimize reads
- Implement efficient WAL event querying
- Add caching for pending row groups
- Optimize merge strategy

## Consistency Guarantees

### Unchanged
- **Write ordering**: Single DO still serializes all writes
- **Strong consistency for writes**: Via DO
- **Eventual consistency for reads**: Via R2 + caching

### New considerations
- **Read-after-write**: May require DO round-trip for unflushed data
- **Bulk atomicity**: Entire bulk op succeeds or fails
- **Merge correctness**: Entity map with latest-wins semantics

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Merge complexity | Medium | Thorough testing of all merge paths |
| WAL corruption | High | Checksums, WAL validation |
| Bulk write failure | Medium | Transaction-style write (metadata last) |
| Read latency increase | Low | Cache pending row groups aggressively |
| Migration data loss | High | Keep old tables until fully migrated |

## Implementation Estimate

| Phase | Effort | Timeline |
|-------|--------|----------|
| Phase 1: WAL batching | 2-3 days | Week 1 |
| Phase 2: Bulk bypass | 3-4 days | Week 1-2 |
| Phase 3: Remove entity storage | 2 days | Week 2 |
| Phase 4: Optimize | 2 days | Week 3 |
| Testing & docs | 3 days | Week 3 |

**Total**: ~2-3 weeks

## Success Metrics

1. **Cost reduction**: 90%+ reduction in DO SQLite charges
2. **Bulk import speed**: 10x faster for large datasets
3. **No regression**: All existing tests pass
4. **Latency**: p50 read latency unchanged or improved

## Related Documents

- [Entity Storage Architecture](./entity-storage.md)
- [Consistency Model](./consistency.md)
- [Graph-First Architecture](./graph-first-architecture.md)

## Appendix: Existing Infrastructure

The codebase already has building blocks for this rewrite:

### SqliteWal (`src/events/sqlite-wal.ts`)
Already implements batched event storage with blob serialization. Can be reused directly.

### EventBatch type (`src/events/types.ts`)
```typescript
interface EventBatch {
  events: Event[]
  minTs: number
  maxTs: number
  count: number
  sizeBytes?: number
}
```

### EventWriter (`src/events/writer.ts`)
Has flush handler pattern that can be connected to WAL.

### ParquetWriter (`src/parquet/writer.ts`)
Can generate row groups for bulk R2 writes.
