---
title: SQLite Row Reduction Opportunities in ParqueDBDO
description: Analysis and recommendations for further reducing SQLite row operations to minimize Cloudflare DO billing costs.
---

# SQLite Row Reduction Opportunities in ParqueDBDO

**Status**: Analysis Complete
**Author**: Claude Code
**Date**: 2026-02-03

## Executive Summary

This document identifies additional SQLite row reduction opportunities in `ParqueDBDO` beyond the existing WAL batching implementation. Cloudflare bills Durable Objects based on SQLite row operations (reads and writes), making row reduction a key cost optimization lever.

**Current Cost Model**:
- DO SQLite operations: ~4.5x more expensive than R2
- Each row read/write incurs fixed billing overhead
- Batch operations amortize overhead across many logical operations

**Current Optimizations Already Implemented**:
1. WAL batching for entity events (`events_wal` table) - ~100 events per row
2. WAL batching for relationship events (`rels_wal` table) - ~100 events per row
3. Bulk create bypass (5+ entities) directly to R2 with metadata row
4. Sequence counters in memory with lazy initialization
5. Compression for event blobs (via `WalOptimizer`)

## Analysis: Additional Row Reduction Opportunities

### Category 1: Read Operations

#### 1.1 `maybeScheduleFlush()` Queries `getUnflushedEventCount()` on Every Operation

**Location**: `src/worker/ParqueDBDO.ts` lines 2302-2315

**Current Behavior**:
```typescript
private async maybeScheduleFlush(): Promise<void> {
  if (this.flushAlarmSet) return
  const count = await this.getUnflushedEventCount()  // SQLite query every time!
  // ...
}
```

**Problem**: Every `create()`, `update()`, `delete()`, `link()`, and `unlink()` calls `maybeScheduleFlush()`, which queries SQLite even when no flush decision needs to be made.

**Cost Impact**:
- 1 SELECT query per write operation
- At 1000 writes: 1000 unnecessary reads

**Recommended Solution**:
```typescript
// Track unflushed count in memory
private unflushedEventCount = 0

async appendEvent(event: Event): Promise<void> {
  // ... existing buffering logic ...
  this.unflushedEventCount++
}

private async maybeScheduleFlush(): Promise<void> {
  if (this.flushAlarmSet) return

  // Use in-memory count instead of SQLite query
  const count = this.unflushedEventCount + this.eventBuffer.length

  if (count >= this.flushConfig.maxEvents) {
    await this.flushToParquet()
    this.unflushedEventCount = 0  // Reset after flush
  } else if (count >= this.flushConfig.minEvents) {
    await this.ctx.storage.setAlarm(Date.now() + this.flushConfig.maxInterval)
    this.flushAlarmSet = true
  }
}

// On DO initialization, sync from SQLite once
private async initializeUnflushedCount(): Promise<void> {
  const rows = [...this.sql.exec<{ total: number }>(
    'SELECT SUM(event_count) as total FROM event_batches WHERE flushed = 0'
  )]
  this.unflushedEventCount = rows[0]?.total || 0
}
```

**Estimated Savings**: 1 row read per write operation

---

#### 1.2 `getEntityFromEvents()` Reads All WAL Batches

**Location**: `src/worker/ParqueDBDO.ts` lines 1306-1366

**Current Behavior**:
```typescript
async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
  // Reads ALL event_batches
  const batchRows = [...this.sql.exec<EventBatchRow>(
    `SELECT batch FROM event_batches WHERE flushed = 0 ORDER BY min_ts ASC`
  )]

  // Then reads ALL events_wal for namespace
  const walRows = [...this.sql.exec<WalRow>(
    `SELECT events FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`,
    ns
  )]
  // ...
}
```

**Problem**:
- Reads all unflushed batches even when looking for a single entity
- No filtering by namespace in `event_batches` query
- Multiple full table scans per entity lookup

**Cost Impact**:
- N batch rows + M WAL rows per entity lookup
- With 100 batches: 100+ row reads per `get()` call

**Recommended Solutions**:

**Option A: Add namespace index to event_batches**
```sql
-- Add column and index for namespace filtering
ALTER TABLE event_batches ADD COLUMN namespaces TEXT;  -- JSON array
CREATE INDEX idx_event_batches_ns ON event_batches(namespaces);

-- Query with namespace filter
SELECT batch FROM event_batches
WHERE flushed = 0 AND namespaces LIKE '%"posts"%'
```

**Option B: Use entity cache more aggressively**
```typescript
// Cache entity state after first reconstruction
private entityStateCache: Map<string, { entity: Entity; walSeq: number }> = new Map()

async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
  const cacheKey = `${ns}/${id}`
  const cached = this.entityStateCache.get(cacheKey)

  if (cached) {
    // Only read events AFTER cached sequence
    const newEvents = await this.getEventsAfterSeq(ns, cached.walSeq)
    // Apply only new events to cached state
    return this.applyNewEvents(cached.entity, newEvents)
  }

  // Full reconstruction only on cache miss
  return this.reconstructFromAllEvents(ns, id)
}
```

**Option C: Maintain entity state table as secondary index**
This trades read costs for write costs but may be net positive for read-heavy workloads:
```typescript
// On flush, write entity state to SQLite for fast lookup
// Only keep most recent version, not full history
CREATE TABLE entity_snapshots (
  ns TEXT NOT NULL,
  id TEXT NOT NULL,
  state BLOB NOT NULL,  -- JSON entity state
  version INTEGER NOT NULL,
  wal_seq INTEGER NOT NULL,  -- Sequence up to which this is valid
  PRIMARY KEY (ns, id)
);

// Read path: snapshot + events after snapshot
async getEntity(ns: string, id: string): Promise<Entity | null> {
  // 1. Read snapshot (1 row)
  const snapshot = this.getSnapshot(ns, id)

  // 2. Read only events after snapshot's wal_seq (few rows)
  const newEvents = this.getEventsAfterSeq(ns, snapshot?.walSeq ?? 0)

  return this.applyEvents(snapshot?.state, newEvents)
}
```

**Estimated Savings**: 10-100x fewer row reads for entity lookups

---

#### 1.3 Counter Initialization Queries Two Tables

**Location**: `src/worker/ParqueDBDO.ts` lines 438-469

**Current Behavior**:
```typescript
private async initializeCounters(): Promise<void> {
  // Query 1: events_wal
  const rows = [...this.sql.exec<CounterRow>(
    `SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`
  )]

  // Query 2: rels_wal
  const relRows = [...this.sql.exec<CounterRow>(
    `SELECT ns, MAX(last_seq) as max_seq FROM rels_wal GROUP BY ns`
  )]
}
```

**Problem**: Two separate queries on every DO initialization.

**Recommended Solution**: Combine into single UNION query
```typescript
private async initializeCounters(): Promise<void> {
  interface CounterRow {
    source: string  // 'entity' or 'rel'
    ns: string
    max_seq: number
  }

  const rows = [...this.sql.exec<CounterRow>(`
    SELECT 'entity' as source, ns, MAX(last_seq) as max_seq
    FROM events_wal GROUP BY ns
    UNION ALL
    SELECT 'rel' as source, ns, MAX(last_seq) as max_seq
    FROM rels_wal GROUP BY ns
  `)]

  for (const row of rows) {
    const key = row.source === 'rel' ? `rel:${row.ns}` : row.ns
    this.counters.set(key, row.max_seq + 1)
  }
}
```

**Estimated Savings**: 1 query instead of 2 per DO cold start

---

### Category 2: Write Operations

#### 2.1 Relationships Table Still Uses Per-Row INSERT

**Location**: `src/worker/ParqueDBDO.ts` lines 1149-1154

**Current Behavior**:
```typescript
// Each link() call does an INSERT
this.sql.exec(
  `INSERT INTO relationships
   (from_ns, from_id, predicate, to_ns, to_id, reverse, version, ...)
   VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
  fromNs, fromEntityId, predicate, toNs, toEntityId, ...
)
```

**Problem**:
- Entity creation with N relationships = N SQLite writes
- Relationships table retains per-row storage unlike events

**Recommended Solutions**:

**Option A: Remove relationships table entirely (pure event sourcing)**

Since `rels_wal` already stores relationship events, the relationships table is redundant. Relationship state can be reconstructed from events like entity state.

```typescript
// Remove: relationships table and all INSERTs to it
// Keep: rels_wal for event storage

async getRelationships(ns: string, id: string): Promise<Relationship[]> {
  // Reconstruct from rels_wal events
  const events = await this.readUnflushedRelEvents(ns)
  const relMap = new Map<string, Relationship>()

  for (const event of events) {
    if (event.target.includes(`:${id}:`)) {
      const key = event.target
      if (event.op === 'CREATE') {
        relMap.set(key, this.eventToRelationship(event))
      } else if (event.op === 'DELETE') {
        relMap.delete(key)
      }
    }
  }

  return [...relMap.values()]
}
```

**Cost Impact**: Eliminates 1 SQLite row per relationship create/update/delete

**Option B: Batch relationship writes**

If the relationships table is needed for query efficiency, batch writes:

```typescript
// Buffer relationship operations
private pendingRelWrites: Array<{
  type: 'insert' | 'update' | 'delete'
  rel: StoredRelationship
}> = []

async link(...): Promise<void> {
  // Buffer instead of immediate write
  this.pendingRelWrites.push({ type: 'insert', rel: { ... } })

  // Flush on threshold or alarm
  if (this.pendingRelWrites.length >= 100) {
    await this.flushPendingRelWrites()
  }
}

private async flushPendingRelWrites(): Promise<void> {
  if (this.pendingRelWrites.length === 0) return

  // Use INSERT ... VALUES (...), (...), (...) for batch insert
  const inserts = this.pendingRelWrites.filter(p => p.type === 'insert')
  if (inserts.length > 0) {
    const placeholders = inserts.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')
    const values = inserts.flatMap(i => [
      i.rel.from_ns, i.rel.from_id, i.rel.predicate, ...
    ])
    this.sql.exec(
      `INSERT INTO relationships (...) VALUES ${placeholders}`,
      ...values
    )
  }

  this.pendingRelWrites = []
}
```

**Estimated Savings**: N relationship rows -> 1 batch row (for writes)

---

#### 2.2 Soft Delete Updates Individual Rows

**Location**: `src/worker/ParqueDBDO.ts` lines 1049-1054, 1213-1218

**Current Behavior**:
```typescript
// delete() updates relationships one at a time
this.sql.exec(
  `UPDATE relationships SET deleted_at = ?, deleted_by = ?
   WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)`,
  now, actor, ns, id, ns, id
)
```

**Issue**: While this is technically one query, it updates multiple rows which may incur per-row billing.

**Recommended Solution**: If billing is per-row-affected, consider:
1. Deferred soft deletes via event log only
2. Batch delete operations
3. Hard delete with event log for audit trail

---

#### 2.3 Entity Cache Exists but Unused in Create Path

**Location**: `src/worker/ParqueDBDO.ts`

**Current Behavior**: The `entityCache` exists but `create()` doesn't populate it.

```typescript
// In create():
return this.toEntity({...})  // Returns entity but doesn't cache it

// Later in get():
const cached = this.entityCache.get(cacheKey)  // Cache miss!
const entityFromEvents = await this.getEntityFromEvents(ns, id)  // Full reconstruction
```

**Recommended Solution**:
```typescript
async create(...): Promise<Entity> {
  // ... create entity ...

  const entity = this.toEntity({...})

  // Cache the newly created entity
  this.cacheEntity(`${ns}/${id}`, entity)

  return entity
}
```

**Estimated Savings**: Avoids full event reconstruction on immediate read-after-write

---

### Category 3: Structural Improvements

#### 3.1 Consider Consolidated Event Tracking

**Current State**: Three separate mechanisms track events:
1. `eventBuffer` - in-memory pending events (legacy)
2. `event_batches` - SQLite batched events (legacy)
3. `events_wal` - namespace-based WAL batches (new)

**Recommendation**: Complete migration to `events_wal` only, removing legacy tables.

```typescript
// Phase 1: Stop writing to event_batches (done)
// Phase 2: Migrate getUnflushedEventCount to use events_wal
// Phase 3: Drop event_batches table after migration period
```

**Estimated Savings**: Simplified queries, fewer tables to scan

---

#### 3.2 Persist Counters to Avoid Cold Start Queries

**Current Behavior**: Counters are initialized by querying WAL tables on every DO instantiation.

**Recommended Solution**:
```typescript
// Store counters in a single row
CREATE TABLE counters (
  key TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);

// On counter increment:
private countersDirty = false

private getNextId(ns: string): string {
  const seq = this.counters.get(ns) || 1
  this.counters.set(ns, seq + 1)
  this.countersDirty = true
  return sqids.encode([seq])
}

// Periodic flush (e.g., on alarm or threshold)
private async persistCounters(): Promise<void> {
  if (!this.countersDirty) return

  // Single bulk write of all counters
  for (const [key, value] of this.counters) {
    this.sql.exec(
      `INSERT OR REPLACE INTO counters (key, value) VALUES (?, ?)`,
      key, value
    )
  }
  this.countersDirty = false
}

// On initialization: read from counters table (fast)
// Only fall back to WAL scan if counters table empty
```

**Estimated Savings**: O(1) counter load vs O(namespaces) WAL scans

---

## Summary: Prioritized Recommendations

| Priority | Optimization | Estimated Savings | Complexity |
|----------|-------------|-------------------|------------|
| **P0** | In-memory unflushed count tracking | 1 read/write | Low |
| **P0** | Cache entities after create/update | 1-100 reads/entity lookup | Low |
| **P1** | Combine counter initialization queries | 1 read/cold start | Low |
| **P1** | Remove relationships table (pure event sourcing) | 1 write/relationship | Medium |
| **P2** | Entity state snapshots for fast lookup | 10-100x read reduction | Medium |
| **P2** | Persist counters table | 2+ reads/cold start | Low |
| **P3** | Remove legacy event tracking (event_batches) | Simplified queries | High (migration) |

## Implementation Checklist

- [ ] P0: Track unflushed event count in memory
- [ ] P0: Cache entities after create/update operations
- [ ] P1: Combine counter initialization into single UNION query
- [ ] P1: Evaluate removing relationships table (migrate to pure event sourcing)
- [ ] P2: Implement entity state snapshots
- [ ] P2: Implement counters persistence table
- [ ] P3: Complete migration away from event_batches table

## Related Documents

- [DO WAL Rewrite Architecture](./DO_WAL_REWRITE.md)
- [Entity Storage Architecture](./entity-storage.md)
- [Consistency Model](./consistency.md)
