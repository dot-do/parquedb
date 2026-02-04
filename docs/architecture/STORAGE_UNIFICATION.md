---
title: Storage Architecture Unification Plan
description: Plan for unifying Node.js (ParqueDBImpl) and Workers (ParqueDBDO) storage behavior
issue: parquedb-5eh7
---

## Executive Summary

ParqueDB currently has two divergent implementations for entity storage:

1. **Node.js (ParqueDBImpl)**: Uses `globalEntityStore` (in-memory Map) with event log buffering
2. **Workers (ParqueDBDO)**: Uses SQLite WAL tables with event-sourcing semantics

An `EventSourcedBackend` abstraction exists but is not yet integrated into both implementations. This document outlines the differences and a plan for unification.

## Current Implementation Analysis

### Node.js: ParqueDBImpl (`src/ParqueDB/core.ts`)

**Source of Truth**: `globalEntityStore` (in-memory Map keyed by StorageBackend)

**Storage Pattern**:
```typescript
// From src/ParqueDB/store.ts
const globalEntityStore = new WeakMap<StorageBackend, Map<string, Entity>>()
const globalEventStore = new WeakMap<StorageBackend, Event[]>()
```

**Key Characteristics**:
- Entities stored directly in memory as `Map<fullId, Entity>`
- Events buffered in `Event[]` array
- Event log periodically flushed to Parquet via StorageBackend
- Relationship index maintained in `globalReverseRelIndex`
- Reconstruction cache for time-travel queries

**Write Path**:
1. Validate entity data
2. Store in `globalEntityStore` immediately
3. Record event in pending buffer
4. Index relationships in `globalReverseRelIndex`
5. Flush events asynchronously

**Read Path**:
1. Look up entity in `globalEntityStore`
2. Apply hydration/relationships if requested
3. For time-travel: reconstruct from events using `reconstructEntityAtTime()`

### Workers: ParqueDBDO (`src/worker/ParqueDBDO.ts`)

**Source of Truth**: SQLite `events_wal` table (event sourcing)

**Storage Pattern**:
```typescript
// Events stored in WAL batches
interface EventBuffer {
  events: Event[]
  firstSeq: number
  lastSeq: number
  sizeBytes: number
}

// In-memory entity cache for fast lookups
private entityCache: Map<string, { entity: Entity; version: number }>
```

**Key Characteristics**:
- Events are the single source of truth (true event sourcing)
- Entity state derived by replaying events via `getEntityFromEvents()`
- SQLite WAL batching for efficient writes
- In-memory LRU cache for reconstructed entities
- Relationships stored in SQLite `relationships` table

**Write Path**:
1. Get current entity state from events (via `getEntityFromEvents()`)
2. Apply update operators to derive new state
3. Append event to namespace buffer
4. Flush buffer to `events_wal` when threshold reached
5. Invalidate entity cache
6. Signal cache invalidation to Workers

**Read Path**:
1. Check in-memory entity cache
2. If miss: reconstruct from `events_wal` + in-memory buffer
3. Cache result for future lookups

## Key Differences

| Aspect | ParqueDBImpl (Node.js) | ParqueDBDO (Workers) |
|--------|------------------------|----------------------|
| **Source of Truth** | In-memory entity Map | Event WAL (true event sourcing) |
| **Entity Storage** | Direct in globalEntityStore | Derived from events |
| **Event Buffering** | Global Event[] array | Per-namespace WAL buffers |
| **Relationship Storage** | In-memory index (globalReverseRelIndex) | SQLite relationships table |
| **Time Travel** | Reconstruct from event log | Replay from event batches |
| **Caching** | Global reconstruction cache | LRU entity cache |
| **Flush Mechanism** | Async event log flush | WAL batch flush + alarm |

## Existing EventSourcedBackend (`src/storage/EventSourcedBackend.ts`)

The `EventSourcedBackend` class provides the unified abstraction we need:

```typescript
interface EventSourcedOperations {
  appendEvent(event: Event): Promise<void>
  getEntityEvents(ns: string, id: string, afterSeq?: number): Promise<Event[]>
  getLatestSnapshot(ns: string, id: string): Promise<EntitySnapshot | null>
  createSnapshot(ns: string, id: string, state: Entity, seq: number): Promise<void>
  flush(): Promise<void>
  getSequence(ns: string): number
  nextSequence(ns: string): number
}
```

**Key Methods**:
- `reconstructEntity(ns, id)`: Derives entity state from snapshot + events
- `appendEvent(event)`: Buffers events with auto-flush
- `flush()`: Persists buffered events to storage

## Unification Plan

### Phase 1: API Contract Alignment (Immediate)

Ensure both implementations expose the same public API:

```typescript
interface ParqueDBCore {
  // CRUD
  create<T>(ns: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  get<T>(ns: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
  update<T>(ns: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  find<T>(ns: string, filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  // Relationships
  link(fromId: string, predicate: string, toId: string, options?: LinkOptions): Promise<void>
  unlink(fromId: string, predicate: string, toId: string, options?: LinkOptions): Promise<void>
  getRelationships(ns: string, id: string, predicate?: string, direction?: 'outbound' | 'inbound'): Promise<Relationship[]>

  // History/Time Travel
  getHistory(ns: string, id: string, options?: HistoryOptions): Promise<HistoryResult>
  getAtVersion<T>(ns: string, id: string, version: number): Promise<Entity<T> | null>

  // Lifecycle
  flush(): Promise<void>
  dispose(): void | Promise<void>
}
```

**Action Items**:
1. [ ] Audit both implementations for API parity
2. [ ] Add missing methods to ParqueDBDO (e.g., `getHistory`, `getAtVersion`)
3. [ ] Align return types (e.g., DeleteResult)

### Phase 2: Integrate EventSourcedBackend into ParqueDBImpl

Replace `globalEntityStore` pattern with `EventSourcedBackend`:

```typescript
// Before
export class ParqueDBImpl {
  private entities: Map<string, Entity>
  private events: Event[]
  // ...

  constructor(config: ParqueDBConfig) {
    this.entities = getEntityStore(config.storage)
    this.events = getEventStore(config.storage)
  }
}

// After
export class ParqueDBImpl {
  private eventSourced: EventSourcedBackend

  constructor(config: ParqueDBConfig) {
    this.eventSourced = new EventSourcedBackend(config.storage, {
      maxBufferedEvents: config.eventLogConfig?.batchSize ?? 100,
      autoSnapshotThreshold: config.snapshotConfig?.threshold ?? 100,
    })
  }

  async get(ns: string, id: string): Promise<Entity | null> {
    return this.eventSourced.reconstructEntity(ns, id)
  }

  async create(ns: string, data: CreateInput): Promise<Entity> {
    const entity = this.buildEntity(ns, data)
    await this.eventSourced.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: entityTarget(ns, entity.$id.split('/')[1]),
      after: entity,
    })
    return entity
  }
}
```

**Benefits**:
- Same event-sourcing semantics as Workers
- Entity state derived from events (not stored directly)
- Automatic snapshots for performance
- Consistent behavior across environments

### Phase 3: Unified Relationship Storage

Create a `RelationshipStore` abstraction that both can use:

```typescript
interface RelationshipStore {
  link(from: EntityId, predicate: string, to: EntityId, options?: LinkOptions): Promise<void>
  unlink(from: EntityId, predicate: string, to: EntityId): Promise<void>
  getOutbound(from: EntityId, predicate?: string): Promise<Relationship[]>
  getInbound(to: EntityId, predicate?: string): Promise<Relationship[]>

  // Event-sourced: relationships are events too
  appendRelEvent(event: Event): Promise<void>
}

// Node.js implementation
class InMemoryRelationshipStore implements RelationshipStore {
  // Uses Map similar to current globalReverseRelIndex
}

// Workers implementation
class SQLiteRelationshipStore implements RelationshipStore {
  // Uses existing relationships table
}
```

### Phase 4: Unified Event Serialization

Both implementations should use the same event format:

```typescript
interface Event {
  id: string           // ULID or Sqids-based
  ts: number           // Unix timestamp
  op: 'CREATE' | 'UPDATE' | 'DELETE' | 'REL_CREATE' | 'REL_DELETE'
  target: string       // "ns:id" for entities, "entity:ns:id:pred:target:ns:id" for rels
  before?: Variant     // Previous state (for UPDATE/DELETE)
  after?: Variant      // New state (for CREATE/UPDATE)
  actor?: string       // Who performed the action
  metadata?: Record<string, unknown>
}
```

### Phase 5: Backend-Specific Optimizations

Allow each environment to optimize while maintaining the same semantics:

```typescript
// Workers: Use SQLite WAL for durability
class DOEventSourcedBackend extends EventSourcedBackend {
  protected async flushNamespace(ns: string): Promise<void> {
    // Write to SQLite events_wal instead of storage backend
  }
}

// Node.js: Use storage backend directly
class NodeEventSourcedBackend extends EventSourcedBackend {
  // Uses base implementation with StorageBackend
}
```

## Migration Strategy

### Step 1: Feature Flag for New Implementation

```typescript
const config: ParqueDBConfig = {
  storage: new MemoryBackend(),
  useUnifiedEventSourcing: true,  // Feature flag
}
```

### Step 2: Parallel Testing

Run both implementations in tests and compare results:

```typescript
describe('Storage Unification', () => {
  it('produces identical results for CRUD operations', async () => {
    const legacy = new ParqueDBImpl({ storage, useUnifiedEventSourcing: false })
    const unified = new ParqueDBImpl({ storage, useUnifiedEventSourcing: true })

    const legacyEntity = await legacy.create('users', data)
    const unifiedEntity = await unified.create('users', data)

    // Compare results
    expect(unifiedEntity).toMatchObject(legacyEntity)
  })
})
```

### Step 3: Gradual Rollout

1. Enable for new namespaces
2. Migrate existing namespaces one at a time
3. Remove legacy code path

## Testing Requirements

### Unit Tests

1. `EventSourcedBackend` entity reconstruction accuracy
2. Event replay ordering and correctness
3. Snapshot creation and loading
4. Cache invalidation behavior

### Integration Tests

1. Same operations produce same results in both environments
2. Event log compatibility (can replay Node.js events in Workers)
3. Time-travel consistency

### Performance Tests

1. Read latency comparison
2. Write throughput comparison
3. Memory usage comparison
4. Event log growth rate

## Files to Modify

### Core Changes

| File | Change |
|------|--------|
| `src/ParqueDB/core.ts` | Integrate EventSourcedBackend |
| `src/ParqueDB/store.ts` | Deprecate globalEntityStore |
| `src/storage/EventSourcedBackend.ts` | Add missing operations |
| `src/worker/ParqueDBDO.ts` | Align API with ParqueDBImpl |

### New Files

| File | Purpose |
|------|---------|
| `src/storage/RelationshipStore.ts` | Unified relationship abstraction |
| `src/ParqueDB/unified-operations.ts` | Shared operation logic |
| `tests/integration/storage-unification.test.ts` | Unification tests |

### Documentation

| File | Change |
|------|--------|
| `docs/architecture/entity-storage.md` | Update with final architecture |
| `docs/migration/storage-unification.md` | Migration guide |

## Success Criteria

1. **API Parity**: Both implementations expose identical public APIs
2. **Behavioral Consistency**: Same inputs produce same outputs
3. **Event Compatibility**: Events from one environment can be replayed in other
4. **Performance**: No regression in read/write latency
5. **Test Coverage**: All operations covered by unified tests

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: API Alignment | 1-2 days | None |
| Phase 2: Integrate EventSourcedBackend | 2-3 days | Phase 1 |
| Phase 3: Unified Relationships | 2 days | Phase 2 |
| Phase 4: Event Serialization | 1 day | Phase 2 |
| Phase 5: Backend Optimizations | 2-3 days | Phases 3-4 |
| Testing & Documentation | 2-3 days | All phases |

**Total: 10-14 days**

## Appendix: Code References

### ParqueDBImpl Entity Storage (Current)

```typescript
// src/ParqueDB/store.ts
const globalEntityStore = new WeakMap<StorageBackend, Map<string, Entity>>()

export function getEntityStore(storage: StorageBackend): Map<string, Entity> {
  let store = globalEntityStore.get(storage)
  if (!store) {
    store = new Map()
    globalEntityStore.set(storage, store)
  }
  return store
}
```

### ParqueDBDO Event Sourcing (Current)

```typescript
// src/worker/ParqueDBDO.ts
async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
  const target = `${ns}:${id}`
  let entity: Entity | null = null

  // 1. Read from events_wal (flushed batches)
  const walRows = [...this.sql.exec<WalRow>(
    `SELECT events FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`,
    ns
  )]

  for (const row of walRows) {
    const walEvents = this.deserializeEventBatch(row.events)
    for (const event of walEvents) {
      if (event.target === target) {
        entity = this.applyEventToEntity(entity, event, ns, id)
      }
    }
  }

  // 2. Read from in-memory namespace event buffer
  const nsBuffer = this.nsEventBuffers.get(ns)
  if (nsBuffer) {
    for (const event of nsBuffer.events) {
      if (event.target === target) {
        entity = this.applyEventToEntity(entity, event, ns, id)
      }
    }
  }

  return entity
}
```

### EventSourcedBackend Reconstruction (Target)

```typescript
// src/storage/EventSourcedBackend.ts
async reconstructEntity(ns: string, id: string): Promise<Entity | null> {
  // Get latest snapshot
  const snapshot = await this.getLatestSnapshot(ns, id)

  // Get events since snapshot
  const afterSeq = snapshot?.seq ?? 0
  const events = await this.getEntityEvents(ns, id, afterSeq > 0 ? afterSeq : undefined)

  // Start with snapshot state or empty
  let entity: Entity | null = snapshot?.state ?? null

  // Replay events to derive current state
  for (const event of events) {
    entity = this.applyEvent(entity, event)
  }

  return entity
}
```
