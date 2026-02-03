---
title: Architecture Overview
description: Technical architecture of ParqueDB covering storage backends, runtime environments, and data flow.
---

# ParqueDB Architecture

ParqueDB is a hybrid document/graph database built on Apache Parquet. This document describes the technical architecture as implemented.

## System Overview

```
                              ParqueDB System Architecture
+------------------------------------------------------------------------------+
|                                  Application                                   |
+------------------------------------------------------------------------------+
                                      |
                    +----------------+----------------+
                    |                                 |
              Node.js/Testing                  Cloudflare Workers
                    |                                 |
        +-----------+-----------+         +----------+----------+
        |     ParqueDBImpl      |         |   ParqueDBWorker    |
        +-----------+-----------+         +----------+----------+
                    |                          |           |
        +-----------+-----------+         +----+----+ +----+----+
        |  globalEntityStore    |         | Reads   | | Writes  |
        |  (WeakMap in-memory)  |         +----+----+ +----+----+
        +-----------+-----------+              |           |
                    |                     +----+----+ +----+----+
        +-----------+-----------+         | ReadPath| |ParqueDBDO|
        |   StorageBackend      |         +----+----+ +----+----+
        |  (MemoryBackend, Fs)  |              |           |
        +-----------------------+         +----+----+ +----+----+
                                          | R2+Cache| | SQLite  |
                                          +---------+ +---------+
```

## Runtime Environments

ParqueDB operates differently depending on the runtime environment. Understanding this dual architecture is critical.

### Node.js / Testing Environment

In Node.js (local development, testing, CLI), ParqueDB uses an in-memory store backed by a storage backend:

**Source of Truth**: `globalEntityStore` (WeakMap keyed by StorageBackend instance)

```typescript
// src/ParqueDB/store.ts - Global state management
const globalEntityStore = new WeakMap<StorageBackend, Map<string, Entity>>()
const globalEventStore = new WeakMap<StorageBackend, Event[]>()

export function getEntityStore(backend: StorageBackend): Map<string, Entity> {
  if (!globalEntityStore.has(backend)) {
    globalEntityStore.set(backend, new Map())
  }
  return globalEntityStore.get(backend)!
}
```

**Data Flow**:
1. CRUD operations modify `globalEntityStore` in-memory
2. Events are buffered in `globalEventStore`
3. Periodic flush writes events to Parquet files via StorageBackend
4. Snapshots enable fast time-travel queries

**Key Implementation**: `src/ParqueDB/core.ts` - `ParqueDBImpl` class

```typescript
export class ParqueDBImpl {
  private storage: StorageBackend
  private entities: Map<string, Entity>  // From globalEntityStore
  private events: Event[]                 // From globalEventStore

  async create<T>(namespace: string, data: CreateInput<T>): Promise<Entity<T>> {
    // 1. Generate ID, apply defaults
    // 2. Store in entities Map (in-memory)
    // 3. Append CREATE event
    // 4. Return entity
  }
}
```

### Cloudflare Workers Environment

In Workers, ParqueDB uses CQRS (Command Query Responsibility Segregation):

**Writes**: Routed through `ParqueDBDO` (Durable Object) for consistency
**Reads**: Direct to R2 via `QueryExecutor` for performance

```
         Worker Request
               |
       +-------+-------+
       |               |
   GET/find()     POST/create()
       |               |
+------+------+  +-----+------+
| QueryExecutor|  | ParqueDBDO |
+------+------+  +-----+------+
       |               |
+------+------+  +-----+------+
| ReadPath    |  | SQLite WAL |
| (Cache+R2)  |  +-----+------+
+-------------+        |
                 Flush to R2
```

**Source of Truth**:
- Writes: SQLite in Durable Object (transactional, single-writer)
- Reads: R2 Parquet files (eventually consistent via cache)

**Key Implementation**: `src/worker/ParqueDBDO.ts`

```typescript
export class ParqueDBDO extends DurableObject<Env> {
  private sql: SqlStorage
  private eventBuffer: Event[] = []
  private entityCache: Map<string, Entity> = new Map()

  async create(ns: string, data: CreateInput): Promise<Entity> {
    // 1. Generate ID with Sqids (short, sequential)
    // 2. Write to SQLite entities table (or skip if WAL-only mode)
    // 3. Buffer CREATE event
    // 4. Schedule flush to R2
    return entity
  }

  async get(ns: string, id: string): Promise<Entity | null> {
    // 1. Check entityCache (LRU)
    // 2. Reconstruct from events_wal
    // 3. Fall back to SQLite table
  }
}
```

## Storage Backends

ParqueDB uses pluggable storage backends implementing the `StorageBackend` interface:

```typescript
// src/types/storage.ts
export interface StorageBackend {
  readonly type: string

  // Read operations
  read(path: string): Promise<Uint8Array>
  readRange(path: string, start: number, end: number): Promise<Uint8Array>
  exists(path: string): Promise<boolean>
  stat(path: string): Promise<FileStat | null>
  list(prefix: string, options?: ListOptions): Promise<ListResult>

  // Write operations
  write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  append(path: string, data: Uint8Array): Promise<void>
  delete(path: string): Promise<boolean>
  deletePrefix(prefix: string): Promise<number>

  // Conditional operations
  writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<WriteResult>
}
```

### Available Backends

| Backend | Location | Use Case |
|---------|----------|----------|
| `MemoryBackend` | `src/storage/MemoryBackend.ts` | Unit tests |
| `FsBackend` | `src/storage/FsBackend.ts` | Node.js local development |
| `FsxBackend` | `src/storage/FsxBackend.ts` | Cross-platform via fsx |
| `R2Backend` | `src/storage/R2Backend.ts` | Cloudflare Workers production |

### Entity Backends (Table Formats)

For entity storage, ParqueDB supports multiple table formats via `EntityBackend`:

```typescript
// src/backends/types.ts
export type BackendType = 'native' | 'iceberg' | 'delta'

export interface EntityBackend {
  readonly type: BackendType
  readonly supportsTimeTravel: boolean
  readonly supportsSchemaEvolution: boolean

  get<T>(ns: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
  find<T>(ns: string, filter?: Filter, options?: FindOptions): Promise<Entity<T>[]>
  create<T>(ns: string, input: CreateInput<T>): Promise<Entity<T>>
  update<T>(ns: string, id: string, update: Update): Promise<Entity<T>>
  delete(ns: string, id: string): Promise<DeleteResult>
}
```

**Supported Formats**:
- **Iceberg**: Apache Iceberg format (DuckDB, Spark, Snowflake compatible)
- **Delta**: Delta Lake format
- **Native**: Simple Parquet format (planned)

## Read Path Architecture

The read path optimizes for latency using caching layers:

```typescript
// src/worker/ReadPath.ts
export class ReadPath {
  constructor(
    private bucket: R2Bucket,
    private cache: Cache,
    private config: CacheConfig
  ) {}

  async readParquet(path: string, options: ReadOptions = {}): Promise<Uint8Array> {
    // 1. Check Cache API (unless skipCache)
    if (!options.skipCache) {
      const cached = await this.cache.match(cacheKey)
      if (cached) {
        // Stale-while-revalidate if near expiry
        if (this.shouldRevalidate(cached)) {
          this.revalidateInBackground(path, cacheKey, ttl)
        }
        return new Uint8Array(await cached.arrayBuffer())
      }
    }

    // 2. Fetch from R2
    const obj = await this.bucket.get(path)

    // 3. Cache for future reads
    await this.cacheResponse(cacheKey, data, obj.etag, ttl)

    return new Uint8Array(data)
  }

  // Range requests for Parquet partial reads (row groups, metadata)
  async readRange(path: string, start: number, end: number): Promise<Uint8Array>
  async readParquetFooter(path: string): Promise<Uint8Array>
  async readParquetMetadata(path: string, metadataLength: number): Promise<Uint8Array>
}
```

### Query Execution

The `QueryExecutor` handles query execution with optimization:

```typescript
// src/worker/QueryExecutor.ts
export class QueryExecutor {
  private dataCache = new Map<string, unknown[]>()      // Parsed parquet data
  private metadataCache = new Map<string, ParquetMetadata>()
  private indexCache: IndexCache | null = null

  async find<T>(ns: string, filter: Filter, options: FindOptions): Promise<FindResult<T>> {
    // 1. Check in-memory data cache
    const cached = this.dataCache.get(path)
    if (cached) { /* return filtered cached data */ }

    // 2. Try secondary index (FTS, hash)
    if (this.indexCache) {
      const indexResult = await this.executeWithIndex(ns, path, filter, options)
      if (indexResult) return indexResult
    }

    // 3. Extract pushdown filters ($id, $index_* columns)
    const pushdownFilter = this.extractPushdownFilter(filter)

    // 4. Read with predicate pushdown (hyparquet)
    if (pushdownFilter) {
      rows = await parquetQuery({ file: asyncBuffer, filter: pushdownFilter, ... })
    } else {
      rows = await this.parquetReader.read(path)
    }

    // 5. Apply remaining MongoDB-style filters
    // 6. Post-process: sort, skip, limit, project
  }
}
```

## Write Path Architecture

### WAL-Based Event Sourcing

Writes are event-sourced with Write-Ahead Log (WAL) batching:

```
Write Request
     |
     v
+----+----+
| create()|
+----+----+
     |
     v
+----+-----+
| appendEvent() |
+----+-----+
     |
     v
+----+-----+          +------------+
| eventBuffer | ---> | events_wal |  (SQLite, batched)
+------------+       +------------+
                           |
                      Alarm/Flush
                           |
                           v
                    +-----------+
                    | R2 Parquet|
                    +-----------+
```

**Key Constants** (from `ParqueDBDO.ts`):
- `EVENT_BATCH_COUNT_THRESHOLD = 100` - Flush after 100 events
- `EVENT_BATCH_SIZE_THRESHOLD = 64KB` - Flush after 64KB
- `BULK_THRESHOLD = 5` - 5+ entities bypass SQLite, write directly to R2

### Bulk Write Optimization

For bulk creates (5+ entities), writes bypass SQLite and go directly to R2:

```typescript
async createMany(ns: string, items: CreateInput[]): Promise<Entity[]> {
  if (items.length >= BULK_THRESHOLD) {
    return this.bulkWriteToR2(ns, items, options)  // Direct R2 write
  }
  // Small batches use WAL
  return Promise.all(items.map(item => this.create(ns, item)))
}

private async bulkWriteToR2(ns: string, items: CreateInput[]): Promise<Entity[]> {
  // 1. Reserve sequence numbers
  // 2. Build Parquet row data
  // 3. Write to data/{ns}/pending/{id}.parquet
  // 4. Record in pending_row_groups table
  // 5. Append CREATE events for each
}
```

## File Layout

```
/warehouse/
├── data/{ns}/
│   ├── data.parquet           # Main entity data
│   └── pending/               # Bulk write pending files
│       └── {ulid}.parquet
├── rels/
│   ├── forward/{ns}.parquet   # Outbound relationships
│   └── reverse/{ns}.parquet   # Inbound relationships
├── events/
│   ├── current.parquet        # Active event log
│   └── archive/               # Archived events by date
│       └── {YYYY-MM-DD}/
├── indexes/
│   ├── fts/{ns}/              # Full-text search
│   ├── vector/{ns}.{field}    # Vector similarity
│   ├── secondary/{ns}.{name}.idx.parquet
│   └── bloom/{ns}.bloom       # Bloom filters
└── _meta/
    ├── manifest.json
    └── schema.json
```

## Collection Access

ParqueDB provides both explicit and proxy-based collection access:

```typescript
// Explicit access
const posts = db.collection('posts')
await posts.find({ status: 'published' })

// Proxy-based access (via Proxy in ParqueDBImpl)
await db.Posts.find({ status: 'published' })

// The Collection class delegates to ParqueDBImpl methods
export class CollectionImpl<T> implements Collection<T> {
  constructor(private db: ParqueDBMethods, public readonly namespace: string) {}

  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    return this.db.find<T>(this.namespace, filter, options)
  }
}
```

## Consistency Model

| Environment | Writes | Reads |
|-------------|--------|-------|
| Node.js | Immediate (in-memory) | Immediate (in-memory) |
| Workers | Strong (DO single-writer) | Eventual (cached R2) |

**Workers Read Consistency Options**:
```typescript
// Default: eventual consistency (cached)
const data = await readPath.readParquet(path)

// Skip cache for fresh data
const fresh = await readPath.readParquet(path, { skipCache: true })

// Read through DO for strong consistency (higher latency)
const entity = await doStub.get(ns, id)
```

## ID Generation

ParqueDB uses Sqids for short, human-friendly IDs:

```typescript
// src/worker/ParqueDBDO.ts
import Sqids from 'sqids'

const sqids = new Sqids()

private getNextId(ns: string): string {
  const seq = this.counters.get(ns) || 1
  this.counters.set(ns, seq + 1)
  return sqids.encode([seq])  // e.g., "Uk", "Ul", "Um"
}
```

Entity IDs are namespaced: `{namespace}/{id}` (e.g., `posts/Uk`)

## Entity State Reconstruction

In WAL-only mode, entity state is derived from events:

```typescript
async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
  const target = `${ns}:${id}`
  let entity: Entity | null = null

  // 1. Read from event_batches (SQLite)
  // 2. Read from in-memory eventBuffer
  // 3. Read from events_wal table
  // 4. Read from namespace event buffers

  for (const event of allEvents) {
    if (event.target === target) {
      entity = this.applyEventToEntity(entity, event, ns, id)
    }
  }

  return entity
}

private applyEventToEntity(current: Entity | null, event: Event): Entity | null {
  switch (event.op) {
    case 'CREATE': return { ...event.after, $id, version: 1 }
    case 'UPDATE': return { ...current, ...event.after, version: current.version + 1 }
    case 'DELETE': return { ...current, deletedAt: new Date(event.ts) }
  }
}
```

## Performance Considerations

### Caching Layers

1. **In-Memory Data Cache** (QueryExecutor): Parsed Parquet data, per-isolate
2. **Edge Cache** (Cache API): Raw bytes, shared across edge
3. **CDN Cache** (r2.dev): Optional edge caching via CDN bucket
4. **R2**: Persistent storage with automatic regional caching

### Predicate Pushdown

Filters on `$id` and `$index_*` columns leverage Parquet row group statistics:

```typescript
private extractPushdownFilter(filter: Filter): Filter | null {
  const pushdownFilter: Filter = {}

  for (const [key, value] of Object.entries(filter)) {
    if (key === '$id' || key.startsWith('$index_')) {
      pushdownFilter[key] = value  // Can use parquet statistics
    }
  }

  return Object.keys(pushdownFilter).length > 0 ? pushdownFilter : null
}
```

### Index Selection

The IndexCache selects the best index for a query:

```typescript
const selected = await this.indexCache.selectIndex(datasetId, filter)
// Returns: { entry: IndexCatalogEntry, type: 'fts' | 'hash', condition: ... }

// Only use index if selectivity > 50% (reduces scan by half)
if (candidateDocIds.length > totalRows * 0.5) {
  return null  // Fall back to full scan
}
```

## Related Documentation

- [ParqueDB API](./parquedb.md) - Main API reference
- [Collection API](./collection.md) - Collection methods
- [Query API](/docs/queries.md) - Filter operators
- [Update Operators](/docs/updates.md) - Update operations
