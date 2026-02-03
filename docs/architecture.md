# ParqueDB Architecture

**Version:** 0.x
**Last Updated:** 2026-02-03

## Overview

ParqueDB is a hybrid relational/document/graph database built on Apache Parquet, designed for Cloudflare Workers and edge computing. It combines the flexibility of document databases, the querying power of relational databases, and the traversal capabilities of graph databases—all while leveraging Parquet's columnar storage format for performance and cost efficiency.

### Key Principles

1. **Parquet as Foundation**: Columnar storage provides efficient compression, predicate pushdown, and analytics capabilities
2. **Edge-First Architecture**: Optimized for Cloudflare Workers with distributed reads and coordinated writes
3. **Graph-First Design**: Relationships are first-class citizens with bidirectional indexes
4. **Time-Travel Built-In**: Change Data Capture (CDC) event log enables point-in-time queries
5. **Schema Flexibility**: Support both typed (schema-defined) and flexible (schema-less) collections

### Core Features

- **MongoDB-style API**: Familiar filter operators (`$eq`, `$gt`, `$in`, etc.) and update operators (`$set`, `$inc`, `$push`)
- **Bidirectional Relationships**: Native support for graph operations with forward and reverse indexes
- **RPC Pipelining**: Integration with Cap'n Proto-style RPC for efficient multi-step operations
- **Multi-Tenant**: Namespace-based isolation with optional cross-namespace references
- **Pure JavaScript**: No native dependencies, runs in Cloudflare Workers, Node.js, and browsers

## Architecture Principles

### Parquet as the Storage Foundation

ParqueDB uses Apache Parquet files as its primary storage format:

```
Parquet Benefits:
├── Columnar Storage
│   ├── Efficient compression (often 10x vs row-based)
│   ├── Column-level encoding (dictionary, run-length, delta)
│   └── Type-specific optimizations
├── Predicate Pushdown
│   ├── Row group statistics (min/max/null_count)
│   ├── Bloom filters for existence checks
│   └── Column indexes (page-level statistics)
├── Partial Reads
│   ├── Column projection (read only needed fields)
│   ├── Row group filtering (skip irrelevant data)
│   └── Page-level access
└── Cross-Platform
    ├── Standard format (Apache project)
    ├── Wide ecosystem support
    └── Pure JS implementation (hyparquet)
```

### Hybrid Relational/Document/Graph Model

ParqueDB unifies three data models:

1. **Relational**: Typed schemas with native columns for efficient filtering
2. **Document**: Variant columns for semi-structured data (JSON-like flexibility)
3. **Graph**: Separate edge storage with bidirectional indexes for traversal

This enables queries like:

```typescript
// Relational: Type-safe queries with predicate pushdown
const highZoneOccupations = await db.Occupations.find({
  jobZone: { $gte: 4 },
  socCode: { $regex: '^15-' }
})

// Document: Flexible data access
const posts = await db.Posts.find({
  'metadata.featured': true,
  'tags.0': 'javascript'
})

// Graph: Bidirectional traversal
const userPosts = await db.Users.related(userId, 'authored', 'forward')
const postComments = await db.Posts.related(postId, 'comments', 'reverse')
```

## Storage Model

### Three-File Architecture

ParqueDB organizes data into three primary file types:

```
/warehouse/
├── data/{collection}/data.parquet         # Entity storage
├── rels/forward/{collection}.parquet      # Forward relationship index
├── rels/reverse/{collection}.parquet      # Reverse relationship index
└── events/current.parquet                 # CDC event log
```

#### 1. Entity Storage (`data/`)

Stores entities with rich metadata and audit fields:

```typescript
// data/{collection}/data.parquet schema
interface Entity {
  // Identity
  $id: string              // Unique identifier (ULID)
  $type: string            // Entity type (e.g., "Person", "Post")

  // Data
  $data: Variant           // Full entity as JSON/Variant (optional)
  name?: string            // Common field for display

  // Schema fields (for typed collections)
  // ... native columns for each schema field

  // Audit fields
  createdAt: timestamp     // Creation timestamp
  createdBy: string        // Creator user/actor ID
  updatedAt: timestamp     // Last update timestamp
  updatedBy: string        // Last updater ID

  // Version control
  version: number          // Optimistic concurrency version
  deleted: boolean         // Soft delete flag
}
```

**Sort Order**: `($id)` for efficient point lookups

**Storage Modes**:

- **Typed Mode**: Schema-defined collections with native Parquet columns
- **Flexible Mode**: Schema-less collections with Variant shredding

#### 2. Relationship Storage (`rels/`)

Relationships are stored separately from entities in two indexes:

```typescript
// rels/forward/{collection}.parquet schema
interface Relationship {
  // Source (from)
  from_ns: string          // Source namespace
  from_id: string          // Source entity ID
  from_name: string        // Source entity name (denormalized)
  from_type: string        // Source entity type

  // Relationship
  predicate: string        // Relationship name (forward)
  reverse: string          // Reverse relationship name

  // Target (to)
  to_ns: string            // Target namespace
  to_id: string            // Target entity ID
  to_name: string          // Target entity name (denormalized)
  to_type: string          // Target entity type

  // Metadata
  createdAt: timestamp
  createdBy: string
  data: Variant            // Relationship properties
}
```

**Forward Index Sort**: `(from_ns, from_id, predicate)` - efficient "find all X related to Y"
**Reverse Index Sort**: `(to_ns, to_id, reverse)` - efficient "find all Y that reference X"

This dual-index design enables:
- O(log n) lookup for both forward and reverse traversal
- No need to scan entity data for graph operations
- Independent scaling of entities vs relationships

#### 3. Event Log (`events/`)

All changes are recorded in a CDC event log for time-travel and audit:

```typescript
// events/current.parquet schema
interface Event {
  // Identity
  id: string               // Event ID (ULID, time-sortable)

  // Target
  target: 'entity' | 'rel' // What changed
  ns: string               // Namespace
  entity_id: string        // Entity or relationship ID

  // Event
  ts: timestamp            // Event timestamp
  op: 'CREATE' | 'UPDATE' | 'DELETE'

  // State
  before: Variant          // Previous state (null for CREATE)
  after: Variant           // New state (null for DELETE)

  // Context
  actor: string            // Who made the change
  metadata: Variant        // Custom metadata (reason, trace_id, etc.)
}
```

**Sort Order**: `(ts, id)` - chronological replay

**Partitioning**: Daily partitions (`events/day=YYYY-MM-DD/`) for retention management

### Typed vs Flexible Storage

ParqueDB supports two storage modes per collection:

#### Typed Storage (Schema-Defined Collections)

When a collection has a schema, fields are stored as native Parquet columns:

```typescript
// Schema definition
const db = DB({
  Occupation: {
    name: 'string!',
    socCode: 'string!#',    // # = indexed
    jobZone: 'int',
    description: 'text',
  }
})

// Resulting Parquet schema
{
  $id: STRING,
  $type: STRING,
  $data: BYTE_ARRAY (optional),  // Full row as JSON
  name: STRING,
  socCode: STRING,
  jobZone: INT64,
  description: STRING,
  createdAt: TIMESTAMP_MILLIS,
  updatedAt: TIMESTAMP_MILLIS,
  version: INT64
}
```

**Benefits**:
- Native predicate pushdown on any field
- Column statistics for row group skipping
- Efficient columnar scans for aggregations
- Type-safe storage

**`$data` Column**: Optionally included (default: true) for fast full-row reads, avoiding expensive columnar reconstruction.

#### Flexible Storage (Schema-Less Collections)

Collections without a schema use Variant shredding:

```typescript
// Flexible collection
const db = DB({
  Posts: 'flexible'
})

// Resulting Parquet schema
{
  $id: STRING,
  $type: STRING,
  $data: BYTE_ARRAY,             // Full entity as Variant
  $index_*: various,             // Shredded hot fields
  createdAt: TIMESTAMP_MILLIS,
  updatedAt: TIMESTAMP_MILLIS,
  version: INT64
}
```

**Benefits**:
- No schema required
- Dynamic field addition
- Partial index shredding for frequently queried fields

### Variant Type for Semi-Structured Data

The Variant type (inspired by ClickHouse and Iceberg) enables flexible data storage:

```typescript
type Variant =
  | { type: 'null' }
  | { type: 'boolean', value: boolean }
  | { type: 'int64', value: bigint }
  | { type: 'float64', value: number }
  | { type: 'string', value: string }
  | { type: 'binary', value: Uint8Array }
  | { type: 'date', value: number }
  | { type: 'timestamp', value: bigint }
  | { type: 'array', elements: Variant[] }
  | { type: 'object', fields: Record<string, Variant> }
```

**Shredding**: Hot fields can be extracted to native columns while preserving the full Variant for completeness.

## Dual Storage Architecture

ParqueDB uses different storage strategies depending on the runtime environment:

### Node.js / Testing Environment

```
+-----------------------------------------------------+
|                  ParqueDB.ts                        |
|                                                     |
|  +----------------------------------------------+   |
|  |         globalEntityStore (WeakMap)          |   |
|  |  Map<StorageBackend, Map<$id, Entity>>       |   |
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
- Entities stored in-memory in `globalEntityStore` (per-backend WeakMap)
- Events buffered in `globalEventStore`
- Periodic flush writes events to Parquet files via StorageBackend
- Entity reconstruction uses in-memory state + event replay

**Use cases:**
- Unit tests with MemoryBackend
- Local development with FsBackend
- Server-side Node.js applications
- CLI tools

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
- **READS**: Direct to R2 via `QueryExecutor`/`ReadPath` with Cache API
- **WRITES**: Routed through `ParqueDBDO` Durable Object for consistency
- `ParqueDBDO` stores entities in SQLite (`entities` table) as WAL
- Events logged to SQLite and periodically flushed to R2 as Parquet

**Why SQLite in Durable Object?**
- Single-writer consistency (no race conditions)
- Transactional updates
- Fast lookups by ID
- Relationship indexes

**Why R2 for reads?**
- Distributed edge caching
- No DO round-trip latency for reads
- Parquet columnar format for efficient scans
- CDN-friendly immutable files

### Source of Truth Summary

| Environment | Writes Source of Truth | Reads Source of Truth |
|------------|------------------------|----------------------|
| Node.js/Testing | `globalEntityStore` (in-memory) | `globalEntityStore` + event replay |
| Workers | `ParqueDBDO` SQLite | R2 Parquet files |

## Relationship System

### Bidirectional Indexes

Relationships are indexed in both directions for efficient traversal:

```typescript
// Forward index: Find all posts authored by user X
// Query: (from_ns='users', from_id='user:123', predicate='authored')
// Index: rels/forward/posts.parquet sorted by (from_ns, from_id, predicate)

// Reverse index: Find all users who authored post Y
// Query: (to_ns='posts', to_id='post:456', reverse='authored')
// Index: rels/reverse/posts.parquet sorted by (to_ns, to_id, reverse)
```

**Performance**: O(log n + k) where k is the result count, using Parquet row group statistics for predicate pushdown.

### Named Relationships

Relationships have both forward and reverse names:

```typescript
// Creating a relationship
await db.link('users', userId, 'posts', postId, {
  predicate: 'authored',    // Forward: user -> post
  reverse: 'author'         // Reverse: post <- user
})

// Forward traversal
const posts = await db.related('users', userId, 'authored', 'forward')

// Reverse traversal
const author = await db.related('posts', postId, 'author', 'reverse')
```

### Graph Operations

**Path Traversal**: Multi-hop graph queries with cycle detection

```typescript
// Find friends of friends
const paths = await db.traverse('users', userId, {
  path: ['friends', 'friends'],
  maxDepth: 3,
  filter: { type: 'User' }
})
```

**Subgraph Extraction**: Extract connected subgraphs

```typescript
// Get user's social network (2-hop radius)
const subgraph = await db.subgraph('users', userId, {
  relationships: ['friends', 'follows'],
  maxHops: 2
})
```

### GraphDL Integration

ParqueDB supports GraphDL relationship operators natively:

```typescript
// Schema with relationship operators
const schema = Graph({
  User: {
    name: 'string',
    organization: '->Organization.members',     // Exact forward reference
    interests: ['~>Topic.interested_users']     // Fuzzy forward reference
  }
})

// Operators:
// -> : Exact forward reference
// ~> : Fuzzy forward reference (AI-matched, with confidence)
// <- : Exact backward reference
// <~ : Fuzzy backward reference
```

## Query Execution

### Filter Evaluation

ParqueDB evaluates filters at three levels:

```
Level 1: Row Group Statistics (min/max/null_count)
├── Skip entire row groups based on statistics
├── Example: jobZone >= 4 when row group max is 3
└── Cost: Negligible (metadata-only)

Level 2: Bloom Filters (existence checks)
├── Skip row groups where value definitely doesn't exist
├── Example: socCode = '15-1252' not in bloom filter
└── Cost: O(k) hash checks

Level 3: Row-Level Filtering (after decompression)
├── Evaluate filter on each matching row
├── Example: Complex nested filters on Variant data
└── Cost: O(n) where n is rows in selected row groups
```

### Predicate Pushdown

Filters on native columns map directly to Parquet predicates:

```typescript
// MongoDB-style filter
{
  status: 'active',
  jobZone: { $gte: 3 },
  createdAt: { $gt: new Date('2024-01-01') }
}

// Maps to Parquet row group filters
[
  { column: 'status', op: 'eq', value: 'active' },
  { column: 'jobZone', op: 'gte', value: 3 },
  { column: 'createdAt', op: 'gt', value: timestamp }
]
```

**Result**: Only row groups matching all predicates are read and decompressed.

### Bloom Filters

ParqueDB uses probabilistic bloom filters for efficient existence checks:

```typescript
// Three-level hierarchy
Level 1: Global (per namespace)
├── entity.bloom        # All entity IDs
├── edge.bloom          # All edge keys
└── type.bloom          # All entity types

Level 2: Per-File
├── {file}.id.bloom     # IDs in this file
├── {file}.edge.bloom   # Edges in this file
└── {file}.type.bloom   # Types in this file

Level 3: Per-Row-Group (embedded in Parquet)
└── Column bloom filters in file footer
```

**Query optimization**:
1. Check global bloom filter
2. Find candidate files via file-level blooms
3. Parquet reader uses row-group blooms automatically

**False positive rate**: Configurable, default 1% (9.6 bits per element)

### Index Usage

**Primary Index**: `$id` column (always indexed, sorted)
**Secondary Indexes**: Native columns in typed collections (automatic via row group statistics)
**Graph Indexes**: Forward and reverse relationship indexes

**Example query plan**:

```typescript
// Query: Find active occupations in job zone 4+
await db.Occupations.find({
  status: 'active',
  jobZone: { $gte: 4 }
})

// Execution plan:
// 1. Read data/occupations.parquet metadata (row group stats)
// 2. Skip row groups where:
//    - jobZone.max < 4 (predicate pushdown)
//    - status not in bloom filter (bloom filter)
// 3. Read matching row groups
// 4. Apply row-level filters
// 5. Return results
```

## Cost Optimization for Cloudflare Workers

ParqueDB is designed for cost efficiency on Cloudflare's pricing model:

### Durable Object WAL Strategy

```
Problem: DO SQLite is 4.5x more expensive than R2 per operation
Solution: Use SQLite as Write-Ahead Log (WAL), not full entity store

Current (Expensive):
├── Each entity = 1 SQLite row
├── Each event = 1 SQLite row
└── Cost scales linearly with entities

Proposed (Efficient):
├── Events batched as BLOBs (~1000 events per row)
├── Bulk writes bypass SQLite entirely
└── Cost reduced by 99%+ for bulk operations
```

### Bulk Bypass to R2

For operations involving 5+ entities, write directly to R2:

```typescript
// Threshold logic
const BULK_THRESHOLD = 5

async createMany(ns: string, data: CreateInput[]): Promise<Entity[]> {
  if (data.length < BULK_THRESHOLD) {
    // Small batch: use WAL path (via DO)
    return Promise.all(data.map(d => this.create(ns, d)))
  }

  // Large batch: stream directly to R2 as pending Parquet
  return this.bulkCreateToR2(ns, data)
}
```

**Cost comparison**:

| Operation | Current (rows) | Optimized (rows) | Savings |
|-----------|---------------|-----------------|---------|
| Create 1 entity | 2 | 0-1 | 50-100% |
| Create 100 entities | 200 | 1 | 99.5% |
| Bulk import 10K | 20,000 | 10 | 99.95% |

### SQLite Minimization

Only use SQLite for:
1. **Event batching**: Store ~1000 events per BLOB row
2. **Pending row groups**: Track unflushed bulk writes
3. **Flush coordination**: Manage background flush to R2

**Not used for**:
- Entity storage (except as transient WAL)
- Relationship storage (indexed in Parquet)
- Query execution (reads go to R2)

### Read Path Optimization

```
Caching Layers:

Layer 1: In-Memory Data Cache (QueryExecutor)
├── Parsed Parquet data
├── Per-isolate, fast
└── TTL: 60s default

Layer 2: Edge Cache (Cache API)
├── Raw Parquet bytes
├── Shared across edge
└── TTL: 60s default

Layer 3: R2 Object Storage
├── Persistent storage
└── Automatically cached by R2
```

**Stale-While-Revalidate**: Serve cached data while fetching fresh in background

## Consistency Model

ParqueDB uses CQRS (Command Query Responsibility Segregation) with different consistency guarantees for reads and writes.

### Write Path: Strong Consistency

All writes routed through Durable Objects:

```
Guarantees:
├── Single-writer semantics (one DO per namespace)
├── Transactional writes (SQLite ACID)
├── Ordered operations (serialized)
└── Optimistic concurrency (expectedVersion support)
```

### Read Path: Eventual Consistency

Reads bypass Durable Objects, going directly to R2 with caching:

```
Consistency Levels:

Cache Hit: Eventually consistent (up to TTL stale)
├── May serve data from 0-60s ago
└── Acceptable for most queries

Cache Miss: Read-after-R2-write consistent
├── Fetches latest from R2
└── R2 propagation lag: milliseconds

After Explicit Invalidation: Strongly consistent
├── Cache invalidated on writes
└── Next read fetches fresh
```

### Read-After-Write

For critical operations requiring read-after-write consistency:

```typescript
// Option 1: Skip cache
const result = await readPath.readParquet(path, { skipCache: true })

// Option 2: Read through DO (higher latency)
const entity = await doStub.get(ns, id)

// Option 3: Invalidate + read
await worker.invalidateCache(ns)
const fresh = await worker.get(ns, id)
```

### Consistency by Operation

| Operation | Consistency | Notes |
|-----------|-------------|-------|
| `create()` | Strong | Via DO, immediately visible in DO |
| `update()` | Strong | Via DO with optimistic locking |
| `delete()` | Strong | Via DO |
| `get()` | Eventual | Cached reads |
| `find()` | Eventual | Cached reads |
| `count()` | Eventual | May count stale data |

## Time-Travel and Audit

### Event-Sourced Foundation

All changes captured in event log:

```typescript
// Query entities as of a specific time
const snapshot = await db.asOf('posts', new Date('2024-01-01'))

// Get entity history
const history = await db.history('posts', postId)
// Returns: Array of states over time

// Replay events to rebuild state
const rebuilt = await db.replay('posts', {
  from: startDate,
  to: endDate
})
```

### Checkpoints for Fast Replay

Instead of replaying all events, use periodic checkpoints:

```
Time-Travel Strategy:

1. Find nearest checkpoint before target time
2. Load checkpoint snapshot
3. Replay events from checkpoint to target
4. Return materialized state

Performance:
├── Without checkpoint: O(all events)
└── With checkpoint: O(events since checkpoint)
```

### Audit Trail

Full audit trail for compliance:

```typescript
// Who changed what, when, and why
interface Event {
  id: string              // ULID (time-sortable)
  ts: timestamp
  actor: string           // User/service that made change
  target: 'entity' | 'rel'
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  before: Variant         // Previous state
  after: Variant          // New state
  metadata: Variant       // Custom (reason, trace_id, etc.)
}
```

## Multi-Tenant Architecture

### Namespace-Based Isolation

Each namespace is a self-contained unit:

```
/warehouse/
├── tenant-a/
│   ├── data.parquet        # All entities for tenant-a
│   ├── events.parquet      # All events for tenant-a
│   └── _meta.parquet       # Namespace metadata
├── tenant-b/
│   └── ...
└── _system/
    ├── namespaces.parquet  # Namespace registry
    ├── schema.parquet      # Global schema definitions
    └── refs.parquet        # Cross-namespace references
```

### Cross-Namespace Operations

**References**: Track relationships between namespaces in `_system/refs.parquet`

```typescript
// Link entities across namespaces
await db.link('tenant-a/users', userId, 'tenant-b/posts', postId, {
  predicate: 'authored',
  crossNamespace: true
})

// Cross-namespace query
const posts = await db.find('tenant-b/posts', {
  'author.ns': 'tenant-a/users',
  'author.id': userId
})
```

**Federated Queries**: Query across multiple namespaces in parallel

```typescript
// Find all active users across all tenants
const users = await db.federated.find({
  type: 'User',
  status: 'active'
}, {
  namespaces: ['tenant-a', 'tenant-b', 'tenant-c']
})
```

### Schema Management

Three schema modes:

1. **Global**: All types defined in `_system/schema.parquet`
2. **Local**: Each namespace defines its own types
3. **Hybrid**: Namespaces can use global types, extend them, or define local types

```typescript
// Global type
_system/schema: { type: 'Person', fields: ['name', 'email'] }

// Local extension
tenant-a/_schema: {
  type: 'Person',
  extends: 'global:Person',
  fields: ['employee_id', 'department']
}

// Effective schema for tenant-a/Person
{ name, email, employee_id, department }
```

## Technology Stack

### Core Dependencies

- **hyparquet** / **hyparquet-writer**: Pure JS Parquet read/write
  - No native dependencies, works in Workers
  - Streaming support for large files
  - Full Parquet 2.0 support (bloom filters, page indexes)

- **capnweb**: RPC pipelining support
  - `RpcTarget` / `RpcPromise` for efficient multi-step operations
  - Cap'n Proto-style promise pipelining

- **fsx**: Cloudflare Worker filesystem abstraction
  - Unified API for R2, local filesystem, memory
  - Enables same code across environments

- **graphdl** / **icetype**: Schema definitions
  - Type-safe schema definitions
  - GraphDL relationship operators
  - Runtime validation

### Storage Backends

ParqueDB supports pluggable storage backends:

- **FsBackend**: Node.js filesystem (local development)
- **MemoryBackend**: In-memory (testing)
- **R2Backend**: Cloudflare R2 (production Workers)
- **S3Backend**: AWS S3 (alternative cloud)
- **FsxBackend**: fsx abstraction (cross-platform)

## Performance Characteristics

### Read Performance

| Operation | Target (p50) | Target (p99) | Optimization |
|-----------|-------------|-------------|--------------|
| Get by ID | 5ms | 20ms | Primary index + bloom filters |
| Find (indexed) | 20ms | 100ms | Predicate pushdown + row group skip |
| Find (scan) | 100ms | 500ms | Columnar compression + caching |
| Count | 50ms | 200ms | Row group metadata only |
| Relationship traverse | 50ms | 200ms | Bidirectional indexes |

### Write Performance

| Operation | Target (p50) | Target (p99) | Notes |
|-----------|-------------|-------------|-------|
| Create | 10ms | 50ms | DO coordination + SQLite |
| Update | 15ms | 75ms | Optimistic locking |
| Delete | 15ms | 75ms | Soft delete by default |
| Bulk create (100) | 100ms | 500ms | Amortized, bypasses DO for 5+ |

### Storage Efficiency

```
Compression Ratios (typical):
├── Text data: 5-10x
├── Numeric data: 3-5x
├── Timestamps: 8-12x (delta encoding)
└── IDs: 2-4x (dictionary encoding)

Storage Overhead:
├── Base entity data: 1x
├── Relationship indexes: 0.3x (forward) + 0.3x (reverse)
├── Event log (7-day): 0.5x
└── Total: ~2.1x raw data
```

### Scalability Limits

- **Max entity size**: 1MB (recommended), 10MB (max)
- **Max file size**: 1GB (recommended), 5GB (max)
- **Max namespace size**: Unlimited (use sharding)
- **Concurrent readers**: Unlimited (edge-cached)
- **Concurrent writers**: 1 per namespace (DO bottleneck)

## Related Documentation

For detailed design documents, see:

- **[Entity Storage](./architecture/entity-storage.md)** - Dual storage architecture details
- **[Graph-First Architecture](./architecture/graph-first-architecture.md)** - Relationship indexing and traversal
- **[DO WAL Rewrite](./architecture/DO_WAL_REWRITE.md)** - Cost optimization strategy
- **[Typed Storage](./architecture/typed-storage.md)** - Typed vs flexible storage modes
- **[Bloom Filter Indexes](./architecture/bloom-filter-indexes.md)** - Probabilistic index design
- **[Consistency Model](./architecture/consistency.md)** - Consistency guarantees and cache behavior
- **[Namespace-Sharded Architecture](./architecture/namespace-sharded-architecture.md)** - Multi-tenant isolation

## Future Roadmap

### Short-Term (v0.x -> v1.0)

- [ ] Complete DO WAL rewrite for cost optimization
- [ ] Implement typed storage mode
- [ ] Add secondary index support (B-tree, hash)
- [ ] Full-text search integration
- [ ] Vector similarity search

### Medium-Term (v1.x)

- [ ] Horizontal namespace sharding (by type, time, hash)
- [ ] Cross-region replication
- [ ] Materialized views
- [ ] GraphQL API layer
- [ ] SQL query interface

### Long-Term (v2.x+)

- [ ] Distributed query execution
- [ ] HTAP (hybrid transactional/analytical processing)
- [ ] Machine learning integration
- [ ] Multi-region active-active

## Conclusion

ParqueDB combines the best aspects of relational, document, and graph databases while leveraging Parquet's columnar storage for efficiency. Its dual storage architecture (in-memory for Node.js, SQLite WAL + R2 for Workers) provides strong consistency for writes and high-performance eventual consistency for reads.

The graph-first design with bidirectional indexes, time-travel via CDC events, and multi-tenant namespace isolation makes ParqueDB suitable for modern applications requiring flexibility, performance, and audit capabilities—all while running efficiently on edge computing platforms like Cloudflare Workers.

---

**Document Version**: 0.x
**Last Updated**: 2026-02-03
**Status**: Living Document
