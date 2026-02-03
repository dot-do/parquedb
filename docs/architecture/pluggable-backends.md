# Pluggable Backend Architecture

## Overview

ParqueDB supports three storage backends for entity data while maintaining a unified relationship index layer on top:

1. **Native** - ParqueDB's simple, custom Parquet format (planned)
2. **Delta Lake** - Implemented directly in ParqueDB
3. **Iceberg** - Via `@dotdo/iceberg` library (including R2 Data Catalog)

The key architectural insight is that **relationships are always stored in ParqueDB's format**, regardless of the entity backend. This enables graph traversal capabilities on top of any table format.

```
┌─────────────────────────────────────────────────────────────────┐
│                      ParqueDB API Layer                         │
│   db.collection('posts').find({ author: 'alice' })             │
│   entity.related('categories')                                  │
│   entity.referencedBy('comments')                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐     ┌──────────────────────────────┐ │
│  │   Entity Backend     │     │   Relationship Index          │ │
│  │   (pluggable)        │     │   (always ParqueDB format)    │ │
│  ├──────────────────────┤     ├──────────────────────────────┤ │
│  │ • NativeBackend      │     │ • SQLite (Durable Objects)   │ │
│  │ • IcebergBackend     │     │ • Parquet (rels/forward/)    │ │
│  │ • DeltaBackend       │     │ • Parquet (rels/reverse/)    │ │
│  └──────────────────────┘     └──────────────────────────────┘ │
│           │                              │                      │
│           │  Entity CRUD                 │  Link/Unlink/Traverse│
│           ▼                              ▼                      │
├─────────────────────────────────────────────────────────────────┤
│                      Storage Layer                              │
│   R2 / S3 / Local FS / Memory                                  │
│   (with optional R2 Data Catalog for managed Iceberg)          │
└─────────────────────────────────────────────────────────────────┘
```

## Why Separate Relationship Storage?

Neither Iceberg nor Delta Lake have native relationship/graph concepts. They're designed for:
- Large analytical tables
- Columnar storage optimized for aggregations
- Schema evolution and time-travel

ParqueDB's relationship model provides:
- Bidirectional graph traversal (forward + reverse indexes)
- Sub-millisecond edge lookups
- Graph query patterns (BFS, path finding)
- IceType schema compatibility

By keeping relationships in our own format, we get:
1. **Best-in-class graph operations** on any entity backend
2. **Interoperability** - entities queryable by DuckDB/Spark via Iceberg
3. **Managed infrastructure** - R2 Data Catalog handles entity compaction
4. **Unified API** - Same MongoDB-style queries regardless of backend

## Backend Interface

```typescript
// src/backends/types.ts

export interface EntityBackend {
  // Metadata
  readonly type: 'native' | 'iceberg' | 'delta'
  readonly supportsTimeTravel: boolean
  readonly supportsSchemaEvolution: boolean
  readonly readOnly: boolean

  // Lifecycle
  initialize(): Promise<void>
  close(): Promise<void>

  // Read operations
  get<T>(ns: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
  find<T>(ns: string, filter?: Filter, options?: FindOptions): Promise<Entity<T>[]>
  count(ns: string, filter?: Filter): Promise<number>
  exists(ns: string, id: string): Promise<boolean>

  // Write operations
  create<T>(ns: string, input: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T>(ns: string, id: string, update: Update, options?: UpdateOptions): Promise<Entity<T>>
  delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult>

  // Batch operations
  bulkCreate<T>(ns: string, inputs: CreateInput<T>[], options?: CreateOptions): Promise<Entity<T>[]>
  bulkUpdate(ns: string, filter: Filter, update: Update, options?: UpdateOptions): Promise<UpdateResult>
  bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>

  // Time-travel (optional)
  snapshot?(ns: string, version: number | Date): Promise<EntityBackend>
  listSnapshots?(ns: string): Promise<SnapshotInfo[]>

  // Schema
  getSchema(ns: string): Promise<EntitySchema | null>
  setSchema?(ns: string, schema: EntitySchema): Promise<void>
  listNamespaces(): Promise<string[]>

  // Maintenance (optional)
  compact?(ns: string, options?: CompactOptions): Promise<CompactResult>
  vacuum?(ns: string, options?: VacuumOptions): Promise<VacuumResult>
  stats?(ns: string): Promise<BackendStats>
}
```

## Backend Implementations

### 1. Native Backend (Planned)

ParqueDB's original simple format:

```typescript
// src/backends/native.ts (planned)

export class NativeBackend implements EntityBackend {
  readonly type = 'native'
  readonly supportsTimeTravel = true  // via event log
  readonly supportsSchemaEvolution = false

  constructor(config: NativeBackendConfig) {}

  // Storage layout:
  // data/{ns}/data.parquet     - Entity data
  // events/current.parquet     - CDC event log
}
```

**Storage Layout:**
```
bucket/
├── data/
│   ├── users/data.parquet
│   ├── posts/data.parquet
│   └── categories/data.parquet
├── events/
│   └── current.parquet
└── rels/
    ├── forward/{ns}.parquet
    └── reverse/{ns}.parquet
```

### 2. Iceberg Backend

Standard Apache Iceberg format, compatible with DuckDB/Spark/Snowflake:

```typescript
// src/backends/iceberg.ts

import { createIcebergBackend, createR2IcebergBackend, IcebergBackend } from 'parquedb/backends'

// Basic Iceberg backend with filesystem catalog
const backend = createIcebergBackend({
  type: 'iceberg',
  storage: storageBackend,   // ParqueDB StorageBackend (R2, S3, FS, Memory)
  warehouse: 'warehouse',     // Base path for tables
  database: 'default',        // Database/namespace prefix
  readOnly: false,            // Optional: read-only mode
})

// Or instantiate the class directly
const backend = new IcebergBackend({
  type: 'iceberg',
  storage: storageBackend,
  warehouse: 'warehouse',
  database: 'mydb',
  catalog: {
    type: 'filesystem',
  },
})

// Iceberg with R2 Data Catalog (Cloudflare managed)
const r2Backend = createR2IcebergBackend(storageBackend, {
  accountId: env.CF_ACCOUNT_ID,
  apiToken: env.R2_DATA_CATALOG_TOKEN,
  bucketName: 'my-bucket',     // Optional
  warehouse: 'warehouse',
  database: 'default',
})
```

**IcebergBackend Properties:**
```typescript
class IcebergBackend implements EntityBackend {
  readonly type = 'iceberg'
  readonly supportsTimeTravel = true   // via snapshots
  readonly supportsSchemaEvolution = true
  readonly readOnly: boolean

  // Internal state
  private storage: StorageBackend
  private warehouse: string
  private database: string
  private tableCache: Map<string, TableMetadata>
}
```

**Storage Layout (Iceberg standard):**
```
bucket/warehouse/db/
├── users/
│   ├── metadata/
│   │   ├── version-hint.text           # Points to latest metadata file
│   │   ├── 1-{uuid}.metadata.json      # Table metadata v1
│   │   ├── snap-{id}-{uuid}.avro       # Manifest list (JSON for now)
│   │   └── {uuid}-m0.avro              # Manifest file (JSON for now)
│   └── data/
│       └── {uuid}.parquet              # Data files
├── posts/
│   └── ...
└── _parquedb/                          # ParqueDB-specific metadata
    └── rels/
        ├── forward/{ns}.parquet
        └── reverse/{ns}.parquet
```

**Iceberg Catalog Options:**
```typescript
type IcebergCatalogConfig =
  | { type: 'filesystem' }                                          // Direct metadata access
  | { type: 'r2-data-catalog'; accountId: string; apiToken: string; bucketName?: string }
  | { type: 'rest'; uri: string; credential?: string; warehouse?: string }
```

### 3. Delta Lake Backend

Delta Lake format implemented directly in ParqueDB:

```typescript
// src/backends/delta.ts

import { createDeltaBackend, DeltaBackend } from 'parquedb/backends'

// Create via factory function
const backend = createDeltaBackend({
  type: 'delta',
  storage: storageBackend,  // ParqueDB StorageBackend
  location: 'warehouse',     // Base path for tables
  readOnly: false,           // Optional: read-only mode
})

// Or instantiate the class directly
const backend = new DeltaBackend({
  type: 'delta',
  storage: storageBackend,
  location: 'warehouse',
})
```

**DeltaBackend Properties:**
```typescript
class DeltaBackend implements EntityBackend {
  readonly type = 'delta'
  readonly supportsTimeTravel = true   // via versions
  readonly supportsSchemaEvolution = true
  readonly readOnly: boolean

  // Internal state
  private storage: StorageBackend
  private location: string
  private versionCache: Map<string, number>
}
```

**Storage Layout (Delta Lake standard):**
```
bucket/
├── users/
│   ├── _delta_log/
│   │   ├── 00000000000000000000.json   # Initial commit (protocol + metadata + data)
│   │   ├── 00000000000000000001.json   # Subsequent commits
│   │   ├── 00000000000000000010.checkpoint.parquet  # Checkpoint every 10 versions
│   │   └── _last_checkpoint             # Pointer to latest checkpoint
│   ├── {uuid}.parquet                   # Data files
│   └── ...
├── posts/
│   └── ...
└── _parquedb/
    └── rels/
        ├── forward/{ns}.parquet
        └── reverse/{ns}.parquet
```

**Delta Log Actions:**
```typescript
// Protocol action (first commit only)
{ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }

// Metadata action (first commit, schema changes)
{ metaData: { id: string, schemaString: string, partitionColumns: [], createdTime: number } }

// Add action (new data file)
{ add: { path: string, size: number, modificationTime: number, dataChange: boolean } }

// Remove action (deleted file)
{ remove: { path: string, deletionTimestamp: number, dataChange: boolean } }

// Commit info
{ commitInfo: { timestamp: number, operation: string, readVersion?: number } }
```

## Entity Storage Schema

Both backends store entities with the same Parquet schema:

```typescript
// Built by buildEntityParquetSchema() in src/backends/parquet-utils.ts

const entitySchema = {
  $id: { type: 'STRING', optional: false },      // "ns/id" format
  $type: { type: 'STRING', optional: false },    // Entity type
  name: { type: 'STRING', optional: false },     // Display name
  createdAt: { type: 'STRING', optional: false }, // ISO timestamp
  createdBy: { type: 'STRING', optional: false },
  updatedAt: { type: 'STRING', optional: false },
  updatedBy: { type: 'STRING', optional: false },
  deletedAt: { type: 'STRING', optional: true },  // Soft delete
  deletedBy: { type: 'STRING', optional: true },
  version: { type: 'INT32', optional: false },   // Optimistic concurrency
  $data: { type: 'STRING', optional: true },     // Base64-encoded Variant (flexible data)
}
```

**Entity Serialization:**
```typescript
// Convert entity to Parquet row
const row = entityToRow(entity)
// Core fields extracted, remaining data encoded as base64 Variant in $data

// Convert row back to entity
const entity = rowToEntity<MyType>(row)
// $data decoded and merged with core fields
```

## Relationship Index Layer

The relationship index is **always** stored in ParqueDB's format, separate from entity data:

```typescript
// src/relationships/index.ts

export interface RelationshipIndex {
  // Write operations
  link(from: EntityRef, predicate: string, to: EntityRef, options?: LinkOptions): Promise<void>
  unlink(from: EntityRef, predicate: string, to: EntityRef): Promise<void>

  // Read operations (forward traversal)
  getOutbound(from: EntityRef, predicate?: string): Promise<Relationship[]>

  // Read operations (reverse traversal)
  getInbound(to: EntityRef, reverse?: string): Promise<Relationship[]>

  // Bulk operations
  getRelationships(refs: EntityRef[], direction: 'outbound' | 'inbound'): Promise<Map<string, Relationship[]>>
}
```

**Relationship Schema:**
```typescript
interface Relationship {
  // Source
  fromNs: string
  fromId: string
  fromType?: string
  fromName?: string

  // Edge
  predicate: string   // Outbound name (e.g., "author")
  reverse: string     // Inbound name (e.g., "posts")

  // Target
  toNs: string
  toId: string
  toType?: string
  toName?: string

  // Audit
  createdAt: number
  createdBy: string
  deletedAt?: number
  deletedBy?: string
  version: number

  // Edge properties
  data?: Record<string, unknown>
}
```

**Index Files:**
```
rels/
├── forward/
│   ├── users.parquet      # Sorted by (from_ns, from_id, predicate)
│   ├── posts.parquet
│   └── categories.parquet
└── reverse/
    ├── users.parquet      # Sorted by (to_ns, to_id, reverse)
    ├── posts.parquet
    └── categories.parquet
```

## IceType Integration

ParqueDB's relationship table implements IceType's relationship operators:

| IceType | ParqueDB Storage | Query Pattern |
|---------|-----------------|---------------|
| `-> User` | `predicate="..."` | Forward lookup by predicate |
| `<- Post.author[]` | `reverse="..."` | Reverse lookup by reverse name |
| `~> Entity` | Future: vector index | Similarity search |
| `<~> Entity` | Future: bidirectional vector | Mutual similarity |

**Example Schema:**
```typescript
// IceType definition
const schema = {
  Post: {
    $type: 'Post',
    author: '-> User.posts',        // predicate="author", reverse="posts"
    categories: '-> Category.posts[]',
  },
  User: {
    $type: 'User',
    posts: '<- Post.author[]',      // Query: WHERE reverse="posts"
  }
}

// ParqueDB relationship row
{
  fromNs: 'posts',
  fromId: 'post-123',
  predicate: 'author',       // IceType: author: '-> User.posts'
  reverse: 'posts',          // IceType: posts: '<- Post.author[]'
  toNs: 'users',
  toId: 'alice',
}
```

## Factory Functions and Usage

```typescript
import {
  // Iceberg
  IcebergBackend,
  createIcebergBackend,
  createR2IcebergBackend,
  // Delta
  DeltaBackend,
  createDeltaBackend,
  // Generic factory
  createBackend,
  // Types
  BackendConfig,
  IcebergBackendConfig,
  DeltaBackendConfig,
} from 'parquedb/backends'

// Generic factory (auto-detects backend type)
const backend = await createBackend({
  type: 'iceberg',  // or 'delta'
  storage: storageBackend,
  warehouse: 'warehouse',
})

// Backend is initialized and ready to use
await backend.create('users', { $type: 'User', name: 'Alice' })
const users = await backend.find('users', { name: 'Alice' })

// Time travel
const snapshot = await backend.snapshot?.('users', new Date('2024-01-01'))
const historicalUsers = await snapshot?.find('users', {})

// Cleanup
await backend.close()
```

## R2 Data Catalog Integration

When using Iceberg backend with R2 Data Catalog:

```typescript
import { createR2IcebergBackend } from 'parquedb/backends'
import { R2StorageBackend } from 'parquedb/storage'

const storage = new R2StorageBackend(env.R2_BUCKET)

const backend = createR2IcebergBackend(storage, {
  accountId: env.CF_ACCOUNT_ID,
  apiToken: env.R2_DATA_CATALOG_TOKEN,
  bucketName: 'my-bucket',
  warehouse: 'warehouse',
  database: 'default',
})

await backend.initialize()
```

**Write Flow:**
1. ParqueDB writes entity Parquet files directly to R2
2. ParqueDB commits metadata via R2 Data Catalog REST API
3. Cloudflare manages compaction automatically (up to 2GB/hour)
4. Relationship indexes written separately to `_parquedb/rels/`

**Read Flow:**
1. Entity queries can use:
   - ParqueDB API (with relationship resolution)
   - DuckDB/Spark directly (standard Iceberg, no relationships)
2. Relationship traversal always uses ParqueDB's index

**Benefits:**
- Cloudflare manages entity table compaction
- Zero egress fees for Workers
- Standard Iceberg = queryable by any engine
- Graph capabilities via ParqueDB's relationship layer

## Configuration Types

```typescript
// Base configuration for all backends
interface BaseBackendConfig {
  storage: StorageBackend  // Required: file I/O backend
  location?: string        // Base path for data
  readOnly?: boolean       // Read-only mode
}

// Native backend (planned)
interface NativeBackendConfig extends BaseBackendConfig {
  type: 'native'
}

// Iceberg backend
interface IcebergBackendConfig extends BaseBackendConfig {
  type: 'iceberg'
  catalog?: IcebergCatalogConfig  // Catalog type
  warehouse?: string              // Warehouse location
  database?: string               // Database/namespace
}

// Delta backend
interface DeltaBackendConfig extends BaseBackendConfig {
  type: 'delta'
}

// Union type
type BackendConfig =
  | NativeBackendConfig
  | IcebergBackendConfig
  | DeltaBackendConfig
```

## Migration Paths

### Native to Iceberg

```typescript
// Export native data to Iceberg format
await db.exportToIceberg({
  destination: 's3://bucket/warehouse',
  catalog: new R2DataCatalogClient({ ... }),
  collections: ['users', 'posts'],  // or '*' for all
  includeRelationships: true,       // Copy rels to _parquedb/rels/
})
```

### Reading Existing Iceberg/Delta Tables

```typescript
// Mount existing Iceberg table as read-only
const externalTable = await db.mountIceberg({
  location: 's3://analytics/events',
  catalog: restCatalog,
  namespace: 'external',
})

// Query with ParqueDB API
const events = await db.external.events.find({
  type: 'purchase',
  createdAt: { $gte: yesterday }
})

// Note: No relationship indexes for external tables unless explicitly built
```

## Performance Characteristics

| Operation | Native | Iceberg | Delta Lake |
|-----------|--------|---------|------------|
| Single entity read | ~5ms | ~10ms | ~10ms |
| Entity scan (1K rows) | ~50ms | ~100ms | ~100ms |
| Entity write | ~10ms | ~50ms* | ~20ms |
| Relationship traverse | ~5ms | ~5ms | ~5ms |
| Time-travel query | ~100ms | ~20ms | ~20ms |
| Schema evolution | N/A | ~50ms | ~50ms |

*Iceberg writes include REST catalog commit overhead

## Future Enhancements

1. **Native backend implementation** - Complete the simple Parquet backend
2. **Relationship replication to Iceberg** - Optionally write relationships as an Iceberg table for analytics queries
3. **Fuzzy relationships** - Vector similarity index for `~>` and `<~>` operators
4. **Cross-backend joins** - Query relationships across different backends
5. **Incremental materialized views** - Cached graph patterns updated on change
