# Pluggable Backend Architecture

## Overview

ParqueDB supports three storage backends for entity data while maintaining a unified relationship index layer on top:

1. **Native** - ParqueDB's simple, custom Parquet format
2. **Delta Lake** - Via `@dotdo/deltalake` library
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
│  │ • DeltaLakeBackend   │     │ • Parquet (rels/reverse/)    │ │
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
  // Lifecycle
  initialize(): Promise<void>
  close(): Promise<void>

  // Read operations
  get(ns: string, id: string): Promise<Entity | null>
  find(ns: string, filter?: Filter, options?: FindOptions): Promise<Entity[]>
  count(ns: string, filter?: Filter): Promise<number>

  // Write operations
  create(ns: string, entity: EntityInput): Promise<Entity>
  update(ns: string, id: string, update: Update): Promise<Entity>
  delete(ns: string, id: string): Promise<void>

  // Batch operations
  bulkCreate(ns: string, entities: EntityInput[]): Promise<Entity[]>
  bulkUpdate(ns: string, filter: Filter, update: Update): Promise<number>
  bulkDelete(ns: string, filter: Filter): Promise<number>

  // Time-travel (backend-specific)
  snapshot?(ns: string, version: number | Date): Promise<EntityBackend>

  // Schema
  schema(ns: string): Promise<Schema | null>

  // Metadata
  readonly type: 'native' | 'iceberg' | 'delta'
  readonly supportsTimeTravel: boolean
  readonly supportsSchemaEvolution: boolean
}
```

## Backend Implementations

### 1. Native Backend

ParqueDB's original simple format:

```typescript
// src/backends/native.ts

export class NativeBackend implements EntityBackend {
  readonly type = 'native'
  readonly supportsTimeTravel = true  // via event log
  readonly supportsSchemaEvolution = false

  constructor(
    private storage: StorageBackend,
    private options: NativeBackendOptions = {}
  ) {}

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

import {
  readTableMetadata,
  MetadataWriter,
  R2DataCatalogClient
} from '@dotdo/iceberg'

export class IcebergBackend implements EntityBackend {
  readonly type = 'iceberg'
  readonly supportsTimeTravel = true   // via snapshots
  readonly supportsSchemaEvolution = true

  constructor(
    private storage: StorageBackend,
    private catalog: IcebergCatalog,
    private options: IcebergBackendOptions = {}
  ) {}

  // Uses standard Iceberg table structure
  // Catalog can be:
  // - R2DataCatalogClient (Cloudflare managed)
  // - RESTCatalog (any Iceberg REST catalog)
  // - FileSystemCatalog (direct metadata access)
}
```

**Storage Layout (Iceberg standard):**
```
bucket/warehouse/db/
├── users/
│   ├── metadata/
│   │   ├── version-hint.txt
│   │   ├── v1.metadata.json
│   │   └── snap-001.avro
│   └── data/
│       └── part-00000.parquet
├── posts/
│   └── ...
└── _parquedb/                    # ParqueDB-specific metadata
    └── rels/
        ├── forward/{ns}.parquet
        └── reverse/{ns}.parquet
```

### 3. Delta Lake Backend

Delta Lake format with transaction log:

```typescript
// src/backends/delta.ts

import { DeltaTable, createStorage } from '@dotdo/deltalake'

export class DeltaLakeBackend implements EntityBackend {
  readonly type = 'delta'
  readonly supportsTimeTravel = true   // via versions
  readonly supportsSchemaEvolution = true

  constructor(
    private storage: StorageBackend,
    private options: DeltaLakeBackendOptions = {}
  ) {}

  // Uses Delta Lake table structure
  // Transaction log in _delta_log/
}
```

**Storage Layout (Delta Lake standard):**
```
bucket/
├── users/
│   ├── _delta_log/
│   │   ├── 00000000000000000000.json
│   │   └── 00000000000000000001.json
│   └── part-00000.parquet
├── posts/
│   └── ...
└── _parquedb/
    └── rels/
        ├── forward/{ns}.parquet
        └── reverse/{ns}.parquet
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

## R2 Data Catalog Integration

When using Iceberg backend with R2 Data Catalog:

```typescript
const db = new ParqueDB({
  backend: {
    type: 'iceberg',
    catalog: {
      type: 'r2-data-catalog',
      accountId: env.CF_ACCOUNT_ID,
      apiToken: env.CF_API_TOKEN,
    },
  },
  storage: env.R2_BUCKET,
})
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

## Configuration

```typescript
// Full configuration options
interface ParqueDBConfig {
  // Backend selection
  backend:
    | { type: 'native' }
    | {
        type: 'iceberg'
        catalog:
          | { type: 'r2-data-catalog', accountId: string, apiToken: string }
          | { type: 'rest', uri: string, credential?: string }
          | { type: 'filesystem' }
      }
    | { type: 'delta' }

  // Storage
  storage: StorageBackend | R2Bucket

  // Warehouse location (for Iceberg/Delta)
  warehouse?: string

  // Relationship index options
  relationships?: {
    // Storage location for relationship indexes
    location?: string  // Default: '_parquedb/rels'

    // Super-node threshold
    superNodeThreshold?: number  // Default: 1000

    // Compaction settings
    compaction?: {
      enabled?: boolean
      targetFileSize?: number  // Default: 128MB
      maxFilesPerCompaction?: number
    }
  }
}
```

## Migration Paths

### Native → Iceberg

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

1. **Relationship replication to Iceberg** - Optionally write relationships as an Iceberg table for analytics queries
2. **Fuzzy relationships** - Vector similarity index for `~>` and `<~` operators
3. **Cross-backend joins** - Query relationships across different backends
4. **Incremental materialized views** - Cached graph patterns updated on change
