# Storage Architecture: StorageBackend vs EntityBackend

ParqueDB uses a two-layer abstraction for data storage. This document explains the distinction between these layers and when to use each.

## Overview

```
Application Code (ParqueDB, Collection)
       |
       v
+---------------------+
|    EntityBackend    |  <-- High-level: Entity CRUD, schemas, time-travel
|  (Iceberg / Delta)  |      Table format logic (commits, snapshots, metadata)
+---------+-----------+
          |
          | uses internally for file I/O
          v
+---------------------+
|   StorageBackend    |  <-- Low-level: File read/write, paths, directories
|  (R2 / S3 / FS)     |      Platform abstraction (cloud vs local)
+---------+-----------+
          |
          v
   Physical Storage
   (R2 bucket, S3 bucket, local filesystem, etc.)
```

## StorageBackend (Low-Level)

**Location**: `src/types/storage.ts`

StorageBackend is the low-level file I/O abstraction. It provides platform-agnostic operations for reading and writing raw bytes to files.

### Key Characteristics

- **Operates on**: Files (paths, bytes, metadata)
- **Knows about**: Directories, file stats, ETags, ranges
- **Does NOT know about**: Entities, schemas, table formats, time-travel

### Core Operations

```typescript
interface StorageBackend {
  // Read operations
  read(path: string): Promise<Uint8Array>
  readRange(path: string, start: number, end: number): Promise<Uint8Array>
  exists(path: string): Promise<boolean>
  stat(path: string): Promise<FileStat | null>
  list(prefix: string, options?: ListOptions): Promise<ListResult>

  // Write operations
  write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<WriteResult>
  append(path: string, data: Uint8Array): Promise<void>
  delete(path: string): Promise<boolean>

  // Directory operations
  mkdir(path: string): Promise<void>
  rmdir(path: string, options?: RmdirOptions): Promise<void>

  // File operations
  copy(source: string, dest: string): Promise<void>
  move(source: string, dest: string): Promise<void>
}
```

### Implementations

| Implementation    | Use Case                          | Location                    |
|-------------------|-----------------------------------|-----------------------------|
| `MemoryBackend`   | Testing, development              | `src/storage/MemoryBackend.ts` |
| `FsBackend`       | Node.js local filesystem          | `src/storage/FsBackend.ts`     |
| `FsxBackend`      | Cloudflare Workers fsx            | `src/storage/FsxBackend.ts`    |
| `R2Backend`       | Cloudflare R2 object storage      | `src/storage/R2Backend.ts`     |
| `DOSqliteBackend` | Cloudflare Durable Objects SQLite | `src/storage/DOSqliteBackend.ts` |
| `RemoteBackend`   | HTTP-based read-only access       | `src/storage/RemoteBackend.ts` |

### When to Use StorageBackend

- Implementing a new storage provider (GCS, Azure Blob, MinIO)
- Direct file operations (downloading backups, cache warming)
- Infrastructure-level features (replication, disaster recovery)
- Custom Parquet file reading outside of entity queries

## EntityBackend (High-Level)

**Location**: `src/backends/types.ts`

EntityBackend is the high-level entity CRUD abstraction. It manages entities using different table formats (Iceberg, Delta Lake) and uses a StorageBackend internally.

### Key Characteristics

- **Operates on**: Entities (typed objects with schemas)
- **Knows about**: Schemas, time-travel, compaction, versions
- **Uses**: StorageBackend for all file I/O
- **Does NOT manage**: Relationships (handled separately by ParqueDB)

### Core Operations

```typescript
interface EntityBackend {
  // Metadata
  readonly type: 'native' | 'iceberg' | 'delta'
  readonly supportsTimeTravel: boolean
  readonly supportsSchemaEvolution: boolean

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

  // Time-travel
  snapshot?(ns: string, version: number | Date): Promise<EntityBackend>
  listSnapshots?(ns: string): Promise<SnapshotInfo[]>

  // Schema
  getSchema(ns: string): Promise<EntitySchema | null>
  setSchema?(ns: string, schema: EntitySchema): Promise<void>

  // Maintenance
  compact?(ns: string, options?: CompactOptions): Promise<CompactResult>
  vacuum?(ns: string, options?: VacuumOptions): Promise<VacuumResult>
}
```

### Implementations

| Implementation   | Table Format | Features                                      |
|------------------|--------------|-----------------------------------------------|
| `IcebergBackend` | Apache Iceberg | Time-travel, schema evolution, R2 Data Catalog |
| `DeltaBackend`   | Delta Lake     | Time-travel, schema evolution, checkpoints     |
| `NativeBackend`  | ParqueDB Native | Simple format, event-log time-travel (planned) |

### Configuration

EntityBackend implementations receive a StorageBackend in their configuration:

```typescript
interface BaseBackendConfig {
  storage: StorageBackend   // <-- File I/O layer
  location?: string         // Base path for data
  readOnly?: boolean
}

interface IcebergBackendConfig extends BaseBackendConfig {
  type: 'iceberg'
  catalog?: IcebergCatalogConfig
  warehouse?: string
  database?: string
}

interface DeltaBackendConfig extends BaseBackendConfig {
  type: 'delta'
  maxRetries?: number
  baseBackoffMs?: number
}
```

### When to Use EntityBackend

- Business logic that operates on entities
- Application features (queries, mutations, subscriptions)
- Adding support for new table formats
- Integrating with analytics engines (via Iceberg/Delta)

## Relationship Between Layers

### How They Work Together

1. Application calls `db.collection('users').find({ name: 'Alice' })`
2. ParqueDB delegates to EntityBackend: `backend.find('users', { name: 'Alice' })`
3. EntityBackend (e.g., IcebergBackend):
   - Reads table metadata via `storage.read('warehouse/users/metadata/v1.json')`
   - Finds relevant data files from manifest
   - Reads Parquet files via `storage.readRange('warehouse/users/data/file.parquet', ...)`
   - Deserializes entities and applies filter
4. Returns matching entities to application

### Storage Layout Example

```
bucket/
├── warehouse/                     # EntityBackend (Iceberg) manages
│   └── users/
│       ├── metadata/
│       │   ├── version-hint.text
│       │   └── v1-uuid.metadata.json
│       └── data/
│           └── part-001.parquet   # StorageBackend reads/writes these files
├── _parquedb/
│   └── rels/                      # Relationships (managed by ParqueDB, not EntityBackend)
│       ├── forward/users.parquet
│       └── reverse/users.parquet
└── events/                        # Event log (managed by ParqueDB)
    └── current.parquet
```

## Summary

| Aspect | StorageBackend | EntityBackend |
|--------|---------------|---------------|
| **Level** | Low-level (files) | High-level (entities) |
| **Operates on** | Paths, bytes, metadata | Entities, schemas, versions |
| **Abstracts** | Storage platforms | Table formats |
| **Implementations** | R2, S3, FS, Memory | Iceberg, Delta, Native |
| **Use when** | File operations, new storage | Entity operations, new formats |
| **Location** | `src/types/storage.ts` | `src/backends/types.ts` |

## See Also

- [Pluggable Backends](pluggable-backends.md) - Details on Iceberg and Delta implementations
- [Entity Storage](entity-storage.md) - Dual storage architecture (DO vs R2)
- `src/storage/` - StorageBackend implementations
- `src/backends/` - EntityBackend implementations
