# Pluggable Entity Backends

ParqueDB supports pluggable backends for entity storage, allowing you to choose the table format that best fits your infrastructure and query tools.

## Available Backends

| Backend | Format | Time Travel | Compatible With |
|---------|--------|-------------|-----------------|
| **Iceberg** | Apache Iceberg | Yes | DuckDB, Spark, Snowflake, Trino |
| **Delta Lake** | Delta Lake | Yes | Spark, Databricks, DuckDB |

Both backends store entities as Parquet files with full CRUD support, time-travel queries, and schema evolution.

## Quick Start

### Iceberg Backend

```typescript
import { createIcebergBackend } from 'parquedb/backends'
import { MemoryBackend } from 'parquedb/storage'

// Create with filesystem catalog
const storage = new MemoryBackend() // or R2Backend, S3Backend, etc.
const backend = createIcebergBackend({
  type: 'iceberg',
  storage,
  warehouse: 'warehouse',
  database: 'mydb',
})

await backend.initialize()

// Create entities
const user = await backend.create('users', {
  $type: 'User',
  name: 'Alice',
  email: 'alice@example.com',
})

// Query entities
const users = await backend.find('users', { role: 'admin' })

// Bulk operations
const posts = await backend.bulkCreate('posts', [
  { $type: 'Post', name: 'Hello World', title: 'My First Post' },
  { $type: 'Post', name: 'Second Post', title: 'Another Day' },
])

await backend.close()
```

### Iceberg with R2 Data Catalog

For Cloudflare R2 with managed Iceberg catalog:

```typescript
import { createR2IcebergBackend } from 'parquedb/backends'

const backend = createR2IcebergBackend(r2Storage, {
  accountId: env.CF_ACCOUNT_ID,
  apiToken: env.R2_DATA_CATALOG_TOKEN,
  warehouse: 'warehouse',
  database: 'production',
})
```

### Delta Lake Backend

```typescript
import { createDeltaBackend } from 'parquedb/backends'

const backend = createDeltaBackend({
  type: 'delta',
  storage,
  location: 'warehouse',
})

await backend.initialize()

// Same API as Iceberg
const user = await backend.create('users', {
  $type: 'User',
  name: 'Bob',
})

await backend.close()
```

## Configuration Options

### Iceberg Configuration

```typescript
interface IcebergBackendConfig {
  type: 'iceberg'
  storage: StorageBackend      // File I/O backend (R2, S3, filesystem)
  warehouse?: string           // Base path for tables
  database?: string            // Database/namespace name
  readOnly?: boolean           // Disable writes
  catalog?: IcebergCatalogConfig
}

// Catalog options
type IcebergCatalogConfig =
  | { type: 'filesystem' }
  | { type: 'r2-data-catalog'; accountId: string; apiToken: string; bucketName?: string }
  | { type: 'rest'; uri: string; credential?: string; warehouse?: string }
```

### Delta Lake Configuration

```typescript
interface DeltaBackendConfig {
  type: 'delta'
  storage: StorageBackend      // File I/O backend
  location?: string            // Base path for tables
  readOnly?: boolean           // Disable writes
}
```

## Time Travel

Both backends support querying historical data.

### Query by Version

```typescript
// Get a read-only view at a specific version
const snapshotBackend = await backend.snapshot('users', 0)
const oldUsers = await snapshotBackend.find('users', {})
```

### Query by Timestamp

```typescript
// Get data as of a specific time
const snapshotBackend = await backend.snapshot('users', new Date('2024-01-15'))
const historicalData = await snapshotBackend.find('users', {})
```

### Query with asOf Option

```typescript
// Query directly with timestamp
const users = await backend.find('users', {}, {
  asOf: new Date('2024-01-15')
})
```

### List Available Snapshots

```typescript
const snapshots = await backend.listSnapshots('users')
// Returns: [{ id: 0, timestamp: Date, operation: 'append', recordCount: 5 }, ...]
```

## Schema Operations

### Get Schema

```typescript
const schema = await backend.getSchema('users')
// Returns: { name: 'users', version: 1, fields: [...] }
```

### Set Schema

```typescript
await backend.setSchema('users', {
  name: 'users',
  fields: [
    { name: '$id', type: 'string', required: true },
    { name: 'name', type: 'string', required: true },
    { name: 'email', type: 'string', nullable: true },
    { name: 'score', type: 'int', nullable: true },
  ],
})
```

### List Namespaces

```typescript
const namespaces = await backend.listNamespaces()
// Returns: ['users', 'posts', 'comments']
```

## Maintenance Operations

### Statistics

```typescript
const stats = await backend.stats('users')
// Returns:
// {
//   recordCount: 1000,
//   totalBytes: 524288,
//   fileCount: 5,
//   snapshotCount: 12,
//   lastModified: Date
// }
```

### Compaction

Merge small files into larger ones for better query performance:

```typescript
const result = await backend.compact('users', {
  targetFileSize: 128 * 1024 * 1024,  // 128MB target
  maxFiles: 100,                       // Max files per run
  dryRun: false,
})
// Returns: { filesCompacted: 10, filesCreated: 2, bytesBefore, bytesAfter, durationMs }
```

### Vacuum

Remove old snapshots and orphaned files:

```typescript
const result = await backend.vacuum('users', {
  retentionMs: 7 * 24 * 60 * 60 * 1000,  // Keep 7 days
  minSnapshots: 5,                        // Always keep at least 5
  dryRun: false,
})
// Returns: { filesDeleted: 20, bytesReclaimed: 10485760, snapshotsExpired: 8 }
```

## CRUD Operations

All backends implement the same `EntityBackend` interface:

```typescript
// Single entity operations
await backend.create(ns, input, options?)
await backend.get(ns, id, options?)
await backend.update(ns, id, update, options?)
await backend.delete(ns, id, options?)

// Query operations
await backend.find(ns, filter?, options?)
await backend.count(ns, filter?)
await backend.exists(ns, id)

// Bulk operations
await backend.bulkCreate(ns, inputs, options?)
await backend.bulkUpdate(ns, filter, update, options?)
await backend.bulkDelete(ns, filter, options?)
```

### Update Operators

```typescript
await backend.update('users', id, {
  $set: { name: 'Alice Updated', score: 100 },
  $inc: { loginCount: 1 },
  $unset: { tempField: true },
})
```

### Find Options

```typescript
const users = await backend.find('users',
  { role: 'admin' },
  {
    sort: { createdAt: -1 },
    skip: 0,
    limit: 10,
    includeDeleted: false,
    asOf: new Date(),  // Time travel
  }
)
```

## Backend Properties

Check backend capabilities:

```typescript
backend.type                    // 'iceberg' | 'delta'
backend.supportsTimeTravel      // true
backend.supportsSchemaEvolution // true
backend.readOnly               // false (unless configured)
```

## Storage Layout

### Iceberg

```
warehouse/
  database/
    namespace/
      metadata/
        version-hint.text
        1-uuid.metadata.json
        snap-123-uuid.avro
        uuid-m0.avro
      data/
        uuid.parquet
```

### Delta Lake

```
warehouse/
  namespace/
    _delta_log/
      00000000000000000000.json
      00000000000000000001.json
      00000000000000000010.checkpoint.parquet
      _last_checkpoint
    uuid.parquet
```

## Soft vs Hard Delete

By default, delete operations are soft deletes (set `deletedAt`):

```typescript
// Soft delete (default) - entity hidden but recoverable
await backend.delete('users', id)

// Hard delete - permanently removed
await backend.delete('users', id, { hard: true })

// Query includes soft-deleted entities
await backend.find('users', {}, { includeDeleted: true })
```
