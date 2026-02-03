---
title: ParqueDB Class API Reference
description: Complete API reference for the ParqueDB class - constructor, collection access, schema management, transactions, and lifecycle methods
---

The `ParqueDB` class is the main entry point for interacting with ParqueDB. It provides both namespace-based methods and proxy-based collection access, along with advanced features like transactions, time-travel, and event logging.

## Constructor & Configuration

### new ParqueDB(config)

Creates a new ParqueDB instance with the specified configuration.

```typescript
new ParqueDB(config: ParqueDBConfig)
```

#### ParqueDBConfig

Configuration options for initializing a ParqueDB instance.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `storage` | `StorageBackend` | Yes | Storage backend for data persistence (MemoryBackend, FSBackend, R2Backend, etc.) |
| `schema` | `Schema` | No | Schema definition for entity validation and relationships |
| `defaultNamespace` | `string` | No | Default namespace for operations (currently unused) |
| `snapshotConfig` | `SnapshotConfig` | No | Configuration for automatic snapshot creation |
| `eventLogConfig` | `EventLogConfig` | No | Configuration for event log rotation and archival |
| `storageRouter` | `IStorageRouter` | No | Storage router for determining storage mode and paths per collection |
| `collectionOptions` | `Map<string, CollectionOptions>` | No | Per-collection options from DB() schema |

#### SnapshotConfig

| Property | Type | Description |
|----------|------|-------------|
| `autoSnapshotThreshold` | `number` | Automatically create snapshot after this many events |

#### EventLogConfig

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `maxEvents` | `number` | 10000 | Maximum number of events to keep in the log |
| `maxAge` | `number` | 604800000 (7 days) | Maximum age of events in milliseconds |
| `archiveOnRotation` | `boolean` | false | Whether to archive rotated events instead of dropping them |

#### Example

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({
  storage: new MemoryBackend(),
  schema: {
    Post: {
      $ns: 'posts',
      title: 'string!',
      content: 'text',
      author: '-> User.posts',
    },
    User: {
      $ns: 'users',
      email: 'email!#',
      name: 'string',
      posts: '<- Post.author[]',
    },
  },
  snapshotConfig: {
    autoSnapshotThreshold: 100,
  },
  eventLogConfig: {
    maxEvents: 5000,
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
    archiveOnRotation: true,
  },
})
```

---

## Collection Access

ParqueDB supports two patterns for accessing collections:

### collection(name)

Get a typed collection interface for a namespace.

```typescript
collection<T = Record<string, unknown>>(namespace: string): Collection<T>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace (e.g., 'posts', 'users') |

#### Returns

`Collection<T>` - A collection interface with `find`, `get`, `create`, `update`, and `delete` methods.

#### Example

```typescript
interface Post {
  title: string
  content: string
  status: 'draft' | 'published'
}

const posts = db.collection<Post>('posts')
const result = await posts.find({ status: 'published' })
```

### Proxy-based Access

Access collections as properties using PascalCase. ParqueDB automatically creates collection instances via Proxy.

```typescript
// Automatically creates a collection for 'posts' namespace
await db.Posts.find({ status: 'published' })
await db.Users.get('users/123')

// Both PascalCase and camelCase work
await db.posts.create({ title: 'Hello' }) // creates in 'posts' namespace
await db.Posts.create({ title: 'Hello' }) // same as above
```

The proxy converts property names to lowercase namespaces:
- `Posts` → `posts`
- `BlogPosts` → `blogposts`
- `userProfiles` → `userprofiles`

For multi-word namespaces, use the `collection()` method for explicit control:
```typescript
// Explicit namespace control
const blogPosts = db.collection('blog-posts')
await blogPosts.create({ title: 'Hello' })
```

---

## Schema Management

### registerSchema(schema)

Register a schema for validation. Schemas define entity types, field types, relationships, and validation rules.

```typescript
registerSchema(schema: Schema): void
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `schema` | `Schema` | Schema definition object with entity types |

#### Example

```typescript
db.registerSchema({
  Post: {
    $ns: 'posts',
    title: 'string!',           // required string
    content: 'text',             // text field
    status: 'string = "draft"',  // default value
    author: '-> User.posts',     // forward relationship
    tags: '-> Tag.posts[]',      // many-to-many
  },
  User: {
    $ns: 'users',
    email: 'email!#',            // required, indexed
    name: 'string!',
    posts: '<- Post.author[]',   // reverse relationship
  },
  Tag: {
    $ns: 'tags',
    name: 'string!#',
    posts: '<- Post.tags[]',
  },
})
```

Schema definitions support:
- **Type notation**: `string`, `int`, `float`, `boolean`, `date`, `email`, `url`, `markdown`, `text`, etc.
- **Modifiers**: `!` (required), `#` (indexed), `?` (optional), `[]` (array)
- **Defaults**: `string = "default value"`
- **Relationships**: `-> Target.field` (forward), `<- Target.field[]` (reverse)

See [Schema Documentation](schemas.md) for complete reference.

### getSchemaValidator()

Get the schema validator instance for advanced validation scenarios.

```typescript
getSchemaValidator(): SchemaValidator | null
```

#### Returns

`SchemaValidator | null` - The schema validator instance, or null if no schema is registered.

#### Example

```typescript
const validator = db.getSchemaValidator()
if (validator) {
  const errors = validator.validate('Post', {
    title: 'My Post',
    content: 'Post content',
  })
  console.log(errors) // Array of validation errors
}
```

---

## Transaction API

### beginTransaction()

Begin a transaction for atomic operations. All changes within a transaction are buffered and only persisted on commit.

```typescript
beginTransaction(): ParqueDBTransaction
```

#### ParqueDBTransaction

The transaction object provides the same CRUD methods as ParqueDB but buffers all changes.

| Method | Signature | Description |
|--------|-----------|-------------|
| `create()` | `create<T>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>` | Create entity within transaction |
| `update()` | `update<T>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> \| null>` | Update entity within transaction |
| `delete()` | `delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>` | Delete entity within transaction |
| `commit()` | `commit(): Promise<void>` | Commit all changes and persist to storage |
| `rollback()` | `rollback(): Promise<void>` | Rollback all changes and discard buffered operations |

#### Transaction Isolation

Transactions in ParqueDB provide:
- **Read uncommitted**: Changes are visible to other operations immediately
- **Atomic commit**: All or nothing - if commit fails, no changes are persisted
- **Rollback support**: Discards all buffered events and reverses in-memory changes

Note: ParqueDB uses an in-memory entity store, so uncommitted changes are visible to concurrent reads. For true isolation, use optimistic concurrency control with `expectedVersion`.

#### Example

```typescript
const tx = db.beginTransaction()

try {
  // Create a post
  const post = await tx.create('posts', {
    $type: 'Post',
    name: 'New Post',
    title: 'Transaction Example',
    content: 'This is a post created in a transaction',
  })

  // Update user's post count
  await tx.update('users', 'users/123', {
    $inc: { postCount: 1 },
  })

  // Link post to user
  await tx.update('posts', post.$id, {
    $link: { author: 'users/123' },
  })

  // Commit all changes atomically
  await tx.commit()
  console.log('Transaction committed successfully')
} catch (error) {
  // Rollback on error
  await tx.rollback()
  console.error('Transaction failed, rolled back:', error)
  throw error
}
```

#### Best Practices

1. **Always use try/catch**: Ensure rollback is called on error
2. **Keep transactions short**: Long transactions increase memory pressure
3. **Use optimistic concurrency**: Add `expectedVersion` to critical updates
4. **Commit explicitly**: Don't rely on implicit commits

---

## Event Log & Time Travel

### getEventLog()

Get the event log interface for querying CDC (Change Data Capture) events.

```typescript
getEventLog(): EventLog
```

#### EventLog Interface

The event log provides methods to query the complete history of changes.

| Method | Signature | Description |
|--------|-----------|-------------|
| `getEvents()` | `getEvents(entityId: EntityId): Promise<Event[]>` | Get all events for a specific entity |
| `getEventsByNamespace()` | `getEventsByNamespace(ns: string): Promise<Event[]>` | Get all events for a namespace |
| `getEventsByTimeRange()` | `getEventsByTimeRange(from: Date, to: Date): Promise<Event[]>` | Get events within a time range |
| `getEventsByOp()` | `getEventsByOp(op: EventOp): Promise<Event[]>` | Get events by operation type ('CREATE', 'UPDATE', 'DELETE') |
| `getRawEvent()` | `getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }>` | Get raw event data with compression info |
| `getEventCount()` | `getEventCount(): Promise<number>` | Get total event count |
| `getConfig()` | `getConfig(): Required<EventLogConfig>` | Get current event log configuration |
| `archiveEvents()` | `archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): Promise<ArchiveEventsResult>` | Archive old events |
| `getArchivedEvents()` | `getArchivedEvents(): Promise<Event[]>` | Get archived events (if archiveOnRotation enabled) |

#### Event Structure

```typescript
interface Event {
  id: string           // Event ID (ULID)
  ts: Date            // Timestamp
  target: string      // Target entity or relationship (e.g., 'posts:abc123' or 'posts:abc123->users:xyz789')
  op: EventOp         // Operation: 'CREATE' | 'UPDATE' | 'DELETE'
  before: unknown     // State before operation (null for CREATE)
  after: unknown      // State after operation (null for DELETE)
  actor?: EntityId    // Who performed the operation
  metadata?: Record<string, unknown>  // Additional metadata
}
```

#### Example

```typescript
const eventLog = db.getEventLog()

// Get all events for an entity
const events = await eventLog.getEvents('posts/abc123' as EntityId)
for (const event of events) {
  console.log(`${event.op} at ${event.ts} by ${event.actor}`)
}

// Get events in a time range
const recentEvents = await eventLog.getEventsByTimeRange(
  new Date('2024-01-01'),
  new Date('2024-02-01')
)

// Get all CREATE operations
const creates = await eventLog.getEventsByOp('CREATE')

// Check event count
const count = await eventLog.getEventCount()
console.log(`Total events: ${count}`)
```

### Time-Travel Queries (asOf option)

Query entity state at a specific point in time by using the `asOf` option with `find()` or `get()` methods.

```typescript
find<T>(namespace: string, filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>
get<T>(namespace: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
```

#### Parameters

The `asOf` parameter is passed as part of the options object:

| Name | Type | Description |
|------|------|-------------|
| `options.asOf` | `Date` | The timestamp to query at |

ParqueDB reconstructs entity state by replaying events up to the specified time.

#### Example

```typescript
// Get post as it existed on January 1st, 2024
const post = await db.get('posts', 'abc123', {
  asOf: new Date('2024-01-01'),
})

// Find all published posts as of a specific time
const posts = await db.find('posts', { status: 'published' }, {
  asOf: new Date('2024-01-01'),
})

// Query historical data for reporting
const snapshot = await db.find('orders', { status: 'completed' }, {
  asOf: new Date('2024-01-01'),
  limit: 1000,
})
console.log(`Orders completed by Jan 1: ${snapshot.total}`)
console.log(`Found ${snapshot.items.length} orders`)
```

### archiveEvents(options)

Archive or drop old events based on configuration or manual threshold. This is a synchronous operation that returns immediately with the results.

```typescript
archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): ArchiveEventsResult
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `options.olderThan` | `Date` | Archive events older than this date |
| `options.maxEvents` | `number` | Archive events exceeding this count |

#### Returns

```typescript
interface ArchiveEventsResult {
  archivedCount: number       // Number of events archived
  droppedCount: number        // Number of events dropped
  oldestEventTs?: number      // Timestamp of oldest remaining event
  newestArchivedTs?: number   // Timestamp of newest archived event
}
```

#### Example

```typescript
// Archive events older than 30 days
const result = db.archiveEvents({
  olderThan: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
})

console.log(`Archived: ${result.archivedCount}, Dropped: ${result.droppedCount}`)

// Archive all but the most recent 1000 events
const result2 = db.archiveEvents({
  maxEvents: 1000,
})

console.log(`Oldest remaining event: ${result2.oldestEventTs}`)
console.log(`Newest archived event: ${result2.newestArchivedTs}`)
```

Event archival behavior depends on `eventLogConfig.archiveOnRotation`:
- **true**: Events are moved to archived storage
- **false**: Events are dropped permanently

---

## Lifecycle Methods

### dispose()

Dispose of the ParqueDB instance and clean up associated global state. Call this when you're done using a ParqueDB instance to prevent memory leaks.

```typescript
dispose(): void
```

After calling `dispose()`:
- The global state (entities, events, snapshots, query stats) for this storage backend is cleared
- The instance should not be used anymore
- Other ParqueDB instances using the same storage backend will also lose their shared state

#### Example

```typescript
const db = new ParqueDB({ storage: new MemoryBackend() })

// Use the database
await db.Posts.create({ title: 'Hello' })

// Clean up when done
db.dispose()

// Don't use db after dispose()
```

**Note**: ParqueDB uses a global entity store keyed by storage backend for persistence across instances. `dispose()` clears this shared state.

### flush()

**Note**: ParqueDB does not expose a public `flush()` method. The database automatically manages persistence through an internal `flushEvents()` mechanism.

ParqueDB buffers events in memory and automatically schedules a flush after each mutation. During transactions, auto-flush is suppressed until `commit()` is called.

For explicit control over persistence:
1. Use transactions with `beginTransaction()` and `commit()`
2. Or rely on automatic flushing after each operation (create, update, delete)

---

## Storage Router Methods

These methods provide access to the storage routing system for advanced use cases.

### getStorageMode(namespace)

Get the storage mode for a collection.

```typescript
getStorageMode(namespace: string): StorageMode
```

#### Returns

- `'typed'` - Typed storage with fixed schema (Parquet with explicit columns)
- `'flexible'` - Flexible storage with Variant data (schema-less)

#### Example

```typescript
const mode = db.getStorageMode('posts')
console.log(mode) // 'typed' or 'flexible'
```

### getDataPath(namespace)

Get the data path for a collection.

```typescript
getDataPath(namespace: string): string
```

#### Returns

The file path where the collection's data is stored.

#### Example

```typescript
const path = db.getDataPath('posts')
console.log(path) // 'data/posts/data.parquet' or 'data/typed/posts.parquet'
```

### hasTypedSchema(namespace)

Check if a collection has a typed schema.

```typescript
hasTypedSchema(namespace: string): boolean
```

#### Example

```typescript
if (db.hasTypedSchema('posts')) {
  console.log('Posts collection uses typed storage')
}
```

### getCollectionOptions(namespace)

Get the collection options for a namespace.

```typescript
getCollectionOptions(namespace: string): CollectionOptions | undefined
```

#### Returns

Collection options including validation mode, storage mode, and other configuration.

### getStorageRouter()

Get the storage router instance for advanced use cases.

```typescript
getStorageRouter(): IStorageRouter | null
```

#### Example

```typescript
const router = db.getStorageRouter()
if (router) {
  console.log('Storage router is configured')
}
```

---

## Snapshot Management

### getSnapshotManager()

Get the snapshot manager for manual snapshot operations and performance optimization.

```typescript
getSnapshotManager(): SnapshotManager
```

#### SnapshotManager Interface

| Method | Signature | Description |
|--------|-----------|-------------|
| `createSnapshot()` | `createSnapshot(entityId: EntityId): Promise<Snapshot>` | Create a snapshot of current entity state |
| `createSnapshotAtEvent()` | `createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot>` | Create snapshot at specific event |
| `listSnapshots()` | `listSnapshots(entityId: EntityId): Promise<Snapshot[]>` | List all snapshots for an entity |
| `deleteSnapshot()` | `deleteSnapshot(snapshotId: string): Promise<void>` | Delete a specific snapshot |
| `pruneSnapshots()` | `pruneSnapshots(options: PruneSnapshotsOptions): Promise<void>` | Prune old snapshots |
| `getRawSnapshot()` | `getRawSnapshot(snapshotId: string): Promise<RawSnapshot>` | Get raw snapshot data |
| `getQueryStats()` | `getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats>` | Get query performance statistics |
| `getStorageStats()` | `getStorageStats(): Promise<SnapshotStorageStats>` | Get storage statistics |

#### Snapshot Structure

```typescript
interface Snapshot {
  id: string                       // Snapshot ID (ULID)
  entityId: EntityId               // Entity this snapshot belongs to
  ns: string                      // Namespace
  sequenceNumber: number           // Event sequence number at snapshot time
  eventId?: string                // Event ID at snapshot time
  createdAt: Date                 // When snapshot was created
  state: Record<string, unknown>  // Entity state at snapshot time
  compressed: boolean             // Whether data is compressed
  size?: number                   // Compressed size in bytes (if compressed)
}
```

#### Example

```typescript
const snapshots = db.getSnapshotManager()

// Create a snapshot for performance optimization
const snapshot = await snapshots.createSnapshot('posts/abc123' as EntityId)
console.log(`Created snapshot at sequence ${snapshot.sequenceNumber}`)

// List all snapshots for an entity
const list = await snapshots.listSnapshots('posts/abc123' as EntityId)
console.log(`Found ${list.length} snapshots`)

// Prune old snapshots (keep minimum 5, delete older than 30 days)
await snapshots.pruneSnapshots({
  olderThan: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
  keepMinimum: 5,
})

// Get query statistics
const stats = await snapshots.getQueryStats('posts/abc123' as EntityId)
console.log(`Snapshots used: ${stats.snapshotsUsed}, Events replayed: ${stats.eventsReplayed}`)

// Get storage statistics
const storageStats = await snapshots.getStorageStats()
console.log(`Total snapshots: ${storageStats.snapshotCount}`)
console.log(`Total size: ${storageStats.totalSize} bytes`)
```

#### Automatic Snapshots

Configure automatic snapshot creation with `snapshotConfig`:

```typescript
const db = new ParqueDB({
  storage: new MemoryBackend(),
  snapshotConfig: {
    autoSnapshotThreshold: 100, // Create snapshot every 100 events
  },
})
```

---

## Index Management

ParqueDB provides methods for managing secondary indexes.

### createIndex(ns, definition)

Create a new index on a collection. Supports hash indexes for equality queries, SST indexes for range queries, FTS indexes for full-text search, and vector indexes for similarity search.

```typescript
createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `ns` | `string` | Collection namespace |
| `definition` | `IndexDefinition` | Index definition with type, fields, and options |

#### Example

```typescript
// Create a hash index for fast equality lookups
await db.createIndex('orders', {
  name: 'idx_status',
  type: 'hash',
  fields: [{ path: 'status' }]
})

// Create an FTS index for full-text search
await db.createIndex('articles', {
  name: 'idx_fts_content',
  type: 'fts',
  fields: [{ path: 'title' }, { path: 'body' }],
  options: { language: 'english' }
})
```

See [Secondary Indexes](architecture/secondary-indexes.md) for complete documentation.

### dropIndex(ns, indexName)

Drop an index and remove it from the collection.

```typescript
dropIndex(ns: string, indexName: string): Promise<void>
```

#### Example

```typescript
await db.dropIndex('orders', 'idx_status')
```

### listIndexes(ns)

List all indexes for a namespace with their metadata.

```typescript
listIndexes(ns: string): Promise<IndexMetadata[]>
```

#### Example

```typescript
const indexes = await db.listIndexes('orders')
for (const index of indexes) {
  console.log(`${index.name} (${index.type}): ${index.fields.map(f => f.path).join(', ')}`)
}
```

### getIndex(ns, indexName)

Get metadata for a specific index.

```typescript
getIndex(ns: string, indexName: string): Promise<IndexMetadata | null>
```

#### Example

```typescript
const index = await db.getIndex('orders', 'idx_status')
if (index) {
  console.log('Index type:', index.type)
  console.log('Indexed fields:', index.fields)
}
```

### rebuildIndex(ns, indexName)

Rebuild an index by reprocessing all documents in the collection.

```typescript
rebuildIndex(ns: string, indexName: string): Promise<void>
```

#### Example

```typescript
// Rebuild the FTS index after schema changes
await db.rebuildIndex('articles', 'idx_fts_content')
```

### getIndexStats(ns, indexName)

Get statistics for an index including size, entry count, and performance metrics.

```typescript
getIndexStats(ns: string, indexName: string): Promise<IndexStats>
```

#### Example

```typescript
const stats = await db.getIndexStats('orders', 'idx_status')
console.log('Index size:', stats.size)
console.log('Entry count:', stats.entryCount)
```

### getIndexManager()

Get the index manager instance for advanced use cases and direct index manipulation.

```typescript
getIndexManager(): IndexManager
```

#### Example

```typescript
const indexManager = db.getIndexManager()
// Use for advanced index operations
```

---

## CRUD Operations

For complete CRUD operation documentation, see the [Collection API Reference](api/collection.md).

ParqueDB provides both namespace-based and collection-based CRUD methods:

### Namespace-based Operations

```typescript
// Find
await db.find('posts', { status: 'published' })

// Get
await db.get('posts', 'abc123')

// Create
await db.create('posts', { $type: 'Post', name: 'My Post', title: 'Hello' })

// Update
await db.update('posts', 'abc123', { $set: { title: 'Updated' } })

// Delete
await db.delete('posts', 'abc123')
```

### Collection-based Operations

```typescript
// Same operations via collection
const posts = db.collection('posts')
await posts.find({ status: 'published' })
await posts.get('abc123')
await posts.create({ title: 'Hello' })
await posts.update('abc123', { $set: { title: 'Updated' } })
await posts.delete('abc123')
```

### Proxy-based Operations

```typescript
// Access via proxy (automatic collection creation)
await db.Posts.find({ status: 'published' })
await db.Posts.get('abc123')
await db.Posts.create({ title: 'Hello' })
await db.Posts.update('abc123', { $set: { title: 'Updated' } })
await db.Posts.delete('abc123')
```

---

## Additional Methods

### deleteMany(namespace, filter, options)

Delete multiple entities matching a filter. Supports both soft and hard deletes.

```typescript
deleteMany(namespace: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace |
| `filter` | `Filter` | MongoDB-style filter to match entities |
| `options` | `DeleteOptions` | Delete options |

#### DeleteOptions

```typescript
interface DeleteOptions {
  soft?: boolean     // Soft delete (default: true)
  actor?: EntityId   // Actor performing the delete
}
```

#### DeleteResult

```typescript
interface DeleteResult {
  deletedCount: number  // Number of entities deleted
}
```

#### Example

```typescript
// Soft delete all draft posts
const result = await db.deleteMany('posts', {
  status: 'draft'
}, {
  soft: true,
  actor: 'users/admin' as EntityId
})

console.log(`Deleted ${result.deletedCount} draft posts`)

// Hard delete all posts older than 1 year
const hardDelete = await db.deleteMany('posts', {
  createdAt: { $lt: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000) }
}, {
  soft: false
})

console.log(`Permanently deleted ${hardDelete.deletedCount} old posts`)
```

### restore(namespace, id, options)

Restore a soft-deleted entity by removing the `deletedAt` and `deletedBy` fields.

```typescript
restore<T>(namespace: string, id: string, options?: { actor?: EntityId }): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace |
| `id` | `string` | Entity ID |
| `options.actor` | `EntityId` | Actor performing the restore |

#### Returns

The restored entity, or `null` if the entity doesn't exist or wasn't soft-deleted.

#### Example

```typescript
// Soft delete a post
await db.delete('posts', 'abc123', { soft: true })

// Later, restore it
const restored = await db.restore('posts', 'abc123', {
  actor: 'users/admin' as EntityId
})

if (restored) {
  console.log('Post restored:', restored)
} else {
  console.log('Post not found or not soft-deleted')
}
```

### upsert(namespace, filter, update, options)

Find and update an entity, or create if not found. If multiple entities match the filter, only the first one is updated.

```typescript
upsert<T>(namespace: string, filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' }): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace |
| `filter` | `Filter` | MongoDB-style filter to find existing entity |
| `update` | `UpdateInput<T>` | Update operations to apply |
| `options.returnDocument` | `'before' \| 'after'` | Which version to return (default: 'after') |

#### Example

```typescript
// Update status or create new order
const order = await db.upsert('orders',
  { orderNumber: 'ORD-12345' },
  {
    $set: { status: 'shipped' },
    $setOnInsert: {
      $type: 'Order',
      name: 'Order 12345',
      createdAt: new Date()
    }
  }
)
```

### upsertMany(namespace, items, options)

Upsert multiple entities in a single operation with support for ordered/unordered execution and per-item optimistic concurrency.

```typescript
upsertMany<T>(namespace: string, items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult>
```

#### Types

```typescript
interface UpsertManyItem<T> {
  filter: Filter
  update: UpdateInput<T>
  options?: {
    expectedVersion?: number
  }
}

interface UpsertManyOptions {
  ordered?: boolean  // Stop on first error if true (default: true)
  actor?: EntityId   // Actor performing the operation
}

interface UpsertManyResult {
  ok: boolean
  insertedCount: number
  modifiedCount: number
  matchedCount: number
  upsertedCount: number
  upsertedIds: EntityId[]
  errors: UpsertManyError[]
}
```

#### Example

```typescript
const result = await db.upsertMany('products', [
  {
    filter: { sku: 'PROD-001' },
    update: { $set: { price: 29.99 }, $inc: { inventory: 10 } }
  },
  {
    filter: { sku: 'PROD-002' },
    update: { $set: { price: 39.99 }, $setOnInsert: { name: 'New Product' } }
  }
], { ordered: false })

console.log(`Inserted: ${result.insertedCount}, Modified: ${result.modifiedCount}`)
```

### getRelated(namespace, id, relationField, options)

Get related entities with pagination support, filtering, sorting, and projection.

```typescript
getRelated<T>(namespace: string, id: string, relationField: string, options?: GetRelatedOptions): Promise<GetRelatedResult<T>>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace |
| `id` | `string` | Entity ID |
| `relationField` | `string` | Relationship field name |
| `options` | `GetRelatedOptions` | Query options |

#### GetRelatedOptions

```typescript
interface GetRelatedOptions {
  cursor?: string           // Cursor for pagination
  limit?: number           // Maximum results
  filter?: Filter          // Filter related entities
  sort?: SortSpec          // Sort order
  project?: Projection     // Field projection
  includeDeleted?: boolean // Include soft-deleted entities
}
```

#### Example

```typescript
// Get all posts by a user with pagination
const result = await db.getRelated('users', 'user123', 'posts', {
  filter: { status: 'published' },
  sort: { createdAt: -1 },
  limit: 10
})

console.log(`Found ${result.total} posts, showing ${result.items.length}`)
if (result.hasMore) {
  console.log(`Next cursor: ${result.nextCursor}`)
}
```

### history(entityId, options)

Get the history of changes for an entity with support for pagination and filtering by operation type.

```typescript
history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `entityId` | `EntityId` | Full entity ID (e.g., 'posts/abc123') |
| `options` | `HistoryOptions` | History query options |

#### HistoryOptions

```typescript
interface HistoryOptions {
  limit?: number      // Maximum results
  cursor?: string     // Cursor for pagination
  op?: EventOp       // Filter by operation type
}
```

#### Example

```typescript
// Get all changes to a post
const history = await db.history('posts/abc123' as EntityId, {
  limit: 20
})

for (const item of history.items) {
  console.log(`${item.op} at ${item.ts} by ${item.actor}`)
  console.log('Before:', item.before)
  console.log('After:', item.after)
}

// Get only UPDATE operations
const updates = await db.history('posts/abc123' as EntityId, {
  op: 'UPDATE'
})
```

### getHistory(namespace, id, options)

Convenience method for `history()` that accepts namespace and ID separately instead of a full EntityId.

```typescript
getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult>
```

#### Example

```typescript
// Using getHistory (namespace + id)
const history1 = await db.getHistory('posts', 'abc123', { limit: 10 })

// Equivalent to using history with full EntityId
const history2 = await db.history('posts/abc123' as EntityId, { limit: 10 })
```

### getAtVersion(namespace, id, version)

Get an entity at a specific version number. Reconstructs the entity state by replaying events up to the specified version.

```typescript
getAtVersion<T>(namespace: string, id: string, version: number): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Collection namespace |
| `id` | `string` | Entity ID |
| `version` | `number` | Version number to retrieve |

#### Returns

The entity at the specified version, or `null` if the entity didn't exist at that version.

#### Example

```typescript
// Get a post at version 5
const postV5 = await db.getAtVersion('posts', 'abc123', 5)

if (postV5) {
  console.log('Post at version 5:', postV5)
  console.log('Title:', postV5.title)
}

// Compare with current version
const current = await db.get('posts', 'abc123')
console.log('Current version:', current?.version)
```

### diff(entityId, t1, t2)

Compute the difference between entity states at two timestamps, showing added, removed, and changed fields with before/after values.

```typescript
diff(entityId: EntityId, t1: Date, t2: Date): Promise<DiffResult>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `entityId` | `EntityId` | Full entity ID (e.g., 'posts/abc123') |
| `t1` | `Date` | First timestamp (typically earlier) |
| `t2` | `Date` | Second timestamp (typically later) |

#### DiffResult

```typescript
interface DiffResult {
  added: string[]    // Fields added between t1 and t2
  removed: string[]  // Fields removed between t1 and t2
  changed: string[]  // Fields that changed values
  values: {          // Before/after values for all changes
    [field: string]: { before: unknown; after: unknown }
  }
}
```

#### Example

```typescript
// Compare post state between two dates
const diff = await db.diff(
  'posts/abc123' as EntityId,
  new Date('2024-01-01'),
  new Date('2024-02-01')
)

console.log('Added fields:', diff.added)
console.log('Removed fields:', diff.removed)
console.log('Changed fields:', diff.changed)

// Show all changes
for (const field of diff.changed) {
  const { before, after } = diff.values[field]!
  console.log(`${field}: ${before} -> ${after}`)
}
```

### revert(entityId, targetTime, options)

Revert an entity to its state at a specific timestamp. This creates a new UPDATE event with revert metadata.

```typescript
revert<T>(entityId: EntityId, targetTime: Date, options?: RevertOptions): Promise<Entity<T>>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `entityId` | `EntityId` | Full entity ID (e.g., 'posts/abc123') |
| `targetTime` | `Date` | Timestamp to revert to (must be in the past) |
| `options` | `RevertOptions` | Revert options |

#### RevertOptions

```typescript
interface RevertOptions {
  actor?: EntityId  // Actor performing the revert
}
```

#### Example

```typescript
// Revert a post to its state yesterday
const revertedPost = await db.revert(
  'posts/abc123' as EntityId,
  new Date(Date.now() - 24 * 60 * 60 * 1000),
  { actor: 'users/admin' as EntityId }
)

console.log('Reverted to:', revertedPost)

// Verify the revert in history
const history = await db.history('posts/abc123' as EntityId, { limit: 1 })
console.log('Latest event metadata:', history.items[0]?.metadata) // { revert: true }
```

---

## Error Handling

### VersionConflictError

Thrown when optimistic concurrency check fails.

```typescript
class VersionConflictError extends Error {
  expectedVersion: number
  actualVersion: number | undefined
}
```

#### Example

```typescript
import { VersionConflictError } from 'parquedb'

try {
  await db.update('posts', 'abc123', {
    $set: { title: 'Updated' },
  }, {
    expectedVersion: 5,
  })
} catch (error) {
  if (error instanceof VersionConflictError) {
    console.log(`Expected version ${error.expectedVersion}, got ${error.actualVersion}`)
    // Retry with current version
  }
}
```

---

## Performance Considerations

### Snapshots

Snapshots improve time-travel query performance by avoiding full event replay. Configure `autoSnapshotThreshold` to automatically create snapshots after a certain number of events.

### Indexes

Use secondary indexes to speed up queries on frequently-accessed fields. FTS indexes enable full-text search, while vector indexes support semantic similarity queries.

### Event Log Management

Configure `eventLogConfig` to manage event log size. Archive old events to reduce memory usage while preserving historical data.

---

## See Also

- [Collection API Reference](api/collection.md) - Complete Collection interface documentation
- [Getting Started](getting-started.md) - Quick start guide
- [Schema Definition](schemas.md) - Complete schema reference
- [Query API](queries.md) - MongoDB-style filtering
- [Update Operators](updates.md) - All update operators
- [Graph-First Architecture](architecture/graph-first-architecture.md) - How relationships work
- [Secondary Indexes](architecture/secondary-indexes.md) - Index types and usage
