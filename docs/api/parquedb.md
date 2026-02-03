# ParqueDB Class API Reference

The `ParqueDB` class is the main entry point for interacting with ParqueDB. It provides both explicit namespace-based methods and proxy-based collection access.

## Constructor

```typescript
new ParqueDB(config: ParqueDBConfig)
```

### ParqueDBConfig

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `storage` | `StorageBackend` | Yes | Storage backend for data persistence |
| `schema` | `Schema` | No | Schema definition for entity validation |
| `defaultNamespace` | `string` | No | Default namespace for operations |
| `snapshotConfig` | `SnapshotConfig` | No | Configuration for automatic snapshots |

### SnapshotConfig

| Property | Type | Description |
|----------|------|-------------|
| `autoSnapshotThreshold` | `number` | Automatically create snapshot after this many events |

### Example

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
      email: 'email!',
      posts: '<- Post.author[]',
    },
  },
  snapshotConfig: {
    autoSnapshotThreshold: 100,
  },
})
```

---

## Collection Access

ParqueDB supports two patterns for accessing collections:

### Explicit Collection Access

```typescript
// Get a collection by namespace
const posts = db.collection('posts')
await posts.find({ status: 'published' })

// Or use namespace-based methods directly
await db.find('posts', { status: 'published' })
```

### Proxy-based Collection Access

```typescript
// Access collections as properties (PascalCase or lowercase)
await db.Posts.find({ status: 'published' })
await db.Users.get('users/123')
```

---

## Core Methods

### registerSchema

Register a schema for validation. Schemas define entity types, field types, and relationships.

```typescript
registerSchema(schema: Schema): void
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `schema` | `Schema` | Schema definition object |

#### Example

```typescript
db.registerSchema({
  Post: {
    $ns: 'posts',
    title: 'string!',
    content: 'text',
    status: 'string = "draft"',
    author: '-> User.posts',
  },
})
```

---

### collection

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

---

### find

Find entities matching a filter.

```typescript
find<T = Record<string, unknown>>(
  namespace: string,
  filter?: Filter,
  options?: FindOptions
): Promise<PaginatedResult<Entity<T>>>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `filter` | `Filter` | MongoDB-style filter (optional) |
| `options` | `FindOptions` | Query options (optional) |

#### FindOptions

| Property | Type | Description |
|----------|------|-------------|
| `sort` | `SortSpec` | Sort order (e.g., `{ createdAt: -1 }`) |
| `limit` | `number` | Maximum number of results |
| `skip` | `number` | Number of results to skip |
| `cursor` | `string` | Cursor for pagination |
| `project` | `Projection` | Field projection |
| `populate` | `PopulateSpec` | Populate related entities |
| `includeDeleted` | `boolean` | Include soft-deleted entities |
| `asOf` | `Date` | Time-travel: query as of specific time |
| `explain` | `boolean` | Explain query plan without executing |
| `hint` | `string \| object` | Hint for index to use |
| `maxTimeMs` | `number` | Maximum time in milliseconds |

#### Returns

```typescript
interface PaginatedResult<T> {
  items: T[]
  total?: number
  nextCursor?: string
  hasMore: boolean
}
```

#### Examples

```typescript
// Simple filter
const result = await db.find('posts', { status: 'published' })

// With operators
const result = await db.find('posts', {
  score: { $gte: 100 },
  status: { $in: ['published', 'featured'] },
})

// With pagination and sorting
const result = await db.find('posts', {}, {
  sort: { createdAt: -1 },
  limit: 20,
  cursor: lastCursor,
})

// Time-travel query
const result = await db.find('posts', {}, {
  asOf: new Date('2024-01-01'),
})
```

---

### get

Get a single entity by ID.

```typescript
get<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  options?: GetOptions
): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `id` | `string` | Entity ID (can be full 'ns/id' or just 'id') |
| `options` | `GetOptions` | Get options (optional) |

#### GetOptions

| Property | Type | Description |
|----------|------|-------------|
| `includeDeleted` | `boolean` | Include soft-deleted entity |
| `asOf` | `Date` | Time-travel: get entity as of specific time |
| `hydrate` | `string[]` | Hydrate related entities (fetch full entity) |
| `maxInbound` | `number` | Maximum inbound references to inline |
| `project` | `Projection` | Field projection |

#### Returns

`Entity<T> | null` - The entity or null if not found.

#### Examples

```typescript
// Get by ID
const post = await db.get('posts', 'posts/abc123')

// Get with hydrated relationships
const post = await db.get('posts', 'abc123', {
  hydrate: ['author', 'categories'],
})

// Get historical state
const post = await db.get('posts', 'abc123', {
  asOf: new Date('2024-01-01'),
})
```

---

### create

Create a new entity.

```typescript
create<T = Record<string, unknown>>(
  namespace: string,
  data: CreateInput<T>,
  options?: CreateOptions
): Promise<Entity<T>>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `data` | `CreateInput<T>` | Entity data |
| `options` | `CreateOptions` | Create options (optional) |

#### CreateInput

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `$type` | `string` | Yes | Entity type name |
| `name` | `string` | Yes | Human-readable display name |
| `...fields` | `unknown` | No | Additional data fields |

#### CreateOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Actor performing the create (for audit) |
| `skipValidation` | `boolean` | Skip schema validation |
| `returnDocument` | `boolean` | Return the created entity (default: true) |

#### Returns

`Entity<T>` - The created entity with generated `$id`, timestamps, and version.

#### Example

```typescript
const post = await db.create('posts', {
  $type: 'Post',
  name: 'My First Post',
  title: 'Hello World',
  content: 'This is my first post.',
  status: 'draft',
}, {
  actor: 'users/admin' as EntityId,
})

console.log(post.$id)       // 'posts/abc123'
console.log(post.version)   // 1
console.log(post.createdAt) // Date
```

---

### update

Update an existing entity.

```typescript
update<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  update: UpdateInput<T>,
  options?: UpdateOptions
): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `id` | `string` | Entity ID |
| `update` | `UpdateInput<T>` | Update operations |
| `options` | `UpdateOptions` | Update options (optional) |

#### UpdateInput

See [Update Operators](#update-operators) section for all available operators.

#### UpdateOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Actor performing the update (for audit) |
| `expectedVersion` | `number` | Expected version for optimistic concurrency |
| `upsert` | `boolean` | Create if not exists |
| `returnDocument` | `'before' \| 'after'` | Return the document before or after update |
| `skipValidation` | `boolean` | Skip schema validation |
| `arrayFilters` | `Filter[]` | Array filters for positional updates |

#### Returns

`Entity<T> | null` - The updated entity or null if not found.

#### Examples

```typescript
// Set fields
const post = await db.update('posts', 'posts/abc123', {
  $set: { status: 'published', publishedAt: new Date() },
})

// Increment counter
await db.update('posts', 'abc123', {
  $inc: { viewCount: 1 },
})

// With optimistic concurrency
await db.update('posts', 'abc123', {
  $set: { title: 'Updated Title' },
}, {
  expectedVersion: 5,
})

// Upsert
await db.update('posts', 'new-post', {
  $set: { title: 'New Post' },
  $setOnInsert: { status: 'draft' },
}, {
  upsert: true,
})
```

---

### delete

Delete an entity (soft delete by default).

```typescript
delete(
  namespace: string,
  id: string,
  options?: DeleteOptions
): Promise<DeleteResult>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `id` | `string` | Entity ID |
| `options` | `DeleteOptions` | Delete options (optional) |

#### DeleteOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Actor performing the delete (for audit) |
| `hard` | `boolean` | Hard delete (permanent, skip soft delete) |
| `expectedVersion` | `number` | Expected version for optimistic concurrency |

#### Returns

```typescript
interface DeleteResult {
  deletedCount: number
}
```

#### Examples

```typescript
// Soft delete (default)
const result = await db.delete('posts', 'posts/abc123')

// Hard delete (permanent)
await db.delete('posts', 'abc123', { hard: true })

// With optimistic concurrency
await db.delete('posts', 'abc123', {
  expectedVersion: 5,
})
```

---

### deleteMany

Delete multiple entities matching a filter.

```typescript
deleteMany(
  namespace: string,
  filter: Filter,
  options?: DeleteOptions
): Promise<DeleteResult>
```

#### Parameters

| Name | Type | Description |
|------|------|-------------|
| `namespace` | `string` | Target namespace |
| `filter` | `Filter` | Filter to match entities |
| `options` | `DeleteOptions` | Delete options (optional) |

#### Example

```typescript
// Delete all drafts
const result = await db.deleteMany('posts', { status: 'draft' })
console.log(result.deletedCount) // Number of deleted entities
```

---

### restore

Restore a soft-deleted entity.

```typescript
restore<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  options?: { actor?: EntityId }
): Promise<Entity<T> | null>
```

#### Example

```typescript
const post = await db.restore('posts', 'posts/abc123')
```

---

## Upsert Operations

### upsert

Find and update an entity, or create if not found.

```typescript
upsert<T = Record<string, unknown>>(
  namespace: string,
  filter: Filter,
  update: UpdateInput<T>,
  options?: { returnDocument?: 'before' | 'after' }
): Promise<Entity<T> | null>
```

#### Example

```typescript
// Find by email or create new user
const user = await db.upsert('users',
  { email: 'user@example.com' },
  {
    $set: { lastLogin: new Date() },
    $setOnInsert: {
      $type: 'User',
      name: 'New User',
      role: 'member',
    },
  }
)
```

---

### upsertMany

Upsert multiple entities in a single operation.

```typescript
upsertMany<T = Record<string, unknown>>(
  namespace: string,
  items: UpsertManyItem<T>[],
  options?: UpsertManyOptions
): Promise<UpsertManyResult>
```

#### UpsertManyItem

| Property | Type | Description |
|----------|------|-------------|
| `filter` | `Filter` | Filter to find existing document |
| `update` | `UpdateInput<T>` | Update operations to apply |
| `options.expectedVersion` | `number` | Expected version for optimistic concurrency |

#### UpsertManyOptions

| Property | Type | Description |
|----------|------|-------------|
| `ordered` | `boolean` | Stop on first error if true (default: true) |
| `actor` | `EntityId` | Actor performing the operation |

#### UpsertManyResult

| Property | Type | Description |
|----------|------|-------------|
| `ok` | `boolean` | Whether all operations succeeded |
| `insertedCount` | `number` | Number of documents inserted |
| `modifiedCount` | `number` | Number of documents modified |
| `matchedCount` | `number` | Number of documents matched |
| `upsertedCount` | `number` | Number of documents upserted (inserted) |
| `upsertedIds` | `EntityId[]` | IDs of upserted documents |
| `errors` | `UpsertManyError[]` | Errors that occurred |

#### Example

```typescript
const result = await db.upsertMany('users', [
  {
    filter: { email: 'alice@example.com' },
    update: { $set: { name: 'Alice' } },
  },
  {
    filter: { email: 'bob@example.com' },
    update: { $set: { name: 'Bob' } },
  },
])
```

---

## Relationship Methods

### getRelated

Get related entities with pagination support.

```typescript
getRelated<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  relationField: string,
  options?: GetRelatedOptions
): Promise<GetRelatedResult<T>>
```

#### GetRelatedOptions

| Property | Type | Description |
|----------|------|-------------|
| `cursor` | `string` | Cursor for pagination |
| `limit` | `number` | Maximum results |
| `filter` | `Filter` | Filter related entities |
| `sort` | `SortSpec` | Sort order |
| `project` | `Projection` | Field projection |
| `includeDeleted` | `boolean` | Include soft-deleted |

#### GetRelatedResult

| Property | Type | Description |
|----------|------|-------------|
| `items` | `Entity<T>[]` | Related entities |
| `total` | `number` | Total count of related entities |
| `hasMore` | `boolean` | Whether there are more results |
| `nextCursor` | `string` | Cursor for next page |

#### Example

```typescript
// Get posts by a user
const posts = await db.getRelated('users', 'users/123', 'posts', {
  limit: 10,
  sort: { createdAt: -1 },
})
```

---

## History and Time-Travel

### history

Get the history of changes for an entity.

```typescript
history(
  entityId: EntityId,
  options?: HistoryOptions
): Promise<HistoryResult>
```

#### HistoryOptions

| Property | Type | Description |
|----------|------|-------------|
| `from` | `Date` | Start of time range |
| `to` | `Date` | End of time range |
| `limit` | `number` | Maximum number of events |
| `cursor` | `string` | Cursor for pagination |
| `op` | `'CREATE' \| 'UPDATE' \| 'DELETE'` | Filter by operation type |
| `actor` | `EntityId` | Filter by actor |

#### HistoryResult

| Property | Type | Description |
|----------|------|-------------|
| `items` | `HistoryItem[]` | History items |
| `hasMore` | `boolean` | Whether there are more results |
| `nextCursor` | `string` | Cursor for next page |

#### HistoryItem

| Property | Type | Description |
|----------|------|-------------|
| `id` | `string` | Event ID |
| `ts` | `Date` | Timestamp |
| `op` | `EventOp` | Operation type |
| `entityId` | `string` | Entity ID |
| `ns` | `string` | Namespace |
| `before` | `Entity \| null` | State before change |
| `after` | `Entity \| null` | State after change |
| `actor` | `EntityId` | Who made the change |
| `metadata` | `Record<string, unknown>` | Additional metadata |

#### Example

```typescript
const history = await db.history('posts/abc123' as EntityId, {
  limit: 50,
  op: 'UPDATE',
})

for (const item of history.items) {
  console.log(`${item.op} at ${item.ts} by ${item.actor}`)
}
```

---

### getHistory

Alias for `history` with namespace-based ID resolution.

```typescript
getHistory(
  namespace: string,
  id: string,
  options?: HistoryOptions
): Promise<HistoryResult>
```

---

### getAtVersion

Get an entity at a specific version number.

```typescript
getAtVersion<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  version: number
): Promise<Entity<T> | null>
```

#### Example

```typescript
// Get the entity as it was at version 3
const post = await db.getAtVersion('posts', 'abc123', 3)
```

---

### diff

Compute the difference between entity states at two timestamps.

```typescript
diff(
  entityId: EntityId,
  t1: Date,
  t2: Date
): Promise<DiffResult>
```

#### DiffResult

| Property | Type | Description |
|----------|------|-------------|
| `added` | `string[]` | Fields that were added |
| `removed` | `string[]` | Fields that were removed |
| `changed` | `string[]` | Fields that were changed |
| `values` | `object` | Before/after values for changed fields |

#### Example

```typescript
const changes = await db.diff(
  'posts/abc123' as EntityId,
  new Date('2024-01-01'),
  new Date('2024-02-01')
)

console.log('Added fields:', changes.added)
console.log('Changed fields:', changes.changed)
for (const field of changes.changed) {
  const { before, after } = changes.values[field]
  console.log(`  ${field}: ${before} -> ${after}`)
}
```

---

### revert

Revert an entity to its state at a specific timestamp.

```typescript
revert<T = Record<string, unknown>>(
  entityId: EntityId,
  targetTime: Date,
  options?: RevertOptions
): Promise<Entity<T>>
```

#### RevertOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Actor performing the revert |

#### Example

```typescript
// Revert to yesterday's state
const post = await db.revert(
  'posts/abc123' as EntityId,
  new Date(Date.now() - 24 * 60 * 60 * 1000)
)
```

---

## Transactions

### beginTransaction

Begin a transaction for atomic operations.

```typescript
beginTransaction(): ParqueDBTransaction
```

#### ParqueDBTransaction

| Method | Description |
|--------|-------------|
| `create(namespace, data, options)` | Create entity within transaction |
| `update(namespace, id, update, options)` | Update entity within transaction |
| `delete(namespace, id, options)` | Delete entity within transaction |
| `commit()` | Commit all changes |
| `rollback()` | Rollback all changes |

#### Example

```typescript
const tx = db.beginTransaction()

try {
  const post = await tx.create('posts', {
    $type: 'Post',
    name: 'New Post',
    title: 'Transaction Example',
  })

  await tx.update('users', 'users/123', {
    $inc: { postCount: 1 },
  })

  await tx.commit()
} catch (error) {
  await tx.rollback()
  throw error
}
```

---

## Event Log

### getEventLog

Get the event log interface for querying events.

```typescript
getEventLog(): EventLog
```

#### EventLog Interface

| Method | Description |
|--------|-------------|
| `getEvents(entityId)` | Get events for a specific entity |
| `getEventsByNamespace(ns)` | Get events by namespace |
| `getEventsByTimeRange(from, to)` | Get events by time range |
| `getEventsByOp(op)` | Get events by operation type |
| `getRawEvent(id)` | Get raw event data |

#### Example

```typescript
const eventLog = db.getEventLog()

// Get all events for an entity
const events = await eventLog.getEvents('posts/abc123' as EntityId)

// Get events in a time range
const recentEvents = await eventLog.getEventsByTimeRange(
  new Date('2024-01-01'),
  new Date()
)
```

---

## Snapshot Management

### getSnapshotManager

Get the snapshot manager for manual snapshot operations.

```typescript
getSnapshotManager(): SnapshotManager
```

#### SnapshotManager Interface

| Method | Description |
|--------|-------------|
| `createSnapshot(entityId)` | Create a snapshot of current state |
| `createSnapshotAtEvent(entityId, eventId)` | Create snapshot at specific event |
| `listSnapshots(entityId)` | List all snapshots for an entity |
| `deleteSnapshot(snapshotId)` | Delete a snapshot |
| `pruneSnapshots(options)` | Prune old snapshots |
| `getRawSnapshot(snapshotId)` | Get raw snapshot data |
| `getQueryStats(entityId)` | Get query statistics |
| `getStorageStats()` | Get storage statistics |

#### Example

```typescript
const snapshots = db.getSnapshotManager()

// Create a snapshot
const snapshot = await snapshots.createSnapshot('posts/abc123' as EntityId)

// List snapshots
const list = await snapshots.listSnapshots('posts/abc123' as EntityId)

// Prune old snapshots
await snapshots.pruneSnapshots({
  olderThan: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
  keepMinimum: 5,
})
```

---

## Index Management

### createIndex

Create a new index on a namespace.

```typescript
createIndex(
  ns: string,
  definition: IndexDefinition
): Promise<IndexMetadata>
```

#### IndexDefinition

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | Index name |
| `type` | `'hash' \| 'sst' \| 'fts'` | Index type |
| `fields` | `IndexField[]` | Fields to index |

#### Examples

```typescript
// Create a hash index for equality lookups
await db.createIndex('orders', {
  name: 'idx_status',
  type: 'hash',
  fields: [{ path: 'status' }],
})

// Create an SST index for range queries
await db.createIndex('products', {
  name: 'idx_price',
  type: 'sst',
  fields: [{ path: 'price' }],
})

// Create an FTS index for full-text search
await db.createIndex('articles', {
  name: 'idx_fts_content',
  type: 'fts',
  fields: [{ path: 'title' }, { path: 'body' }],
})
```

---

### dropIndex

Drop an index.

```typescript
dropIndex(ns: string, indexName: string): Promise<void>
```

---

### listIndexes

List all indexes for a namespace.

```typescript
listIndexes(ns: string): Promise<IndexMetadata[]>
```

---

### getIndex

Get metadata for a specific index.

```typescript
getIndex(ns: string, indexName: string): Promise<IndexMetadata | null>
```

---

### rebuildIndex

Rebuild an index.

```typescript
rebuildIndex(ns: string, indexName: string): Promise<void>
```

---

### getIndexStats

Get statistics for an index.

```typescript
getIndexStats(ns: string, indexName: string): Promise<IndexStats>
```

---

### getIndexManager

Get the index manager instance for advanced use cases.

```typescript
getIndexManager(): IndexManager
```

---

## Update Operators

ParqueDB supports MongoDB-style update operators:

### Field Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$set` | Set field values | `{ $set: { status: 'published' } }` |
| `$unset` | Remove fields | `{ $unset: { tempField: '' } }` |
| `$rename` | Rename fields | `{ $rename: { oldName: 'newName' } }` |
| `$setOnInsert` | Set only on insert | `{ $setOnInsert: { createdBy: 'system' } }` |

### Numeric Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$inc` | Increment | `{ $inc: { viewCount: 1 } }` |
| `$mul` | Multiply | `{ $mul: { price: 1.1 } }` |
| `$min` | Set to minimum | `{ $min: { lowScore: 50 } }` |
| `$max` | Set to maximum | `{ $max: { highScore: 100 } }` |

### Array Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$push` | Push to array | `{ $push: { tags: 'new' } }` |
| `$pull` | Remove from array | `{ $pull: { tags: 'old' } }` |
| `$addToSet` | Add unique to array | `{ $addToSet: { tags: 'unique' } }` |
| `$pop` | Remove first/last | `{ $pop: { queue: 1 } }` |

### Relationship Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$link` | Add relationship | `{ $link: { author: 'users/123' } }` |
| `$unlink` | Remove relationship | `{ $unlink: { author: 'users/123' } }` |

### Date Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$currentDate` | Set to current date | `{ $currentDate: { updatedAt: true } }` |

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
try {
  await db.update('posts', 'abc123', {
    $set: { title: 'Updated' },
  }, {
    expectedVersion: 5,
  })
} catch (error) {
  if (error instanceof VersionConflictError) {
    console.log(`Expected version ${error.expectedVersion}, got ${error.actualVersion}`)
  }
}
```

---

## Type Definitions

### Entity

```typescript
// Entity type properly uses TData for typed data fields
type Entity<TData = Record<string, unknown>> = EntityRef & AuditFields & TData & {
  [key: string]: unknown
}

interface EntityRef {
  $id: EntityId
  $type: string
  name: string
}

interface AuditFields {
  createdAt: Date
  createdBy: EntityId
  updatedAt: Date
  updatedBy: EntityId
  deletedAt?: Date
  deletedBy?: EntityId
  version: number
}
```

### EntityId

```typescript
type EntityId = `${string}/${string}` & { readonly __brand: unique symbol }
```

### Filter

```typescript
interface Filter {
  [field: string]: FieldFilter | undefined
  $and?: Filter[]
  $or?: Filter[]
  $not?: Filter
  $nor?: Filter[]
  $text?: TextOperator['$text']
  $vector?: VectorOperator['$vector']
  $geo?: GeoOperator['$geo']
}
```
