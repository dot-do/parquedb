---
title: Collection Class
description: Fluent API for working with entities in a specific namespace. Provides find, create, update, delete, and upsert operations.
---

The `Collection` class provides a fluent API for working with entities in a specific namespace. Collections are obtained via `db.collection(namespace)` or through proxy-based access like `db.Posts`.

## Overview

```typescript
interface Collection<T = Record<string, unknown>> {
  readonly namespace: string
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>
  get(id: string, options?: GetOptions): Promise<Entity<T> | null>
  create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(id: string, options?: DeleteOptions): Promise<DeleteResult>
  deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
  upsert(filter: Filter, update: UpdateInput<T>, options?: UpsertOptions): Promise<Entity<T> | null>
  upsertMany(items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult>
}
```

## Obtaining a Collection

### Explicit Access

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({ storage: new MemoryBackend() })

// Get a typed collection
interface Post {
  title: string
  content: string
  status: 'draft' | 'published'
}

const posts = db.collection<Post>('posts')
```

### Proxy-based Access

```typescript
// Access via PascalCase property
const posts = db.Posts

// Access via lowercase property
const users = db.users
```

---

## Properties

### namespace

The namespace identifier for this collection.

```typescript
readonly namespace: string
```

#### Example

```typescript
const posts = db.collection('posts')
console.log(posts.namespace) // 'posts'
```

---

## Methods

### find

Find entities matching a filter with optional pagination and sorting.

```typescript
find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | `Filter` | No | MongoDB-style filter query |
| `options` | `FindOptions` | No | Query options for pagination, sorting, etc. |

#### FindOptions

| Property | Type | Description |
|----------|------|-------------|
| `sort` | `SortSpec` | Sort order, e.g., `{ createdAt: -1 }` for descending |
| `limit` | `number` | Maximum number of results to return |
| `skip` | `number` | Number of results to skip (offset-based pagination) |
| `cursor` | `string` | Cursor for cursor-based pagination |
| `project` | `Projection` | Field projection (include/exclude fields) |
| `populate` | `PopulateSpec` | Populate related entities |
| `includeDeleted` | `boolean` | Include soft-deleted entities (default: false) |
| `asOf` | `Date` | Time-travel: query as of a specific timestamp |
| `explain` | `boolean` | Return query plan instead of executing |
| `hint` | `string \| object` | Hint for index selection |
| `maxTimeMs` | `number` | Maximum execution time in milliseconds |

#### Returns

```typescript
interface PaginatedResult<T> {
  items: T[]           // Array of matching entities
  total?: number       // Total count (if available)
  nextCursor?: string  // Cursor for next page
  hasMore: boolean     // Whether more results exist
}
```

#### Examples

```typescript
// Find all entities
const all = await posts.find()

// Find with simple equality filter
const published = await posts.find({ status: 'published' })

// Find with operators
const popular = await posts.find({
  status: 'published',
  viewCount: { $gte: 1000 },
})

// Find with logical operators
const featured = await posts.find({
  $or: [
    { featured: true },
    { viewCount: { $gte: 10000 } },
  ],
})

// Find with pagination
const page1 = await posts.find({}, { limit: 20 })
const page2 = await posts.find({}, {
  limit: 20,
  cursor: page1.nextCursor
})

// Find with sorting
const recent = await posts.find({}, {
  sort: { createdAt: -1 },
  limit: 10,
})

// Find with projection
const titles = await posts.find({}, {
  project: { title: 1, status: 1 },
})

// Find with time-travel
const pastState = await posts.find({}, {
  asOf: new Date('2024-01-01'),
})

// Find including deleted
const withDeleted = await posts.find({}, {
  includeDeleted: true,
})
```

---

### findOne

Find a single entity matching a filter.

```typescript
findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | `Filter` | No | MongoDB-style filter query |
| `options` | `FindOptions` | No | Query options |

#### Returns

`Entity<T> | null` - The first matching entity or null if not found.

#### Examples

```typescript
// Find first matching entity
const post = await posts.findOne({ slug: 'hello-world' })

// Find with sorting (get the most recent)
const latest = await posts.findOne({}, {
  sort: { createdAt: -1 },
})

// Find by email (unique field)
const user = await users.findOne({ email: 'alice@example.com' })
```

---

### get

Get a single entity by its ID.

```typescript
get(id: string, options?: GetOptions): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | `string` | Yes | Entity ID (full 'ns/id' or just 'id') |
| `options` | `GetOptions` | No | Get options |

#### GetOptions

| Property | Type | Description |
|----------|------|-------------|
| `includeDeleted` | `boolean` | Include if soft-deleted |
| `asOf` | `Date` | Time-travel: get state at specific time |
| `hydrate` | `string[]` | Hydrate related entities |
| `maxInbound` | `number` | Maximum inbound references to inline |
| `project` | `Projection` | Field projection |

#### Returns

`Entity<T> | null` - The entity or null if not found.

#### Examples

```typescript
// Get by full ID
const post = await posts.get('posts/abc123')

// Get by short ID (namespace is inferred)
const post = await posts.get('abc123')

// Get with hydrated relationships
const post = await posts.get('abc123', {
  hydrate: ['author', 'categories'],
})

// Get historical state
const oldPost = await posts.get('abc123', {
  asOf: new Date('2024-01-01'),
})

// Get including deleted
const deletedPost = await posts.get('abc123', {
  includeDeleted: true,
})

// Get with projection
const titleOnly = await posts.get('abc123', {
  project: { title: 1 },
})
```

---

### create

Create a new entity in the collection.

```typescript
create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `data` | `CreateInput<T>` | Yes | Entity data to create |
| `options` | `CreateOptions` | No | Create options |

#### CreateInput

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `$type` | `string` | Yes | Entity type name (e.g., 'Post', 'User') |
| `name` | `string` | Yes | Human-readable display name |
| `...fields` | `unknown` | No | Additional data fields |

#### CreateOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Who is creating the entity (for audit trail) |
| `skipValidation` | `boolean` | Skip schema validation |
| `returnDocument` | `boolean` | Return created entity (default: true) |

#### Returns

`Entity<T>` - The created entity with:
- `$id`: Generated unique identifier (format: 'namespace/id')
- `createdAt` / `updatedAt`: Timestamps
- `createdBy` / `updatedBy`: Actor references
- `version`: 1 (initial version)

#### Examples

```typescript
// Create a basic entity
const post = await posts.create({
  $type: 'Post',
  name: 'My First Post',
  title: 'Hello World',
  content: 'This is my first post!',
  status: 'draft',
})

console.log(post.$id)       // 'posts/abc123'
console.log(post.version)   // 1
console.log(post.createdAt) // Date

// Create with actor for audit
const post = await posts.create({
  $type: 'Post',
  name: 'Admin Post',
  title: 'Announcement',
  content: 'Important news...',
}, {
  actor: 'users/admin' as EntityId,
})

// Create with relationships
const post = await posts.create({
  $type: 'Post',
  name: 'Tech Article',
  title: 'Database Design',
  author: { 'Alice': 'users/alice' },
  categories: {
    'Technology': 'categories/tech',
    'Databases': 'categories/db',
  },
})
```

---

### update

Update an existing entity.

```typescript
update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | `string` | Yes | Entity ID to update |
| `update` | `UpdateInput<T>` | Yes | Update operations |
| `options` | `UpdateOptions` | No | Update options |

#### UpdateInput

Update operations using MongoDB-style operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `$set` | Set field values | `{ $set: { status: 'published' } }` |
| `$unset` | Remove fields | `{ $unset: { tempField: '' } }` |
| `$inc` | Increment numeric field | `{ $inc: { viewCount: 1 } }` |
| `$mul` | Multiply numeric field | `{ $mul: { price: 1.1 } }` |
| `$min` | Set to minimum value | `{ $min: { lowScore: score } }` |
| `$max` | Set to maximum value | `{ $max: { highScore: score } }` |
| `$push` | Push to array | `{ $push: { tags: 'new' } }` |
| `$pull` | Remove from array | `{ $pull: { tags: 'old' } }` |
| `$addToSet` | Add unique to array | `{ $addToSet: { tags: 'unique' } }` |
| `$pop` | Remove first/last from array | `{ $pop: { queue: 1 } }` |
| `$currentDate` | Set to current date | `{ $currentDate: { updatedAt: true } }` |
| `$link` | Add relationship | `{ $link: { author: 'users/123' } }` |
| `$unlink` | Remove relationship | `{ $unlink: { author: 'users/123' } }` |
| `$setOnInsert` | Set only on insert (with upsert) | `{ $setOnInsert: { createdBy: 'system' } }` |

#### UpdateOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Who is updating (for audit trail) |
| `expectedVersion` | `number` | Expected version for optimistic locking |
| `upsert` | `boolean` | Create if entity doesn't exist |
| `returnDocument` | `'before' \| 'after'` | Return state before or after update |
| `skipValidation` | `boolean` | Skip schema validation |
| `arrayFilters` | `Filter[]` | Filters for positional array updates |

#### Returns

`Entity<T> | null` - Updated entity, or null if not found (and upsert is false).

#### Examples

```typescript
// Simple field update
const post = await posts.update('abc123', {
  $set: { status: 'published', publishedAt: new Date() },
})

// Increment counter
await posts.update('abc123', {
  $inc: { viewCount: 1 },
})

// Multiple operators
await posts.update('abc123', {
  $set: { status: 'featured' },
  $inc: { featureCount: 1 },
  $currentDate: { lastFeaturedAt: true },
})

// Array operations
await posts.update('abc123', {
  $push: { tags: 'featured' },
  $addToSet: { categories: 'popular' },
})

// Push with modifiers
await posts.update('abc123', {
  $push: {
    comments: {
      $each: [newComment1, newComment2],
      $position: 0,  // Insert at beginning
      $slice: 100,   // Keep only 100 items
    },
  },
})

// Relationship operations
await posts.update('abc123', {
  $link: {
    author: 'users/alice' as EntityId,
    categories: ['categories/tech', 'categories/db'] as EntityId[],
  },
})

// Unlink relationships
await posts.update('abc123', {
  $unlink: { categories: 'categories/old' as EntityId },
})

// With optimistic concurrency
try {
  await posts.update('abc123', {
    $set: { title: 'New Title' },
  }, {
    expectedVersion: 5,
  })
} catch (error) {
  if (error instanceof VersionConflictError) {
    console.log('Conflict detected, please retry')
  }
}

// Upsert (create if not exists)
await posts.update('new-post', {
  $set: { title: 'New Post' },
  $setOnInsert: {
    $type: 'Post',
    name: 'New Post',
    status: 'draft',
  },
}, {
  upsert: true,
})

// Return document before update
const original = await posts.update('abc123', {
  $set: { status: 'archived' },
}, {
  returnDocument: 'before',
})
```

---

### delete

Delete an entity (soft delete by default).

```typescript
delete(id: string, options?: DeleteOptions): Promise<DeleteResult>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | `string` | Yes | Entity ID to delete |
| `options` | `DeleteOptions` | No | Delete options |

#### DeleteOptions

| Property | Type | Description |
|----------|------|-------------|
| `actor` | `EntityId` | Who is deleting (for audit trail) |
| `hard` | `boolean` | Permanently delete (skip soft delete) |
| `expectedVersion` | `number` | Expected version for optimistic locking |

#### Returns

```typescript
interface DeleteResult {
  deletedCount: number  // Number of entities deleted (0 or 1)
}
```

#### Examples

```typescript
// Soft delete (default)
const result = await posts.delete('abc123')
console.log(result.deletedCount) // 1

// Hard delete (permanent)
await posts.delete('abc123', { hard: true })

// With optimistic concurrency
await posts.delete('abc123', {
  expectedVersion: 5,
})

// With actor for audit trail
await posts.delete('abc123', {
  actor: 'users/admin' as EntityId,
})
```

---

### deleteMany

Delete multiple entities matching a filter.

```typescript
deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | `Filter` | Yes | Filter to match entities to delete |
| `options` | `DeleteOptions` | No | Delete options |

#### Returns

```typescript
interface DeleteResult {
  deletedCount: number  // Total number of entities deleted
}
```

#### Examples

```typescript
// Delete all drafts
const result = await posts.deleteMany({ status: 'draft' })
console.log(`Deleted ${result.deletedCount} drafts`)

// Delete old posts
await posts.deleteMany({
  createdAt: { $lt: new Date('2023-01-01') },
})

// Hard delete spam
await posts.deleteMany(
  { spam: true },
  { hard: true }
)
```

---

### upsert

Find and update an entity, or create it if not found.

```typescript
upsert(
  filter: Filter,
  update: UpdateInput<T>,
  options?: { returnDocument?: 'before' | 'after' }
): Promise<Entity<T> | null>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | `Filter` | Yes | Filter to find existing entity |
| `update` | `UpdateInput<T>` | Yes | Update operations |
| `options` | `object` | No | Options |

#### Returns

`Entity<T> | null` - The updated or created entity.

#### Behavior

1. Find entity matching filter
2. If found: apply update operations
3. If not found: create new entity with:
   - Non-operator fields from filter
   - Values from `$set`
   - Values from `$setOnInsert`

#### Examples

```typescript
// Upsert user by email
const user = await users.upsert(
  { email: 'alice@example.com' },
  {
    $set: { lastLogin: new Date() },
    $setOnInsert: {
      $type: 'User',
      name: 'Alice',
      role: 'member',
    },
  }
)

// Upsert with increment
const stats = await stats.upsert(
  { date: today },
  {
    $inc: { pageViews: 1 },
    $setOnInsert: {
      $type: 'DailyStats',
      name: `Stats ${today}`,
    },
  }
)
```

---

### upsertMany

Upsert multiple entities in a single operation.

```typescript
upsertMany(
  items: UpsertManyItem<T>[],
  options?: UpsertManyOptions
): Promise<UpsertManyResult>
```

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `items` | `UpsertManyItem<T>[]` | Yes | Array of upsert items |
| `options` | `UpsertManyOptions` | No | Batch options |

#### UpsertManyItem

```typescript
interface UpsertManyItem<T> {
  filter: Filter                    // Filter to find existing
  update: UpdateInput<T>            // Update operations
  options?: {
    expectedVersion?: number        // Optimistic concurrency
  }
}
```

#### UpsertManyOptions

| Property | Type | Description |
|----------|------|-------------|
| `ordered` | `boolean` | Stop on first error (default: true) |
| `actor` | `EntityId` | Actor for all operations |

#### UpsertManyResult

```typescript
interface UpsertManyResult {
  ok: boolean                 // All operations succeeded
  insertedCount: number       // New entities created
  modifiedCount: number       // Existing entities updated
  matchedCount: number        // Entities matched by filters
  upsertedCount: number       // Total upserted (inserted)
  upsertedIds: EntityId[]     // IDs of new entities
  errors: UpsertManyError[]   // Errors that occurred
}

interface UpsertManyError {
  index: number               // Index of failed item
  filter: Filter              // Filter that was used
  error: Error                // The error
}
```

#### Examples

```typescript
// Bulk upsert users
const result = await users.upsertMany([
  {
    filter: { email: 'alice@example.com' },
    update: {
      $set: { name: 'Alice Smith' },
      $setOnInsert: { $type: 'User', role: 'member' },
    },
  },
  {
    filter: { email: 'bob@example.com' },
    update: {
      $set: { name: 'Bob Jones' },
      $setOnInsert: { $type: 'User', role: 'member' },
    },
  },
])

console.log(`Inserted: ${result.insertedCount}`)
console.log(`Modified: ${result.modifiedCount}`)

// Continue on errors
const result = await users.upsertMany(items, {
  ordered: false,  // Don't stop on first error
})

if (!result.ok) {
  for (const error of result.errors) {
    console.error(`Item ${error.index} failed:`, error.error.message)
  }
}
```

---

## Filter Operators

Collections support MongoDB-style filter operators:

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal | `{ status: { $eq: 'published' } }` |
| `$ne` | Not equal | `{ status: { $ne: 'draft' } }` |
| `$gt` | Greater than | `{ score: { $gt: 100 } }` |
| `$gte` | Greater than or equal | `{ score: { $gte: 100 } }` |
| `$lt` | Less than | `{ score: { $lt: 50 } }` |
| `$lte` | Less than or equal | `{ score: { $lte: 50 } }` |
| `$in` | In array | `{ status: { $in: ['draft', 'review'] } }` |
| `$nin` | Not in array | `{ status: { $nin: ['deleted'] } }` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$and` | All conditions match | `{ $and: [{ a: 1 }, { b: 2 }] }` |
| `$or` | Any condition matches | `{ $or: [{ a: 1 }, { b: 2 }] }` |
| `$not` | Negation | `{ $not: { status: 'draft' } }` |
| `$nor` | None match | `{ $nor: [{ a: 1 }, { b: 2 }] }` |

### String Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$regex` | Regular expression | `{ title: { $regex: '^Hello' } }` |
| `$startsWith` | Starts with prefix | `{ title: { $startsWith: 'Hello' } }` |
| `$endsWith` | Ends with suffix | `{ title: { $endsWith: '!' } }` |
| `$contains` | Contains substring | `{ title: { $contains: 'world' } }` |

### Array Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$all` | Array contains all | `{ tags: { $all: ['a', 'b'] } }` |
| `$elemMatch` | Element matches | `{ items: { $elemMatch: { qty: { $gt: 5 } } } }` |
| `$size` | Array size | `{ tags: { $size: 3 } }` |

### Existence Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$exists` | Field exists | `{ email: { $exists: true } }` |
| `$type` | Field type check | `{ data: { $type: 'object' } }` |

### Special Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$text` | Full-text search | `{ $text: { $search: 'hello world' } }` |
| `$vector` | Vector similarity | `{ $vector: { $near: vec, $k: 10 } }` |
| `$geo` | Geospatial query | `{ $geo: { $near: { lng, lat } } }` |

---

## Type Definitions

### Entity

```typescript
// Entity type properly uses TData for typed data fields
type Entity<TData = Record<string, unknown>> = EntityRef & AuditFields & TData & {
  [key: string]: unknown
}

interface EntityRef {
  $id: EntityId          // Full entity ID (namespace/id)
  $type: string          // Entity type name
  name: string           // Display name
}

interface AuditFields {
  createdAt: Date        // Creation timestamp
  createdBy: EntityId    // Creator ID
  updatedAt: Date        // Last update timestamp
  updatedBy: EntityId    // Last updater ID
  deletedAt?: Date       // Soft delete timestamp
  deletedBy?: EntityId   // Who deleted
  version: number        // Version number
}
```

### SortSpec

```typescript
interface SortSpec {
  [field: string]: 1 | -1 | 'asc' | 'desc'
}

// Examples:
{ createdAt: -1 }           // Descending
{ createdAt: 'desc' }       // Same as above
{ name: 1, createdAt: -1 }  // Multi-field sort
```

### Projection

```typescript
interface Projection {
  [field: string]: 0 | 1 | boolean
}

// Examples:
{ title: 1, content: 1 }  // Include only these
{ password: 0 }           // Exclude this field
```

### PopulateSpec

```typescript
type PopulateSpec =
  | string[]                                    // ['author', 'categories']
  | { [predicate: string]: boolean | PopulateOptions }

interface PopulateOptions {
  limit?: number
  sort?: SortSpec
  cursor?: string
  filter?: Filter
  populate?: PopulateSpec  // Nested populate
}
```

---

## Error Handling

### VersionConflictError

Thrown when optimistic concurrency check fails.

```typescript
import { VersionConflictError } from 'parquedb'

try {
  await posts.update('abc123',
    { $set: { title: 'New' } },
    { expectedVersion: 5 }
  )
} catch (error) {
  if (error instanceof VersionConflictError) {
    console.log(`Expected ${error.expectedVersion}, got ${error.actualVersion}`)
    // Handle conflict (retry, merge, etc.)
  }
}
```

### Validation Errors

Thrown when entity data fails schema validation.

```typescript
try {
  await posts.create({
    $type: 'Post',
    name: 'Test',
    // Missing required 'title' field
  })
} catch (error) {
  console.log(error.message) // 'Missing required field: title'
}
```

---

## Best Practices

### Use TypeScript Generics

```typescript
interface Post {
  title: string
  content: string
  status: 'draft' | 'published'
  viewCount: number
}

const posts = db.collection<Post>('posts')

// Now you get type safety
const post = await posts.create({
  $type: 'Post',
  name: 'My Post',
  title: 'Hello',
  content: 'World',
  status: 'draft',  // TypeScript validates this
  viewCount: 0,
})
```

### Handle Pagination

```typescript
async function* getAllPosts() {
  let cursor: string | undefined

  while (true) {
    const result = await posts.find({}, {
      limit: 100,
      cursor,
      sort: { createdAt: -1 },
    })

    for (const post of result.items) {
      yield post
    }

    if (!result.hasMore) break
    cursor = result.nextCursor
  }
}

for await (const post of getAllPosts()) {
  console.log(post.title)
}
```

### Optimistic Concurrency

```typescript
async function updateWithRetry(id: string, update: UpdateInput, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const current = await posts.get(id)
    if (!current) throw new Error('Not found')

    try {
      return await posts.update(id, update, {
        expectedVersion: current.version,
      })
    } catch (error) {
      if (error instanceof VersionConflictError && attempt < maxRetries - 1) {
        continue // Retry
      }
      throw error
    }
  }
}
```

### Batch Operations

```typescript
// Prefer upsertMany for bulk operations
const result = await posts.upsertMany(
  items.map(item => ({
    filter: { externalId: item.id },
    update: {
      $set: item.data,
      $setOnInsert: { $type: 'Post', name: item.title },
    },
  })),
  { ordered: false }  // Continue on errors
)
```
