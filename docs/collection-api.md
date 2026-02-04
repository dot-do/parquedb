---
title: Collection API
description: Complete API reference for ParqueDB Collection class methods
---

# Collection API Reference

The Collection class provides a MongoDB-style API for querying, mutating, and managing entities in ParqueDB. Each collection represents a namespace of entities with a consistent schema.

**Important:** This documentation describes the Collection API interface, which is implemented by:
- **Standalone `Collection` class** (`src/Collection.ts`) - In-memory implementation for testing and development
- **`ParqueDB` collections** (`src/ParqueDB/`) - Production implementation with persistent storage backends

For production use, always instantiate collections via the `ParqueDB` class with a storage backend. The standalone `Collection` class is useful for unit tests and quick prototyping but does not persist data.

## Overview

Collections are accessed via the database instance using property notation:

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

// For production use with persistent storage
const db = new ParqueDB({ storage: new MemoryBackend() })

const posts = db.Posts        // Collection<Post>
const users = db.Users        // Collection<User>
const comments = db.Comments  // Collection<Comment>
```

Or explicitly created with `collection()`:

```typescript
const posts = db.collection('posts')
```

**Note:** The standalone `Collection` class exported from the package is a simplified in-memory implementation intended for testing and development only. For production use with persistent storage backends (MemoryBackend, FsBackend, R2Backend, etc.), always use the `ParqueDB` class which properly integrates with storage backends.

## Query Methods

### find(filter?, options?)

Query multiple entities matching a filter.

**Signature:**
```typescript
async find(filter?: Filter, options?: FindOptions<T>): Promise<ResultArray<Entity<T>>>
```

**Parameters:**
- `filter` (optional): MongoDB-style filter criteria
- `options` (optional): Query options for sorting, pagination, projection

**Returns:** `ResultArray<T>` - a standard array with additional metadata properties via Proxy:
- `$total`: Total count of matching entities (optional, may require `{ countTotal: true }`)
- `$next`: Cursor for next page (if more results exist)
- `$prev`: Cursor for previous page (if navigating backwards)

The result is a standard JavaScript array, so you can iterate directly without `.items`.

**Examples:**

```typescript
// Find all posts - iterate directly
const allPosts = await db.Posts.find()
for (const post of allPosts) {
  console.log(post.title)
}

// Find with filter
const published = await db.Posts.find({ status: 'published' })
console.log(`Found ${published.$total} published posts`)

// Find with options
const recent = await db.Posts.find(
  { status: 'published' },
  {
    sort: { createdAt: -1 },
    limit: 10,
    skip: 0
  }
)

// With projection (select specific fields)
const titles = await db.Posts.find(
  { status: 'published' },
  { project: { title: 1, author: 1 } }
)

// Cursor-based pagination using $next
const page1 = await db.Posts.find(
  { status: 'published' },
  { limit: 20 }
)
console.log(`Page 1: ${page1.length} items, total: ${page1.$total}`)

if (page1.$next) {
  const page2 = await db.Posts.find(
    { status: 'published' },
    { limit: 20, cursor: page1.$next }
  )
}

// Complex filter with operators
const filtered = await db.Posts.find({
  viewCount: { $gte: 100 },
  tags: { $in: ['javascript', 'typescript'] },
  createdAt: { $gt: new Date('2024-01-01') }
})
```

**Find Options:**
- `sort`: Sort specification (e.g., `{ createdAt: -1, title: 1 }`)
- `limit`: Maximum number of results
- `skip`: Number of results to skip (offset pagination)
- `cursor`: Entity ID to start after (cursor pagination)
- `project`: Field projection (inclusion or exclusion)
- `populate`: Populate related entities (experimental)
- `includeDeleted`: Include soft-deleted entities (default: false)
- `asOf`: Time-travel query (retrieve historical state)

See [Query API](./queries.md) for detailed filter operator documentation.

---

### findOne(filter?, options?)

Find a single entity matching the filter.

**Signature:**
```typescript
async findOne(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T> | null>
```

**Parameters:**
- `filter` (optional): MongoDB-style filter criteria
- `options` (optional): Query options (same as `find`)

**Returns:** Single matching entity or `null` if not found

**Examples:**

```typescript
// Find first matching document
const post = await db.Posts.findOne({ slug: 'hello-world' })

if (post) {
  console.log(post.title)
} else {
  console.log('Post not found')
}

// Find with options
const latest = await db.Posts.findOne(
  { status: 'published' },
  { sort: { createdAt: -1 } }
)

// With projection
const author = await db.Posts.findOne(
  { $id: 'posts/123' },
  { project: { author: 1, createdAt: 1 } }
)
```

---

### get(id, options?)

Get an entity by ID with auto-hydrated relationships.

**Signature:**
```typescript
async get(id: string, options?: GetOptions): Promise<Entity<T> | null>
```

**Parameters:**
- `id`: Entity ID (with or without namespace prefix). If no namespace prefix is provided (no `/`), the collection's namespace is automatically prepended. When using `$id` directive, you can use the short ID (e.g., `'hello-world'` instead of `'post/hello-world'`).
- `options` (optional): Get options

**Returns:** Entity with relationships auto-hydrated, or `null` if not found

**Auto-Hydrated Relationships:**

Entities returned from `get()` have their relationships automatically populated:

- **Forward relationships** (`-> Target`): Fully hydrated with the related entity
- **Reverse relationships** (`<- Target.field[]`): Arrays with `$total`, `$next` metadata

```typescript
// Schema with $id directive
const db = DB({
  User: {
    $id: 'email',
    $name: 'name',
    email: 'string!#',
    name: 'string!',
    posts: '<- Post.author[]'
  },
  Post: {
    $id: 'slug',
    $name: 'title',
    slug: 'string!#',
    title: 'string!',
    author: '-> User'
  }
})

// Get by short ID (when $id directive is set)
const post = await db.Post.get('hello-world')

// Forward relationships are fully hydrated
console.log(post.author.name)     // 'Alice' - not just an ID!
console.log(post.author.email)    // 'alice@example.com'

// Reverse relationships are arrays with pagination metadata
const user = await db.User.get('alice@example.com')
console.log(user.posts.$total)    // Total count of posts
for (const p of user.posts) {
  console.log(p.title)            // First 10 posts (default limit)
}
if (user.posts.$next) {
  // More posts available via cursor
}
```

**Examples:**

```typescript
// Get by full ID
const post = await db.Posts.get('posts/abc123')

// Get by short ID (namespace inferred, or using $id field value)
const post = await db.Posts.get('hello-world')

// With projection
const post = await db.Posts.get('abc123', {
  project: { title: 1, content: 1 }
})

// Include soft-deleted
const deletedPost = await db.Posts.get('abc123', {
  includeDeleted: true
})

// Time-travel: get historical state
const pastState = await db.Posts.get('abc123', {
  asOf: new Date('2024-01-01')
})

// Limit inbound relationships
const post = await db.Posts.get('abc123', {
  maxInbound: 5  // Only include first 5 inbound references
})
```

**Get Options:**
- `includeDeleted`: Include soft-deleted entity
- `asOf`: Time-travel to specific point in time
- `project`: Field projection
- `maxInbound`: Maximum inbound relationships to include (default: 10)

**Manual Relationship Traversal:**

For more control, you can also traverse relationships manually:

```typescript
const post = await db.Posts.get('abc123')

// Traverse outbound relationships with options
const categories = await post.related('categories', {
  limit: 10,
  sort: { name: 1 }
})

// Traverse inbound relationships with filtering
const comments = await post.referencedBy('comments', {
  filter: { status: 'approved' },
  limit: 20,
  sort: { createdAt: -1 }
})

// Results are also ResultArrays
console.log(categories.$total)    // Total count
console.log(categories.$next)     // Cursor for next page
```

---

### findPaginated(filter?, options?)

Find entities with paginated result metadata.

**Signature:**
```typescript
async findPaginated(
  filter?: Filter,
  options?: FindOptions<T>
): Promise<PaginatedResult<Entity<T>>>
```

**Parameters:**
- `filter` (optional): MongoDB-style filter criteria
- `options` (optional): Query options (same as `find`)

**Returns:** Paginated result with metadata

**Result Shape:**
```typescript
interface PaginatedResult<T> {
  items: T[]           // Current page items
  total?: number       // Total matching documents (optional)
  hasMore: boolean     // Whether more results exist
  nextCursor?: string  // Cursor for next page
}
```

**Examples:**

```typescript
// Basic pagination
const page1 = await db.Posts.findPaginated(
  { status: 'published' },
  { limit: 20 }
)

console.log(page1.items)      // First 20 posts
console.log(page1.total)      // Total published posts
console.log(page1.hasMore)    // true if more than 20
console.log(page1.nextCursor) // 'posts/xyz' for next page

// Get next page
if (page1.hasMore) {
  const page2 = await db.Posts.findPaginated(
    { status: 'published' },
    { limit: 20, cursor: page1.nextCursor }
  )
}

// With sorting
const sorted = await db.Posts.findPaginated(
  { featured: true },
  {
    sort: { viewCount: -1 },
    limit: 10
  }
)
```

---

### count(filter?)

Count entities matching a filter.

**Signature:**
```typescript
async count(filter?: Filter): Promise<number>
```

**Parameters:**
- `filter` (optional): MongoDB-style filter criteria

**Returns:** Count of matching entities

**Examples:**

```typescript
// Count all posts
const total = await db.Posts.count()

// Count with filter
const published = await db.Posts.count({ status: 'published' })

// Complex filter
const recentPopular = await db.Posts.count({
  createdAt: { $gte: new Date('2024-01-01') },
  viewCount: { $gte: 1000 }
})
```

---

### countDocuments(filter?)

Alias for `count()`. Provided for MongoDB compatibility.

**Signature:**
```typescript
async countDocuments(filter?: Filter): Promise<number>
```

---

### estimatedDocumentCount()

Get estimated document count (fast, approximate).

**Signature:**
```typescript
async estimatedDocumentCount(): Promise<number>
```

**Returns:** Approximate count (excludes soft-deleted by default)

**Examples:**

```typescript
// Fast count (uses collection statistics)
const approx = await db.Posts.estimatedDocumentCount()
console.log(`Approximately ${approx} posts`)
```

---

### exists(id)

Check if an entity exists by ID.

**Signature:**
```typescript
async exists(id: string): Promise<boolean>
```

**Parameters:**
- `id`: Entity ID (with or without namespace prefix)

**Returns:** `true` if entity exists, `false` otherwise

**Examples:**

```typescript
// Check existence
const exists = await db.Posts.exists('abc123')
if (exists) {
  console.log('Post exists')
} else {
  console.log('Post not found')
}

// Use to avoid try/catch
if (await db.Posts.exists(postId)) {
  const post = await db.Posts.get(postId)
  // ... work with post
}
```

---

## Mutation Methods

### create(data, options?)

Create a new entity.

**Signature:**
```typescript
async create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
```

**Parameters:**
- `data`: Entity data including `$type` and `name`
- `options` (optional): Create options

**Returns:** Created entity with generated ID and audit fields

**Examples:**

```typescript
// Basic create
const post = await db.Posts.create({
  $type: 'Post',
  name: 'hello-world',
  title: 'Hello World',
  content: 'My first post!'
})

console.log(post.$id)       // 'posts/abc123'
console.log(post.createdAt) // Date
console.log(post.createdBy) // 'system/system'
console.log(post.version)   // 1

// With custom ID
const user = await db.Users.create({
  $id: 'alice',  // Will become 'users/alice'
  $type: 'User',
  name: 'alice',
  email: 'alice@example.com'
})

// With relationships (object format with display names as keys)
const post = await db.Posts.create({
  $type: 'Post',
  name: 'my-post',
  title: 'My Post',
  author: { 'Alice': 'users/alice' },
  categories: {
    'Tech': 'categories/tech',
    'Database': 'categories/db'
  }
})

// Alternatively, use $link operator after creation
const post = await db.Posts.create({
  $type: 'Post',
  name: 'my-post',
  title: 'My Post'
})
await db.Posts.update(post.$id, {
  $link: {
    author: 'users/alice',
    categories: ['categories/tech', 'categories/db']
  }
})

// With actor for audit
const post = await db.Posts.create(
  {
    $type: 'Post',
    name: 'authored-post',
    title: 'Authored Post'
  },
  { actor: 'users/alice' }
)
console.log(post.createdBy) // 'users/alice'
```

**Create Options:**
- `actor`: Entity ID of user performing the create (for audit)
- `skipValidation`: Skip schema validation
- `validateOnWrite`: Validation mode ('strict' | 'permissive' | 'warn')
- `returnDocument`: Return the created entity (default: true)

**Required Fields:**
- `$type`: Entity type (e.g., 'Post', 'User')
- `name`: Human-readable name (used for display and relationships)

**Auto-Generated Fields:**
- `$id`: Unique entity ID (ULID format)
- `createdAt`: Creation timestamp
- `createdBy`: Creator entity ID
- `updatedAt`: Last update timestamp (same as createdAt on create)
- `updatedBy`: Last updater entity ID
- `version`: Optimistic concurrency version (starts at 1)

---

### createMany(dataArray)

Create multiple entities in a single operation.

**Note:** This method is not yet implemented in the current Collection API. Use individual `create()` calls in a loop or see the "Bulk Operations" section for workarounds.

**Planned Signature:**
```typescript
async createMany(dataArray: CreateInput<T>[]): Promise<Entity<T>[]>
```

**Planned Parameters:**
- `dataArray`: Array of entity data objects

**Planned Usage:**

```typescript
// Create multiple posts (when implemented)
const posts = await db.Posts.createMany([
  {
    $type: 'Post',
    name: 'post-1',
    title: 'First Post',
    content: 'Content 1'
  },
  {
    $type: 'Post',
    name: 'post-2',
    title: 'Second Post',
    content: 'Content 2'
  },
  {
    $type: 'Post',
    name: 'post-3',
    title: 'Third Post',
    content: 'Content 3'
  }
])

console.log(posts.length) // 3
console.log(posts[0].$id) // 'posts/...'
```

**Current Workaround:**

```typescript
// Use individual creates in a loop
const posts: Entity<Post>[] = []
for (const data of postDataArray) {
  const post = await db.Posts.create(data)
  posts.push(post)
}

// Or use Promise.all for parallel creation
const posts = await Promise.all(
  postDataArray.map(data => db.Posts.create(data))
)
```

---

### update(id, update, options?)

Update an entity by ID.

**Signature:**
```typescript
async update(
  id: string,
  update: UpdateInput<T>,
  options?: UpdateOptions
): Promise<UpdateResult>
```

**Parameters:**
- `id`: Entity ID (with or without namespace prefix)
- `update`: Update operators object
- `options` (optional): Update options

**Returns:** Result with matched and modified counts

**Result Shape:**
```typescript
interface UpdateResult {
  matchedCount: number   // Number of entities matched (0 or 1)
  modifiedCount: number  // Number of entities modified (0 or 1)
}
```

**Examples:**

```typescript
// Set field values
const result = await db.Posts.update('abc123', {
  $set: {
    status: 'published',
    publishedAt: new Date()
  }
})
console.log(result.modifiedCount) // 1

// Increment counter
await db.Posts.update('abc123', {
  $inc: { viewCount: 1 }
})

// Push to array
await db.Posts.update('abc123', {
  $push: { tags: 'featured' }
})

// Multiple operators
await db.Posts.update('abc123', {
  $set: { status: 'published' },
  $inc: { version: 1 },
  $currentDate: { updatedAt: true },
  $push: { tags: 'breaking' }
})

// Add relationships
await db.Posts.update('abc123', {
  $link: {
    author: 'users/alice',
    categories: ['categories/tech', 'categories/db']
  }
})

// Remove relationships
await db.Posts.update('abc123', {
  $unlink: {
    categories: 'categories/old-category'
  }
})

// With optimistic concurrency
await db.Posts.update(
  'abc123',
  { $set: { title: 'Updated Title' } },
  { expectedVersion: 5 }
)
// Throws if version doesn't match

// Upsert (create if not exists)
await db.Posts.update(
  'new-id',
  {
    $set: {
      $type: 'Post',
      name: 'new-post',
      title: 'New Post'
    }
  },
  { upsert: true }
)
```

**Update Options:**
- `actor`: Entity ID of user performing the update
- `expectedVersion`: Expected version for optimistic concurrency control
- `upsert`: Create entity if it doesn't exist
- `returnDocument`: Return 'before' or 'after' state (not yet implemented)
- `skipValidation`: Skip schema validation
- `validateOnWrite`: Validation mode

**Available Update Operators:**

Field operators:
- `$set`: Set field values
- `$unset`: Remove fields
- `$rename`: Rename fields
- `$setOnInsert`: Set on upsert only

Numeric operators:
- `$inc`: Increment by value
- `$mul`: Multiply by value
- `$min`: Set to minimum
- `$max`: Set to maximum

Array operators:
- `$push`: Add to array
- `$pull`: Remove from array
- `$pullAll`: Remove multiple values
- `$addToSet`: Add if not present
- `$pop`: Remove first (-1) or last (1)

Date operators:
- `$currentDate`: Set to current date

Relationship operators:
- `$link`: Create relationships
- `$unlink`: Remove relationships

Advanced:
- `$bit`: Bitwise operations
- `$embed`: Generate embeddings (ParqueDB AI feature)

See [Update Operators](./updates.md) for detailed documentation.

---

### updateMany(filter, update, options?)

Update multiple entities matching a filter.

**Signature:**
```typescript
async updateMany(
  filter: Filter,
  update: UpdateInput<T>,
  options?: UpdateOptions
): Promise<UpdateResult>
```

**Parameters:**
- `filter`: MongoDB-style filter criteria
- `update`: Update operators object
- `options` (optional): Update options

**Returns:** Result with matched and modified counts

**Examples:**

```typescript
// Update all matching documents
const result = await db.Posts.updateMany(
  { status: 'draft' },
  { $set: { status: 'published' } }
)
console.log(`Published ${result.modifiedCount} posts`)

// Increment counter for all
await db.Posts.updateMany(
  { featured: true },
  { $inc: { priority: 10 } }
)

// Add tag to multiple posts
await db.Posts.updateMany(
  { category: 'tech' },
  { $push: { tags: 'technology' } }
)
```

---

### upsert(filter, data)

Create or update an entity (not yet implemented in current API).

Use `update()` with `{ upsert: true }` option instead:

```typescript
await db.Posts.update(
  'post-id',
  { $set: data },
  { upsert: true }
)
```

---

### delete(id, options?)

Delete an entity by ID (soft delete by default).

**Signature:**
```typescript
async delete(id: string, options?: DeleteOptions): Promise<DeleteResult>
```

**Parameters:**
- `id`: Entity ID (with or without namespace prefix)
- `options` (optional): Delete options

**Returns:** Result with deleted count

**Result Shape:**
```typescript
interface DeleteResult {
  deletedCount: number  // Number of entities deleted (0 or 1)
}
```

**Examples:**

```typescript
// Soft delete (default - sets deletedAt)
const result = await db.Posts.delete('abc123')
console.log(result.deletedCount) // 1

// Hard delete (permanent removal)
await db.Posts.delete('abc123', { hard: true })

// With optimistic concurrency
await db.Posts.delete('abc123', {
  expectedVersion: 5
})
// Throws if version doesn't match

// With actor for audit
await db.Posts.delete('abc123', {
  actor: 'users/alice'
})
```

**Delete Options:**
- `actor`: Entity ID of user performing the delete
- `hard`: Permanent delete (default: false for soft delete)
- `expectedVersion`: Expected version for optimistic concurrency

**Soft Delete Behavior:**

Soft-deleted entities:
- Have `deletedAt` and `deletedBy` fields set
- Are excluded from queries by default
- Can be retrieved with `{ includeDeleted: true }` option
- Increment version number

```typescript
// Entity after soft delete
{
  $id: 'posts/abc123',
  status: 'published',
  deletedAt: Date,      // Added
  deletedBy: EntityId,  // Added
  version: 6            // Incremented
}
```

---

### deleteMany(filter, options?)

Delete multiple entities matching a filter.

**Signature:**
```typescript
async deleteMany(
  filter: Filter,
  options?: DeleteOptions
): Promise<DeleteResult>
```

**Parameters:**
- `filter`: MongoDB-style filter criteria
- `options` (optional): Delete options

**Returns:** Result with deleted count

**Examples:**

```typescript
// Soft delete all matching
const result = await db.Posts.deleteMany(
  { status: 'draft', createdAt: { $lt: new Date('2023-01-01') } }
)
console.log(`Deleted ${result.deletedCount} old drafts`)

// Hard delete all matching
await db.Posts.deleteMany(
  { status: 'spam' },
  { hard: true }
)

// With actor
await db.Posts.deleteMany(
  { author: 'users/bob' },
  { actor: 'users/admin' }
)
```

---

## Aggregation Methods

### aggregate(pipeline, options?)

Execute an aggregation pipeline.

**Signature:**
```typescript
async aggregate<R = unknown>(
  pipeline: AggregationStage[],
  options?: AggregateOptions
): Promise<R[]>
```

**Parameters:**
- `pipeline`: Array of aggregation stages
- `options` (optional): Aggregation options

**Returns:** Array of aggregated results

**Examples:**

```typescript
// Count by status
const statusCounts = await db.Posts.aggregate([
  {
    $group: {
      _id: '$status',
      count: { $sum: 1 }
    }
  }
])
// Result: [{ _id: 'published', count: 50 }, { _id: 'draft', count: 10 }]

// Average view count by author
const avgViews = await db.Posts.aggregate([
  {
    $match: { status: 'published' }
  },
  {
    $group: {
      _id: '$author',
      avgViews: { $avg: '$viewCount' },
      totalPosts: { $sum: 1 }
    }
  },
  {
    $sort: { avgViews: -1 }
  },
  {
    $limit: 10
  }
])

// Complex aggregation with multiple stages
const topAuthors = await db.Posts.aggregate([
  // Filter published posts
  {
    $match: {
      status: 'published',
      createdAt: { $gte: new Date('2024-01-01') }
    }
  },
  // Group by author
  {
    $group: {
      _id: '$author',
      postCount: { $sum: 1 },
      totalViews: { $sum: '$viewCount' },
      avgViews: { $avg: '$viewCount' },
      titles: { $push: '$title' }
    }
  },
  // Add computed field
  {
    $addFields: {
      score: {
        $multiply: ['$postCount', '$avgViews']
      }
    }
  },
  // Sort by score
  {
    $sort: { score: -1 }
  },
  // Top 5
  {
    $limit: 5
  },
  // Reshape output
  {
    $project: {
      author: '$_id',
      posts: '$postCount',
      views: '$totalViews',
      score: 1,
      _id: 0
    }
  }
])
```

**Aggregation Options:**
- `maxTimeMs`: Maximum execution time in milliseconds
- `allowDiskUse`: Allow disk use for large aggregations
- `hint`: Index hint for optimization
- `includeDeleted`: Include soft-deleted entities
- `asOf`: Time-travel aggregation
- `explain`: Explain query plan without executing
- `indexManager`: Index manager for index-aware execution

**Available Aggregation Stages:**

Filtering & matching:
- `$match`: Filter documents
- `$limit`: Limit number of results
- `$skip`: Skip number of results
- `$sample`: Random sample of documents

Grouping & aggregating:
- `$group`: Group by field(s) with accumulators
- `$count`: Count documents in pipeline

Reshaping:
- `$project`: Select/reshape fields
- `$addFields` / `$set`: Add computed fields
- `$unset`: Remove fields
- `$replaceRoot`: Replace document root

Array operations:
- `$unwind`: Deconstruct array field

Sorting:
- `$sort`: Sort documents

Joining (experimental):
- `$lookup`: Join with another collection

**Common Aggregation Patterns:**

```typescript
// Count by category
await db.Posts.aggregate([
  { $group: { _id: '$category', count: { $sum: 1 } } }
])

// Sum of values
await db.Orders.aggregate([
  { $group: { _id: null, total: { $sum: '$amount' } } }
])

// Average, min, max
await db.Products.aggregate([
  {
    $group: {
      _id: null,
      avgPrice: { $avg: '$price' },
      minPrice: { $min: '$price' },
      maxPrice: { $max: '$price' }
    }
  }
])

// Group with multiple fields
await db.Sales.aggregate([
  {
    $group: {
      _id: { year: '$year', quarter: '$quarter' },
      revenue: { $sum: '$amount' },
      count: { $sum: 1 }
    }
  }
])

// Collect unique values
await db.Posts.aggregate([
  { $unwind: '$tags' },
  { $group: { _id: null, allTags: { $addToSet: '$tags' } } }
])
```

**Accumulators:**
- `$sum`: Sum values (use 1 to count)
- `$avg`: Average of values
- `$min`: Minimum value
- `$max`: Maximum value
- `$first`: First value in group
- `$last`: Last value in group
- `$push`: Array of all values
- `$addToSet`: Array of unique values

---

## Relationship Methods

### getRelated(id, predicate, options?)

Get entities related via an outbound relationship (not a direct method, use `get()` then `related()`).

**Usage via get():**

```typescript
const post = await db.Posts.get('abc123')

// Traverse outbound relationship
const categories = await post.related('categories', {
  filter: { active: true },
  sort: { name: 1 },
  limit: 10
})

console.log(categories.items)     // Array of Category entities
console.log(categories.total)     // Total count
console.log(categories.hasMore)   // Boolean
console.log(categories.nextCursor) // Pagination cursor
```

**Options:**
- `filter`: Filter related entities
- `sort`: Sort related entities
- `limit`: Maximum results
- `cursor`: Pagination cursor
- `includeDeleted`: Include soft-deleted
- `asOf`: Time-travel
- `project`: Field projection

---

### addRelation(fromId, predicate, toId)

Add a relationship between entities (use `update()` with `$link`).

**Usage:**

```typescript
// Add single relationship
await db.Posts.update('post-123', {
  $link: { author: 'users/alice' }
})

// Add multiple relationships
await db.Posts.update('post-123', {
  $link: {
    categories: ['categories/tech', 'categories/db']
  }
})
```

---

### removeRelation(fromId, predicate, toId)

Remove a relationship between entities (use `update()` with `$unlink`).

**Usage:**

```typescript
// Remove single relationship
await db.Posts.update('post-123', {
  $unlink: { author: 'users/alice' }
})

// Remove multiple relationships
await db.Posts.update('post-123', {
  $unlink: {
    categories: ['categories/old']
  }
})

// Remove all relationships for predicate
await db.Posts.update('post-123', {
  $unlink: { categories: '$all' }
})
```

---

### referencedBy(reverse, options?)

Get entities that reference this entity (inbound relationships).

**Usage via get():**

```typescript
const user = await db.Users.get('alice')

// Get all posts by this user
const posts = await user.referencedBy('posts', {
  filter: { status: 'published' },
  sort: { createdAt: -1 },
  limit: 20
})

console.log(posts.items)      // Array of Post entities
console.log(posts.total)      // Total count (optional)
console.log(posts.hasMore)    // Boolean
console.log(posts.nextCursor) // Pagination cursor (optional)
```

---

## Index Methods

ParqueDB automatically creates indexes for common patterns. Manual index management is not yet exposed in the Collection API.

**Automatic Indexes:**
- Primary key index on `$id`
- Bloom filters for existence checks
- Column statistics for range queries

**Future Index Methods:**

```typescript
// Create index (planned)
await db.Posts.createIndex(
  { title: 'text', content: 'text' },
  { name: 'post-fulltext' }
)

// Drop index (planned)
await db.Posts.dropIndex('post-fulltext')

// List indexes (planned)
const indexes = await db.Posts.listIndexes()
```

See [Secondary Indexes](./architecture/secondary-indexes.md) for details on index architecture.

---

## Bulk Operations

### bulkWrite(operations)

Execute multiple write operations in a single call (not yet implemented).

**Planned Usage:**

```typescript
const result = await db.Posts.bulkWrite([
  {
    insertOne: {
      document: { $type: 'Post', name: 'post-1', title: 'Post 1' }
    }
  },
  {
    updateOne: {
      filter: { $id: 'posts/123' },
      update: { $set: { status: 'published' } }
    }
  },
  {
    deleteOne: {
      filter: { $id: 'posts/456' }
    }
  }
], {
  ordered: true  // Stop on first error
})

console.log(result.insertedCount)  // 1
console.log(result.modifiedCount)  // 1
console.log(result.deletedCount)   // 1
```

**Current Workaround:**

Use individual operations in sequence:

```typescript
// Create multiple (use individual creates)
const posts: Entity<Post>[] = []
for (const data of postDataArray) {
  const post = await db.Posts.create(data)
  posts.push(post)
}

// Update multiple
for (const id of ids) {
  await db.Posts.update(id, update)
}

// Delete multiple (use deleteMany)
await db.Posts.deleteMany(filter)
```

**Performance Considerations:**

For bulk operations:
- Use `Promise.all()` for parallel creates when order doesn't matter
- Use `updateMany()` or `deleteMany()` when possible to update/delete by filter
- Batch operations in groups of 100-1000 for optimal performance
- Consider using transactions for consistency (when available)

---

## Query Builder

### builder()

Create a fluent query builder for this collection.

**Signature:**
```typescript
builder(): QueryBuilder<T>
```

**Returns:** QueryBuilder instance

**Examples:**

```typescript
// Fluent query building
const results = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .andWhere('score', 'gte', 80)
  .orderBy('createdAt', 'desc')
  .limit(10)
  .find()

// Complex filters
const featured = await db.Posts.builder()
  .where('featured', 'eq', true)
  .andWhere('viewCount', 'gte', 1000)
  .orWhere('author', 'in', ['users/alice', 'users/bob'])
  .find()

// Build filter and options separately
const { filter, options } = db.Posts.builder()
  .where('category', 'in', ['tech', 'science'])
  .orderBy('views', 'desc')
  .select(['title', 'author'])
  .build()

// Then use with find
const posts = await db.Posts.find(filter, options)

// Pagination
const page1 = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .orderBy('createdAt', 'desc')
  .limit(20)
  .find()

const page2 = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .orderBy('createdAt', 'desc')
  .limit(20)
  .cursor(page1[page1.length - 1].$id)
  .find()

// Find one
const latest = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .orderBy('createdAt', 'desc')
  .findOne()

// Count
const publishedCount = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .count()
```

**Builder Methods:**
- `where(field, operator, value)`: Add filter condition
- `andWhere(field, operator, value)`: Add AND condition
- `orWhere(field, operator, value)`: Add OR condition
- `orderBy(field, direction)`: Add sort
- `limit(n)`: Set limit
- `skip(n)`: Set skip
- `cursor(id)`: Set cursor for pagination
- `select(fields)`: Set projection
- `build()`: Build filter and options
- `find()`: Execute query and return results
- `findOne()`: Execute query and return first result or null
- `count()`: Execute count query and return count

---

## Advanced Features

### Time-Travel Queries

Query entity state at any point in time using the event log:

```typescript
// Get historical state
const pastPost = await db.Posts.get('abc123', {
  asOf: new Date('2024-01-01')
})

// Find historical results
const pastPosts = await db.Posts.find(
  { status: 'draft' },
  { asOf: new Date('2024-01-01') }
)

// Aggregate historical data
const pastStats = await db.Posts.aggregate(
  [{ $group: { _id: '$status', count: { $sum: 1 } } }],
  { asOf: new Date('2024-01-01') }
)
```

**Note:** Time-travel queries rely on the event log. The standalone in-memory `Collection` class has basic event log support for testing. For production time-travel with persistent storage, use `ParqueDB` with a storage backend that implements event log persistence.

### Optimistic Concurrency

Use version numbers to prevent conflicting updates:

```typescript
const post = await db.Posts.get('abc123')
console.log(post.version) // 5

// Update with version check
try {
  await db.Posts.update(
    'abc123',
    { $set: { title: 'New Title' } },
    { expectedVersion: 5 }
  )
} catch (err) {
  // Version mismatch - another update happened
  console.error('Concurrent modification detected')
  // Re-fetch and retry
}

// Also works with delete
await db.Posts.delete('abc123', {
  expectedVersion: 6
})
```

### Soft Delete Recovery

Soft-deleted entities can be queried and recovered:

```typescript
// Delete softly
await db.Posts.delete('abc123')

// Find deleted entities
const deleted = await db.Posts.find(
  { deletedAt: { $exists: true } },
  { includeDeleted: true }
)

// Get specific deleted entity
const post = await db.Posts.get('abc123', {
  includeDeleted: true
})

// Recover by unsetting deletedAt
await db.Posts.update('abc123', {
  $unset: { deletedAt: '', deletedBy: '' }
})
```

### Batch Processing

Process large datasets efficiently:

```typescript
// Process in batches with cursor pagination
let cursor: string | undefined
const batchSize = 100

while (true) {
  const batch = await db.Posts.find(
    { status: 'draft' },
    { limit: batchSize, cursor }
  )

  if (batch.length === 0) break

  // Process batch
  for (const post of batch) {
    await processPost(post)
  }

  // Update cursor
  cursor = batch[batch.length - 1]?.$id
  if (batch.length < batchSize) break
}
```

---

## Type Safety

Collections can be strongly typed using TypeScript generics:

```typescript
import { Collection, Entity } from 'parquedb'

interface Post {
  title: string
  content: string
  status: 'draft' | 'published'
  viewCount: number
  tags: string[]
  author: string  // EntityId reference
}

// Type-safe collection
const posts = new Collection<Post>('posts')

// TypeScript knows the shape
const post = await posts.create({
  $type: 'Post',
  name: 'hello',
  title: 'Hello',     // ✓ typed
  content: 'World',   // ✓ typed
  status: 'draft',    // ✓ enum
  viewCount: 0,       // ✓ typed
  tags: []            // ✓ typed
})

// Type-safe updates
await posts.update(post.$id, {
  $set: { status: 'published' },  // ✓ typed
  $inc: { viewCount: 1 }          // ✓ typed
})

// Type-safe queries
const published = await posts.find({
  status: 'published',  // ✓ typed
  viewCount: { $gte: 100 }  // ✓ typed
})
```

---

## Error Handling

All Collection methods can throw errors that should be handled:

```typescript
import { Collection } from 'parquedb'

try {
  const post = await db.Posts.get('nonexistent')
} catch (err) {
  console.error('Entity not found:', err.message)
}

try {
  await db.Posts.update('abc123', {
    $set: { title: 'Updated' }
  }, {
    expectedVersion: 5
  })
} catch (err) {
  console.error('Version mismatch:', err.message)
}

// Validation errors
try {
  await db.Posts.create({
    $type: 'Post',
    // Missing required 'name' field
    title: 'No Name'
  })
} catch (err) {
  console.error('Validation error:', err.message)
}
```

**Common Errors:**
- `Entity not found: {id}` - ID doesn't exist or entity is soft-deleted
- `Version mismatch: expected {N}, got {M}` - Optimistic concurrency conflict
- `$type is required` - Missing required field on create
- `name is required` - Missing required field on create
- `Invalid filter operator: {operator}` - Unknown filter operator
- `Invalid cursor format: malformed cursor` - Malformed pagination cursor
- `Invalid cursor: tampered or expired cursor` - Tampered cursor detected
- `Limit cannot be negative` - Negative limit value
- `Skip cannot be negative` - Negative skip value

---

## Performance Tips

1. **Use projection to limit data transfer:**
   ```typescript
   await db.Posts.find({}, {
     project: { title: 1, author: 1 }
   })
   ```

2. **Use cursor pagination for large result sets:**
   ```typescript
   const { items, nextCursor } = await db.Posts.findPaginated(
     {},
     { limit: 100 }
   )
   ```

3. **Batch creates with Promise.all():**
   ```typescript
   await Promise.all(dataArray.map(data => db.Posts.create(data)))  // Parallel creation
   ```

4. **Use estimatedDocumentCount() for fast counts:**
   ```typescript
   const approx = await db.Posts.estimatedDocumentCount()  // Fast
   const exact = await db.Posts.count()  // Slower
   ```

5. **Filter with column statistics:**
   ```typescript
   // ParqueDB uses Parquet column statistics automatically
   await db.Posts.find({
     createdAt: { $gte: new Date('2024-01-01') }
   })
   // Uses min/max statistics to skip row groups
   ```

6. **Use aggregation for analytics:**
   ```typescript
   // More efficient than loading all data
   await db.Posts.aggregate([
     { $group: { _id: '$status', count: { $sum: 1 } } }
   ])
   ```

---

## See Also

- [Getting Started](./getting-started.md) - Quick start guide
- [Query API](./queries.md) - Filter operators reference
- [Update Operators](./updates.md) - Update operators reference
- [Schema Definition](./schemas.md) - Schema types and validation
- [Graph-First Architecture](./architecture/graph-first-architecture.md) - Relationship indexing
