# Getting Started with ParqueDB

This example demonstrates the core features of ParqueDB through a simple blog application with users, posts, and comments.

## Running the Example

```bash
# From the parquedb root directory
npx tsx examples/getting-started/index.ts
```

## Features Demonstrated

### 1. Database Initialization

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({
  storage: new MemoryBackend(), // For production: FsBackend or R2Backend
})
```

### 2. Creating Entities

Entities require `$type` and `name` fields:

```typescript
const user = await db.Users.create({
  $type: 'User',
  name: 'Alice Johnson',
  email: 'alice@example.com',
  role: 'admin',
})
```

### 3. Establishing Relationships

Use `$link` to create relationships between entities:

```typescript
await db.Posts.update(post.$id, {
  $link: { author: [user.$id] },
})
```

### 4. Querying with Filters

MongoDB-style filters are supported:

```typescript
// Simple equality
const published = await db.Posts.find({ status: 'published' })

// Comparison operators
const popular = await db.Posts.find({ viewCount: { $gte: 100 } })

// Array operators
const tutorials = await db.Posts.find({ tags: { $in: ['tutorial'] } })

// Logical operators
const filtered = await db.Posts.find({
  $and: [
    { status: 'published' },
    { viewCount: { $gt: 50 } }
  ]
})
```

### 5. Sorting and Pagination

```typescript
// Sorted results with limit
const topPosts = await db.Posts.find(
  {},
  { sort: { viewCount: -1 }, limit: 10 }
)

// Paginated results
const result = await db.Posts.findPaginated({}, { limit: 20 })
// result.items, result.hasMore, result.nextCursor, result.total
```

### 6. Update Operations

```typescript
// Set fields
await db.Posts.update(id, { $set: { status: 'published' } })

// Increment numeric fields
await db.Posts.update(id, { $inc: { viewCount: 1 } })

// Push to arrays
await db.Posts.update(id, { $push: { tags: 'featured' } })

// Remove fields
await db.Posts.update(id, { $unset: { draft: true } })
```

### 7. Aggregation Pipelines

```typescript
const stats = await db.Posts.aggregate([
  { $match: { status: 'published' } },
  { $group: {
    _id: '$author',
    totalViews: { $sum: '$viewCount' },
    avgViews: { $avg: '$viewCount' },
    count: { $count: {} }
  }},
  { $sort: { totalViews: -1 } }
])
```

### 8. Delete Operations

```typescript
// Soft delete (default) - sets deletedAt timestamp
await db.Posts.delete(id)

// Find soft-deleted entities
const deleted = await db.Posts.find({}, { includeDeleted: true })

// Hard delete - permanently removes entity
await db.Posts.delete(id, { hard: true })
```

### 9. Query Builder (Fluent API)

```typescript
const results = await db.Posts
  .builder()
  .where('status', 'eq', 'published')
  .andWhere('viewCount', 'gte', 100)
  .orderBy('viewCount', 'desc')
  .limit(10)
  .find()
```

## Storage Backends

ParqueDB supports multiple storage backends:

| Backend | Use Case |
|---------|----------|
| `MemoryBackend` | Testing and development |
| `FsBackend` | Node.js with local filesystem |
| `R2Backend` | Cloudflare Workers with R2 |
| `S3Backend` | AWS S3 compatible storage |

## Next Steps

- See `examples/ai-database/` for AI integration examples
- See `examples/imdb/` for working with large datasets
- See `examples/onet/` for complex relationship modeling
- Check the main README for full API documentation

## Entity Structure

All entities in ParqueDB have these core fields:

```typescript
interface Entity {
  $id: string      // Unique ID (namespace/id format)
  $type: string    // Entity type
  name: string     // Human-readable name

  // Audit fields (auto-managed)
  createdAt: Date
  createdBy: string
  updatedAt: Date
  updatedBy: string
  version: number

  // Optional soft-delete fields
  deletedAt?: Date
  deletedBy?: string
}
```
