---
title: Getting Started
description: Installation, basic usage, and common operations with ParqueDB
---

This guide covers installation, basic usage, and common operations with ParqueDB.

## Prerequisites

Before getting started, make sure you have:

- **Node.js 18+** or a modern browser with ES2020 support
- **npm** or **yarn** package manager
- Basic familiarity with TypeScript (recommended but not required)

ParqueDB also runs in:
- **Cloudflare Workers** with R2 storage
- **Browsers** with in-memory storage for development

## Installation

```bash
npm install parquedb
```

ParqueDB has minimal dependencies:
- `hyparquet` - Parquet file reading
- `hyparquet-writer` - Parquet file writing

## Creating a Database

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({
  storage: new MemoryBackend()
})
```

For production use, see [Storage Backends](./storage-backends.md) for other options.

## Basic CRUD Operations

### Create

Every entity requires `$type` and `name` fields:

```typescript
const post = await db.Posts.create({
  $type: 'Post',
  name: 'my-first-post',
  title: 'My First Post',
  content: 'Hello, world!',
  tags: ['intro', 'welcome']
})

console.log(post.$id)        // 'posts/abc123'
console.log(post.createdAt)  // Date object
console.log(post.version)    // 1
```

### Read

Get a single entity by ID:

```typescript
// Full ID
const post = await db.Posts.get('posts/abc123')

// Short ID (namespace is inferred)
const post = await db.Posts.get('abc123')

// Returns null if not found
const missing = await db.Posts.get('nonexistent')  // null
```

Find multiple entities:

```typescript
// Find all
const allPosts = await db.Posts.find()

// With filter
const published = await db.Posts.find({ status: 'published' })

// Returns { items: Entity[], hasMore: boolean, nextCursor?: string }
const result = await db.Posts.find({ status: 'draft' }, { limit: 10 })
```

### Update

Use MongoDB-style update operators:

```typescript
// $set - Set field values
await db.Posts.update(post.$id, {
  $set: { status: 'published', publishedAt: new Date() }
})

// $unset - Remove fields
await db.Posts.update(post.$id, {
  $unset: { draft: true }
})

// $inc - Increment numeric fields
await db.Posts.update(post.$id, {
  $inc: { viewCount: 1 }
})

// $push - Add to array
await db.Posts.update(post.$id, {
  $push: { tags: 'featured' }
})

// $pull - Remove from array
await db.Posts.update(post.$id, {
  $pull: { tags: 'draft' }
})

// $addToSet - Add unique value to array
await db.Posts.update(post.$id, {
  $addToSet: { tags: 'pinned' }
})
```

### Delete

Soft delete (default):

```typescript
// Soft delete - sets deletedAt timestamp
await db.Posts.delete(post.$id)

// Entity still exists but is excluded from queries
const post = await db.Posts.get(post.$id)  // null

// Include deleted entities
const post = await db.Posts.get(post.$id, { includeDeleted: true })
```

Hard delete (permanent):

```typescript
await db.Posts.delete(post.$id, { hard: true })
```

## Using Collections

ParqueDB provides two ways to access collections:

### Property Access

Access collections as properties (PascalCase):

```typescript
db.Posts    // Collection for 'posts' namespace
db.Users    // Collection for 'users' namespace
db.Comments // Collection for 'comments' namespace
```

### Method Access

Use the `collection()` method for dynamic or typed access:

```typescript
// Dynamic namespace
const posts = db.collection('posts')

// With TypeScript types
interface PostData {
  title: string
  content: string
  status: 'draft' | 'published'
}

const posts = db.collection<PostData>('posts')
const post = await posts.create({
  $type: 'Post',
  name: 'typed-post',
  title: 'Type Safe',
  content: 'This is type checked!',
  status: 'draft'  // TypeScript ensures valid value
})
```

## Filtering and Querying

### Simple Equality

```typescript
// Single field
const published = await db.Posts.find({ status: 'published' })

// Multiple fields (AND)
const featured = await db.Posts.find({
  status: 'published',
  featured: true
})
```

### Comparison Operators

```typescript
// Greater than
const recent = await db.Posts.find({
  viewCount: { $gt: 100 }
})

// Less than or equal
const old = await db.Posts.find({
  createdAt: { $lte: new Date('2024-01-01') }
})

// In array
const selected = await db.Posts.find({
  status: { $in: ['published', 'featured'] }
})

// Not equal
const notDraft = await db.Posts.find({
  status: { $ne: 'draft' }
})
```

### Logical Operators

```typescript
// OR
const result = await db.Posts.find({
  $or: [
    { status: 'published' },
    { featured: true }
  ]
})

// AND (explicit)
const result = await db.Posts.find({
  $and: [
    { status: 'published' },
    { viewCount: { $gte: 100 } }
  ]
})

// NOT
const result = await db.Posts.find({
  $not: { status: 'archived' }
})
```

### Array Operators

```typescript
// Contains all values
const tagged = await db.Posts.find({
  tags: { $all: ['featured', 'pinned'] }
})

// Array size
const multipleTags = await db.Posts.find({
  tags: { $size: 3 }
})
```

### Existence Operators

```typescript
// Field exists
const withImage = await db.Posts.find({
  coverImage: { $exists: true }
})

// Field does not exist
const noImage = await db.Posts.find({
  coverImage: { $exists: false }
})
```

### String Operators

```typescript
// Regex match
const matching = await db.Posts.find({
  title: { $regex: /^hello/i }
})
```

## Working with Relationships

ParqueDB has first-class support for entity relationships. You can link entities together using the `$link` and `$unlink` operators.

### Linking Entities

Use `$link` to create relationships between entities:

```typescript
// Create a user
const user = await db.Users.create({
  $type: 'User',
  name: 'alice',
  email: 'alice@example.com',
})

// Create a post
const post = await db.Posts.create({
  $type: 'Post',
  name: 'hello-world',
  title: 'Hello World',
  content: 'My first post!',
})

// Link the post to the user as author
await db.Posts.update(post.$id, {
  $link: { author: user.$id },
})

// The post now has an author relationship
const linkedPost = await db.Posts.get(post.$id)
console.log(linkedPost.author)  // { 'alice': 'users/abc123' }
```

### Unlinking Entities

Use `$unlink` to remove relationships:

```typescript
// Remove the author relationship
await db.Posts.update(post.$id, {
  $unlink: { author: user.$id },
})
```

### Multiple Relationships

Link an entity to multiple targets:

```typescript
// Create categories
const tech = await db.Categories.create({
  $type: 'Category',
  name: 'tech',
  slug: 'technology',
})

const tutorial = await db.Categories.create({
  $type: 'Category',
  name: 'tutorials',
  slug: 'tutorials',
})

// Link post to multiple categories
await db.Posts.update(post.$id, {
  $link: { categories: [tech.$id, tutorial.$id] },
})
```

### Defining Relationships in Schema

For bidirectional relationships, define them in your schema:

```typescript
const schema = {
  User: {
    $ns: 'users',
    name: 'string!',
    email: 'email!',
    // Reverse relationship: User.posts shows all posts where Post.author = this user
    posts: '<- Post.author[]',
  },
  Post: {
    $ns: 'posts',
    title: 'string!',
    content: 'markdown!',
    // Forward relationship: Post.author links to a User
    author: '-> User.posts',
    // Many-to-many: Post can have multiple categories
    categories: '-> Category.posts[]',
  },
  Category: {
    $ns: 'categories',
    name: 'string!',
    slug: 'string!',
    // Reverse: all posts in this category
    posts: '<- Post.categories[]',
  },
}

const db = new ParqueDB({ storage, schema })
```

Relationship syntax:
- `-> Target.reverse` - Forward relationship (stored on this entity)
- `<- Source.predicate[]` - Reverse relationship (computed from source entities)
- `[]` suffix indicates a to-many relationship

### Combining Updates with Links

You can combine `$link` with other update operators:

```typescript
await db.Posts.update(post.$id, {
  $set: { status: 'published', publishedAt: new Date() },
  $link: { author: user.$id },
})
```

## Query Options

### Sorting

```typescript
// Ascending
const oldest = await db.Posts.find({}, {
  sort: { createdAt: 1 }
})

// Descending
const newest = await db.Posts.find({}, {
  sort: { createdAt: -1 }
})

// Multiple fields
const sorted = await db.Posts.find({}, {
  sort: { status: 1, createdAt: -1 }
})

// String format
const sorted = await db.Posts.find({}, {
  sort: { createdAt: 'desc' }
})
```

### Pagination

```typescript
// Limit results
const topTen = await db.Posts.find({}, { limit: 10 })

// Skip results (offset)
const page2 = await db.Posts.find({}, { skip: 10, limit: 10 })

// Cursor-based pagination (preferred for large datasets)
const page1 = await db.Posts.find({}, { limit: 10 })
const page2 = await db.Posts.find({}, {
  limit: 10,
  cursor: page1.items[page1.items.length - 1].$id
})
```

### Projection

```typescript
// Include only specific fields
const titles = await db.Posts.find({}, {
  project: { title: 1, status: 1 }
})

// Exclude specific fields
const noContent = await db.Posts.find({}, {
  project: { content: 0 }
})
```

## Optimistic Concurrency

Use version checking to prevent lost updates:

```typescript
const post = await db.Posts.get('posts/abc123')

// Update only if version matches
await db.Posts.update(post.$id, {
  $set: { title: 'Updated Title' }
}, {
  expectedVersion: post.version
})

// Throws error if version changed
// Error: Version mismatch: expected 1, got 2
```

## Actor Tracking

Track who performed operations:

```typescript
const actor = 'users/current-user-id'

await db.Posts.create({
  $type: 'Post',
  name: 'tracked',
  title: 'Tracked Post'
}, {
  actor
})

await db.Posts.update(post.$id, {
  $set: { status: 'published' }
}, {
  actor
})

// Entity has createdBy and updatedBy fields
const post = await db.Posts.get('posts/abc123')
console.log(post.createdBy)  // 'users/current-user-id'
console.log(post.updatedBy)  // 'users/current-user-id'
```

## Upsert

Create or update in a single operation:

```typescript
// Using collection.upsert()
const entity = await db.Posts.upsert(
  { slug: 'my-post' },  // filter
  {
    $set: { title: 'My Post', content: 'Updated!' },
    $setOnInsert: { $type: 'Post', name: 'my-post' }
  }
)

// Using update with upsert option
await db.Posts.update('new-id', {
  $set: { title: 'New Post' }
}, {
  upsert: true
})
```

## Restore Deleted Entities

Restore soft-deleted entities:

```typescript
// Soft delete
await db.Posts.delete(post.$id)

// Restore
await db.restore('posts', post.$id.split('/')[1])
```

## Next Steps

Now that you know the basics, explore these topics:

- [Schema Definition](./schema.md) - Define types, validation, and relationships
- [Storage Backends](./storage-backends.md) - Choose the right storage for your environment
- [Cloudflare Workers](./workers.md) - Deploy to the edge with CQRS architecture

### Architecture Deep Dives

- [Graph-First Architecture](./architecture/GRAPH_FIRST_ARCHITECTURE.md) - How relationships are indexed
- [Secondary Indexes](./architecture/SECONDARY_INDEXES.md) - B-tree, hash, and full-text indexes
- [Namespace Sharded Architecture](./architecture/NAMESPACE_SHARDED_ARCHITECTURE.md) - Multi-tenant design

### Example Datasets

Check out the [examples](../examples) folder for real-world usage:
- IMDB - Movie database with actors and directors
- O*NET - Occupational data with skills and abilities
- UNSPSC - Product classification hierarchy
- Wikidata - Knowledge graph import
