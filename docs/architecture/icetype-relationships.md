# IceType Relationship Implementation via ParqueDB

## Overview

ParqueDB's relationship table provides an elegant, direct implementation of IceType's relationship model. This document explains the mapping and shows how ParqueDB can serve as the runtime for IceType schemas.

## IceType Relationship Operators

IceType defines four relationship operators:

| Operator | Name | Direction | Storage |
|----------|------|-----------|---------|
| `->` | Forward | Outbound | FK column / rel row |
| `<-` | Backward | Inbound | Query on inverse |
| `~>` | Fuzzy Forward | Outbound | Vector similarity |
| `<~` | Fuzzy Backward | Inbound | Vector similarity |

## Direct Mapping to ParqueDB

### The Core Insight

An IceType relationship declaration like:

```typescript
Post: {
  author: '-> User.posts',        // Forward to User, inverse is "posts"
}
```

Maps directly to a ParqueDB relationship row:

```typescript
{
  fromNs: 'posts',
  fromId: 'post-123',
  predicate: 'author',      // The "author" from -> User.posts
  reverse: 'posts',         // The "posts" from -> User.posts
  toNs: 'users',
  toId: 'alice',
}
```

And the inverse declaration:

```typescript
User: {
  posts: '<- Post.author[]',      // Backward from Post.author
}
```

Is a **query** on the same row using the reverse index:

```sql
SELECT * FROM relationships
WHERE to_ns = 'users'
  AND to_id = 'alice'
  AND reverse = 'posts'
```

### Complete Mapping Table

| IceType Declaration | ParqueDB Representation |
|--------------------|------------------------|
| `author: '-> User.posts'` | Row: `predicate='author', reverse='posts', to_ns='users'` |
| `posts: '<- Post.author[]'` | Query: `WHERE to_ns=? AND reverse='posts'` |
| `categories: '-> Category.items[]'` | Multiple rows with `predicate='categories'` |
| `similar: '~> Product[]'` | Future: vector index query |

## Schema Compilation

When compiling an IceType schema, ParqueDB extracts relationship metadata:

```typescript
// Input: IceType schema
const schema = {
  Post: {
    $type: 'Post',
    $ns: 'posts',
    title: 'string!',
    author: '-> User.posts',
    categories: '-> Category.posts[]',
  },
  User: {
    $type: 'User',
    $ns: 'users',
    name: 'string!',
    posts: '<- Post.author[]',
  },
  Category: {
    $type: 'Category',
    $ns: 'categories',
    name: 'string!',
    posts: '<- Post.categories[]',
  }
}

// Output: Compiled relationship definitions
const relationships = {
  'posts': {
    'author': {
      direction: 'forward',
      targetNs: 'users',
      targetType: 'User',
      reverse: 'posts',
      cardinality: 'one',
    },
    'categories': {
      direction: 'forward',
      targetNs: 'categories',
      targetType: 'Category',
      reverse: 'posts',
      cardinality: 'many',
    },
  },
  'users': {
    'posts': {
      direction: 'backward',
      sourceNs: 'posts',
      sourceType: 'Post',
      sourceField: 'author',
      cardinality: 'many',
    },
  },
  'categories': {
    'posts': {
      direction: 'backward',
      sourceNs: 'posts',
      sourceType: 'Post',
      sourceField: 'categories',
      cardinality: 'many',
    },
  },
}
```

## Runtime Operations

### Creating a Forward Relationship

```typescript
// IceType: author: '-> User.posts'
await db.posts.create({
  title: 'Hello World',
  author: 'users/alice',  // EntityRef format
})

// ParqueDB internally calls:
await db.link('posts/post-123', 'author', 'users/alice')

// Which creates relationship row:
{
  fromNs: 'posts',
  fromId: 'post-123',
  predicate: 'author',
  reverse: 'posts',       // Derived from schema
  toNs: 'users',
  toId: 'alice',
  createdAt: Date.now(),
  createdBy: 'system',
  version: 1,
}
```

### Querying Forward Relationships

```typescript
// Get the author of a post
const post = await db.posts.get('post-123')
const author = await post.related('author')

// SQL equivalent:
SELECT u.* FROM users u
INNER JOIN relationships r
  ON r.to_ns = 'users' AND r.to_id = u.id
WHERE r.from_ns = 'posts'
  AND r.from_id = 'post-123'
  AND r.predicate = 'author'
  AND r.deleted_at IS NULL
```

### Querying Backward Relationships

```typescript
// IceType: posts: '<- Post.author[]'
const user = await db.users.get('alice')
const posts = await user.related('posts')

// This queries the REVERSE index:
SELECT p.* FROM posts p
INNER JOIN relationships r
  ON r.from_ns = 'posts' AND r.from_id = p.id
WHERE r.to_ns = 'users'
  AND r.to_id = 'alice'
  AND r.reverse = 'posts'
  AND r.deleted_at IS NULL
```

## Cardinality Handling

### One-to-One (`-> User`)

```typescript
Post: {
  author: '-> User.posts',  // No [], means one-to-one
}

// Returns single entity
const author: User = await post.related('author')
```

### One-to-Many (`-> Category[]`)

```typescript
Post: {
  categories: '-> Category.posts[]',  // [], means one-to-many
}

// Returns array
const categories: Category[] = await post.related('categories')
```

### Many-to-One (`<- Post.author[]`)

```typescript
User: {
  posts: '<- Post.author[]',  // All posts where author = this user
}

// Returns array with pagination
const { items, total, hasMore } = await user.related('posts', {
  limit: 10,
  offset: 0,
})
```

## Bidirectional Consistency

ParqueDB ensures bidirectional consistency by storing both names in a single row:

```
┌─────────────────────────────────────────────────────────────┐
│                    Relationship Row                         │
├───────────────┬──────────────┬───────────────┬─────────────┤
│   from_id     │  predicate   │   reverse     │   to_id     │
│  posts/123    │   "author"   │   "posts"     │ users/alice │
├───────────────┴──────────────┴───────────────┴─────────────┤
│                                                             │
│  Forward Query (author):                                    │
│  WHERE from_id='posts/123' AND predicate='author'          │
│                                                             │
│  Reverse Query (posts):                                     │
│  WHERE to_id='users/alice' AND reverse='posts'             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

This means:
- **No separate reverse storage** - same row serves both directions
- **Atomic updates** - link/unlink affects both directions at once
- **Consistent naming** - predicate/reverse always paired correctly

## Index Structure

### Forward Index

File: `rels/forward/{ns}.parquet`

Sorted by: `(from_ns, from_id, predicate, to_id)`

```
┌─────────────────────────────────────────────────────────────┐
│ Row Group 1: from_ns='posts', from_id='post-001'           │
├─────────────────────────────────────────────────────────────┤
│ posts/post-001 │ author     │ posts  │ users/alice         │
│ posts/post-001 │ categories │ posts  │ categories/tech     │
│ posts/post-001 │ categories │ posts  │ categories/db       │
├─────────────────────────────────────────────────────────────┤
│ Row Group 2: from_ns='posts', from_id='post-002'           │
├─────────────────────────────────────────────────────────────┤
│ posts/post-002 │ author     │ posts  │ users/bob           │
│ posts/post-002 │ categories │ posts  │ categories/news     │
└─────────────────────────────────────────────────────────────┘
```

### Reverse Index

File: `rels/reverse/{ns}.parquet`

Sorted by: `(to_ns, to_id, reverse, from_id)`

```
┌─────────────────────────────────────────────────────────────┐
│ Row Group 1: to_ns='users', to_id='alice'                  │
├─────────────────────────────────────────────────────────────┤
│ posts/post-001 │ author     │ posts  │ users/alice         │
│ posts/post-005 │ author     │ posts  │ users/alice         │
│ posts/post-008 │ author     │ posts  │ users/alice         │
├─────────────────────────────────────────────────────────────┤
│ Row Group 2: to_ns='users', to_id='bob'                    │
├─────────────────────────────────────────────────────────────┤
│ posts/post-002 │ author     │ posts  │ users/bob           │
└─────────────────────────────────────────────────────────────┘
```

## Fuzzy Relationships (Future)

IceType's `~>` and `<~` operators represent semantic/AI-matched relationships:

```typescript
Product: {
  similar: '~> Product[]',        // Similar products (vector similarity)
  recommended: '~> Product[]',    // Recommended products
}
```

### Implementation Approach

```typescript
// Fuzzy relationship storage
interface FuzzyRelationship extends Relationship {
  // Standard fields
  predicate: string    // e.g., "similar"
  reverse: string      // e.g., "similar" (often symmetric)

  // Fuzzy-specific
  score: number        // Similarity score (0-1)
  embedding?: Float32Array  // Optional: cached embedding
  method: 'vector' | 'semantic' | 'collaborative'
}

// Query with similarity threshold
const similar = await product.related('similar', {
  minScore: 0.8,
  limit: 10,
})
```

### Vector Index Integration

```
rels/
├── forward/{ns}.parquet
├── reverse/{ns}.parquet
└── vectors/
    └── {ns}/
        ├── embeddings.parquet    # entity_id → embedding vector
        └── index.hnsw            # HNSW index for ANN search
```

## GraphDL Integration

ParqueDB integrates with GraphDL for schema parsing:

```typescript
import { Graph } from '@graphdl/core'
import { graphToParqueDB } from '@parquedb/graphdl'

// Define schema using GraphDL
const graph = Graph({
  User: {
    name: 'string!',
    posts: '<- Post.author[]',
  },
  Post: {
    title: 'string!',
    author: '-> User.posts',
    categories: '-> Category.posts[]',
  },
  Category: {
    name: 'string!',
    posts: '<- Post.categories[]',
  },
})

// Convert to ParqueDB schema
const schema = graphToParqueDB(graph)

// Initialize database with schema
const db = new ParqueDB({
  schema,
  backend: { type: 'iceberg', catalog: r2Catalog },
  storage: r2Bucket,
})
```

## Query Examples

### Find Posts by Author

```typescript
// Direct relationship traversal
const alicePosts = await db.users.get('alice').related('posts')

// Or via filter
const alicePosts = await db.posts.find({
  author: 'users/alice'
})
```

### Find Posts in Category

```typescript
// Reverse traversal
const techPosts = await db.categories.get('tech').related('posts')

// Multi-hop: Find all posts by users in a certain category
const posts = await db.posts.find({
  'author.posts.categories': { $contains: 'categories/tech' }
})
```

### Count Relationships

```typescript
// How many posts does Alice have?
const count = await db.users.get('alice').count('posts')

// Embedded in entity (via $count in RelSet)
const user = await db.users.get('alice', { include: ['posts.$count'] })
console.log(user.posts.$count)  // 42
```

## Performance Optimizations

### Bloom Filters

Each relationship index file includes Bloom filters for fast existence checks:

```typescript
// Check if relationship exists without full scan
const exists = await db.relationshipExists(
  'posts/post-123',
  'author',
  'users/alice'
)
// Uses Bloom filter → O(1) negative, O(log n) positive
```

### Super-Node Handling

High-degree nodes (>1000 relationships) get dedicated adjacency files:

```
rels/
├── forward/posts.parquet
├── reverse/users.parquet
└── adjacency/
    └── users/
        └── alice/
            ├── outgoing.parquet  # Alice's outbound rels
            └── incoming.parquet  # Alice's inbound rels
```

### Materialized Counts

Frequently-accessed counts can be materialized:

```typescript
// Entity includes materialized relationship counts
{
  $id: 'users/alice',
  name: 'Alice',
  $rels: {
    posts: { $count: 42 },
    followers: { $count: 1000 },
    following: { $count: 150 },
  }
}
```

## Summary

ParqueDB's relationship table provides a **direct, efficient implementation** of IceType's relationship model:

1. **Forward (`->`)** → `predicate` field, forward index lookup
2. **Backward (`<-`)** → `reverse` field, reverse index lookup
3. **Bidirectional** → Single row stores both directions
4. **Cardinality** → Enforced at query time via schema
5. **Fuzzy (`~>`, `<~`)** → Future: vector similarity index

This architecture enables:
- **Schema-first development** with IceType/GraphDL
- **Efficient graph traversal** in both directions
- **Pluggable entity backends** (Native, Iceberg, Delta Lake)
- **Interoperability** with analytics tools via Iceberg
