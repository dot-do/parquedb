# ParqueDB

A hybrid relational/document/graph database built on Apache Parquet, designed for Node.js, browsers, and Cloudflare Workers.

## Features

- **Parquet-native storage** - Columnar format with compression, predicate pushdown, and efficient partial reads
- **Hybrid data model** - Combine document flexibility (Variant type) with relational integrity and graph traversal
- **MongoDB-style API** - Familiar query operators, no `where` wrapper needed
- **Bidirectional relationships** - First-class support for `predicate` (outbound) and `reverse` (inbound) relationships
- **Time-travel** - Query data as of any point in time via CDC event log
- **RPC pipelining** - Chain operations without round-trips using capnweb RpcPromise
- **Multi-environment** - Works with local filesystem, fsx, R2, or S3

## Installation

```bash
npm install parquedb
```

## Quick Start

```typescript
import { ParqueDB } from 'parquedb'

// Initialize with storage backend
const db = new ParqueDB(storage, schema)

// MongoDB-style queries (no 'where' wrapper needed)
const posts = await db.Posts.find({ status: 'published' })

// With options
const recent = await db.Posts.find(
  { status: 'published' },
  { sort: { createdAt: -1 }, limit: 10 }
)

// Get by ID
const post = await db.Posts.get('building-parquedb')

// Create with relationships
const newPost = await db.Posts.create({
  $type: 'Post',
  name: 'My Post',
  title: 'My Post',
  content: 'Hello world...',
  author: { 'Nathan': 'users/nathan' },
  categories: { 'Tech': 'categories/tech' }
})

// Update with MongoDB operators
await db.Posts.update('post-123', {
  $set: { status: 'published' },
  $inc: { viewCount: 1 },
  $push: { tags: 'featured' }
})
```

## Data Model

### Entity Structure

Every entity has a consistent structure:

```typescript
{
  // Identity (JSON-LD style)
  $id: 'posts/building-parquedb',    // ns/id
  $type: 'Post',                      // Entity type
  name: 'Building ParqueDB',          // Human-readable label

  // Audit fields (always present)
  createdAt: '2024-01-15T10:00:00Z',
  createdBy: 'users/nathan',
  updatedAt: '2024-01-16T14:30:00Z',
  updatedBy: 'users/nathan',
  version: 3,

  // Data fields (from Variant, optionally shredded)
  title: 'Building ParqueDB',
  content: 'A new kind of database...',
  status: 'published',

  // Outbound predicates (relationships this entity owns)
  author: { 'Nathan Clevenger': 'users/nathan' },
  categories: { 'Databases': 'categories/databases' },

  // Inbound reverses (other entities referencing this one)
  comments: {
    'Great post!': 'comments/abc123',
    $count: 1523,
    $next: 'cursor_xyz...'
  }
}
```

### File Layout

```
{base}/
├── _meta/
│   ├── manifest.json           # DB metadata
│   └── schema.json             # GraphDL/IceType schema
├── data/
│   └── {ns}/
│       └── data.parquet        # Entity storage
├── rels/
│   ├── forward/                # Outbound relationships
│   │   └── {ns}.parquet
│   └── reverse/                # Inbound relationships
│       └── {ns}.parquet
├── events/
│   ├── current.parquet         # CDC event log
│   └── archive/
└── indexes/
    ├── fts/                    # Full-text search
    ├── vector/                 # Vector embeddings
    └── secondary/              # B-tree, hash indexes
```

## Query API

### Filter Operators

```typescript
// Comparison
{ field: value }                    // Equality (implicit)
{ field: { $eq: value } }           // Equality (explicit)
{ field: { $ne: value } }           // Not equal
{ field: { $gt: value } }           // Greater than
{ field: { $gte: value } }          // Greater than or equal
{ field: { $lt: value } }           // Less than
{ field: { $lte: value } }          // Less than or equal
{ field: { $in: [values] } }        // In array
{ field: { $nin: [values] } }       // Not in array

// Logical
{ $and: [filters] }                 // All conditions
{ $or: [filters] }                  // Any condition
{ $not: filter }                    // Negation
{ $nor: [filters] }                 // None of conditions

// String
{ field: { $regex: /pattern/ } }    // Regex match
{ field: { $startsWith: 'prefix' } }
{ field: { $contains: 'substring' } }

// Array
{ field: { $all: [values] } }       // Contains all
{ field: { $elemMatch: filter } }   // Element matches
{ field: { $size: 3 } }             // Array length

// Special
{ field: { $exists: true } }        // Field exists
{ $text: { $search: 'query' } }     // Full-text search
{ $vector: { $near: [...], $k: 10 } } // Vector similarity
```

### Update Operators

```typescript
// Field updates
{ $set: { field: value } }          // Set field value
{ $unset: { field: '' } }           // Remove field
{ $rename: { old: 'new' } }         // Rename field

// Numeric
{ $inc: { field: 1 } }              // Increment
{ $mul: { field: 2 } }              // Multiply
{ $min: { field: value } }          // Set if less
{ $max: { field: value } }          // Set if greater

// Array
{ $push: { field: value } }         // Add to array
{ $pull: { field: value } }         // Remove from array
{ $addToSet: { field: value } }     // Add unique
{ $pop: { field: 1 } }              // Remove last (-1 for first)

// Relationships
{ $link: { predicate: 'ns/id' } }   // Add relationship
{ $unlink: { predicate: 'ns/id' } } // Remove relationship
```

### Find Options

```typescript
db.Posts.find(filter, {
  sort: { createdAt: -1 },          // Sort order (1 asc, -1 desc)
  limit: 20,                        // Max results
  skip: 0,                          // Offset
  cursor: 'abc...',                 // Pagination cursor

  project: { title: 1, content: 1 }, // Field projection

  populate: ['author'],             // Fetch related entities
  populate: {                       // With options
    author: true,
    comments: { limit: 5 }
  },

  includeDeleted: false,            // Include soft-deleted
  asOf: new Date('2024-01-01')      // Time-travel
})
```

## Relationships

### Defining Relationships (Schema)

```typescript
const schema = {
  Post: {
    $type: 'schema:BlogPosting',

    // Outbound predicates
    author: '-> User.posts',           // predicate -> Target.reverse
    categories: '-> Category.posts[]', // Array relationship

    // Inbound (for documentation)
    comments: '<- Comment.post[]',
    likes: '<- Like.post[]',
  },

  Comment: {
    post: '-> Post.comments',
    author: '-> User.comments',
  }
}
```

### Working with Relationships

```typescript
// Relationships returned with entity
const post = await db.Posts.get('post-123')
post.author       // { 'Nathan': 'users/nathan' }
post.comments     // { 'Great!': 'comments/abc', $count: 100, $next: '...' }

// Traverse relationships
const comments = await db.Posts.referencedBy('post-123', 'comments', {
  limit: 50,
  cursor: post.comments.$next
})

// Populate related entities
const postWithAuthor = await db.Posts.get('post-123', {
  hydrate: ['author']
})

// Link/unlink
await db.Posts.link('post-123', 'categories', 'categories/tech')
await db.Posts.unlink('post-123', 'categories', 'categories/old')
```

## RPC Pipelining

Using capnweb's RpcPromise, operations chain without intermediate round-trips:

```typescript
import { RpcClient } from 'capnweb'

const client = new RpcClient('https://my-worker.workers.dev/rpc')
const db = client.import<ParqueDB>('db')

// All of this pipelines to ONE request!
const feed = await db.Posts
  .find({ status: 'published' }, { limit: 20 })
  .map(async post => ({
    ...post,
    author: await db.Users.get(post.author.$id),
    topComments: await db.Posts.referencedBy(post.$id, 'comments', { limit: 3 })
  }))

// The .map() runs on the Worker, only results transfer
```

## Storage Backends

```typescript
import { ParqueDB, FsBackend, FsxBackend, R2Backend, S3Backend } from 'parquedb'

// Local filesystem (Node.js)
const db = new ParqueDB(new FsBackend('./data'), schema)

// fsx (Cloudflare Workers)
const db = new ParqueDB(new FsxBackend(env.FSX_DO), schema)

// R2 (Cloudflare)
const db = new ParqueDB(new R2Backend(env.R2_BUCKET), schema)

// S3 (AWS)
const db = new ParqueDB(new S3Backend({ bucket: 'my-bucket', region: 'us-east-1' }), schema)

// Memory (testing)
const db = new ParqueDB(new MemoryBackend(), schema)
```

## Cloudflare Worker Example

```typescript
import { ParqueDB, R2Backend } from 'parquedb'
import { RpcServer } from 'capnweb'

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const db = new ParqueDB(new R2Backend(env.R2_BUCKET), schema)

    // REST API
    if (request.url.includes('/api/')) {
      const posts = await db.Posts.find({ status: 'published' })
      return Response.json(posts)
    }

    // RPC endpoint (enables pipelining)
    if (request.url.includes('/rpc')) {
      const server = new RpcServer()
      server.export('db', db)
      return server.handleRequest(request)
    }
  }
}
```

## Indexing

### Full-Text Search

```typescript
// Define FTS index in schema
const schema = {
  Post: {
    content: { type: 'markdown', index: 'fts' },
    title: { type: 'string', index: 'fts' }
  }
}

// Query
const results = await db.Posts.find({
  $text: { $search: 'parquet database', $language: 'en' }
})
```

### Vector Similarity

```typescript
// Define vector index
const schema = {
  Post: {
    embedding: { type: 'vector', dimensions: 1536, index: 'vector' }
  }
}

// Query
const similar = await db.Posts.find({
  $vector: { $near: queryEmbedding, $k: 10, $field: 'embedding' }
})
```

### Secondary Indexes

```typescript
const schema = {
  User: {
    email: { type: 'email', index: 'unique' },
    status: { type: 'string', index: true },
    score: { type: 'float', index: true }
  }
}
```

## Time Travel

```typescript
// Query data as of a specific time
const oldPost = await db.Posts.get('post-123', {
  asOf: new Date('2024-01-01')
})

// Find with time travel
const historicalPosts = await db.Posts.find(
  { status: 'published' },
  { asOf: new Date('2024-01-01') }
)

// Get entity history
const history = await db.Posts.history('post-123', {
  from: new Date('2024-01-01'),
  to: new Date('2024-02-01')
})
```

## Dependencies

- [hyparquet](https://github.com/hyparam/hyparquet) - Pure JS Parquet reader
- [hyparquet-writer](https://github.com/hyparam/hyparquet-writer) - Pure JS Parquet writer
- [capnweb](https://github.com/cloudflare/capnweb) - RPC pipelining (optional)
- [fsx](https://github.com/dotdo-ai/fsx) - Cloudflare Worker filesystem (optional)

## License

MIT
