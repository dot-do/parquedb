# ParqueDB Documentation

ParqueDB is a Parquet-based database that runs in Node.js, browsers, and Cloudflare Workers. It provides MongoDB-style operations with columnar storage for efficient analytics.

## What is ParqueDB?

ParqueDB combines the flexibility of document databases with the efficiency of columnar storage. Built on the [Apache Parquet](https://parquet.apache.org/) format, it enables:

- **Efficient Storage**: Columnar format with compression reduces storage costs
- **Fast Analytics**: Column pruning and predicate pushdown for fast queries
- **Multi-Environment**: Run the same code in Node.js, browsers, or Cloudflare Workers
- **Graph Support**: First-class support for relationships between entities

## Key Features

### Parquet Storage

Data is stored in Apache Parquet format, providing:
- Columnar compression (often 10x smaller than JSON)
- Predicate pushdown for efficient filtering
- Column pruning to read only needed fields
- Bloom filters for fast negative lookups

### Graph Support

Built-in support for entity relationships:
- Define relationships in your schema with predicate pairs
- Traverse relationships with `$link` and `$unlink` operators
- Query related entities efficiently

### Multi-Environment

ParqueDB runs everywhere:
- **Node.js**: Using `FsBackend` for local development
- **Browsers**: Using `MemoryBackend` for testing
- **Cloudflare Workers**: Using `R2Backend` and `DOSqliteBackend` for production

## Quick Start

### Installation

```bash
npm install parquedb
```

### Basic Usage

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

// Create a database instance
const db = new ParqueDB({
  storage: new MemoryBackend()
})

// Create an entity
const post = await db.Posts.create({
  $type: 'Post',
  name: 'hello-world',
  title: 'Hello World',
  content: 'This is my first post!',
  status: 'draft'
})

// Find entities
const drafts = await db.Posts.find({ status: 'draft' })

// Update an entity
await db.Posts.update(post.$id, {
  $set: { status: 'published' }
})

// Delete an entity (soft delete by default)
await db.Posts.delete(post.$id)
```

### With Schema Validation

```typescript
import { ParqueDB, MemoryBackend, parseSchema } from 'parquedb'

// Define your schema
const schema = {
  Post: {
    title: 'string!',
    content: 'markdown!',
    status: 'enum(draft,published,archived) = draft',
    author: '-> User.posts'
  },
  User: {
    name: 'string!',
    email: { type: 'email!', index: 'unique' },
    posts: '<- Post.author[]'
  }
}

const db = new ParqueDB({
  storage: new MemoryBackend(),
  schema
})

// Schema validation is automatic on create/update
const post = await db.Posts.create({
  $type: 'Post',
  name: 'hello',
  title: 'Hello World',
  content: '# Welcome\n\nThis is markdown!'
})
```

## Documentation

- [Getting Started](./getting-started.md) - Installation and basic usage
- [Schema Definition](./schema.md) - Defining types, fields, and relationships
- [Storage Backends](./storage-backends.md) - MemoryBackend, FsBackend, R2Backend, and more
- [Cloudflare Workers](./workers.md) - CQRS architecture and RPC client usage

## Core Concepts

### Entities

Every entity in ParqueDB has:
- `$id` - Unique identifier in format `namespace/id`
- `$type` - Entity type name
- `name` - Human-readable name (required)
- `createdAt`, `updatedAt` - Timestamps
- `createdBy`, `updatedBy` - Actor references
- `version` - Optimistic concurrency control

### Collections

Access collections via property or method:

```typescript
// Property access (PascalCase becomes lowercase namespace)
const posts = db.Posts      // namespace: 'posts'
const users = db.Users      // namespace: 'users'

// Method access
const posts = db.collection('posts')
const users = db.collection<UserData>('users')  // with types
```

### Operations

ParqueDB supports MongoDB-style operations:

| Operation | Description |
|-----------|-------------|
| `find(filter, options)` | Find multiple entities |
| `findOne(filter, options)` | Find a single entity |
| `get(id, options)` | Get entity by ID |
| `create(data, options)` | Create new entity |
| `update(id, update, options)` | Update entity |
| `delete(id, options)` | Delete entity (soft by default) |
| `count(filter)` | Count matching entities |

## License

MIT
