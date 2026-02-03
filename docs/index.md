---
title: ParqueDB Documentation
description: A hybrid relational/document/graph database built on Apache Parquet
---

# ParqueDB

**A hybrid relational/document/graph database built on Apache Parquet for Node.js, browsers, and Cloudflare Workers.**

ParqueDB combines MongoDB's flexible document API with Parquet's columnar efficiency and first-class graph relationships. Built for modern JavaScript runtimes with zero WASM dependencies.

## Quick Install

```bash
npm install parquedb
```

## Key Features

### Parquet-Based Storage
Store data in Apache Parquet format for 8-10x compression, efficient column pruning, and predicate pushdown. Query only the columns you need with native Bloom filter support.

### MongoDB-Style API
Familiar query and update operators (`$set`, `$inc`, `$push`, `$gte`, `$regex`) with full TypeScript type safety. No SQL required (but available if you want it).

### Bidirectional Relationships
Graph database built-in. Define relationships once, traverse in both directions. No joins, no `$lookup`, no additional queries.

```typescript
// Define once
const db = DB({
  User: { email: 'string!#', posts: '<- Post.author[]' },
  Post: { title: 'string!', author: '-> User.posts' }
})

// Traverse both ways
const user = await db.User.get(userId)
const posts = await db.Post.find({ author: user.$id })  // Forward
// Relationships automatically indexed in both directions
```

### Time-Travel Queries
Built-in event sourcing with full audit history. Query your data as it existed at any point in time.

```typescript
// Query state at specific timestamp
const postsLastWeek = await db.Posts.find(
  { status: 'published' },
  { asOf: new Date('2024-01-01') }
)
```

### Cloudflare Workers Support
Native support for Cloudflare Workers with R2 storage and Durable Objects. CQRS architecture with reads directly from R2 and writes through DOs for consistency.

```typescript
// Read from R2 with Cache API
const posts = await db.Posts.find({ status: 'published' })

// Write through Durable Object
await db.Posts.create({ title: 'Hello World' })
```

### TypeScript-First
Full type inference from schema definitions. Get autocomplete for collection names, fields, and query operators.

```typescript
const db = DB({
  User: { email: 'string!#', name: 'string', age: 'int?' }
})

// TypeScript knows your schema
await db.User.create({ email: 'alice@example.com' })  // Autocomplete works!
```

## Quick Example

```typescript
import { DB } from 'parquedb'

// Define your schema with relationships
const db = DB({
  User: {
    email: 'string!#',      // Required, indexed
    name: 'string',
    role: 'string'
  },
  Post: {
    title: 'string!',
    content: 'text',
    status: 'string',
    author: '-> User'       // Relationship to User
  }
})

// Create entities
const user = await db.User.create({
  email: 'alice@example.com',
  name: 'Alice'
})

const post = await db.Post.create({
  title: 'Hello World',
  content: 'My first post!',
  status: 'draft',
  author: user.$id
})

// Query with MongoDB-style filters
const published = await db.Post.find({ status: 'published' })

// Update with operators
await db.Post.update(post.$id, {
  $set: { status: 'published' },
  $inc: { viewCount: 1 }
})

// SQL queries also supported
const results = await db.sql`
  SELECT * FROM posts
  WHERE status = ${'published'}
  ORDER BY createdAt DESC
`
```

## Use Cases

### Edge Databases
Deploy document databases to Cloudflare Workers with R2 storage. Sub-10ms reads globally with Cache API, strong consistency through Durable Objects.

### Content Management
Build CMS systems with the [Payload CMS adapter](./integrations/payload.md). Get MongoDB-like flexibility with Parquet's columnar efficiency and native graph relationships.

### Event Sourcing
Built-in event log captures every change with full audit history. Replay state at any point in time or build projections from the event stream.

### Analytics
Columnar storage enables efficient analytical queries. Scan millions of rows reading only needed columns. Use secondary indexes for 400x faster lookups.

## Performance Highlights

Secondary indexes transform query performance at scale:

| Query Type (100K docs) | Full Scan | With Index | Improvement |
|------------------------|-----------|------------|-------------|
| Equality lookup | 200ms | 0.5ms | **400x faster** |
| Point lookup | 200ms | 0.3ms | **667x faster** |
| Range query | 250ms | 2ms | **125x faster** |

See [Benchmarks](./benchmarks.md) for comprehensive performance data across all operations.

## Getting Started

### Installation & Basic Usage
[Getting Started Guide](./getting-started.md) - Install ParqueDB, define schemas, and perform CRUD operations.

### Schema Definition
[Schema Documentation](./schema-definition.md) - Complete guide to types, validation, relationships, and indexes.

### Query Operations
[Query API](./query-api.md) - MongoDB-style filters, sorting, pagination, and projection.

### Update Operations
[Update Operators](./update-operators.md) - All available operators: `$set`, `$inc`, `$push`, `$link`, and more.

## Tools & Integrations

### ParqueDB Studio
[Studio Documentation](./studio.md) - Launch an admin UI to browse and edit Parquet data with customizable layouts.

```bash
npx parquedb studio
```

### Payload CMS Adapter
[Payload Integration](./integrations/payload.md) - Use ParqueDB as the database backend for Payload CMS.

### SQL Interface
[SQL Documentation](./integrations/sql.md) - Use familiar SQL queries with automatic translation to ParqueDB operations.

## Deployment

### Cloudflare Workers
[Workers Deployment Guide](./deployment/cloudflare-workers.md) - Deploy to the edge with R2 storage and Durable Objects.

### Node.js Server
[Node.js Deployment Guide](./deployment/node-standalone.md) - Run as a standalone server with filesystem or S3 storage.

### R2 Configuration
[R2 Setup Guide](./deployment/r2-setup.md) - Configure Cloudflare R2 for production use.

## Architecture

### Core Concepts
[Architecture Overview](./architecture.md) - Three-file storage model, entity structure, and dual storage architecture.

### Graph Relationships
[Graph-First Architecture](./architecture/graph-first-architecture.md) - Bidirectional relationship indexing and traversal.

### Secondary Indexes
[Index Types](./architecture/secondary-indexes.md) - Hash, SST, full-text, and vector similarity indexes.

### Bloom Filters
[Bloom Filter Indexes](./architecture/bloom-filter-indexes.md) - Probabilistic existence checks for fast negative lookups.

## Why ParqueDB?

ParqueDB fills a unique gap: a document database optimized for Cloudflare Workers with columnar storage and graph relationships.

| Feature | ParqueDB | DuckDB | SQLite | MongoDB |
|---------|----------|--------|--------|---------|
| **Cloudflare Workers** | Native R2/DO | No | Durable Objects | External |
| **Storage Format** | Parquet | Parquet (read) | B-tree | BSON |
| **Query API** | MongoDB | SQL | SQL | MongoDB |
| **Graph Support** | Built-in | Joins | Joins | $lookup |
| **Time-Travel** | Built-in | Snapshots | WAL | Change streams |
| **Pure JavaScript** | Yes | WASM | WASM | Native |

## API Reference

- [ParqueDB API](./parquedb-api.md) - Main database class and configuration
- [Collection API](./collection-api.md) - CRUD operations and query methods
- [Backend API](./backends.md) - Storage backend implementations

## Community & Support

- [GitHub Repository](https://github.com/dot-do/parquedb)
- [Issue Tracker](https://github.com/dot-do/parquedb/issues)
- [Discussions](https://github.com/dot-do/parquedb/discussions)

## License

MIT - see [LICENSE](https://github.com/dot-do/parquedb/blob/main/LICENSE) for details.
