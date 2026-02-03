# ParqueDB

[![npm version](https://img.shields.io/npm/v/parquedb.svg)](https://www.npmjs.com/package/parquedb)
[![Build Status](https://github.com/dot-do/parquedb/workflows/CI/badge.svg)](https://github.com/dot-do/parquedb/actions)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A hybrid relational/document/graph database built on Apache Parquet. ParqueDB provides a MongoDB-style API with bidirectional relationships, time-travel queries, and first-class support for Node.js, browsers, and Cloudflare Workers.

## Why ParqueDB?

ParqueDB fills a unique gap in the database landscape: a document database with columnar storage, graph relationships, and native Cloudflare Workers support.

### Comparison with Alternatives

| Feature | ParqueDB | DuckDB | SQLite | MongoDB |
|---------|----------|--------|--------|---------|
| **Cloudflare Workers** | Native R2/DO support | No native support | Durable Objects only | External service |
| **Storage Format** | Parquet (columnar) | Parquet (read) | B-tree pages | BSON documents |
| **Query API** | MongoDB-style | SQL | SQL | MongoDB |
| **Graph Relationships** | Built-in bidirectional | Joins only | Joins only | $lookup only |
| **Time-Travel** | Built-in event sourcing | Snapshots | WAL replay | Change streams |
| **Compression** | 8-10x (Parquet) | 8-10x | 2-3x | 2-3x |
| **Pure JavaScript** | Yes (hyparquet) | WASM | WASM | Native driver |

### Key Differentiators

1. **Built for Cloudflare Workers** - The only document database designed from the ground up for the Workers runtime with R2 storage and Durable Objects for consistency.

2. **Columnar + Document Flexibility** - Get Parquet's analytical performance (read only needed columns) with MongoDB's schema flexibility. Variant shredding delivers up to 20x faster queries on indexed columns.

3. **First-Class Graph Relationships** - Bidirectional relationships are indexed and traversable from either direction. No joins, no $lookup, no additional queries.

4. **Event Sourcing Built-In** - Full audit history with point-in-time reconstruction. Query your data as it existed at any moment in time.

### Performance Highlights

Secondary indexes transform query performance at scale:

| Query Type (100K docs) | Full Scan | With Index | Improvement |
|------------------------|-----------|------------|-------------|
| Equality lookup | 200ms | 0.5ms | **400x faster** |
| Point lookup | 200ms | 0.3ms | **667x faster** |
| Range query | 250ms | 2ms | **125x faster** |

See [BENCHMARKS.md](./docs/BENCHMARKS.md) for comprehensive performance data.

## Features

- **Parquet-native columnar storage** - Efficient compression and predicate pushdown using Apache Parquet via hyparquet
- **Graph database with relationships** - Bidirectional relationship indexing with forward/reverse traversal
- **MongoDB-style query API** - Familiar filter and update operators
- **Event sourcing with time-travel** - Full audit history with point-in-time reconstruction
- **Multi-environment support** - Runs in Node.js, browsers, and Cloudflare Workers
- **CQRS architecture for Workers** - Reads direct to R2 with caching, writes through Durable Objects
- **Secondary indexes** - Hash, SST, and full-text search indexes for fast lookups

## Installation

```bash
npm install parquedb
```

## Quick Start

```typescript
import { DB } from 'parquedb'

// Define your schema
const db = DB({
  User: { email: 'string!#', name: 'string', role: 'string' },
  Post: { title: 'string!', content: 'text', author: '-> User' }
})

// CRUD operations
const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
const posts = await db.Post.find({ author: user.$id })
await db.Post.update(posts[0].$id, { $set: { title: 'Updated' } })

// SQL queries
const results = await db.sql`SELECT * FROM posts WHERE author = ${user.$id}`
```

## Storage Backends

ParqueDB supports multiple storage backends for different environments:

| Backend | Environment | Description |
|---------|-------------|-------------|
| `MemoryBackend` | All | In-memory storage for testing and development |
| `FsBackend` | Node.js | Local filesystem storage |
| `R2Backend` | Cloudflare Workers | Cloudflare R2 object storage |
| `FsxBackend` | Cloudflare Workers | Cloudflare Worker filesystem |
| `DOSqliteBackend` | Cloudflare Workers | Durable Object SQLite for metadata |

```typescript
import { ParqueDB, FsBackend, R2Backend } from 'parquedb'

// Node.js with filesystem
const db = new ParqueDB({
  storage: new FsBackend({ root: './data' })
})

// Cloudflare Workers with R2
const db = new ParqueDB({
  storage: new R2Backend(env.BUCKET, { prefix: 'parquedb/' })
})
```

## Type-Safe Configuration

ParqueDB supports two approaches for full TypeScript type safety:

### Option 1: Schema Export

```typescript
// parquedb.config.ts
import { defineConfig, defineSchema } from 'parquedb/config'

export const schema = defineSchema({
  User: { email: 'string!#', name: 'string' },
  Post: { title: 'string!', author: '-> User' }
})

export default defineConfig({ storage: 'fs', schema })

// src/db.ts
import { DB } from 'parquedb'
import { schema } from '../parquedb.config'
export const db = DB(schema)  // Fully typed!
```

### Option 2: Generate Types

```bash
npx parquedb generate --output src/db.generated.ts
```

This reads `parquedb.config.ts` and generates typed exports.

## ParqueDB Studio

Launch an admin UI for your Parquet data:

```bash
npx parquedb studio
```

Configure layouts in your schema:

```typescript
const db = DB({
  Post: {
    title: 'string!', content: 'text', status: 'string',
    $layout: [['title'], 'content'],           // Field arrangement
    $sidebar: ['status', 'createdAt'],         // Sidebar fields
    $studio: { label: 'Blog Posts', status: { options: ['draft', 'published'] } }
  }
})
```

See [Studio Documentation](./docs/studio.md) for details.

## Payload CMS Adapter

Use ParqueDB as a database backend for [Payload CMS](https://payloadcms.com/):

```typescript
import { buildConfig } from 'payload'
import { parquedbAdapter } from 'parquedb/payload'
import { FsBackend } from 'parquedb'

export default buildConfig({
  db: parquedbAdapter({ storage: new FsBackend('./data') }),
  collections: [{ slug: 'posts', fields: [{ name: 'title', type: 'text' }] }]
})
```

Deploy to Cloudflare Workers with R2 storage. See [Payload Adapter Documentation](./docs/integrations/payload.md).

## Query Operators

ParqueDB supports MongoDB-style filter operators:

### Unknown Operator Validation

ParqueDB can validate query operators to catch typos and incorrect usage. Configure the behavior for unknown operators:

```typescript
import { setFilterConfig } from 'parquedb'

// Configure globally
setFilterConfig({ unknownOperatorBehavior: 'warn' })  // Log warnings (recommended)
setFilterConfig({ unknownOperatorBehavior: 'error' }) // Throw errors (strict)
setFilterConfig({ unknownOperatorBehavior: 'ignore' }) // Silent (default, backward compatible)

// Or pass config per query
const results = await db.Posts.find(
  { score: { $customOp: 100 } },  // Will trigger validation
  { config: { unknownOperatorBehavior: 'error' } }
)
```

This helps catch common mistakes like:
- Typos: `{ score: { $get: 50 } }` instead of `{ $gte: 50 }`
- Wrong case: `{ score: { $GT: 50 } }` instead of `{ $gt: 50 }`
- Invalid operators: `{ name: { $match: 'test' } }` (use `$regex` instead)

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equals | `{ status: { $eq: 'published' } }` |
| `$ne` | Not equals | `{ status: { $ne: 'draft' } }` |
| `$gt` | Greater than | `{ views: { $gt: 100 } }` |
| `$gte` | Greater than or equal | `{ views: { $gte: 100 } }` |
| `$lt` | Less than | `{ views: { $lt: 1000 } }` |
| `$lte` | Less than or equal | `{ views: { $lte: 1000 } }` |
| `$in` | In array | `{ status: { $in: ['published', 'featured'] } }` |
| `$nin` | Not in array | `{ status: { $nin: ['draft', 'archived'] } }` |

### String Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$regex` | Regular expression | `{ title: { $regex: '^Hello' } }` |
| `$startsWith`* | Starts with prefix | `{ title: { $startsWith: 'Hello' } }` |
| `$endsWith`* | Ends with suffix | `{ title: { $endsWith: 'World' } }` |
| `$contains`* | Contains substring | `{ title: { $contains: 'ello' } }` |

*ParqueDB Extension - These operators are not available in MongoDB. Use `$regex` for MongoDB compatibility (e.g., `$regex: '^prefix'`, `$regex: 'suffix$'`, `$regex: 'substring'`).

### Array Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$all` | Contains all values | `{ tags: { $all: ['tech', 'db'] } }` |
| `$elemMatch` | Element matches filter | `{ items: { $elemMatch: { qty: { $gt: 5 } } } }` |
| `$size` | Array has size | `{ tags: { $size: 3 } }` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$and` | All conditions match | `{ $and: [{ status: 'published' }, { views: { $gt: 100 } }] }` |
| `$or` | Any condition matches | `{ $or: [{ status: 'featured' }, { views: { $gt: 1000 } }] }` |
| `$not` | Condition does not match | `{ $not: { status: 'draft' } }` |
| `$nor` | None of conditions match | `{ $nor: [{ status: 'draft' }, { status: 'archived' }] }` |

### Special Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$exists` | Field exists | `{ email: { $exists: true } }` |
| `$type` | Field type check | `{ data: { $type: 'object' } }` |
| `$text` | Full-text search | `{ $text: { $search: 'parquet database' } }` |
| `$vector` | Vector similarity | `{ $vector: { $near: embedding, $k: 10, $field: 'embedding' } }` |

## Update Operators

ParqueDB supports MongoDB-style update operators:

### Field Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$set` | Set field values | `{ $set: { status: 'published' } }` |
| `$unset` | Remove fields | `{ $unset: { tempField: '' } }` |
| `$rename` | Rename fields | `{ $rename: { oldName: 'newName' } }` |

### Numeric Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$inc` | Increment | `{ $inc: { views: 1 } }` |
| `$mul` | Multiply | `{ $mul: { price: 1.1 } }` |
| `$min` | Set to minimum | `{ $min: { lowScore: 50 } }` |
| `$max` | Set to maximum | `{ $max: { highScore: 100 } }` |

### Array Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$push` | Add to array | `{ $push: { tags: 'new' } }` |
| `$pull` | Remove from array | `{ $pull: { tags: 'old' } }` |
| `$addToSet` | Add if not exists | `{ $addToSet: { tags: 'unique' } }` |
| `$pop` | Remove first/last | `{ $pop: { tags: 1 } }` |

### Relationship Operators (ParqueDB-specific)

| Operator | Description | Example |
|----------|-------------|---------|
| `$link` | Create relationship | `{ $link: { author: 'users/123' } }` |
| `$unlink` | Remove relationship | `{ $unlink: { author: 'users/123' } }` |

## Relationships

ParqueDB provides first-class support for bidirectional relationships:

```typescript
// Create a relationship
await db.Posts.update(postId, {
  $link: { author: 'users/nathan' }
})

// Relationships are bidirectional - query from either side
const userPosts = await db.Users.get('users/nathan', {
  populate: ['posts']  // Reverse traversal
})

// Link multiple targets
await db.Posts.update(postId, {
  $link: {
    categories: ['categories/tech', 'categories/databases']
  }
})

// Remove relationships
await db.Posts.update(postId, {
  $unlink: { categories: 'categories/tech' }
})
```

## Cloudflare Workers

ParqueDB provides a CQRS architecture optimized for Cloudflare Workers:

- **Reads**: Direct to R2 with Cache API for high throughput
- **Writes**: Routed through Durable Objects for strong consistency

```typescript
import { ParqueDBWorker } from 'parquedb/worker'

export default {
  async fetch(request, env, ctx) {
    const db = new ParqueDBWorker(ctx, env)

    // Find with caching
    const posts = await db.find('posts', { status: 'published' })

    // Write through DO
    const post = await db.create('posts', {
      $type: 'Post',
      name: 'New Post',
      title: 'New Post'
    })

    return Response.json(posts)
  }
}
```

### wrangler.toml

```toml
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-data"

[[durable_objects.bindings]]
name = "PARQUEDB"
class_name = "ParqueDBDO"
```

## Indexes

ParqueDB supports multiple index types for optimized queries:

```typescript
import { IndexManager, HashIndex, SSTIndex, FTSIndex } from 'parquedb'

// Hash index for exact lookups
const hashIndex = new HashIndex(backend, 'posts', 'status')
await hashIndex.build()

// SST index for range queries
const sstIndex = new SSTIndex(backend, 'posts', 'createdAt')
await sstIndex.build()

// Full-text search index
const ftsIndex = new FTSIndex(backend, 'posts', ['title', 'content'])
await ftsIndex.build()
```

## Examples

See the [examples](./examples) folder for complete examples:

- [IMDB](./examples/imdb) - Movie database with actors and directors
- [Wikidata](./examples/wikidata) - Knowledge graph import
- [UNSPSC](./examples/unspsc) - Product classification hierarchy
- [O*NET](./examples/onet) - Occupational data
- [Wiktionary](./examples/wiktionary) - Dictionary entries
- [Common Crawl](./examples/commoncrawl) - Web crawl data

## Documentation

- [Getting Started](./docs/getting-started.md)
- [ParqueDB Studio](./docs/studio.md) - Admin UI
- [Payload CMS Adapter](./docs/integrations/payload.md)
- [Architecture Overview](./docs/architecture)
  - [Graph-First Architecture](./docs/architecture/GRAPH_FIRST_ARCHITECTURE.md)
  - [Secondary Indexes](./docs/architecture/SECONDARY_INDEXES.md)
  - [Namespace Sharded Architecture](./docs/architecture/NAMESPACE_SHARDED_ARCHITECTURE.md)

## License

MIT
