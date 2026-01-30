# ParqueDB

A Parquet-native database with graph relationships, event sourcing, and first-class support for Node.js, browsers, and Cloudflare Workers.

## Features

- **Parquet-native columnar storage** - Efficient compression and predicate pushdown using Apache Parquet via hyparquet
- **Graph database with relationships** - Bidirectional relationship indexing with forward/reverse traversal
- **Multi-environment support** - Runs in Node.js, browsers, and Cloudflare Workers
- **Event sourcing with time-travel queries** - Full audit history with point-in-time reconstruction
- **CQRS architecture for Workers** - Reads direct to R2 with caching, writes through Durable Objects for consistency
- **MongoDB-style query API** - Familiar filter operators ($eq, $gt, $in, $regex, etc.) and update operators ($set, $inc, $push, etc.)

## Quick Start

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({ storage: new MemoryBackend() })

// Create
const post = await db.Posts.create({
  $type: 'Post',
  name: 'Hello World',
  title: 'Hello World',
  status: 'published'
})

// Query
const posts = await db.Posts.find({ title: { $startsWith: 'Hello' } })

// Update with operators
await db.Posts.update(post.$id, {
  $set: { status: 'featured' },
  $inc: { views: 1 }
})

// Relationships
await db.Posts.update(post.$id, {
  $link: { author: 'users/nathan' }
})
```

## Installation

```bash
npm install parquedb
```

## Storage Backends

ParqueDB supports multiple storage backends:

| Backend | Environment | Description |
|---------|-------------|-------------|
| `MemoryBackend` | All | In-memory storage for testing and development |
| `FsBackend` | Node.js | Local filesystem storage |
| `R2Backend` | Cloudflare Workers | Cloudflare R2 object storage |
| `FsxBackend` | Cloudflare Workers | Cloudflare Worker filesystem |
| `DOSqliteBackend` | Cloudflare Workers | Durable Object SQLite for metadata |

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

// Node.js with filesystem
const db = new ParqueDB({
  storage: new FsBackend({ root: './data' })
})
```

See [docs/architecture](./docs/architecture) for detailed storage architecture documentation.

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

See [docs/architecture/NAMESPACE_SHARDED_ARCHITECTURE.md](./docs/architecture/NAMESPACE_SHARDED_ARCHITECTURE.md) for Worker architecture details.

## Examples

See the [examples](./examples) folder for complete examples:

- [IMDB](./examples/imdb) - Movie database with actors and directors
- [Wikidata](./examples/wikidata) - Knowledge graph import
- [UNSPSC](./examples/unspsc) - Product classification hierarchy
- [O*NET](./examples/onet) - Occupational data
- [Wiktionary](./examples/wiktionary) - Dictionary entries
- [Common Crawl](./examples/commoncrawl) - Web crawl data

## Documentation

- [Architecture Overview](./docs/architecture)
  - [Graph-First Architecture](./docs/architecture/GRAPH_FIRST_ARCHITECTURE.md)
  - [Graph Edge Indexing](./docs/architecture/GRAPH_EDGE_INDEXING.md)
  - [Graph Query Patterns](./docs/architecture/GRAPH_QUERY_PATTERNS.md)
  - [Secondary Indexes](./docs/architecture/SECONDARY_INDEXES.md)
  - [Bloom Filter Indexes](./docs/architecture/BLOOM_FILTER_INDEXES.md)
  - [Namespace Sharded Architecture](./docs/architecture/NAMESPACE_SHARDED_ARCHITECTURE.md)

## License

MIT
