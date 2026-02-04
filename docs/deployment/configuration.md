---
title: Configuration
description: Complete reference for all ParqueDB configuration options across Node.js, Cloudflare Workers, and browser environments.
---

Complete reference for all ParqueDB configuration options across different deployment environments.

## Table of Contents

- [Overview](#overview)
- [ParqueDB Options](#parquedb-options)
- [Storage Backends](#storage-backends)
- [Cloudflare Worker Configuration](#cloudflare-worker-configuration)
- [Cache Configuration](#cache-configuration)
- [Index Configuration](#index-configuration)
- [Query Options](#query-options)
- [Environment Variables](#environment-variables)

---

## Overview

ParqueDB configuration varies based on your deployment environment:

| Environment | Configuration Method |
|-------------|---------------------|
| Node.js | Constructor options + environment variables |
| Cloudflare Workers | `wrangler.jsonc` + environment variables |
| Browser | Constructor options only |

---

## ParqueDB Options

### Constructor Options

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

const db = new ParqueDB({
  // Required: Storage backend
  storage: new FsBackend('./data'),

  // Optional: Schema definition
  schema: {
    Post: {
      $ns: 'posts',
      title: 'string!',
      content: 'markdown!',
      status: 'enum(draft,published,archived) = draft',
      author: '-> User.posts'
    },
    User: {
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      posts: '<- Post.author[]'
    }
  },

  // Optional: Default actor for audit trails
  defaultActor: 'system/anonymous',

  // Optional: Enable event sourcing
  eventSourcing: true,

  // Optional: Enable time-travel queries
  timeTravel: true
})
```

### Option Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `storage` | `StorageBackend` | Required | Storage backend instance |
| `schema` | `Record<string, SchemaDefinition>` | `undefined` | Schema definitions for validation |
| `defaultActor` | `string` | `'system/anonymous'` | Default actor ID for operations |
| `eventSourcing` | `boolean` | `false` | Enable event log for CDC |
| `timeTravel` | `boolean` | `false` | Enable point-in-time queries |

---

## Storage Backends

### MemoryBackend

In-memory storage for testing and development.

```typescript
import { MemoryBackend } from 'parquedb'

const storage = new MemoryBackend()

// Data is lost when process exits
const db = new ParqueDB({ storage })
```

| Feature | Support |
|---------|---------|
| Persistence | No |
| Atomic writes | Yes |
| Range reads | Yes |
| Best for | Testing, development |

### FsBackend

Node.js filesystem storage.

```typescript
import { FsBackend } from 'parquedb'

const storage = new FsBackend('./data')

// Or with absolute path
const storage = new FsBackend('/var/lib/parquedb/data')
```

| Feature | Support |
|---------|---------|
| Persistence | Yes |
| Atomic writes | Yes (temp file + rename) |
| Range reads | Yes |
| Best for | Node.js standalone deployments |

**Security**: FsBackend includes path traversal protection. Paths cannot escape the root directory.

### R2Backend

Cloudflare R2 object storage.

```typescript
import { R2Backend } from 'parquedb'

// In a Cloudflare Worker
const storage = new R2Backend(env.BUCKET, {
  prefix: 'parquedb/'  // Optional: key prefix
})
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `prefix` | `string` | `''` | Prefix for all R2 keys |

| Feature | Support |
|---------|---------|
| Persistence | Yes |
| Atomic writes | Yes |
| Range reads | Yes |
| Multipart upload | Yes |
| Best for | Cloudflare Workers production |

### DOSqliteBackend

Cloudflare Durable Object SQLite storage for metadata.

```typescript
import { DOSqliteBackend } from 'parquedb'

// In a Durable Object
const storage = new DOSqliteBackend(this.ctx.storage.sql, {
  prefix: ''  // Optional: key prefix
})
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `prefix` | `string` | `''` | Prefix for all keys |

| Feature | Support |
|---------|---------|
| Persistence | Yes (DO storage) |
| ACID transactions | Yes |
| Max blob size | 2MB |
| Best for | DO metadata, small datasets |

---

## Cloudflare Worker Configuration

### wrangler.jsonc Reference

```jsonc
{
  // =============================================================================
  // Basic Settings
  // =============================================================================

  // JSON schema for IDE autocomplete
  "$schema": "node_modules/wrangler/config-schema.json",

  // Worker name (used in URLs and dashboard)
  "name": "my-parquedb-api",

  // Entry point
  "main": "src/index.ts",

  // Cloudflare runtime version
  "compatibility_date": "2026-01-30",

  // Enable Node.js APIs
  "compatibility_flags": ["nodejs_compat"],

  // =============================================================================
  // Routes (Optional)
  // =============================================================================

  // Custom domain routing
  "routes": [
    {
      "pattern": "api.example.com/*",
      "zone_name": "example.com"
    }
  ],

  // Or use workers.dev subdomain (default)
  // Worker available at: my-parquedb-api.youraccount.workers.dev

  // =============================================================================
  // Durable Objects
  // =============================================================================

  "durable_objects": {
    "bindings": [
      {
        // Binding name (used in code as env.PARQUEDB)
        "name": "PARQUEDB",
        // Class name (must be exported from entry point)
        "class_name": "ParqueDBDO"
      }
    ]
  },

  // DO migrations - required for new DOs
  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["ParqueDBDO"]
    }
  ],

  // =============================================================================
  // R2 Buckets
  // =============================================================================

  "r2_buckets": [
    {
      // Binding name (used in code as env.BUCKET)
      "binding": "BUCKET",
      // Production bucket name
      "bucket_name": "my-parquedb-data",
      // Preview bucket for wrangler dev
      "preview_bucket_name": "my-parquedb-data-preview"
    },
    {
      // Optional: CDN bucket with public access
      "binding": "CDN_BUCKET",
      "bucket_name": "my-parquedb-cdn"
    }
  ],

  // =============================================================================
  // Workers AI (Optional)
  // =============================================================================

  "ai": {
    "binding": "AI"
  },

  // =============================================================================
  // Environment Variables
  // =============================================================================

  "vars": {
    "ENVIRONMENT": "development",
    "LOG_LEVEL": "info",

    // Cache configuration
    "CACHE_DATA_TTL": "60",
    "CACHE_METADATA_TTL": "300",
    "CACHE_BLOOM_TTL": "600",
    "CACHE_STALE_WHILE_REVALIDATE": "true",

    // CDN URL for edge caching
    "CDN_R2_DEV_URL": "https://cdn.example.com/parquedb"
  },

  // =============================================================================
  // Development Settings
  // =============================================================================

  "dev": {
    "port": 8787,
    "local_protocol": "http",
    "ip": "0.0.0.0"  // Listen on all interfaces
  },

  // =============================================================================
  // Build Configuration
  // =============================================================================

  "build": {
    "command": "npm run build",
    "watch_dir": "src"
  },

  // =============================================================================
  // Limits
  // =============================================================================

  "limits": {
    "cpu_ms": 50  // Max CPU time per request (ms)
  },

  // =============================================================================
  // Environment Overrides
  // =============================================================================

  "env": {
    "production": {
      "vars": {
        "ENVIRONMENT": "production",
        "LOG_LEVEL": "warn"
      },
      "r2_buckets": [
        {
          "binding": "BUCKET",
          "bucket_name": "my-parquedb-data-prod"
        }
      ]
    },
    "staging": {
      "vars": {
        "ENVIRONMENT": "staging"
      },
      "r2_buckets": [
        {
          "binding": "BUCKET",
          "bucket_name": "my-parquedb-data-staging"
        }
      ]
    }
  }
}
```

### wrangler.jsonc Options Reference

| Option | Type | Description |
|--------|------|-------------|
| `name` | `string` | Worker name |
| `main` | `string` | Entry point file |
| `compatibility_date` | `string` | Runtime version (YYYY-MM-DD) |
| `compatibility_flags` | `string[]` | Feature flags |
| `routes` | `Route[]` | Custom domain routing |
| `durable_objects` | `object` | DO bindings |
| `migrations` | `Migration[]` | DO migrations |
| `r2_buckets` | `R2Binding[]` | R2 bucket bindings |
| `ai` | `object` | Workers AI binding |
| `vars` | `Record<string, string>` | Environment variables |
| `dev` | `object` | Development settings |
| `build` | `object` | Build configuration |
| `limits` | `object` | Resource limits |
| `env` | `object` | Environment-specific overrides |

---

## Cache Configuration

### CacheConfig Options

```typescript
import { CacheStrategy, DEFAULT_CACHE_CONFIG } from 'parquedb/worker'

interface CacheConfig {
  // TTL for Parquet data files (seconds)
  dataTtl: number

  // TTL for metadata/schema (seconds)
  metadataTtl: number

  // TTL for bloom filters (seconds)
  bloomTtl: number

  // Use stale-while-revalidate
  staleWhileRevalidate: boolean

  // Maximum size to cache (bytes, 0 = no limit)
  maxCacheSize?: number
}
```

### Preset Configurations

```typescript
import {
  DEFAULT_CACHE_CONFIG,
  READ_HEAVY_CACHE_CONFIG,
  WRITE_HEAVY_CACHE_CONFIG,
  NO_CACHE_CONFIG
} from 'parquedb/worker'
```

| Preset | Data TTL | Metadata TTL | Bloom TTL | Use Case |
|--------|----------|--------------|-----------|----------|
| `DEFAULT_CACHE_CONFIG` | 60s | 300s | 600s | Balanced workloads |
| `READ_HEAVY_CACHE_CONFIG` | 300s | 900s | 1800s | Analytics, dashboards |
| `WRITE_HEAVY_CACHE_CONFIG` | 15s | 60s | 120s | Frequently updated data |
| `NO_CACHE_CONFIG` | 0 | 0 | 0 | Development, debugging |

### Environment-Based Configuration

```typescript
import { createCacheStrategy } from 'parquedb/worker'

// Reads from env vars:
// - CACHE_DATA_TTL
// - CACHE_METADATA_TTL
// - CACHE_BLOOM_TTL
// - CACHE_STALE_WHILE_REVALIDATE
const strategy = createCacheStrategy(env)
```

### Custom Configuration

```typescript
const customConfig: CacheConfig = {
  dataTtl: 120,          // 2 minutes
  metadataTtl: 600,      // 10 minutes
  bloomTtl: 1800,        // 30 minutes
  staleWhileRevalidate: true
}

const strategy = new CacheStrategy(customConfig)
```

---

## Index Configuration

### Secondary Index Types

ParqueDB supports multiple index types:

| Index Type | Use Case | Lookup Time |
|------------|----------|-------------|
| `HashIndex` | Exact equality (`status = 'published'`) | O(1) |
| `SSTIndex` | Range queries (`price >= 100`) | O(log n) |
| `FTSIndex` | Full-text search | O(1) per term |
| `BloomFilter` | Negative lookups (ID not exists) | O(1) |

### Index Configuration

```typescript
import { IndexManager, HashIndex, SSTIndex, FTSIndex } from 'parquedb'

// Hash index for equality lookups
const statusIndex = new HashIndex(storage, 'posts', 'status')
await statusIndex.build()

// SST index for range queries
const dateIndex = new SSTIndex(storage, 'posts', 'createdAt')
await dateIndex.build()

// Full-text search index
const ftsIndex = new FTSIndex(storage, 'posts', ['title', 'content'])
await ftsIndex.build()
```

### Index Catalog

Indexes are tracked in `{namespace}/indexes/_catalog.json`:

```json
{
  "indexes": [
    {
      "name": "status_hash",
      "type": "hash",
      "field": "status",
      "path": "posts/indexes/status_hash.idx",
      "createdAt": "2026-01-30T12:00:00Z"
    },
    {
      "name": "createdAt_sst",
      "type": "sst",
      "field": "createdAt",
      "path": "posts/indexes/createdAt_sst.sst",
      "createdAt": "2026-01-30T12:00:00Z"
    }
  ]
}
```

---

## Query Options

### FindOptions

```typescript
interface FindOptions<T = unknown> {
  // Maximum results to return
  limit?: number

  // Number of results to skip (offset pagination)
  skip?: number

  // Cursor for cursor-based pagination
  cursor?: string

  // Sort specification
  sort?: Record<string, 1 | -1 | 'asc' | 'desc'>

  // Projection (fields to include/exclude)
  project?: Record<string, 0 | 1>

  // Index hint
  hint?: { index: string }

  // Include deleted entities
  includeDeleted?: boolean
}
```

### Usage Examples

```typescript
// Pagination with limit and cursor
const page1 = await db.Posts.find({}, { limit: 20 })
const page2 = await db.Posts.find({}, { limit: 20, cursor: page1.nextCursor })

// Sorting
const newest = await db.Posts.find({}, {
  sort: { createdAt: -1, title: 1 }
})

// Projection (include only specific fields)
const titles = await db.Posts.find({}, {
  project: { title: 1, status: 1 }
})

// Projection (exclude fields)
const noContent = await db.Posts.find({}, {
  project: { content: 0, rawHtml: 0 }
})

// Index hint
const byStatus = await db.Posts.find({ status: 'published' }, {
  hint: { index: 'status_hash' }
})
```

### GetOptions

```typescript
interface GetOptions {
  // Include deleted entities
  includeDeleted?: boolean

  // Populate relationships
  populate?: string[]
}
```

### CreateOptions

```typescript
interface CreateOptions {
  // Actor performing the operation (for audit)
  actor?: string

  // Skip schema validation
  skipValidation?: boolean
}
```

### UpdateOptions

```typescript
interface UpdateOptions {
  // Actor performing the operation
  actor?: string

  // Expected version for optimistic concurrency
  expectedVersion?: number

  // Create if not exists
  upsert?: boolean
}
```

### DeleteOptions

```typescript
interface DeleteOptions {
  // Actor performing the operation
  actor?: string

  // Hard delete (permanent, no soft delete)
  hard?: boolean

  // Expected version for optimistic concurrency
  expectedVersion?: number
}
```

---

## Environment Variables

### Node.js Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | HTTP server port |
| `NODE_ENV` | `development` | Environment name |
| `DATA_DIR` | `./data` | Data directory path |
| `LOG_LEVEL` | `info` | Logging level (debug, info, warn, error) |

### Cloudflare Worker Environment Variables

Set in `wrangler.jsonc` under `vars`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `development` | Environment name |
| `LOG_LEVEL` | `info` | Logging level |
| `CACHE_DATA_TTL` | `60` | Data cache TTL (seconds) |
| `CACHE_METADATA_TTL` | `300` | Metadata cache TTL (seconds) |
| `CACHE_BLOOM_TTL` | `600` | Bloom filter cache TTL (seconds) |
| `CACHE_STALE_WHILE_REVALIDATE` | `true` | Enable stale-while-revalidate |
| `CDN_R2_DEV_URL` | - | CDN URL for public R2 access |

### Secrets (Cloudflare)

Set via Wrangler CLI (not in config files):

```bash
# Set a secret
npx wrangler secret put AUTH_SECRET

# Set for specific environment
npx wrangler secret put AUTH_SECRET --env production

# Delete a secret
npx wrangler secret delete AUTH_SECRET
```

| Secret | Description |
|--------|-------------|
| `AUTH_SECRET` | Authentication secret for API tokens |

---

## Performance Tuning Options

### Query Performance

```typescript
// Enable query explain for debugging
const plan = await db.Posts.explain({ status: 'published' })
console.log(plan)
// {
//   usesIndex: true,
//   indexName: 'status_hash',
//   estimatedRows: 1000,
//   scanType: 'index_lookup'
// }
```

### Memory Limits

For Cloudflare Workers, memory is limited to 128MB. For large queries:

```typescript
// Use pagination
const allPosts: Post[] = []
let cursor: string | undefined

do {
  const result = await db.Posts.find({}, { limit: 100, cursor })
  allPosts.push(...result.items)
  cursor = result.nextCursor
} while (cursor)

// Use projection to reduce memory
const ids = await db.Posts.find({}, {
  project: { $id: 1 }
})
```

### Worker CPU Limits

Default CPU limit is 50ms per request. For complex queries:

```typescript
// Split into multiple requests
// Or increase limit in wrangler.jsonc:
{
  "limits": {
    "cpu_ms": 100  // Max 100ms CPU time
  }
}
```

---

## Write Scaling

When deploying to Cloudflare Workers, ParqueDB uses Durable Objects which have an inherent write throughput limit of approximately 30 requests per second per namespace due to their single-writer consistency model.

For most applications, this limit is sufficient. However, high-write workloads may require mitigation strategies:

- **Write batching**: Combine multiple writes into single requests
- **Queue-based writes**: Use Cloudflare Queues to absorb traffic spikes
- **Namespace sharding**: Route different entity types to separate DOs
- **Time-bucket partitioning**: Route time-series data to time-bucketed DOs

See [Write Scaling Guide](../architecture/WRITE_SCALING.md) for detailed guidance on when and how to scale writes.

---

## Next Steps

- [Cloudflare Workers Guide](./cloudflare-workers.md) - Deploy to Cloudflare
- [Node.js Standalone Guide](./node-standalone.md) - Deploy to Node.js
- [R2 Setup Guide](./r2-setup.md) - Configure R2 storage
- [Write Scaling Guide](../architecture/WRITE_SCALING.md) - Scale write throughput
- [Architecture Overview](../architecture/) - Understand internals
