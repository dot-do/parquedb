---
title: Cloudflare Workers Deployment
description: Deploy ParqueDB to Cloudflare Workers with R2 storage and Durable Objects for a production-ready edge database.
---

Deploy ParqueDB to Cloudflare Workers for a globally distributed edge database with R2 storage and Durable Objects for strong consistency.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Setup](#project-setup)
- [wrangler.jsonc Configuration](#wranglerjsonc-configuration)
- [Durable Object Setup](#durable-object-setup)
- [R2 Bucket Configuration](#r2-bucket-configuration)
- [Worker Implementation](#worker-implementation)
- [Deployment](#deployment)
- [Environment Configuration](#environment-configuration)
- [Caching Strategies](#caching-strategies)
- [Monitoring and Debugging](#monitoring-and-debugging)
- [Common Issues and Solutions](#common-issues-and-solutions)
- [Cost Optimization](#cost-optimization)
- [Next Steps](#next-steps)

---

## Architecture Overview

ParqueDB uses a CQRS (Command Query Responsibility Segregation) architecture on Cloudflare Workers:

```
                    +-------------------------------------------------------+
                    |                   Cloudflare Network                   |
                    |                                                        |
    Request ------->|  +---------------+      +----------------------+       |
                    |  |   ParqueDB    |      |   Durable Object     |       |
                    |  |   Worker      |<---->|   (ParqueDBDO)       |       |
                    |  |               |      |   - SQLite metadata  |       |
                    |  |  - Read: R2   |      |   - Write operations |       |
                    |  |  - Write: DO  |      |   - Event log        |       |
                    |  +-------+-------+      +----------------------+       |
                    |          |                                             |
                    |          v                                             |
                    |  +---------------+      +----------------------+       |
                    |  |   Cache API   |      |      R2 Bucket       |       |
                    |  |   (500MB-5GB) |<---->|  - Parquet files     |       |
                    |  |               |      |  - Indexes           |       |
                    |  +---------------+      |  - Relationships     |       |
                    |                         +----------------------+       |
                    +-------------------------------------------------------+
```

**Key Components:**

| Component | Purpose | Details |
|-----------|---------|---------|
| **ParqueDBWorker** | HTTP handler | Routes reads to R2, writes to Durable Objects |
| **ParqueDBDO** | Write consistency | SQLite-backed DO for ACID writes and event log |
| **R2 Bucket** | Data storage | Stores Parquet files, indexes, and relationships |
| **Cache API** | Performance | Caches hot data at edge (500MB free, 5GB+ paid) |

**Read Path (Optimized for Latency):**
1. Request arrives at Worker
2. Check Cache API for cached Parquet data
3. On cache miss, read from R2
4. Cache response for future reads

**Write Path (Optimized for Consistency):**
1. Request arrives at Worker
2. Route to Durable Object by namespace
3. DO writes to SQLite (source of truth)
4. Append event to WAL for batching
5. Periodic flush to R2 as Parquet files
6. Invalidate cache

---

## Prerequisites

### 1. Cloudflare Account

Sign up at [dash.cloudflare.com](https://dash.cloudflare.com). The free tier includes:
- 100,000 Worker requests/day
- 10GB R2 storage
- 1M R2 Class B operations (reads)

**Recommended:** Workers Paid plan ($5/month) for:
- 10M requests/month included
- No daily request limits
- Durable Objects included
- Higher R2 limits

### 2. Wrangler CLI

Install and authenticate:

```bash
# Install Wrangler globally
npm install -g wrangler
# or
pnpm add -g wrangler

# Verify installation
wrangler --version

# Authenticate with Cloudflare
wrangler login

# Verify authentication
wrangler whoami
```

### 3. Node.js 18+

```bash
node --version  # Should be v18.0.0 or higher
```

---

## Quick Start

For an existing project, add ParqueDB Worker support in 5 steps:

```bash
# 1. Install ParqueDB
npm install parquedb

# 2. Create R2 bucket
npx wrangler r2 bucket create my-parquedb-data

# 3. Create wrangler.jsonc (see configuration section)

# 4. Create src/index.ts (see worker implementation section)

# 5. Deploy
npx wrangler deploy
```

---

## Project Setup

### Create New Project

```bash
# Initialize new Worker project
npx wrangler init my-parquedb-api
cd my-parquedb-api

# Install dependencies
npm install parquedb
npm install -D @cloudflare/workers-types
```

### Project Structure

```
my-parquedb-api/
├── src/
│   ├── index.ts          # Worker entrypoint
│   └── types.ts          # TypeScript types
├── wrangler.jsonc        # Cloudflare configuration
├── package.json
└── tsconfig.json
```

### TypeScript Configuration

Update `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "lib": ["ES2022"],
    "types": ["@cloudflare/workers-types"],
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "strict": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "isolatedModules": true,
    "noEmit": true
  },
  "include": ["src/**/*"]
}
```

---

## wrangler.jsonc Configuration

Create `wrangler.jsonc` with the complete ParqueDB configuration:

```jsonc
{
  // =============================================================================
  // Basic Settings
  // =============================================================================

  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-parquedb-api",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-30",
  "compatibility_flags": ["nodejs_compat"],

  // =============================================================================
  // Durable Objects - REQUIRED for write operations
  // =============================================================================

  "durable_objects": {
    "bindings": [
      {
        "name": "PARQUEDB",
        "class_name": "ParqueDBDO"
      }
    ]
  },

  // DO migrations - SQLite-backed Durable Objects
  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["ParqueDBDO"]
    }
  ],

  // =============================================================================
  // R2 Storage - REQUIRED for Parquet file storage
  // =============================================================================

  "r2_buckets": [
    {
      "binding": "BUCKET",
      "bucket_name": "my-parquedb-data",
      "preview_bucket_name": "my-parquedb-data-preview"
    }
  ],

  // =============================================================================
  // Environment Variables (optional)
  // =============================================================================

  "vars": {
    "ENVIRONMENT": "development"
  },

  // =============================================================================
  // Development Settings
  // =============================================================================

  "dev": {
    "port": 8787,
    "local_protocol": "http"
  },

  // =============================================================================
  // Environment Overrides
  // =============================================================================

  "env": {
    "production": {
      "vars": {
        "ENVIRONMENT": "production"
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

### Configuration Reference

| Option | Required | Description |
|--------|----------|-------------|
| `name` | Yes | Worker name (appears in dashboard and URLs) |
| `main` | Yes | Entry point file path |
| `compatibility_date` | Yes | Cloudflare runtime version |
| `compatibility_flags` | Yes | `["nodejs_compat"]` enables Node.js APIs |
| `durable_objects` | Yes | Durable Object class bindings for writes |
| `migrations` | Yes | DO schema migrations (required for SQLite DOs) |
| `r2_buckets` | Yes | R2 bucket bindings for storage |
| `vars` | No | Environment variables |

---

## Durable Object Setup

### How ParqueDBDO Works

The `ParqueDBDO` Durable Object handles all write operations:

1. **SQLite Metadata**: Fast lookups and entity storage
2. **WAL Batching**: Events buffered before writing to Parquet
3. **Sequence Counters**: Generates short IDs using Sqids
4. **Event Sourcing**: Supports time-travel queries

### Export the Durable Object

In your Worker entry point, you MUST export the `ParqueDBDO` class:

```typescript
// src/index.ts
export { ParqueDBDO } from 'parquedb/worker'
```

**Important:** Without this export, Cloudflare cannot instantiate the Durable Object.

### DO Routing Strategy

ParqueDB routes to DOs by namespace:

```typescript
// Each namespace gets its own DO instance
const doId = env.PARQUEDB.idFromName(ns)  // 'posts' -> posts DO
const stub = env.PARQUEDB.get(doId)
```

This provides:
- Namespace isolation
- Parallel writes to different namespaces
- Consistent ordering within a namespace

### Migration Requirements

The `migrations` array in `wrangler.jsonc` is required for SQLite-backed DOs:

```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  }
]
```

When adding new DO classes or changing storage, increment the tag:

```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  },
  {
    "tag": "v2",
    "new_sqlite_classes": ["DatabaseIndexDO"]
  }
]
```

---

## R2 Bucket Configuration

### Create Buckets

```bash
# Production bucket
npx wrangler r2 bucket create my-parquedb-data-prod

# Staging bucket
npx wrangler r2 bucket create my-parquedb-data-staging

# Preview bucket (local development)
npx wrangler r2 bucket create my-parquedb-data-preview

# Verify
npx wrangler r2 bucket list
```

### R2 Binding Configuration

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",              // Access as env.BUCKET
    "bucket_name": "my-parquedb-data",
    "preview_bucket_name": "my-parquedb-data-preview"
  }
]
```

### Data Layout in R2

ParqueDB stores data in a hierarchical structure:

```
my-parquedb-data/
├── data/
│   ├── posts/
│   │   ├── data.parquet          # Entity data
│   │   └── pending/              # Bulk writes pending merge
│   │       └── {ulid}.parquet
│   └── users/
│       └── data.parquet
├── rels/
│   ├── forward/
│   │   └── posts.parquet         # Outgoing relationships
│   └── reverse/
│       └── posts.parquet         # Incoming relationships
├── indexes/
│   └── bloom/
│       └── posts.bloom           # Bloom filters for ID lookups
└── events/
    └── archive/
        └── 2026/01/30/
            └── {checkpoint}.parquet
```

### Optional: CDN Bucket for Public Access

For public datasets with edge caching:

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",
    "bucket_name": "my-parquedb-data"
  },
  {
    "binding": "CDN_BUCKET",
    "bucket_name": "my-parquedb-cdn"
  }
],
"vars": {
  "CDN_R2_DEV_URL": "https://cdn.yourdomain.com/parquedb"
}
```

---

## Worker Implementation

### Basic Worker

Create `src/index.ts`:

```typescript
import { ParqueDBWorker } from 'parquedb/worker'

// REQUIRED: Export Durable Object for Cloudflare runtime
export { ParqueDBDO } from 'parquedb/worker'

// Environment type definition
interface Env {
  PARQUEDB: DurableObjectNamespace
  BUCKET: R2Bucket
  CDN_BUCKET?: R2Bucket
  ENVIRONMENT?: string
  CDN_R2_DEV_URL?: string
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      })
    }

    // Create ParqueDB Worker instance
    const db = new ParqueDBWorker(ctx, env)

    try {
      // Health check
      if (path === '/health') {
        return Response.json({
          status: 'healthy',
          timestamp: new Date().toISOString(),
          environment: env.ENVIRONMENT,
        })
      }

      // REST API routes
      // GET /posts - List posts
      if (path === '/posts' && request.method === 'GET') {
        const filter = url.searchParams.get('filter')
          ? JSON.parse(url.searchParams.get('filter')!)
          : {}
        const limit = parseInt(url.searchParams.get('limit') || '20')

        const result = await db.find('posts', filter, {
          limit,
          sort: { createdAt: -1 },
        })

        return Response.json(result)
      }

      // GET /posts/:id - Get single post
      const getMatch = path.match(/^\/posts\/([^/]+)$/)
      if (getMatch && request.method === 'GET') {
        const post = await db.get('posts', getMatch[1])
        if (!post) {
          return Response.json({ error: 'Not found' }, { status: 404 })
        }
        return Response.json(post)
      }

      // POST /posts - Create post
      if (path === '/posts' && request.method === 'POST') {
        const data = await request.json()
        const post = await db.create('posts', data)
        return Response.json(post, { status: 201 })
      }

      // PATCH /posts/:id - Update post
      const patchMatch = path.match(/^\/posts\/([^/]+)$/)
      if (patchMatch && request.method === 'PATCH') {
        const data = await request.json()
        const result = await db.update('posts', patchMatch[1], { $set: data })
        return Response.json(result)
      }

      // DELETE /posts/:id - Delete post
      const deleteMatch = path.match(/^\/posts\/([^/]+)$/)
      if (deleteMatch && request.method === 'DELETE') {
        await db.delete('posts', deleteMatch[1])
        return Response.json({ success: true })
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      console.error('ParqueDB error:', error)
      return Response.json(
        { error: error instanceof Error ? error.message : 'Internal error' },
        { status: 500 }
      )
    }
  },
}
```

### Using the Built-in HTTP Handler

ParqueDB includes a complete HTTP handler with routing:

```typescript
import parquedbWorker, { ParqueDBDO } from 'parquedb/worker'

export { ParqueDBDO }

export default parquedbWorker
```

This provides these routes:
- `GET /` - API overview
- `GET /health` - Health check
- `GET /datasets` - List datasets
- `GET /datasets/:dataset/:collection` - List entities
- `GET /datasets/:dataset/:collection/:id` - Get entity
- `GET /datasets/:dataset/:collection/:id/:predicate` - Get relationships
- `GET /ns/:namespace` - Query namespace (legacy)
- `GET /debug/*` - Debug endpoints

### ParqueDBWorker API

```typescript
const db = new ParqueDBWorker(ctx, env)

// READ operations (go directly to R2 with caching)
const posts = await db.find('posts', { status: 'published' }, {
  limit: 20,
  sort: { createdAt: -1 },
  project: { title: 1, author: 1 },
})

const post = await db.get('posts', 'abc123')
const count = await db.count('posts', { status: 'draft' })
const exists = await db.exists('posts', { $id: 'posts/abc123' })

// WRITE operations (delegated to Durable Object)
const created = await db.create('posts', {
  $type: 'Post',
  name: 'My Post',
  title: 'Hello World',
  content: '...',
})

await db.update('posts', 'abc123', {
  $set: { title: 'Updated Title' },
  $inc: { views: 1 },
})

await db.delete('posts', 'abc123')

// Relationship operations
await db.link('posts', 'abc123', 'author', 'users', 'user456')
await db.unlink('posts', 'abc123', 'author', 'users', 'user456')
const related = await db.related('posts', 'abc123', { predicate: 'author' })

// Cache management
await db.invalidateCache('posts')
const stats = await db.getCacheStats()
```

---

## Deployment

### Local Development

```bash
# Start development server
npx wrangler dev

# Access at http://localhost:8787
```

Test locally:

```bash
# Health check
curl http://localhost:8787/health

# Create a post
curl -X POST http://localhost:8787/posts \
  -H "Content-Type: application/json" \
  -d '{"$type":"Post","name":"test","title":"Hello"}'

# List posts
curl http://localhost:8787/posts
```

### Deploy to Staging

```bash
npx wrangler deploy --env staging
```

### Deploy to Production

```bash
npx wrangler deploy --env production
```

### Verify Deployment

```bash
# List deployments
npx wrangler deployments list

# Tail live logs
npx wrangler tail

# Filter by errors
npx wrangler tail --status error
```

### Rollback

```bash
# List deployments
npx wrangler deployments list

# Rollback to previous version
npx wrangler rollback
```

### CI/CD with GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Cloudflare Workers

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm ci

      - name: Deploy to Cloudflare Workers
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          environment: production
```

Create the API token:
1. Go to Cloudflare Dashboard > API Tokens
2. Create token with "Edit Cloudflare Workers" permissions
3. Add to GitHub: Settings > Secrets > Actions

---

## Environment Configuration

### Environment Variables

Set non-sensitive values in `wrangler.jsonc`:

```jsonc
"vars": {
  "ENVIRONMENT": "production",
  "LOG_LEVEL": "info",
  "CACHE_DATA_TTL": "60",
  "CACHE_METADATA_TTL": "300",
  "CACHE_BLOOM_TTL": "600"
}
```

### Secrets

Set sensitive values using Wrangler:

```bash
# Set a secret
npx wrangler secret put AUTH_SECRET
# Enter value when prompted

# Set for specific environment
npx wrangler secret put AUTH_SECRET --env production

# List secrets
npx wrangler secret list
```

Access in code:

```typescript
interface Env {
  AUTH_SECRET?: string
}

export default {
  async fetch(request: Request, env: Env) {
    const authHeader = request.headers.get('Authorization')
    if (authHeader !== `Bearer ${env.AUTH_SECRET}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 })
    }
    // ...
  }
}
```

### Custom Domains

Add routes in `wrangler.jsonc`:

```jsonc
"routes": [
  {
    "pattern": "api.example.com/*",
    "zone_name": "example.com"
  }
]
```

---

## Caching Strategies

### Built-in Cache Configurations

```typescript
import {
  ParqueDBWorker,
  DEFAULT_CACHE_CONFIG,      // Balanced (60s data, 300s metadata)
  READ_HEAVY_CACHE_CONFIG,   // Aggressive (300s data, 900s metadata)
  WRITE_HEAVY_CACHE_CONFIG,  // Conservative (15s data, 60s metadata)
  NO_CACHE_CONFIG,           // No caching (debugging)
} from 'parquedb/worker'
```

### Cache TTL Guidelines

| Workload | Data TTL | Metadata TTL | Bloom TTL | Use Case |
|----------|----------|--------------|-----------|----------|
| Read-heavy | 300s | 900s | 1800s | Analytics, dashboards |
| Balanced | 60s | 300s | 600s | General APIs |
| Write-heavy | 15s | 60s | 120s | Real-time updates |

### Configure via Environment

```jsonc
"vars": {
  "CACHE_DATA_TTL": "60",
  "CACHE_METADATA_TTL": "300",
  "CACHE_BLOOM_TTL": "600",
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

---

## Monitoring and Debugging

### Debug Endpoints

The built-in Worker provides debug endpoints:

- `GET /debug/r2` - R2 bucket contents
- `GET /debug/entity?ns=posts&id=abc123` - Entity details
- `GET /debug/indexes` - Index information
- `GET /debug/cache` - Cache statistics

### Cache Statistics

```typescript
const stats = await db.getCacheStats()
// Returns: { hits, misses, hitRatio, cachedBytes, fetchedBytes }
```

### Structured Logging

```typescript
import { logger } from 'parquedb/utils'

logger.info('Query executed', {
  namespace: 'posts',
  filter: { status: 'published' },
  duration: 45,
  rowsReturned: 20,
})
```

### Wrangler Tail

Stream logs in real-time:

```bash
# All logs
npx wrangler tail

# Filter by errors
npx wrangler tail --status error

# Filter by search term
npx wrangler tail --search "posts"

# JSON output for parsing
npx wrangler tail --format json
```

---

## Common Issues and Solutions

### "Durable Object not found"

**Cause:** DO class not exported or migration not applied.

**Solution:**

1. Ensure DO is exported in entry point:
```typescript
export { ParqueDBDO } from 'parquedb/worker'
```

2. Verify migrations in `wrangler.jsonc`:
```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  }
]
```

3. Redeploy:
```bash
npx wrangler deploy
```

### "R2 bucket not found"

**Cause:** Bucket doesn't exist or binding name mismatch.

**Solution:**

1. Verify bucket exists:
```bash
npx wrangler r2 bucket list
```

2. Create if missing:
```bash
npx wrangler r2 bucket create my-parquedb-data
```

3. Check binding name matches code:
```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",  // Must match env.BUCKET in code
    "bucket_name": "my-parquedb-data"
  }
]
```

### "nodejs_compat required"

**Cause:** Missing compatibility flag for Node.js APIs.

**Solution:** Add to `wrangler.jsonc`:
```jsonc
"compatibility_flags": ["nodejs_compat"]
```

### Slow First Request (Cold Start)

**Cause:** Worker cold start + R2 file fetch + cache miss.

**Solutions:**

1. Enable caching with appropriate TTL
2. Use CDN bucket for public datasets
3. Pre-warm cache with a cron trigger:

```jsonc
"triggers": {
  "crons": ["*/5 * * * *"]  // Every 5 minutes
}
```

```typescript
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const db = new ParqueDBWorker(ctx, env)
    await db.find('posts', {}, { limit: 1 })  // Pre-warm cache
  }
}
```

### "Memory limit exceeded"

**Cause:** Loading too much data into Worker memory (128MB limit).

**Solutions:**

1. Use pagination:
```typescript
const posts = await db.find('posts', {}, { limit: 100 })
```

2. Use projection to reduce memory:
```typescript
const posts = await db.find('posts', {}, {
  project: { title: 1, status: 1 }
})
```

### "CPU time limit exceeded"

**Cause:** Query taking too long (50ms limit on free tier).

**Solutions:**

1. Add indexes to frequently queried fields
2. Use limit to reduce result size
3. Upgrade to Workers Unbound for longer CPU time:
```jsonc
"usage_model": "unbound"
```

---

## Cost Optimization

### Estimated Costs (1M requests/day)

| Component | Free Tier | Beyond Free |
|-----------|-----------|-------------|
| Worker Requests | 100K/day | $0.50/million |
| R2 Storage (100GB) | 10GB | $1.35/month |
| R2 Class A (writes) | 1M/month | $4.50/million |
| R2 Class B (reads) | 10M/month | $0.36/million |
| Durable Objects | 400K req/day | $0.15/million |

### Optimization Tips

1. **Enable Aggressive Caching** for read-heavy workloads
2. **Use Projection** to fetch only needed fields
3. **Batch Writes** with `createMany()` (5+ entities go directly to R2)
4. **Use Bloom Filters** for fast negative lookups
5. **Partition Data** by common query fields

---

## Next Steps

- **[R2 Setup Guide](./r2-setup.md)** - Detailed R2 configuration, public access, lifecycle management
- **[Configuration Reference](./configuration.md)** - Complete configuration options
- **[Query API](../queries.md)** - MongoDB-style filtering and pagination
- **[Getting Started](../getting-started.md)** - Basic CRUD operations
- **[Architecture Overview](../architecture.md)** - ParqueDB internals
