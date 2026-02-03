---
title: Worker Integration Guide
description: Complete guide for integrating ParqueDB with Cloudflare Workers, including DO configuration, R2 setup, authentication, and deployment.
---

This guide provides practical guidance for deploying ParqueDB to Cloudflare Workers with Durable Objects for writes and R2 for storage.

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [Worker Setup](#worker-setup)
- [Durable Object Configuration](#durable-object-configuration)
- [R2 Integration](#r2-integration)
- [Authentication](#authentication)
- [Rate Limiting](#rate-limiting)
- [Cache Invalidation](#cache-invalidation)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

Get ParqueDB running on Cloudflare Workers in 5 minutes:

```bash
# 1. Install ParqueDB
npm install parquedb

# 2. Create R2 bucket
npx wrangler r2 bucket create my-parquedb-data

# 3. Copy wrangler.jsonc template (see below)

# 4. Create src/index.ts (see below)

# 5. Deploy
npx wrangler deploy
```

### Minimal wrangler.jsonc

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-parquedb-api",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-30",
  "compatibility_flags": ["nodejs_compat"],

  "durable_objects": {
    "bindings": [{ "name": "PARQUEDB", "class_name": "ParqueDBDO" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["ParqueDBDO"] }],
  "r2_buckets": [{ "binding": "BUCKET", "bucket_name": "my-parquedb-data" }]
}
```

### Minimal src/index.ts

```typescript
import parquedbWorker, { ParqueDBDO } from 'parquedb/worker'

// REQUIRED: Export DO class for Cloudflare runtime
export { ParqueDBDO }

// Use built-in HTTP handler
export default parquedbWorker
```

---

## Architecture Overview

ParqueDB uses CQRS (Command Query Responsibility Segregation) on Cloudflare Workers:

```
                 +---------------------------------------------------------+
                 |                    Cloudflare Edge                       |
                 |                                                          |
   Request ----> |  +----------------+        +------------------------+    |
                 |  |  ParqueDBWorker|        |    ParqueDBDO          |    |
                 |  |  (HTTP handler)|<------>|    (Durable Object)    |    |
                 |  |                |        |                        |    |
                 |  | READ: R2 direct|        | - SQLite (metadata)    |    |
                 |  | WRITE: via DO  |        | - Event sourcing       |    |
                 |  +-------+--------+        | - WAL batching         |    |
                 |          |                 | - Cache invalidation   |    |
                 |          v                 +------------------------+    |
                 |  +----------------+        +------------------------+    |
                 |  |   Cache API    |        |      R2 Bucket         |    |
                 |  |  (edge cache)  |<------>|  - Parquet files       |    |
                 |  +----------------+        |  - Indexes             |    |
                 |                            |  - Relationships       |    |
                 |                            +------------------------+    |
                 +---------------------------------------------------------+
```

### Read Path (Low Latency)

1. Check Cache API for cached data
2. On miss, read from R2
3. Cache response for future requests

### Write Path (Strong Consistency)

1. Route to Durable Object by namespace
2. DO writes to SQLite (source of truth)
3. Append event to WAL for batching
4. Flush to R2 as Parquet files periodically
5. Signal cache invalidation to Workers

---

## Worker Setup

### Project Structure

```
my-parquedb-api/
├── src/
│   ├── index.ts          # Worker entry point
│   └── types.ts          # Environment types
├── wrangler.jsonc        # Cloudflare configuration
├── package.json
└── tsconfig.json
```

### Environment Type Definition

Create `src/types.ts`:

```typescript
export interface Env {
  // Durable Object binding
  PARQUEDB: DurableObjectNamespace

  // R2 storage
  BUCKET: R2Bucket
  CDN_BUCKET?: R2Bucket

  // Optional: Rate limiting
  RATE_LIMITER?: DurableObjectNamespace

  // Optional: Database index
  DATABASE_INDEX?: DurableObjectNamespace

  // Environment variables
  ENVIRONMENT?: string
  CDN_R2_DEV_URL?: string

  // Cache configuration
  CACHE_DATA_TTL?: string
  CACHE_METADATA_TTL?: string
  CACHE_BLOOM_TTL?: string
}
```

### Custom Worker Implementation

For custom routing and business logic:

```typescript
import { ParqueDBWorker, ParqueDBDO, buildErrorResponse } from 'parquedb/worker'
import type { Env } from './types'

export { ParqueDBDO }

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname
    const startTime = performance.now()

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      })
    }

    // Create ParqueDB worker instance
    const db = new ParqueDBWorker(ctx, env)

    try {
      // Health check
      if (path === '/health') {
        return Response.json({
          status: 'healthy',
          timestamp: new Date().toISOString(),
        })
      }

      // Custom API routes
      if (path.startsWith('/api/posts')) {
        return handlePostsAPI(request, db, path)
      }

      // 404
      return Response.json({ error: 'Not Found' }, { status: 404 })
    } catch (error) {
      return buildErrorResponse(request, error as Error, 500, startTime)
    }
  },
}

async function handlePostsAPI(
  request: Request,
  db: ParqueDBWorker,
  path: string
): Promise<Response> {
  const method = request.method

  // GET /api/posts - List posts
  if (path === '/api/posts' && method === 'GET') {
    const result = await db.find('posts', {}, {
      sort: { createdAt: -1 },
      limit: 20,
    })
    return Response.json(result)
  }

  // GET /api/posts/:id - Get single post
  const getMatch = path.match(/^\/api\/posts\/([^/]+)$/)
  if (getMatch && method === 'GET') {
    const post = await db.get('posts', getMatch[1]!)
    if (!post) {
      return Response.json({ error: 'Not found' }, { status: 404 })
    }
    return Response.json(post)
  }

  // POST /api/posts - Create post
  if (path === '/api/posts' && method === 'POST') {
    const data = await request.json() as Record<string, unknown>
    const post = await db.create('posts', data)
    return Response.json(post, { status: 201 })
  }

  // PATCH /api/posts/:id - Update post
  const patchMatch = path.match(/^\/api\/posts\/([^/]+)$/)
  if (patchMatch && method === 'PATCH') {
    const data = await request.json() as Record<string, unknown>
    const result = await db.update('posts', patchMatch[1]!, { $set: data })
    return Response.json(result)
  }

  // DELETE /api/posts/:id - Delete post
  const deleteMatch = path.match(/^\/api\/posts\/([^/]+)$/)
  if (deleteMatch && method === 'DELETE') {
    await db.delete('posts', deleteMatch[1]!)
    return Response.json({ success: true })
  }

  return Response.json({ error: 'Not found' }, { status: 404 })
}
```

---

## Durable Object Configuration

### Why Durable Objects?

ParqueDB uses Durable Objects for write operations because:

1. **Strong Consistency**: Single-writer pattern ensures no conflicts
2. **SQLite Storage**: Embedded SQLite for fast metadata operations
3. **Event Sourcing**: WAL batching reduces R2 write costs
4. **Namespace Isolation**: Each namespace gets its own DO instance

### DO Routing Strategy

ParqueDB routes writes to DOs by namespace:

```typescript
// Internal routing logic
const doId = env.PARQUEDB.idFromName(namespace)  // e.g., 'posts'
const stub = env.PARQUEDB.get(doId)
const result = await stub.create(namespace, data, options)
```

Benefits:
- Parallel writes to different namespaces
- Sequential writes within a namespace
- Predictable scaling

### Migration Configuration

The `migrations` array is required for SQLite-backed DOs:

```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  }
]
```

When adding new DO classes:

```jsonc
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["ParqueDBDO"] },
  { "tag": "v2", "new_sqlite_classes": ["RateLimitDO", "DatabaseIndexDO"] }
]
```

### DO Tables (Internal)

ParqueDBDO creates these SQLite tables:

| Table | Purpose |
|-------|---------|
| `entities` | Entity metadata (soft deletes, versions) |
| `relationships` | Graph edges with shredded fields |
| `events_wal` | Batched entity events |
| `rels_wal` | Batched relationship events |
| `event_batches` | Legacy event batches |
| `checkpoints` | Flush checkpoints |
| `pending_row_groups` | Bulk write tracking |

### Exporting the DO Class

**Critical**: You must export `ParqueDBDO` from your entry point:

```typescript
// This is REQUIRED for Cloudflare to instantiate the DO
export { ParqueDBDO } from 'parquedb/worker'
```

Without this export, you'll get "Durable Object class not found" errors.

---

## R2 Integration

### Bucket Setup

```bash
# Create buckets for each environment
npx wrangler r2 bucket create my-parquedb-data-prod
npx wrangler r2 bucket create my-parquedb-data-staging
npx wrangler r2 bucket create my-parquedb-data-preview

# Verify
npx wrangler r2 bucket list
```

### R2 Configuration

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",
    "bucket_name": "my-parquedb-data",
    "preview_bucket_name": "my-parquedb-data-preview"
  }
]
```

### Data Layout

ParqueDB stores data in R2 with this structure:

```
my-parquedb-data/
├── data/
│   └── {namespace}/
│       ├── data.parquet          # Main entity data
│       └── pending/              # Bulk writes pending merge
│           └── {ulid}.parquet
├── rels/
│   ├── forward/{namespace}.parquet    # Outgoing edges
│   └── reverse/{namespace}.parquet    # Incoming edges
├── indexes/
│   └── bloom/{namespace}.bloom        # Bloom filters
└── events/
    └── archive/{year}/{month}/{day}/  # Event archives
        └── {checkpoint}.parquet
```

### Environment-Specific Buckets

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",
    "bucket_name": "my-parquedb-data-dev"
  }
],

"env": {
  "production": {
    "r2_buckets": [
      {
        "binding": "BUCKET",
        "bucket_name": "my-parquedb-data-prod"
      }
    ]
  }
}
```

### CDN Bucket for Public Data

For public datasets with edge caching:

```jsonc
"r2_buckets": [
  { "binding": "BUCKET", "bucket_name": "my-parquedb-data" },
  { "binding": "CDN_BUCKET", "bucket_name": "my-parquedb-cdn" }
],
"vars": {
  "CDN_R2_DEV_URL": "https://pub-xxxxx.r2.dev"
}
```

---

## Authentication

### Bearer Token Authentication

ParqueDB supports Bearer token authentication for private databases:

```typescript
// Extract token from Authorization header
const token = request.headers.get('Authorization')?.replace('Bearer ', '')

if (!token) {
  return Response.json({ error: 'Unauthorized' }, { status: 401 })
}

// Validate token (implement your own validation)
const user = await validateToken(token)
```

### JWT Token Validation

ParqueDB's public routes extract JWT claims:

```typescript
// Supported JWT claims for ownership check:
// - sub (subject) - standard user ID
// - username - common username field
// - preferred_username - OIDC standard
```

### API Key Authentication

Simple API key authentication:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const apiKey = request.headers.get('X-API-Key')

    if (apiKey !== env.API_KEY) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 })
    }

    // Process authenticated request
    const db = new ParqueDBWorker(ctx, env)
    // ...
  }
}
```

### Visibility Levels

ParqueDB supports database visibility levels:

| Visibility | Anonymous Read | Listed Publicly |
|------------|----------------|-----------------|
| `public` | Yes | Yes |
| `unlisted` | Yes | No |
| `private` | No | No |

```typescript
import { allowsAnonymousRead } from 'parquedb'

if (!allowsAnonymousRead(database.visibility) && !isOwner) {
  return Response.json({ error: 'Authentication required' }, { status: 401 })
}
```

### Secrets Management

Store sensitive values as secrets, not environment variables:

```bash
# Set secret
npx wrangler secret put AUTH_SECRET

# Set for specific environment
npx wrangler secret put AUTH_SECRET --env production

# List secrets
npx wrangler secret list
```

---

## Rate Limiting

### Built-in Rate Limiter

ParqueDB includes a Durable Object-based rate limiter:

```typescript
import { RateLimitDO, getRateLimiter, buildRateLimitResponse } from 'parquedb/worker'

export { RateLimitDO }

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const result = await getRateLimiter(env, request).checkLimit('query')

    if (!result.allowed) {
      return buildRateLimitResponse(result)
    }

    // Process request
  }
}
```

### Rate Limit Configuration

```jsonc
"durable_objects": {
  "bindings": [
    { "name": "PARQUEDB", "class_name": "ParqueDBDO" },
    { "name": "RATE_LIMITER", "class_name": "RateLimitDO" }
  ]
}
```

### Endpoint Categories

| Category | Use Case | Default Limit |
|----------|----------|---------------|
| `public` | List public databases | 100/min |
| `database` | Database metadata | 200/min |
| `query` | Collection queries | 500/min |
| `file` | Raw file access | 1000/min |

### Response Headers

Rate limited responses include standard headers:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1706789400
Retry-After: 30
```

---

## Cache Invalidation

### CQRS Cache Coherence

ParqueDB uses version-based cache invalidation:

1. DO tracks invalidation version per namespace
2. Workers compare local version with DO version
3. Stale caches are invalidated on version mismatch

```typescript
// Check if cache is stale
const { wasInvalidated, version } = await db.validateAndInvalidateCache('posts')

if (wasInvalidated) {
  console.log('Cache invalidated, new version:', version)
}
```

### Automatic Invalidation

Write operations automatically invalidate caches:

```typescript
// Create triggers cache invalidation
await db.create('posts', { ... })
// Cache for 'posts' namespace is invalidated

// Update triggers cache invalidation
await db.update('posts', 'id123', { $set: { ... } })
// Cache for 'posts' namespace is invalidated
```

### Cache Configuration

```jsonc
"vars": {
  "CACHE_DATA_TTL": "60",           // 1 minute for data
  "CACHE_METADATA_TTL": "300",       // 5 minutes for metadata
  "CACHE_BLOOM_TTL": "600",          // 10 minutes for bloom filters
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

### Cache Presets

```typescript
import {
  DEFAULT_CACHE_CONFIG,       // Balanced (60s data)
  READ_HEAVY_CACHE_CONFIG,    // Analytics (300s data)
  WRITE_HEAVY_CACHE_CONFIG,   // Realtime (15s data)
  NO_CACHE_CONFIG             // Development
} from 'parquedb/worker'
```

---

## Deployment

### Local Development

```bash
# Start dev server
npx wrangler dev

# With specific environment
npx wrangler dev --env staging
```

### Deploy to Production

```bash
# Deploy to default environment
npx wrangler deploy

# Deploy to specific environment
npx wrangler deploy --env production

# Deploy with dry run
npx wrangler deploy --dry-run
```

### CI/CD with GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Cloudflare Workers

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - name: Install dependencies
        run: npm ci

      - name: Type check
        run: npm run typecheck

      - name: Deploy to Cloudflare Workers
        if: github.ref == 'refs/heads/main'
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          environment: production
```

### Create API Token

1. Go to Cloudflare Dashboard > API Tokens
2. Create token with "Edit Cloudflare Workers" template
3. Add to GitHub: Settings > Secrets > Actions > `CLOUDFLARE_API_TOKEN`

### Rollback

```bash
# List deployments
npx wrangler deployments list

# Rollback to previous version
npx wrangler rollback
```

---

## Monitoring

### Debug Endpoints

Built-in Worker provides debug endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /debug/r2` | R2 bucket contents |
| `GET /debug/entity?ns=posts&id=abc` | Entity details |
| `GET /debug/indexes` | Index information |
| `GET /debug/cache` | Cache statistics |

### Cache Statistics

```typescript
const stats = await db.getCacheStats()
// {
//   hits: 1234,
//   misses: 56,
//   hitRatio: 0.96,
//   cachedBytes: 5242880,
//   fetchedBytes: 524288
// }
```

### Storage Statistics

```typescript
const storageStats = db.getStorageStats()
// {
//   cdnHits: 100,
//   primaryHits: 50,
//   edgeHits: 200,
//   cacheHits: 150,
//   totalReads: 500,
//   usingCdn: true,
//   usingEdge: true
// }
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

# JSON output
npx wrangler tail --format json
```

### Structured Logging

```typescript
import { logger } from 'parquedb/utils'

logger.info('Query executed', {
  namespace: 'posts',
  duration: 45,
  rowsReturned: 20,
})
```

---

## Troubleshooting

### Common Issues

#### "Durable Object class not found"

**Cause**: DO class not exported from entry point.

**Fix**: Export the DO class:
```typescript
export { ParqueDBDO } from 'parquedb/worker'
```

#### "R2 bucket not found"

**Cause**: Bucket doesn't exist or binding mismatch.

**Fix**:
```bash
# Check bucket exists
npx wrangler r2 bucket list

# Create if missing
npx wrangler r2 bucket create my-parquedb-data
```

#### "nodejs_compat required"

**Cause**: Missing compatibility flag.

**Fix**: Add to wrangler.jsonc:
```jsonc
"compatibility_flags": ["nodejs_compat"]
```

#### Slow First Request (Cold Start)

**Cause**: Worker cold start + R2 fetch + cache miss.

**Fix**:
1. Enable caching with appropriate TTL
2. Use CDN bucket for public data
3. Pre-warm cache with cron trigger:

```jsonc
"triggers": {
  "crons": ["*/5 * * * *"]
}
```

```typescript
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const db = new ParqueDBWorker(ctx, env)
    await db.find('posts', {}, { limit: 1 })  // Pre-warm
  }
}
```

#### Memory Limit Exceeded

**Cause**: Loading too much data (128MB limit).

**Fix**:
1. Use pagination: `{ limit: 100 }`
2. Use projection: `{ project: { title: 1 } }`

#### CPU Time Exceeded

**Cause**: Complex query exceeding 50ms CPU limit.

**Fix**:
1. Add indexes to frequently queried fields
2. Use limit to reduce result size
3. Upgrade to Workers Unbound:

```jsonc
"limits": {
  "cpu_ms": 100
}
```

### Getting Help

1. Check debug endpoints for diagnostics
2. Use `npx wrangler tail` for live logs
3. Review Cloudflare dashboard for metrics
4. Open an issue on GitHub with reproduction steps

---

## Next Steps

- [Cloudflare Workers Deployment](../deployment/cloudflare-workers.md) - Full deployment guide
- [R2 Setup](../deployment/r2-setup.md) - Detailed R2 configuration
- [Configuration Reference](../deployment/configuration.md) - All configuration options
- [Query API](../queries.md) - MongoDB-style filtering
- [Architecture](../architecture.md) - System internals
