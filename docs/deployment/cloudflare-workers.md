---
title: Cloudflare Workers
description: Deploy ParqueDB to Cloudflare Workers with R2 storage and Durable Objects for a production-ready edge database.
---

Deploy ParqueDB to Cloudflare Workers for a globally distributed edge database with R2 storage and Durable Objects for strong consistency.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [ParqueDB Configuration](#parquedb-configuration)
- [Deployment Steps](#deployment-steps)
- [Advanced Topics](#advanced-topics)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

ParqueDB uses a CQRS (Command Query Responsibility Segregation) architecture on Cloudflare Workers for optimal performance and consistency:

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                   Cloudflare Network                     │
                    │                                                          │
    Request ───────▶│  ┌─────────────────┐      ┌──────────────────────┐      │
                    │  │   ParqueDB      │      │   Durable Object     │      │
                    │  │   Worker        │◀────▶│   (ParqueDBDO)       │      │
                    │  │                 │      │   - SQLite metadata  │      │
                    │  │  - Read: R2     │      │   - Write operations │      │
                    │  │  - Write: DO    │      │   - Event log        │      │
                    │  └────────┬────────┘      └──────────────────────┘      │
                    │           │                                              │
                    │           ▼                                              │
                    │  ┌─────────────────┐      ┌──────────────────────┐      │
                    │  │   Cache API     │      │      R2 Bucket       │      │
                    │  │   (500MB-5GB)   │◀────▶│  - Parquet files     │      │
                    │  │                 │      │  - Indexes           │      │
                    │  └─────────────────┘      │  - Relationships     │      │
                    │                           └──────────────────────┘      │
                    └─────────────────────────────────────────────────────────┘
```

**Key Components:**

- **ParqueDB Worker**: Handles HTTP requests, routes reads to R2, writes to Durable Objects
- **Durable Object (ParqueDBDO)**: Single-writer per namespace for consistency, uses SQLite for metadata and event log
- **R2 Bucket**: Stores Parquet data files, secondary indexes, and relationship graphs
- **Cache API**: Caches frequently accessed data at the edge (500MB free tier, up to 5GB on paid plans)

**Benefits:**

- **Low Latency**: Read from cache or R2 in <50ms globally
- **Strong Consistency**: Durable Objects ensure ACID writes
- **Zero Egress Costs**: R2 to Worker data transfer is free
- **Scalability**: Automatically scales to handle any traffic
- **Cost Effective**: Pay per request, no idle costs

---

## Prerequisites

Before deploying ParqueDB to Cloudflare Workers, ensure you have:

### 1. Cloudflare Account

Sign up for a free Cloudflare account:
- Go to [Cloudflare Dashboard](https://dash.cloudflare.com)
- Create account (free tier includes Workers and R2)
- Add payment method (required for Workers Paid plan features)

**Recommended Plan**: Workers Paid ($5/month)
- 10 million requests/month included
- No daily request limits
- Durable Objects and R2 included

### 2. Wrangler CLI

Install the Wrangler CLI for deployment:

```bash
npm install -g wrangler
# or with pnpm
pnpm add -g wrangler
# or with yarn
yarn global add wrangler
```

Verify installation:

```bash
wrangler --version
# Should output: ⛅️ wrangler 4.x.x
```

### 3. Authentication

Authenticate Wrangler with your Cloudflare account:

```bash
wrangler login
```

This opens a browser to authorize Wrangler. Once authenticated, you can deploy workers and manage R2 buckets.

Verify authentication:

```bash
wrangler whoami
```

### 4. Node.js and Package Manager

ParqueDB requires Node.js 18+ and a package manager:

```bash
node --version  # Should be v18.0.0 or higher
npm --version   # or pnpm/yarn
```

---

## Project Setup

### 1. Initialize Worker Project

Create a new Cloudflare Worker project:

```bash
# Using Wrangler (recommended)
npx wrangler init my-parquedb-api
cd my-parquedb-api

# Or clone the ParqueDB starter template
git clone https://github.com/parquedb/worker-starter my-parquedb-api
cd my-parquedb-api
```

### 2. Install Dependencies

Install ParqueDB and required dependencies:

```bash
npm install parquedb
# or
pnpm add parquedb
```

ParqueDB automatically includes:
- `hyparquet` - Pure JS Parquet reader
- `hyparquet-writer` - Pure JS Parquet writer

### 3. Project Structure

Your project should have this structure:

```
my-parquedb-api/
├── src/
│   └── index.ts          # Worker entrypoint
├── wrangler.jsonc        # Cloudflare configuration
├── package.json
├── tsconfig.json
└── README.md
```

### 4. TypeScript Configuration

Create or update `tsconfig.json`:

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

Install types:

```bash
npm install -D @cloudflare/workers-types
```

---

## wrangler.jsonc Configuration

Create `wrangler.jsonc` in your project root with the following configuration:

### Basic Configuration

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
  // Durable Objects - Required for write operations
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
  // R2 Storage - Required for Parquet file storage
  // =============================================================================

  "r2_buckets": [
    {
      "binding": "BUCKET",
      "bucket_name": "my-parquedb-data",
      "preview_bucket_name": "my-parquedb-data-preview"
    }
  ],

  // =============================================================================
  // Environment Variables
  // =============================================================================

  "vars": {
    "ENVIRONMENT": "development",
    "LOG_LEVEL": "info"
  },

  // =============================================================================
  // Development Settings
  // =============================================================================

  "dev": {
    "port": 8787,
    "local_protocol": "http",
    "ip": "0.0.0.0"
  }
}
```

### Configuration Options Explained

| Option | Required | Description |
|--------|----------|-------------|
| `name` | Yes | Worker name (appears in dashboard and URLs) |
| `main` | Yes | Entry point file path |
| `compatibility_date` | Yes | Cloudflare runtime version (YYYY-MM-DD) |
| `compatibility_flags` | Yes | `["nodejs_compat"]` enables Node.js APIs (crypto, buffer, etc.) |
| `durable_objects` | Yes | Durable Object class bindings |
| `migrations` | Yes | DO schema migrations (required for SQLite-backed DOs) |
| `r2_buckets` | Yes | R2 bucket bindings for storage |
| `vars` | No | Environment variables |
| `dev` | No | Development server settings |

### Environment-Specific Configuration

Add environment-specific overrides:

```jsonc
{
  // ... base configuration above ...

  "env": {
    "production": {
      "vars": {
        "ENVIRONMENT": "production",
        "LOG_LEVEL": "warn",
        "CACHE_DATA_TTL": "300",
        "CACHE_METADATA_TTL": "900",
        "CACHE_BLOOM_TTL": "1800"
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
        "ENVIRONMENT": "staging",
        "LOG_LEVEL": "info"
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

---

## R2 Bucket Setup

### Create R2 Buckets

Use Wrangler to create R2 buckets for each environment:

```bash
# Production bucket
npx wrangler r2 bucket create my-parquedb-data-prod

# Staging bucket
npx wrangler r2 bucket create my-parquedb-data-staging

# Preview bucket (for local development)
npx wrangler r2 bucket create my-parquedb-data-preview
```

### Verify Bucket Creation

```bash
npx wrangler r2 bucket list
```

You should see your buckets listed.

### R2 Bucket Configuration

Configure R2 bucket bindings in `wrangler.jsonc`:

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",              // Environment variable name
    "bucket_name": "my-parquedb-data", // Production bucket
    "preview_bucket_name": "my-parquedb-data-preview" // Development bucket
  },
  {
    "binding": "CDN_BUCKET",          // Optional: separate CDN bucket
    "bucket_name": "my-parquedb-cdn"  // For public data with custom domain
  }
]
```

### Data Layout in R2

ParqueDB stores data in a hierarchical structure:

```
my-parquedb-data/
├── data/
│   ├── posts/
│   │   └── data.parquet       # Entity data
│   ├── users/
│   │   └── data.parquet
│   └── comments/
│       └── data.parquet
├── rels/
│   ├── forward/
│   │   ├── posts.parquet      # Outgoing relationships (post->author)
│   │   └── users.parquet      # Outgoing relationships (user->posts)
│   └── reverse/
│       ├── posts.parquet      # Incoming relationships (post<-comment)
│       └── users.parquet      # Incoming relationships (user<-post)
├── indexes/
│   ├── bloom/
│   │   ├── posts.bloom        # Bloom filters for fast ID lookups
│   │   └── users.bloom
│   └── secondary/
│       ├── posts_status.idx   # Secondary indexes for queries
│       └── users_email.idx
└── events/
    └── current.parquet        # Event log for time-travel queries
```

For detailed R2 setup including public access and custom domains, see [R2 Setup Guide](./r2-setup.md).

---

## ParqueDB Configuration

### Worker Entry Point

Create `src/index.ts` with ParqueDB integration:

```typescript
import { ParqueDBWorker, ParqueDBDO } from 'parquedb/worker'
import type { Env } from './types'

// Export the Durable Object class for Cloudflare runtime
export { ParqueDBDO }

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const worker = new ParqueDBWorker(ctx, env)
    const url = new URL(request.url)

    try {
      // Handle CORS preflight
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
          },
        })
      }

      // Health check
      if (url.pathname === '/health') {
        return Response.json({
          status: 'healthy',
          timestamp: new Date().toISOString(),
          environment: env.ENVIRONMENT,
        })
      }

      // GET /posts - List posts
      if (url.pathname === '/posts' && request.method === 'GET') {
        const posts = await worker.find('posts', {
          status: 'published',
        }, {
          limit: 20,
          sort: { createdAt: -1 },
          project: { title: 1, excerpt: 1, author: 1, createdAt: 1 },
        })
        return Response.json(posts)
      }

      // GET /posts/:id - Get single post
      const postMatch = url.pathname.match(/^\/posts\/([^/]+)$/)
      if (postMatch && request.method === 'GET') {
        const post = await worker.get('posts', postMatch[1])
        if (!post) {
          return Response.json({ error: 'Post not found' }, { status: 404 })
        }
        return Response.json(post)
      }

      // POST /posts - Create post
      if (url.pathname === '/posts' && request.method === 'POST') {
        const data = await request.json() as any
        const post = await worker.create('posts', {
          ...data,
          status: 'draft',
          createdAt: new Date(),
        })
        return Response.json(post, { status: 201 })
      }

      // PATCH /posts/:id - Update post
      const patchMatch = url.pathname.match(/^\/posts\/([^/]+)$/)
      if (patchMatch && request.method === 'PATCH') {
        const id = patchMatch[1]
        const data = await request.json() as any
        await worker.update('posts', id, { $set: data })
        const updated = await worker.get('posts', id)
        return Response.json(updated)
      }

      // DELETE /posts/:id - Delete post
      const deleteMatch = url.pathname.match(/^\/posts\/([^/]+)$/)
      if (deleteMatch && request.method === 'DELETE') {
        await worker.delete('posts', deleteMatch[1])
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

### Type Definitions

Create `src/types.ts` for TypeScript types:

```typescript
export interface Env {
  // Durable Object binding
  PARQUEDB: DurableObjectNamespace

  // R2 bucket bindings
  BUCKET: R2Bucket
  CDN_BUCKET?: R2Bucket

  // Environment variables
  ENVIRONMENT: string
  LOG_LEVEL?: string
  CACHE_DATA_TTL?: string
  CACHE_METADATA_TTL?: string
  CACHE_BLOOM_TTL?: string
  CDN_R2_DEV_URL?: string
}
```

### R2Backend Setup

The `ParqueDBWorker` automatically configures R2Backend using the `BUCKET` binding. For custom configuration:

```typescript
import { R2Backend, ReadPath, QueryExecutor } from 'parquedb/worker'

// Custom ReadPath with caching
const cache = await caches.open('parquedb')
const readPath = new ReadPath(env.BUCKET, cache, {
  dataTtl: 60,
  metadataTtl: 300,
  bloomTtl: 600,
  staleWhileRevalidate: true,
})

// Custom QueryExecutor
const queryExecutor = new QueryExecutor(
  readPath,
  env.BUCKET,
  env.CDN_BUCKET,
  env.CDN_R2_DEV_URL
)
```

### ParqueDBDO Durable Object

The `ParqueDBDO` Durable Object is automatically exported from `parquedb/worker`. It handles:

- **Write Operations**: CREATE, UPDATE, DELETE with ACID guarantees
- **SQLite Metadata**: Fast lookups and relationship traversal
- **Event Log**: Change data capture for time-travel queries
- **Flush to R2**: Periodic batching of changes to Parquet files

No additional configuration needed - it works out of the box.

### Storage Router Configuration

ParqueDB uses a storage router to coordinate reads (R2) and writes (DO):

```typescript
// Reads go directly to R2 via QueryExecutor
const posts = await worker.find('posts', { status: 'published' })

// Writes go through Durable Object for consistency
const post = await worker.create('posts', { title: 'Hello World' })

// Cache invalidation happens automatically after writes
```

This CQRS pattern provides:
- **High read throughput**: Cached R2 reads in parallel
- **Strong write consistency**: Single-writer Durable Object
- **Eventual consistency**: Cache invalidates after writes

---

## Environment Variables

### Setting Secrets

For sensitive values like API keys, use Wrangler secrets:

```bash
# Set a secret
npx wrangler secret put AUTH_SECRET

# Prompt will appear to enter the value
# Value is encrypted and not visible in wrangler.jsonc

# Set secret for specific environment
npx wrangler secret put AUTH_SECRET --env production

# List secrets
npx wrangler secret list

# Delete a secret
npx wrangler secret delete AUTH_SECRET
```

Access secrets in your worker:

```typescript
export default {
  async fetch(request: Request, env: Env) {
    const authHeader = request.headers.get('Authorization')
    if (authHeader !== `Bearer ${env.AUTH_SECRET}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 })
    }
    // ... handle request
  }
}
```

### Cache Configuration

Configure caching behavior via environment variables:

```jsonc
"vars": {
  "CACHE_DATA_TTL": "60",              // 1 minute for data files
  "CACHE_METADATA_TTL": "300",         // 5 minutes for metadata
  "CACHE_BLOOM_TTL": "600",            // 10 minutes for bloom filters
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

**Cache TTL Guidelines:**

| Workload | Data TTL | Metadata TTL | Bloom TTL | Use Case |
|----------|----------|--------------|-----------|----------|
| Read-heavy | 300s (5min) | 900s (15min) | 1800s (30min) | Analytics, dashboards |
| Balanced | 60s (1min) | 300s (5min) | 600s (10min) | General APIs |
| Write-heavy | 15s | 60s (1min) | 120s (2min) | Real-time updates |

### Custom Domain Variables

If using a custom domain for R2 CDN:

```jsonc
"vars": {
  "CDN_R2_DEV_URL": "https://cdn.yourdomain.com/parquedb"
}
```

This enables edge caching of public datasets via your custom domain.

---

## Deployment Steps

### Local Development

Test your worker locally before deploying:

```bash
# Start local development server
npx wrangler dev

# Worker available at: http://localhost:8787
```

Test endpoints:

```bash
# Health check
curl http://localhost:8787/health

# List posts
curl http://localhost:8787/posts

# Create a post
curl -X POST http://localhost:8787/posts \
  -H "Content-Type: application/json" \
  -d '{"title":"Hello World","content":"My first post"}'
```

**Note**: Local development uses preview buckets and a local Durable Object instance.

### Deploy to Staging

Deploy to staging environment for testing:

```bash
# Deploy with staging environment
npx wrangler deploy --env staging

# Your worker will be available at:
# https://my-parquedb-api-staging.youraccount.workers.dev
```

Test staging deployment:

```bash
curl https://my-parquedb-api-staging.youraccount.workers.dev/health
```

### Deploy to Production

Deploy to production after testing:

```bash
# Deploy with production environment
npx wrangler deploy --env production

# Your worker will be available at:
# https://my-parquedb-api.youraccount.workers.dev
```

### Deploy with Dry Run

Preview deployment changes without applying:

```bash
# Dry run shows what would change
npx wrangler deploy --dry-run

# Shows:
# - Environment variables
# - Bindings
# - Routes
# - Code changes
```

### Verify Deployment

Check deployment status:

```bash
# List recent deployments
npx wrangler deployments list

# Shows:
# ┌────────────────────────────────────────┬────────────┬─────────────────────┐
# │ Created                                │ Author     │ Deployment ID       │
# ├────────────────────────────────────────┼────────────┼─────────────────────┤
# │ 2026-01-30T12:00:00Z                   │ you        │ abc123...           │
# └────────────────────────────────────────┴────────────┴─────────────────────┘

# Tail live logs
npx wrangler tail

# Filter by status
npx wrangler tail --status error

# Filter by search string
npx wrangler tail --search "ParqueDB"
```

### Rollback Deployment

If issues occur, rollback to previous version:

```bash
# List deployments
npx wrangler deployments list

# Rollback to previous deployment
npx wrangler rollback

# Or rollback to specific deployment ID
npx wrangler rollback --deployment-id abc123...
```

### CI/CD Integration

Automate deployments with GitHub Actions:

```yaml
# .github/workflows/deploy.yml
name: Deploy to Cloudflare Workers

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    name: Deploy
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm install

      - name: Deploy to Cloudflare Workers
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          environment: production
```

Set `CLOUDFLARE_API_TOKEN` in GitHub repository secrets:
1. Go to Cloudflare Dashboard > API Tokens
2. Create token with "Edit Cloudflare Workers" permissions
3. Add to GitHub: Settings > Secrets > Actions > New repository secret

---

## Advanced Topics

### Custom Domains

Use your own domain for the Worker API:

#### 1. Add Domain to Cloudflare

1. Go to Cloudflare Dashboard
2. Click "Add a site"
3. Enter your domain (e.g., `example.com`)
4. Follow DNS setup instructions

#### 2. Configure Route in wrangler.jsonc

```jsonc
"routes": [
  {
    "pattern": "api.example.com/*",
    "zone_name": "example.com"
  }
]
```

#### 3. Deploy

```bash
npx wrangler deploy
```

Your worker is now available at `https://api.example.com/*`

#### 4. Custom Domain for R2 (CDN)

Enable public access to R2 via custom domain:

1. Go to R2 > Your Bucket > Settings
2. Click "Add Custom Domain"
3. Enter subdomain: `cdn.example.com`
4. Cloudflare automatically configures DNS

Update worker config:

```jsonc
"vars": {
  "CDN_R2_DEV_URL": "https://cdn.example.com/parquedb"
}
```

### Caching Strategies

ParqueDB provides built-in caching strategies:

```typescript
import {
  DEFAULT_CACHE_CONFIG,
  READ_HEAVY_CACHE_CONFIG,
  WRITE_HEAVY_CACHE_CONFIG,
  NO_CACHE_CONFIG,
} from 'parquedb/worker'

// Read-heavy workloads (analytics)
const worker = new ParqueDBWorker(ctx, env, {
  cacheConfig: READ_HEAVY_CACHE_CONFIG,
})

// Write-heavy workloads (real-time)
const worker = new ParqueDBWorker(ctx, env, {
  cacheConfig: WRITE_HEAVY_CACHE_CONFIG,
})

// No caching (debugging)
const worker = new ParqueDBWorker(ctx, env, {
  cacheConfig: NO_CACHE_CONFIG,
})
```

**Cache Presets:**

| Preset | Data TTL | Metadata TTL | Bloom TTL | Best For |
|--------|----------|--------------|-----------|----------|
| `DEFAULT_CACHE_CONFIG` | 60s | 300s | 600s | General APIs |
| `READ_HEAVY_CACHE_CONFIG` | 300s | 900s | 1800s | Analytics, rarely changing data |
| `WRITE_HEAVY_CACHE_CONFIG` | 15s | 60s | 120s | Frequently updated data |
| `NO_CACHE_CONFIG` | 0 | 0 | 0 | Development, debugging |

### Cost Optimization Tips

#### 1. Enable Aggressive Caching

For read-heavy workloads, increase cache TTL:

```jsonc
"vars": {
  "CACHE_DATA_TTL": "3600",      // 1 hour
  "CACHE_METADATA_TTL": "7200",  // 2 hours
  "CACHE_BLOOM_TTL": "14400"     // 4 hours
}
```

This reduces R2 reads by 60x (from 1 read/minute to 1 read/hour).

#### 2. Use CDN Bucket for Public Data

Move public datasets to a CDN bucket with custom domain:

```jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",
    "bucket_name": "private-data"
  },
  {
    "binding": "CDN_BUCKET",
    "bucket_name": "public-data"  // Enable public access + custom domain
  }
]
```

Cloudflare edge network caches public R2 data automatically.

#### 3. Optimize Parquet Row Groups

Larger row groups = fewer files = lower R2 costs:

```typescript
// When writing Parquet files
const rowGroupSize = 10000  // 10K rows per group (default: 1000)
```

#### 4. Use Projection to Reduce Data Transfer

Only fetch needed fields:

```typescript
// Bad: Fetches all fields
const posts = await worker.find('posts', {})

// Good: Fetches only needed fields (50% less data)
const posts = await worker.find('posts', {}, {
  project: { title: 1, author: 1, createdAt: 1 }
})
```

#### 5. Batch Writes

Use `createMany` for bulk inserts:

```typescript
// Bad: 100 separate writes (100 DO operations)
for (const item of items) {
  await worker.create('posts', item)
}

// Good: 1 batch write (1 DO operation)
await worker.createMany('posts', items)
```

**Cost Estimate for 1M requests/day:**

| Component | Free Tier | Cost Beyond Free |
|-----------|-----------|------------------|
| Workers Requests | 100K/day | $0.50/million |
| Workers CPU | First 400K CPU-ms | $0.02/million CPU-ms |
| R2 Storage (100GB) | 10GB | $1.35/month |
| R2 Class A (writes) | 1M/month | $4.50/million |
| R2 Class B (reads) | 10M/month | $0.36/million |
| Durable Objects | 400K requests/day | $0.15/million requests |
| **Total** | **$0** (within free tier) | **~$25/month** for 1M req/day |

### Monitoring and Logging

#### Built-in Analytics

Cloudflare Dashboard provides:
- Request count and success rate
- CPU time usage
- Cache hit ratio
- Error logs

Access at: Workers & Pages > Your Worker > Analytics

#### Custom Logging

Add structured logging:

```typescript
import { logger } from 'parquedb/utils'

// Log with context
logger.info('Query executed', {
  namespace: 'posts',
  filter: { status: 'published' },
  duration: 45,
  rowsScanned: 1000,
  rowsReturned: 20,
})

// Error logging
logger.error('Query failed', {
  namespace: 'posts',
  error: error.message,
  stack: error.stack,
})
```

#### Wrangler Tail

Stream logs in real-time:

```bash
# All logs
npx wrangler tail

# Filter by status
npx wrangler tail --status error

# Filter by search
npx wrangler tail --search "posts"

# JSON output
npx wrangler tail --format json
```

#### Integration with External Services

Send logs to external services:

```typescript
// Sentry integration
import * as Sentry from '@sentry/browser'

Sentry.init({
  dsn: env.SENTRY_DSN,
  environment: env.ENVIRONMENT,
})

try {
  await worker.find('posts', filter)
} catch (error) {
  Sentry.captureException(error)
  throw error
}
```

### Performance Tuning

#### 1. Index Hot Fields

Create secondary indexes for frequently queried fields:

```typescript
// In Durable Object or setup script
import { HashIndex } from 'parquedb/indexes'

const statusIndex = new HashIndex(storage, 'posts', 'status')
await statusIndex.build()
```

Queries on indexed fields are 10-100x faster.

#### 2. Use Bloom Filters

Enable bloom filters for fast negative lookups:

```typescript
// Bloom filters are built automatically during flush
// Check if ID exists before fetching
const exists = await worker.exists('posts', { $id: 'posts/123' })
```

Bloom filters reduce unnecessary R2 reads by 90% for missing IDs.

#### 3. Optimize Queries

```typescript
// Bad: Full table scan
const posts = await worker.find('posts', {})

// Good: Use index
const posts = await worker.find('posts', { status: 'published' })

// Best: Use index + limit + projection
const posts = await worker.find('posts', { status: 'published' }, {
  limit: 20,
  project: { title: 1, author: 1 },
  hint: { index: 'status_hash' }
})
```

#### 4. Pagination Strategies

Use cursor-based pagination for large result sets:

```typescript
// First page
const page1 = await worker.find('posts', {}, { limit: 20 })

// Next page using cursor
const page2 = await worker.find('posts', {}, {
  limit: 20,
  cursor: page1.nextCursor
})
```

Cursor pagination is 10x faster than offset pagination for large datasets.

---

## Troubleshooting

### Common Issues

#### "Durable Object not found"

**Cause**: DO class not exported or migration not applied.

**Solution**:

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

3. Deploy with migrations:
   ```bash
   npx wrangler deploy
   ```

#### "R2 bucket not found"

**Cause**: Bucket doesn't exist or binding name mismatch.

**Solution**:

1. Verify bucket exists:
   ```bash
   npx wrangler r2 bucket list
   ```

2. Create if missing:
   ```bash
   npx wrangler r2 bucket create my-parquedb-data
   ```

3. Check binding name matches:
   ```jsonc
   "r2_buckets": [
     {
       "binding": "BUCKET",  // Must match code: env.BUCKET
       "bucket_name": "my-parquedb-data"
     }
   ]
   ```

#### "nodejs_compat required"

**Cause**: Missing compatibility flag for Node.js APIs.

**Solution**: Add to `wrangler.jsonc`:
```jsonc
"compatibility_flags": ["nodejs_compat"]
```

#### Slow First Request (Cold Start)

**Cause**: Worker cold start + R2 file fetch + cache miss.

**Solutions**:

1. **Enable caching** with longer TTL:
   ```typescript
   const worker = new ParqueDBWorker(ctx, env, {
     cacheConfig: READ_HEAVY_CACHE_CONFIG
   })
   ```

2. **Use CDN bucket** for public datasets:
   ```jsonc
   "vars": {
     "CDN_R2_DEV_URL": "https://cdn.example.com/parquedb"
   }
   ```

3. **Pre-warm cache** with a cron trigger:
   ```jsonc
   "triggers": {
     "crons": ["*/5 * * * *"]  // Every 5 minutes
   }
   ```
   ```typescript
   export default {
     async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
       // Pre-warm cache
       const worker = new ParqueDBWorker(ctx, env)
       await worker.find('posts', { status: 'published' }, { limit: 1 })
     }
   }
   ```

#### "Memory limit exceeded"

**Cause**: Loading too much data into Worker memory (128MB limit).

**Solutions**:

1. **Use pagination**:
   ```typescript
   const posts = await worker.find('posts', {}, { limit: 100 })
   ```

2. **Use projection** to reduce memory:
   ```typescript
   const posts = await worker.find('posts', {}, {
     project: { title: 1, status: 1 }  // Only fetch 2 fields
   })
   ```

3. **Stream large exports** (requires custom implementation):
   ```typescript
   // Not recommended for Workers - use Durable Objects for large exports
   ```

#### "CPU time limit exceeded"

**Cause**: Query taking too long (default: 50ms CPU limit on free tier).

**Solutions**:

1. **Optimize query** with indexes:
   ```typescript
   // Add index hint
   await worker.find('posts', { status: 'published' }, {
     hint: { index: 'status_hash' }
   })
   ```

2. **Increase CPU limit** (paid plan):
   ```jsonc
   "limits": {
     "cpu_ms": 100  // Max 100ms CPU time
   }
   ```

3. **Use Workers Unbound** for long-running queries (paid plan):
   ```jsonc
   "usage_model": "unbound"
   ```

### Debug Mode

Enable debug logging for troubleshooting:

```jsonc
"vars": {
  "LOG_LEVEL": "debug"
}
```

This logs:
- Query plans and execution time
- Cache hits/misses
- R2 read/write operations
- DO RPC calls

### Getting Help

1. **Check ParqueDB Documentation**: [parquedb.dev/docs](https://parquedb.dev/docs)
2. **GitHub Issues**: [github.com/parquedb/parquedb/issues](https://github.com/parquedb/parquedb/issues)
3. **Cloudflare Workers Docs**: [developers.cloudflare.com/workers](https://developers.cloudflare.com/workers)
4. **Cloudflare Discord**: [discord.gg/cloudflaredev](https://discord.gg/cloudflaredev)

---

## Next Steps

Now that you have ParqueDB deployed on Cloudflare Workers, explore these topics:

- **[R2 Setup Guide](./r2-setup.md)** - Detailed R2 bucket configuration, public access, and cost optimization
- **[Configuration Reference](./configuration.md)** - Complete reference for all ParqueDB configuration options
- **[Node.js Standalone Guide](./node-standalone.md)** - Deploy ParqueDB without Cloudflare
- **[Getting Started](../getting-started.md)** - Basic ParqueDB usage and CRUD operations
- **[Query API](../queries.md)** - MongoDB-style filtering, sorting, and pagination
- **[Schema Definition](../schemas.md)** - Define typed schemas with validation and relationships
- **[Architecture Overview](../architecture/graph-first-architecture.md)** - Understand ParqueDB internals
