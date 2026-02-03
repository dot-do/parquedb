---
title: Cloudflare Workers
description: Deploy ParqueDB to Cloudflare Workers with R2 storage and Durable Objects for a production-ready setup.
---

This guide covers deploying ParqueDB to Cloudflare Workers with R2 storage and Durable Objects for a production-ready setup.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [wrangler.jsonc Configuration](#wranglerjsonc-configuration)
- [R2 Bucket Setup](#r2-bucket-setup)
- [Durable Object Configuration](#durable-object-configuration)
- [Environment Variables](#environment-variables)
- [Deployment](#deployment)
- [Monitoring and Logging](#monitoring-and-logging)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

ParqueDB uses a CQRS (Command Query Responsibility Segregation) architecture on Cloudflare Workers:

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

- **Worker**: Handles HTTP requests, routes reads to R2, writes to Durable Objects
- **Durable Object (ParqueDBDO)**: Single-writer for consistency, uses SQLite for metadata
- **R2 Bucket**: Stores Parquet data files, indexes, and relationship graphs
- **Cache API**: Caches frequently accessed data at the edge (free tier: 500MB)

---

## Prerequisites

Before deploying, ensure you have:

1. **Cloudflare Account** with Workers and R2 access
2. **Wrangler CLI** installed:
   ```bash
   npm install -g wrangler
   # or
   pnpm add -g wrangler
   ```
3. **Authenticated with Cloudflare**:
   ```bash
   wrangler login
   ```
4. **ParqueDB project** with dependencies installed:
   ```bash
   npm install parquedb
   # or add to your existing project
   npm install hyparquet hyparquet-writer
   ```

---

## Project Setup

### 1. Initialize Worker Project

If starting fresh:

```bash
npx wrangler init my-parquedb-api
cd my-parquedb-api
npm install parquedb
```

### 2. Project Structure

```
my-parquedb-api/
├── src/
│   └── index.ts          # Worker entrypoint
├── wrangler.jsonc        # Cloudflare configuration
├── package.json
└── tsconfig.json
```

### 3. Worker Entry Point

Create your worker entry point (`src/index.ts`):

```typescript
import { ParqueDBWorker, ParqueDBDO } from 'parquedb/worker'
import type { Env } from 'parquedb'

// Export the Durable Object class for Cloudflare runtime
export { ParqueDBDO }

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const worker = new ParqueDBWorker(ctx, env)
    const url = new URL(request.url)

    try {
      // Example: GET /posts - List posts
      if (url.pathname === '/posts' && request.method === 'GET') {
        const posts = await worker.find('posts', {
          status: 'published'
        }, {
          limit: 20,
          sort: { createdAt: -1 }
        })
        return Response.json(posts)
      }

      // Example: GET /posts/:id - Get single post
      const postMatch = url.pathname.match(/^\/posts\/([^/]+)$/)
      if (postMatch && request.method === 'GET') {
        const post = await worker.get('posts', postMatch[1])
        if (!post) {
          return Response.json({ error: 'Not found' }, { status: 404 })
        }
        return Response.json(post)
      }

      // Example: POST /posts - Create post
      if (url.pathname === '/posts' && request.method === 'POST') {
        const data = await request.json()
        const post = await worker.create('posts', data)
        return Response.json(post, { status: 201 })
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      console.error('ParqueDB error:', error)
      return Response.json(
        { error: error instanceof Error ? error.message : 'Internal error' },
        { status: 500 }
      )
    }
  }
}
```

---

## wrangler.jsonc Configuration

Create `wrangler.jsonc` in your project root:

```jsonc
{
  // =============================================================================
  // Basic Configuration
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
  // Production Environment
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

### Configuration Options Explained

| Option | Description |
|--------|-------------|
| `name` | Worker name (appears in dashboard and URLs) |
| `main` | Entry point file |
| `compatibility_date` | Cloudflare runtime version |
| `compatibility_flags` | Enable Node.js compatibility for crypto, etc. |
| `durable_objects.bindings` | DO class bindings (PARQUEDB is required) |
| `migrations` | DO schema migrations (use `new_sqlite_classes` for SQLite-backed DOs) |
| `r2_buckets` | R2 bucket bindings |
| `vars` | Environment variables |

---

## R2 Bucket Setup

### Create Buckets via Wrangler

```bash
# Create production bucket
npx wrangler r2 bucket create my-parquedb-data-prod

# Create staging bucket
npx wrangler r2 bucket create my-parquedb-data-staging

# Create preview bucket for local development
npx wrangler r2 bucket create my-parquedb-data-preview
```

### Verify Bucket Creation

```bash
npx wrangler r2 bucket list
```

### Data Layout

ParqueDB stores data in the following structure:

```
my-parquedb-data/
├── data/
│   ├── posts/
│   │   └── data.parquet       # Entity data
│   ├── users/
│   │   └── data.parquet
│   └── ...
├── rels/
│   ├── forward/
│   │   └── posts.parquet      # Outgoing relationships
│   └── reverse/
│       └── users.parquet      # Incoming relationships
├── indexes/
│   ├── bloom/
│   │   └── posts.bloom        # Bloom filters for fast ID lookups
│   └── secondary/
│       └── posts_status.idx   # Secondary indexes
└── events/
    └── current.parquet        # Event log for time-travel
```

For detailed R2 setup including public access, see [R2 Setup Guide](./r2-setup.md).

---

## Durable Object Configuration

### Understanding DO Migrations

Durable Objects require migrations when:
- First deployment (creating new DO classes)
- Changing from storage API to SQLite
- Adding new DO classes

```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  }
]
```

### Migration Best Practices

1. **Never remove migration entries** - Only append new ones
2. **Increment tags** for each migration: `v1`, `v2`, `v3`...
3. **Test migrations** in staging before production

### Adding a New Migration

```jsonc
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  },
  {
    "tag": "v2",
    "renamed_classes": [
      { "from": "OldDO", "to": "NewDO" }
    ]
  }
]
```

---

## Environment Variables

### Setting Secrets

For sensitive values, use Wrangler secrets:

```bash
# Set auth secret
npx wrangler secret put AUTH_SECRET

# Set for specific environment
npx wrangler secret put AUTH_SECRET --env production
```

### Cache Configuration

```jsonc
"vars": {
  "ENVIRONMENT": "production",
  "CACHE_DATA_TTL": "60",         // 1 minute for data
  "CACHE_METADATA_TTL": "300",    // 5 minutes for metadata
  "CACHE_BLOOM_TTL": "600",       // 10 minutes for bloom filters
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

### Custom Domain Variables

```jsonc
"vars": {
  "CDN_R2_DEV_URL": "https://cdn.yourdomain.com/parquedb"
}
```

---

## Deployment

### Development

```bash
# Start local development server
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

# Deploy with dry-run (shows what would change)
npx wrangler deploy --dry-run
```

### Verify Deployment

```bash
# Check deployment status
npx wrangler deployments list

# Tail logs
npx wrangler tail
```

### Rollback

```bash
# List deployments
npx wrangler deployments list

# Rollback to previous version
npx wrangler rollback
```

---

## Monitoring and Logging

### Wrangler Tail

Real-time log streaming:

```bash
npx wrangler tail

# Filter by status
npx wrangler tail --status error

# Filter by search string
npx wrangler tail --search "ParqueDB"
```

### Cloudflare Dashboard

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Navigate to Workers & Pages
3. Select your worker
4. View Analytics, Logs, and Metrics

### Custom Logging

```typescript
import { logger } from 'parquedb/utils'

// Log with structured data
logger.info('Query executed', {
  namespace: 'posts',
  duration: 45,
  rowsScanned: 1000,
  rowsReturned: 20
})

// Error logging
logger.error('Query failed', {
  namespace: 'posts',
  error: error.message
})
```

### Health Check Endpoint

Add a health check to your worker:

```typescript
if (url.pathname === '/health') {
  return Response.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    environment: env.ENVIRONMENT
  })
}
```

---

## Troubleshooting

### Common Issues

#### "Durable Object not found"

**Cause**: DO class not exported or migration not applied.

**Solution**:
```typescript
// Ensure DO is exported in your entry point
export { ParqueDBDO } from 'parquedb/worker'
```

And verify migrations in `wrangler.jsonc`.

#### "R2 bucket not found"

**Cause**: Bucket doesn't exist or binding name mismatch.

**Solution**:
```bash
# Verify bucket exists
npx wrangler r2 bucket list

# Create if missing
npx wrangler r2 bucket create my-parquedb-data
```

#### "nodejs_compat required"

**Cause**: Missing compatibility flag.

**Solution**: Add to `wrangler.jsonc`:
```jsonc
"compatibility_flags": ["nodejs_compat"]
```

#### Slow first request (cold start)

**Cause**: Worker cold start + R2 file fetch.

**Solutions**:
1. Enable caching:
   ```typescript
   const worker = new ParqueDBWorker(ctx, env, {
     cache: READ_HEAVY_CACHE_CONFIG
   })
   ```
2. Use CDN bucket for public datasets
3. Implement warm-up endpoint

#### "Memory limit exceeded"

**Cause**: Loading too much data into Worker memory.

**Solutions**:
1. Use pagination:
   ```typescript
   await worker.find('posts', {}, { limit: 100 })
   ```
2. Use projection to limit fields:
   ```typescript
   await worker.find('posts', {}, {
     project: { title: 1, status: 1 }
   })
   ```
3. Use streaming for large exports (requires custom implementation)

### Debug Mode

Enable debug logging:

```jsonc
"vars": {
  "LOG_LEVEL": "debug"
}
```

### Getting Help

1. Check [ParqueDB GitHub Issues](https://github.com/parquedb/parquedb/issues)
2. Review [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
3. Join the ParqueDB Discord community

---

## Next Steps

- [R2 Setup Guide](./r2-setup.md) - Detailed R2 configuration
- [Configuration Reference](./configuration.md) - All config options
- [Node.js Standalone Guide](./node-standalone.md) - Deploy without Cloudflare
- [Getting Started](../getting-started.md) - Basic usage guide
