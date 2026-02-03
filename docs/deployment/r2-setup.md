---
title: R2 Setup
description: Configure Cloudflare R2 object storage for ParqueDB data, including bucket creation, data loading, and cost estimation.
---

This guide covers deploying ParqueDB data to Cloudflare R2 object storage for production use with Cloudflare Workers.

## Table of Contents

- [What is Cloudflare R2](#what-is-cloudflare-r2)
- [R2 Bucket Setup](#r2-bucket-setup)
- [Configuring R2Backend](#configuring-r2backend)
- [Wrangler Configuration](#wrangler-configuration)
- [Loading Data to R2](#loading-data-to-r2)
- [Data Layout in R2](#data-layout-in-r2)
- [Querying from Workers](#querying-from-workers)
- [Advanced Topics](#advanced-topics)
- [Cost Estimation](#cost-estimation)
- [Pre-loaded Public Datasets](#pre-loaded-public-datasets)

---

## What is Cloudflare R2

### Overview

Cloudflare R2 is an S3-compatible object storage service that runs on Cloudflare's global network. Unlike traditional cloud object storage, R2 eliminates egress fees, making it ideal for data-intensive applications.

**Key Features:**

- **S3 Compatible API**: Works with existing S3 tools and libraries
- **Zero Egress Fees**: No bandwidth charges for data reads
- **Global Distribution**: Data accessible from 300+ cities worldwide
- **Automatic Caching**: Integrates with Cloudflare's CDN
- **Simple Pricing**: Pay only for storage and operations
- **Workers Integration**: Direct binding to Cloudflare Workers

### Benefits for ParqueDB

ParqueDB is designed to take full advantage of R2's unique capabilities:

1. **Cost-Effective Analytics**: Zero egress fees mean you can query large datasets without bandwidth costs
2. **Edge Performance**: Parquet files cached at Cloudflare's edge for sub-100ms query times
3. **Columnar Efficiency**: Read only the columns you need, reducing data transfer
4. **Scalable Storage**: Store terabytes of Parquet data affordably
5. **Built-in CDN**: Public datasets automatically distributed globally
6. **Worker Integration**: Direct R2 bindings avoid HTTP overhead

**Cost Comparison Example:**

For a 100GB dataset with 1M queries/month:
- **AWS S3 + Lambda**: $1.35 (storage) + $90 (egress @ $0.09/GB) = ~$91/month
- **Cloudflare R2 + Workers**: $1.35 (storage) + $10.80 (operations) = ~$12/month
- **Savings**: 87% cost reduction

### When to Use R2 for ParqueDB

R2 is ideal for:
- Public datasets and APIs
- Analytics dashboards
- High-traffic read-heavy workloads
- Cost-sensitive applications
- Edge-first architectures

Consider alternatives if:
- You need ACID transactions (use Durable Objects with SQLite)
- You require sub-millisecond latency (use KV for hot data)
- You're already heavily invested in AWS ecosystem

---

## R2 Bucket Setup

### Creating a Bucket via Wrangler CLI

The recommended way to create R2 buckets is using Wrangler CLI:

```bash
# Create a production bucket
npx wrangler r2 bucket create parquedb

# Create a preview bucket for development
npx wrangler r2 bucket create parquedb-preview

# Create buckets for different environments
npx wrangler r2 bucket create parquedb-staging
npx wrangler r2 bucket create parquedb-test

# Verify bucket creation
npx wrangler r2 bucket list
```

**Advantages of Wrangler CLI:**
- Scriptable and automatable
- Supports CI/CD pipelines
- Version controlled configuration
- Faster than dashboard for multiple buckets

### Creating a Bucket via Dashboard

1. Log in to the [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Navigate to **R2 Object Storage** in the sidebar
3. Click **Create bucket**
4. Enter a bucket name (e.g., `parquedb`)
5. Select your preferred location hint (optional)
6. Click **Create bucket**

**Advantages of Dashboard:**
- Visual interface
- Easy to configure public access
- View bucket metrics and usage
- Configure lifecycle policies

### Bucket Naming Conventions

Follow these best practices for bucket naming:

```bash
# Good: Environment suffix
parquedb-prod
parquedb-staging
parquedb-dev

# Good: Project and environment
myapp-parquedb-prod
analytics-parquedb-prod

# Good: Team and purpose
data-team-datasets
public-datasets-prod

# Avoid: Special characters or spaces
parquedb_prod        # Underscores work but hyphens preferred
parquedb.prod        # Dots can conflict with domain names
parquedb prod        # Spaces not allowed
```

**Naming Rules:**
- 3-63 characters
- Lowercase letters, numbers, hyphens
- Must start and end with letter or number
- No dots (can conflict with custom domains)
- Globally unique within your account

---

## Configuring R2Backend

### Basic Setup with R2 Binding

The `R2Backend` class provides a storage backend implementation for ParqueDB using Cloudflare R2:

```typescript
import { R2Backend } from 'parquedb/storage'
import type { R2Bucket } from '@cloudflare/workers-types'

// In your Worker fetch handler
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Create R2 backend from Worker binding
    const storage = new R2Backend(env.BUCKET)

    // Use with ParqueDB
    const db = new ParqueDB({ storage })

    // Now you can query data
    const results = await db.find('posts', { status: 'published' })
    return Response.json(results)
  }
}
```

### Configuration Options

The `R2Backend` constructor accepts an optional configuration object:

```typescript
interface R2BackendOptions {
  /** Prefix for all keys (optional) */
  prefix?: string

  /** TTL for multipart uploads in milliseconds (default: 30 minutes) */
  multipartUploadTTL?: number
}
```

#### Using a Prefix

Prefixes help organize data within a single bucket:

```typescript
// Separate environments in one bucket
const prodStorage = new R2Backend(env.BUCKET, { prefix: 'prod/' })
const stagingStorage = new R2Backend(env.BUCKET, { prefix: 'staging/' })

// Separate teams or projects
const analyticsStorage = new R2Backend(env.BUCKET, { prefix: 'analytics/' })
const publicStorage = new R2Backend(env.BUCKET, { prefix: 'public/' })

// All paths will be automatically prefixed
await prodStorage.write('data/posts/data.parquet', data)
// Actually writes to: prod/data/posts/data.parquet
```

**When to use prefixes:**
- Multiple environments in one bucket (dev/staging/prod)
- Multi-tenant applications
- Separating public vs private data
- Testing without separate buckets

**When to use separate buckets:**
- Production isolation (recommended)
- Different access control requirements
- Lifecycle policy differences
- Cost tracking per environment

#### Multipart Upload TTL

Configure how long incomplete multipart uploads are tracked:

```typescript
const storage = new R2Backend(env.BUCKET, {
  multipartUploadTTL: 60 * 60 * 1000 // 1 hour (default: 30 minutes)
})

// The backend automatically cleans up stale uploads
storage.cleanupStaleUploads() // Returns count of cleaned uploads

// Check active uploads
console.log(`Active uploads: ${storage.activeUploadCount}`)
```

### Error Handling

The R2Backend throws specific error types for different failure scenarios:

```typescript
import {
  R2OperationError,
  R2NotFoundError,
  R2ETagMismatchError
} from 'parquedb/storage'

try {
  const data = await storage.read('data/posts/data.parquet')
} catch (error) {
  if (error instanceof R2NotFoundError) {
    // File doesn't exist
    console.error('File not found:', error.key)
    return Response.json({ error: 'Data not found' }, { status: 404 })
  }

  if (error instanceof R2ETagMismatchError) {
    // Conditional write failed (version mismatch)
    console.error('Version conflict:', error.expectedEtag, 'vs', error.actualEtag)
    return Response.json({ error: 'Data was modified' }, { status: 409 })
  }

  if (error instanceof R2OperationError) {
    // General R2 operation failure
    console.error('R2 operation failed:', error.operation, error.message)
    return Response.json({ error: 'Storage error' }, { status: 500 })
  }

  // Unknown error
  throw error
}
```

#### Common Error Scenarios

**File Not Found:**
```typescript
try {
  const data = await storage.read('missing.parquet')
} catch (error) {
  if (error instanceof R2NotFoundError) {
    // Handle gracefully - maybe return empty results
    return []
  }
}
```

**Conditional Write Failures:**
```typescript
try {
  // Write only if ETag matches (optimistic locking)
  await storage.writeConditional('data.parquet', newData, expectedETag)
} catch (error) {
  if (error instanceof R2ETagMismatchError) {
    // Data was modified by another writer
    // Option 1: Retry with fresh read
    // Option 2: Return conflict error to client
  }
}
```

**Operation Timeouts:**
```typescript
try {
  await storage.write('large-file.parquet', hugeData)
} catch (error) {
  if (error instanceof R2OperationError) {
    // May be timeout or network issue
    // Consider retry with exponential backoff
  }
}
```

### R2Backend API Reference

#### Read Operations

```typescript
// Read entire file
const data: Uint8Array = await storage.read('path/to/file.parquet')

// Read byte range (efficient for column scanning)
const chunk: Uint8Array = await storage.readRange('file.parquet', 0, 1024)

// Check if file exists
const exists: boolean = await storage.exists('file.parquet')

// Get file metadata
const stat = await storage.stat('file.parquet')
// Returns: { path, size, mtime, etag, contentType, metadata }
```

#### Write Operations

```typescript
// Write file
const result = await storage.write('file.parquet', data, {
  contentType: 'application/octet-stream',
  cacheControl: 'public, max-age=3600',
  metadata: { 'x-parquedb-version': '1' }
})
// Returns: { etag, size, versionId }

// Atomic write
await storage.writeAtomic('file.parquet', data)

// Conditional write (optimistic locking)
await storage.writeConditional('file.parquet', data, expectedETag)

// Append to file (uses read-modify-write with retry)
await storage.append('log.txt', newLogData)
```

#### Multipart Uploads

For files larger than 5MB, use multipart uploads:

```typescript
// Automatic chunking
await storage.writeStreaming('large.parquet', largeData, {
  partSize: 10 * 1024 * 1024, // 10MB parts
  contentType: 'application/octet-stream'
})

// Manual multipart control
const uploadId = await storage.startMultipartUpload('file.parquet')
try {
  const parts = []
  for (let i = 0; i < chunks.length; i++) {
    const { etag } = await storage.uploadPart('file.parquet', uploadId, i + 1, chunks[i])
    parts.push({ partNumber: i + 1, etag })
  }
  await storage.completeMultipartUpload('file.parquet', uploadId, parts)
} catch (error) {
  await storage.abortMultipartUpload('file.parquet', uploadId)
  throw error
}
```

#### List and Delete Operations

```typescript
// List files with prefix
const result = await storage.list('data/posts/', {
  limit: 100,
  includeMetadata: true,
  delimiter: '/'
})
// Returns: { files: string[], prefixes?: string[], cursor?, hasMore, stats? }

// Delete single file
const deleted: boolean = await storage.delete('file.parquet')

// Delete all files with prefix
const count: number = await storage.deletePrefix('old-data/')
```

#### File Operations

```typescript
// Copy file
await storage.copy('source.parquet', 'destination.parquet')

// Move file (copy + delete)
await storage.move('old-path.parquet', 'new-path.parquet')
```

---

## Wrangler Configuration

### R2 Bindings in wrangler.toml

### Basic R2 Bindings

Add R2 bucket bindings to your `wrangler.toml`:

```toml
# =============================================================================
# R2 Storage Configuration
# =============================================================================

[[r2_buckets]]
binding = "BUCKET"                         # Binding name in env.BUCKET
bucket_name = "parquedb-prod"              # Production bucket
preview_bucket_name = "parquedb-preview"   # Local dev bucket (wrangler dev)
```

**Binding Name Best Practices:**
- Use `BUCKET` for primary storage (matches ParqueDB defaults)
- Use descriptive names for multiple bindings: `DATA_BUCKET`, `ASSETS_BUCKET`
- Use UPPER_SNAKE_CASE for consistency with environment variables

### Environment-Specific Buckets

Configure different buckets per environment:

```toml
# Default environment (development)
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-dev"
preview_bucket_name = "parquedb-preview"

# Production environment
[env.production]
[[env.production.r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-prod"

# Staging environment
[env.staging]
[[env.staging.r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-staging"

# Test environment (ephemeral)
[env.test]
[[env.test.r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-test"
```

**Deploy to specific environment:**
```bash
# Deploy to production
npx wrangler deploy --env production

# Deploy to staging
npx wrangler deploy --env staging

# Local development uses preview_bucket_name
npx wrangler dev
```

### Multiple Bucket Bindings

Use multiple buckets for different purposes:

```toml
# Primary data storage (private)
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "parquedb-data"
preview_bucket_name = "parquedb-data-preview"

# Public datasets (read-only)
[[r2_buckets]]
binding = "PUBLIC_BUCKET"
bucket_name = "parquedb-public"

# User uploads
[[r2_buckets]]
binding = "UPLOADS_BUCKET"
bucket_name = "parquedb-uploads"

# Backups and archives
[[r2_buckets]]
binding = "ARCHIVE_BUCKET"
bucket_name = "parquedb-archive"
```

**Usage in Worker:**
```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Different storage backends for different purposes
    const dataStorage = new R2Backend(env.DATA_BUCKET)
    const publicStorage = new R2Backend(env.PUBLIC_BUCKET)
    const uploadsStorage = new R2Backend(env.UPLOADS_BUCKET)

    // Route based on data type
    const url = new URL(request.url)
    if (url.pathname.startsWith('/public/')) {
      // Serve public datasets
      const data = await publicStorage.read(url.pathname.slice(8))
      return new Response(data)
    }

    // Use main data storage for API queries
    const db = new ParqueDB({ storage: dataStorage })
    return handleQuery(db, request)
  }
}
```

### Jurisdiction and Location

R2 supports jurisdiction and location hints for data residency:

```toml
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-eu"
jurisdiction = "eu"  # EU data residency

[[r2_buckets]]
binding = "BUCKET_US"
bucket_name = "parquedb-us"
# jurisdiction defaults to "default" (automatic)
```

**Jurisdiction Options:**
- `eu` - European Union data residency
- `fedramp` - FedRAMP compliance (requires Enterprise plan)
- `default` - Automatic (Cloudflare chooses optimal location)

**Note:** Jurisdiction is set at bucket creation time and cannot be changed. You must create a new bucket to change jurisdiction.

### Complete wrangler.toml Example

```toml
name = "parquedb-api"
main = "src/index.ts"
compatibility_date = "2026-01-30"
compatibility_flags = ["nodejs_compat"]

# =============================================================================
# R2 Storage
# =============================================================================

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-dev"
preview_bucket_name = "parquedb-preview"

# =============================================================================
# Production Environment
# =============================================================================

[env.production]

vars = {
  ENVIRONMENT = "production"
  CACHE_DATA_TTL = "300"
  CACHE_METADATA_TTL = "900"
}

[[env.production.r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-prod"
jurisdiction = "eu"  # If EU data residency required

# =============================================================================
# Staging Environment
# =============================================================================

[env.staging]

vars = { ENVIRONMENT = "staging" }

[[env.staging.r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb-staging"
```

### Public Access vs Private

#### Private Buckets (Default)

By default, R2 buckets are private and only accessible via Workers bindings:

```typescript
// Access via Worker binding (secure, no public URL)
const data = await env.BUCKET.get('onet/occupations/data.parquet')
```

#### Public Buckets

To enable public HTTP access to your R2 bucket:

1. **Via Dashboard:**
   - Go to R2 > Your Bucket > Settings
   - Enable "Public access"
   - Note the public URL: `https://pub-{hash}.r2.dev/{key}`

2. **Via Custom Domain:**
   ```toml
   # In wrangler.toml (requires a zone on Cloudflare)
   [[r2_buckets]]
   binding = "BUCKET"
   bucket_name = "parquedb"

   # Custom domain configuration is done in the dashboard
   # R2 > Bucket > Settings > Custom Domains
   ```

3. **Access Control for Public Buckets:**
   ```typescript
   // Worker that serves public data with caching
   export default {
     async fetch(request: Request, env: Env) {
       const url = new URL(request.url)
       const key = url.pathname.slice(1) // Remove leading /

       // Only allow .parquet files
       if (!key.endsWith('.parquet')) {
         return new Response('Not Found', { status: 404 })
       }

       const object = await env.BUCKET.get(key)
       if (!object) {
         return new Response('Not Found', { status: 404 })
       }

       return new Response(object.body, {
         headers: {
           'Content-Type': 'application/octet-stream',
           'Cache-Control': 'public, max-age=3600',
         },
       })
     },
   }
   ```

---

## Loading Data to R2

### Using the R2Backend for Data Upload

ParqueDB provides `R2Backend` for direct R2 operations:

```typescript
import { R2Backend } from 'parquedb'

// In your Worker
export default {
  async fetch(request: Request, env: Env) {
    const r2 = new R2Backend(env.BUCKET, {
      prefix: 'parquedb/'  // Optional prefix for all keys
    })

    // Write a Parquet file
    await r2.write('onet/occupations/data.parquet', parquetData, {
      contentType: 'application/octet-stream',
      metadata: {
        'x-parquedb-version': '1',
        'x-parquedb-rows': '1000',
      }
    })

    return new Response('OK')
  }
}
```

### Multipart Upload for Large Files

For files larger than 5MB, use multipart uploads:

```typescript
import { R2Backend } from 'parquedb'

async function uploadLargeFile(
  r2: R2Backend,
  path: string,
  data: Uint8Array
): Promise<void> {
  const PART_SIZE = 10 * 1024 * 1024 // 10MB parts

  // Use writeStreaming for automatic chunking
  await r2.writeStreaming(path, data, {
    partSize: PART_SIZE,
    contentType: 'application/octet-stream',
  })
}

// Or use manual multipart control
async function uploadWithProgress(
  r2: R2Backend,
  path: string,
  data: Uint8Array,
  onProgress: (percent: number) => void
): Promise<void> {
  const PART_SIZE = 10 * 1024 * 1024
  const numParts = Math.ceil(data.length / PART_SIZE)

  // Start multipart upload
  const uploadId = await r2.startMultipartUpload(path)
  const parts: Array<{ partNumber: number; etag: string }> = []

  try {
    for (let i = 0; i < numParts; i++) {
      const start = i * PART_SIZE
      const end = Math.min(start + PART_SIZE, data.length)
      const chunk = data.slice(start, end)

      const { etag } = await r2.uploadPart(path, uploadId, i + 1, chunk)
      parts.push({ partNumber: i + 1, etag })

      onProgress(((i + 1) / numParts) * 100)
    }

    // Complete upload
    await r2.completeMultipartUpload(path, uploadId, parts)
  } catch (error) {
    // Abort on error to clean up partial upload
    await r2.abortMultipartUpload(path, uploadId)
    throw error
  }
}
```

### Example: Data Loader Worker

A complete Worker for loading external datasets into R2:

```typescript
import { R2Backend } from 'parquedb'
import { parquetWrite } from 'hyparquet-writer'

interface Env {
  BUCKET: R2Bucket
  LOADER_API_KEY: string
}

interface LoadRequest {
  source: string      // URL or dataset identifier
  namespace: string   // Target namespace in R2
  format: 'csv' | 'json' | 'parquet'
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Verify API key
    const apiKey = request.headers.get('X-API-Key')
    if (apiKey !== env.LOADER_API_KEY) {
      return new Response('Unauthorized', { status: 401 })
    }

    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 })
    }

    const body = await request.json() as LoadRequest
    const { source, namespace, format } = body

    try {
      // Fetch source data
      const response = await fetch(source)
      if (!response.ok) {
        throw new Error(`Failed to fetch: ${response.status}`)
      }

      let data: Record<string, unknown>[]

      // Parse based on format
      switch (format) {
        case 'json':
          data = await response.json() as Record<string, unknown>[]
          break
        case 'csv':
          const text = await response.text()
          data = parseCSV(text)
          break
        case 'parquet':
          // Direct copy for Parquet files
          const parquetData = new Uint8Array(await response.arrayBuffer())
          const r2 = new R2Backend(env.BUCKET)
          await r2.writeStreaming(
            `${namespace}/data.parquet`,
            parquetData
          )
          return Response.json({
            success: true,
            path: `${namespace}/data.parquet`,
            size: parquetData.length
          })
        default:
          throw new Error(`Unsupported format: ${format}`)
      }

      // Convert to Parquet
      const parquetBuffer = await parquetWrite({
        schema: inferSchema(data),
        data,
      })

      // Upload to R2
      const r2 = new R2Backend(env.BUCKET)
      await r2.writeStreaming(
        `${namespace}/data.parquet`,
        new Uint8Array(parquetBuffer)
      )

      return Response.json({
        success: true,
        path: `${namespace}/data.parquet`,
        rows: data.length,
        size: parquetBuffer.byteLength,
      })
    } catch (error) {
      return Response.json(
        { success: false, error: (error as Error).message },
        { status: 500 }
      )
    }
  },
}

// Helper functions
function parseCSV(text: string): Record<string, unknown>[] {
  const lines = text.trim().split('\n')
  const headers = lines[0].split(',').map(h => h.trim())

  return lines.slice(1).map(line => {
    const values = line.split(',')
    const row: Record<string, unknown> = {}
    headers.forEach((h, i) => {
      row[h] = values[i]?.trim() ?? null
    })
    return row
  })
}

function inferSchema(data: Record<string, unknown>[]): Record<string, string> {
  const schema: Record<string, string> = {}
  if (data.length === 0) return schema

  const sample = data[0]
  for (const [key, value] of Object.entries(sample)) {
    if (typeof value === 'number') {
      schema[key] = Number.isInteger(value) ? 'INT64' : 'DOUBLE'
    } else if (typeof value === 'boolean') {
      schema[key] = 'BOOLEAN'
    } else {
      schema[key] = 'UTF8'
    }
  }
  return schema
}
```

### Loading via CLI Script

For local development or one-time loads, use a Node.js script with the Wrangler R2 commands:

```typescript
// scripts/load-to-r2.ts
import { execSync } from 'child_process'
import { readFileSync } from 'fs'

const BUCKET = 'parquedb'

async function loadDataset(
  localPath: string,
  r2Path: string
): Promise<void> {
  console.log(`Uploading ${localPath} to r2://${BUCKET}/${r2Path}`)

  execSync(
    `npx wrangler r2 object put ${BUCKET}/${r2Path} --file ${localPath}`,
    { stdio: 'inherit' }
  )

  console.log('Upload complete')
}

// Load O*NET dataset
await loadDataset(
  './data/onet/occupations.parquet',
  'onet/occupations/data.parquet'
)

// Load with partitions
for (const type of ['movie', 'tvSeries', 'short']) {
  await loadDataset(
    `./data/imdb/titles-${type}.parquet`,
    `imdb/titles/type=${type}/data.parquet`
  )
}
```

---

## Data Layout in R2

ParqueDB uses a hierarchical layout optimized for efficient querying:

```
parquedb/
  onet/
    occupations/data.parquet
    skills/data.parquet
    abilities/data.parquet
    knowledge/data.parquet
    tasks/data.parquet
    _meta/
      schema.json
      stats.json
  imdb/
    titles/
      type=movie/data.parquet
      type=tvSeries/data.parquet
      type=tvMovie/data.parquet
      type=short/data.parquet
    names/data.parquet
    ratings/data.parquet
    _meta/
      schema.json
  indexes/
    bloom/
      onet-occupations.bloom
      imdb-titles.bloom
  rels/
    forward/
      occupations.parquet
    reverse/
      skills.parquet
```

### Layout Conventions

| Path Pattern | Description |
|--------------|-------------|
| `{namespace}/data.parquet` | Main data file for a namespace |
| `{namespace}/{partition}={value}/data.parquet` | Partitioned data files |
| `{namespace}/_meta/schema.json` | Schema definition |
| `{namespace}/_meta/stats.json` | Data statistics |
| `indexes/bloom/{namespace}.bloom` | Bloom filter for fast ID lookups |
| `rels/forward/{namespace}.parquet` | Outgoing relationships |
| `rels/reverse/{namespace}.parquet` | Incoming relationships |

### Partitioning Strategy

Use partitioning for large datasets to enable partition pruning:

```typescript
// Good: Partition by common filter field
imdb/titles/type=movie/data.parquet      // 500K rows
imdb/titles/type=tvSeries/data.parquet   // 200K rows

// Query only reads the movie partition
await db.find('imdb/titles', { type: 'movie' })
```

Recommended partition sizes:
- **Minimum:** 10MB per partition (avoid too many small files)
- **Maximum:** 1GB per partition (balance read performance)
- **Optimal:** 50-200MB per partition

---

## Querying from Workers

### R2Backend Configuration

```typescript
import { R2Backend, ParqueDBWorker } from 'parquedb'
import type { Env } from './types'

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    // Create ParqueDB Worker instance
    const db = new ParqueDBWorker(ctx, env)

    // Query data
    const occupations = await db.find('onet/occupations', {
      median_wage: { $gte: 100000 }
    }, {
      sort: { median_wage: -1 },
      limit: 10,
      project: { title: 1, median_wage: 1, description: 1 }
    })

    return Response.json(occupations)
  }
}
```

### Direct R2Backend Usage

For read-only access without full ParqueDB features:

```typescript
import { R2Backend } from 'parquedb'
import { parquetRead } from 'hyparquet'

export default {
  async fetch(request: Request, env: Env) {
    const r2 = new R2Backend(env.BUCKET, { prefix: 'parquedb/' })

    // Read Parquet file
    const data = await r2.read('onet/occupations/data.parquet')

    // Parse with hyparquet
    const rows: Record<string, unknown>[] = []
    await parquetRead({
      file: {
        byteLength: data.length,
        slice: (start, end) => data.slice(start, end),
      },
      onComplete: (result) => rows.push(...result),
    })

    return Response.json(rows.slice(0, 10))
  }
}
```

### Caching Strategies

ParqueDB provides built-in caching via the `CacheStrategy` class:

```typescript
import {
  CacheStrategy,
  DEFAULT_CACHE_CONFIG,
  READ_HEAVY_CACHE_CONFIG,
  WRITE_HEAVY_CACHE_CONFIG
} from 'parquedb'

// Default: Balanced caching
// - Data: 1 minute TTL
// - Metadata: 5 minutes TTL
// - Bloom filters: 10 minutes TTL
const defaultStrategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

// Read-heavy workloads (analytics dashboards)
// - Data: 5 minutes TTL
// - Metadata: 15 minutes TTL
// - Bloom filters: 30 minutes TTL
const analyticsStrategy = new CacheStrategy(READ_HEAVY_CACHE_CONFIG)

// Write-heavy workloads (frequently updated data)
// - Data: 15 seconds TTL
// - Metadata: 1 minute TTL
// - Bloom filters: 2 minutes TTL
const realtimeStrategy = new CacheStrategy(WRITE_HEAVY_CACHE_CONFIG)
```

#### Custom Cache Configuration

```typescript
const customStrategy = new CacheStrategy({
  dataTtl: 120,           // 2 minutes
  metadataTtl: 600,       // 10 minutes
  bloomTtl: 1800,         // 30 minutes
  staleWhileRevalidate: true,
})

// Use with ReadPath
const readPath = new ReadPath(env.BUCKET, cache, customStrategy.config)
```

#### Environment-based Cache Configuration

```toml
# wrangler.toml
[vars]
CACHE_DATA_TTL = "60"
CACHE_METADATA_TTL = "300"
CACHE_BLOOM_TTL = "600"
CACHE_STALE_WHILE_REVALIDATE = "true"
```

```typescript
import { createCacheStrategy } from 'parquedb'

const strategy = createCacheStrategy(env)
```

### Example Queries

```typescript
// Find all high-wage tech occupations
const techJobs = await db.find('onet/occupations', {
  $and: [
    { median_wage: { $gte: 120000 } },
    {
      $or: [
        { title: { $regex: 'Software' } },
        { title: { $regex: 'Data' } },
        { title: { $regex: 'Engineer' } }
      ]
    }
  ]
}, {
  sort: { median_wage: -1 },
  limit: 20
})

// Get movies by rating with pagination
const topMovies = await db.find('imdb/titles', {
  type: 'movie',
  averageRating: { $gte: 8.0 },
  numVotes: { $gte: 100000 }
}, {
  sort: { averageRating: -1, numVotes: -1 },
  limit: 50,
  cursor: lastCursor  // For pagination
})

// Count by category
const movieCount = await db.count('imdb/titles', { type: 'movie' })
const seriesCount = await db.count('imdb/titles', { type: 'tvSeries' })
```

---

## Advanced Topics

### Multi-Region Considerations

R2 automatically replicates data globally, but understanding its architecture helps optimize performance:

#### How R2 Replication Works

1. **Single Source of Truth**: Data written to one location
2. **Global Replication**: Automatically replicated to Cloudflare's network
3. **Edge Caching**: Frequently accessed data cached at edge
4. **Smart Routing**: Requests routed to nearest location with data

#### Optimizing for Multi-Region Access

**1. Choose the Right Jurisdiction**

```toml
# EU jurisdiction - optimized for European access
[[r2_buckets]]
binding = "BUCKET_EU"
bucket_name = "parquedb-eu"
jurisdiction = "eu"

# Default jurisdiction - global optimization
[[r2_buckets]]
binding = "BUCKET_GLOBAL"
bucket_name = "parquedb-global"
jurisdiction = "default"
```

**2. Use Regional Buckets for Hot Data**

```typescript
// Route requests to region-specific buckets
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const region = request.cf?.region || 'US'

    // Choose bucket based on client region
    const bucket = region.startsWith('EU') ? env.BUCKET_EU : env.BUCKET_US
    const storage = new R2Backend(bucket)

    const db = new ParqueDB({ storage })
    return handleQuery(db, request)
  }
}
```

**3. Leverage Edge Caching**

```typescript
import { CacheStrategy, READ_HEAVY_CACHE_CONFIG } from 'parquedb'

// Cache frequently accessed data at the edge
const storage = new R2Backend(env.BUCKET)
const cache = new CacheStrategy(READ_HEAVY_CACHE_CONFIG)

// Cache responses at edge for global distribution
const response = await db.find('popular-posts', {})
const cachedResponse = new Response(JSON.stringify(response), {
  headers: {
    'Content-Type': 'application/json',
    'Cache-Control': 'public, max-age=300, s-maxage=3600'
  }
})

return cachedResponse
```

**4. Partition Data by Region**

```typescript
// Store regional data in separate prefixes
const usStorage = new R2Backend(env.BUCKET, { prefix: 'us/' })
const euStorage = new R2Backend(env.BUCKET, { prefix: 'eu/' })
const apacStorage = new R2Backend(env.BUCKET, { prefix: 'apac/' })

// Write to regional prefix
await usStorage.write('data/users/data.parquet', usUserData)
await euStorage.write('data/users/data.parquet', euUserData)
```

#### Latency Expectations

| Scenario | Typical Latency | Optimization |
|----------|----------------|--------------|
| First request (cache miss) | 200-500ms | Use warmup, preload |
| Cached at edge | 50-100ms | Default behavior |
| Same region bucket | 100-200ms | Choose jurisdiction |
| Cross-region bucket | 300-800ms | Use regional buckets |

### Lifecycle Policies

R2 does not currently support automatic lifecycle policies like S3. Use Durable Objects for custom lifecycle management:

#### Custom Lifecycle Manager

```typescript
// Durable Object for managing data lifecycle
export class LifecycleManager extends DurableObject {
  async scheduleCleanup(namespace: string, retentionDays: number) {
    const cutoff = Date.now() - retentionDays * 24 * 60 * 60 * 1000

    const storage = new R2Backend(this.env.BUCKET)

    // List old files
    const files = await storage.list(`data/${namespace}/`, {
      includeMetadata: true
    })

    // Delete files older than retention period
    for (const stat of files.stats || []) {
      if (stat.mtime.getTime() < cutoff) {
        await storage.delete(stat.path)
      }
    }
  }
}
```

#### Scheduled Cleanup Worker

```typescript
// Worker triggered by Cron Trigger
export default {
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    const storage = new R2Backend(env.BUCKET)

    // Archive old events
    const archiveDate = new Date()
    archiveDate.setDate(archiveDate.getDate() - 90)

    // Move old events to archive prefix
    const events = await storage.list('events/', { includeMetadata: true })
    for (const stat of events.stats || []) {
      if (stat.mtime < archiveDate) {
        const archivePath = `archive/${stat.path}`
        await storage.move(stat.path, archivePath)
      }
    }

    // Delete very old archives
    const deleteDate = new Date()
    deleteDate.setFullYear(deleteDate.getFullYear() - 1)

    const archives = await storage.list('archive/', { includeMetadata: true })
    for (const stat of archives.stats || []) {
      if (stat.mtime < deleteDate) {
        await storage.delete(stat.path)
      }
    }
  }
}
```

#### Backup and Archive Strategy

```typescript
// Daily backup to archive bucket
export async function backupToArchive(
  sourceStorage: R2Backend,
  archiveStorage: R2Backend,
  namespace: string
): Promise<void> {
  const timestamp = new Date().toISOString().split('T')[0] // YYYY-MM-DD

  // List all files in namespace
  const files = await sourceStorage.list(`data/${namespace}/`)

  // Copy to archive with timestamp
  for (const file of files.files) {
    const archivePath = `backups/${timestamp}/${file}`
    const data = await sourceStorage.read(file)
    await archiveStorage.write(archivePath, data)
  }
}
```

### Access Control

R2 access is controlled at the Worker level. Implement custom access control in your Worker:

#### API Key Authentication

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Verify API key
    const apiKey = request.headers.get('X-API-Key')
    if (!apiKey || apiKey !== env.API_KEY) {
      return new Response('Unauthorized', { status: 401 })
    }

    const storage = new R2Backend(env.BUCKET)
    const db = new ParqueDB({ storage })

    // Process authenticated request
    return handleQuery(db, request)
  }
}
```

#### Role-Based Access Control

```typescript
interface User {
  id: string
  role: 'admin' | 'user' | 'readonly'
}

async function checkAccess(
  user: User,
  operation: 'read' | 'write',
  namespace: string
): Promise<boolean> {
  // Admins can do anything
  if (user.role === 'admin') return true

  // Read-only users can only read
  if (user.role === 'readonly') return operation === 'read'

  // Users can read/write their own namespace
  return namespace.startsWith(`users/${user.id}/`)
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const user = await authenticateUser(request)
    if (!user) {
      return new Response('Unauthorized', { status: 401 })
    }

    const url = new URL(request.url)
    const namespace = url.pathname.split('/')[1]
    const operation = request.method === 'GET' ? 'read' : 'write'

    if (!await checkAccess(user, operation, namespace)) {
      return new Response('Forbidden', { status: 403 })
    }

    const storage = new R2Backend(env.BUCKET)
    const db = new ParqueDB({ storage })
    return handleQuery(db, request)
  }
}
```

#### Namespace Isolation

Use prefixes to isolate tenant data:

```typescript
// Multi-tenant isolation
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const tenantId = request.headers.get('X-Tenant-ID')
    if (!tenantId) {
      return new Response('Tenant ID required', { status: 400 })
    }

    // Each tenant gets isolated storage
    const storage = new R2Backend(env.BUCKET, {
      prefix: `tenants/${tenantId}/`
    })

    const db = new ParqueDB({ storage })
    return handleQuery(db, request)
  }
}
```

#### Public vs Private Data

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // Public data - no authentication required
    if (url.pathname.startsWith('/public/')) {
      const storage = new R2Backend(env.PUBLIC_BUCKET)
      const path = url.pathname.slice(8) // Remove /public/

      const data = await storage.read(path)
      return new Response(data, {
        headers: {
          'Content-Type': 'application/octet-stream',
          'Cache-Control': 'public, max-age=3600'
        }
      })
    }

    // Private data - authentication required
    const user = await authenticateUser(request)
    if (!user) {
      return new Response('Unauthorized', { status: 401 })
    }

    const storage = new R2Backend(env.PRIVATE_BUCKET)
    const db = new ParqueDB({ storage })
    return handleQuery(db, request)
  }
}
```

### Cost Optimization

Strategies to minimize R2 costs while maintaining performance:

#### 1. Optimize File Sizes

```typescript
// Good: Files between 50MB-200MB
// - Efficient for columnar scanning
// - Good compression ratio
// - Reasonable request counts

// Too small: Many small files (< 10MB)
// - Increases request costs (Class A operations)
// - Poor compression
// - More metadata overhead

// Too large: Very large files (> 1GB)
// - Slow first query
// - More data transfer per request
// - Harder to partition

// Optimal partitioning
async function optimizeFileSize(
  storage: R2Backend,
  namespace: string,
  targetSize: number = 100 * 1024 * 1024 // 100MB
): Promise<void> {
  // Compact small files
  const files = await storage.list(`data/${namespace}/`, {
    includeMetadata: true
  })

  const smallFiles = (files.stats || []).filter(f => f.size < targetSize / 2)
  if (smallFiles.length > 1) {
    // Merge small files into one
    const merged = await mergeParquetFiles(smallFiles)
    await storage.write(`data/${namespace}/merged.parquet`, merged)

    // Delete old files
    for (const file of smallFiles) {
      await storage.delete(file.path)
    }
  }
}
```

#### 2. Leverage Caching

```typescript
// Cache frequently accessed data
import { CacheStrategy, READ_HEAVY_CACHE_CONFIG } from 'parquedb'

// Long cache for static data
const staticCache = new CacheStrategy({
  dataTtl: 3600,        // 1 hour
  metadataTtl: 7200,    // 2 hours
  bloomTtl: 14400,      // 4 hours
  staleWhileRevalidate: true
})

// Short cache for dynamic data
const dynamicCache = new CacheStrategy({
  dataTtl: 60,          // 1 minute
  metadataTtl: 300,     // 5 minutes
  bloomTtl: 600,        // 10 minutes
  staleWhileRevalidate: true
})
```

#### 3. Minimize Column Reads

```typescript
// Bad: Reading all columns (transfers more data)
const posts = await db.find('posts', {}, {
  limit: 100
})

// Good: Project only needed columns (reduces data transfer)
const posts = await db.find('posts', {}, {
  limit: 100,
  project: {
    title: 1,
    status: 1,
    createdAt: 1
  }
})

// Savings: If posts have 50 fields, projection reduces data by ~95%
```

#### 4. Use Bloom Filters

```typescript
// Enable bloom filters for fast ID lookups
// Reduces unnecessary reads for non-existent IDs

const storage = new R2Backend(env.BUCKET)
const db = new ParqueDB({
  storage,
  indexes: {
    bloom: {
      enabled: true,
      falsePositiveRate: 0.01 // 1% false positive rate
    }
  }
})

// Bloom filter check avoids R2 read if ID doesn't exist
const post = await db.get('posts', 'non-existent-id')
// Returns null immediately without R2 request
```

#### 5. Batch Operations

```typescript
// Bad: Many small requests
for (const id of userIds) {
  await db.get('users', id) // 100 requests
}

// Good: Single query with filter
const users = await db.find('users', {
  $id: { $in: userIds }
}) // 1 request
```

#### 6. Partition Data

```typescript
// Partition large datasets for efficient querying
// Example: Time-based partitioning

// Write to date-partitioned files
const date = new Date().toISOString().split('T')[0]
await storage.write(
  `data/events/date=${date}/data.parquet`,
  eventData
)

// Query only relevant partitions (partition pruning)
const todayEvents = await db.find('events', {
  date: '2026-02-03'  // Only reads data/events/date=2026-02-03/
})
```

#### 7. Compress Data

```typescript
// Parquet files are already compressed, but you can optimize:

// Choose compression codec
import { parquetWrite } from 'hyparquet-writer'

const data = await parquetWrite({
  schema: schema,
  data: rows,
  compression: 'ZSTD'  // Or GZIP, SNAPPY, LZ4
})

// Compression comparison:
// - ZSTD: Best compression, slower (recommended for cold data)
// - GZIP: Good compression, moderate speed (default)
// - SNAPPY: Fast, less compression (recommended for hot data)
// - LZ4: Fastest, least compression (for frequently updated data)
```

#### 8. Monitor and Alert

```typescript
// Track R2 usage and costs
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const startTime = Date.now()
    let bytesRead = 0
    let requestCount = 0

    const storage = new R2Backend(env.BUCKET)
    const trackedStorage = new Proxy(storage, {
      get(target, prop) {
        if (prop === 'read' || prop === 'readRange') {
          return async (...args: any[]) => {
            requestCount++
            const result = await (target as any)[prop](...args)
            bytesRead += result.length
            return result
          }
        }
        return (target as any)[prop]
      }
    })

    const db = new ParqueDB({ storage: trackedStorage })
    const response = await handleQuery(db, request)

    // Log metrics
    console.log(JSON.stringify({
      duration: Date.now() - startTime,
      bytesRead,
      requestCount,
      estimatedCost: (requestCount * 0.00036 / 1000) + (bytesRead * 0.015 / (1024 ** 3))
    }))

    return response
  }
}
```

#### Cost Optimization Checklist

- [ ] Files between 50-200MB for optimal balance
- [ ] Caching enabled with appropriate TTLs
- [ ] Projection used to read only needed columns
- [ ] Bloom filters enabled for ID lookups
- [ ] Batch operations instead of loops
- [ ] Data partitioned by common filters
- [ ] Compression codec optimized for use case
- [ ] Monitoring and alerting in place
- [ ] Old data archived or deleted
- [ ] Public data using CDN caching

---

## Cost Estimation

### R2 Pricing (as of 2024)

| Resource | Free Tier | Cost Beyond Free Tier |
|----------|-----------|----------------------|
| Storage | 10 GB/month | $0.015/GB/month |
| Class A Operations (writes) | 1M/month | $4.50/million |
| Class B Operations (reads) | 10M/month | $0.36/million |
| Egress | Unlimited | Free |

### Storage Costs by Dataset Size

| Dataset Size | Monthly Storage Cost |
|--------------|---------------------|
| 1 GB | Free |
| 10 GB | Free |
| 50 GB | $0.60/month |
| 100 GB | $1.35/month |
| 500 GB | $7.35/month |
| 1 TB | $15.00/month |

### Request Costs for Typical Patterns

**Read-Heavy API (1M queries/day):**
```
Reads: 30M/month x $0.36/million = $10.80/month
Writes: 100K/month (updates) x $4.50/million = $0.45/month
Total: ~$11.25/month (assuming beyond free tier)
```

**Analytics Dashboard (100K queries/day):**
```
Reads: 3M/month = Free (within 10M limit)
Storage: 50GB = $0.60/month
Total: ~$0.60/month
```

**High-Traffic API (10M queries/day):**
```
Reads: 300M/month x $0.36/million = $108/month
Writes: 1M/month x $4.50/million = $4.50/month
Storage: 100GB = $1.35/month
Total: ~$114/month
```

### Comparison to Other Solutions

| Solution | 100GB Storage | 1M Reads/day | Notes |
|----------|---------------|--------------|-------|
| **R2 + ParqueDB** | $1.35/mo | $10.80/mo | Zero egress, columnar efficiency |
| AWS S3 + Athena | $2.30/mo + $5/TB scanned | Variable | High query costs for large scans |
| BigQuery | Storage: $2/mo | $5/TB scanned | Good for large analytical queries |
| Planetscale | $29/mo (starter) | Included | Full SQL, higher base cost |
| Supabase | $25/mo (pro) | Included | Full Postgres, higher base cost |

**ParqueDB + R2 Advantages:**
- Zero egress fees (significant for high-traffic APIs)
- Columnar format reduces bytes scanned
- Edge-cached queries via Cloudflare network
- Pay only for actual usage

---

## Pre-loaded Public Datasets

ParqueDB maintains pre-loaded public datasets available for immediate use.

### Available Public Datasets

| Dataset | Size | Description | Bucket |
|---------|------|-------------|--------|
| O*NET 29.0 | ~50MB | Occupational data (jobs, skills, wages) | `parquedb-public` |
| IMDB Basics | ~500MB | Movies, TV shows, ratings | `parquedb-public` |
| World Cities | ~10MB | Global city data with populations | `parquedb-public` |

### Accessing Public Datasets

Public datasets are available via HTTP:

```typescript
// Fetch from public R2 bucket
const response = await fetch(
  'https://parquedb-public.r2.dev/onet/occupations/data.parquet'
)
const data = new Uint8Array(await response.arrayBuffer())
```

Or reference directly in your Worker:

```typescript
// wrangler.toml - Reference public bucket (read-only)
[[r2_buckets]]
binding = "PUBLIC_DATA"
bucket_name = "parquedb-public"
jurisdiction = "default"
```

### Requesting Access to Pre-loaded Data

To request access to private pre-loaded datasets or to add your organization's data to the public collection:

1. **Open an Issue:** Visit the [ParqueDB GitHub repository](https://github.com/parquedb/parquedb/issues)
2. **Contact:** Email datasets@parquedb.dev with:
   - Dataset description
   - Intended use case
   - Organization (if applicable)
3. **Community Datasets:** Submit a PR to add public datasets to the registry

### Loading Your Own Copy

To load public datasets into your own R2 bucket:

```bash
# Download and upload O*NET data
curl -O https://parquedb-public.r2.dev/onet/occupations/data.parquet
npx wrangler r2 object put your-bucket/onet/occupations/data.parquet \
  --file occupations/data.parquet

# Or use the loader script
npx parquedb load \
  --source https://parquedb-public.r2.dev/onet/ \
  --bucket your-bucket \
  --prefix onet/
```

---

## Next Steps

- [Getting Started Guide](../getting-started.md) - Basic ParqueDB usage
- [Schema Definition](../schema.md) - Define your data schema
- [Cloudflare Workers Guide](../workers.md) - Full Workers integration
- [Architecture Overview](../architecture/) - Understanding ParqueDB internals
