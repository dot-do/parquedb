# Deploying ParqueDB to Cloudflare R2

This guide covers deploying ParqueDB data to Cloudflare R2 object storage for production use with Cloudflare Workers.

## Table of Contents

- [R2 Bucket Setup](#r2-bucket-setup)
- [Loading Data to R2](#loading-data-to-r2)
- [Data Layout in R2](#data-layout-in-r2)
- [Querying from Workers](#querying-from-workers)
- [Cost Estimation](#cost-estimation)
- [Pre-loaded Public Datasets](#pre-loaded-public-datasets)

---

## R2 Bucket Setup

### Creating a Bucket via Wrangler

The recommended way to create R2 buckets is using Wrangler CLI:

```bash
# Create a production bucket
npx wrangler r2 bucket create parquedb

# Create a preview bucket for development
npx wrangler r2 bucket create parquedb-preview
```

### Creating a Bucket via Dashboard

1. Log in to the [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Navigate to **R2 Object Storage** in the sidebar
3. Click **Create bucket**
4. Enter a bucket name (e.g., `parquedb`)
5. Select your preferred location hint (optional)
6. Click **Create bucket**

### Configuring wrangler.toml Bindings

Add R2 bucket bindings to your `wrangler.toml`:

```toml
# =============================================================================
# R2 Storage
# =============================================================================

# R2 bucket for Parquet file storage
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "parquedb"
preview_bucket_name = "parquedb-preview"  # Used during `wrangler dev`

# Optional: Multiple buckets for different datasets
[[r2_buckets]]
binding = "DATASETS_BUCKET"
bucket_name = "parquedb-datasets"
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
