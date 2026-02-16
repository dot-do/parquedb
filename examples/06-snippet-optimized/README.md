# Snippet-Optimized ParqueDB

Query massive datasets (Common Crawl, Wikidata, etc.) for **free** on Cloudflare's edge using Snippets.

## Constraints

Cloudflare Snippets have strict limits:

| Resource | Limit | Our Usage | Status |
|----------|-------|-----------|--------|
| Script size | < 32KB | ~29KB (hyparquet core) | ✅ |
| Memory | < 32MB | ~2MB per 5K rows | ✅ |
| CPU time | < 5ms | ~1-2ms per point lookup | ✅ |
| Fetch subrequests | ≤ 5 | 1 meta + 1-2 row groups | ✅ |
| Static asset size | < 25MB | Partition files accordingly | ✅ |

### Benchmark Results (5K row groups)

| Operation | Time | Memory |
|-----------|------|--------|
| Metadata only | 0.04ms | negligible |
| Full read (5K rows) | 2.19ms | ~2MB |
| Point lookup | 1.16ms | ~2MB |
| 10K rows | ~4ms | ~4MB |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Static Assets (<25MB each, FREE hosting)                   │
│                                                             │
│  data/                                                      │
│  ├── entities/                                              │
│  │   ├── by-id/           # Sorted by $id for point lookups│
│  │   │   ├── 0000.parquet # IDs starting with 0            │
│  │   │   ├── 0001.parquet # IDs starting with 1            │
│  │   │   └── ...                                           │
│  │   ├── by-type/         # Sorted by $type for type scans │
│  │   │   ├── person.parquet                                │
│  │   │   ├── org.parquet                                   │
│  │   │   └── ...                                           │
│  │   └── by-date/         # Sorted by date for time ranges │
│  │       ├── 2024-01.parquet                               │
│  │       └── ...                                           │
│  └── index/                                                 │
│      └── manifest.json    # Pre-computed routing index     │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP Range Requests
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Cloudflare Snippet (<32KB)                                 │
│                                                             │
│  1. Parse query → determine partition file                  │
│  2. Fetch Parquet footer (metadata, ~1-2KB)                │
│  3. Check row group statistics → skip irrelevant groups    │
│  4. Range-fetch only matching row group(s)                 │
│  5. Decompress + filter → return results                   │
│                                                             │
│  Total: ~0.6ms CPU, <5 fetches, <5MB memory                │
└─────────────────────────────────────────────────────────────┘
```

## Key Optimizations

### 1. Small Row Groups (10K rows)

Large row groups (100K+) require decompressing entire chunks even for single-row lookups.
With 10K row groups, a point lookup takes ~0.6ms instead of ~13ms.

### 2. Sorted Partitions

Store the same data sorted multiple ways for different query patterns:
- **by-id/**: For `get(id)` lookups
- **by-type/**: For `find({ $type: 'Person' })`
- **by-date/**: For `find({ createdAt: { $gte: '2024-01-01' } })`

### 3. Row Group Statistics

Parquet stores min/max statistics per row group. With sorted data:
- Looking for ID `abc123`? Check statistics to find the one row group containing it
- Skip 99% of data without reading it

### 4. Pre-computed Manifest

```json
{
  "partitions": {
    "by-id/0000.parquet": { "minId": "0000...", "maxId": "0fff..." },
    "by-id/0001.parquet": { "minId": "1000...", "maxId": "1fff..." }
  }
}
```

Route to the correct file before any Parquet reads.

## Usage

```typescript
// In your Cloudflare Snippet
import { snippetQuery } from './query'

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const id = url.searchParams.get('id')

    if (id) {
      const entity = await snippetQuery.get(id)
      return Response.json(entity)
    }

    return new Response('Missing id parameter', { status: 400 })
  }
}
```

## Measuring CPU Time

CPU time is only visible in Worker tail logs. Use `wrangler tail` or write logs to R2:

```typescript
// In your worker
const start = Date.now()
const cpuStart = performance.now()

const result = await query(id)

const cpuTime = performance.now() - cpuStart
const wallTime = Date.now() - start

// Log to R2 via your logging service
await logToR2({
  timestamp: new Date().toISOString(),
  cpuTimeMs: cpuTime,
  wallTimeMs: wallTime,
  query: { type: 'get', id },
  result: { found: !!result }
})
```

## Files

- `partition.ts` - Create partitioned Parquet files
- `query.ts` - Snippet-compatible query implementation
- `manifest.ts` - Build routing manifest
- `worker.ts` - Example Cloudflare Worker/Snippet
- `benchmark.ts` - Local benchmark simulating snippet constraints
