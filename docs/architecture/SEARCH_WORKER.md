# ParqueDB Search Worker Architecture

## Overview

The ParqueDB search worker is designed to run within **Cloudflare Snippet constraints**:
- 5ms CPU time (not wall time!)
- 32KB bundle size
- 32MB RAM
- 5 subrequests

The key architectural insight is that **async I/O (R2 fetches) does not count toward the 5ms CPU limit**. This allows us to build a full-featured search engine by shifting computation from request-time to build-time.

## CPU Time vs Wall Time

```
┌─────────────────────────────────────────────────────────────┐
│                    Request Timeline                          │
├─────────────────────────────────────────────────────────────┤
│  CPU: Parse query     │  I/O: Fetch index  │  CPU: Score    │
│      (~0.1ms)         │     (~50ms)        │    (~0.5ms)    │
├─────────────────────────────────────────────────────────────┤
│  I/O: Fetch docs      │  CPU: Filter/Sort  │  CPU: Response │
│     (~100ms)          │     (~0.3ms)       │    (~0.1ms)    │
└─────────────────────────────────────────────────────────────┘

Total Wall Time: ~200ms
Total CPU Time:  ~1ms  ✓ Under 5ms limit!
```

## Pre-Indexed Architecture

### Build Time (unlimited compute)
```
Source Data (Parquet/JSON)
         │
         ▼
┌─────────────────────┐
│   build-indexes.ts  │  ← Heavy computation here
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│              R2 Bucket                       │
├─────────────────────────────────────────────┤
│  indexes/{dataset}/                          │
│    ├── meta.json         (562 bytes)        │
│    ├── inverted.json     (294 KB full)      │
│    ├── terms/                                │
│    │   ├── a.json        (~17 KB)           │
│    │   ├── b.json        (~17 KB)           │
│    │   └── ...           (sharded by letter)│
│    ├── hash-{field}.json (48-184 KB)        │
│    └── docs-{N}.json     (~100 KB per shard)│
└─────────────────────────────────────────────┘
```

### Request Time (5ms CPU budget)
```
Query: "matrix"
    │
    ▼
┌─────────────────────────────────────────────┐
│  1. Parse query (CPU: ~0.1ms)               │
│     terms = ["matrix"]                       │
│     letter = "m"                             │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│  2. Fetch term shard (I/O: ~50ms, CPU: 0)   │
│     GET indexes/imdb/terms/m.json           │
│     → {"matrix": [1,5,99], "man": [...]}    │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│  3. Score documents (CPU: ~0.5ms)           │
│     indices = [1, 5, 99]                    │
│     scores = {1: 10, 5: 10, 99: 10}         │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│  4. Fetch doc shards (I/O: ~100ms, CPU: 0)  │
│     GET indexes/imdb/docs-0.json            │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│  5. Filter/Sort/Highlight (CPU: ~0.3ms)     │
│     Apply range filters, sort, highlight    │
└─────────────────────────────────────────────┘
    │
    ▼
Response JSON
```

## Index Structures

### Inverted Index (Full-Text Search)
```json
{
  "matrix": [1, 5, 99, 234, 567],
  "reloaded": [1],
  "revolutions": [5],
  "love": [12, 45, 78, 89, 123, ...]
}
```
Term → list of document indices where term appears.

### Term Shards (Memory Optimization)
Instead of loading the full 294KB inverted index, we shard by first letter:
- `terms/m.json` contains all terms starting with 'm' (~15KB)
- Query "matrix" only loads the 'm' shard
- Reduces JSON.parse CPU time significantly

### Hash Indexes (Exact Filters)
```json
// hash-titleType.json
{
  "movie": [0, 1, 5, 10, ...],
  "tvSeries": [2, 3, 4, ...],
  "short": [6, 7, ...]
}
```
Field value → list of document indices. Used for `?type=movie` filters.

### Document Shards (Pagination)
```
docs-0.json: documents 0-499
docs-1.json: documents 500-999
docs-2.json: documents 1000-1499
...
```
500 documents per shard (~100KB each). Only fetch shards needed for current page.

## CPU Budget Guards (v9)

For larger datasets, v9 adds CPU protection:

```typescript
// Limits to prevent CPU blowout
const MAX_FUZZY_CHECKS = 100     // Cap Levenshtein comparisons
const MAX_SPELL_CHECKS = 50      // Cap spell correction iterations
const MAX_PREFIX_MATCHES = 10    // Cap prefix expansion per term
const MAX_FACET_DOCS = 200       // Cap docs for aggregations
const MAX_QUERY_TERMS = 5        // Cap query complexity
const MAX_DOC_SHARDS = 3         // Cap concurrent shard fetches
```

### Fuzzy Matching Optimization
```typescript
// BEFORE: O(n) for all terms
for (const term of indexTerms) {
  if (levenshtein(query, term) === 1) ...
}

// AFTER: Early termination + length filtering
let checks = 0
for (const term of indexTerms) {
  if (checks++ >= MAX_FUZZY_CHECKS) break
  if (Math.abs(term.length - query.length) > 1) continue
  if (levenshteinFast(query, term, 1) === 1) ...
}
```

### Levenshtein with Early Termination
```typescript
function levenshteinFast(a: string, b: string, maxDist: number): number {
  // Skip if length difference exceeds max distance
  if (Math.abs(a.length - b.length) > maxDist) return maxDist + 1

  // Track minimum in each row for early termination
  for (let i = 1; i <= m; i++) {
    let minInRow = Infinity
    for (let j = 1; j <= n; j++) {
      curr[j] = Math.min(...)
      minInRow = Math.min(minInRow, curr[j])
    }
    // If minimum exceeds maxDist, no point continuing
    if (minInRow > maxDist) return maxDist + 1
  }
}
```

## Version Comparison

| Version | Bundle | Features | CPU Strategy |
|---------|--------|----------|--------------|
| v2 | 3.08 KB | 4 | Basic sharding |
| v3 | 4.07 KB | 4 | +stemming, scoring |
| v4 | 4.60 KB | 8 | +filters, sorting |
| v5 | 5.66 KB | 17 | +fuzzy, synonyms, facets |
| v6 | 6.19 KB | 23 | +wildcards, autocomplete |
| v7 | 7.74 KB | 29 | +spell correct, MLT |
| v8 | 8.42 KB | 36 | +templates, sampling |
| v9 | 5.13 KB | 15 | CPU guards, monitoring |

## Scaling Considerations

### Current (10K docs)
- Term shards: ~15KB, 874 terms per shard
- CPU time: <1ms
- Safe with all features enabled

### 100K docs
- Term shards: ~150KB, ~8K terms per shard
- CPU time: ~2-3ms (estimated)
- v9 guards become important

### 1M docs
- Term shards: ~1.5MB, ~80K terms per shard
- CPU time: May exceed 5ms without optimization
- Consider:
  - BK-trees for fuzzy (O(log n) instead of O(n))
  - Pre-computed fuzzy candidates at build time
  - Bloom filters for existence checks

## Deployment

```bash
# Build indexes
cd snippets && npx tsx scripts/build-indexes.ts

# Deploy worker
wrangler deploy --config worker/wrangler-zen.toml

# Test
curl "https://cdn.workers.do/search-v9/imdb?q=matrix&timing=true"
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `/health` | Version, features, CPU budget |
| `/{dataset}?q=...` | Full-text search |
| `/suggest/{dataset}?q=...` | Autocomplete |
| `/mlt/{dataset}?id=...` | More Like This (v7+) |

## Query Parameters

| Param | Description | Example |
|-------|-------------|---------|
| `q` | Search query | `q=matrix` |
| `limit` | Results per page (max 50) | `limit=20` |
| `offset` | Pagination offset | `offset=20` |
| `type` | Filter by type | `type=movie` |
| `year_gte/lte` | Year range | `year_gte=2000` |
| `sort` | Sort field:direction | `sort=startYear:desc` |
| `facets` | Facet fields | `facets=genres,titleType` |
| `stats` | Stats fields | `stats=startYear` |
| `timing` | Show timing breakdown | `timing=true` |
| `~` | Enable fuzzy matching | `q=matrx~` |

## Caching

- In-memory caches for hot data (term shards, hash indexes, doc shards)
- LRU eviction when cache exceeds 10-15 entries
- HTTP Cache-Control: `public, max-age=3600`
- ETag support for conditional requests (v8+)
