# ParqueDB Cloudflare Snippets

Minimal data search snippets for Cloudflare edge deployment.

## Real Dataset Examples

This package includes snippets for real-world datasets:

| Snippet | Dataset | Data Size | Rows | Bundle Size | Description |
|---------|---------|-----------|------|-------------|-------------|
| onet-search | O*NET Occupations | 295KB JSON | 1,016 | 1.5KB | Search occupations by title/code |
| unspsc-lookup | UNSPSC Categories | 1.3MB JSON | 4,262 | 2.2KB | Look up product/service categories |
| imdb-search | IMDB Titles | 2MB JSON | 10,000 | 2.4KB | Search movie/TV titles |
| product-lookup | Example | - | - | 7KB | Product lookup with parquet |
| category-filter | Example | - | - | 8KB | Category filtering with parquet |

## Deploy to cdn.workers.do/search

### 1. Create Data Files

```bash
cd snippets

# Generate JSON data files from source parquet
bun scripts/create-json-data.ts
```

### 2. Upload Data to R2

```bash
# Upload JSON data files (requires valid CLOUDFLARE_API_TOKEN)
wrangler r2 object put cdn/parquedb-benchmarks/snippets/onet-occupations.json \
  --file="../data/snippets/onet-occupations.json" --remote

wrangler r2 object put cdn/parquedb-benchmarks/snippets/unspsc.json \
  --file="../data/snippets/unspsc.json" --remote

wrangler r2 object put cdn/parquedb-benchmarks/snippets/imdb-titles.json \
  --file="../data/snippets/imdb-titles.json" --remote
```

### 3. Build & Deploy Snippets

```bash
# Build all snippets
pnpm build

# Deploy to Cloudflare (requires CF_API_TOKEN and CF_ZONE_ID)
pnpm deploy
```

## API Endpoints

Once deployed, access via cdn.workers.do:

```bash
# O*NET Occupation Search
curl "https://cdn.workers.do/search/occupations?q=engineer"
curl "https://cdn.workers.do/search/occupations?code=11-1011"
curl "https://cdn.workers.do/search/occupations/11-1011.00"

# UNSPSC Category Lookup
curl "https://cdn.workers.do/search/unspsc?q=pet"
curl "https://cdn.workers.do/search/unspsc/segments"
curl "https://cdn.workers.do/search/unspsc/10111302"

# IMDB Title Search
curl "https://cdn.workers.do/search/titles?q=godfather"
curl "https://cdn.workers.do/search/titles?type=movie&minYear=2020"
curl "https://cdn.workers.do/search/titles/types"
```

## What are Cloudflare Snippets?

[Cloudflare Snippets](https://developers.cloudflare.com/rules/snippets/) are lightweight JavaScript modules that run at the edge. Constraints:

- **1MB bundle size limit** - Must be highly optimized
- **Limited CPU time** - ~5ms for simple operations
- **Limited memory** - ~128MB
- **No Durable Objects** - Stateless only

Snippets are ideal for:
- Simple lookups from pre-built data files
- Text search on small datasets
- Edge caching with static data

## Project Structure

```
snippets/
├── README.md
├── package.json
├── build.ts            # Bundle and size reporting
├── deploy.ts           # Cloudflare Snippets API deployment
├── lib/
│   ├── parquet-tiny.ts # Minimal Parquet reader (~6KB)
│   ├── filter.ts       # MongoDB-style filters (~1KB)
│   └── types.ts
├── examples/
│   ├── onet-search/    # JSON-based occupation search
│   ├── unspsc-lookup/  # JSON-based category lookup
│   ├── imdb-search/    # JSON-based title search
│   ├── product-lookup/ # Parquet with index example
│   └── category-filter/# Parquet filter example
└── scripts/
    ├── create-json-data.ts  # Generate JSON from parquet
    └── test-local.ts        # Local testing
```

## Local Development

```bash
# Install dependencies
pnpm install

# Build snippets
pnpm build

# Test locally with mock data
bun scripts/test-local.ts
```

## Findings: Snippets Limits

### Bundle Size (1MB limit)

Our JSON-based snippets are tiny:

| Snippet | Minified | ~Gzipped |
|---------|----------|----------|
| onet-search | 1.5 KB | 473 B |
| unspsc-lookup | 2.2 KB | 667 B |
| imdb-search | 2.4 KB | 736 B |

Parquet-based snippets include the ~6KB reader library.

### Data Size vs Performance

| Data Size | Parse Time | Filter Time | Status |
|-----------|------------|-------------|--------|
| 100KB JSON | ~5ms | ~1ms | Good |
| 500KB JSON | ~15ms | ~3ms | OK |
| 1MB JSON | ~30ms | ~5ms | Borderline |
| 2MB JSON | ~60ms | ~10ms | May timeout |

**Recommendation**: Keep data files under 500KB for best performance.

### What Works

- Simple text search with `.includes()`
- ID/code lookups with `.find()`
- Pagination with `.slice()`
- JSON parsing up to ~2MB
- Caching with response headers

### What Doesn't Work in Snippets

1. **Large files**: >2MB will timeout or OOM
2. **Complex Parquet**: Compressed/nested schemas
3. **Aggregations**: GROUP BY, COUNT, SUM
4. **Joins**: Multi-file queries
5. **External APIs**: No outbound fetch (except origin)

### When to Use Workers Instead

| Use Case | Snippets | Workers |
|----------|----------|---------|
| Simple ID lookup (<10K rows) | Yes | Yes |
| Text search (<10K rows) | Yes | Yes |
| Compressed Parquet | No | Yes |
| Files > 2MB | No | Yes |
| Time-travel queries | No | Yes |
| Aggregations | No | Yes |
| External API calls | No | Yes |

## Creating Your Own Snippet

### Option 1: JSON-based (Recommended)

```typescript
interface MyData {
  id: string
  name: string
}

const DATA_URL = 'https://cdn.workers.do/my-data.json'
let cache: MyData[] | null = null

async function loadData(): Promise<MyData[]> {
  if (cache) return cache
  const res = await fetch(DATA_URL)
  cache = await res.json()
  return cache
}

export default {
  async fetch(request: Request): Promise<Response> {
    const data = await loadData()
    const q = new URL(request.url).searchParams.get('q')?.toLowerCase()

    const results = q
      ? data.filter(d => d.name.toLowerCase().includes(q))
      : data

    return Response.json({ data: results.slice(0, 50) })
  }
}
```

### Option 2: Parquet-based (For smaller bundles at read time)

```typescript
import { parseFooter, readRows, arrayBufferToAsyncBuffer } from '../../lib/parquet-tiny'

const DATA_URL = 'https://cdn.workers.do/my-data.parquet'

export default {
  async fetch(request: Request): Promise<Response> {
    const res = await fetch(DATA_URL)
    const buffer = await res.arrayBuffer()
    const asyncBuffer = arrayBufferToAsyncBuffer(buffer)
    const footer = await parseFooter(asyncBuffer)
    const rows = await readRows(asyncBuffer, footer)

    return Response.json({ data: rows })
  }
}
```

**Note**: The parquet-tiny library has limitations. Use JSON for reliability.

## License

MIT - Same as ParqueDB
