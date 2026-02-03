# Using ParqueDB in the Browser

ParqueDB can query Parquet files directly in the browser using HTTP range requests. This enables:
- Querying large datasets without downloading entire files
- Real-time analytics dashboards
- Offline-capable data exploration
- Direct access to CDN-cached Parquet files

## Quick Start

### Via CDN (UMD)

```html
<script src="https://unpkg.com/parquedb/dist/browser.min.js"></script>
<script>
  const { openRemoteDB, RemoteBackend } = ParqueDB

  async function queryData() {
    // Open a public database
    const db = await openRemoteDB('parquedb-benchmarks', {
      baseUrl: 'https://cdn.workers.do'
    })

    // Query products
    const products = await db.collection('native').find({
      category: 'electronics'
    })

    console.log(products)
  }

  queryData()
</script>
```

### Via ES Modules

```html
<script type="module">
  import { openRemoteDB, RemoteBackend } from 'https://esm.sh/parquedb'

  const db = await openRemoteDB('parquedb-benchmarks', {
    baseUrl: 'https://cdn.workers.do'
  })

  const results = await db.Products.find({ price: { $lt: 100 } })
</script>
```

### Via npm (Bundled)

```bash
npm install parquedb
```

```typescript
import { openRemoteDB, RemoteBackend } from 'parquedb'

const db = await openRemoteDB('parquedb-benchmarks', {
  baseUrl: 'https://cdn.workers.do'
})

const products = await db.Products.find({ category: 'electronics' })
```

## Low-Level: RemoteBackend

For more control, use `RemoteBackend` directly:

```typescript
import { RemoteBackend } from 'parquedb/storage'
import { ParquetReader } from 'parquedb/parquet'

// Create storage backend pointing to CDN
const storage = new RemoteBackend({
  baseUrl: 'https://cdn.workers.do/parquedb-benchmarks',
  timeout: 30000, // 30 second timeout
})

// Read Parquet file metadata
const reader = new ParquetReader({ storage })
const metadata = await reader.readMetadata('native/data.parquet')

console.log('Row count:', metadata.numRows)
console.log('Columns:', metadata.schema.map(f => f.name))

// Query with filtering
const rows = await reader.read('native/data.parquet', {
  columns: ['$id', 'name', 'price'],
  filter: { column: 'category', op: 'eq', value: 'electronics' },
  limit: 100,
})
```

## Direct Parquet Queries

Query any Parquet file on the web:

```typescript
import { parquetQuery } from 'parquedb/parquet'

// Create a file-like object for HTTP range requests
function createHttpFile(url: string) {
  let fileSize: number | null = null

  return {
    get byteLength() {
      return fileSize ?? 0
    },
    async slice(start: number, end: number): Promise<ArrayBuffer> {
      const response = await fetch(url, {
        headers: { Range: `bytes=${start}-${end - 1}` }
      })

      // Cache file size from Content-Range header
      if (fileSize === null) {
        const range = response.headers.get('Content-Range')
        if (range) {
          fileSize = parseInt(range.split('/')[1])
        }
      }

      return response.arrayBuffer()
    }
  }
}

// Query any Parquet file
const file = createHttpFile('https://cdn.workers.do/parquedb-benchmarks/native/data.parquet')

// First, get file size
const head = await fetch(file.url, { method: 'HEAD' })
file.byteLength = parseInt(head.headers.get('Content-Length') || '0')

const rows = await parquetQuery({
  file,
  columns: ['name', 'category', 'price'],
  filter: { category: 'electronics' },
})

console.log(rows)
```

## Accessing CDN-Cached Data

ParqueDB benchmark data is stored on Cloudflare's CDN:

```
Base URL: https://cdn.workers.do/parquedb-benchmarks/
```

### Available Datasets

| Path | Format | Description |
|------|--------|-------------|
| `native/data.parquet` | Raw Parquet | Benchmark products |
| `iceberg/` | Apache Iceberg | Iceberg table format |
| `delta/` | Delta Lake | Delta table format |

### Example: Reading Iceberg Table

```typescript
import { RemoteBackend } from 'parquedb/storage'

const storage = new RemoteBackend({
  baseUrl: 'https://cdn.workers.do/parquedb-benchmarks/iceberg'
})

// 1. Read table metadata
const metadataJson = await storage.read('metadata/v1.metadata.json')
const metadata = JSON.parse(new TextDecoder().decode(metadataJson))

// 2. Get current snapshot
const snapshot = metadata.snapshots.find(
  s => s.snapshotId === metadata.currentSnapshotId
)

// 3. Read manifest list
const manifestListJson = await storage.read(snapshot.manifestList)
const manifestList = JSON.parse(new TextDecoder().decode(manifestListJson))

// 4. Read manifest
const manifestJson = await storage.read(manifestList.manifests[0].path)
const manifest = JSON.parse(new TextDecoder().decode(manifestJson))

// 5. Query data file
const dataPath = manifest.entries[0].dataFile.filePath
// ... use ParquetReader to query
```

## Authentication

For private databases, pass an auth token:

```typescript
const db = await openRemoteDB('username/private-dataset', {
  baseUrl: 'https://parquedb.workers.do',
  token: 'your-api-token',
})
```

Or with RemoteBackend:

```typescript
const storage = new RemoteBackend({
  baseUrl: 'https://parquedb.workers.do/db/username/private-dataset',
  token: 'your-api-token',
  headers: {
    'X-Custom-Header': 'value',
  },
})
```

## Performance Tips

### 1. Use Column Projection

Only request the columns you need:

```typescript
const rows = await reader.read('data.parquet', {
  columns: ['id', 'name'], // Only these columns
})
```

### 2. Use Predicate Pushdown

Filter at the Parquet level to skip row groups:

```typescript
const rows = await reader.read('data.parquet', {
  filter: { column: 'year', op: 'gte', value: 2020 },
})
```

### 3. Limit Results

Don't fetch more than you need:

```typescript
const rows = await reader.read('data.parquet', {
  limit: 100,
})
```

### 4. Use CDN URLs

Always use CDN URLs for better caching:
- `https://cdn.workers.do/...` (CDN-cached)
- Not `https://parquedb.workers.do/...` (origin)

### 5. Handle Large Files

For files > 100MB, consider streaming:

```typescript
for await (const row of reader.stream('large-file.parquet')) {
  // Process row by row
  if (shouldStop) break
}
```

## Browser Compatibility

| Browser | Supported | Notes |
|---------|-----------|-------|
| Chrome 80+ | ✅ | Full support |
| Firefox 75+ | ✅ | Full support |
| Safari 14+ | ✅ | Full support |
| Edge 80+ | ✅ | Full support |
| IE 11 | ❌ | Not supported |

Required browser features:
- `fetch` with Range headers
- `ArrayBuffer`
- `TextDecoder`
- ES2020 (async/await, BigInt)

## Error Handling

```typescript
import { NetworkError, NotFoundError } from 'parquedb/storage'

try {
  const data = await storage.read('missing-file.parquet')
} catch (error) {
  if (error instanceof NotFoundError) {
    console.log('File not found:', error.path)
  } else if (error instanceof NetworkError) {
    console.log('Network error:', error.message)
    // Retry logic here
  } else {
    throw error
  }
}
```

## React Example

```tsx
import { useEffect, useState } from 'react'
import { openRemoteDB } from 'parquedb'

function ProductList() {
  const [products, setProducts] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function loadProducts() {
      const db = await openRemoteDB('parquedb-benchmarks', {
        baseUrl: 'https://cdn.workers.do'
      })

      const results = await db.collection('native').find({
        category: 'electronics'
      }, {
        limit: 20,
        sort: { price: -1 }
      })

      setProducts(results)
      setLoading(false)
    }

    loadProducts()
  }, [])

  if (loading) return <div>Loading...</div>

  return (
    <ul>
      {products.map(p => (
        <li key={p.$id}>{p.name} - ${p.price}</li>
      ))}
    </ul>
  )
}
```

## Vue Example

```vue
<template>
  <div>
    <div v-if="loading">Loading...</div>
    <ul v-else>
      <li v-for="product in products" :key="product.$id">
        {{ product.name }} - ${{ product.price }}
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { openRemoteDB } from 'parquedb'

const products = ref([])
const loading = ref(true)

onMounted(async () => {
  const db = await openRemoteDB('parquedb-benchmarks', {
    baseUrl: 'https://cdn.workers.do'
  })

  products.value = await db.collection('native').find({
    category: 'electronics'
  })

  loading.value = false
})
</script>
```
