# Search Client API

ParqueDB provides a typesafe, tree-shakable search client for querying pre-indexed datasets.

## Installation

```typescript
// From main package
import { search, createSearchClient } from 'parquedb'

// Tree-shakable import (recommended for smaller bundles)
import { search, createSearchClient } from 'parquedb/search'
```

## Quick Start

```typescript
import { search } from 'parquedb'

// Full-text search
const { data } = await search.imdb('matrix', { type: 'movie' })

// With filters
const movies = await search.imdb('love', {
  type: 'movie',
  year_gte: 2000,
  limit: 20
})

// Autocomplete
const { suggestions } = await search.imdb.suggest('mat')

// Vector similarity search
const similar = await search.imdb.vector(embedding, { limit: 10 })

// Hybrid search (FTS + vector)
const hybrid = await search.imdb.hybrid('romantic comedy', embedding, {
  fts_weight: 0.7
})
```

## Built-in Datasets

The default `search` client includes three pre-indexed datasets:

| Dataset | Description | Example |
|---------|-------------|---------|
| `imdb` | IMDB titles (movies, TV shows) | `search.imdb('matrix')` |
| `onet` | O*NET occupations | `search.onet('software engineer')` |
| `unspsc` | UNSPSC product codes | `search.unspsc('office supplies')` |

## API Reference

### DatasetClient

Each dataset exposes the following methods:

#### Full-text Search

```typescript
// With query
const results = await search.imdb('search term', options?)

// Browse mode (no query)
const results = await search.imdb({ type: 'movie', limit: 50 })
```

#### Autocomplete

```typescript
const { suggestions, query } = await search.imdb.suggest('mat')
// suggestions: ['matrix', 'matter', 'matthew', ...]
```

#### Vector Search

```typescript
const results = await search.imdb.vector(embedding, {
  limit: 10,
  // other SearchOptions
})
```

#### Hybrid Search

```typescript
const results = await search.imdb.hybrid('love story', embedding, {
  fts_weight: 0.7,  // 0-1, weight for FTS vs vector (default: 0.5)
  limit: 20
})
```

### SearchOptions

```typescript
interface SearchOptions {
  limit?: number      // Max results (default: 20, max: 50)
  offset?: number     // Pagination offset
  sort?: string       // Sort field: "field:asc" or "field:desc"
  facets?: string[]   // Fields to compute facets for
  stats?: string[]    // Fields to compute stats for
  timing?: boolean    // Include timing breakdown
  highlight?: boolean // Enable result highlighting
}
```

### SearchResult

```typescript
interface SearchResult<T> {
  data: T[]                           // Matching documents
  total: number                       // Total matches
  limit: number                       // Applied limit
  offset: number                      // Applied offset
  facets?: Record<string, Facet[]>   // Facet counts
  stats?: Record<string, Stats>      // Field statistics
  timing?: Record<string, number>    // Performance breakdown
  didYouMean?: string                // Spelling suggestion
}
```

## IMDB Filters

```typescript
interface IMDBFilters {
  type?: string | string[]  // 'movie', 'tvSeries', 'short', etc.
  year_gte?: number         // Minimum year
  year_lte?: number         // Maximum year
  runtime_gte?: number      // Minimum runtime (minutes)
  runtime_lte?: number      // Maximum runtime (minutes)
}

// Example
const action = await search.imdb('action', {
  type: ['movie', 'tvSeries'],
  year_gte: 2020,
  year_lte: 2024,
  limit: 10
})
```

## Custom Datasets

Create a typed client for your own search endpoints:

```typescript
import { createSearchClient } from 'parquedb/search'

// Define your document and filter types
interface Product {
  id: string
  name: string
  price: number
  category: string
}

interface ProductFilters {
  category?: string | string[]
  price_gte?: number
  price_lte?: number
}

// Create typed client
const search = createSearchClient<{
  products: [Product, ProductFilters]
  categories: [Category, CategoryFilters]
}>({
  baseUrl: 'https://api.example.com/search'
})

// Use with full type safety
const { data } = await search.products('laptop', {
  category: 'electronics',
  price_lte: 1000
})
// data is typed as Product[]
```

## Standalone Functions

For maximum tree-shaking, use standalone functions:

```typescript
import { query, suggest, vectorSearch, hybridSearch } from 'parquedb/search'

// One-shot search
const results = await query('imdb', 'matrix', { type: 'movie' })

// One-shot suggest
const { suggestions } = await suggest('imdb', 'mat')

// One-shot vector search
const similar = await vectorSearch('imdb', embedding, { limit: 10 })

// One-shot hybrid search
const hybrid = await hybridSearch('imdb', 'love', embedding, {
  fts_weight: 0.7
})
```

## Configuration

### Custom Base URL

```typescript
const search = createSearchClient({
  baseUrl: 'https://your-domain.com/search'
})
```

### Default Base URL

The default client points to:
```
https://cdn.workers.do/search-v10
```

## Response Types

### IMDBTitle

```typescript
interface IMDBTitle {
  tconst: string           // IMDB ID (e.g., "tt0133093")
  titleType: string        // "movie", "tvSeries", etc.
  primaryTitle: string     // Display title
  originalTitle: string    // Original title
  isAdult: boolean
  startYear: number | null
  endYear: number | null
  runtimeMinutes: number | null
  genres: string[]
}
```

### ONETOccupation

```typescript
interface ONETOccupation {
  code: string        // O*NET code (e.g., "15-1252.00")
  title: string       // Job title
  description: string // Job description
}
```

### UNSPSCCode

```typescript
interface UNSPSCCode {
  commodityCode: string
  commodityTitle: string
  classCode: string
  classTitle: string
  familyCode: string
  familyTitle: string
  segmentCode: string
  segmentTitle: string
}
```

## Error Handling

```typescript
try {
  const results = await search.imdb('matrix')
} catch (error) {
  if (error.message.includes('Search error: 429')) {
    // Rate limited
  } else if (error.message.includes('Search error: 500')) {
    // Server error
  }
}
```

## Performance

The search API is optimized for Cloudflare Workers:

- **CPU time**: <1ms per query (async I/O doesn't count)
- **Bundle size**: ~5KB gzipped
- **Latency**: ~50-200ms (mostly network I/O)

See [Search Worker Architecture](../architecture/SEARCH_WORKER.md) for details.
