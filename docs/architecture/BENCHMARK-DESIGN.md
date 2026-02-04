# ParqueDB Benchmark Design

## Overview

This document defines the comprehensive benchmark framework for ParqueDB, covering real-world query patterns across multiple datasets, storage backends, and runtime environments.

## Design Principles

1. **Measure real parquet performance** - No fake in-memory caches hiding actual I/O
2. **Use public CDN** - `cdn.workers.do` with fetch + range headers is the primary path
3. **Real-world query patterns** - Patterns users actually execute, not arbitrary limits
4. **Cover the matrix** - Storage × Runtime × Dataset combinations

---

## Datasets

### Production Datasets (Web-only for large)

| Dataset | Size | Collections | Primary Use Case |
|---------|------|-------------|------------------|
| **IMDB** | 100K-1M titles | titles, people, cast | Entertainment discovery |
| **O*NET** | 1K occupations | occupations, skills, abilities, knowledge | Career exploration |
| **UNSPSC** | 70K commodities | segments, families, classes, commodities | Product taxonomy |
| **Wikidata** | TBD | items, properties, claims | Knowledge graph |
| **Wiktionary** | TBD | words, definitions, translations | Dictionary/linguistics |

### Example Datasets (Local FS compatible)

| Dataset | Size | Collections | Primary Use Case |
|---------|------|-------------|------------------|
| **Blog** | 5K posts | posts, authors, comments, tags | Content management |
| **E-commerce** | 50K products | products, categories, orders, customers | Online retail |

---

## Query Pattern Categories

### 1. Point Lookups (Target: <10ms)
- Single entity by ID
- Critical for: entity pages, API endpoints

### 2. Filtered Lists (Target: 20-100ms)
- Equality filters: `{ titleType: 'movie' }`
- Range filters: `{ price: { $gte: 50, $lte: 200 } }`
- Compound filters: Multiple conditions

### 3. Relationship Traversal (Target: 50-150ms)
- One-hop: Movie → Cast, Occupation → Skills
- Reverse: Skills → Occupations requiring it
- Multi-hop: Actor → Movies → Other Actors

### 4. Full-Text Search (Target: 30-100ms)
- Title/name search
- Autocomplete/prefix search
- Scoped search (FTS + filter)

### 5. Aggregations (Target: 100ms-1s)
- Count by category
- Average rating by type
- Distribution queries

### 6. Bulk Operations (Target: varies)
- Batch validation (100-500 IDs)
- Pagination (cursor-based)

---

## Storage × Runtime Matrix

### Storage Backends

| Backend | Description | Range Reads | Best For |
|---------|-------------|-------------|----------|
| **Public CDN** | `cdn.workers.do/parquedb/*` | Via fetch() | Browser, Workers |
| **R2 API** | Direct R2 bucket access | Via R2 API | Workers (private data) |
| **Local FS** | Node.js file system | Native | Development, CLI |
| **Iceberg** | Apache Iceberg format | Via metadata | Time-travel, schema evolution |
| **Delta** | Delta Lake format | Via transaction log | ACID transactions |

### Runtime Environments

| Runtime | Storage Options | Notes |
|---------|-----------------|-------|
| **Browser** | Public CDN only | fetch() with Range headers |
| **Worker** | Public CDN, R2 API, Iceberg, Delta | Full access |
| **Node.js** | Local FS, Public CDN, R2 (via API) | Development + CLI |

### Valid Combinations

```
Browser:
  └── Public CDN (fetch + range)

Worker:
  ├── Public CDN (fetch + range) [preferred for reads]
  ├── R2 API [for private data, writes]
  ├── Iceberg [time-travel]
  └── Delta [transactions]

Node.js:
  ├── Local FS [small datasets only]
  ├── Public CDN (fetch + range)
  └── R2 API (via wrangler/SDK)
```

---

## Dataset-Specific Query Patterns

### IMDB (15 patterns)

| # | Pattern | Category | Target |
|---|---------|----------|--------|
| 1 | Title by ID (tt0111161) | Point lookup | 5ms |
| 2 | Person by ID (nm0000138) | Point lookup | 5ms |
| 3 | Top-rated movies (rating≥8, votes≥100K) | Filtered + sorted | 50ms |
| 4 | Movies by year range (2010-2019) | Range filter | 100ms |
| 5 | Genre filter (Action + rating≥7) | Compound | 50ms |
| 6 | Filmography (person → titles) | Relationship | 100ms |
| 7 | Cast of movie (title → people) | Relationship | 50ms |
| 8 | Title search ("Shawshank") | FTS | 30ms |
| 9 | Autocomplete ("The God") | Prefix search | 20ms |
| 10 | TV series episode count | Aggregation | 500ms |
| 11 | Multi-genre filter (Action AND Sci-Fi) | Compound | 100ms |
| 12 | People by profession (directors) | Filtered list | 100ms |
| 13 | Recent high-rated releases | Compound | 50ms |
| 14 | Related movies (same cast) | Multi-hop | 200ms |
| 15 | Count by title type | Aggregation | 500ms |

### O*NET (15 patterns)

| # | Pattern | Category | Target |
|---|---------|----------|--------|
| 1 | Occupation by SOC code | Point lookup | 5ms |
| 2 | Job Zone = 4 (Bachelor's) | Equality | 20ms |
| 3 | Job Zone ≤ 2 (entry-level) | Range | 20ms |
| 4 | SOC prefix 15-* (Computer) | Range | 50ms |
| 5 | Skills for occupation | Relationship | 50ms |
| 6 | High importance skills (≥4.0) | Range | 100ms |
| 7 | Occupations requiring skill X | Reverse lookup | 100ms |
| 8 | Skill + level compound | Compound | 50ms |
| 9 | Skill gap (2 occupations) | Parallel queries | 100ms |
| 10 | Core tasks for occupation | Filtered | 30ms |
| 11 | Hot technologies | Equality | 50ms |
| 12 | Title search ("data scientist") | FTS | 30ms |
| 13 | Count by job zone | Aggregation | 100ms |
| 14 | Average skill importance | Aggregation | 200ms |
| 15 | Tech stack with UNSPSC | Multi-hop | 150ms |

### UNSPSC (15 patterns)

| # | Pattern | Category | Target |
|---|---------|----------|--------|
| 1 | Exact code lookup (43101501) | Point lookup | 5ms |
| 2 | Segment filter (43 = IT) | Equality | 50ms |
| 3 | Family drill-down (4310) | Equality | 20ms |
| 4 | Class filter (431015) | Equality | 20ms |
| 5 | Code prefix search (4310*) | Prefix | 50ms |
| 6 | Multi-segment (43, 44) | $in query | 100ms |
| 7 | Text search ("laptop") | FTS | 50ms |
| 8 | Breadcrumb (4 parallel) | Parallel point | 20ms |
| 9 | Sibling commodities | Filtered | 20ms |
| 10 | Bulk validation (500 codes) | Batch $in | 300ms |
| 11 | FTS + hierarchy scope | Compound | 100ms |
| 12 | Deprecated codes in segment | Compound | 30ms |
| 13 | Hierarchy export (segment) | Large result | 500ms |
| 14 | Segment distribution | Aggregation | 200ms |
| 15 | Code range (43000000-44000000) | Range | 100ms |

### Blog (10 patterns)

| # | Pattern | Category | Target |
|---|---------|----------|--------|
| 1 | Published posts (paginated) | Filtered + sorted | 20ms |
| 2 | Posts by author | Filtered | 20ms |
| 3 | Posts by tag | Array filter | 30ms |
| 4 | Full-text search | FTS | 50ms |
| 5 | Recent posts (last week) | Range | 20ms |
| 6 | Popular posts (by views) | Top-N | 20ms |
| 7 | Single post + author | Point + join | 10ms |
| 8 | Posts with comment counts | Aggregation | 100ms |
| 9 | Draft posts for author | Compound | 20ms |
| 10 | Posts updated since | Range | 20ms |

### E-commerce (10 patterns)

| # | Pattern | Category | Target |
|---|---------|----------|--------|
| 1 | Products by category (hierarchy) | Hierarchy | 50ms |
| 2 | Price range filter | Range | 30ms |
| 3 | In-stock items only | Equality | 20ms |
| 4 | Multi-facet filter | Compound | 50ms |
| 5 | Product search | FTS | 50ms |
| 6 | Customer order history | Filtered | 20ms |
| 7 | Low stock inventory | Range | 30ms |
| 8 | Best sellers by category | Top-N | 30ms |
| 9 | Related products | Compound | 30ms |
| 10 | Order analytics (date range) | Aggregation | 200ms |

---

## Benchmark Implementation

### Measurement Methodology

```typescript
interface BenchmarkResult {
  pattern: string
  dataset: string
  storage: 'cdn' | 'r2' | 'fs' | 'iceberg' | 'delta'
  runtime: 'browser' | 'worker' | 'node'

  // Timing
  latencyMs: {
    p50: number
    p95: number
    p99: number
    min: number
    max: number
  }

  // I/O
  bytesRead: number
  rangeRequests: number
  rowGroupsScanned: number
  rowGroupsSkipped: number

  // Results
  rowsReturned: number

  // Metadata
  iterations: number
  warmupIterations: number
  timestamp: string
}
```

### Execution Flow

1. **Warmup phase**: 2-3 iterations to populate CDN cache
2. **Measurement phase**: 10+ iterations for statistical significance
3. **Record raw timings**: Not averaged, capture distribution
4. **Capture I/O metrics**: Bytes read, range requests, row groups

### What NOT to Cache

- Parsed parquet data (dataCache) - **REMOVE**
- Entity JSON responses - **REMOVE**
- Query results - **REMOVE**

### What CAN be Cached

- Parquet metadata (footer) - in-memory, optional optimization
- Row group statistics - in-memory, optional optimization
- The parquet file itself - **Cloudflare CDN handles this**

---

## Implementation Plan

### Phase 1: Clean Up Wrong Caching
- [ ] Remove `dataCache` (LRU parsed data cache)
- [ ] Remove entity response caching
- [ ] Remove `fileCache` whole-file caching
- [ ] Verify queries go through real parquet path

### Phase 2: Query Pattern Implementation
- [ ] IMDB patterns (15)
- [ ] O*NET patterns (15)
- [ ] UNSPSC patterns (15)
- [ ] Blog patterns (10)
- [ ] E-commerce patterns (10)

### Phase 3: Storage Backend Testing
- [ ] Public CDN (fetch + range)
- [ ] R2 API direct
- [ ] Local FS (Node only, small datasets)
- [ ] Iceberg format
- [ ] Delta format

### Phase 4: Runtime Coverage
- [ ] Browser (CDN only)
- [ ] Worker (all backends)
- [ ] Node.js (FS + CDN)

### Phase 5: Reporting & CI
- [ ] Generate benchmark reports
- [ ] Regression detection
- [ ] Performance tracking over time

---

## Files to Modify/Create

### Remove
- `src/worker/QueryExecutor.ts` - Remove `dataCache`, `metadataCache`
- `src/worker/handlers/entity.ts` - Already removed response caching

### Create
- `tests/benchmarks/patterns/imdb.ts`
- `tests/benchmarks/patterns/onet.ts`
- `tests/benchmarks/patterns/unspsc.ts`
- `tests/benchmarks/patterns/blog.ts`
- `tests/benchmarks/patterns/ecommerce.ts`
- `tests/benchmarks/runner.ts` - Unified runner
- `tests/benchmarks/report.ts` - Report generation

### Modify
- Existing benchmark files to use new patterns
