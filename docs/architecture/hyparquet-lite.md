# hyparquet-lite: Minimal Parquet for Cloudflare Snippets

## Overview

This document explores creating a minimal hyparquet implementation for Cloudflare Snippets, which have extreme constraints:
- **32KB package size limit**
- **2MB memory limit**
- **5ms execution time**
- **2-5 subrequests** (varies by plan: Pro=2, Business=3, Enterprise=5)

Current hyparquet total source: ~134KB (unminified), ~4000 lines of code.

## Status

**Research Complete with POC (2026-02-03)**

Key findings from research and proof-of-concept testing:

1. **No existing `hyparquet-lite` package exists on npm** - confirmed via npm search
2. **hyparquet v1.24.1** is the current version with zero dependencies
3. **Tree-shaking works excellently** - `sideEffects: false` in package.json
4. **Metadata-only import bundles to just 4KB gzipped** (verified via POC)
5. **Full parquetRead import bundles to 12.5KB gzipped** (verified via POC)
6. **Cloudflare Snippets constraints**: 32KB package size, 2MB memory, 5ms CPU

### Actual Bundle Size Measurements (POC)

| Import | Minified | Gzipped | Fits Snippets? |
|--------|----------|---------|----------------|
| `parquetMetadata, parquetSchema` only | 10.8KB | 4.0KB | YES |
| `parquetRead, parquetMetadata, parquetSchema` | 39.2KB | 12.5KB | YES |
| `parquetRead, parquetReadObjects` | 39.3KB | 12.5KB | YES |

### Key Discovery

**No separate package needed!** The existing hyparquet package with proper tree-shaking
already achieves the target bundle sizes:

- Metadata-only: **4KB gzipped** - well within 32KB Snippets limit
- Full reading: **12.5KB gzipped** - fits in Snippets with room to spare

### Recommendation

**Option B (Tree-Shaking) is now recommended** over Option C (Separate Package):

1. Use existing hyparquet with selective imports
2. Bundle with esbuild (as Cloudflare does automatically)
3. Import only `parquetMetadata` and `parquetSchema` for Snippets
4. Full hyparquet available for Workers without any changes

This avoids:
- Creating a separate package to maintain
- Code duplication
- Version sync issues

## Current hyparquet Architecture

### Module Dependency Graph

```
index.js (3KB) - Main exports
    ├── metadata.js (14KB) - Footer/metadata parsing [CORE]
    │   ├── thrift.js (5KB) - TCompactProtocol [CORE]
    │   ├── constants.js (2KB) - Type enums [CORE]
    │   ├── convert.js (6KB) - Type conversion
    │   ├── schema.js (4KB) - Schema tree [CORE]
    │   └── geoparquet.js (1.5KB) - Geo column marking
    │
    ├── read.js (7KB) - High-level reading API
    │   ├── plan.js (5KB) - Query planning
    │   │   ├── filter.js (5KB) - Row group filtering
    │   │   └── schema.js
    │   ├── rowgroup.js (8KB) - Row group reading
    │   │   ├── column.js (7KB) - Column reading
    │   │   │   ├── datapage.js (9KB) - Page decoding
    │   │   │   │   ├── encoding.js (5KB) - RLE/BitPack [CORE]
    │   │   │   │   ├── delta.js (3KB) - Delta encoding
    │   │   │   │   ├── plain.js (5KB) - Plain encoding [CORE]
    │   │   │   │   └── snappy.js (4KB) - Snappy decompression
    │   │   │   └── convert.js
    │   │   └── assemble.js (8KB) - Nested structure assembly
    │   │       └── variant.js (11KB) - Variant type support
    │   └── utils.js (9KB) - AsyncBuffer, flatten, etc.
    │
    ├── query.js (6KB) - SQL-like query interface
    └── snappy.js (4KB) - Built-in Snappy
```

### File Size Breakdown (Source)

| File | Size | Purpose | Tier |
|------|------|---------|------|
| metadata.js | 14KB | Footer parsing, schema extraction | TINY |
| variant.js | 11KB | Parquet Variant type decoding | SMALL |
| datapage.js | 9KB | Data page v1/v2 decoding | SMALL |
| utils.js | 9KB | AsyncBuffer, network helpers | SMALL |
| assemble.js | 8KB | Nested list/map/struct assembly | SMALL |
| rowgroup.js | 8KB | Row group orchestration | SMALL |
| column.js | 7KB | Column chunk reading | SMALL |
| read.js | 7KB | High-level parquetRead API | SMALL |
| convert.js | 6KB | Type conversion (dates, decimals) | SMALL |
| query.js | 6KB | SQL-like query interface | FULL |
| plain.js | 5KB | PLAIN encoding | TINY |
| encoding.js | 5KB | RLE/BitPack hybrid | TINY |
| plan.js | 5KB | Query planning | SMALL |
| filter.js | 5KB | Row group skip filtering | SMALL |
| thrift.js | 5KB | TCompact protocol | TINY |
| schema.js | 4KB | Schema tree utilities | TINY |
| snappy.js | 4KB | Snappy decompression | TINY |
| delta.js | 3KB | Delta binary packed | SMALL |
| wkb.js | 3KB | WKB geometry parsing | FULL |
| index.js | 3KB | Re-exports | - |
| constants.js | 2KB | Type enums | TINY |
| indexes.js | 2KB | Column/offset index | SMALL |
| geoparquet.js | 1.5KB | Geoparquet metadata | FULL |
| node.js | 1KB | Node.js specific | FULL |

### Compression Codec Sizes

| Codec | Package | Size | Notes |
|-------|---------|------|-------|
| Snappy | Built-in | 4KB | Most common, included by default |
| ZSTD | fzstd | 33KB | Popular for newer files |
| Brotli | hyparquet-compressors | 139KB | Rarely used in parquet |
| GZIP | hyparquet-compressors | 11KB | Moderate usage |
| LZ4 | hyparquet-compressors | 3KB | Fast but less common |

## Proposed Tiered Exports

### Tier 1: `/tiny` - Metadata Only (~8KB minified)

**Target: Cloudflare Snippets (32KB memory, 5ms CPU)**

Scope:
- Parse parquet footer and extract metadata
- Schema inspection
- Row group statistics for predicate pushdown
- No actual data reading

Files included:
- `metadata.js` (14KB) - Footer parsing
- `thrift.js` (5KB) - TCompact protocol
- `constants.js` (2KB) - Type enums
- `schema.js` (4KB) - Schema utilities (partial)

Estimated minified+gzipped: **~4-5KB**

```typescript
// hyparquet/tiny
export { parquetMetadata, parquetMetadataAsync, parquetSchema } from './metadata.js'
export { deserializeTCompactProtocol } from './thrift.js'
export type { FileMetaData, SchemaElement, RowGroup, ColumnChunk } from './types.js'
```

API surface:
```typescript
// Fetch just the footer (last 4-8KB of file)
const footer = await fetch(url, { headers: { Range: 'bytes=-8192' } })
const metadata = parquetMetadata(await footer.arrayBuffer())

// Inspect schema
const schema = parquetSchema(metadata)
console.log(schema.children.map(c => c.element.name))

// Check row count
console.log(metadata.num_rows)

// Get column statistics for predicate pushdown
const stats = metadata.row_groups[0].columns[0].meta_data.statistics
```

Use cases:
- Schema discovery
- Row count queries
- Pre-flight checks before redirecting to Worker
- Column statistics for query routing

### Tier 2: `/small` - Single Column Reading (~20KB minified)

**Target: Workers (128MB memory, 30s CPU)**

Scope:
- Everything in `/tiny`
- Read single flat columns (no nested structures)
- PLAIN and RLE_DICTIONARY encodings
- Snappy compression only
- No type conversion (raw values)

Additional files:
- `plain.js` (5KB) - Plain encoding
- `encoding.js` (5KB) - RLE/BitPack
- `snappy.js` (4KB) - Decompression
- `datapage.js` (9KB, partial) - Page header parsing
- `column.js` (7KB, partial) - Column reading

Estimated minified+gzipped: **~12-15KB**

```typescript
// hyparquet/small
export * from 'hyparquet/tiny'
export { readColumnChunk } from './column.js'
export { snappyUncompress } from './snappy.js'
export { readPlain } from './plain.js'
export { readRleBitPackedHybrid } from './encoding.js'
```

API surface:
```typescript
// Read a single column from a known offset
const metadata = parquetMetadata(footerBuffer)
const column = metadata.row_groups[0].columns[0]
const offset = column.meta_data.data_page_offset
const size = column.meta_data.total_compressed_size

const chunk = await fetch(url, {
  headers: { Range: `bytes=${offset}-${offset + size}` }
})
const values = readColumnChunk(await chunk.arrayBuffer(), metadata, 0, 0)
```

What's excluded:
- Nested structures (LIST, MAP, STRUCT)
- Type conversion (dates, decimals, timestamps)
- Delta encodings
- Variant type
- Query planning/filtering
- Multi-column assembly

Use cases:
- Reading ID columns for lookups
- Fetching specific scalar columns
- Building indexes from parquet
- Simple analytics queries

### Tier 3: Full (`hyparquet`) - Current Package (~50KB minified)

Everything included:
- All encodings (PLAIN, RLE, DELTA_*, BYTE_STREAM_SPLIT)
- All types including Variant, Geometry
- Nested structure assembly
- Type conversion
- Query interface with filtering
- Full AsyncBuffer utilities

Estimated minified+gzipped: **~25-30KB** (without external compressors)

## Code Splitting Strategy

### Option A: Separate Entry Points

Create separate package entry points:

```json
{
  "exports": {
    ".": "./src/index.js",
    "./tiny": "./src/tiny.js",
    "./small": "./src/small.js"
  }
}
```

Pros:
- Clean separation
- Bundlers can tree-shake effectively
- Clear API boundaries

Cons:
- Code duplication for shared utilities
- Need to maintain multiple entry points

### Option B: Conditional Exports with Tree Shaking

Keep single entry, rely on bundlers:

```typescript
// Only import what you need
import { parquetMetadata } from 'hyparquet'
// Bundler should tree-shake everything else
```

Pros:
- Single source of truth
- Simpler maintenance

Cons:
- Relies on bundler effectiveness
- Can't guarantee size limits
- Side effects may break tree-shaking

### Option C: Separate Packages (Recommended for Snippets)

Create `hyparquet-tiny` as a separate package:

```
hyparquet-tiny/
  src/
    metadata.js  # Standalone, no imports
    thrift.js    # Standalone, no imports
    schema.js    # Minimal, imports thrift
    index.js     # Re-exports
```

Pros:
- Guaranteed minimal size
- No dependency on main package
- Can be optimized specifically for edge

Cons:
- Code duplication
- Maintenance burden
- Version sync issues

## Cloudflare Snippets Implementation Strategy

### Constraint Analysis

With 32KB memory and 5ms CPU:
- Can't parse full metadata if >32KB
- Can read ~500KB at 100MB/s in 5ms
- 5 subrequests limits what we can fetch

### Recommended Architecture

```
Request Flow:

  ┌─────────┐    ┌─────────────┐    ┌────────────────┐
  │ Request │───>│  Snippet    │───>│  Worker        │
  │         │    │ (32KB,5ms)  │    │ (Full parsing) │
  └─────────┘    └─────────────┘    └────────────────┘
                      │
                      │ Direct response
                      │ (if possible)
                      ▼
```

**Snippet responsibilities:**
1. Parse 4KB footer to get metadata length
2. If metadata < 32KB, parse schema in-snippet
3. Return schema info OR redirect to Worker
4. Check row group statistics for predicate pushdown
5. Return 304/cached if possible

**Worker responsibilities:**
1. Full parquet reading
2. Complex queries
3. Multi-column assembly
4. Type conversion

### Implementation for `/tiny`

```typescript
// hyparquet-tiny/src/index.js
// Self-contained ~8KB implementation

const decoder = new TextDecoder()

// Inline thrift parser (minimal)
function readVarInt(view, offset) {
  let result = 0, shift = 0
  while (true) {
    const byte = view.getUint8(offset++)
    result |= (byte & 0x7f) << shift
    if (!(byte & 0x80)) return [result, offset]
    shift += 7
  }
}

// ... minimal TCompact implementation

export function parquetMetadata(arrayBuffer) {
  const view = new DataView(arrayBuffer)
  // Validate PAR1 magic
  if (view.getUint32(view.byteLength - 4, true) !== 0x31524150) {
    throw new Error('Invalid parquet file')
  }
  // Read metadata length
  const metadataLength = view.getUint32(view.byteLength - 8, true)
  // Parse thrift metadata
  // ... minimal implementation
}

export function parquetSchema(metadata) {
  // Build schema tree
}
```

## Bundle Size Estimates

| Export | Source | Minified | Gzipped | Use Case |
|--------|--------|----------|---------|----------|
| `/tiny` | ~25KB | ~8KB | ~4KB | Snippets, metadata |
| `/small` | ~55KB | ~20KB | ~12KB | Workers, single columns |
| Full | ~134KB | ~50KB | ~25KB | Full parsing |
| +ZSTD | +33KB | +15KB | +8KB | Modern files |
| +Brotli | +139KB | +50KB | +20KB | Rare usage |

## Feature Matrix

| Feature | `/tiny` | `/small` | Full |
|---------|---------|----------|------|
| Footer parsing | Y | Y | Y |
| Schema inspection | Y | Y | Y |
| Row group stats | Y | Y | Y |
| Column reading | - | Y | Y |
| PLAIN encoding | - | Y | Y |
| RLE/DICTIONARY | - | Y | Y |
| DELTA encodings | - | - | Y |
| BYTE_STREAM_SPLIT | - | - | Y |
| Snappy compression | - | Y | Y |
| ZSTD compression | - | - | opt |
| GZIP compression | - | - | opt |
| Type conversion | - | - | Y |
| Nested structures | - | - | Y |
| Variant type | - | - | Y |
| Geometry types | - | - | Y |
| Query interface | - | - | Y |
| Row filtering | - | - | Y |
| AsyncBuffer | - | partial | Y |

## Recommendations

### For ParqueDB Cloudflare Snippets

1. **Create `hyparquet-tiny`** as a separate, self-contained package
   - ~4KB gzipped fits comfortably in Snippet limits
   - Can do schema discovery and row group routing
   - Returns early for cached/simple requests

2. **Use full hyparquet in Workers** for actual data reading
   - 128MB memory is plenty
   - Can use all features including Variant

3. **Consider `/small` for edge caching**
   - Read ID columns to build indexes
   - Simple scalar queries at edge
   - Defer complex queries to origin

### Implementation Priority

1. **Phase 1**: Extract metadata-only code path
   - Test current hyparquet with minimal imports
   - Measure actual bundle sizes

2. **Phase 2**: Create standalone `hyparquet-tiny`
   - Self-contained, no dependencies
   - Optimized for code size

3. **Phase 3**: Add `/small` export to main package
   - Tree-shakeable column reading
   - Snappy-only compression path

## Files to Include per Tier

### `/tiny` (metadata only)
```
src/
  tiny/
    index.js          # Exports
    thrift-min.js     # Minimal TCompact (inline, ~2KB)
    metadata-min.js   # Footer parsing only (~3KB)
    schema-min.js     # Schema tree (~1KB)
    constants.js      # Type enums (~1KB)
```

### `/small` (single column reading)
```
src/
  small/
    index.js          # Re-exports tiny + column reading
    column-flat.js    # Flat column reading (~3KB)
    plain.js          # PLAIN encoding (~2KB)
    encoding.js       # RLE/BitPack (~2KB)
    snappy.js         # Decompression (~3KB)
    datapage-min.js   # Page header only (~2KB)
```

### Full (current)
```
src/
  index.js            # Full exports
  *.js                # All modules
```

## Proof of Concept Results

### POC Location

`poc/hyparquet-lite/` contains the proof-of-concept code:

- `tiny-import.js` - Metadata-only import test
- `full-import.js` - Full import test
- `test-real-file.js` - Real parquet file parsing test
- `test-footer-only.js` - Simulated Snippets range-request scenario
- `snippet-example.js` - Example Cloudflare Snippet implementation

### Real File Test Results

Testing against `data/onet-optimized/data.parquet` (187KB file):

```
File size: 187028 bytes
Metadata length: 655 bytes
Footer size needed: 663 bytes (0.65KB)

Parsed metadata:
- Version: 2
- Num rows: 1170
- Row groups: 2
- Columns: $id, $type, name, data

Memory usage: ~5.7KB heap
```

### Snippet Compatibility Verified

1. **Footer size (663 bytes)** << 32KB package limit
2. **Bundle size (4KB gzipped)** << 32KB package limit
3. **Memory usage (~6KB)** << 2MB memory limit
4. **Single parse operation** << 5ms CPU limit

## Next Steps

1. ~~Benchmark actual tree-shaking with current hyparquet~~ DONE
2. ~~Prototype `hyparquet-tiny` standalone package~~ NOT NEEDED
3. Test in actual Cloudflare Snippets environment (requires deployment)
4. ~~Measure real-world memory and CPU usage~~ DONE
5. Consider contributing tiered exports upstream to hyparquet (optional)

## Implementation Checklist

- [x] Research hyparquet capabilities
- [x] Verify bundle size with tree-shaking
- [x] Test metadata parsing with real files
- [x] Simulate range-request footer parsing
- [x] Document Snippet-compatible usage pattern
- [ ] Deploy test Snippet to Cloudflare (future work)

## Research Sources

- [hyparquet GitHub](https://github.com/hyparam/hyparquet)
- [hyparquet npm](https://www.npmjs.com/package/hyparquet)
- [Cloudflare Snippets Documentation](https://developers.cloudflare.com/rules/snippets/)
- [Cloudflare Workers Limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Bundlephobia](https://bundlephobia.com)
- [bundlejs](https://bundlejs.com)

## Related Issues

- Issue: parquedb-jomp - Explore hyparquet-lite for Cloudflare Snippets (CLOSED)
