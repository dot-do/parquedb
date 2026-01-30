# ParqueDB Secondary Indexing System

**Design Document: Comprehensive Secondary Indexes for Parquet-Based Storage**

This document details a complete secondary indexing system for ParqueDB, building on the graph-first architecture with support for B-tree indexes, hash indexes, composite indexes, expression indexes, and advanced features like covering indexes and partial indexes.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Index Types Overview](#index-types-overview)
3. [B-Tree Style Indexes](#b-tree-style-indexes)
4. [Hash Indexes](#hash-indexes)
5. [Composite/Compound Indexes](#compositecompound-indexes)
6. [Partial Indexes](#partial-indexes)
7. [Expression Indexes](#expression-indexes)
8. [Sparse Indexes](#sparse-indexes)
9. [Nested Path Indexes](#nested-path-indexes)
10. [Array Element Indexes](#array-element-indexes)
11. [Unique Indexes and Constraints](#unique-indexes-and-constraints)
12. [Covering Indexes](#covering-indexes)
13. [Zone Maps / Skip Indexes](#zone-maps--skip-indexes)
14. [Index File Format](#index-file-format)
15. [Index Metadata and Statistics](#index-metadata-and-statistics)
16. [Query Planning and Index Selection](#query-planning-and-index-selection)
17. [Index Maintenance](#index-maintenance)
18. [Implementation Details](#implementation-details)

---

## Executive Summary

ParqueDB's secondary indexing system is designed for:

1. **Parquet-Native Integration** - Indexes complement Parquet's columnar storage
2. **Variant-Aware Indexing** - First-class support for semi-structured data paths
3. **Minimal Overhead** - Sparse, partial, and expression indexes reduce storage
4. **Query Optimization** - Statistics-driven query planning with cost estimation
5. **Incremental Maintenance** - Efficient updates without full rebuilds

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Primary Index Format | Parquet files with sorted keys | Leverages existing infrastructure, column pruning |
| B-tree Alternative | Sorted String Tables (SST) | Better for range scans, immutable, cache-friendly |
| Hash Index Format | Bucketed Parquet files | Consistent hashing for O(1) lookups |
| Metadata Format | JSON + embedded Parquet stats | Human-readable config, machine-optimized stats |

---

## Index Types Overview

```
Index Types
├── Point Lookup Indexes
│   ├── Hash Index          - O(1) equality lookups
│   └── Unique Index        - Hash + uniqueness constraint
│
├── Range Scan Indexes
│   ├── B-Tree Index        - Ordered traversal, range queries
│   ├── SST Index           - Immutable sorted string tables
│   └── Zone Maps           - Row group skip indexes
│
├── Specialized Indexes
│   ├── Composite Index     - Multi-column ordered index
│   ├── Partial Index       - Filtered subset of rows
│   ├── Expression Index    - Index on computed values
│   ├── Sparse Index        - Only non-null values
│   ├── Covering Index      - Includes additional columns
│   ├── Nested Path Index   - Variant field paths
│   └── Array Index         - Array element lookups
│
└── Auxiliary Structures
    ├── Bloom Filters       - Probabilistic membership
    ├── Min/Max Statistics  - Column range metadata
    └── Distinct Counts     - Cardinality estimation
```

---

## B-Tree Style Indexes

### Overview

B-tree style indexes provide efficient range queries on Variant fields. Since Parquet is immutable, we implement LSM-tree inspired Sorted String Tables (SSTs) that provide B-tree semantics.

### Structure

```
indexes/
├── {namespace}/
│   └── {table}/
│       └── {index_name}/
│           ├── manifest.json           # Index metadata
│           ├── level-0/                # Recent writes (unsorted)
│           │   ├── sst-001.parquet
│           │   └── sst-002.parquet
│           ├── level-1/                # Sorted, compacted
│           │   └── sst-010.parquet
│           └── level-2/                # Fully merged
│               └── sst-100.parquet
```

### SST File Format (Parquet-Based)

```
sst-{id}.parquet
├── index_key: BYTE_ARRAY          # Serialized, comparable key
├── row_group_id: INT32            # Source row group
├── row_offset: INT64              # Offset within row group
├── primary_key: BYTE_ARRAY        # For joining back to data
└── [included_columns...]          # For covering indexes
```

**Sort Order**: `(index_key, primary_key)` - enables efficient range scans and duplicate handling.

### Key Serialization

To enable byte-wise comparison across types, keys are serialized with type-prefixed encoding:

```typescript
/**
 * Serialize a Variant value into a comparable byte array.
 * Type prefix ensures correct ordering across types.
 */
function serializeIndexKey(value: Variant): Uint8Array {
  const encoder = new KeyEncoder();

  switch (value.type) {
    case 'null':
      encoder.writeByte(0x00);
      break;

    case 'boolean':
      encoder.writeByte(0x01);
      encoder.writeByte(value.value ? 1 : 0);
      break;

    case 'int64':
      encoder.writeByte(0x02);
      // XOR with sign bit for correct ordering
      encoder.writeInt64Ordered(value.value);
      break;

    case 'float64':
      encoder.writeByte(0x03);
      encoder.writeFloat64Ordered(value.value);
      break;

    case 'string':
      encoder.writeByte(0x04);
      encoder.writeString(value.value);
      encoder.writeByte(0x00); // Null terminator for prefix matching
      break;

    case 'timestamp':
      encoder.writeByte(0x05);
      encoder.writeInt64Ordered(value.value);
      break;

    case 'date':
      encoder.writeByte(0x06);
      encoder.writeInt32Ordered(value.value);
      break;

    case 'binary':
      encoder.writeByte(0x07);
      encoder.writeLength(value.value.length);
      encoder.writeBytes(value.value);
      break;

    case 'array':
      encoder.writeByte(0x08);
      encoder.writeLength(value.elements.length);
      for (const elem of value.elements) {
        encoder.writeBytes(serializeIndexKey(elem));
      }
      break;

    case 'object':
      encoder.writeByte(0x09);
      const sortedKeys = Object.keys(value.fields).sort();
      encoder.writeLength(sortedKeys.length);
      for (const key of sortedKeys) {
        encoder.writeString(key);
        encoder.writeBytes(serializeIndexKey(value.fields[key]));
      }
      break;
  }

  return encoder.toBytes();
}

/**
 * Key encoder with ordered numeric encoding.
 */
class KeyEncoder {
  private buffer: number[] = [];

  writeByte(b: number): void {
    this.buffer.push(b & 0xFF);
  }

  writeInt64Ordered(value: bigint): void {
    // XOR with 0x8000000000000000n to flip sign bit
    // This makes negative numbers sort before positive
    const ordered = value ^ (1n << 63n);
    for (let i = 7; i >= 0; i--) {
      this.buffer.push(Number((ordered >> BigInt(i * 8)) & 0xFFn));
    }
  }

  writeFloat64Ordered(value: number): void {
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value, false); // Big-endian
    let bits = view.getBigUint64(0, false);

    // If negative, flip all bits; if positive, flip sign bit
    if (value < 0) {
      bits = ~bits;
    } else {
      bits ^= (1n << 63n);
    }

    for (let i = 7; i >= 0; i--) {
      this.buffer.push(Number((bits >> BigInt(i * 8)) & 0xFFn));
    }
  }

  writeString(s: string): void {
    const bytes = new TextEncoder().encode(s);
    this.writeLength(bytes.length);
    for (const b of bytes) {
      this.buffer.push(b);
    }
  }

  writeLength(len: number): void {
    // Variable-length encoding
    if (len < 128) {
      this.buffer.push(len);
    } else {
      this.buffer.push(0x80 | (len >> 24));
      this.buffer.push((len >> 16) & 0xFF);
      this.buffer.push((len >> 8) & 0xFF);
      this.buffer.push(len & 0xFF);
    }
  }

  writeBytes(bytes: Uint8Array): void {
    for (const b of bytes) {
      this.buffer.push(b);
    }
  }

  toBytes(): Uint8Array {
    return new Uint8Array(this.buffer);
  }
}
```

### Range Query Execution

```typescript
interface RangeQuery {
  gte?: Variant;    // Greater than or equal
  gt?: Variant;     // Greater than
  lte?: Variant;    // Less than or equal
  lt?: Variant;     // Less than
  prefix?: string;  // String prefix match
}

async function rangeQuery(
  indexName: string,
  range: RangeQuery,
  options?: { limit?: number; reverse?: boolean }
): Promise<IndexEntry[]> {
  const manifest = await loadManifest(indexName);
  const results: IndexEntry[] = [];

  // Serialize range bounds
  const lowerBound = range.gte ? serializeIndexKey(range.gte) :
                     range.gt ? serializeIndexKey(range.gt) : null;
  const upperBound = range.lte ? serializeIndexKey(range.lte) :
                     range.lt ? serializeIndexKey(range.lt) : null;
  const lowerInclusive = !!range.gte;
  const upperInclusive = !!range.lte;

  // For prefix queries, derive bounds
  if (range.prefix) {
    const prefixBytes = serializeIndexKey({ type: 'string', value: range.prefix });
    lowerBound = prefixBytes;
    upperBound = incrementBytes(prefixBytes);
    lowerInclusive = true;
    upperInclusive = false;
  }

  // Query each level, merge results
  for (const level of manifest.levels) {
    for (const sst of level.files) {
      // Use SST bloom filter and zone maps for pruning
      if (!sstMayContainRange(sst, lowerBound, upperBound)) {
        continue;
      }

      const entries = await scanSST(sst, {
        lowerBound,
        upperBound,
        lowerInclusive,
        upperInclusive,
        reverse: options?.reverse
      });

      results.push(...entries);
    }
  }

  // Merge and deduplicate (later levels supersede earlier)
  const merged = mergeIndexEntries(results);

  // Apply limit
  if (options?.limit) {
    return merged.slice(0, options.limit);
  }

  return merged;
}
```

### Compaction Strategy

```typescript
interface CompactionConfig {
  level0FileLimit: number;       // Trigger compaction when exceeded
  levelMultiplier: number;       // Size ratio between levels
  maxLevelSize: number[];        // Max bytes per level
  mergeWidth: number;            // Files to merge at once
}

const DEFAULT_COMPACTION: CompactionConfig = {
  level0FileLimit: 4,
  levelMultiplier: 10,
  maxLevelSize: [
    10 * 1024 * 1024,      // L0: 10 MB
    100 * 1024 * 1024,     // L1: 100 MB
    1024 * 1024 * 1024,    // L2: 1 GB
    10 * 1024 * 1024 * 1024, // L3: 10 GB
  ],
  mergeWidth: 4,
};

async function compact(indexName: string): Promise<void> {
  const manifest = await loadManifest(indexName);

  for (let level = 0; level < manifest.levels.length - 1; level++) {
    const currentLevel = manifest.levels[level];
    const nextLevel = manifest.levels[level + 1];

    // Check if compaction needed
    if (level === 0) {
      if (currentLevel.files.length < DEFAULT_COMPACTION.level0FileLimit) {
        continue;
      }
    } else {
      const levelSize = currentLevel.files.reduce((sum, f) => sum + f.size, 0);
      if (levelSize < DEFAULT_COMPACTION.maxLevelSize[level]) {
        continue;
      }
    }

    // Select files to compact
    const filesToCompact = selectFilesForCompaction(currentLevel, nextLevel);

    // Merge and write new SST
    const merged = await mergeSSTFiles(filesToCompact);
    const newSST = await writeSST(merged, level + 1);

    // Update manifest atomically
    await updateManifest(indexName, {
      remove: filesToCompact.map(f => f.id),
      add: [newSST]
    });
  }
}
```

---

## Hash Indexes

### Overview

Hash indexes provide O(1) equality lookups, ideal for primary key access and exact match queries.

### Structure

```
indexes/
├── {namespace}/
│   └── {table}/
│       └── {index_name}.hash/
│           ├── manifest.json
│           ├── bucket-0000.parquet
│           ├── bucket-0001.parquet
│           └── ...bucket-{N}.parquet
```

### Bucket Assignment

```typescript
const DEFAULT_BUCKET_COUNT = 256;
const MAX_BUCKET_SIZE = 100 * 1024 * 1024; // 100 MB

interface HashIndexConfig {
  bucketCount: number;
  hashFunction: 'xxhash64' | 'murmur3' | 'fnv1a';
}

function computeBucket(key: Variant, config: HashIndexConfig): number {
  const keyBytes = serializeIndexKey(key);
  const hash = xxhash64(keyBytes);
  return Number(hash % BigInt(config.bucketCount));
}

/**
 * Hash index bucket file format
 */
interface HashBucketEntry {
  key_hash: bigint;           // Full 64-bit hash for verification
  index_key: Uint8Array;      // Serialized key
  primary_key: Uint8Array;    // Reference to source row
  row_group_id: number;
  row_offset: number;
  [included: string]: any;    // Covering index columns
}
```

### Hash Index Operations

```typescript
async function hashLookup(
  indexName: string,
  key: Variant
): Promise<IndexEntry | null> {
  const config = await loadHashConfig(indexName);
  const bucket = computeBucket(key, config);

  const bucketFile = `${indexName}.hash/bucket-${bucket.toString().padStart(4, '0')}.parquet`;
  const keyBytes = serializeIndexKey(key);
  const keyHash = xxhash64(keyBytes);

  // Use bloom filter for early rejection
  const metadata = await loadBucketMetadata(bucketFile);
  if (!metadata.bloomFilter.mayContain(keyHash)) {
    return null;
  }

  // Scan bucket for exact match
  const entries = await parquetRead({
    file: bucketFile,
    filter: { key_hash: { eq: keyHash } },
    columns: ['index_key', 'primary_key', 'row_group_id', 'row_offset']
  });

  // Verify exact key match (hash collision check)
  for (const entry of entries) {
    if (bytesEqual(entry.index_key, keyBytes)) {
      return entry;
    }
  }

  return null;
}

async function hashInsert(
  indexName: string,
  key: Variant,
  entry: IndexEntry
): Promise<void> {
  const config = await loadHashConfig(indexName);
  const bucket = computeBucket(key, config);

  // Append to write-ahead buffer
  await appendToWAL(indexName, {
    operation: 'insert',
    bucket,
    key,
    entry
  });

  // Periodically flush WAL to bucket files
  if (await shouldFlushWAL(indexName)) {
    await flushHashIndex(indexName);
  }
}
```

### Dynamic Rehashing

```typescript
interface RehashState {
  oldBucketCount: number;
  newBucketCount: number;
  migratedBuckets: Set<number>;
  inProgress: boolean;
}

async function rehashIfNeeded(indexName: string): Promise<void> {
  const manifest = await loadManifest(indexName);

  // Check if any bucket exceeds size threshold
  const oversizedBuckets = manifest.buckets.filter(
    b => b.size > MAX_BUCKET_SIZE
  );

  if (oversizedBuckets.length === 0) {
    return;
  }

  // Double bucket count
  const newBucketCount = manifest.bucketCount * 2;

  // Start incremental rehash
  const rehashState: RehashState = {
    oldBucketCount: manifest.bucketCount,
    newBucketCount,
    migratedBuckets: new Set(),
    inProgress: true
  };

  await saveRehashState(indexName, rehashState);

  // Migrate buckets incrementally
  for (let oldBucket = 0; oldBucket < manifest.bucketCount; oldBucket++) {
    await migrateBucket(indexName, oldBucket, rehashState);
    rehashState.migratedBuckets.add(oldBucket);
    await saveRehashState(indexName, rehashState);
  }

  // Finalize
  rehashState.inProgress = false;
  await updateManifest(indexName, { bucketCount: newBucketCount });
}

async function migrateBucket(
  indexName: string,
  oldBucket: number,
  state: RehashState
): Promise<void> {
  const entries = await readBucket(indexName, oldBucket);

  // Redistribute entries to new buckets
  const newBuckets = new Map<number, HashBucketEntry[]>();

  for (const entry of entries) {
    const newBucket = Number(entry.key_hash % BigInt(state.newBucketCount));
    if (!newBuckets.has(newBucket)) {
      newBuckets.set(newBucket, []);
    }
    newBuckets.get(newBucket)!.push(entry);
  }

  // Write new bucket files
  for (const [bucket, bucketEntries] of newBuckets) {
    await writeBucket(indexName, bucket, bucketEntries);
  }

  // Remove old bucket
  await deleteBucket(indexName, oldBucket);
}
```

---

## Composite/Compound Indexes

### Overview

Composite indexes support queries on multiple columns with a defined key order. They enable efficient range scans on leading columns and equality matches on any prefix.

### Definition

```typescript
interface CompositeIndexDefinition {
  name: string;
  table: string;
  columns: CompositeColumn[];
  unique?: boolean;
  partial?: PartialIndexPredicate;
}

interface CompositeColumn {
  /** Column name or Variant path */
  path: string;
  /** Sort direction */
  direction: 'asc' | 'desc';
  /** Null handling */
  nulls: 'first' | 'last';
}

// Example: Index on (namespace ASC, type ASC, created_at DESC)
const exampleIndex: CompositeIndexDefinition = {
  name: 'idx_ns_type_created',
  table: 'nodes',
  columns: [
    { path: 'ns', direction: 'asc', nulls: 'last' },
    { path: 'type', direction: 'asc', nulls: 'last' },
    { path: 'data.created_at', direction: 'desc', nulls: 'first' }
  ]
};
```

### Composite Key Serialization

```typescript
function serializeCompositeKey(
  row: Record<string, Variant>,
  columns: CompositeColumn[]
): Uint8Array {
  const encoder = new KeyEncoder();

  for (const col of columns) {
    const value = extractPath(row, col.path);

    // Handle nulls
    if (value === null || value.type === 'null') {
      encoder.writeByte(col.nulls === 'first' ? 0x00 : 0xFF);
      continue;
    }

    // Write value marker (not null)
    encoder.writeByte(col.nulls === 'first' ? 0x01 : 0xFE);

    // Serialize value
    let keyBytes = serializeIndexKey(value);

    // For descending order, flip all bits
    if (col.direction === 'desc') {
      keyBytes = flipBits(keyBytes);
    }

    encoder.writeBytes(keyBytes);
  }

  return encoder.toBytes();
}

function flipBits(bytes: Uint8Array): Uint8Array {
  const flipped = new Uint8Array(bytes.length);
  for (let i = 0; i < bytes.length; i++) {
    flipped[i] = bytes[i] ^ 0xFF;
  }
  return flipped;
}
```

### Query Optimization for Composites

```typescript
interface CompositeQuery {
  // Equality predicates (must be leading columns)
  eq: Record<string, Variant>;
  // Range predicates (at most one, after equalities)
  range?: {
    column: string;
    gte?: Variant;
    gt?: Variant;
    lte?: Variant;
    lt?: Variant;
  };
}

function canUseCompositeIndex(
  query: CompositeQuery,
  index: CompositeIndexDefinition
): { usable: boolean; prefixLength: number; reason?: string } {
  let prefixLength = 0;

  // Check equality predicates match leading columns
  for (const col of index.columns) {
    if (col.path in query.eq) {
      prefixLength++;
    } else {
      break;
    }
  }

  // Check range predicate is on next column
  if (query.range) {
    const rangeColIndex = index.columns.findIndex(
      c => c.path === query.range!.column
    );

    if (rangeColIndex === prefixLength) {
      prefixLength++;
    } else if (rangeColIndex !== -1) {
      // Range column not immediately after equalities
      return {
        usable: true,
        prefixLength,
        reason: `Range on ${query.range.column} cannot use full index`
      };
    }
  }

  return {
    usable: prefixLength > 0,
    prefixLength
  };
}
```

### Index-Only Scans

```typescript
/**
 * Determine if query can be satisfied by index alone
 */
function isIndexOnlyScan(
  query: Query,
  index: CompositeIndexDefinition,
  requestedColumns: string[]
): boolean {
  const indexedPaths = new Set(index.columns.map(c => c.path));

  // Add covering columns if any
  if (index.covering) {
    for (const col of index.covering) {
      indexedPaths.add(col);
    }
  }

  // Check all requested columns are in index
  for (const col of requestedColumns) {
    if (!indexedPaths.has(col)) {
      return false;
    }
  }

  return true;
}
```

---

## Partial Indexes

### Overview

Partial indexes only index rows matching a predicate, reducing storage and maintenance overhead.

### Definition

```typescript
interface PartialIndexDefinition {
  name: string;
  table: string;
  columns: string[];
  predicate: PartialIndexPredicate;
  indexType: 'btree' | 'hash';
}

type PartialIndexPredicate =
  | { column: string; op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte'; value: Variant }
  | { column: string; op: 'in'; values: Variant[] }
  | { column: string; op: 'is_not_null' }
  | { column: string; op: 'is_null' }
  | { and: PartialIndexPredicate[] }
  | { or: PartialIndexPredicate[] }
  | { not: PartialIndexPredicate };

// Example: Index only active users
const activeUsersIndex: PartialIndexDefinition = {
  name: 'idx_active_users_email',
  table: 'nodes',
  columns: ['data.email'],
  predicate: {
    and: [
      { column: 'type', op: 'eq', value: { type: 'string', value: 'User' } },
      { column: 'data.status', op: 'eq', value: { type: 'string', value: 'active' } },
      { column: 'deleted', op: 'eq', value: { type: 'boolean', value: false } }
    ]
  },
  indexType: 'btree'
};
```

### Predicate Evaluation

```typescript
function evaluatePredicate(
  row: Record<string, Variant>,
  predicate: PartialIndexPredicate
): boolean {
  if ('and' in predicate) {
    return predicate.and.every(p => evaluatePredicate(row, p));
  }

  if ('or' in predicate) {
    return predicate.or.some(p => evaluatePredicate(row, p));
  }

  if ('not' in predicate) {
    return !evaluatePredicate(row, predicate.not);
  }

  const value = extractPath(row, predicate.column);

  switch (predicate.op) {
    case 'eq':
      return variantEquals(value, predicate.value);
    case 'neq':
      return !variantEquals(value, predicate.value);
    case 'gt':
      return variantCompare(value, predicate.value) > 0;
    case 'gte':
      return variantCompare(value, predicate.value) >= 0;
    case 'lt':
      return variantCompare(value, predicate.value) < 0;
    case 'lte':
      return variantCompare(value, predicate.value) <= 0;
    case 'in':
      return predicate.values.some(v => variantEquals(value, v));
    case 'is_not_null':
      return value !== null && value.type !== 'null';
    case 'is_null':
      return value === null || value.type === 'null';
  }
}
```

### Query Planning with Partial Indexes

```typescript
interface QueryPredicate {
  column: string;
  op: string;
  value: Variant;
}

function partialIndexApplicable(
  queryPredicates: QueryPredicate[],
  indexPredicate: PartialIndexPredicate
): boolean {
  // Check if query predicates imply index predicate
  // This ensures we only search the index when results must be in it

  const normalizedQuery = normalizePredicates(queryPredicates);
  const normalizedIndex = normalizePredicate(indexPredicate);

  return implies(normalizedQuery, normalizedIndex);
}

function implies(
  query: NormalizedPredicate,
  index: NormalizedPredicate
): boolean {
  // Convert to DNF and check if query implies index
  // Query implies index if every clause of query implies some clause of index

  // Example: query (status='active' AND type='User')
  //          implies index (status='active')

  return checkImplication(query, index);
}
```

---

## Expression Indexes

### Overview

Expression indexes allow indexing computed values, enabling efficient queries on derived data.

### Definition

```typescript
interface ExpressionIndexDefinition {
  name: string;
  table: string;
  expression: IndexExpression;
  indexType: 'btree' | 'hash';
  partial?: PartialIndexPredicate;
}

type IndexExpression =
  | { type: 'column'; path: string }
  | { type: 'lower'; arg: IndexExpression }
  | { type: 'upper'; arg: IndexExpression }
  | { type: 'concat'; args: IndexExpression[] }
  | { type: 'substring'; arg: IndexExpression; start: number; length?: number }
  | { type: 'coalesce'; args: IndexExpression[] }
  | { type: 'extract'; arg: IndexExpression; field: 'year' | 'month' | 'day' | 'hour' }
  | { type: 'json_extract'; arg: IndexExpression; path: string }
  | { type: 'hash'; arg: IndexExpression; algorithm: 'md5' | 'sha256' }
  | { type: 'length'; arg: IndexExpression }
  | { type: 'arithmetic'; op: '+' | '-' | '*' | '/'; left: IndexExpression; right: IndexExpression };

// Example: Case-insensitive email index
const caseInsensitiveEmail: ExpressionIndexDefinition = {
  name: 'idx_email_lower',
  table: 'nodes',
  expression: {
    type: 'lower',
    arg: { type: 'column', path: 'data.email' }
  },
  indexType: 'hash'
};

// Example: Year extraction for time-based queries
const yearIndex: ExpressionIndexDefinition = {
  name: 'idx_created_year',
  table: 'nodes',
  expression: {
    type: 'extract',
    arg: { type: 'column', path: 'data.created_at' },
    field: 'year'
  },
  indexType: 'btree'
};

// Example: Computed full name
const fullNameIndex: ExpressionIndexDefinition = {
  name: 'idx_full_name',
  table: 'nodes',
  expression: {
    type: 'concat',
    args: [
      { type: 'coalesce', args: [
        { type: 'column', path: 'data.first_name' },
        { type: 'literal', value: '' }
      ]},
      { type: 'literal', value: ' ' },
      { type: 'coalesce', args: [
        { type: 'column', path: 'data.last_name' },
        { type: 'literal', value: '' }
      ]}
    ]
  },
  indexType: 'btree',
  partial: { column: 'type', op: 'eq', value: { type: 'string', value: 'Person' } }
};
```

### Expression Evaluation

```typescript
function evaluateExpression(
  row: Record<string, Variant>,
  expr: IndexExpression
): Variant {
  switch (expr.type) {
    case 'column':
      return extractPath(row, expr.path) ?? { type: 'null' };

    case 'lower':
      const lowerArg = evaluateExpression(row, expr.arg);
      if (lowerArg.type !== 'string') return { type: 'null' };
      return { type: 'string', value: lowerArg.value.toLowerCase() };

    case 'upper':
      const upperArg = evaluateExpression(row, expr.arg);
      if (upperArg.type !== 'string') return { type: 'null' };
      return { type: 'string', value: upperArg.value.toUpperCase() };

    case 'concat':
      const parts = expr.args.map(a => {
        const v = evaluateExpression(row, a);
        return v.type === 'string' ? v.value : '';
      });
      return { type: 'string', value: parts.join('') };

    case 'coalesce':
      for (const arg of expr.args) {
        const v = evaluateExpression(row, arg);
        if (v.type !== 'null') return v;
      }
      return { type: 'null' };

    case 'extract':
      const tsArg = evaluateExpression(row, expr.arg);
      if (tsArg.type !== 'timestamp') return { type: 'null' };
      const date = new Date(Number(tsArg.value / 1000n));
      switch (expr.field) {
        case 'year': return { type: 'int64', value: BigInt(date.getUTCFullYear()) };
        case 'month': return { type: 'int64', value: BigInt(date.getUTCMonth() + 1) };
        case 'day': return { type: 'int64', value: BigInt(date.getUTCDate()) };
        case 'hour': return { type: 'int64', value: BigInt(date.getUTCHours()) };
      }

    case 'json_extract':
      const jsonArg = evaluateExpression(row, expr.arg);
      if (jsonArg.type !== 'object') return { type: 'null' };
      return extractPath({ root: jsonArg }, `root.${expr.path}`) ?? { type: 'null' };

    case 'hash':
      const hashArg = evaluateExpression(row, expr.arg);
      const bytes = serializeIndexKey(hashArg);
      const hashValue = expr.algorithm === 'md5' ? md5(bytes) : sha256(bytes);
      return { type: 'string', value: hashValue };

    case 'length':
      const lenArg = evaluateExpression(row, expr.arg);
      if (lenArg.type === 'string') {
        return { type: 'int64', value: BigInt(lenArg.value.length) };
      }
      if (lenArg.type === 'array') {
        return { type: 'int64', value: BigInt(lenArg.elements.length) };
      }
      return { type: 'null' };

    case 'arithmetic':
      const left = evaluateExpression(row, expr.left);
      const right = evaluateExpression(row, expr.right);
      return performArithmetic(left, right, expr.op);

    default:
      return { type: 'null' };
  }
}
```

### Expression Matching in Queries

```typescript
/**
 * Check if query expression matches index expression
 */
function expressionMatches(
  queryExpr: QueryExpression,
  indexExpr: IndexExpression
): boolean {
  // Normalize both expressions
  const normalizedQuery = normalizeExpression(queryExpr);
  const normalizedIndex = normalizeExpression(indexExpr);

  return deepEquals(normalizedQuery, normalizedIndex);
}

/**
 * Rewrite query to use expression index
 */
function rewriteForExpressionIndex(
  query: Query,
  index: ExpressionIndexDefinition
): Query | null {
  // Find matching expression in query predicates
  for (const predicate of query.predicates) {
    if (expressionMatches(predicate.expression, index.expression)) {
      return {
        ...query,
        useIndex: index.name,
        predicates: query.predicates.map(p =>
          expressionMatches(p.expression, index.expression)
            ? { ...p, useIndexColumn: 'index_key' }
            : p
        )
      };
    }
  }

  return null;
}
```

---

## Sparse Indexes

### Overview

Sparse indexes only contain entries for rows where the indexed column is not null, reducing storage for optional fields.

### Definition

```typescript
interface SparseIndexDefinition {
  name: string;
  table: string;
  column: string;
  indexType: 'btree' | 'hash';
  // Sparse is implicit - only non-null values indexed
}

// Example: Index on optional phone number
const phoneIndex: SparseIndexDefinition = {
  name: 'idx_phone',
  table: 'nodes',
  column: 'data.phone',
  indexType: 'hash'
};
```

### Storage Optimization

```typescript
interface SparseIndexStats {
  totalRows: number;
  indexedRows: number;
  nullRows: number;
  sparsity: number;  // nullRows / totalRows
  storageSavings: number;  // bytes saved vs full index
}

async function buildSparseIndex(
  definition: SparseIndexDefinition
): Promise<SparseIndexStats> {
  const stats: SparseIndexStats = {
    totalRows: 0,
    indexedRows: 0,
    nullRows: 0,
    sparsity: 0,
    storageSavings: 0
  };

  const indexEntries: IndexEntry[] = [];

  for await (const rowGroup of scanTable(definition.table)) {
    for (const row of rowGroup) {
      stats.totalRows++;

      const value = extractPath(row, definition.column);

      if (value === null || value.type === 'null') {
        stats.nullRows++;
        continue;  // Skip null values
      }

      stats.indexedRows++;
      indexEntries.push({
        key: serializeIndexKey(value),
        primaryKey: row.primary_key,
        rowGroupId: rowGroup.id,
        rowOffset: row.offset
      });
    }
  }

  // Build index from non-null entries only
  if (definition.indexType === 'btree') {
    await buildBTreeIndex(definition.name, indexEntries);
  } else {
    await buildHashIndex(definition.name, indexEntries);
  }

  stats.sparsity = stats.nullRows / stats.totalRows;
  stats.storageSavings = estimateStorageSavings(stats);

  return stats;
}
```

### Query Planning with Sparse Indexes

```typescript
function canUseSparseIndex(
  query: Query,
  sparseIndex: SparseIndexDefinition
): boolean {
  // Sparse index can only be used when:
  // 1. Query explicitly filters on indexed column being not null
  // 2. Query has equality/range predicate on indexed column (implies not null)

  for (const predicate of query.predicates) {
    if (predicate.column === sparseIndex.column) {
      // Equality or range implies not null
      if (['eq', 'gt', 'gte', 'lt', 'lte', 'in'].includes(predicate.op)) {
        return true;
      }
      // Explicit IS NOT NULL
      if (predicate.op === 'is_not_null') {
        return true;
      }
    }
  }

  return false;
}
```

---

## Nested Path Indexes

### Overview

Nested path indexes support efficient queries on Variant field paths like `data.user.email`.

### Path Syntax

```typescript
/**
 * Path syntax for nested Variant fields:
 * - data.user.email      - Object path
 * - data.tags[0]         - Array index
 * - data.tags[*]         - All array elements
 * - data.users[*].email  - Nested array element field
 */
type VariantPath = string;

interface NestedPathIndexDefinition {
  name: string;
  table: string;
  path: VariantPath;
  indexType: 'btree' | 'hash';
  sparse?: boolean;  // Only index when path exists
  partial?: PartialIndexPredicate;
}

// Example: Index on nested email field
const nestedEmailIndex: NestedPathIndexDefinition = {
  name: 'idx_user_email',
  table: 'nodes',
  path: 'data.contact.email',
  indexType: 'hash',
  sparse: true
};

// Example: Index on deeply nested field
const deepNestedIndex: NestedPathIndexDefinition = {
  name: 'idx_billing_country',
  table: 'nodes',
  path: 'data.billing.address.country',
  indexType: 'btree',
  sparse: true
};
```

### Path Extraction

```typescript
function extractPath(root: Variant, path: string): Variant | null {
  const parts = parsePath(path);
  let current: Variant | null = root;

  for (const part of parts) {
    if (current === null || current.type === 'null') {
      return null;
    }

    if (part.type === 'field') {
      if (current.type !== 'object') return null;
      current = current.fields[part.name] ?? null;
    } else if (part.type === 'index') {
      if (current.type !== 'array') return null;
      current = current.elements[part.index] ?? null;
    } else if (part.type === 'wildcard') {
      // Return array of all values at this path
      if (current.type !== 'array') return null;
      const results: Variant[] = [];
      for (const elem of current.elements) {
        const extracted = extractPath(elem, part.remainder);
        if (extracted !== null && extracted.type !== 'null') {
          results.push(extracted);
        }
      }
      return { type: 'array', elements: results };
    }
  }

  return current;
}

interface PathPart {
  type: 'field' | 'index' | 'wildcard';
  name?: string;
  index?: number;
  remainder?: string;
}

function parsePath(path: string): PathPart[] {
  const parts: PathPart[] = [];
  const regex = /\.?([a-zA-Z_][a-zA-Z0-9_]*|\[\d+\]|\[\*\])/g;
  let match;

  while ((match = regex.exec(path)) !== null) {
    const part = match[1];

    if (part.startsWith('[') && part.endsWith(']')) {
      const inner = part.slice(1, -1);
      if (inner === '*') {
        parts.push({
          type: 'wildcard',
          remainder: path.slice(regex.lastIndex)
        });
        return parts;
      } else {
        parts.push({ type: 'index', index: parseInt(inner, 10) });
      }
    } else {
      parts.push({ type: 'field', name: part });
    }
  }

  return parts;
}
```

### Schema Evolution Handling

```typescript
/**
 * Handle schema evolution for nested path indexes.
 * When variant schema changes, indexes may need updating.
 */
interface PathIndexMigration {
  oldPath: string;
  newPath: string;
  transformation?: (old: Variant) => Variant;
}

async function migrateNestedIndex(
  indexName: string,
  migration: PathIndexMigration
): Promise<void> {
  const definition = await loadIndexDefinition(indexName);

  if (definition.path !== migration.oldPath) {
    throw new Error(`Index path mismatch`);
  }

  // Rebuild index with new path
  await rebuildIndex({
    ...definition,
    path: migration.newPath,
    transformation: migration.transformation
  });
}
```

---

## Array Element Indexes

### Overview

Array element indexes enable efficient queries on array contents, supporting both positional and element-value lookups.

### Definition

```typescript
interface ArrayIndexDefinition {
  name: string;
  table: string;
  arrayPath: string;            // Path to array field
  elementPath?: string;         // Path within array element (for object arrays)
  indexType: 'btree' | 'hash';
  multiEntry: boolean;          // Index each element separately
}

// Example: Index all tags in a tags array
const tagsIndex: ArrayIndexDefinition = {
  name: 'idx_tags',
  table: 'nodes',
  arrayPath: 'data.tags',
  indexType: 'hash',
  multiEntry: true
};

// Example: Index email from array of contacts
const contactEmailsIndex: ArrayIndexDefinition = {
  name: 'idx_contact_emails',
  table: 'nodes',
  arrayPath: 'data.contacts',
  elementPath: 'email',
  indexType: 'hash',
  multiEntry: true
};
```

### Multi-Entry Index Building

```typescript
async function buildArrayIndex(
  definition: ArrayIndexDefinition
): Promise<void> {
  const entries: IndexEntry[] = [];

  for await (const rowGroup of scanTable(definition.table)) {
    for (const row of rowGroup) {
      const array = extractPath(row, definition.arrayPath);

      if (array === null || array.type !== 'array') {
        continue;
      }

      if (definition.multiEntry) {
        // Create one index entry per array element
        for (let i = 0; i < array.elements.length; i++) {
          const element = array.elements[i];
          const value = definition.elementPath
            ? extractPath(element, definition.elementPath)
            : element;

          if (value === null || value.type === 'null') {
            continue;
          }

          entries.push({
            key: serializeIndexKey(value),
            primaryKey: row.primary_key,
            rowGroupId: rowGroup.id,
            rowOffset: row.offset,
            arrayIndex: i  // Track position for ordering
          });
        }
      } else {
        // Index entire array as single key
        entries.push({
          key: serializeIndexKey(array),
          primaryKey: row.primary_key,
          rowGroupId: rowGroup.id,
          rowOffset: row.offset
        });
      }
    }
  }

  if (definition.indexType === 'btree') {
    await buildBTreeIndex(definition.name, entries);
  } else {
    await buildHashIndex(definition.name, entries);
  }
}
```

### Array Query Operators

```typescript
interface ArrayQuery {
  // Contains element
  contains?: Variant;
  // Contains all elements
  containsAll?: Variant[];
  // Contains any element
  containsAny?: Variant[];
  // Array length conditions
  length?: {
    eq?: number;
    gt?: number;
    gte?: number;
    lt?: number;
    lte?: number;
  };
}

async function queryArrayIndex(
  indexName: string,
  query: ArrayQuery
): Promise<IndexEntry[]> {
  const definition = await loadIndexDefinition(indexName);

  if (query.contains) {
    // Simple element lookup
    return await lookupIndex(indexName, query.contains);
  }

  if (query.containsAll) {
    // Intersection of all element lookups
    const resultSets = await Promise.all(
      query.containsAll.map(v => lookupIndex(indexName, v))
    );
    return intersectByPrimaryKey(resultSets);
  }

  if (query.containsAny) {
    // Union of all element lookups
    const resultSets = await Promise.all(
      query.containsAny.map(v => lookupIndex(indexName, v))
    );
    return unionByPrimaryKey(resultSets);
  }

  throw new Error('Invalid array query');
}
```

---

## Unique Indexes and Constraints

### Overview

Unique indexes enforce uniqueness constraints while providing efficient point lookups.

### Definition

```typescript
interface UniqueIndexDefinition {
  name: string;
  table: string;
  columns: string[];
  partial?: PartialIndexPredicate;
  deferrable?: boolean;          // Check at commit time
  nullsDistinct?: boolean;       // Whether NULLs are considered distinct
}

// Example: Unique email per namespace
const uniqueEmailIndex: UniqueIndexDefinition = {
  name: 'idx_unique_email',
  table: 'nodes',
  columns: ['ns', 'data.email'],
  partial: {
    and: [
      { column: 'type', op: 'eq', value: { type: 'string', value: 'User' } },
      { column: 'deleted', op: 'eq', value: { type: 'boolean', value: false } }
    ]
  },
  nullsDistinct: true
};

// Example: Unique edge constraint
const uniqueEdgeIndex: UniqueIndexDefinition = {
  name: 'idx_unique_edge',
  table: 'edges',
  columns: ['ns', 'from_id', 'rel_type', 'to_id'],
  partial: { column: 'deleted', op: 'eq', value: { type: 'boolean', value: false } }
};
```

### Uniqueness Enforcement

```typescript
interface UniqueViolation {
  index: string;
  existingKey: Variant[];
  conflictingPrimaryKey: Uint8Array;
}

async function checkUniqueness(
  definition: UniqueIndexDefinition,
  row: Record<string, Variant>
): Promise<UniqueViolation | null> {
  // Check if row matches partial predicate
  if (definition.partial) {
    if (!evaluatePredicate(row, definition.partial)) {
      return null;  // Row not subject to uniqueness constraint
    }
  }

  // Extract key values
  const keyValues = definition.columns.map(col => extractPath(row, col));

  // Handle NULL handling
  if (definition.nullsDistinct !== false) {
    // If any key column is NULL, uniqueness is not enforced
    if (keyValues.some(v => v === null || v.type === 'null')) {
      return null;
    }
  }

  // Look up in index
  const compositeKey = serializeCompositeKey(
    row,
    definition.columns.map(c => ({ path: c, direction: 'asc', nulls: 'last' }))
  );

  const existing = await lookupHashIndex(definition.name, { type: 'binary', value: compositeKey });

  if (existing && !bytesEqual(existing.primaryKey, row.primary_key)) {
    return {
      index: definition.name,
      existingKey: keyValues,
      conflictingPrimaryKey: existing.primaryKey
    };
  }

  return null;
}

async function enforceUniqueness(
  table: string,
  row: Record<string, Variant>,
  operation: 'insert' | 'update'
): Promise<void> {
  const uniqueIndexes = await getUniqueIndexes(table);

  for (const index of uniqueIndexes) {
    const violation = await checkUniqueness(index, row);

    if (violation) {
      throw new UniqueConstraintViolationError(
        `Duplicate key violates unique constraint "${violation.index}"`,
        violation
      );
    }
  }
}
```

### Deferred Constraint Checking

```typescript
interface DeferredConstraintContext {
  pendingChecks: Map<string, Set<Uint8Array>>;  // index -> primary keys
}

function beginTransaction(): DeferredConstraintContext {
  return { pendingChecks: new Map() };
}

async function deferredInsert(
  ctx: DeferredConstraintContext,
  table: string,
  row: Record<string, Variant>
): Promise<void> {
  const uniqueIndexes = await getUniqueIndexes(table);

  for (const index of uniqueIndexes) {
    if (index.deferrable) {
      // Queue for commit-time check
      if (!ctx.pendingChecks.has(index.name)) {
        ctx.pendingChecks.set(index.name, new Set());
      }
      ctx.pendingChecks.get(index.name)!.add(row.primary_key);
    } else {
      // Immediate check
      const violation = await checkUniqueness(index, row);
      if (violation) {
        throw new UniqueConstraintViolationError(
          `Immediate unique constraint violation`,
          violation
        );
      }
    }
  }
}

async function commitTransaction(
  ctx: DeferredConstraintContext
): Promise<void> {
  // Check all deferred constraints
  for (const [indexName, primaryKeys] of ctx.pendingChecks) {
    for (const pk of primaryKeys) {
      const row = await lookupByPrimaryKey(pk);
      const index = await loadIndexDefinition(indexName);
      const violation = await checkUniqueness(index, row);

      if (violation) {
        throw new UniqueConstraintViolationError(
          `Deferred unique constraint violation on commit`,
          violation
        );
      }
    }
  }
}
```

---

## Covering Indexes

### Overview

Covering indexes include additional columns beyond the index key, enabling index-only scans.

### Definition

```typescript
interface CoveringIndexDefinition {
  name: string;
  table: string;
  keyColumns: CompositeColumn[];
  includedColumns: string[];      // Additional columns stored in index
  indexType: 'btree' | 'hash';
  partial?: PartialIndexPredicate;
}

// Example: Cover common query columns
const ordersByCustomer: CoveringIndexDefinition = {
  name: 'idx_orders_by_customer',
  table: 'nodes',
  keyColumns: [
    { path: 'data.customer_id', direction: 'asc', nulls: 'last' },
    { path: 'data.created_at', direction: 'desc', nulls: 'first' }
  ],
  includedColumns: [
    'data.order_total',
    'data.status',
    'data.item_count'
  ],
  indexType: 'btree',
  partial: { column: 'type', op: 'eq', value: { type: 'string', value: 'Order' } }
};
```

### Index-Only Scan Optimization

```typescript
interface QueryPlan {
  type: 'table_scan' | 'index_scan' | 'index_only_scan';
  index?: string;
  columns: string[];
  estimatedCost: number;
}

function planIndexOnlyScan(
  query: Query,
  coveringIndex: CoveringIndexDefinition
): QueryPlan | null {
  const indexedColumns = new Set([
    ...coveringIndex.keyColumns.map(c => c.path),
    ...coveringIndex.includedColumns
  ]);

  // Check if all requested columns are in index
  const allCovered = query.projection.every(col => indexedColumns.has(col));

  if (!allCovered) {
    return null;
  }

  // Check if predicates can use index
  const keyUsability = canUseCompositeIndex(
    query.predicates,
    { columns: coveringIndex.keyColumns }
  );

  if (!keyUsability.usable) {
    return null;
  }

  return {
    type: 'index_only_scan',
    index: coveringIndex.name,
    columns: query.projection,
    estimatedCost: estimateIndexScanCost(coveringIndex, query)
  };
}
```

### Storage Format for Covering Indexes

```
covering_index.parquet
├── index_key: BYTE_ARRAY              # Composite key
├── primary_key: BYTE_ARRAY            # Row identifier
├── row_group_id: INT32                # Source location (for non-covering fallback)
├── row_offset: INT64
├── included_col_1: <type>             # First included column
├── included_col_2: <type>             # Second included column
└── ...
```

---

## Zone Maps / Skip Indexes

### Overview

Zone maps (skip indexes) store min/max statistics per row group, enabling efficient row group pruning.

### Parquet Integration

```typescript
/**
 * Zone map statistics per row group per column.
 * These are extracted from Parquet row group metadata.
 */
interface ZoneMapEntry {
  rowGroupId: number;
  column: string;
  min: Variant;
  max: Variant;
  nullCount: number;
  distinctCount?: number;
  hasBloomFilter: boolean;
}

interface ZoneMapIndex {
  table: string;
  columns: string[];
  entries: Map<number, Map<string, ZoneMapEntry>>;  // rowGroupId -> column -> stats
}
```

### Zone Map Extraction

```typescript
async function extractZoneMaps(
  parquetFile: string
): Promise<ZoneMapEntry[]> {
  const metadata = await readParquetMetadata(parquetFile);
  const entries: ZoneMapEntry[] = [];

  for (const rowGroup of metadata.rowGroups) {
    for (const column of rowGroup.columns) {
      const stats = column.statistics;

      if (!stats) {
        continue;
      }

      entries.push({
        rowGroupId: rowGroup.id,
        column: column.path.join('.'),
        min: deserializeStatValue(stats.min, column.type),
        max: deserializeStatValue(stats.max, column.type),
        nullCount: stats.nullCount,
        distinctCount: stats.distinctCount,
        hasBloomFilter: column.bloomFilterOffset !== undefined
      });
    }
  }

  return entries;
}
```

### Row Group Pruning

```typescript
interface PruningResult {
  includedRowGroups: number[];
  excludedRowGroups: number[];
  pruningRatio: number;
}

function pruneRowGroups(
  zoneMaps: ZoneMapIndex,
  predicates: QueryPredicate[]
): PruningResult {
  const totalRowGroups = new Set(zoneMaps.entries.keys());
  const excluded = new Set<number>();

  for (const [rowGroupId, columnStats] of zoneMaps.entries) {
    for (const predicate of predicates) {
      const stats = columnStats.get(predicate.column);

      if (!stats) {
        continue;
      }

      if (canPruneRowGroup(stats, predicate)) {
        excluded.add(rowGroupId);
        break;
      }
    }
  }

  const included = [...totalRowGroups].filter(rg => !excluded.has(rg));

  return {
    includedRowGroups: included,
    excludedRowGroups: [...excluded],
    pruningRatio: excluded.size / totalRowGroups.size
  };
}

function canPruneRowGroup(
  stats: ZoneMapEntry,
  predicate: QueryPredicate
): boolean {
  switch (predicate.op) {
    case 'eq':
      // Prune if value outside [min, max]
      return variantCompare(predicate.value, stats.min) < 0 ||
             variantCompare(predicate.value, stats.max) > 0;

    case 'gt':
      // Prune if max <= value
      return variantCompare(stats.max, predicate.value) <= 0;

    case 'gte':
      // Prune if max < value
      return variantCompare(stats.max, predicate.value) < 0;

    case 'lt':
      // Prune if min >= value
      return variantCompare(stats.min, predicate.value) >= 0;

    case 'lte':
      // Prune if min > value
      return variantCompare(stats.min, predicate.value) > 0;

    case 'in':
      // Prune if no value in list overlaps [min, max]
      return !predicate.values.some(v =>
        variantCompare(v, stats.min) >= 0 && variantCompare(v, stats.max) <= 0
      );

    case 'is_null':
      // Prune if no nulls
      return stats.nullCount === 0;

    case 'is_not_null':
      // Prune if all nulls
      return stats.nullCount === stats.rowCount;
  }

  return false;
}
```

### Zone Map Storage

```
zone_maps/
├── {namespace}/
│   └── {table}/
│       ├── manifest.json           # Zone map metadata
│       └── stats.parquet           # Columnar statistics storage
```

```
stats.parquet
├── file_path: STRING               # Source Parquet file
├── row_group_id: INT32
├── column_path: STRING
├── type_id: INT8                   # Variant type
├── min_value: BYTE_ARRAY           # Serialized min
├── max_value: BYTE_ARRAY           # Serialized max
├── null_count: INT64
├── distinct_count: INT64           # Optional
├── row_count: INT64
└── bloom_filter_offset: INT64      # If available
```

---

## Index File Format

### Overview

ParqueDB uses Parquet files for index storage, enabling:
- Column pruning for index-only scans
- Compression (ZSTD)
- Predicate pushdown
- Reuse of existing infrastructure

### Common Index File Structure

```
{index_name}/
├── manifest.json                    # Index metadata
├── data/
│   ├── part-0000.parquet           # Index data partitions
│   ├── part-0001.parquet
│   └── ...
├── bloom/
│   └── bloom-{rowGroupId}.bin      # Bloom filters
└── stats/
    └── zone-maps.parquet           # Min/max statistics
```

### Manifest Format

```typescript
interface IndexManifest {
  version: number;
  indexId: string;
  name: string;
  table: string;
  type: 'btree' | 'hash' | 'composite' | 'unique' | 'covering';

  // Column definitions
  keyColumns: IndexColumnDef[];
  includedColumns?: string[];

  // Index configuration
  config: {
    sparse?: boolean;
    partial?: PartialIndexPredicate;
    expression?: IndexExpression;
    unique?: boolean;
    nullsDistinct?: boolean;
  };

  // Storage layout
  files: IndexFile[];
  totalSize: number;
  totalEntries: number;

  // Statistics
  stats: {
    createdAt: string;
    lastUpdated: string;
    buildDuration: number;
    distinctKeys: number;
    avgEntriesPerKey: number;
  };

  // LSM-tree specific (for B-tree indexes)
  levels?: LSMLevel[];

  // Hash-specific
  hashConfig?: {
    bucketCount: number;
    hashFunction: string;
  };
}

interface IndexColumnDef {
  path: string;
  direction?: 'asc' | 'desc';
  nulls?: 'first' | 'last';
  type: 'variant' | 'string' | 'int64' | 'float64' | 'timestamp' | 'boolean';
}

interface IndexFile {
  id: string;
  path: string;
  size: number;
  entryCount: number;
  minKey: string;      // Base64-encoded
  maxKey: string;      // Base64-encoded
  level?: number;      // For LSM-tree
  bucket?: number;     // For hash index
  bloomFilterPath?: string;
}

interface LSMLevel {
  level: number;
  files: string[];
  totalSize: number;
  fileCount: number;
}
```

### Index Data Parquet Schema

```
index_data.parquet
├── index_key: BYTE_ARRAY           # Serialized key (always present)
├── primary_key: BYTE_ARRAY         # Row identifier
├── row_group_id: INT32             # Source row group
├── row_offset: INT64               # Offset in row group
├── [key_col_1]: <type>             # Original key column (for covering)
├── [key_col_2]: <type>
├── [included_1]: <type>            # Included columns (for covering)
└── [included_2]: <type>

Sort order: (index_key, primary_key)
Compression: ZSTD
Row group size: 128 MB (larger for better compression)
```

---

## Index Metadata and Statistics

### Statistics Collection

```typescript
interface IndexStatistics {
  // Cardinality
  totalEntries: number;
  distinctKeys: number;
  avgEntriesPerKey: number;
  maxEntriesPerKey: number;

  // Size
  totalSizeBytes: number;
  avgKeySizeBytes: number;
  avgEntrySizeBytes: number;

  // Distribution
  keyDistribution: HistogramBucket[];
  nullPercentage: number;

  // Performance
  avgLookupLatencyMs: number;
  avgScanLatencyMs: number;
  cacheHitRate: number;

  // Freshness
  lastUpdated: string;
  pendingUpdates: number;
  staleness: number;  // 0.0 - 1.0
}

interface HistogramBucket {
  lowerBound: Variant;
  upperBound: Variant;
  frequency: number;
  distinctCount: number;
}

async function collectIndexStatistics(
  indexName: string
): Promise<IndexStatistics> {
  const manifest = await loadManifest(indexName);
  const stats: IndexStatistics = {
    totalEntries: 0,
    distinctKeys: 0,
    avgEntriesPerKey: 0,
    maxEntriesPerKey: 0,
    totalSizeBytes: 0,
    avgKeySizeBytes: 0,
    avgEntrySizeBytes: 0,
    keyDistribution: [],
    nullPercentage: 0,
    avgLookupLatencyMs: 0,
    avgScanLatencyMs: 0,
    cacheHitRate: 0,
    lastUpdated: new Date().toISOString(),
    pendingUpdates: 0,
    staleness: 0
  };

  // Sample entries for distribution analysis
  const sample = await sampleIndexEntries(indexName, 10000);

  // Compute histogram
  stats.keyDistribution = computeHistogram(sample, 100);

  // Compute cardinality using HyperLogLog
  const hll = new HyperLogLog(14);
  for (const entry of sample) {
    hll.add(entry.key);
  }
  stats.distinctKeys = hll.estimate();

  // Aggregate file statistics
  for (const file of manifest.files) {
    stats.totalEntries += file.entryCount;
    stats.totalSizeBytes += file.size;
  }

  stats.avgEntriesPerKey = stats.totalEntries / stats.distinctKeys;
  stats.avgEntrySizeBytes = stats.totalSizeBytes / stats.totalEntries;

  return stats;
}
```

### Cost Model for Index Selection

```typescript
interface CostModel {
  // Base costs (in arbitrary units)
  seqPageRead: number;      // Sequential page read
  randPageRead: number;     // Random page read
  cpuTupleProcess: number;  // Process one tuple
  cpuIndexLookup: number;   // Index lookup overhead
  cpuHashCompute: number;   // Hash computation

  // System parameters
  pageSize: number;
  bufferPoolSize: number;
  effectiveCacheSize: number;
}

const DEFAULT_COST_MODEL: CostModel = {
  seqPageRead: 1.0,
  randPageRead: 4.0,
  cpuTupleProcess: 0.01,
  cpuIndexLookup: 0.005,
  cpuHashCompute: 0.001,
  pageSize: 8192,
  bufferPoolSize: 256 * 1024 * 1024,
  effectiveCacheSize: 1024 * 1024 * 1024
};

interface ScanCostEstimate {
  indexLookupCost: number;
  pageReadCost: number;
  cpuCost: number;
  totalCost: number;
  estimatedRows: number;
}

function estimateIndexScanCost(
  index: IndexManifest,
  query: Query,
  tableStats: TableStatistics,
  costModel: CostModel = DEFAULT_COST_MODEL
): ScanCostEstimate {
  // Estimate selectivity
  const selectivity = estimateSelectivity(query.predicates, index.stats);
  const estimatedRows = tableStats.rowCount * selectivity;

  // Index lookup cost
  const indexPages = Math.ceil(index.totalSize / costModel.pageSize);
  const indexLevels = Math.ceil(Math.log(indexPages) / Math.log(100)); // ~100 entries per page
  const indexLookupCost = indexLevels * costModel.randPageRead;

  // Page read cost for matching rows
  const dataPages = Math.ceil(estimatedRows / tableStats.rowsPerPage);
  const pageReadCost = dataPages * (
    estimatedRows < 100 ? costModel.randPageRead : costModel.seqPageRead
  );

  // CPU cost
  const cpuCost = estimatedRows * costModel.cpuTupleProcess +
                  indexLookupCost * costModel.cpuIndexLookup;

  return {
    indexLookupCost,
    pageReadCost,
    cpuCost,
    totalCost: indexLookupCost + pageReadCost + cpuCost,
    estimatedRows
  };
}

function estimateSelectivity(
  predicates: QueryPredicate[],
  indexStats: IndexStatistics
): number {
  let selectivity = 1.0;

  for (const predicate of predicates) {
    switch (predicate.op) {
      case 'eq':
        // Assume uniform distribution
        selectivity *= 1 / indexStats.distinctKeys;
        break;

      case 'in':
        selectivity *= predicate.values.length / indexStats.distinctKeys;
        break;

      case 'gt':
      case 'gte':
      case 'lt':
      case 'lte':
        // Use histogram for range selectivity
        selectivity *= estimateRangeSelectivity(predicate, indexStats.keyDistribution);
        break;

      case 'is_null':
        selectivity *= indexStats.nullPercentage;
        break;

      case 'is_not_null':
        selectivity *= 1 - indexStats.nullPercentage;
        break;
    }
  }

  return Math.max(selectivity, 1 / indexStats.totalEntries);
}
```

---

## Query Planning and Index Selection

### Query Planner Architecture

```typescript
interface QueryPlanner {
  plan(query: Query): Promise<QueryPlan>;
}

interface QueryPlan {
  type: 'table_scan' | 'index_scan' | 'index_only_scan' | 'bitmap_scan';
  index?: string;
  scanDirection?: 'forward' | 'backward';
  predicates: PushedPredicate[];
  projection: string[];
  estimatedCost: number;
  estimatedRows: number;
  children?: QueryPlan[];
}

interface PushedPredicate {
  column: string;
  op: string;
  value: Variant;
  pushedToIndex: boolean;
  pushedToParquet: boolean;
}
```

### Index Selection Algorithm

```typescript
async function selectBestIndex(
  query: Query,
  availableIndexes: IndexManifest[],
  tableStats: TableStatistics
): Promise<{ index: IndexManifest | null; plan: QueryPlan }> {
  const candidates: Array<{ index: IndexManifest; plan: QueryPlan; cost: number }> = [];

  // Evaluate each index
  for (const index of availableIndexes) {
    const applicability = evaluateIndexApplicability(query, index);

    if (!applicability.applicable) {
      continue;
    }

    const cost = estimateIndexScanCost(index, query, tableStats);

    // Check for index-only scan opportunity
    const isIndexOnly = canDoIndexOnlyScan(query, index);

    candidates.push({
      index,
      plan: {
        type: isIndexOnly ? 'index_only_scan' : 'index_scan',
        index: index.name,
        predicates: applicability.pushedPredicates,
        projection: query.projection,
        estimatedCost: cost.totalCost,
        estimatedRows: cost.estimatedRows
      },
      cost: cost.totalCost
    });
  }

  // Consider table scan
  const tableScanCost = estimateTableScanCost(query, tableStats);
  candidates.push({
    index: null,
    plan: {
      type: 'table_scan',
      predicates: query.predicates.map(p => ({ ...p, pushedToParquet: true })),
      projection: query.projection,
      estimatedCost: tableScanCost.totalCost,
      estimatedRows: tableScanCost.estimatedRows
    },
    cost: tableScanCost.totalCost
  });

  // Select minimum cost
  candidates.sort((a, b) => a.cost - b.cost);

  return {
    index: candidates[0].index,
    plan: candidates[0].plan
  };
}

interface IndexApplicability {
  applicable: boolean;
  usableColumns: number;
  pushedPredicates: PushedPredicate[];
  reason?: string;
}

function evaluateIndexApplicability(
  query: Query,
  index: IndexManifest
): IndexApplicability {
  // Check partial index predicate
  if (index.config.partial) {
    if (!queryImpliesPredicate(query.predicates, index.config.partial)) {
      return {
        applicable: false,
        usableColumns: 0,
        pushedPredicates: [],
        reason: 'Query does not satisfy partial index predicate'
      };
    }
  }

  // Match query predicates to index columns
  let usableColumns = 0;
  const pushedPredicates: PushedPredicate[] = [];

  for (const indexCol of index.keyColumns) {
    const matchingPred = query.predicates.find(p => p.column === indexCol.path);

    if (!matchingPred) {
      break;  // Index columns must be used in order
    }

    // Range predicates can only be on last used column
    if (['gt', 'gte', 'lt', 'lte'].includes(matchingPred.op)) {
      usableColumns++;
      pushedPredicates.push({ ...matchingPred, pushedToIndex: true, pushedToParquet: false });
      break;
    }

    // Equality predicates can continue to next column
    if (matchingPred.op === 'eq' || matchingPred.op === 'in') {
      usableColumns++;
      pushedPredicates.push({ ...matchingPred, pushedToIndex: true, pushedToParquet: false });
      continue;
    }

    break;
  }

  return {
    applicable: usableColumns > 0,
    usableColumns,
    pushedPredicates
  };
}
```

### Bitmap Scan Planning

```typescript
/**
 * Bitmap scan: combine multiple indexes using bitmap AND/OR
 */
interface BitmapScanPlan extends QueryPlan {
  type: 'bitmap_scan';
  bitmapNodes: BitmapNode[];
}

type BitmapNode =
  | { type: 'index_bitmap'; index: string; predicate: QueryPredicate }
  | { type: 'bitmap_and'; children: BitmapNode[] }
  | { type: 'bitmap_or'; children: BitmapNode[] };

function planBitmapScan(
  query: Query,
  indexes: IndexManifest[]
): BitmapScanPlan | null {
  // Group predicates by applicable index
  const predicateIndexes = new Map<QueryPredicate, IndexManifest[]>();

  for (const pred of query.predicates) {
    const applicable = indexes.filter(idx =>
      idx.keyColumns[0]?.path === pred.column
    );
    if (applicable.length > 0) {
      predicateIndexes.set(pred, applicable);
    }
  }

  if (predicateIndexes.size < 2) {
    return null;  // Need multiple predicates for bitmap scan to be worthwhile
  }

  // Build bitmap tree
  const bitmapNodes: BitmapNode[] = [];

  for (const [pred, idxs] of predicateIndexes) {
    // Select best index for this predicate
    const bestIdx = idxs[0];  // Simplified; would use cost estimation
    bitmapNodes.push({
      type: 'index_bitmap',
      index: bestIdx.name,
      predicate: pred
    });
  }

  // Combine with AND (all predicates must match)
  const combinedBitmap: BitmapNode = bitmapNodes.length === 1
    ? bitmapNodes[0]
    : { type: 'bitmap_and', children: bitmapNodes };

  return {
    type: 'bitmap_scan',
    bitmapNodes: [combinedBitmap],
    predicates: query.predicates.map(p => ({
      ...p,
      pushedToIndex: predicateIndexes.has(p),
      pushedToParquet: !predicateIndexes.has(p)
    })),
    projection: query.projection,
    estimatedCost: estimateBitmapScanCost(combinedBitmap, query),
    estimatedRows: 0  // Computed during execution
  };
}
```

---

## Index Maintenance

### Write Path Integration

```typescript
interface IndexUpdateBatch {
  inserts: IndexEntry[];
  updates: Array<{ old: IndexEntry; new: IndexEntry }>;
  deletes: IndexEntry[];
}

async function updateIndexes(
  table: string,
  changes: RowChange[]
): Promise<void> {
  const indexes = await getIndexesForTable(table);

  for (const index of indexes) {
    const batch = buildIndexUpdateBatch(index, changes);

    if (batch.inserts.length === 0 &&
        batch.updates.length === 0 &&
        batch.deletes.length === 0) {
      continue;
    }

    await applyIndexUpdates(index, batch);
  }
}

function buildIndexUpdateBatch(
  index: IndexManifest,
  changes: RowChange[]
): IndexUpdateBatch {
  const batch: IndexUpdateBatch = {
    inserts: [],
    updates: [],
    deletes: []
  };

  for (const change of changes) {
    // Check partial predicate
    const oldMatches = change.before && evaluatePartial(index, change.before);
    const newMatches = change.after && evaluatePartial(index, change.after);

    if (!oldMatches && newMatches) {
      // New row matches index predicate
      batch.inserts.push(buildIndexEntry(index, change.after!));
    } else if (oldMatches && !newMatches) {
      // Row no longer matches
      batch.deletes.push(buildIndexEntry(index, change.before!));
    } else if (oldMatches && newMatches) {
      // Check if indexed columns changed
      const oldKey = extractIndexKey(index, change.before!);
      const newKey = extractIndexKey(index, change.after!);

      if (!bytesEqual(oldKey, newKey)) {
        batch.updates.push({
          old: buildIndexEntry(index, change.before!),
          new: buildIndexEntry(index, change.after!)
        });
      } else if (index.includedColumns) {
        // Check if included columns changed
        const includedChanged = index.includedColumns.some(col =>
          !variantEquals(
            extractPath(change.before!, col),
            extractPath(change.after!, col)
          )
        );

        if (includedChanged) {
          batch.updates.push({
            old: buildIndexEntry(index, change.before!),
            new: buildIndexEntry(index, change.after!)
          });
        }
      }
    }
  }

  return batch;
}
```

### Incremental Index Updates

```typescript
async function applyIndexUpdates(
  index: IndexManifest,
  batch: IndexUpdateBatch
): Promise<void> {
  if (index.type === 'btree') {
    // Write to WAL / memtable
    await appendToIndexWAL(index.name, batch);

    // Trigger compaction if needed
    if (await shouldCompact(index.name)) {
      await scheduleCompaction(index.name);
    }
  } else if (index.type === 'hash') {
    // Direct bucket updates
    for (const entry of batch.inserts) {
      await hashInsert(index.name, entry.key, entry);
    }

    for (const { old, new: newEntry } of batch.updates) {
      await hashDelete(index.name, old.key, old.primaryKey);
      await hashInsert(index.name, newEntry.key, newEntry);
    }

    for (const entry of batch.deletes) {
      await hashDelete(index.name, entry.key, entry.primaryKey);
    }
  }

  // Update manifest statistics
  await updateIndexStats(index.name, {
    entriesAdded: batch.inserts.length,
    entriesRemoved: batch.deletes.length,
    entriesUpdated: batch.updates.length
  });
}
```

### Index Rebuild

```typescript
interface RebuildOptions {
  parallel?: number;        // Parallel workers
  checkpoint?: boolean;     // Create checkpoint before rebuild
  online?: boolean;         // Allow queries during rebuild
  dropExisting?: boolean;   // Drop old index before rebuild
}

async function rebuildIndex(
  indexName: string,
  options: RebuildOptions = {}
): Promise<RebuildResult> {
  const definition = await loadIndexDefinition(indexName);
  const startTime = Date.now();

  // Create checkpoint if requested
  if (options.checkpoint) {
    await createIndexCheckpoint(indexName);
  }

  // Build new index in temporary location
  const tempName = `${indexName}_rebuild_${Date.now()}`;

  let entriesProcessed = 0;
  const newEntries: IndexEntry[] = [];

  // Scan table and build entries
  for await (const rowGroup of scanTable(definition.table, {
    parallel: options.parallel
  })) {
    for (const row of rowGroup) {
      // Check partial predicate
      if (definition.partial && !evaluatePredicate(row, definition.partial)) {
        continue;
      }

      // Evaluate expression or extract columns
      const key = definition.expression
        ? evaluateExpression(row, definition.expression)
        : extractCompositeKey(row, definition.keyColumns);

      // Handle sparse index
      if (definition.sparse && (key === null || key.type === 'null')) {
        continue;
      }

      newEntries.push({
        key: serializeIndexKey(key),
        primaryKey: row.primary_key,
        rowGroupId: rowGroup.id,
        rowOffset: row.offset,
        included: definition.includedColumns?.map(col => extractPath(row, col))
      });

      entriesProcessed++;
    }
  }

  // Sort and write index files
  if (definition.type === 'btree') {
    newEntries.sort((a, b) => compareBytes(a.key, b.key));
    await writeBTreeIndex(tempName, newEntries);
  } else {
    await writeHashIndex(tempName, newEntries, definition.hashConfig);
  }

  // Atomic swap
  if (options.dropExisting) {
    await dropIndex(indexName);
  }
  await renameIndex(tempName, indexName);

  // Update manifest
  await updateManifest(indexName, {
    lastRebuilt: new Date().toISOString(),
    rebuildDuration: Date.now() - startTime,
    totalEntries: entriesProcessed
  });

  return {
    entriesProcessed,
    duration: Date.now() - startTime,
    newSize: await getIndexSize(indexName)
  };
}
```

### Compaction

```typescript
interface CompactionTask {
  indexName: string;
  level: number;
  inputFiles: string[];
  priority: 'low' | 'normal' | 'high';
}

class CompactionScheduler {
  private queue: CompactionTask[] = [];
  private running: Set<string> = new Set();

  async schedule(task: CompactionTask): Promise<void> {
    // Avoid duplicate compaction for same index/level
    const key = `${task.indexName}:${task.level}`;
    if (this.running.has(key)) {
      return;
    }

    this.queue.push(task);
    this.queue.sort((a, b) => {
      const priorityOrder = { high: 0, normal: 1, low: 2 };
      return priorityOrder[a.priority] - priorityOrder[b.priority];
    });

    await this.processQueue();
  }

  private async processQueue(): Promise<void> {
    while (this.queue.length > 0) {
      const task = this.queue.shift()!;
      const key = `${task.indexName}:${task.level}`;

      this.running.add(key);

      try {
        await this.runCompaction(task);
      } finally {
        this.running.delete(key);
      }
    }
  }

  private async runCompaction(task: CompactionTask): Promise<void> {
    const manifest = await loadManifest(task.indexName);

    // Read and merge input files
    const entries: IndexEntry[] = [];
    for (const file of task.inputFiles) {
      const fileEntries = await readIndexFile(file);
      entries.push(...fileEntries);
    }

    // Sort and deduplicate
    entries.sort((a, b) => {
      const keyCompare = compareBytes(a.key, b.key);
      if (keyCompare !== 0) return keyCompare;
      return compareBytes(a.primaryKey, b.primaryKey);
    });

    const deduplicated = deduplicateEntries(entries);

    // Write new file at target level
    const newFile = await writeSST(deduplicated, task.level);

    // Update manifest atomically
    await updateManifest(task.indexName, {
      remove: task.inputFiles,
      add: [newFile],
      level: task.level
    });

    // Delete old files
    for (const file of task.inputFiles) {
      await deleteIndexFile(file);
    }
  }
}
```

---

## Implementation Details

### TypeScript Type Definitions

```typescript
// /src/indexes/types.ts

import { Variant, Namespace, EntityId, Timestamp } from '../graph-schemas';

/**
 * Index entry stored in index files
 */
export interface IndexEntry {
  /** Serialized index key */
  key: Uint8Array;
  /** Primary key of source row */
  primaryKey: Uint8Array;
  /** Source row group ID */
  rowGroupId: number;
  /** Offset within row group */
  rowOffset: number;
  /** Array index (for array indexes) */
  arrayIndex?: number;
  /** Included column values (for covering indexes) */
  included?: Variant[];
}

/**
 * Index definition (stored in manifest)
 */
export type IndexDefinition =
  | BTreeIndexDefinition
  | HashIndexDefinition
  | CompositeIndexDefinition
  | UniqueIndexDefinition
  | CoveringIndexDefinition
  | PartialIndexDefinition
  | ExpressionIndexDefinition
  | SparseIndexDefinition
  | NestedPathIndexDefinition
  | ArrayIndexDefinition;

export interface BTreeIndexDefinition {
  type: 'btree';
  name: string;
  table: string;
  column: string;
  direction?: 'asc' | 'desc';
  nulls?: 'first' | 'last';
}

export interface HashIndexDefinition {
  type: 'hash';
  name: string;
  table: string;
  column: string;
  bucketCount?: number;
  hashFunction?: 'xxhash64' | 'murmur3' | 'fnv1a';
}

// ... additional definition types as shown above

/**
 * Query predicate for index selection
 */
export interface QueryPredicate {
  column: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'is_null' | 'is_not_null' | 'like' | 'contains';
  value?: Variant;
  values?: Variant[];
}

/**
 * Query for planning
 */
export interface Query {
  table: string;
  predicates: QueryPredicate[];
  projection: string[];
  orderBy?: Array<{ column: string; direction: 'asc' | 'desc' }>;
  limit?: number;
  offset?: number;
}

/**
 * Index operation result
 */
export interface IndexOperationResult {
  success: boolean;
  entriesAffected: number;
  duration: number;
  error?: string;
}
```

### Index Manager Implementation

```typescript
// /src/indexes/IndexManager.ts

import { parquetRead, parquetWrite } from 'hyparquet';
import { IndexDefinition, IndexEntry, Query, QueryPlan } from './types';

export class IndexManager {
  private bucket: R2Bucket;
  private namespace: string;
  private manifestCache: Map<string, IndexManifest> = new Map();
  private compactionScheduler: CompactionScheduler;

  constructor(bucket: R2Bucket, namespace: string) {
    this.bucket = bucket;
    this.namespace = namespace;
    this.compactionScheduler = new CompactionScheduler();
  }

  /**
   * Create a new index
   */
  async createIndex(definition: IndexDefinition): Promise<void> {
    // Validate definition
    this.validateDefinition(definition);

    // Check for existing index
    if (await this.indexExists(definition.name)) {
      throw new Error(`Index ${definition.name} already exists`);
    }

    // Build initial index
    await this.buildIndex(definition);
  }

  /**
   * Drop an index
   */
  async dropIndex(name: string): Promise<void> {
    const manifest = await this.loadManifest(name);

    // Delete all index files
    for (const file of manifest.files) {
      await this.bucket.delete(file.path);
    }

    // Delete manifest
    await this.bucket.delete(this.manifestPath(name));

    // Clear cache
    this.manifestCache.delete(name);
  }

  /**
   * Query an index
   */
  async query(
    indexName: string,
    predicates: QueryPredicate[],
    options?: { limit?: number; offset?: number }
  ): Promise<IndexEntry[]> {
    const manifest = await this.loadManifest(indexName);

    if (manifest.type === 'hash') {
      return this.queryHashIndex(manifest, predicates, options);
    } else {
      return this.queryBTreeIndex(manifest, predicates, options);
    }
  }

  /**
   * Select best index for a query
   */
  async planQuery(query: Query): Promise<QueryPlan> {
    const indexes = await this.listIndexes(query.table);
    const tableStats = await this.getTableStats(query.table);

    const { index, plan } = await selectBestIndex(query, indexes, tableStats);

    return plan;
  }

  /**
   * Update indexes after row changes
   */
  async onRowChange(table: string, changes: RowChange[]): Promise<void> {
    const indexes = await this.listIndexes(table);

    for (const index of indexes) {
      const batch = buildIndexUpdateBatch(index, changes);
      if (batch.inserts.length > 0 || batch.updates.length > 0 || batch.deletes.length > 0) {
        await applyIndexUpdates(index, batch);
      }
    }
  }

  // Private helper methods...

  private async loadManifest(name: string): Promise<IndexManifest> {
    if (this.manifestCache.has(name)) {
      return this.manifestCache.get(name)!;
    }

    const data = await this.bucket.get(this.manifestPath(name));
    if (!data) {
      throw new Error(`Index ${name} not found`);
    }

    const manifest = JSON.parse(await data.text()) as IndexManifest;
    this.manifestCache.set(name, manifest);

    return manifest;
  }

  private manifestPath(name: string): string {
    return `indexes/${this.namespace}/${name}/manifest.json`;
  }

  private validateDefinition(definition: IndexDefinition): void {
    if (!definition.name || !definition.table) {
      throw new Error('Index name and table are required');
    }

    // Additional validation based on type...
  }
}
```

### Integration with Query Engine

```typescript
// /src/query/QueryExecutor.ts

import { IndexManager } from '../indexes/IndexManager';
import { Query, QueryPlan } from '../indexes/types';

export class QueryExecutor {
  private indexManager: IndexManager;

  constructor(indexManager: IndexManager) {
    this.indexManager = indexManager;
  }

  async execute(query: Query): Promise<QueryResult> {
    // Plan query (select indexes)
    const plan = await this.indexManager.planQuery(query);

    // Execute plan
    switch (plan.type) {
      case 'table_scan':
        return this.executeTableScan(query, plan);

      case 'index_scan':
        return this.executeIndexScan(query, plan);

      case 'index_only_scan':
        return this.executeIndexOnlyScan(query, plan);

      case 'bitmap_scan':
        return this.executeBitmapScan(query, plan as BitmapScanPlan);

      default:
        throw new Error(`Unknown plan type: ${plan.type}`);
    }
  }

  private async executeIndexScan(
    query: Query,
    plan: QueryPlan
  ): Promise<QueryResult> {
    // Query index for matching primary keys
    const indexEntries = await this.indexManager.query(
      plan.index!,
      plan.predicates.filter(p => p.pushedToIndex),
      { limit: query.limit }
    );

    // Fetch full rows by primary key
    const rows = await this.fetchRows(
      query.table,
      indexEntries.map(e => e.primaryKey),
      query.projection
    );

    // Apply remaining predicates
    const filtered = rows.filter(row =>
      plan.predicates
        .filter(p => !p.pushedToIndex)
        .every(p => evaluatePredicate(row, p))
    );

    return {
      rows: filtered,
      rowsScanned: indexEntries.length,
      indexUsed: plan.index
    };
  }

  private async executeIndexOnlyScan(
    query: Query,
    plan: QueryPlan
  ): Promise<QueryResult> {
    // All data comes from index - no table fetch needed
    const indexEntries = await this.indexManager.query(
      plan.index!,
      plan.predicates,
      { limit: query.limit }
    );

    // Project from index entries directly
    const rows = indexEntries.map(entry => {
      const row: Record<string, Variant> = {};
      // Map included columns to projection
      // ...
      return row;
    });

    return {
      rows,
      rowsScanned: 0,  // No table rows scanned
      indexUsed: plan.index,
      indexOnly: true
    };
  }

  // Additional execution methods...
}
```

---

## Summary

This secondary indexing system provides:

1. **B-Tree Style Indexes** - LSM-tree based SSTs for efficient range queries
2. **Hash Indexes** - O(1) equality lookups with dynamic rehashing
3. **Composite Indexes** - Multi-column ordered indexes with prefix matching
4. **Partial Indexes** - Filtered indexes for subset queries
5. **Expression Indexes** - Computed value indexes
6. **Sparse Indexes** - Storage-efficient optional field indexes
7. **Nested Path Indexes** - Deep Variant field indexing
8. **Array Element Indexes** - Multi-entry array content indexes
9. **Unique Indexes** - Constraint enforcement with deferred checking
10. **Covering Indexes** - Index-only scans
11. **Zone Maps** - Row group pruning statistics

The design leverages Parquet's columnar format for index storage, enabling:
- Column pruning
- Predicate pushdown
- Efficient compression
- Reuse of hyparquet infrastructure

Query planning uses cost-based optimization with:
- Selectivity estimation
- Cardinality statistics
- Index applicability analysis
- Bitmap scan combination

Index maintenance supports:
- Incremental updates
- Background compaction
- Online rebuilds
- Atomic manifest updates

---

*Architecture Design Document - ParqueDB Secondary Indexing System*
