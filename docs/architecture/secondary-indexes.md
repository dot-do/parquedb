---
title: Secondary Indexes
description: Comprehensive secondary indexing system for ParqueDB including B-tree indexes, hash indexes, composite indexes, partial indexes, expression indexes, covering indexes, and zone maps with Parquet-native integration.
---

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
```

---

## Hash Indexes

Hash indexes provide O(1) equality lookups by distributing entries across buckets:

```typescript
interface HashIndexConfig {
  name: string;
  column: string;
  bucketCount: number;  // Power of 2, e.g., 256
  hashFunction: 'xxhash64' | 'murmur3';
}

// Structure:
// indexes/{ns}/{table}/{index_name}/
// ├── bucket-000.parquet
// ├── bucket-001.parquet
// └── ...bucket-255.parquet
```

### Lookup Algorithm

```typescript
async function hashLookup(
  bucket: R2Bucket,
  index: HashIndexConfig,
  key: Variant
): Promise<string[]> {
  // 1. Hash the key to find bucket
  const hash = xxhash64(serializeIndexKey(key));
  const bucketId = Number(hash % BigInt(index.bucketCount));

  // 2. Read the bucket file
  const bucketPath = `indexes/${ns}/${table}/${index.name}/bucket-${bucketId.toString().padStart(3, '0')}.parquet`;
  const entries = await parquetScan(bucket, bucketPath, {
    filter: { index_key: { eq: serializeIndexKey(key) } }
  });

  // 3. Return primary keys
  return entries.map(e => e.primary_key);
}
```

---

## Composite/Compound Indexes

Composite indexes support multi-column queries with prefix matching:

```typescript
interface CompositeIndexConfig {
  name: string;
  columns: string[];  // Order matters!
  // Example: ['type', 'created_at', 'id']
}

// Key format: concatenation of serialized column values
// Enables prefix queries: type only, type+created_at, or all three
```

---

## Partial Indexes

Partial indexes only include rows matching a predicate, reducing storage and improving query speed:

```typescript
interface PartialIndexConfig {
  name: string;
  column: string;
  predicate: Filter;  // Only index rows matching this
}

// Example: Index only active users
const activeUsersIndex: PartialIndexConfig = {
  name: 'active_users_email',
  column: 'data.email',
  predicate: { 'data.status': { eq: 'active' } }
};
```

---

## Expression Indexes

Index computed values for frequently-used transformations:

```typescript
interface ExpressionIndexConfig {
  name: string;
  expression: string;  // SQL-like expression
  // Example: "LOWER(data.email)"
}
```

---

## Zone Maps / Skip Indexes

Parquet automatically provides zone maps (min/max statistics per row group). ParqueDB enhances this with:

```typescript
interface ZoneMapStats {
  rowGroup: number;
  column: string;
  min: Variant;
  max: Variant;
  nullCount: bigint;
  distinctCount?: bigint;  // HyperLogLog estimate
}
```

---

## Index Maintenance

### Incremental Updates

When entities are created/updated/deleted:

1. **Identify affected indexes** - Which indexes cover the changed columns?
2. **Compute new keys** - Extract values from new state
3. **Append to level-0** - Write new index entries
4. **Background compaction** - Merge SST files periodically

### Compaction Strategy

```typescript
interface CompactionConfig {
  level0FileThreshold: number;  // Compact when L0 has this many files
  levelMultiplier: number;      // Each level is N times larger
  maxLevels: number;
}

// Default: L0 max 4 files, 10x multiplier, 4 levels
// L0: 4 x 64MB = 256MB (unsorted)
// L1: 640MB (sorted)
// L2: 6.4GB (sorted)
// L3: 64GB (sorted)
```

---

## Query Planning and Index Selection

The query planner evaluates available indexes and selects the optimal access path:

```typescript
interface QueryPlan {
  estimatedCost: number;
  steps: Array<{
    type: 'index_scan' | 'full_scan' | 'bloom_check';
    index?: string;
    predicate?: Filter;
    estimatedRows: number;
  }>;
}

function selectIndex(
  query: ParsedQuery,
  availableIndexes: IndexMetadata[]
): QueryPlan {
  const candidates: QueryPlan[] = [];

  for (const index of availableIndexes) {
    if (canUseIndex(query, index)) {
      const plan = buildIndexPlan(query, index);
      candidates.push(plan);
    }
  }

  // Also consider full scan
  candidates.push(buildFullScanPlan(query));

  // Return lowest cost plan
  return candidates.sort((a, b) => a.estimatedCost - b.estimatedCost)[0];
}
```

---

## Implementation Details

### Index Creation

```typescript
async function createIndex(
  bucket: R2Bucket,
  ns: string,
  table: string,
  config: IndexConfig
): Promise<void> {
  // 1. Write index metadata
  await writeIndexManifest(bucket, ns, table, config);

  // 2. Scan existing data and build initial index
  const dataFiles = await listDataFiles(bucket, ns, table);

  for (const file of dataFiles) {
    const rows = await parquetScan(bucket, file.path);
    const indexEntries = rows.map(row => ({
      index_key: extractIndexKey(row, config),
      primary_key: row.id,
      row_group_id: file.rowGroup,
      row_offset: row._offset
    }));

    await writeIndexSST(bucket, ns, table, config.name, indexEntries);
  }
}
```

### Index Lookup

```typescript
async function indexLookup(
  bucket: R2Bucket,
  ns: string,
  table: string,
  indexName: string,
  predicate: IndexPredicate
): Promise<string[]> {
  const manifest = await readIndexManifest(bucket, ns, table, indexName);

  // Search all levels, merging results
  const results: string[] = [];

  for (const level of manifest.levels) {
    for (const sst of level.files) {
      // Use zone maps to skip irrelevant files
      if (!overlaps(sst.minKey, sst.maxKey, predicate)) continue;

      const entries = await parquetScan(bucket, sst.path, {
        filter: predicateToFilter(predicate)
      });

      results.push(...entries.map(e => e.primary_key));
    }
  }

  return results;
}
```

---

*Design Document - ParqueDB Secondary Indexing System*
