---
title: Bloom Filter Indexes
description: Probabilistic data structures for efficient existence checks and set membership in ParqueDB, including standard Bloom filters, counting Bloom filters, cuckoo filters, and edge existence optimization.
---

**Design Document**: Probabilistic data structures for efficient existence checks and set membership

This document details the bloom filter indexing strategy for ParqueDB, enabling fast existence checks for entities, edges, and compound keys without scanning row groups.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Bloom Filter Fundamentals](#bloom-filter-fundamentals)
3. [Parquet Built-in vs External Bloom Filters](#parquet-built-in-vs-external-bloom-filters)
4. [Bloom Filter Hierarchy](#bloom-filter-hierarchy)
5. [Sizing Formulas and Tradeoffs](#sizing-formulas-and-tradeoffs)
6. [Counting Bloom Filters for Deletes](#counting-bloom-filters-for-deletes)
7. [Cuckoo Filters as Alternative](#cuckoo-filters-as-alternative)
8. [Blocked Bloom Filters for Cache Efficiency](#blocked-bloom-filters-for-cache-efficiency)
9. [Compound Key Bloom Filters](#compound-key-bloom-filters)
10. [Edge Existence Bloom Filters](#edge-existence-bloom-filters)
11. [Index File Format](#index-file-format)
12. [Write Path Integration](#write-path-integration)
13. [Query Optimization](#query-optimization)
14. [Integration with hyparquet](#integration-with-hyparquet)
15. [Implementation](#implementation)

---

## Executive Summary

ParqueDB uses probabilistic data structures to accelerate existence checks across its graph-first architecture:

| Structure | Use Case | False Positive Rate | Delete Support |
|-----------|----------|---------------------|----------------|
| Standard Bloom | Entity existence, edge lookup | 1% | No |
| Counting Bloom | Mutable datasets with deletes | 1% | Yes |
| Cuckoo Filter | High-cardinality sets | 3% | Yes |
| Blocked Bloom | Cache-optimized scans | 1% | No |

**Key Benefits**:
- Skip 90%+ of row groups for point lookups
- O(1) edge existence checks before graph traversal
- Sub-millisecond compound key verification
- Memory-efficient set membership for namespace/type filtering

---

## Bloom Filter Fundamentals

### How Bloom Filters Work

A Bloom filter is a space-efficient probabilistic data structure that tests whether an element is a member of a set. It can produce false positives but never false negatives.

```
+------------------------------------------------------------------+
|                    Bloom Filter (m bits)                          |
|  0  0  1  0  0  1  0  0  1  1  0  0  1  0  0  1  0  0  1  0      |
|        ^        ^        ^                    ^                    |
|        |        |        |                    |                    |
|        +--------+--------+--------------------+                    |
|                    hash functions (k=4)                            |
|                          ^                                         |
|                          |                                         |
|                    "user:12345"                                    |
+------------------------------------------------------------------+
```

### Core Properties

```typescript
interface BloomFilterProperties {
  // Number of bits in the filter
  m: number;

  // Number of hash functions
  k: number;

  // Number of elements inserted
  n: number;

  // False positive probability
  // p = (1 - e^(-kn/m))^k
  falsePositiveRate: number;
}
```

### Membership Test Logic

```typescript
function contains(filter: Uint8Array, key: string, k: number): boolean {
  const hashes = computeHashes(key, k);

  for (const hash of hashes) {
    const bitIndex = hash % (filter.length * 8);
    const byteIndex = Math.floor(bitIndex / 8);
    const bitOffset = bitIndex % 8;

    if ((filter[byteIndex] & (1 << bitOffset)) === 0) {
      return false; // Definitely not in set
    }
  }

  return true; // Probably in set (may be false positive)
}
```

---

## Parquet Built-in vs External Bloom Filters

### Parquet Native Bloom Filters (Since Parquet 2.0)

Parquet supports column-level bloom filters stored in the file footer:

```
+-----------------------------------------------------+
|                  Parquet File                        |
+----------------------------------------------------- +
|  Row Group 0                                         |
|    Column Chunk: id                                  |
|      Data Pages                                      |
|    Column Chunk: ns                                  |
|      Data Pages                                      |
+-----------------------------------------------------+
|  Row Group 1                                         |
|    ...                                               |
+-----------------------------------------------------+
|  File Footer                                         |
|    Schema                                            |
|    Row Group Metadata                                |
|    +---------------------------------------------+   |
|    |  Bloom Filter Headers                       |   |
|    |    Column: id                               |   |
|    |      Row Group 0: offset=1234, length=4096  |   |
|    |      Row Group 1: offset=5330, length=4096  |   |
|    +---------------------------------------------+   |
|    +---------------------------------------------+   |
|    |  Bloom Filter Data (after footer)           |   |
|    |    [Filter bytes for id, RG0]               |   |
|    |    [Filter bytes for id, RG1]               |   |
|    +---------------------------------------------+   |
+-----------------------------------------------------+
```

**Parquet Bloom Filter Specification**:
```typescript
interface ParquetBloomFilterHeader {
  // Number of bytes in the filter
  numBytes: number;

  // Hash algorithm (always XXHASH in Parquet)
  algorithm: 'XXHASH';

  // Hash strategy (always MURMUR3_X64_128)
  hash: 'MURMUR3_X64_128';

  // Compression (UNCOMPRESSED or ZSTD)
  compression: 'UNCOMPRESSED' | 'ZSTD';
}
```

### External Bloom Filter Files

For ParqueDB, we use **external bloom filter files** in addition to Parquet's built-in filters:

```
/warehouse/{namespace}/
├── nodes/
│   ├── data/
│   │   └── {uuid}.parquet
│   └── bloom/
│       ├── entity.bloom           # Global entity existence
│       ├── {uuid}.id.bloom        # Per-file id bloom
│       └── {uuid}.compound.bloom  # Per-file ns+id bloom
├── edges/
│   └── bloom/
│       ├── edge.bloom             # Global edge existence
│       └── {uuid}.edge.bloom      # Per-file edge bloom
```

### Decision Matrix: Built-in vs External

| Aspect | Parquet Built-in | External Bloom Files |
|--------|------------------|---------------------|
| Column filtering | Per-column in footer | Flexible compound keys |
| Update granularity | Rewrite entire file | Update in place |
| Query integration | Automatic with readers | Manual integration |
| Cross-file queries | Must read each footer | Single global filter |
| Deletion support | No | Yes (with counting) |
| Custom hash functions | No (XXHASH only) | Yes |
| Compression | Limited | Full control |

**Recommendation**: Use both:
- **Parquet built-in**: For column-level filtering within row groups (automatic with hyparquet)
- **External files**: For global existence checks, compound keys, and edge lookups

---

## Bloom Filter Hierarchy

ParqueDB uses a three-level bloom filter hierarchy:

```
Level 1: Global (per namespace)
├── entity.bloom        # All entity IDs in namespace
├── edge.bloom          # All edge keys (from|rel|to)
└── type.bloom          # All entity types

Level 2: Per-File
├── {file}.id.bloom     # Entity IDs in this file
├── {file}.edge.bloom   # Edges in this file
└── {file}.type.bloom   # Types in this file

Level 3: Per-Row-Group (embedded in Parquet)
└── Column bloom filters in file footer
```

### Query Flow with Hierarchy

```typescript
async function entityExists(ns: string, id: string): Promise<boolean> {
  // Level 1: Check global bloom
  const globalBloom = await loadBloom(`${ns}/bloom/entity.bloom`);
  if (!globalBloom.mightContain(id)) {
    return false; // Definitely doesn't exist
  }

  // Level 2: Find candidate files
  const fileManifest = await loadManifest(ns);
  const candidateFiles: string[] = [];

  for (const file of fileManifest.files) {
    const fileBloom = await loadBloom(`${ns}/bloom/${file.id}.id.bloom`);
    if (fileBloom.mightContain(id)) {
      candidateFiles.push(file.path);
    }
  }

  // Level 3: Scan candidate files (Parquet built-in bloom used automatically)
  for (const filePath of candidateFiles) {
    const result = await parquetQuery(filePath, {
      filter: { id: { eq: id } },
      limit: 1
    });
    if (result.length > 0) return true;
  }

  return false; // False positive at some level
}
```

---

## Sizing Formulas and Tradeoffs

### Optimal Parameters

Given desired false positive rate `p` and expected elements `n`:

```typescript
function calculateBloomParams(n: number, p: number): BloomFilterParams {
  // Optimal number of bits
  const m = Math.ceil(-n * Math.log(p) / (Math.LN2 ** 2));

  // Optimal number of hash functions
  const k = Math.round((m / n) * Math.LN2);

  // Actual false positive rate
  const actualP = Math.pow(1 - Math.exp(-k * n / m), k);

  return {
    numBits: m,
    numHashes: k,
    numBytes: Math.ceil(m / 8),
    expectedFPR: actualP,
    bitsPerElement: m / n
  };
}

// Example: 1M elements, 1% FPR
// m = 9,585,059 bits (~1.2 MB)
// k = 7 hash functions
// 9.6 bits per element
```

### Size vs FPR Tradeoffs

| Elements | FPR | Size | Bits/Element |
|----------|-----|------|--------------|
| 1M | 10% | 600 KB | 4.8 |
| 1M | 1% | 1.2 MB | 9.6 |
| 1M | 0.1% | 1.8 MB | 14.4 |
| 10M | 1% | 12 MB | 9.6 |
| 100M | 1% | 120 MB | 9.6 |

---

## Counting Bloom Filters for Deletes

Standard Bloom filters don't support deletion. Counting Bloom filters use counters instead of bits:

```typescript
interface CountingBloomFilter {
  counters: Uint8Array;  // 4 bits per counter (max 15)
  numCounters: number;
  numHashes: number;
}

function add(filter: CountingBloomFilter, key: string): void {
  const hashes = computeHashes(key, filter.numHashes);
  for (const hash of hashes) {
    const idx = hash % filter.numCounters;
    const current = getCounter(filter.counters, idx);
    if (current < 15) {
      setCounter(filter.counters, idx, current + 1);
    }
  }
}

function remove(filter: CountingBloomFilter, key: string): void {
  const hashes = computeHashes(key, filter.numHashes);
  for (const hash of hashes) {
    const idx = hash % filter.numCounters;
    const current = getCounter(filter.counters, idx);
    if (current > 0) {
      setCounter(filter.counters, idx, current - 1);
    }
  }
}

function mightContain(filter: CountingBloomFilter, key: string): boolean {
  const hashes = computeHashes(key, filter.numHashes);
  for (const hash of hashes) {
    const idx = hash % filter.numCounters;
    if (getCounter(filter.counters, idx) === 0) {
      return false;
    }
  }
  return true;
}
```

**Tradeoff**: 4x storage overhead (4 bits vs 1 bit per position)

---

## Cuckoo Filters as Alternative

Cuckoo filters offer similar FPR with deletion support and better space efficiency:

```typescript
interface CuckooFilter {
  buckets: Uint8Array[];  // 4 fingerprints per bucket
  fingerprintBits: number;  // 8-16 bits
  numBuckets: number;
}

// Advantages:
// - Supports deletion
// - Better space efficiency at low FPR
// - Faster lookups (2 bucket checks)

// Disadvantages:
// - Insertion can fail (requires resizing)
// - More complex implementation
```

---

## Compound Key Bloom Filters

For edge existence and multi-column lookups:

```typescript
interface CompoundKeyBloom {
  // Key format: hash(field1 || separator || field2 || ...)
  fields: string[];
  separator: string;  // Use non-printable character
}

// Edge existence: hash(from_id || '\x00' || rel_type || '\x00' || to_id)
function edgeKey(fromId: string, relType: string, toId: string): string {
  return `${fromId}\x00${relType}\x00${toId}`;
}

// Namespace + ID: hash(ns || '\x00' || id)
function nsIdKey(ns: string, id: string): string {
  return `${ns}\x00${id}`;
}
```

---

## Edge Existence Bloom Filters

Optimized bloom filters for graph traversal:

```typescript
interface EdgeBloomConfig {
  // Separate filters for different access patterns
  forwardEdges: BloomFilter;   // Key: from_id|rel_type|to_id
  reverseEdges: BloomFilter;   // Key: to_id|rel_type|from_id
  relationshipTypes: BloomFilter;  // Key: rel_type
}

async function edgeExists(
  ns: string,
  fromId: string,
  relType: string,
  toId: string
): Promise<boolean> {
  const bloom = await loadEdgeBloom(ns);

  // Check forward edge bloom
  const forwardKey = `${fromId}\x00${relType}\x00${toId}`;
  if (!bloom.forwardEdges.mightContain(forwardKey)) {
    return false;
  }

  // Bloom says maybe - verify with actual lookup
  return await verifyEdgeExists(ns, fromId, relType, toId);
}
```

---

## Write Path Integration

### On Entity Create/Update

```typescript
async function onEntityWrite(
  ns: string,
  entity: Entity,
  fileId: string
): Promise<void> {
  // Update global bloom
  const globalBloom = await loadOrCreateBloom(`${ns}/bloom/entity.bloom`);
  globalBloom.add(entity.id);
  await saveBloom(`${ns}/bloom/entity.bloom`, globalBloom);

  // Update file-level bloom
  const fileBloom = await loadOrCreateBloom(`${ns}/bloom/${fileId}.id.bloom`);
  fileBloom.add(entity.id);
  await saveBloom(`${ns}/bloom/${fileId}.id.bloom`, fileBloom);

  // Update type bloom
  const typeBloom = await loadOrCreateBloom(`${ns}/bloom/type.bloom`);
  typeBloom.add(entity.type);
  await saveBloom(`${ns}/bloom/type.bloom`, typeBloom);
}
```

### On Edge Create

```typescript
async function onEdgeWrite(
  ns: string,
  edge: Edge,
  fileId: string
): Promise<void> {
  const edgeKey = `${edge.from_id}\x00${edge.rel_type}\x00${edge.to_id}`;
  const reverseKey = `${edge.to_id}\x00${edge.rel_type}\x00${edge.from_id}`;

  // Global edge bloom
  const globalBloom = await loadOrCreateEdgeBloom(`${ns}/bloom/edge.bloom`);
  globalBloom.forwardEdges.add(edgeKey);
  globalBloom.reverseEdges.add(reverseKey);
  globalBloom.relationshipTypes.add(edge.rel_type);
  await saveEdgeBloom(`${ns}/bloom/edge.bloom`, globalBloom);
}
```

---

## Query Optimization

### Bloom-Accelerated Queries

```typescript
async function findByIds(
  ns: string,
  ids: string[]
): Promise<Entity[]> {
  // 1. Filter IDs using global bloom
  const globalBloom = await loadBloom(`${ns}/bloom/entity.bloom`);
  const possibleIds = ids.filter(id => globalBloom.mightContain(id));

  if (possibleIds.length === 0) {
    return []; // None exist
  }

  // 2. Find candidate files
  const fileManifest = await loadManifest(ns);
  const fileToIds = new Map<string, string[]>();

  for (const id of possibleIds) {
    for (const file of fileManifest.files) {
      const fileBloom = await loadBloom(`${ns}/bloom/${file.id}.id.bloom`);
      if (fileBloom.mightContain(id)) {
        const existing = fileToIds.get(file.path) || [];
        existing.push(id);
        fileToIds.set(file.path, existing);
      }
    }
  }

  // 3. Batch queries per file
  const results: Entity[] = [];
  for (const [filePath, fileIds] of fileToIds) {
    const entities = await parquetQuery(filePath, {
      filter: { id: { in: fileIds } }
    });
    results.push(...entities);
  }

  return results;
}
```

---

## Implementation

### Bloom Filter Class

```typescript
class BloomFilter {
  private bits: Uint8Array;
  private numHashes: number;
  private size: number;

  constructor(expectedElements: number, falsePositiveRate: number = 0.01) {
    const params = calculateBloomParams(expectedElements, falsePositiveRate);
    this.bits = new Uint8Array(params.numBytes);
    this.numHashes = params.numHashes;
    this.size = params.numBits;
  }

  add(key: string): void {
    const hashes = this.computeHashes(key);
    for (const hash of hashes) {
      const idx = hash % this.size;
      this.bits[Math.floor(idx / 8)] |= (1 << (idx % 8));
    }
  }

  mightContain(key: string): boolean {
    const hashes = this.computeHashes(key);
    for (const hash of hashes) {
      const idx = hash % this.size;
      if ((this.bits[Math.floor(idx / 8)] & (1 << (idx % 8))) === 0) {
        return false;
      }
    }
    return true;
  }

  private computeHashes(key: string): number[] {
    // Use double hashing: h_i(x) = h1(x) + i * h2(x)
    const h1 = xxhash32(key, 0);
    const h2 = xxhash32(key, h1);

    const hashes: number[] = [];
    for (let i = 0; i < this.numHashes; i++) {
      hashes.push((h1 + i * h2) >>> 0);
    }
    return hashes;
  }

  serialize(): Uint8Array {
    const header = new Uint8Array(8);
    new DataView(header.buffer).setUint32(0, this.size, true);
    new DataView(header.buffer).setUint32(4, this.numHashes, true);
    return concatBytes(header, this.bits);
  }

  static deserialize(data: Uint8Array): BloomFilter {
    const view = new DataView(data.buffer);
    const size = view.getUint32(0, true);
    const numHashes = view.getUint32(4, true);
    const bits = data.slice(8);

    const filter = Object.create(BloomFilter.prototype);
    filter.bits = bits;
    filter.numHashes = numHashes;
    filter.size = size;
    return filter;
  }
}
```

---

*Design Document - ParqueDB Bloom Filter Indexes*
