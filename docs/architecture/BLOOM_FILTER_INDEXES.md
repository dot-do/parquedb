# ParqueDB: Bloom Filter and Probabilistic Indexes

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
┌─────────────────────────────────────────────────────────────────┐
│                    Bloom Filter (m bits)                        │
│  0  0  1  0  0  1  0  0  1  1  0  0  1  0  0  1  0  0  1  0    │
│        ▲        ▲        ▲                    ▲                  │
│        │        │        │                    │                  │
│        └────────┴────────┴────────────────────┘                  │
│                    hash functions (k=4)                          │
│                          ▲                                       │
│                          │                                       │
│                    "user:12345"                                  │
└─────────────────────────────────────────────────────────────────┘
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
┌─────────────────────────────────────────────────────┐
│                  Parquet File                        │
├─────────────────────────────────────────────────────┤
│  Row Group 0                                         │
│    Column Chunk: id                                  │
│      Data Pages                                      │
│    Column Chunk: ns                                  │
│      Data Pages                                      │
├─────────────────────────────────────────────────────┤
│  Row Group 1                                         │
│    ...                                               │
├─────────────────────────────────────────────────────┤
│  File Footer                                         │
│    Schema                                            │
│    Row Group Metadata                                │
│    ┌─────────────────────────────────────────────┐  │
│    │  Bloom Filter Headers                        │  │
│    │    Column: id                                │  │
│    │      Row Group 0: offset=1234, length=4096  │  │
│    │      Row Group 1: offset=5330, length=4096  │  │
│    └─────────────────────────────────────────────┘  │
│    ┌─────────────────────────────────────────────┐  │
│    │  Bloom Filter Data (after footer)           │  │
│    │    [Filter bytes for id, RG0]               │  │
│    │    [Filter bytes for id, RG1]               │  │
│    └─────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
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

ParqueDB uses a three-level bloom filter hierarchy for optimal performance:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Level 1: Global Bloom                         │
│   "Does entity/edge exist anywhere in namespace?"               │
│   Size: ~1MB per 10M entities at 1% FPR                         │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Bloom(ns + type + id) for all entities                 │   │
│   └─────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ If MAYBE
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Level 2: File Bloom                           │
│   "Does entity/edge exist in this Parquet file?"                │
│   Size: ~100KB per 100K entities per file at 1% FPR             │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Bloom per Parquet file (stored in sidecar .bloom file) │   │
│   └─────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ If MAYBE
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Level 3: Row Group Bloom                      │
│   "Does entity/edge exist in this row group?"                   │
│   Size: ~10KB per 10K entities per row group at 1% FPR          │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Parquet built-in bloom filter per row group            │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Hierarchical Lookup Algorithm

```typescript
interface BloomHierarchy {
  global: BloomFilter;
  files: Map<string, BloomFilter>;  // filename -> filter
  // Row group filters are in Parquet footer
}

async function lookupEntity(
  hierarchy: BloomHierarchy,
  ns: string,
  type: string,
  id: string
): Promise<'NOT_FOUND' | 'MAYBE_EXISTS' | Node> {
  const key = `${ns}|${type}|${id}`;

  // Level 1: Global check
  if (!hierarchy.global.contains(key)) {
    return 'NOT_FOUND';  // Definitely not in any file
  }

  // Level 2: Find candidate files
  const candidateFiles: string[] = [];
  for (const [filename, filter] of hierarchy.files) {
    if (filter.contains(key)) {
      candidateFiles.push(filename);
    }
  }

  if (candidateFiles.length === 0) {
    return 'NOT_FOUND';  // Global was false positive
  }

  // Level 3: Check row groups within candidate files
  for (const filename of candidateFiles) {
    const node = await scanFileWithBloom(filename, ns, type, id);
    if (node) return node;
  }

  return 'NOT_FOUND';  // All were false positives
}
```

---

## Sizing Formulas and Tradeoffs

### Optimal Bloom Filter Parameters

Given desired false positive rate `p` and expected number of elements `n`:

```typescript
interface BloomSizing {
  /**
   * Optimal number of bits per element
   * m/n = -1.44 * log2(p)
   *
   * For p = 1%:  m/n = 9.58 bits/element
   * For p = 0.1%: m/n = 14.38 bits/element
   */
  bitsPerElement: number;

  /**
   * Optimal number of hash functions
   * k = (m/n) * ln(2) = -log2(p)
   *
   * For p = 1%:  k = 6.64 ≈ 7
   * For p = 0.1%: k = 9.97 ≈ 10
   */
  numHashFunctions: number;

  /**
   * Total filter size in bytes
   * bytes = ceil(m / 8)
   */
  totalBytes: number;
}

function calculateBloomSize(n: number, p: number): BloomSizing {
  const m = Math.ceil(-1.44 * n * Math.log2(p));
  const k = Math.round(-Math.log2(p));

  return {
    bitsPerElement: m / n,
    numHashFunctions: k,
    totalBytes: Math.ceil(m / 8)
  };
}
```

### Size Estimation Table

| Elements (n) | FPR (p) | Bits/Element | Hash Functions (k) | Total Size |
|-------------|---------|--------------|-------------------|------------|
| 10,000 | 1% | 9.58 | 7 | 12 KB |
| 10,000 | 0.1% | 14.38 | 10 | 18 KB |
| 100,000 | 1% | 9.58 | 7 | 120 KB |
| 100,000 | 0.1% | 14.38 | 10 | 180 KB |
| 1,000,000 | 1% | 9.58 | 7 | 1.2 MB |
| 1,000,000 | 0.1% | 14.38 | 10 | 1.8 MB |
| 10,000,000 | 1% | 9.58 | 7 | 12 MB |
| 10,000,000 | 0.1% | 14.38 | 10 | 18 MB |

### ParqueDB Default Configurations

```typescript
const BLOOM_CONFIGS = {
  // Global entity bloom (namespace level)
  globalEntity: {
    expectedElements: 10_000_000,  // 10M entities per namespace
    falsePositiveRate: 0.01,       // 1%
    // Result: ~12 MB per namespace
  },

  // Per-file bloom
  fileBloom: {
    expectedElements: 100_000,     // 100K entities per file
    falsePositiveRate: 0.01,       // 1%
    // Result: ~120 KB per file
  },

  // Per-row-group bloom (Parquet built-in)
  rowGroupBloom: {
    expectedElements: 10_000,      // 10K entities per row group
    falsePositiveRate: 0.01,       // 1%
    // Result: ~12 KB per row group
  },

  // Edge existence bloom
  edgeBloom: {
    expectedElements: 100_000_000, // 100M edges
    falsePositiveRate: 0.001,      // 0.1% (lower for edge checks)
    // Result: ~180 MB for edge bloom
  }
} as const;
```

### Dynamic Resizing Strategy

```typescript
interface DynamicBloomConfig {
  initialCapacity: number;
  growthFactor: number;
  maxFalsePositiveRate: number;
  rebuildThreshold: number;  // When FPR exceeds this, rebuild
}

class ScalableBloomFilter {
  private filters: BloomFilter[] = [];
  private config: DynamicBloomConfig;
  private totalInserted = 0;

  constructor(config: DynamicBloomConfig) {
    this.config = config;
    this.filters.push(this.createFilter(config.initialCapacity));
  }

  insert(key: string): void {
    // Insert into newest filter
    this.filters[this.filters.length - 1].insert(key);
    this.totalInserted++;

    // Check if we need a new filter
    if (this.getCurrentFPR() > this.config.maxFalsePositiveRate) {
      const newCapacity =
        this.filters[this.filters.length - 1].capacity * this.config.growthFactor;
      this.filters.push(this.createFilter(newCapacity));
    }
  }

  contains(key: string): boolean {
    // Check all filters (any positive = maybe exists)
    return this.filters.some(f => f.contains(key));
  }

  // Periodically compact to single filter
  async compact(): Promise<void> {
    if (this.filters.length <= 1) return;

    const newCapacity = Math.ceil(this.totalInserted * 1.5);
    const newFilter = this.createFilter(newCapacity);

    // Must rebuild from source data (can't merge bloom filters)
    // This is triggered during compaction jobs
  }
}
```

---

## Counting Bloom Filters for Deletes

Standard bloom filters don't support deletion. For ParqueDB's soft-delete model, we use **Counting Bloom Filters** (CBF).

### Counting Bloom Filter Structure

```typescript
interface CountingBloomFilter {
  // Array of counters (4-bit counters allow counts 0-15)
  counters: Uint8Array;  // 2 counters per byte

  // Number of hash functions
  k: number;

  // Overflow tracking for counters > 15
  overflow: Set<number>;
}

class CountingBloom {
  private counters: Uint8Array;
  private k: number;
  private overflow = new Set<number>();

  constructor(m: number, k: number) {
    // 4 bits per counter = 2 counters per byte
    this.counters = new Uint8Array(Math.ceil(m / 2));
    this.k = k;
  }

  private getCounter(index: number): number {
    const byteIndex = Math.floor(index / 2);
    const isHighNibble = index % 2 === 1;

    if (this.overflow.has(index)) {
      return 16; // Saturated
    }

    const byte = this.counters[byteIndex];
    return isHighNibble ? (byte >> 4) : (byte & 0x0F);
  }

  private setCounter(index: number, value: number): void {
    if (value > 15) {
      this.overflow.add(index);
      return;
    }

    const byteIndex = Math.floor(index / 2);
    const isHighNibble = index % 2 === 1;

    if (isHighNibble) {
      this.counters[byteIndex] =
        (this.counters[byteIndex] & 0x0F) | (value << 4);
    } else {
      this.counters[byteIndex] =
        (this.counters[byteIndex] & 0xF0) | value;
    }
  }

  insert(key: string): void {
    const hashes = computeHashes(key, this.k);
    for (const hash of hashes) {
      const index = hash % (this.counters.length * 2);
      const current = this.getCounter(index);
      this.setCounter(index, current + 1);
    }
  }

  delete(key: string): boolean {
    const hashes = computeHashes(key, this.k);

    // First verify all counters are > 0
    for (const hash of hashes) {
      const index = hash % (this.counters.length * 2);
      if (this.getCounter(index) === 0) {
        return false; // Element was never inserted
      }
    }

    // Decrement all counters
    for (const hash of hashes) {
      const index = hash % (this.counters.length * 2);
      const current = this.getCounter(index);
      if (!this.overflow.has(index)) {
        this.setCounter(index, current - 1);
      }
    }

    return true;
  }

  contains(key: string): boolean {
    const hashes = computeHashes(key, this.k);
    return hashes.every(hash => {
      const index = hash % (this.counters.length * 2);
      return this.getCounter(index) > 0;
    });
  }
}
```

### Memory Overhead Comparison

| Filter Type | Bits per Element | Delete Support | Memory for 1M elements |
|-------------|------------------|----------------|------------------------|
| Standard Bloom | 10 bits | No | 1.25 MB |
| Counting Bloom (4-bit) | 40 bits | Yes | 5 MB |
| Counting Bloom (8-bit) | 80 bits | Yes | 10 MB |

### When to Use Counting Bloom Filters

```typescript
type BloomFilterStrategy =
  | { type: 'standard'; reason: 'append-only-data' }
  | { type: 'counting'; reason: 'frequent-deletes' }
  | { type: 'rebuild'; reason: 'periodic-compaction' };

function selectStrategy(config: DatasetConfig): BloomFilterStrategy {
  // For immutable Parquet files, standard bloom is sufficient
  // Deletes are handled by rebuilding during compaction
  if (config.compactionFrequency === 'daily') {
    return { type: 'standard', reason: 'append-only-data' };
  }

  // For real-time delete requirements
  if (config.deleteLatencyMs < 1000) {
    return { type: 'counting', reason: 'frequent-deletes' };
  }

  // For moderate delete needs
  return { type: 'rebuild', reason: 'periodic-compaction' };
}
```

---

## Cuckoo Filters as Alternative

Cuckoo filters offer better space efficiency and native delete support.

### Cuckoo Filter Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                    Cuckoo Filter Buckets                         │
├─────────────────────────────────────────────────────────────────┤
│  Bucket 0: [fp1] [fp2] [  ] [  ]  (4 entries per bucket)       │
│  Bucket 1: [fp3] [  ] [  ] [  ]                                 │
│  Bucket 2: [fp4] [fp5] [fp6] [  ]                               │
│  ...                                                             │
│  Bucket n: [fp7] [fp8] [  ] [  ]                                │
└─────────────────────────────────────────────────────────────────┘

Insertion: item can be in bucket h1(x) OR bucket h2(x) = h1(x) XOR hash(fp)
Lookup:    check both buckets for matching fingerprint
Delete:    remove fingerprint from bucket
```

### Cuckoo Filter Implementation

```typescript
interface CuckooFilter {
  buckets: Uint8Array[];  // Each bucket holds 4 fingerprints
  bucketCount: number;
  fingerprintBits: number;
  maxKicks: number;  // Maximum relocations before rebuild
}

class CuckooFilterImpl {
  private buckets: Uint16Array;  // 16-bit fingerprints
  private bucketCount: number;
  private readonly ENTRIES_PER_BUCKET = 4;
  private readonly MAX_KICKS = 500;

  constructor(capacity: number) {
    // Each bucket holds 4 entries
    this.bucketCount = Math.ceil(capacity / (this.ENTRIES_PER_BUCKET * 0.95));
    this.buckets = new Uint16Array(this.bucketCount * this.ENTRIES_PER_BUCKET);
  }

  private fingerprint(key: string): number {
    // 16-bit fingerprint (non-zero)
    const hash = xxhash64(key);
    const fp = (hash & 0xFFFF) || 1;
    return fp;
  }

  private index1(key: string): number {
    return xxhash64(key) % this.bucketCount;
  }

  private index2(i1: number, fp: number): number {
    // i2 = i1 XOR hash(fingerprint)
    return (i1 ^ (xxhash32(fp.toString()) % this.bucketCount)) % this.bucketCount;
  }

  insert(key: string): boolean {
    const fp = this.fingerprint(key);
    const i1 = this.index1(key);
    const i2 = this.index2(i1, fp);

    // Try to insert in bucket i1
    if (this.insertIntoBucket(i1, fp)) return true;

    // Try to insert in bucket i2
    if (this.insertIntoBucket(i2, fp)) return true;

    // Must relocate existing items
    let currentIndex = Math.random() < 0.5 ? i1 : i2;
    let currentFp = fp;

    for (let kick = 0; kick < this.MAX_KICKS; kick++) {
      // Swap with random entry in bucket
      const entryIndex = Math.floor(Math.random() * this.ENTRIES_PER_BUCKET);
      const bucketStart = currentIndex * this.ENTRIES_PER_BUCKET;

      const evictedFp = this.buckets[bucketStart + entryIndex];
      this.buckets[bucketStart + entryIndex] = currentFp;
      currentFp = evictedFp;

      // Find alternate bucket for evicted item
      currentIndex = this.index2(currentIndex, currentFp);

      if (this.insertIntoBucket(currentIndex, currentFp)) {
        return true;
      }
    }

    // Filter is full, need to resize
    return false;
  }

  private insertIntoBucket(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET;
    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === 0) {
        this.buckets[start + i] = fp;
        return true;
      }
    }
    return false;
  }

  contains(key: string): boolean {
    const fp = this.fingerprint(key);
    const i1 = this.index1(key);
    const i2 = this.index2(i1, fp);

    return this.bucketContains(i1, fp) || this.bucketContains(i2, fp);
  }

  private bucketContains(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET;
    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === fp) {
        return true;
      }
    }
    return false;
  }

  delete(key: string): boolean {
    const fp = this.fingerprint(key);
    const i1 = this.index1(key);
    const i2 = this.index2(i1, fp);

    // Try to delete from bucket i1
    if (this.deleteFromBucket(i1, fp)) return true;

    // Try to delete from bucket i2
    return this.deleteFromBucket(i2, fp);
  }

  private deleteFromBucket(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET;
    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === fp) {
        this.buckets[start + i] = 0;
        return true;
      }
    }
    return false;
  }
}
```

### Bloom vs Cuckoo Comparison

| Aspect | Bloom Filter | Cuckoo Filter |
|--------|--------------|---------------|
| Space efficiency | 10 bits/element | 12 bits/element |
| Lookup time | O(k) hashes | O(1) - 2 lookups |
| Deletion | No (or counting) | Yes (native) |
| False positive rate | Configurable | ~3% (fixed) |
| Max load factor | 100% | 95% |
| Insert failure | Never | Possible (resize) |
| Cache efficiency | Poor | Good (2 locations) |

### ParqueDB Recommendation

```typescript
type FilterSelection = {
  filter: 'bloom' | 'cuckoo';
  reason: string;
};

function selectFilterType(useCase: string): FilterSelection {
  switch (useCase) {
    case 'entity-existence':
      // Low FPR needed, no deletes (compaction rebuilds)
      return { filter: 'bloom', reason: 'Configurable FPR, space efficient' };

    case 'edge-existence':
      // Many lookups, occasional deletes
      return { filter: 'cuckoo', reason: 'Fast lookup, delete support' };

    case 'hot-path-cache':
      // Cache-friendly access pattern
      return { filter: 'cuckoo', reason: 'Only 2 cache lines per lookup' };

    default:
      return { filter: 'bloom', reason: 'Default choice' };
  }
}
```

---

## Blocked Bloom Filters for Cache Efficiency

Standard bloom filters have poor cache locality. **Blocked Bloom Filters** partition the filter into cache-line-sized blocks.

### Blocked Bloom Filter Structure

```
┌─────────────────────────────────────────────────────────────────┐
│               Standard Bloom Filter (poor cache)                │
│   Hash positions scattered across entire bit array              │
│   ■ □ □ □ □ □ □ □ □ □ □ □ □ ■ □ □ □ □ □ □ ■ □ □ □ □ □ □ ■    │
│   ↑                         ↑             ↑             ↑       │
│   │                         │             │             │       │
│   └────── 4 cache misses for k=4 hash functions ────────┘       │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│               Blocked Bloom Filter (cache-friendly)             │
│   All hash positions within single 64-byte cache line           │
│                                                                  │
│   Block 0 (64 bytes = 512 bits)                                 │
│   ┌────────────────────────────────────────────────────────┐    │
│   │ ■ □ □ ■ □ □ □ □ □ ■ □ □ □ □ ■ □ ... (all k bits)     │    │
│   └────────────────────────────────────────────────────────┘    │
│                                                                  │
│   Block 1 (64 bytes)                                            │
│   ┌────────────────────────────────────────────────────────┐    │
│   │ □ □ □ □ □ □ □ □ □ □ □ □ □ □ □ □ ...                   │    │
│   └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Blocked Bloom Filter Implementation

```typescript
interface BlockedBloomFilter {
  blocks: Uint8Array[];  // Each block is 64 bytes (cache line)
  blockCount: number;
  k: number;  // Hash functions per block
}

class BlockedBloom {
  private blocks: Uint8Array;
  private blockCount: number;
  private k: number;

  // 64 bytes = 512 bits per block (cache line size)
  private readonly BLOCK_SIZE = 64;
  private readonly BLOCK_BITS = 512;

  constructor(expectedElements: number, fpr: number) {
    // Calculate bits needed
    const totalBits = Math.ceil(-1.44 * expectedElements * Math.log2(fpr));
    this.blockCount = Math.ceil(totalBits / this.BLOCK_BITS);
    this.k = Math.round(-Math.log2(fpr));

    this.blocks = new Uint8Array(this.blockCount * this.BLOCK_SIZE);
  }

  private selectBlock(key: string): number {
    // First hash selects the block
    const hash = xxhash64(key);
    return Number(hash % BigInt(this.blockCount));
  }

  private hashesWithinBlock(key: string, blockIndex: number): number[] {
    // Generate k hash positions within the 512-bit block
    const hashes: number[] = [];
    const seed = blockIndex * 31;

    // Use double hashing: h(i) = h1 + i * h2
    const h1 = murmur3_32(key, seed) % this.BLOCK_BITS;
    const h2 = murmur3_32(key, seed + 1) % this.BLOCK_BITS;

    for (let i = 0; i < this.k; i++) {
      hashes.push((h1 + i * h2) % this.BLOCK_BITS);
    }

    return hashes;
  }

  insert(key: string): void {
    const blockIndex = this.selectBlock(key);
    const blockOffset = blockIndex * this.BLOCK_SIZE;
    const hashes = this.hashesWithinBlock(key, blockIndex);

    for (const bitPos of hashes) {
      const byteIndex = Math.floor(bitPos / 8);
      const bitOffset = bitPos % 8;
      this.blocks[blockOffset + byteIndex] |= (1 << bitOffset);
    }
  }

  contains(key: string): boolean {
    const blockIndex = this.selectBlock(key);
    const blockOffset = blockIndex * this.BLOCK_SIZE;
    const hashes = this.hashesWithinBlock(key, blockIndex);

    // Single cache line read for all k checks
    for (const bitPos of hashes) {
      const byteIndex = Math.floor(bitPos / 8);
      const bitOffset = bitPos % 8;
      if ((this.blocks[blockOffset + byteIndex] & (1 << bitOffset)) === 0) {
        return false;
      }
    }

    return true;
  }
}
```

### Performance Comparison

| Operation | Standard Bloom | Blocked Bloom | Improvement |
|-----------|---------------|---------------|-------------|
| Lookup (k=7) | 7 cache misses | 1 cache miss | 7x fewer misses |
| Batch lookup (1000) | 7000 random accesses | 1000 sequential + random | 3-5x faster |
| Memory bandwidth | High (random) | Low (sequential) | 2-4x better |

### When to Use Blocked Bloom

```typescript
const BLOCKED_BLOOM_THRESHOLDS = {
  // Use blocked bloom when filter exceeds L3 cache
  minSizeForBlocked: 8 * 1024 * 1024,  // 8 MB

  // Use blocked bloom for batch operations
  batchSizeThreshold: 100,

  // Standard bloom for small filters that fit in cache
  maxSizeForStandard: 1 * 1024 * 1024,  // 1 MB
};

function selectBloomType(
  filterSize: number,
  queryPattern: 'point' | 'batch'
): 'standard' | 'blocked' {
  if (filterSize < BLOCKED_BLOOM_THRESHOLDS.maxSizeForStandard) {
    return 'standard';  // Small filter, fits in cache anyway
  }

  if (filterSize > BLOCKED_BLOOM_THRESHOLDS.minSizeForBlocked) {
    return 'blocked';  // Large filter, cache efficiency matters
  }

  // Medium size - depends on query pattern
  return queryPattern === 'batch' ? 'blocked' : 'standard';
}
```

---

## Compound Key Bloom Filters

ParqueDB needs bloom filters for compound keys like `(ns, id)` and `(ns, type, id)`.

### Compound Key Encoding

```typescript
/**
 * Compound key encoding strategies for bloom filters
 */
type CompoundKeyStrategy =
  | 'concatenation'   // Simple: "ns|type|id"
  | 'hierarchical'    // Multiple filters: ns, ns+type, ns+type+id
  | 'xor-folding';    // XOR hashes together

interface CompoundKeyConfig {
  strategy: CompoundKeyStrategy;
  delimiter: string;
  hashFunction: 'xxhash64' | 'murmur3';
}

const COMPOUND_KEY_CONFIG: CompoundKeyConfig = {
  strategy: 'concatenation',
  delimiter: '|',
  hashFunction: 'xxhash64'
};
```

### Concatenation Strategy (Recommended)

```typescript
/**
 * Simple and effective: concatenate key parts with delimiter
 */
class CompoundKeyBloom {
  private bloom: BloomFilter;
  private delimiter = '|';

  constructor(expectedElements: number, fpr: number) {
    this.bloom = new BloomFilter(expectedElements, fpr);
  }

  // Entity key: ns + type + id
  insertEntity(ns: string, type: string, id: string): void {
    const key = `${ns}${this.delimiter}${type}${this.delimiter}${id}`;
    this.bloom.insert(key);
  }

  containsEntity(ns: string, type: string, id: string): boolean {
    const key = `${ns}${this.delimiter}${type}${this.delimiter}${id}`;
    return this.bloom.contains(key);
  }

  // Edge key: ns + from_id + rel_type + to_id
  insertEdge(ns: string, fromId: string, relType: string, toId: string): void {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}${this.delimiter}${toId}`;
    this.bloom.insert(key);
  }

  containsEdge(ns: string, fromId: string, relType: string, toId: string): boolean {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}${this.delimiter}${toId}`;
    return this.bloom.contains(key);
  }
}
```

### Hierarchical Bloom Filters (Prefix Queries)

For queries that filter by prefix (e.g., "all entities in namespace X"):

```typescript
/**
 * Multiple bloom filters for different key prefixes
 * Enables efficient prefix queries
 */
class HierarchicalBloom {
  // Level 0: namespace only
  private nsBloom: BloomFilter;

  // Level 1: namespace + type
  private nsTypeBloom: BloomFilter;

  // Level 2: full key (namespace + type + id)
  private fullKeyBloom: BloomFilter;

  constructor(config: {
    expectedNamespaces: number;
    expectedTypes: number;
    expectedEntities: number;
    fpr: number;
  }) {
    this.nsBloom = new BloomFilter(config.expectedNamespaces, config.fpr);
    this.nsTypeBloom = new BloomFilter(
      config.expectedNamespaces * config.expectedTypes,
      config.fpr
    );
    this.fullKeyBloom = new BloomFilter(config.expectedEntities, config.fpr);
  }

  insert(ns: string, type: string, id: string): void {
    this.nsBloom.insert(ns);
    this.nsTypeBloom.insert(`${ns}|${type}`);
    this.fullKeyBloom.insert(`${ns}|${type}|${id}`);
  }

  // Check if namespace exists (any entities)
  hasNamespace(ns: string): boolean {
    return this.nsBloom.contains(ns);
  }

  // Check if type exists in namespace
  hasType(ns: string, type: string): boolean {
    return this.nsTypeBloom.contains(`${ns}|${type}`);
  }

  // Check if specific entity exists
  hasEntity(ns: string, type: string, id: string): boolean {
    return this.fullKeyBloom.contains(`${ns}|${type}|${id}`);
  }

  // Optimized: check all levels to skip early
  maybeHasEntity(ns: string, type: string, id: string): boolean {
    // Early exit if namespace doesn't exist
    if (!this.nsBloom.contains(ns)) return false;

    // Early exit if type doesn't exist in namespace
    if (!this.nsTypeBloom.contains(`${ns}|${type}`)) return false;

    // Final check for full key
    return this.fullKeyBloom.contains(`${ns}|${type}|${id}`);
  }
}
```

### XOR Folding Strategy (Space Efficient)

```typescript
/**
 * XOR hash components together
 * More compact but slightly higher FPR due to hash collisions
 */
function xorFoldedHash(parts: string[]): bigint {
  let result = 0n;

  for (const part of parts) {
    const hash = xxhash64(part);
    result ^= hash;
  }

  return result;
}

class XORFoldedBloom {
  private bloom: BloomFilter;

  insert(ns: string, type: string, id: string): void {
    const hash = xorFoldedHash([ns, type, id]);
    this.bloom.insertHash(hash);
  }

  contains(ns: string, type: string, id: string): boolean {
    const hash = xorFoldedHash([ns, type, id]);
    return this.bloom.containsHash(hash);
  }
}
```

---

## Edge Existence Bloom Filters

Efficient edge existence checks are critical for graph traversal.

### Edge Bloom Filter Design

```typescript
/**
 * Specialized bloom filter for edge existence queries
 *
 * Key format: ns|from_id|rel_type|to_id
 *
 * Query patterns:
 * 1. Exact edge: "Does edge (A, knows, B) exist?"
 * 2. Outgoing: "Does A have any 'knows' edges?"
 * 3. Incoming: "Does B have any incoming 'knows' edges?"
 */
interface EdgeBloomFilters {
  // Full edge existence
  exactEdge: BloomFilter;

  // Outgoing edge existence: ns|from_id|rel_type
  outgoing: BloomFilter;

  // Incoming edge existence: ns|to_id|rel_type
  incoming: BloomFilter;
}

class EdgeExistenceFilter {
  private exact: BloomFilter;
  private outgoing: BloomFilter;
  private incoming: BloomFilter;

  constructor(expectedEdges: number, fpr: number = 0.001) {
    // Exact edges need all edges
    this.exact = new BloomFilter(expectedEdges, fpr);

    // Outgoing/incoming are sparser (unique from_id+rel_type combinations)
    // Estimate: 10% of edges are unique outgoing patterns
    this.outgoing = new BloomFilter(expectedEdges * 0.1, fpr);
    this.incoming = new BloomFilter(expectedEdges * 0.1, fpr);
  }

  insertEdge(edge: Edge): void {
    // Exact edge key
    const exactKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}|${edge.to_id}`;
    this.exact.insert(exactKey);

    // Outgoing pattern
    const outKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}`;
    this.outgoing.insert(outKey);

    // Incoming pattern
    const inKey = `${edge.ns}|${edge.to_id}|${edge.rel_type}`;
    this.incoming.insert(inKey);

    // For bidirectional edges, also add reverse
    if (edge.bidirectional) {
      const reverseExact = `${edge.ns}|${edge.to_id}|${edge.rel_type}|${edge.from_id}`;
      this.exact.insert(reverseExact);

      const reverseOut = `${edge.ns}|${edge.to_id}|${edge.rel_type}`;
      this.outgoing.insert(reverseOut);

      const reverseIn = `${edge.ns}|${edge.from_id}|${edge.rel_type}`;
      this.incoming.insert(reverseIn);
    }
  }

  /**
   * Check if exact edge exists
   */
  hasEdge(ns: string, fromId: string, relType: string, toId: string): boolean {
    const key = `${ns}|${fromId}|${relType}|${toId}`;
    return this.exact.contains(key);
  }

  /**
   * Check if node has any outgoing edges of type
   * Useful for: "Does user have any 'follows' relationships?"
   */
  hasOutgoing(ns: string, fromId: string, relType: string): boolean {
    const key = `${ns}|${fromId}|${relType}`;
    return this.outgoing.contains(key);
  }

  /**
   * Check if node has any incoming edges of type
   * Useful for: "Does post have any comments?"
   */
  hasIncoming(ns: string, toId: string, relType: string): boolean {
    const key = `${ns}|${toId}|${relType}`;
    return this.incoming.contains(key);
  }

  /**
   * Optimized traversal: skip nodes with no outgoing edges
   */
  async traverse(
    ns: string,
    fromId: string,
    relType: string,
    edgeStore: EdgeStore
  ): Promise<Edge[]> {
    // Fast path: bloom filter says no outgoing edges
    if (!this.hasOutgoing(ns, fromId, relType)) {
      return [];
    }

    // Bloom filter says maybe - do actual lookup
    return edgeStore.getOutgoing(ns, fromId, relType);
  }
}
```

### Edge Bloom Per Row Group

```typescript
/**
 * Per-row-group bloom filters for edge data files
 * Enables skipping row groups during edge lookups
 */
interface RowGroupEdgeBloom {
  fileId: string;
  rowGroupIndex: number;

  // Bloom filter for exact edges in this row group
  edgeBloom: Uint8Array;

  // Min/max values for zone map combination
  stats: {
    fromId: { min: string; max: string };
    toId: { min: string; max: string };
    relType: { distinctValues: string[] };
  };
}

class RowGroupEdgeFilter {
  private rowGroupBlooms: Map<string, RowGroupEdgeBloom[]>;

  /**
   * Find candidate row groups for edge lookup
   */
  findCandidateRowGroups(
    ns: string,
    fromId: string,
    relType: string,
    toId: string
  ): { fileId: string; rowGroupIndex: number }[] {
    const candidates: { fileId: string; rowGroupIndex: number }[] = [];
    const edgeKey = `${ns}|${fromId}|${relType}|${toId}`;

    for (const [fileId, rowGroups] of this.rowGroupBlooms) {
      for (const rg of rowGroups) {
        // Check zone maps first (cheap)
        if (fromId < rg.stats.fromId.min || fromId > rg.stats.fromId.max) {
          continue;
        }
        if (toId < rg.stats.toId.min || toId > rg.stats.toId.max) {
          continue;
        }
        if (!rg.stats.relType.distinctValues.includes(relType)) {
          continue;
        }

        // Check bloom filter
        if (this.checkBloom(rg.edgeBloom, edgeKey)) {
          candidates.push({ fileId, rowGroupIndex: rg.rowGroupIndex });
        }
      }
    }

    return candidates;
  }
}
```

---

## Index File Format

### Bloom Filter File Format (.bloom)

```typescript
/**
 * ParqueDB Bloom Filter File Format
 *
 * File extension: .bloom
 * Magic bytes: "PQBL" (ParQueDB BLoom)
 */
interface BloomFilterFile {
  header: BloomFileHeader;
  filters: BloomFilterEntry[];
  footer: BloomFileFooter;
}

interface BloomFileHeader {
  magic: 'PQBL';           // 4 bytes
  version: number;          // 2 bytes (currently 1)
  filterType: FilterType;   // 1 byte
  compression: Compression; // 1 byte
  filterCount: number;      // 4 bytes
  reserved: Uint8Array;     // 4 bytes (future use)
}

type FilterType =
  | 0x01  // Standard bloom
  | 0x02  // Counting bloom
  | 0x03  // Cuckoo filter
  | 0x04  // Blocked bloom

type Compression =
  | 0x00  // Uncompressed
  | 0x01  // ZSTD
  | 0x02  // LZ4

interface BloomFilterEntry {
  // Entry header
  scope: FilterScope;        // 1 byte
  keyType: KeyType;          // 1 byte
  elementCount: number;      // 4 bytes
  falsePositiveRate: number; // 4 bytes (float32)
  hashFunctions: number;     // 1 byte
  filterLength: number;      // 4 bytes

  // Scope metadata (variable length)
  scopeMetadata: ScopeMetadata;

  // Filter data (compressed if specified in header)
  filterData: Uint8Array;
}

type FilterScope =
  | { type: 'global' }
  | { type: 'file'; fileId: string }
  | { type: 'row_group'; fileId: string; rowGroupIndex: number };

type KeyType =
  | 0x01  // Entity: ns|type|id
  | 0x02  // Edge exact: ns|from_id|rel_type|to_id
  | 0x03  // Edge outgoing: ns|from_id|rel_type
  | 0x04  // Edge incoming: ns|to_id|rel_type
  | 0x05  // Custom

interface BloomFileFooter {
  checksum: number;          // CRC32 of all filter data
  createdAt: bigint;         // Timestamp
  sourceFiles: string[];     // Source Parquet files
  totalElements: number;     // Total elements across all filters
}
```

### File Layout Example

```
/warehouse/example.com~crm/
├── nodes/
│   ├── data/
│   │   ├── 00000001.parquet
│   │   └── 00000002.parquet
│   └── bloom/
│       ├── global.bloom              # Global entity bloom
│       │   Header: PQBL v1, Standard, ZSTD
│       │   Filter 0: scope=global, key=entity, 10M elements, 1% FPR
│       │
│       ├── 00000001.bloom            # Per-file bloom
│       │   Header: PQBL v1, Blocked, ZSTD
│       │   Filter 0: scope=file, key=entity, 100K elements
│       │   Filter 1: scope=row_group:0, key=entity, 10K elements
│       │   Filter 2: scope=row_group:1, key=entity, 10K elements
│       │
│       └── 00000002.bloom
│
├── edges/
│   ├── forward/
│   │   ├── 00000001.parquet
│   │   └── 00000002.parquet
│   └── bloom/
│       ├── edge.bloom                # Global edge bloom
│       │   Filter 0: key=edge_exact, 100M elements, 0.1% FPR
│       │   Filter 1: key=edge_outgoing, 10M elements
│       │   Filter 2: key=edge_incoming, 10M elements
│       │
│       └── 00000001.bloom            # Per-file edge bloom
```

### Reading Bloom Filter Files

```typescript
async function readBloomFile(path: string): Promise<BloomFilterFile> {
  const buffer = await readFile(path);
  const view = new DataView(buffer);

  // Read header
  const magic = String.fromCharCode(
    view.getUint8(0), view.getUint8(1),
    view.getUint8(2), view.getUint8(3)
  );
  if (magic !== 'PQBL') {
    throw new Error('Invalid bloom filter file');
  }

  const version = view.getUint16(4, true);
  const filterType = view.getUint8(6);
  const compression = view.getUint8(7);
  const filterCount = view.getUint32(8, true);

  // Read filters
  const filters: BloomFilterEntry[] = [];
  let offset = 16; // After header

  for (let i = 0; i < filterCount; i++) {
    const entry = readBloomEntry(buffer, offset, compression);
    filters.push(entry);
    offset += entry.totalSize;
  }

  // Read footer
  const footer = readFooter(buffer, offset);

  return { header: { magic, version, filterType, compression, filterCount }, filters, footer };
}

function readBloomEntry(
  buffer: ArrayBuffer,
  offset: number,
  compression: Compression
): BloomFilterEntry {
  const view = new DataView(buffer);

  const scope = view.getUint8(offset);
  const keyType = view.getUint8(offset + 1);
  const elementCount = view.getUint32(offset + 2, true);
  const fpr = view.getFloat32(offset + 6, true);
  const hashFunctions = view.getUint8(offset + 10);
  const filterLength = view.getUint32(offset + 11, true);

  // Read scope metadata
  const [scopeMetadata, metadataSize] = readScopeMetadata(buffer, offset + 15, scope);

  // Read filter data
  let filterData = new Uint8Array(buffer, offset + 15 + metadataSize, filterLength);

  // Decompress if needed
  if (compression === 0x01) {
    filterData = decompressZstd(filterData);
  } else if (compression === 0x02) {
    filterData = decompressLz4(filterData);
  }

  return {
    scope,
    keyType,
    elementCount,
    falsePositiveRate: fpr,
    hashFunctions,
    filterLength,
    scopeMetadata,
    filterData,
    totalSize: 15 + metadataSize + filterLength
  };
}
```

---

## Write Path Integration

### Bloom Filter Updates on Writes

```typescript
/**
 * Bloom filter update strategy during writes
 */
interface BloomWriteConfig {
  // Update global bloom synchronously (for consistency)
  syncGlobalUpdate: boolean;

  // Buffer file-level updates
  fileUpdateBufferSize: number;

  // Rebuild threshold (% of false positives)
  rebuildThreshold: number;
}

class BloomWriteManager {
  private globalBloom: BloomFilter;
  private fileBlooms: Map<string, BloomFilter>;
  private pendingInserts: Map<string, string[]>;
  private config: BloomWriteConfig;

  async insertEntity(entity: Node, targetFile: string): Promise<void> {
    const key = `${entity.ns}|${entity.type}|${entity.id}`;

    // 1. Update global bloom (sync for consistency)
    if (this.config.syncGlobalUpdate) {
      this.globalBloom.insert(key);
      await this.persistGlobalBloom();
    } else {
      this.globalBloom.insert(key);
      this.scheduleGlobalPersist();
    }

    // 2. Buffer file-level insert
    if (!this.pendingInserts.has(targetFile)) {
      this.pendingInserts.set(targetFile, []);
    }
    this.pendingInserts.get(targetFile)!.push(key);

    // 3. Flush if buffer is full
    if (this.pendingInserts.get(targetFile)!.length >= this.config.fileUpdateBufferSize) {
      await this.flushFileBloom(targetFile);
    }
  }

  async insertEdge(edge: Edge, targetFile: string): Promise<void> {
    // Insert exact edge key
    const exactKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}|${edge.to_id}`;

    // Insert outgoing pattern
    const outKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}`;

    // Insert incoming pattern
    const inKey = `${edge.ns}|${edge.to_id}|${edge.rel_type}`;

    await Promise.all([
      this.edgeExactBloom.insert(exactKey),
      this.edgeOutgoingBloom.insert(outKey),
      this.edgeIncomingBloom.insert(inKey)
    ]);
  }

  private async flushFileBloom(fileId: string): Promise<void> {
    const pending = this.pendingInserts.get(fileId) || [];
    const bloom = this.fileBlooms.get(fileId) || this.createFileBloom();

    for (const key of pending) {
      bloom.insert(key);
    }

    await this.persistFileBloom(fileId, bloom);
    this.pendingInserts.set(fileId, []);
    this.fileBlooms.set(fileId, bloom);
  }

  /**
   * Called during compaction to rebuild bloom filters
   */
  async rebuildFromParquet(parquetFiles: string[]): Promise<void> {
    // Reset global bloom
    this.globalBloom = new BloomFilter(
      this.estimateElementCount(parquetFiles),
      this.config.targetFPR
    );

    // Process each file
    for (const file of parquetFiles) {
      const entities = await scanParquetFile(file);
      const fileBloom = this.createFileBloom();

      for (const entity of entities) {
        const key = `${entity.ns}|${entity.type}|${entity.id}`;
        this.globalBloom.insert(key);
        fileBloom.insert(key);
      }

      await this.persistFileBloom(file, fileBloom);
    }

    await this.persistGlobalBloom();
  }
}
```

### Handling Deletes

```typescript
/**
 * Delete handling with soft deletes and compaction
 */
class BloomDeleteHandler {
  private countingBloom: CountingBloomFilter | null;
  private deletedKeys: Set<string>;  // Track deletes until compaction

  constructor(useCountingBloom: boolean) {
    if (useCountingBloom) {
      this.countingBloom = new CountingBloomFilter(10_000_000, 0.01);
    }
    this.deletedKeys = new Set();
  }

  async deleteEntity(ns: string, type: string, id: string): Promise<void> {
    const key = `${ns}|${type}|${id}`;

    if (this.countingBloom) {
      // Counting bloom: decrement counters
      this.countingBloom.delete(key);
    } else {
      // Standard bloom: track for compaction
      this.deletedKeys.add(key);
    }
  }

  async checkExists(ns: string, type: string, id: string): Promise<boolean> {
    const key = `${ns}|${type}|${id}`;

    // Check if explicitly deleted
    if (this.deletedKeys.has(key)) {
      return false;
    }

    // Check bloom filter
    if (this.countingBloom) {
      return this.countingBloom.contains(key);
    }

    // For standard bloom, we can't distinguish deleted items
    // Must check actual data
    return true;  // "maybe exists"
  }

  /**
   * Compaction: rebuild bloom excluding deleted keys
   */
  async compactionRebuild(
    sourceFiles: string[],
    deletedEntities: Set<string>
  ): Promise<BloomFilter> {
    const newBloom = new BloomFilter(
      await this.estimateRemainingCount(sourceFiles, deletedEntities),
      0.01
    );

    for (const file of sourceFiles) {
      const entities = await scanParquetFile(file);

      for (const entity of entities) {
        if (entity.deleted) continue;

        const key = `${entity.ns}|${entity.type}|${entity.id}`;
        if (!deletedEntities.has(key)) {
          newBloom.insert(key);
        }
      }
    }

    return newBloom;
  }
}
```

---

## Query Optimization

### Row Group Skipping with Bloom Filters

```typescript
/**
 * Query optimizer that combines bloom filters with statistics
 */
class BloomQueryOptimizer {
  private bloomFilters: BloomFilterManager;
  private statsCache: Map<string, RowGroupStats[]>;

  /**
   * Optimize point lookup query
   */
  async optimizePointLookup(
    ns: string,
    type: string,
    id: string
  ): Promise<QueryPlan> {
    const key = `${ns}|${type}|${id}`;

    // Step 1: Check global bloom
    if (!this.bloomFilters.global.contains(key)) {
      return { type: 'empty', reason: 'Global bloom negative' };
    }

    // Step 2: Find candidate files
    const candidateFiles: FileCandidate[] = [];

    for (const [fileId, fileBloom] of this.bloomFilters.files) {
      if (!fileBloom.contains(key)) {
        continue;  // Skip file
      }

      // Step 3: Find candidate row groups within file
      const rowGroupCandidates: number[] = [];
      const stats = this.statsCache.get(fileId) || [];

      for (let rgIdx = 0; rgIdx < stats.length; rgIdx++) {
        const rgStats = stats[rgIdx];

        // Check zone maps
        if (id < rgStats.stats.id?.min || id > rgStats.stats.id?.max) {
          continue;
        }

        // Check per-row-group bloom if available
        const rgBloom = this.bloomFilters.getRowGroupBloom(fileId, rgIdx);
        if (rgBloom && !rgBloom.contains(key)) {
          continue;
        }

        rowGroupCandidates.push(rgIdx);
      }

      if (rowGroupCandidates.length > 0) {
        candidateFiles.push({ fileId, rowGroups: rowGroupCandidates });
      }
    }

    if (candidateFiles.length === 0) {
      return { type: 'empty', reason: 'No candidate files after bloom filtering' };
    }

    return {
      type: 'scan',
      files: candidateFiles,
      estimatedRows: this.estimateRows(candidateFiles),
      bloomFiltered: true
    };
  }

  /**
   * Optimize edge existence check
   */
  async optimizeEdgeExists(
    ns: string,
    fromId: string,
    relType: string,
    toId: string
  ): Promise<QueryPlan> {
    const edgeKey = `${ns}|${fromId}|${relType}|${toId}`;

    // Fast path: bloom says no
    if (!this.bloomFilters.edgeExact.contains(edgeKey)) {
      return { type: 'empty', reason: 'Edge bloom negative' };
    }

    // Need to scan - find candidate files in forward index
    return this.findEdgeCandidates(ns, fromId, relType, toId);
  }

  /**
   * Optimize graph traversal with bloom pre-filtering
   */
  async optimizeTraversal(
    ns: string,
    startId: string,
    relType: string,
    depth: number
  ): Promise<TraversalPlan> {
    const plan: TraversalPlan = {
      startId,
      relType,
      depth,
      pruneNodes: [],
      estimatedExpansion: 1
    };

    // Check if start node has any outgoing edges
    const outKey = `${ns}|${startId}|${relType}`;
    if (!this.bloomFilters.edgeOutgoing.contains(outKey)) {
      plan.pruneNodes.push(startId);
      plan.estimatedExpansion = 0;
      return plan;
    }

    // Estimate expansion factor from historical data
    plan.estimatedExpansion = await this.getAverageOutDegree(ns, relType);

    return plan;
  }
}

interface QueryPlan {
  type: 'empty' | 'scan' | 'index';
  reason?: string;
  files?: FileCandidate[];
  estimatedRows?: number;
  bloomFiltered?: boolean;
}

interface FileCandidate {
  fileId: string;
  rowGroups: number[];
}

interface TraversalPlan {
  startId: string;
  relType: string;
  depth: number;
  pruneNodes: string[];
  estimatedExpansion: number;
}
```

### Query Statistics and Bloom Effectiveness

```typescript
/**
 * Track bloom filter effectiveness for tuning
 */
interface BloomStats {
  // Queries that bloom returned negative (true negatives + false negatives)
  bloomNegatives: number;

  // Queries that bloom returned positive
  bloomPositives: number;

  // Bloom positives that were actually found (true positives)
  truePositives: number;

  // Bloom positives that weren't found (false positives)
  falsePositives: number;

  // Rows skipped due to bloom filters
  rowsSkipped: number;

  // Rows scanned
  rowsScanned: number;
}

class BloomStatsCollector {
  private stats: BloomStats = {
    bloomNegatives: 0,
    bloomPositives: 0,
    truePositives: 0,
    falsePositives: 0,
    rowsSkipped: 0,
    rowsScanned: 0
  };

  recordLookup(
    bloomResult: boolean,
    actualResult: boolean,
    rowsSkipped: number,
    rowsScanned: number
  ): void {
    if (!bloomResult) {
      this.stats.bloomNegatives++;
    } else {
      this.stats.bloomPositives++;
      if (actualResult) {
        this.stats.truePositives++;
      } else {
        this.stats.falsePositives++;
      }
    }
    this.stats.rowsSkipped += rowsSkipped;
    this.stats.rowsScanned += rowsScanned;
  }

  getEffectiveness(): {
    observedFPR: number;
    skipRatio: number;
    recommendation: string;
  } {
    const observedFPR = this.stats.bloomPositives > 0
      ? this.stats.falsePositives / this.stats.bloomPositives
      : 0;

    const totalRows = this.stats.rowsSkipped + this.stats.rowsScanned;
    const skipRatio = totalRows > 0
      ? this.stats.rowsSkipped / totalRows
      : 0;

    let recommendation = 'Bloom filters performing well';

    if (observedFPR > 0.05) {
      recommendation = 'Consider increasing bloom filter size to reduce FPR';
    } else if (skipRatio < 0.5) {
      recommendation = 'Low skip ratio - verify bloom filter is being used correctly';
    }

    return { observedFPR, skipRatio, recommendation };
  }
}
```

---

## Integration with hyparquet

### Reading Parquet with Bloom Filter Skipping

```typescript
import { parquetRead, parquetMetadata } from 'hyparquet';

/**
 * hyparquet integration for bloom filter row group skipping
 */
class HyparquetBloomReader {
  private bloomFilters: BloomFilterManager;

  /**
   * Read entities with bloom filter optimization
   */
  async readEntities(
    fileBuffer: ArrayBuffer,
    filter: { ns?: string; type?: string; id?: string }
  ): Promise<Node[]> {
    // Get file metadata
    const metadata = await parquetMetadata(fileBuffer);

    // Build candidate row groups
    const candidateRowGroups: number[] = [];

    for (let rgIdx = 0; rgIdx < metadata.row_groups.length; rgIdx++) {
      const rg = metadata.row_groups[rgIdx];

      // Check statistics (zone maps)
      if (filter.id) {
        const idCol = rg.columns.find(c => c.meta_data?.path_in_schema.join('.') === 'id');
        const stats = idCol?.meta_data?.statistics;

        if (stats) {
          if (filter.id < stats.min_value || filter.id > stats.max_value) {
            continue;  // Skip based on zone maps
          }
        }
      }

      // Check Parquet built-in bloom filter (if available)
      const bloomHeader = await this.getParquetBloomHeader(fileBuffer, rgIdx, 'id');
      if (bloomHeader && filter.id) {
        const bloomData = await this.readParquetBloomData(fileBuffer, bloomHeader);
        if (!this.checkParquetBloom(bloomData, filter.id)) {
          continue;  // Skip based on Parquet bloom
        }
      }

      // Check external bloom filter
      if (filter.ns && filter.type && filter.id) {
        const key = `${filter.ns}|${filter.type}|${filter.id}`;
        const rgBloom = this.bloomFilters.getRowGroupBloom(metadata.fileId, rgIdx);
        if (rgBloom && !rgBloom.contains(key)) {
          continue;  // Skip based on external bloom
        }
      }

      candidateRowGroups.push(rgIdx);
    }

    // Read only candidate row groups
    const results: Node[] = [];

    for (const rgIdx of candidateRowGroups) {
      const rg = metadata.row_groups[rgIdx];

      await parquetRead({
        file: fileBuffer,
        rowStart: rg.row_start,
        rowEnd: rg.row_start + rg.num_rows,
        columns: ['ns', 'id', 'ts', 'type', 'data', 'deleted', 'version'],
        onComplete: (rows) => {
          for (const row of rows) {
            // Apply remaining filters
            if (filter.ns && row.ns !== filter.ns) continue;
            if (filter.type && row.type !== filter.type) continue;
            if (filter.id && row.id !== filter.id) continue;
            if (row.deleted) continue;

            results.push(row as Node);
          }
        }
      });
    }

    return results;
  }

  /**
   * Read Parquet's built-in bloom filter from footer
   */
  private async getParquetBloomHeader(
    buffer: ArrayBuffer,
    rowGroupIndex: number,
    columnName: string
  ): Promise<ParquetBloomHeader | null> {
    // Parquet bloom filter is stored after footer
    // Need to parse footer to find bloom filter offsets

    const metadata = await parquetMetadata(buffer);
    const rg = metadata.row_groups[rowGroupIndex];
    const col = rg.columns.find(c =>
      c.meta_data?.path_in_schema.join('.') === columnName
    );

    if (!col?.meta_data?.bloom_filter_offset) {
      return null;
    }

    // Read bloom filter header
    const view = new DataView(buffer);
    const offset = Number(col.meta_data.bloom_filter_offset);

    return {
      numBytes: view.getUint32(offset, true),
      algorithm: 'XXHASH',
      offset: offset + 4
    };
  }

  private checkParquetBloom(
    bloomData: Uint8Array,
    value: string
  ): boolean {
    // Parquet uses split block bloom filter
    // See: https://github.com/apache/parquet-format/blob/master/BloomFilter.md

    const hash = xxhash64(value);
    const numBits = bloomData.length * 8;

    // Split block bloom filter check
    const blockIndex = Number(hash % BigInt(bloomData.length / 32));
    const blockOffset = blockIndex * 32;

    // Check 8 bits using SALT values
    const SALT = [
      0x47b6137bn, 0x44974d91n, 0x8824ad5bn, 0xa2b7289dn,
      0x705495c7n, 0x2df1424bn, 0x9efc4947n, 0x5c6bfb31n
    ];

    for (let i = 0; i < 8; i++) {
      const bitIndex = Number((hash * SALT[i]) >> 27n) % 256;
      const byteIndex = blockOffset + Math.floor(bitIndex / 8);
      const bitOffset = bitIndex % 8;

      if ((bloomData[byteIndex] & (1 << bitOffset)) === 0) {
        return false;
      }
    }

    return true;
  }
}
```

### Writing Parquet with Bloom Filters

```typescript
import { parquetWrite } from 'hyparquet/write';

/**
 * Write Parquet files with bloom filter generation
 */
class HyparquetBloomWriter {
  private bloomConfig: BloomWriteConfig;

  async writeNodesWithBloom(
    nodes: Node[],
    outputPath: string
  ): Promise<{ parquetPath: string; bloomPath: string }> {
    // Group by row group size
    const rowGroupSize = 10000;
    const rowGroups: Node[][] = [];

    for (let i = 0; i < nodes.length; i += rowGroupSize) {
      rowGroups.push(nodes.slice(i, i + rowGroupSize));
    }

    // Build bloom filters while writing
    const fileBloom = new BloomFilter(nodes.length, 0.01);
    const rowGroupBlooms: BloomFilter[] = [];

    for (const rg of rowGroups) {
      const rgBloom = new BloomFilter(rg.length, 0.01);

      for (const node of rg) {
        const key = `${node.ns}|${node.type}|${node.id}`;
        fileBloom.insert(key);
        rgBloom.insert(key);
      }

      rowGroupBlooms.push(rgBloom);
    }

    // Write Parquet file
    const parquetBuffer = await parquetWrite({
      schema: NODE_PARQUET_SCHEMA,
      rows: nodes,
      rowGroupSize,
      compression: 'ZSTD',
      // Enable Parquet built-in bloom filters for id column
      bloomFilterColumns: ['id']
    });

    await writeFile(outputPath, parquetBuffer);

    // Write external bloom filter file
    const bloomPath = outputPath.replace('.parquet', '.bloom');
    const bloomFile = this.serializeBloomFile({
      fileBloom,
      rowGroupBlooms
    });

    await writeFile(bloomPath, bloomFile);

    return { parquetPath: outputPath, bloomPath };
  }

  private serializeBloomFile(blooms: {
    fileBloom: BloomFilter;
    rowGroupBlooms: BloomFilter[];
  }): Uint8Array {
    const entries: BloomFilterEntry[] = [];

    // File-level bloom
    entries.push({
      scope: { type: 'file' },
      keyType: 0x01,  // Entity
      elementCount: blooms.fileBloom.count,
      falsePositiveRate: blooms.fileBloom.fpr,
      hashFunctions: blooms.fileBloom.k,
      filterData: blooms.fileBloom.serialize()
    });

    // Row group blooms
    for (let i = 0; i < blooms.rowGroupBlooms.length; i++) {
      const rgBloom = blooms.rowGroupBlooms[i];
      entries.push({
        scope: { type: 'row_group', rowGroupIndex: i },
        keyType: 0x01,
        elementCount: rgBloom.count,
        falsePositiveRate: rgBloom.fpr,
        hashFunctions: rgBloom.k,
        filterData: rgBloom.serialize()
      });
    }

    return this.encodeBloomFile(entries);
  }
}
```

---

## Implementation

### Core Bloom Filter Class

```typescript
/**
 * Production-ready Bloom Filter implementation for ParqueDB
 */
export class BloomFilter {
  private bits: Uint8Array;
  private k: number;
  private m: number;
  private n: number = 0;

  constructor(expectedElements: number, falsePositiveRate: number = 0.01) {
    // Calculate optimal parameters
    this.m = Math.ceil(-1.44 * expectedElements * Math.log2(falsePositiveRate));
    this.k = Math.round(-Math.log2(falsePositiveRate));

    // Allocate bit array
    this.bits = new Uint8Array(Math.ceil(this.m / 8));
  }

  /**
   * Insert an element into the bloom filter
   */
  insert(key: string): void {
    const hashes = this.computeHashes(key);

    for (const hash of hashes) {
      const bitIndex = hash % this.m;
      const byteIndex = Math.floor(bitIndex / 8);
      const bitOffset = bitIndex % 8;
      this.bits[byteIndex] |= (1 << bitOffset);
    }

    this.n++;
  }

  /**
   * Check if an element might be in the set
   * Returns false = definitely not in set
   * Returns true = probably in set (may be false positive)
   */
  contains(key: string): boolean {
    const hashes = this.computeHashes(key);

    for (const hash of hashes) {
      const bitIndex = hash % this.m;
      const byteIndex = Math.floor(bitIndex / 8);
      const bitOffset = bitIndex % 8;

      if ((this.bits[byteIndex] & (1 << bitOffset)) === 0) {
        return false;
      }
    }

    return true;
  }

  /**
   * Compute k hash values using double hashing
   * h(i) = h1 + i * h2
   */
  private computeHashes(key: string): number[] {
    // Use two independent hashes
    const h1 = this.murmur3_32(key, 0);
    const h2 = this.murmur3_32(key, h1);

    const hashes: number[] = [];
    for (let i = 0; i < this.k; i++) {
      hashes.push(Math.abs((h1 + i * h2) | 0));
    }

    return hashes;
  }

  /**
   * MurmurHash3 32-bit implementation
   */
  private murmur3_32(key: string, seed: number): number {
    let h1 = seed;
    const c1 = 0xcc9e2d51;
    const c2 = 0x1b873593;

    const encoder = new TextEncoder();
    const data = encoder.encode(key);
    const len = data.length;
    const nblocks = Math.floor(len / 4);

    // Body
    for (let i = 0; i < nblocks; i++) {
      let k1 = (data[i * 4] |
                (data[i * 4 + 1] << 8) |
                (data[i * 4 + 2] << 16) |
                (data[i * 4 + 3] << 24)) >>> 0;

      k1 = Math.imul(k1, c1);
      k1 = (k1 << 15) | (k1 >>> 17);
      k1 = Math.imul(k1, c2);

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);
      h1 = Math.imul(h1, 5) + 0xe6546b64;
    }

    // Tail
    let k1 = 0;
    const tail = len & 3;
    if (tail >= 3) k1 ^= data[nblocks * 4 + 2] << 16;
    if (tail >= 2) k1 ^= data[nblocks * 4 + 1] << 8;
    if (tail >= 1) {
      k1 ^= data[nblocks * 4];
      k1 = Math.imul(k1, c1);
      k1 = (k1 << 15) | (k1 >>> 17);
      k1 = Math.imul(k1, c2);
      h1 ^= k1;
    }

    // Finalization
    h1 ^= len;
    h1 ^= h1 >>> 16;
    h1 = Math.imul(h1, 0x85ebca6b);
    h1 ^= h1 >>> 13;
    h1 = Math.imul(h1, 0xc2b2ae35);
    h1 ^= h1 >>> 16;

    return h1 >>> 0;
  }

  /**
   * Serialize bloom filter to bytes
   */
  serialize(): Uint8Array {
    const header = new Uint8Array(12);
    const view = new DataView(header.buffer);

    view.setUint32(0, this.m, true);
    view.setUint32(4, this.k, true);
    view.setUint32(8, this.n, true);

    const result = new Uint8Array(12 + this.bits.length);
    result.set(header);
    result.set(this.bits, 12);

    return result;
  }

  /**
   * Deserialize bloom filter from bytes
   */
  static deserialize(data: Uint8Array): BloomFilter {
    const view = new DataView(data.buffer, data.byteOffset);

    const m = view.getUint32(0, true);
    const k = view.getUint32(4, true);
    const n = view.getUint32(8, true);

    const filter = new BloomFilter(1, 0.01);  // Dummy params
    filter.m = m;
    filter.k = k;
    filter.n = n;
    filter.bits = data.slice(12);

    return filter;
  }

  /**
   * Get current false positive rate based on elements inserted
   */
  get currentFPR(): number {
    return Math.pow(1 - Math.exp(-this.k * this.n / this.m), this.k);
  }

  /**
   * Get number of elements inserted
   */
  get count(): number {
    return this.n;
  }

  /**
   * Get target false positive rate
   */
  get fpr(): number {
    return Math.pow(1 - Math.exp(-this.k * this.m / this.m), this.k);
  }

  /**
   * Get size in bytes
   */
  get sizeBytes(): number {
    return this.bits.length;
  }
}
```

### Bloom Filter Manager

```typescript
/**
 * Manages bloom filters across the storage hierarchy
 */
export class BloomFilterManager {
  private globalEntityBloom: BloomFilter;
  private globalEdgeBloom: BloomFilter;
  private fileBlooms: Map<string, BloomFilter> = new Map();
  private rowGroupBlooms: Map<string, BloomFilter[]> = new Map();
  private edgeOutgoingBloom: BloomFilter;
  private edgeIncomingBloom: BloomFilter;

  constructor(config: BloomConfig) {
    this.globalEntityBloom = new BloomFilter(
      config.expectedEntities,
      config.entityFPR
    );

    this.globalEdgeBloom = new BloomFilter(
      config.expectedEdges,
      config.edgeFPR
    );

    this.edgeOutgoingBloom = new BloomFilter(
      config.expectedEdges * 0.1,
      config.edgeFPR
    );

    this.edgeIncomingBloom = new BloomFilter(
      config.expectedEdges * 0.1,
      config.edgeFPR
    );
  }

  /**
   * Load bloom filters from storage
   */
  async loadFromStorage(basePath: string): Promise<void> {
    // Load global entity bloom
    try {
      const globalData = await readFile(`${basePath}/nodes/bloom/global.bloom`);
      const file = parseBloomFile(globalData);
      this.globalEntityBloom = BloomFilter.deserialize(file.filters[0].filterData);
    } catch {
      // No existing bloom filter
    }

    // Load global edge bloom
    try {
      const edgeData = await readFile(`${basePath}/edges/bloom/edge.bloom`);
      const file = parseBloomFile(edgeData);

      for (const filter of file.filters) {
        switch (filter.keyType) {
          case 0x02:
            this.globalEdgeBloom = BloomFilter.deserialize(filter.filterData);
            break;
          case 0x03:
            this.edgeOutgoingBloom = BloomFilter.deserialize(filter.filterData);
            break;
          case 0x04:
            this.edgeIncomingBloom = BloomFilter.deserialize(filter.filterData);
            break;
        }
      }
    } catch {
      // No existing edge bloom filter
    }

    // Load file-level blooms
    const bloomFiles = await glob(`${basePath}/nodes/bloom/*.bloom`);
    for (const bloomFile of bloomFiles) {
      if (bloomFile.includes('global')) continue;

      const fileId = path.basename(bloomFile, '.bloom');
      const data = await readFile(bloomFile);
      const file = parseBloomFile(data);

      // File-level bloom
      const fileBloom = file.filters.find(f => f.scope.type === 'file');
      if (fileBloom) {
        this.fileBlooms.set(fileId, BloomFilter.deserialize(fileBloom.filterData));
      }

      // Row group blooms
      const rgBlooms = file.filters
        .filter(f => f.scope.type === 'row_group')
        .sort((a, b) => a.scope.rowGroupIndex - b.scope.rowGroupIndex)
        .map(f => BloomFilter.deserialize(f.filterData));

      if (rgBlooms.length > 0) {
        this.rowGroupBlooms.set(fileId, rgBlooms);
      }
    }
  }

  /**
   * Check entity existence in hierarchy
   */
  maybeHasEntity(ns: string, type: string, id: string): boolean {
    const key = `${ns}|${type}|${id}`;
    return this.globalEntityBloom.contains(key);
  }

  /**
   * Check edge existence
   */
  maybeHasEdge(
    ns: string,
    fromId: string,
    relType: string,
    toId: string
  ): boolean {
    const key = `${ns}|${fromId}|${relType}|${toId}`;
    return this.globalEdgeBloom.contains(key);
  }

  /**
   * Check if node has outgoing edges of type
   */
  maybeHasOutgoing(ns: string, fromId: string, relType: string): boolean {
    const key = `${ns}|${fromId}|${relType}`;
    return this.edgeOutgoingBloom.contains(key);
  }

  /**
   * Check if node has incoming edges of type
   */
  maybeHasIncoming(ns: string, toId: string, relType: string): boolean {
    const key = `${ns}|${toId}|${relType}`;
    return this.edgeIncomingBloom.contains(key);
  }

  /**
   * Get candidate files for entity lookup
   */
  getCandidateFiles(ns: string, type: string, id: string): string[] {
    const key = `${ns}|${type}|${id}`;
    const candidates: string[] = [];

    for (const [fileId, bloom] of this.fileBlooms) {
      if (bloom.contains(key)) {
        candidates.push(fileId);
      }
    }

    return candidates;
  }

  /**
   * Get candidate row groups within a file
   */
  getCandidateRowGroups(
    fileId: string,
    ns: string,
    type: string,
    id: string
  ): number[] {
    const key = `${ns}|${type}|${id}`;
    const rgBlooms = this.rowGroupBlooms.get(fileId);

    if (!rgBlooms) {
      return []; // All row groups are candidates
    }

    const candidates: number[] = [];
    for (let i = 0; i < rgBlooms.length; i++) {
      if (rgBlooms[i].contains(key)) {
        candidates.push(i);
      }
    }

    return candidates;
  }

  /**
   * Insert entity into all relevant bloom filters
   */
  insertEntity(node: Node, fileId: string, rowGroupIndex: number): void {
    const key = `${node.ns}|${node.type}|${node.id}`;

    // Global
    this.globalEntityBloom.insert(key);

    // File-level
    let fileBloom = this.fileBlooms.get(fileId);
    if (!fileBloom) {
      fileBloom = new BloomFilter(100000, 0.01);
      this.fileBlooms.set(fileId, fileBloom);
    }
    fileBloom.insert(key);

    // Row group
    let rgBlooms = this.rowGroupBlooms.get(fileId);
    if (!rgBlooms) {
      rgBlooms = [];
      this.rowGroupBlooms.set(fileId, rgBlooms);
    }
    while (rgBlooms.length <= rowGroupIndex) {
      rgBlooms.push(new BloomFilter(10000, 0.01));
    }
    rgBlooms[rowGroupIndex].insert(key);
  }

  /**
   * Insert edge into all relevant bloom filters
   */
  insertEdge(edge: Edge): void {
    const exactKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}|${edge.to_id}`;
    const outKey = `${edge.ns}|${edge.from_id}|${edge.rel_type}`;
    const inKey = `${edge.ns}|${edge.to_id}|${edge.rel_type}`;

    this.globalEdgeBloom.insert(exactKey);
    this.edgeOutgoingBloom.insert(outKey);
    this.edgeIncomingBloom.insert(inKey);
  }

  /**
   * Persist bloom filters to storage
   */
  async persistToStorage(basePath: string): Promise<void> {
    // Save global entity bloom
    await writeFile(
      `${basePath}/nodes/bloom/global.bloom`,
      this.serializeBloomFile([{
        scope: { type: 'global' },
        keyType: 0x01,
        bloom: this.globalEntityBloom
      }])
    );

    // Save global edge blooms
    await writeFile(
      `${basePath}/edges/bloom/edge.bloom`,
      this.serializeBloomFile([
        { scope: { type: 'global' }, keyType: 0x02, bloom: this.globalEdgeBloom },
        { scope: { type: 'global' }, keyType: 0x03, bloom: this.edgeOutgoingBloom },
        { scope: { type: 'global' }, keyType: 0x04, bloom: this.edgeIncomingBloom }
      ])
    );

    // Save per-file blooms
    for (const [fileId, fileBloom] of this.fileBlooms) {
      const entries = [
        { scope: { type: 'file' }, keyType: 0x01, bloom: fileBloom }
      ];

      const rgBlooms = this.rowGroupBlooms.get(fileId);
      if (rgBlooms) {
        for (let i = 0; i < rgBlooms.length; i++) {
          entries.push({
            scope: { type: 'row_group', rowGroupIndex: i },
            keyType: 0x01,
            bloom: rgBlooms[i]
          });
        }
      }

      await writeFile(
        `${basePath}/nodes/bloom/${fileId}.bloom`,
        this.serializeBloomFile(entries)
      );
    }
  }

  private serializeBloomFile(entries: BloomEntry[]): Uint8Array {
    // Implementation of bloom file format serialization
    // (See Index File Format section for details)
    const header = new Uint8Array(16);
    header.set([0x50, 0x51, 0x42, 0x4C]); // "PQBL"
    // ... rest of serialization
    return header;
  }
}

interface BloomConfig {
  expectedEntities: number;
  expectedEdges: number;
  entityFPR: number;
  edgeFPR: number;
}

interface BloomEntry {
  scope: { type: 'global' } | { type: 'file' } | { type: 'row_group'; rowGroupIndex: number };
  keyType: number;
  bloom: BloomFilter;
}
```

---

## Performance Expectations

### Bloom Filter Operation Costs

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| Insert | O(k) | - | k hash computations |
| Lookup | O(k) | - | k hash computations + k bit reads |
| Serialize | O(m/8) | m/8 bytes | Copy bit array |
| Deserialize | O(m/8) | m/8 bytes | Copy bit array |

### Expected Query Improvements

| Query Type | Without Bloom | With Bloom | Improvement |
|------------|---------------|------------|-------------|
| Point lookup (miss) | Scan all RGs | 0 RGs | 100% skip |
| Point lookup (hit) | Scan all RGs | 1-2 RGs | 90%+ skip |
| Edge existence (miss) | Scan forward index | 0 scans | 100% skip |
| Edge existence (hit) | Scan forward index | Targeted scan | 80%+ skip |
| Graph traversal | Full edge scan | Pruned traversal | 50-90% skip |

### Memory Budget Guidelines

| Workload | Entity Bloom | Edge Bloom | Total |
|----------|--------------|------------|-------|
| Small (1M entities) | 1.2 MB | 1.8 MB | 3 MB |
| Medium (10M entities) | 12 MB | 18 MB | 30 MB |
| Large (100M entities) | 120 MB | 180 MB | 300 MB |

---

## Summary

ParqueDB's bloom filter indexing strategy provides:

1. **Three-level hierarchy** (global, file, row group) for efficient filtering
2. **Specialized edge blooms** for exact, outgoing, and incoming edge queries
3. **Integration with hyparquet** for combined zone map and bloom filtering
4. **External bloom files** for compound keys and cross-file queries
5. **Support for counting bloom filters** when delete tracking is needed
6. **Cuckoo filter alternative** for high-cardinality, deletion-heavy workloads
7. **Blocked bloom filters** for cache-efficient batch operations

The design balances:
- **Space efficiency**: ~10 bits per element at 1% FPR
- **Query performance**: Skip 90%+ of row groups for point lookups
- **Write overhead**: Minimal - just k hash computations per insert
- **Flexibility**: Multiple filter types for different access patterns

---

*Architecture Design Document - ParqueDB Bloom Filter Indexes*
