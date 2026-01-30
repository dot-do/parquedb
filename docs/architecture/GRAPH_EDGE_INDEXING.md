# ParqueDB: Advanced Edge Indexing and Traversal Strategies

**Supplement to Graph-First Architecture**

This document provides deep technical analysis of edge storage and indexing strategies optimized for Parquet's columnar format, building on the core graph-first architecture.

---

## Table of Contents

1. [Edge Index File Organization](#edge-index-file-organization)
2. [Sort Key Selection and Trade-offs](#sort-key-selection-and-trade-offs)
3. [Row Group Optimization](#row-group-optimization)
4. [Bloom Filter Design](#bloom-filter-design)
5. [Super-Node Handling](#super-node-handling)
6. [Query Execution Plans](#query-execution-plans)
7. [Compaction Strategies](#compaction-strategies)
8. [Benchmark Considerations](#benchmark-considerations)

---

## Edge Index File Organization

### The Three-Index Strategy

Parquet files are immutable and sorted by a single key. To enable efficient traversal in multiple directions, we maintain three separate edge index files per namespace:

```
edges/
├── forward/                  # Primary: outgoing edge lookup
│   └── {partition}/
│       └── {uuid}.parquet   # Sort: (ns, from_id, rel_type, to_id, ts DESC)
│
├── reverse/                  # Secondary: incoming edge lookup
│   └── {partition}/
│       └── {uuid}.parquet   # Sort: (ns, to_id, rel_type, from_id, ts DESC)
│
└── type/                     # Tertiary: relationship analytics
    └── {partition}/
        └── {uuid}.parquet   # Sort: (ns, rel_type, ts DESC, from_id, to_id)
```

### Why Three Indexes Instead of One?

**Problem**: Parquet predicate pushdown only works efficiently when filtering on sort key prefix columns.

**Example**: With sort order `(from_id, rel_type, to_id)`:
- `WHERE from_id = 'A' AND rel_type = 'knows'` - Excellent (uses prefix)
- `WHERE to_id = 'B'` - Poor (full scan required)

**Solution**: Maintain pre-sorted copies for each access pattern.

### Storage Overhead Analysis

```
Base edge data: N edges × avg_row_size
Three indexes:  3N × avg_row_size = 3x storage

With Parquet compression (ZSTD level 3):
- Dictionary encoding on ns, rel_type, operator: ~90% reduction
- Delta encoding on ts: ~70% reduction
- Effective overhead: ~1.8x (not 3x)

Trade-off: 1.8x storage for O(log n) lookup in any direction
```

---

## Sort Key Selection and Trade-offs

### Forward Index: `(ns, from_id, rel_type, to_id, ts DESC)`

**Optimized queries**:
```sql
-- Find all edges from node A
SELECT * FROM edges_forward WHERE ns = 'crm' AND from_id = 'user:123'

-- Find specific relationship type from A
SELECT * FROM edges_forward
WHERE ns = 'crm' AND from_id = 'user:123' AND rel_type = 'follows'

-- Check if specific edge exists
SELECT * FROM edges_forward
WHERE ns = 'crm' AND from_id = 'user:123' AND rel_type = 'follows' AND to_id = 'user:456'
```

**Why `ts DESC` at the end?**
- Most queries want latest state
- Enables efficient "latest version" queries without sorting
- Time-travel queries can still use range predicates

### Reverse Index: `(ns, to_id, rel_type, from_id, ts DESC)`

**Optimized queries**:
```sql
-- Find all edges pointing TO node B (backlinks)
SELECT * FROM edges_reverse WHERE ns = 'crm' AND to_id = 'post:789'

-- Find who follows user X (reverse of "follows" relationship)
SELECT * FROM edges_reverse
WHERE ns = 'crm' AND to_id = 'user:123' AND rel_type = 'follows'
```

### Type Index: `(ns, rel_type, ts DESC, from_id, to_id)`

**Optimized queries**:
```sql
-- Analytics: count all "follows" relationships
SELECT COUNT(*) FROM edges_type WHERE ns = 'crm' AND rel_type = 'follows'

-- Recent activity: last 1000 "purchase" events
SELECT * FROM edges_type
WHERE ns = 'crm' AND rel_type = 'purchase'
ORDER BY ts DESC LIMIT 1000

-- Relationship statistics per type
SELECT rel_type, COUNT(*) FROM edges_type WHERE ns = 'crm' GROUP BY rel_type
```

### Alternative Sort Keys Considered

| Sort Order | Use Case | Rejected Because |
|------------|----------|------------------|
| `(from_id, to_id, rel_type)` | Edge existence check | Rare to query without rel_type |
| `(rel_type, from_id, to_id)` | Type-first queries | ns filtering becomes expensive |
| `(ts, from_id, to_id)` | Time-series edges | Most traversals are point-in-time |
| `(hash(from_id), ...)` | Even distribution | Breaks range queries |

---

## Row Group Optimization

### Row Group Size Selection

Parquet row groups are the unit of predicate pushdown. Optimal sizing balances:

```
Small row groups (1MB):
  + Fine-grained predicate pushdown
  + Lower memory during scan
  - More metadata overhead
  - Worse compression ratios

Large row groups (128MB):
  + Better compression
  + Fewer file seeks
  - Coarse predicate pushdown
  - Higher memory usage
```

**Recommendation for edges**: 8-16MB row groups
- Typical edge row: ~200 bytes
- Rows per group: ~40,000-80,000 edges
- Balance between pushdown granularity and compression

### Row Group Partitioning by Node Degree

```typescript
interface RowGroupStrategy {
  // Partition high-degree nodes into their own row groups
  // This enables skipping entire row groups for low-degree traversals

  strategy: 'degree_aware';

  // Row groups are organized:
  // - RG 0-N: Normal nodes (degree < threshold)
  // - RG N+1...: One row group per super-node

  superNodeThreshold: 10000;  // edges
  normalRowGroupSize: 50000;  // edges
}
```

### Zone Maps (Min/Max Statistics)

Parquet automatically maintains min/max statistics per column per row group:

```typescript
interface RowGroupZoneMap {
  rowGroup: number;
  columns: {
    from_id: { min: 'user:001', max: 'user:999' };
    to_id: { min: 'post:100', max: 'post:500' };
    rel_type: { min: 'author', max: 'follows' };  // Dictionary encoded
    ts: { min: 1706000000000n, max: 1706100000000n };
  };
}

// Query: WHERE from_id = 'user:500' AND rel_type = 'follows'
// Zone map check:
//   - from_id 'user:500' in ['user:001', 'user:999']? YES
//   - rel_type 'follows' in ['author', 'follows']? YES
// Result: Row group may contain matching rows, scan it
```

---

## Bloom Filter Design

### Edge Existence Bloom Filters

For "does edge exist?" queries, Bloom filters provide O(1) probabilistic checks:

```typescript
interface EdgeBloomFilterConfig {
  // Key format: hash(from_id || rel_type || to_id)
  keyFormat: 'composite';

  // Parameters for 1% false positive rate at 100K edges
  numBits: 958506;      // ~117KB per row group
  numHashFunctions: 7;

  // Store in Parquet file footer or sidecar
  storageLocation: 'parquet_footer';
}

// Bloom filter per row group in edge file
interface EdgeFileBloomFilters {
  rowGroups: Array<{
    rowGroupIndex: number;
    edgeCount: number;
    bloomFilter: Uint8Array;
  }>;
}
```

### Query Flow with Bloom Filters

```typescript
async function edgeExists(
  ns: string,
  fromId: string,
  relType: string,
  toId: string
): Promise<boolean> {
  const key = hashEdgeKey(fromId, relType, toId);

  // 1. Load bloom filters for namespace (cached)
  const filters = await loadBloomFilters(ns);

  // 2. Check each row group's bloom filter
  const candidateRowGroups: number[] = [];
  for (const { rowGroupIndex, bloomFilter } of filters.rowGroups) {
    if (bloomFilter.mightContain(key)) {
      candidateRowGroups.push(rowGroupIndex);
    }
  }

  // 3. If no candidates, edge definitely doesn't exist
  if (candidateRowGroups.length === 0) {
    return false;
  }

  // 4. Scan only candidate row groups
  for (const rgIndex of candidateRowGroups) {
    const found = await scanRowGroup(rgIndex, { ns, fromId, relType, toId });
    if (found) return true;
  }

  return false;
}
```

### False Positive Rate Trade-offs

| FPR | Bits per edge | Memory (1M edges) | Extra scans |
|-----|---------------|-------------------|-------------|
| 10% | 4.8 bits | 600 KB | ~10% queries |
| 1% | 9.6 bits | 1.2 MB | ~1% queries |
| 0.1% | 14.4 bits | 1.8 MB | ~0.1% queries |

**Recommendation**: 1% FPR - good balance for typical workloads.

---

## Super-Node Handling

### The Super-Node Problem

Social graphs have power-law degree distributions:
- Most users: 10-100 connections
- Popular users: 10,000-1,000,000 followers

Naive edge storage causes:
- Hot row groups (always scanned)
- Memory pressure during traversal
- Slow writes (large row group rewrites)

### Dedicated Adjacency Files

For nodes exceeding the degree threshold, create dedicated files:

```
adjacency/
└── {ns}/
    └── {node_id}/
        ├── outgoing.parquet    # Sort: (rel_type, to_id, ts DESC)
        ├── incoming.parquet    # Sort: (rel_type, from_id, ts DESC)
        └── manifest.json       # Metadata
```

**Manifest format**:
```json
{
  "node_id": "user:celebrity",
  "created_at": "2024-01-22T00:00:00Z",
  "outgoing_count": 150000,
  "incoming_count": 2500000,
  "rel_type_counts": {
    "follows": { "outgoing": 1000, "incoming": 2500000 },
    "posts": { "outgoing": 15000, "incoming": 0 }
  },
  "last_compaction": "2024-01-21T12:00:00Z"
}
```

### Hybrid Query Routing

```typescript
async function traverseFrom(
  ns: string,
  fromId: string,
  relType: string
): Promise<Edge[]> {
  // 1. Check if super-node adjacency exists
  const adjacencyPath = `adjacency/${ns}/${fromId}/outgoing.parquet`;
  const hasAdjacency = await fileExists(adjacencyPath);

  if (hasAdjacency) {
    // 2a. Query dedicated adjacency file
    return await scanAdjacencyFile(adjacencyPath, { relType });
  } else {
    // 2b. Query main edge index
    return await scanEdgeIndex('forward', { ns, fromId, relType });
  }
}
```

### Incremental Adjacency Updates

Super-node adjacency files use append-only updates:

```typescript
interface AdjacencyUpdate {
  // New edges appended to delta file
  deltaFile: string;  // adjacency/{ns}/{node_id}/delta_{timestamp}.parquet

  // Periodic compaction merges deltas
  compactionTrigger: {
    deltaCount: 10;      // Compact after 10 delta files
    deltaSize: '100MB';  // Or total delta size exceeds 100MB
  };
}
```

---

## Query Execution Plans

### Single-Hop Traversal

**Query**: Find all users that user:123 follows

```typescript
// Execution plan
{
  operation: 'TRAVERSE_FORWARD',
  inputs: { ns: 'social', from_id: 'user:123', rel_type: 'follows' },
  steps: [
    {
      step: 'CHECK_ADJACENCY',
      path: 'adjacency/social/user:123/outgoing.parquet',
      estimatedCost: 'O(1) existence check'
    },
    {
      step: 'SCAN_INDEX',
      index: 'forward',
      predicates: [
        { column: 'ns', op: '=', value: 'social' },
        { column: 'from_id', op: '=', value: 'user:123' },
        { column: 'rel_type', op: '=', value: 'follows' },
        { column: 'deleted', op: '=', value: false }
      ],
      projection: ['to_id', 'ts', 'data', 'confidence'],
      estimatedCost: 'O(log n + k) where k = result count'
    }
  ]
}
```

### Multi-Hop Traversal (BFS)

**Query**: Find friends-of-friends (2-hop)

```typescript
// Execution plan
{
  operation: 'MULTI_HOP_BFS',
  inputs: {
    ns: 'social',
    start_id: 'user:123',
    path: ['follows', 'follows'],
    max_depth: 2
  },
  steps: [
    {
      step: 'HOP_1',
      traverse: { from: 'user:123', rel: 'follows' },
      output: 'level_1_nodes[]',
      estimatedCardinality: 100
    },
    {
      step: 'HOP_2',
      traverse: { from: 'level_1_nodes[]', rel: 'follows' },
      output: 'level_2_nodes[]',
      // Batch traversal: single scan with IN predicate
      batchSize: 1000,
      estimatedCardinality: 10000
    },
    {
      step: 'DEDUPE',
      // Remove start node and level 1 from results
      exclude: ['user:123', 'level_1_nodes[]']
    }
  ]
}
```

### Bidirectional Search

**Query**: Find shortest path between user:A and user:B

```typescript
// Execution plan - meet in the middle
{
  operation: 'BIDIRECTIONAL_SEARCH',
  inputs: { ns: 'social', start: 'user:A', end: 'user:B', rel_type: 'knows' },
  steps: [
    {
      step: 'PARALLEL_EXPAND',
      forward: { from: 'user:A', visited: Set<string>, frontier: Set<string> },
      backward: { from: 'user:B', visited: Set<string>, frontier: Set<string> }
    },
    {
      step: 'CHECK_INTERSECTION',
      // After each expansion, check if frontiers intersect
      condition: 'forward.frontier ∩ backward.frontier ≠ ∅'
    },
    {
      step: 'RECONSTRUCT_PATH',
      // Build path from intersection point
    }
  ],
  maxDepth: 6,  // Social networks: 6 degrees of separation
  estimatedCost: 'O(b^(d/2)) vs O(b^d) for unidirectional'
}
```

### Time-Travel Query

**Query**: Graph state as of timestamp T

```typescript
// Execution plan
{
  operation: 'TIME_TRAVEL_TRAVERSE',
  inputs: { ns: 'social', from_id: 'user:123', as_of: 1706000000000n },
  steps: [
    {
      step: 'FIND_CHECKPOINT',
      // Find nearest checkpoint before target time
      targetTime: 1706000000000n,
      checkpointPath: 'checkpoints/social/2024-01-22T00:00:00Z/'
    },
    {
      step: 'LOAD_SNAPSHOT',
      // Load edges from checkpoint
      snapshotFile: 'edges_snapshot.parquet'
    },
    {
      step: 'REPLAY_EVENTS',
      // Apply events from checkpoint to target time
      eventsPath: 'events/day=2024-01-22/',
      filter: { ts: { gt: checkpointTime, lte: targetTime } }
    },
    {
      step: 'TRAVERSE_MATERIALIZED',
      // Traverse on materialized graph state
    }
  ]
}
```

---

## Compaction Strategies

### Edge File Compaction

Over time, edge files accumulate:
- Small files from writes
- Deleted edges (tombstones)
- Outdated versions

**Compaction goals**:
1. Merge small files into optimal-sized files
2. Remove deleted edges
3. Keep only latest version per edge (optional)
4. Re-sort for optimal predicate pushdown

### Compaction Algorithm

```typescript
interface CompactionConfig {
  // Trigger compaction when:
  triggers: {
    fileCount: 100;        // Too many small files
    deletedRatio: 0.3;     // 30% of edges deleted
    sizeRatio: 0.5;        // Files < 50% of target size
  };

  // Target output:
  targetFileSize: '128MB';
  targetRowGroupSize: '16MB';

  // Retention:
  keepVersions: 1;         // Latest only (unless time-travel needed)
  tombstoneRetention: '7d'; // Keep deletes for event replay
}

async function compactEdgeIndex(
  ns: string,
  index: 'forward' | 'reverse' | 'type'
): Promise<void> {
  // 1. List all files for this index
  const files = await listFiles(`edges/${index}/${ns}/`);

  // 2. Group files by partition
  const partitions = groupByPartition(files);

  for (const partition of partitions) {
    // 3. Read all edges, filtering deleted
    const edges: Edge[] = [];
    for (const file of partition.files) {
      const fileEdges = await readParquet(file);
      edges.push(...fileEdges.filter(e => !e.deleted));
    }

    // 4. Deduplicate by (from_id, rel_type, to_id), keeping latest
    const deduplicated = deduplicateEdges(edges);

    // 5. Sort by index order
    const sorted = sortEdges(deduplicated, index);

    // 6. Write new compacted file
    const outputFile = `edges/${index}/${ns}/${newUUID()}.parquet`;
    await writeParquet(outputFile, sorted, {
      rowGroupSize: '16MB',
      compression: 'ZSTD'
    });

    // 7. Atomically swap file references
    await updateManifest(ns, index, {
      remove: partition.files,
      add: [outputFile]
    });

    // 8. Delete old files (after retention period)
    await scheduleDelete(partition.files, '24h');
  }
}
```

### Compaction Impact on Queries

During compaction, queries must handle file transitions:

```typescript
// Manifest-based file listing
interface EdgeIndexManifest {
  version: number;
  files: Array<{
    path: string;
    rowCount: number;
    minKey: string;
    maxKey: string;
    status: 'active' | 'compacting' | 'deleted';
  }>;
}

// Query reads manifest snapshot
async function queryWithManifest(
  ns: string,
  index: string,
  predicate: Predicate
): Promise<Edge[]> {
  // 1. Get current manifest (atomic read)
  const manifest = await getManifest(ns, index);

  // 2. Query only 'active' files
  const activeFiles = manifest.files.filter(f => f.status === 'active');

  // 3. Use min/max keys for file pruning
  const relevantFiles = activeFiles.filter(f =>
    keyInRange(predicate.fromId, f.minKey, f.maxKey)
  );

  // 4. Scan relevant files
  return await scanFiles(relevantFiles, predicate);
}
```

---

## Benchmark Considerations

### Test Scenarios

| Scenario | Description | Key Metrics |
|----------|-------------|-------------|
| Point lookup | Single edge existence | Latency p50/p99 |
| Fan-out | All edges from node | Throughput, latency |
| Fan-in | All edges to node | Throughput, latency |
| 2-hop BFS | Friends of friends | Latency, memory |
| Path finding | Shortest path A-B | Latency, hops explored |
| Bulk insert | 1M edges | Throughput, file count |
| Time-travel | Query at past time | Latency overhead |

### Expected Performance Characteristics

```typescript
// Synthetic benchmark targets (Cloudflare Workers environment)
const benchmarkTargets = {
  // Point operations
  edgeExists: { p50: '5ms', p99: '20ms' },
  singleEdgeRead: { p50: '10ms', p99: '50ms' },

  // Traversals (100 avg edges per node)
  forwardTraverse: { p50: '50ms', p99: '200ms' },
  reverseTraverse: { p50: '50ms', p99: '200ms' },

  // Multi-hop (assuming 100 edges/node, 2 hops)
  twoHopBFS: { p50: '500ms', p99: '2000ms' },

  // Bulk operations
  bulkInsert1M: { throughput: '10K edges/sec' },

  // Time-travel overhead
  timeTravelOverhead: { factor: '2-5x vs current state' }
};
```

### Memory Budget

Cloudflare Workers: 128MB memory limit

```typescript
// Memory allocation strategy
const memoryBudget = {
  total: 128 * 1024 * 1024,  // 128MB

  // Allocations
  parquetMetadata: 5 * 1024 * 1024,   // 5MB - cached schemas
  bloomFilters: 10 * 1024 * 1024,     // 10MB - edge existence
  rowGroupBuffer: 20 * 1024 * 1024,   // 20MB - single RG read
  resultBuffer: 50 * 1024 * 1024,     // 50MB - query results
  traversalState: 30 * 1024 * 1024,   // 30MB - BFS visited set
  overhead: 13 * 1024 * 1024          // 13MB - runtime, stack
};
```

---

## Summary

The three-index strategy for edge storage provides:

1. **O(log n) lookups** in any direction via dedicated sorted indexes
2. **Predicate pushdown** through careful sort key selection
3. **O(1) existence checks** via Bloom filters
4. **Super-node isolation** preventing hot-spot row groups
5. **Efficient compaction** maintaining query performance over time

Key trade-offs:
- **Storage**: ~1.8x overhead (with compression)
- **Write amplification**: 3x (one write per index)
- **Complexity**: Manifest-based file management

This architecture enables graph operations at analytical database scale while maintaining sub-second latency for traversal queries.

---

*Technical Supplement to ParqueDB Graph-First Architecture*
