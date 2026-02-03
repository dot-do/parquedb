---
title: Graph-First Architecture
description: Graph-optimized architecture for ParqueDB, treating relationships (edges) as first-class citizens with separate edge storage, bidirectional traversal indexes, and unified CDC event log for time-travel.
---

**Architecture Perspective**: Graph-first with edges as first-class citizens

This document presents a graph-optimized architecture for ParqueDB, treating relationships (edges) as equal peers to entities (nodes), with a unified CDC event log for time-travel and replayability.

---

## Executive Summary

Traditional databases treat relationships as foreign keys embedded in entity tables. This architecture inverts that model:

1. **nodes.parquet** - Entity data with Variant payload
2. **edges.parquet** - Relationships stored separately for efficient traversal
3. **events.parquet** - Unified CDC log enabling time-travel for both

This separation enables:
- O(1) edge lookups via sorted indexes
- Efficient graph traversal without entity table scans
- Independent scaling of entity data vs relationship cardinality
- Native support for graphdl relationship operators (`->`, `~>`, `<-`, `<~`)

---

## Schema Definitions

### 1. nodes.parquet - Entity Storage

```
nodes.parquet
├── ns: string               # Namespace (e.g., "example.com/crm")
├── id: string               # Entity identifier
├── ts: int64                # Timestamp (microseconds since epoch)
├── type: string             # Entity type (e.g., "Person", "Organization")
├── data: variant            # Semi-structured entity payload
├── version: int32           # Optimistic concurrency version
├── deleted: boolean         # Soft delete flag
└── metadata: variant        # System metadata (created_by, etc.)
```

**Variant Type Structure** (inspired by ClickHouse/Iceberg):
```typescript
type Variant =
  | { type: 'null' }
  | { type: 'boolean', value: boolean }
  | { type: 'int64', value: bigint }
  | { type: 'float64', value: number }
  | { type: 'string', value: string }
  | { type: 'binary', value: Uint8Array }
  | { type: 'date', value: number }  // days since epoch
  | { type: 'timestamp', value: bigint }  // microseconds
  | { type: 'array', elements: Variant[] }
  | { type: 'object', fields: Record<string, Variant> }
```

**Parquet Physical Layout**:
```
Row Group 0 (ns="example.com/crm", type="Person")
├── ns: BYTE_ARRAY, DICT_ENCODED
├── id: BYTE_ARRAY, PLAIN
├── ts: INT64, DELTA_ENCODING
├── type: BYTE_ARRAY, DICT_ENCODED
├── data: BYTE_ARRAY (shredded variant)
│   ├── data.name: BYTE_ARRAY, DICT_ENCODED
│   ├── data.email: BYTE_ARRAY, PLAIN
│   ├── data.age: INT32, DELTA_ENCODING
│   └── data._variant_metadata: BYTE_ARRAY (path + type info)
├── version: INT32
├── deleted: BOOLEAN
└── metadata: BYTE_ARRAY (variant)
```

**Sort Order**: `(ns, type, id, ts DESC)` - enables efficient type-scoped queries with latest-first ordering.

---

### 2. edges.parquet - Relationship Storage

```
edges.parquet
├── ns: string               # Namespace
├── from_id: string          # Source entity ID
├── to_id: string            # Target entity ID
├── rel_type: string         # Relationship type (e.g., "author", "knows")
├── ts: int64                # Timestamp
├── data: variant            # Edge properties (weight, metadata)
├── operator: string         # graphdl operator: "->", "~>", "<-", "<~"
├── bidirectional: boolean   # If true, implicit reverse edge exists
├── confidence: float32      # For fuzzy (~>) relationships: 0.0-1.0
├── version: int32           # Optimistic concurrency
└── deleted: boolean         # Soft delete
```

**Relationship Operator Semantics** (from graphdl):

| Operator | Direction | Match Mode | Use Case |
|----------|-----------|------------|----------|
| `->` | forward | exact | Foreign key reference |
| `~>` | forward | fuzzy | AI-matched semantic reference |
| `<-` | backward | exact | Backlink/parent reference |
| `<~` | backward | fuzzy | AI-matched backlink |

**Parquet Physical Layout**:
```
Row Group 0 (ns="example.com/crm")
├── ns: BYTE_ARRAY, DICT_ENCODED
├── from_id: BYTE_ARRAY, PLAIN
├── to_id: BYTE_ARRAY, PLAIN
├── rel_type: BYTE_ARRAY, DICT_ENCODED
├── ts: INT64, DELTA_ENCODING
├── data: BYTE_ARRAY (variant)
├── operator: BYTE_ARRAY, DICT_ENCODED (4 values max)
├── bidirectional: BOOLEAN
├── confidence: FLOAT
├── version: INT32
└── deleted: BOOLEAN
```

**Sort Orders** (multiple files for different access patterns):

1. **Forward Traversal Index**: `(ns, from_id, rel_type, to_id, ts DESC)`
   - Query: "Find all posts by author X"
   - Pattern: `WHERE from_id = 'user:123' AND rel_type = 'author'`

2. **Reverse Traversal Index**: `(ns, to_id, rel_type, from_id, ts DESC)`
   - Query: "Find all comments on post Y"
   - Pattern: `WHERE to_id = 'post:456' AND rel_type = 'comment_on'`

3. **Type-Scoped Index**: `(ns, rel_type, from_id, to_id, ts DESC)`
   - Query: "Find all 'knows' relationships in namespace"
   - Pattern: `WHERE rel_type = 'knows'`

---

### 3. events.parquet - Unified CDC Log

```
events.parquet
├── event_id: string         # Unique event identifier (ULID)
├── target: string           # "node" | "edge"
├── ns: string               # Namespace
├── entity_id: string        # For nodes: id; for edges: from_id|rel_type|to_id
├── ts: int64                # Event timestamp
├── op: string               # "INSERT" | "UPDATE" | "DELETE"
├── before: variant          # Previous state (null for INSERT)
├── after: variant           # New state (null for DELETE)
├── tx_id: string            # Transaction ID for grouping
├── user_id: string          # Who made the change
└── metadata: variant        # Additional context (reason, trace_id)
```

**Event ID Format** (ULID for time-ordering):
```
01ARZ3NDEKTSV4RRFFQ69G5FAV
└┬─────────────────────────┘
 └─ 48-bit timestamp + 80-bit random
```

**Parquet Physical Layout**:
```
Row Group 0 (time-partitioned: 2024-01-22)
├── event_id: BYTE_ARRAY, PLAIN
├── target: BYTE_ARRAY, DICT_ENCODED (2 values)
├── ns: BYTE_ARRAY, DICT_ENCODED
├── entity_id: BYTE_ARRAY, PLAIN
├── ts: INT64, DELTA_ENCODING
├── op: BYTE_ARRAY, DICT_ENCODED (3 values)
├── before: BYTE_ARRAY (variant, nullable)
├── after: BYTE_ARRAY (variant, nullable)
├── tx_id: BYTE_ARRAY, DICT_ENCODED
├── user_id: BYTE_ARRAY, DICT_ENCODED
└── metadata: BYTE_ARRAY (variant)
```

**Sort Order**: `(ts, event_id)` - enables efficient time-range scans for replay.

**Partitioning**: `day={YYYY-MM-DD}` - daily partitions for retention management.

---

## Index Strategies for Graph Operations

### Adjacency List Materialization

For high-degree nodes, materialize adjacency lists as separate files:

```
adjacency/
├── {ns}/
│   ├── {node_id}/
│   │   ├── outgoing.parquet      # (rel_type, to_id, ts, data)
│   │   └── incoming.parquet      # (rel_type, from_id, ts, data)
```

**Threshold**: Nodes with >1000 edges get dedicated adjacency files.

### Bloom Filters for Edge Existence

Store Bloom filters per row group for quick edge existence checks:

```typescript
interface EdgeBloomFilter {
  namespace: string;
  rowGroup: number;
  // Bloom filter keyed on: hash(from_id + rel_type + to_id)
  filter: Uint8Array;
  falsePositiveRate: 0.01;
}
```

**Query**: "Does edge (A, knows, B) exist?" - Check bloom filter before reading row group.

### Zone Maps for Range Queries

Parquet row group statistics enable predicate pushdown:

```typescript
interface RowGroupStats {
  rowGroup: number;
  from_id: { min: string, max: string, nullCount: number };
  to_id: { min: string, max: string, nullCount: number };
  rel_type: { distinctValues: string[] };
  ts: { min: bigint, max: bigint };
}
```

---

## Graph Query Patterns

### Pattern 1: Forward Traversal

**Query**: Find all entities that User X "knows"

```typescript
// Using forward index: (ns, from_id, rel_type, to_id, ts DESC)
async function traverse(
  ns: string,
  fromId: string,
  relType: string,
  options?: { limit?: number; asOf?: bigint }
): Promise<Edge[]> {
  const filter = {
    ns: { eq: ns },
    from_id: { eq: fromId },
    rel_type: { eq: relType },
    deleted: { eq: false },
    ...(options?.asOf && { ts: { lte: options.asOf } })
  };

  return await parquet.scan('edges_forward.parquet', {
    filter,
    projection: ['to_id', 'ts', 'data', 'confidence'],
    limit: options?.limit
  });
}
```

### Pattern 2: Reverse Traversal

**Query**: Find all entities that reference Post Y

```typescript
// Using reverse index: (ns, to_id, rel_type, from_id, ts DESC)
async function reverseTraverse(
  ns: string,
  toId: string,
  relType?: string
): Promise<Edge[]> {
  const filter = {
    ns: { eq: ns },
    to_id: { eq: toId },
    deleted: { eq: false },
    ...(relType && { rel_type: { eq: relType } })
  };

  return await parquet.scan('edges_reverse.parquet', {
    filter,
    projection: ['from_id', 'rel_type', 'ts', 'data']
  });
}
```

### Pattern 3: Path Traversal (Multi-hop)

**Query**: Find friends-of-friends (A -> knows -> B -> knows -> C)

```typescript
async function pathTraversal(
  ns: string,
  startId: string,
  pathSpec: string[],  // ['knows', 'knows']
  maxDepth: number = 3
): Promise<PathResult[]> {
  const visited = new Set<string>();
  const paths: PathResult[] = [];

  async function dfs(
    currentId: string,
    depth: number,
    path: string[]
  ): Promise<void> {
    if (depth >= pathSpec.length) {
      paths.push({ nodes: path, depth });
      return;
    }

    if (visited.has(currentId)) return;
    visited.add(currentId);

    const edges = await traverse(ns, currentId, pathSpec[depth]);

    for (const edge of edges) {
      await dfs(edge.to_id, depth + 1, [...path, edge.to_id]);
    }
  }

  await dfs(startId, 0, [startId]);
  return paths;
}
```

### Pattern 4: Bidirectional Edge Resolution

**Query**: For edges marked bidirectional, resolve both directions

```typescript
async function resolveBidirectional(
  ns: string,
  nodeId: string,
  relType: string
): Promise<{ outgoing: Edge[]; incoming: Edge[] }> {
  const [outgoing, incoming] = await Promise.all([
    // Direct edges from this node
    parquet.scan('edges_forward.parquet', {
      filter: { ns: { eq: ns }, from_id: { eq: nodeId }, rel_type: { eq: relType } }
    }),
    // Edges where this node is target AND bidirectional=true
    parquet.scan('edges_reverse.parquet', {
      filter: {
        ns: { eq: ns },
        to_id: { eq: nodeId },
        rel_type: { eq: relType },
        bidirectional: { eq: true }
      }
    })
  ]);

  return { outgoing, incoming };
}
```

### Pattern 5: Fuzzy Relationship Matching

**Query**: Find semantic relationships with confidence threshold

```typescript
// For ~> and <~ operators
async function fuzzyMatch(
  ns: string,
  fromId: string,
  relType: string,
  minConfidence: number = 0.8
): Promise<Edge[]> {
  return await parquet.scan('edges_forward.parquet', {
    filter: {
      ns: { eq: ns },
      from_id: { eq: fromId },
      rel_type: { eq: relType },
      operator: { in: ['~>', '<~'] },
      confidence: { gte: minConfidence }
    },
    projection: ['to_id', 'confidence', 'data']
  });
}
```

---

## Time-Travel on Graph State

### Point-in-Time Graph Snapshot

```typescript
async function graphAsOf(
  ns: string,
  timestamp: bigint
): Promise<{ nodes: Node[]; edges: Edge[] }> {
  // Replay events up to timestamp
  const events = await parquet.scan('events.parquet', {
    filter: {
      ns: { eq: ns },
      ts: { lte: timestamp }
    },
    sort: [{ column: 'ts', order: 'asc' }, { column: 'event_id', order: 'asc' }]
  });

  const nodeState = new Map<string, Node>();
  const edgeState = new Map<string, Edge>();

  for (const event of events) {
    if (event.target === 'node') {
      applyNodeEvent(nodeState, event);
    } else {
      applyEdgeEvent(edgeState, event);
    }
  }

  return {
    nodes: Array.from(nodeState.values()),
    edges: Array.from(edgeState.values())
  };
}

function applyNodeEvent(state: Map<string, Node>, event: Event): void {
  switch (event.op) {
    case 'INSERT':
    case 'UPDATE':
      state.set(event.entity_id, event.after as Node);
      break;
    case 'DELETE':
      state.delete(event.entity_id);
      break;
  }
}
```

### Incremental Time-Travel

For efficient incremental queries, maintain checkpoints:

```
checkpoints/
├── {ns}/
│   ├── 2024-01-22T00:00:00Z/
│   │   ├── nodes_snapshot.parquet
│   │   ├── edges_snapshot.parquet
│   │   └── checkpoint.json
│   └── 2024-01-23T00:00:00Z/
│       └── ...
```

Query pattern:
1. Load nearest checkpoint before target timestamp
2. Replay events from checkpoint to target
3. Return materialized state

---

## Materialized Paths for Common Traversals

### Materialized Path Table

For frequently traversed paths, pre-compute and store:

```
paths.parquet
├── ns: string
├── path_type: string        # e.g., "org_hierarchy", "social_2hop"
├── start_id: string
├── end_id: string
├── hops: int32              # Number of edges in path
├── path: array<string>      # [node1, edge1, node2, edge2, ...]
├── total_weight: float64    # Aggregated edge weights
├── computed_at: int64       # When this path was materialized
└── valid_until: int64       # Expiration timestamp
```

### Path Materialization Strategy

```typescript
interface PathMaterializationConfig {
  pathType: string;
  startNodeFilter: (node: Node) => boolean;
  relTypes: string[];
  maxDepth: number;
  aggregation: 'shortest' | 'all' | 'weighted';
  refreshInterval: number;  // seconds
}

const orgHierarchyConfig: PathMaterializationConfig = {
  pathType: 'org_hierarchy',
  startNodeFilter: (n) => n.type === 'Person',
  relTypes: ['reports_to'],
  maxDepth: 10,
  aggregation: 'shortest',
  refreshInterval: 3600  // hourly
};
```

### Incremental Path Updates

When edges change, update affected paths:

```typescript
async function onEdgeChange(event: Event): Promise<void> {
  if (event.target !== 'edge') return;

  const edge = event.after as Edge;

  // Find all materialized paths that include this edge
  const affectedPaths = await parquet.scan('paths.parquet', {
    filter: {
      ns: { eq: edge.ns },
      path: { contains: edge.from_id }  // Check if path includes affected node
    }
  });

  // Mark paths for recomputation
  for (const path of affectedPaths) {
    await invalidatePath(path);
  }

  // Trigger async recomputation
  await schedulePathRecomputation(edge.ns, affectedPaths.map(p => p.path_type));
}
```

---

## Supporting graphdl Relationship Operators

### Schema Definition Integration

```typescript
import { Graph, parseOperator } from '@graphdl/core';

// Define schema with relationship operators
const schema = Graph({
  User: {
    $type: 'https://schema.org.ai/Person',
    name: 'string',
    email: 'email',
    // Exact forward reference with backref
    organization: '->Organization.members',
    // Fuzzy forward reference (AI-matched)
    interests: ['~>Topic.interested_users'],
  },

  Organization: {
    $type: 'https://schema.org.ai/Organization',
    name: 'string',
  },

  Topic: {
    $type: 'https://schema.org.ai/Thing',
    name: 'string',
  }
});

// Convert to ParqueDB edge definitions
function schemaToEdges(schema: ParsedGraph): EdgeDefinition[] {
  const edges: EdgeDefinition[] = [];

  for (const [entityName, entity] of schema.entities) {
    for (const [fieldName, field] of entity.fields) {
      if (field.isRelation) {
        edges.push({
          from_type: entityName,
          rel_type: fieldName,
          to_type: field.type,
          operator: field.operator,
          backref: field.backref,
          isArray: field.isArray,
          confidence_required: field.operator.includes('~') ? 0.8 : null
        });
      }
    }
  }

  return edges;
}
```

### Operator-Aware Query Execution

```typescript
async function queryByOperator(
  ns: string,
  fromId: string,
  field: ParsedField
): Promise<Node[]> {
  const { operator, type: targetType, threshold } = parseOperator(field.raw);

  // Determine index based on operator direction
  const isForward = operator === '->' || operator === '~>';
  const index = isForward ? 'edges_forward.parquet' : 'edges_reverse.parquet';
  const idField = isForward ? 'from_id' : 'to_id';
  const targetField = isForward ? 'to_id' : 'from_id';

  // Build filter
  const filter: Record<string, any> = {
    ns: { eq: ns },
    [idField]: { eq: fromId },
    rel_type: { eq: field.name },
    operator: { eq: operator }
  };

  // Add confidence filter for fuzzy operators
  if (operator === '~>' || operator === '<~') {
    filter.confidence = { gte: threshold || 0.8 };
  }

  // Fetch edges
  const edges = await parquet.scan(index, { filter });

  // Resolve target nodes
  const targetIds = edges.map(e => e[targetField]);
  return await parquet.scan('nodes.parquet', {
    filter: {
      ns: { eq: ns },
      id: { in: targetIds },
      type: { eq: targetType }
    }
  });
}
```

---

## Storage Layout on R2/fsx

```
/warehouse/
├── {namespace}/                      # e.g., "example.com/crm"
│   ├── nodes/
│   │   ├── metadata/
│   │   │   └── v{n}.metadata.json    # Iceberg-style metadata
│   │   ├── manifests/
│   │   │   └── snap-{id}.json
│   │   └── data/
│   │       ├── type=Person/
│   │       │   └── {uuid}.parquet
│   │       └── type=Organization/
│   │           └── {uuid}.parquet
│   │
│   ├── edges/
│   │   ├── forward/                  # Sorted by (from_id, rel_type)
│   │   │   └── {uuid}.parquet
│   │   ├── reverse/                  # Sorted by (to_id, rel_type)
│   │   │   └── {uuid}.parquet
│   │   └── type/                     # Sorted by (rel_type, from_id)
│   │       └── {uuid}.parquet
│   │
│   ├── events/
│   │   ├── day=2024-01-22/
│   │   │   └── {uuid}.parquet
│   │   └── day=2024-01-23/
│   │       └── {uuid}.parquet
│   │
│   ├── adjacency/                    # For high-degree nodes
│   │   └── {node_id}/
│   │       ├── outgoing.parquet
│   │       └── incoming.parquet
│   │
│   ├── paths/                        # Materialized paths
│   │   └── {path_type}/
│   │       └── {uuid}.parquet
│   │
│   └── checkpoints/                  # Time-travel checkpoints
│       └── {timestamp}/
│           ├── nodes_snapshot.parquet
│           └── edges_snapshot.parquet
```

---

## Performance Characteristics

### Read Patterns

| Operation | Index Used | Time Complexity | Notes |
|-----------|------------|-----------------|-------|
| Get node by ID | nodes (ns, type, id) | O(log n) | Single row group scan |
| Forward traverse | edges_forward | O(log n + k) | k = result count |
| Reverse traverse | edges_reverse | O(log n + k) | k = result count |
| Edge existence | Bloom filter | O(1) | False positives require scan |
| Time-travel (point) | events + checkpoint | O(m) | m = events since checkpoint |
| Path lookup | paths (materialized) | O(log n) | Pre-computed |
| Multi-hop traverse | edges_forward (repeated) | O(d * k) | d = depth, k = avg degree |

### Write Patterns

| Operation | Files Updated | Latency | Notes |
|-----------|---------------|---------|-------|
| Insert node | nodes + events | ~50ms | CDC buffered |
| Insert edge | edges (3 indexes) + events | ~100ms | 3 sorted files |
| Update node | nodes + events | ~50ms | Append-only |
| Delete edge | edges + events | ~100ms | Soft delete |
| Compaction | All data files | Background | Hourly/daily |

### Storage Overhead

```
Base entity: 1x (nodes.parquet)
With edges:
  - Forward index: +0.3x
  - Reverse index: +0.3x
  - Type index:    +0.2x
Events CDC:  +0.5x (7-day retention)
Materialized paths: +0.1x (for configured paths)

Total: ~2.4x raw entity storage
```

---

## Comparison: Inline Arrays vs Separate Edges

| Aspect | Inline Arrays | Separate Edges (This Architecture) |
|--------|---------------|-----------------------------------|
| Storage | Denormalized in entity | Normalized, deduplicated |
| Read single entity | One file read | One file + edge lookup |
| Graph traversal | Full entity scan | Index-only scan |
| Update single edge | Rewrite entire entity | Append to edge file |
| Bidirectional | Store twice or compute | Single row + flag |
| High-cardinality | Entity bloat | Dedicated adjacency files |
| Time-travel | Per-entity versioning | Per-edge versioning |
| Analytics | Limited | Full relationship analytics |

**Recommendation**: Separate edge storage is superior for:
- Graphs with >10 avg edges per node
- Applications requiring reverse traversal
- Time-travel on relationship state
- Relationship-centric analytics

---

## Implementation with hyparquet

### Reading with hyparquet

```typescript
import { parquetRead } from 'hyparquet';
import { toJson } from 'hyparquet/variants';

async function readNodes(
  bucket: R2Bucket,
  ns: string,
  filter: { type?: string; ids?: string[] }
): Promise<Node[]> {
  const files = await listDataFiles(bucket, `${ns}/nodes/data/`);
  const results: Node[] = [];

  for (const file of files) {
    // Apply partition pruning
    if (filter.type && !file.path.includes(`type=${filter.type}`)) {
      continue;
    }

    const data = await bucket.get(file.path);
    const buffer = await data.arrayBuffer();

    await parquetRead({
      file: buffer,
      rowStart: 0,
      rowEnd: Infinity,
      columns: ['ns', 'id', 'ts', 'type', 'data', 'deleted'],
      onComplete: (rows) => {
        for (const row of rows) {
          if (row.deleted) continue;
          if (filter.ids && !filter.ids.includes(row.id)) continue;

          results.push({
            ns: row.ns,
            id: row.id,
            ts: row.ts,
            type: row.type,
            data: toJson(row.data)  // Convert Variant to JSON
          });
        }
      }
    });
  }

  return results;
}
```

### Writing with hyparquet

```typescript
import { parquetWrite } from 'hyparquet/write';
import { toVariant } from 'hyparquet/variants';

async function writeEdges(
  bucket: R2Bucket,
  ns: string,
  edges: Edge[]
): Promise<void> {
  // Prepare rows with Variant encoding
  const rows = edges.map(edge => ({
    ns: edge.ns,
    from_id: edge.from_id,
    to_id: edge.to_id,
    rel_type: edge.rel_type,
    ts: edge.ts,
    data: toVariant(edge.data),
    operator: edge.operator,
    bidirectional: edge.bidirectional,
    confidence: edge.confidence,
    version: edge.version,
    deleted: false
  }));

  // Sort for forward index
  rows.sort((a, b) =>
    a.ns.localeCompare(b.ns) ||
    a.from_id.localeCompare(b.from_id) ||
    a.rel_type.localeCompare(b.rel_type) ||
    a.to_id.localeCompare(b.to_id)
  );

  const buffer = await parquetWrite({
    schema: EDGE_SCHEMA,
    rows,
    compression: 'ZSTD'
  });

  const filename = `${ns}/edges/forward/${crypto.randomUUID()}.parquet`;
  await bucket.put(filename, buffer);

  // Also write reverse index (re-sorted)
  rows.sort((a, b) =>
    a.ns.localeCompare(b.ns) ||
    a.to_id.localeCompare(b.to_id) ||
    a.rel_type.localeCompare(b.rel_type) ||
    a.from_id.localeCompare(b.from_id)
  );

  const reverseBuffer = await parquetWrite({
    schema: EDGE_SCHEMA,
    rows,
    compression: 'ZSTD'
  });

  const reverseFilename = `${ns}/edges/reverse/${crypto.randomUUID()}.parquet`;
  await bucket.put(reverseFilename, reverseBuffer);
}
```

---

## Conclusion

This graph-first architecture positions ParqueDB as a native graph database built on Parquet:

1. **Edges are first-class** - Not embedded in entities, but stored and indexed independently
2. **Multiple traversal patterns** - Forward, reverse, and type-based indexes
3. **Time-travel built-in** - Unified CDC log enables point-in-time graph snapshots
4. **graphdl native** - Direct support for relationship operators (`->`, `~>`, `<-`, `<~`)
5. **Materialized paths** - Pre-computed paths for common traversal patterns
6. **Variant type** - Semi-structured data in both nodes and edge properties

The separation of nodes and edges enables efficient graph operations while maintaining the analytical power of columnar storage. Combined with hyparquet for pure-JS read/write and fsx for edge persistence, this architecture delivers a fully-featured graph database on Cloudflare Workers.

---

*Architecture Design Document - ParqueDB Graph-First Perspective*
