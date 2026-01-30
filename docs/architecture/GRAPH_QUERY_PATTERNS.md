# ParqueDB: Graph Query Patterns and Optimization

**Practical Guide to Graph Operations on Parquet**

This document provides detailed query patterns, optimization techniques, and implementation strategies for the graph-first architecture.

---

## Table of Contents

1. [Core Query Operations](#core-query-operations)
2. [graphdl Operator Implementation](#graphdl-operator-implementation)
3. [Traversal Algorithms](#traversal-algorithms)
4. [Materialized View Patterns](#materialized-view-patterns)
5. [Time-Travel Implementation](#time-travel-implementation)
6. [Caching Strategies](#caching-strategies)
7. [Batch Processing Patterns](#batch-processing-patterns)

---

## Core Query Operations

### Edge Lookup Patterns

#### Pattern 1: Forward Edge Lookup

**Use case**: "What does entity A point to?"

```typescript
import { parquetRead } from 'hyparquet';

interface ForwardLookupOptions {
  ns: string;
  fromId: string;
  relType?: string;      // Optional: filter by relationship type
  limit?: number;
  asOf?: bigint;         // Optional: time-travel
}

async function forwardLookup(
  bucket: R2Bucket,
  options: ForwardLookupOptions
): Promise<Edge[]> {
  const { ns, fromId, relType, limit, asOf } = options;

  // 1. Build predicate for Parquet pushdown
  const predicates: Predicate[] = [
    { column: 'ns', op: 'eq', value: ns },
    { column: 'from_id', op: 'eq', value: fromId },
    { column: 'deleted', op: 'eq', value: false }
  ];

  if (relType) {
    predicates.push({ column: 'rel_type', op: 'eq', value: relType });
  }

  if (asOf) {
    predicates.push({ column: 'ts', op: 'lte', value: asOf });
  }

  // 2. List forward index files
  const manifest = await getManifest(bucket, ns, 'forward');
  const files = pruneByZoneMap(manifest.files, { from_id: fromId });

  // 3. Scan with projection (only needed columns)
  const results: Edge[] = [];
  for (const file of files) {
    const data = await bucket.get(file.path);
    const buffer = await data.arrayBuffer();

    await parquetRead({
      file: buffer,
      columns: ['to_id', 'rel_type', 'ts', 'data', 'operator', 'confidence'],
      rowStart: 0,
      rowEnd: limit ? limit - results.length : Infinity,
      onComplete: (rows) => {
        for (const row of rows) {
          if (matchesPredicates(row, predicates)) {
            results.push(rowToEdge(row, ns, fromId));
            if (limit && results.length >= limit) return;
          }
        }
      }
    });

    if (limit && results.length >= limit) break;
  }

  return results;
}
```

#### Pattern 2: Reverse Edge Lookup (Backlinks)

**Use case**: "What points to entity B?"

```typescript
interface ReverseLookupOptions {
  ns: string;
  toId: string;
  relType?: string;
  limit?: number;
}

async function reverseLookup(
  bucket: R2Bucket,
  options: ReverseLookupOptions
): Promise<Edge[]> {
  const { ns, toId, relType, limit } = options;

  // Use reverse index (sorted by to_id)
  const manifest = await getManifest(bucket, ns, 'reverse');
  const files = pruneByZoneMap(manifest.files, { to_id: toId });

  const predicates: Predicate[] = [
    { column: 'ns', op: 'eq', value: ns },
    { column: 'to_id', op: 'eq', value: toId },
    { column: 'deleted', op: 'eq', value: false }
  ];

  if (relType) {
    predicates.push({ column: 'rel_type', op: 'eq', value: relType });
  }

  // ... scan logic similar to forwardLookup
  return scanWithPredicates(bucket, manifest, predicates, {
    projection: ['from_id', 'rel_type', 'ts', 'data', 'operator'],
    limit
  });
}
```

#### Pattern 3: Edge Existence Check

**Use case**: "Does edge (A, rel, B) exist?"

```typescript
async function edgeExists(
  bucket: R2Bucket,
  ns: string,
  fromId: string,
  relType: string,
  toId: string
): Promise<boolean> {
  // 1. Check Bloom filter first (O(1))
  const bloomFilters = await getBloomFilters(bucket, ns, 'forward');
  const key = hashEdgeKey(fromId, relType, toId);

  const candidateRGs: number[] = [];
  for (const { rowGroup, filter } of bloomFilters) {
    if (bloomMightContain(filter, key)) {
      candidateRGs.push(rowGroup);
    }
  }

  // 2. If Bloom filter says no, definitely doesn't exist
  if (candidateRGs.length === 0) {
    return false;
  }

  // 3. Check candidate row groups
  for (const rgIndex of candidateRGs) {
    const found = await scanRowGroupForEdge(bucket, ns, rgIndex, {
      fromId, relType, toId
    });
    if (found) return true;
  }

  return false;
}

// Bloom filter helpers
function hashEdgeKey(fromId: string, relType: string, toId: string): bigint {
  // Use xxHash64 for fast, good distribution
  const input = `${fromId}|${relType}|${toId}`;
  return xxHash64(input);
}

function bloomMightContain(filter: Uint8Array, key: bigint): boolean {
  const numBits = filter.length * 8;
  const numHashes = 7;  // Optimal for 1% FPR

  for (let i = 0; i < numHashes; i++) {
    const hash = (key + BigInt(i) * 0x517cc1b727220a95n) % BigInt(numBits);
    const byteIndex = Number(hash / 8n);
    const bitIndex = Number(hash % 8n);
    if ((filter[byteIndex] & (1 << bitIndex)) === 0) {
      return false;
    }
  }
  return true;
}
```

#### Pattern 4: Relationship Type Scan

**Use case**: "Find all edges of type X in namespace"

```typescript
async function scanByRelType(
  bucket: R2Bucket,
  ns: string,
  relType: string,
  options: { limit?: number; offset?: number; since?: bigint }
): Promise<{ edges: Edge[]; hasMore: boolean }> {
  // Use type index (sorted by rel_type, ts)
  const manifest = await getManifest(bucket, ns, 'type');

  const predicates: Predicate[] = [
    { column: 'ns', op: 'eq', value: ns },
    { column: 'rel_type', op: 'eq', value: relType },
    { column: 'deleted', op: 'eq', value: false }
  ];

  if (options.since) {
    predicates.push({ column: 'ts', op: 'gt', value: options.since });
  }

  return await paginatedScan(bucket, manifest, predicates, {
    projection: ['from_id', 'to_id', 'ts', 'data'],
    limit: options.limit,
    offset: options.offset
  });
}
```

---

## graphdl Operator Implementation

### Operator Semantics

| Operator | Name | Direction | Match | Confidence |
|----------|------|-----------|-------|------------|
| `->` | exact forward | forward | exact | null |
| `~>` | fuzzy forward | forward | fuzzy | 0.0-1.0 |
| `<-` | exact backward | backward | exact | null |
| `<~` | fuzzy backward | backward | fuzzy | 0.0-1.0 |

### Schema to Edge Mapping

```typescript
import { parseGraphDL } from 'graphdl';

// Input graphdl schema
const schema = `
User:
  name: string
  email: email
  organization: ->Organization.members    # exact forward with backref
  interests: [~>Topic.interested]         # fuzzy forward array

Organization:
  name: string

Topic:
  name: string
`;

// Parse and extract relationship definitions
function extractRelationships(schema: string): RelationshipDef[] {
  const parsed = parseGraphDL(schema);
  const relationships: RelationshipDef[] = [];

  for (const [entityName, entity] of Object.entries(parsed.entities)) {
    for (const [fieldName, field] of Object.entries(entity.fields)) {
      if (field.isReference) {
        const { operator, target, backref, isArray } = parseReference(field.raw);

        relationships.push({
          fromType: entityName,
          relType: fieldName,
          toType: target,
          operator,
          backref,
          isArray,
          confidenceRequired: operator.includes('~') ? 0.8 : null
        });
      }
    }
  }

  return relationships;
}

// Reference parser
function parseReference(raw: string): {
  operator: '->' | '~>' | '<-' | '<~';
  target: string;
  backref?: string;
  isArray: boolean;
} {
  const isArray = raw.startsWith('[') && raw.endsWith(']');
  const inner = isArray ? raw.slice(1, -1) : raw;

  // Match: operator + TargetEntity[.backref]
  const match = inner.match(/^(->|~>|<-|<~)(\w+)(?:\.(\w+))?$/);
  if (!match) throw new Error(`Invalid reference: ${raw}`);

  return {
    operator: match[1] as any,
    target: match[2],
    backref: match[3],
    isArray
  };
}
```

### Operator-Aware Query Execution

```typescript
interface ResolveOptions {
  ns: string;
  fromId: string;
  field: RelationshipDef;
  minConfidence?: number;
}

async function resolveRelationship(
  bucket: R2Bucket,
  options: ResolveOptions
): Promise<Node[]> {
  const { ns, fromId, field, minConfidence = 0.8 } = options;

  // 1. Determine index based on operator direction
  const isForward = field.operator === '->' || field.operator === '~>';
  const index = isForward ? 'forward' : 'reverse';
  const lookupId = isForward ? fromId : null;  // Reverse needs different handling

  // 2. Build predicates
  const predicates: Predicate[] = [
    { column: 'ns', op: 'eq', value: ns },
    { column: isForward ? 'from_id' : 'to_id', op: 'eq', value: fromId },
    { column: 'rel_type', op: 'eq', value: field.relType },
    { column: 'operator', op: 'eq', value: field.operator },
    { column: 'deleted', op: 'eq', value: false }
  ];

  // 3. Add confidence filter for fuzzy operators
  if (field.operator === '~>' || field.operator === '<~') {
    predicates.push({
      column: 'confidence',
      op: 'gte',
      value: minConfidence
    });
  }

  // 4. Fetch edges
  const edges = await scanWithPredicates(bucket, await getManifest(bucket, ns, index), predicates, {
    projection: ['from_id', 'to_id', 'confidence', 'data']
  });

  // 5. Resolve target nodes
  const targetIds = edges.map(e => isForward ? e.to_id : e.from_id);

  if (targetIds.length === 0) {
    return field.isArray ? [] : null;
  }

  // 6. Batch fetch nodes
  const nodes = await batchGetNodes(bucket, ns, targetIds, field.toType);

  return field.isArray ? nodes : nodes[0] ?? null;
}
```

### Bidirectional Relationship Resolution

When `bidirectional: true`, queries implicitly include reverse edges:

```typescript
async function resolveBidirectional(
  bucket: R2Bucket,
  ns: string,
  nodeId: string,
  relType: string
): Promise<{ peers: Node[]; direction: 'outgoing' | 'incoming' }[]> {
  // 1. Get explicit outgoing edges
  const outgoing = await forwardLookup(bucket, { ns, fromId: nodeId, relType });

  // 2. Get incoming edges where bidirectional=true
  const incoming = await scanWithPredicates(
    bucket,
    await getManifest(bucket, ns, 'reverse'),
    [
      { column: 'ns', op: 'eq', value: ns },
      { column: 'to_id', op: 'eq', value: nodeId },
      { column: 'rel_type', op: 'eq', value: relType },
      { column: 'bidirectional', op: 'eq', value: true },
      { column: 'deleted', op: 'eq', value: false }
    ],
    { projection: ['from_id', 'ts', 'data'] }
  );

  // 3. Resolve all peer nodes
  const outgoingNodes = await batchGetNodes(bucket, ns, outgoing.map(e => e.to_id));
  const incomingNodes = await batchGetNodes(bucket, ns, incoming.map(e => e.from_id));

  return [
    { peers: outgoingNodes, direction: 'outgoing' },
    { peers: incomingNodes, direction: 'incoming' }
  ];
}
```

---

## Traversal Algorithms

### Breadth-First Search (BFS)

```typescript
interface BFSOptions {
  ns: string;
  startId: string;
  relTypes: string[];    // Relationship types to traverse
  maxDepth: number;
  maxNodes?: number;     // Limit total nodes visited
  direction?: 'forward' | 'reverse' | 'both';
}

interface BFSResult {
  nodes: Map<string, { depth: number; path: string[] }>;
  edges: Edge[];
  truncated: boolean;
}

async function breadthFirstSearch(
  bucket: R2Bucket,
  options: BFSOptions
): Promise<BFSResult> {
  const { ns, startId, relTypes, maxDepth, maxNodes = 10000, direction = 'forward' } = options;

  const visited = new Map<string, { depth: number; path: string[] }>();
  const allEdges: Edge[] = [];
  let frontier = [startId];
  let depth = 0;
  let truncated = false;

  visited.set(startId, { depth: 0, path: [startId] });

  while (frontier.length > 0 && depth < maxDepth) {
    depth++;
    const nextFrontier: string[] = [];

    // Batch process current frontier
    const batches = chunkArray(frontier, 100);  // Process 100 nodes at a time

    for (const batch of batches) {
      // Parallel edge lookups
      const edgePromises = batch.flatMap(nodeId =>
        relTypes.map(async relType => {
          if (direction === 'forward' || direction === 'both') {
            return await forwardLookup(bucket, { ns, fromId: nodeId, relType });
          }
          if (direction === 'reverse' || direction === 'both') {
            return await reverseLookup(bucket, { ns, toId: nodeId, relType });
          }
          return [];
        })
      );

      const edgeResults = await Promise.all(edgePromises);

      for (const edges of edgeResults) {
        for (const edge of edges) {
          allEdges.push(edge);

          const targetId = direction === 'reverse' ? edge.from_id : edge.to_id;

          if (!visited.has(targetId)) {
            if (visited.size >= maxNodes) {
              truncated = true;
              break;
            }

            const parentPath = visited.get(edge.from_id)?.path ?? [startId];
            visited.set(targetId, {
              depth,
              path: [...parentPath, targetId]
            });
            nextFrontier.push(targetId);
          }
        }
        if (truncated) break;
      }
      if (truncated) break;
    }

    frontier = nextFrontier;
  }

  return { nodes: visited, edges: allEdges, truncated };
}
```

### Depth-First Search (DFS) with Path Recording

```typescript
interface DFSOptions {
  ns: string;
  startId: string;
  endId?: string;        // Optional: stop when found
  relTypes: string[];
  maxDepth: number;
  findAll?: boolean;     // Find all paths vs first path
}

interface PathResult {
  path: string[];        // Node IDs
  edges: Edge[];         // Edges in path
  depth: number;
}

async function depthFirstSearch(
  bucket: R2Bucket,
  options: DFSOptions
): Promise<PathResult[]> {
  const { ns, startId, endId, relTypes, maxDepth, findAll = false } = options;

  const paths: PathResult[] = [];
  const visited = new Set<string>();

  async function dfs(
    currentId: string,
    currentPath: string[],
    currentEdges: Edge[],
    depth: number
  ): Promise<boolean> {
    // Check if we found target
    if (endId && currentId === endId) {
      paths.push({
        path: currentPath,
        edges: currentEdges,
        depth
      });
      return !findAll;  // Return true to stop if not finding all
    }

    // Depth limit
    if (depth >= maxDepth) {
      return false;
    }

    // Cycle detection
    if (visited.has(currentId)) {
      return false;
    }
    visited.add(currentId);

    // Explore neighbors
    for (const relType of relTypes) {
      const edges = await forwardLookup(bucket, {
        ns,
        fromId: currentId,
        relType
      });

      for (const edge of edges) {
        const found = await dfs(
          edge.to_id,
          [...currentPath, edge.to_id],
          [...currentEdges, edge],
          depth + 1
        );

        if (found && !findAll) {
          visited.delete(currentId);
          return true;
        }
      }
    }

    visited.delete(currentId);  // Backtrack for other paths
    return false;
  }

  await dfs(startId, [startId], [], 0);

  return paths;
}
```

### Shortest Path (Dijkstra with Edge Weights)

```typescript
interface ShortestPathOptions {
  ns: string;
  startId: string;
  endId: string;
  relTypes: string[];
  weightField?: string;  // Edge data field to use as weight
  maxCost?: number;
}

interface ShortestPathResult {
  path: string[];
  edges: Edge[];
  totalCost: number;
  found: boolean;
}

async function dijkstraShortestPath(
  bucket: R2Bucket,
  options: ShortestPathOptions
): Promise<ShortestPathResult> {
  const { ns, startId, endId, relTypes, weightField = 'weight', maxCost = Infinity } = options;

  // Priority queue: [cost, nodeId, path, edges]
  const pq = new MinHeap<[number, string, string[], Edge[]]>(
    (a, b) => a[0] - b[0]
  );
  const visited = new Map<string, number>();  // nodeId -> best cost

  pq.push([0, startId, [startId], []]);

  while (!pq.isEmpty()) {
    const [cost, nodeId, path, edges] = pq.pop()!;

    // Found target
    if (nodeId === endId) {
      return { path, edges, totalCost: cost, found: true };
    }

    // Skip if we've seen this node with lower cost
    if (visited.has(nodeId) && visited.get(nodeId)! <= cost) {
      continue;
    }
    visited.set(nodeId, cost);

    // Cost limit
    if (cost > maxCost) {
      continue;
    }

    // Explore neighbors
    for (const relType of relTypes) {
      const outEdges = await forwardLookup(bucket, {
        ns,
        fromId: nodeId,
        relType
      });

      for (const edge of outEdges) {
        const edgeCost = getEdgeWeight(edge, weightField);
        const newCost = cost + edgeCost;

        if (!visited.has(edge.to_id) || visited.get(edge.to_id)! > newCost) {
          pq.push([
            newCost,
            edge.to_id,
            [...path, edge.to_id],
            [...edges, edge]
          ]);
        }
      }
    }
  }

  return { path: [], edges: [], totalCost: Infinity, found: false };
}

function getEdgeWeight(edge: Edge, weightField: string): number {
  if (edge.data?.type === 'object' && edge.data.fields[weightField]) {
    const weightVariant = edge.data.fields[weightField];
    if (weightVariant.type === 'float64' || weightVariant.type === 'int64') {
      return Number(weightVariant.value);
    }
  }
  return 1;  // Default weight
}
```

---

## Materialized View Patterns

### Organizational Hierarchy (Transitive Closure)

Pre-compute all ancestor/descendant relationships:

```typescript
interface HierarchyMaterialization {
  pathType: 'org_hierarchy';
  relType: 'reports_to';
  aggregation: 'all_ancestors';
}

async function materializeHierarchy(
  bucket: R2Bucket,
  ns: string
): Promise<void> {
  // 1. Find all root nodes (no incoming 'reports_to' edges)
  const allNodes = await scanNodes(bucket, ns, { type: 'Person' });
  const hasManager = new Set<string>();

  const edges = await scanByRelType(bucket, ns, 'reports_to', {});
  for (const edge of edges.edges) {
    hasManager.add(edge.from_id);  // This person has a manager
  }

  const roots = allNodes.filter(n => !hasManager.has(n.id));

  // 2. BFS from each root, recording paths
  const paths: MaterializedPath[] = [];

  for (const root of roots) {
    const bfs = await breadthFirstSearch(bucket, {
      ns,
      startId: root.id,
      relTypes: ['reports_to'],
      maxDepth: 20,
      direction: 'reverse'  // Follow "reports_to" backwards = subordinates
    });

    // Create path entries for each subordinate
    for (const [nodeId, info] of bfs.nodes) {
      if (nodeId === root.id) continue;

      paths.push({
        ns,
        path_type: 'org_hierarchy',
        start_id: nodeId,
        end_id: root.id,
        hops: info.depth,
        path: info.path,
        total_weight: info.depth,  // depth as "distance from CEO"
        computed_at: BigInt(Date.now()) * 1000n,
        valid_until: BigInt(Date.now() + 3600000) * 1000n  // 1 hour
      });
    }
  }

  // 3. Write materialized paths
  await writePaths(bucket, ns, 'org_hierarchy', paths);
}

// Query using materialized view
async function getReportingChain(
  bucket: R2Bucket,
  ns: string,
  personId: string
): Promise<string[]> {
  const paths = await scanPaths(bucket, ns, 'org_hierarchy', {
    start_id: personId
  });

  if (paths.length === 0) {
    // Fall back to live traversal
    const live = await depthFirstSearch(bucket, {
      ns,
      startId: personId,
      relTypes: ['reports_to'],
      maxDepth: 20
    });
    return live[0]?.path ?? [personId];
  }

  // Return shortest path to root
  return paths
    .sort((a, b) => a.hops - b.hops)[0]
    .path;
}
```

### Social Graph (2-Hop Connections)

Pre-compute "friends of friends":

```typescript
async function materializeFOF(
  bucket: R2Bucket,
  ns: string
): Promise<void> {
  const users = await scanNodes(bucket, ns, { type: 'User' });

  const paths: MaterializedPath[] = [];

  for (const user of users) {
    // Get direct friends
    const friends = await forwardLookup(bucket, {
      ns,
      fromId: user.id,
      relType: 'follows'
    });

    const friendIds = friends.map(e => e.to_id);

    // Get friends of friends
    for (const friendId of friendIds) {
      const fof = await forwardLookup(bucket, {
        ns,
        fromId: friendId,
        relType: 'follows'
      });

      for (const edge of fof) {
        // Exclude direct friends and self
        if (edge.to_id === user.id || friendIds.includes(edge.to_id)) {
          continue;
        }

        paths.push({
          ns,
          path_type: 'social_fof',
          start_id: user.id,
          end_id: edge.to_id,
          hops: 2,
          path: [user.id, friendId, edge.to_id],
          total_weight: 2,
          computed_at: BigInt(Date.now()) * 1000n,
          valid_until: BigInt(Date.now() + 86400000) * 1000n  // 24 hours
        });
      }
    }
  }

  await writePaths(bucket, ns, 'social_fof', paths);
}
```

---

## Time-Travel Implementation

### Snapshot + Event Replay

```typescript
interface TimeTravelOptions {
  ns: string;
  asOf: bigint;  // Microseconds since epoch
}

async function getGraphAsOf(
  bucket: R2Bucket,
  options: TimeTravelOptions
): Promise<{ nodes: Map<string, Node>; edges: Map<string, Edge> }> {
  const { ns, asOf } = options;

  // 1. Find nearest checkpoint before target time
  const checkpoints = await listCheckpoints(bucket, ns);
  const checkpoint = findNearestCheckpoint(checkpoints, asOf);

  // 2. Load checkpoint state
  let nodeState: Map<string, Node>;
  let edgeState: Map<string, Edge>;

  if (checkpoint) {
    nodeState = await loadCheckpointNodes(bucket, checkpoint.path);
    edgeState = await loadCheckpointEdges(bucket, checkpoint.path);
  } else {
    nodeState = new Map();
    edgeState = new Map();
  }

  // 3. Replay events from checkpoint to target time
  const startTs = checkpoint?.timestamp ?? 0n;
  const events = await getEventRange(bucket, ns, startTs, asOf);

  for (const event of events) {
    if (event.target === 'node') {
      applyNodeEvent(nodeState, event);
    } else {
      applyEdgeEvent(edgeState, event);
    }
  }

  return { nodes: nodeState, edges: edgeState };
}

function applyNodeEvent(state: Map<string, Node>, event: CDCEvent): void {
  switch (event.op) {
    case 'INSERT':
      state.set(event.entity_id, variantToNode(event.after!));
      break;
    case 'UPDATE':
      state.set(event.entity_id, variantToNode(event.after!));
      break;
    case 'DELETE':
      state.delete(event.entity_id);
      break;
  }
}

function applyEdgeEvent(state: Map<string, Edge>, event: CDCEvent): void {
  // Edge entity_id format: from_id|rel_type|to_id
  switch (event.op) {
    case 'INSERT':
      state.set(event.entity_id, variantToEdge(event.after!));
      break;
    case 'UPDATE':
      state.set(event.entity_id, variantToEdge(event.after!));
      break;
    case 'DELETE':
      state.delete(event.entity_id);
      break;
  }
}
```

### Incremental Time-Travel Query

For queries that don't need full graph state:

```typescript
async function traverseAsOf(
  bucket: R2Bucket,
  ns: string,
  fromId: string,
  relType: string,
  asOf: bigint
): Promise<Edge[]> {
  // 1. Query current edges with ts filter
  const currentEdges = await forwardLookup(bucket, {
    ns,
    fromId,
    relType,
    asOf  // Only edges created before asOf
  });

  // 2. Check for DELETE events after edge creation but before asOf
  const edgesToCheck = currentEdges.map(e => ({
    edge: e,
    entityId: `${e.from_id}|${e.rel_type}|${e.to_id}`
  }));

  const deletedEdges = new Set<string>();

  for (const { edge, entityId } of edgesToCheck) {
    const deleteEvents = await getEventRange(bucket, ns, edge.ts, asOf, {
      entity_id: entityId,
      op: 'DELETE',
      target: 'edge'
    });

    if (deleteEvents.length > 0) {
      deletedEdges.add(entityId);
    }
  }

  // 3. Filter out deleted edges
  return currentEdges.filter(e => {
    const entityId = `${e.from_id}|${e.rel_type}|${e.to_id}`;
    return !deletedEdges.has(entityId);
  });
}
```

---

## Caching Strategies

### Edge Cache (Hot Node Optimization)

```typescript
interface EdgeCache {
  // LRU cache for frequently accessed node edges
  cache: Map<string, { edges: Edge[]; fetchedAt: number }>;
  maxSize: number;
  ttlMs: number;
}

const edgeCache: EdgeCache = {
  cache: new Map(),
  maxSize: 1000,  // Cache edges for 1000 nodes
  ttlMs: 60000    // 1 minute TTL
};

async function cachedForwardLookup(
  bucket: R2Bucket,
  options: ForwardLookupOptions
): Promise<Edge[]> {
  const cacheKey = `${options.ns}:${options.fromId}:${options.relType ?? '*'}`;

  // Check cache
  const cached = edgeCache.cache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < edgeCache.ttlMs) {
    // Apply additional filters to cached data
    let edges = cached.edges;
    if (options.asOf) {
      edges = edges.filter(e => e.ts <= options.asOf!);
    }
    if (options.limit) {
      edges = edges.slice(0, options.limit);
    }
    return edges;
  }

  // Fetch and cache
  const edges = await forwardLookup(bucket, {
    ...options,
    limit: undefined,  // Fetch all for cache
    asOf: undefined    // Current state for cache
  });

  // LRU eviction
  if (edgeCache.cache.size >= edgeCache.maxSize) {
    const oldest = Array.from(edgeCache.cache.entries())
      .sort((a, b) => a[1].fetchedAt - b[1].fetchedAt)[0];
    edgeCache.cache.delete(oldest[0]);
  }

  edgeCache.cache.set(cacheKey, { edges, fetchedAt: Date.now() });

  // Apply filters to result
  let result = edges;
  if (options.asOf) {
    result = result.filter(e => e.ts <= options.asOf!);
  }
  if (options.limit) {
    result = result.slice(0, options.limit);
  }

  return result;
}
```

### Manifest Cache (File Metadata)

```typescript
// Cache Parquet file metadata to avoid repeated fetches
interface ManifestCache {
  manifests: Map<string, { manifest: EdgeIndexManifest; fetchedAt: number }>;
  ttlMs: number;
}

const manifestCache: ManifestCache = {
  manifests: new Map(),
  ttlMs: 300000  // 5 minutes
};

async function getCachedManifest(
  bucket: R2Bucket,
  ns: string,
  index: 'forward' | 'reverse' | 'type'
): Promise<EdgeIndexManifest> {
  const cacheKey = `${ns}:${index}`;

  const cached = manifestCache.manifests.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < manifestCache.ttlMs) {
    return cached.manifest;
  }

  const manifest = await fetchManifest(bucket, ns, index);
  manifestCache.manifests.set(cacheKey, {
    manifest,
    fetchedAt: Date.now()
  });

  return manifest;
}
```

---

## Batch Processing Patterns

### Bulk Edge Insert

```typescript
interface BulkInsertOptions {
  ns: string;
  edges: Edge[];
  writeEvents?: boolean;
}

async function bulkInsertEdges(
  bucket: R2Bucket,
  options: BulkInsertOptions
): Promise<{ inserted: number; errors: Error[] }> {
  const { ns, edges, writeEvents = true } = options;
  const errors: Error[] = [];

  // 1. Validate edges
  const validEdges: Edge[] = [];
  for (const edge of edges) {
    try {
      validateEdge(edge);
      validEdges.push(edge);
    } catch (e) {
      errors.push(e as Error);
    }
  }

  // 2. Group by partition key (if using partitioning)
  const partitions = groupByPartition(validEdges);

  // 3. Write to each index in parallel
  const writePromises: Promise<void>[] = [];

  for (const [partition, partitionEdges] of partitions) {
    // Forward index
    writePromises.push(
      appendToIndex(bucket, ns, 'forward', partition, partitionEdges, {
        sortBy: ['from_id', 'rel_type', 'to_id', 'ts']
      })
    );

    // Reverse index
    writePromises.push(
      appendToIndex(bucket, ns, 'reverse', partition, partitionEdges, {
        sortBy: ['to_id', 'rel_type', 'from_id', 'ts']
      })
    );

    // Type index
    writePromises.push(
      appendToIndex(bucket, ns, 'type', partition, partitionEdges, {
        sortBy: ['rel_type', 'ts', 'from_id', 'to_id']
      })
    );
  }

  await Promise.all(writePromises);

  // 4. Write CDC events
  if (writeEvents) {
    const events: CDCEvent[] = validEdges.map(edge => ({
      event_id: generateULID(),
      target: 'edge',
      ns,
      entity_id: `${edge.from_id}|${edge.rel_type}|${edge.to_id}`,
      ts: edge.ts,
      op: 'INSERT',
      before: null,
      after: edgeToVariant(edge),
      tx_id: generateTransactionId(),
      user_id: 'system',
      metadata: { type: 'object', fields: {} }
    }));

    await appendEvents(bucket, ns, events);
  }

  return { inserted: validEdges.length, errors };
}
```

### Batch Traversal (Parallel Fan-out)

```typescript
async function batchTraverse(
  bucket: R2Bucket,
  ns: string,
  nodeIds: string[],
  relType: string
): Promise<Map<string, Edge[]>> {
  const results = new Map<string, Edge[]>();

  // Process in batches to manage concurrency
  const BATCH_SIZE = 50;
  const batches = chunkArray(nodeIds, BATCH_SIZE);

  for (const batch of batches) {
    const promises = batch.map(async nodeId => {
      const edges = await forwardLookup(bucket, { ns, fromId: nodeId, relType });
      return { nodeId, edges };
    });

    const batchResults = await Promise.all(promises);

    for (const { nodeId, edges } of batchResults) {
      results.set(nodeId, edges);
    }
  }

  return results;
}
```

---

## Summary

This document provides comprehensive patterns for:

1. **Core edge operations**: Forward, reverse, existence checks with Bloom filters
2. **graphdl integration**: Operator-aware query execution with confidence thresholds
3. **Graph algorithms**: BFS, DFS, Dijkstra shortest path
4. **Materialized views**: Hierarchy traversals, social connections
5. **Time-travel**: Snapshot + event replay for historical queries
6. **Caching**: Edge cache and manifest cache for hot paths
7. **Batch processing**: Bulk inserts and parallel traversals

These patterns leverage Parquet's columnar strengths while working around its immutability constraints through careful index design and caching strategies.

---

*Query Patterns Guide - ParqueDB Graph-First Architecture*
