# ParqueDB: Namespace-Sharded Multi-File Architecture

**Architecture Perspective**: Multi-tenant isolation through namespace-based file sharding

This document presents a namespace-sharded architecture for ParqueDB, optimizing for multi-tenant isolation while enabling efficient cross-namespace operations on Cloudflare R2.

---

## Executive Summary

Traditional single-file or table-per-entity designs face challenges at scale:
- File size limits (R2 object size, memory constraints)
- Multi-tenant isolation requirements
- Cross-namespace query complexity
- Schema evolution across tenants

This architecture shards data by namespace:

```
{ns}/data.parquet      - Entity data for namespace
{ns}/events.parquet    - Events for namespace
_system/schema.parquet - Schema definitions (graphdl)
_system/refs.parquet   - Cross-namespace references
```

**Key Benefits**:
- **Tenant Isolation**: Each namespace is a self-contained unit
- **Independent Scaling**: Namespaces can be sharded further as they grow
- **Schema Flexibility**: Per-namespace schemas with global type registry
- **Efficient Queries**: Namespace-scoped queries read only relevant files
- **Time-Travel**: Per-namespace and global consistency points

---

## File Organization

### Complete Storage Layout

```
/warehouse/
├── _system/                           # Global system namespace
│   ├── schema.parquet                 # Global schema registry (graphdl types)
│   ├── refs.parquet                   # Cross-namespace reference index
│   ├── namespaces.parquet             # Namespace registry and metadata
│   ├── events.parquet                 # System-level events (schema changes)
│   └── config.parquet                 # Global configuration
│
├── {tenant-a}/                        # Tenant namespace
│   ├── data.parquet                   # Entity data (nodes)
│   ├── edges.parquet                  # Relationships (optional, if graph-first)
│   ├── events.parquet                 # CDC events for this namespace
│   ├── _schema.parquet                # Namespace-local schema overrides
│   ├── _meta.parquet                  # Namespace metadata (stats, checkpoints)
│   └── _shards/                       # Sub-shards when namespace grows
│       ├── type=Person/
│       │   └── data.parquet
│       └── type=Order/
│           └── data.parquet
│
├── {tenant-b}/
│   └── ...
│
└── _refs/                             # Materialized cross-namespace references
    ├── {source-ns}_{target-ns}.parquet
    └── _index.parquet                 # Reference lookup index
```

### Naming Conventions

```typescript
/**
 * Namespace naming rules:
 * - Valid characters: [a-z0-9-_.]
 * - No leading underscore (reserved for system)
 * - Max length: 128 characters
 * - Dots allowed for hierarchical namespaces (org.team.project)
 */
type Namespace = string & { __brand: 'Namespace' }

/**
 * Reserved system namespaces (prefixed with underscore)
 */
const SYSTEM_NAMESPACES = {
  SYSTEM: '_system',      // Global schema and config
  REFS: '_refs',          // Cross-namespace references
  ARCHIVE: '_archive',    // Archived/deleted namespaces
  TEMP: '_temp',          // Temporary query results
} as const

/**
 * File naming within namespace
 */
const NAMESPACE_FILES = {
  DATA: 'data.parquet',           // Primary entity data
  EDGES: 'edges.parquet',         // Relationships (graph mode)
  EVENTS: 'events.parquet',       // CDC event log
  SCHEMA: '_schema.parquet',      // Local schema overrides
  META: '_meta.parquet',          // Namespace metadata
  SHARDS_DIR: '_shards',          // Sub-shard directory
} as const
```

---

## Schema Definitions

### 1. Namespace Registry (`_system/namespaces.parquet`)

```typescript
/**
 * Namespace registry - tracks all namespaces and their configuration
 */
interface NamespaceRecord {
  // Identity
  ns: string                    // Namespace identifier
  display_name: string          // Human-readable name

  // Ownership
  owner_id: string              // Owner account/user
  created_at: bigint            // Creation timestamp
  created_by: string            // Creator user ID

  // Configuration
  schema_mode: 'global' | 'local' | 'hybrid'  // Schema inheritance mode
  isolation_level: 'strict' | 'shared'        // Multi-tenant isolation

  // Quotas
  max_entities: bigint | null   // Entity count limit (null = unlimited)
  max_storage_bytes: bigint | null  // Storage limit

  // Status
  status: 'active' | 'suspended' | 'archived' | 'deleted'

  // Sharding
  shard_strategy: 'none' | 'type' | 'time' | 'hash'
  shard_count: number           // Number of active shards

  // Time-travel
  retention_days: number        // Event retention period
  checkpoint_interval_hours: number

  // Metadata
  metadata: Variant             // Custom namespace metadata
}

const NAMESPACE_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  display_name: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  owner_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  schema_mode: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  isolation_level: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  max_entities: { type: 'INT64', encoding: 'PLAIN', optional: true },
  max_storage_bytes: { type: 'INT64', encoding: 'PLAIN', optional: true },
  status: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  shard_strategy: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  shard_count: { type: 'INT32', encoding: 'PLAIN' },
  retention_days: { type: 'INT32', encoding: 'PLAIN' },
  checkpoint_interval_hours: { type: 'INT32', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const
```

### 2. Global Schema Registry (`_system/schema.parquet`)

```typescript
/**
 * Global schema registry - graphdl type definitions
 *
 * Stores schema definitions that can be inherited by namespaces.
 * Namespaces can use global types, extend them, or define local-only types.
 */
interface SchemaDefinition {
  // Identity
  schema_id: string             // Unique schema identifier (ULID)
  type_name: string             // Type name (e.g., "Person", "Order")
  namespace: string | null      // null = global, string = namespace-scoped

  // Schema.org/graphdl type
  schema_type: string           // e.g., "https://schema.org/Person"

  // Version
  version: number               // Schema version for evolution
  created_at: bigint
  deprecated_at: bigint | null  // When deprecated (null if active)

  // Definition (graphdl format stored as Variant)
  definition: Variant           // Full graphdl type definition

  // Field definitions (denormalized for query efficiency)
  fields: Variant               // Array of field definitions

  // Relationships (extracted from graphdl operators)
  relationships: Variant        // Array of relationship definitions

  // Validation
  validators: Variant           // JSON Schema or custom validators

  // Metadata
  description: string
  metadata: Variant
}

/**
 * Field definition within a schema
 */
interface FieldDefinition {
  name: string
  type: string                  // Primitive or reference type
  required: boolean
  array: boolean

  // For relationships
  is_relation: boolean
  relation_operator: '->' | '~>' | '<-' | '<~' | null
  relation_target: string | null  // Target type name
  relation_backref: string | null // Backref field name

  // Validation
  constraints: Variant          // min, max, pattern, etc.
}

const SCHEMA_PARQUET_SCHEMA = {
  schema_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  type_name: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  namespace: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  schema_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  version: { type: 'INT32', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  deprecated_at: { type: 'INT64', encoding: 'PLAIN', optional: true },
  definition: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  fields: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  relationships: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  validators: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  description: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const
```

### 3. Cross-Namespace References (`_system/refs.parquet`)

```typescript
/**
 * Cross-namespace reference index
 *
 * Tracks all references between entities in different namespaces.
 * Enables efficient cross-namespace joins and integrity checks.
 */
interface CrossNamespaceRef {
  // Reference identity
  ref_id: string                // Unique reference ID (ULID)

  // Source
  source_ns: string             // Source namespace
  source_id: string             // Source entity ID
  source_type: string           // Source entity type

  // Target
  target_ns: string             // Target namespace
  target_id: string             // Target entity ID
  target_type: string           // Target entity type

  // Relationship
  rel_type: string              // Relationship type
  operator: '->' | '~>' | '<-' | '<~'
  bidirectional: boolean

  // Metadata
  created_at: bigint
  created_by: string

  // Reference properties
  data: Variant                 // Edge properties
  confidence: number | null     // For fuzzy references

  // Status
  deleted: boolean
  verified: boolean             // Has target been verified to exist?
  last_verified_at: bigint | null
}

const REFS_PARQUET_SCHEMA = {
  ref_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  source_ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  source_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  source_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  target_ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  target_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  target_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  rel_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  operator: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  confidence: { type: 'FLOAT', encoding: 'PLAIN', optional: true },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
  verified: { type: 'BOOLEAN', encoding: 'PLAIN' },
  last_verified_at: { type: 'INT64', encoding: 'PLAIN', optional: true },
} as const
```

### 4. Namespace Data (`{ns}/data.parquet`)

```typescript
/**
 * Entity data within a namespace
 *
 * Each namespace has its own data.parquet with all entities.
 * Sort order: (type, id, ts DESC) for efficient type-scoped queries.
 */
interface NamespaceEntity {
  // Identity (no ns field - implicit from file path)
  id: string                    // Entity identifier
  type: string                  // Entity type

  // Version
  ts: bigint                    // Timestamp (microseconds)
  version: number               // Optimistic concurrency version

  // Data
  data: Variant                 // Entity payload

  // Metadata
  created_at: bigint
  created_by: string
  updated_at: bigint
  updated_by: string

  // Status
  deleted: boolean

  // Cross-namespace
  external_refs: Variant        // Array of external reference IDs
}

const DATA_PARQUET_SCHEMA = {
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  version: { type: 'INT32', encoding: 'PLAIN' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  updated_at: { type: 'INT64', encoding: 'PLAIN' },
  updated_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
  external_refs: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
} as const
```

### 5. Namespace Events (`{ns}/events.parquet`)

```typescript
/**
 * CDC event log for a namespace
 *
 * Captures all changes within the namespace for time-travel.
 * Sort order: (ts, event_id) for chronological replay.
 */
interface NamespaceEvent {
  // Event identity
  event_id: string              // ULID

  // Target
  entity_id: string             // Entity ID
  entity_type: string           // Entity type

  // Event
  ts: bigint                    // Event timestamp
  op: 'INSERT' | 'UPDATE' | 'DELETE'

  // State
  before: Variant | null        // Previous state
  after: Variant | null         // New state

  // Change details
  changed_fields: string[]      // Fields that changed (for UPDATE)

  // Context
  tx_id: string                 // Transaction ID
  user_id: string
  trace_id: string | null       // Distributed trace ID

  // Cross-namespace
  triggers_external: boolean    // Does this event affect external refs?

  // Metadata
  metadata: Variant
}

const EVENTS_PARQUET_SCHEMA = {
  event_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  entity_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  entity_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  op: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  before: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  after: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  changed_fields: { type: 'LIST', element: { type: 'BYTE_ARRAY' } },
  tx_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  user_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  trace_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  triggers_external: { type: 'BOOLEAN', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const
```

### 6. Namespace Metadata (`{ns}/_meta.parquet`)

```typescript
/**
 * Namespace metadata and statistics
 *
 * Stores aggregate statistics, checkpoints, and operational metadata.
 */
interface NamespaceMetadata {
  // Stats
  stat_type: 'count' | 'checkpoint' | 'config' | 'index'

  // For count stats
  entity_type: string | null
  entity_count: bigint | null

  // For checkpoints
  checkpoint_ts: bigint | null
  checkpoint_event_id: string | null

  // For index stats
  index_name: string | null
  index_size_bytes: bigint | null

  // Common
  updated_at: bigint
  data: Variant                 // Additional type-specific data
}
```

---

## Schema Modes

### Global Schema Mode

All types are defined in `_system/schema.parquet`. Namespaces use these types directly.

```typescript
// Namespace configuration
{ schema_mode: 'global' }

// Query resolution:
// 1. Look up type in _system/schema.parquet
// 2. Apply type definition to {ns}/data.parquet
```

### Local Schema Mode

Each namespace defines its own types in `{ns}/_schema.parquet`. No inheritance.

```typescript
// Namespace configuration
{ schema_mode: 'local' }

// Query resolution:
// 1. Look up type in {ns}/_schema.parquet
// 2. Fall back to error if not found
```

### Hybrid Schema Mode (Recommended)

Namespaces can use global types, extend them, or define local-only types.

```typescript
// Namespace configuration
{ schema_mode: 'hybrid' }

// Query resolution:
// 1. Check {ns}/_schema.parquet for local override
// 2. If extends global: merge with _system/schema.parquet
// 3. If local-only: use local definition
// 4. Fall back to _system/schema.parquet
```

**Local Schema Override Example**:

```typescript
// Global schema (_system/schema.parquet)
{
  type_name: 'Person',
  fields: [
    { name: 'name', type: 'string', required: true },
    { name: 'email', type: 'string', required: true }
  ]
}

// Local override ({ns}/_schema.parquet)
{
  type_name: 'Person',
  extends: 'global:Person',  // Inherit from global
  fields: [
    // Add namespace-specific fields
    { name: 'employee_id', type: 'string', required: true },
    { name: 'department', type: 'string', required: false }
  ]
}

// Effective schema (merged)
{
  type_name: 'Person',
  fields: [
    { name: 'name', type: 'string', required: true },
    { name: 'email', type: 'string', required: true },
    { name: 'employee_id', type: 'string', required: true },
    { name: 'department', type: 'string', required: false }
  ]
}
```

---

## Cross-Namespace Query Patterns

### Pattern 1: Simple Cross-Namespace Join

**Query**: Find all orders in `tenant-a` that reference products in `tenant-b`

```typescript
async function crossNamespaceJoin(
  sourceNs: string,
  targetNs: string,
  relType: string
): Promise<JoinResult[]> {
  // Step 1: Read cross-namespace references
  const refs = await parquet.scan('_system/refs.parquet', {
    filter: {
      source_ns: { eq: sourceNs },
      target_ns: { eq: targetNs },
      rel_type: { eq: relType },
      deleted: { eq: false }
    }
  });

  // Step 2: Batch lookup source entities
  const sourceIds = refs.map(r => r.source_id);
  const sources = await parquet.scan(`${sourceNs}/data.parquet`, {
    filter: { id: { in: sourceIds } }
  });

  // Step 3: Batch lookup target entities
  const targetIds = refs.map(r => r.target_id);
  const targets = await parquet.scan(`${targetNs}/data.parquet`, {
    filter: { id: { in: targetIds } }
  });

  // Step 4: Join results
  return joinResults(refs, sources, targets);
}
```

### Pattern 2: Federated Query Across Multiple Namespaces

**Query**: Find all users across all tenant namespaces with email domain "@example.com"

```typescript
async function federatedQuery(
  filter: QueryFilter,
  options: { namespaces?: string[], excludeSystem?: boolean }
): Promise<FederatedResult[]> {
  // Step 1: Get namespace list
  let namespaces: string[];
  if (options.namespaces) {
    namespaces = options.namespaces;
  } else {
    const nsRecords = await parquet.scan('_system/namespaces.parquet', {
      filter: { status: { eq: 'active' } }
    });
    namespaces = nsRecords.map(r => r.ns);
  }

  // Step 2: Parallel query across namespaces
  const results = await Promise.all(
    namespaces.map(async (ns) => {
      try {
        const entities = await parquet.scan(`${ns}/data.parquet`, {
          filter,
          limit: 1000  // Per-namespace limit
        });
        return entities.map(e => ({ ns, ...e }));
      } catch (err) {
        // Handle namespace-level errors gracefully
        console.warn(`Query failed for namespace ${ns}:`, err);
        return [];
      }
    })
  );

  return results.flat();
}
```

### Pattern 3: Cross-Namespace Graph Traversal

**Query**: Traverse relationships that cross namespace boundaries

```typescript
async function crossNamespaceTraversal(
  startNs: string,
  startId: string,
  pathSpec: Array<{ relType: string, direction: 'forward' | 'reverse' }>,
  maxDepth: number = 3
): Promise<TraversalPath[]> {
  const paths: TraversalPath[] = [];

  async function traverse(
    currentNs: string,
    currentId: string,
    depth: number,
    path: TraversalNode[]
  ): Promise<void> {
    if (depth >= pathSpec.length || depth >= maxDepth) {
      paths.push({ nodes: path, depth });
      return;
    }

    const step = pathSpec[depth];

    // Check for cross-namespace refs first
    const crossRefs = await parquet.scan('_system/refs.parquet', {
      filter: {
        [step.direction === 'forward' ? 'source_ns' : 'target_ns']: { eq: currentNs },
        [step.direction === 'forward' ? 'source_id' : 'target_id']: { eq: currentId },
        rel_type: { eq: step.relType },
        deleted: { eq: false }
      }
    });

    // Also check local refs (within namespace)
    const localRefs = await parquet.scan(`${currentNs}/edges.parquet`, {
      filter: {
        [step.direction === 'forward' ? 'from_id' : 'to_id']: { eq: currentId },
        rel_type: { eq: step.relType },
        deleted: { eq: false }
      }
    });

    // Traverse cross-namespace refs
    for (const ref of crossRefs) {
      const nextNs = step.direction === 'forward' ? ref.target_ns : ref.source_ns;
      const nextId = step.direction === 'forward' ? ref.target_id : ref.source_id;

      await traverse(nextNs, nextId, depth + 1, [
        ...path,
        { ns: nextNs, id: nextId, crossNamespace: true }
      ]);
    }

    // Traverse local refs
    for (const ref of localRefs) {
      const nextId = step.direction === 'forward' ? ref.to_id : ref.from_id;

      await traverse(currentNs, nextId, depth + 1, [
        ...path,
        { ns: currentNs, id: nextId, crossNamespace: false }
      ]);
    }
  }

  await traverse(startNs, startId, 0, [{ ns: startNs, id: startId, crossNamespace: false }]);
  return paths;
}
```

### Pattern 4: Reference Verification

**Query**: Verify all cross-namespace references are still valid

```typescript
async function verifyReferences(
  batchSize: number = 1000
): Promise<VerificationResult> {
  const invalid: CrossNamespaceRef[] = [];
  let verified = 0;
  let offset = 0;

  while (true) {
    // Read batch of unverified refs
    const refs = await parquet.scan('_system/refs.parquet', {
      filter: {
        deleted: { eq: false },
        verified: { eq: false }
      },
      limit: batchSize,
      offset
    });

    if (refs.length === 0) break;

    // Group by target namespace for batch lookup
    const byTargetNs = groupBy(refs, 'target_ns');

    for (const [targetNs, nsRefs] of Object.entries(byTargetNs)) {
      const targetIds = nsRefs.map(r => r.target_id);

      // Check which targets exist
      const existing = await parquet.scan(`${targetNs}/data.parquet`, {
        filter: {
          id: { in: targetIds },
          deleted: { eq: false }
        },
        projection: ['id']
      });

      const existingIds = new Set(existing.map(e => e.id));

      for (const ref of nsRefs) {
        if (existingIds.has(ref.target_id)) {
          await markRefVerified(ref.ref_id);
          verified++;
        } else {
          invalid.push(ref);
        }
      }
    }

    offset += batchSize;
  }

  return { verified, invalid, invalidCount: invalid.length };
}
```

---

## Sharding Strategy

### When to Shard a Namespace

```typescript
interface ShardingThresholds {
  // Trigger sharding when any threshold exceeded
  maxFileSize: number         // e.g., 1GB
  maxEntityCount: number      // e.g., 10 million
  maxRowGroupCount: number    // e.g., 1000 row groups
}

const DEFAULT_THRESHOLDS: ShardingThresholds = {
  maxFileSize: 1024 * 1024 * 1024,  // 1GB
  maxEntityCount: 10_000_000,
  maxRowGroupCount: 1000,
};

async function shouldShard(ns: string): Promise<boolean> {
  const meta = await readNamespaceMeta(ns);
  const fileStats = await getFileStats(`${ns}/data.parquet`);

  return (
    fileStats.size > DEFAULT_THRESHOLDS.maxFileSize ||
    meta.entityCount > DEFAULT_THRESHOLDS.maxEntityCount ||
    fileStats.rowGroupCount > DEFAULT_THRESHOLDS.maxRowGroupCount
  );
}
```

### Sharding Strategies

#### 1. Type-Based Sharding

Split data files by entity type. Best for namespaces with distinct entity types.

```
{ns}/
├── data.parquet              # Legacy/small types
└── _shards/
    ├── type=Person/
    │   └── data.parquet
    ├── type=Order/
    │   └── data.parquet
    └── type=Product/
        └── data.parquet
```

**Query Routing**:
```typescript
async function routeQuery(ns: string, filter: QueryFilter): Promise<string[]> {
  const typeShard = filter.type?.eq;

  if (typeShard) {
    const shardPath = `${ns}/_shards/type=${typeShard}/data.parquet`;
    if (await exists(shardPath)) {
      return [shardPath];
    }
  }

  // Fall back to all shards
  return await listShards(ns);
}
```

#### 2. Time-Based Sharding

Split by time period. Best for event-like data with temporal access patterns.

```
{ns}/
├── data.parquet              # Current period
└── _shards/
    ├── period=2024-01/
    │   └── data.parquet
    ├── period=2024-02/
    │   └── data.parquet
    └── period=2024-03/
        └── data.parquet
```

**Query Routing**:
```typescript
async function routeTimeQuery(
  ns: string,
  filter: QueryFilter
): Promise<string[]> {
  const tsFilter = filter.ts || filter.created_at;

  if (tsFilter?.gte && tsFilter?.lte) {
    const periods = getPeriodsInRange(tsFilter.gte, tsFilter.lte);
    return periods.map(p => `${ns}/_shards/period=${p}/data.parquet`);
  }

  return await listShards(ns);
}
```

#### 3. Hash-Based Sharding

Distribute by ID hash. Best for uniform access patterns.

```
{ns}/
└── _shards/
    ├── shard=0/
    │   └── data.parquet
    ├── shard=1/
    │   └── data.parquet
    ├── shard=2/
    │   └── data.parquet
    └── shard=3/
        └── data.parquet
```

**Query Routing**:
```typescript
function getShardForId(id: string, shardCount: number): number {
  const hash = fnv1a(id);
  return hash % shardCount;
}

async function routeIdQuery(
  ns: string,
  filter: QueryFilter,
  shardCount: number
): Promise<string[]> {
  const idFilter = filter.id;

  if (idFilter?.eq) {
    const shard = getShardForId(idFilter.eq, shardCount);
    return [`${ns}/_shards/shard=${shard}/data.parquet`];
  }

  if (idFilter?.in) {
    const shards = new Set(idFilter.in.map(id => getShardForId(id, shardCount)));
    return [...shards].map(s => `${ns}/_shards/shard=${s}/data.parquet`);
  }

  return await listShards(ns);
}
```

### Shard Metadata

Track shard information for query planning:

```typescript
interface ShardMetadata {
  ns: string
  shard_id: string            // e.g., "type=Person", "shard=0"
  strategy: 'type' | 'time' | 'hash'

  // Stats
  entity_count: bigint
  file_size_bytes: bigint
  row_group_count: number

  // Bounds (for range-based sharding)
  min_ts: bigint | null
  max_ts: bigint | null

  // For type sharding
  entity_type: string | null

  // For hash sharding
  hash_min: number | null
  hash_max: number | null

  // Maintenance
  last_compaction: bigint
  needs_compaction: boolean
}
```

---

## Event Ordering

### Local Event Ordering (Within Namespace)

Events within a namespace are totally ordered by `(ts, event_id)`:

```typescript
interface LocalEventOrder {
  // Events have monotonically increasing ULID event_ids
  // ULID = 48-bit timestamp + 80-bit random
  // Sort by (ts, event_id) gives deterministic total order
}

async function replayNamespaceEvents(
  ns: string,
  fromTs: bigint,
  toTs: bigint
): Promise<NamespaceEvent[]> {
  return await parquet.scan(`${ns}/events.parquet`, {
    filter: {
      ts: { gte: fromTs, lte: toTs }
    },
    sort: [
      { column: 'ts', order: 'asc' },
      { column: 'event_id', order: 'asc' }
    ]
  });
}
```

### Global Event Ordering (Cross-Namespace)

For cross-namespace consistency, use a global sequence number:

```typescript
/**
 * Global event sequence for cross-namespace ordering
 */
interface GlobalEventSequence {
  seq: bigint                   // Global sequence number
  ns: string                    // Namespace
  event_id: string              // Local event ID
  ts: bigint                    // Event timestamp
}

// Store in _system/events.parquet
async function assignGlobalSequence(
  ns: string,
  eventId: string,
  ts: bigint
): Promise<bigint> {
  // Atomic increment of global sequence
  const seq = await atomicIncrement('_system/sequence');

  await appendEvent('_system/events.parquet', {
    seq,
    ns,
    event_id: eventId,
    ts
  });

  return seq;
}
```

### Causal Ordering for Related Events

Track causal dependencies for events that span namespaces:

```typescript
interface CausalEvent extends NamespaceEvent {
  // Vector clock for causal ordering
  vector_clock: Record<string, bigint>  // { ns: seq }

  // Parent events this event depends on
  depends_on: Array<{ ns: string, event_id: string }>
}

function happensBefore(a: CausalEvent, b: CausalEvent): boolean {
  // a happens-before b if a's vector clock <= b's for all namespaces
  for (const [ns, seq] of Object.entries(a.vector_clock)) {
    if (seq > (b.vector_clock[ns] || 0n)) {
      return false;
    }
  }
  return true;
}
```

---

## Time-Travel Across Namespaces

### Single Namespace Time-Travel

```typescript
async function namespaceAsOf(
  ns: string,
  timestamp: bigint
): Promise<NamespaceSnapshot> {
  // Option 1: Replay from beginning (expensive for large namespaces)
  // Option 2: Use checkpoint + delta replay (recommended)

  // Find nearest checkpoint before timestamp
  const checkpoint = await findCheckpointBefore(ns, timestamp);

  if (checkpoint) {
    // Load checkpoint state
    const state = await loadCheckpoint(ns, checkpoint.ts);

    // Replay events from checkpoint to target timestamp
    const events = await parquet.scan(`${ns}/events.parquet`, {
      filter: {
        ts: { gt: checkpoint.ts, lte: timestamp }
      },
      sort: [{ column: 'ts', order: 'asc' }]
    });

    return applyEvents(state, events);
  } else {
    // Full replay from beginning
    const events = await parquet.scan(`${ns}/events.parquet`, {
      filter: { ts: { lte: timestamp } },
      sort: [{ column: 'ts', order: 'asc' }]
    });

    return buildStateFromEvents(events);
  }
}
```

### Cross-Namespace Time-Travel

For consistent point-in-time queries across namespaces:

```typescript
interface GlobalSnapshot {
  timestamp: bigint
  global_seq: bigint            // Global sequence number at snapshot
  namespace_snapshots: Map<string, NamespaceSnapshot>
}

async function globalAsOf(
  timestamp: bigint,
  namespaces: string[]
): Promise<GlobalSnapshot> {
  // Find global sequence at timestamp
  const seqRecord = await parquet.scan('_system/events.parquet', {
    filter: { ts: { lte: timestamp } },
    sort: [{ column: 'seq', order: 'desc' }],
    limit: 1
  });

  const globalSeq = seqRecord[0]?.seq || 0n;

  // Load each namespace at timestamp
  const snapshots = new Map<string, NamespaceSnapshot>();

  await Promise.all(
    namespaces.map(async (ns) => {
      const snapshot = await namespaceAsOf(ns, timestamp);
      snapshots.set(ns, snapshot);
    })
  );

  // Verify cross-namespace consistency
  await verifyConsistency(snapshots, globalSeq);

  return {
    timestamp,
    global_seq: globalSeq,
    namespace_snapshots: snapshots
  };
}
```

### Checkpoint Management

```typescript
interface Checkpoint {
  ns: string
  ts: bigint
  event_id: string              // Last event included
  global_seq: bigint | null     // Global sequence (for cross-namespace)

  // Snapshot files
  data_snapshot: string         // Path to data snapshot
  edges_snapshot: string | null // Path to edges snapshot (if graph mode)

  // Stats
  entity_count: bigint
  event_count: bigint           // Events since previous checkpoint
}

async function createCheckpoint(ns: string): Promise<Checkpoint> {
  const currentTs = BigInt(Date.now()) * 1000n;

  // Read current state
  const data = await parquet.scan(`${ns}/data.parquet`, {
    filter: { deleted: { eq: false } }
  });

  // Write snapshot
  const snapshotPath = `${ns}/checkpoints/${currentTs}/data.parquet`;
  await parquet.write(snapshotPath, data);

  // Get last event
  const lastEvent = await parquet.scan(`${ns}/events.parquet`, {
    sort: [{ column: 'ts', order: 'desc' }],
    limit: 1
  });

  // Record checkpoint
  const checkpoint: Checkpoint = {
    ns,
    ts: currentTs,
    event_id: lastEvent[0]?.event_id,
    global_seq: await getGlobalSeq(ns, lastEvent[0]?.event_id),
    data_snapshot: snapshotPath,
    edges_snapshot: null,
    entity_count: BigInt(data.length),
    event_count: await countEventsSinceLastCheckpoint(ns)
  };

  await appendMeta(ns, { type: 'checkpoint', ...checkpoint });

  return checkpoint;
}
```

---

## Cloudflare R2 Integration

### R2-Optimized File Operations

```typescript
import { FSX } from '@graphdl/fsx';

interface R2Config {
  bucket: R2Bucket
  prefix: string              // Base path prefix (e.g., 'warehouse/')
}

class R2ParquetStore {
  private fsx: FSX;
  private cache: Map<string, ArrayBuffer>;

  constructor(config: R2Config) {
    this.fsx = new FSX(config.bucket, { prefix: config.prefix });
    this.cache = new Map();
  }

  /**
   * List namespaces using R2 prefix queries
   */
  async listNamespaces(): Promise<string[]> {
    const objects = await this.fsx.list('', {
      delimiter: '/',
      // R2 returns common prefixes for directory-like listing
    });

    return objects.commonPrefixes
      .filter(p => !p.startsWith('_'))  // Exclude system namespaces
      .map(p => p.replace(/\/$/, ''));
  }

  /**
   * List files in a namespace
   */
  async listNamespaceFiles(ns: string): Promise<FileInfo[]> {
    const objects = await this.fsx.list(`${ns}/`, {
      delimiter: '/',
    });

    return objects.objects.map(obj => ({
      path: obj.key,
      size: obj.size,
      lastModified: obj.uploaded,
      etag: obj.httpEtag,
    }));
  }

  /**
   * Read Parquet file with range requests for metadata
   */
  async readParquet(
    path: string,
    options?: { columns?: string[], filter?: QueryFilter }
  ): Promise<any[]> {
    // Check cache first
    if (this.cache.has(path)) {
      return this.parseParquet(this.cache.get(path)!, options);
    }

    // For metadata-only reads, use range request
    if (options?.columns?.length === 0) {
      const footer = await this.readParquetFooter(path);
      return [{ rowCount: footer.numRows, schema: footer.schema }];
    }

    // Full file read
    const data = await this.fsx.read(path);

    // Cache small files
    if (data.byteLength < 10 * 1024 * 1024) {  // 10MB
      this.cache.set(path, data);
    }

    return this.parseParquet(data, options);
  }

  /**
   * Read only Parquet footer using range request
   * R2 supports HTTP Range headers for partial reads
   */
  async readParquetFooter(path: string): Promise<ParquetMetadata> {
    // Parquet footer is at end of file
    // Read last 8 bytes to get footer length
    const fileSize = await this.getFileSize(path);
    const footerLengthBuffer = await this.fsx.readRange(
      path,
      fileSize - 8,
      8
    );

    const footerLength = new DataView(footerLengthBuffer).getUint32(0, true);

    // Read footer
    const footerBuffer = await this.fsx.readRange(
      path,
      fileSize - 8 - footerLength,
      footerLength
    );

    return parseParquetMetadata(footerBuffer);
  }

  /**
   * Write Parquet file atomically
   */
  async writeParquet(
    path: string,
    data: any[],
    schema: ParquetSchema
  ): Promise<void> {
    const buffer = await serializeParquet(data, schema);

    // R2 writes are atomic
    await this.fsx.write(path, buffer);

    // Invalidate cache
    this.cache.delete(path);
  }

  /**
   * Append to Parquet file (read-modify-write)
   *
   * Note: For high-throughput appends, use a write buffer
   * and batch appends into periodic flushes.
   */
  async appendParquet(
    path: string,
    newData: any[],
    schema: ParquetSchema
  ): Promise<void> {
    // Read existing data
    const existing = await this.readParquet(path).catch(() => []);

    // Merge and write
    const merged = [...existing, ...newData];
    await this.writeParquet(path, merged, schema);
  }
}
```

### R2 Prefix Query Optimization

```typescript
/**
 * R2 list operations are optimized for prefix queries.
 * Design file paths to leverage this.
 */

// Good: Namespace-scoped queries are fast
await r2.list({ prefix: 'tenant-a/' });

// Good: Type-sharded queries are fast
await r2.list({ prefix: 'tenant-a/_shards/type=Person/' });

// Bad: Cross-cutting queries require full list
// Instead, use the namespace registry
async function findNamespacesByOwner(ownerId: string): Promise<string[]> {
  // Query namespace registry instead of listing all files
  const records = await parquet.scan('_system/namespaces.parquet', {
    filter: { owner_id: { eq: ownerId } }
  });
  return records.map(r => r.ns);
}
```

### Concurrent Access Handling

```typescript
/**
 * R2 provides strong read-after-write consistency.
 * Use conditional writes for optimistic concurrency.
 */

class ConcurrentParquetStore extends R2ParquetStore {
  /**
   * Conditional write using R2's httpMetadata
   */
  async conditionalWrite(
    path: string,
    data: any[],
    schema: ParquetSchema,
    expectedEtag: string
  ): Promise<boolean> {
    try {
      const buffer = await serializeParquet(data, schema);

      await this.fsx.write(path, buffer, {
        onlyIf: {
          etagMatches: expectedEtag
        }
      });

      return true;
    } catch (err) {
      if (err.message.includes('precondition failed')) {
        return false;  // Concurrent modification detected
      }
      throw err;
    }
  }

  /**
   * Retry with exponential backoff
   */
  async writeWithRetry(
    path: string,
    transform: (data: any[]) => any[],
    schema: ParquetSchema,
    maxRetries: number = 3
  ): Promise<void> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      // Read current state
      const obj = await this.fsx.head(path);
      const data = await this.readParquet(path);

      // Apply transformation
      const newData = transform(data);

      // Try conditional write
      if (await this.conditionalWrite(path, newData, schema, obj.httpEtag)) {
        return;
      }

      // Backoff before retry
      await sleep(Math.pow(2, attempt) * 100);
    }

    throw new Error(`Failed to write after ${maxRetries} retries`);
  }
}
```

### Write Buffer for High-Throughput

```typescript
/**
 * Buffer writes and flush periodically to reduce R2 operations
 */
class BufferedParquetWriter {
  private buffer: Map<string, any[]> = new Map();
  private flushInterval: number;
  private maxBufferSize: number;

  constructor(
    private store: R2ParquetStore,
    options: { flushIntervalMs?: number, maxBufferSize?: number } = {}
  ) {
    this.flushInterval = options.flushIntervalMs || 1000;
    this.maxBufferSize = options.maxBufferSize || 10000;

    // Periodic flush
    setInterval(() => this.flushAll(), this.flushInterval);
  }

  async append(path: string, record: any): Promise<void> {
    if (!this.buffer.has(path)) {
      this.buffer.set(path, []);
    }

    const pathBuffer = this.buffer.get(path)!;
    pathBuffer.push(record);

    // Flush if buffer too large
    if (pathBuffer.length >= this.maxBufferSize) {
      await this.flush(path);
    }
  }

  async flush(path: string): Promise<void> {
    const records = this.buffer.get(path);
    if (!records || records.length === 0) return;

    this.buffer.set(path, []);  // Clear buffer

    await this.store.appendParquet(path, records, getSchemaForPath(path));
  }

  async flushAll(): Promise<void> {
    const paths = [...this.buffer.keys()];
    await Promise.all(paths.map(p => this.flush(p)));
  }
}
```

---

## FSX Integration

### FSX Interface for Namespace Operations

```typescript
import { FSX, FSXConfig } from '@graphdl/fsx';

interface NamespaceFS {
  // File operations scoped to namespace
  read(file: string): Promise<ArrayBuffer>
  write(file: string, data: ArrayBuffer): Promise<void>
  list(): Promise<string[]>
  delete(file: string): Promise<void>
  exists(file: string): Promise<boolean>

  // Namespace metadata
  getMeta(): Promise<NamespaceMetadata>
  getStats(): Promise<NamespaceStats>
}

class NamespaceFSX implements NamespaceFS {
  private fsx: FSX;
  private ns: string;

  constructor(fsx: FSX, ns: string) {
    this.fsx = fsx;
    this.ns = ns;
  }

  private path(file: string): string {
    return `${this.ns}/${file}`;
  }

  async read(file: string): Promise<ArrayBuffer> {
    return this.fsx.read(this.path(file));
  }

  async write(file: string, data: ArrayBuffer): Promise<void> {
    // Validate namespace exists
    if (!await this.namespaceExists()) {
      throw new Error(`Namespace ${this.ns} does not exist`);
    }

    return this.fsx.write(this.path(file), data);
  }

  async list(): Promise<string[]> {
    const objects = await this.fsx.list(this.path(''));
    return objects.map(o => o.replace(`${this.ns}/`, ''));
  }

  async delete(file: string): Promise<void> {
    return this.fsx.delete(this.path(file));
  }

  async exists(file: string): Promise<boolean> {
    return this.fsx.exists(this.path(file));
  }

  private async namespaceExists(): Promise<boolean> {
    // Check namespace registry
    const records = await this.fsx.parquet.scan('_system/namespaces.parquet', {
      filter: { ns: { eq: this.ns } }
    });
    return records.length > 0;
  }

  async getMeta(): Promise<NamespaceMetadata> {
    return this.fsx.parquet.scan(this.path('_meta.parquet'));
  }

  async getStats(): Promise<NamespaceStats> {
    const meta = await this.getMeta();
    const files = await this.list();

    let totalSize = 0;
    for (const file of files) {
      const stat = await this.fsx.stat(this.path(file));
      totalSize += stat.size;
    }

    return {
      ns: this.ns,
      entityCount: meta.find(m => m.stat_type === 'count')?.entity_count || 0n,
      fileCount: files.length,
      totalSizeBytes: totalSize,
    };
  }
}
```

### Namespace Lifecycle Management

```typescript
class NamespaceManager {
  constructor(private fsx: FSX) {}

  /**
   * Create a new namespace
   */
  async createNamespace(
    ns: string,
    config: Partial<NamespaceRecord>
  ): Promise<void> {
    // Validate namespace name
    if (!isValidNamespace(ns)) {
      throw new Error(`Invalid namespace: ${ns}`);
    }

    // Check if exists
    const exists = await this.namespaceExists(ns);
    if (exists) {
      throw new Error(`Namespace ${ns} already exists`);
    }

    // Create namespace record
    const record: NamespaceRecord = {
      ns,
      display_name: config.display_name || ns,
      owner_id: config.owner_id || 'system',
      created_at: BigInt(Date.now()) * 1000n,
      created_by: config.created_by || 'system',
      schema_mode: config.schema_mode || 'hybrid',
      isolation_level: config.isolation_level || 'strict',
      max_entities: config.max_entities || null,
      max_storage_bytes: config.max_storage_bytes || null,
      status: 'active',
      shard_strategy: 'none',
      shard_count: 0,
      retention_days: config.retention_days || 30,
      checkpoint_interval_hours: config.checkpoint_interval_hours || 24,
      metadata: config.metadata || { type: 'object', fields: {} },
    };

    // Add to registry
    await this.fsx.parquet.append(
      '_system/namespaces.parquet',
      [record],
      NAMESPACE_PARQUET_SCHEMA
    );

    // Initialize namespace files
    await this.initializeNamespaceFiles(ns);
  }

  private async initializeNamespaceFiles(ns: string): Promise<void> {
    // Create empty data file
    await this.fsx.parquet.write(
      `${ns}/data.parquet`,
      [],
      DATA_PARQUET_SCHEMA
    );

    // Create empty events file
    await this.fsx.parquet.write(
      `${ns}/events.parquet`,
      [],
      EVENTS_PARQUET_SCHEMA
    );

    // Create empty meta file
    await this.fsx.parquet.write(
      `${ns}/_meta.parquet`,
      [{
        stat_type: 'count',
        entity_type: null,
        entity_count: 0n,
        updated_at: BigInt(Date.now()) * 1000n,
        data: { type: 'object', fields: {} }
      }],
      META_PARQUET_SCHEMA
    );
  }

  /**
   * Delete a namespace (soft delete)
   */
  async deleteNamespace(ns: string): Promise<void> {
    // Update status to deleted
    await this.updateNamespaceStatus(ns, 'deleted');

    // Move files to archive
    const files = await this.fsx.list(`${ns}/`);
    for (const file of files) {
      await this.fsx.move(file, `_archive/${ns}/${file.replace(`${ns}/`, '')}`);
    }
  }

  /**
   * Archive a namespace (move to cold storage)
   */
  async archiveNamespace(ns: string): Promise<void> {
    await this.updateNamespaceStatus(ns, 'archived');

    // Optionally compress files
    // R2 doesn't have storage tiers, but we can prefix for lifecycle rules
    const files = await this.fsx.list(`${ns}/`);
    for (const file of files) {
      await this.fsx.move(file, `_archive/${ns}/${file.replace(`${ns}/`, '')}`);
    }
  }

  /**
   * Clone a namespace
   */
  async cloneNamespace(
    sourceNs: string,
    targetNs: string,
    options?: { includeEvents?: boolean }
  ): Promise<void> {
    // Create target namespace
    const sourceConfig = await this.getNamespaceConfig(sourceNs);
    await this.createNamespace(targetNs, {
      ...sourceConfig,
      display_name: `${sourceConfig.display_name} (clone)`,
    });

    // Copy data file
    const data = await this.fsx.read(`${sourceNs}/data.parquet`);
    await this.fsx.write(`${targetNs}/data.parquet`, data);

    // Optionally copy events
    if (options?.includeEvents) {
      const events = await this.fsx.read(`${sourceNs}/events.parquet`);
      await this.fsx.write(`${targetNs}/events.parquet`, events);
    }

    // Copy schema overrides
    if (await this.fsx.exists(`${sourceNs}/_schema.parquet`)) {
      const schema = await this.fsx.read(`${sourceNs}/_schema.parquet`);
      await this.fsx.write(`${targetNs}/_schema.parquet`, schema);
    }
  }
}
```

---

## Query API

### Unified Query Interface

```typescript
interface ParqueDBQuery {
  // Single namespace queries
  from(ns: string): NamespaceQuery

  // Cross-namespace queries
  federated(namespaces: string[]): FederatedQuery

  // Schema queries
  schema(): SchemaQuery

  // Time-travel
  asOf(timestamp: bigint): ParqueDBQuery
}

interface NamespaceQuery {
  type(entityType: string): NamespaceQuery
  filter(predicate: QueryFilter): NamespaceQuery
  select(columns: string[]): NamespaceQuery
  limit(n: number): NamespaceQuery
  offset(n: number): NamespaceQuery
  orderBy(column: string, direction: 'asc' | 'desc'): NamespaceQuery

  // Execute
  execute(): Promise<any[]>
  count(): Promise<number>
  first(): Promise<any | null>

  // Relationships
  join(relType: string, options?: JoinOptions): NamespaceQuery
  crossJoin(targetNs: string, relType: string): FederatedQuery
}

// Example usage
const results = await db
  .from('tenant-a')
  .type('Person')
  .filter({
    'data.email': { contains: '@example.com' },
    deleted: { eq: false }
  })
  .select(['id', 'data.name', 'data.email'])
  .orderBy('created_at', 'desc')
  .limit(100)
  .execute();
```

### Query Execution Plan

```typescript
interface QueryPlan {
  type: 'single' | 'federated' | 'cross-namespace'

  // Files to read
  files: Array<{
    path: string
    rowGroupsToRead: number[] | 'all'
    columns: string[]
  }>

  // Predicate pushdown
  pushdownFilters: QueryFilter[]

  // Post-scan filters (couldn't push down)
  postFilters: QueryFilter[]

  // Join plan (if cross-namespace)
  joinPlan?: {
    type: 'hash' | 'nested-loop'
    leftFiles: string[]
    rightFiles: string[]
    joinKey: string
  }

  // Estimated cost
  estimatedRowsScanned: number
  estimatedBytesRead: number
}

function planQuery(query: ParsedQuery): QueryPlan {
  const plan: QueryPlan = {
    type: query.namespaces.length > 1 ? 'federated' : 'single',
    files: [],
    pushdownFilters: [],
    postFilters: [],
    estimatedRowsScanned: 0,
    estimatedBytesRead: 0,
  };

  for (const ns of query.namespaces) {
    // Determine which files to read
    const files = resolveFiles(ns, query);

    for (const file of files) {
      // Read Parquet metadata for predicate pushdown
      const metadata = await readParquetMetadata(file);

      // Determine row groups to scan based on zone maps
      const rowGroups = filterRowGroups(metadata, query.filters);

      plan.files.push({
        path: file,
        rowGroupsToRead: rowGroups.length === metadata.rowGroupCount
          ? 'all'
          : rowGroups,
        columns: query.columns,
      });

      plan.estimatedRowsScanned += rowGroups.reduce(
        (sum, rg) => sum + metadata.rowGroups[rg].numRows,
        0
      );
    }
  }

  // Separate pushdown vs post filters
  for (const filter of query.filters) {
    if (canPushDown(filter)) {
      plan.pushdownFilters.push(filter);
    } else {
      plan.postFilters.push(filter);
    }
  }

  return plan;
}
```

---

## Performance Characteristics

### Read Performance

| Operation | Files Read | Complexity | Notes |
|-----------|------------|------------|-------|
| Get entity by ID (single ns) | 1 | O(log n) | Row group zone maps |
| List entities by type (single ns) | 1-N | O(log n + k) | Type sharding helps |
| Cross-namespace join | 2 + refs | O(n*m) | Use refs index |
| Federated query (M namespaces) | M | O(M * log n) | Parallel reads |
| Time-travel (with checkpoint) | 2 | O(k) | k = events since checkpoint |

### Write Performance

| Operation | Files Written | Latency | Notes |
|-----------|---------------|---------|-------|
| Insert entity | 2 | ~50ms | data + events |
| Insert with cross-ref | 3 | ~100ms | + refs file |
| Batch insert (buffered) | 2 | ~10ms/record | Amortized with buffer |
| Schema update | 1-2 | ~100ms | _system/schema + local |

### Storage Overhead

```
Base namespace:
  - data.parquet:    1x (primary data)
  - events.parquet:  0.5x (7-day retention)
  - _meta.parquet:   <1KB (stats only)
  - _schema.parquet: <10KB (if local overrides)

System overhead:
  - _system/schema.parquet:     <100KB (global types)
  - _system/namespaces.parquet: <1KB per namespace
  - _system/refs.parquet:       Variable (cross-refs only)

Total: ~1.5x raw entity storage + cross-ref overhead
```

---

## Migration from Single-File Architecture

### Migration Strategy

```typescript
async function migrateToNamespaceSharded(
  sourceFile: string,
  nsField: string = 'ns'
): Promise<void> {
  // Step 1: Read all data
  const data = await parquet.scan(sourceFile);

  // Step 2: Group by namespace
  const byNamespace = groupBy(data, nsField);

  // Step 3: Create namespaces
  for (const ns of Object.keys(byNamespace)) {
    await namespaceManager.createNamespace(ns, {
      schema_mode: 'global'
    });
  }

  // Step 4: Write data to namespace files
  for (const [ns, entities] of Object.entries(byNamespace)) {
    // Remove ns field (now implicit)
    const cleaned = entities.map(({ [nsField]: _, ...rest }) => rest);

    await parquet.write(`${ns}/data.parquet`, cleaned);
  }

  // Step 5: Extract cross-namespace references
  const refs = extractCrossNamespaceRefs(data);
  await parquet.write('_system/refs.parquet', refs);

  // Step 6: Migrate events (if exist)
  if (await exists(sourceFile.replace('.parquet', '_events.parquet'))) {
    await migrateEvents(sourceFile, byNamespace);
  }
}
```

---

## Conclusion

The namespace-sharded architecture provides a robust foundation for multi-tenant Parquet databases:

1. **Isolation by Default**: Each namespace is a self-contained unit with its own data, events, and optional schema overrides

2. **Flexible Schema Management**: Global, local, or hybrid schema modes support diverse multi-tenant requirements

3. **Efficient Cross-Namespace Operations**: Dedicated reference index (`_system/refs.parquet`) enables fast cross-namespace joins without full scans

4. **Scalable Sharding**: Namespaces can be further sharded by type, time, or hash as they grow

5. **R2-Optimized**: File organization leverages R2 prefix queries and atomic writes

6. **Time-Travel Ready**: Per-namespace event logs with global sequence numbers enable consistent point-in-time queries

This architecture balances tenant isolation with operational efficiency, making it suitable for SaaS applications, data marketplaces, and federated data systems built on ParqueDB.

---

*Architecture Design Document - ParqueDB Namespace-Sharded Multi-File Perspective*
