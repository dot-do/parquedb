---
title: Namespace-Sharded Architecture
description: Multi-tenant isolation through namespace-based file sharding, with support for cross-namespace operations, flexible schema management, and R2-optimized storage on Cloudflare Workers.
---

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

## Conclusion

The namespace-sharded architecture provides a robust foundation for multi-tenant Parquet databases:

1. **Isolation by Default**: Each namespace is a self-contained unit with its own data, events, and optional schema overrides

2. **Flexible Schema Management**: Global, local, or hybrid schema modes support diverse multi-tenant requirements

3. **Efficient Cross-Namespace Operations**: Dedicated reference index (`_system/refs.parquet`) enables fast cross-namespace joins without full scans

4. **Scalable Sharding**: Namespaces can be further sharded by type, time, or hash as they grow

5. **R2-Optimized**: File organization leverages R2 prefix queries and atomic writes

6. **Time-Travel Ready**: Per-namespace event logs with global sequence numbers enable consistent point-in-time queries

This architecture balances tenant isolation with operational efficiency, making it suitable for SaaS applications, data marketplaces, and federated data systems built on ParqueDB.

## Related Documentation

- [DO Write Bottleneck](./do-write-bottleneck.md) - Durable Object write scaling via sharding
- [Entity Storage Architecture](./entity-storage.md) - Dual storage model (DO + R2)
- [Consistency Model](./consistency.md) - Read/write consistency guarantees

---

*Architecture Design Document - ParqueDB Namespace-Sharded Multi-File Perspective*
