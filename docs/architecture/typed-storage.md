---
title: Typed Storage Architecture
description: Design for typed/columnar storage in DB() factory with native columns for schema-defined fields and optional $data variant column for fast full-row reads.
---

## Overview

ParqueDB's `DB()` factory supports schema definitions, but currently all collections use the same variant-shredded storage pattern. This document designs typed/columnar storage for schema-defined collections while maintaining backward compatibility with flexible mode.

## Goals

1. **Native column storage** for schema-defined fields (predicate pushdown, filtering, statistics)
2. **Optional `$data` variant column** for fast full-row reads (no columnar reconstruction)
3. **Flexible mode** preservation for schema-less collections
4. **Configuration options** for storage behavior per collection

## Storage Modes

### 1. Typed Mode (Schema-Defined Collections)

When a collection has a schema, write to `data/{collection}.parquet` with:

```
data/occupations.parquet
├── $id          (STRING, indexed, required)
├── $type        (STRING, required)
├── $data        (BYTE_ARRAY/JSON, optional - full row blob)
├── name         (STRING, required)
├── socCode      (STRING, required, indexed)
├── jobZone      (INT32, optional)
├── createdAt    (TIMESTAMP_MILLIS, required)
├── updatedAt    (TIMESTAMP_MILLIS, required)
├── version      (INT64, required)
```

**Benefits:**
- Native predicate pushdown on any schema field
- Column statistics for row group skipping
- Efficient columnar scans for aggregations
- Type-safe storage matching schema definitions

**`$data` Column:**
- Contains the complete entity as Variant/JSON for fast full-row reads
- Enabled by default (`includeDataVariant: true`)
- Can be disabled for append-only/log collections where columnar access is primary
- Avoids expensive columnar-to-row reconstruction on reads

### 2. Flexible Mode (Schema-Less Collections)

Collections marked as `'flexible'` use existing variant-shredded storage:

```
data/{ns}/data.parquet
├── $id          (STRING, indexed, required)
├── $type        (STRING, required)
├── $index_*     (various - shredded index fields from Variant)
├── $data        (BYTE_ARRAY/JSON - full Variant blob)
├── createdAt    (TIMESTAMP_MILLIS, required)
├── updatedAt    (TIMESTAMP_MILLIS, required)
├── version      (INT64, required)
```

**Benefits:**
- No schema required
- Dynamic field addition
- Partial index shredding for hot fields

## Configuration API

```typescript
const db = DB({
  // Typed collection with full configuration
  Occupation: {
    $options: {
      includeDataVariant: true,     // Include $data column (default: true)
      compression: 'lz4',           // Per-collection compression
      rowGroupSize: 50000,          // Per-collection row group size
    },
    name: 'string!',
    socCode: 'string!#',            // # = indexed
    jobZone: 'int',
  },

  // Typed collection, optimized for append-only logs (no $data)
  AuditLog: {
    $options: { includeDataVariant: false },
    action: 'string!',
    actor: 'string!',
    timestamp: 'datetime!',
    details: 'json',
  },

  // Flexible mode - variant-shredded storage
  Posts: 'flexible',
})
```

### `$options` Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `includeDataVariant` | boolean | `true` | Include `$data` Variant column for fast full-row reads |
| `compression` | string | `'lz4'` | Compression codec: 'lz4', 'snappy', 'gzip', 'zstd', 'none' |
| `rowGroupSize` | number | `50000` | Rows per row group |
| `sortBy` | string[] | `['$id']` | Columns to sort by for clustering |

## Storage Router Design

```typescript
interface StorageRouter {
  /**
   * Determine storage mode for a collection
   */
  getStorageMode(collection: string): 'typed' | 'flexible'

  /**
   * Get storage path for a collection
   */
  getStoragePath(collection: string): string

  /**
   * Get Parquet schema for writing
   */
  getParquetSchema(collection: string): ParquetSchema

  /**
   * Route a write operation to the correct storage handler
   */
  routeWrite(collection: string, entities: Entity[]): Promise<void>

  /**
   * Route a read operation to the correct storage handler
   */
  routeRead(collection: string, options: ReadOptions): Promise<Entity[]>
}
```

### Implementation

```typescript
class StorageRouterImpl implements StorageRouter {
  private schema: Schema
  private collectionOptions: Map<string, CollectionOptions>
  private flexibleCollections: Set<string>

  getStorageMode(collection: string): 'typed' | 'flexible' {
    if (this.flexibleCollections.has(collection)) {
      return 'flexible'
    }
    // Has schema definition = typed mode
    if (this.schema[collection]) {
      return 'typed'
    }
    // Unknown collection = flexible by default
    return 'flexible'
  }

  getStoragePath(collection: string): string {
    const mode = this.getStorageMode(collection)
    if (mode === 'typed') {
      // Typed: data/{collection}.parquet
      return `data/${collection.toLowerCase()}.parquet`
    }
    // Flexible: data/{ns}/data.parquet
    return `data/${collection.toLowerCase()}/data.parquet`
  }

  getParquetSchema(collection: string): ParquetSchema {
    const mode = this.getStorageMode(collection)
    if (mode === 'typed') {
      return this.buildTypedSchema(collection)
    }
    return this.buildFlexibleSchema(collection)
  }
}
```

## Parquet Schema Generator

### Type Mapping: IceType/GraphDL to Parquet

| IceType/GraphDL | Parquet Type | Parquet Logical Type |
|-----------------|--------------|----------------------|
| `string` | BYTE_ARRAY | STRING |
| `string!` | BYTE_ARRAY (required) | STRING |
| `text` | BYTE_ARRAY | STRING |
| `int` | INT64 | INT64 |
| `int32` | INT32 | INT32 |
| `float` | FLOAT | - |
| `double` | DOUBLE | - |
| `number` | DOUBLE | - |
| `boolean` | BOOLEAN | - |
| `date` | INT32 | DATE |
| `datetime` | INT64 | TIMESTAMP_MILLIS |
| `timestamp` | INT64 | TIMESTAMP_MILLIS |
| `uuid` | FIXED_LEN_BYTE_ARRAY(16) | UUID |
| `json` | BYTE_ARRAY | JSON |
| `binary` | BYTE_ARRAY | - |
| `decimal(p,s)` | INT64 or BYTE_ARRAY | DECIMAL(p,s) |
| `varchar(n)` | BYTE_ARRAY | STRING |
| `enum(...)` | BYTE_ARRAY | ENUM |
| `vector(n)` | BYTE_ARRAY | - (custom encoding) |

### Schema Generator Implementation

```typescript
interface ParquetSchemaGenerator {
  /**
   * Generate Parquet schema from TypeDefinition
   */
  fromTypeDefinition(
    typeDef: TypeDefinition,
    options: SchemaGeneratorOptions
  ): ParquetSchema

  /**
   * Generate Parquet field schema from field type string
   */
  fieldToParquet(
    fieldName: string,
    fieldType: string
  ): ParquetFieldSchema
}

interface SchemaGeneratorOptions {
  /** Include $data Variant column */
  includeDataVariant: boolean
  /** Include system fields ($id, $type, createdAt, etc.) */
  includeSystemFields: boolean
  /** Include relationship fields as columns */
  includeRelationships: boolean
}
```

### Implementation

```typescript
class ParquetSchemaGeneratorImpl implements ParquetSchemaGenerator {
  private typeMap: Record<string, ParquetFieldSchema> = {
    'string': { type: 'STRING', optional: true },
    'text': { type: 'STRING', optional: true },
    'int': { type: 'INT64', optional: true },
    'int32': { type: 'INT32', optional: true },
    'float': { type: 'FLOAT', optional: true },
    'double': { type: 'DOUBLE', optional: true },
    'number': { type: 'DOUBLE', optional: true },
    'boolean': { type: 'BOOLEAN', optional: true },
    'date': { type: 'INT32', optional: true }, // DATE logical type
    'datetime': { type: 'INT64', optional: true }, // TIMESTAMP_MILLIS
    'timestamp': { type: 'INT64', optional: true }, // TIMESTAMP_MILLIS
    'uuid': { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 16, optional: true },
    'json': { type: 'BYTE_ARRAY', optional: true }, // JSON logical type
    'binary': { type: 'BYTE_ARRAY', optional: true },
  }

  fromTypeDefinition(
    typeDef: TypeDefinition,
    options: SchemaGeneratorOptions
  ): ParquetSchema {
    const schema: ParquetSchema = {}

    // Always include system fields
    if (options.includeSystemFields) {
      schema.$id = { type: 'STRING', optional: false }
      schema.$type = { type: 'STRING', optional: false }
      schema.createdAt = { type: 'INT64', optional: false } // TIMESTAMP_MILLIS
      schema.updatedAt = { type: 'INT64', optional: false }
      schema.version = { type: 'INT64', optional: false }
    }

    // Include $data Variant column if enabled
    if (options.includeDataVariant) {
      schema.$data = { type: 'BYTE_ARRAY', optional: true } // JSON or Variant
    }

    // Add fields from type definition
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      // Skip system properties
      if (fieldName.startsWith('$')) continue

      const fieldSchema = this.fieldToParquet(fieldName, fieldDef)
      if (fieldSchema) {
        schema[fieldName] = fieldSchema
      }
    }

    return schema
  }

  fieldToParquet(fieldName: string, fieldDef: string | FieldDefinition): ParquetFieldSchema | null {
    // Parse field type string
    const typeStr = typeof fieldDef === 'string' ? fieldDef : fieldDef.type

    // Skip relationships (stored separately)
    if (isRelationString(typeStr)) {
      return null
    }

    const { type, required, isArray } = parseFieldType(typeStr)

    // Get base Parquet type
    const baseSchema = this.typeMap[type]
    if (!baseSchema) {
      // Unknown type - use JSON/Variant
      return { type: 'BYTE_ARRAY', optional: !required }
    }

    return {
      ...baseSchema,
      optional: !required,
      repetitionType: isArray ? 'REPEATED' : (required ? 'REQUIRED' : 'OPTIONAL'),
    }
  }
}
```

## `$data` Variant Column Design

### Purpose

The `$data` column serves two purposes:
1. **Fast full-row reads**: Avoid expensive columnar-to-row reconstruction
2. **Schema evolution**: New fields are accessible even before schema migration

### Encoding

The `$data` column uses Parquet Variant encoding (when available) or JSON:

```typescript
interface DataColumnEncoder {
  /**
   * Encode entity to $data column value
   */
  encode(entity: Entity): Uint8Array | string

  /**
   * Decode $data column value to entity
   */
  decode(data: Uint8Array | string): Entity
}
```

### Read Path Optimization

When reading full entities:
1. **If `$data` present**: Decode `$data` directly (O(1) per row)
2. **If `$data` absent**: Reconstruct from columns (O(columns) per row)

```typescript
function readEntity(row: ParquetRow, schema: ParquetSchema): Entity {
  // Fast path: use $data if available
  if (row.$data != null) {
    return decodeVariant(row.$data)
  }

  // Slow path: reconstruct from columns
  return reconstructFromColumns(row, schema)
}
```

### When to Disable `$data`

Set `includeDataVariant: false` when:
- Collection is append-only (logs, events, metrics)
- Primary access is columnar (aggregations, analytics)
- Storage size is critical (avoid duplication)
- Fields rarely accessed together

## Write Path

### Typed Mode Write Flow

```
Entity -> StorageRouter.routeWrite()
       -> getStorageMode() = 'typed'
       -> getParquetSchema()
       -> ParquetWriter.write()
          -> Convert entity to columns
          -> Add $data Variant (if enabled)
          -> Write Parquet file
```

### Flexible Mode Write Flow (Existing)

```
Entity -> StorageRouter.routeWrite()
       -> getStorageMode() = 'flexible'
       -> Use existing variant-shredded flow
          -> Extract shred fields
          -> Encode $data Variant
          -> Write Parquet file
```

## Read Path

### Typed Mode Read Flow

```
Query -> StorageRouter.routeRead()
      -> getStorageMode() = 'typed'
      -> Apply predicate pushdown on native columns
      -> Read matching row groups
      -> Reconstruct entities (from $data or columns)
```

### Predicate Pushdown

With native columns, filters map directly to Parquet predicates:

```typescript
// MongoDB-style filter
{ status: 'active', jobZone: { $gte: 3 } }

// Maps to Parquet predicates
[
  { column: 'status', op: 'eq', value: 'active' },
  { column: 'jobZone', op: 'gte', value: 3 }
]
```

## Migration Path

### Phase 1: Storage Router
- Add `StorageRouter` interface and implementation
- Route based on schema presence
- Backward compatible with existing flexible storage

### Phase 2: Schema Generator
- Implement `ParquetSchemaGenerator`
- Add type mapping from IceType to Parquet
- Generate schemas for typed collections

### Phase 3: Typed Writer
- Extend `ParquetWriter` for typed mode
- Add `$data` column encoding
- Write native columns from entity fields

### Phase 4: Typed Reader
- Extend `ParquetReader` for typed mode
- Add predicate pushdown on native columns
- Implement $data-based reconstruction

### Phase 5: `$options` Support
- Parse `$options` from collection schema
- Apply per-collection configuration
- Support `includeDataVariant`, compression, etc.

## File Structure Changes

### Current Structure (Flexible)
```
data/
├── posts/
│   └── data.parquet       # Variant-shredded
├── users/
│   └── data.parquet       # Variant-shredded
```

### New Structure (Mixed)
```
data/
├── occupations.parquet    # Typed (native columns)
├── skills.parquet         # Typed (native columns)
├── posts/
│   └── data.parquet       # Flexible (variant-shredded)
```

## Integration Points

### DB() Factory
```typescript
// In db.ts
function DB(input: DBInput, config: DBConfig = {}): DBInstance {
  // ... existing code ...

  // Create storage router with schema info
  const storageRouter = new StorageRouterImpl({
    schema: parqueDBSchema,
    flexibleCollections: getFlexibleCollections(input),
    collectionOptions: getCollectionOptions(input),
  })

  // Inject router into ParqueDB
  const db = new ParqueDB({
    ...parqueDBConfig,
    storageRouter,
  })

  // ... rest of existing code ...
}
```

### ParqueDB Class
```typescript
class ParqueDBImpl {
  private storageRouter?: StorageRouter

  async create(ns: string, input: CreateInput): Promise<Entity> {
    // ... validation ...

    // Route to correct storage
    if (this.storageRouter) {
      await this.storageRouter.routeWrite(ns, [entity])
    } else {
      // Existing flexible write path
    }

    // ... rest of existing code ...
  }
}
```

## Performance Considerations

### Write Performance
- Typed mode may be slightly slower due to columnar encoding
- `$data` adds ~30-50% storage overhead but enables fast reads
- Row group size affects both write throughput and read efficiency

### Read Performance
- Predicate pushdown on native columns is significantly faster
- `$data` enables O(1) entity reconstruction vs O(columns)
- Column pruning reduces I/O for partial reads

### Storage Overhead
- `$data` duplicates data but enables different access patterns
- Consider disabling for large collections with columnar access patterns

## References

- [Variant Shredding](./variant-shredding.md) - Existing shredded storage
- [Entity Storage](./entity-storage.md) - Dual storage architecture
- [Secondary Indexes](./secondary-indexes.md) - Index architecture
- [Parquet Variant Encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/)
