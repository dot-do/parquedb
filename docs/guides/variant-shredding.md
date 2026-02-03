---
title: Variant Shredding Guide
description: Configure variant shredding to optimize query performance on frequently-accessed fields
---

Variant shredding extracts frequently-queried fields from the Variant blob into dedicated Parquet columns, enabling predicate pushdown and statistics-based row group skipping.

## When to Use Variant Shredding

Use variant shredding when:

- **Filtering on specific fields**: Queries like `{ status: 'published' }` benefit from shredded columns
- **High selectivity filters**: Fields that filter out most rows (e.g., filtering by type or category)
- **Range queries**: Numeric or date fields used in `$gt`, `$lt`, `$gte`, `$lte` operations
- **Sorting**: Fields used in `sort` clauses benefit from columnar access

Avoid shredding:

- **Rarely queried fields**: Text content, descriptions, or payloads read only on entity retrieval
- **High cardinality unique fields**: Fields like UUIDs with no filtering benefit
- **Large nested objects**: Keep complex JSON structures in the Variant blob

## Configuration Options

### Schema-Level Shredding (`$shred`)

Explicitly specify fields to shred in your schema:

```typescript
const schema: Schema = {
  Post: {
    // Shred these fields for efficient filtering
    $shred: ['status', 'publishedAt', 'authorId'],

    title: 'string!',
    content: 'markdown!',
    status: 'string = "draft"',
    publishedAt: 'datetime',
    authorId: 'uuid!',
    metadata: 'json',  // Kept in Variant blob
  },
}
```

### Index Modifier (`#`)

Fields with the `#` index modifier are automatically shredded:

```typescript
const schema: Schema = {
  Product: {
    // Indexed fields are automatically shredded
    sku: 'string##!',      // Unique index -> shredded
    category: 'string#!',  // Standard index -> shredded
    price: 'decimal(10,2)#!',

    // Not shredded (no index)
    description: 'text',
    specifications: 'json',
  },
}
```

### Backend-Level ShredConfig

Configure shredding at the backend level for global defaults:

```typescript
import { createIcebergBackend } from 'parquedb/backends'
import { R2Backend } from 'parquedb/storage'

const backend = createIcebergBackend({
  type: 'iceberg',
  storage: new R2Backend(env.BUCKET),
  warehouse: 'warehouse',
  database: 'mydb',
  shredding: {
    // Always shred these fields across all collections
    fields: ['status', 'createdAt', 'type'],

    // Auto-detect fields to shred based on usage patterns
    autoDetect: true,

    // Cardinality threshold for auto-detection (default: 1000)
    autoDetectThreshold: 500,
  },
})
```

### ShredConfig Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fields` | `string[]` | `[]` | Fields to always shred across all collections |
| `autoDetect` | `boolean` | `false` | Enable automatic field detection for shredding |
| `autoDetectThreshold` | `number` | `1000` | Cardinality threshold below which fields are auto-shredded |

### Type-Based Auto-Shredding

Certain field types are automatically shredded when `autoDetect` is enabled:

```typescript
// These types are shredded by default (good statistics, low cardinality)
const autoShredTypes = [
  'enum',      // Enumerated values
  'boolean',   // True/false
  'date',      // Date values
  'datetime',  // Timestamps
  'timestamp', // Precise timestamps
  'int',       // Integers
  'float',     // Floating point
]
```

## Example Configurations

### Event Log Schema

Optimize for time-range and type filtering:

```typescript
const schema: Schema = {
  Event: {
    $shred: ['eventType', 'timestamp', 'userId', 'severity'],

    eventType: 'string!',       // Filter: { eventType: 'user.login' }
    timestamp: 'timestamp!',    // Range: { timestamp: { $gte: lastHour } }
    userId: 'uuid!',            // Filter: { userId: 'abc123' }
    severity: 'enum(info,warn,error)!',

    // Large payload stays in Variant
    payload: 'json',
    stackTrace: 'text',
    metadata: 'json',
  },
}
```

### E-Commerce Catalog

Optimize for category browsing and price filtering:

```typescript
const schema: Schema = {
  Product: {
    $shred: ['category', 'price', 'inStock', 'brand'],

    name: 'string!',
    category: 'string#!',       // Browse by category
    price: 'decimal(10,2)#!',   // Price range filters
    inStock: 'boolean!',        // In-stock filter
    brand: 'string#!',          // Brand filter

    // Full content in Variant
    description: 'markdown',
    specifications: 'json',
    images: 'json',
  },
}
```

### Multi-Tenant SaaS

Optimize for tenant isolation and status filtering:

```typescript
const schema: Schema = {
  Document: {
    $shred: ['tenantId', 'status', 'createdAt', 'ownerId'],

    tenantId: 'uuid!',          // Tenant isolation
    status: 'enum(draft,active,archived)!',
    createdAt: 'timestamp!',
    ownerId: 'uuid!',

    title: 'string!',
    content: 'text',
    settings: 'json',
  },
}
```

## Performance Benefits

Variant shredding provides significant query performance improvements:

| Query Type | Without Shredding | With Shredding | Speedup |
|------------|-------------------|----------------|---------|
| Equality filter (100K rows) | 200ms | 5-10ms | 20-40x |
| Range filter (100K rows) | 250ms | 8-15ms | 15-30x |
| Multi-field AND filter | 300ms | 10-20ms | 15-30x |
| Index column projection | 100ms | 5ms | 20x |

### How It Works

1. **Statistics-based skipping**: Parquet stores min/max statistics per row group. Shredded columns enable the query engine to skip row groups that cannot match the filter.

2. **Columnar reads**: Only the shredded columns are read for filtering, avoiding full Variant deserialization.

3. **Predicate pushdown**: Filters are pushed down to the storage layer, reducing I/O.

### Storage Overhead

Shredding adds minimal storage overhead (typically 5-15%):

| Configuration | File Size | Overhead |
|---------------|-----------|----------|
| Variant only (baseline) | 100 MB | - |
| 3 shredded fields | 105 MB | +5% |
| 6 shredded fields | 110 MB | +10% |
| 10 shredded fields | 115 MB | +15% |

The query performance benefits typically outweigh the modest storage increase.

## Best Practices

1. **Shred filter fields**: Prioritize fields used in `find()` filter predicates.

2. **Shred sort fields**: Fields used in `sort` options benefit from columnar access.

3. **Limit shred count**: Shred 3-10 fields per collection. More fields increase storage without proportional benefit.

4. **Use indexes for equality**: The `#` modifier combines shredding with secondary indexes for O(1) lookups.

5. **Monitor query patterns**: Use auto-detection initially, then explicitly configure based on observed queries.

6. **Consider cardinality**: Low-cardinality fields (status, type, category) are ideal candidates.

7. **Review periodically**: As query patterns evolve, update shredding configuration.

## Troubleshooting

### Queries Not Using Shredded Columns

Verify the field is shredded:

```typescript
// Check schema configuration
const type = await db.getSchema('posts')
console.log(type.$shred) // ['status', 'publishedAt', ...]
```

### High Storage Overhead

Reduce shredded field count or exclude large string fields:

```typescript
// Before: Shredding large text field
$shred: ['status', 'description', 'content']

// After: Only shred filter fields
$shred: ['status']
```

### Auto-Detection Not Working

Ensure `autoDetect` is enabled and check the threshold:

```typescript
const backend = createIcebergBackend({
  // ...
  shredding: {
    autoDetect: true,
    autoDetectThreshold: 100, // Lower threshold for more aggressive detection
  },
})
```

## Related Documentation

- [Schema Definition](../schemas.md) - Complete schema definition reference
- [Query Performance](../benchmarks.md#variant-shredding-benefits) - Benchmark results
- [Architecture: Variant Shredding](../architecture/variant-shredding.md) - Technical implementation details
