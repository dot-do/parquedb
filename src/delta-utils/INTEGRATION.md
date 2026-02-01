# Delta Lake Utilities Integration Guide

This document describes how the shared utilities in `delta-utils` can be used to integrate ParqueDB with Delta Lake.

## Overview

The `delta-utils` module provides common patterns extracted from both ParqueDB and the Delta Lake implementation at `~/projects/deltalake`. These utilities enable:

1. **Shared Storage Backends** - Common interface for R2, S3, filesystem, and memory storage
2. **Parquet Integration** - AsyncBuffer for efficient Parquet file reading
3. **Filter Semantics** - MongoDB-style filters with zone map pruning
4. **Transaction Logging** - NDJSON-based transaction log format
5. **Change Data Capture** - Unified CDC record format
6. **Retry Logic** - Exponential backoff for handling concurrency conflicts

## Module Structure

```
src/delta-utils/
  index.ts           # Main exports
  storage.ts         # Storage backend types and AsyncBuffer
  filter.ts          # Filter types and zone map pruning
  transaction-log.ts # Transaction log utilities
  cdc.ts             # Change Data Capture
  retry.ts           # Retry with exponential backoff
  variant.ts         # Parquet VARIANT encoding
```

## Integration Patterns

### 1. Storage Backend

Both ParqueDB and Delta Lake can use the same storage abstraction:

```typescript
import { MinimalStorageBackend, createAsyncBuffer } from './delta-utils'

// Create an AsyncBuffer for Parquet reading
const buffer = await createAsyncBuffer(storage, 'data/table.parquet')

// Use with hyparquet
const rows = await parquetReadObjects({ file: buffer })
```

For Delta Lake's optimistic concurrency control:

```typescript
import { ConditionalStorageBackend, VersionMismatchError } from './delta-utils'

try {
  const newVersion = await storage.writeConditional(path, data, expectedVersion)
} catch (error) {
  if (error instanceof VersionMismatchError) {
    // Another writer modified the file - refresh and retry
  }
}
```

### 2. Filter Translation

MongoDB-style filters can be translated to Parquet predicates for pushdown:

```typescript
import { matchesFilter, filterToZoneMapPredicates, canSkipZoneMap } from './delta-utils'

// Filter documents in memory
const matches = data.filter(row => matchesFilter(row, {
  status: 'active',
  score: { $gte: 100 }
}))

// Convert to zone map predicates for row group skipping
const predicates = filterToZoneMapPredicates(filter)

// Check if a row group can be skipped
for (const predicate of predicates) {
  if (canSkipZoneMap(rowGroupStats[predicate.column], predicate)) {
    continue // Skip this row group
  }
}
```

### 3. Transaction Log

Both systems can use a consistent transaction log format:

```typescript
import {
  serializeCommit,
  parseCommit,
  formatVersion,
  createAddAction,
  createRemoveAction,
} from './delta-utils'

// Create a commit
const actions = [
  createAddAction({
    path: 'data/part-00001.parquet',
    size: 12345,
    modificationTime: Date.now(),
    dataChange: true,
  }),
]

// Serialize to NDJSON
const commitData = serializeCommit(actions)

// Write to versioned log file
const logPath = `_delta_log/${formatVersion(version)}.json`
await storage.write(logPath, new TextEncoder().encode(commitData))

// Parse a commit
const parsedActions = parseCommit(await storage.read(logPath))
```

### 4. Change Data Capture

Unified CDC format for cross-system interoperability:

```typescript
import { CDCProducer, CDCConsumer, cdcRecordToDeltaRecords } from './delta-utils'

// ParqueDB producing CDC records
const producer = new CDCProducer({
  source: { database: 'mydb', collection: 'users' },
  system: 'parquedb',
})

const record = await producer.create('user-123', { name: 'Alice' })

// Convert to Delta Lake CDC format
const deltaRecords = cdcRecordToDeltaRecords(record, 1n, new Date())

// Delta Lake consuming records
const consumer = new CDCConsumer({
  operations: ['c', 'u', 'd'], // Create, Update, Delete
})

consumer.subscribe(async (record) => {
  console.log(`${record._op}: ${record._id}`)
})
```

### 5. Retry with Backoff

Handle concurrency conflicts gracefully:

```typescript
import { withRetry, isRetryableError, VersionMismatchError } from './delta-utils'

const result = await withRetry(async () => {
  // This may throw VersionMismatchError on conflict
  return await table.write(rows)
}, {
  maxRetries: 5,
  baseDelay: 100,
  onRetry: ({ attempt, error }) => {
    console.log(`Retry ${attempt} after: ${error.message}`)
  }
})

// With metrics
const { result, metrics } = await withRetry(fn, { returnMetrics: true })
console.log(`Succeeded after ${metrics.attempts} attempts`)
```

### 6. Variant Encoding

Efficient semi-structured data storage:

```typescript
import { encodeVariant, decodeVariant, isEncodable } from './delta-utils'

// Check if value can be encoded
if (isEncodable(document)) {
  // Encode for storage
  const { metadata, value } = encodeVariant(document)

  // Store in Parquet VARIANT column
  // ...

  // Decode when reading
  const decoded = decodeVariant({ metadata, value })
}
```

## Future Integration Steps

### Phase 1: Shared Storage

1. Move ParqueDB storage backends to use `MinimalStorageBackend` interface
2. Add `ConditionalStorageBackend` support to ParqueDB for optimistic concurrency
3. Update Delta Lake to use the same interfaces

### Phase 2: Filter Unification

1. Use `filterToZoneMapPredicates` in both ParqueDB and Delta Lake
2. Standardize zone map statistics format
3. Share filter evaluation logic

### Phase 3: Transaction Log

1. ParqueDB can adopt Delta Lake-style transaction logs for ACID
2. Share checkpoint format and versioning
3. Enable cross-system log replay for migrations

### Phase 4: CDC Integration

1. ParqueDB event log can emit CDC records
2. Delta Lake CDC can be consumed by ParqueDB
3. Enable real-time sync between systems

## Compatibility Notes

- All utilities are designed for both Node.js and Cloudflare Workers
- No Node.js-specific APIs are used in the core utilities
- TypeScript strict mode is used throughout
- Dependencies are minimal (only `hyparquet` for Parquet operations)

## Testing

Tests should be added for:

```bash
# Run delta-utils tests
npm test -- --filter delta-utils

# Test specific modules
npm test -- tests/unit/delta-utils/storage.test.ts
npm test -- tests/unit/delta-utils/filter.test.ts
npm test -- tests/unit/delta-utils/retry.test.ts
```
