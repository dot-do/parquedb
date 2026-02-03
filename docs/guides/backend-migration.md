# Backend Migration Guide

ParqueDB supports seamless migration between storage backends (Native Parquet, Apache Iceberg, Delta Lake). Like schema evolution, backend evolution is a first-class feature.

## Quick Start

### Automatic Migration

When creating a backend, set `migrateFrom: 'auto'` to automatically migrate existing data:

```typescript
import { createBackendWithMigration } from 'parquedb/backends'

// Start with simple native Parquet
const db = DB({ schema: MySchema })
await db.Users.create({ name: 'Alice' })

// Later, switch to Iceberg with automatic migration
const icebergBackend = await createBackendWithMigration({
  type: 'iceberg',
  storage: r2Backend,
  warehouse: 'warehouse',
  migrateFrom: 'auto', // Auto-detect and migrate native → iceberg
})
```

### CLI Migration

```bash
# Check current backend status
parquedb backend-status

# Migrate all namespaces to Iceberg
parquedb migrate-backend --to=iceberg

# Migrate specific namespaces
parquedb migrate-backend --to=delta --namespaces=users,posts

# Dry run (see what would happen)
parquedb migrate-backend --to=iceberg --dry-run
```

## How It Works

### 1. Format Detection

ParqueDB auto-detects the format of existing data:

```typescript
import { detectExistingFormat } from 'parquedb/backends'

const { formats, primary } = await detectExistingFormat(storage, 'users')
// formats: ['native', 'iceberg'] - all formats found
// primary: 'iceberg' - recommended format to read from
```

### 2. Migration Process

When migrating from Native to Iceberg:

1. **Scan** - Read all entities from native Parquet files
2. **Transform** - Convert to Iceberg row format with proper schema
3. **Write** - Create Iceberg metadata, manifests, and data files
4. **Verify** - Confirm entity count matches
5. **Cleanup** (optional) - Delete source files

### 3. Zero-Downtime Migration

For production workloads, use the dual-read strategy:

```typescript
// Phase 1: Write to new format, read from both
const backend = await createBackendWithMigration({
  type: 'iceberg',
  storage,
  migrateFrom: 'native',
  keepSource: true, // Keep native files during migration
})

// Phase 2: After migration completes, read only from new format
// Old native files can be archived or deleted
```

## Migration Paths

| From | To | Support | Notes |
|------|-----|---------|-------|
| Native | Iceberg | ✅ Full | Recommended for analytics |
| Native | Delta | ✅ Full | Recommended for Databricks |
| Iceberg | Delta | ✅ Full | Preserves time-travel history |
| Delta | Iceberg | ✅ Full | Preserves time-travel history |
| Iceberg | Native | ⚠️ Lossy | Loses time-travel/ACID |
| Delta | Native | ⚠️ Lossy | Loses time-travel/ACID |

## Best Practices

### 1. Start Simple, Evolve Later

```typescript
// Start with native Parquet (simplest)
const db = DB({ schema: MySchema })

// When you need time-travel or external engine compatibility:
const db = DB({ schema: MySchema }, {
  backend: {
    type: 'iceberg',
    migrateFrom: 'auto',
  }
})
```

### 2. Test Migration First

```bash
# Always dry-run before actual migration
parquedb migrate-backend --to=iceberg --dry-run

# Check the output, then run for real
parquedb migrate-backend --to=iceberg
```

### 3. Keep Backups

```bash
# Migration with source retention
parquedb migrate-backend --to=iceberg --keep-source

# Verify, then clean up later
parquedb cleanup-backend --format=native --namespaces=users
```

### 4. Monitor Progress

```typescript
await migrateBackend({
  storage,
  from: 'native',
  to: 'iceberg',
  onProgress: (progress) => {
    console.log(`${progress.namespace}: ${progress.entitiesMigrated}/${progress.totalEntities}`)
  },
})
```

## Troubleshooting

### "No data found for namespace"

The namespace doesn't have data in the expected location. Check storage paths:
- Native: `data/{namespace}/data.parquet` or `{namespace}.parquet`
- Iceberg: `{namespace}/metadata/v1.metadata.json`
- Delta: `{namespace}/_delta_log/00000000000000000000.json`

### "Migration failed: Schema mismatch"

The source and target schemas don't match. Use schema evolution first:

```typescript
// Evolve schema before migration
await backend.evolveSchema('users', {
  addColumn: { name: 'email', type: 'string', nullable: true },
})
```

### "Conflict with existing data"

Data already exists in the target format. Options:
1. Use `--force` to overwrite
2. Use `--merge` to combine (deduplicates by `$id`)
3. Delete target data first

## API Reference

### `createBackendWithMigration(config)`

Creates a backend with automatic migration support.

```typescript
interface BackendConfigWithMigration extends BackendConfig {
  migrateFrom?: 'native' | 'iceberg' | 'delta' | 'auto'
}
```

### `migrateBackend(config)`

Manually migrate data between formats.

```typescript
interface MigrationConfig {
  storage: StorageBackend
  from: 'native' | 'iceberg' | 'delta' | 'auto'
  to: 'native' | 'iceberg' | 'delta'
  namespaces?: string[] | '*'
  batchSize?: number
  deleteSource?: boolean
  onProgress?: (progress: MigrationProgress) => void
}
```

### `detectExistingFormat(storage, namespace)`

Detect what formats exist for a namespace.

```typescript
const { formats, primary } = await detectExistingFormat(storage, 'users')
```

### `discoverNamespaces(storage)`

Find all namespaces in storage.

```typescript
const namespaces = await discoverNamespaces(storage)
// ['users', 'posts', 'comments']
```
