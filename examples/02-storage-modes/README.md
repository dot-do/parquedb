# Storage Modes

ParqueDB supports two storage formats, optimized for different use cases.

## Typed Mode

Native columns for schema fields. Best for:
- Structured data with known schema
- SQL queries with predicate pushdown
- Analytics workloads

```typescript
const db = DB({
  User: {
    $id: 'email',
    $options: {
      includeDataVariant: true,  // Include $data blob for fast full-row reads
    },
    email: 'string!#',
    name: 'string',
    role: 'string',
  }
}, { storage: new FsBackend('.db') })
```

**Storage:** `.db/data/user.parquet` with columns `$id`, `email`, `name`, `role`, `$data`

### Without $data

For append-only workloads where you only need columnar access:

```typescript
AuditLog: {
  $options: {
    includeDataVariant: false,  // Skip $data - columnar only
  },
  action: 'string!',
  actor: 'string!',
  timestamp: 'datetime!',
}
```

**Storage:** `.db/data/auditlog.parquet` with columns only (no JSON blob)

## Flexible Mode

Variant-shredded storage for schemaless data:

```typescript
Event: 'flexible'
```

**Storage:** `.db/data/event/data.parquet` with `$id`, `$data`, `$index_*` columns

Hot fields are automatically indexed as `$index_type`, `$index_url`, etc.

## Run

```bash
npx tsx examples/02-storage-modes/index.ts
```
