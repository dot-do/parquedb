# CLAUDE.md - Agent Instructions for ParqueDB

## Project Overview

ParqueDB is a hybrid relational/document/graph database built on Apache Parquet. It provides a MongoDB-style API with bidirectional relationships, time-travel, and RPC pipelining support for Cloudflare Workers.

## Architecture

### Core Concepts

1. **Three-file storage model**:
   - `data/{ns}/data.parquet` - Entity storage with Variant data
   - `rels/{forward,reverse}/{ns}.parquet` - Bidirectional relationship indexes
   - `events/current.parquet` - CDC event log for time-travel

2. **Entity structure**: Every entity has `$id`, `$type`, `name`, audit fields, data fields, and relationships

3. **Relationships**: Stored with `predicate` (outbound name) and `reverse` (inbound name), indexed both directions

4. **Variant type**: Semi-structured data with optional shredding for hot fields

### Key Dependencies

- `hyparquet` / `hyparquet-writer` - Pure JS Parquet read/write
- `capnweb` - RPC pipelining (RpcTarget, RpcPromise)
- `fsx` - Cloudflare Worker filesystem abstraction
- `graphdl` / `icetype` - Schema definitions

## Project Structure

```
src/
├── index.ts              # Main exports
├── db.ts                 # ParqueDB class (RpcTarget)
├── collection.ts         # Collection class (RpcTarget)
├── types/
│   ├── index.ts          # Re-exports
│   ├── entity.ts         # Entity, EntityRef, AuditFields
│   ├── filter.ts         # MongoDB-style filter operators
│   ├── update.ts         # Update operators ($set, $inc, etc.)
│   ├── options.ts        # FindOptions, GetOptions, etc.
│   ├── schema.ts         # Schema definitions
│   └── storage.ts        # StorageBackend interface
├── storage/
│   ├── backend.ts        # StorageBackend interface
│   ├── fs.ts             # Node.js filesystem
│   ├── fsx.ts            # Cloudflare fsx
│   ├── r2.ts             # Cloudflare R2
│   ├── s3.ts             # AWS S3
│   └── memory.ts         # In-memory (testing)
├── query/
│   ├── executor.ts       # Query execution
│   ├── filter.ts         # Filter evaluation
│   ├── sort.ts           # Sorting
│   └── project.ts        # Projection
├── mutation/
│   ├── create.ts         # Create operations
│   ├── update.ts         # Update operations
│   ├── delete.ts         # Delete operations
│   └── operators.ts      # $set, $inc, $push, etc.
├── relationships/
│   ├── manager.ts        # Relationship CRUD
│   ├── traverse.ts       # Graph traversal
│   └── index.ts          # Relationship indexing
├── events/
│   ├── log.ts            # Event logging
│   ├── replay.ts         # Time-travel replay
│   └── types.ts          # Event types
├── indexes/
│   ├── fts/              # Full-text search
│   ├── vector/           # Vector similarity
│   ├── bloom/            # Bloom filters
│   └── secondary/        # B-tree, hash indexes
└── utils/
    ├── parquet.ts        # Parquet helpers
    ├── variant.ts        # Variant encoding
    └── id.ts             # ID generation (ULID)
```

## Development Workflow

### Issue Tracking with bd (beads)

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

### TDD Workflow

All features follow Red-Green-Refactor:

1. **Red**: Write failing test first
2. **Green**: Implement minimum code to pass
3. **Refactor**: Clean up while keeping tests green

```bash
npm test              # Run all tests
npm test -- --watch   # Watch mode
npm test -- <pattern> # Run specific tests
```

### Code Style

- TypeScript strict mode
- ESM modules
- Prefer `interface` over `type` for objects
- Use branded types for IDs: `type EntityId = string & { __brand: 'EntityId' }`
- Async/await over raw Promises
- Descriptive variable names

## Key Implementation Details

### Proxy-based Collection Access

```typescript
// db.Posts.find() works via Proxy
return new Proxy(this, {
  get(target, prop: string) {
    if (prop in target) return (target as any)[prop]
    const ns = prop.charAt(0).toLowerCase() + prop.slice(1)
    return target.collection(ns)
  }
})
```

### RpcTarget Integration

```typescript
import { RpcTarget, RpcPromise } from 'capnweb'

export class ParqueDB extends RpcTarget {
  // Methods return RpcPromise for pipelining
  find(ns: string, filter?: Filter): RpcPromise<Entity[]>
}
```

### Filter Execution

Filters are evaluated against Parquet row groups using:
1. Column statistics (min/max) for predicate pushdown
2. Bloom filters for existence checks
3. Row-level filtering for remaining predicates

### Relationship Storage

```
rels/forward/{ns}.parquet:
  from_ns, from_id, from_name, from_type,
  predicate, reverse,
  to_ns, to_id, to_name, to_type,
  createdAt, createdBy, data

rels/reverse/{ns}.parquet:
  (same schema, sorted by to_ns, to_id, reverse)
```

### Event Log Format

```
events/current.parquet:
  id (ULID), ts, target ('entity'|'rel'), op ('CREATE'|'UPDATE'|'DELETE'),
  ns, entity_id, before (Variant), after (Variant), actor, metadata
```

## Testing Strategy

### Unit Tests
- Pure functions (filter evaluation, update operators)
- Variant encoding/decoding
- ID generation

### Integration Tests
- CRUD operations with MemoryBackend
- Relationship traversal
- Query execution

### E2E Tests
- Full workflow with real storage
- RPC pipelining
- Time-travel queries

## Session Completion Checklist

**MANDATORY before ending session:**

1. [ ] Create issues for remaining work (`bd create`)
2. [ ] Run tests if code changed (`npm test`)
3. [ ] Update issue status (`bd close <id>`)
4. [ ] Sync and push:
   ```bash
   git add .
   git commit -m "..."
   bd sync
   git push
   ```
5. [ ] Verify: `git status` shows "up to date with origin"

**CRITICAL: Work is NOT complete until `git push` succeeds.**

## Common Tasks

### Adding a New Filter Operator

1. Add type to `src/types/filter.ts`
2. Add evaluation logic to `src/query/filter.ts`
3. Add tests to `tests/query/filter.test.ts`
4. Update README.md documentation

### Adding a New Update Operator

1. Add type to `src/types/update.ts`
2. Add application logic to `src/mutation/operators.ts`
3. Add tests to `tests/mutation/operators.test.ts`
4. Update README.md documentation

### Adding a Storage Backend

1. Create `src/storage/{name}.ts` implementing `StorageBackend`
2. Add tests to `tests/storage/{name}.test.ts`
3. Export from `src/storage/index.ts`
4. Add example to README.md

## Architecture Decisions

See `docs/architecture/` for detailed design documents:
- `GRAPH_FIRST_ARCHITECTURE.md` - Relationship indexing
- `SECONDARY_INDEXES.md` - Index types and strategies
- `BLOOM_FILTER_INDEXES.md` - Probabilistic indexes
- `NAMESPACE_SHARDED_ARCHITECTURE.md` - Multi-tenant sharding

## Performance Targets

| Operation | Target (p50) | Target (p99) |
|-----------|-------------|-------------|
| Get by ID | 5ms | 20ms |
| Find (indexed) | 20ms | 100ms |
| Find (scan) | 100ms | 500ms |
| Create | 10ms | 50ms |
| Update | 15ms | 75ms |
| Relationship traverse | 50ms | 200ms |
