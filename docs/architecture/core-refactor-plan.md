# ParqueDB Core Refactor Plan

This document outlines the plan to refactor `src/ParqueDB/core.ts` from a 3133-line monolith into smaller, focused modules.

## Current State Analysis

### File Size and Structure

- **File**: `src/ParqueDB/core.ts`
- **Lines**: 3133
- **Size**: ~116KB
- **Single Class**: `ParqueDBImpl` containing all database operations

### Current Module Dependencies

The existing `src/ParqueDB/` directory contains:

```
src/ParqueDB/
  index.ts          # Re-exports (22 lines)
  types.ts          # Type definitions
  validation.ts     # Validation utilities
  store.ts          # Global state stores
  collection.ts     # CollectionImpl wrapper
  relationships.ts  # Relationship utilities (already extracted)
  core.ts           # THE MONOLITH (3133 lines)
```

### Major Responsibilities in core.ts

| Responsibility | Lines | Methods |
|----------------|-------|---------|
| **Initialization/Lifecycle** | ~100 | constructor, dispose, registerSchema, getSchemaValidator |
| **CRUD Operations** | ~650 | find, get, create, update, delete, deleteMany, restore |
| **Upsert Operations** | ~200 | upsert, upsertMany, extractFilterFields, buildUpsertCreateData |
| **Relationship Management** | ~350 | applyRelationshipOperators, indexRelationshipsForEntity, unindexRelationshipsForEntity, hydrateEntity, getRelated |
| **Event Log & Time-Travel** | ~600 | recordEvent, flushEvents, scheduleFlush, maybeRotateEventLog, archiveEvents, getArchivedEvents, getEventLog, history, getAtVersion, reconstructEntityAtTime |
| **Snapshot Management** | ~250 | getSnapshotManager (createSnapshot, createSnapshotAtEvent, listSnapshots, deleteSnapshot, pruneSnapshots, getRawSnapshot, getQueryStats, getStorageStats) |
| **Transactions** | ~150 | beginTransaction (create, update, delete, commit, rollback) |
| **Diff & Revert** | ~150 | diff, revert |
| **Index Management** | ~100 | createIndex, dropIndex, listIndexes, getIndex, rebuildIndex, getIndexStats, getIndexManager |
| **Validation & Schema** | ~200 | validateAgainstSchema, legacyValidateAgainstSchema, isFieldRequired, hasDefault, validateFieldType, applySchemaDefaults |
| **Storage Router** | ~60 | getStorageMode, getDataPath, hasTypedSchema, getCollectionOptions, getStorageRouter |
| **Collection Access** | ~20 | collection |

---

## Proposed File Structure

```
src/ParqueDB/
  index.ts              # Re-exports (updated)
  types.ts              # Existing types
  validation.ts         # Existing validation
  store.ts              # Existing global stores

  core.ts               # SLIM: Class definition, constructor, dispose, proxy setup (~150 lines)
  crud.ts               # NEW: find, get, create, update, delete, deleteMany, restore (~700 lines)
  upsert.ts             # NEW: upsert, upsertMany, helper methods (~250 lines)
  relationships.ts      # ENHANCED: Add relationship operators from core (~400 lines)
  events.ts             # NEW: Event log, time-travel, history (~650 lines)
  snapshots.ts          # NEW: Snapshot management (~300 lines)
  transactions.ts       # NEW: Transaction support (~200 lines)
  time-travel.ts        # NEW: diff, revert, reconstructEntityAtTime (~200 lines)
  indexes.ts            # NEW: Index management wrapper (~150 lines)
  schema-validation.ts  # NEW: Schema validation methods (~250 lines)
  storage-router.ts     # NEW: Storage routing utilities (~80 lines)
  collection.ts         # Existing CollectionImpl
```

---

## Detailed Migration Plan

### Phase 1: Extract Pure Utilities (Low Risk)

#### 1.1 Create `schema-validation.ts`

Extract validation-related methods that don't depend on instance state:

```typescript
// src/ParqueDB/schema-validation.ts

export function validateAgainstSchema(
  schema: Schema,
  schemaValidator: SchemaValidator | null,
  namespace: string,
  data: CreateInput,
  validateOnWrite?: boolean | ValidationMode
): void

export function legacyValidateAgainstSchema(
  schema: Schema,
  namespace: string,
  data: CreateInput
): void

export function isFieldRequired(fieldDef: unknown): boolean

export function hasDefault(fieldDef: unknown): boolean

export function validateFieldType(
  fieldName: string,
  value: unknown,
  fieldDef: unknown,
  typeName: string
): void

export function applySchemaDefaults<T>(
  schema: Schema,
  data: CreateInput<T>
): CreateInput<T>
```

**Migration Steps:**
1. Create new file with exported functions
2. Functions receive `schema` as parameter instead of `this.schema`
3. Update `core.ts` to import and call these functions
4. Run tests to verify no regressions

#### 1.2 Create `storage-router.ts`

Extract storage routing utilities:

```typescript
// src/ParqueDB/storage-router.ts

export function getStorageMode(
  storageRouter: IStorageRouter | null,
  namespace: string
): StorageMode

export function getDataPath(
  storageRouter: IStorageRouter | null,
  namespace: string
): string

export function hasTypedSchema(
  storageRouter: IStorageRouter | null,
  namespace: string
): boolean
```

---

### Phase 2: Extract Event System (Medium Risk)

#### 2.1 Create `events.ts`

This is a critical module that handles the event log, CDC, and time-travel queries.

```typescript
// src/ParqueDB/events.ts

export interface EventSystemConfig {
  storage: StorageBackend
  events: Event[]
  archivedEvents: Event[]
  eventLogConfig: Required<EventLogConfig>
  snapshotConfig: SnapshotConfig
  snapshots: Snapshot[]
  inTransaction: boolean
}

export class EventSystem {
  private pendingEvents: Event[] = []
  private flushPromise: Promise<void> | null = null

  constructor(private config: EventSystemConfig)

  // Core event recording
  async recordEvent(op: EventOp, target: string, before: Entity | null, after: Entity | null, actor?: EntityId, meta?: Record<string, unknown>): Promise<void>

  // Flush management
  private async flushEvents(): Promise<void>
  private scheduleFlush(): Promise<void>

  // Rotation
  private maybeRotateEventLog(): void

  // Archive
  archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): ArchiveEventsResult
  getArchivedEvents(): Event[]

  // Event log interface
  getEventLog(): EventLog

  // History
  async history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult>
  async getAtVersion<T>(namespace: string, id: string, version: number): Promise<Entity<T> | null>
}
```

**Migration Steps:**
1. Create `EventSystem` class in new file
2. Move all event-related methods
3. Add `EventSystem` instance to `ParqueDBImpl`
4. Delegate event methods to `EventSystem`
5. Update tests

---

### Phase 3: Extract Snapshot System (Medium Risk)

#### 3.1 Create `snapshots.ts`

```typescript
// src/ParqueDB/snapshots.ts

export interface SnapshotSystemConfig {
  storage: StorageBackend
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  snapshotConfig: SnapshotConfig
}

export class SnapshotSystem implements SnapshotManager {
  constructor(private config: SnapshotSystemConfig)

  async createSnapshot(entityId: EntityId): Promise<Snapshot>
  async createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot>
  async listSnapshots(entityId: EntityId): Promise<Snapshot[]>
  async deleteSnapshot(snapshotId: string): Promise<void>
  async pruneSnapshots(options: PruneSnapshotsOptions): Promise<void>
  async getRawSnapshot(snapshotId: string): Promise<RawSnapshot>
  async getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats>
  async getStorageStats(): Promise<SnapshotStorageStats>
}
```

---

### Phase 4: Extract Time-Travel (Medium Risk)

#### 4.1 Create `time-travel.ts`

```typescript
// src/ParqueDB/time-travel.ts

export interface TimeTravelConfig {
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
}

export class TimeTravelSystem {
  constructor(private config: TimeTravelConfig)

  // Core reconstruction
  reconstructEntityAtTime(fullId: string, asOf: Date): Entity | null

  // Diff
  async diff(entityId: EntityId, t1: Date, t2: Date): Promise<DiffResult>

  // Revert
  async revert<T>(
    entityId: EntityId,
    targetTime: Date,
    options?: RevertOptions
  ): Promise<Entity<T>>
}
```

---

### Phase 5: Extract Transactions (Low Risk)

#### 5.1 Create `transactions.ts`

```typescript
// src/ParqueDB/transactions.ts

export interface TransactionContext {
  entities: Map<string, Entity>
  events: Event[]
  pendingEvents: Event[]
  inTransaction: boolean
  flushEvents: () => Promise<void>
  create: <T>(ns: string, data: CreateInput<T>, options?: CreateOptions) => Promise<Entity<T>>
  update: <T>(ns: string, id: string, update: UpdateInput<T>, options?: UpdateOptions) => Promise<Entity<T> | null>
  delete: (ns: string, id: string, options?: DeleteOptions) => Promise<DeleteResult>
}

export function beginTransaction(ctx: TransactionContext): ParqueDBTransaction
```

---

### Phase 6: Extract CRUD Operations (High Risk)

#### 6.1 Create `crud.ts`

This is the most complex extraction due to dependencies on multiple subsystems.

```typescript
// src/ParqueDB/crud.ts

export interface CRUDContext {
  storage: StorageBackend
  entities: Map<string, Entity>
  schema: Schema
  schemaValidator: SchemaValidator | null
  indexManager: IndexManager
  reverseRelIndex: Map<string, Map<string, Set<string>>>
  embeddingProvider: EmbeddingProvider | null
  recordEvent: (op: EventOp, target: string, before: Entity | null, after: Entity | null, actor?: EntityId) => Promise<void>
  reconstructEntityAtTime: (fullId: string, asOf: Date) => Entity | null
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  events: Event[]
}

export class CRUDOperations {
  constructor(private ctx: CRUDContext)

  async find<T>(namespace: string, filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>
  async get<T>(namespace: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
  async create<T>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  async update<T>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  async delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  async deleteMany(namespace: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
  async restore<T>(namespace: string, id: string, options?: { actor?: EntityId }): Promise<Entity<T> | null>
  async getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult>
}
```

#### 6.2 Create `upsert.ts`

```typescript
// src/ParqueDB/upsert.ts

export function extractFilterFields(filter: Filter): Record<string, unknown>

export function buildUpsertCreateData<T>(
  filterFields: Record<string, unknown>,
  update: UpdateInput<T>
): Record<string, unknown>

export async function upsert<T>(
  ctx: CRUDContext,
  namespace: string,
  filter: Filter,
  update: UpdateInput<T>,
  options?: { returnDocument?: 'before' | 'after' }
): Promise<Entity<T> | null>

export async function upsertMany<T>(
  ctx: CRUDContext,
  namespace: string,
  items: UpsertManyItem<T>[],
  options?: UpsertManyOptions
): Promise<UpsertManyResult>
```

---

### Phase 7: Enhance Relationships Module

The existing `relationships.ts` already has `applyRelationshipOperators`. Add:

```typescript
// src/ParqueDB/relationships.ts (enhanced)

// Existing exports...

// Add from core.ts:
export function indexRelationshipsForEntity(
  reverseRelIndex: ReverseRelIndex,
  sourceId: string,
  entity: Entity
): void

export function unindexRelationshipsForEntity(
  reverseRelIndex: ReverseRelIndex,
  sourceId: string,
  entity: Entity
): void

export function hydrateEntity<T>(
  entities: Map<string, Entity>,
  schema: Schema,
  reverseRelIndex: ReverseRelIndex,
  entity: Entity<T>,
  fullId: string,
  hydrateFields: string[],
  maxInbound: number
): Entity<T>

export async function getRelated<T>(
  entities: Map<string, Entity>,
  schema: Schema,
  reverseRelIndex: ReverseRelIndex,
  namespace: string,
  id: string,
  relationField: string,
  options?: GetRelatedOptions
): Promise<GetRelatedResult<T>>
```

---

### Phase 8: Refactor Core to Slim Coordinator

After all extractions, `core.ts` becomes a slim coordinator:

```typescript
// src/ParqueDB/core.ts (after refactor, ~200 lines)

import { EventSystem } from './events'
import { SnapshotSystem } from './snapshots'
import { TimeTravelSystem } from './time-travel'
import { CRUDOperations } from './crud'
import * as SchemaValidation from './schema-validation'
import * as StorageRouter from './storage-router'
import { beginTransaction } from './transactions'

export class ParqueDBImpl {
  // Instance properties
  private storage: StorageBackend
  private schema: Schema = {}
  private schemaValidator: SchemaValidator | null = null
  private collections = new Map<string, CollectionImpl>()
  private indexManager: IndexManager

  // Subsystems
  private eventSystem: EventSystem
  private snapshotSystem: SnapshotSystem
  private timeTravelSystem: TimeTravelSystem
  private crudOps: CRUDOperations

  // Shared state (via global stores)
  private entities: Map<string, Entity>
  private events: Event[]
  private archivedEvents: Event[]
  private snapshots: Snapshot[]
  private queryStats: Map<string, SnapshotQueryStats>
  private reverseRelIndex: Map<string, Map<string, Set<string>>>

  constructor(config: ParqueDBConfig) {
    // Initialize shared state from global stores
    // Initialize subsystems
    // Wire up dependencies
  }

  dispose(): void { /* cleanup */ }

  // Schema
  registerSchema(schema: Schema): void
  getSchemaValidator(): SchemaValidator | null

  // Collection access
  collection<T>(namespace: string): Collection<T>

  // Delegate to CRUD
  find = (...args) => this.crudOps.find(...args)
  get = (...args) => this.crudOps.get(...args)
  create = (...args) => this.crudOps.create(...args)
  update = (...args) => this.crudOps.update(...args)
  delete = (...args) => this.crudOps.delete(...args)
  deleteMany = (...args) => this.crudOps.deleteMany(...args)
  restore = (...args) => this.crudOps.restore(...args)
  upsert = (...args) => upsert(this.crudContext, ...args)
  upsertMany = (...args) => upsertMany(this.crudContext, ...args)
  getRelated = (...args) => getRelated(...)
  getHistory = (...args) => this.crudOps.getHistory(...args)

  // Delegate to EventSystem
  getEventLog = () => this.eventSystem.getEventLog()
  archiveEvents = (...args) => this.eventSystem.archiveEvents(...args)
  getArchivedEvents = () => this.eventSystem.getArchivedEvents()
  history = (...args) => this.eventSystem.history(...args)
  getAtVersion = (...args) => this.eventSystem.getAtVersion(...args)

  // Delegate to SnapshotSystem
  getSnapshotManager = () => this.snapshotSystem

  // Delegate to TimeTravelSystem
  diff = (...args) => this.timeTravelSystem.diff(...args)
  revert = (...args) => this.timeTravelSystem.revert(...args)

  // Transactions
  beginTransaction = () => beginTransaction(this.transactionContext)

  // Delegate to IndexManager
  createIndex = (...args) => this.indexManager.createIndex(...args)
  dropIndex = (...args) => this.indexManager.dropIndex(...args)
  listIndexes = (...args) => this.indexManager.listIndexes(...args)
  getIndex = (...args) => this.indexManager.getIndexMetadata(...args)
  rebuildIndex = (...args) => this.indexManager.rebuildIndex(...args)
  getIndexStats = (...args) => this.indexManager.getIndexStats(...args)
  getIndexManager = () => this.indexManager

  // Storage router
  getStorageMode = (ns) => StorageRouter.getStorageMode(this.storageRouter, ns)
  getDataPath = (ns) => StorageRouter.getDataPath(this.storageRouter, ns)
  hasTypedSchema = (ns) => StorageRouter.hasTypedSchema(this.storageRouter, ns)
  getCollectionOptions = (ns) => this.collectionOptions.get(normalizeNamespace(ns))
  getStorageRouter = () => this.storageRouter
}
```

---

## Testing Strategy

### Unit Tests

Each new module should have its own test file:

```
tests/unit/ParqueDB/
  schema-validation.test.ts  # Pure function tests
  storage-router.test.ts     # Pure function tests
  events.test.ts             # EventSystem class tests
  snapshots.test.ts          # SnapshotSystem class tests
  time-travel.test.ts        # TimeTravelSystem class tests
  transactions.test.ts       # Transaction tests
  crud.test.ts               # CRUD operations tests
  upsert.test.ts             # Upsert operations tests
```

### Integration Tests

Keep existing integration tests passing:

```
tests/integration/
  parquedb.test.ts           # Full workflow tests
  time-travel.test.ts        # Time-travel scenarios
  relationships.test.ts      # Relationship traversal
```

### Migration Testing Approach

1. **Before each phase**: Ensure all tests pass
2. **After extraction**: Run full test suite
3. **Type checking**: Run `tsc --noEmit` to catch type errors
4. **API compatibility**: Ensure `ParqueDBImpl` public API unchanged

---

## Risk Assessment

### High Risk Areas

1. **CRUD Operations** - Tightly coupled to events, snapshots, indexes
   - Mitigation: Extract last, use context object pattern

2. **Event System** - Core to all mutations
   - Mitigation: Maintain same event format, test replay scenarios

3. **Shared State** - Global stores accessed by multiple modules
   - Mitigation: Pass store references explicitly, avoid circular dependencies

### Medium Risk Areas

1. **Snapshots** - Depends on events and entity reconstruction
2. **Time-travel** - Depends on events and snapshots
3. **Relationships** - Partially extracted, needs enhancement

### Low Risk Areas

1. **Schema validation** - Pure functions, no state
2. **Storage router** - Pure functions, no state
3. **Transactions** - Self-contained, clear boundaries
4. **Index management** - Already delegated to IndexManager

---

## Implementation Timeline

| Phase | Description | Estimated Effort | Risk |
|-------|-------------|------------------|------|
| 1 | Extract utilities (schema-validation, storage-router) | 2 hours | Low |
| 2 | Extract EventSystem | 4 hours | Medium |
| 3 | Extract SnapshotSystem | 2 hours | Medium |
| 4 | Extract TimeTravelSystem | 2 hours | Medium |
| 5 | Extract Transactions | 1 hour | Low |
| 6 | Extract CRUD + Upsert | 6 hours | High |
| 7 | Enhance Relationships | 2 hours | Medium |
| 8 | Slim down core.ts | 2 hours | Medium |
| - | Testing & cleanup | 4 hours | - |
| **Total** | | **~25 hours** | |

---

## Success Criteria

1. **core.ts reduced to <250 lines**
2. **All existing tests pass**
3. **No public API changes** (backward compatible)
4. **Each module <500 lines**
5. **Clear separation of concerns**
6. **Improved testability** (easier to mock subsystems)

---

## Future Considerations

After this refactor, consider:

1. **Lazy loading** - Only load subsystems when needed
2. **Dependency injection** - Allow custom implementations
3. **Plugin architecture** - Make subsystems pluggable
4. **Worker-specific modules** - Separate code paths for Workers vs Node.js

---

## Appendix: Method-to-Module Mapping

| Method | Current Location | Target Module |
|--------|------------------|---------------|
| `constructor` | core.ts | core.ts |
| `dispose` | core.ts | core.ts |
| `registerSchema` | core.ts | core.ts |
| `getSchemaValidator` | core.ts | core.ts |
| `collection` | core.ts | core.ts |
| `find` | core.ts | crud.ts |
| `get` | core.ts | crud.ts |
| `create` | core.ts | crud.ts |
| `update` | core.ts | crud.ts |
| `delete` | core.ts | crud.ts |
| `deleteMany` | core.ts | crud.ts |
| `restore` | core.ts | crud.ts |
| `upsert` | core.ts | upsert.ts |
| `upsertMany` | core.ts | upsert.ts |
| `extractFilterFields` | core.ts | upsert.ts |
| `buildUpsertCreateData` | core.ts | upsert.ts |
| `getRelated` | core.ts | relationships.ts |
| `applyRelationshipOperators` | core.ts | relationships.ts |
| `indexRelationshipsForEntity` | core.ts | relationships.ts |
| `unindexRelationshipsForEntity` | core.ts | relationships.ts |
| `hydrateEntity` | core.ts | relationships.ts |
| `recordEvent` | core.ts | events.ts |
| `flushEvents` | core.ts | events.ts |
| `scheduleFlush` | core.ts | events.ts |
| `maybeRotateEventLog` | core.ts | events.ts |
| `archiveEvents` | core.ts | events.ts |
| `getArchivedEvents` | core.ts | events.ts |
| `getEventLog` | core.ts | events.ts |
| `history` | core.ts | events.ts |
| `getHistory` | core.ts | crud.ts (delegates to events.ts) |
| `getAtVersion` | core.ts | events.ts |
| `reconstructEntityAtTime` | core.ts | time-travel.ts |
| `diff` | core.ts | time-travel.ts |
| `revert` | core.ts | time-travel.ts |
| `getSnapshotManager` | core.ts | snapshots.ts |
| `beginTransaction` | core.ts | transactions.ts |
| `validateAgainstSchema` | core.ts | schema-validation.ts |
| `legacyValidateAgainstSchema` | core.ts | schema-validation.ts |
| `isFieldRequired` | core.ts | schema-validation.ts |
| `hasDefault` | core.ts | schema-validation.ts |
| `validateFieldType` | core.ts | schema-validation.ts |
| `applySchemaDefaults` | core.ts | schema-validation.ts |
| `getStorageMode` | core.ts | storage-router.ts |
| `getDataPath` | core.ts | storage-router.ts |
| `hasTypedSchema` | core.ts | storage-router.ts |
| `getCollectionOptions` | core.ts | core.ts |
| `getStorageRouter` | core.ts | core.ts |
| `createIndex` | core.ts | indexes.ts (wrapper) |
| `dropIndex` | core.ts | indexes.ts (wrapper) |
| `listIndexes` | core.ts | indexes.ts (wrapper) |
| `getIndex` | core.ts | indexes.ts (wrapper) |
| `rebuildIndex` | core.ts | indexes.ts (wrapper) |
| `getIndexStats` | core.ts | indexes.ts (wrapper) |
| `getIndexManager` | core.ts | core.ts |
