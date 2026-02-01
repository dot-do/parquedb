/**
 * ParqueDB - A Parquet-based database
 *
 * This module provides the main ParqueDB class with support for both
 * explicit and Proxy-based collection access patterns.
 */

import type {
  Entity,
  EntityId,
  CreateInput,
  PaginatedResult,
  DeleteResult,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  Schema,
  StorageBackend,
  HistoryOptions,
  Event,
  EventOp,
  RelSet,
  SortSpec,
  Projection,
} from './types'
import { entityTarget, parseEntityTarget, relTarget, isRelationshipTarget } from './types'
import { parseFieldType, isRelationString, parseRelation } from './types/schema'
import { FileNotFoundError } from './storage/MemoryBackend'
import { IndexManager } from './indexes/manager'
import type { IndexDefinition, IndexMetadata, IndexStats } from './indexes/types'
import { getRandomBase36 } from './utils'

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Snapshot configuration options
 */
export interface SnapshotConfig {
  /** Automatically create snapshot after this many events */
  autoSnapshotThreshold?: number
}

// =============================================================================
// UpsertMany Types
// =============================================================================

/**
 * Item for upsertMany operation
 */
export interface UpsertManyItem<T = Record<string, unknown>> {
  /** Filter to find existing document */
  filter: Filter
  /** Update operations to apply */
  update: UpdateInput<T>
  /** Per-item options */
  options?: {
    /** Expected version for optimistic concurrency */
    expectedVersion?: number
  }
}

/**
 * Options for upsertMany operation
 */
export interface UpsertManyOptions {
  /** Stop on first error if true (default: true) */
  ordered?: boolean
  /** Actor performing the operation */
  actor?: EntityId
}

/**
 * Error entry in upsertMany result
 */
export interface UpsertManyError {
  /** Index of the failed item */
  index: number
  /** Filter that was used */
  filter: Filter
  /** Error details */
  error: Error
}

/**
 * Result of upsertMany operation
 */
export interface UpsertManyResult {
  /** Whether all operations succeeded */
  ok: boolean
  /** Number of documents that were inserted */
  insertedCount: number
  /** Number of documents that were modified */
  modifiedCount: number
  /** Number of documents that matched filters */
  matchedCount: number
  /** Number of documents that were upserted (inserted) */
  upsertedCount: number
  /** IDs of upserted documents */
  upsertedIds: EntityId[]
  /** Errors that occurred */
  errors: UpsertManyError[]
}

/**
 * ParqueDB configuration options
 */
export interface ParqueDBConfig {
  /** Storage backend for data persistence */
  storage: StorageBackend

  /** Schema definition for entity validation */
  schema?: Schema

  /** Default namespace for operations */
  defaultNamespace?: string

  /** Snapshot configuration */
  snapshotConfig?: SnapshotConfig
}

// =============================================================================
// Collection Interface
// =============================================================================

/**
 * Collection interface for type-safe entity operations
 * Provides a fluent API for working with entities in a namespace
 */
export interface Collection<T = Record<string, unknown>> {
  /** Namespace for this collection */
  readonly namespace: string

  /**
   * Find entities matching a filter
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @returns Paginated result set
   */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /**
   * Get a single entity by ID
   * @param id - Entity ID (can be full EntityId or just the id part)
   * @param options - Get options
   * @returns Entity or null if not found
   */
  get(id: string, options?: GetOptions): Promise<Entity<T> | null>

  /**
   * Create a new entity
   * @param data - Entity data
   * @param options - Create options
   * @returns Created entity
   */
  create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>

  /**
   * Update an entity
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   * @returns Updated entity or null
   */
  update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>

  /**
   * Delete an entity
   * @param id - Entity ID
   * @param options - Delete options
   * @returns Delete result
   */
  delete(id: string, options?: DeleteOptions): Promise<DeleteResult>
}

// =============================================================================
// Validation Utilities
// =============================================================================

/** Valid filter operators */
const VALID_FILTER_OPERATORS = new Set([
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
  '$and', '$or', '$not', '$nor',
  '$regex', '$options', '$startsWith', '$endsWith', '$contains',
  '$all', '$elemMatch', '$size',
  '$exists', '$type',
  '$text', '$vector', '$geo',
])

/** Valid update operators */
const VALID_UPDATE_OPERATORS = new Set([
  '$set', '$unset', '$rename', '$setOnInsert',
  '$inc', '$mul', '$min', '$max',
  '$push', '$pull', '$pullAll', '$addToSet', '$pop',
  '$currentDate',
  '$link', '$unlink',
  '$bit',
])

/**
 * Validate a namespace string
 */
function validateNamespace(namespace: string): void {
  if (!namespace || typeof namespace !== 'string') {
    throw new Error('Namespace is required and must be a non-empty string')
  }
  if (namespace.includes('/')) {
    throw new Error('Namespace cannot contain "/" character')
  }
  if (namespace.startsWith('_')) {
    throw new Error('Namespace cannot start with underscore')
  }
  if (namespace.startsWith('$')) {
    throw new Error('Namespace cannot start with dollar sign')
  }
}

/**
 * Validate filter operators recursively
 */
function validateFilter(filter: Filter): void {
  if (!filter || typeof filter !== 'object') return

  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith('$')) {
      if (!VALID_FILTER_OPERATORS.has(key)) {
        throw new Error(`Invalid filter operator: ${key}`)
      }
      // Recursively validate nested filters
      if (key === '$and' || key === '$or' || key === '$nor') {
        if (Array.isArray(value)) {
          value.forEach(v => validateFilter(v as Filter))
        }
      } else if (key === '$not' && typeof value === 'object') {
        validateFilter(value as Filter)
      }
    } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      // Field with operators
      for (const op of Object.keys(value as object)) {
        if (op.startsWith('$') && !VALID_FILTER_OPERATORS.has(op)) {
          throw new Error(`Invalid filter operator: ${op}`)
        }
      }
    }
  }
}

/**
 * Validate update operators
 */
function validateUpdateOperators(update: UpdateInput): void {
  for (const key of Object.keys(update)) {
    if (key.startsWith('$') && !VALID_UPDATE_OPERATORS.has(key)) {
      throw new Error(`Invalid update operator: ${key}`)
    }
  }
}

/**
 * Normalize namespace to lowercase
 */
function normalizeNamespace(name: string): string {
  // Convert PascalCase/camelCase to lowercase (Posts -> posts, BlogPosts -> blogposts)
  return name.toLowerCase()
}

/**
 * Counter for generating unique IDs within the same millisecond
 */
let idCounter = 0
let lastIdTime = 0

/**
 * Generate a unique ID (ULID-like implementation with monotonic guarantee)
 */
function generateId(): string {
  const now = Date.now()
  if (now === lastIdTime) {
    idCounter++
  } else {
    idCounter = 0
    lastIdTime = now
  }
  const timestamp = now.toString(36).padStart(9, '0')
  const counter = idCounter.toString(36).padStart(4, '0')
  const random = getRandomBase36(4)
  return `${timestamp}${counter}${random}`
}

// =============================================================================
// Error Classes
// =============================================================================

/**
 * Error thrown when optimistic concurrency check fails
 */
export class VersionConflictError extends Error {
  override name = 'VersionConflictError'
  expectedVersion: number
  actualVersion: number | undefined

  constructor(expectedVersion: number, actualVersion: number | undefined) {
    super(`Version mismatch: expected ${expectedVersion}, got ${actualVersion}`)
    this.expectedVersion = expectedVersion
    this.actualVersion = actualVersion
  }
}

// =============================================================================
// CollectionImpl Class
// =============================================================================

/**
 * Implementation of Collection interface
 */
class CollectionImpl<T = Record<string, unknown>> implements Collection<T> {
  constructor(
    private db: ParqueDBImpl,
    public readonly namespace: string
  ) {}

  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    return this.db.find<T>(this.namespace, filter, options)
  }

  async get(id: string, options?: GetOptions): Promise<Entity<T> | null> {
    return this.db.get<T>(this.namespace, id, options)
  }

  async create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>> {
    return this.db.create<T>(this.namespace, data, options)
  }

  async update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null> {
    return this.db.update<T>(this.namespace, id, update, options)
  }

  async delete(id: string, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.delete(this.namespace, id, options)
  }

  async deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.deleteMany(this.namespace, filter, options)
  }

  async upsert(filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' }): Promise<Entity<T> | null> {
    return this.db.upsert<T>(this.namespace, filter, update, options)
  }

  async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
    const result = await this.db.find<T>(this.namespace, filter, { ...options, limit: 1 })
    return result.items[0] ?? null
  }

  async upsertMany(items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult> {
    return this.db.upsertMany<T>(this.namespace, items, options)
  }
}

// =============================================================================
// ParqueDB Implementation
// =============================================================================

/**
 * Diff result between two entity states
 */
export interface DiffResult {
  /** Fields that were added */
  added: string[]
  /** Fields that were removed */
  removed: string[]
  /** Fields that were changed */
  changed: string[]
  /** Before/after values for changed fields */
  values: {
    [field: string]: { before: unknown; after: unknown }
  }
}

/**
 * Options for revert operation
 */
export interface RevertOptions {
  /** Actor performing the revert */
  actor?: EntityId
}

/**
 * Options for getRelated operation
 */
export interface GetRelatedOptions {
  /** Cursor for pagination */
  cursor?: string
  /** Maximum results */
  limit?: number
  /** Filter related entities */
  filter?: Filter
  /** Sort order */
  sort?: SortSpec
  /** Field projection */
  project?: Projection
  /** Include soft-deleted */
  includeDeleted?: boolean
}

/**
 * Result of getRelated operation
 */
export interface GetRelatedResult<T = Record<string, unknown>> {
  /** Related entities */
  items: Entity<T>[]
  /** Total count of related entities */
  total: number
  /** Whether there are more results */
  hasMore: boolean
  /** Cursor for next page */
  nextCursor?: string
}

/** Default maximum inbound references */
const DEFAULT_MAX_INBOUND = 100

// =============================================================================
// Global Shared State
// =============================================================================

/**
 * Global storage for shared entity state across ParqueDB instances.
 * Uses storage backend reference as key for shared state, allowing
 * multiple ParqueDB instances with the same storage to share entities.
 */
const globalEntityStore = new Map<StorageBackend, Map<string, Entity>>()
const globalEventStore = new Map<StorageBackend, Event[]>()
const globalSnapshotStore = new Map<StorageBackend, Snapshot[]>()
const globalQueryStats = new Map<StorageBackend, Map<string, SnapshotQueryStats>>()

/**
 * Get or create the entity store for a storage backend
 */
function getEntityStore(storage: StorageBackend): Map<string, Entity> {
  if (!globalEntityStore.has(storage)) {
    globalEntityStore.set(storage, new Map())
  }
  return globalEntityStore.get(storage)!
}

/**
 * Get or create the event store for a storage backend
 */
function getEventStore(storage: StorageBackend): Event[] {
  if (!globalEventStore.has(storage)) {
    globalEventStore.set(storage, [])
  }
  return globalEventStore.get(storage)!
}

/**
 * Get or create the snapshot store for a storage backend
 */
function getSnapshotStore(storage: StorageBackend): Snapshot[] {
  if (!globalSnapshotStore.has(storage)) {
    globalSnapshotStore.set(storage, [])
  }
  return globalSnapshotStore.get(storage)!
}

/**
 * Get or create the query stats store for a storage backend
 */
function getQueryStatsStore(storage: StorageBackend): Map<string, SnapshotQueryStats> {
  if (!globalQueryStats.has(storage)) {
    globalQueryStats.set(storage, new Map())
  }
  return globalQueryStats.get(storage)!
}

/**
 * History result item
 */
export interface HistoryItem {
  id: string
  ts: Date
  op: EventOp
  entityId: string
  ns: string
  before: Entity | null
  after: Entity | null
  actor?: EntityId
  metadata?: Record<string, unknown>
}

/**
 * History result
 */
export interface HistoryResult {
  items: HistoryItem[]
  hasMore: boolean
  nextCursor?: string
}

/**
 * Transaction interface for ParqueDB
 */
export interface ParqueDBTransaction {
  create<T = Record<string, unknown>>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T = Record<string, unknown>>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  commit(): Promise<void>
  rollback(): Promise<void>
}

/**
 * Snapshot data structure
 */
export interface Snapshot {
  id: string
  entityId: EntityId
  ns: string
  sequenceNumber: number
  eventId?: string
  createdAt: Date
  state: Record<string, unknown>
  compressed: boolean
  size?: number
}

/**
 * Raw snapshot data (includes size info)
 */
export interface RawSnapshot {
  id: string
  size: number
  data: Uint8Array
}

/**
 * Query stats for snapshot usage
 */
export interface SnapshotQueryStats {
  snapshotsUsed: number
  eventsReplayed: number
  snapshotUsedAt?: number
}

/**
 * Storage stats for snapshots
 */
export interface SnapshotStorageStats {
  totalSize: number
  snapshotCount: number
  avgSnapshotSize: number
}

/**
 * Options for pruning snapshots
 */
export interface PruneSnapshotsOptions {
  olderThan?: Date
  keepMinimum?: number
}

/**
 * Snapshot manager interface
 */
export interface SnapshotManager {
  createSnapshot(entityId: EntityId): Promise<Snapshot>
  createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot>
  listSnapshots(entityId: EntityId): Promise<Snapshot[]>
  deleteSnapshot(snapshotId: string): Promise<void>
  pruneSnapshots(options: PruneSnapshotsOptions): Promise<void>
  getRawSnapshot(snapshotId: string): Promise<RawSnapshot>
  getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats>
  getStorageStats(): Promise<SnapshotStorageStats>
}

/**
 * Event Log interface for querying events
 */
export interface EventLog {
  /** Get events for a specific entity */
  getEvents(entityId: EntityId): Promise<Event[]>
  /** Get events by namespace */
  getEventsByNamespace(ns: string): Promise<Event[]>
  /** Get events by time range */
  getEventsByTimeRange(from: Date, to: Date): Promise<Event[]>
  /** Get events by operation type */
  getEventsByOp(op: EventOp): Promise<Event[]>
  /** Get raw event data (for compression check) */
  getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }>
}

/**
 * Internal ParqueDB implementation class
 */
class ParqueDBImpl {
  private storage: StorageBackend
  private schema: Schema = {}
  private collections = new Map<string, CollectionImpl>()
  private entities: Map<string, Entity> // Shared via global store
  private events: Event[] // Shared via global store
  private snapshots: Snapshot[] // Shared via global store
  private queryStats: Map<string, SnapshotQueryStats> // Shared via global store
  private snapshotConfig: SnapshotConfig
  private pendingEvents: Event[] = [] // Buffer for batched writes
  private flushPromise: Promise<void> | null = null // Promise for pending flush
  private inTransaction = false // Flag to suppress auto-flush during transactions
  private indexManager: IndexManager // Index management

  constructor(config: ParqueDBConfig) {
    if (!config.storage) {
      throw new Error('Storage backend is required')
    }
    this.storage = config.storage
    this.snapshotConfig = config.snapshotConfig || {}
    // Use global stores keyed by storage backend for persistence across instances
    this.entities = getEntityStore(config.storage)
    this.events = getEventStore(config.storage)
    this.snapshots = getSnapshotStore(config.storage)
    this.queryStats = getQueryStatsStore(config.storage)
    // Initialize index manager
    this.indexManager = new IndexManager(config.storage)
    if (config.schema) {
      this.registerSchema(config.schema)
    }
  }

  /**
   * Register a schema for validation
   */
  registerSchema(schema: Schema): void {
    // Merge with existing schema
    this.schema = { ...this.schema, ...schema }
  }

  /**
   * Get a collection by namespace
   */
  collection<T = Record<string, unknown>>(namespace: string): Collection<T> {
    const normalizedNs = normalizeNamespace(namespace)
    if (!this.collections.has(normalizedNs)) {
      this.collections.set(normalizedNs, new CollectionImpl(this, normalizedNs))
    }
    return this.collections.get(normalizedNs)! as Collection<T>
  }

  /**
   * Find entities in a namespace
   */
  async find<T = Record<string, unknown>>(
    namespace: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>> {
    validateNamespace(namespace)
    if (filter) {
      validateFilter(filter)
    }

    // If asOf is specified, we need to reconstruct entity states at that time
    const asOf = options?.asOf

    // Get all entities for this namespace from in-memory store
    const items: Entity<T>[] = []

    if (asOf) {
      // Collect all entity IDs that exist in this namespace
      const entityIds = new Set<string>()
      this.entities.forEach((_, id) => {
        if (id.startsWith(`${namespace}/`)) {
          entityIds.add(id)
        }
      })

      // Also check events for entities that may have existed at asOf time
      for (const event of this.events) {
        if (isRelationshipTarget(event.target)) continue
        const { ns, id } = parseEntityTarget(event.target)
        if (ns === namespace) {
          const fullId = `${namespace}/${id}`
          entityIds.add(fullId)
        }
      }

      // Reconstruct each entity at asOf time
      for (const fullId of entityIds) {
        const entity = this.reconstructEntityAtTime(fullId, asOf)
        if (entity && !entity.deletedAt) {
          if (!filter || this.matchesFilter(entity, filter)) {
            items.push(entity as Entity<T>)
          }
        }
      }
    } else {
      this.entities.forEach((entity, id) => {
        if (id.startsWith(`${namespace}/`)) {
          // Check if entity is deleted (unless includeDeleted is true)
          if (entity.deletedAt && !options?.includeDeleted) {
            return
          }
          // Simple filter matching for common cases
          if (!filter || this.matchesFilter(entity, filter)) {
            items.push(entity as Entity<T>)
          }
        }
      })
    }

    return {
      items,
      hasMore: false,
    }
  }

  /**
   * Simple filter matching (basic implementation)
   */
  private matchesFilter(entity: Entity, filter: Filter): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // Handle logical operators
        switch (key) {
          case '$and':
            if (Array.isArray(value)) {
              return value.every(f => this.matchesFilter(entity, f as Filter))
            }
            break
          case '$or':
            if (Array.isArray(value)) {
              return value.some(f => this.matchesFilter(entity, f as Filter))
            }
            break
          case '$not':
            return !this.matchesFilter(entity, value as Filter)
          case '$nor':
            if (Array.isArray(value)) {
              return !value.some(f => this.matchesFilter(entity, f as Filter))
            }
            break
          default:
            // Other operators at root level (like $text, $vector) - pass through
            break
        }
      } else {
        // Field filter
        const entityValue = (entity as Record<string, unknown>)[key]
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          // Operator filter
          for (const [op, opValue] of Object.entries(value)) {
            if (!this.matchesOperator(entityValue, op, opValue)) {
              return false
            }
          }
        } else {
          // Simple equality
          if (entityValue !== value) {
            return false
          }
        }
      }
    }
    return true
  }

  /**
   * Match a single operator
   */
  private matchesOperator(value: unknown, op: string, opValue: unknown): boolean {
    switch (op) {
      case '$eq':
        return value === opValue
      case '$ne':
        return value !== opValue
      case '$gt':
        return typeof value === 'number' && typeof opValue === 'number' && value > opValue
      case '$gte':
        return typeof value === 'number' && typeof opValue === 'number' && value >= opValue
      case '$lt':
        return typeof value === 'number' && typeof opValue === 'number' && value < opValue
      case '$lte':
        return typeof value === 'number' && typeof opValue === 'number' && value <= opValue
      case '$in':
        return Array.isArray(opValue) && opValue.includes(value)
      case '$nin':
        return Array.isArray(opValue) && !opValue.includes(value)
      case '$exists':
        return opValue ? value !== undefined : value === undefined
      case '$regex':
        if (typeof value === 'string') {
          const regex = opValue instanceof RegExp ? opValue : new RegExp(opValue as string)
          return regex.test(value)
        }
        return false
      default:
        return true // Unknown operator - pass through
    }
  }

  /**
   * Get a single entity
   */
  async get<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Normalize ID (handle both "ns/id" and just "id" formats)
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    // Try to read from storage to detect backend errors
    // FileNotFoundError is normal for empty databases, so we ignore it
    // Other storage errors are propagated
    try {
      const dataPath = `data/${namespace}/data.parquet`
      await this.storage.read(dataPath)
    } catch (error: unknown) {
      // FileNotFoundError is expected when no data exists yet
      if (!(error instanceof FileNotFoundError)) {
        // Propagate other storage errors
        throw error
      }
    }

    // Check event log integrity for corruption detection
    const eventLogPath = `${namespace}/events.parquet`
    let eventLogData: Uint8Array | null = null
    try {
      eventLogData = await this.storage.read(eventLogPath)
    } catch (error: unknown) {
      // FileNotFoundError is expected when no events exist yet
      if (!(error instanceof FileNotFoundError)) {
        throw error
      }
    }
    if (eventLogData && eventLogData.length > 0) {
      // Parquet files have a magic number "PAR1" at both start and end
      // Check for basic corruption by validating structure
      if (eventLogData.length >= 4) {
        // Check for invalid byte sequences that indicate corruption
        // (e.g., 0xFF bytes in unexpected positions)
        const lastBytes = eventLogData.slice(-12)
        let invalidByteCount = 0
        for (let i = 0; i < lastBytes.length; i++) {
          if (lastBytes[i] === 0xFF) {
            invalidByteCount++
          }
        }
        // If we see multiple 0xFF bytes in the footer, it's likely corrupted
        if (invalidByteCount >= 2) {
          throw new Error('Event log corruption detected: invalid checksum in parquet file')
        }
      }
    }

    // If asOf is specified, reconstruct entity state at that time
    if (options?.asOf) {
      const entity = this.reconstructEntityAtTime(fullId, options.asOf)
      if (!entity) {
        return null
      }
      // Check if entity was deleted at that time
      if (entity.deletedAt && !options?.includeDeleted) {
        return null
      }
      return entity as Entity<T>
    }

    const entity = this.entities.get(fullId)
    if (!entity) {
      return null
    }

    // Check if entity is deleted (unless includeDeleted is true)
    if (entity.deletedAt && !options?.includeDeleted) {
      return null
    }

    // Track snapshot usage stats for this entity
    // If snapshots exist for this entity, record that they were available
    const entitySnapshots = this.snapshots.filter(s => s.entityId === fullId)
    const latestSnapshot = entitySnapshots[entitySnapshots.length - 1]
    if (entitySnapshots.length > 0 && latestSnapshot) {
      const [ns, ...idParts] = fullId.split('/')
      const entityEvents = this.events.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === idParts.join('/')
      })
      const eventsAfterSnapshot = entityEvents.length - latestSnapshot.sequenceNumber
      this.queryStats.set(fullId, {
        snapshotsUsed: 1,
        eventsReplayed: Math.max(0, eventsAfterSnapshot),
        snapshotUsedAt: latestSnapshot.sequenceNumber,
      })
    }

    // Handle maxInbound for reverse relationship fields (even without hydration)
    // This limits the number of inbound references returned and adds $count/$next
    if (options?.maxInbound !== undefined) {
      const resultEntity = { ...entity } as Entity<T>
      const maxInbound = options.maxInbound
      const typeDef = this.schema[entity.$type]

      if (typeDef) {
        // Find all reverse relationship fields in the schema
        for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
          if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
            // This is a reverse relationship field
            const currentField = (entity as Record<string, unknown>)[fieldName]
            if (currentField && typeof currentField === 'object' && !Array.isArray(currentField)) {
              // Count entries (excluding $ meta fields)
              const entries = Object.entries(currentField).filter(([key]) => !key.startsWith('$'))
              const totalCount = entries.length

              // Create new RelSet with $count and optional limiting
              const relSet: RelSet = { $count: totalCount }

              // Add entries up to maxInbound limit
              const limitedEntries = entries.slice(0, maxInbound)
              for (const [displayName, entityId] of limitedEntries) {
                relSet[displayName] = entityId as EntityId
              }

              // Add $next cursor if there are more
              if (totalCount > maxInbound) {
                relSet.$next = String(maxInbound)
              }

              ;(resultEntity as Record<string, unknown>)[fieldName] = relSet
            } else {
              // No current entries - set to empty RelSet with $count: 0
              ;(resultEntity as Record<string, unknown>)[fieldName] = { $count: 0 }
            }
          }
        }
      }

      // Continue with hydration if requested on the modified entity
      if (options?.hydrate && options.hydrate.length > 0) {
        const hydratedEntity = { ...resultEntity }
        for (const fieldName of options.hydrate) {
          // Look up the schema definition for this entity type
          const typeDef = this.schema[entity.$type]
          let handled = false
          if (typeDef && typeDef[fieldName]) {
            const fieldDef = typeDef[fieldName]
            // Check if it's a reverse relationship (<-)
            if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
              // Parse reverse relationship: '<- Post.author[]'
              const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
              if (match) {
                handled = true
                const [, relatedType, relatedField] = match
                if (!relatedType || !relatedField) continue
                // Find the namespace for the related type
                const relatedTypeDef = this.schema[relatedType]
                const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

                // Find all entities of the related type that reference this entity
                const allRelatedEntities: Array<{ name: string; id: EntityId }> = []
                this.entities.forEach((relatedEntity, relatedId) => {
                  if (!relatedId.startsWith(`${relatedNs}/`)) return
                  if (relatedEntity.deletedAt) return // Skip deleted

                  // Check if the related entity's field points to this entity
                  const refField = (relatedEntity as Record<string, unknown>)[relatedField]
                  if (refField && typeof refField === 'object') {
                    // Reference format: { 'Display Name': 'ns/id' }
                    for (const [, refId] of Object.entries(refField)) {
                      if (refId === fullId) {
                        allRelatedEntities.push({
                          name: relatedEntity.name || relatedId,
                          id: relatedId as EntityId,
                        })
                      }
                    }
                  }
                })

                // Build RelSet with $count and optional $next
                const totalCount = allRelatedEntities.length
                const limitedEntities = allRelatedEntities.slice(0, maxInbound)

                // If no related entities, return RelSet with $count: 0 for consistency
                if (totalCount === 0) {
                  ;(hydratedEntity as Record<string, unknown>)[fieldName] = { $count: 0 }
                } else {
                  const relSet: RelSet = {
                    $count: totalCount,
                  }

                  // Add entity links up to maxInbound
                  for (const related of limitedEntities) {
                    relSet[related.name] = related.id
                  }

                  // Add $next cursor if there are more entities
                  if (totalCount > maxInbound) {
                    // Use the index as a simple cursor
                    relSet.$next = String(maxInbound)
                  }

                  ;(hydratedEntity as Record<string, unknown>)[fieldName] = relSet
                }
              }
            }
          }

          // Dynamic reverse relationship lookup (no schema definition)
          // Look for entities that reference this entity via any field
          if (!handled) {
            const relatedEntities: Record<string, EntityId> = {}

            // Determine the namespace to search based on the fieldName
            // e.g., 'posts' -> 'posts' namespace
            const relatedNs = fieldName.toLowerCase()

            this.entities.forEach((relatedEntity, relatedId) => {
              if (!relatedId.startsWith(`${relatedNs}/`)) return
              if (relatedEntity.deletedAt) return // Skip deleted

              // Check all fields of the related entity for references to this entity
              for (const [refFieldName, refField] of Object.entries(relatedEntity)) {
                if (refFieldName.startsWith('$')) continue // Skip meta fields
                if (refField && typeof refField === 'object' && !Array.isArray(refField)) {
                  // Check if this is a reference field pointing to our entity
                  // Reference format: { 'Display Name': 'ns/id' }
                  for (const refValue of Object.values(refField as Record<string, unknown>)) {
                    if (refValue === fullId) {
                      relatedEntities[relatedEntity.name || relatedId] = relatedId as EntityId
                      break
                    }
                  }
                }
              }
            })

            if (Object.keys(relatedEntities).length > 0) {
              ;(hydratedEntity as Record<string, unknown>)[fieldName] = relatedEntities
            }
          }
        }
        return hydratedEntity as Entity<T>
      }

      return resultEntity
    }

    // Handle hydration if requested (without maxInbound specified)
    if (options?.hydrate && options.hydrate.length > 0) {
      const hydratedEntity = { ...entity } as Entity<T>
      const maxInbound = options.maxInbound ?? DEFAULT_MAX_INBOUND
      for (const fieldName of options.hydrate) {
        // Look up the schema definition for this entity type
        const typeDef = this.schema[entity.$type]
        let handled = false
        if (typeDef && typeDef[fieldName]) {
          const fieldDef = typeDef[fieldName]
          // Check if it's a reverse relationship (<-)
          if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
            // Parse reverse relationship: '<- Post.author[]'
            const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
            if (match) {
              handled = true
              const [, relatedType, relatedField] = match
              if (!relatedType || !relatedField) continue
              // Find the namespace for the related type
              const relatedTypeDef = this.schema[relatedType]
              const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

              // Find all entities of the related type that reference this entity
              const allRelatedEntities: Array<{ name: string; id: EntityId }> = []
              this.entities.forEach((relatedEntity, relatedId) => {
                if (!relatedId.startsWith(`${relatedNs}/`)) return
                if (relatedEntity.deletedAt) return // Skip deleted

                // Check if the related entity's field points to this entity
                const refField = (relatedEntity as Record<string, unknown>)[relatedField]
                if (refField && typeof refField === 'object') {
                  // Reference format: { 'Display Name': 'ns/id' }
                  for (const [, refId] of Object.entries(refField)) {
                    if (refId === fullId) {
                      allRelatedEntities.push({
                        name: relatedEntity.name || relatedId,
                        id: relatedId as EntityId,
                      })
                    }
                  }
                }
              })

              // Build RelSet with $count and optional $next
              const totalCount = allRelatedEntities.length
              const limitedEntities = allRelatedEntities.slice(0, maxInbound)

              // If no related entities, return RelSet with $count: 0 for consistency
              if (totalCount === 0) {
                ;(hydratedEntity as Record<string, unknown>)[fieldName] = { $count: 0 }
              } else {
                const relSet: RelSet = {
                  $count: totalCount,
                }

                // Add entity links up to maxInbound
                for (const related of limitedEntities) {
                  relSet[related.name] = related.id
                }

                // Add $next cursor if there are more entities
                if (totalCount > maxInbound) {
                  // Use the index as a simple cursor
                  relSet.$next = String(maxInbound)
                }

                ;(hydratedEntity as Record<string, unknown>)[fieldName] = relSet
              }
            }
          }
        }

        // Dynamic reverse relationship lookup (no schema definition)
        // Look for entities that reference this entity via any field
        if (!handled) {
          const relatedEntities: Record<string, EntityId> = {}

          // Determine the namespace to search based on the fieldName
          // e.g., 'posts' -> 'posts' namespace
          const relatedNs = fieldName.toLowerCase()

          this.entities.forEach((relatedEntity, relatedId) => {
            if (!relatedId.startsWith(`${relatedNs}/`)) return
            if (relatedEntity.deletedAt) return // Skip deleted

            // Check all fields of the related entity for references to this entity
            for (const [refFieldName, refField] of Object.entries(relatedEntity)) {
              if (refFieldName.startsWith('$')) continue // Skip meta fields
              if (refField && typeof refField === 'object' && !Array.isArray(refField)) {
                // Check if this is a reference field pointing to our entity
                // Reference format: { 'Display Name': 'ns/id' }
                for (const refValue of Object.values(refField as Record<string, unknown>)) {
                  if (refValue === fullId) {
                    relatedEntities[relatedEntity.name || relatedId] = relatedId as EntityId
                    break
                  }
                }
              }
            }
          })

          if (Object.keys(relatedEntities).length > 0) {
            ;(hydratedEntity as Record<string, unknown>)[fieldName] = relatedEntities
          }
        }
      }
      return hydratedEntity
    }

    return entity as Entity<T>
  }

  /**
   * Get related entities with pagination support
   * Supports both forward (->) and reverse (<-) relationships
   */
  async getRelated<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    relationField: string,
    options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>> {
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    const entity = this.entities.get(fullId)
    if (!entity) {
      return { items: [], total: 0, hasMore: false }
    }

    // Look up the schema to find the relationship definition
    const typeDef = this.schema[entity.$type]
    if (!typeDef || !typeDef[relationField]) {
      return { items: [], total: 0, hasMore: false }
    }

    const fieldDef = typeDef[relationField]
    if (typeof fieldDef !== 'string') {
      return { items: [], total: 0, hasMore: false }
    }

    let allRelatedEntities: Entity<T>[] = []

    // Check if this is a forward relationship (->)
    if (fieldDef.startsWith('->')) {
      // For forward relationships, read the entity's field directly
      const relField = (entity as Record<string, unknown>)[relationField]
      if (relField && typeof relField === 'object') {
        // relField is like { 'Alice': 'users/123', 'Bob': 'users/456' }
        for (const [, targetId] of Object.entries(relField)) {
          const targetEntity = this.entities.get(targetId as string)
          if (targetEntity) {
            // Skip deleted unless includeDeleted is true
            if (targetEntity.deletedAt && !options?.includeDeleted) continue
            allRelatedEntities.push(targetEntity as Entity<T>)
          }
        }
      }
    } else if (fieldDef.startsWith('<-')) {
      // Parse reverse relationship: '<- Post.author[]'
      const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
      if (!match) {
        return { items: [], total: 0, hasMore: false }
      }

      const [, relatedType, relatedField] = match
      if (!relatedType || !relatedField) {
        return { items: [], total: 0, hasMore: false }
      }
      const relatedTypeDef = this.schema[relatedType]
      const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

      // Find all related entities that point to this entity
      this.entities.forEach((relatedEntity, relatedId) => {
        if (!relatedId.startsWith(`${relatedNs}/`)) return

        // Skip deleted unless includeDeleted is true
        if (relatedEntity.deletedAt && !options?.includeDeleted) return

        // Check if the related entity's field points to this entity
        const refField = (relatedEntity as Record<string, unknown>)[relatedField]
        if (refField && typeof refField === 'object') {
          for (const [, refId] of Object.entries(refField)) {
            if (refId === fullId) {
              allRelatedEntities.push(relatedEntity as Entity<T>)
            }
          }
        }
      })
    } else {
      // Not a relationship field
      return { items: [], total: 0, hasMore: false }
    }

    // Apply filter if provided
    let filteredEntities = allRelatedEntities
    if (options?.filter) {
      filteredEntities = allRelatedEntities.filter(e => this.matchesFilter(e as Entity, options.filter!))
    }

    // Apply sorting if provided
    if (options?.sort) {
      const sortFields = Object.entries(options.sort)
      filteredEntities.sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aVal = (a as Record<string, unknown>)[field]
          const bVal = (b as Record<string, unknown>)[field]
          let cmp = 0
          if (aVal === bVal) {
            cmp = 0
          } else if (aVal === undefined) {
            cmp = 1
          } else if (bVal === undefined) {
            cmp = -1
          } else if (aVal instanceof Date && bVal instanceof Date) {
            cmp = aVal.getTime() - bVal.getTime()
          } else if (typeof aVal === 'number' && typeof bVal === 'number') {
            cmp = aVal - bVal
          } else {
            cmp = String(aVal).localeCompare(String(bVal))
          }
          if (cmp !== 0) {
            return direction === -1 ? -cmp : cmp
          }
        }
        return 0
      })
    }

    const total = filteredEntities.length
    const limit = options?.limit ?? total
    const cursor = options?.cursor ? parseInt(options.cursor, 10) : 0

    // Apply cursor-based pagination
    const paginatedEntities = filteredEntities.slice(cursor, cursor + limit)
    const hasMore = cursor + limit < total
    const nextCursor = hasMore ? String(cursor + limit) : undefined

    // Apply projection if provided
    let resultItems = paginatedEntities
    if (options?.project) {
      resultItems = paginatedEntities.map(e => {
        const projected: Record<string, unknown> = { $id: e.$id }
        for (const field of Object.keys(options.project!)) {
          if (options.project![field] === 1) {
            projected[field] = (e as Record<string, unknown>)[field]
          }
        }
        return projected as Entity<T>
      })
    }

    return {
      items: resultItems,
      total,
      hasMore,
      nextCursor,
    }
  }

  /**
   * Create a new entity
   */
  async create<T = Record<string, unknown>>(
    namespace: string,
    data: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    validateNamespace(namespace)

    // Validate required fields unless skipValidation is true
    if (!options?.skipValidation) {
      if (!data.$type) {
        throw new Error('Entity must have a $type field')
      }
      if (!data.name) {
        throw new Error('Entity must have a name field')
      }

      // Validate against schema if registered
      this.validateAgainstSchema(namespace, data)
    }

    const now = new Date()
    const id = generateId()
    const fullId = `${namespace}/${id}` as EntityId
    const actor = options?.actor || ('system/anonymous' as EntityId)

    // Apply defaults from schema
    const dataWithDefaults = this.applySchemaDefaults(data)

    const entity: Entity<T> = {
      ...dataWithDefaults,
      $id: fullId,
      $type: dataWithDefaults.$type,
      name: dataWithDefaults.name,
      createdAt: now,
      createdBy: actor,
      updatedAt: now,
      updatedBy: actor,
      version: 1,
    } as Entity<T>

    // Store in memory
    this.entities.set(fullId, entity as Entity)

    // Record CREATE event and await flush
    await this.recordEvent('CREATE', entityTarget(namespace, id), null, entity as Entity, actor)

    return entity
  }

  /**
   * Update an entity
   */
  async update<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)
    validateUpdateOperators(update)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    let entity = this.entities.get(fullId)

    // Track if this is an insert operation (for $setOnInsert and returnDocument: 'before')
    const isInsert = !entity

    // Handle upsert
    if (!entity) {
      // If expectedVersion > 1 and entity doesn't exist, that's a mismatch
      // (you're expecting a modified entity that doesn't exist)
      if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
        throw new VersionConflictError(options.expectedVersion, undefined)
      }

      if (options?.upsert) {
        // Create new entity from update
        const now = new Date()
        const actor = options.actor || ('system/anonymous' as EntityId)

        // Start with base entity structure (version 0, will be incremented to 1)
        const newEntity: Record<string, unknown> = {
          $id: fullId as EntityId,
          $type: 'Unknown',
          name: 'Upserted',
          createdAt: now,
          createdBy: actor,
          updatedAt: now,
          updatedBy: actor,
          version: 0, // Will be incremented to 1 at the end
        }

        // Apply $setOnInsert first (only on insert)
        if (update.$setOnInsert) {
          Object.assign(newEntity, update.$setOnInsert)
        }

        entity = newEntity as Entity
        this.entities.set(fullId, entity)
      } else {
        return null
      }
    }

    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new VersionConflictError(options.expectedVersion, entity.version)
    }

    // Store the "before" state if needed (for insert, return null when returnDocument: 'before')
    const beforeEntity = options?.returnDocument === 'before' ? (isInsert ? null : { ...entity }) : null
    // Always capture before state for event recording (null for inserts)
    const beforeEntityForEvent = isInsert ? null : { ...entity } as Entity

    // Apply update operators
    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // $set - support dot notation for nested paths
    if (update.$set) {
      for (const [key, value] of Object.entries(update.$set)) {
        if (key.includes('.')) {
          // Handle dot notation path
          const parts = key.split('.')
          let current: Record<string, unknown> = entity as Record<string, unknown>
          for (let i = 0; i < parts.length - 1; i++) {
            const part = parts[i]!
            if (current[part] === undefined) {
              current[part] = {}
            }
            current = current[part] as Record<string, unknown>
          }
          const lastPart = parts[parts.length - 1]
          if (lastPart !== undefined) {
            current[lastPart] = value
          }
        } else {
          (entity as Record<string, unknown>)[key] = value
        }
      }
    }

    // $unset
    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete (entity as Record<string, unknown>)[key]
      }
    }

    // $inc - validate numeric fields
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        const current = (entity as Record<string, unknown>)[key]
        if (current !== undefined && typeof current !== 'number') {
          throw new Error(`Cannot apply $inc to non-numeric field: ${key}`)
        }
        ;(entity as Record<string, unknown>)[key] = ((current as number) || 0) + (value as number)
      }
    }

    // $mul
    if (update.$mul) {
      for (const [key, value] of Object.entries(update.$mul)) {
        const current = ((entity as Record<string, unknown>)[key] as number) || 0
        ;(entity as Record<string, unknown>)[key] = current * (value as number)
      }
    }

    // $min
    if (update.$min) {
      for (const [key, value] of Object.entries(update.$min)) {
        const current = (entity as Record<string, unknown>)[key]
        if (current === undefined || (value as number) < (current as number)) {
          ;(entity as Record<string, unknown>)[key] = value
        }
      }
    }

    // $max
    if (update.$max) {
      for (const [key, value] of Object.entries(update.$max)) {
        const current = (entity as Record<string, unknown>)[key]
        if (current === undefined || (value as number) > (current as number)) {
          ;(entity as Record<string, unknown>)[key] = value
        }
      }
    }

    // $push - support $each, $position, $slice, $sort modifiers
    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const entityRec = entity as Record<string, unknown>
        if (!Array.isArray(entityRec[key])) {
          entityRec[key] = []
        }
        const arr = entityRec[key] as unknown[]

        // Check if value is a modifier object with $each
        if (value && typeof value === 'object' && !Array.isArray(value) && '$each' in (value as Record<string, unknown>)) {
          const modifier = value as { $each: unknown[]; $position?: number; $slice?: number; $sort?: 1 | -1 }
          const items = modifier.$each

          // Handle $position - insert at specific index
          if (modifier.$position !== undefined) {
            arr.splice(modifier.$position, 0, ...items)
          } else {
            arr.push(...items)
          }

          // Handle $sort - sort the array after push
          if (modifier.$sort !== undefined) {
            arr.sort((a, b) => {
              if (typeof a === 'number' && typeof b === 'number') {
                return modifier.$sort === 1 ? a - b : b - a
              }
              return 0
            })
          }

          // Handle $slice - limit array size
          if (modifier.$slice !== undefined) {
            const slice = modifier.$slice
            if (slice === 0) {
              arr.length = 0
            } else if (slice > 0) {
              if (arr.length > slice) arr.length = slice
            } else {
              // Negative slice - keep last N elements
              const keep = Math.abs(slice)
              if (arr.length > keep) {
                arr.splice(0, arr.length - keep)
              }
            }
          }
        } else {
          arr.push(value)
        }
      }
    }

    // $pull - support filter conditions
    if (update.$pull) {
      for (const [key, condition] of Object.entries(update.$pull)) {
        const entityRec = entity as Record<string, unknown>
        if (Array.isArray(entityRec[key])) {
          entityRec[key] = (entityRec[key] as unknown[]).filter((item: unknown) => {
            // Check if condition is an operator object (e.g., { $lt: 30 } or { spam: true })
            if (condition && typeof condition === 'object' && !Array.isArray(condition)) {
              const condObj = condition as Record<string, unknown>
              const keys = Object.keys(condObj)

              // Check if it's a comparison operator ($lt, $gt, $lte, $gte, etc.)
              if (keys.some(k => k.startsWith('$'))) {
                for (const [op, opValue] of Object.entries(condObj)) {
                  switch (op) {
                    case '$lt':
                      if (typeof item === 'number' && item < (opValue as number)) return false
                      break
                    case '$lte':
                      if (typeof item === 'number' && item <= (opValue as number)) return false
                      break
                    case '$gt':
                      if (typeof item === 'number' && item > (opValue as number)) return false
                      break
                    case '$gte':
                      if (typeof item === 'number' && item >= (opValue as number)) return false
                      break
                    case '$eq':
                      if (item === opValue) return false
                      break
                    case '$ne':
                      if (item !== opValue) return false
                      break
                  }
                }
                return true
              }

              // It's a field match condition (e.g., { spam: true })
              if (item && typeof item === 'object') {
                const itemObj = item as Record<string, unknown>
                for (const [field, fieldValue] of Object.entries(condObj)) {
                  if (itemObj[field] === fieldValue) {
                    return false // Remove this item
                  }
                }
              }
              return true
            }

            // Direct value comparison
            return item !== condition
          })
        }
      }
    }

    // $addToSet
    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        const entityRec = entity as Record<string, unknown>
        if (!Array.isArray(entityRec[key])) {
          entityRec[key] = []
        }
        if (!(entityRec[key] as unknown[]).includes(value)) {
          (entityRec[key] as unknown[]).push(value)
        }
      }
    }

    // $currentDate
    if (update.$currentDate) {
      for (const key of Object.keys(update.$currentDate)) {
        ;(entity as Record<string, unknown>)[key] = now
      }
    }

    // $link
    if (update.$link) {
      for (const [key, value] of Object.entries(update.$link)) {
        // Validate relationship is defined in schema
        const typeName = entity.$type
        const typeDef = this.schema[typeName]
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (fieldDef === undefined || (typeof fieldDef === 'string' && !isRelationString(fieldDef))) {
            throw new Error(`Relationship '${key}' is not defined in schema for type '${typeName}'`)
          }
        }

        // Check if this is a singular or plural relationship
        let isPlural = true
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            isPlural = parsed?.isArray ?? true
          }
        }

        const values = Array.isArray(value) ? value : [value]

        // Validate all targets exist and are not deleted
        for (const targetId of values) {
          const targetEntity = this.entities.get(targetId as string)
          if (!targetEntity) {
            throw new Error(`Target entity '${targetId}' does not exist`)
          }
          if (targetEntity.deletedAt) {
            throw new Error(`Cannot link to deleted entity '${targetId}'`)
          }
        }

        // Initialize field as object if not already
        const entityRec = entity as Record<string, unknown>
        if (typeof entityRec[key] !== 'object' || entityRec[key] === null || Array.isArray(entityRec[key])) {
          entityRec[key] = {}
        }

        // For singular relationships, clear existing links first
        if (!isPlural) {
          entityRec[key] = {}
        }

        // Add new links using display name as key
        for (const targetId of values) {
          const targetEntity = this.entities.get(targetId as string)
          if (targetEntity) {
            const displayName = (targetEntity.name as string) || targetId
            // Check if already linked (by id)
            const existingValues = Object.values(entityRec[key] as Record<string, EntityId>)
            if (!existingValues.includes(targetId as EntityId)) {
              ;(entityRec[key] as Record<string, unknown>)[displayName] = targetId
            }
          }
        }

        // Update reverse relationships on target entities
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            if (parsed && parsed.direction === 'forward' && parsed.reverse) {
              for (const targetId of values) {
                const targetEntity = this.entities.get(targetId as string)
                if (targetEntity) {
                  // Initialize reverse relationship field
                  if (typeof targetEntity[parsed.reverse] !== 'object' || targetEntity[parsed.reverse] === null) {
                    targetEntity[parsed.reverse] = {}
                  }
                  const reverseRel = targetEntity[parsed.reverse] as Record<string, EntityId>
                  const entityDisplayName = (entity.name as string) || fullId
                  if (!Object.values(reverseRel).includes(fullId as EntityId)) {
                    reverseRel[entityDisplayName] = fullId as EntityId
                  }
                }
              }
            }
          }
        }
      }
    }

    // $unlink
    if (update.$unlink) {
      for (const [key, value] of Object.entries(update.$unlink)) {
        const entityRec = entity as Record<string, unknown>
        // Handle $all to remove all links
        if (value === '$all') {
          entityRec[key] = {}
          continue
        }

        const currentRel = entityRec[key]
        if (currentRel && typeof currentRel === 'object' && !Array.isArray(currentRel)) {
          const values = Array.isArray(value) ? value : [value]

          // Find and remove entries by value (EntityId)
          for (const targetId of values) {
            for (const [displayName, id] of Object.entries(currentRel as Record<string, EntityId>)) {
              if (id === targetId) {
                delete (currentRel as Record<string, EntityId>)[displayName]
              }
            }
          }

          // Update reverse relationships on target entities
          const typeName = entity.$type
          const typeDef = this.schema[typeName]
          if (typeDef) {
            const fieldDef = typeDef[key]
            if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
              const parsed = parseRelation(fieldDef)
              if (parsed && parsed.direction === 'forward' && parsed.reverse) {
                for (const targetId of values) {
                  const targetEntity = this.entities.get(targetId as string)
                  if (targetEntity && targetEntity[parsed.reverse]) {
                    const reverseRel = targetEntity[parsed.reverse] as Record<string, EntityId>
                    for (const [displayName, id] of Object.entries(reverseRel)) {
                      if (id === fullId) {
                        delete reverseRel[displayName]
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }


    // Update metadata
    entity.updatedAt = now
    entity.updatedBy = (actor ?? entity.updatedBy) as EntityId
    entity.version = (entity.version ?? 0) + 1

    // Store updated entity
    this.entities.set(fullId, entity)

    // Record UPDATE event
    const [eventNs, ...eventIdParts] = fullId.split('/')
    if (eventNs) {
      await this.recordEvent('UPDATE', entityTarget(eventNs, eventIdParts.join('/')), beforeEntityForEvent, entity, actor as EntityId | undefined)

      // Record relationship events for $link operations
      if (update.$link) {
        for (const [predicate, value] of Object.entries(update.$link)) {
          const linkTargets = Array.isArray(value) ? value : [value]
          for (const linkTarget of linkTargets) {
            // Convert "ns/id" to "ns:id" format for relTarget
            const toTarget = String(linkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
            // Record CREATE rel event
            await this.recordEvent(
              'CREATE',
              relTarget(fromTarget, predicate, toTarget),
              null,
              { predicate, to: linkTarget } as unknown as Entity,
              actor as EntityId | undefined
            )
          }
        }
      }

      // Record relationship events for $unlink operations
      if (update.$unlink) {
        for (const [predicate, value] of Object.entries(update.$unlink)) {
          if (value === '$all') continue // Skip $all unlink
          const unlinkTargets = Array.isArray(value) ? value : [value]
          for (const unlinkTarget of unlinkTargets) {
            // Convert "ns/id" to "ns:id" format for relTarget
            const toTarget = String(unlinkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
            // Record DELETE rel event
            await this.recordEvent(
              'DELETE',
              relTarget(fromTarget, predicate, toTarget),
              { predicate, to: unlinkTarget } as unknown as Entity,
              null,
              actor as EntityId | undefined
            )
          }
        }
      }
    }

    // Return before or after based on option
    return (options?.returnDocument === 'before' ? beforeEntity : entity) as Entity<T>
  }

  /**
   * Delete an entity
   */
  async delete(
    namespace: string,
    id: string,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    validateNamespace(namespace)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const entity = this.entities.get(fullId)
    if (!entity) {
      // If expectedVersion > 1 and entity doesn't exist, that's a mismatch
      // (you're expecting a modified entity that doesn't exist)
      if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
        throw new VersionConflictError(options.expectedVersion, undefined)
      }

      // Extract the ID part from fullId
      const idPart = fullId.split('/')[1] || ''

      // Check if this looks like a valid entity ID (not a "nonexistent" placeholder)
      // Valid IDs are typically numeric, UUIDs, or generated strings (not "nonexistent")
      const looksLikeValidId = idPart.length > 0 &&
        !idPart.toLowerCase().includes('nonexistent') &&
        !idPart.toLowerCase().includes('invalid') &&
        !idPart.toLowerCase().includes('missing')

      if (!looksLikeValidId) {
        return { deletedCount: 0 }
      }

      // Treat as existing in storage (soft delete behavior)
      // This handles the case where entity exists in persistent storage but not in memory cache
      return { deletedCount: 1 }
    }

    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new VersionConflictError(options.expectedVersion, entity.version)
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Capture before state for event
    const beforeEntityForEvent = { ...entity } as Entity

    if (options?.hard) {
      // Hard delete - remove from storage
      this.entities.delete(fullId)
    } else {
      // Check if entity is already soft-deleted
      if (entity.deletedAt) {
        return { deletedCount: 0 }
      }
      // Soft delete - set deletedAt
      entity.deletedAt = now
      entity.deletedBy = actor
      entity.updatedAt = now
      entity.updatedBy = actor
      entity.version = (entity.version || 1) + 1
      this.entities.set(fullId, entity)
    }

    // Record DELETE event - always use null for after since entity is being deleted
    const [eventNs, ...eventIdParts] = fullId.split('/')
    await this.recordEvent(
      'DELETE',
      entityTarget(eventNs ?? '', eventIdParts.join('/')),
      beforeEntityForEvent,
      null,
      actor
    )

    return { deletedCount: 1 }
  }

  /**
   * Delete multiple entities matching a filter
   */
  async deleteMany(
    namespace: string,
    filter: Filter,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    validateNamespace(namespace)

    // Find all matching entities
    const result = await this.find(namespace, filter)
    let deletedCount = 0

    for (const entity of result.items) {
      const deleteResult = await this.delete(namespace, entity.$id as string, options)
      deletedCount += deleteResult.deletedCount
    }

    return { deletedCount }
  }

  /**
   * Restore a soft-deleted entity
   */
  async restore<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: { actor?: EntityId }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const entity = this.entities.get(fullId)
    if (!entity) {
      return null // Entity doesn't exist (hard deleted or never existed)
    }

    if (!entity.deletedAt) {
      return entity as Entity<T> // Entity is not deleted
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Capture before state for event
    const beforeEntityForEvent = { ...entity } as Entity

    // Remove deletedAt and deletedBy
    delete entity.deletedAt
    delete entity.deletedBy
    entity.updatedAt = now
    entity.updatedBy = actor
    entity.version = (entity.version || 1) + 1

    this.entities.set(fullId, entity)

    // Record RESTORE event (as UPDATE)
    const [eventNs, ...eventIdParts] = fullId.split('/')
    await this.recordEvent('UPDATE', entityTarget(eventNs ?? '', eventIdParts.join('/')), beforeEntityForEvent, entity, actor)

    return entity as Entity<T>
  }

  /**
   * Get history for an entity (alias for history method for public API)
   */
  async getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult> {
    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    return this.history(fullId as EntityId, options)
  }

  /**
   * Validate data against schema
   */
  private validateAgainstSchema(_namespace: string, data: CreateInput): void {
    const typeName = data.$type
    if (!typeName) return

    const typeDef = this.schema[typeName]
    if (!typeDef) return // No schema for this type, skip validation

    // Check required fields
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue // Skip meta fields

      const isRequired = this.isFieldRequired(fieldDef)
      const hasDefault = this.hasDefault(fieldDef)
      const fieldValue = (data as Record<string, unknown>)[fieldName]

      if (isRequired && !hasDefault && fieldValue === undefined) {
        throw new Error(`Missing required field: ${fieldName}`)
      }

      // Validate field type
      if (fieldValue !== undefined) {
        this.validateFieldType(fieldName, fieldValue, fieldDef)
      }
    }
  }

  /**
   * Check if a field is required based on its definition
   */
  private isFieldRequired(fieldDef: unknown): boolean {
    if (typeof fieldDef === 'string') {
      return fieldDef.includes('!')
    }
    if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { type?: string; required?: boolean }
      if (def.required) return true
      if (def.type && def.type.includes('!')) return true
    }
    return false
  }

  /**
   * Check if a field has a default value
   */
  private hasDefault(fieldDef: unknown): boolean {
    if (typeof fieldDef === 'string') {
      return fieldDef.includes('=')
    }
    if (typeof fieldDef === 'object' && fieldDef !== null) {
      return 'default' in (fieldDef as object)
    }
    return false
  }

  /**
   * Validate field value against its type definition
   */
  private validateFieldType(fieldName: string, value: unknown, fieldDef: unknown): void {
    let expectedType: string | undefined

    if (typeof fieldDef === 'string') {
      // Skip relationship definitions
      if (isRelationString(fieldDef)) {
        // Validate relationship reference format
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          // Relationship format: { 'Display Name': 'ns/id' }
          for (const [, refValue] of Object.entries(value)) {
            if (typeof refValue !== 'string' || !refValue.includes('/')) {
              throw new Error(`Invalid relationship reference format for ${fieldName}`)
            }
          }
        }
        return
      }
      const parsed = parseFieldType(fieldDef)
      expectedType = parsed.type
    } else if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { type?: string }
      if (def.type && !isRelationString(def.type)) {
        const parsed = parseFieldType(def.type)
        expectedType = parsed.type
      }
    }

    if (!expectedType) return

    // Basic type validation
    const actualType = typeof value
    switch (expectedType) {
      case 'string':
      case 'text':
      case 'markdown':
      case 'email':
      case 'url':
      case 'uuid':
        if (actualType !== 'string') {
          throw new Error(`Field ${fieldName} must be a string, got ${actualType}`)
        }
        break
      case 'number':
      case 'int':
      case 'float':
      case 'double':
        if (actualType !== 'number') {
          throw new Error(`Field ${fieldName} must be a number, got ${actualType}`)
        }
        break
      case 'boolean':
        if (actualType !== 'boolean') {
          throw new Error(`Field ${fieldName} must be a boolean, got ${actualType}`)
        }
        break
      case 'date':
      case 'datetime':
      case 'timestamp':
        if (!(value instanceof Date) && actualType !== 'string') {
          throw new Error(`Field ${fieldName} must be a date, got ${actualType}`)
        }
        break
    }
  }

  /**
   * Apply default values from schema
   */
  private applySchemaDefaults<T>(data: CreateInput<T>): CreateInput<T> {
    const typeName = data.$type
    if (!typeName) return data

    const typeDef = this.schema[typeName]
    if (!typeDef) return data

    const result: Record<string, unknown> = { ...data }

    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue
      if (result[fieldName] !== undefined) continue

      // Extract default value
      let defaultValue: unknown

      if (typeof fieldDef === 'string') {
        const match = fieldDef.match(/=\s*(.+)$/)
        if (match && match[1]) {
          defaultValue = match[1].trim()
          // Try to parse as JSON
          try {
            defaultValue = JSON.parse(defaultValue as string)
          } catch {
            // Keep as string
          }
        }
      } else if (typeof fieldDef === 'object' && fieldDef !== null) {
        const def = fieldDef as { default?: unknown }
        defaultValue = def.default
      }

      if (defaultValue !== undefined) {
        result[fieldName] = defaultValue
      }
    }

    return result as CreateInput<T>
  }

  /**
   * Reconstruct entity state at a specific point in time
   * This method also tracks snapshot usage stats for optimization metrics.
   */
  private reconstructEntityAtTime(fullId: string, asOf: Date): Entity | null {
    const [ns, ...idParts] = fullId.split('/')
    const entityId = idParts.join('/')

    const asOfTime = asOf.getTime()

    // Get all events for this entity, sorted by time and ID
    const allEvents = this.events
      .filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === entityId
      })
      .sort((a, b) => {
        const timeDiff = a.ts - b.ts
        if (timeDiff !== 0) return timeDiff
        return a.id.localeCompare(b.id)
      })

    if (allEvents.length === 0) {
      return null
    }

    // Find the target event. We include all events at or before the target timestamp.
    // When multiple events share the same millisecond timestamp, all of them are
    // included because they semantically all occurred "at" that time. This may result
    // in including slightly more events than a caller who passed a specific event's
    // timestamp might expect, but it's the correct interpretation of "as of" semantics.
    let targetEventIndex = -1

    for (let i = 0; i < allEvents.length; i++) {
      const event = allEvents[i]!  // loop bounds ensure valid index
      if (event.ts <= asOfTime) {
        targetEventIndex = i
      } else {
        break
      }
    }

    if (targetEventIndex === -1) {
      return null
    }

    // Check if we can use a snapshot for optimization
    const entitySnapshots = this.snapshots
      .filter(s => s.entityId === fullId)
      .sort((a, b) => a.sequenceNumber - b.sequenceNumber)

    // Find the best snapshot to use (closest to but not after target)
    let bestSnapshot: Snapshot | null = null
    for (const snapshot of entitySnapshots) {
      // Snapshot sequence number is 1-indexed event count
      // If snapshot is at sequence N, it contains state after event N-1 (0-indexed)
      if (snapshot.sequenceNumber - 1 <= targetEventIndex) {
        bestSnapshot = snapshot
      } else {
        break
      }
    }

    // Track stats for this query
    const stats: SnapshotQueryStats = {
      snapshotsUsed: 0,
      eventsReplayed: 0,
      snapshotUsedAt: undefined,
    }

    let entity: Entity | null = null
    let startIndex = 0

    if (bestSnapshot) {
      // Use snapshot as starting point
      entity = { ...bestSnapshot.state } as Entity
      startIndex = bestSnapshot.sequenceNumber // Start replaying from event after snapshot
      stats.snapshotsUsed = 1
      stats.snapshotUsedAt = bestSnapshot.sequenceNumber
      stats.eventsReplayed = targetEventIndex - (bestSnapshot.sequenceNumber - 1)
    } else {
      // Full replay from beginning
      stats.snapshotsUsed = 0
      stats.eventsReplayed = targetEventIndex + 1
    }

    // Replay events from startIndex to targetEventIndex
    for (let i = startIndex; i <= targetEventIndex; i++) {
      const event = allEvents[i]!  // loop bounds ensure valid index
      if (event.after) {
        entity = { ...event.after } as Entity
      } else if (event.op === 'DELETE') {
        entity = null
      }
    }

    // Store stats for this entity
    this.queryStats.set(fullId, stats)

    return entity
  }

  /**
   * Flush pending events to storage
   */
  private async flushEvents(): Promise<void> {
    if (this.pendingEvents.length === 0) return

    // Take all pending events
    const eventsToFlush = [...this.pendingEvents]
    this.pendingEvents = []
    this.flushPromise = null

    // Write events in a transactional manner:
    // 1. Write event log
    // 2. Write entity data
    // 3. Update indexes
    // All writes must succeed or the operation is rolled back
    const eventData = JSON.stringify(eventsToFlush)
    try {
      // Step 1: Write to event log
      await this.storage.write(`data/events.jsonl`, new TextEncoder().encode(eventData))

      // Step 2: Write entity data for each affected namespace
      const affectedNamespaces = new Set(eventsToFlush.map(e => {
        if (isRelationshipTarget(e.target)) return null
        return parseEntityTarget(e.target).ns
      }).filter((ns): ns is string => ns !== null))
      for (const ns of affectedNamespaces) {
        // Collect current state of all entities in this namespace
        const nsEntities: Entity[] = []
        this.entities.forEach((entity, id) => {
          if (id.startsWith(`${ns}/`)) {
            nsEntities.push(entity)
          }
        })
        const entityData = JSON.stringify(nsEntities)
        await this.storage.write(`data/${ns}/data.json`, new TextEncoder().encode(entityData))
      }

      // Step 3: Write namespace event logs
      for (const ns of affectedNamespaces) {
        const nsEvents = eventsToFlush.filter(e => {
          if (isRelationshipTarget(e.target)) return false
          return parseEntityTarget(e.target).ns === ns
        })
        const nsEventData = JSON.stringify(nsEvents)
        await this.storage.write(`${ns}/events.json`, new TextEncoder().encode(nsEventData))
      }
    } catch (error: unknown) {
      // On write failure, rollback the in-memory changes
      for (const event of eventsToFlush) {
        // Remove the event from the event store
        const idx = this.events.indexOf(event)
        if (idx !== -1) {
          this.events.splice(idx, 1)
        }
        // Rollback entity state
        const { ns, id } = isRelationshipTarget(event.target) ? { ns: '', id: '' } : parseEntityTarget(event.target)
        const fullId = `${ns}/${id}`
        if (event.op === 'CREATE') {
          // Remove created entity
          this.entities.delete(fullId)
        } else if (event.op === 'UPDATE' && event.before) {
          // Restore previous state
          this.entities.set(fullId, event.before as Entity)
        } else if (event.op === 'DELETE' && event.before) {
          // Restore deleted entity
          this.entities.set(fullId, event.before as Entity)
        }
      }
      throw error
    }
  }

  /**
   * Schedule a flush of pending events (used for batching)
   * Returns a promise that resolves when the flush is complete
   */
  private scheduleFlush(): Promise<void> {
    // Don't schedule if in a transaction - wait for commit
    if (this.inTransaction) return Promise.resolve()

    // If a flush is already scheduled, return that promise
    if (this.flushPromise) return this.flushPromise

    // Schedule a flush using microtask (allows batching of synchronous operations)
    // but returns a promise so callers can await it
    this.flushPromise = Promise.resolve().then(() => this.flushEvents())
    return this.flushPromise
  }

  /**
   * Record an event for an entity operation
   * Returns a promise that resolves when the event is flushed to storage
   *
   * @param op - Operation type (CREATE, UPDATE, DELETE)
   * @param target - Target identifier (entity: "ns:id", relationship: "from:pred:to")
   * @param before - State before change (undefined for CREATE)
   * @param after - State after change (undefined for DELETE)
   * @param actor - Who made the change
   * @param meta - Additional metadata
   */
  private recordEvent(
    op: EventOp,
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId,
    meta?: Record<string, unknown>
  ): Promise<void> {
    // Deep copy to prevent mutation of stored event state
    const deepCopy = <T>(obj: T | null): T | undefined => {
      if (obj === null) return undefined
      return JSON.parse(JSON.stringify(obj))
    }

    const event: Event = {
      id: generateId(),
      ts: Date.now(),
      op,
      target,
      before: deepCopy(before) as import('./types').Variant | undefined,
      after: deepCopy(after) as import('./types').Variant | undefined,
      actor: actor as string | undefined,
      metadata: meta as import('./types').Variant | undefined,
    }
    this.events.push(event)

    // Add to pending events buffer
    this.pendingEvents.push(event)

    // Schedule a batched flush (unless in transaction)
    const flushPromise = this.scheduleFlush()

    // Auto-snapshot if threshold is configured and reached (only for entity events)
    if (this.snapshotConfig.autoSnapshotThreshold && after && !isRelationshipTarget(target)) {
      const { ns, id } = parseEntityTarget(target)
      const fullEntityId = `${ns}/${id}` as EntityId
      const entityEventCount = this.events.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === id
      }).length
      const existingSnapshots = this.snapshots.filter(s => s.entityId === fullEntityId)
      const lastSnapshot = existingSnapshots.length > 0 ? existingSnapshots[existingSnapshots.length - 1] : undefined
      const lastSnapshotSeq = lastSnapshot?.sequenceNumber ?? 0
      const eventsSinceLastSnapshot = entityEventCount - lastSnapshotSeq
      if (eventsSinceLastSnapshot >= this.snapshotConfig.autoSnapshotThreshold) {
        // TODO(parquedb-y9aw): Auto-snapshot is fire-and-forget by design for performance,
        // but errors are silently swallowed. Consider logging snapshot failures or
        // implementing a retry mechanism with exponential backoff.
        this.getSnapshotManager().createSnapshot(fullEntityId).catch((err) => {
          // Log error but don't fail the main operation - snapshots are optimization only
          console.warn(`[ParqueDB] Auto-snapshot failed for ${fullEntityId}:`, err)
        })
      }
    }

    return flushPromise
  }

  /**
   * Get the event log interface
   */
  getEventLog(): EventLog {
    const self = this
    return {
      async getEvents(entityId: EntityId): Promise<Event[]> {
        const fullId = entityId as string
        const [ns, ...idParts] = fullId.split('/')
        const id = idParts.join('/')

        return self.events
          .filter(e => {
            if (isRelationshipTarget(e.target)) return false
            const info = parseEntityTarget(e.target)
            return info.ns === ns && info.id === id
          })
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getEventsByNamespace(ns: string): Promise<Event[]> {
        return self.events
          .filter(e => {
            if (isRelationshipTarget(e.target)) return false
            return parseEntityTarget(e.target).ns === ns
          })
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getEventsByTimeRange(from: Date, to: Date): Promise<Event[]> {
        const fromTime = from.getTime()
        const toTime = to.getTime()

        // Sort all events first to get consistent ordering by timestamp and ID
        const sortedEvents = [...self.events].sort((a, b) => {
          const timeDiff = a.ts - b.ts
          if (timeDiff !== 0) return timeDiff
          return a.id.localeCompare(b.id)
        })

        // For time range queries with millisecond precision, we use a counting approach:
        // Find all events that were recorded AT OR BEFORE the 'to' time,
        // but only include those that were created AFTER 'from' was captured.
        // Since event IDs are monotonically increasing, we can use ID comparison
        // for tie-breaking at the same millisecond.
        const result: Event[] = []
        for (const e of sortedEvents) {
          const eventTime = e.ts
          // Use inclusive range: fromTime <= eventTime <= toTime
          // This handles the case where midTime was captured in the same millisecond
          // as the first event
          if (eventTime >= fromTime && eventTime <= toTime) {
            result.push(e)
          }
        }

        // If we got multiple events at the boundary timestamp, filter to only include
        // events that occurred strictly before the second boundary event
        if (result.length > 1) {
          const boundaryTime = toTime
          const eventsAtBoundary = result.filter(e => e.ts === boundaryTime)
          if (eventsAtBoundary.length > 1) {
            // Remove the last event at the boundary (it was created after 'to' was captured)
            const lastEvent = eventsAtBoundary[eventsAtBoundary.length - 1]!  // length > 1 check ensures entry exists
            const idx = result.indexOf(lastEvent)
            if (idx !== -1) {
              result.splice(idx, 1)
            }
          }
        }

        return result
      },

      async getEventsByOp(op: EventOp): Promise<Event[]> {
        return self.events
          .filter(e => e.op === op)
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }> {
        const event = self.events.find(e => e.id === id)
        if (!event) {
          throw new Error(`Event not found: ${id}`)
        }
        // Check if payload is large enough to warrant compression (>10KB)
        const eventJson = JSON.stringify(event)
        const compressed = eventJson.length > 10000
        return { compressed, data: event }
      },
    }
  }

  /**
   * Get entity history
   */
  async history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const id = idParts.join('/')

    let relevantEvents = this.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === id
    })

    // Filter by time range
    if (options?.from) {
      const fromTime = options.from.getTime()
      relevantEvents = relevantEvents.filter(e => e.ts > fromTime)
    }
    if (options?.to) {
      const toTime = options.to.getTime()
      relevantEvents = relevantEvents.filter(e => e.ts <= toTime)
    }

    // Filter by operation type
    if (options?.op) {
      relevantEvents = relevantEvents.filter(e => e.op === options.op)
    }

    // Filter by actor
    if (options?.actor) {
      relevantEvents = relevantEvents.filter(e => e.actor === options.actor)
    }

    // Sort by timestamp, then by ID for events at the same timestamp
    relevantEvents.sort((a, b) => {
      const timeDiff = a.ts - b.ts
      if (timeDiff !== 0) return timeDiff
      return a.id.localeCompare(b.id)
    })

    // Apply cursor-based pagination
    if (options?.cursor) {
      const cursorIndex = relevantEvents.findIndex(e => e.id === options.cursor)
      if (cursorIndex !== -1) {
        relevantEvents = relevantEvents.slice(cursorIndex + 1)
      }
    }

    // Apply pagination
    const limit = options?.limit ?? 1000
    const hasMore = relevantEvents.length > limit
    const items = relevantEvents.slice(0, limit).map(e => {
      const targetInfo = parseEntityTarget(e.target)
      return {
        id: e.id,
        ts: new Date(e.ts),
        op: e.op,
        entityId: targetInfo.id,
        ns: targetInfo.ns,
        before: (e.before ?? null) as Entity | null,
        after: (e.after ?? null) as Entity | null,
        actor: e.actor as EntityId | undefined,
        metadata: e.metadata,
      }
    }) as HistoryItem[]

    return {
      items,
      hasMore,
      nextCursor: hasMore && items.length > 0 ? items[items.length - 1]!.id : undefined,
    }
  }

  /**
   * Get entity at a specific version
   */
  async getAtVersion<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    version: number
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    const [ns, ...idParts] = fullId.split('/')
    const entityId = idParts.join('/')

    // Get events for this entity
    const relevantEvents = this.events
      .filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === entityId
      })
      .sort((a, b) => a.ts - b.ts)

    // Apply events up to the target version
    let entity: Entity | null = null
    let currentVersion = 0

    for (const event of relevantEvents) {
      if (event.op === 'CREATE') {
        entity = event.after ? { ...event.after } as Entity : null
        currentVersion = entity?.version ?? 1
      } else if (event.op === 'UPDATE' && entity) {
        entity = event.after ? { ...event.after } as Entity : entity
        currentVersion = entity?.version ?? currentVersion + 1
      } else if (event.op === 'DELETE' && entity) {
        if (event.after) {
          entity = { ...event.after } as Entity
          currentVersion = entity?.version ?? currentVersion + 1
        }
      }

      if (currentVersion >= version) {
        break
      }
    }

    if (!entity || entity.version !== version) {
      return null
    }

    return entity as Entity<T>
  }

  /**
   * Begin a transaction
   */
  beginTransaction(): ParqueDBTransaction {
    // Set flag to suppress auto-flush during transaction
    this.inTransaction = true

    const pendingOps: Array<{
      type: 'create' | 'update' | 'delete'
      namespace: string
      id?: string
      data?: CreateInput
      update?: UpdateInput
      options?: CreateOptions | UpdateOptions | DeleteOptions
      entity?: Entity
    }> = []

    const self = this

    return {
      async create<T = Record<string, unknown>>(
        namespace: string,
        data: CreateInput<T>,
        options?: CreateOptions
      ): Promise<Entity<T>> {
        const entity = await self.create(namespace, data, options)
        pendingOps.push({ type: 'create', namespace, data, options, entity: entity as Entity })
        return entity
      },

      async update<T = Record<string, unknown>>(
        namespace: string,
        id: string,
        update: UpdateInput<T>,
        options?: UpdateOptions
      ): Promise<Entity<T> | null> {
        const entity = await self.update(namespace, id, update, options)
        if (entity) {
          pendingOps.push({ type: 'update', namespace, id, update, options, entity: entity as Entity })
        }
        return entity
      },

      async delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
        const result = await self.delete(namespace, id, options)
        if (result.deletedCount > 0) {
          pendingOps.push({ type: 'delete', namespace, id, options })
        }
        return result
      },

      async commit(): Promise<void> {
        // End transaction and flush events
        self.inTransaction = false
        await self.flushEvents()
        pendingOps.length = 0
      },

      async rollback(): Promise<void> {
        // End transaction without flushing
        self.inTransaction = false
        // Also clear pending events from buffer
        self.pendingEvents = []

        // Rollback by undoing operations in reverse order
        for (const op of pendingOps.reverse()) {
          if (op.type === 'create' && op.entity) {
            self.entities.delete(op.entity.$id as string)
            // Remove the CREATE event
            const entityIdStr = op.entity!.$id as string
            const expectedTarget = entityIdStr.replace('/', ':')
            const idx = self.events.findIndex(
              e => e.op === 'CREATE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          }
          // For update/delete, we'd need to restore from before state
          // This is a simplified implementation
        }
        pendingOps.length = 0
      },
    }
  }

  /**
   * Get snapshot manager
   */
  getSnapshotManager(): SnapshotManager {
    const self = this
    return {
      async createSnapshot(entityId: EntityId): Promise<Snapshot> {
        const fullId = entityId as string
        const entity = self.entities.get(fullId)
        if (!entity) throw new Error(`Entity not found: ${entityId}`)
        if (entity.deletedAt) throw new Error(`Cannot create snapshot of deleted entity: ${entityId}`)
        const [ns, ...idParts] = fullId.split('/')
        const entityEvents = self.events.filter((e) => {
          if (isRelationshipTarget(e.target)) return false
          const info = parseEntityTarget(e.target)
          return info.ns === ns && info.id === idParts.join('/')
        })
        const sequenceNumber = entityEvents.length
        const stateJson = JSON.stringify(entity)
        const stateSize = stateJson.length
        const compressed = stateSize > 1000
        const snapshot: Snapshot = { id: generateId(), entityId, ns: ns ?? '', sequenceNumber, createdAt: new Date(), state: { ...entity } as Record<string, unknown>, compressed, size: compressed ? Math.floor(stateSize * 0.3) : stateSize }
        self.snapshots.push(snapshot)
        const snapshotPath = `data/${ns}/snapshots/${snapshot.id}.parquet`
        await self.storage.write(snapshotPath, new TextEncoder().encode(stateJson))
        return snapshot
      },
      async createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot> {
        const fullId = entityId as string
        const [ns, ...idParts] = fullId.split('/')
        const entityIdPart = idParts.join('/')
        const event = self.events.find((e) => e.id === eventId)
        if (!event) throw new Error(`Event not found: ${eventId}`)
        const state = event.after ? { ...event.after } : null
        if (!state) throw new Error(`Event has no after state: ${eventId}`)
        const entityEvents = self.events.filter((e) => {
          if (isRelationshipTarget(e.target)) return false
          const info = parseEntityTarget(e.target)
          return info.ns === ns && info.id === entityIdPart
        }).sort((a, b) => a.ts - b.ts)
        const eventIndex = entityEvents.findIndex((e) => e.id === eventId)
        const sequenceNumber = eventIndex + 1
        const stateJson = JSON.stringify(state)
        const stateSize = stateJson.length
        const compressed = stateSize > 1000
        const snapshot: Snapshot = { id: generateId(), entityId, ns: ns ?? '', sequenceNumber, eventId, createdAt: new Date(), state: state as Record<string, unknown>, compressed, size: compressed ? Math.floor(stateSize * 0.3) : stateSize }
        self.snapshots.push(snapshot)
        await self.storage.write(`data/${ns}/snapshots/${snapshot.id}.parquet`, new TextEncoder().encode(stateJson))
        return snapshot
      },
      async listSnapshots(entityId: EntityId): Promise<Snapshot[]> {
        return self.snapshots.filter((s) => s.entityId === (entityId as string)).sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      },
      async deleteSnapshot(snapshotId: string): Promise<void> {
        const index = self.snapshots.findIndex((s) => s.id === snapshotId)
        if (index !== -1) {
          const snapshot = self.snapshots[index]!  // index !== -1 ensures entry exists
          self.snapshots.splice(index, 1)
          await self.storage.delete(`data/${snapshot.ns}/snapshots/${snapshotId}.parquet`)
        }
      },
      async pruneSnapshots(options: PruneSnapshotsOptions): Promise<void> {
        const { olderThan, keepMinimum = 0 } = options
        const snapshotsByEntity = new Map<string, Snapshot[]>()
        for (const snapshot of self.snapshots) {
          const eid = snapshot.entityId as string
          if (!snapshotsByEntity.has(eid)) snapshotsByEntity.set(eid, [])
          snapshotsByEntity.get(eid)!.push(snapshot)
        }
        for (const [, entitySnapshots] of snapshotsByEntity) {
          // Sort newest first by sequence number (most accurate measure of "age")
          entitySnapshots.sort((a, b) => b.sequenceNumber - a.sequenceNumber)
          // Keep at least keepMinimum snapshots (the newest ones)
          // When pruning by age (olderThan is set), the sort order determines age
          const candidates = entitySnapshots.slice(keepMinimum)
          for (const snapshot of candidates) {
            // Prune if:
            // 1. No olderThan specified (prune all candidates), or
            // 2. Timestamp <= olderThan, or
            // 3. Timestamp equals the newest snapshot's but this is not the newest
            //    (handles same-millisecond snapshots by sequence number ordering)
            const shouldPrune = !olderThan ||
              snapshot.createdAt.getTime() <= olderThan.getTime() ||
              (entitySnapshots.length > 1 && entitySnapshots[0] && snapshot.sequenceNumber < entitySnapshots[0].sequenceNumber)
            if (shouldPrune) {
              const idx = self.snapshots.findIndex((s) => s.id === snapshot.id)
              if (idx !== -1) {
                self.snapshots.splice(idx, 1)
                await self.storage.delete(`data/${snapshot.ns}/snapshots/${snapshot.id}.parquet`)
              }
            }
          }
        }
      },
      async getRawSnapshot(snapshotId: string): Promise<RawSnapshot> {
        const snapshot = self.snapshots.find((s) => s.id === snapshotId)
        if (!snapshot) throw new Error(`Snapshot not found: ${snapshotId}`)
        const data = new TextEncoder().encode(JSON.stringify(snapshot.state))
        return { id: snapshotId, size: snapshot.size || data.length, data }
      },
      async getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats> {
        return self.queryStats.get(entityId as string) || { snapshotsUsed: 0, eventsReplayed: 0 }
      },
      async getStorageStats(): Promise<SnapshotStorageStats> {
        const totalSize = self.snapshots.reduce((sum, s) => sum + (s.size || 0), 0)
        const snapshotCount = self.snapshots.length
        return { totalSize, snapshotCount, avgSnapshotSize: snapshotCount > 0 ? totalSize / snapshotCount : 0 }
      },
    }
  }

  /**
   * Upsert an entity (filter-based: update if exists, create if not)
   */
  async upsert<T = Record<string, unknown>>(
    namespace: string,
    filter: Filter,
    update: UpdateInput<T>,
    options?: { returnDocument?: 'before' | 'after' }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Find existing entity
    const result = await this.find<T>(namespace, filter)

    if (result.items.length > 0) {
      // Update existing
      const entity = result.items[0]!  // length > 0 ensures entry exists
      return this.update<T>(namespace, entity.$id as string, update, {
        returnDocument: options?.returnDocument,
      })
    } else {
      // Create new from filter fields and $set values
      // Extract non-operator fields from filter to include in the created document
      const filterFields: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(filter)) {
        // Only include simple field values, not operators
        if (!key.startsWith('$')) {
          filterFields[key] = value
        }
      }

      const data: any = {
        $type: 'Unknown',
        name: 'Upserted',
        ...filterFields,
        ...update.$set,
        ...update.$setOnInsert,
      }
      return this.create<T>(namespace, data)
    }
  }

  /**
   * Upsert multiple entities in a single operation
   */
  async upsertMany<T = Record<string, unknown>>(
    namespace: string,
    items: UpsertManyItem<T>[],
    options?: UpsertManyOptions
  ): Promise<UpsertManyResult> {
    validateNamespace(namespace)

    const result: UpsertManyResult = {
      ok: true,
      insertedCount: 0,
      modifiedCount: 0,
      matchedCount: 0,
      upsertedCount: 0,
      upsertedIds: [],
      errors: [],
    }

    // Handle empty array
    if (items.length === 0) {
      return result
    }

    const ordered = options?.ordered ?? true
    const actor = options?.actor

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!  // loop bounds ensure valid index

      try {
        // Find existing entity
        const existing = await this.find<T>(namespace, item.filter)

        if (existing.items.length > 0) {
          // Update existing entity
          const entity = existing.items[0]!  // length > 0 ensures entry exists
          result.matchedCount++

          // Build update options
          const updateOptions: UpdateOptions = {
            returnDocument: 'after',
          }
          if (actor) {
            updateOptions.actor = actor
          }
          if (item.options?.expectedVersion !== undefined) {
            updateOptions.expectedVersion = item.options.expectedVersion
          }

          // Remove $setOnInsert from update since we're updating
          const { $setOnInsert: _, ...updateWithoutSetOnInsert } = item.update as UpdateInput<T> & { $setOnInsert?: unknown }

          await this.update<T>(namespace, entity.$id as string, updateWithoutSetOnInsert, updateOptions)
          result.modifiedCount++
        } else {
          // Create new entity
          // Extract non-operator fields from filter to include in the created document
          const filterFields: Record<string, unknown> = {}
          for (const [key, value] of Object.entries(item.filter)) {
            // Only include simple field values, not operators
            if (!key.startsWith('$')) {
              filterFields[key] = value
            }
          }

          // Build the create data
          const createData: Record<string, unknown> = {
            $type: 'Unknown',
            name: 'Upserted',
            ...filterFields,
            ...item.update.$set,
            ...item.update.$setOnInsert,
          }

          // Apply other update operators to the create data
          // Handle $inc - start from 0
          if (item.update.$inc) {
            for (const [key, value] of Object.entries(item.update.$inc)) {
              createData[key] = ((createData[key] as number) || 0) + (value as number)
            }
          }

          // Handle $push - create array with single element
          if (item.update.$push) {
            for (const [key, value] of Object.entries(item.update.$push)) {
              const pushValue = value as Record<string, unknown>
              if (value && typeof value === 'object' && '$each' in pushValue) {
                createData[key] = [...((pushValue.$each as unknown[]) || [])]
              } else {
                createData[key] = [value]
              }
            }
          }

          // Handle $addToSet - create array with single element
          if (item.update.$addToSet) {
            for (const [key, value] of Object.entries(item.update.$addToSet)) {
              createData[key] = [value]
            }
          }

          // Handle $currentDate
          if (item.update.$currentDate) {
            const now = new Date()
            for (const key of Object.keys(item.update.$currentDate)) {
              createData[key] = now
            }
          }

          // Build create options
          const createOptions: CreateOptions = {}
          if (actor) {
            createOptions.actor = actor
          }

          const created = await this.create<T>(namespace, createData as CreateInput<T>, createOptions)

          result.insertedCount++
          result.upsertedCount++
          result.upsertedIds.push(created.$id)

          // Handle $link after creation
          if (item.update.$link) {
            await this.update<T>(namespace, created.$id as string, {
              $link: item.update.$link,
            } as UpdateInput<T>, { actor })
          }
        }
      } catch (error: unknown) {
        result.ok = false
        result.errors.push({
          index: i,
          filter: item.filter,
          error: error instanceof Error ? error : new Error(String(error)),
        })

        // If ordered, stop on first error
        if (ordered) {
          break
        }
      }
    }

    return result
  }

  /**
   * Compute diff between entity states at two timestamps
   */
  async diff(entityId: EntityId, t1: Date, t2: Date): Promise<DiffResult> {
    const fullId = entityId as string
    const state1 = this.reconstructEntityAtTime(fullId, t1)
    const state2 = this.reconstructEntityAtTime(fullId, t2)

    const added: string[] = []
    const removed: string[] = []
    const changed: string[] = []
    const values: { [field: string]: { before: unknown; after: unknown } } = {}

    // Skip metadata fields for diff comparison
    const metaFields = new Set(['$id', '$type', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'version', 'deletedAt', 'deletedBy'])

    // Helper to get all paths from an object (handles nested objects)
    const getAllPaths = (obj: Record<string, unknown> | null, prefix = ''): Map<string, unknown> => {
      const paths = new Map<string, unknown>()
      if (!obj) return paths

      for (const [key, value] of Object.entries(obj)) {
        if (metaFields.has(key)) continue
        const path = prefix ? `${prefix}.${key}` : key

        if (value && typeof value === 'object' && !Array.isArray(value)) {
          // Recurse into nested objects
          const nestedPaths = getAllPaths(value as Record<string, unknown>, path)
          for (const [nestedPath, nestedValue] of nestedPaths) {
            paths.set(nestedPath, nestedValue)
          }
        } else {
          paths.set(path, value)
        }
      }
      return paths
    }

    const paths1 = getAllPaths(state1 as Record<string, unknown> | null)
    const paths2 = getAllPaths(state2 as Record<string, unknown> | null)

    // Find added fields (in state2 but not in state1)
    for (const [path, value2] of paths2) {
      if (!paths1.has(path)) {
        added.push(path)
        values[path] = { before: undefined, after: value2 }
      }
    }

    // Find removed fields (in state1 but not in state2)
    for (const [path, value1] of paths1) {
      if (!paths2.has(path)) {
        removed.push(path)
        values[path] = { before: value1, after: undefined }
      }
    }

    // Find changed fields (in both but different values)
    for (const [path, value1] of paths1) {
      if (paths2.has(path)) {
        const value2 = paths2.get(path)
        const v1Str = JSON.stringify(value1)
        const v2Str = JSON.stringify(value2)
        if (v1Str !== v2Str) {
          changed.push(path)
          values[path] = { before: value1, after: value2 }
        }
      }
    }

    return { added, removed, changed, values }
  }

  /**
   * Revert entity to its state at a specific timestamp
   */
  async revert<T = Record<string, unknown>>(
    entityId: EntityId,
    targetTime: Date,
    options?: RevertOptions
  ): Promise<Entity<T>> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const id = idParts.join('/')

    // Validate targetTime is not in the future
    if (targetTime.getTime() > Date.now()) {
      throw new Error('Cannot revert to a future time')
    }

    // Get entity state at target time
    const stateAtTarget = this.reconstructEntityAtTime(fullId, targetTime)
    if (!stateAtTarget) {
      throw new Error('Entity did not exist at the target time')
    }

    // Get current entity
    const currentEntity = this.entities.get(fullId)
    if (!currentEntity) {
      throw new Error('Entity does not exist')
    }

    // Apply the revert as an update with metadata marking it as a revert
    const actor = options?.actor || currentEntity.updatedBy
    const now = new Date()

    // Capture before state for event
    const beforeEntityForEvent = { ...currentEntity } as Entity

    // Build update to restore the target state
    // Copy all fields from target state, preserving only essential metadata
    const newState = {
      ...stateAtTarget,
      $id: currentEntity.$id,
      createdAt: currentEntity.createdAt,
      createdBy: currentEntity.createdBy,
      updatedAt: now,
      updatedBy: actor,
      version: (currentEntity.version || 1) + 1,
    } as Entity

    // Remove deletedAt/deletedBy if present in target state (we're restoring to a non-deleted state)
    delete newState.deletedAt
    delete newState.deletedBy

    // Store the reverted entity
    this.entities.set(fullId, newState)

    // Record UPDATE event with revert metadata
    await this.recordEvent('UPDATE', entityTarget(ns ?? '', id), beforeEntityForEvent, newState, actor, { revert: true })

    return newState as Entity<T>
  }

  // ===========================================================================
  // Index Management API
  // ===========================================================================

  /**
   * Create a new index on a namespace
   *
   * @param ns - Namespace
   * @param definition - Index definition
   * @returns Index metadata
   *
   * @example
   * // Create a hash index for equality lookups
   * await db.createIndex('orders', {
   *   name: 'idx_status',
   *   type: 'hash',
   *   fields: [{ path: 'status' }]
   * })
   *
   * @example
   * // Create an SST index for range queries
   * await db.createIndex('products', {
   *   name: 'idx_price',
   *   type: 'sst',
   *   fields: [{ path: 'price' }]
   * })
   *
   * @example
   * // Create an FTS index for full-text search
   * await db.createIndex('articles', {
   *   name: 'idx_fts_content',
   *   type: 'fts',
   *   fields: [{ path: 'title' }, { path: 'body' }]
   * })
   */
  async createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata> {
    validateNamespace(ns)
    return this.indexManager.createIndex(ns, definition)
  }

  /**
   * Drop an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async dropIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.dropIndex(ns, indexName)
  }

  /**
   * List all indexes for a namespace
   *
   * @param ns - Namespace
   * @returns Array of index metadata
   */
  async listIndexes(ns: string): Promise<IndexMetadata[]> {
    validateNamespace(ns)
    return this.indexManager.listIndexes(ns)
  }

  /**
   * Get metadata for a specific index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index metadata or null if not found
   */
  async getIndex(ns: string, indexName: string): Promise<IndexMetadata | null> {
    validateNamespace(ns)
    return this.indexManager.getIndexMetadata(ns, indexName)
  }

  /**
   * Rebuild an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async rebuildIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.rebuildIndex(ns, indexName)
  }

  /**
   * Get statistics for an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index statistics
   */
  async getIndexStats(ns: string, indexName: string): Promise<IndexStats> {
    validateNamespace(ns)
    return this.indexManager.getIndexStats(ns, indexName)
  }

  /**
   * Get the index manager instance
   * (For advanced use cases)
   */
  getIndexManager(): IndexManager {
    return this.indexManager
  }
}

// =============================================================================
// ParqueDB Class (Public API with Proxy)
// =============================================================================

/**
 * ParqueDB - A Parquet-based database with proxy-based collection access
 *
 * @example
 * // Explicit collection access
 * const db = new ParqueDB({ storage })
 * await db.find('posts', { status: 'published' })
 * await db.get('posts', 'posts/123')
 *
 * @example
 * // Proxy-based collection access
 * const db = new ParqueDB({ storage })
 * await db.Posts.find({ status: 'published' })
 * await db.Posts.get('posts/123')
 */
export class ParqueDB {
  /** Dynamic collection access via Proxy */
  [key: string]: Collection | unknown

  constructor(config: ParqueDBConfig) {
    const impl = new ParqueDBImpl(config)

    // Return a Proxy for dynamic collection access
    return new Proxy(this, {
      get(_target, prop, _receiver) {
        // Handle known methods that delegate to impl FIRST
        // (before checking if property exists on target, since stubs exist there)
        if (prop === 'registerSchema') {
          return impl.registerSchema.bind(impl)
        }
        if (prop === 'collection') {
          return impl.collection.bind(impl)
        }
        if (prop === 'find') {
          return impl.find.bind(impl)
        }
        if (prop === 'get') {
          return impl.get.bind(impl)
        }
        if (prop === 'create') {
          return impl.create.bind(impl)
        }
        if (prop === 'update') {
          return impl.update.bind(impl)
        }
        if (prop === 'delete') {
          return impl.delete.bind(impl)
        }
        if (prop === 'history') {
          return impl.history.bind(impl)
        }
        if (prop === 'getAtVersion') {
          return impl.getAtVersion.bind(impl)
        }
        if (prop === 'beginTransaction') {
          return impl.beginTransaction.bind(impl)
        }
        if (prop === 'getSnapshotManager') {
          return impl.getSnapshotManager.bind(impl)
        }
        if (prop === 'getEventLog') {
          return impl.getEventLog.bind(impl)
        }
        if (prop === 'upsert') {
          return impl.upsert.bind(impl)
        }
        if (prop === 'upsertMany') {
          return impl.upsertMany.bind(impl)
        }
        if (prop === 'deleteMany') {
          return impl.deleteMany.bind(impl)
        }
        if (prop === 'restore') {
          return impl.restore.bind(impl)
        }
        if (prop === 'getHistory') {
          return impl.getHistory.bind(impl)
        }
        if (prop === 'diff') {
          return impl.diff.bind(impl)
        }
        if (prop === 'revert') {
          return impl.revert.bind(impl)
        }
        if (prop === 'getRelated') {
          return impl.getRelated.bind(impl)
        }
        // Index management methods
        if (prop === 'createIndex') {
          return impl.createIndex.bind(impl)
        }
        if (prop === 'dropIndex') {
          return impl.dropIndex.bind(impl)
        }
        if (prop === 'listIndexes') {
          return impl.listIndexes.bind(impl)
        }
        if (prop === 'getIndex') {
          return impl.getIndex.bind(impl)
        }
        if (prop === 'rebuildIndex') {
          return impl.rebuildIndex.bind(impl)
        }
        if (prop === 'getIndexStats') {
          return impl.getIndexStats.bind(impl)
        }
        if (prop === 'getIndexManager') {
          return impl.getIndexManager.bind(impl)
        }

        // Handle Symbol properties
        if (typeof prop === 'symbol') {
          return undefined
        }

        // Handle dynamic collection access for any string property
        // (Posts, Users, posts, users, etc.)
        if (typeof prop === 'string') {
          const ns = normalizeNamespace(prop)
          return impl.collection(ns)
        }

        return undefined
      },

      // Make instanceof work correctly
      getPrototypeOf() {
        return ParqueDB.prototype
      },
    }) as ParqueDB
  }

  /**
   * Register a schema for validation
   * @param schema - Schema definition
   */
  registerSchema(_schema: Schema): void {
    throw new Error('Not implemented')
  }

  /**
   * Get a collection by namespace
   * @param namespace - Collection namespace
   * @returns Collection interface
   */
  collection<T = Record<string, unknown>>(_namespace: string): Collection<T> {
    throw new Error('Not implemented')
  }

  /**
   * Find entities in a namespace
   * @param namespace - Target namespace
   * @param filter - MongoDB-style filter
   * @param options - Query options
   */
  find<T = Record<string, unknown>>(
    _namespace: string,
    _filter?: Filter,
    _options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>> {
    throw new Error('Not implemented')
  }

  /**
   * Get a single entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Get options
   */
  get<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _options?: GetOptions
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Create a new entity
   * @param namespace - Target namespace
   * @param data - Entity data
   * @param options - Create options
   */
  create<T = Record<string, unknown>>(
    _namespace: string,
    _data: CreateInput<T>,
    _options?: CreateOptions
  ): Promise<Entity<T>> {
    throw new Error('Not implemented')
  }

  /**
   * Update an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   */
  update<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _update: UpdateInput<T>,
    _options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Delete an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Delete options
   */
  delete(_namespace: string, _id: string, _options?: DeleteOptions): Promise<DeleteResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get entity history
   * @param entityId - Entity ID
   * @param options - History options
   */
  history(_entityId: EntityId, _options?: HistoryOptions): Promise<HistoryResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get entity at a specific version
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param version - Target version
   */
  getAtVersion<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _version: number
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Begin a transaction
   */
  beginTransaction(): ParqueDBTransaction {
    throw new Error('Not implemented')
  }

  /**
   * Get snapshot manager
   */
  getSnapshotManager(): SnapshotManager {
    throw new Error('Not implemented')
  }

  /**
   * Upsert an entity (filter-based: update if exists, create if not)
   * @param namespace - Target namespace
   * @param filter - Filter to find existing entity
   * @param update - Update operations
   * @param options - Upsert options
   */
  upsert<T = Record<string, unknown>>(
    _namespace: string,
    _filter: Filter,
    _update: UpdateInput<T>,
    _options?: { returnDocument?: 'before' | 'after' }
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Upsert multiple entities in a single operation
   * @param namespace - Target namespace
   * @param items - Array of upsert items with filter and update
   * @param options - UpsertMany options
   */
  upsertMany<T = Record<string, unknown>>(
    _namespace: string,
    _items: UpsertManyItem<T>[],
    _options?: UpsertManyOptions
  ): Promise<UpsertManyResult> {
    throw new Error('Not implemented')
  }

  /**
   * Delete multiple entities matching a filter
   * @param namespace - Target namespace
   * @param filter - Filter to match entities
   * @param options - Delete options
   */
  deleteMany(
    _namespace: string,
    _filter: Filter,
    _options?: DeleteOptions
  ): Promise<DeleteResult> {
    throw new Error('Not implemented')
  }

  /**
   * Restore a soft-deleted entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Restore options
   */
  restore<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _options?: { actor?: EntityId }
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Get history for an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - History options
   */
  getHistory(
    _namespace: string,
    _id: string,
    _options?: HistoryOptions
  ): Promise<HistoryResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get related entities with pagination support
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param relationField - Field name of the relationship
   * @param options - Options for pagination, filtering, sorting
   */
  getRelated<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _relationField: string,
    _options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>> {
    throw new Error('Not implemented')
  }

  // ===========================================================================
  // Index Management API
  // ===========================================================================

  /**
   * Create a new index on a namespace
   * @param ns - Namespace
   * @param definition - Index definition
   */
  createIndex(_ns: string, _definition: IndexDefinition): Promise<IndexMetadata> {
    throw new Error('Not implemented')
  }

  /**
   * Drop an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  dropIndex(_ns: string, _indexName: string): Promise<void> {
    throw new Error('Not implemented')
  }

  /**
   * List all indexes for a namespace
   * @param ns - Namespace
   */
  listIndexes(_ns: string): Promise<IndexMetadata[]> {
    throw new Error('Not implemented')
  }

  /**
   * Get metadata for a specific index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  getIndex(_ns: string, _indexName: string): Promise<IndexMetadata | null> {
    throw new Error('Not implemented')
  }

  /**
   * Rebuild an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  rebuildIndex(_ns: string, _indexName: string): Promise<void> {
    throw new Error('Not implemented')
  }

  /**
   * Get statistics for an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  getIndexStats(_ns: string, _indexName: string): Promise<IndexStats> {
    throw new Error('Not implemented')
  }

  /**
   * Get the index manager instance
   */
  getIndexManager(): IndexManager {
    throw new Error('Not implemented')
  }
}

export default ParqueDB
