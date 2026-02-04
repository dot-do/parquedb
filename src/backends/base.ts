/**
 * Base Entity Backend
 *
 * Abstract base class providing shared functionality for entity backends.
 * Includes common implementations for read operations, entity manipulation,
 * and helper methods used by IcebergBackend, DeltaBackend, and others.
 */

import type {
  EntityBackend,
  BackendType,
  EntitySchema,
  SnapshotInfo,
  CompactOptions,
  CompactResult,
  VacuumOptions,
  VacuumResult,
  BackendStats,
} from './types'
import { ReadOnlyError } from './types'
import type { Entity, EntityId, EntityData, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import { entityId, SYSTEM_ACTOR } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'

// Import shared Parquet utilities
import {
  matchesFilter,
  generateEntityId,
  extractDataFields,
} from './parquet-utils'

// Import shared entity utilities
import {
  applyUpdate as applyUpdateUtil,
  createDefaultEntity as createDefaultEntityUtil,
  sortEntitiesImmutable,
  applyPaginationFromOptions,
} from './entity-utils'

// =============================================================================
// Abstract Base Entity Backend
// =============================================================================

/**
 * Abstract base class for entity backends
 *
 * Provides common implementations for:
 * - Lifecycle management (initialize/close with idempotency)
 * - Read operations (get, count, exists)
 * - Entity manipulation (applyUpdate, createDefaultEntity)
 * - Sorting and filtering helpers
 * - Read-only checks
 *
 * Subclasses must implement:
 * - Abstract read method: findInternal()
 * - Abstract write methods: createInternal(), updateInternal(), deleteInternal()
 * - Abstract batch methods: bulkCreateInternal(), bulkUpdateInternal(), bulkDeleteInternal()
 * - Abstract schema methods: getSchema(), listNamespaces()
 * - Optional: snapshot(), listSnapshots(), setSchema(), compact(), vacuum(), stats()
 */
export abstract class BaseEntityBackend implements EntityBackend {
  // ===========================================================================
  // Metadata (to be set by subclass)
  // ===========================================================================

  abstract readonly type: BackendType
  abstract readonly supportsTimeTravel: boolean
  abstract readonly supportsSchemaEvolution: boolean
  readonly readOnly: boolean

  // ===========================================================================
  // Protected State
  // ===========================================================================

  protected storage: StorageBackend
  protected location: string
  protected initialized = false

  // ===========================================================================
  // Constructor
  // ===========================================================================

  constructor(config: { storage: StorageBackend; location?: string | undefined; readOnly?: boolean | undefined }) {
    this.storage = config.storage
    this.location = config.location ?? ''
    this.readOnly = config.readOnly ?? false
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Initialize the backend
   *
   * Subclasses can override initializeInternal() for custom initialization.
   * This method ensures idempotency.
   */
  async initialize(): Promise<void> {
    if (this.initialized) return

    // Ensure base directory exists
    if (this.location) {
      await this.storage.mkdir(this.location).catch(() => {
        // Directory might already exist
      })
    }

    await this.initializeInternal()
    this.initialized = true
  }

  /**
   * Close the backend and release resources
   *
   * Subclasses can override closeInternal() for custom cleanup.
   */
  async close(): Promise<void> {
    await this.closeInternal()
    this.initialized = false
  }

  /**
   * Custom initialization logic for subclasses
   * @protected
   */
  protected async initializeInternal(): Promise<void> {
    // Override in subclass if needed
  }

  /**
   * Custom cleanup logic for subclasses
   * @protected
   */
  protected async closeInternal(): Promise<void> {
    // Override in subclass if needed
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Get a single entity by ID
   *
   * Default implementation uses find() with an ID filter.
   */
  async get<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    const fullId = id.includes('/') ? id : entityId(ns, id)
    const entities = await this.find<T>(ns, { $id: fullId }, { limit: 1, ...options })
    return entities[0] ?? null
  }

  /**
   * Find entities matching a filter
   *
   * Delegates to findInternal() and applies soft delete filtering.
   */
  async find<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    const entities = await this.findInternal<T>(ns, filter, options)

    // Apply soft delete filter unless includeDeleted
    if (!options?.includeDeleted) {
      return entities.filter(e => !e.deletedAt)
    }

    return entities
  }

  /**
   * Count entities matching a filter
   *
   * Default implementation reads all matching entities.
   * Subclasses can override for optimized counting.
   */
  async count(ns: string, filter?: Filter): Promise<number> {
    const entities = await this.find(ns, filter)
    return entities.length
  }

  /**
   * Check if an entity exists
   */
  async exists(ns: string, id: string): Promise<boolean> {
    const entity = await this.get(ns, id)
    return entity !== null
  }

  /**
   * Internal find implementation
   *
   * Must be implemented by subclasses to handle format-specific reading.
   * Should NOT filter soft deletes - that's handled by find().
   * Should handle sorting, skip, and limit.
   *
   * @protected
   */
  protected abstract findInternal<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]>

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  /**
   * Create a new entity
   */
  async create<T extends EntityData = EntityData>(
    ns: string,
    input: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    this.assertWritable('create')

    const now = new Date()
    const actor = options?.actor ?? SYSTEM_ACTOR
    const id = generateEntityId()

    const entity: Entity<T> = {
      $id: entityId(ns, id),
      $type: input.$type,
      name: input.name,
      createdAt: now,
      createdBy: actor,
      updatedAt: now,
      updatedBy: actor,
      version: 1,
      ...extractDataFields(input),
    } as Entity<T>

    await this.createInternal(ns, entity)
    return entity
  }

  /**
   * Update an existing entity
   */
  async update<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    update: Update,
    options?: UpdateOptions
  ): Promise<Entity<T>> {
    this.assertWritable('update')

    // Get existing entity
    const existing = await this.get<T>(ns, id)
    if (!existing && !options?.upsert) {
      throw this.entityNotFoundError(ns, id)
    }

    const now = new Date()
    const actor = options?.actor ?? SYSTEM_ACTOR

    // Apply update operators
    const updated = this.applyUpdate(existing ?? this.createDefaultEntity<T>(ns, id), update)

    // Update audit fields
    const entity: Entity<T> = {
      ...updated,
      updatedAt: now,
      updatedBy: actor,
      version: (existing?.version ?? 0) + 1,
    } as Entity<T>

    await this.updateInternal(ns, entity)
    return entity
  }

  /**
   * Delete an entity
   */
  async delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
    this.assertWritable('delete')

    const entity = await this.get(ns, id)
    if (!entity) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      await this.hardDeleteInternal(ns, [`${ns}/${id}`])
    } else {
      // Soft delete: update deletedAt
      const now = new Date()
      const actor = options?.actor ?? SYSTEM_ACTOR
      const deleted = {
        ...entity,
        deletedAt: now,
        deletedBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
      await this.updateInternal(ns, deleted)
    }

    return { deletedCount: 1 }
  }

  /**
   * Internal create implementation
   * @protected
   */
  protected abstract createInternal<T>(ns: string, entity: Entity<T>): Promise<void>

  /**
   * Internal update implementation
   * @protected
   */
  protected abstract updateInternal<T>(ns: string, entity: Entity<T>): Promise<void>

  /**
   * Internal hard delete implementation
   * @protected
   */
  protected abstract hardDeleteInternal(ns: string, ids: (string | EntityId)[]): Promise<void>

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  /**
   * Create multiple entities
   */
  async bulkCreate<T extends EntityData = EntityData>(
    ns: string,
    inputs: CreateInput<T>[],
    options?: CreateOptions
  ): Promise<Entity<T>[]> {
    this.assertWritable('bulkCreate')

    const now = new Date()
    const actor = options?.actor ?? SYSTEM_ACTOR

    const entities = inputs.map(input => {
      const id = generateEntityId()
      return {
        $id: entityId(ns, id),
        $type: input.$type,
        name: input.name,
        createdAt: now,
        createdBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: 1,
        ...extractDataFields(input),
      } as Entity<T>
    })

    await this.bulkCreateInternal(ns, entities)
    return entities
  }

  /**
   * Update multiple entities matching a filter
   */
  async bulkUpdate(
    ns: string,
    filter: Filter,
    update: Update,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    this.assertWritable('bulkUpdate')

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { matchedCount: 0, modifiedCount: 0 }
    }

    const now = new Date()
    const actor = options?.actor ?? SYSTEM_ACTOR

    const updated = entities.map(entity => {
      const result = this.applyUpdate(entity, update)
      return {
        ...result,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
    })

    await this.bulkUpdateInternal(ns, updated)

    return {
      matchedCount: entities.length,
      modifiedCount: updated.length,
    }
  }

  /**
   * Delete multiple entities matching a filter
   */
  async bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    this.assertWritable('bulkDelete')

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      const ids = entities.map(e => e.$id)
      await this.hardDeleteInternal(ns, ids)
    } else {
      const now = new Date()
      const actor = options?.actor ?? SYSTEM_ACTOR

      const deleted = entities.map(entity => ({
        ...entity,
        deletedAt: now,
        deletedBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }))

      await this.bulkUpdateInternal(ns, deleted)
    }

    return { deletedCount: entities.length }
  }

  /**
   * Internal bulk create implementation
   * @protected
   */
  protected abstract bulkCreateInternal<T>(ns: string, entities: Entity<T>[]): Promise<void>

  /**
   * Internal bulk update implementation
   * @protected
   */
  protected abstract bulkUpdateInternal<T>(ns: string, entities: Entity<T>[]): Promise<void>

  // ===========================================================================
  // Time Travel (optional, to be implemented by subclass)
  // ===========================================================================

  async snapshot?(ns: string, version: number | Date): Promise<EntityBackend>

  async listSnapshots?(ns: string): Promise<SnapshotInfo[]>

  // ===========================================================================
  // Schema
  // ===========================================================================

  abstract getSchema(ns: string): Promise<EntitySchema | null>

  abstract listNamespaces(): Promise<string[]>

  async setSchema?(ns: string, schema: EntitySchema): Promise<void>

  // ===========================================================================
  // Maintenance (optional, to be implemented by subclass)
  // ===========================================================================

  async compact?(ns: string, options?: CompactOptions): Promise<CompactResult>

  async vacuum?(ns: string, options?: VacuumOptions): Promise<VacuumResult>

  async stats?(ns: string): Promise<BackendStats>

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Assert that the backend is writable
   * @throws {ReadOnlyError} If the backend is read-only
   */
  protected assertWritable(operation: string): void {
    if (this.readOnly) {
      throw new ReadOnlyError(operation, this.constructor.name)
    }
  }

  /**
   * Create an error for entity not found
   *
   * Subclasses can override to use a specific error class.
   */
  protected entityNotFoundError(ns: string, id: string): Error {
    return new Error(`Entity not found: ${ns}/${id}`)
  }

  /**
   * Apply update operators to an entity
   *
   * Supports all MongoDB-style operators via the mutation/operators module:
   * - Field: $set, $unset, $rename, $setOnInsert
   * - Numeric: $inc, $mul, $min, $max
   * - Array: $push, $pull, $pullAll, $addToSet, $pop
   * - Date: $currentDate
   * - Bitwise: $bit
   *
   * Delegates to shared entity-utils for consistent behavior across backends.
   */
  protected applyUpdate<T>(entity: Entity<T>, update: Update): Entity<T> {
    return applyUpdateUtil(entity, update)
  }

  /**
   * Create a default entity for upsert operations
   *
   * Delegates to shared entity-utils for consistent behavior across backends.
   */
  protected createDefaultEntity<T>(ns: string, id: string): Entity<T> {
    return createDefaultEntityUtil<T>(ns, id)
  }

  /**
   * Sort entities by specified fields
   *
   * Handles null/undefined values (sorted to end), strings, numbers, and dates.
   * Delegates to shared entity-utils for consistent behavior across backends.
   */
  protected sortEntities<T>(
    entities: Entity<T>[],
    sort: Record<string, 1 | -1>
  ): Entity<T>[] {
    return sortEntitiesImmutable(entities, sort)
  }

  /**
   * Apply skip and limit to an array of entities
   *
   * Delegates to shared entity-utils for consistent behavior across backends.
   */
  protected applyPagination<T>(
    entities: Entity<T>[],
    options?: { skip?: number | undefined; limit?: number | undefined }
  ): Entity<T>[] {
    return applyPaginationFromOptions(entities, options)
  }

  /**
   * Filter entities by a MongoDB-style filter
   *
   * Re-exported from parquet-utils for convenience.
   */
  protected filterEntities<T>(
    entities: Entity<T>[],
    filter?: Filter
  ): Entity<T>[] {
    if (!filter || Object.keys(filter).length === 0) {
      return entities
    }
    return entities.filter(e => matchesFilter(e as Record<string, unknown>, filter))
  }

  /**
   * Generate a unique UUID
   *
   * Uses crypto.getRandomValues when available, falls back to Math.random.
   */
  protected generateUUID(): string {
    const bytes = new Uint8Array(16)
    if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
      crypto.getRandomValues(bytes)
    } else {
      for (let i = 0; i < 16; i++) {
        bytes[i] = Math.floor(Math.random() * 256)
      }
    }

    // Set version 4 and variant
    bytes[6] = (bytes[6]! & 0x0f) | 0x40
    bytes[8] = (bytes[8]! & 0x3f) | 0x80

    const hex = Array.from(bytes)
      .map(b => b.toString(16).padStart(2, '0'))
      .join('')
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
  }
}
