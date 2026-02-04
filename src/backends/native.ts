/**
 * Native Entity Backend
 *
 * A simple Parquet-based backend for entity storage without the overhead
 * of Iceberg or Delta Lake. Stores entities directly as Parquet files:
 * - data/{ns}/data.parquet
 *
 * Features:
 * - Basic CRUD operations
 * - No time-travel (single file per namespace)
 * - No schema evolution
 * - No transaction log or manifest files
 *
 * Best for:
 * - Simple use cases that don't need time-travel
 * - Embedded scenarios with minimal dependencies
 * - Development and testing
 */

import type {
  EntityBackend,
  NativeBackendConfig,
  EntitySchema,
  BackendStats,
} from './types'
import { ReadOnlyError } from './types'
import type { Entity, EntityId, EntityData, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'

// Import shared Parquet utilities
import {
  entityToRow,
  rowToEntity,
  buildEntityParquetSchema,
  matchesFilter,
  generateEntityId,
  extractDataFields,
} from './parquet-utils'

// Import shared entity utilities
import {
  applyUpdate as applyUpdateUtil,
  createDefaultEntity as createDefaultEntityUtil,
  sortEntities,
  applyPagination,
} from './entity-utils'

// Import Parquet utilities
import { ParquetWriter } from '../parquet/writer'
import { readParquet } from '../parquet/reader'

// =============================================================================
// Native Backend Implementation
// =============================================================================

/**
 * Native Parquet-based entity backend
 *
 * Each namespace becomes a single Parquet file at data/{ns}/data.parquet
 * with the following schema:
 * - $id: string (primary key)
 * - $type: string
 * - name: string
 * - createdAt, createdBy, updatedAt, updatedBy: audit fields
 * - deletedAt, deletedBy: soft delete fields
 * - version: int
 * - $data: binary (Variant - full entity data)
 */
export class NativeBackend implements EntityBackend {
  readonly type = 'native' as const
  readonly supportsTimeTravel = false
  readonly supportsSchemaEvolution = false
  readonly readOnly: boolean

  private storage: StorageBackend
  private location: string
  private initialized = false

  // In-memory cache of entities per namespace for efficient operations
  private entityCache = new Map<string, Map<string, Entity>>()

  constructor(config: NativeBackendConfig) {
    this.storage = config.storage
    this.location = config.location ?? ''
    this.readOnly = config.readOnly ?? false
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return

    // Ensure base directory exists
    if (this.location) {
      await this.storage.mkdir(this.location).catch(() => {
        // Directory might already exist
      })
    }

    this.initialized = true
  }

  async close(): Promise<void> {
    this.entityCache.clear()
    this.initialized = false
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  async get<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    const entities = await this.find<T>(ns, { $id: `${ns}/${id}` as EntityId }, { limit: 1, ...options })
    return entities[0] ?? null
  }

  async find<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    // Load entities from disk if not cached
    const entityMap = await this.loadEntities<T>(ns)

    // Convert to array and apply filter
    let entities = Array.from(entityMap.values()) as Entity<T>[]

    // Apply soft delete filter unless includeDeleted
    if (!options?.includeDeleted) {
      entities = entities.filter(e => !(e as Entity<T> & { deletedAt?: Date | undefined }).deletedAt)
    }

    // Apply filter
    if (filter && Object.keys(filter).length > 0) {
      entities = entities.filter(e => matchesFilter(e as Record<string, unknown>, filter))
    }

    // Apply sorting
    sortEntities(entities, options?.sort)

    // Apply skip and limit
    return applyPagination(entities, options?.skip, options?.limit)
  }

  async count(ns: string, filter?: Filter): Promise<number> {
    const entities = await this.find(ns, filter)
    return entities.length
  }

  async exists(ns: string, id: string): Promise<boolean> {
    const entity = await this.get(ns, id)
    return entity !== null
  }

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  async create<T extends EntityData = EntityData>(
    ns: string,
    input: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new ReadOnlyError('create', 'NativeBackend')
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId
    const id = generateEntityId()

    const entity: Entity<T> = {
      $id: `${ns}/${id}` as EntityId,
      $type: input.$type,
      name: input.name,
      createdAt: now,
      createdBy: actor,
      updatedAt: now,
      updatedBy: actor,
      version: 1,
      ...extractDataFields(input),
    } as Entity<T>

    // Load existing entities, add new one, save
    const entityMap = await this.loadEntities<T>(ns)
    entityMap.set(entity.$id, entity)
    await this.saveEntities(ns, entityMap)

    return entity
  }

  async update<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    update: Update,
    options?: UpdateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new ReadOnlyError('update', 'NativeBackend')
    }

    const fullId = `${ns}/${id}` as EntityId
    const entityMap = await this.loadEntities<T>(ns)
    const existing = entityMap.get(fullId) as Entity<T> | undefined

    if (!existing && !options?.upsert) {
      throw new Error(`Entity not found: ${ns}/${id}`)
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    // Apply update operators
    const updated = applyUpdateUtil(existing ?? createDefaultEntityUtil<T>(ns, id), update)

    // Update audit fields
    const entity: Entity<T> = {
      ...updated,
      updatedAt: now,
      updatedBy: actor,
      version: (existing?.version ?? 0) + 1,
    } as Entity<T>

    entityMap.set(entity.$id, entity)
    await this.saveEntities(ns, entityMap)

    return entity
  }

  async delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('delete', 'NativeBackend')
    }

    const fullId = `${ns}/${id}` as EntityId
    const entityMap = await this.loadEntities(ns)
    const entity = entityMap.get(fullId)

    if (!entity) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      // Hard delete: remove from map
      entityMap.delete(fullId)
    } else {
      // Soft delete: update deletedAt
      const now = new Date()
      const actor = options?.actor ?? 'system/parquedb' as EntityId
      const deleted = {
        ...entity,
        deletedAt: now,
        deletedBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
      entityMap.set(fullId, deleted)
    }

    await this.saveEntities(ns, entityMap)
    return { deletedCount: 1 }
  }

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  async bulkCreate<T extends EntityData = EntityData>(
    ns: string,
    inputs: CreateInput<T>[],
    options?: CreateOptions
  ): Promise<Entity<T>[]> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkCreate', 'NativeBackend')
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId
    const entityMap = await this.loadEntities<T>(ns)

    const entities = inputs.map(input => {
      const id = generateEntityId()
      const entity: Entity<T> = {
        $id: `${ns}/${id}` as EntityId,
        $type: input.$type,
        name: input.name,
        createdAt: now,
        createdBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: 1,
        ...extractDataFields(input),
      } as Entity<T>
      entityMap.set(entity.$id, entity)
      return entity
    })

    await this.saveEntities(ns, entityMap)
    return entities
  }

  async bulkUpdate(
    ns: string,
    filter: Filter,
    update: Update,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkUpdate', 'NativeBackend')
    }

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { matchedCount: 0, modifiedCount: 0 }
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId
    const entityMap = await this.loadEntities(ns)

    for (const entity of entities) {
      const updated = applyUpdateUtil(entity, update)
      const result = {
        ...updated,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
      entityMap.set(entity.$id, result)
    }

    await this.saveEntities(ns, entityMap)

    return {
      matchedCount: entities.length,
      modifiedCount: entities.length,
    }
  }

  async bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkDelete', 'NativeBackend')
    }

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { deletedCount: 0 }
    }

    const entityMap = await this.loadEntities(ns)

    if (options?.hard) {
      // Hard delete
      for (const entity of entities) {
        entityMap.delete(entity.$id)
      }
    } else {
      // Soft delete
      const now = new Date()
      const actor = options?.actor ?? 'system/parquedb' as EntityId

      for (const entity of entities) {
        const deleted = {
          ...entity,
          deletedAt: now,
          deletedBy: actor,
          updatedAt: now,
          updatedBy: actor,
          version: entity.version + 1,
        }
        entityMap.set(entity.$id, deleted)
      }
    }

    await this.saveEntities(ns, entityMap)
    return { deletedCount: entities.length }
  }

  // ===========================================================================
  // Schema
  // ===========================================================================

  async getSchema(ns: string): Promise<EntitySchema | null> {
    // Check if namespace has data
    const dataPath = this.getDataPath(ns)
    const exists = await this.storage.exists(dataPath)
    if (!exists) {
      return null
    }

    // Return default entity schema
    return {
      name: ns,
      fields: [
        { name: '$id', type: 'string', required: true },
        { name: '$type', type: 'string', required: true },
        { name: 'name', type: 'string', required: true },
        { name: 'createdAt', type: 'timestamp', required: true },
        { name: 'createdBy', type: 'string', required: true },
        { name: 'updatedAt', type: 'timestamp', required: true },
        { name: 'updatedBy', type: 'string', required: true },
        { name: 'deletedAt', type: 'timestamp', nullable: true },
        { name: 'deletedBy', type: 'string', nullable: true },
        { name: 'version', type: 'int', required: true },
        { name: '$data', type: 'binary', nullable: true },
      ],
    }
  }

  async listNamespaces(): Promise<string[]> {
    const prefix = this.location ? `${this.location}/data/` : 'data/'
    try {
      const result = await this.storage.list(prefix)
      const namespaces = new Set<string>()

      for (const file of result.files) {
        // Extract namespace from path like data/{ns}/data.parquet
        const relativePath = file.slice(prefix.length)
        const parts = relativePath.split('/')
        if (parts.length >= 1 && parts[0]) {
          namespaces.add(parts[0])
        }
      }

      return Array.from(namespaces)
    } catch {
      return []
    }
  }

  // ===========================================================================
  // Stats
  // ===========================================================================

  async stats(ns: string): Promise<BackendStats> {
    const dataPath = this.getDataPath(ns)

    try {
      const stat = await this.storage.stat(dataPath)

      // Check if file exists (stat returns null for non-existent files)
      if (!stat) {
        return {
          recordCount: 0,
          totalBytes: 0,
          fileCount: 0,
        }
      }

      const entityMap = await this.loadEntities(ns)
      const activeEntities = Array.from(entityMap.values()).filter(
        e => !(e as Entity & { deletedAt?: Date | undefined }).deletedAt
      )

      return {
        recordCount: activeEntities.length,
        totalBytes: stat.size ?? 0,
        fileCount: 1,
        lastModified: stat.mtime,
      }
    } catch {
      return {
        recordCount: 0,
        totalBytes: 0,
        fileCount: 0,
      }
    }
  }

  // ===========================================================================
  // Internal Helpers
  // ===========================================================================

  /**
   * Get the data file path for a namespace
   */
  private getDataPath(ns: string): string {
    return this.location
      ? `${this.location}/data/${ns}/data.parquet`
      : `data/${ns}/data.parquet`
  }

  /**
   * Load all entities from disk for a namespace
   */
  private async loadEntities<T = Record<string, unknown>>(ns: string): Promise<Map<string, Entity<T>>> {
    // Check cache
    const cached = this.entityCache.get(ns)
    if (cached) {
      return cached as Map<string, Entity<T>>
    }

    const dataPath = this.getDataPath(ns)
    const entityMap = new Map<string, Entity<T>>()

    try {
      const rows = await readParquet<Record<string, unknown>>(this.storage, dataPath)
      for (const row of rows) {
        const entity = rowToEntity<T>(row)
        entityMap.set(entity.$id, entity)
      }
    } catch {
      // File doesn't exist yet - return empty map
    }

    // Update cache
    this.entityCache.set(ns, entityMap as Map<string, Entity>)
    return entityMap
  }

  /**
   * Save all entities to disk for a namespace
   */
  private async saveEntities<T = Record<string, unknown>>(
    ns: string,
    entityMap: Map<string, Entity<T>>
  ): Promise<void> {
    const dataPath = this.getDataPath(ns)

    // Ensure directory exists
    const dirPath = dataPath.substring(0, dataPath.lastIndexOf('/'))
    await this.storage.mkdir(dirPath).catch(() => {})

    // Convert entities to rows
    const entities = Array.from(entityMap.values())
    const rows = entities.map(entity => entityToRow(entity))

    // Write Parquet file
    const parquetSchema = buildEntityParquetSchema()
    const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
    await writer.write(dataPath, rows, parquetSchema)

    // Update cache
    this.entityCache.set(ns, entityMap as Map<string, Entity>)
  }

}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a Native backend
 */
export function createNativeBackend(config: NativeBackendConfig): NativeBackend {
  return new NativeBackend(config)
}
