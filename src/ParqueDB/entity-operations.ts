/**
 * ParqueDB Entity Operations Module
 *
 * Contains CRUD operations for entities: find, get, create, update, delete, restore.
 * These functions operate on entity state through a context object for dependency injection.
 */

import type {
  Entity,
  EntityData,
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
  Event,
} from '../types'

import { entityTarget, parseEntityTarget, isRelationshipTarget } from '../types'
import { parseFieldType, isRelationString } from '../types/schema'
import { FileNotFoundError } from '../storage/MemoryBackend'
import { deepClone } from '../utils'
import { matchesFilter as canonicalMatchesFilter } from '../query/filter'
import { sortEntities } from '../query/sort'
import { applyOperators } from '../mutation/operators'
// asMutableEntity is used for type assertions internally
import pluralize from 'pluralize'

import type { IndexManager } from '../indexes/manager'
import type { SchemaValidator } from '../schema/validator'

import type {
  Snapshot,
  SnapshotQueryStats,
  SnapshotConfig,
} from './types'

import {
  VersionConflictError,
  ValidationError,
} from './types'

import { validateNamespace, validateFilter, validateUpdateOperators, toFullId, resolveEntityId } from './validation'
import { parseRelation } from '../types/schema'

// =============================================================================
// Types - Focused Context Interfaces
// =============================================================================

/**
 * Minimal context for entity store operations.
 * Used by functions that only need to read/write entities.
 */
export interface EntityStoreContext {
  entities: Map<string, Entity>
}

/**
 * Context for query operations.
 * Used by find and get operations.
 */
export interface EntityQueryContext extends EntityStoreContext {
  storage: StorageBackend
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  indexManager: IndexManager
  embeddingProvider: import('../embeddings/provider').EmbeddingProvider | null
  reconstructEntityAtTime: (fullId: string, asOf: Date) => Entity | null
  detectParquetCorruption: (data: Uint8Array, filePath: string) => void
}

/**
 * Context for entity mutation operations (create/update/delete).
 * Extends query context with mutation-specific dependencies.
 */
export interface EntityMutationContext extends EntityQueryContext {
  schema: Schema
  schemaValidator: SchemaValidator | null
  snapshotConfig: SnapshotConfig
  recordEvent: (
    op: import('../types').EventOp,
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId | undefined,
    meta?: Record<string, unknown> | undefined
  ) => Promise<void>
  indexRelationshipsForEntity: (sourceId: string, entity: Entity) => void
  unindexRelationshipsForEntity: (sourceId: string, entity: Entity) => void
  applyRelationshipOperators: <T extends Record<string, unknown> = Record<string, unknown>>(entity: Entity, fullId: string, update: UpdateInput<T>) => Entity
}

/**
 * Full context object for entity operations.
 * Provides access to all shared state and dependencies.
 *
 * @deprecated Prefer using focused contexts (EntityQueryContext, EntityMutationContext)
 * where possible for better testability and reduced coupling.
 */
export interface EntityOperationsContext extends EntityMutationContext {
  // All properties are inherited from EntityMutationContext
  // This interface exists for backward compatibility
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Derive entity type from namespace/collection name
 * e.g., 'posts' -> 'Post', 'users' -> 'User', 'categories' -> 'Category'
 */
export function deriveTypeFromNamespace(namespace: string): string {
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}

/**
 * Check if a field is required based on its schema definition.
 */
export function isFieldRequired(fieldDef: unknown): boolean {
  if (typeof fieldDef === 'string') {
    return fieldDef.includes('!')
  }

  if (typeof fieldDef === 'object' && fieldDef !== null) {
    const def = fieldDef as { type?: string | undefined; required?: boolean | undefined }
    if (def.required) return true
    if (def.type && def.type.includes('!')) return true
  }

  return false
}

/**
 * Check if a field has a default value defined in its schema.
 */
export function hasDefault(fieldDef: unknown): boolean {
  if (typeof fieldDef === 'string') {
    return fieldDef.includes('=')
  }

  if (typeof fieldDef === 'object' && fieldDef !== null) {
    return 'default' in (fieldDef as object)
  }

  return false
}

/**
 * Validate a field value against its type definition from the schema.
 */
export function validateFieldType(
  fieldName: string,
  value: unknown,
  fieldDef: unknown,
  typeName: string
): void {
  let expectedType: string | undefined

  if (typeof fieldDef === 'string') {
    // Skip relationship definitions
    if (isRelationString(fieldDef)) {
      // Validate relationship reference format
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        for (const [, refValue] of Object.entries(value)) {
          if (typeof refValue !== 'string' || !refValue.includes('/')) {
            throw new ValidationError(
              'validation',
              typeName,
              'Invalid relationship reference format (must be "ns/id")',
              { fieldName }
            )
          }
        }
      }
      return
    }
    const parsed = parseFieldType(fieldDef)
    expectedType = parsed.type
  } else if (typeof fieldDef === 'object' && fieldDef !== null) {
    const def = fieldDef as { type?: string | undefined }
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
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'string',
          actualType,
        })
      }
      break
    case 'number':
    case 'int':
    case 'float':
    case 'double':
      if (actualType !== 'number') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'number',
          actualType,
        })
      }
      break
    case 'boolean':
      if (actualType !== 'boolean') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'boolean',
          actualType,
        })
      }
      break
    case 'date':
    case 'datetime':
    case 'timestamp':
      if (!(value instanceof Date) && actualType !== 'string') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'date',
          actualType,
        })
      }
      break
  }
}

/**
 * Apply default values from the schema to create input data.
 */
export function applySchemaDefaults<T extends EntityData = EntityData>(
  data: CreateInput<T>,
  schema: Schema
): CreateInput<T> {
  const typeName = data.$type
  if (!typeName) return data

  const typeDef = schema[typeName]
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
          // Intentionally ignored: value is not valid JSON, keep as raw string
        }
      }
    } else if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { default?: unknown | undefined }
      defaultValue = def.default
    }

    if (defaultValue !== undefined) {
      result[fieldName] = defaultValue
    }
  }

  return result as CreateInput<T>
}

// =============================================================================
// Auto-Resolve Relationships
// =============================================================================

/**
 * Get the namespace for a type from its schema definition.
 *
 * NOTE: This intentionally does NOT use the $ns directive because the Proxy-based
 * collection access (db.User, db.Post) uses the lowercased type name, not $ns.
 * If $ns support is needed, entities should be created via db.collection(ns).
 *
 * @param _schema - The schema (currently unused, kept for future $ns support)
 * @param typeName - The type name (e.g., 'User', 'Organization')
 * @returns The namespace (lowercased type name)
 */
function getNamespaceForType(_schema: Schema, typeName: string): string {
  // Match the proxy behavior: lowercase the type name
  return typeName.toLowerCase()
}

/**
 * Result of resolving a relationship ID
 */
export interface ResolvedRelationshipId {
  /** The full entity ID (e.g., 'user/alice@example.com') */
  fullId: string
  /** Display name from the target entity (e.g., 'Alice') */
  displayName: string
}

/**
 * Error thrown when relationship resolution fails
 */
export class RelationshipResolutionError extends Error {
  constructor(
    public readonly targetId: string,
    public readonly reason: 'not_found' | 'soft_deleted' | 'invalid_format',
    message: string
  ) {
    super(message)
    this.name = 'RelationshipResolutionError'
  }
}

/**
 * Resolve a single relationship ID to its full form.
 *
 * This function takes a short ID (e.g., 'alice@example.com') or full ID
 * (e.g., 'user/alice@example.com') and resolves it to:
 * 1. The full entity ID
 * 2. The display name from the target entity
 *
 * @param idValue - The ID to resolve (short or full form)
 * @param targetNs - The target namespace (used when idValue is short)
 * @param entities - The entity store to look up targets
 * @returns The resolved relationship info
 * @throws {RelationshipResolutionError} if target entity doesn't exist or is soft-deleted
 *
 * @example
 * ```typescript
 * // Resolve short ID
 * const result = resolveRelationshipId('alice@example.com', 'user', entities)
 * // Returns: { fullId: 'user/alice@example.com', displayName: 'Alice' }
 *
 * // Resolve full ID (namespace in idValue takes precedence)
 * const result = resolveRelationshipId('organization/acme', 'user', entities)
 * // Returns: { fullId: 'organization/acme', displayName: 'ACME Corp' }
 * ```
 */
export function resolveRelationshipId(
  idValue: string,
  targetNs: string,
  entities: Map<string, Entity>
): ResolvedRelationshipId {
  // Determine full ID
  const fullTargetId = idValue.includes('/') ? idValue : `${targetNs}/${idValue}`

  // Validate target exists
  const targetEntity = entities.get(fullTargetId)
  if (!targetEntity) {
    throw new RelationshipResolutionError(
      fullTargetId,
      'not_found',
      `Related entity '${fullTargetId}' does not exist`
    )
  }

  // Check if soft-deleted
  if (targetEntity.deletedAt) {
    throw new RelationshipResolutionError(
      fullTargetId,
      'soft_deleted',
      `Related entity '${fullTargetId}' does not exist`
    )
  }

  // Use target entity's name as display name, or fall back to ID
  const displayName = String(targetEntity.name || fullTargetId)

  return { fullId: fullTargetId, displayName }
}

/**
 * Resolve multiple relationship IDs in batch.
 *
 * Optimized for bulk operations - validates all targets exist before
 * returning results. Fails fast on first error.
 *
 * @param idValues - Array of IDs to resolve
 * @param targetNs - The target namespace
 * @param entities - The entity store
 * @returns Array of resolved relationship info
 * @throws {RelationshipResolutionError} if any target doesn't exist
 */
export function resolveRelationshipIdsBatch(
  idValues: string[],
  targetNs: string,
  entities: Map<string, Entity>
): ResolvedRelationshipId[] {
  return idValues.map(idValue => resolveRelationshipId(idValue, targetNs, entities))
}

/**
 * Auto-resolve relationship IDs in create data.
 *
 * When a schema defines a forward relationship like `author: '-> User'`, and the
 * create data contains a short ID (e.g., 'alice@example.com'), this function
 * resolves it to a full entity ID (e.g., 'user/alice@example.com') and converts
 * it to the relationship object format (e.g., { 'Alice': 'user/alice@example.com' }).
 *
 * Resolution rules:
 * 1. If the value already contains '/', it's already a full ID - convert to object
 * 2. If the target type has a $id directive, use targetNs/shortId
 * 3. Otherwise, treat as full ID (must contain '/')
 *
 * @param ctx - The mutation context with entities and schema
 * @param typeName - The type being created (e.g., 'Post')
 * @param data - The create data with potential short IDs
 * @returns Data with resolved relationship IDs in object format
 * @throws {RelationshipResolutionError} if a related entity does not exist or is soft-deleted
 */
export function autoResolveRelationships<T extends EntityData = EntityData>(
  ctx: EntityMutationContext,
  typeName: string,
  data: CreateInput<T>
): CreateInput<T> {
  const typeDef = ctx.schema[typeName]
  if (!typeDef) return data

  const result: Record<string, unknown> = { ...data }

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    // Skip non-string definitions and meta fields
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef !== 'string') continue
    if (!isRelationString(fieldDef)) continue

    // Only handle forward relationships (->)
    const parsed = parseRelation(fieldDef)
    if (!parsed || parsed.direction !== 'forward') continue

    const targetTypeName = parsed.toType
    if (!targetTypeName) continue

    const fieldValue = result[fieldName]
    if (fieldValue === undefined || fieldValue === null) continue

    // Get the target type's namespace
    const targetNs = getNamespaceForType(ctx.schema, targetTypeName)

    // Handle array relationships - use batch resolution for efficiency
    if (Array.isArray(fieldValue)) {
      const stringIds = fieldValue.filter((id): id is string => typeof id === 'string')
      const resolved = resolveRelationshipIdsBatch(stringIds, targetNs, ctx.entities)

      const relObject: Record<string, string> = {}
      for (const { fullId, displayName } of resolved) {
        relObject[displayName] = fullId
      }
      result[fieldName] = relObject
    } else if (typeof fieldValue === 'string') {
      // Single relationship value - convert to object format
      const { fullId, displayName } = resolveRelationshipId(fieldValue, targetNs, ctx.entities)
      result[fieldName] = { [displayName]: fullId }
    }
  }

  return result as CreateInput<T>
}

// =============================================================================
// CRUD Operations
// =============================================================================

/**
 * Find entities in a namespace
 *
 * Uses EntityQueryContext - only requires query-related dependencies.
 */
export async function findEntities<T extends EntityData = EntityData>(
  ctx: EntityQueryContext,
  namespace: string,
  filter?: Filter,
  options?: FindOptions
): Promise<PaginatedResult<Entity<T>>> {
  validateNamespace(namespace)
  if (filter) {
    validateFilter(filter)
  }

  // Normalize vector filter: convert text queries to embeddings if needed
  let normalizedFilter = filter
  const { normalizeVectorFilter, isTextVectorQuery } = await import('../query/vector-query')
  if (filter && isTextVectorQuery(filter)) {
    const result = await normalizeVectorFilter(filter, ctx.embeddingProvider ?? undefined)
    normalizedFilter = result.filter
  }

  // If asOf is specified, we need to reconstruct entity states at that time
  const asOf = options?.asOf

  // Get all entities for this namespace from in-memory store
  let items: Entity<T>[] = []

  if (asOf) {
    // Collect all entity IDs that exist in this namespace
    const entityIds = new Set<string>()
    ctx.entities.forEach((_, id) => {
      if (id.startsWith(`${namespace}/`)) {
        entityIds.add(id)
      }
    })

    // Also check events for entities that may have existed at asOf time
    for (const event of ctx.events) {
      if (isRelationshipTarget(event.target)) continue
      const { ns, id } = parseEntityTarget(event.target)
      if (ns === namespace) {
        const fullId = `${namespace}/${id}`
        entityIds.add(fullId)
      }
    }

    // Reconstruct each entity at asOf time
    for (const fullId of entityIds) {
      const entity = ctx.reconstructEntityAtTime(fullId, asOf)
      if (entity && !entity.deletedAt) {
        if (!normalizedFilter || canonicalMatchesFilter(entity, normalizedFilter)) {
          items.push(entity as Entity<T>)
        }
      }
    }
  } else {
    // Try to use indexes if filter is present
    let candidateDocIds: Set<string> | null = null

    if (normalizedFilter) {
      const selectedIndex = await ctx.indexManager.selectIndex(namespace, normalizedFilter)

      if (selectedIndex) {
        // Index found - use it to narrow down candidate documents
        if (selectedIndex.type === 'fts' && normalizedFilter.$text) {
          // Use FTS index for full-text search
          const ftsResults = await ctx.indexManager.ftsSearch(
            namespace,
            normalizedFilter.$text.$search,
            {
              language: normalizedFilter.$text.$language,
              limit: options?.limit,
              minScore: normalizedFilter.$text.$minScore,
            }
          )
          candidateDocIds = new Set(ftsResults.map(r => `${namespace}/${r.docId}`))
        } else if (selectedIndex.type === 'vector' && normalizedFilter.$vector) {
          // Use vector index for similarity search
          const vectorResults = await ctx.indexManager.vectorSearch(
            namespace,
            selectedIndex.index.name,
            normalizedFilter.$vector.$near ?? normalizedFilter.$vector.query as number[],
            normalizedFilter.$vector.$k ?? normalizedFilter.$vector.topK,
            {
              minScore: normalizedFilter.$vector.$minScore ?? normalizedFilter.$vector.minScore,
            }
          )
          candidateDocIds = new Set(vectorResults.docIds.map(id => `${namespace}/${id}`))
        }
      }
    }

    // Filter entities - either from index candidates or full scan
    ctx.entities.forEach((entity, id) => {
      if (id.startsWith(`${namespace}/`)) {
        // If we have candidate IDs from index, only consider those
        if (candidateDocIds !== null && !candidateDocIds.has(id)) {
          return
        }

        // Check if entity is deleted (unless includeDeleted is true)
        if (entity.deletedAt && !options?.includeDeleted) {
          return
        }

        // Apply remaining filter conditions
        if (!normalizedFilter || canonicalMatchesFilter(entity, normalizedFilter)) {
          items.push(entity as Entity<T>)
        }
      }
    })
  }

  // Apply sort using reusable utility
  if (options?.sort) {
    sortEntities(items, options.sort)
  }

  // Calculate total count before pagination
  const totalCount = items.length

  // Apply cursor-based pagination
  if (options?.cursor) {
    const cursorIndex = items.findIndex(e => e.$id === options.cursor)
    if (cursorIndex >= 0) {
      items = items.slice(cursorIndex + 1)
    } else {
      // Cursor not found - return empty
      items = []
    }
  }

  // Apply skip
  if (options?.skip && options.skip > 0) {
    items = items.slice(options.skip)
  }

  // Apply limit
  const limit = options?.limit
  let hasMore = false
  let nextCursor: string | undefined
  if (limit !== undefined && limit > 0) {
    hasMore = items.length > limit
    if (hasMore) {
      items = items.slice(0, limit)
    }
    // Set nextCursor to last item's $id if there are more results
    if (hasMore && items.length > 0) {
      nextCursor = items[items.length - 1]?.$id
    }
  }

  return {
    items,
    hasMore,
    nextCursor,
    total: totalCount,
  }
}

/**
 * Get a single entity
 *
 * Uses EntityQueryContext - only requires query-related dependencies.
 */
export async function getEntity<T extends EntityData = EntityData>(
  ctx: EntityQueryContext,
  namespace: string,
  id: string,
  options?: GetOptions
): Promise<Entity<T> | null> {
  validateNamespace(namespace)

  // Normalize ID (handle both "ns/id" and just "id" formats)
  const fullId = toFullId(namespace, id)

  // Try to read from storage to detect backend errors
  try {
    const dataPath = `data/${namespace}/data.parquet`
    await ctx.storage.read(dataPath)
  } catch (error: unknown) {
    if (!(error instanceof FileNotFoundError)) {
      throw error
    }
  }

  // Check event log integrity for corruption detection
  const eventLogPath = `${namespace}/events.parquet`
  let eventLogData: Uint8Array | null = null
  try {
    eventLogData = await ctx.storage.read(eventLogPath)
  } catch (error: unknown) {
    if (!(error instanceof FileNotFoundError)) {
      throw error
    }
  }

  if (eventLogData) {
    ctx.detectParquetCorruption(eventLogData, eventLogPath)
  }

  // If asOf is specified, reconstruct entity state at that time
  if (options?.asOf) {
    const entity = ctx.reconstructEntityAtTime(fullId, options.asOf)
    if (!entity) {
      return null
    }
    if (entity.deletedAt && !options?.includeDeleted) {
      return null
    }
    return entity as Entity<T>
  }

  const entity = ctx.entities.get(fullId)
  if (!entity) {
    return null
  }

  // Check if entity is deleted (unless includeDeleted is true)
  if (entity.deletedAt && !options?.includeDeleted) {
    return null
  }

  // Track snapshot usage stats for this entity
  const entitySnapshots = ctx.snapshots.filter(s => s.entityId === fullId)
  const latestSnapshot = entitySnapshots[entitySnapshots.length - 1]
  if (entitySnapshots.length > 0 && latestSnapshot) {
    const [ns, ...idParts] = fullId.split('/')
    const entityEvents = ctx.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === idParts.join('/')
    })
    const eventsAfterSnapshot = entityEvents.length - latestSnapshot.sequenceNumber
    ctx.queryStats.set(fullId, {
      snapshotsUsed: 1,
      eventsReplayed: Math.max(0, eventsAfterSnapshot),
      snapshotUsedAt: latestSnapshot.sequenceNumber,
    })
  }

  return entity as Entity<T>
}

/**
 * Create a new entity
 *
 * Uses EntityMutationContext - requires mutation-related dependencies.
 */
export async function createEntity<T extends EntityData = EntityData>(
  ctx: EntityMutationContext,
  namespace: string,
  data: CreateInput<T>,
  options?: CreateOptions,
  validateAgainstSchema?: (namespace: string, data: CreateInput, validateOnWrite?: boolean | import('../types').ValidationMode) => void
): Promise<Entity<T>> {
  validateNamespace(namespace)

  const now = new Date()

  // Auto-derive $type from namespace (needed early to check $id directive)
  const derivedType = data.$type || deriveTypeFromNamespace(namespace)

  // Resolve entity ID using the utility function
  const { fullId, localId: entityIdPart } = resolveEntityId({
    namespace,
    typeName: derivedType,
    schema: ctx.schema,
    data: data as Record<string, unknown>,
  })

  // Check for duplicate ID (entity already exists with this ID)
  const existingEntity = ctx.entities.get(fullId)
  if (existingEntity && !existingEntity.deletedAt) {
    throw new ValidationError(
      'create',
      derivedType,
      `Entity with ID '${fullId}' already exists`
    )
  }

  const actor = options?.actor || ('system/anonymous' as EntityId)

  // Auto-derive name using $name directive (defaults to 'name' field if not specified)
  const dataAsRecord = data as Record<string, unknown>
  const typeDef = ctx.schema[derivedType]
  // $name defaults to 'name' - only specify if using a different field
  const nameFieldName = typeDef?.$name ?? 'name'
  let derivedName: unknown

  // If explicit 'name' provided AND $name points to a different field, explicit name wins
  if (data.name !== undefined && nameFieldName !== 'name') {
    derivedName = data.name
  } else if (typeof nameFieldName === 'string') {
    // Use the configured $name field (or default 'name' field)
    const nameValue = dataAsRecord[nameFieldName]
    if (nameValue != null && nameValue !== '') {
      derivedName = nameValue
    } else {
      // Fall back to other common name fields, then entity ID
      derivedName = dataAsRecord.title || dataAsRecord.label || entityIdPart
    }
  } else {
    // Fall back to common name fields or entityIdPart
    derivedName = dataAsRecord.name || dataAsRecord.title || dataAsRecord.label || entityIdPart
  }

  // Apply defaults from schema
  const dataWithDefaults = applySchemaDefaults(data, ctx.schema)

  // Auto-resolve short relationship IDs to full IDs before validation
  const dataWithResolvedRels = autoResolveRelationships(ctx, derivedType, dataWithDefaults)

  // Determine if validation should run
  const shouldValidate = !options?.skipValidation && options?.validateOnWrite !== false

  // Validate against schema if registered (using derived type)
  if (shouldValidate && validateAgainstSchema) {
    validateAgainstSchema(namespace, { ...dataWithResolvedRels, $type: derivedType }, options?.validateOnWrite)
  }

  const entity = {
    ...dataWithResolvedRels,
    $id: fullId,
    $type: derivedType,
    name: derivedName,
    createdAt: now,
    createdBy: actor,
    updatedAt: now,
    updatedBy: actor,
    version: 1,
  } as unknown as Entity<T>

  // Store in memory
  ctx.entities.set(fullId, entity as Entity)

  // Update reverse relationship index for any relationships in the initial data
  ctx.indexRelationshipsForEntity(fullId, entity as Entity)

  // Update indexes - add new document
  await ctx.indexManager.onDocumentAdded(namespace, entityIdPart, entity as Record<string, unknown>, 0, 0)

  // Record CREATE event and await flush
  await ctx.recordEvent('CREATE', entityTarget(namespace, entityIdPart), null, entity as Entity, actor)

  return entity
}

/**
 * Update an entity
 *
 * Uses EntityMutationContext - requires mutation-related dependencies.
 */
export async function updateEntity<T extends EntityData = EntityData>(
  ctx: EntityMutationContext,
  namespace: string,
  id: string,
  update: UpdateInput<T>,
  options?: UpdateOptions
): Promise<Entity<T> | null> {
  validateNamespace(namespace)
  validateUpdateOperators(update)

  const fullId = toFullId(namespace, id)

  let entity = ctx.entities.get(fullId)

  // Track if this is an insert operation
  const isInsert = !entity

  // Handle upsert
  if (!entity) {
    if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
      throw new VersionConflictError(options.expectedVersion, undefined, {
        namespace,
        entityId: id,
      })
    }

    if (options?.upsert) {
      const now = new Date()
      const actor = options.actor || ('system/anonymous' as EntityId)

      const newEntity: Record<string, unknown> = {
        $id: fullId as EntityId,
        $type: 'Unknown',
        name: 'Upserted',
        createdAt: now,
        createdBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: 0,
      }

      if (update.$setOnInsert) {
        Object.assign(newEntity, update.$setOnInsert)
      }

      entity = newEntity as Entity
      ctx.entities.set(fullId, entity)
    } else {
      return null
    }
  }

  // Check version for optimistic concurrency
  if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
    throw new VersionConflictError(options.expectedVersion, entity.version, {
      namespace,
      entityId: id,
    })
  }

  // Clone the entity before mutating
  entity = deepClone(entity)

  const beforeEntity = options?.returnDocument === 'before' ? (isInsert ? null : { ...entity }) : null
  const beforeEntityForEvent = isInsert ? null : { ...entity } as Entity

  const now = new Date()
  const actor = options?.actor || entity.updatedBy

  // Apply basic update operators
  const operatorResult = applyOperators(entity as Record<string, unknown>, update, {
    isInsert,
    timestamp: now,
  })
  entity = operatorResult.document as Entity

  // Apply relationship operators
  entity = ctx.applyRelationshipOperators(entity, fullId, update)

  // Update metadata
  entity.updatedAt = now
  entity.updatedBy = (actor ?? entity.updatedBy) as EntityId
  entity.version = (entity.version ?? 0) + 1

  // Store updated entity
  ctx.entities.set(fullId, entity)

  // Update indexes
  const [entityNs, entityIdStr] = fullId.split('/')
  if (entityNs && entityIdStr) {
    await ctx.indexManager.onDocumentUpdated(
      entityNs,
      entityIdStr,
      beforeEntity as Record<string, unknown>,
      entity as Record<string, unknown>,
      0,
      0
    )
  }

  // Record UPDATE event
  const [eventNs, ...eventIdParts] = fullId.split('/')
  if (eventNs) {
    await ctx.recordEvent('UPDATE', entityTarget(eventNs, eventIdParts.join('/')), beforeEntityForEvent, entity, actor as EntityId | undefined)
  }

  return (options?.returnDocument === 'before' ? beforeEntity : entity) as Entity<T>
}

/**
 * Delete an entity
 *
 * Uses EntityMutationContext - requires mutation-related dependencies.
 */
export async function deleteEntity(
  ctx: EntityMutationContext,
  namespace: string,
  id: string,
  options?: DeleteOptions
): Promise<DeleteResult> {
  validateNamespace(namespace)

  const fullId = toFullId(namespace, id)
  const entity = ctx.entities.get(fullId)

  if (!entity) {
    if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
      throw new VersionConflictError(options.expectedVersion, undefined, {
        namespace,
        entityId: id,
      })
    }
    return { deletedCount: 0 }
  }

  if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
    throw new VersionConflictError(options.expectedVersion, entity.version, {
      namespace,
      entityId: id,
    })
  }

  const now = new Date()
  const actor = options?.actor || entity.updatedBy
  const beforeEntityForEvent = { ...entity } as Entity

  if (options?.hard) {
    ctx.entities.delete(fullId)
    ctx.unindexRelationshipsForEntity(fullId, entity)

    const [entityNs, entityIdStr] = fullId.split('/')
    if (entityNs && entityIdStr) {
      await ctx.indexManager.onDocumentRemoved(entityNs, entityIdStr, entity as Record<string, unknown>)
    }
  } else {
    if (entity.deletedAt) {
      return { deletedCount: 0 }
    }

    const cloned = deepClone(entity)
    cloned.deletedAt = now
    cloned.deletedBy = actor
    cloned.updatedAt = now
    cloned.updatedBy = actor
    cloned.version = (cloned.version || 1) + 1
    ctx.entities.set(fullId, cloned)

    const [entityNs, entityIdStr] = fullId.split('/')
    if (entityNs && entityIdStr) {
      await ctx.indexManager.onDocumentUpdated(
        entityNs,
        entityIdStr,
        entity as Record<string, unknown>,
        cloned as Record<string, unknown>,
        0,
        0
      )
    }
  }

  // DELETE events always have after: null to signify the entity is deleted
  // (regardless of soft or hard delete - the entity is "gone" from normal queries)
  const [eventNs, ...eventIdParts] = fullId.split('/')
  await ctx.recordEvent(
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
 *
 * Uses EntityMutationContext - requires mutation-related dependencies.
 */
export async function deleteManyEntities(
  ctx: EntityMutationContext,
  namespace: string,
  filter: Filter,
  options?: DeleteOptions
): Promise<DeleteResult> {
  validateNamespace(namespace)
  validateFilter(filter)

  const result = await findEntities(ctx, namespace, filter)
  let deletedCount = 0

  for (const entity of result.items) {
    const deleteResult = await deleteEntity(ctx, namespace, entity.$id as string, options)
    deletedCount += deleteResult.deletedCount
  }

  return { deletedCount }
}

/**
 * Restore a soft-deleted entity
 *
 * Uses EntityMutationContext - requires mutation-related dependencies.
 */
export async function restoreEntity<T extends EntityData = EntityData>(
  ctx: EntityMutationContext,
  namespace: string,
  id: string,
  options?: { actor?: EntityId | undefined }
): Promise<Entity<T> | null> {
  validateNamespace(namespace)

  const fullId = toFullId(namespace, id)
  const entity = ctx.entities.get(fullId)

  if (!entity) {
    return null
  }

  if (!entity.deletedAt) {
    return entity as Entity<T>
  }

  const now = new Date()
  const actor = options?.actor || entity.updatedBy
  const beforeEntityForEvent = { ...entity } as Entity

  const cloned = deepClone(entity)
  delete cloned.deletedAt
  delete cloned.deletedBy
  cloned.updatedAt = now
  cloned.updatedBy = actor
  cloned.version = (cloned.version || 1) + 1

  ctx.entities.set(fullId, cloned)

  const [eventNs, ...eventIdParts] = fullId.split('/')
  await ctx.recordEvent('UPDATE', entityTarget(eventNs ?? '', eventIdParts.join('/')), beforeEntityForEvent, cloned, actor)

  return cloned as Entity<T>
}
