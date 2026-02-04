/**
 * ParqueDB Write Path Module
 *
 * Contains write operations for entities: create, update, delete, deleteMany, restore.
 * @module ParqueDB/write-path
 */

import type {
  Entity,
  EntityData,
  EntityId,
  CreateInput,
  DeleteResult,
  UpdateInput,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  Schema,
  Filter,
  ValidationMode,
} from '../types'

import { entityTarget } from '../types'
import { generateId, deepClone } from '../utils'
import { applyOperators } from '../mutation/operators'

import type { SchemaValidator } from '../schema/validator'
import type { SnapshotConfig } from './types'
import { VersionConflictError } from './types'
import { validateNamespace, validateFilter, validateUpdateOperators, toFullId } from './validation'
import { findEntities, type ReadPathContext } from './read-path'

// Import helper functions from entity-operations (canonical source)
import {
  deriveTypeFromNamespace,
  isFieldRequired,
  hasDefault,
  validateFieldType,
  applySchemaDefaults,
} from './entity-operations'

// Re-export helpers for backward compatibility
export { deriveTypeFromNamespace, isFieldRequired, hasDefault, validateFieldType, applySchemaDefaults }

// =============================================================================
// Types
// =============================================================================

/** Context for write/mutation operations. Extends ReadPathContext. */
export interface WritePathContext extends ReadPathContext {
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
  applyRelationshipOperators: <T extends Record<string, unknown> = Record<string, unknown>>(
    entity: Entity,
    fullId: string,
    update: UpdateInput<T>
  ) => Entity
}

// =============================================================================
// Write Operations
// =============================================================================

/** Create a new entity in a namespace. */
export async function createEntity<T extends EntityData = EntityData>(
  ctx: WritePathContext,
  namespace: string,
  data: CreateInput<T>,
  options?: CreateOptions,
  validateAgainstSchema?: (namespace: string, data: CreateInput, validateOnWrite?: boolean | ValidationMode) => void
): Promise<Entity<T>> {
  validateNamespace(namespace)

  const now = new Date()

  // Use provided $id if present, otherwise generate a new one
  let fullId: EntityId
  let entityIdPart: string
  if (data.$id) {
    const providedId = String(data.$id)
    if (providedId.includes('/')) {
      fullId = providedId as EntityId
      entityIdPart = providedId.split('/').slice(1).join('/')
    } else {
      entityIdPart = providedId
      fullId = `${namespace}/${entityIdPart}` as EntityId
    }
  } else {
    entityIdPart = generateId()
    fullId = `${namespace}/${entityIdPart}` as EntityId
  }

  const actor = options?.actor || ('system/anonymous' as EntityId)

  // Auto-derive $type from namespace if not provided
  const derivedType = data.$type || deriveTypeFromNamespace(namespace)

  // Auto-derive name from common fields or use id
  const dataRecord = data as Record<string, unknown>
  const derivedName = data.name || dataRecord.title || dataRecord.label || entityIdPart

  // Apply defaults from schema
  const dataWithDefaults = applySchemaDefaults(data, ctx.schema)

  // Determine if validation should run
  const shouldValidate = !options?.skipValidation && options?.validateOnWrite !== false

  // Validate against schema if registered (using derived type)
  if (shouldValidate && validateAgainstSchema) {
    validateAgainstSchema(namespace, { ...dataWithDefaults, $type: derivedType }, options?.validateOnWrite)
  }

  const entity = {
    ...dataWithDefaults,
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

/** Update an existing entity. Supports $set, $inc, $push, $link, etc. */
export async function updateEntity<T extends EntityData = EntityData>(
  ctx: WritePathContext,
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

/** Delete an entity. Soft delete by default, use hard: true for permanent deletion. */
export async function deleteEntity(
  ctx: WritePathContext,
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

/** Delete multiple entities matching a filter. */
export async function deleteManyEntities(
  ctx: WritePathContext,
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

/** Restore a soft-deleted entity. */
export async function restoreEntity<T extends EntityData = EntityData>(
  ctx: WritePathContext,
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
