/**
 * ParqueDB Write Path Module
 *
 * Contains write operations for entities: create, update, delete, deleteMany, restore.
 * These functions operate on entity state through a context object for dependency injection.
 *
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
import { parseFieldType, isRelationString } from '../types/schema'
import pluralize from 'pluralize'

import type { SchemaValidator } from '../schema/validator'

import type {
  SnapshotConfig,
} from './types'

import {
  VersionConflictError,
  ValidationError,
} from './types'

import { validateNamespace, validateFilter, validateUpdateOperators, toFullId } from './validation'
import { findEntities, type ReadPathContext } from './read-path'

// =============================================================================
// Types
// =============================================================================

/**
 * Context for write/mutation operations.
 *
 * Extends ReadPathContext with additional dependencies needed for mutations.
 */
export interface WritePathContext extends ReadPathContext {
  /** Schema definitions for validation */
  schema: Schema
  /** Schema validator instance (if schema validation is enabled) */
  schemaValidator: SchemaValidator | null
  /** Configuration for automatic snapshots */
  snapshotConfig: SnapshotConfig
  /** Function to record events to the event log */
  recordEvent: (
    op: import('../types').EventOp,
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId | undefined,
    meta?: Record<string, unknown> | undefined
  ) => Promise<void>
  /** Function to index relationships for an entity */
  indexRelationshipsForEntity: (sourceId: string, entity: Entity) => void
  /** Function to remove relationship indexes for an entity */
  unindexRelationshipsForEntity: (sourceId: string, entity: Entity) => void
  /** Function to apply $link/$unlink operators */
  applyRelationshipOperators: <T extends Record<string, unknown> = Record<string, unknown>>(
    entity: Entity,
    fullId: string,
    update: UpdateInput<T>
  ) => Entity
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Derive entity type from namespace/collection name.
 *
 * Converts a plural namespace to a singular PascalCase type name.
 *
 * @param namespace - The namespace (e.g., 'posts', 'users', 'categories')
 * @returns The derived type name (e.g., 'Post', 'User', 'Category')
 *
 * @example
 * ```typescript
 * deriveTypeFromNamespace('posts')     // 'Post'
 * deriveTypeFromNamespace('users')     // 'User'
 * deriveTypeFromNamespace('categories') // 'Category'
 * ```
 */
export function deriveTypeFromNamespace(namespace: string): string {
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}

/**
 * Check if a field is required based on its schema definition.
 *
 * @param fieldDef - The field definition from schema
 * @returns True if the field is required (has '!' modifier or required: true)
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
 *
 * @param fieldDef - The field definition from schema
 * @returns True if the field has a default value
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
 *
 * @param fieldName - The field name for error messages
 * @param value - The value to validate
 * @param fieldDef - The field definition from schema
 * @param typeName - The type name for error messages
 * @throws {ValidationError} If the value doesn't match the expected type
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
 *
 * @param data - The create input data
 * @param schema - The schema definitions
 * @returns The data with defaults applied
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
// Write Operations
// =============================================================================

/**
 * Create a new entity in a namespace.
 *
 * @param ctx - Write context with required dependencies
 * @param namespace - Collection/namespace to create in
 * @param data - Entity data (can include $id for custom IDs)
 * @param options - Create options (actor, validation settings)
 * @param validateAgainstSchema - Optional function to validate against schema
 * @returns The created entity with system fields populated
 *
 * @example
 * ```typescript
 * // Basic create
 * const post = await createEntity(ctx, 'posts', {
 *   title: 'Hello World',
 *   content: 'This is my first post'
 * })
 *
 * // With custom ID
 * const user = await createEntity(ctx, 'users', {
 *   $id: 'admin',
 *   name: 'Admin User',
 *   email: 'admin@example.com'
 * })
 * ```
 */
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

/**
 * Update an existing entity.
 *
 * @param ctx - Write context with required dependencies
 * @param namespace - Collection/namespace
 * @param id - Entity ID
 * @param update - Update operators ($set, $inc, $push, $link, etc.)
 * @param options - Update options (actor, upsert, expectedVersion)
 * @returns The updated entity or null if not found
 *
 * @example
 * ```typescript
 * // Basic update
 * const updated = await updateEntity(ctx, 'posts', 'post-123', {
 *   $set: { title: 'Updated Title' },
 *   $inc: { viewCount: 1 }
 * })
 *
 * // With optimistic concurrency
 * const updated = await updateEntity(ctx, 'posts', 'post-123', {
 *   $set: { status: 'published' }
 * }, {
 *   expectedVersion: 3
 * })
 * ```
 */
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

/**
 * Delete an entity.
 *
 * By default performs a soft delete (sets deletedAt). Use hard: true for permanent deletion.
 *
 * @param ctx - Write context with required dependencies
 * @param namespace - Collection/namespace
 * @param id - Entity ID
 * @param options - Delete options (hard, actor, expectedVersion)
 * @returns Result with deletedCount
 *
 * @example
 * ```typescript
 * // Soft delete
 * const result = await deleteEntity(ctx, 'posts', 'post-123')
 *
 * // Hard delete
 * const result = await deleteEntity(ctx, 'posts', 'post-123', { hard: true })
 * ```
 */
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

/**
 * Delete multiple entities matching a filter.
 *
 * @param ctx - Write context with required dependencies
 * @param namespace - Collection/namespace
 * @param filter - MongoDB-style filter to match entities
 * @param options - Delete options (hard, actor)
 * @returns Result with total deletedCount
 *
 * @example
 * ```typescript
 * // Delete all draft posts
 * const result = await deleteManyEntities(ctx, 'posts', { status: 'draft' })
 *
 * // Hard delete old entries
 * const result = await deleteManyEntities(ctx, 'logs', {
 *   createdAt: { $lt: thirtyDaysAgo }
 * }, { hard: true })
 * ```
 */
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

/**
 * Restore a soft-deleted entity.
 *
 * Removes the deletedAt and deletedBy fields, making the entity active again.
 *
 * @param ctx - Write context with required dependencies
 * @param namespace - Collection/namespace
 * @param id - Entity ID
 * @param options - Restore options (actor)
 * @returns The restored entity or null if not found
 *
 * @example
 * ```typescript
 * const restored = await restoreEntity(ctx, 'posts', 'post-123')
 * ```
 */
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
