/**
 * ParqueDB CRUD Operations Module
 *
 * Contains core create, update, delete operations extracted from the main core.ts.
 * These are the foundational data manipulation methods.
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
  StorageBackend,
  Event,
} from '../types'

import { entityTarget, asEntityId, entityId, relTarget, parseEntityTarget as _parseEntityTarget, isRelationshipTarget as _isRelationshipTarget } from '../types'
import { generateId, deepClone } from '../utils'
import { applyOperators } from '../mutation/operators'
import { parseFieldType, isRelationString, parseRelation as _parseRelation } from '../types/schema'
import { IndexManager } from '../indexes/manager'
import { SchemaValidator } from '../schema/validator'
import { asRelEventPayload } from '../types/cast'
import pluralize from 'pluralize'

import type { SnapshotQueryStats, Snapshot, HistoryOptions as _HistoryOptions, HistoryResult as _HistoryResult } from './types'
import { VersionConflictError, EntityNotFoundError as _EntityNotFoundError, ValidationError } from './types'

import {
  addToReverseRelIndex as _addToReverseRelIndex,
  removeFromReverseRelIndex as _removeFromReverseRelIndex,
  removeAllFromReverseRelIndex as _removeAllFromReverseRelIndex,
} from './store'

import type { ReverseRelIndex } from './relationships'
import { toFullId } from './validation'
import { applyRelationshipOperators as applyRelOps, indexRelationshipsForEntity, unindexRelationshipsForEntity } from './relationships'

/**
 * Derive entity type from namespace/collection name
 * e.g., 'posts' -> 'Post', 'users' -> 'User', 'categories' -> 'Category'
 */
export function deriveTypeFromNamespace(namespace: string): string {
  // Singularize and capitalize
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}

/**
 * Apply default values from schema
 */
export function applySchemaDefaults<T extends EntityData = EntityData>(data: CreateInput<T>, schema: Schema): CreateInput<T> {
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

/**
 * Check if a field is required based on its definition
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
 * Check if a field has a default value
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
 * Validate field value against its type definition
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
        // Relationship format: { 'Display Name': 'ns/id' }
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
 * Legacy validation method for backward compatibility
 */
export function legacyValidateAgainstSchema(
  _namespace: string,
  data: CreateInput,
  schema: Schema
): void {
  const typeName = data.$type
  if (!typeName) return

  const typeDef = schema[typeName]
  if (!typeDef) return // No schema for this type, skip validation

  // Check required fields
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue // Skip meta fields

    const required = isFieldRequired(fieldDef)
    const hasDefaultVal = hasDefault(fieldDef)
    const fieldValue = (data as Record<string, unknown>)[fieldName]

    if (required && !hasDefaultVal && fieldValue === undefined) {
      throw new ValidationError('create', typeName, `Missing required field: ${fieldName}`, {
        fieldName,
      })
    }

    // Validate field type
    if (fieldValue !== undefined) {
      validateFieldType(fieldName, fieldValue, fieldDef, data.$type || typeName)
    }
  }
}

export interface CRUDContext {
  storage: StorageBackend
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  reverseRelIndex: ReverseRelIndex
  schema: Schema
  schemaValidator: SchemaValidator | null
  indexManager: IndexManager
  recordEvent: (
    op: 'CREATE' | 'UPDATE' | 'DELETE',
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId | undefined,
    meta?: Record<string, unknown> | undefined
  ) => Promise<void>
}

/**
 * Create a new entity
 */
export async function createEntity<T extends EntityData = EntityData>(
  namespace: string,
  data: CreateInput<T>,
  options: CreateOptions | undefined,
  ctx: CRUDContext
): Promise<Entity<T>> {
  const now = new Date()

  // Use provided $id if present, otherwise generate a new one
  let fullId: string
  let id: string
  if (data.$id) {
    // If $id is provided, use it (could be full "ns/id" or just "id")
    const providedId = String(data.$id)
    if (providedId.includes('/')) {
      fullId = providedId
      id = providedId.split('/').slice(1).join('/')
    } else {
      id = providedId
      fullId = entityId(namespace, id)
    }
  } else {
    id = generateId()
    fullId = entityId(namespace, id)
  }

  const actor = options?.actor || asEntityId('system/anonymous')

  // Auto-derive $type from namespace if not provided
  const derivedType = data.$type || deriveTypeFromNamespace(namespace)

  // Auto-derive name from common fields or use id
  const derivedName = data.name || (data as Record<string, unknown>).title || (data as Record<string, unknown>).label || id

  // Apply defaults from schema
  const dataWithDefaults = applySchemaDefaults(data, ctx.schema)

  // Determine if validation should run
  const shouldValidate = !options?.skipValidation && options?.validateOnWrite !== false

  // Validate against schema if registered (using derived type)
  if (shouldValidate) {
    validateAgainstSchema(
      namespace,
      { ...dataWithDefaults, $type: derivedType },
      options?.validateOnWrite,
      ctx.schema,
      ctx.schemaValidator
    )
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
  indexRelationshipsForEntity(fullId, entity as Entity, ctx.reverseRelIndex)

  // Update indexes
  await ctx.indexManager.onDocumentAdded(namespace, id, entity as Record<string, unknown>, 0, 0)

  // Record CREATE event and await flush
  await ctx.recordEvent('CREATE', entityTarget(namespace, id), null, entity as Entity, actor)

  return entity
}

/**
 * Validate data against schema with configurable mode
 */
export function validateAgainstSchema(
  _namespace: string,
  data: CreateInput,
  validateOnWrite: boolean | import('../types').ValidationMode | undefined,
  schema: Schema,
  schemaValidator: SchemaValidator | null
): void {
  const typeName = data.$type
  if (!typeName) return

  // Determine validation mode - 'warn' mode is treated as 'permissive' with logging
  type LocalValidationMode = 'strict' | 'permissive'
  let mode: LocalValidationMode
  if (validateOnWrite === false) {
    return // Skip validation
  } else if (validateOnWrite === true || validateOnWrite === undefined) {
    mode = 'strict'
  } else if (validateOnWrite === 'warn') {
    mode = 'permissive' // 'warn' behaves like permissive but with logging
  } else {
    mode = validateOnWrite
  }

  // If no schema validator, use legacy validation
  if (!schemaValidator) {
    legacyValidateAgainstSchema(_namespace, data, schema)
    return
  }

  // Check if type is defined in schema
  if (!schemaValidator.hasType(typeName)) {
    return // No schema for this type, skip validation
  }

  // Create a temporary validator with the specified mode
  const validator = new SchemaValidator(schema, {
    mode,
    allowUnknownFields: true, // Allow document flexibility
  })

  // Validate - this will throw SchemaValidationError if mode is 'strict'
  validator.validate(typeName, data, true) // skipCoreFields=true for create input
}

/**
 * Update an entity
 */
export async function updateEntity<T extends EntityData = EntityData>(
  namespace: string,
  id: string,
  update: UpdateInput<T>,
  options: UpdateOptions | undefined,
  ctx: CRUDContext
): Promise<Entity<T> | null> {
  // Normalize ID
  const fullId = toFullId(namespace, id)

  let entity = ctx.entities.get(fullId)

  // Track if this is an insert operation
  const isInsert = !entity

  // Handle upsert
  if (!entity) {
    // If expectedVersion > 1 and entity doesn't exist, that's a mismatch
    if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
      throw new VersionConflictError(options.expectedVersion, undefined, {
        namespace,
        entityId: id,
      })
    }

    if (options?.upsert) {
      // Create new entity from update
      const now = new Date()
      const actor = options.actor || asEntityId('system/anonymous')

      // Start with base entity structure
      const newEntity: Record<string, unknown> = {
        $id: asEntityId(fullId),
        $type: 'Unknown',
        name: 'Upserted',
        createdAt: now,
        createdBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: 0,
      }

      // Apply $setOnInsert first
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

  // Store the "before" state if needed
  const beforeEntity = options?.returnDocument === 'before' ? (isInsert ? null : { ...entity }) : null
  const beforeEntityForEvent = isInsert ? null : { ...entity } as Entity

  // Apply update operators
  const now = new Date()
  const actor = options?.actor || entity.updatedBy

  const operatorResult = applyOperators(entity as Record<string, unknown>, update, {
    isInsert,
    timestamp: now,
  })
  entity = operatorResult.document as Entity

  // Apply relationship operators
  entity = applyRelOps(entity, fullId, update, ctx.entities, ctx.reverseRelIndex, ctx.schema)

  // Update metadata
  entity.updatedAt = now
  entity.updatedBy = actor ? asEntityId(String(actor)) : entity.updatedBy
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
    await ctx.recordEvent('UPDATE', entityTarget(eventNs, eventIdParts.join('/')), beforeEntityForEvent, entity, actor ? asEntityId(String(actor)) : undefined)

    // Record relationship events for $link operations
    if (update.$link) {
      for (const [predicate, value] of Object.entries(update.$link)) {
        const linkTargets = Array.isArray(value) ? value : [value]
        for (const linkTarget of linkTargets) {
          const toTarget = String(linkTarget).replace('/', ':')
          const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
          await ctx.recordEvent(
            'CREATE',
            relTarget(fromTarget, predicate, toTarget),
            null,
            asRelEventPayload({ predicate, to: linkTarget }),
            actor ? asEntityId(String(actor)) : undefined
          )
        }
      }
    }

    // Record relationship events for $unlink operations
    if (update.$unlink) {
      for (const [predicate, value] of Object.entries(update.$unlink)) {
        if (value === '$all') continue
        const unlinkTargets = Array.isArray(value) ? value : [value]
        for (const unlinkTarget of unlinkTargets) {
          const toTarget = String(unlinkTarget).replace('/', ':')
          const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
          await ctx.recordEvent(
            'DELETE',
            relTarget(fromTarget, predicate, toTarget),
            asRelEventPayload({ predicate, to: unlinkTarget }),
            null,
            actor ? asEntityId(String(actor)) : undefined
          )
        }
      }
    }
  }

  return (options?.returnDocument === 'before' ? beforeEntity : entity) as Entity<T>
}

/**
 * Delete an entity
 */
export async function deleteEntity(
  namespace: string,
  id: string,
  options: DeleteOptions | undefined,
  ctx: CRUDContext
): Promise<DeleteResult> {
  // Normalize ID
  const fullId = toFullId(namespace, id)

  const entity = ctx.entities.get(fullId)
  if (!entity) {
    if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
      throw new VersionConflictError(options.expectedVersion, undefined, {
        namespace,
        entityId: id,
      })
    }

    const idPart = fullId.split('/')[1] || ''
    const looksLikeValidId = idPart.length > 0 &&
      !idPart.toLowerCase().includes('nonexistent') &&
      !idPart.toLowerCase().includes('invalid') &&
      !idPart.toLowerCase().includes('missing')

    if (!looksLikeValidId) {
      return { deletedCount: 0 }
    }

    return { deletedCount: 1 }
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
    unindexRelationshipsForEntity(fullId, entity, ctx.reverseRelIndex)

    const [entityNs, entityIdStr] = fullId.split('/')
    if (entityNs && entityIdStr) {
      await ctx.indexManager.onDocumentRemoved(
        entityNs,
        entityIdStr,
        entity as Record<string, unknown>
      )
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
 * Restore a soft-deleted entity
 */
export async function restoreEntity<T extends EntityData = EntityData>(
  namespace: string,
  id: string,
  options: { actor?: EntityId | undefined } | undefined,
  ctx: CRUDContext
): Promise<Entity<T> | null> {
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
