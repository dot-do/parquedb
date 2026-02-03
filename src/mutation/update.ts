/**
 * Update Operation Module for ParqueDB Mutation Layer
 *
 * Provides functions for updating entities with operator application,
 * optimistic concurrency, relationship management, and event generation.
 */

import type {
  Entity,
  EntityId,
  UpdateInput,
  Schema,
} from '../types'
import {
  MutationContext,
  UpdateResult,
  MutationEvent,
  MutationOperationError,
  MutationErrorCodes,
  RelationshipOperation,
} from './types'
import { applyOperators, validateUpdateOperators } from './operators'
import { entityTarget, relTarget } from '../types/entity'
import { isRelationString, parseRelation } from '../types/schema'

// =============================================================================
// Update Operation
// =============================================================================

/**
 * Options for the update operation
 */
export interface UpdateOperationOptions {
  /** Schema for relationship validation */
  schema?: Schema

  /** Expected version for optimistic concurrency */
  expectedVersion?: number

  /** Create if not exists */
  upsert?: boolean

  /** Return document before or after update */
  returnDocument?: 'before' | 'after'

  /** Function to get entity by ID */
  getEntity?: (id: string) => Entity | undefined

  /** Function to set entity by ID */
  setEntity?: (id: string, entity: Entity) => void
}

/**
 * Execute an update operation
 *
 * @param context - Mutation context
 * @param entityId - Full entity ID (ns/id)
 * @param update - Update operators to apply
 * @param existingEntity - The existing entity (or undefined if not found)
 * @param options - Update operation options
 * @returns Update result with entity and events
 */
export function executeUpdate<T = Record<string, unknown>>(
  context: MutationContext,
  entityId: string,
  update: UpdateInput<T>,
  existingEntity: Entity | undefined,
  options?: UpdateOperationOptions
): UpdateResult<T> {
  // Validate update operators
  validateUpdateOperators(update)

  const events: MutationEvent[] = []
  let entity = existingEntity ? { ...existingEntity } : undefined
  const isInsert = !entity

  // Handle upsert
  if (!entity) {
    if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
      throw new VersionConflictError(options.expectedVersion, undefined)
    }

    if (options?.upsert) {
      // Create new entity for upsert
      entity = createUpsertEntity(context, entityId)
    } else {
      return {
        entity: null,
        modified: false,
        upserted: false,
        events: [],
      }
    }
  }

  // Check version for optimistic concurrency
  if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
    throw new VersionConflictError(options.expectedVersion, entity.version)
  }

  // Store the "before" state
  const beforeEntity = isInsert ? null : { ...entity }

  // Apply update operators
  const applyResult = applyOperators(entity as Record<string, unknown>, update, {
    isInsert,
    timestamp: context.timestamp,
  })

  // Update entity with result
  entity = applyResult.document as Entity

  // Process relationship operations
  if (applyResult.relationshipOps.length > 0 && options?.schema) {
    const relEvents = processRelationshipOps(
      context,
      entityId,
      entity,
      applyResult.relationshipOps,
      options.schema,
      options.getEntity,
      options.setEntity
    )
    events.push(...relEvents)
  }

  // Update metadata
  entity.updatedAt = context.timestamp
  entity.updatedBy = context.actor
  entity.version = (entity.version ?? 0) + 1

  // Generate UPDATE event
  const [ns, ...idParts] = entityId.split('/')
  const id = idParts.join('/')
  if (ns) {
    events.unshift({
      op: isInsert ? 'CREATE' : 'UPDATE',
      target: entityTarget(ns, id),
      before: beforeEntity as Record<string, unknown> | null,
      after: entity as Record<string, unknown>,
      actor: context.actor,
      timestamp: context.timestamp,
    })
  }

  // Determine which entity state to return
  const returnEntity = options?.returnDocument === 'before'
    ? (isInsert ? null : beforeEntity)
    : entity

  return {
    entity: returnEntity as Entity<T> | null,
    modified: true,
    upserted: isInsert,
    events,
  }
}

/**
 * Create a base entity for upsert operations
 */
function createUpsertEntity(context: MutationContext, entityId: string): Entity {
  return {
    $id: entityId as EntityId,
    $type: 'Unknown',
    name: 'Upserted',
    createdAt: context.timestamp,
    createdBy: context.actor,
    updatedAt: context.timestamp,
    updatedBy: context.actor,
    version: 0, // Will be incremented to 1
  } as Entity
}

// =============================================================================
// Relationship Processing
// =============================================================================

/**
 * Process relationship operations from update
 */
function processRelationshipOps(
  context: MutationContext,
  entityId: string,
  entity: Entity,
  ops: RelationshipOperation[],
  schema: Schema,
  getEntity?: (id: string) => Entity | undefined,
  setEntity?: (id: string, entity: Entity) => void
): MutationEvent[] {
  const events: MutationEvent[] = []
  const typeName = entity.$type
  const typeDef = schema[typeName]

  for (const op of ops) {
    const { type, predicate, targets } = op

    if (type === 'link') {
      // Validate relationship is defined in schema
      if (typeDef) {
        const fieldDef = typeDef[predicate]
        if (fieldDef === undefined || (typeof fieldDef === 'string' && !isRelationString(fieldDef))) {
          throw new MutationOperationError(
            MutationErrorCodes.RELATIONSHIP_ERROR,
            `Relationship '${predicate}' is not defined in schema for type '${typeName}'`
          )
        }
      }

      // Check if singular or plural relationship
      let isPlural = true
      if (typeDef) {
        const fieldDef = typeDef[predicate]
        if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
          const parsed = parseRelation(fieldDef)
          isPlural = parsed?.isArray ?? true
        }
      }

      // Validate targets exist (if getEntity provided)
      if (getEntity) {
        for (const targetId of targets) {
          const targetEntity = getEntity(targetId)
          if (!targetEntity) {
            throw new MutationOperationError(
              MutationErrorCodes.RELATIONSHIP_ERROR,
              `Target entity '${targetId}' does not exist`
            )
          }
          if (targetEntity.deletedAt) {
            throw new MutationOperationError(
              MutationErrorCodes.RELATIONSHIP_ERROR,
              `Cannot link to deleted entity '${targetId}'`
            )
          }
        }
      }

      // Initialize relationship field
      if (typeof entity[predicate] !== 'object' || entity[predicate] === null || Array.isArray(entity[predicate])) {
        (entity as Record<string, unknown>)[predicate] = {}
      }

      // For singular relationships, clear existing links first
      if (!isPlural) {
        (entity as Record<string, unknown>)[predicate] = {}
      }

      // Add links
      for (const targetId of targets) {
        const targetEntity = getEntity?.(targetId)
        const displayName = targetEntity?.name || targetId
        const existingValues = Object.values((entity as Record<string, unknown>)[predicate] as Record<string, EntityId>)

        if (!existingValues.includes(targetId as EntityId)) {
          ((entity as Record<string, unknown>)[predicate] as Record<string, unknown>)[displayName] = targetId
        }

        // Update reverse relationships
        if (typeDef && setEntity && getEntity) {
          const fieldDef = typeDef[predicate]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            if (parsed && parsed.direction === 'forward' && parsed.reverse) {
              const target = getEntity(targetId)
              if (target) {
                if (typeof target[parsed.reverse] !== 'object' || target[parsed.reverse] === null) {
                  (target as Record<string, unknown>)[parsed.reverse] = {}
                }
                const reverseRel = target[parsed.reverse] as Record<string, EntityId>
                const entityDisplayName = entity.name || entityId
                if (!Object.values(reverseRel).includes(entityId as EntityId)) {
                  reverseRel[entityDisplayName] = entityId as EntityId
                }
                setEntity(targetId, target)
              }
            }
          }
        }

        // Generate link event
        const [ns, ...idParts] = entityId.split('/')
        const id = idParts.join('/')
        const toTarget = targetId.replace('/', ':')
        const fromTarget = entityTarget(ns || '', id)

        events.push({
          op: 'CREATE',
          target: relTarget(fromTarget, predicate, toTarget),
          before: null,
          after: { predicate, to: targetId },
          actor: context.actor,
          timestamp: context.timestamp,
          isRelationship: true,
          predicate,
        })
      }
    } else if (type === 'unlink') {
      const currentRel = (entity as Record<string, unknown>)[predicate]

      if (targets.length === 0) {
        // Unlink all
        (entity as Record<string, unknown>)[predicate] = {}
      } else if (currentRel && typeof currentRel === 'object' && !Array.isArray(currentRel)) {
        // Unlink specific targets
        for (const targetId of targets) {
          for (const [displayName, id] of Object.entries(currentRel as Record<string, EntityId>)) {
            if (id === targetId) {
              delete (currentRel as Record<string, EntityId>)[displayName]
            }
          }

          // Update reverse relationships
          if (typeDef && setEntity && getEntity) {
            const fieldDef = typeDef[predicate]
            if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
              const parsed = parseRelation(fieldDef)
              if (parsed && parsed.direction === 'forward' && parsed.reverse) {
                const target = getEntity(targetId)
                if (target && target[parsed.reverse]) {
                  const reverseRel = target[parsed.reverse] as Record<string, EntityId>
                  for (const [dName, rid] of Object.entries(reverseRel)) {
                    if (rid === entityId) {
                      delete reverseRel[dName]
                    }
                  }
                  setEntity(targetId, target)
                }
              }
            }
          }

          // Generate unlink event
          const [ns, ...idParts] = entityId.split('/')
          const id = idParts.join('/')
          const toTarget = targetId.replace('/', ':')
          const fromTarget = entityTarget(ns || '', id)

          events.push({
            op: 'DELETE',
            target: relTarget(fromTarget, predicate, toTarget),
            before: { predicate, to: targetId },
            after: null,
            actor: context.actor,
            timestamp: context.timestamp,
            isRelationship: true,
            predicate,
          })
        }
      }
    }
  }

  return events
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
