/**
 * ParqueDB Relationship Operations Module
 *
 * Contains operations for managing entity relationships: link/unlink,
 * relationship indexing, reverse lookups, and hydration.
 */

import type {
  Entity,
  EntityId,
  UpdateInput,
  Schema,
  RelSet,
} from '../types'

import { isRelationString, parseRelation } from '../types/schema'
import { matchesFilter as canonicalMatchesFilter } from '../query/filter'
import { sortEntities } from '../query/sort'
import { asMutableEntity } from '../types/cast'
// DEFAULT_MAX_INBOUND is used externally from core.ts

import {
  addToReverseRelIndex,
  removeFromReverseRelIndex,
  getFromReverseRelIndex,
  getAllFromReverseRelIndexByNs,
  removeAllFromReverseRelIndex,
} from './store'

import type { GetRelatedOptions, GetRelatedResult } from './types'
import { RelationshipError } from './types'
import { validateNamespace, toFullId } from './validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Context object for relationship operations.
 * Provides access to shared state and dependencies.
 */
export interface RelationshipOperationsContext {
  schema: Schema
  entities: Map<string, Entity>
  reverseRelIndex: Map<string, Map<string, Set<string>>>
}

// =============================================================================
// Relationship Indexing
// =============================================================================

/**
 * Index all relationships from an entity into the reverse relationship index.
 * This scans the entity for relationship fields (objects with entity ID values)
 * and adds them to the reverse index for O(1) reverse lookups.
 *
 * @param ctx - The relationship operations context
 * @param sourceId - The full entity ID (e.g., "posts/abc")
 * @param entity - The entity to index relationships from
 */
export function indexRelationshipsForEntity(
  ctx: RelationshipOperationsContext,
  sourceId: string,
  entity: Entity
): void {
  for (const [fieldName, fieldValue] of Object.entries(entity)) {
    // Skip meta fields and non-object values
    if (fieldName.startsWith('$')) continue
    if (!fieldValue || typeof fieldValue !== 'object' || Array.isArray(fieldValue)) continue

    // Check if this looks like a relationship field: { displayName: 'ns/id' }
    for (const targetId of Object.values(fieldValue as Record<string, unknown>)) {
      if (typeof targetId === 'string' && targetId.includes('/')) {
        addToReverseRelIndex(ctx.reverseRelIndex, sourceId, fieldName, targetId)
      }
    }
  }
}

/**
 * Remove all relationship indexes for an entity.
 * Call this before deleting an entity or before re-indexing after update.
 *
 * @param ctx - The relationship operations context
 * @param sourceId - The full entity ID (e.g., "posts/abc")
 * @param entity - The entity to remove relationship indexes for
 */
export function unindexRelationshipsForEntity(
  ctx: RelationshipOperationsContext,
  sourceId: string,
  entity: Entity
): void {
  removeAllFromReverseRelIndex(ctx.reverseRelIndex, sourceId, entity)
}

// =============================================================================
// Relationship Operators
// =============================================================================

/**
 * Apply relationship operators ($link, $unlink) to an entity.
 * These operators need entity store access for reverse relationship management.
 *
 * @param ctx - The relationship operations context
 * @param entity - The entity to modify
 * @param fullId - The full entity ID (ns/id format)
 * @param update - The update input containing relationship operators
 * @returns The modified entity
 */
export function applyRelationshipOperators<T = Record<string, unknown>>(
  ctx: RelationshipOperationsContext,
  entity: Entity,
  fullId: string,
  update: UpdateInput<T>
): Entity {
  // $link - add relationships
  if (update.$link) {
    for (const [key, value] of Object.entries(update.$link)) {
      // Validate relationship is defined in schema
      const typeName = entity.$type
      const typeDef = ctx.schema[typeName]
      if (typeDef) {
        const fieldDef = typeDef[key]
        if (fieldDef === undefined || (typeof fieldDef === 'string' && !isRelationString(fieldDef))) {
          throw new RelationshipError(
            'Link',
            typeName,
            'Relationship is not defined in schema',
            { relationshipName: key }
          )
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
        const targetEntity = ctx.entities.get(targetId as string)
        if (!targetEntity) {
          throw new RelationshipError(
            'Link',
            typeName,
            'Target entity does not exist',
            { entityId: entity.$id as string, relationshipName: key, targetId: targetId as string }
          )
        }
        if (targetEntity.deletedAt) {
          throw new RelationshipError(
            'Link',
            typeName,
            'Cannot link to deleted entity',
            { entityId: entity.$id as string, relationshipName: key, targetId: targetId as string }
          )
        }
      }

      // Initialize field as object if not already
      const entityRec = asMutableEntity(entity)
      if (typeof entityRec[key] !== 'object' || entityRec[key] === null || Array.isArray(entityRec[key])) {
        entityRec[key] = {}
      }

      // For singular relationships, clear existing links first and update reverse index
      if (!isPlural) {
        const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
        if (oldLinks && typeof oldLinks === 'object') {
          for (const oldTargetId of Object.values(oldLinks)) {
            if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
              removeFromReverseRelIndex(ctx.reverseRelIndex, fullId, key, oldTargetId)
            }
          }
        }
        entityRec[key] = {}
      }

      // Add new links using display name as key
      const relLinks = entityRec[key] as Record<string, EntityId>
      for (const targetId of values) {
        const targetEntity = ctx.entities.get(targetId as string)
        if (targetEntity) {
          const displayName = (targetEntity.name as string) || targetId
          const existingValues = Object.values(relLinks)
          if (!existingValues.includes(targetId as EntityId)) {
            relLinks[displayName] = targetId as EntityId
            addToReverseRelIndex(ctx.reverseRelIndex, fullId, key, targetId as string)
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
              const targetEntity = ctx.entities.get(targetId as string)
              if (targetEntity) {
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

  // $unlink - remove relationships
  if (update.$unlink) {
    for (const [key, value] of Object.entries(update.$unlink)) {
      const entityRec = asMutableEntity(entity)

      // Handle $all to remove all links
      if (value === '$all') {
        const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
        if (oldLinks && typeof oldLinks === 'object') {
          for (const oldTargetId of Object.values(oldLinks)) {
            if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
              removeFromReverseRelIndex(ctx.reverseRelIndex, fullId, key, oldTargetId)
            }
          }
        }
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
              removeFromReverseRelIndex(ctx.reverseRelIndex, fullId, key, targetId as string)
            }
          }
        }

        // Update reverse relationships on target entities
        const typeName = entity.$type
        const typeDef = ctx.schema[typeName]
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            if (parsed && parsed.direction === 'forward' && parsed.reverse) {
              for (const targetId of values) {
                const targetEntity = ctx.entities.get(targetId as string)
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

  return entity
}

// =============================================================================
// Reverse Relationship Parsing and Lookup
// =============================================================================

/**
 * Parse a reverse relationship definition string.
 *
 * Parses strings like "<- Post.author[]" into their components:
 * - relatedType: The type name (e.g., "Post")
 * - relatedField: The field name on that type (e.g., "author")
 * - relatedNs: The namespace for the related type (from schema or lowercased type)
 *
 * @param schema - The schema definition
 * @param fieldDef - The relationship definition string (e.g., "<- Post.author[]")
 * @returns Object with relatedNs and relatedField, or null if parse fails
 */
export function parseReverseRelation(
  schema: Schema,
  fieldDef: string
): { relatedNs: string; relatedField: string } | null {
  const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
  if (!match) return null

  const [, relatedType, relatedField] = match
  if (!relatedType || !relatedField) return null

  const relatedTypeDef = schema[relatedType]
  const relatedNs = (relatedTypeDef?.$ns as string) || relatedType.toLowerCase()

  return { relatedNs, relatedField }
}

/**
 * Get source entity IDs from a reverse relationship.
 *
 * Uses the reverse relationship index for O(1) lookup to find all entities
 * that reference the given target entity via the specified relationship.
 *
 * @param ctx - The relationship operations context
 * @param targetId - The full ID of the target entity (e.g., "users/123")
 * @param relatedNs - The namespace of the source entities
 * @param relatedField - The field name on source entities that points to target
 * @param options - Optional settings for filtering (includeDeleted)
 * @returns Array of source entity IDs
 */
export function getReverseRelatedIds(
  ctx: RelationshipOperationsContext,
  targetId: string,
  relatedNs: string,
  relatedField: string,
  options?: { includeDeleted?: boolean }
): string[] {
  const sourceIds = getFromReverseRelIndex(ctx.reverseRelIndex, targetId, relatedNs, relatedField)
  const result: string[] = []

  for (const sourceId of sourceIds) {
    const entity = ctx.entities.get(sourceId)
    if (!entity) continue
    if (entity.deletedAt && !options?.includeDeleted) continue
    result.push(sourceId)
  }

  return result
}

// =============================================================================
// Entity Hydration
// =============================================================================

/**
 * Hydrate reverse relationship fields for an entity.
 *
 * This method populates relationship fields with actual entity references,
 * handling both schema-defined reverse relationships (using "<-" syntax)
 * and dynamic lookups for relationships not defined in the schema.
 *
 * @param ctx - The relationship operations context
 * @param entity - The entity to hydrate with relationship data
 * @param fullId - The full entity ID (e.g., "users/123")
 * @param hydrateFields - Array of field names to hydrate
 * @param maxInbound - Maximum number of inbound references to include per field
 * @returns The entity with hydrated relationship fields
 */
export function hydrateEntity<T>(
  ctx: RelationshipOperationsContext,
  entity: Entity<T>,
  fullId: string,
  hydrateFields: string[],
  maxInbound: number
): Entity<T> {
  const hydratedEntity = { ...entity } as Entity<T>

  for (const fieldName of hydrateFields) {
    const typeDef = ctx.schema[entity.$type]
    let handled = false

    if (typeDef && typeDef[fieldName]) {
      const fieldDef = typeDef[fieldName]
      if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
        const parsed = parseReverseRelation(ctx.schema, fieldDef)
        if (parsed) {
          handled = true

          const sourceIds = getReverseRelatedIds(ctx, fullId, parsed.relatedNs, parsed.relatedField)

          const allRelatedEntities: Array<{ name: string; id: EntityId }> = []
          for (const relatedId of sourceIds) {
            const relatedEntity = ctx.entities.get(relatedId)
            if (relatedEntity) {
              allRelatedEntities.push({
                name: relatedEntity.name || relatedId,
                id: relatedId as EntityId,
              })
            }
          }

          const totalCount = allRelatedEntities.length
          const limitedEntities = allRelatedEntities.slice(0, maxInbound)

          const mutableHydrated = asMutableEntity(hydratedEntity)
          if (totalCount === 0) {
            mutableHydrated[fieldName] = { $count: 0 }
          } else {
            const relSet: RelSet = {
              $count: totalCount,
            }

            for (const related of limitedEntities) {
              relSet[related.name] = related.id
            }

            if (totalCount > maxInbound) {
              relSet.$next = String(maxInbound)
            }

            mutableHydrated[fieldName] = relSet
          }
        }
      }
    }

    // Dynamic reverse relationship lookup (no schema definition)
    if (!handled) {
      const relatedEntities: Record<string, EntityId> = {}
      const relatedNs = fieldName.toLowerCase()

      const fieldToSourcesMap = getAllFromReverseRelIndexByNs(ctx.reverseRelIndex, fullId, relatedNs)

      const allSourceIds = new Set<string>()
      for (const sourceSet of fieldToSourcesMap.values()) {
        for (const sourceId of sourceSet) {
          allSourceIds.add(sourceId)
        }
      }

      for (const relatedId of allSourceIds) {
        const relatedEntity = ctx.entities.get(relatedId)
        if (!relatedEntity) continue
        if (relatedEntity.deletedAt) continue

        relatedEntities[relatedEntity.name || relatedId] = relatedId as EntityId
      }

      if (Object.keys(relatedEntities).length > 0) {
        asMutableEntity(hydratedEntity)[fieldName] = relatedEntities
      }
    }
  }

  return hydratedEntity
}

/**
 * Apply maxInbound limiting to reverse relationship fields.
 *
 * @param ctx - The relationship operations context
 * @param entity - The entity to apply limiting to
 * @param maxInbound - Maximum number of inbound references to include
 * @returns The entity with limited relationship fields
 */
export function applyMaxInboundToEntity<T>(
  ctx: RelationshipOperationsContext,
  entity: Entity<T>,
  maxInbound: number
): Entity<T> {
  const resultEntity = { ...entity } as Entity<T>
  const typeDef = ctx.schema[entity.$type]

  if (typeDef) {
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
        const mutableResult = asMutableEntity(resultEntity)
        const currentField = asMutableEntity(entity)[fieldName]
        if (currentField && typeof currentField === 'object' && !Array.isArray(currentField)) {
          const entries = Object.entries(currentField).filter(([key]) => !key.startsWith('$'))
          const totalCount = entries.length

          const relSet: RelSet = { $count: totalCount }

          const limitedEntries = entries.slice(0, maxInbound)
          for (const [displayName, entityId] of limitedEntries) {
            relSet[displayName] = entityId as EntityId
          }

          if (totalCount > maxInbound) {
            relSet.$next = String(maxInbound)
          }

          mutableResult[fieldName] = relSet
        } else {
          mutableResult[fieldName] = { $count: 0 }
        }
      }
    }
  }

  return resultEntity
}

// =============================================================================
// Get Related Operation
// =============================================================================

/**
 * Get related entities with pagination support.
 * Supports both forward (->) and reverse (<-) relationships.
 *
 * @param ctx - The relationship operations context
 * @param namespace - The namespace of the source entity
 * @param id - The ID of the source entity
 * @param relationField - The relationship field name
 * @param options - Pagination and filtering options
 * @returns Paginated related entities
 */
export async function getRelatedEntities<T = Record<string, unknown>>(
  ctx: RelationshipOperationsContext,
  namespace: string,
  id: string,
  relationField: string,
  options?: GetRelatedOptions
): Promise<GetRelatedResult<T>> {
  validateNamespace(namespace)

  const fullId = toFullId(namespace, id)
  const entity = ctx.entities.get(fullId)
  if (!entity) {
    return { items: [], total: 0, hasMore: false }
  }

  const typeDef = ctx.schema[entity.$type]
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
    const relField = asMutableEntity(entity)[relationField]
    if (relField && typeof relField === 'object') {
      for (const [, targetId] of Object.entries(relField)) {
        const targetEntity = ctx.entities.get(targetId as string)
        if (targetEntity) {
          if (targetEntity.deletedAt && !options?.includeDeleted) continue
          allRelatedEntities.push(targetEntity as Entity<T>)
        }
      }
    }
  } else if (fieldDef.startsWith('<-')) {
    const parsed = parseReverseRelation(ctx.schema, fieldDef)
    if (!parsed) {
      return { items: [], total: 0, hasMore: false }
    }

    const sourceIds = getReverseRelatedIds(ctx, fullId, parsed.relatedNs, parsed.relatedField, {
      includeDeleted: options?.includeDeleted,
    })

    for (const sourceId of sourceIds) {
      const relatedEntity = ctx.entities.get(sourceId)
      if (relatedEntity) {
        allRelatedEntities.push(relatedEntity as Entity<T>)
      }
    }
  } else {
    return { items: [], total: 0, hasMore: false }
  }

  // Apply filter if provided
  let filteredEntities = allRelatedEntities
  if (options?.filter) {
    filteredEntities = allRelatedEntities.filter(e => canonicalMatchesFilter(e as Entity, options.filter!))
  }

  // Apply sorting
  if (options?.sort) {
    sortEntities(filteredEntities, options.sort)
  }

  const total = filteredEntities.length
  const limit = options?.limit ?? total
  const cursor = options?.cursor ? parseInt(options.cursor, 10) : 0

  const paginatedEntities = filteredEntities.slice(cursor, cursor + limit)
  const hasMore = cursor + limit < total
  const nextCursor = hasMore ? String(cursor + limit) : undefined

  // Apply projection if provided
  let resultItems = paginatedEntities
  if (options?.project) {
    resultItems = paginatedEntities.map(e => {
      const projected: Record<string, unknown> = { $id: e.$id }
      const mutableE = asMutableEntity(e)
      for (const field of Object.keys(options.project!)) {
        if (options.project![field] === 1) {
          projected[field] = mutableE[field]
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
