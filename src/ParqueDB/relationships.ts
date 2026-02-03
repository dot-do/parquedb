/**
 * ParqueDB Relationships Module
 *
 * Handles relationship management including:
 * - $link and $unlink operators
 * - Reverse relationship indexing
 * - Entity hydration with relationships
 */

import type { Entity, EntityId, UpdateInput, RelSet } from '../types'
import { asEntityId, entityId as makeEntityId } from '../types'
import { isRelationString, parseRelation } from '../types/schema'
import { DEFAULT_MAX_INBOUND } from '../constants'
import type { Schema } from '../types'
import {
  addToReverseRelIndex,
  removeFromReverseRelIndex,
  removeAllFromReverseRelIndex,
  getFromReverseRelIndex,
  getAllFromReverseRelIndexByNs,
} from './store'
import { RelationshipError } from './types'

/**
 * Reverse relationship index type
 */
export type ReverseRelIndex = Map<string, Map<string, Set<string>>>

/**
 * Apply relationship operators ($link, $unlink) to an entity
 * These operators need entity store access for reverse relationship management.
 *
 * @param entity - The entity to modify
 * @param fullId - The full entity ID (ns/id format)
 * @param update - The update input containing relationship operators
 * @param entities - Entity store map
 * @param reverseRelIndex - Reverse relationship index
 * @param schema - Schema definitions
 * @returns The modified entity
 */
export function applyRelationshipOperators<T = Record<string, unknown>>(
  entity: Entity,
  fullId: string,
  update: UpdateInput<T>,
  entities: Map<string, Entity>,
  reverseRelIndex: ReverseRelIndex,
  schema: Schema
): Entity {
  // $link - add relationships
  if (update.$link) {
    for (const [key, value] of Object.entries(update.$link)) {
      // Validate relationship is defined in schema
      const typeName = entity.$type
      const typeDef = schema[typeName]
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
        const targetEntity = entities.get(targetId as string)
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
      const entityRec = entity as Record<string, unknown>
      if (typeof entityRec[key] !== 'object' || entityRec[key] === null || Array.isArray(entityRec[key])) {
        entityRec[key] = {}
      }

      // For singular relationships, clear existing links first and update reverse index
      if (!isPlural) {
        // Remove old links from reverse index before clearing
        const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
        if (oldLinks && typeof oldLinks === 'object') {
          for (const oldTargetId of Object.values(oldLinks)) {
            if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
              removeFromReverseRelIndex(reverseRelIndex, fullId, key, oldTargetId)
            }
          }
        }
        entityRec[key] = {}
      }

      // Add new links using display name as key
      for (const targetId of values) {
        const targetEntity = entities.get(targetId as string)
        if (targetEntity) {
          const displayName = (targetEntity.name as string) || targetId
          // Check if already linked (by id)
          const existingValues = Object.values(entityRec[key] as Record<string, EntityId>)
          const targetEntityId = asEntityId(String(targetId))
          if (!existingValues.includes(targetEntityId)) {
            ;(entityRec[key] as Record<string, unknown>)[displayName] = targetId
            // Add to reverse index for O(1) reverse lookups
            addToReverseRelIndex(reverseRelIndex, fullId, key, targetId as string)
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
              const targetEntity = entities.get(targetId as string)
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

  // $unlink - remove relationships
  if (update.$unlink) {
    for (const [key, value] of Object.entries(update.$unlink)) {
      const entityRec = entity as Record<string, unknown>
      // Handle $all to remove all links
      if (value === '$all') {
        // Remove all from reverse index before clearing
        const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
        if (oldLinks && typeof oldLinks === 'object') {
          for (const oldTargetId of Object.values(oldLinks)) {
            if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
              removeFromReverseRelIndex(reverseRelIndex, fullId, key, oldTargetId)
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
              // Remove from reverse index
              removeFromReverseRelIndex(reverseRelIndex, fullId, key, targetId as string)
            }
          }
        }

        // Update reverse relationships on target entities
        const typeName = entity.$type
        const typeDef = schema[typeName]
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            if (parsed && parsed.direction === 'forward' && parsed.reverse) {
              for (const targetId of values) {
                const targetEntity = entities.get(targetId as string)
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

/**
 * Index all relationships from an entity into the reverse relationship index.
 * This scans the entity for relationship fields (objects with entity ID values)
 * and adds them to the reverse index for O(1) reverse lookups.
 *
 * @param sourceId - The full entity ID (e.g., "posts/abc")
 * @param entity - The entity to index relationships from
 * @param reverseRelIndex - Reverse relationship index
 */
export function indexRelationshipsForEntity(
  sourceId: string,
  entity: Entity,
  reverseRelIndex: ReverseRelIndex
): void {
  for (const [fieldName, fieldValue] of Object.entries(entity)) {
    // Skip meta fields and non-object values
    if (fieldName.startsWith('$')) continue
    if (!fieldValue || typeof fieldValue !== 'object' || Array.isArray(fieldValue)) continue

    // Check if this looks like a relationship field: { displayName: 'ns/id' }
    for (const targetId of Object.values(fieldValue as Record<string, unknown>)) {
      if (typeof targetId === 'string' && targetId.includes('/')) {
        addToReverseRelIndex(reverseRelIndex, sourceId, fieldName, targetId)
      }
    }
  }
}

/**
 * Remove all relationship indexes for an entity.
 * Call this before deleting an entity or before re-indexing after update.
 *
 * @param sourceId - The full entity ID (e.g., "posts/abc")
 * @param entity - The entity to remove relationship indexes for
 * @param reverseRelIndex - Reverse relationship index
 */
export function unindexRelationshipsForEntity(
  sourceId: string,
  entity: Entity,
  reverseRelIndex: ReverseRelIndex
): void {
  removeAllFromReverseRelIndex(reverseRelIndex, sourceId, entity)
}

/**
 * Hydrate reverse relationship fields for an entity
 * This handles both schema-defined reverse relationships and dynamic lookups.
 */
export function hydrateEntity<T>(
  entity: Entity<T>,
  fullId: string,
  hydrateFields: string[],
  maxInbound: number,
  entities: Map<string, Entity>,
  reverseRelIndex: ReverseRelIndex,
  schema: Schema
): Entity<T> {
  const hydratedEntity = { ...entity } as Entity<T>

  for (const fieldName of hydrateFields) {
    // Look up the schema definition for this entity type
    const typeDef = schema[entity.$type]
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
          const relatedTypeDef = schema[relatedType]
          const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

          // Use reverse relationship index for O(1) lookup instead of scanning all entities
          const sourceIds = getFromReverseRelIndex(reverseRelIndex, fullId, relatedNs, relatedField)

          // Batch load related entities from the index
          const allRelatedEntities: Array<{ name: string; id: EntityId }> = []
          for (const relatedId of sourceIds) {
            const relatedEntity = entities.get(relatedId)
            if (!relatedEntity) continue
            if (relatedEntity.deletedAt) continue // Skip deleted

            allRelatedEntities.push({
              name: relatedEntity.name || relatedId,
              id: asEntityId(relatedId),
            })
          }

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

      // Use reverse relationship index for O(1) lookup
      // Get all fields from this namespace that reference our entity
      const fieldToSourcesMap = getAllFromReverseRelIndexByNs(reverseRelIndex, fullId, relatedNs)

      // Collect all unique source entities across all fields
      const allSourceIds = new Set<string>()
      for (const sourceSet of fieldToSourcesMap.values()) {
        for (const sourceId of sourceSet) {
          allSourceIds.add(sourceId)
        }
      }

      // Batch load related entities
      for (const relatedId of allSourceIds) {
        const relatedEntity = entities.get(relatedId)
        if (!relatedEntity) continue
        if (relatedEntity.deletedAt) continue // Skip deleted

        relatedEntities[relatedEntity.name || relatedId] = asEntityId(relatedId)
      }

      if (Object.keys(relatedEntities).length > 0) {
        ;(hydratedEntity as Record<string, unknown>)[fieldName] = relatedEntities
      }
    }
  }

  return hydratedEntity
}

/**
 * Apply maxInbound limiting to reverse relationship fields
 */
export function applyMaxInbound<T>(
  entity: Entity<T>,
  maxInbound: number,
  schema: Schema
): Entity<T> {
  const resultEntity = { ...entity } as Entity<T>
  const typeDef = schema[entity.$type]

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
          for (const [displayName, entId] of limitedEntries) {
            relSet[displayName] = asEntityId(String(entId))
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

  return resultEntity
}
