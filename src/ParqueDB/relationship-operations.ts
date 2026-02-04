/**
 * ParqueDB Relationship Operations Module
 *
 * Contains operations for managing entity relationships: link/unlink,
 * relationship indexing, reverse lookups, and hydration.
 */

import type {
  Entity,
  EntityData,
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
export function applyRelationshipOperators<T extends EntityData = EntityData>(
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
  options?: { includeDeleted?: boolean | undefined }
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
 * Context for hydration operations.
 * Provides caching and circular reference detection to optimize hydration.
 */
export interface HydrationContext {
  /**
   * Cache of already-hydrated entities by their fullId.
   * Prevents fetching the same entity multiple times during a single hydration pass.
   */
  cache: Map<string, Entity>

  /**
   * Set of entity IDs currently being hydrated.
   * Used to detect and prevent circular reference infinite loops (A -> B -> A).
   */
  visited: Set<string>
}

/**
 * Create a new hydration context for a hydration pass.
 */
export function createHydrationContext(): HydrationContext {
  return {
    cache: new Map(),
    visited: new Set(),
  }
}

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
 * Array with metadata for reverse relationships
 * Allows accessing array methods like `length` and `[0]` while also
 * having $total and $next properties for pagination metadata.
 */
export interface HydratedArray<T> extends Array<T> {
  $total: number
  $next?: string | undefined
}

/**
 * Type utility for hydrated forward relationships.
 * Converts a forward relationship field from EntityId to the full Entity type.
 *
 * @typeParam T - The target entity type
 */
export type HydratedForwardRel<T> = T | null

/**
 * Type utility for hydrated reverse relationships.
 * Converts a reverse relationship field to an array of entities with pagination metadata.
 *
 * @typeParam T - The related entity type
 */
export type HydratedReverseRel<T> = HydratedArray<T>

/**
 * Type for an entity with all relationships hydrated.
 *
 * This is a utility type that transforms relationship fields from their storage format
 * (EntityId strings) to their hydrated format (full Entity objects or arrays).
 *
 * Forward relationships (->): Transformed to Entity<T> | null
 * Reverse relationships (<-[]): Transformed to HydratedArray<Entity<T>>
 *
 * @typeParam T - The base entity data type
 * @typeParam ForwardFields - Keys of fields that are forward relationships
 * @typeParam ReverseFields - Keys of fields that are reverse relationships
 *
 * @example
 * ```typescript
 * interface PostData {
 *   title: string
 *   author: EntityId      // Forward relationship
 * }
 *
 * interface UserData {
 *   name: string
 *   posts: unknown        // Reverse relationship
 * }
 *
 * // The hydrated Post type
 * type HydratedPost = HydratedEntity<PostData, 'author', never>
 * // { title: string; author: Entity<UserData> | null }
 *
 * // The hydrated User type
 * type HydratedUser = HydratedEntity<UserData, never, 'posts'>
 * // { name: string; posts: HydratedArray<Entity<PostData>> }
 * ```
 */
export type HydratedEntity<
  T,
  ForwardFields extends keyof T = never,
  ReverseFields extends keyof T = never
> = Omit<T, ForwardFields | ReverseFields> & {
  [K in ForwardFields]: HydratedForwardRel<Entity>
} & {
  [K in ReverseFields]: HydratedReverseRel<Entity>
}

/**
 * Create a hydrated array with $total and $next metadata.
 */
function createHydratedArray<T>(
  items: T[],
  total: number,
  next?: string
): HydratedArray<T> {
  const arr = [...items] as HydratedArray<T>
  arr.$total = total
  if (next !== undefined) {
    arr.$next = next
  }
  return arr
}

/**
 * Auto-hydrate all relationship fields on an entity based on schema definitions.
 *
 * This is called automatically by get() when depth > 0 (default: 1).
 * It detects all relationship fields from the schema and:
 * - Forward relationships (->): fetches and populates the full related entity
 * - Reverse relationships (<-[]): fetches related entities as an array with $total/$next
 *
 * Features:
 * - **Caching**: Reuses already-fetched entities to avoid redundant lookups
 * - **Circular Reference Detection**: Prevents infinite loops when A -> B -> A
 * - **Batch-Friendly**: Collects all target IDs before fetching for potential batching
 *
 * @param ctx - The relationship operations context
 * @param entity - The entity to auto-hydrate
 * @param fullId - The full entity ID (e.g., "users/123")
 * @param maxInbound - Maximum number of inbound references to include per field
 * @param includeDeleted - Whether to include soft-deleted entities
 * @param hydrationCtx - Optional hydration context for caching and circular detection
 * @returns The entity with all relationship fields auto-hydrated
 */
export function autoHydrateEntity<T>(
  ctx: RelationshipOperationsContext,
  entity: Entity<T>,
  fullId: string,
  maxInbound: number,
  includeDeleted = false,
  hydrationCtx?: HydrationContext
): Entity<T> {
  // Create or reuse hydration context
  const hCtx = hydrationCtx ?? createHydrationContext()

  // Check for circular reference - if we're already hydrating this entity, return it as-is
  if (hCtx.visited.has(fullId)) {
    return entity
  }

  // Mark this entity as being visited to prevent circular hydration
  hCtx.visited.add(fullId)

  // Check if we have a cached hydrated version
  const cached = hCtx.cache.get(fullId)
  if (cached) {
    return cached as Entity<T>
  }

  const hydratedEntity = { ...entity } as Entity<T>
  const typeDef = ctx.schema[entity.$type]

  if (!typeDef) {
    // No schema definition, cache and return entity as-is
    hCtx.cache.set(fullId, hydratedEntity)
    return hydratedEntity
  }

  const mutableHydrated = asMutableEntity(hydratedEntity)

  // Collect all target IDs for forward relationships first (batch-friendly)
  const forwardRelTargets: Array<{
    fieldName: string
    targetId: string
  }> = []

  // First pass: identify all forward relationship targets
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef !== 'string') continue
    if (!fieldDef.startsWith('->')) continue

    const currentValue = asMutableEntity(entity)[fieldName]

    if (currentValue === undefined || currentValue === null) {
      mutableHydrated[fieldName] = null
      continue
    }

    // Handle RelLink object { displayName: entityId }
    if (typeof currentValue === 'object' && !Array.isArray(currentValue)) {
      const relLink = currentValue as Record<string, EntityId>
      const targetIds = Object.values(relLink)
      if (targetIds.length > 0) {
        const targetId = targetIds[0]
        if (typeof targetId === 'string' && targetId.includes('/')) {
          forwardRelTargets.push({ fieldName, targetId })
        }
      }
      continue
    }

    // Handle direct EntityId string
    if (typeof currentValue === 'string' && currentValue.includes('/')) {
      forwardRelTargets.push({ fieldName, targetId: currentValue })
    }
  }

  // Second pass: fetch and hydrate forward relationships (using cache)
  for (const { fieldName, targetId } of forwardRelTargets) {
    // Check cache first
    let targetEntity = hCtx.cache.get(targetId)

    if (!targetEntity) {
      // Fetch from entity store
      targetEntity = ctx.entities.get(targetId) ?? undefined
      if (targetEntity) {
        // Cache the fetched entity
        hCtx.cache.set(targetId, targetEntity)
      }
    }

    if (targetEntity && (!targetEntity.deletedAt || includeDeleted)) {
      mutableHydrated[fieldName] = targetEntity
    } else {
      mutableHydrated[fieldName] = null
    }
  }

  // Third pass: handle reverse relationships
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef !== 'string') continue
    if (!fieldDef.startsWith('<-')) continue

    const parsed = parseReverseRelation(ctx.schema, fieldDef)
    if (!parsed) continue

    // Use reverse index to find related entities
    const sourceIds = getReverseRelatedIds(ctx, fullId, parsed.relatedNs, parsed.relatedField, {
      includeDeleted,
    })

    // Batch fetch related entities (using cache)
    const allRelatedEntities: Entity[] = []
    for (const sourceId of sourceIds) {
      // Check cache first
      let relatedEntity = hCtx.cache.get(sourceId)

      if (!relatedEntity) {
        // Fetch from entity store
        relatedEntity = ctx.entities.get(sourceId) ?? undefined
        if (relatedEntity) {
          // Cache the fetched entity
          hCtx.cache.set(sourceId, relatedEntity)
        }
      }

      if (relatedEntity) {
        allRelatedEntities.push(relatedEntity)
      }
    }

    const totalCount = allRelatedEntities.length
    const limitedEntities = allRelatedEntities.slice(0, maxInbound)
    const nextCursor = totalCount > maxInbound ? String(maxInbound) : undefined

    // Create a hydrated array with $total and $next metadata
    mutableHydrated[fieldName] = createHydratedArray(
      limitedEntities,
      totalCount,
      nextCursor
    )
  }

  // Cache the hydrated entity
  hCtx.cache.set(fullId, hydratedEntity)

  return hydratedEntity
}

/**
 * Return raw entity with relationship fields as EntityId strings (for depth: 0).
 *
 * This ensures forward relationships return their stored EntityId values,
 * and reverse relationships are not populated at all.
 *
 * @param ctx - The relationship operations context
 * @param entity - The entity to process
 * @returns The entity with raw relationship IDs
 */
export function getRawRelationshipEntity<T>(
  ctx: RelationshipOperationsContext,
  entity: Entity<T>
): Entity<T> {
  const rawEntity = { ...entity } as Entity<T>
  const typeDef = ctx.schema[entity.$type]

  if (!typeDef) {
    return rawEntity
  }

  const mutableRaw = asMutableEntity(rawEntity)

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    // Skip meta fields
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef !== 'string') continue

    // Forward relationship: -> Target
    if (fieldDef.startsWith('->')) {
      const currentValue = asMutableEntity(entity)[fieldName]

      if (currentValue === undefined || currentValue === null) {
        // Keep undefined/null as-is for depth: 0
        continue
      }

      // Handle RelLink object { displayName: entityId } - extract the EntityId
      if (typeof currentValue === 'object' && !Array.isArray(currentValue)) {
        const relLink = currentValue as Record<string, EntityId>
        const targetIds = Object.values(relLink)
        if (targetIds.length > 0 && typeof targetIds[0] === 'string') {
          mutableRaw[fieldName] = targetIds[0]
        }
      }
      // If already a string, keep as-is
    }

    // Reverse relationships are not populated at depth: 0
    // They stay as undefined (or whatever the raw value is)
  }

  return rawEntity
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
 * Empty result constant to avoid repeated object creation.
 * Used when no related entities are found.
 */
const EMPTY_RESULT: GetRelatedResult<never> = Object.freeze({
  items: [],
  total: 0,
  hasMore: false,
})

/**
 * Fetch related entities via a forward relationship (->) from an entity.
 *
 * Forward relationships store target entity IDs as values in a relationship field.
 * This function iterates through those IDs and fetches the corresponding entities.
 *
 * @param ctx - The relationship operations context
 * @param entity - The source entity containing the relationship field
 * @param relationField - The name of the relationship field
 * @param includeDeleted - Whether to include soft-deleted entities
 * @returns Array of related entities
 */
function fetchForwardRelatedEntities<T extends EntityData>(
  ctx: RelationshipOperationsContext,
  entity: Entity,
  relationField: string,
  includeDeleted: boolean
): Entity<T>[] {
  const relField = asMutableEntity(entity)[relationField]
  if (!relField || typeof relField !== 'object') {
    return []
  }

  const results: Entity<T>[] = []

  // Batch collect all target IDs first to minimize lookups
  const targetIds = Object.values(relField) as string[]

  for (const targetId of targetIds) {
    const targetEntity = ctx.entities.get(targetId)
    if (targetEntity) {
      // Skip soft-deleted entities unless explicitly requested
      if (targetEntity.deletedAt && !includeDeleted) continue
      results.push(targetEntity as Entity<T>)
    }
  }

  return results
}

/**
 * Fetch related entities via a reverse relationship (<-) from an entity.
 *
 * Reverse relationships find entities that reference the source entity via
 * a specified field. Uses the reverse relationship index for O(1) lookups.
 *
 * @param ctx - The relationship operations context
 * @param fullId - The full ID of the source entity (namespace/id format)
 * @param fieldDef - The relationship definition string (e.g., "<- Post.author[]")
 * @param includeDeleted - Whether to include soft-deleted entities
 * @returns Array of related entities
 */
function fetchReverseRelatedEntities<T extends EntityData>(
  ctx: RelationshipOperationsContext,
  fullId: string,
  fieldDef: string,
  includeDeleted: boolean
): Entity<T>[] {
  const parsed = parseReverseRelation(ctx.schema, fieldDef)
  if (!parsed) {
    return []
  }

  // Use batched ID lookup from reverse index
  const sourceIds = getReverseRelatedIds(ctx, fullId, parsed.relatedNs, parsed.relatedField, {
    includeDeleted,
  })

  const results: Entity<T>[] = []
  for (const sourceId of sourceIds) {
    const relatedEntity = ctx.entities.get(sourceId)
    if (relatedEntity) {
      results.push(relatedEntity as Entity<T>)
    }
  }

  return results
}

/**
 * Apply projection to a list of entities.
 *
 * Projection selects a subset of fields to include in the result,
 * reducing data transfer and memory usage for large entities.
 *
 * @param entities - The entities to project
 * @param projection - Map of field names to include (1 = include)
 * @returns Projected entities with only selected fields
 */
function applyProjection<T extends EntityData>(
  entities: Entity<T>[],
  projection: Record<string, unknown>
): Entity<T>[] {
  return entities.map(entity => {
    // Always include $id in projected results
    const projected: Record<string, unknown> = { $id: entity.$id }
    const mutableE = asMutableEntity(entity)

    for (const [field, include] of Object.entries(projection)) {
      if (include === 1) {
        projected[field] = mutableE[field]
      }
    }

    return projected as Entity<T>
  })
}

/**
 * Get related entities with pagination support.
 *
 * Traverses relationships defined in the schema to return related entities.
 * Supports both forward relationships (`->`) that point from this entity to others,
 * and reverse relationships (`<-`) that point from other entities to this one.
 *
 * ## Forward Relationships (->)
 * Forward relationships store target entity IDs directly on the source entity.
 * Example: `author: '-> User'` means this entity links to a User entity.
 *
 * ## Reverse Relationships (<-)
 * Reverse relationships are computed by looking up which entities reference
 * this one via a specific field. Uses an O(1) reverse index for efficiency.
 * Example: `posts: '<- Post.author[]'` finds all Posts that reference this entity
 * via their `author` field.
 *
 * ## Performance
 * - Forward lookups are O(n) where n is the number of targets
 * - Reverse lookups are O(1) using the reverse relationship index
 * - Filtering is applied after fetching all related entities
 * - Sorting and pagination are applied after filtering
 *
 * @param ctx - The relationship operations context
 * @param namespace - The namespace of the source entity
 * @param id - The ID of the source entity (can be short ID or full EntityId)
 * @param relationField - The relationship field name as defined in the schema
 * @param options - Pagination, filtering, and sorting options
 * @returns Paginated result containing related entities
 *
 * @throws {RelationshipError} When the source entity does not exist
 * @throws {RelationshipError} When the relationship is not defined in schema
 * @throws {RelationshipError} When the field is not a relationship type
 *
 * @example
 * ```typescript
 * // Get posts by an author (reverse relationship)
 * const posts = await getRelatedEntities(ctx, 'users', 'alice@example.com', 'posts')
 *
 * // Get author of a post (forward relationship)
 * const author = await getRelatedEntities(ctx, 'posts', 'my-post', 'author')
 *
 * // With pagination and filtering
 * const publishedPosts = await getRelatedEntities(ctx, 'users', userId, 'posts', {
 *   filter: { status: 'published' },
 *   sort: { createdAt: -1 },
 *   limit: 10,
 * })
 * ```
 */
export async function getRelatedEntities<T extends EntityData = EntityData>(
  ctx: RelationshipOperationsContext,
  namespace: string,
  id: string,
  relationField: string,
  options?: GetRelatedOptions
): Promise<GetRelatedResult<T>> {
  // Validate and normalize inputs
  validateNamespace(namespace)
  const fullId = toFullId(namespace, id)

  // Fetch source entity
  const entity = ctx.entities.get(fullId)
  if (!entity) {
    throw new RelationshipError(
      'GetRelated',
      namespace,
      'Entity not found',
      { entityId: fullId, relationshipName: relationField }
    )
  }

  // Validate relationship field exists in schema
  const typeDef = ctx.schema[entity.$type]
  if (!typeDef || !(relationField in typeDef)) {
    throw new RelationshipError(
      'GetRelated',
      entity.$type,
      `Relationship '${relationField}' is not defined in schema`,
      { entityId: fullId, relationshipName: relationField }
    )
  }

  // Validate field is a relationship type (string starting with -> or <-)
  const fieldDef = typeDef[relationField]
  if (typeof fieldDef !== 'string') {
    throw new RelationshipError(
      'GetRelated',
      entity.$type,
      `Field '${relationField}' is not a relationship`,
      { entityId: fullId, relationshipName: relationField }
    )
  }

  // Fetch related entities based on relationship direction
  const includeDeleted = options?.includeDeleted ?? false
  let relatedEntities: Entity<T>[]

  if (fieldDef.startsWith('->')) {
    // Forward relationship: this entity -> target entities
    relatedEntities = fetchForwardRelatedEntities<T>(ctx, entity, relationField, includeDeleted)
  } else if (fieldDef.startsWith('<-')) {
    // Reverse relationship: other entities -> this entity
    relatedEntities = fetchReverseRelatedEntities<T>(ctx, fullId, fieldDef, includeDeleted)
  } else {
    // Not a recognized relationship syntax
    return EMPTY_RESULT as GetRelatedResult<T>
  }

  // Apply optional filter to narrow results
  if (options?.filter) {
    relatedEntities = relatedEntities.filter(e =>
      canonicalMatchesFilter(e as Entity, options.filter!)
    )
  }

  // Apply optional sorting
  if (options?.sort) {
    sortEntities(relatedEntities, options.sort)
  }

  // Calculate pagination values
  const total = relatedEntities.length
  const limit = options?.limit ?? total
  const cursor = options?.cursor ? parseInt(options.cursor, 10) : 0

  // Apply pagination slice
  const paginatedEntities = relatedEntities.slice(cursor, cursor + limit)
  const hasMore = cursor + limit < total
  const nextCursor = hasMore ? String(cursor + limit) : undefined

  // Apply optional projection to reduce result size
  const resultItems = options?.project
    ? applyProjection(paginatedEntities, options.project)
    : paginatedEntities

  return {
    items: resultItems,
    total,
    hasMore,
    nextCursor,
  }
}
