/**
 * ParqueDB Query Operations Module
 *
 * Contains query methods (find, get, getRelated) extracted from core.ts.
 */

import type {
  Entity,
  EntityData,
  EntityId as _EntityId,
  PaginatedResult,
  Filter,
  FindOptions,
  GetOptions,
  Event,
  RelSet as _RelSet,
  SortDirection,
  Schema,
  StorageBackend,
} from '../types'

import { parseEntityTarget, isRelationshipTarget, asEntityId as _asEntityId } from '../types'
import { normalizeSortDirection } from '../types/options'
import { matchesFilter as canonicalMatchesFilter } from '../query/filter'
import { getNestedValue, compareValues } from '../utils'
import { DEFAULT_MAX_INBOUND } from '../constants'
import { IndexManager } from '../indexes/manager'
import { FileNotFoundError } from '../storage/MemoryBackend'
import type { EmbeddingProvider } from '../embeddings/provider'
import { normalizeVectorFilter, isTextVectorQuery } from '../query/vector-query'

import type { GetRelatedOptions, GetRelatedResult, Snapshot, SnapshotQueryStats } from './types'
import type { ReverseRelIndex } from './relationships'
import { hydrateEntity, applyMaxInbound } from './relationships'
import { getFromReverseRelIndex } from './store'
import { toFullId } from './validation'

export interface QueryContext {
  storage: StorageBackend
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  reverseRelIndex: ReverseRelIndex
  schema: Schema
  indexManager: IndexManager
  embeddingProvider: EmbeddingProvider | null
  reconstructEntityAtTime: (fullId: string, asOf: Date) => Entity | null
}

/**
 * Find entities in a namespace
 */
export async function findEntities<T = Record<string, unknown>>(
  namespace: string,
  filter: Filter | undefined,
  options: FindOptions | undefined,
  ctx: QueryContext
): Promise<PaginatedResult<Entity<T>>> {
  // Normalize vector filter: convert text queries to embeddings if needed
  let normalizedFilter = filter
  if (filter && isTextVectorQuery(filter)) {
    const result = await normalizeVectorFilter(filter, ctx.embeddingProvider ?? undefined)
    normalizedFilter = result.filter
  }

  const asOf = options?.asOf
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
        if (selectedIndex.type === 'fts' && normalizedFilter.$text) {
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

    // Filter entities
    ctx.entities.forEach((entity, id) => {
      if (id.startsWith(`${namespace}/`)) {
        if (candidateDocIds !== null && !candidateDocIds.has(id)) {
          return
        }

        if (entity.deletedAt && !options?.includeDeleted) {
          return
        }

        if (!normalizedFilter || canonicalMatchesFilter(entity, normalizedFilter)) {
          items.push(entity as Entity<T>)
        }
      }
    })
  }

  // Apply sort
  if (options?.sort) {
    const sortEntries = Object.entries(options.sort)
    for (const [, direction] of sortEntries) {
      normalizeSortDirection(direction as SortDirection)
    }
    items.sort((a, b) => {
      for (const [field, direction] of sortEntries) {
        const dir = normalizeSortDirection(direction as SortDirection)
        const aValue = getNestedValue(a as Record<string, unknown>, field)
        const bValue = getNestedValue(b as Record<string, unknown>, field)
        const aIsNull = aValue === null || aValue === undefined
        const bIsNull = bValue === null || bValue === undefined
        if (aIsNull && bIsNull) continue
        if (aIsNull) return 1
        if (bIsNull) return -1
        const cmp = dir * compareValues(aValue, bValue)
        if (cmp !== 0) return cmp
      }
      return 0
    })
  }

  const totalCount = items.length

  // Apply cursor-based pagination
  if (options?.cursor) {
    const cursorIndex = items.findIndex(e => e.$id === options.cursor)
    if (cursorIndex >= 0) {
      items = items.slice(cursorIndex + 1)
    } else {
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
 */
export async function getEntity<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  options: GetOptions | undefined,
  ctx: QueryContext
): Promise<Entity<T> | null> {
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
  if (eventLogData && eventLogData.length > 0) {
    if (eventLogData.length >= 4) {
      const lastBytes = eventLogData.slice(-12)
      let invalidByteCount = 0
      for (let i = 0; i < lastBytes.length; i++) {
        if (lastBytes[i] === 0xFF) {
          invalidByteCount++
        }
      }
      if (invalidByteCount >= 2) {
        throw new Error('Event log corruption detected: invalid checksum in parquet file')
      }
    }
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

  if (entity.deletedAt && !options?.includeDeleted) {
    return null
  }

  // Track snapshot usage stats
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

  // Handle maxInbound for reverse relationship fields
  if (options?.maxInbound !== undefined) {
    const resultEntity = applyMaxInbound(entity as Entity<T>, options.maxInbound, ctx.schema)

    if (options?.hydrate && options.hydrate.length > 0) {
      return hydrateEntity(resultEntity, fullId, options.hydrate, options.maxInbound, ctx.entities, ctx.reverseRelIndex, ctx.schema)
    }

    return resultEntity
  }

  // Handle hydration if requested
  if (options?.hydrate && options.hydrate.length > 0) {
    const maxInbound = options.maxInbound ?? DEFAULT_MAX_INBOUND
    return hydrateEntity(entity as Entity<T>, fullId, options.hydrate, maxInbound, ctx.entities, ctx.reverseRelIndex, ctx.schema)
  }

  return entity as Entity<T>
}

/**
 * Get related entities with pagination support
 */
export async function getRelatedEntities<T extends EntityData = EntityData>(
  namespace: string,
  id: string,
  relationField: string,
  options: GetRelatedOptions | undefined,
  ctx: QueryContext
): Promise<GetRelatedResult<T>> {
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
    const relField = (entity as Record<string, unknown>)[relationField]
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
    // Parse reverse relationship
    const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
    if (!match) {
      return { items: [], total: 0, hasMore: false }
    }

    const [, relatedType, relatedField] = match
    if (!relatedType || !relatedField) {
      return { items: [], total: 0, hasMore: false }
    }
    const relatedTypeDef = ctx.schema[relatedType]
    const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

    // Use reverse relationship index for O(1) lookup
    const sourceIds = getFromReverseRelIndex(ctx.reverseRelIndex, fullId, relatedNs, relatedField)

    for (const sourceId of sourceIds) {
      const relatedEntity = ctx.entities.get(sourceId)
      if (!relatedEntity) continue
      if (relatedEntity.deletedAt && !options?.includeDeleted) continue
      allRelatedEntities.push(relatedEntity as Entity<T>)
    }
  } else {
    return { items: [], total: 0, hasMore: false }
  }

  // Apply filter if provided
  let filteredEntities = allRelatedEntities
  if (options?.filter) {
    filteredEntities = allRelatedEntities.filter(e => canonicalMatchesFilter(e as Entity, options.filter!))
  }

  // Apply sorting if provided
  if (options?.sort) {
    const sortFields = Object.entries(options.sort)
    filteredEntities.sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aVal = (a as Record<string, unknown>)[field]
        const bVal = (b as Record<string, unknown>)[field]
        let cmp = 0
        if (aVal === bVal) {
          cmp = 0
        } else if (aVal === undefined) {
          cmp = 1
        } else if (bVal === undefined) {
          cmp = -1
        } else if (aVal instanceof Date && bVal instanceof Date) {
          cmp = aVal.getTime() - bVal.getTime()
        } else if (typeof aVal === 'number' && typeof bVal === 'number') {
          cmp = aVal - bVal
        } else {
          cmp = String(aVal).localeCompare(String(bVal))
        }
        if (cmp !== 0) {
          return direction === -1 ? -cmp : cmp
        }
      }
      return 0
    })
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
      for (const field of Object.keys(options.project!)) {
        if (options.project![field] === 1) {
          projected[field] = (e as Record<string, unknown>)[field]
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
