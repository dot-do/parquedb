/**
 * ParqueDB Read Path Module
 *
 * Contains read operations for entities: find, get, query.
 * These functions operate on entity state through a context object for dependency injection.
 *
 * @module ParqueDB/read-path
 */

import type {
  Entity,
  EntityData,
  PaginatedResult,
  Filter,
  FindOptions,
  GetOptions,
  StorageBackend,
  Event,
} from '../types'

import { parseEntityTarget, isRelationshipTarget } from '../types'
import { FileNotFoundError } from '../storage/MemoryBackend'
import { matchesFilter as canonicalMatchesFilter } from '../query/filter'
import { sortEntities } from '../query/sort'

import type { IndexManager } from '../indexes/manager'

import type {
  Snapshot,
  SnapshotQueryStats,
} from './types'

import { validateNamespace, validateFilter, toFullId } from './validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Context for read/query operations.
 *
 * Provides access to all shared state needed for read operations.
 * This context is designed for minimal coupling - it only includes
 * what's needed for reading entities.
 */
export interface ReadPathContext {
  /** Storage backend for reading raw data */
  storage: StorageBackend
  /** In-memory entity store */
  entities: Map<string, Entity>
  /** Event log for time-travel queries */
  events: Event[]
  /** Snapshot store for efficient point-in-time queries */
  snapshots: Snapshot[]
  /** Query statistics for monitoring snapshot usage */
  queryStats: Map<string, SnapshotQueryStats>
  /** Index manager for accelerated queries */
  indexManager: IndexManager
  /** Embedding provider for vector queries */
  embeddingProvider: import('../embeddings/provider').EmbeddingProvider | null
  /** Function to reconstruct entity state at a point in time */
  reconstructEntityAtTime: (fullId: string, asOf: Date) => Entity | null
  /** Function to detect corruption in Parquet files */
  detectParquetCorruption: (data: Uint8Array, filePath: string) => void
}

// =============================================================================
// Read Operations
// =============================================================================

/**
 * Find entities in a namespace matching a filter.
 *
 * Supports:
 * - MongoDB-style filters ($eq, $ne, $gt, $lt, $in, $and, $or, etc.)
 * - Full-text search via $text filter
 * - Vector similarity search via $vector filter
 * - Time-travel queries via asOf option
 * - Pagination via cursor, skip, limit
 * - Sorting via sort option
 *
 * @param ctx - Read context with required dependencies
 * @param namespace - Collection/namespace to query
 * @param filter - Optional MongoDB-style filter
 * @param options - Query options (pagination, sort, time-travel)
 * @returns Paginated result with matching entities
 *
 * @example
 * ```typescript
 * // Basic filter
 * const published = await findEntities(ctx, 'posts', { status: 'published' })
 *
 * // With pagination
 * const page1 = await findEntities(ctx, 'posts', {}, { limit: 10 })
 * const page2 = await findEntities(ctx, 'posts', {}, { limit: 10, cursor: page1.nextCursor })
 *
 * // Time-travel query
 * const yesterday = await findEntities(ctx, 'posts', {}, { asOf: new Date('2024-01-01') })
 * ```
 */
export async function findEntities<T extends EntityData = EntityData>(
  ctx: ReadPathContext,
  namespace: string,
  filter?: Filter,
  options?: FindOptions<T>
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
 * Get a single entity by ID.
 *
 * Supports:
 * - Time-travel queries via asOf option
 * - Including soft-deleted entities via includeDeleted option
 * - Corruption detection in event logs
 *
 * @param ctx - Read context with required dependencies
 * @param namespace - Collection/namespace to query
 * @param id - Entity ID (can be local "id" or full "namespace/id")
 * @param options - Query options (time-travel, includeDeleted)
 * @returns The entity or null if not found
 *
 * @example
 * ```typescript
 * // Basic get
 * const user = await getEntity(ctx, 'users', 'user-123')
 *
 * // Time-travel get
 * const userYesterday = await getEntity(ctx, 'users', 'user-123', {
 *   asOf: new Date('2024-01-01')
 * })
 *
 * // Include deleted
 * const deletedUser = await getEntity(ctx, 'users', 'user-123', {
 *   includeDeleted: true
 * })
 * ```
 */
export async function getEntity<T extends EntityData = EntityData>(
  ctx: ReadPathContext,
  namespace: string,
  id: string,
  options?: GetOptions<T>
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
 * Query entities with advanced options.
 *
 * This is an alias for findEntities that provides a more explicit name
 * for query operations. It supports all the same options as findEntities.
 *
 * @param ctx - Read context with required dependencies
 * @param namespace - Collection/namespace to query
 * @param filter - Optional MongoDB-style filter
 * @param options - Query options (pagination, sort, time-travel)
 * @returns Paginated result with matching entities
 *
 * @example
 * ```typescript
 * const results = await queryEntities(ctx, 'posts', {
 *   $and: [
 *     { status: 'published' },
 *     { createdAt: { $gte: new Date('2024-01-01') } }
 *   ]
 * }, {
 *   sort: { createdAt: -1 },
 *   limit: 20
 * })
 * ```
 */
export async function queryEntities<T extends EntityData = EntityData>(
  ctx: ReadPathContext,
  namespace: string,
  filter?: Filter,
  options?: FindOptions<T>
): Promise<PaginatedResult<Entity<T>>> {
  return findEntities<T>(ctx, namespace, filter, options)
}
