/**
 * Delete operations for Payload CMS adapter
 */

import type { ParqueDB } from '../../../ParqueDB'
import type { EntityId } from '../../../types'
import type {
  DeleteOneArgs,
  DeleteManyArgs,
  DeleteVersionsArgs,
  DeleteResult,
  ResolvedAdapterConfig,
  PayloadWhere,
} from '../types'
import { translatePayloadFilter, combineFilters } from '../filter'
import { extractLocalId } from '../transform'

/**
 * Delete a single document by ID
 */
export async function deleteOne(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: DeleteOneArgs
): Promise<DeleteResult> {
  const { collection, id, where, req } = args

  const actor = getActor(req, config)
  const localId = String(id)

  // If where clause is provided, verify it matches
  if (where) {
    const filter = translatePayloadFilter(where)
    const combined = combineFilters({ id: localId }, filter)

    const existing = await db.find(collection, combined, { limit: 1 })

    if (existing.items.length === 0) {
      // Document doesn't match filter
      return { docs: [], errors: [] }
    }
  }

  try {
    const result = await db.delete(collection, localId, { actor })

    if (result.deletedCount > 0) {
      return { docs: [{ id: localId }], errors: [] }
    }

    return { docs: [], errors: [] }
  } catch (error) {
    return {
      docs: [],
      errors: [{
        id: localId,
        message: error instanceof Error ? error.message : 'Unknown error',
      }],
    }
  }
}

/**
 * Delete multiple documents matching a filter
 */
export async function deleteMany(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: DeleteManyArgs
): Promise<DeleteResult> {
  const { collection, where, req } = args

  const actor = getActor(req, config)
  const filter = translatePayloadFilter(where)

  // Find all matching documents first
  const findResult = await db.find(collection, filter, { limit: 10000 })

  const docs: Array<{ id: string | number }> = []
  const errors: Array<{ id: string | number; message: string }> = []

  // Delete each document
  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)

    try {
      const result = await db.delete(collection, localId, { actor })

      if (result.deletedCount > 0) {
        docs.push({ id: localId })
      }
    } catch (error) {
      errors.push({
        id: localId,
        message: error instanceof Error ? error.message : 'Unknown error',
      })
    }
  }

  return { docs, errors }
}

/**
 * Delete versions matching a filter
 */
export async function deleteVersions(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: DeleteVersionsArgs
): Promise<DeleteResult> {
  const { collection, where, req } = args

  const versionsCollection = `${collection}${config.versionsSuffix}`
  const actor = getActor(req, config)
  const filter = translatePayloadFilter(where)

  // Find all matching versions
  const findResult = await db.find(versionsCollection, filter, { limit: 10000 })

  const docs: Array<{ id: string | number }> = []
  const errors: Array<{ id: string | number; message: string }> = []

  // Delete each version
  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)

    try {
      const result = await db.delete(versionsCollection, localId, { actor })

      if (result.deletedCount > 0) {
        docs.push({ id: localId })
      }
    } catch (error) {
      errors.push({
        id: localId,
        message: error instanceof Error ? error.message : 'Unknown error',
      })
    }
  }

  return { docs, errors }
}

/**
 * Delete global versions matching a filter
 */
export async function deleteGlobalVersions(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    slug: string
    where: PayloadWhere
    req?: { transactionID?: string | number; user?: Record<string, unknown> }
  }
): Promise<DeleteResult> {
  const { slug, where, req } = args

  const versionsCollection = `${config.globalsCollection}${config.versionsSuffix}`
  const actor = getActor(req, config)

  // Build filter with slug
  let filter = translatePayloadFilter(where)
  filter = combineFilters(filter, { globalSlug: slug })

  // Find all matching versions
  const findResult = await db.find(versionsCollection, filter, { limit: 10000 })

  const docs: Array<{ id: string | number }> = []
  const errors: Array<{ id: string | number; message: string }> = []

  // Delete each version
  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)

    try {
      const result = await db.delete(versionsCollection, localId, { actor })

      if (result.deletedCount > 0) {
        docs.push({ id: localId })
      }
    } catch (error) {
      errors.push({
        id: localId,
        message: error instanceof Error ? error.message : 'Unknown error',
      })
    }
  }

  return { docs, errors }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Get actor from request context
 */
function getActor(
  req: { user?: Record<string, unknown> } | undefined,
  config: ResolvedAdapterConfig
): EntityId {
  if (req?.user && typeof req.user === 'object' && 'id' in req.user) {
    const userId = String(req.user.id)
    if (userId.includes('/')) {
      return userId as EntityId
    }
    return `users/${userId}` as EntityId
  }
  return config.defaultActor
}
