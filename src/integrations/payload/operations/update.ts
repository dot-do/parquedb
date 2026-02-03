/**
 * Update operations for Payload CMS adapter
 */

import type { ParqueDB } from '../../../ParqueDB'
import type { EntityId, UpdateInput } from '../../../types'
import type {
  UpdateOneArgs,
  UpdateManyArgs,
  UpsertArgs,
  ResolvedAdapterConfig,
  UpdateManyResult,
} from '../types'
import { translatePayloadFilter, combineFilters } from '../filter'
import { toParqueDBUpdate, toPayloadDoc, extractLocalId } from '../transform'

/**
 * Update a single document by ID
 */
export async function updateOne<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: UpdateOneArgs
): Promise<T | null> {
  const { collection, id, data, req, draft, locale, select } = args

  const actor = getActor(req, config)
  const localId = String(id)

  // Build update input
  const update = toParqueDBUpdate(data, {
    collection,
    actor,
    isUpdate: true,
  })

  // Handle draft status
  if (draft !== undefined) {
    update['$set'] = update['$set'] || {}
    ;(update['$set'] as Record<string, unknown>)['_status'] = draft ? 'draft' : 'published'
  }

  // Handle locale
  if (locale) {
    update['$set'] = update['$set'] || {}
    ;(update['$set'] as Record<string, unknown>)['locale'] = locale
  }

  // Execute update
  const entity = await db.update(collection, localId, update as UpdateInput, {
    actor,
    returnDocument: 'after',
  })

  if (!entity) {
    return null
  }

  return toPayloadDoc<T>(entity, { collection, select })
}

/**
 * Update multiple documents matching a filter
 */
export async function updateMany(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: UpdateManyArgs
): Promise<UpdateManyResult> {
  const { collection, where, data, req, draft, locale } = args

  const actor = getActor(req, config)

  // Build filter
  let filter = translatePayloadFilter(where)

  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  // Build update
  const update = toParqueDBUpdate(data, {
    collection,
    actor,
    isUpdate: true,
  })

  // Handle draft status
  if (draft !== undefined) {
    update['$set'] = update['$set'] || {}
    ;(update['$set'] as Record<string, unknown>)['_status'] = draft ? 'draft' : 'published'
  }

  // Find all matching documents first
  const findResult = await db.find(collection, filter, { limit: 10000 })

  const docs: Array<{ id: string | number }> = []
  const errors: Array<{ id: string | number; message: string }> = []

  // Update each document
  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)

    try {
      await db.update(collection, localId, update as UpdateInput, { actor })
      docs.push({ id: localId })
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
 * Update a global document
 */
export async function updateGlobal<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    slug: string
    data: Record<string, unknown>
    req?: { transactionID?: string | number; user?: Record<string, unknown> }
    draft?: boolean
    locale?: string
    select?: Record<string, boolean>
  }
): Promise<T | null> {
  const { slug, data, req, draft, locale, select } = args
  const globalsCollection = config.globalsCollection
  const actor = getActor(req, config)

  // Find the global by slug
  const result = await db.find(globalsCollection, { slug }, { limit: 1 })

  if (result.items.length === 0) {
    // Global doesn't exist, create it
    const { createGlobal } = await import('./create')
    const created = await createGlobal(db, config, { slug, data, req, draft })
    return created as T
  }

  const entity = result.items[0]!
  const localId = extractLocalId(entity.$id)

  // Build update
  const update = toParqueDBUpdate(data, {
    collection: globalsCollection,
    actor,
    isUpdate: true,
    existingEntity: entity,
  })

  // Handle draft status
  if (draft !== undefined) {
    update['$set'] = update['$set'] || {}
    ;(update['$set'] as Record<string, unknown>)['_status'] = draft ? 'draft' : 'published'
  }

  // Handle locale
  if (locale) {
    update['$set'] = update['$set'] || {}
    ;(update['$set'] as Record<string, unknown>)['locale'] = locale
  }

  // Execute update
  const updated = await db.update(globalsCollection, localId, update as UpdateInput, {
    actor,
    returnDocument: 'after',
  })

  if (!updated) {
    return null
  }

  // Transform to Payload format
  const doc: Record<string, unknown> = {}
  const excludeFields = new Set(['$id', '$type', 'name', 'slug', 'version', 'createdBy', 'updatedBy', 'deletedAt', 'deletedBy'])

  for (const [key, value] of Object.entries(updated)) {
    if (!excludeFields.has(key)) {
      // Apply selection
      if (select && !select[key]) {
        continue
      }
      doc[key] = value instanceof Date ? value.toISOString() : value
    }
  }

  return doc as T
}

/**
 * Update a version document
 */
export async function updateVersion<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    collection: string
    id: string | number
    versionData?: Record<string, unknown>
    locale?: string
    req?: { transactionID?: string | number; user?: Record<string, unknown> }
    select?: Record<string, boolean>
  }
): Promise<T | null> {
  const { collection, id, versionData, locale, req } = args
  const versionsCollection = `${collection}${config.versionsSuffix}`
  const actor = getActor(req, config)
  const localId = String(id)

  // Build update
  // Note: Store as 'versionData' to avoid collision with ParqueDB's 'version' field
  const update: Record<string, unknown> = { $set: {} }

  if (versionData) {
    (update['$set'] as Record<string, unknown>)['versionData'] = versionData
  }

  if (locale) {
    (update['$set'] as Record<string, unknown>)['locale'] = locale
  }

  // Execute update
  const entity = await db.update(versionsCollection, localId, update as UpdateInput, {
    actor,
    returnDocument: 'after',
  })

  if (!entity) {
    return null
  }

  return {
    id: extractLocalId(entity.$id),
    parent: (entity as Record<string, unknown>)['parent'],
    version: (entity as Record<string, unknown>)['versionData'],
    createdAt: entity.createdAt instanceof Date ? entity.createdAt.toISOString() : entity.createdAt,
    updatedAt: entity.updatedAt instanceof Date ? entity.updatedAt.toISOString() : entity.updatedAt,
    latest: (entity as Record<string, unknown>)['latest'] ?? false,
    autosave: (entity as Record<string, unknown>)['autosave'] ?? false,
    publishedLocale: (entity as Record<string, unknown>)['publishedLocale'],
    snapshot: (entity as Record<string, unknown>)['snapshot'],
  } as T
}

/**
 * Update a global version
 */
export async function updateGlobalVersion<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    slug: string
    id: string | number
    versionData?: Record<string, unknown>
    locale?: string
    req?: { transactionID?: string | number; user?: Record<string, unknown> }
    select?: Record<string, boolean>
  }
): Promise<T | null> {
  const { id, versionData, locale, req } = args
  const versionsCollection = `${config.globalsCollection}${config.versionsSuffix}`
  const _actor = getActor(req, config)
  const localId = String(id)

  // Build update
  // Note: Store as 'versionData' to avoid collision with ParqueDB's 'version' field
  const update: Record<string, unknown> = { $set: {} }

  if (versionData) {
    (update['$set'] as Record<string, unknown>)['versionData'] = versionData
  }

  if (locale) {
    (update['$set'] as Record<string, unknown>)['locale'] = locale
  }

  // Execute update
  const entity = await db.update(versionsCollection, localId, update as UpdateInput, {
    actor: _actor,
    returnDocument: 'after',
  })

  if (!entity) {
    return null
  }

  return {
    id: extractLocalId(entity.$id),
    parent: (entity as Record<string, unknown>)['parent'],
    version: (entity as Record<string, unknown>)['versionData'],
    createdAt: entity.createdAt instanceof Date ? entity.createdAt.toISOString() : entity.createdAt,
    updatedAt: entity.updatedAt instanceof Date ? entity.updatedAt.toISOString() : entity.updatedAt,
    latest: (entity as Record<string, unknown>)['latest'] ?? false,
    autosave: (entity as Record<string, unknown>)['autosave'] ?? false,
    publishedLocale: (entity as Record<string, unknown>)['publishedLocale'],
    snapshot: (entity as Record<string, unknown>)['snapshot'],
  } as T
}

/**
 * Upsert a document
 */
export async function upsert<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: UpsertArgs
): Promise<T | null> {
  const { collection, where, data, req, draft, locale, select } = args

  // actor is available from getActor if needed for logging in future
  void getActor(req, config)

  // Build filter
  const filter = translatePayloadFilter(where)

  // Try to find existing document
  const existing = await db.find(collection, filter, { limit: 1 })

  if (existing.items.length > 0) {
    // Update existing
    const entity = existing.items[0]!
    const localId = extractLocalId(entity.$id)

    return updateOne(db, config, {
      collection,
      id: localId,
      data,
      req,
      draft,
      locale,
      select,
    })
  }

  // Create new
  const { create } = await import('./create')
  const created = await create(db, config, { collection, data, req, draft })
  return created as T
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
