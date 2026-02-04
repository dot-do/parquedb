/**
 * Create operations for Payload CMS adapter
 */

import type { ParqueDB } from '../../../ParqueDB'
import type { Entity, EntityId } from '../../../types'
import type { CreateArgs, ResolvedAdapterConfig } from '../types'
import { toParqueDBInput, toPayloadDoc } from '../transform'

/**
 * Create a new document
 */
export async function create(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: CreateArgs
): Promise<Record<string, unknown>> {
  const { collection, data, req, draft } = args

  // Get actor from request or use default
  const actor = getActor(req, config)

  // Transform Payload data to ParqueDB input
  const input = toParqueDBInput(data, {
    collection,
    actor,
  })

  // Add draft status if specified
  if (draft) {
    input['_status'] = 'draft'
  }

  // Create the entity
  const entity = await db.create(collection, input, { actor })

  // Store the local ID as a regular 'id' field for Payload's findOne({ id: ... }) queries
  // ParqueDB uses $id format (ns/localId), but Payload expects 'id' to be queryable
  const localId = entity.$id.split('/')[1]
  if (localId && !(entity as Record<string, unknown>)['id']) {
    // Update the entity to include the id field
    await db.update(collection, localId, { $set: { id: localId } }, { actor })
    ;(entity as Record<string, unknown>)['id'] = localId
  }

  // Transform back to Payload format
  const doc = toPayloadDoc(entity, { collection })

  if (!doc) {
    throw new Error('Failed to create document')
  }

  return doc
}

/**
 * Create a global document
 */
export async function createGlobal(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    slug: string
    data: Record<string, unknown>
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
    draft?: boolean | undefined
  }
): Promise<Record<string, unknown>> {
  const { slug, data, req, draft } = args
  const globalsCollection = config.globalsCollection

  const actor = getActor(req, config)

  // Check if global already exists
  const existing = await db.find(globalsCollection, { slug }, { limit: 1 })

  const globalData = {
    ...data,
    slug,
    _status: draft ? 'draft' : 'published',
  }

  let entity: Entity

  if (existing.items.length > 0) {
    // Update existing global
    const existingEntity = existing.items[0]!
    const updated = await db.update(
      globalsCollection,
      existingEntity.$id.split('/')[1]!,
      { $set: globalData },
      { actor }
    )
    if (!updated) {
      throw new Error(`Failed to update global: ${slug}`)
    }
    entity = updated
  } else {
    // Create new global
    const input = toParqueDBInput(globalData, {
      collection: globalsCollection,
      actor,
    })
    input.name = slug
    entity = await db.create(globalsCollection, input, { actor })
  }

  // Transform back to Payload format, excluding internal fields
  const doc: Record<string, unknown> = {}
  const excludeFields = new Set(['$id', '$type', 'name', 'slug', 'version', 'createdBy', 'updatedBy', 'deletedAt', 'deletedBy'])

  for (const [key, value] of Object.entries(entity)) {
    if (!excludeFields.has(key)) {
      doc[key] = value instanceof Date ? value.toISOString() : value
    }
  }

  return doc
}

/**
 * Create a version document
 */
export async function createVersion(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    collection: string
    parent: string | number
    versionData: Record<string, unknown>
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
    autosave?: boolean | undefined
    createdAt?: string | undefined
    publishedLocale?: string | undefined
    snapshot?: boolean | undefined
    updatedAt?: string | undefined
  }
): Promise<Record<string, unknown>> {
  const {
    collection,
    parent,
    versionData,
    req,
    autosave = false,
    createdAt,
    publishedLocale,
    snapshot = false,
    updatedAt,
  } = args

  const versionsCollection = `${collection}${config.versionsSuffix}`
  const actor = getActor(req, config)
  const parentId = String(parent)

  // Mark previous versions as not latest using batched updateMany
  // instead of N+1 individual updates (find + update per item)
  await db.collection(versionsCollection).updateMany(
    { parent: parentId, latest: true },
    { $set: { latest: false } },
    { actor }
  )

  // Create the version
  // Note: We use 'versionData' as the field name to avoid collision with
  // ParqueDB's built-in 'version' field (optimistic concurrency)
  const input = toParqueDBInput(
    {
      parent: parentId,
      versionData: versionData,
      latest: true,
      autosave,
      publishedLocale,
      snapshot,
    },
    { collection: versionsCollection, actor }
  )

  input.name = `Version of ${parentId}`
  input.$type = `${capitalize(collection)}Version`

  const entity = await db.create(versionsCollection, input, { actor })

  return {
    id: entity.$id.split('/')[1],
    parent: parentId,
    version: versionData,
    createdAt: createdAt || entity.createdAt.toISOString(),
    updatedAt: updatedAt || entity.updatedAt.toISOString(),
    latest: true,
    autosave,
    publishedLocale,
    snapshot,
  }
}

/**
 * Create a global version document
 */
export async function createGlobalVersion(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: {
    slug: string
    parent: string | number
    versionData: Record<string, unknown>
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
    autosave?: boolean | undefined
    createdAt?: string | undefined
    publishedLocale?: string | undefined
    snapshot?: boolean | undefined
    updatedAt?: string | undefined
  }
): Promise<Record<string, unknown>> {
  const {
    slug,
    parent,
    versionData,
    req,
    autosave = false,
    createdAt,
    publishedLocale,
    snapshot = false,
    updatedAt,
  } = args

  const versionsCollection = `${config.globalsCollection}${config.versionsSuffix}`
  const actor = getActor(req, config)
  const parentId = String(parent)

  // Mark previous versions as not latest using batched updateMany
  // instead of N+1 individual updates (find + update per item)
  await db.collection(versionsCollection).updateMany(
    { globalSlug: slug, latest: true },
    { $set: { latest: false } },
    { actor }
  )

  // Create the version
  // Note: We use 'versionData' as the field name to avoid collision with
  // ParqueDB's built-in 'version' field (optimistic concurrency)
  const input = toParqueDBInput(
    {
      parent: parentId,
      globalSlug: slug,
      versionData: versionData,
      latest: true,
      autosave,
      publishedLocale,
      snapshot,
    },
    { collection: versionsCollection, actor }
  )

  input.name = `Global version of ${slug}`
  input.$type = 'GlobalVersion'

  const entity = await db.create(versionsCollection, input, { actor })

  return {
    id: entity.$id.split('/')[1],
    parent: parentId,
    version: versionData,
    createdAt: createdAt || entity.createdAt.toISOString(),
    updatedAt: updatedAt || entity.updatedAt.toISOString(),
    latest: true,
    autosave,
    publishedLocale,
    snapshot,
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Get actor from request context
 */
function getActor(
  req: { user?: Record<string, unknown> | undefined } | undefined,
  config: ResolvedAdapterConfig
): EntityId {
  if (req?.user && typeof req.user === 'object' && 'id' in req.user) {
    const userId = String(req.user.id)
    // Check if it's already a full entity ID
    if (userId.includes('/')) {
      return userId as EntityId
    }
    return `users/${userId}` as EntityId
  }
  return config.defaultActor
}

/**
 * Capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1)
}
