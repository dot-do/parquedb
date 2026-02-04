/**
 * Find operations for Payload CMS adapter
 */

import type { ParqueDB } from '../../../ParqueDB'
import type { Entity, FindOptions as ParqueDBFindOptions } from '../../../types'
import type {
  FindArgs,
  FindOneArgs,
  CountArgs,
  DistinctArgs,
  QueryDraftsArgs,
  FindVersionsArgs,
  FindGlobalArgs,
  FindGlobalVersionsArgs,
  PayloadPaginatedDocs,
  ResolvedAdapterConfig,
  PayloadWhere,
} from '../types'
import { translatePayloadFilter, translatePayloadSort, combineFilters } from '../filter'
import { toPayloadDoc, toPayloadDocs, buildPaginationInfo, toPayloadGlobal } from '../transform'

/**
 * Find documents with pagination
 */
export async function find<T = Record<string, unknown>>(
  db: ParqueDB,
  _config: ResolvedAdapterConfig,
  args: FindArgs
): Promise<PayloadPaginatedDocs<T>> {
  const {
    collection,
    where,
    sort,
    limit = 10,
    page = 1,
    pagination = true,
    draft,
    locale,
    select,
  } = args

  // Build filter
  let filter = translatePayloadFilter(where)

  // Add draft filter if specified
  if (draft === false) {
    filter = combineFilters(filter, { _status: { $ne: 'draft' } })
  } else if (draft === true) {
    filter = combineFilters(filter, { _status: 'draft' })
  }

  // Add locale filter if specified
  if (locale && locale !== 'all') {
    filter = combineFilters(filter, { locale })
  }

  // Build query options
  const options: ParqueDBFindOptions = {
    sort: translatePayloadSort(sort),
    limit: pagination ? limit : 0,
    skip: pagination ? (page - 1) * limit : 0,
  }

  // Execute query
  const result = await db.find(collection, filter, options)

  // Get total count for pagination
  const totalDocs = result.total ?? result.items.length

  // Transform documents
  const docs = toPayloadDocs<T>(result.items, { collection, select })

  // Build pagination info
  const paginationInfo = buildPaginationInfo(totalDocs, limit, page)

  return {
    docs,
    ...paginationInfo,
  }
}

/**
 * Find a single document
 */
export async function findOne<T = Record<string, unknown>>(
  db: ParqueDB,
  _config: ResolvedAdapterConfig,
  args: FindOneArgs
): Promise<T | null> {
  const { collection, where, draft, locale, select } = args

  // Build filter
  let filter = translatePayloadFilter(where)

  // Add draft filter if specified
  if (draft === false) {
    filter = combineFilters(filter, { _status: { $ne: 'draft' } })
  }

  // Add locale filter
  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  // Execute query with limit 1
  const result = await db.find(collection, filter, { limit: 1 })

  if (result.items.length === 0) {
    return null
  }

  return toPayloadDoc<T>(result.items[0]!, { collection, select })
}

/**
 * Count documents matching a filter
 */
export async function count(
  db: ParqueDB,
  _config: ResolvedAdapterConfig,
  args: CountArgs
): Promise<number> {
  const { collection, where, locale } = args

  // Build filter
  let filter = translatePayloadFilter(where)

  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  // Use aggregation to count
  const result = await db.find(collection, filter, { limit: 0 })
  return result.total ?? 0
}

/**
 * Find distinct values for a field
 */
export async function findDistinct<T = unknown>(
  db: ParqueDB,
  _config: ResolvedAdapterConfig,
  args: DistinctArgs
): Promise<T[]> {
  const { collection, field, where } = args

  const filter = translatePayloadFilter(where)

  // Fetch all matching documents and extract unique values
  // Note: This is a simple implementation; production would use aggregation
  const result = await db.find(collection, filter, { limit: 10000 })

  const values = new Set<T>()

  for (const entity of result.items) {
    const value = getNestedValue(entity, field)
    if (value !== undefined && value !== null) {
      if (Array.isArray(value)) {
        for (const v of value) {
          values.add(v as T)
        }
      } else {
        values.add(value as T)
      }
    }
  }

  return Array.from(values)
}

/**
 * Query draft documents
 */
export async function queryDrafts<T = Record<string, unknown>>(
  db: ParqueDB,
  _config: ResolvedAdapterConfig,
  args: QueryDraftsArgs
): Promise<PayloadPaginatedDocs<T>> {
  const {
    collection,
    where,
    sort,
    limit = 10,
    page = 1,
    pagination = true,
    locale,
  } = args

  // Build filter with draft status
  const baseFilter = translatePayloadFilter(where)
  const filter = combineFilters(baseFilter, { _status: 'draft' })

  // Build query options
  const options: ParqueDBFindOptions = {
    sort: translatePayloadSort(sort),
    limit: pagination ? limit : 0,
    skip: pagination ? (page - 1) * limit : 0,
  }

  // Add locale filter
  if (locale) {
    options.filter = combineFilters(filter, { locale })
  }

  // Execute query
  const result = await db.find(collection, filter, options)
  const totalDocs = result.total ?? result.items.length

  // Transform documents
  const docs = toPayloadDocs<T>(result.items, { collection })

  // Build pagination info
  const paginationInfo = buildPaginationInfo(totalDocs, limit, page)

  return {
    docs,
    ...paginationInfo,
  }
}

/**
 * Find versions of documents
 */
export async function findVersions<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: FindVersionsArgs
): Promise<PayloadPaginatedDocs<T>> {
  const {
    collection,
    where,
    sort,
    limit = 10,
    page = 1,
    pagination = true,
    locale,
    skip,
  } = args

  const { versionsSuffix } = config
  const versionsCollection = `${collection}${versionsSuffix}`

  // Build filter
  let filter = translatePayloadFilter(where)

  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  // Build query options
  const options: ParqueDBFindOptions = {
    sort: translatePayloadSort(sort) || { createdAt: -1 },
    limit: pagination ? limit : 0,
    skip: skip ?? (pagination ? (page - 1) * limit : 0),
  }

  // Execute query
  const result = await db.find(versionsCollection, filter, options)
  const totalDocs = result.total ?? result.items.length

  // Transform version documents
  // Note: We store version data as 'versionData' to avoid collision with
  // ParqueDB's built-in 'version' field, but return it as 'version' for Payload
  const docs = result.items.map((entity: Entity) => ({
    id: entity.$id.split('/')[1],
    parent: (entity as Record<string, unknown>)['parent'],
    version: (entity as Record<string, unknown>)['versionData'],
    createdAt: entity.createdAt instanceof Date ? entity.createdAt.toISOString() : entity.createdAt,
    updatedAt: entity.updatedAt instanceof Date ? entity.updatedAt.toISOString() : entity.updatedAt,
    latest: (entity as Record<string, unknown>)['latest'] ?? false,
    autosave: (entity as Record<string, unknown>)['autosave'] ?? false,
    publishedLocale: (entity as Record<string, unknown>)['publishedLocale'],
    snapshot: (entity as Record<string, unknown>)['snapshot'],
  })) as T[]

  // Build pagination info
  const paginationInfo = buildPaginationInfo(totalDocs, limit, page)

  return {
    docs,
    ...paginationInfo,
  }
}

/**
 * Find a global document
 */
export async function findGlobal<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: FindGlobalArgs
): Promise<T | null> {
  const { slug, locale, draft } = args
  const globalsCollection = config.globalsCollection

  // Build filter
  let filter: Record<string, unknown> = { slug }

  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  if (draft === false) {
    filter = combineFilters(filter, { _status: { $ne: 'draft' } })
  }

  // Execute query
  const result = await db.find(globalsCollection, filter, { limit: 1 })

  if (result.items.length === 0) {
    return null
  }

  return toPayloadGlobal<T>(result.items[0]!, slug)
}

/**
 * Find global versions
 */
export async function findGlobalVersions<T = Record<string, unknown>>(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: FindGlobalVersionsArgs
): Promise<PayloadPaginatedDocs<T>> {
  const {
    slug,
    where,
    sort,
    limit = 10,
    page = 1,
    pagination = true,
    locale,
    skip,
  } = args

  const versionsCollection = `${config.globalsCollection}${config.versionsSuffix}`

  // Build filter
  let filter = translatePayloadFilter(where)
  filter = combineFilters(filter, { globalSlug: slug })

  if (locale) {
    filter = combineFilters(filter, { locale })
  }

  // Build query options
  const options: ParqueDBFindOptions = {
    sort: translatePayloadSort(sort) || { createdAt: -1 },
    limit: pagination ? limit : 0,
    skip: skip ?? (pagination ? (page - 1) * limit : 0),
  }

  // Execute query
  const result = await db.find(versionsCollection, filter, options)
  const totalDocs = result.total ?? result.items.length

  // Transform version documents (for global versions)
  const docs = result.items.map((entity: Entity) => ({
    id: entity.$id.split('/')[1],
    parent: (entity as Record<string, unknown>)['parent'],
    version: (entity as Record<string, unknown>)['versionData'],
    createdAt: entity.createdAt instanceof Date ? entity.createdAt.toISOString() : entity.createdAt,
    updatedAt: entity.updatedAt instanceof Date ? entity.updatedAt.toISOString() : entity.updatedAt,
    latest: (entity as Record<string, unknown>)['latest'] ?? false,
    autosave: (entity as Record<string, unknown>)['autosave'] ?? false,
    publishedLocale: (entity as Record<string, unknown>)['publishedLocale'],
    snapshot: (entity as Record<string, unknown>)['snapshot'],
  })) as T[]

  // Build pagination info
  const paginationInfo = buildPaginationInfo(totalDocs, limit, page)

  return {
    docs,
    ...paginationInfo,
  }
}

/**
 * Count versions
 */
export async function countVersions(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: { collection: string; where?: PayloadWhere | undefined }
): Promise<number> {
  const versionsCollection = `${args.collection}${config.versionsSuffix}`
  const filter = translatePayloadFilter(args.where)

  const result = await db.find(versionsCollection, filter, { limit: 0 })
  return result.total ?? 0
}

/**
 * Count global versions
 */
export async function countGlobalVersions(
  db: ParqueDB,
  config: ResolvedAdapterConfig,
  args: { slug: string; where?: PayloadWhere | undefined }
): Promise<number> {
  const versionsCollection = `${config.globalsCollection}${config.versionsSuffix}`

  let filter = translatePayloadFilter(args.where)
  filter = combineFilters(filter, { globalSlug: args.slug })

  const result = await db.find(versionsCollection, filter, { limit: 0 })
  return result.total ?? 0
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Get a nested value from an object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined
    }
    if (typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }

  return current
}
