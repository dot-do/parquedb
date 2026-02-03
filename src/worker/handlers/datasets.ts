/**
 * Dataset Route Handlers
 *
 * Handles /datasets/* routes for browsing available datasets and collections.
 */

import { buildResponse, buildErrorResponse, type ExtendedError } from '../responses'
import { parseQueryFilter, parseQueryOptions } from '../routing'
import { DATASETS } from '../datasets'
import type { EntityRecord } from '../../types/entity'
import type { HandlerContext } from './types'
import { entityAsRecord, statsAsRecord } from '../../types/cast'

/**
 * Check if an error is a "File not found" error and return a 404 response if so.
 * Returns null if the error is not a file-not-found error.
 */
export function handleFileNotFoundError(
  error: unknown,
  request: Request,
  startTime: number,
  defaultPath: string
): Response | null {
  const errorMessage = error instanceof Error ? error.message : String(error)
  if (errorMessage.includes('File not found')) {
    // Extract the file path from the error message
    const match = errorMessage.match(/File not found: (.+)/)
    const filePath = match ? match[1] : defaultPath
    const extendedError = new Error(`Dataset file not found: ${filePath}`) as ExtendedError
    extendedError.code = 'DATASET_NOT_FOUND'
    extendedError.hint = 'This collection may not have been uploaded yet.'
    return buildErrorResponse(request, extendedError, 404, startTime)
  }
  return null
}

/**
 * Handle /datasets - List all datasets
 */
export function handleDatasetsList(context: HandlerContext): Response {
  const { request, baseUrl, startTime } = context

  const datasetLinks: Record<string, string> = { self: `${baseUrl}/datasets` }
  const datasetList = Object.entries(DATASETS).map(([key, ds]) => {
    datasetLinks[key] = `${baseUrl}/datasets/${key}`
    return {
      id: key,
      ...ds,
      href: `${baseUrl}/datasets/${key}`,
    }
  })

  return buildResponse(request, {
    api: {
      resource: 'datasets',
      description: 'Available example datasets',
      count: datasetList.length,
    },
    links: {
      home: baseUrl,
      ...datasetLinks,
    },
    items: datasetList,
  }, startTime)
}

/**
 * Handle /datasets/:dataset - Dataset detail
 */
export function handleDatasetDetail(
  context: HandlerContext,
  datasetId: string
): Response {
  const { request, url, baseUrl, startTime } = context
  const dataset = DATASETS[datasetId]

  if (!dataset) {
    return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404, startTime)
  }

  const collectionLinks: Record<string, string> = {}
  for (const col of dataset.collections) {
    collectionLinks[col] = `${baseUrl}/datasets/${datasetId}/${col}`
  }

  // Build collections as object map {name: href} or array [{name, href}] with ?arrays
  const useArrays = url.searchParams.has('arrays')
  const collectionsData = useArrays
    ? dataset.collections.map(col => ({
        name: col,
        href: `${baseUrl}/datasets/${datasetId}/${col}`,
      }))
    : Object.fromEntries(dataset.collections.map(col => [
        col,
        `${baseUrl}/datasets/${datasetId}/${col}`,
      ]))

  return buildResponse(request, {
    api: {
      resource: 'dataset',
      id: datasetId,
      name: dataset.name,
      description: dataset.description,
      source: dataset.source,
    },
    links: {
      self: `${baseUrl}/datasets/${datasetId}`,
      home: baseUrl,
      datasets: `${baseUrl}/datasets`,
      ...collectionLinks,
    },
    data: {
      collections: collectionsData,
    },
  }, startTime)
}

/**
 * Handle /datasets/:dataset/:collection - Collection list
 */
export async function handleCollectionList(
  context: HandlerContext,
  datasetId: string,
  collectionId: string
): Promise<Response> {
  const { request, url, baseUrl, path, startTime, worker } = context
  const dataset = DATASETS[datasetId]

  if (!dataset) {
    return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404, startTime)
  }

  // Map dataset/collection to namespace using prefix
  const prefix = dataset.prefix || datasetId
  const ns = `${prefix}/${collectionId}`

  const filter = parseQueryFilter(url.searchParams)
  const options = parseQueryOptions(url.searchParams)
  if (!options.limit) options.limit = 20

  let result
  try {
    result = await worker.find<EntityRecord>(ns, filter, options)
  } catch (error) {
    // Handle "File not found" errors with 404 instead of 500
    const notFoundResponse = handleFileNotFoundError(error, request, startTime, `${ns}.parquet`)
    if (notFoundResponse) return notFoundResponse
    // Re-throw other errors
    throw error
  }

  // Build enriched items with href links
  const enrichedItems: unknown[] = []
  const itemLinks: Record<string, string> = {}

  // Get known predicates for this collection
  const knownPredicates = dataset.predicates?.[collectionId] || []

  if (result.items) {
    for (const item of result.items) {
      const entity = entityAsRecord(item)
      const entityId = entity.$id || entity.id
      if (entityId) {
        const localId = String(entityId).split('/').pop() || ''
        const href = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(localId)}`

        // Add to quick links (first 10)
        if (Object.keys(itemLinks).length < 10) {
          const linkName = String(entity.name || localId)
          itemLinks[linkName] = href
        }

        // Find relationship predicates - check for JSON-encoded relationships
        const predicates: string[] = []
        for (const predicate of knownPredicates) {
          const rawValue = entity[predicate]
          if (rawValue) {
            // Parse JSON-encoded relationship data
            try {
              const parsed = typeof rawValue === 'string' ? JSON.parse(rawValue) : rawValue
              if (parsed && typeof parsed === 'object' && parsed.$count > 0) {
                predicates.push(predicate)
              }
            } catch {
              // Intentionally ignored: not JSON, check if it's already an object
              if (typeof rawValue === 'object') {
                predicates.push(predicate)
              }
            }
          }
        }

        // Build relationship links
        const relLinks: Record<string, string> = {}
        for (const pred of predicates) {
          relLinks[pred] = `${href}/${pred}`
        }

        // Enrich item with href and relationship links
        enrichedItems.push({
          $id: entity.$id,
          $type: entity.$type,
          name: entity.name,
          description: entity.description,
          // Show relationship counts
          ...(predicates.length > 0 ? {
            _relationships: predicates.map(p => {
              const rawValue = entity[p]
              try {
                const parsed = typeof rawValue === 'string' ? JSON.parse(rawValue) : rawValue
                return { predicate: p, count: parsed?.$count || 0 }
              } catch {
                // Intentionally ignored: relationship data not parseable, default to zero count
                return { predicate: p, count: 0 }
              }
            }),
          } : {}),
          _links: {
            self: href,
            ...relLinks,
          },
        })
      } else {
        enrichedItems.push(item)
      }
    }
  }

  // Build pagination links with different limits
  const currentLimit = options.limit || 20
  const currentSkip = options.skip || 0
  const basePath = `${baseUrl}${path}`
  const useArrays = url.searchParams.has('arrays')
  const arrayParam = useArrays ? '&arrays' : ''

  const paginationLinks: Record<string, string> = {}

  // Add limit option links
  const limitOptions = [20, 50, 100, 500, 1000]
  for (const limit of limitOptions) {
    if (limit !== currentLimit) {
      paginationLinks[`limit${limit}`] = `${basePath}?limit=${limit}${arrayParam}`
    }
  }

  // Add next/prev links if applicable
  if (result.hasMore) {
    const nextCursor = statsAsRecord(result.stats)?.nextCursor
    if (nextCursor) {
      paginationLinks.next = `${basePath}?cursor=${nextCursor}&limit=${currentLimit}${arrayParam}`
    } else {
      paginationLinks.next = `${basePath}?skip=${currentSkip + currentLimit}&limit=${currentLimit}${arrayParam}`
    }
  }
  if (currentSkip > 0) {
    const prevSkip = Math.max(0, currentSkip - currentLimit)
    paginationLinks.prev = `${basePath}?skip=${prevSkip}&limit=${currentLimit}${arrayParam}`
  }

  return buildResponse(request, {
    api: {
      resource: 'collection',
      dataset: datasetId,
      collection: collectionId,
      filter: Object.keys(filter).length > 0 ? filter : undefined,
      limit: options.limit,
      skip: options.skip,
      returned: enrichedItems.length,
      hasMore: result.hasMore,
    },
    links: {
      self: `${baseUrl}${path}${url.search}`,
      dataset: `${baseUrl}/datasets/${datasetId}`,
      home: baseUrl,
      ...paginationLinks,
      ...itemLinks,
    },
    items: enrichedItems,
    stats: statsAsRecord(result.stats),
  }, startTime)
}
