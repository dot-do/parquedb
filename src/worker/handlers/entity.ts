/**
 * Entity Detail Handler
 *
 * Handles /datasets/:dataset/:collection/:id route for entity detail view.
 */

import {
  buildResponse,
  buildErrorResponse,
  createTimingContext,
  markTiming,
  measureTiming,
} from '../responses'
import { DATASETS } from '../datasets'
import { handleFileNotFoundError } from './datasets'
import type { EntityRecord } from '../../types/entity'
import type { HandlerContext } from './types'
import { entityAsRecord, asParam } from '../../types/cast'

/**
 * Handle /datasets/:dataset/:collection/:id - Entity detail
 */
export async function handleEntityDetail(
  context: HandlerContext,
  datasetId: string,
  collectionId: string,
  entityId: string
): Promise<Response> {
  const { request, url, baseUrl, startTime, worker, ctx } = context
  const timing = createTimingContext()

  // Check Cache API for cached data response (bypasses all data processing)
  markTiming(timing, 'cache_check_start')
  const cache = await caches.open('parquedb-responses')
  const cacheKey = new Request(`https://parquedb/entity/${datasetId}/${collectionId}/${entityId}`)
  const cachedResponse = await cache.match(cacheKey)
  measureTiming(timing, 'cache_check', 'cache_check_start')

  if (cachedResponse) {
    // Return cached response with updated timing info
    const cachedData = await cachedResponse.json() as { api: unknown; links: unknown; data: unknown; relationships: unknown }
    return buildResponse(request, cachedData as Parameters<typeof buildResponse>[1], timing, worker.getStorageStats())
  }

  // Construct possible $id formats (data may use : or / as separator)
  // e.g., "title:tt0000000" or "knowledge/2.C.2.b"
  // Note: collectionId is plural (titles) but data often uses singular (title)
  const singularType = collectionId.endsWith('s') ? collectionId.slice(0, -1) : collectionId
  const idFormats = [
    `${singularType}:${entityId}`,   // title:tt0000000 (singular with colon)
    `${collectionId}:${entityId}`,   // titles:tt0000000 (plural with colon)
    `${collectionId}/${entityId}`,   // titles/tt0000000
    entityId,                         // Just the raw ID
  ]

  // PARALLEL: Fetch entity (trying multiple ID formats) and relationships simultaneously
  markTiming(timing, 'parallel_start')
  let entityResult
  let allRels
  // Namespace uses dataset prefix (e.g., "onet" -> "onet-graph") + collection
  const dataset = DATASETS[datasetId]
  const prefix = dataset?.prefix ?? datasetId
  const namespace = `${prefix}/${collectionId}`
  try {
    // Try to find entity with any of the ID formats
    const [result1, result2, result3, result4, rels] = await Promise.all([
      worker.find<EntityRecord>(namespace, { $id: idFormats[0] }, { limit: 1 }),
      worker.find<EntityRecord>(namespace, { $id: idFormats[1] }, { limit: 1 }),
      worker.find<EntityRecord>(namespace, { $id: idFormats[2] }, { limit: 1 }),
      worker.find<EntityRecord>(namespace, { $id: idFormats[3] }, { limit: 1 }),
      worker.getRelationships(datasetId, entityId),
    ])

    // Use the first result that found something
    entityResult = result1.items.length > 0 ? result1 :
                   result2.items.length > 0 ? result2 :
                   result3.items.length > 0 ? result3 :
                   result4
    allRels = rels
  } catch (error) {
    // Handle "File not found" errors with 404 instead of 500
    const notFoundResponse = handleFileNotFoundError(error, request, startTime, `${datasetId}/${collectionId}.parquet`)
    if (notFoundResponse) return notFoundResponse
    // Re-throw other errors
    throw error
  }
  measureTiming(timing, 'parallel', 'parallel_start')

  if (entityResult.items.length === 0) {
    return buildErrorResponse(request, new Error(`Entity '${entityId}' not found in ${datasetId}/${collectionId}`), 404, startTime)
  }

  const entity = entityResult.items[0]
  if (!entity) {
    return buildErrorResponse(request, new Error(`Entity '${entityId}' not found in ${datasetId}/${collectionId}`), 404, startTime)
  }
  const entityRaw = entityAsRecord(asParam<Record<string, unknown>>(entity))

  // Group relationships by predicate
  const relationships: Record<string, {
    count: number
    href: string
    items: Array<{ name: string; href: string; importance?: number | undefined; level?: number | undefined }>
  }> = {}

  for (const rel of allRels) {
    if (!relationships[rel.predicate]) {
      relationships[rel.predicate] = {
        count: 0,
        href: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/${rel.predicate}`,
        items: [],
      }
    }
    relationships[rel.predicate]!.count++
    relationships[rel.predicate]!.items.push({
      name: rel.to_name,
      href: `${baseUrl}/datasets/${datasetId}/${rel.to_ns}/${encodeURIComponent(rel.to_id)}`,
      ...(rel.importance ? { importance: rel.importance } : {}),
      ...(rel.level ? { level: rel.level } : {}),
    })
  }

  // Sort items by importance (descending) within each predicate
  for (const pred of Object.keys(relationships)) {
    relationships[pred]!.items.sort((a, b) => (b.importance || 0) - (a.importance || 0))
  }

  // Build response
  const useArrays = url.searchParams.has('arrays')
  const selfUrl = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`

  // Convert to object map format unless ?arrays is set
  const relWithItems: Record<string, Record<string, string>> = {}
  for (const [pred, rel] of Object.entries(relationships)) {
    const itemsMap: Record<string, string> = { $id: rel.href }
    for (const item of rel.items) {
      itemsMap[item.name] = item.href
    }
    relWithItems[pred] = itemsMap
  }

  // Build response data
  const responseData = {
    api: {
      resource: 'entity',
      dataset: datasetId,
      collection: collectionId,
      id: entityId,
      type: entityRaw.$type,
    },
    links: {
      self: selfUrl,
      collection: `${baseUrl}/datasets/${datasetId}/${collectionId}`,
      dataset: `${baseUrl}/datasets/${datasetId}`,
      home: baseUrl,
    },
    data: {
      $id: selfUrl,
      $type: entityRaw.$type,
      ...entityRaw,  // Include all entity fields
    },
    relationships: Object.keys(relationships).length > 0
      ? (useArrays ? relationships : relWithItems)
      : undefined,
  }

  // Cache the response data for 1 hour (without user-specific info)
  ctx.waitUntil(
    cache.put(cacheKey, Response.json(responseData, {
      headers: { 'Cache-Control': 'public, max-age=3600' }
    }))
  )

  return buildResponse(request, responseData, timing, worker.getStorageStats())
}
