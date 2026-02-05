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
  const { request, url, baseUrl, startTime, worker } = context
  const timing = createTimingContext()

  // Standard $id format is {collection}/{entityId}
  // e.g., "occupations/11-1011.00" or "titles/tt0000001"
  const standardId = `${collectionId}/${entityId}`

  // Namespace uses dataset prefix (e.g., "onet" -> "onet-graph") + collection
  const dataset = DATASETS[datasetId]
  const prefix = dataset?.prefix ?? datasetId
  const namespace = `${prefix}/${collectionId}`

  // PARALLEL: Fetch entity and relationships simultaneously
  // Track individual timings while still running in parallel
  markTiming(timing, 'parallel_start')
  let entityResult
  let allRels
  let entityTime = 0
  let relsTime = 0
  try {
    const [result, rels] = await Promise.all([
      (async () => {
        const t0 = performance.now()
        const r = await worker.find<EntityRecord>(namespace, { $id: standardId }, { limit: 1 })
        entityTime = performance.now() - t0
        return r
      })(),
      (async () => {
        const t0 = performance.now()
        const r = await worker.getRelationships(prefix, standardId)
        relsTime = performance.now() - t0
        return r
      })(),
    ])

    entityResult = result
    allRels = rels
  } catch (error) {
    // Handle "File not found" errors with 404 instead of 500
    const notFoundResponse = handleFileNotFoundError(error, request, startTime, `${datasetId}/${collectionId}.parquet`)
    if (notFoundResponse) return notFoundResponse
    // Re-throw other errors
    throw error
  }
  measureTiming(timing, 'parallel', 'parallel_start')
  // Add individual timings
  timing.durations.set('entity', entityTime)
  timing.durations.set('rels', relsTime)

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

  return buildResponse(request, responseData, timing, worker.getStorageStats())
}
