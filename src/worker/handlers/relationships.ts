/**
 * Relationship Traversal Handler
 *
 * Handles /datasets/:dataset/:collection/:id/:predicate route for relationship navigation.
 */

import {
  buildResponse,
  createTimingContext,
  markTiming,
  measureTiming,
} from '../responses'
import { handleFileNotFoundError } from './datasets'
import type { HandlerContext } from './types'

/**
 * Handle /datasets/:dataset/:collection/:id/:predicate - Relationship traversal
 */
export async function handleRelationshipTraversal(
  context: HandlerContext,
  datasetId: string,
  collectionId: string,
  entityId: string,
  predicate: string
): Promise<Response> {
  const { request, url, baseUrl, worker, startTime } = context
  const timing = createTimingContext()

  // Read relationships from rels.parquet
  markTiming(timing, 'rels_start')
  let rels
  try {
    rels = await worker.getRelationships(datasetId, entityId, predicate)
  } catch (error) {
    // Handle "File not found" errors with 404 instead of 500
    const notFoundResponse = handleFileNotFoundError(error, request, startTime, `${datasetId}/rels.parquet`)
    if (notFoundResponse) return notFoundResponse
    // Re-throw other errors
    throw error
  }
  measureTiming(timing, 'rels', 'rels_start')

  // Convert to display format
  const items = rels.map(rel => ({
    $id: `${baseUrl}/datasets/${datasetId}/${rel.to_ns}/${encodeURIComponent(rel.to_id)}`,
    name: rel.to_name,
    type: rel.to_type,
    ...(rel.importance ? { importance: rel.importance } : {}),
    ...(rel.level ? { level: rel.level } : {}),
  }))

  // Sort by importance
  items.sort((a, b) => (b.importance || 0) - (a.importance || 0))

  // Pagination
  const limit = parseInt(url.searchParams.get('limit') || '100')
  const skip = parseInt(url.searchParams.get('skip') || '0')
  const paginatedItems = items.slice(skip, skip + limit)

  return buildResponse(request, {
    api: {
      resource: 'relationships',
      dataset: datasetId,
      collection: collectionId,
      id: entityId,
      predicate,
      count: items.length,
    },
    links: {
      self: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/${predicate}`,
      entity: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`,
    },
    items: paginatedItems,
  }, timing, worker.getStorageStats())
}
