/**
 * HNSW Search Algorithms
 *
 * Search implementations for the HNSW vector index including:
 * - Standard nearest neighbor search
 * - Hybrid search with pre/post filtering strategies
 * - Layer-level search operations
 */

import type { HNSWNode, SearchCandidate } from './hnsw-types'
import type {
  VectorSearchOptions,
  VectorSearchResult,
  VectorMetric,
  HybridSearchOptions,
  HybridSearchResult,
  HybridSearchStrategy,
} from '../types'
import { distanceToScore } from './distance'
import { MinHeap, MaxHeap } from './hnsw-heap'
import { logger } from '../../utils/logger'
import { DEFAULT_HNSW_EF_SEARCH } from '../../constants'

/**
 * Context required for search operations
 */
export interface SearchContext {
  /** Function to get a node by ID (may return undefined if evicted) */
  getNode: (nodeId: number) => HNSWNode | undefined
  /** Distance function for comparing vectors */
  distanceFn: (a: number[], b: number[]) => number
  /** Distance metric name */
  metric: VectorMetric
  /** Document ID to node ID mapping */
  docIdToNodeId: Map<string, number>
  /** Total node count */
  totalNodeCount: number
  /** Vector dimensions */
  dimensions: number
}

/**
 * Graph state for search operations
 */
export interface GraphState {
  /** Entry point node ID (null if empty) */
  entryPoint: number | null
  /** Maximum layer in the graph */
  maxLayerInGraph: number
  /** Size of node cache */
  cacheSize: number
}

/**
 * Search a single layer for nearest neighbors
 *
 * @param query - Query vector
 * @param entryPointId - Entry point node ID for this layer
 * @param ef - Size of dynamic candidate list
 * @param layer - Layer number to search
 * @param ctx - Search context
 * @returns Array of search candidates sorted by distance
 */
export function searchLayer(
  query: number[],
  entryPointId: number,
  ef: number,
  layer: number,
  ctx: SearchContext
): SearchCandidate[] {
  const visited = new Set<number>()
  const candidates = new MinHeap()
  const results = new MaxHeap()

  const entryNode = ctx.getNode(entryPointId)
  if (!entryNode) return []

  const entryDistance = ctx.distanceFn(query, entryNode.vector)
  candidates.push({ nodeId: entryPointId, distance: entryDistance })
  results.push({ nodeId: entryPointId, distance: entryDistance })
  visited.add(entryPointId)

  while (candidates.size > 0) {
    const current = candidates.pop()!

    // Check if we can terminate (closest candidate is farther than farthest result)
    const farthestResult = results.peek()
    if (farthestResult && current.distance > farthestResult.distance) {
      break
    }

    // Explore neighbors
    const currentNode = ctx.getNode(current.nodeId)
    if (!currentNode) continue

    const connections = currentNode.connections.get(layer) ?? []
    for (const neighborId of connections) {
      if (visited.has(neighborId)) continue
      visited.add(neighborId)

      const neighbor = ctx.getNode(neighborId)
      if (!neighbor) continue

      const distance = ctx.distanceFn(query, neighbor.vector)
      const farthest = results.peek()

      if (results.size < ef || (farthest && distance < farthest.distance)) {
        candidates.push({ nodeId: neighborId, distance })
        results.push({ nodeId: neighborId, distance })

        if (results.size > ef) {
          results.pop()
        }
      }
    }
  }

  return results.toArray()
}

/**
 * Search for k nearest neighbors
 *
 * @param query - Query vector
 * @param k - Number of results to return
 * @param ctx - Search context
 * @param state - Graph state
 * @param options - Search options
 * @returns Search results
 */
export function search(
  query: number[],
  k: number,
  ctx: SearchContext,
  state: GraphState,
  options?: VectorSearchOptions
): VectorSearchResult {
  if (state.cacheSize === 0 || state.entryPoint === null) {
    return {
      docIds: [],
      rowGroups: [],
      scores: [],
      exact: false,
      entriesScanned: 0,
    }
  }

  const efSearch = options?.efSearch ?? Math.max(k, DEFAULT_HNSW_EF_SEARCH)
  const minScore = options?.minScore

  // Start from entry point and traverse down layers
  let currentNodeId = state.entryPoint
  const entryNode = ctx.getNode(currentNodeId)
  if (!entryNode) {
    // Entry point was evicted, return empty result
    return {
      docIds: [],
      rowGroups: [],
      scores: [],
      exact: false,
      entriesScanned: 0,
    }
  }
  let currentDistance = ctx.distanceFn(query, entryNode.vector)

  // Greedy search through upper layers
  for (let layer = state.maxLayerInGraph; layer > 0; layer--) {
    let improved = true
    while (improved) {
      improved = false
      const node = ctx.getNode(currentNodeId)
      if (!node) break // Node was evicted

      const connections = node.connections.get(layer) ?? []

      for (const neighborId of connections) {
        const neighbor = ctx.getNode(neighborId)
        if (!neighbor) continue

        const distance = ctx.distanceFn(query, neighbor.vector)
        if (distance < currentDistance) {
          currentNodeId = neighborId
          currentDistance = distance
          improved = true
        }
      }
    }
  }

  // Search bottom layer with ef candidates
  const candidates = searchLayer(query, currentNodeId, efSearch, 0, ctx)

  // Filter and sort results
  const results: Array<{
    docId: string
    rowGroup: number
    distance: number
  }> = []

  let entriesScanned = 0
  for (const candidate of candidates) {
    entriesScanned++
    const node = ctx.getNode(candidate.nodeId)
    if (!node) continue

    const score = distanceToScore(candidate.distance, ctx.metric)

    // Apply minimum score filter
    if (minScore !== undefined && score < minScore) continue

    results.push({
      docId: node.docId,
      rowGroup: node.rowGroup,
      distance: candidate.distance,
    })
  }

  // Sort by distance (ascending) and take top k
  results.sort((a, b) => a.distance - b.distance)
  const topK = results.slice(0, k)

  const rowGroupsSet = new Set<number>()
  for (const r of topK) {
    rowGroupsSet.add(r.rowGroup)
  }

  return {
    docIds: topK.map(r => r.docId),
    rowGroups: [...rowGroupsSet],
    scores: topK.map(r => distanceToScore(r.distance, ctx.metric)),
    exact: false,
    entriesScanned,
  }
}

/**
 * Pre-filter search: Only consider vectors in the candidate set.
 * Uses brute force search over candidates (more efficient for small candidate sets).
 *
 * @param query - Query vector
 * @param k - Number of results to return
 * @param candidateIds - Set of document IDs to consider
 * @param ctx - Search context
 * @param options - Search options
 * @returns Hybrid search results
 */
export function preFilterSearch(
  query: number[],
  k: number,
  candidateIds: Set<string>,
  ctx: SearchContext,
  options?: VectorSearchOptions
): HybridSearchResult {
  const minScore = options?.minScore

  const results: Array<{
    docId: string
    rowGroup: number
    distance: number
  }> = []

  let entriesScanned = 0

  for (const docId of candidateIds) {
    const nodeId = ctx.docIdToNodeId.get(docId)
    if (nodeId === undefined) continue

    const node = ctx.getNode(nodeId)
    if (!node) continue // Node was evicted from cache

    entriesScanned++
    const distance = ctx.distanceFn(query, node.vector)
    const score = distanceToScore(distance, ctx.metric)

    // Apply minimum score filter
    if (minScore !== undefined && score < minScore) continue

    results.push({
      docId: node.docId,
      rowGroup: node.rowGroup,
      distance,
    })
  }

  // Sort by distance and take top k
  results.sort((a, b) => a.distance - b.distance)
  const topK = results.slice(0, k)

  const rowGroupsSet = new Set<number>()
  for (const r of topK) {
    rowGroupsSet.add(r.rowGroup)
  }

  return {
    docIds: topK.map(r => r.docId),
    rowGroups: [...rowGroupsSet],
    scores: topK.map(r => distanceToScore(r.distance, ctx.metric)),
    exact: true, // Brute force is exact
    entriesScanned,
    strategyUsed: 'pre-filter',
    preFilterCount: candidateIds.size,
  }
}

/**
 * Post-filter search: Perform vector search, caller will filter results.
 * Over-fetches to account for results that will be filtered out.
 *
 * @param query - Query vector
 * @param k - Number of results to return
 * @param overFetchMultiplier - Multiplier for over-fetching
 * @param ctx - Search context
 * @param state - Graph state
 * @param options - Search options
 * @returns Hybrid search results
 */
export function postFilterSearch(
  query: number[],
  k: number,
  overFetchMultiplier: number,
  ctx: SearchContext,
  state: GraphState,
  options?: VectorSearchOptions
): HybridSearchResult {
  // Over-fetch to account for filtering
  const fetchK = k * overFetchMultiplier

  // Use standard search with increased k
  const searchResult = search(query, fetchK, ctx, state, options)

  return {
    ...searchResult,
    strategyUsed: 'post-filter',
    postFilterCount: searchResult.docIds.length,
  }
}

/**
 * Hybrid search combining vector similarity with metadata filtering.
 *
 * Supports two strategies:
 * - 'pre-filter': Restricts vector search to a set of candidate IDs
 * - 'post-filter': Performs full vector search, results are filtered by caller
 *
 * Strategy selection for 'auto' mode uses cost-based analysis:
 * - Pre-filter is efficient when candidate set is small (brute-force O(n) on small n)
 * - Post-filter is efficient when filter selectivity is high (HNSW O(log n) + filter)
 *
 * @param query - Query vector
 * @param k - Number of results to return
 * @param ctx - Search context
 * @param state - Graph state
 * @param options - Hybrid search options including candidateIds for pre-filtering
 * @returns Hybrid search results with strategy reasoning
 */
export function hybridSearch(
  query: number[],
  k: number,
  ctx: SearchContext,
  state: GraphState,
  options?: HybridSearchOptions
): HybridSearchResult {
  const strategy = options?.strategy ?? 'auto'
  const candidateIds = options?.candidateIds
  const overFetchMultiplier = options?.overFetchMultiplier ?? 3
  const efSearch = options?.efSearch ?? Math.max(k, DEFAULT_HNSW_EF_SEARCH)

  // Determine actual strategy to use with cost-based analysis
  let actualStrategy: HybridSearchStrategy = strategy
  let strategyReason = ''

  if (strategy === 'auto') {
    if (candidateIds && candidateIds.size > 0) {
      // Cost-based strategy selection
      const candidateCount = candidateIds.size
      const indexSize = ctx.totalNodeCount
      const selectivity = candidateCount / indexSize

      // Estimate pre-filter cost: brute-force search over candidates
      // Cost = O(candidateCount * dimensions) for distance computations
      const preFilterCost = candidateCount * ctx.dimensions * 0.01

      // Estimate post-filter cost: HNSW search + over-fetch + filter
      // Cost = O(efSearch * log(indexSize)) for HNSW + O(k * overFetch) for filtering
      const hnswLayers = Math.max(1, Math.floor(Math.log2(indexSize) / 2))
      const postFilterCost = efSearch * hnswLayers * 5 + k * overFetchMultiplier * 0.5

      // Additional factor: post-filter risk of not getting enough results
      // If selectivity is very low, we might need to over-fetch even more
      const postFilterRiskPenalty = selectivity < 0.1 ? 50 : (selectivity < 0.3 ? 20 : 0)
      const adjustedPostFilterCost = postFilterCost + postFilterRiskPenalty

      // Choose strategy based on cost comparison
      if (preFilterCost < adjustedPostFilterCost) {
        actualStrategy = 'pre-filter'
        strategyReason = `Pre-filter selected (cost: ${preFilterCost.toFixed(0)} vs ${adjustedPostFilterCost.toFixed(0)}): ` +
          `${candidateCount.toLocaleString()} candidates (${(selectivity * 100).toFixed(1)}% selectivity) ` +
          `efficient for brute-force scan`
      } else {
        actualStrategy = 'post-filter'
        strategyReason = `Post-filter selected (cost: ${adjustedPostFilterCost.toFixed(0)} vs ${preFilterCost.toFixed(0)}): ` +
          `HNSW search more efficient for ${indexSize.toLocaleString()} vectors, ` +
          `selectivity ${(selectivity * 100).toFixed(1)}% is high enough`
      }

      // Edge case: very small candidate sets always use pre-filter
      if (candidateCount < k * 2) {
        actualStrategy = 'pre-filter'
        strategyReason = `Pre-filter selected: candidate count (${candidateCount}) ` +
          `is smaller than 2x topK (${k * 2}), brute-force is optimal`
      }

      // Edge case: very large candidate sets (>80%) should use post-filter
      if (selectivity > 0.8 && candidateCount > 1000) {
        actualStrategy = 'post-filter'
        strategyReason = `Post-filter selected: selectivity ${(selectivity * 100).toFixed(1)}% is too high, ` +
          `HNSW search with filtering is more efficient`
      }
    } else {
      // No candidates means no filtering needed - use standard HNSW search
      actualStrategy = 'post-filter'
      strategyReason = 'Post-filter selected: no candidate set provided, using standard HNSW search'
    }

    logger.debug(`[VectorIndex.hybridSearch] ${strategyReason}`)
  } else {
    strategyReason = `Strategy explicitly set to ${strategy}`
  }

  // Handle pre-filter strategy with candidates
  if (actualStrategy === 'pre-filter' && candidateIds) {
    // Empty candidate set returns empty results
    if (candidateIds.size === 0) {
      return {
        docIds: [],
        rowGroups: [],
        scores: [],
        exact: true,
        entriesScanned: 0,
        strategyUsed: 'pre-filter',
        preFilterCount: 0,
      }
    }
    return preFilterSearch(query, k, candidateIds, ctx, options)
  } else {
    return postFilterSearch(query, k, overFetchMultiplier, ctx, state, options)
  }
}
