/**
 * HNSW Vector Index for ParqueDB
 *
 * Hierarchical Navigable Small World (HNSW) graph for approximate nearest neighbor search.
 * Provides efficient O(log n) search with high recall for vector similarity queries.
 *
 * Memory Management:
 * - Uses LRU cache with configurable max nodes and max bytes limits
 * - Supports Product Quantization (PQ) for vector compression
 * - Automatically evicts least recently used nodes when limits are exceeded
 *
 * Storage format: indexes/vector/{ns}.{name}.hnsw
 *
 * References:
 * - Malkov, Y., & Yashunin, D. (2018). "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs"
 */

import type { StorageBackend } from '../../types/storage'
import type {
  IndexDefinition,
  IndexStats,
  VectorSearchOptions,
  VectorSearchResult,
  VectorMetric,
  HybridSearchOptions,
  HybridSearchResult,
  HybridSearchStrategy,
} from '../types'
import { getDistanceFunction, distanceToScore } from './distance'
import { logger } from '../../utils/logger'
import { LRUCache, type LRUCacheOptions } from './lru-cache'
import {
  DEFAULT_HNSW_M,
  DEFAULT_HNSW_EF_CONSTRUCTION,
  DEFAULT_HNSW_EF_SEARCH,
  DEFAULT_VECTOR_INDEX_MAX_NODES,
  DEFAULT_VECTOR_INDEX_MAX_BYTES,
  MAX_HNSW_LEVEL,
} from '../../constants'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_M = DEFAULT_HNSW_M // Number of connections per layer
const DEFAULT_EF_CONSTRUCTION = DEFAULT_HNSW_EF_CONSTRUCTION // Size of dynamic candidate list during construction
const DEFAULT_EF_SEARCH = DEFAULT_HNSW_EF_SEARCH // Size of dynamic candidate list during search
const DEFAULT_METRIC: VectorMetric = 'cosine'
/** Level generation factor for HNSW algorithm - reserved for future use */
export const _ML = 1 / Math.log(DEFAULT_M)

// File format magic and version
const MAGIC = new Uint8Array([0x50, 0x51, 0x56, 0x49]) // "PQVI"
const VERSION = 2 // Bumped for incremental update support

/**
 * Error thrown when index configuration doesn't match serialized data.
 * This error should not be silently caught - it indicates a configuration problem.
 */
export class VectorIndexConfigError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'VectorIndexConfigError'
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Options for memory-bounded vector index
 */
export interface VectorIndexMemoryOptions {
  /** Maximum number of nodes to keep in memory */
  maxNodes?: number
  /** Maximum memory in bytes */
  maxBytes?: number
  /** Callback when a node is evicted from cache */
  onEvict?: (nodeId: number) => void
}

/**
 * Metadata about a row group for incremental update tracking
 */
export interface RowGroupMetadata {
  /** Row group number */
  rowGroup: number
  /** Number of vectors indexed from this row group */
  vectorCount: number
  /** Minimum row offset indexed */
  minRowOffset: number
  /** Maximum row offset indexed */
  maxRowOffset: number
  /** Checksum/hash of the row group content for change detection */
  checksum?: string
  /** Timestamp when this row group was indexed */
  indexedAt: number
}

/**
 * Result of an incremental update operation
 */
export interface IncrementalUpdateResult {
  /** Number of vectors removed (from stale row groups) */
  removed: number
  /** Number of vectors added (from new/changed row groups) */
  added: number
  /** Row groups that were updated */
  updatedRowGroups: number[]
  /** Row groups that were removed */
  removedRowGroups: number[]
  /** Whether the update was successful */
  success: boolean
  /** Error message if update failed */
  error?: string
}

/**
 * Options for incremental update
 */
export interface IncrementalUpdateOptions {
  /** Row group checksums for change detection (rowGroup -> checksum) */
  checksums?: Map<number, string>
  /** Specific row groups to update (if not provided, auto-detect changes) */
  rowGroupsToUpdate?: number[]
  /** Row group number remapping after compaction (oldRowGroup -> newRowGroup) */
  rowGroupRemapping?: Map<number, number>
  /** Progress callback */
  onProgress?: (processed: number, total: number) => void
}

interface HNSWNode {
  id: number
  docId: string
  vector: number[]
  rowGroup: number
  rowOffset: number
  /** Connections at each layer: layer -> array of node IDs */
  connections: Map<number, number[]>
  /** Maximum layer this node exists in */
  maxLayer: number
}

interface SearchCandidate {
  nodeId: number
  distance: number
}

// =============================================================================
// Priority Queue (Min-Heap)
// =============================================================================

class MinHeap {
  private heap: SearchCandidate[] = []

  push(candidate: SearchCandidate): void {
    this.heap.push(candidate)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): SearchCandidate | undefined {
    if (this.heap.length === 0) return undefined
    const result = this.heap[0]
    const last = this.heap.pop()
    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.bubbleDown(0)
    }
    return result
  }

  peek(): SearchCandidate | undefined {
    return this.heap[0]
  }

  get size(): number {
    return this.heap.length
  }

  toArray(): SearchCandidate[] {
    return [...this.heap].sort((a, b) => a.distance - b.distance)
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.heap[parentIndex]!.distance <= this.heap[index]!.distance) break
      this.swap(parentIndex, index)
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length
    while (true) {
      const leftChild = 2 * index + 1
      const rightChild = 2 * index + 2
      let smallest = index

      if (
        leftChild < length &&
        this.heap[leftChild]!.distance < this.heap[smallest]!.distance
      ) {
        smallest = leftChild
      }

      if (
        rightChild < length &&
        this.heap[rightChild]!.distance < this.heap[smallest]!.distance
      ) {
        smallest = rightChild
      }

      if (smallest === index) break
      this.swap(index, smallest)
      index = smallest
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i]!
    this.heap[i] = this.heap[j]!
    this.heap[j] = temp
  }
}

// Max-heap for maintaining top-k results
class MaxHeap {
  private heap: SearchCandidate[] = []

  push(candidate: SearchCandidate): void {
    this.heap.push(candidate)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): SearchCandidate | undefined {
    if (this.heap.length === 0) return undefined
    const result = this.heap[0]
    const last = this.heap.pop()
    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.bubbleDown(0)
    }
    return result
  }

  peek(): SearchCandidate | undefined {
    return this.heap[0]
  }

  get size(): number {
    return this.heap.length
  }

  toArray(): SearchCandidate[] {
    return [...this.heap].sort((a, b) => a.distance - b.distance)
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.heap[parentIndex]!.distance >= this.heap[index]!.distance) break
      this.swap(parentIndex, index)
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length
    while (true) {
      const leftChild = 2 * index + 1
      const rightChild = 2 * index + 2
      let largest = index

      if (
        leftChild < length &&
        this.heap[leftChild]!.distance > this.heap[largest]!.distance
      ) {
        largest = leftChild
      }

      if (
        rightChild < length &&
        this.heap[rightChild]!.distance > this.heap[largest]!.distance
      ) {
        largest = rightChild
      }

      if (largest === index) break
      this.swap(index, largest)
      index = largest
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i]!
    this.heap[i] = this.heap[j]!
    this.heap[j] = temp
  }
}

// =============================================================================
// VectorIndex Class
// =============================================================================

/**
 * HNSW-based vector index for approximate nearest neighbor search
 *
 * Memory-bounded implementation with LRU eviction policy.
 * When the index grows beyond configured limits, least recently used
 * nodes are evicted to maintain memory bounds.
 */
export class VectorIndex {
  /** LRU cache for nodes with memory limits */
  private nodeCache: LRUCache<HNSWNode>
  /** DocId to node ID mapping (kept in memory, small overhead) */
  private docIdToNodeId: Map<string, number> = new Map()
  /** Entry point node ID */
  private entryPoint: number | null = null
  /** Maximum layer in the graph */
  private maxLayerInGraph: number = -1
  /** Next node ID */
  private nextNodeId: number = 0
  /** Whether index is loaded */
  private loaded: boolean = false
  /** Total node count (may exceed cache size) */
  private totalNodeCount: number = 0
  /** Nodes that have been evicted (for persistence) */
  private evictedNodeIds: Set<number> = new Set()
  /** Memory limit options */
  private readonly memoryOptions: VectorIndexMemoryOptions

  /** HNSW parameters */
  private readonly m: number
  private readonly efConstruction: number
  private readonly dimensions: number
  private readonly metric: VectorMetric
  private readonly distanceFn: (a: number[], b: number[]) => number

  // ===========================================================================
  // Incremental Update Tracking
  // ===========================================================================

  /** Row group metadata for incremental updates */
  private rowGroupMetadata: Map<number, RowGroupMetadata> = new Map()
  /** Index version/etag for change detection */
  private indexVersion: number = 0
  /** Timestamp when index was last built/updated */
  private lastUpdatedAt: number = 0
  /** Node ID to row group mapping for efficient removal */
  private nodeIdToRowGroup: Map<number, number> = new Map()

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = '',
    memoryOptions?: VectorIndexMemoryOptions
  ) {
    const options = definition.vectorOptions ?? { dimensions: 128 }
    this.dimensions = options.dimensions
    this.metric = options.metric ?? DEFAULT_METRIC
    this.m = options.m ?? DEFAULT_M
    this.efConstruction = options.efConstruction ?? DEFAULT_EF_CONSTRUCTION
    this.distanceFn = getDistanceFunction(this.metric)

    // Configure memory limits
    this.memoryOptions = {
      maxNodes: memoryOptions?.maxNodes ?? DEFAULT_VECTOR_INDEX_MAX_NODES,
      maxBytes: memoryOptions?.maxBytes ?? DEFAULT_VECTOR_INDEX_MAX_BYTES,
      onEvict: memoryOptions?.onEvict,
    }

    // Initialize LRU cache with memory limits
    const cacheOptions: LRUCacheOptions = {
      maxEntries: this.memoryOptions.maxNodes,
      maxBytes: this.memoryOptions.maxBytes,
      onEvict: (nodeId: number, node: unknown) => {
        this.evictedNodeIds.add(nodeId)
        if (this.memoryOptions.onEvict) {
          this.memoryOptions.onEvict(nodeId)
        }
        logger.debug(`VectorIndex: evicted node ${nodeId} (${(node as HNSWNode).docId})`)
      },
    }

    this.nodeCache = new LRUCache<HNSWNode>(cacheOptions, this.calculateNodeSize.bind(this))
  }

  /**
   * Calculate memory size of a node in bytes
   */
  private calculateNodeSize(node: HNSWNode): number {
    let size = 0
    // Vector: 8 bytes per float64
    size += node.vector.length * 8
    // DocId string (approximate)
    size += node.docId.length * 2
    // Fixed fields (id, rowGroup, rowOffset, maxLayer)
    size += 16
    // Connections Map overhead
    size += 48 // Map base overhead
    for (const connections of node.connections.values()) {
      size += 8 // Layer number
      size += connections.length * 4 // Node IDs (4 bytes each)
    }
    return size
  }

  // ===========================================================================
  // Accessor Methods (for backward compatibility)
  // ===========================================================================

  /**
   * Get a node from the cache
   * @private
   */
  private getNode(nodeId: number): HNSWNode | undefined {
    return this.nodeCache.get(nodeId)
  }

  /**
   * Set a node in the cache
   * @private
   */
  private setNode(nodeId: number, node: HNSWNode): void {
    this.nodeCache.set(nodeId, node)
    this.evictedNodeIds.delete(nodeId) // No longer evicted
  }

  /**
   * Delete a node from cache
   * @private
   */
  private deleteNode(nodeId: number): boolean {
    return this.nodeCache.delete(nodeId)
  }

  /**
   * Get all nodes in cache (for iteration)
   * @private
   */
  private *iterateNodes(): IterableIterator<HNSWNode> {
    yield* this.nodeCache.values()
  }

  // ===========================================================================
  // Loading and Saving
  // ===========================================================================

  /**
   * Load the index from storage
   */
  async load(): Promise<void> {
    if (this.loaded) return

    const path = this.getIndexPath()
    const exists = await this.storage.exists(path)

    if (!exists) {
      this.loaded = true
      return
    }

    try {
      const data = await this.storage.read(path)
      this.deserialize(data)
      this.loaded = true
    } catch (error: unknown) {
      // Re-throw configuration errors - these should not be silently caught
      if (error instanceof VectorIndexConfigError) {
        throw error
      }
      logger.warn(`Vector index load failed for ${path}, starting fresh`, error)
      this.clear()
      this.loaded = true
    }
  }

  /**
   * Save the index to storage
   */
  async save(): Promise<void> {
    const path = this.getIndexPath()
    const data = this.serialize()
    await this.storage.write(path, data)
    // Clear evictedNodeIds after successful persistence to prevent memory leak
    // The evicted nodes are now tracked in the serialized data, so we don't need
    // to keep them in memory anymore
    this.evictedNodeIds.clear()
  }

  /**
   * Check if index is ready
   */
  get ready(): boolean {
    return this.loaded
  }

  // ===========================================================================
  // Search Operations
  // ===========================================================================

  /**
   * Search for k nearest neighbors
   *
   * @param query - Query vector
   * @param k - Number of results to return
   * @param options - Search options
   * @returns Search results
   */
  search(
    query: number[],
    k: number,
    options?: VectorSearchOptions
  ): VectorSearchResult {
    if (this.nodeCache.size === 0 || this.entryPoint === null) {
      return {
        docIds: [],
        rowGroups: [],
        scores: [],
        exact: false,
        entriesScanned: 0,
      }
    }

    const efSearch = options?.efSearch ?? Math.max(k, DEFAULT_EF_SEARCH)
    const minScore = options?.minScore

    // Start from entry point and traverse down layers
    let currentNodeId = this.entryPoint
    const entryNode = this.getNode(currentNodeId)
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
    let currentDistance = this.distanceFn(query, entryNode.vector)

    // Greedy search through upper layers
    for (let layer = this.maxLayerInGraph; layer > 0; layer--) {
      let improved = true
      while (improved) {
        improved = false
        const node = this.getNode(currentNodeId)
        if (!node) break // Node was evicted

        const connections = node.connections.get(layer) ?? []

        for (const neighborId of connections) {
          const neighbor = this.getNode(neighborId)
          if (!neighbor) continue

          const distance = this.distanceFn(query, neighbor.vector)
          if (distance < currentDistance) {
            currentNodeId = neighborId
            currentDistance = distance
            improved = true
          }
        }
      }
    }

    // Search bottom layer with ef candidates
    const candidates = this.searchLayer(query, currentNodeId, efSearch, 0)

    // Filter and sort results
    const results: Array<{
      docId: string
      rowGroup: number
      distance: number
    }> = []

    let entriesScanned = 0
    for (const candidate of candidates) {
      entriesScanned++
      const node = this.getNode(candidate.nodeId)
      if (!node) continue

      const score = distanceToScore(candidate.distance, this.metric)

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
      scores: topK.map(r => distanceToScore(r.distance, this.metric)),
      exact: false,
      entriesScanned,
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
   * @param options - Hybrid search options including candidateIds for pre-filtering
   * @returns Hybrid search results with strategy reasoning
   */
  hybridSearch(
    query: number[],
    k: number,
    options?: HybridSearchOptions
  ): HybridSearchResult {
    const strategy = options?.strategy ?? 'auto'
    const candidateIds = options?.candidateIds
    const overFetchMultiplier = options?.overFetchMultiplier ?? 3
    const efSearch = options?.efSearch ?? Math.max(k, DEFAULT_EF_SEARCH)

    // Determine actual strategy to use with cost-based analysis
    let actualStrategy: HybridSearchStrategy = strategy
    let strategyReason = ''

    if (strategy === 'auto') {
      if (candidateIds && candidateIds.size > 0) {
        // Cost-based strategy selection
        const candidateCount = candidateIds.size
        const indexSize = this.totalNodeCount
        const selectivity = candidateCount / indexSize

        // Estimate pre-filter cost: brute-force search over candidates
        // Cost = O(candidateCount * dimensions) for distance computations
        const preFilterCost = candidateCount * this.dimensions * 0.01

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
      return this.preFilterSearch(query, k, candidateIds, options)
    } else {
      return this.postFilterSearch(query, k, overFetchMultiplier, options)
    }
  }

  /**
   * Pre-filter search: Only consider vectors in the candidate set
   */
  private preFilterSearch(
    query: number[],
    k: number,
    candidateIds: Set<string>,
    options?: VectorSearchOptions
  ): HybridSearchResult {
    const minScore = options?.minScore

    // Brute force search over candidates (more efficient for small candidate sets)
    // For large candidate sets, we could use a modified HNSW search
    const results: Array<{
      docId: string
      rowGroup: number
      distance: number
    }> = []

    let entriesScanned = 0

    for (const docId of candidateIds) {
      const nodeId = this.docIdToNodeId.get(docId)
      if (nodeId === undefined) continue

      const node = this.getNode(nodeId)
      if (!node) continue // Node was evicted from cache

      entriesScanned++
      const distance = this.distanceFn(query, node.vector)
      const score = distanceToScore(distance, this.metric)

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
      scores: topK.map(r => distanceToScore(r.distance, this.metric)),
      exact: true, // Brute force is exact
      entriesScanned,
      strategyUsed: 'pre-filter',
      preFilterCount: candidateIds.size,
    }
  }

  /**
   * Post-filter search: Perform vector search, caller will filter results
   * Over-fetches to account for results that will be filtered out
   * Note: Returns over-fetched results; caller is responsible for final limiting
   */
  private postFilterSearch(
    query: number[],
    k: number,
    overFetchMultiplier: number,
    options?: VectorSearchOptions
  ): HybridSearchResult {
    // Over-fetch to account for filtering
    const fetchK = k * overFetchMultiplier

    // Use standard search with increased k
    const searchResult = this.search(query, fetchK, options)

    return {
      ...searchResult,
      strategyUsed: 'post-filter',
      postFilterCount: searchResult.docIds.length,
    }
  }

  /**
   * Get all document IDs in the index (for pre-filtering intersection)
   */
  getAllDocIds(): Set<string> {
    return new Set(this.docIdToNodeId.keys())
  }

  /**
   * Check if a document exists in the index
   */
  hasDocument(docId: string): boolean {
    return this.docIdToNodeId.has(docId)
  }

  // ===========================================================================
  // Modification Operations
  // ===========================================================================

  /**
   * Insert a vector into the index
   *
   * @param vector - Vector to insert
   * @param docId - Document ID
   * @param rowGroup - Row group number
   * @param rowOffset - Row offset within row group
   */
  insert(
    vector: number[],
    docId: string,
    rowGroup: number,
    rowOffset: number
  ): void {
    // Validate dimensions
    if (vector.length !== this.dimensions) {
      return // Skip vectors with wrong dimensions
    }

    // Check if document already exists
    if (this.docIdToNodeId.has(docId)) {
      // Update existing
      this.update(vector, docId, rowGroup, rowOffset)
      return
    }

    // Generate random layer for this node
    const nodeLayer = this.getRandomLevel()

    // Create new node
    const nodeId = this.nextNodeId++
    const node: HNSWNode = {
      id: nodeId,
      docId,
      vector,
      rowGroup,
      rowOffset,
      connections: new Map(),
      maxLayer: nodeLayer,
    }

    // Initialize empty connection lists for each layer
    for (let l = 0; l <= nodeLayer; l++) {
      node.connections.set(l, [])
    }

    this.setNode(nodeId, node)
    this.docIdToNodeId.set(docId, nodeId)
    this.totalNodeCount++

    // Track node to row group mapping for incremental updates
    this.nodeIdToRowGroup.set(nodeId, rowGroup)
    this.updateRowGroupMetadata(rowGroup, rowOffset)

    // Handle first node
    if (this.entryPoint === null) {
      this.entryPoint = nodeId
      this.maxLayerInGraph = nodeLayer
      return
    }

    // Find entry point and insert
    let currentNodeId = this.entryPoint
    const entryNode = this.getNode(currentNodeId)
    if (!entryNode) {
      // Entry point was evicted, use this node as new entry
      this.entryPoint = nodeId
      this.maxLayerInGraph = nodeLayer
      return
    }
    let currentDistance = this.distanceFn(vector, entryNode.vector)

    // Traverse upper layers greedily
    for (let layer = this.maxLayerInGraph; layer > nodeLayer; layer--) {
      let improved = true
      while (improved) {
        improved = false
        const currentNode = this.getNode(currentNodeId)
        if (!currentNode) break // Node was evicted

        const connections = currentNode.connections.get(layer) ?? []

        for (const neighborId of connections) {
          const neighbor = this.getNode(neighborId)
          if (!neighbor) continue

          const distance = this.distanceFn(vector, neighbor.vector)
          if (distance < currentDistance) {
            currentNodeId = neighborId
            currentDistance = distance
            improved = true
          }
        }
      }
    }

    // Insert into layers from nodeLayer down to 0
    for (let layer = Math.min(nodeLayer, this.maxLayerInGraph); layer >= 0; layer--) {
      // Find neighbors at this layer
      const candidates = this.searchLayer(
        vector,
        currentNodeId,
        this.efConstruction,
        layer
      )

      // Select M neighbors to connect to
      const neighbors = this.selectNeighbors(vector, candidates, this.m)

      // Add bidirectional connections
      const nodeConnections = node.connections.get(layer) ?? []
      for (const neighbor of neighbors) {
        nodeConnections.push(neighbor.nodeId)

        const neighborNode = this.getNode(neighbor.nodeId)
        if (neighborNode) {
          const neighborConnections = neighborNode.connections.get(layer) ?? []
          neighborConnections.push(nodeId)

          // Prune if necessary
          if (neighborConnections.length > this.m * 2) {
            const prunedNeighbors = this.pruneConnections(
              neighborNode.vector,
              neighborConnections,
              this.m * 2,
              layer
            )
            neighborNode.connections.set(layer, prunedNeighbors)
          } else {
            neighborNode.connections.set(layer, neighborConnections)
          }
        }
      }
      node.connections.set(layer, nodeConnections)

      // Update entry point for next layer
      if (candidates.length > 0) {
        currentNodeId = candidates[0]!.nodeId
      }
    }

    // Update global entry point if this node has higher layer
    if (nodeLayer > this.maxLayerInGraph) {
      this.entryPoint = nodeId
      this.maxLayerInGraph = nodeLayer
    }
  }

  /**
   * Remove a document from the index
   *
   * @param docId - Document ID to remove
   * @returns true if removed
   */
  remove(docId: string): boolean {
    const nodeId = this.docIdToNodeId.get(docId)
    if (nodeId === undefined) return false

    // Get row group for metadata tracking before removal
    const rowGroup = this.nodeIdToRowGroup.get(nodeId)

    const node = this.getNode(nodeId)
    if (!node) {
      // Node was evicted from cache but still tracked - clean up mapping
      this.docIdToNodeId.delete(docId)
      this.evictedNodeIds.delete(nodeId)
      this.nodeIdToRowGroup.delete(nodeId)
      this.totalNodeCount--
      // Update row group metadata
      if (rowGroup !== undefined) {
        this.decrementRowGroupMetadata(rowGroup)
      }
      return true
    }

    // Remove connections to this node from all neighbors
    for (let layer = 0; layer <= node.maxLayer; layer++) {
      const connections = node.connections.get(layer) ?? []
      for (const neighborId of connections) {
        const neighbor = this.getNode(neighborId)
        if (neighbor) {
          const neighborConnections = neighbor.connections.get(layer) ?? []
          const filtered = neighborConnections.filter(id => id !== nodeId)
          neighbor.connections.set(layer, filtered)
        }
      }
    }

    // Remove node
    this.deleteNode(nodeId)
    this.docIdToNodeId.delete(docId)
    this.evictedNodeIds.delete(nodeId)
    this.nodeIdToRowGroup.delete(nodeId)
    this.totalNodeCount--

    // Update row group metadata
    if (rowGroup !== undefined) {
      this.decrementRowGroupMetadata(rowGroup)
    }

    // Update entry point if necessary
    if (this.entryPoint === nodeId) {
      if (this.nodeCache.size === 0) {
        this.entryPoint = null
        this.maxLayerInGraph = -1
      } else {
        // Find new entry point (node with highest layer among cached nodes)
        let newEntryPoint: number | null = null
        let maxLayer = -1
        for (const n of this.iterateNodes()) {
          if (n.maxLayer > maxLayer) {
            maxLayer = n.maxLayer
            newEntryPoint = n.id
          }
        }
        this.entryPoint = newEntryPoint
        this.maxLayerInGraph = maxLayer
      }
    }

    return true
  }

  /**
   * Decrement the vector count for a row group
   * @private
   */
  private decrementRowGroupMetadata(rowGroup: number): void {
    const metadata = this.rowGroupMetadata.get(rowGroup)
    if (metadata) {
      metadata.vectorCount--
      if (metadata.vectorCount <= 0) {
        this.rowGroupMetadata.delete(rowGroup)
      }
    }
  }

  /**
   * Update a document's vector
   *
   * @param vector - New vector
   * @param docId - Document ID
   * @param rowGroup - Row group number
   * @param rowOffset - Row offset within row group
   * @returns true if updated
   */
  update(
    vector: number[],
    docId: string,
    rowGroup: number,
    rowOffset: number
  ): boolean {
    if (vector.length !== this.dimensions) {
      return false
    }

    // Simple implementation: remove and reinsert
    const existed = this.docIdToNodeId.has(docId)
    if (existed) {
      this.remove(docId)
    }

    this.insert(vector, docId, rowGroup, rowOffset)
    return existed
  }

  /**
   * Clear all entries from the index
   */
  clear(): void {
    this.nodeCache.clear()
    this.docIdToNodeId.clear()
    this.evictedNodeIds.clear()
    this.entryPoint = null
    this.maxLayerInGraph = -1
    this.nextNodeId = 0
    this.totalNodeCount = 0
    // Clear incremental update tracking
    this.rowGroupMetadata.clear()
    this.nodeIdToRowGroup.clear()
    this.indexVersion = 0
    this.lastUpdatedAt = 0
  }

  // ===========================================================================
  // Incremental Update Operations
  // ===========================================================================

  /**
   * Update row group metadata when inserting a vector
   * @private
   */
  private updateRowGroupMetadata(rowGroup: number, rowOffset: number): void {
    const existing = this.rowGroupMetadata.get(rowGroup)
    if (existing) {
      existing.vectorCount++
      existing.minRowOffset = Math.min(existing.minRowOffset, rowOffset)
      existing.maxRowOffset = Math.max(existing.maxRowOffset, rowOffset)
      existing.indexedAt = Date.now()
    } else {
      this.rowGroupMetadata.set(rowGroup, {
        rowGroup,
        vectorCount: 1,
        minRowOffset: rowOffset,
        maxRowOffset: rowOffset,
        indexedAt: Date.now(),
      })
    }
  }

  /**
   * Get metadata for all indexed row groups
   */
  getRowGroupMetadata(): Map<number, RowGroupMetadata> {
    return new Map(this.rowGroupMetadata)
  }

  /**
   * Get the current index version
   */
  getIndexVersion(): number {
    return this.indexVersion
  }

  /**
   * Get the timestamp of the last update
   */
  getLastUpdatedAt(): number {
    return this.lastUpdatedAt
  }

  /**
   * Get document IDs for a specific row group
   */
  getDocIdsForRowGroup(rowGroup: number): string[] {
    const docIds: string[] = []
    for (const [docId, nodeId] of this.docIdToNodeId) {
      const rg = this.nodeIdToRowGroup.get(nodeId)
      if (rg === rowGroup) {
        docIds.push(docId)
      }
    }
    return docIds
  }

  /**
   * Remove all vectors from a specific row group
   * @returns Number of vectors removed
   */
  removeRowGroup(rowGroup: number): number {
    const docIdsToRemove = this.getDocIdsForRowGroup(rowGroup)
    let removed = 0
    for (const docId of docIdsToRemove) {
      if (this.remove(docId)) {
        removed++
      }
    }
    this.rowGroupMetadata.delete(rowGroup)
    return removed
  }

  /**
   * Detect which row groups have changed based on checksums
   *
   * @param currentChecksums - Map of current row group checksums
   * @returns Object with added, modified, and removed row groups
   */
  detectChangedRowGroups(currentChecksums: Map<number, string>): {
    added: number[]
    modified: number[]
    removed: number[]
  } {
    const added: number[] = []
    const modified: number[] = []
    const removed: number[] = []

    // Check for added and modified row groups
    for (const [rowGroup, checksum] of currentChecksums) {
      const existing = this.rowGroupMetadata.get(rowGroup)
      if (!existing) {
        added.push(rowGroup)
      } else if (existing.checksum !== checksum) {
        modified.push(rowGroup)
      }
    }

    // Check for removed row groups
    for (const rowGroup of this.rowGroupMetadata.keys()) {
      if (!currentChecksums.has(rowGroup)) {
        removed.push(rowGroup)
      }
    }

    return { added, modified, removed }
  }

  /**
   * Apply row group renumbering after compaction
   *
   * When Parquet files are compacted, row group numbers may change.
   * This method updates the internal mappings to reflect the new numbering.
   *
   * @param remapping - Map from old row group number to new row group number
   */
  remapRowGroups(remapping: Map<number, number>): void {
    // Update row group metadata
    const newRowGroupMetadata = new Map<number, RowGroupMetadata>()
    for (const [oldRowGroup, metadata] of this.rowGroupMetadata) {
      const newRowGroup = remapping.get(oldRowGroup)
      if (newRowGroup !== undefined) {
        newRowGroupMetadata.set(newRowGroup, {
          ...metadata,
          rowGroup: newRowGroup,
        })
      }
      // If no mapping exists, the row group was removed
    }
    this.rowGroupMetadata = newRowGroupMetadata

    // Update node to row group mapping
    const newNodeIdToRowGroup = new Map<number, number>()
    for (const [nodeId, oldRowGroup] of this.nodeIdToRowGroup) {
      const newRowGroup = remapping.get(oldRowGroup)
      if (newRowGroup !== undefined) {
        newNodeIdToRowGroup.set(nodeId, newRowGroup)
      }
    }
    this.nodeIdToRowGroup = newNodeIdToRowGroup

    // Update nodes in cache
    for (const node of this.iterateNodes()) {
      const newRowGroup = remapping.get(node.rowGroup)
      if (newRowGroup !== undefined) {
        node.rowGroup = newRowGroup
      }
    }

    this.indexVersion++
    this.lastUpdatedAt = Date.now()
  }

  /**
   * Perform an incremental update of the index
   *
   * This method efficiently updates the index when the underlying data changes,
   * without requiring a full rebuild. It:
   * 1. Removes vectors from stale (deleted/modified) row groups
   * 2. Inserts vectors from new/changed row groups
   * 3. Optionally handles row group renumbering after compaction
   *
   * @param data - Iterator over documents to add/update
   * @param options - Incremental update options
   * @returns Result of the incremental update
   */
  async incrementalUpdate(
    data: AsyncIterable<{
      doc: Record<string, unknown>
      docId: string
      rowGroup: number
      rowOffset: number
    }>,
    options?: IncrementalUpdateOptions
  ): Promise<IncrementalUpdateResult> {
    const result: IncrementalUpdateResult = {
      removed: 0,
      added: 0,
      updatedRowGroups: [],
      removedRowGroups: [],
      success: true,
    }

    try {
      // Handle row group remapping first (e.g., after compaction)
      if (options?.rowGroupRemapping) {
        this.remapRowGroups(options.rowGroupRemapping)
      }

      // Determine which row groups to update
      let rowGroupsToProcess: Set<number>

      if (options?.rowGroupsToUpdate) {
        // Explicit list of row groups to update
        rowGroupsToProcess = new Set(options.rowGroupsToUpdate)
      } else if (options?.checksums) {
        // Auto-detect changes from checksums
        const changes = this.detectChangedRowGroups(options.checksums)

        // Remove vectors from deleted row groups
        for (const rowGroup of changes.removed) {
          const removedCount = this.removeRowGroup(rowGroup)
          result.removed += removedCount
          result.removedRowGroups.push(rowGroup)
        }

        // Process added and modified row groups
        rowGroupsToProcess = new Set([...changes.added, ...changes.modified])

        // Remove vectors from modified row groups before re-inserting
        for (const rowGroup of changes.modified) {
          const removedCount = this.removeRowGroup(rowGroup)
          result.removed += removedCount
        }
      } else {
        // No checksums or explicit list - process all incoming data
        rowGroupsToProcess = new Set<number>()
      }

      // Process incoming data
      let processed = 0
      let total = 0

      // First pass: count total if progress callback is provided
      if (options?.onProgress) {
        const dataArray: Array<{
          doc: Record<string, unknown>
          docId: string
          rowGroup: number
          rowOffset: number
        }> = []
        for await (const item of data) {
          dataArray.push(item)
        }
        total = dataArray.length

        // Process the collected data
        for (const { doc, docId, rowGroup, rowOffset } of dataArray) {
          // Skip if we have a specific set and this row group isn't in it
          if (rowGroupsToProcess.size > 0 && !rowGroupsToProcess.has(rowGroup)) {
            continue
          }

          const vector = this.extractVector(doc)
          if (vector !== undefined && vector.length === this.dimensions) {
            this.insert(vector, docId, rowGroup, rowOffset)
            result.added++
            if (!result.updatedRowGroups.includes(rowGroup)) {
              result.updatedRowGroups.push(rowGroup)
            }
          }

          processed++
          if (processed % 100 === 0) {
            options.onProgress(processed, total)
          }
        }

        options.onProgress(processed, total)
      } else {
        // No progress callback - process directly from iterator
        for await (const { doc, docId, rowGroup, rowOffset } of data) {
          // Skip if we have a specific set and this row group isn't in it
          if (rowGroupsToProcess.size > 0 && !rowGroupsToProcess.has(rowGroup)) {
            continue
          }

          const vector = this.extractVector(doc)
          if (vector !== undefined && vector.length === this.dimensions) {
            this.insert(vector, docId, rowGroup, rowOffset)
            result.added++
            if (!result.updatedRowGroups.includes(rowGroup)) {
              result.updatedRowGroups.push(rowGroup)
            }
          }
        }
      }

      // Update checksums in metadata if provided
      if (options?.checksums) {
        for (const [rowGroup, checksum] of options.checksums) {
          const metadata = this.rowGroupMetadata.get(rowGroup)
          if (metadata) {
            metadata.checksum = checksum
          }
        }
      }

      // Update index version and timestamp
      this.indexVersion++
      this.lastUpdatedAt = Date.now()

    } catch (error) {
      result.success = false
      result.error = error instanceof Error ? error.message : String(error)
      logger.error('VectorIndex incremental update failed', error)
    }

    return result
  }

  /**
   * Perform an incremental update from an array of documents (for testing)
   */
  incrementalUpdateFromArray(
    data: Array<{
      doc: Record<string, unknown>
      docId: string
      rowGroup: number
      rowOffset: number
    }>,
    options?: Omit<IncrementalUpdateOptions, 'onProgress'>
  ): IncrementalUpdateResult {
    const result: IncrementalUpdateResult = {
      removed: 0,
      added: 0,
      updatedRowGroups: [],
      removedRowGroups: [],
      success: true,
    }

    try {
      // Handle row group remapping first (e.g., after compaction)
      if (options?.rowGroupRemapping) {
        this.remapRowGroups(options.rowGroupRemapping)
      }

      // Determine which row groups to update
      let rowGroupsToProcess: Set<number>

      if (options?.rowGroupsToUpdate) {
        rowGroupsToProcess = new Set(options.rowGroupsToUpdate)
      } else if (options?.checksums) {
        const changes = this.detectChangedRowGroups(options.checksums)

        for (const rowGroup of changes.removed) {
          const removedCount = this.removeRowGroup(rowGroup)
          result.removed += removedCount
          result.removedRowGroups.push(rowGroup)
        }

        rowGroupsToProcess = new Set([...changes.added, ...changes.modified])

        for (const rowGroup of changes.modified) {
          const removedCount = this.removeRowGroup(rowGroup)
          result.removed += removedCount
        }
      } else {
        rowGroupsToProcess = new Set<number>()
      }

      // Process incoming data
      for (const { doc, docId, rowGroup, rowOffset } of data) {
        if (rowGroupsToProcess.size > 0 && !rowGroupsToProcess.has(rowGroup)) {
          continue
        }

        const vector = this.extractVector(doc)
        if (vector !== undefined && vector.length === this.dimensions) {
          this.insert(vector, docId, rowGroup, rowOffset)
          result.added++
          if (!result.updatedRowGroups.includes(rowGroup)) {
            result.updatedRowGroups.push(rowGroup)
          }
        }
      }

      // Update checksums
      if (options?.checksums) {
        for (const [rowGroup, checksum] of options.checksums) {
          const metadata = this.rowGroupMetadata.get(rowGroup)
          if (metadata) {
            metadata.checksum = checksum
          }
        }
      }

      this.indexVersion++
      this.lastUpdatedAt = Date.now()

    } catch (error) {
      result.success = false
      result.error = error instanceof Error ? error.message : String(error)
    }

    return result
  }

  // ===========================================================================
  // Build Operations
  // ===========================================================================

  /**
   * Build index from a data iterator
   */
  async build(
    data: AsyncIterable<{
      doc: Record<string, unknown>
      docId: string
      rowGroup: number
      rowOffset: number
    }>,
    options?: { onProgress?: (processed: number) => void }
  ): Promise<void> {
    this.clear()

    let processed = 0
    for await (const { doc, docId, rowGroup, rowOffset } of data) {
      const vector = this.extractVector(doc)
      if (vector !== undefined && vector.length === this.dimensions) {
        this.insert(vector, docId, rowGroup, rowOffset)
      }

      processed++
      if (options?.onProgress && processed % 1000 === 0) {
        options.onProgress(processed)
      }
    }
  }

  /**
   * Build from an array of documents (for testing)
   */
  buildFromArray(
    data: Array<{
      doc: Record<string, unknown>
      docId: string
      rowGroup: number
      rowOffset: number
    }>
  ): void {
    this.clear()

    for (const { doc, docId, rowGroup, rowOffset } of data) {
      const vector = this.extractVector(doc)
      if (vector !== undefined && vector.length === this.dimensions) {
        this.insert(vector, docId, rowGroup, rowOffset)
      }
    }
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    return {
      entryCount: this.totalNodeCount,
      sizeBytes: this.nodeCache.bytes,
      dimensions: this.dimensions,
      maxLayer: this.maxLayerInGraph,
      // Additional memory stats
      cachedNodes: this.nodeCache.size,
      evictedNodes: this.evictedNodeIds.size,
      maxNodes: this.memoryOptions.maxNodes,
      maxBytes: this.memoryOptions.maxBytes,
    } as IndexStats
  }

  /**
   * Get the number of entries (total, including evicted)
   */
  get size(): number {
    return this.totalNodeCount
  }

  /**
   * Get the number of nodes currently in cache
   */
  get cachedSize(): number {
    return this.nodeCache.size
  }

  /**
   * Get current memory usage in bytes
   */
  get memoryUsage(): number {
    return this.nodeCache.bytes
  }

  /**
   * Get memory limits configuration
   */
  get memoryLimits(): { maxNodes: number; maxBytes: number } {
    return {
      maxNodes: this.memoryOptions.maxNodes ?? DEFAULT_VECTOR_INDEX_MAX_NODES,
      maxBytes: this.memoryOptions.maxBytes ?? DEFAULT_VECTOR_INDEX_MAX_BYTES,
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/vector/${this.namespace}.${this.definition.name}.hnsw`
  }

  /**
   * Generate random level for a new node.
   * Uses geometric distribution with probability 1/M per level.
   * Capped at MAX_HNSW_LEVEL to prevent unbounded graph depth.
   */
  private getRandomLevel(): number {
    let level = 0
    while (Math.random() < 1 / this.m && level < MAX_HNSW_LEVEL) {
      level++
    }
    return level
  }

  /**
   * Search a single layer for nearest neighbors
   */
  private searchLayer(
    query: number[],
    entryPointId: number,
    ef: number,
    layer: number
  ): SearchCandidate[] {
    const visited = new Set<number>()
    const candidates = new MinHeap()
    const results = new MaxHeap()

    const entryNode = this.getNode(entryPointId)
    if (!entryNode) return []

    const entryDistance = this.distanceFn(query, entryNode.vector)
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
      const currentNode = this.getNode(current.nodeId)
      if (!currentNode) continue

      const connections = currentNode.connections.get(layer) ?? []
      for (const neighborId of connections) {
        if (visited.has(neighborId)) continue
        visited.add(neighborId)

        const neighbor = this.getNode(neighborId)
        if (!neighbor) continue

        const distance = this.distanceFn(query, neighbor.vector)
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
   * Select M best neighbors from candidates
   */
  private selectNeighbors(
    vector: number[],
    candidates: SearchCandidate[],
    m: number
  ): SearchCandidate[] {
    // Simple heuristic: take M closest
    return candidates.slice(0, m)
  }

  /**
   * Prune connections to maintain M limit
   */
  private pruneConnections(
    nodeVector: number[],
    connections: number[],
    maxConnections: number,
    _layer: number
  ): number[] {
    if (connections.length <= maxConnections) {
      return connections
    }

    // Calculate distances and sort
    const withDistances = connections
      .map(id => {
        const node = this.getNode(id)
        if (!node) return null
        return {
          id,
          distance: this.distanceFn(nodeVector, node.vector),
        }
      })
      .filter((x): x is { id: number; distance: number } => x !== null)

    withDistances.sort((a, b) => a.distance - b.distance)

    return withDistances.slice(0, maxConnections).map(x => x.id)
  }

  private extractVector(doc: Record<string, unknown>): number[] | undefined {
    const firstField = this.definition.fields[0]
    if (!firstField) return undefined

    const value = this.getNestedValue(doc, firstField.path)
    if (!Array.isArray(value)) return undefined

    // Verify all elements are numbers
    if (!value.every(v => typeof v === 'number')) return undefined

    return value as number[]
  }

  private getNestedValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current === null || current === undefined) return undefined
      if (typeof current !== 'object') return undefined
      current = (current as Record<string, unknown>)[part]
    }

    return current
  }

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize the index to bytes
   * Note: Only serializes nodes currently in cache
   * Format v2 includes incremental update metadata
   */
  private serialize(): Uint8Array {
    const encoder = new TextEncoder()

    // Calculate size
    // Header: magic(4) + version(1) + dimensions(4) + m(4) + nodeCount(4) + entryPoint(4) + maxLayer(1) + metric(1)
    // V2 additions: indexVersion(4) + lastUpdatedAt(8) + rowGroupMetadataCount(4) + nodeIdToRowGroupCount(4)
    let totalSize = 4 + 1 + 4 + 4 + 4 + 4 + 1 + 1 + 4 + 8 + 4 + 4

    // Row group metadata
    for (const metadata of this.rowGroupMetadata.values()) {
      // rowGroup(4) + vectorCount(4) + minRowOffset(4) + maxRowOffset(4) + indexedAt(8) + checksumLen(4) + checksum
      totalSize += 4 + 4 + 4 + 4 + 8 + 4
      if (metadata.checksum) {
        totalSize += encoder.encode(metadata.checksum).length
      }
    }

    // Node to row group mappings
    totalSize += this.nodeIdToRowGroup.size * 8 // nodeId(4) + rowGroup(4)

    // Nodes
    for (const node of this.iterateNodes()) {
      totalSize += 4 // node ID
      totalSize += 4 + node.docId.length // docId length + docId
      totalSize += node.vector.length * 8 // vector
      totalSize += 4 + 4 // rowGroup, rowOffset
      totalSize += 1 // maxLayer
      totalSize += 4 // number of layers with connections

      for (const connections of node.connections.values()) {
        totalSize += 1 // layer number
        totalSize += 4 // number of connections
        totalSize += connections.length * 4 // connection IDs
      }
    }

    const buffer = new ArrayBuffer(totalSize)
    const view = new DataView(buffer)
    const bytes = new Uint8Array(buffer)
    let offset = 0

    // Header
    bytes.set(MAGIC, offset)
    offset += 4

    view.setUint8(offset, VERSION)
    offset += 1

    view.setUint32(offset, this.dimensions, false)
    offset += 4

    view.setUint32(offset, this.m, false)
    offset += 4

    view.setUint32(offset, this.nodeCache.size, false)
    offset += 4

    view.setInt32(offset, this.entryPoint ?? -1, false)
    offset += 4

    view.setInt8(offset, this.maxLayerInGraph)
    offset += 1

    // Metric (0 = cosine, 1 = euclidean, 2 = dot)
    const metricCode = this.metric === 'euclidean' ? 1 : this.metric === 'dot' ? 2 : 0
    view.setUint8(offset, metricCode)
    offset += 1

    // V2: Incremental update metadata
    view.setUint32(offset, this.indexVersion, false)
    offset += 4

    // Store lastUpdatedAt as BigInt64
    view.setBigInt64(offset, BigInt(this.lastUpdatedAt), false)
    offset += 8

    // Row group metadata
    view.setUint32(offset, this.rowGroupMetadata.size, false)
    offset += 4

    for (const metadata of this.rowGroupMetadata.values()) {
      view.setUint32(offset, metadata.rowGroup, false)
      offset += 4
      view.setUint32(offset, metadata.vectorCount, false)
      offset += 4
      view.setUint32(offset, metadata.minRowOffset, false)
      offset += 4
      view.setUint32(offset, metadata.maxRowOffset, false)
      offset += 4
      view.setBigInt64(offset, BigInt(metadata.indexedAt), false)
      offset += 8

      const checksumBytes = metadata.checksum ? encoder.encode(metadata.checksum) : new Uint8Array(0)
      view.setUint32(offset, checksumBytes.length, false)
      offset += 4
      if (checksumBytes.length > 0) {
        bytes.set(checksumBytes, offset)
        offset += checksumBytes.length
      }
    }

    // Node to row group mappings
    view.setUint32(offset, this.nodeIdToRowGroup.size, false)
    offset += 4

    for (const [nodeId, rowGroup] of this.nodeIdToRowGroup) {
      view.setUint32(offset, nodeId, false)
      offset += 4
      view.setUint32(offset, rowGroup, false)
      offset += 4
    }

    // Nodes
    for (const node of this.iterateNodes()) {
      view.setUint32(offset, node.id, false)
      offset += 4

      const docIdBytes = encoder.encode(node.docId)
      view.setUint32(offset, docIdBytes.length, false)
      offset += 4
      bytes.set(docIdBytes, offset)
      offset += docIdBytes.length

      for (let i = 0; i < node.vector.length; i++) {
        view.setFloat64(offset, node.vector[i]!, false)
        offset += 8
      }

      view.setUint32(offset, node.rowGroup, false)
      offset += 4
      view.setUint32(offset, node.rowOffset, false)
      offset += 4

      view.setUint8(offset, node.maxLayer)
      offset += 1

      const layerCount = node.connections.size
      view.setUint32(offset, layerCount, false)
      offset += 4

      for (const [layer, connections] of node.connections) {
        view.setUint8(offset, layer)
        offset += 1

        view.setUint32(offset, connections.length, false)
        offset += 4

        for (const connId of connections) {
          view.setUint32(offset, connId, false)
          offset += 4
        }
      }
    }

    return bytes.slice(0, offset)
  }

  /**
   * Deserialize the index from bytes
   * Supports both v1 (no incremental metadata) and v2 (with incremental metadata)
   */
  private deserialize(data: Uint8Array): void {
    this.clear()

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const decoder = new TextDecoder()
    let offset = 0

    // Verify magic
    for (let i = 0; i < 4; i++) {
      if (data[offset + i] !== MAGIC[i]) {
        throw new Error('Invalid vector index: bad magic')
      }
    }
    offset += 4

    const version = view.getUint8(offset)
    offset += 1

    // Support both v1 and v2
    if (version !== 1 && version !== VERSION) {
      throw new Error(`Unsupported vector index version: ${version}`)
    }

    const dimensions = view.getUint32(offset, false)
    offset += 4
    if (dimensions !== this.dimensions) {
      throw new VectorIndexConfigError(
        `Dimension mismatch: index has ${dimensions}, expected ${this.dimensions}`
      )
    }

    const __m = view.getUint32(offset, false)
    offset += 4
    void __m // Reserved field for future use

    const nodeCount = view.getUint32(offset, false)
    offset += 4

    const entryPoint = view.getInt32(offset, false)
    offset += 4
    this.entryPoint = entryPoint === -1 ? null : entryPoint

    this.maxLayerInGraph = view.getInt8(offset)
    offset += 1

    const serializedMetricCode = view.getUint8(offset)
    offset += 1
    // Validate metric code matches (0 = cosine, 1 = euclidean, 2 = dot)
    const expectedMetricCode = this.metric === 'euclidean' ? 1 : this.metric === 'dot' ? 2 : 0
    if (serializedMetricCode !== expectedMetricCode) {
      const metricNames = ['cosine', 'euclidean', 'dot'] as const
      const serializedMetric = metricNames[serializedMetricCode] ?? `unknown(${serializedMetricCode})`
      throw new VectorIndexConfigError(
        `Metric mismatch: index was serialized with '${serializedMetric}' but loaded with '${this.metric}'`
      )
    }

    // V2: Read incremental update metadata
    if (version >= 2) {
      this.indexVersion = view.getUint32(offset, false)
      offset += 4

      this.lastUpdatedAt = Number(view.getBigInt64(offset, false))
      offset += 8

      // Read row group metadata
      const rowGroupMetadataCount = view.getUint32(offset, false)
      offset += 4

      for (let i = 0; i < rowGroupMetadataCount; i++) {
        const rowGroup = view.getUint32(offset, false)
        offset += 4
        const vectorCount = view.getUint32(offset, false)
        offset += 4
        const minRowOffset = view.getUint32(offset, false)
        offset += 4
        const maxRowOffset = view.getUint32(offset, false)
        offset += 4
        const indexedAt = Number(view.getBigInt64(offset, false))
        offset += 8

        const checksumLen = view.getUint32(offset, false)
        offset += 4
        let checksum: string | undefined
        if (checksumLen > 0) {
          checksum = decoder.decode(data.slice(offset, offset + checksumLen))
          offset += checksumLen
        }

        this.rowGroupMetadata.set(rowGroup, {
          rowGroup,
          vectorCount,
          minRowOffset,
          maxRowOffset,
          indexedAt,
          checksum,
        })
      }

      // Read node to row group mappings
      const nodeToRowGroupCount = view.getUint32(offset, false)
      offset += 4

      for (let i = 0; i < nodeToRowGroupCount; i++) {
        const nodeId = view.getUint32(offset, false)
        offset += 4
        const rowGroup = view.getUint32(offset, false)
        offset += 4
        this.nodeIdToRowGroup.set(nodeId, rowGroup)
      }
    }

    // Read nodes
    for (let i = 0; i < nodeCount; i++) {
      const nodeId = view.getUint32(offset, false)
      offset += 4

      const docIdLen = view.getUint32(offset, false)
      offset += 4
      const docId = decoder.decode(data.slice(offset, offset + docIdLen))
      offset += docIdLen

      const vector: number[] = []
      for (let j = 0; j < dimensions; j++) {
        vector.push(view.getFloat64(offset, false))
        offset += 8
      }

      const rowGroup = view.getUint32(offset, false)
      offset += 4
      const rowOffset = view.getUint32(offset, false)
      offset += 4

      const maxLayer = view.getUint8(offset)
      offset += 1

      const layerCount = view.getUint32(offset, false)
      offset += 4

      const connections = new Map<number, number[]>()
      for (let l = 0; l < layerCount; l++) {
        const layer = view.getUint8(offset)
        offset += 1

        const connCount = view.getUint32(offset, false)
        offset += 4

        const conns: number[] = []
        for (let c = 0; c < connCount; c++) {
          conns.push(view.getUint32(offset, false))
          offset += 4
        }
        connections.set(layer, conns)
      }

      const node: HNSWNode = {
        id: nodeId,
        docId,
        vector,
        rowGroup,
        rowOffset,
        connections,
        maxLayer,
      }

      this.setNode(nodeId, node)
      this.docIdToNodeId.set(docId, nodeId)
      this.totalNodeCount++

      // For v1 indexes, rebuild the nodeIdToRowGroup mapping
      if (version === 1) {
        this.nodeIdToRowGroup.set(nodeId, rowGroup)
        this.updateRowGroupMetadata(rowGroup, rowOffset)
      }

      if (nodeId >= this.nextNodeId) {
        this.nextNodeId = nodeId + 1
      }
    }
  }
}
