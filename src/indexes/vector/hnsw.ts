/**
 * HNSW Vector Index for ParqueDB
 *
 * Hierarchical Navigable Small World (HNSW) graph for approximate nearest neighbor search.
 * Provides efficient O(log n) search with high recall for vector similarity queries.
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
  VectorIndexEntry,
  VectorMetric,
} from '../types'
import { getDistanceFunction, distanceToScore } from './distance'
import { logger } from '../../utils/logger'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_M = 16 // Number of connections per layer
const DEFAULT_EF_CONSTRUCTION = 200 // Size of dynamic candidate list during construction
const DEFAULT_EF_SEARCH = 50 // Size of dynamic candidate list during search
const DEFAULT_METRIC: VectorMetric = 'cosine'
const ML = 1 / Math.log(DEFAULT_M) // Level generation factor

// File format magic and version
const MAGIC = new Uint8Array([0x50, 0x51, 0x56, 0x49]) // "PQVI"
const VERSION = 1

// =============================================================================
// Types
// =============================================================================

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
 */
export class VectorIndex {
  /** All nodes in the graph */
  private nodes: Map<number, HNSWNode> = new Map()
  /** DocId to node ID mapping */
  private docIdToNodeId: Map<string, number> = new Map()
  /** Entry point node ID */
  private entryPoint: number | null = null
  /** Maximum layer in the graph */
  private maxLayerInGraph: number = -1
  /** Next node ID */
  private nextNodeId: number = 0
  /** Whether index is loaded */
  private loaded: boolean = false

  /** HNSW parameters */
  private readonly m: number
  private readonly efConstruction: number
  private readonly dimensions: number
  private readonly metric: VectorMetric
  private readonly distanceFn: (a: number[], b: number[]) => number

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = ''
  ) {
    const options = definition.vectorOptions ?? { dimensions: 128 }
    this.dimensions = options.dimensions
    this.metric = options.metric ?? DEFAULT_METRIC
    this.m = options.m ?? DEFAULT_M
    this.efConstruction = options.efConstruction ?? DEFAULT_EF_CONSTRUCTION
    this.distanceFn = getDistanceFunction(this.metric)
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
    if (this.nodes.size === 0 || this.entryPoint === null) {
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
    let currentDistance = this.distanceFn(
      query,
      this.nodes.get(currentNodeId)!.vector
    )

    // Greedy search through upper layers
    for (let layer = this.maxLayerInGraph; layer > 0; layer--) {
      let improved = true
      while (improved) {
        improved = false
        const node = this.nodes.get(currentNodeId)!
        const connections = node.connections.get(layer) ?? []

        for (const neighborId of connections) {
          const neighbor = this.nodes.get(neighborId)
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
      const node = this.nodes.get(candidate.nodeId)
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

    this.nodes.set(nodeId, node)
    this.docIdToNodeId.set(docId, nodeId)

    // Handle first node
    if (this.entryPoint === null) {
      this.entryPoint = nodeId
      this.maxLayerInGraph = nodeLayer
      return
    }

    // Find entry point and insert
    let currentNodeId = this.entryPoint
    let currentDistance = this.distanceFn(
      vector,
      this.nodes.get(currentNodeId)!.vector
    )

    // Traverse upper layers greedily
    for (let layer = this.maxLayerInGraph; layer > nodeLayer; layer--) {
      let improved = true
      while (improved) {
        improved = false
        const currentNode = this.nodes.get(currentNodeId)!
        const connections = currentNode.connections.get(layer) ?? []

        for (const neighborId of connections) {
          const neighbor = this.nodes.get(neighborId)
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

        const neighborNode = this.nodes.get(neighbor.nodeId)
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

    const node = this.nodes.get(nodeId)
    if (!node) return false

    // Remove connections to this node from all neighbors
    for (let layer = 0; layer <= node.maxLayer; layer++) {
      const connections = node.connections.get(layer) ?? []
      for (const neighborId of connections) {
        const neighbor = this.nodes.get(neighborId)
        if (neighbor) {
          const neighborConnections = neighbor.connections.get(layer) ?? []
          const filtered = neighborConnections.filter(id => id !== nodeId)
          neighbor.connections.set(layer, filtered)
        }
      }
    }

    // Remove node
    this.nodes.delete(nodeId)
    this.docIdToNodeId.delete(docId)

    // Update entry point if necessary
    if (this.entryPoint === nodeId) {
      if (this.nodes.size === 0) {
        this.entryPoint = null
        this.maxLayerInGraph = -1
      } else {
        // Find new entry point (node with highest layer)
        let newEntryPoint: number | null = null
        let maxLayer = -1
        for (const [id, n] of this.nodes) {
          if (n.maxLayer > maxLayer) {
            maxLayer = n.maxLayer
            newEntryPoint = id
          }
        }
        this.entryPoint = newEntryPoint
        this.maxLayerInGraph = maxLayer
      }
    }

    return true
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
    this.nodes.clear()
    this.docIdToNodeId.clear()
    this.entryPoint = null
    this.maxLayerInGraph = -1
    this.nextNodeId = 0
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
    let sizeBytes = 0
    for (const node of this.nodes.values()) {
      // Vector size + connections + metadata
      sizeBytes += node.vector.length * 8 // 8 bytes per float64
      sizeBytes += node.docId.length
      sizeBytes += 16 // rowGroup, rowOffset, id, maxLayer

      for (const connections of node.connections.values()) {
        sizeBytes += connections.length * 4 // 4 bytes per node ID
      }
    }

    return {
      entryCount: this.nodes.size,
      sizeBytes,
      dimensions: this.dimensions,
      maxLayer: this.maxLayerInGraph,
    }
  }

  /**
   * Get the number of entries
   */
  get size(): number {
    return this.nodes.size
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/vector/${this.namespace}.${this.definition.name}.hnsw`
  }

  /**
   * Generate random level for a new node
   */
  private getRandomLevel(): number {
    let level = 0
    while (Math.random() < 1 / this.m && level < 32) {
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

    const entryNode = this.nodes.get(entryPointId)
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
      const currentNode = this.nodes.get(current.nodeId)
      if (!currentNode) continue

      const connections = currentNode.connections.get(layer) ?? []
      for (const neighborId of connections) {
        if (visited.has(neighborId)) continue
        visited.add(neighborId)

        const neighbor = this.nodes.get(neighborId)
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
        const node = this.nodes.get(id)
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
   */
  private serialize(): Uint8Array {
    // Calculate size
    let totalSize = 4 + 1 + 4 + 4 + 4 + 4 + 1 + 1 // header
    for (const node of this.nodes.values()) {
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
    const encoder = new TextEncoder()
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

    view.setUint32(offset, this.nodes.size, false)
    offset += 4

    view.setInt32(offset, this.entryPoint ?? -1, false)
    offset += 4

    view.setInt8(offset, this.maxLayerInGraph)
    offset += 1

    // Metric (0 = cosine, 1 = euclidean, 2 = dot)
    const metricCode = this.metric === 'euclidean' ? 1 : this.metric === 'dot' ? 2 : 0
    view.setUint8(offset, metricCode)
    offset += 1

    // Nodes
    for (const node of this.nodes.values()) {
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
    if (version !== VERSION) {
      throw new Error(`Unsupported vector index version: ${version}`)
    }

    const dimensions = view.getUint32(offset, false)
    offset += 4
    if (dimensions !== this.dimensions) {
      throw new Error(
        `Dimension mismatch: index has ${dimensions}, expected ${this.dimensions}`
      )
    }

    const _m = view.getUint32(offset, false)
    offset += 4

    const nodeCount = view.getUint32(offset, false)
    offset += 4

    const entryPoint = view.getInt32(offset, false)
    offset += 4
    this.entryPoint = entryPoint === -1 ? null : entryPoint

    this.maxLayerInGraph = view.getInt8(offset)
    offset += 1

    const _metricCode = view.getUint8(offset)
    offset += 1

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

      this.nodes.set(nodeId, node)
      this.docIdToNodeId.set(docId, nodeId)

      if (nodeId >= this.nextNodeId) {
        this.nextNodeId = nodeId + 1
      }
    }
  }
}
