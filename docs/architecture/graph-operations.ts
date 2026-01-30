/**
 * ParqueDB Graph-First Architecture - Graph Operations
 *
 * This module provides the core graph traversal and query operations
 * built on the edge-centric storage model.
 */

import type {
  Namespace,
  EntityId,
  RelationType,
  Timestamp,
  Node,
  Edge,
  CDCEvent,
  MaterializedPath,
  RelationshipOperator,
  Variant,
  QueryFilter,
  TraversalOptions,
  PathResult,
  EdgeBloomFilter,
  RowGroupStats,
} from './graph-schemas'

// ============================================================================
// Bucket Interface (R2/fsx abstraction)
// ============================================================================

/**
 * Storage bucket interface compatible with Cloudflare R2 and fsx
 */
export interface StorageBucket {
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<void>
  list(options?: { prefix?: string }): Promise<{ objects: { key: string }[] }>
  delete(key: string): Promise<void>
}

// ============================================================================
// Index Manifest Types
// ============================================================================

/**
 * File entry in an edge index manifest
 */
export interface ManifestFileEntry {
  path: string
  rowCount: number
  sizeBytes: number
  minKey: {
    ns: string
    primaryId: string  // from_id for forward, to_id for reverse
    relType: string
  }
  maxKey: {
    ns: string
    primaryId: string
    relType: string
  }
  status: 'active' | 'compacting' | 'deleted'
  createdAt: Timestamp
}

/**
 * Edge index manifest for tracking Parquet files
 */
export interface EdgeIndexManifest {
  version: number
  indexType: 'forward' | 'reverse' | 'type'
  namespace: Namespace
  files: ManifestFileEntry[]
  lastCompaction: Timestamp
  totalRowCount: number
  totalSizeBytes: number
}

// ============================================================================
// Query Builder
// ============================================================================

/**
 * Predicate for Parquet pushdown
 */
export interface Predicate {
  column: string
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'contains'
  value: unknown
}

/**
 * Scan options for Parquet reads
 */
export interface ScanOptions {
  predicates: Predicate[]
  projection: string[]
  limit?: number
  offset?: number
}

/**
 * Build predicates from filter object
 */
export function buildPredicates<T>(filter: QueryFilter<T>): Predicate[] {
  const predicates: Predicate[] = []

  for (const [column, operator] of Object.entries(filter)) {
    if (operator && typeof operator === 'object') {
      const op = operator as Record<string, unknown>
      if ('eq' in op) predicates.push({ column, op: 'eq', value: op.eq })
      if ('neq' in op) predicates.push({ column, op: 'neq', value: op.neq })
      if ('gt' in op) predicates.push({ column, op: 'gt', value: op.gt })
      if ('gte' in op) predicates.push({ column, op: 'gte', value: op.gte })
      if ('lt' in op) predicates.push({ column, op: 'lt', value: op.lt })
      if ('lte' in op) predicates.push({ column, op: 'lte', value: op.lte })
      if ('in' in op) predicates.push({ column, op: 'in', value: op.in })
      if ('contains' in op) predicates.push({ column, op: 'contains', value: op.contains })
    }
  }

  return predicates
}

// ============================================================================
// Zone Map Filtering
// ============================================================================

/**
 * Check if a value falls within a zone map range
 */
function inRange(value: string, min: string, max: string): boolean {
  return value >= min && value <= max
}

/**
 * Prune files using zone map statistics
 */
export function pruneFilesByZoneMap(
  files: ManifestFileEntry[],
  filter: { primaryId?: string; relType?: string }
): ManifestFileEntry[] {
  return files.filter(file => {
    if (filter.primaryId) {
      if (!inRange(filter.primaryId, file.minKey.primaryId, file.maxKey.primaryId)) {
        return false
      }
    }
    if (filter.relType) {
      if (!inRange(filter.relType, file.minKey.relType, file.maxKey.relType)) {
        return false
      }
    }
    return true
  })
}

// ============================================================================
// Bloom Filter Operations
// ============================================================================

/**
 * Hash function for Bloom filter (xxHash64-style)
 */
export function hashEdgeKey(fromId: string, relType: string, toId: string): bigint {
  // Simplified hash - in production use xxHash64
  const str = `${fromId}|${relType}|${toId}`
  let hash = 0n
  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31n + BigInt(str.charCodeAt(i))) & 0xFFFFFFFFFFFFFFFFn
  }
  return hash
}

/**
 * Check if Bloom filter might contain key
 */
export function bloomMightContain(
  filter: Uint8Array,
  key: bigint,
  numHashes: number = 7
): boolean {
  const numBits = BigInt(filter.length * 8)

  for (let i = 0; i < numHashes; i++) {
    // Double hashing technique
    const hash = (key + BigInt(i) * 0x517cc1b727220a95n) % numBits
    const byteIndex = Number(hash / 8n)
    const bitIndex = Number(hash % 8n)

    if ((filter[byteIndex] & (1 << bitIndex)) === 0) {
      return false
    }
  }

  return true
}

/**
 * Add key to Bloom filter
 */
export function bloomAdd(
  filter: Uint8Array,
  key: bigint,
  numHashes: number = 7
): void {
  const numBits = BigInt(filter.length * 8)

  for (let i = 0; i < numHashes; i++) {
    const hash = (key + BigInt(i) * 0x517cc1b727220a95n) % numBits
    const byteIndex = Number(hash / 8n)
    const bitIndex = Number(hash % 8n)

    filter[byteIndex] |= (1 << bitIndex)
  }
}

/**
 * Create optimal Bloom filter for given capacity and FPR
 */
export function createBloomFilter(
  expectedItems: number,
  falsePositiveRate: number = 0.01
): { filter: Uint8Array; numHashes: number } {
  // Optimal number of bits: -n * ln(p) / (ln(2)^2)
  const numBits = Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.LN2 * Math.LN2))

  // Optimal number of hashes: (m/n) * ln(2)
  const numHashes = Math.round((numBits / expectedItems) * Math.LN2)

  // Round up to full bytes
  const numBytes = Math.ceil(numBits / 8)

  return {
    filter: new Uint8Array(numBytes),
    numHashes
  }
}

// ============================================================================
// Graph Traversal Core
// ============================================================================

/**
 * Forward edge lookup result
 */
export interface ForwardLookupResult {
  edges: Edge[]
  fromCache: boolean
  filesScanned: number
}

/**
 * Traversal direction
 */
export type TraversalDirection = 'forward' | 'reverse' | 'both'

/**
 * BFS state for graph traversal
 */
export interface BFSState {
  visited: Map<EntityId, { depth: number; path: EntityId[]; via?: Edge }>
  frontier: Set<EntityId>
  depth: number
  edgesTraversed: Edge[]
}

/**
 * Initialize BFS state
 */
export function initBFSState(startId: EntityId): BFSState {
  const visited = new Map<EntityId, { depth: number; path: EntityId[] }>()
  visited.set(startId, { depth: 0, path: [startId] })

  return {
    visited,
    frontier: new Set([startId]),
    depth: 0,
    edgesTraversed: []
  }
}

/**
 * Expand BFS frontier by one hop
 */
export function expandBFSFrontier(
  state: BFSState,
  edges: Map<EntityId, Edge[]>,
  direction: TraversalDirection
): Set<EntityId> {
  const nextFrontier = new Set<EntityId>()
  state.depth++

  for (const nodeId of state.frontier) {
    const nodeEdges = edges.get(nodeId) ?? []

    for (const edge of nodeEdges) {
      const targetId = direction === 'reverse' ? edge.from_id : edge.to_id

      if (!state.visited.has(targetId)) {
        const parentInfo = state.visited.get(nodeId)!
        state.visited.set(targetId, {
          depth: state.depth,
          path: [...parentInfo.path, targetId],
          via: edge
        })
        state.edgesTraversed.push(edge)
        nextFrontier.add(targetId)
      }
    }
  }

  state.frontier = nextFrontier
  return nextFrontier
}

// ============================================================================
// Path Finding Utilities
// ============================================================================

/**
 * Min-heap for Dijkstra's algorithm
 */
export class MinHeap<T> {
  private heap: T[] = []

  constructor(private compare: (a: T, b: T) => number) {}

  push(item: T): void {
    this.heap.push(item)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): T | undefined {
    if (this.heap.length === 0) return undefined
    if (this.heap.length === 1) return this.heap.pop()

    const result = this.heap[0]
    this.heap[0] = this.heap.pop()!
    this.bubbleDown(0)
    return result
  }

  peek(): T | undefined {
    return this.heap[0]
  }

  isEmpty(): boolean {
    return this.heap.length === 0
  }

  get size(): number {
    return this.heap.length
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.compare(this.heap[index], this.heap[parentIndex]) >= 0) break
      ;[this.heap[index], this.heap[parentIndex]] = [this.heap[parentIndex], this.heap[index]]
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    while (true) {
      const leftChild = 2 * index + 1
      const rightChild = 2 * index + 2
      let smallest = index

      if (leftChild < this.heap.length &&
          this.compare(this.heap[leftChild], this.heap[smallest]) < 0) {
        smallest = leftChild
      }

      if (rightChild < this.heap.length &&
          this.compare(this.heap[rightChild], this.heap[smallest]) < 0) {
        smallest = rightChild
      }

      if (smallest === index) break

      ;[this.heap[index], this.heap[smallest]] = [this.heap[smallest], this.heap[index]]
      index = smallest
    }
  }
}

/**
 * Dijkstra state entry
 */
export interface DijkstraEntry {
  cost: number
  nodeId: EntityId
  path: EntityId[]
  edges: Edge[]
}

/**
 * Create Dijkstra priority queue
 */
export function createDijkstraQueue(): MinHeap<DijkstraEntry> {
  return new MinHeap<DijkstraEntry>((a, b) => a.cost - b.cost)
}

// ============================================================================
// Variant Type Utilities
// ============================================================================

/**
 * Convert Variant to JSON
 */
export function variantToJson(variant: Variant): unknown {
  switch (variant.type) {
    case 'null':
      return null
    case 'boolean':
      return variant.value
    case 'int64':
      return Number(variant.value)  // Lossy for large values
    case 'float64':
      return variant.value
    case 'string':
      return variant.value
    case 'binary':
      return Array.from(variant.value)  // Convert to number array
    case 'date':
      return new Date(variant.value * 86400000).toISOString().split('T')[0]
    case 'timestamp':
      return new Date(Number(variant.value / 1000n)).toISOString()
    case 'array':
      return variant.elements.map(variantToJson)
    case 'object':
      const obj: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(variant.fields)) {
        obj[key] = variantToJson(value)
      }
      return obj
  }
}

/**
 * Convert JSON to Variant
 */
export function jsonToVariant(value: unknown): Variant {
  if (value === null || value === undefined) {
    return { type: 'null' }
  }

  if (typeof value === 'boolean') {
    return { type: 'boolean', value }
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { type: 'int64', value: BigInt(value) }
    }
    return { type: 'float64', value }
  }

  if (typeof value === 'string') {
    // Check if ISO date
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
      const days = Math.floor(new Date(value).getTime() / 86400000)
      return { type: 'date', value: days }
    }
    // Check if ISO timestamp
    if (/^\d{4}-\d{2}-\d{2}T/.test(value)) {
      const ts = BigInt(new Date(value).getTime()) * 1000n
      return { type: 'timestamp', value: ts }
    }
    return { type: 'string', value }
  }

  if (typeof value === 'bigint') {
    return { type: 'int64', value }
  }

  if (value instanceof Uint8Array) {
    return { type: 'binary', value }
  }

  if (Array.isArray(value)) {
    return { type: 'array', elements: value.map(jsonToVariant) }
  }

  if (typeof value === 'object') {
    const fields: Record<string, Variant> = {}
    for (const [key, val] of Object.entries(value)) {
      fields[key] = jsonToVariant(val)
    }
    return { type: 'object', fields }
  }

  throw new Error(`Unsupported value type: ${typeof value}`)
}

/**
 * Extract field from Variant object
 */
export function variantGet(variant: Variant, path: string): Variant | undefined {
  const parts = path.split('.')
  let current: Variant = variant

  for (const part of parts) {
    if (current.type === 'object') {
      const field = current.fields[part]
      if (!field) return undefined
      current = field
    } else if (current.type === 'array') {
      const index = parseInt(part, 10)
      if (isNaN(index) || index < 0 || index >= current.elements.length) {
        return undefined
      }
      current = current.elements[index]
    } else {
      return undefined
    }
  }

  return current
}

// ============================================================================
// CDC Event Utilities
// ============================================================================

/**
 * Generate ULID for event ID
 */
export function generateULID(): string {
  const timestamp = Date.now()
  const timestampPart = timestamp.toString(36).padStart(10, '0')

  // Random part (80 bits = ~13.3 base36 chars)
  const randomPart = Array.from(
    { length: 16 },
    () => Math.floor(Math.random() * 36).toString(36)
  ).join('')

  return (timestampPart + randomPart).toUpperCase()
}

/**
 * Extract timestamp from ULID
 */
export function ulidTimestamp(ulid: string): number {
  const timestampPart = ulid.slice(0, 10).toLowerCase()
  return parseInt(timestampPart, 36)
}

/**
 * Create edge entity ID for CDC events
 */
export function edgeEntityId(edge: Edge): string {
  return `${edge.from_id}|${edge.rel_type}|${edge.to_id}`
}

/**
 * Parse edge entity ID
 */
export function parseEdgeEntityId(entityId: string): {
  fromId: string
  relType: string
  toId: string
} {
  const parts = entityId.split('|')
  if (parts.length !== 3) {
    throw new Error(`Invalid edge entity ID: ${entityId}`)
  }
  return {
    fromId: parts[0],
    relType: parts[1],
    toId: parts[2]
  }
}

// ============================================================================
// Edge Validation
// ============================================================================

/**
 * Validate edge structure
 */
export function validateEdge(edge: Edge): void {
  if (!edge.ns) {
    throw new Error('Edge missing namespace')
  }
  if (!edge.from_id) {
    throw new Error('Edge missing from_id')
  }
  if (!edge.to_id) {
    throw new Error('Edge missing to_id')
  }
  if (!edge.rel_type) {
    throw new Error('Edge missing rel_type')
  }
  if (!edge.ts) {
    throw new Error('Edge missing timestamp')
  }

  // Validate operator
  const validOperators: RelationshipOperator[] = ['->', '~>', '<-', '<~']
  if (!validOperators.includes(edge.operator)) {
    throw new Error(`Invalid operator: ${edge.operator}`)
  }

  // Validate confidence for fuzzy operators
  if (edge.operator === '~>' || edge.operator === '<~') {
    if (edge.confidence === null || edge.confidence === undefined) {
      throw new Error('Fuzzy operator requires confidence score')
    }
    if (edge.confidence < 0 || edge.confidence > 1) {
      throw new Error('Confidence must be between 0 and 1')
    }
  }
}

// ============================================================================
// Batch Utilities
// ============================================================================

/**
 * Chunk array into batches
 */
export function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size))
  }
  return chunks
}

/**
 * Group edges by partition key
 */
export function groupEdgesByPartition(
  edges: Edge[],
  partitionKey: (edge: Edge) => string
): Map<string, Edge[]> {
  const groups = new Map<string, Edge[]>()

  for (const edge of edges) {
    const key = partitionKey(edge)
    const group = groups.get(key) ?? []
    group.push(edge)
    groups.set(key, group)
  }

  return groups
}

/**
 * Sort edges by index order
 */
export function sortEdges(
  edges: Edge[],
  index: 'forward' | 'reverse' | 'type'
): Edge[] {
  return [...edges].sort((a, b) => {
    // Namespace first
    const nsCompare = a.ns.localeCompare(b.ns)
    if (nsCompare !== 0) return nsCompare

    switch (index) {
      case 'forward':
        // (ns, from_id, rel_type, to_id, ts DESC)
        return a.from_id.localeCompare(b.from_id) ||
               a.rel_type.localeCompare(b.rel_type) ||
               a.to_id.localeCompare(b.to_id) ||
               Number(b.ts - a.ts)

      case 'reverse':
        // (ns, to_id, rel_type, from_id, ts DESC)
        return a.to_id.localeCompare(b.to_id) ||
               a.rel_type.localeCompare(b.rel_type) ||
               a.from_id.localeCompare(b.from_id) ||
               Number(b.ts - a.ts)

      case 'type':
        // (ns, rel_type, ts DESC, from_id, to_id)
        return a.rel_type.localeCompare(b.rel_type) ||
               Number(b.ts - a.ts) ||
               a.from_id.localeCompare(b.from_id) ||
               a.to_id.localeCompare(b.to_id)
    }
  })
}

/**
 * Deduplicate edges by key, keeping latest version
 */
export function deduplicateEdges(edges: Edge[]): Edge[] {
  const byKey = new Map<string, Edge>()

  for (const edge of edges) {
    const key = edgeEntityId(edge)
    const existing = byKey.get(key)

    if (!existing || edge.ts > existing.ts) {
      byKey.set(key, edge)
    }
  }

  return Array.from(byKey.values())
}

// ============================================================================
// Time-Travel Utilities
// ============================================================================

/**
 * Checkpoint metadata
 */
export interface Checkpoint {
  path: string
  timestamp: Timestamp
  nodeCount: number
  edgeCount: number
  eventIdUpTo: string
}

/**
 * Find nearest checkpoint before target time
 */
export function findNearestCheckpoint(
  checkpoints: Checkpoint[],
  targetTime: Timestamp
): Checkpoint | null {
  const before = checkpoints
    .filter(cp => cp.timestamp <= targetTime)
    .sort((a, b) => Number(b.timestamp - a.timestamp))

  return before[0] ?? null
}

/**
 * Calculate event replay range
 */
export function getEventReplayRange(
  checkpoint: Checkpoint | null,
  targetTime: Timestamp
): { startTime: Timestamp; endTime: Timestamp } {
  return {
    startTime: checkpoint?.timestamp ?? 0n as Timestamp,
    endTime: targetTime
  }
}

// ============================================================================
// graphdl Integration
// ============================================================================

/**
 * Parse graphdl reference syntax
 */
export function parseGraphDLReference(ref: string): {
  isArray: boolean
  operator: RelationshipOperator
  targetType: string
  backref?: string
} {
  const isArray = ref.startsWith('[') && ref.endsWith(']')
  const inner = isArray ? ref.slice(1, -1) : ref

  // Match: operator + TargetEntity[.backref]
  const match = inner.match(/^(->|~>|<-|<~)(\w+)(?:\.(\w+))?$/)

  if (!match) {
    throw new Error(`Invalid graphdl reference: ${ref}`)
  }

  return {
    isArray,
    operator: match[1] as RelationshipOperator,
    targetType: match[2],
    backref: match[3]
  }
}

/**
 * Determine index for operator
 */
export function indexForOperator(operator: RelationshipOperator): 'forward' | 'reverse' {
  switch (operator) {
    case '->':
    case '~>':
      return 'forward'
    case '<-':
    case '<~':
      return 'reverse'
  }
}

/**
 * Determine if operator is fuzzy
 */
export function isFuzzyOperator(operator: RelationshipOperator): boolean {
  return operator === '~>' || operator === '<~'
}

// ============================================================================
// Performance Monitoring
// ============================================================================

/**
 * Query execution stats
 */
export interface QueryStats {
  filesScanned: number
  rowGroupsScanned: number
  rowsScanned: number
  rowsReturned: number
  bloomFilterChecks: number
  bloomFilterHits: number
  cacheHits: number
  cacheMisses: number
  durationMs: number
}

/**
 * Create empty query stats
 */
export function createQueryStats(): QueryStats {
  return {
    filesScanned: 0,
    rowGroupsScanned: 0,
    rowsScanned: 0,
    rowsReturned: 0,
    bloomFilterChecks: 0,
    bloomFilterHits: 0,
    cacheHits: 0,
    cacheMisses: 0,
    durationMs: 0
  }
}

/**
 * Merge query stats
 */
export function mergeQueryStats(a: QueryStats, b: QueryStats): QueryStats {
  return {
    filesScanned: a.filesScanned + b.filesScanned,
    rowGroupsScanned: a.rowGroupsScanned + b.rowGroupsScanned,
    rowsScanned: a.rowsScanned + b.rowsScanned,
    rowsReturned: a.rowsReturned + b.rowsReturned,
    bloomFilterChecks: a.bloomFilterChecks + b.bloomFilterChecks,
    bloomFilterHits: a.bloomFilterHits + b.bloomFilterHits,
    cacheHits: a.cacheHits + b.cacheHits,
    cacheMisses: a.cacheMisses + b.cacheMisses,
    durationMs: a.durationMs + b.durationMs
  }
}
