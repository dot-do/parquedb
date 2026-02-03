/**
 * HNSW Serialization
 *
 * Serialize and deserialize HNSW index data to/from binary format.
 *
 * Format versions:
 * - v1: Original format
 * - v2: Added incremental update metadata
 * - v3: Added configurable precision support
 */

import type { HNSWNode, RowGroupMetadata, VectorPrecision } from './hnsw-types'
import { VectorIndexConfigError } from './hnsw-types'
import type { VectorMetric } from '../types'

// File format magic and version
export const HNSW_MAGIC = new Uint8Array([0x50, 0x51, 0x56, 0x49]) // "PQVI"
export const HNSW_VERSION = 3 // Bumped for configurable precision support

/**
 * Context required for serialization
 */
export interface SerializationContext {
  /** Vector dimensions */
  dimensions: number
  /** HNSW M parameter */
  m: number
  /** Distance metric */
  metric: VectorMetric
  /** Vector precision */
  precision: VectorPrecision
  /** Entry point node ID */
  entryPoint: number | null
  /** Maximum layer in graph */
  maxLayerInGraph: number
  /** Index version */
  indexVersion: number
  /** Last updated timestamp */
  lastUpdatedAt: number
  /** Row group metadata */
  rowGroupMetadata: Map<number, RowGroupMetadata>
  /** Node ID to row group mapping */
  nodeIdToRowGroup: Map<number, number>
  /** Iterator over nodes to serialize */
  iterateNodes: () => IterableIterator<HNSWNode>
  /** Number of nodes in cache */
  nodeCacheSize: number
}

/**
 * Result of deserialization
 */
export interface DeserializationResult {
  /** Entry point node ID */
  entryPoint: number | null
  /** Maximum layer in graph */
  maxLayerInGraph: number
  /** Index version */
  indexVersion: number
  /** Last updated timestamp */
  lastUpdatedAt: number
  /** Row group metadata */
  rowGroupMetadata: Map<number, RowGroupMetadata>
  /** Node ID to row group mapping */
  nodeIdToRowGroup: Map<number, number>
  /** Deserialized nodes */
  nodes: HNSWNode[]
  /** Next node ID to use */
  nextNodeId: number
}

/**
 * Context required for deserialization validation
 */
export interface DeserializationConfig {
  /** Expected dimensions */
  dimensions: number
  /** Expected metric */
  metric: VectorMetric
  /** Expected precision */
  precision: VectorPrecision
}

/**
 * Serialize the HNSW index to binary format
 *
 * @param ctx - Serialization context
 * @returns Serialized binary data
 */
export function serialize(ctx: SerializationContext): Uint8Array {
  const encoder = new TextEncoder()
  const bytesPerDimension = ctx.precision === 'float64' ? 8 : 4

  // Calculate size
  // Header: magic(4) + version(1) + dimensions(4) + m(4) + nodeCount(4) + entryPoint(4) + maxLayer(1) + metric(1)
  // V2 additions: indexVersion(4) + lastUpdatedAt(8) + rowGroupMetadataCount(4) + nodeIdToRowGroupCount(4)
  // V3 additions: precision(1)
  let totalSize = 4 + 1 + 4 + 4 + 4 + 4 + 1 + 1 + 4 + 8 + 4 + 4 + 1

  // Row group metadata
  for (const metadata of ctx.rowGroupMetadata.values()) {
    // rowGroup(4) + vectorCount(4) + minRowOffset(4) + maxRowOffset(4) + indexedAt(8) + checksumLen(4) + checksum
    totalSize += 4 + 4 + 4 + 4 + 8 + 4
    if (metadata.checksum) {
      totalSize += encoder.encode(metadata.checksum).length
    }
  }

  // Node to row group mappings
  totalSize += ctx.nodeIdToRowGroup.size * 8 // nodeId(4) + rowGroup(4)

  // Nodes
  for (const node of ctx.iterateNodes()) {
    totalSize += 4 // node ID
    totalSize += 4 + node.docId.length // docId length + docId
    totalSize += node.vector.length * bytesPerDimension // vector (precision-dependent)
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
  bytes.set(HNSW_MAGIC, offset)
  offset += 4

  view.setUint8(offset, HNSW_VERSION)
  offset += 1

  view.setUint32(offset, ctx.dimensions, false)
  offset += 4

  view.setUint32(offset, ctx.m, false)
  offset += 4

  view.setUint32(offset, ctx.nodeCacheSize, false)
  offset += 4

  view.setInt32(offset, ctx.entryPoint ?? -1, false)
  offset += 4

  view.setInt8(offset, ctx.maxLayerInGraph)
  offset += 1

  // Metric (0 = cosine, 1 = euclidean, 2 = dot)
  const metricCode = ctx.metric === 'euclidean' ? 1 : ctx.metric === 'dot' ? 2 : 0
  view.setUint8(offset, metricCode)
  offset += 1

  // V2: Incremental update metadata
  view.setUint32(offset, ctx.indexVersion, false)
  offset += 4

  // Store lastUpdatedAt as BigInt64
  view.setBigInt64(offset, BigInt(ctx.lastUpdatedAt), false)
  offset += 8

  // Row group metadata
  view.setUint32(offset, ctx.rowGroupMetadata.size, false)
  offset += 4

  for (const metadata of ctx.rowGroupMetadata.values()) {
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
  view.setUint32(offset, ctx.nodeIdToRowGroup.size, false)
  offset += 4

  for (const [nodeId, rowGroup] of ctx.nodeIdToRowGroup) {
    view.setUint32(offset, nodeId, false)
    offset += 4
    view.setUint32(offset, rowGroup, false)
    offset += 4
  }

  // V3: Precision (0 = float32, 1 = float64)
  const precisionCode = ctx.precision === 'float64' ? 1 : 0
  view.setUint8(offset, precisionCode)
  offset += 1

  // Nodes
  for (const node of ctx.iterateNodes()) {
    view.setUint32(offset, node.id, false)
    offset += 4

    const docIdBytes = encoder.encode(node.docId)
    view.setUint32(offset, docIdBytes.length, false)
    offset += 4
    bytes.set(docIdBytes, offset)
    offset += docIdBytes.length

    if (ctx.precision === 'float64') {
      for (let i = 0; i < node.vector.length; i++) {
        view.setFloat64(offset, node.vector[i]!, false)
        offset += 8
      }
    } else {
      for (let i = 0; i < node.vector.length; i++) {
        view.setFloat32(offset, node.vector[i]!, false)
        offset += 4
      }
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
 * Deserialize the HNSW index from binary format.
 * Supports v1 (no incremental metadata), v2 (with incremental metadata), and v3 (with precision).
 *
 * @param data - Binary data to deserialize
 * @param config - Configuration for validation
 * @returns Deserialization result
 */
export function deserialize(
  data: Uint8Array,
  config: DeserializationConfig
): DeserializationResult {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const decoder = new TextDecoder()
  let offset = 0

  // Verify magic
  for (let i = 0; i < 4; i++) {
    if (data[offset + i] !== HNSW_MAGIC[i]) {
      throw new Error('Invalid vector index: bad magic')
    }
  }
  offset += 4

  const version = view.getUint8(offset)
  offset += 1

  // Support v1, v2, and v3
  if (version !== 1 && version !== 2 && version !== HNSW_VERSION) {
    throw new Error(`Unsupported vector index version: ${version}`)
  }

  const dimensions = view.getUint32(offset, false)
  offset += 4
  if (dimensions !== config.dimensions) {
    throw new VectorIndexConfigError(
      `Dimension mismatch: index has ${dimensions}, expected ${config.dimensions}`
    )
  }

  const __m = view.getUint32(offset, false)
  offset += 4
  void __m // Reserved field for future use

  const nodeCount = view.getUint32(offset, false)
  offset += 4

  const entryPointValue = view.getInt32(offset, false)
  offset += 4
  const entryPoint = entryPointValue === -1 ? null : entryPointValue

  const maxLayerInGraph = view.getInt8(offset)
  offset += 1

  const serializedMetricCode = view.getUint8(offset)
  offset += 1
  // Validate metric code matches (0 = cosine, 1 = euclidean, 2 = dot)
  const expectedMetricCode = config.metric === 'euclidean' ? 1 : config.metric === 'dot' ? 2 : 0
  if (serializedMetricCode !== expectedMetricCode) {
    const metricNames = ['cosine', 'euclidean', 'dot'] as const
    const serializedMetric = metricNames[serializedMetricCode] ?? `unknown(${serializedMetricCode})`
    throw new VectorIndexConfigError(
      `Metric mismatch: index was serialized with '${serializedMetric}' but loaded with '${config.metric}'`
    )
  }

  // Initialize result
  const result: DeserializationResult = {
    entryPoint,
    maxLayerInGraph,
    indexVersion: 0,
    lastUpdatedAt: 0,
    rowGroupMetadata: new Map(),
    nodeIdToRowGroup: new Map(),
    nodes: [],
    nextNodeId: 0,
  }

  // V2: Read incremental update metadata
  if (version >= 2) {
    result.indexVersion = view.getUint32(offset, false)
    offset += 4

    result.lastUpdatedAt = Number(view.getBigInt64(offset, false))
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

      result.rowGroupMetadata.set(rowGroup, {
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
      result.nodeIdToRowGroup.set(nodeId, rowGroup)
    }
  }

  // V3: Read precision
  let serializedPrecision: VectorPrecision
  if (version >= 3) {
    const serializedPrecisionCode = view.getUint8(offset)
    offset += 1
    serializedPrecision = serializedPrecisionCode === 1 ? 'float64' : 'float32'
    if (serializedPrecision !== config.precision) {
      throw new VectorIndexConfigError(
        `Precision mismatch: index was serialized with '${serializedPrecision}' but loaded with '${config.precision}'`
      )
    }
  } else {
    // v1 and v2 always used float64
    serializedPrecision = 'float64'
  }
  const serializedBpd = serializedPrecision === 'float64' ? 8 : 4

  // Read nodes
  for (let i = 0; i < nodeCount; i++) {
    const nodeId = view.getUint32(offset, false)
    offset += 4

    const docIdLen = view.getUint32(offset, false)
    offset += 4
    const docId = decoder.decode(data.slice(offset, offset + docIdLen))
    offset += docIdLen

    const vector: number[] = []
    if (serializedBpd === 8) {
      for (let j = 0; j < dimensions; j++) {
        vector.push(view.getFloat64(offset, false))
        offset += 8
      }
    } else {
      for (let j = 0; j < dimensions; j++) {
        vector.push(view.getFloat32(offset, false))
        offset += 4
      }
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

    result.nodes.push(node)

    // Track for v1 compatibility
    if (version === 1) {
      result.nodeIdToRowGroup.set(nodeId, rowGroup)
    }

    if (nodeId >= result.nextNodeId) {
      result.nextNodeId = nodeId + 1
    }
  }

  return result
}
