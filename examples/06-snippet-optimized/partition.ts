/**
 * Partition data into snippet-optimized Parquet files
 *
 * Creates <25MB files with 10K row groups, sorted by key
 */

import { parquetWriteBuffer } from 'hyparquet-writer'
import { writeCompressors } from '../../src/parquet/compression'
import type { Entity } from '../../src/types/entity'

/** Partition configuration */
export interface PartitionConfig {
  /** Maximum file size in bytes (default: 24MB to stay under 25MB limit) */
  maxFileSize?: number
  /** Rows per row group (default: 10000 for ~1ms reads) */
  rowGroupSize?: number
  /** Compression codec */
  codec?: 'SNAPPY' | 'UNCOMPRESSED' | 'GZIP' | 'ZSTD'
}

const DEFAULT_CONFIG: Required<PartitionConfig> = {
  maxFileSize: 24 * 1024 * 1024, // 24MB
  rowGroupSize: 10_000,
  codec: 'SNAPPY',
}

/** Result of partitioning */
export interface PartitionResult {
  files: Array<{
    name: string
    buffer: ArrayBuffer
    rowCount: number
    minKey: string
    maxKey: string
  }>
  manifest: PartitionManifest
}

/** Manifest for routing queries to correct partition */
export interface PartitionManifest {
  version: 1
  sortKey: string
  partitions: Array<{
    file: string
    minKey: string
    maxKey: string
    rowCount: number
    sizeBytes: number
  }>
}

/**
 * Partition entities by ID for point lookups
 *
 * @example
 * ```typescript
 * const entities = await loadEntities()
 * const result = partitionById(entities)
 *
 * for (const file of result.files) {
 *   await storage.write(`data/by-id/${file.name}`, file.buffer)
 * }
 * await storage.write('data/by-id/manifest.json', JSON.stringify(result.manifest))
 * ```
 */
export function partitionById(
  entities: Entity[],
  config: PartitionConfig = {}
): PartitionResult {
  const cfg = { ...DEFAULT_CONFIG, ...config }

  // Sort by $id
  const sorted = [...entities].sort((a, b) =>
    String(a.$id).localeCompare(String(b.$id))
  )

  return partitionSorted(sorted, '$id', cfg)
}

/**
 * Partition entities by type for type-based queries
 */
export function partitionByType(
  entities: Entity[],
  config: PartitionConfig = {}
): PartitionResult {
  const cfg = { ...DEFAULT_CONFIG, ...config }

  // Sort by $type, then $id
  const sorted = [...entities].sort((a, b) => {
    const typeCompare = String(a.$type).localeCompare(String(b.$type))
    if (typeCompare !== 0) return typeCompare
    return String(a.$id).localeCompare(String(b.$id))
  })

  return partitionSorted(sorted, '$type', cfg)
}

/**
 * Partition entities by date for time-range queries
 */
export function partitionByDate(
  entities: Entity[],
  dateField: string = 'createdAt',
  config: PartitionConfig = {}
): PartitionResult {
  const cfg = { ...DEFAULT_CONFIG, ...config }

  // Sort by date field, then $id
  const sorted = [...entities].sort((a, b) => {
    const aDate = a[dateField] as Date | string | number
    const bDate = b[dateField] as Date | string | number
    const dateCompare = new Date(aDate).getTime() - new Date(bDate).getTime()
    if (dateCompare !== 0) return dateCompare
    return String(a.$id).localeCompare(String(b.$id))
  })

  return partitionSorted(sorted, dateField, cfg)
}

/**
 * Internal: partition pre-sorted data
 */
function partitionSorted(
  sorted: Entity[],
  sortKey: string,
  config: Required<PartitionConfig>
): PartitionResult {
  const files: PartitionResult['files'] = []
  const partitions: PartitionManifest['partitions'] = []

  // Estimate rows per file based on avg entity size
  // Start with a reasonable estimate, adjust if files are too large
  const sampleSize = Math.min(100, sorted.length)
  const sampleBuffer = writeEntities(sorted.slice(0, sampleSize), config)
  const avgBytesPerRow = sampleBuffer.byteLength / sampleSize
  const estimatedRowsPerFile = Math.floor(config.maxFileSize / avgBytesPerRow)

  let fileIndex = 0
  let offset = 0

  while (offset < sorted.length) {
    // Take a chunk
    let chunkSize = Math.min(estimatedRowsPerFile, sorted.length - offset)
    let chunk = sorted.slice(offset, offset + chunkSize)

    // Write and check size
    let buffer = writeEntities(chunk, config)

    // If too large, reduce chunk size
    while (buffer.byteLength > config.maxFileSize && chunkSize > config.rowGroupSize) {
      chunkSize = Math.floor(chunkSize * 0.8)
      chunk = sorted.slice(offset, offset + chunkSize)
      buffer = writeEntities(chunk, config)
    }

    const minKey = String(chunk[0][sortKey as keyof Entity])
    const maxKey = String(chunk[chunk.length - 1][sortKey as keyof Entity])

    const fileName = `${String(fileIndex).padStart(4, '0')}.parquet`

    files.push({
      name: fileName,
      buffer,
      rowCount: chunk.length,
      minKey,
      maxKey,
    })

    partitions.push({
      file: fileName,
      minKey,
      maxKey,
      rowCount: chunk.length,
      sizeBytes: buffer.byteLength,
    })

    offset += chunkSize
    fileIndex++
  }

  return {
    files,
    manifest: {
      version: 1,
      sortKey,
      partitions,
    },
  }
}

/**
 * Write entities to Parquet buffer
 */
function writeEntities(
  entities: Entity[],
  config: Required<PartitionConfig>
): ArrayBuffer {
  if (entities.length === 0) {
    throw new Error('Cannot write empty entity list')
  }

  // Extract all unique keys from entities
  const allKeys = new Set<string>()
  for (const entity of entities) {
    for (const key of Object.keys(entity)) {
      allKeys.add(key)
    }
  }

  // Build column data
  const columnData = Array.from(allKeys).map(key => ({
    name: key,
    data: entities.map(e => {
      const value = e[key as keyof Entity]
      // Convert dates to ISO strings for Parquet
      if (value instanceof Date) {
        return value.toISOString()
      }
      // Convert objects to JSON strings
      if (typeof value === 'object' && value !== null) {
        return JSON.stringify(value)
      }
      return value
    }),
  }))

  return parquetWriteBuffer({
    columnData,
    statistics: true,
    rowGroupSize: config.rowGroupSize,
    codec: config.codec,
    compressors: writeCompressors,
  })
}

/**
 * Estimate partition count for a dataset
 */
export function estimatePartitionCount(
  entityCount: number,
  avgEntitySizeBytes: number = 500,
  config: PartitionConfig = {}
): number {
  const maxFileSize = config.maxFileSize ?? DEFAULT_CONFIG.maxFileSize
  const rowsPerFile = Math.floor(maxFileSize / avgEntitySizeBytes)
  return Math.ceil(entityCount / rowsPerFile)
}
