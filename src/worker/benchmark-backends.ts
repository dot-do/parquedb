/**
 * Backend Benchmark - R2 Performance for Iceberg, Delta Lake, and Native Parquet
 *
 * Compares the performance of different table formats on Cloudflare R2:
 * - Native Parquet (direct hyparquet read/write)
 * - Apache Iceberg (with metadata, manifests, snapshots)
 * - Delta Lake (with transaction log)
 *
 * Endpoints:
 * - GET /benchmark/backends - Run full backend comparison
 * - GET /benchmark/backends?backend=iceberg - Run specific backend
 * - GET /benchmark/backends?operations=write,read - Run specific operations
 *
 * Query params:
 * - backend: iceberg|delta|native|all (default: all)
 * - operations: write,read,query,time-travel (default: all)
 * - size: number of entities (default: 100)
 * - iterations: number of iterations (default: 3)
 */

import { parquetQuery } from 'hyparquet'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { compressors } from '../parquet/compressors'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

type R2Bucket = {
  get(key: string, options?: { range?: { offset: number; length: number } | undefined }): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<unknown>
  delete(key: string | string[]): Promise<void>
  head(key: string): Promise<{ size: number } | null>
  list(options?: { prefix?: string | undefined; limit?: number | undefined; cursor?: string | undefined }): Promise<{ objects: { key: string; size: number }[]; truncated: boolean; cursor?: string | undefined }>
}

export interface BackendBenchmarkConfig {
  backends: ('native' | 'iceberg' | 'delta')[]
  operations: ('write' | 'read' | 'query' | 'time-travel')[]
  size: number
  iterations: number
  cleanup: boolean
}

export interface LatencyStats {
  p50: number
  p95: number
  p99: number
  mean: number
  min: number
  max: number
  samples: number
}

export interface OperationResult {
  operation: string
  latency: LatencyStats
  bytesWritten?: number | undefined
  bytesRead?: number | undefined
  rowsAffected?: number | undefined
  metadata?: Record<string, unknown> | undefined
}

export interface BackendResult {
  backend: string
  operations: OperationResult[]
  totalTimeMs: number
  storageBytes: number
}

export interface BackendBenchmarkResult {
  config: BackendBenchmarkConfig
  results: BackendResult[]
  comparison: {
    fastestWrite: { backend: string; p50Ms: number }
    fastestRead: { backend: string; p50Ms: number }
    fastestQuery: { backend: string; p50Ms: number }
    smallestStorage: { backend: string; bytes: number }
    recommendation: string
  }
  metadata: {
    timestamp: string
    colo?: string | undefined
    durationMs: number
  }
}

// =============================================================================
// Constants
// =============================================================================

// Use parquedb-benchmarks prefix so data is accessible via cdn.workers.do/parquedb-benchmarks/
const BENCHMARK_PREFIX = 'parquedb-benchmarks'
const CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'toys', 'food', 'auto']
const STATUSES = ['active', 'inactive', 'pending', 'archived']

// =============================================================================
// Utility Functions
// =============================================================================

function calculateStats(samples: number[]): LatencyStats {
  if (samples.length === 0) {
    return { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0, samples: 0 }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = sorted.reduce((a, b) => a + b, 0)

  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))] ?? 0
  }

  return {
    p50: Math.round(percentile(50) * 100) / 100,
    p95: Math.round(percentile(95) * 100) / 100,
    p99: Math.round(percentile(99) * 100) / 100,
    mean: Math.round((sum / n) * 100) / 100,
    min: Math.round((sorted[0] ?? 0) * 100) / 100,
    max: Math.round((sorted[n - 1] ?? 0) * 100) / 100,
    samples: n,
  }
}

function generateProducts(count: number): Record<string, unknown>[] {
  const products: Record<string, unknown>[] = []
  const payload = 'x'.repeat(100) // Realistic payload

  for (let i = 0; i < count; i++) {
    products.push({
      $id: `products/${String(i).padStart(8, '0')}`,
      $type: 'Product',
      name: `Product ${i}`,
      category: CATEGORIES[i % CATEGORIES.length],
      status: STATUSES[i % STATUSES.length],
      price: 10 + (i % 990),
      rating: 1 + (i % 50) / 10,
      createdYear: 2015 + (i % 10),
      description: `Product ${i} description. ${payload}`,
      createdAt: new Date().toISOString(),
      version: 1,
    })
  }

  return products
}

function createTrackedR2File(bucket: R2Bucket, key: string, size: number) {
  const file = {
    byteLength: size,
    bytesRead: 0,
    slice: async (start: number, end: number): Promise<ArrayBuffer> => {
      const obj = await bucket.get(key, { range: { offset: start, length: end - start } })
      if (!obj) throw new Error(`Object not found: ${key}`)
      file.bytesRead += (end - start)
      return obj.arrayBuffer()
    },
  }
  return file
}

async function cleanupPrefix(bucket: R2Bucket, prefix: string): Promise<void> {
  let cursor: string | undefined
  do {
    const result = await bucket.list({ prefix, limit: 1000, cursor })
    if (result.objects.length > 0) {
      const keys = result.objects.map(o => o.key)
      await bucket.delete(keys)
    }
    cursor = result.truncated ? result.cursor : undefined
  } while (cursor)
}

async function getStorageSize(bucket: R2Bucket, prefix: string): Promise<number> {
  let totalSize = 0
  let cursor: string | undefined
  do {
    const result = await bucket.list({ prefix, limit: 1000, cursor })
    for (const obj of result.objects) {
      totalSize += obj.size
    }
    cursor = result.truncated ? result.cursor : undefined
  } while (cursor)
  return totalSize
}

// =============================================================================
// Native Parquet Benchmark
// =============================================================================

async function benchmarkNativeParquet(
  bucket: R2Bucket,
  config: BackendBenchmarkConfig
): Promise<BackendResult> {
  const startTime = performance.now()
  const operations: OperationResult[] = []
  const prefix = `${BENCHMARK_PREFIX}/native`
  const dataPath = `${prefix}/data.parquet`

  try {
    // Clean up any existing data
    await cleanupPrefix(bucket, prefix)

    const products = generateProducts(config.size)

    // Write benchmark
    if (config.operations.includes('write')) {
      const writeLatencies: number[] = []
      let totalBytesWritten = 0

      for (let i = 0; i < config.iterations; i++) {
        const writeStart = performance.now()

        const buffer = parquetWriteBuffer({
          columnData: [
            { name: '$id', type: 'STRING', data: products.map(p => p.$id as string) },
            { name: '$type', type: 'STRING', data: products.map(p => p.$type as string) },
            { name: 'name', type: 'STRING', data: products.map(p => p.name as string) },
            { name: 'category', type: 'STRING', data: products.map(p => p.category as string) },
            { name: 'status', type: 'STRING', data: products.map(p => p.status as string) },
            { name: 'price', type: 'DOUBLE', data: products.map(p => p.price as number) },
            { name: 'rating', type: 'DOUBLE', data: products.map(p => p.rating as number) },
            { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
          ],
          rowGroupSize: Math.min(1000, config.size),
        })

        await bucket.put(dataPath, buffer)
        writeLatencies.push(performance.now() - writeStart)
        totalBytesWritten = buffer.byteLength
      }

      operations.push({
        operation: 'write',
        latency: calculateStats(writeLatencies),
        bytesWritten: totalBytesWritten,
        rowsAffected: config.size,
      })
    }

    // Read benchmark
    if (config.operations.includes('read')) {
      const readLatencies: number[] = []
      let totalBytesRead = 0

      const head = await bucket.head(dataPath)
      if (!head) throw new Error('Data file not found')

      for (let i = 0; i < config.iterations; i++) {
        const file = createTrackedR2File(bucket, dataPath, head.size)
        const readStart = performance.now()

        await parquetQuery({
          file,
          columns: ['$id', 'name', 'category'],
          compressors,
        })

        readLatencies.push(performance.now() - readStart)
        totalBytesRead = file.bytesRead
      }

      operations.push({
        operation: 'read',
        latency: calculateStats(readLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: config.size,
      })
    }

    // Query benchmark
    if (config.operations.includes('query')) {
      const queryLatencies: number[] = []
      let totalBytesRead = 0
      let rowsReturned = 0

      const head = await bucket.head(dataPath)
      if (!head) throw new Error('Data file not found')

      for (let i = 0; i < config.iterations; i++) {
        const file = createTrackedR2File(bucket, dataPath, head.size)
        const queryStart = performance.now()

        const rows = await parquetQuery({
          file,
          columns: ['$id', 'category', 'price'],
          filter: { category: 'electronics' },
          compressors,
        })

        queryLatencies.push(performance.now() - queryStart)
        totalBytesRead = file.bytesRead
        rowsReturned = rows.length
      }

      operations.push({
        operation: 'query',
        latency: calculateStats(queryLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: rowsReturned,
        metadata: { filter: "category = 'electronics'" },
      })
    }

    const storageBytes = await getStorageSize(bucket, prefix)

    return {
      backend: 'native',
      operations,
      totalTimeMs: Math.round(performance.now() - startTime),
      storageBytes,
    }
  } finally {
    if (config.cleanup) {
      await cleanupPrefix(bucket, prefix)
    }
  }
}

// =============================================================================
// Iceberg Benchmark
// =============================================================================

async function benchmarkIceberg(
  bucket: R2Bucket,
  config: BackendBenchmarkConfig
): Promise<BackendResult> {
  const startTime = performance.now()
  const operations: OperationResult[] = []
  const prefix = `${BENCHMARK_PREFIX}/iceberg`

  try {
    await cleanupPrefix(bucket, prefix)

    const products = generateProducts(config.size)

    // Iceberg table structure:
    // - metadata/v{version}.metadata.json
    // - data/part-{uuid}.parquet
    // - manifest/manifest-{uuid}.avro (or .json)

    // Write benchmark - simulates Iceberg write with metadata
    if (config.operations.includes('write')) {
      const writeLatencies: number[] = []
      let totalBytesWritten = 0

      for (let i = 0; i < config.iterations; i++) {
        const writeStart = performance.now()
        const snapshotId = Date.now()

        // 1. Write data file
        const dataBuffer = parquetWriteBuffer({
          columnData: [
            { name: '$id', type: 'STRING', data: products.map(p => p.$id as string) },
            { name: '$type', type: 'STRING', data: products.map(p => p.$type as string) },
            { name: 'name', type: 'STRING', data: products.map(p => p.name as string) },
            { name: 'category', type: 'STRING', data: products.map(p => p.category as string) },
            { name: 'status', type: 'STRING', data: products.map(p => p.status as string) },
            { name: 'price', type: 'DOUBLE', data: products.map(p => p.price as number) },
            { name: 'rating', type: 'DOUBLE', data: products.map(p => p.rating as number) },
            { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
          ],
          rowGroupSize: Math.min(1000, config.size),
        })

        const dataPath = `${prefix}/data/part-${snapshotId}.parquet`
        await bucket.put(dataPath, dataBuffer)

        // 2. Write manifest list (simplified JSON for benchmark)
        const manifestList = {
          manifests: [{
            path: `${prefix}/manifest/manifest-${snapshotId}.json`,
            length: 0,
            partitionSpecId: 0,
            addedDataFilesCount: 1,
            addedRowsCount: config.size,
          }],
        }
        await bucket.put(`${prefix}/manifest-list-${snapshotId}.json`, new TextEncoder().encode(JSON.stringify(manifestList)))

        // 3. Write manifest (simplified JSON)
        const manifest = {
          entries: [{
            status: 1, // ADDED
            dataFile: {
              filePath: dataPath,
              fileFormat: 'PARQUET',
              recordCount: config.size,
              fileSizeInBytes: dataBuffer.byteLength,
            },
          }],
        }
        await bucket.put(`${prefix}/manifest/manifest-${snapshotId}.json`, new TextEncoder().encode(JSON.stringify(manifest)))

        // 4. Write table metadata
        const metadata = {
          formatVersion: 2,
          tableUuid: `benchmark-${snapshotId}`,
          location: prefix,
          lastSequenceNumber: i + 1,
          lastUpdatedMs: Date.now(),
          currentSnapshotId: snapshotId,
          snapshots: [{
            snapshotId,
            timestampMs: Date.now(),
            manifestList: `${prefix}/manifest-list-${snapshotId}.json`,
          }],
          schemas: [{ schemaId: 0, fields: [] }],
          currentSchemaId: 0,
          partitionSpecs: [{ specId: 0, fields: [] }],
          defaultSpecId: 0,
          sortOrders: [{ orderId: 0, fields: [] }],
          defaultSortOrderId: 0,
        }
        await bucket.put(`${prefix}/metadata/v${i + 1}.metadata.json`, new TextEncoder().encode(JSON.stringify(metadata)))

        writeLatencies.push(performance.now() - writeStart)
        totalBytesWritten = dataBuffer.byteLength
      }

      operations.push({
        operation: 'write',
        latency: calculateStats(writeLatencies),
        bytesWritten: totalBytesWritten,
        rowsAffected: config.size,
        metadata: { format: 'iceberg-v2', manifestFormat: 'json' },
      })
    }

    // Read benchmark - simulates Iceberg read path
    if (config.operations.includes('read')) {
      const readLatencies: number[] = []
      let totalBytesRead = 0

      // Find latest metadata
      const metadataList = await bucket.list({ prefix: `${prefix}/metadata/`, limit: 100 })
      if (metadataList.objects.length === 0) throw new Error('No metadata found')

      const latestMetadata = metadataList.objects.sort((a, b) => b.key.localeCompare(a.key))[0]!

      for (let i = 0; i < config.iterations; i++) {
        const readStart = performance.now()
        let bytesRead = 0

        // 1. Read table metadata
        const metaObj = await bucket.get(latestMetadata.key)
        if (!metaObj) throw new Error('Metadata not found')
        const metaBuffer = await metaObj.arrayBuffer()
        bytesRead += metaBuffer.byteLength
        const metadata = JSON.parse(new TextDecoder().decode(metaBuffer))

        // 2. Read manifest list
        const manifestListObj = await bucket.get(metadata.snapshots[0].manifestList)
        if (!manifestListObj) throw new Error('Manifest list not found')
        const manifestListBuffer = await manifestListObj.arrayBuffer()
        bytesRead += manifestListBuffer.byteLength
        const manifestList = JSON.parse(new TextDecoder().decode(manifestListBuffer))

        // 3. Read manifest
        const manifestObj = await bucket.get(manifestList.manifests[0].path)
        if (!manifestObj) throw new Error('Manifest not found')
        const manifestBuffer = await manifestObj.arrayBuffer()
        bytesRead += manifestBuffer.byteLength
        const manifest = JSON.parse(new TextDecoder().decode(manifestBuffer))

        // 4. Read data file
        const dataPath = manifest.entries[0].dataFile.filePath
        const dataHead = await bucket.head(dataPath)
        if (!dataHead) throw new Error('Data file not found')

        const file = createTrackedR2File(bucket, dataPath, dataHead.size)
        await parquetQuery({
          file,
          columns: ['$id', 'name', 'category'],
          compressors,
        })
        bytesRead += file.bytesRead

        readLatencies.push(performance.now() - readStart)
        totalBytesRead = bytesRead
      }

      operations.push({
        operation: 'read',
        latency: calculateStats(readLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: config.size,
        metadata: { metadataReads: 3, dataReads: 1 },
      })
    }

    // Query benchmark
    if (config.operations.includes('query')) {
      const queryLatencies: number[] = []
      let totalBytesRead = 0
      let rowsReturned = 0

      // Find data file (same as read path but with filtering)
      const metadataList = await bucket.list({ prefix: `${prefix}/metadata/`, limit: 100 })
      const latestMetadata = metadataList.objects.sort((a, b) => b.key.localeCompare(a.key))[0]

      if (latestMetadata) {
        for (let i = 0; i < config.iterations; i++) {
          const queryStart = performance.now()
          let bytesRead = 0

          // Follow the same metadata path
          const metaObj = await bucket.get(latestMetadata.key)
          if (!metaObj) throw new Error('Metadata not found')
          const metaBuffer = await metaObj.arrayBuffer()
          bytesRead += metaBuffer.byteLength
          const metadata = JSON.parse(new TextDecoder().decode(metaBuffer))

          const manifestListObj = await bucket.get(metadata.snapshots[0].manifestList)
          if (!manifestListObj) throw new Error('Manifest list not found')
          const manifestListBuffer = await manifestListObj.arrayBuffer()
          bytesRead += manifestListBuffer.byteLength
          const manifestList = JSON.parse(new TextDecoder().decode(manifestListBuffer))

          const manifestObj = await bucket.get(manifestList.manifests[0].path)
          if (!manifestObj) throw new Error('Manifest not found')
          const manifestBuffer = await manifestObj.arrayBuffer()
          bytesRead += manifestBuffer.byteLength
          const manifest = JSON.parse(new TextDecoder().decode(manifestBuffer))

          const dataPath = manifest.entries[0].dataFile.filePath
          const dataHead = await bucket.head(dataPath)
          if (!dataHead) throw new Error('Data file not found')

          const file = createTrackedR2File(bucket, dataPath, dataHead.size)
          const rows = await parquetQuery({
            file,
            columns: ['$id', 'category', 'price'],
            filter: { category: 'electronics' },
            compressors,
          })
          bytesRead += file.bytesRead

          queryLatencies.push(performance.now() - queryStart)
          totalBytesRead = bytesRead
          rowsReturned = rows.length
        }
      }

      operations.push({
        operation: 'query',
        latency: calculateStats(queryLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: rowsReturned,
        metadata: { filter: "category = 'electronics'", metadataOverhead: 3 },
      })
    }

    // Time travel benchmark
    if (config.operations.includes('time-travel')) {
      const metadataList = await bucket.list({ prefix: `${prefix}/metadata/`, limit: 100 })
      const snapshotCount = metadataList.objects.length

      operations.push({
        operation: 'time-travel',
        latency: { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0, samples: 0 },
        metadata: { snapshotsAvailable: snapshotCount, supported: true },
      })
    }

    const storageBytes = await getStorageSize(bucket, prefix)

    return {
      backend: 'iceberg',
      operations,
      totalTimeMs: Math.round(performance.now() - startTime),
      storageBytes,
    }
  } finally {
    if (config.cleanup) {
      await cleanupPrefix(bucket, prefix)
    }
  }
}

// =============================================================================
// Delta Lake Benchmark
// =============================================================================

async function benchmarkDelta(
  bucket: R2Bucket,
  config: BackendBenchmarkConfig
): Promise<BackendResult> {
  const startTime = performance.now()
  const operations: OperationResult[] = []
  const prefix = `${BENCHMARK_PREFIX}/delta`

  try {
    await cleanupPrefix(bucket, prefix)

    const products = generateProducts(config.size)

    // Delta table structure:
    // - _delta_log/00000000000000000000.json (commit files)
    // - _delta_log/_last_checkpoint
    // - part-{uuid}.parquet (data files)

    // Write benchmark
    if (config.operations.includes('write')) {
      const writeLatencies: number[] = []
      let totalBytesWritten = 0

      for (let i = 0; i < config.iterations; i++) {
        const writeStart = performance.now()
        const version = i

        // 1. Write data file
        const dataBuffer = parquetWriteBuffer({
          columnData: [
            { name: '$id', type: 'STRING', data: products.map(p => p.$id as string) },
            { name: '$type', type: 'STRING', data: products.map(p => p.$type as string) },
            { name: 'name', type: 'STRING', data: products.map(p => p.name as string) },
            { name: 'category', type: 'STRING', data: products.map(p => p.category as string) },
            { name: 'status', type: 'STRING', data: products.map(p => p.status as string) },
            { name: 'price', type: 'DOUBLE', data: products.map(p => p.price as number) },
            { name: 'rating', type: 'DOUBLE', data: products.map(p => p.rating as number) },
            { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
          ],
          rowGroupSize: Math.min(1000, config.size),
        })

        const dataPath = `${prefix}/part-${Date.now()}.parquet`
        await bucket.put(dataPath, dataBuffer)

        // 2. Write commit log entry
        const commitActions = [
          // Protocol
          { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
          // Metadata (first commit only)
          ...(version === 0 ? [{
            metaData: {
              id: `benchmark-${Date.now()}`,
              schemaString: JSON.stringify({ fields: [] }),
              partitionColumns: [],
              createdTime: Date.now(),
            },
          }] : []),
          // Add action
          {
            add: {
              path: dataPath.replace(`${prefix}/`, ''),
              size: dataBuffer.byteLength,
              modificationTime: Date.now(),
              dataChange: true,
              stats: JSON.stringify({
                numRecords: config.size,
              }),
            },
          },
          // Commit info
          {
            commitInfo: {
              timestamp: Date.now(),
              operation: version === 0 ? 'CREATE TABLE' : 'WRITE',
              operationParameters: {},
            },
          },
        ]

        const commitPath = `${prefix}/_delta_log/${String(version).padStart(20, '0')}.json`
        await bucket.put(commitPath, new TextEncoder().encode(commitActions.map(a => JSON.stringify(a)).join('\n')))

        writeLatencies.push(performance.now() - writeStart)
        totalBytesWritten = dataBuffer.byteLength
      }

      operations.push({
        operation: 'write',
        latency: calculateStats(writeLatencies),
        bytesWritten: totalBytesWritten,
        rowsAffected: config.size,
        metadata: { format: 'delta-lake', commitLogFormat: 'json' },
      })
    }

    // Read benchmark
    if (config.operations.includes('read')) {
      const readLatencies: number[] = []
      let totalBytesRead = 0

      // Find latest commit
      const commitList = await bucket.list({ prefix: `${prefix}/_delta_log/`, limit: 100 })
      const commits = commitList.objects.filter(o => o.key.endsWith('.json') && !o.key.includes('checkpoint'))
        .sort((a, b) => b.key.localeCompare(a.key))

      if (commits.length === 0) throw new Error('No commits found')

      for (let i = 0; i < config.iterations; i++) {
        const readStart = performance.now()
        let bytesRead = 0

        // 1. Read commit log to find data files
        const dataFiles: string[] = []
        for (const commit of commits) {
          const commitObj = await bucket.get(commit.key)
          if (!commitObj) continue
          const commitBuffer = await commitObj.arrayBuffer()
          bytesRead += commitBuffer.byteLength

          const lines = new TextDecoder().decode(commitBuffer).split('\n')
          for (const line of lines) {
            if (!line.trim()) continue
            const action = JSON.parse(line)
            if (action.add) {
              dataFiles.push(`${prefix}/${action.add.path}`)
            }
          }
        }

        // 2. Read latest data file
        const latestFile = dataFiles[dataFiles.length - 1]
        if (!latestFile) throw new Error('No data files found')

        const dataHead = await bucket.head(latestFile)
        if (!dataHead) throw new Error('Data file not found')

        const file = createTrackedR2File(bucket, latestFile, dataHead.size)
        await parquetQuery({
          file,
          columns: ['$id', 'name', 'category'],
          compressors,
        })
        bytesRead += file.bytesRead

        readLatencies.push(performance.now() - readStart)
        totalBytesRead = bytesRead
      }

      operations.push({
        operation: 'read',
        latency: calculateStats(readLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: config.size,
        metadata: { commitReads: commits.length, dataReads: 1 },
      })
    }

    // Query benchmark
    if (config.operations.includes('query')) {
      const queryLatencies: number[] = []
      let totalBytesRead = 0
      let rowsReturned = 0

      const commitList = await bucket.list({ prefix: `${prefix}/_delta_log/`, limit: 100 })
      const commits = commitList.objects.filter(o => o.key.endsWith('.json') && !o.key.includes('checkpoint'))
        .sort((a, b) => b.key.localeCompare(a.key))

      if (commits.length > 0) {
        for (let i = 0; i < config.iterations; i++) {
          const queryStart = performance.now()
          let bytesRead = 0

          // Read commits
          const dataFiles: string[] = []
          for (const commit of commits) {
            const commitObj = await bucket.get(commit.key)
            if (!commitObj) continue
            const commitBuffer = await commitObj.arrayBuffer()
            bytesRead += commitBuffer.byteLength

            const lines = new TextDecoder().decode(commitBuffer).split('\n')
            for (const line of lines) {
              if (!line.trim()) continue
              const action = JSON.parse(line)
              if (action.add) {
                dataFiles.push(`${prefix}/${action.add.path}`)
              }
            }
          }

          const latestFile = dataFiles[dataFiles.length - 1]
          if (!latestFile) throw new Error('No data files found')

          const dataHead = await bucket.head(latestFile)
          if (!dataHead) throw new Error('Data file not found')

          const file = createTrackedR2File(bucket, latestFile, dataHead.size)
          const rows = await parquetQuery({
            file,
            columns: ['$id', 'category', 'price'],
            filter: { category: 'electronics' },
            compressors,
          })
          bytesRead += file.bytesRead

          queryLatencies.push(performance.now() - queryStart)
          totalBytesRead = bytesRead
          rowsReturned = rows.length
        }
      }

      operations.push({
        operation: 'query',
        latency: calculateStats(queryLatencies),
        bytesRead: totalBytesRead,
        rowsAffected: rowsReturned,
        metadata: { filter: "category = 'electronics'" },
      })
    }

    // Time travel
    if (config.operations.includes('time-travel')) {
      const commitList = await bucket.list({ prefix: `${prefix}/_delta_log/`, limit: 100 })
      const commits = commitList.objects.filter(o => o.key.endsWith('.json') && !o.key.includes('checkpoint'))

      operations.push({
        operation: 'time-travel',
        latency: { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0, samples: 0 },
        metadata: { versionsAvailable: commits.length, supported: true },
      })
    }

    const storageBytes = await getStorageSize(bucket, prefix)

    return {
      backend: 'delta',
      operations,
      totalTimeMs: Math.round(performance.now() - startTime),
      storageBytes,
    }
  } finally {
    if (config.cleanup) {
      await cleanupPrefix(bucket, prefix)
    }
  }
}

// =============================================================================
// Main Handler
// =============================================================================

export async function handleBackendsBenchmarkRequest(
  request: Request,
  bucket: R2Bucket
): Promise<Response> {
  const url = new URL(request.url)
  const startTime = performance.now()

  // Parse config from query params
  const backendsParam = url.searchParams.get('backend') ?? 'all'
  const backends = backendsParam === 'all'
    ? ['native', 'iceberg', 'delta'] as const
    : backendsParam.split(',').filter(b => ['native', 'iceberg', 'delta'].includes(b)) as ('native' | 'iceberg' | 'delta')[]

  const opsParam = url.searchParams.get('operations') ?? 'write,read,query'
  const operations = opsParam.split(',').filter(o => ['write', 'read', 'query', 'time-travel'].includes(o)) as ('write' | 'read' | 'query' | 'time-travel')[]

  const size = parseInt(url.searchParams.get('size') ?? '100')
  const iterations = parseInt(url.searchParams.get('iterations') ?? '3')
  const cleanup = url.searchParams.get('cleanup') !== 'false'

  const config: BackendBenchmarkConfig = {
    backends,
    operations,
    size,
    iterations,
    cleanup,
  }

  try {
    logger.info(`Running backend benchmark: ${backends.join(', ')}`)

    const results: BackendResult[] = []

    for (const backend of backends) {
      logger.info(`Benchmarking ${backend}...`)
      switch (backend) {
        case 'native':
          results.push(await benchmarkNativeParquet(bucket, config))
          break
        case 'iceberg':
          results.push(await benchmarkIceberg(bucket, config))
          break
        case 'delta':
          results.push(await benchmarkDelta(bucket, config))
          break
      }
    }

    // Calculate comparison
    const getP50 = (result: BackendResult, op: string) =>
      result.operations.find(o => o.operation === op)?.latency.p50 ?? Infinity

    const writeResults = results.map(r => ({ backend: r.backend, p50: getP50(r, 'write') }))
    const readResults = results.map(r => ({ backend: r.backend, p50: getP50(r, 'read') }))
    const queryResults = results.map(r => ({ backend: r.backend, p50: getP50(r, 'query') }))
    const storageResults = results.map(r => ({ backend: r.backend, bytes: r.storageBytes }))

    const fastestWrite = writeResults.reduce((a, b) => a.p50 < b.p50 ? a : b)
    const fastestRead = readResults.reduce((a, b) => a.p50 < b.p50 ? a : b)
    const fastestQuery = queryResults.reduce((a, b) => a.p50 < b.p50 ? a : b)
    const smallestStorage = storageResults.reduce((a, b) => a.bytes < b.bytes ? a : b)

    // Generate recommendation
    let recommendation = 'native'
    if (operations.includes('time-travel')) {
      recommendation = fastestQuery.backend === 'iceberg' ? 'iceberg' : 'delta'
    } else if (fastestQuery.backend === fastestWrite.backend) {
      recommendation = fastestQuery.backend
    }

    const response: BackendBenchmarkResult = {
      config,
      results,
      comparison: {
        fastestWrite: { backend: fastestWrite.backend, p50Ms: fastestWrite.p50 },
        fastestRead: { backend: fastestRead.backend, p50Ms: fastestRead.p50 },
        fastestQuery: { backend: fastestQuery.backend, p50Ms: fastestQuery.p50 },
        smallestStorage: { backend: smallestStorage.backend, bytes: smallestStorage.bytes },
        recommendation: `Use '${recommendation}' for your workload`,
      },
      metadata: {
        timestamp: new Date().toISOString(),
        colo: request.cf?.colo as string | undefined,
        durationMs: Math.round(performance.now() - startTime),
      },
    }

    return Response.json(response, {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Server-Timing': `total;dur=${response.metadata.durationMs}`,
      },
    })
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    const stack = error instanceof Error ? error.stack : undefined
    logger.error(`Backend benchmark failed: ${message}`)
    return Response.json({
      error: true,
      message,
      stack,
    }, { status: 500 })
  }
}
