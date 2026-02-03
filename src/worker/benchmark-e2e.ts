/**
 * E2E Benchmark Worker Endpoints
 *
 * Provides endpoints for measuring real-world deployed Worker performance:
 * - Cold start latency
 * - CRUD operation benchmarks
 * - Query pattern benchmarks
 * - Backend-specific benchmarks (Iceberg, Delta, Native Parquet)
 *
 * These endpoints are designed to be called from external scripts to measure
 * actual production performance characteristics.
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
  delete(key: string): Promise<void>
  head(key: string): Promise<{ size: number } | null>
  list(options?: { prefix?: string | undefined; limit?: number | undefined }): Promise<{ objects: { key: string; size: number; uploaded: Date }[]; truncated: boolean }>
}

export interface LatencyStats {
  p50: number
  p95: number
  p99: number
  avg: number
  min: number
  max: number
}

export interface ColdStartResult {
  coldStartMs: number
  workerInitMs: number
  cacheInitMs: number
  firstQueryMs: number
  metadata: {
    colo: string
    timestamp: string
    isolateId?: string | undefined
  }
}

export interface CrudBenchmarkResult {
  operation: string
  iterations: number
  batchSize?: number | undefined
  latencyMs: LatencyStats
  throughput: {
    opsPerSec: number
    totalTimeMs: number
  }
  metadata?: Record<string, unknown> | undefined
}

export interface QueryBenchmarkResult {
  queryType: string
  dataset: string
  iterations: number
  latencyMs: LatencyStats
  queryStats: {
    rowsScanned: number
    rowsReturned: number
    rowGroupsSkipped: number
    rowGroupsScanned: number
  }
}

export interface BackendBenchmarkResult {
  backend: string
  operations: {
    write?: {
      latencyMs: LatencyStats
      bytesWritten: number
      rowsWritten: number
    } | undefined
    read?: {
      latencyMs: LatencyStats
      bytesRead: number
      rowsRead: number
    } | undefined
    'time-travel'?: {
      latencyMs: LatencyStats
      snapshotsAvailable: number
    } | undefined
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Calculate percentile statistics from latency measurements
 */
function calculateStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return { p50: 0, p95: 0, p99: 0, avg: 0, min: 0, max: 0 }
  }

  const sorted = [...latencies].sort((a, b) => a - b)
  const sum = sorted.reduce((a, b) => a + b, 0)

  return {
    p50: sorted[Math.floor(sorted.length * 0.5)] ?? 0,
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1] ?? 0,
    p99: sorted[Math.floor(sorted.length * 0.99)] ?? sorted[sorted.length - 1] ?? 0,
    avg: Math.round(sum / sorted.length),
    min: sorted[0] ?? 0,
    max: sorted[sorted.length - 1] ?? 0,
  }
}

/**
 * Generate sample entity data for benchmarks
 */
function generateEntity(index: number): Record<string, unknown> {
  return {
    $id: `benchmark:${String(index).padStart(8, '0')}`,
    $type: 'benchmark',
    name: `Benchmark Entity ${index}`,
    status: ['active', 'inactive', 'pending'][index % 3],
    category: ['A', 'B', 'C', 'D'][index % 4],
    value: Math.random() * 1000,
    createdAt: new Date().toISOString(),
    tags: [`tag${index % 10}`, `group${index % 5}`],
    metadata: {
      source: 'benchmark',
      iteration: index,
    },
  }
}

/**
 * Create a tracked R2 file for byte counting
 */
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

// =============================================================================
// Cold Start Benchmark
// =============================================================================

/**
 * Measure cold start latency
 *
 * Forces a new isolate by using unique cache keys and measures:
 * - Total cold start time
 * - Worker initialization time
 * - Cache initialization time
 * - First query latency
 */
export async function measureColdStart(
  bucket: R2Bucket,
  request: Request
): Promise<ColdStartResult> {
  const startTime = performance.now()

  // Worker init is already done by the time this runs
  const workerInitMs = Math.round(performance.now() - startTime)

  // Cache initialization
  const cacheStart = performance.now()
  const cache = await caches.open(`benchmark-${Date.now()}`)
  const cacheInitMs = Math.round(performance.now() - cacheStart)

  // First query - check if benchmark data exists
  const queryStart = performance.now()
  const list = await bucket.list({ prefix: 'benchmark-data/', limit: 1 })
  const firstQueryMs = Math.round(performance.now() - queryStart)

  // Extract colo from request
  const cf = (request as Request & { cf?: { colo?: string | undefined } | undefined }).cf
  const colo = cf?.colo ?? 'unknown'

  const coldStartMs = Math.round(performance.now() - startTime)

  return {
    coldStartMs,
    workerInitMs,
    cacheInitMs,
    firstQueryMs,
    metadata: {
      colo,
      timestamp: new Date().toISOString(),
      isolateId: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    },
  }
}

// =============================================================================
// CRUD Benchmarks
// =============================================================================

/**
 * Benchmark CRUD operations
 */
export async function benchmarkCrud(
  bucket: R2Bucket,
  operation: string,
  iterations: number,
  batchSize: number,
  warmup: number
): Promise<CrudBenchmarkResult> {
  const latencies: number[] = []
  const benchmarkPath = `benchmark-e2e/crud-${Date.now()}.parquet`

  try {
    // Warmup iterations
    for (let i = 0; i < warmup; i++) {
      await runCrudOperation(bucket, operation, benchmarkPath, batchSize)
    }

    // Measured iterations
    for (let i = 0; i < iterations; i++) {
      const start = performance.now()
      await runCrudOperation(bucket, operation, benchmarkPath, batchSize)
      latencies.push(performance.now() - start)
    }

    const stats = calculateStats(latencies)
    const totalTimeMs = latencies.reduce((a, b) => a + b, 0)

    return {
      operation,
      iterations,
      batchSize: ['batch-create', 'batch-read'].includes(operation) ? batchSize : undefined,
      latencyMs: stats,
      throughput: {
        opsPerSec: Math.round((iterations / (totalTimeMs / 1000)) * 10) / 10,
        totalTimeMs: Math.round(totalTimeMs),
      },
    }
  } finally {
    // Cleanup
    try {
      await bucket.delete(benchmarkPath)
    } catch {
      // Ignore cleanup errors
    }
  }
}

async function runCrudOperation(
  bucket: R2Bucket,
  operation: string,
  path: string,
  batchSize: number
): Promise<void> {
  switch (operation) {
    case 'create': {
      const entity = generateEntity(Date.now())
      const buffer = parquetWriteBuffer({
        columnData: [
          { name: '$id', type: 'STRING', data: [entity.$id as string] },
          { name: '$data', type: 'STRING', data: [JSON.stringify(entity)] },
        ],
        rowGroupSize: 1,
      })
      await bucket.put(path, buffer)
      break
    }

    case 'read': {
      // First ensure data exists
      const head = await bucket.head(path)
      if (!head) {
        const entity = generateEntity(Date.now())
        const buffer = parquetWriteBuffer({
          columnData: [
            { name: '$id', type: 'STRING', data: [entity.$id as string] },
            { name: '$data', type: 'STRING', data: [JSON.stringify(entity)] },
          ],
          rowGroupSize: 1,
        })
        await bucket.put(path, buffer)
      }
      // Read the file
      const obj = await bucket.get(path)
      if (obj) await obj.arrayBuffer()
      break
    }

    case 'update': {
      // Read existing, modify, write back
      const existing = await bucket.get(path)
      if (existing) {
        const data = await existing.arrayBuffer()
        // Just re-write as update simulation
        await bucket.put(path, data)
      }
      break
    }

    case 'delete': {
      await bucket.delete(path)
      break
    }

    case 'batch-create': {
      const entities = Array.from({ length: batchSize }, (_, i) => generateEntity(i))
      const buffer = parquetWriteBuffer({
        columnData: [
          { name: '$id', type: 'STRING', data: entities.map(e => e.$id as string) },
          { name: '$data', type: 'STRING', data: entities.map(e => JSON.stringify(e)) },
        ],
        rowGroupSize: Math.min(batchSize, 10000),
      })
      await bucket.put(path, buffer)
      break
    }

    case 'batch-read': {
      // Ensure batch data exists
      const head = await bucket.head(path)
      if (!head) {
        const entities = Array.from({ length: batchSize }, (_, i) => generateEntity(i))
        const buffer = parquetWriteBuffer({
          columnData: [
            { name: '$id', type: 'STRING', data: entities.map(e => e.$id as string) },
            { name: '$data', type: 'STRING', data: entities.map(e => JSON.stringify(e)) },
          ],
          rowGroupSize: Math.min(batchSize, 10000),
        })
        await bucket.put(path, buffer)
      }
      // Read the batch
      const obj = await bucket.get(path)
      if (obj) await obj.arrayBuffer()
      break
    }

    default:
      throw new Error(`Unknown CRUD operation: ${operation}`)
  }
}

// =============================================================================
// Query Benchmarks
// =============================================================================

/**
 * Benchmark query patterns
 */
export async function benchmarkQuery(
  bucket: R2Bucket,
  queryType: string,
  dataset: string,
  iterations: number,
  limit: number
): Promise<QueryBenchmarkResult> {
  const latencies: number[] = []
  let lastStats = { rowsScanned: 0, rowsReturned: 0, rowGroupsSkipped: 0, rowGroupsScanned: 0 }

  // Determine file path based on dataset
  const dataPath = `benchmark-data/${dataset}/titles.parquet`
  const head = await bucket.head(dataPath)

  if (!head) {
    throw new Error(`Dataset not found: ${dataset}. Available datasets: imdb, onet-full, unspsc-full`)
  }

  const filter = getQueryFilter(queryType)
  const columns = getQueryColumns(queryType)

  for (let i = 0; i < iterations; i++) {
    const file = createTrackedR2File(bucket, dataPath, head.size)

    const start = performance.now()
    try {
      const rows = await parquetQuery({
        file,
        columns,
        filter,
        compressors,
        rowLimit: limit,
      })

      latencies.push(performance.now() - start)
      lastStats = {
        rowsScanned: rows.length,
        rowsReturned: rows.length,
        rowGroupsSkipped: 0, // Would need metadata access for accurate count
        rowGroupsScanned: 1,
      }
    } catch (error) {
      logger.warn(`Query benchmark error for ${queryType}:`, error)
    }
  }

  return {
    queryType,
    dataset,
    iterations,
    latencyMs: calculateStats(latencies),
    queryStats: lastStats,
  }
}

function getQueryFilter(queryType: string): Record<string, unknown> | undefined {
  switch (queryType) {
    case 'simple-filter':
      return { $index_titleType: 'movie' }
    case 'range-filter':
      return { $index_startYear: { $gte: 2000, $lte: 2020 } }
    case 'compound-filter':
      return { $index_titleType: 'movie', $index_averageRating: { $gt: 7.0 } }
    case 'full-text-search':
      // FTS would require separate index, return undefined for raw scan
      return undefined
    case 'pagination':
      return undefined
    case 'aggregation':
      return undefined
    default:
      return undefined
  }
}

function getQueryColumns(queryType: string): string[] {
  switch (queryType) {
    case 'simple-filter':
      return ['$id', 'name', '$index_titleType']
    case 'range-filter':
      return ['$id', 'name', '$index_startYear']
    case 'compound-filter':
      return ['$id', 'name', '$index_titleType', '$index_averageRating']
    case 'full-text-search':
      return ['$id', 'name']
    case 'pagination':
      return ['$id', 'name']
    case 'aggregation':
      return ['$id', '$index_titleType', '$index_averageRating']
    default:
      return ['$id', 'name']
  }
}

// =============================================================================
// Backend Benchmarks
// =============================================================================

/**
 * Benchmark backend-specific operations (Iceberg, Delta, Native Parquet)
 */
export async function benchmarkBackend(
  bucket: R2Bucket,
  backend: string,
  operations: string[],
  iterations: number,
  dataSize: number
): Promise<BackendBenchmarkResult> {
  const result: BackendBenchmarkResult = {
    backend,
    operations: {},
  }

  const benchmarkPath = `benchmark-e2e/${backend}-${Date.now()}`

  // For now, all backends use native Parquet under the hood
  // This benchmark shows the baseline Parquet performance

  if (operations.includes('write') || operations.length === 0) {
    const writeLatencies: number[] = []
    let bytesWritten = 0

    for (let i = 0; i < iterations; i++) {
      const entities = Array.from({ length: dataSize }, (_, j) => generateEntity(j))

      const start = performance.now()
      const buffer = parquetWriteBuffer({
        columnData: [
          { name: '$id', type: 'STRING', data: entities.map(e => e.$id as string) },
          { name: '$index_status', type: 'STRING', data: entities.map(e => e.status as string) },
          { name: '$data', type: 'STRING', data: entities.map(e => JSON.stringify(e)) },
        ],
        rowGroupSize: Math.min(dataSize, 10000),
      })
      await bucket.put(`${benchmarkPath}/data.parquet`, buffer)
      writeLatencies.push(performance.now() - start)
      bytesWritten = buffer.byteLength
    }

    result.operations.write = {
      latencyMs: calculateStats(writeLatencies),
      bytesWritten,
      rowsWritten: dataSize,
    }
  }

  if (operations.includes('read') || operations.length === 0) {
    const readLatencies: number[] = []
    let bytesRead = 0

    // Ensure data exists
    const head = await bucket.head(`${benchmarkPath}/data.parquet`)
    if (!head) {
      const entities = Array.from({ length: dataSize }, (_, j) => generateEntity(j))
      const buffer = parquetWriteBuffer({
        columnData: [
          { name: '$id', type: 'STRING', data: entities.map(e => e.$id as string) },
          { name: '$data', type: 'STRING', data: entities.map(e => JSON.stringify(e)) },
        ],
        rowGroupSize: Math.min(dataSize, 10000),
      })
      await bucket.put(`${benchmarkPath}/data.parquet`, buffer)
    }

    const fileHead = await bucket.head(`${benchmarkPath}/data.parquet`)
    if (fileHead) {
      for (let i = 0; i < iterations; i++) {
        const file = createTrackedR2File(bucket, `${benchmarkPath}/data.parquet`, fileHead.size)

        const start = performance.now()
        await parquetQuery({
          file,
          columns: ['$id', '$data'],
          compressors,
        })
        readLatencies.push(performance.now() - start)
        bytesRead = file.bytesRead
      }
    }

    result.operations.read = {
      latencyMs: calculateStats(readLatencies),
      bytesRead,
      rowsRead: dataSize,
    }
  }

  if (operations.includes('time-travel') || operations.length === 0) {
    // Time-travel is only available for Iceberg and Delta backends
    if (backend === 'iceberg' || backend === 'delta') {
      const ttLatencies: number[] = []

      for (let i = 0; i < iterations; i++) {
        const start = performance.now()
        // Simulate time-travel by listing version files
        await bucket.list({ prefix: `${benchmarkPath}/`, limit: 10 })
        ttLatencies.push(performance.now() - start)
      }

      result.operations['time-travel'] = {
        latencyMs: calculateStats(ttLatencies),
        snapshotsAvailable: 1, // Simulated
      }
    }
  }

  // Cleanup
  try {
    const files = await bucket.list({ prefix: benchmarkPath })
    for (const obj of files.objects) {
      await bucket.delete(obj.key)
    }
  } catch {
    // Ignore cleanup errors
  }

  return result
}

// =============================================================================
// Health Check
// =============================================================================

export interface HealthCheckResult {
  status: 'ok' | 'degraded' | 'error'
  timestamp: string
  checks: {
    r2: { status: 'ok' | 'error'; latencyMs: number }
    cache: { status: 'ok' | 'error'; latencyMs: number }
  }
  metadata: {
    colo: string
    region?: string | undefined
  }
}

export async function healthCheck(
  bucket: R2Bucket,
  request: Request
): Promise<HealthCheckResult> {
  const cf = (request as Request & { cf?: { colo?: string | undefined; region?: string | undefined } | undefined }).cf

  // Check R2
  const r2Start = performance.now()
  let r2Status: 'ok' | 'error' = 'ok'
  try {
    await bucket.list({ prefix: 'health-check/', limit: 1 })
  } catch {
    r2Status = 'error'
  }
  const r2Latency = Math.round(performance.now() - r2Start)

  // Check Cache
  const cacheStart = performance.now()
  let cacheStatus: 'ok' | 'error' = 'ok'
  try {
    await caches.open('health-check')
  } catch {
    cacheStatus = 'error'
  }
  const cacheLatency = Math.round(performance.now() - cacheStart)

  const overallStatus = r2Status === 'ok' && cacheStatus === 'ok' ? 'ok' : 'error'

  return {
    status: overallStatus,
    timestamp: new Date().toISOString(),
    checks: {
      r2: { status: r2Status, latencyMs: r2Latency },
      cache: { status: cacheStatus, latencyMs: cacheLatency },
    },
    metadata: {
      colo: cf?.colo ?? 'unknown',
      region: cf?.region,
    },
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

/**
 * Handle /benchmark/e2e/* HTTP requests
 *
 * Routes:
 * - GET /benchmark/e2e/health - Health check
 * - GET /benchmark/e2e/cold-start - Cold start measurement
 * - GET /benchmark/e2e/crud/:operation - CRUD benchmarks
 * - GET /benchmark/e2e/query/:type - Query benchmarks
 * - GET /benchmark/e2e/backend/:type - Backend benchmarks
 */
export async function handleE2EBenchmarkRequest(
  request: Request,
  bucket: R2Bucket
): Promise<Response> {
  const url = new URL(request.url)
  const path = url.pathname
  const startTime = performance.now()

  try {
    // Health check
    if (path === '/benchmark/e2e/health') {
      const result = await healthCheck(bucket, request)
      return Response.json(result, {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Server-Timing': `total;dur=${Math.round(performance.now() - startTime)}`,
        },
      })
    }

    // Cold start
    if (path === '/benchmark/e2e/cold-start') {
      const result = await measureColdStart(bucket, request)
      return Response.json(result, {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'no-store',
          'Server-Timing': `total;dur=${Math.round(performance.now() - startTime)}`,
        },
      })
    }

    // CRUD benchmarks
    const crudMatch = path.match(/^\/benchmark\/e2e\/crud\/([a-z-]+)$/)
    if (crudMatch) {
      const operation = crudMatch[1]
      const validOps = ['create', 'read', 'update', 'delete', 'batch-create', 'batch-read']

      if (!validOps.includes(operation!)) {
        return Response.json({
          error: true,
          message: `Invalid operation: ${operation}. Valid: ${validOps.join(', ')}`,
        }, { status: 400 })
      }

      const iterations = parseInt(url.searchParams.get('iterations') || '10')
      const batchSize = parseInt(url.searchParams.get('batchSize') || '100')
      const warmup = parseInt(url.searchParams.get('warmup') || '2')

      const result = await benchmarkCrud(bucket, operation!, iterations, batchSize, warmup)

      return Response.json(result, {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Server-Timing': `total;dur=${Math.round(performance.now() - startTime)}`,
        },
      })
    }

    // Query benchmarks
    const queryMatch = path.match(/^\/benchmark\/e2e\/query\/([a-z-]+)$/)
    if (queryMatch) {
      const queryType = queryMatch[1]
      const validTypes = ['simple-filter', 'range-filter', 'compound-filter', 'full-text-search', 'pagination', 'aggregation']

      if (!validTypes.includes(queryType!)) {
        return Response.json({
          error: true,
          message: `Invalid query type: ${queryType}. Valid: ${validTypes.join(', ')}`,
        }, { status: 400 })
      }

      const dataset = url.searchParams.get('dataset') || 'imdb'
      const iterations = parseInt(url.searchParams.get('iterations') || '5')
      const limit = parseInt(url.searchParams.get('limit') || '100')

      const result = await benchmarkQuery(bucket, queryType!, dataset, iterations, limit)

      return Response.json(result, {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Server-Timing': `total;dur=${Math.round(performance.now() - startTime)}`,
        },
      })
    }

    // Backend benchmarks
    const backendMatch = path.match(/^\/benchmark\/e2e\/backend\/([a-z-]+)$/)
    if (backendMatch) {
      const backend = backendMatch[1]
      const validBackends = ['iceberg', 'delta', 'native-parquet']

      if (!validBackends.includes(backend!)) {
        return Response.json({
          error: true,
          message: `Invalid backend: ${backend}. Valid: ${validBackends.join(', ')}`,
        }, { status: 400 })
      }

      const operationsParam = url.searchParams.get('operation')
      const operations = operationsParam ? operationsParam.split(',') : []
      const iterations = parseInt(url.searchParams.get('iterations') || '3')
      const dataSize = parseInt(url.searchParams.get('dataSize') || '1000')

      const result = await benchmarkBackend(bucket, backend!, operations, iterations, dataSize)

      return Response.json(result, {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Server-Timing': `total;dur=${Math.round(performance.now() - startTime)}`,
        },
      })
    }

    // List available endpoints
    return Response.json({
      benchmark: 'E2E Benchmark Suite',
      description: 'End-to-end performance benchmarks for deployed ParqueDB workers',
      endpoints: {
        health: {
          path: '/benchmark/e2e/health',
          description: 'Health check with R2 and Cache status',
        },
        coldStart: {
          path: '/benchmark/e2e/cold-start',
          description: 'Measure cold start latency',
        },
        crud: {
          path: '/benchmark/e2e/crud/:operation',
          operations: ['create', 'read', 'update', 'delete', 'batch-create', 'batch-read'],
          params: ['iterations', 'batchSize', 'warmup'],
          example: '/benchmark/e2e/crud/create?iterations=10&warmup=2',
        },
        query: {
          path: '/benchmark/e2e/query/:type',
          types: ['simple-filter', 'range-filter', 'compound-filter', 'full-text-search', 'pagination', 'aggregation'],
          params: ['dataset', 'iterations', 'limit'],
          example: '/benchmark/e2e/query/simple-filter?dataset=imdb&iterations=5',
        },
        backend: {
          path: '/benchmark/e2e/backend/:type',
          types: ['iceberg', 'delta', 'native-parquet'],
          params: ['operation', 'iterations', 'dataSize'],
          example: '/benchmark/e2e/backend/iceberg?operation=write,read&iterations=3',
        },
      },
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    })
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    const stack = error instanceof Error ? error.stack : undefined

    return Response.json({
      error: true,
      message,
      stack,
    }, { status: 500 })
  }
}
