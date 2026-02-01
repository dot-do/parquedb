/**
 * R2 Benchmark Module
 *
 * Real-world benchmarks measuring actual R2 I/O performance.
 * This is the only benchmark that matters for Workers deployment.
 */

import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetQuery } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// Use the Cloudflare R2Bucket type directly
type R2Bucket = {
  get(key: string, options?: { range?: { offset: number; length: number } }): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<unknown>
  delete(key: string): Promise<void>
  head(key: string): Promise<{ size: number } | null>
}

// =============================================================================
// Types
// =============================================================================

export interface BenchmarkConfig {
  sizes: number[]
  iterations: number
  rowGroupSize: number
}

export interface BenchmarkResult {
  config: BenchmarkConfig
  datasets: DatasetResult[]
  summary: Summary
}

interface DatasetResult {
  size: number
  fileSize: { v1: number; v3: number; overhead: string }
  writeTime: { v1: number; v3: number }
  queries: QueryResult[]
}

interface QueryResult {
  name: string
  v1: QueryStats
  v3: QueryStats
  speedup: number
  bytesReduction: string
}

interface QueryStats {
  median: number
  p95: number
  rows: number
  bytesRead: number
}

interface Summary {
  avgSpeedup: number
  bestSpeedup: { query: string; speedup: number; size: number }
  avgBytesReduction: number
}

// =============================================================================
// Data Generation
// =============================================================================

const CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'toys', 'food', 'auto']
const STATUSES = ['active', 'inactive', 'pending', 'archived']
const TYPES = ['product', 'review', 'user', 'order'] // For type-prefixed $id tests

function generateProducts(count: number, sortByCategory = true) {
  const products = []
  const largePayload = 'x'.repeat(200) // Realistic JSON payload size

  for (let i = 0; i < count; i++) {
    const type = TYPES[i % TYPES.length]
    products.push({
      // Type-prefixed ID (like ParqueDB ns:id pattern)
      id: `${type}:${String(i).padStart(8, '0')}`,
      type, // Redundant but useful for comparison
      name: `Product ${i}`,
      category: CATEGORIES[i % CATEGORIES.length],
      status: STATUSES[i % STATUSES.length],
      price: 10 + (i % 990),
      rating: 1 + (i % 50) / 10,
      createdYear: 2015 + (i % 10),
      description: `Product ${i} description. ${largePayload}`,
    })
  }

  // Sort by $id (type-prefixed) so row groups contain clustered types
  // This enables predicate pushdown via row-group statistics on $id
  if (sortByCategory) {
    products.sort((a, b) => a.id.localeCompare(b.id))
  }

  return products
}

// =============================================================================
// R2 File Operations
// =============================================================================

async function writeV1ToR2(
  bucket: R2Bucket,
  products: ReturnType<typeof generateProducts>,
  path: string,
  rowGroupSize: number
): Promise<number> {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: products.map(p => p.id) },
      { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
    ],
    rowGroupSize,
  })

  await bucket.put(path, buffer)
  return buffer.byteLength
}

async function writeV3ToR2(
  bucket: R2Bucket,
  products: ReturnType<typeof generateProducts>,
  path: string,
  rowGroupSize: number
): Promise<number> {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: products.map(p => p.id) },
      { name: '$index_category', type: 'STRING', data: products.map(p => p.category) },
      { name: '$index_status', type: 'STRING', data: products.map(p => p.status) },
      { name: '$index_price', type: 'DOUBLE', data: products.map(p => p.price) },
      { name: '$index_rating', type: 'DOUBLE', data: products.map(p => p.rating) },
      { name: '$index_createdYear', type: 'INT32', data: products.map(p => p.createdYear) },
      { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
    ],
    rowGroupSize,
  })

  await bucket.put(path, buffer)
  return buffer.byteLength
}

// =============================================================================
// R2 Query with Byte Tracking
// =============================================================================

interface TrackedFile {
  byteLength: number
  bytesRead: number
  slice: (start: number, end: number) => Promise<ArrayBuffer>
}

function createTrackedR2File(bucket: R2Bucket, key: string, size: number): TrackedFile {
  const file: TrackedFile = {
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
// Query Definitions
// =============================================================================

interface QueryDef {
  name: string
  v1Filter: (p: { category: string; status: string; price: number; rating: number }) => boolean
  v3Filter: Record<string, unknown>
  v3Columns: string[]
}

const QUERIES: QueryDef[] = [
  {
    name: 'String equality (category)',
    v1Filter: (p) => p.category === 'electronics',
    v3Filter: { '$index_category': 'electronics' },
    v3Columns: ['$index_category'],
  },
  {
    name: 'Compound (category + status)',
    v1Filter: (p) => p.category === 'electronics' && p.status === 'active',
    v3Filter: { '$index_category': 'electronics', '$index_status': 'active' },
    v3Columns: ['$index_category', '$index_status'],
  },
  {
    name: 'Numeric range (price)',
    v1Filter: (p) => p.price >= 100 && p.price <= 200,
    v3Filter: { '$index_price': { $gte: 100, $lte: 200 } },
    v3Columns: ['$index_price'],
  },
  {
    name: 'Low selectivity (rating > 4.5)',
    v1Filter: (p) => p.rating > 4.5,
    v3Filter: { '$index_rating': { $gt: 4.5 } },
    v3Columns: ['$index_rating'],
  },
  {
    name: 'Count only (no $data)',
    v1Filter: (p) => p.category === 'electronics',
    v3Filter: { '$index_category': 'electronics' },
    v3Columns: ['$index_category'],
    // Special: V3 doesn't read $data column
  },
]

// =============================================================================
// Benchmark Runner
// =============================================================================

export async function runR2Benchmark(
  bucket: R2Bucket,
  config: BenchmarkConfig = {
    sizes: [10_000, 50_000],
    iterations: 3,
    rowGroupSize: 10_000,
  }
): Promise<BenchmarkResult> {
  const datasets: DatasetResult[] = []
  const allSpeedups: number[] = []
  const allBytesReductions: number[] = []
  let bestSpeedup = { query: '', speedup: 0, size: 0 }

  for (const size of config.sizes) {
    console.log(`\nBenchmarking ${size.toLocaleString()} products...`)

    // Generate data
    const products = generateProducts(size)

    // Write to R2
    const v1Path = `benchmark/v1-${size}.parquet`
    const v3Path = `benchmark/v3-${size}.parquet`

    const v1WriteStart = performance.now()
    const v1Size = await writeV1ToR2(bucket, products, v1Path, config.rowGroupSize)
    const v1WriteTime = performance.now() - v1WriteStart

    const v3WriteStart = performance.now()
    const v3Size = await writeV3ToR2(bucket, products, v3Path, config.rowGroupSize)
    const v3WriteTime = performance.now() - v3WriteStart

    const overhead = ((v3Size - v1Size) / v1Size * 100).toFixed(1)

    const queryResults: QueryResult[] = []

    for (const query of QUERIES) {
      const isCountOnly = query.name.includes('Count only')

      // V1 benchmark
      const v1Times: number[] = []
      let v1Rows = 0
      let v1Bytes = 0

      for (let i = 0; i < config.iterations; i++) {
        const file = createTrackedR2File(bucket, v1Path, v1Size)
        const start = performance.now()

        const rows = await parquetQuery({
          file,
          columns: ['$id', '$data'],
          compressors,
        })

        const filtered = rows.filter((row) => {
          const data = JSON.parse((row as { $data: string }).$data)
          return query.v1Filter(data)
        })

        v1Times.push(performance.now() - start)
        v1Rows = filtered.length
        v1Bytes = file.bytesRead
      }

      // V3 benchmark
      const v3Times: number[] = []
      let v3Rows = 0
      let v3Bytes = 0

      for (let i = 0; i < config.iterations; i++) {
        const file = createTrackedR2File(bucket, v3Path, v3Size)
        const start = performance.now()

        // For count-only, don't read $data column
        const columns = isCountOnly
          ? ['$id', ...query.v3Columns]
          : ['$id', '$data', ...query.v3Columns]

        const rows = await parquetQuery({
          file,
          columns,
          filter: query.v3Filter,
          compressors,
        })

        v3Times.push(performance.now() - start)
        v3Rows = rows.length
        v3Bytes = file.bytesRead
      }

      // Calculate stats
      v1Times.sort((a, b) => a - b)
      v3Times.sort((a, b) => a - b)

      const v1Median = v1Times[Math.floor(v1Times.length / 2)]
      const v3Median = v3Times[Math.floor(v3Times.length / 2)]
      const speedup = v1Median / v3Median
      const bytesReduction = ((v1Bytes - v3Bytes) / v1Bytes * 100).toFixed(0)

      allSpeedups.push(speedup)
      allBytesReductions.push(parseFloat(bytesReduction))

      if (speedup > bestSpeedup.speedup) {
        bestSpeedup = { query: query.name, speedup, size }
      }

      queryResults.push({
        name: query.name,
        v1: {
          median: Math.round(v1Median),
          p95: Math.round(v1Times[Math.floor(v1Times.length * 0.95)]),
          rows: v1Rows,
          bytesRead: v1Bytes,
        },
        v3: {
          median: Math.round(v3Median),
          p95: Math.round(v3Times[Math.floor(v3Times.length * 0.95)]),
          rows: v3Rows,
          bytesRead: v3Bytes,
        },
        speedup: Math.round(speedup * 10) / 10,
        bytesReduction: `${bytesReduction}%`,
      })
    }

    datasets.push({
      size,
      fileSize: { v1: v1Size, v3: v3Size, overhead: `${overhead}%` },
      writeTime: { v1: Math.round(v1WriteTime), v3: Math.round(v3WriteTime) },
      queries: queryResults,
    })

    // Cleanup
    await bucket.delete(v1Path)
    await bucket.delete(v3Path)
  }

  const avgSpeedup = allSpeedups.reduce((a, b) => a + b, 0) / allSpeedups.length
  const avgBytesReduction = allBytesReductions.reduce((a, b) => a + b, 0) / allBytesReductions.length

  return {
    config,
    datasets,
    summary: {
      avgSpeedup: Math.round(avgSpeedup * 10) / 10,
      bestSpeedup,
      avgBytesReduction: Math.round(avgBytesReduction),
    },
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

export async function handleBenchmarkRequest(
  request: Request,
  bucket: R2Bucket
): Promise<Response> {
  const url = new URL(request.url)
  const startTime = performance.now()

  // Parse config from query params
  const sizesParam = url.searchParams.get('sizes')
  const sizes = sizesParam
    ? sizesParam.split(',').map(s => parseInt(s))
    : [10_000, 50_000]

  const iterations = parseInt(url.searchParams.get('iterations') || '3')
  const rowGroupSize = parseInt(url.searchParams.get('rowGroupSize') || '10000')

  try {
    const result = await runR2Benchmark(bucket, { sizes, iterations, rowGroupSize })

    const totalTime = Math.round(performance.now() - startTime)

    return Response.json({
      benchmark: 'R2 Variant Shredding Performance',
      description: 'Real-world R2 I/O measurements comparing V1 (JSON blob) vs V3 (dual index)',
      totalTimeMs: totalTime,
      ...result,
      interpretation: {
        v1: 'Traditional approach: Store all data in $data JSON blob, parse and filter in JS',
        v3: 'Dual Variant: $id + $index_* columns for filtering + $data for full entity',
        speedup: `V3 is ${result.summary.avgSpeedup}x faster on average`,
        bytesReduction: `V3 reads ${result.summary.avgBytesReduction}% fewer bytes from R2`,
        bestCase: `Best: ${result.summary.bestSpeedup.query} at ${result.summary.bestSpeedup.size.toLocaleString()} rows (${result.summary.bestSpeedup.speedup.toFixed(1)}x)`,
      },
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Server-Timing': `total;dur=${totalTime}`,
      },
    })
  } catch (error) {
    return Response.json({
      error: true,
      message: (error as Error).message,
      stack: (error as Error).stack,
    }, { status: 500 })
  }
}
