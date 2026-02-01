/**
 * Full ParqueDB Benchmark Suite
 *
 * Tests various query types across different dataset sizes:
 * - Point lookups (by ID)
 * - Equality filters (string)
 * - Range queries (numeric)
 * - Compound filters (AND/OR)
 * - Low selectivity queries
 * - Graph traversals (relationships)
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetQuery } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const BENCHMARK_DIR = '/tmp/parquedb-full-benchmark'
const DATASET_SIZES = [1_000, 10_000, 100_000, 1_000_000]
const ROWS_PER_GROUP = 50_000
const ITERATIONS = 5

// Data distribution
const CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'toys', 'food', 'auto']
const STATUSES = ['active', 'inactive', 'pending', 'archived']
const REGIONS = ['north', 'south', 'east', 'west', 'central']

// =============================================================================
// Utilities
// =============================================================================

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

function formatNumber(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return String(n)
}

function avg(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

function p95(arr) {
  const sorted = [...arr].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length * 0.95)]
}

// =============================================================================
// Data Generation
// =============================================================================

function generateProducts(count) {
  const products = []

  for (let i = 0; i < count; i++) {
    const category = CATEGORIES[i % CATEGORIES.length]
    const status = STATUSES[i % STATUSES.length]
    const region = REGIONS[i % REGIONS.length]
    const price = 10 + (i % 990) // 10-999
    const rating = 1 + (i % 50) / 10 // 1.0-5.9
    const createdYear = 2015 + (i % 10) // 2015-2024
    const inventory = i % 1000

    products.push({
      id: `prod_${String(i).padStart(8, '0')}`,
      name: `Product ${i}`,
      category,
      status,
      region,
      price,
      rating,
      createdYear,
      inventory,
      tags: [category, status, `tag_${i % 20}`],
      metadata: {
        sku: `SKU-${i}`,
        weight: (i % 100) / 10,
        dimensions: { w: i % 50, h: i % 30, d: i % 20 }
      }
    })
  }

  return products
}

function generateRelationships(products, avgRelsPerProduct = 5) {
  const relationships = []
  const count = products.length

  for (let i = 0; i < count; i++) {
    // Each product has ~avgRelsPerProduct relationships
    const numRels = 1 + (i % (avgRelsPerProduct * 2))

    for (let j = 0; j < numRels; j++) {
      const targetIdx = (i + 1 + j * 7) % count // Deterministic but varied
      relationships.push({
        from_id: products[i].id,
        to_id: products[targetIdx].id,
        predicate: j % 2 === 0 ? 'similar_to' : 'bought_together',
        weight: (i + j) % 100 / 100,
        createdAt: Date.now() - (i * 1000)
      })
    }
  }

  return relationships
}

// =============================================================================
// File Writers
// =============================================================================

async function writeProductsV1(products, path) {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: products.map(p => p.id) },
      { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'ZSTD',
  })
  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

async function writeProductsV3(products, path) {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: products.map(p => p.id) },
      { name: '$index_category', type: 'STRING', data: products.map(p => p.category) },
      { name: '$index_status', type: 'STRING', data: products.map(p => p.status) },
      { name: '$index_region', type: 'STRING', data: products.map(p => p.region) },
      { name: '$index_price', type: 'DOUBLE', data: products.map(p => p.price) },
      { name: '$index_rating', type: 'DOUBLE', data: products.map(p => p.rating) },
      { name: '$index_createdYear', type: 'INT32', data: products.map(p => p.createdYear) },
      { name: '$index_inventory', type: 'INT32', data: products.map(p => p.inventory) },
      { name: '$data', type: 'STRING', data: products.map(p => JSON.stringify(p)) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'ZSTD',
  })
  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

async function writeRelationships(relationships, path) {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: 'from_id', type: 'STRING', data: relationships.map(r => r.from_id) },
      { name: 'to_id', type: 'STRING', data: relationships.map(r => r.to_id) },
      { name: 'predicate', type: 'STRING', data: relationships.map(r => r.predicate) },
      { name: 'weight', type: 'DOUBLE', data: relationships.map(r => r.weight) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'ZSTD',
  })
  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

// =============================================================================
// Query Helpers
// =============================================================================

function createFileReader(fileBuffer) {
  return {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }
}

async function loadFile(path) {
  const fileBuffer = await fs.readFile(path)
  return createFileReader(fileBuffer)
}

// =============================================================================
// Query Functions - V1 (JSON blob, no pushdown)
// =============================================================================

async function queryV1(file, filterFn) {
  const rows = await parquetQuery({ file, columns: ['$id', '$data'], compressors })
  return rows.filter(row => filterFn(JSON.parse(row.$data)))
}

// =============================================================================
// Query Functions - V3 (Dual index, with pushdown)
// =============================================================================

async function queryV3(file, filter, indexColumns = []) {
  const columns = ['$id', '$data', ...indexColumns]
  const rows = await parquetQuery({ file, columns, filter, compressors })
  return rows
}

// =============================================================================
// Benchmark Queries
// =============================================================================

const QUERIES = [
  {
    name: 'Point lookup (by ID)',
    description: 'Single record by exact ID',
    selectivity: (count) => 1 / count,
    v1Filter: (targetId) => (p) => p.id === targetId,
    v3Filter: (targetId) => ({ '$id': targetId }),
    v3Columns: [],
    setup: (products) => products[Math.floor(products.length / 2)].id,
  },
  {
    name: 'Equality (category)',
    description: 'All products in "electronics" category',
    selectivity: () => 1 / CATEGORIES.length,
    v1Filter: () => (p) => p.category === 'electronics',
    v3Filter: () => ({ '$index_category': 'electronics' }),
    v3Columns: ['$index_category'],
    setup: () => null,
  },
  {
    name: 'Equality (status)',
    description: 'All "active" products',
    selectivity: () => 1 / STATUSES.length,
    v1Filter: () => (p) => p.status === 'active',
    v3Filter: () => ({ '$index_status': 'active' }),
    v3Columns: ['$index_status'],
    setup: () => null,
  },
  {
    name: 'Range (price)',
    description: 'Products priced $100-$200',
    selectivity: () => 100 / 990,
    v1Filter: () => (p) => p.price >= 100 && p.price <= 200,
    v3Filter: () => ({ '$index_price': { $gte: 100, $lte: 200 } }),
    v3Columns: ['$index_price'],
    setup: () => null,
  },
  {
    name: 'Range (year)',
    description: 'Products created 2022-2024',
    selectivity: () => 3 / 10,
    v1Filter: () => (p) => p.createdYear >= 2022 && p.createdYear <= 2024,
    v3Filter: () => ({ '$index_createdYear': { $gte: 2022, $lte: 2024 } }),
    v3Columns: ['$index_createdYear'],
    setup: () => null,
  },
  {
    name: 'Low selectivity (rating > 4.5)',
    description: 'High-rated products only (~10%)',
    selectivity: () => 0.1,
    v1Filter: () => (p) => p.rating > 4.5,
    v3Filter: () => ({ '$index_rating': { $gt: 4.5 } }),
    v3Columns: ['$index_rating'],
    setup: () => null,
  },
  {
    name: 'Very low selectivity (inventory = 0)',
    description: 'Out of stock (~0.1%)',
    selectivity: () => 1 / 1000,
    v1Filter: () => (p) => p.inventory === 0,
    v3Filter: () => ({ '$index_inventory': 0 }),
    v3Columns: ['$index_inventory'],
    setup: () => null,
  },
  {
    name: 'Compound AND (category + status)',
    description: 'Active electronics',
    selectivity: () => (1 / CATEGORIES.length) * (1 / STATUSES.length),
    v1Filter: () => (p) => p.category === 'electronics' && p.status === 'active',
    v3Filter: () => ({ '$index_category': 'electronics', '$index_status': 'active' }),
    v3Columns: ['$index_category', '$index_status'],
    setup: () => null,
  },
  {
    name: 'Compound (category + price range)',
    description: 'Electronics under $500',
    selectivity: () => (1 / CATEGORIES.length) * (490 / 990),
    v1Filter: () => (p) => p.category === 'electronics' && p.price < 500,
    v3Filter: () => ({ '$index_category': 'electronics', '$index_price': { $lt: 500 } }),
    v3Columns: ['$index_category', '$index_price'],
    setup: () => null,
  },
]

// =============================================================================
// Graph Query Benchmarks
// =============================================================================

async function benchmarkGraphQueries(products, relsFile, size) {
  console.log('\n  Graph Traversal Queries:')

  const targetProduct = products[Math.floor(products.length / 2)]

  // 1-hop: Find all related products
  const oneHopTimes = []
  let oneHopCount = 0
  for (let i = 0; i < ITERATIONS; i++) {
    const start = performance.now()
    const rels = await parquetQuery({
      file: relsFile,
      filter: { from_id: targetProduct.id },
      compressors,
    })
    oneHopTimes.push(performance.now() - start)
    oneHopCount = rels.length
  }

  console.log(`    1-hop (outbound from ID):`)
  console.log(`      Median: ${median(oneHopTimes).toFixed(2)}ms  P95: ${p95(oneHopTimes).toFixed(2)}ms  Results: ${oneHopCount}`)

  // Reverse lookup: Find products that link TO this one
  const reverseTimes = []
  let reverseCount = 0
  for (let i = 0; i < ITERATIONS; i++) {
    const start = performance.now()
    const rels = await parquetQuery({
      file: relsFile,
      filter: { to_id: targetProduct.id },
      compressors,
    })
    reverseTimes.push(performance.now() - start)
    reverseCount = rels.length
  }

  console.log(`    1-hop (inbound to ID):`)
  console.log(`      Median: ${median(reverseTimes).toFixed(2)}ms  P95: ${p95(reverseTimes).toFixed(2)}ms  Results: ${reverseCount}`)

  // Filter by predicate
  const predicateTimes = []
  let predicateCount = 0
  for (let i = 0; i < ITERATIONS; i++) {
    const start = performance.now()
    const rels = await parquetQuery({
      file: relsFile,
      filter: { predicate: 'similar_to' },
      compressors,
    })
    predicateTimes.push(performance.now() - start)
    predicateCount = rels.length
  }

  console.log(`    Filter by predicate ('similar_to'):`)
  console.log(`      Median: ${median(predicateTimes).toFixed(2)}ms  P95: ${p95(predicateTimes).toFixed(2)}ms  Results: ${predicateCount.toLocaleString()}`)

  return {
    oneHop: { median: median(oneHopTimes), p95: p95(oneHopTimes), count: oneHopCount },
    reverse: { median: median(reverseTimes), p95: p95(reverseTimes), count: reverseCount },
    predicate: { median: median(predicateTimes), p95: p95(predicateTimes), count: predicateCount },
  }
}

// =============================================================================
// Main Benchmark Runner
// =============================================================================

async function runBenchmark() {
  console.log('═'.repeat(100))
  console.log('ParqueDB Full Benchmark Suite')
  console.log('═'.repeat(100))
  console.log()
  console.log(`Iterations per query: ${ITERATIONS}`)
  console.log(`Row group size: ${ROWS_PER_GROUP.toLocaleString()}`)
  console.log(`Dataset sizes: ${DATASET_SIZES.map(formatNumber).join(', ')}`)
  console.log()

  await fs.mkdir(BENCHMARK_DIR, { recursive: true })

  const allResults = []

  for (const size of DATASET_SIZES) {
    console.log('\n' + '─'.repeat(100))
    console.log(`Dataset: ${formatNumber(size)} products`)
    console.log('─'.repeat(100))

    // Generate data
    console.log('\nGenerating data...')
    const genStart = performance.now()
    const products = generateProducts(size)
    const relationships = generateRelationships(products)
    console.log(`  Products: ${products.length.toLocaleString()} in ${(performance.now() - genStart).toFixed(0)}ms`)
    console.log(`  Relationships: ${relationships.length.toLocaleString()}`)

    // Write files
    console.log('\nWriting files...')
    const v1Path = join(BENCHMARK_DIR, `products-v1-${size}.parquet`)
    const v3Path = join(BENCHMARK_DIR, `products-v3-${size}.parquet`)
    const relsPath = join(BENCHMARK_DIR, `relationships-${size}.parquet`)

    let writeStart = performance.now()
    const v1Size = await writeProductsV1(products, v1Path)
    console.log(`  V1 (JSON):  ${formatBytes(v1Size).padStart(12)} in ${(performance.now() - writeStart).toFixed(0)}ms`)

    writeStart = performance.now()
    const v3Size = await writeProductsV3(products, v3Path)
    console.log(`  V3 (Dual):  ${formatBytes(v3Size).padStart(12)} in ${(performance.now() - writeStart).toFixed(0)}ms`)
    console.log(`  Overhead:   ${((v3Size - v1Size) / v1Size * 100).toFixed(1)}%`)

    writeStart = performance.now()
    const relsSize = await writeRelationships(relationships, relsPath)
    console.log(`  Rels:       ${formatBytes(relsSize).padStart(12)} in ${(performance.now() - writeStart).toFixed(0)}ms`)

    // Load files into memory for fair comparison
    console.log('\nLoading files into memory...')
    const v1File = await loadFile(v1Path)
    const v3File = await loadFile(v3Path)
    const relsFile = await loadFile(relsPath)

    // Run query benchmarks
    console.log('\n  Entity Queries:')
    console.log('  ' + '─'.repeat(96))
    console.log('  ' + 'Query'.padEnd(35) + 'V1 (JSON)'.padStart(15) + 'V3 (Index)'.padStart(15) + 'Speedup'.padStart(10) + 'Rows'.padStart(12) + 'Select%'.padStart(10))
    console.log('  ' + '─'.repeat(96))

    const sizeResults = { size, queries: [], graph: null }

    for (const query of QUERIES) {
      const setupData = query.setup(products)
      const v1FilterFn = query.v1Filter(setupData)
      const v3Filter = query.v3Filter(setupData)

      // V1 benchmark
      const v1Times = []
      let v1Count = 0
      for (let i = 0; i < ITERATIONS; i++) {
        const start = performance.now()
        const results = await queryV1(v1File, v1FilterFn)
        v1Times.push(performance.now() - start)
        v1Count = results.length
      }

      // V3 benchmark
      const v3Times = []
      let v3Count = 0
      for (let i = 0; i < ITERATIONS; i++) {
        const start = performance.now()
        const results = await queryV3(v3File, v3Filter, query.v3Columns)
        v3Times.push(performance.now() - start)
        v3Count = results.length
      }

      const v1Median = median(v1Times)
      const v3Median = median(v3Times)
      const speedup = v1Median / v3Median
      const selectivity = (v1Count / size * 100).toFixed(2)

      // Verify counts match
      const countMatch = v1Count === v3Count ? '' : ' ⚠️'

      console.log('  ' +
        query.name.padEnd(35) +
        `${v1Median.toFixed(1)}ms`.padStart(15) +
        `${v3Median.toFixed(1)}ms`.padStart(15) +
        `${speedup.toFixed(1)}x`.padStart(10) +
        `${v1Count.toLocaleString()}${countMatch}`.padStart(12) +
        `${selectivity}%`.padStart(10)
      )

      sizeResults.queries.push({
        name: query.name,
        v1Median,
        v3Median,
        speedup,
        rows: v1Count,
        selectivity: v1Count / size,
      })
    }

    // Graph benchmarks
    sizeResults.graph = await benchmarkGraphQueries(products, relsFile, size)

    allResults.push(sizeResults)
  }

  // Summary
  console.log('\n' + '═'.repeat(100))
  console.log('SUMMARY')
  console.log('═'.repeat(100))

  console.log('\nAverage Speedup by Dataset Size:')
  for (const result of allResults) {
    const avgSpeedup = avg(result.queries.map(q => q.speedup))
    const maxSpeedup = Math.max(...result.queries.map(q => q.speedup))
    const minSpeedup = Math.min(...result.queries.map(q => q.speedup))
    console.log(`  ${formatNumber(result.size).padEnd(6)}: Avg ${avgSpeedup.toFixed(1)}x  (Min: ${minSpeedup.toFixed(1)}x, Max: ${maxSpeedup.toFixed(1)}x)`)
  }

  console.log('\nBest Speedups (across all sizes):')
  const allQueries = allResults.flatMap(r => r.queries.map(q => ({ ...q, size: r.size })))
  const sortedBySpeedup = [...allQueries].sort((a, b) => b.speedup - a.speedup)
  for (const q of sortedBySpeedup.slice(0, 5)) {
    console.log(`  ${q.speedup.toFixed(1)}x - ${q.name} @ ${formatNumber(q.size)} (${(q.selectivity * 100).toFixed(2)}% selectivity)`)
  }

  console.log('\n' + '═'.repeat(100))
  console.log('Benchmark complete!')
  console.log('═'.repeat(100))

  // Save results to JSON
  const resultsPath = join(BENCHMARK_DIR, 'results.json')
  await fs.writeFile(resultsPath, JSON.stringify(allResults, null, 2))
  console.log(`\nResults saved to: ${resultsPath}`)
}

runBenchmark().catch(console.error)
