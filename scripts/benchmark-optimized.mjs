/**
 * Optimized ParqueDB Benchmark
 *
 * Tests the real performance benefits of predicate pushdown with:
 * - LZ4 compression (fast)
 * - Column projection (only read needed columns)
 * - Comparison with/without JSON parsing overhead
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetQuery, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const BENCHMARK_DIR = '/tmp/parquedb-optimized-benchmark'
const DATASET_SIZES = [10_000, 100_000, 500_000, 1_000_000]
const ROWS_PER_GROUP = 10_000 // Smaller row groups for better pushdown
const ITERATIONS = 5

const CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'toys', 'food', 'auto']
const STATUSES = ['active', 'inactive', 'pending', 'archived']

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
    products.push({
      id: `prod_${String(i).padStart(8, '0')}`,
      name: `Product ${i}`,
      category: CATEGORIES[i % CATEGORIES.length],
      status: STATUSES[i % STATUSES.length],
      price: 10 + (i % 990),
      rating: 1 + (i % 50) / 10,
      createdYear: 2015 + (i % 10),
    })
  }
  return products
}

// =============================================================================
// File Operations
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

async function writeV3(products, path) {
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
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'LZ4_RAW',
  })
  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

// =============================================================================
// Query Scenarios
// =============================================================================

const SCENARIOS = [
  {
    name: 'Count only (no data fetch)',
    description: 'Filter + count, no $data column',
    filter: { '$index_category': 'electronics' },
    columns: ['$id'],
    selectivity: 1 / 8,
  },
  {
    name: 'Count + IDs',
    description: 'Filter and return IDs only',
    filter: { '$index_category': 'electronics', '$index_status': 'active' },
    columns: ['$id'],
    selectivity: 1 / 32,
  },
  {
    name: 'Filter + full data',
    description: 'Filter and return full entity data',
    filter: { '$index_category': 'electronics' },
    columns: ['$id', '$data', '$index_category'],
    selectivity: 1 / 8,
  },
  {
    name: 'Range query (price)',
    description: 'Numeric range filter',
    filter: { '$index_price': { $gte: 100, $lte: 200 } },
    columns: ['$id', '$index_price'],
    selectivity: 100 / 990,
  },
  {
    name: 'Range query (year)',
    description: 'Integer range filter',
    filter: { '$index_createdYear': { $gte: 2022, $lte: 2024 } },
    columns: ['$id', '$index_createdYear'],
    selectivity: 3 / 10,
  },
  {
    name: 'Very low selectivity',
    description: 'Rating > 4.9 (~2% of data)',
    filter: { '$index_rating': { $gt: 4.9 } },
    columns: ['$id', '$index_rating'],
    selectivity: 0.02,
  },
  {
    name: 'Point lookup',
    description: 'Single record by ID',
    filter: null, // Will be set dynamically
    columns: ['$id', '$data'],
    selectivity: null, // 1/N
    dynamic: true,
  },
]

// =============================================================================
// Comparison: With vs Without Row Group Statistics
// =============================================================================

async function measureRowGroupSkipping(file, filter, columns) {
  // Get metadata to see row group info
  const metadata = await parquetMetadataAsync(file)
  const totalRowGroups = metadata.row_groups.length
  const totalRows = metadata.num_rows

  const start = performance.now()
  const rows = await parquetQuery({ file, filter, columns, compressors })
  const elapsed = performance.now() - start

  return {
    elapsed,
    rowsReturned: rows.length,
    totalRows,
    totalRowGroups,
    rowGroupsAccessed: 'N/A', // Would need instrumentation in hyparquet
  }
}

// =============================================================================
// Main Benchmark
// =============================================================================

async function runBenchmark() {
  console.log('═'.repeat(100))
  console.log('Optimized ParqueDB Benchmark')
  console.log('═'.repeat(100))
  console.log()
  console.log(`Row group size: ${ROWS_PER_GROUP.toLocaleString()} (smaller = better pushdown potential)`)
  console.log(`Compression: LZ4_RAW (fast)`)
  console.log(`Iterations: ${ITERATIONS}`)
  console.log()

  await fs.mkdir(BENCHMARK_DIR, { recursive: true })

  const results = []

  for (const size of DATASET_SIZES) {
    console.log('\n' + '─'.repeat(100))
    console.log(`Dataset: ${formatNumber(size)} products (${Math.ceil(size / ROWS_PER_GROUP)} row groups)`)
    console.log('─'.repeat(100))

    // Generate and write
    console.log('\nGenerating data...')
    const products = generateProducts(size)

    const path = join(BENCHMARK_DIR, `products-${size}.parquet`)
    console.log('Writing file...')
    const writeStart = performance.now()
    const fileSize = await writeV3(products, path)
    console.log(`  Size: ${formatBytes(fileSize)} in ${(performance.now() - writeStart).toFixed(0)}ms`)

    // Load file
    const fileBuffer = await fs.readFile(path)
    const file = createFileReader(fileBuffer)

    // Get metadata
    const metadata = await parquetMetadataAsync(file)
    console.log(`  Row groups: ${metadata.row_groups.length}`)
    console.log(`  Columns: ${metadata.schema.slice(1).map(s => s.name).join(', ')}`)

    // Run scenarios
    console.log('\n  Queries:')
    console.log('  ' + '─'.repeat(96))
    console.log('  ' + 'Scenario'.padEnd(35) + 'Median'.padStart(12) + 'P95'.padStart(12) + 'Rows'.padStart(12) + 'Select%'.padStart(10) + 'Throughput'.padStart(15))
    console.log('  ' + '─'.repeat(96))

    const sizeResults = { size, scenarios: [] }

    for (const scenario of SCENARIOS) {
      let filter = scenario.filter
      let expectedSelectivity = scenario.selectivity

      // Handle dynamic scenarios
      if (scenario.dynamic) {
        const targetId = products[Math.floor(products.length / 2)].id
        filter = { '$id': targetId }
        expectedSelectivity = 1 / size
      }

      const times = []
      let rowCount = 0

      for (let i = 0; i < ITERATIONS; i++) {
        const start = performance.now()
        const rows = await parquetQuery({
          file,
          filter,
          columns: scenario.columns,
          compressors,
        })
        times.push(performance.now() - start)
        rowCount = rows.length
      }

      const med = median(times)
      const p = p95(times)
      const actualSelectivity = rowCount / size
      const throughput = (size / med * 1000).toFixed(0) // rows/sec

      console.log('  ' +
        scenario.name.padEnd(35) +
        `${med.toFixed(1)}ms`.padStart(12) +
        `${p.toFixed(1)}ms`.padStart(12) +
        rowCount.toLocaleString().padStart(12) +
        `${(actualSelectivity * 100).toFixed(2)}%`.padStart(10) +
        `${formatNumber(Number(throughput))}/s`.padStart(15)
      )

      sizeResults.scenarios.push({
        name: scenario.name,
        median: med,
        p95: p,
        rows: rowCount,
        selectivity: actualSelectivity,
        throughputPerSec: Number(throughput),
      })
    }

    results.push(sizeResults)
  }

  // Summary table
  console.log('\n' + '═'.repeat(100))
  console.log('SUMMARY: Query Latency by Dataset Size')
  console.log('═'.repeat(100))

  // Create comparison table
  const scenarioNames = SCENARIOS.map(s => s.name)
  console.log('\n' + 'Scenario'.padEnd(35) + DATASET_SIZES.map(s => formatNumber(s).padStart(12)).join(''))
  console.log('─'.repeat(35 + DATASET_SIZES.length * 12))

  for (const scenarioName of scenarioNames) {
    const row = [scenarioName.padEnd(35)]
    for (const sizeResult of results) {
      const scenario = sizeResult.scenarios.find(s => s.name === scenarioName)
      row.push(`${scenario.median.toFixed(1)}ms`.padStart(12))
    }
    console.log(row.join(''))
  }

  // Throughput summary
  console.log('\n' + '═'.repeat(100))
  console.log('SUMMARY: Throughput (rows/sec)')
  console.log('═'.repeat(100))

  console.log('\n' + 'Scenario'.padEnd(35) + DATASET_SIZES.map(s => formatNumber(s).padStart(12)).join(''))
  console.log('─'.repeat(35 + DATASET_SIZES.length * 12))

  for (const scenarioName of scenarioNames) {
    const row = [scenarioName.padEnd(35)]
    for (const sizeResult of results) {
      const scenario = sizeResult.scenarios.find(s => s.name === scenarioName)
      row.push(formatNumber(scenario.throughputPerSec).padStart(12))
    }
    console.log(row.join(''))
  }

  // Scaling analysis
  console.log('\n' + '═'.repeat(100))
  console.log('SCALING ANALYSIS')
  console.log('═'.repeat(100))

  console.log('\nLatency scaling factor (1M / 10K):')
  const baseline = results.find(r => r.size === 10_000)
  const largest = results.find(r => r.size === 1_000_000)

  if (baseline && largest) {
    for (const scenarioName of scenarioNames) {
      const baseScenario = baseline.scenarios.find(s => s.name === scenarioName)
      const largeScenario = largest.scenarios.find(s => s.name === scenarioName)
      const scaleFactor = largeScenario.median / baseScenario.median
      const expectedLinear = largest.size / baseline.size // 100x
      const efficiency = (expectedLinear / scaleFactor * 100).toFixed(0)
      console.log(`  ${scenarioName.padEnd(35)} ${scaleFactor.toFixed(1)}x (${efficiency}% efficient vs linear)`)
    }
  }

  console.log('\n' + '═'.repeat(100))
  console.log('Benchmark complete!')
  console.log('═'.repeat(100))
}

runBenchmark().catch(console.error)
