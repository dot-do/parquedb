/**
 * Star Schema Benchmark (SSB) Example for ParqueDB
 *
 * This example demonstrates:
 * - Defining a star schema with fact and dimension tables
 * - Generating synthetic benchmark data at different scale factors
 * - Running the 13 official SSB queries
 * - Measuring and reporting query performance
 *
 * Usage:
 *   npx tsx examples/ssb/index.ts [options]
 *
 * Options:
 *   --scale-factor=N    Scale factor (1, 10, 100) - default: 1
 *   --skip-generate     Skip data generation (use existing data)
 *   --queries-only      Run queries without benchmarking
 *   --output-dir=PATH   Output directory for data - default: .db/ssb
 *   --iterations=N      Benchmark iterations - default: 3
 *   --verbose           Show detailed progress
 *
 * @example
 * ```bash
 * # Generate SF1 data and run benchmark
 * npx tsx examples/ssb/index.ts
 *
 * # Run with SF10
 * npx tsx examples/ssb/index.ts --scale-factor=10
 *
 * # Skip generation, run queries only
 * npx tsx examples/ssb/index.ts --skip-generate --queries-only
 * ```
 */

import { rm } from 'fs/promises'
import { DB } from '../../src/db'
import { FsBackend } from '../../src/storage'
import { ssbSchema, SSB_SCALE_FACTORS } from './schema'
import { generateSSB, type GenerateStats } from './generate'
import {
  runSSBQueries,
  runQ11, runQ12, runQ13,
  runQ21, runQ22, runQ23,
  runQ31, runQ32, runQ33, runQ34,
  runQ41, runQ42, runQ43,
  type SSBQueryResults,
} from './queries'

// =============================================================================
// Types
// =============================================================================

export interface SSBBenchmarkOptions {
  /** Scale factor (1, 10, or 100) */
  scaleFactor?: number
  /** Output directory for data */
  outputDir?: string
  /** Skip data generation */
  skipGenerate?: boolean
  /** Run queries only (no benchmark timing) */
  queriesOnly?: boolean
  /** Number of benchmark iterations */
  iterations?: number
  /** Number of warmup iterations */
  warmupIterations?: number
  /** Show verbose output */
  verbose?: boolean
}

export interface QueryBenchmark {
  name: string
  description: string
  coldMs: number
  warmMs: number[]
  avgMs: number
  minMs: number
  maxMs: number
  rowsReturned: number
}

export interface SSBBenchmarkResults {
  scaleFactor: number
  generationStats?: GenerateStats
  queryResults: SSBQueryResults
  benchmarks: QueryBenchmark[]
  summary: {
    totalQueries: number
    avgQueryTimeMs: number
    minQueryTimeMs: number
    maxQueryTimeMs: number
    q1AvgMs: number
    q2AvgMs: number
    q3AvgMs: number
    q4AvgMs: number
  }
  durationMs: number
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Run a single query benchmark
 */
async function benchmarkQuery<T>(
  name: string,
  description: string,
  fn: () => Promise<T>,
  getRowCount: (result: T) => number,
  iterations: number,
): Promise<QueryBenchmark> {
  // Cold start
  const coldStart = performance.now()
  const coldResult = await fn()
  const coldMs = performance.now() - coldStart

  // Warm iterations
  const warmMs: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    warmMs.push(performance.now() - start)
  }

  const allTimes = [coldMs, ...warmMs]
  const avgMs = allTimes.reduce((a, b) => a + b, 0) / allTimes.length
  const minMs = Math.min(...allTimes)
  const maxMs = Math.max(...allTimes)

  return {
    name,
    description,
    coldMs,
    warmMs,
    avgMs,
    minMs,
    maxMs,
    rowsReturned: getRowCount(coldResult),
  }
}

/**
 * Run the full SSB benchmark suite
 */
export async function runSSBBenchmark(
  options: SSBBenchmarkOptions = {}
): Promise<SSBBenchmarkResults> {
  const {
    scaleFactor = 1,
    outputDir = '.db/ssb',
    skipGenerate = false,
    queriesOnly = false,
    iterations = 3,
    warmupIterations = 1,
    verbose = false,
  } = options

  const startTime = performance.now()
  const log = verbose ? console.log : () => {}

  console.log('='.repeat(70))
  console.log('Star Schema Benchmark (SSB) for ParqueDB')
  console.log('='.repeat(70))
  console.log(`Scale Factor: SF${scaleFactor}`)
  console.log(`Output Directory: ${outputDir}`)
  console.log(`Iterations: ${iterations} (+ ${warmupIterations} warmup)`)
  console.log()

  let generationStats: GenerateStats | undefined

  // Generate data if needed
  if (!skipGenerate) {
    console.log('Generating SSB data...')
    console.log()

    // Clean output directory
    await rm(outputDir, { recursive: true, force: true })

    generationStats = await generateSSB({
      scaleFactor,
      outputDir,
      onProgress: verbose ? (info) => {
        process.stdout.write(`\r  ${info.table}: ${info.percentage}%`)
      } : undefined,
    })

    console.log()
    console.log('Data generation complete:')
    console.log(`  LINEORDER: ${generationStats.tables.lineorder.toLocaleString()} rows`)
    console.log(`  CUSTOMER:  ${generationStats.tables.customer.toLocaleString()} rows`)
    console.log(`  SUPPLIER:  ${generationStats.tables.supplier.toLocaleString()} rows`)
    console.log(`  PART:      ${generationStats.tables.part.toLocaleString()} rows`)
    console.log(`  DATE:      ${generationStats.tables.date.toLocaleString()} rows`)
    console.log(`  Duration:  ${(generationStats.durationMs / 1000).toFixed(1)}s`)
    console.log()
  }

  // Create database connection
  const db = DB(ssbSchema, {
    storage: new FsBackend(outputDir),
  })

  let queryResults: SSBQueryResults
  const benchmarks: QueryBenchmark[] = []

  try {
    if (queriesOnly) {
      // Run queries without benchmarking
      console.log('Running SSB queries...')
      queryResults = await runSSBQueries(db)
    } else {
      // Run benchmarks
      console.log('Running SSB query benchmarks...')
      console.log()

      // Q1 Flight
      console.log('Q1 Flight: Revenue Sum Queries')
      const q1Params = { year: 1993, discountLo: 1, discountHi: 3, quantityLt: 25 }

      benchmarks.push(await benchmarkQuery(
        'Q1.1', 'Revenue sum with discount/quantity filters',
        () => runQ11(db, q1Params),
        () => 1,
        iterations
      ))
      log(`  Q1.1: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg`)

      benchmarks.push(await benchmarkQuery(
        'Q1.2', 'Revenue sum for specific month',
        () => runQ12(db, { ...q1Params, yearmonthnum: 199401, discountLo: 4, discountHi: 6 }),
        () => 1,
        iterations
      ))
      log(`  Q1.2: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg`)

      benchmarks.push(await benchmarkQuery(
        'Q1.3', 'Revenue sum for specific week',
        () => runQ13(db, { ...q1Params, year: 1994, weeknuminyear: 6, discountLo: 5, discountHi: 7 }),
        () => 1,
        iterations
      ))
      log(`  Q1.3: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg`)

      // Q2 Flight
      console.log('\nQ2 Flight: Revenue by Brand/Category')

      benchmarks.push(await benchmarkQuery(
        'Q2.1', 'Revenue by brand for AMERICA region',
        () => runQ21(db, { region: 'AMERICA', category: 'MFGR#12' }),
        (r) => r.length,
        iterations
      ))
      log(`  Q2.1: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q2.2', 'Revenue by brand for ASIA region',
        () => runQ22(db, { region: 'ASIA', brands: ['MFGR#2221', 'MFGR#2228'] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q2.2: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q2.3', 'Revenue by brand for EUROPE region',
        () => runQ23(db, { region: 'EUROPE', brands: ['MFGR#2239'] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q2.3: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      // Q3 Flight
      console.log('\nQ3 Flight: Revenue by Customer/Supplier')

      benchmarks.push(await benchmarkQuery(
        'Q3.1', 'Revenue by nation for ASIA',
        () => runQ31(db, { customerRegion: 'ASIA', supplierRegion: 'ASIA', yearRange: [1992, 1997] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q3.1: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q3.2', 'Revenue by city for UNITED STATES',
        () => runQ32(db, { customerNation: 'UNITED STATES', supplierNation: 'UNITED STATES', yearRange: [1992, 1997] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q3.2: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q3.3', 'Revenue by city for specific cities',
        () => runQ33(db, { customerCities: ['UNITED KI1', 'UNITED KI5'], supplierCities: ['UNITED KI1', 'UNITED KI5'], yearRange: [1992, 1997] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q3.3: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q3.4', 'Revenue by city/month',
        () => runQ34(db, { customerCities: ['UNITED KI1', 'UNITED KI5'], supplierCities: ['UNITED KI1', 'UNITED KI5'], yearmonth: 'Dec1997' }),
        (r) => r.length,
        iterations
      ))
      log(`  Q3.4: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      // Q4 Flight
      console.log('\nQ4 Flight: Profit Queries')

      benchmarks.push(await benchmarkQuery(
        'Q4.1', 'Profit by nation/year for AMERICA',
        () => runQ41(db, { customerRegion: 'AMERICA', supplierRegion: 'AMERICA', manufacturers: ['MFGR#1', 'MFGR#2'] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q4.1: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q4.2', 'Profit by nation/category',
        () => runQ42(db, { customerRegion: 'AMERICA', supplierRegion: 'AMERICA', manufacturers: ['MFGR#1', 'MFGR#2'], yearRange: [1997, 1998] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q4.2: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      benchmarks.push(await benchmarkQuery(
        'Q4.3', 'Profit by city/brand',
        () => runQ43(db, { customerRegion: 'AMERICA', supplierNation: 'UNITED STATES', categories: ['MFGR#14'], yearRange: [1997, 1998] }),
        (r) => r.length,
        iterations
      ))
      log(`  Q4.3: ${benchmarks[benchmarks.length - 1].avgMs.toFixed(2)}ms avg (${benchmarks[benchmarks.length - 1].rowsReturned} rows)`)

      // Build query results from benchmarks
      // (Re-run queries to get actual results)
      queryResults = await runSSBQueries(db)
    }

    // Calculate summary statistics
    const allAvgTimes = benchmarks.map(b => b.avgMs)
    const q1Times = benchmarks.filter(b => b.name.startsWith('Q1')).map(b => b.avgMs)
    const q2Times = benchmarks.filter(b => b.name.startsWith('Q2')).map(b => b.avgMs)
    const q3Times = benchmarks.filter(b => b.name.startsWith('Q3')).map(b => b.avgMs)
    const q4Times = benchmarks.filter(b => b.name.startsWith('Q4')).map(b => b.avgMs)

    const avg = (arr: number[]) => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0

    const summary = {
      totalQueries: benchmarks.length,
      avgQueryTimeMs: avg(allAvgTimes),
      minQueryTimeMs: allAvgTimes.length > 0 ? Math.min(...allAvgTimes) : 0,
      maxQueryTimeMs: allAvgTimes.length > 0 ? Math.max(...allAvgTimes) : 0,
      q1AvgMs: avg(q1Times),
      q2AvgMs: avg(q2Times),
      q3AvgMs: avg(q3Times),
      q4AvgMs: avg(q4Times),
    }

    const durationMs = performance.now() - startTime

    // Print summary
    console.log()
    console.log('='.repeat(70))
    console.log('Benchmark Summary')
    console.log('='.repeat(70))
    console.log(`Scale Factor:     SF${scaleFactor}`)
    console.log(`Total Duration:   ${(durationMs / 1000).toFixed(1)}s`)
    console.log()
    console.log('Query Performance:')
    console.log(`  Q1 Flight Avg:  ${summary.q1AvgMs.toFixed(2)}ms`)
    console.log(`  Q2 Flight Avg:  ${summary.q2AvgMs.toFixed(2)}ms`)
    console.log(`  Q3 Flight Avg:  ${summary.q3AvgMs.toFixed(2)}ms`)
    console.log(`  Q4 Flight Avg:  ${summary.q4AvgMs.toFixed(2)}ms`)
    console.log()
    console.log(`  Overall Avg:    ${summary.avgQueryTimeMs.toFixed(2)}ms`)
    console.log(`  Min:            ${summary.minQueryTimeMs.toFixed(2)}ms`)
    console.log(`  Max:            ${summary.maxQueryTimeMs.toFixed(2)}ms`)
    console.log('='.repeat(70))

    return {
      scaleFactor,
      generationStats,
      queryResults: queryResults!,
      benchmarks,
      summary,
      durationMs,
    }
  } finally {
    db.dispose()
  }
}

/**
 * Format benchmark results as a report string
 */
export function formatBenchmarkReport(results: SSBBenchmarkResults): string {
  const lines: string[] = []

  lines.push('='.repeat(70))
  lines.push('SSB Benchmark Report')
  lines.push('='.repeat(70))
  lines.push('')
  lines.push(`Scale Factor: SF${results.scaleFactor}`)
  lines.push(`Total Duration: ${(results.durationMs / 1000).toFixed(1)}s`)
  lines.push('')

  if (results.generationStats) {
    lines.push('Data Generation:')
    lines.push(`  LINEORDER: ${results.generationStats.tables.lineorder.toLocaleString()} rows`)
    lines.push(`  CUSTOMER:  ${results.generationStats.tables.customer.toLocaleString()} rows`)
    lines.push(`  SUPPLIER:  ${results.generationStats.tables.supplier.toLocaleString()} rows`)
    lines.push(`  PART:      ${results.generationStats.tables.part.toLocaleString()} rows`)
    lines.push(`  DATE:      ${results.generationStats.tables.date.toLocaleString()} rows`)
    lines.push(`  Duration:  ${(results.generationStats.durationMs / 1000).toFixed(1)}s`)
    lines.push('')
  }

  lines.push('Query Benchmarks:')
  lines.push('-'.repeat(70))
  lines.push('Query    Description                              Avg(ms)  Rows')
  lines.push('-'.repeat(70))

  for (const b of results.benchmarks) {
    const name = b.name.padEnd(8)
    const desc = b.description.slice(0, 40).padEnd(40)
    const avg = b.avgMs.toFixed(2).padStart(8)
    const rows = b.rowsReturned.toString().padStart(6)
    lines.push(`${name} ${desc} ${avg} ${rows}`)
  }

  lines.push('-'.repeat(70))
  lines.push('')
  lines.push('Summary:')
  lines.push(`  Q1 Flight Avg: ${results.summary.q1AvgMs.toFixed(2)}ms`)
  lines.push(`  Q2 Flight Avg: ${results.summary.q2AvgMs.toFixed(2)}ms`)
  lines.push(`  Q3 Flight Avg: ${results.summary.q3AvgMs.toFixed(2)}ms`)
  lines.push(`  Q4 Flight Avg: ${results.summary.q4AvgMs.toFixed(2)}ms`)
  lines.push('')
  lines.push(`  Overall Avg: ${results.summary.avgQueryTimeMs.toFixed(2)}ms`)
  lines.push(`  Min: ${results.summary.minQueryTimeMs.toFixed(2)}ms`)
  lines.push(`  Max: ${results.summary.maxQueryTimeMs.toFixed(2)}ms`)
  lines.push('='.repeat(70))

  return lines.join('\n')
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2)
  const options: SSBBenchmarkOptions = {}

  for (const arg of args) {
    if (arg.startsWith('--scale-factor=')) {
      options.scaleFactor = parseInt(arg.split('=')[1], 10)
    } else if (arg === '--skip-generate') {
      options.skipGenerate = true
    } else if (arg === '--queries-only') {
      options.queriesOnly = true
    } else if (arg.startsWith('--output-dir=')) {
      options.outputDir = arg.split('=')[1]
    } else if (arg.startsWith('--iterations=')) {
      options.iterations = parseInt(arg.split('=')[1], 10)
    } else if (arg === '--verbose') {
      options.verbose = true
    } else if (arg === '--help') {
      console.log(`
SSB Benchmark for ParqueDB

Usage: npx tsx examples/ssb/index.ts [options]

Options:
  --scale-factor=N    Scale factor (1, 10, 100) - default: 1
  --skip-generate     Skip data generation (use existing data)
  --queries-only      Run queries without benchmarking
  --output-dir=PATH   Output directory for data - default: .db/ssb
  --iterations=N      Benchmark iterations - default: 3
  --verbose           Show detailed progress
  --help              Show this help message

Examples:
  npx tsx examples/ssb/index.ts                      # SF1 benchmark
  npx tsx examples/ssb/index.ts --scale-factor=10   # SF10 benchmark
  npx tsx examples/ssb/index.ts --skip-generate     # Re-run on existing data
`)
      process.exit(0)
    }
  }

  try {
    const results = await runSSBBenchmark(options)
    console.log('\nBenchmark complete!')

    // Optionally write results to file
    // await writeFile('.db/ssb-results.json', JSON.stringify(results, null, 2))
  } catch (error) {
    console.error('Benchmark failed:', error)
    process.exit(1)
  }
}

// Run if executed directly
main().catch(console.error)

// =============================================================================
// Exports
// =============================================================================

export {
  // Schema
  ssbSchema,
  ssbFullSchema,
  SSB_SCALE_FACTORS,
  SSB_REGIONS,
  SSB_NATIONS,
  SSB_SEGMENTS,
  type LineOrder,
  type Customer,
  type Supplier,
  type Part,
  type DateDim,
} from './schema'

export {
  // Generator
  generateSSB,
  generateSSBInMemory,
  generateDateDimension,
  generateCustomers,
  generateSuppliers,
  generateParts,
  generateLineOrders,
  type GenerateSSBOptions,
  type GenerateStats,
  type ProgressInfo,
} from './generate'

export {
  // Queries
  runSSBQueries,
  runQ11, runQ12, runQ13,
  runQ21, runQ22, runQ23,
  runQ31, runQ32, runQ33, runQ34,
  runQ41, runQ42, runQ43,
  runQ11WithFindAPI,
  type SSBQueryResults,
  type Q1Result,
  type Q2Result,
  type Q3Result,
  type Q4Result,
  type Q1Params,
  type Q2Params,
  type Q3Params,
  type Q4Params,
} from './queries'
