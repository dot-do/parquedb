/**
 * TPC-H Benchmark Example for ParqueDB
 *
 * Demonstrates loading and querying the TPC-H dataset with ParqueDB.
 *
 * Features:
 * - Snowflake schema: LINEITEM, ORDERS, CUSTOMER, PART, SUPPLIER, PARTSUPP, NATION, REGION
 * - Bidirectional relationships for graph traversal
 * - SQL interface for complex analytical queries
 * - Benchmark runner for performance testing
 *
 * @example
 * ```bash
 * # Generate data and run benchmark
 * npx tsx examples/tpch/index.ts
 *
 * # Generate larger dataset
 * npx tsx examples/tpch/index.ts --scale-factor=1
 *
 * # Run only queries (assumes data exists)
 * npx tsx examples/tpch/index.ts --skip-generate
 * ```
 */

import { DB, type DBInstance } from '../../src/db'
import { FsBackend } from '../../src/storage'
import { tpchSchema, tpchFullSchema, TPCH_SCALE_FACTORS } from './schema'
import { generateTPCH, type GenerateStats, type GenerateTPCHOptions } from './generate'
import {
  runTPCHQueries,
  runTPCHBenchmark,
  formatBenchmarkReport,
  runQ1,
  runQ3,
  runQ5,
  runQ6,
  runQ10,
  runQ12,
  runQ14,
  runQ19,
  type TPCHQueryResults,
  type BenchmarkSummary,
} from './queries'

// =============================================================================
// Schema Exports
// =============================================================================

export {
  // Type definitions
  type Region,
  type Nation,
  type Supplier,
  type Part,
  type PartSupp,
  type Customer,
  type Order,
  type LineItem,

  // Schema definitions
  tpchSchema,
  tpchFullSchema,

  // Constants
  TPCH_SCALE_FACTORS,
  TPCH_REGIONS,
  TPCH_NATIONS,
  TPCH_SEGMENTS,
  TPCH_PRIORITIES,
  TPCH_SHIP_MODES,
  TPCH_SHIP_INSTRUCTIONS,
  TPCH_CONTAINERS,
  TPCH_TYPE_SYLLABLES,
  TPCH_NAME_SYLLABLES,
  TPCH_DATE_RANGE,
} from './schema'

// =============================================================================
// Generator Exports
// =============================================================================

export {
  generateTPCH,
  generateTPCHInMemory,
  generateRegions,
  generateNations,
  generateSuppliers,
  generateParts,
  generatePartSupps,
  generateCustomers,
  generateOrders,
  generateLineItems,
  type GenerateTPCHOptions,
  type GenerateStats,
  type ProgressInfo,
} from './generate'

// =============================================================================
// Query Exports
// =============================================================================

export {
  // Query runners
  runTPCHQueries,
  runTPCHBenchmark,
  formatBenchmarkReport,

  // Individual queries
  runQ1,
  runQ3,
  runQ5,
  runQ6,
  runQ10,
  runQ12,
  runQ14,
  runQ19,

  // Types
  type Q1Result,
  type Q3Result,
  type Q5Result,
  type Q6Result,
  type Q10Result,
  type Q12Result,
  type Q14Result,
  type Q19Result,
  type TPCHQueryResults,
  type BenchmarkResult,
  type BenchmarkSummary,

  // Parameter types
  type Q1Params,
  type Q3Params,
  type Q5Params,
  type Q6Params,
  type Q10Params,
  type Q12Params,
  type Q14Params,
  type Q19Params,
} from './queries'

// =============================================================================
// Main Entry Point
// =============================================================================

export interface RunTPCHOptions {
  /** Scale factor (0.01, 0.1, 1, 10) */
  scaleFactor?: number
  /** Output directory for generated data */
  outputDir?: string
  /** Skip data generation (use existing data) */
  skipGenerate?: boolean
  /** Number of benchmark iterations */
  iterations?: number
  /** Number of warmup iterations */
  warmupIterations?: number
  /** Verbose output */
  verbose?: boolean
  /** Seed for reproducible generation */
  seed?: number
}

export interface RunTPCHResults {
  generateStats?: GenerateStats
  queryResults: TPCHQueryResults
  benchmark: BenchmarkSummary
}

/**
 * Run the complete TPC-H example: generate data, run queries, and benchmark
 */
export async function runTPCH(options: RunTPCHOptions = {}): Promise<RunTPCHResults> {
  const {
    scaleFactor = 0.01,
    outputDir = '.db/tpch',
    skipGenerate = false,
    iterations = 3,
    warmupIterations = 1,
    verbose = true,
    seed = 42,
  } = options

  let generateStats: GenerateStats | undefined

  // 1. Generate data (unless skipped)
  if (!skipGenerate) {
    console.log('='.repeat(70))
    console.log(`TPC-H Data Generation (SF=${scaleFactor})`)
    console.log('='.repeat(70))
    console.log('')

    generateStats = await generateTPCH({
      scaleFactor,
      outputDir,
      seed,
      onProgress: verbose
        ? (info) => {
            if (info.percentage % 10 === 0 || info.percentage === 100) {
              console.log(`  ${info.table}: ${info.percentage}%`)
            }
          }
        : undefined,
    })

    console.log('\nGeneration Summary:')
    console.log(`  Region: ${generateStats.tables.region.toLocaleString()} rows`)
    console.log(`  Nation: ${generateStats.tables.nation.toLocaleString()} rows`)
    console.log(`  Supplier: ${generateStats.tables.supplier.toLocaleString()} rows`)
    console.log(`  Part: ${generateStats.tables.part.toLocaleString()} rows`)
    console.log(`  PartSupp: ${generateStats.tables.partsupp.toLocaleString()} rows`)
    console.log(`  Customer: ${generateStats.tables.customer.toLocaleString()} rows`)
    console.log(`  Orders: ${generateStats.tables.orders.toLocaleString()} rows`)
    console.log(`  LineItem: ${generateStats.tables.lineitem.toLocaleString()} rows`)
    console.log(`  Duration: ${(generateStats.durationMs / 1000).toFixed(1)}s`)
    console.log('')
  }

  // 2. Create database connection
  const db = DB(tpchSchema, {
    storage: new FsBackend(outputDir),
  })

  try {
    // 3. Run queries
    console.log('='.repeat(70))
    console.log('TPC-H Query Suite')
    console.log('='.repeat(70))
    console.log('')

    const queryResults = await runTPCHQueries(db)

    // 4. Run benchmark
    console.log('')
    console.log('='.repeat(70))
    console.log('TPC-H Benchmark')
    console.log('='.repeat(70))
    console.log('')

    const benchmark = await runTPCHBenchmark(db, {
      iterations,
      warmupIterations,
      verbose,
    })

    // 5. Print results
    console.log('')
    console.log(formatBenchmarkReport(benchmark))

    return {
      generateStats,
      queryResults,
      benchmark,
    }
  } finally {
    db.dispose()
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2)
  const options: RunTPCHOptions = {}

  for (const arg of args) {
    if (arg.startsWith('--scale-factor=')) {
      options.scaleFactor = parseFloat(arg.split('=')[1])
    } else if (arg.startsWith('--output=')) {
      options.outputDir = arg.split('=')[1]
    } else if (arg === '--skip-generate') {
      options.skipGenerate = true
    } else if (arg.startsWith('--iterations=')) {
      options.iterations = parseInt(arg.split('=')[1])
    } else if (arg.startsWith('--warmup=')) {
      options.warmupIterations = parseInt(arg.split('=')[1])
    } else if (arg === '--quiet') {
      options.verbose = false
    } else if (arg.startsWith('--seed=')) {
      options.seed = parseInt(arg.split('=')[1])
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
TPC-H Benchmark for ParqueDB

Usage: npx tsx examples/tpch/index.ts [options]

Options:
  --scale-factor=<n>  Scale factor (0.01, 0.1, 1, 10). Default: 0.01
  --output=<dir>      Output directory. Default: .db/tpch
  --skip-generate     Skip data generation (use existing data)
  --iterations=<n>    Number of benchmark iterations. Default: 3
  --warmup=<n>        Number of warmup iterations. Default: 1
  --quiet             Suppress progress output
  --seed=<n>          Random seed for reproducible generation. Default: 42
  --help, -h          Show this help message

Examples:
  # Generate SF0.01 and run benchmark
  npx tsx examples/tpch/index.ts

  # Generate SF1 dataset
  npx tsx examples/tpch/index.ts --scale-factor=1

  # Run benchmark on existing data
  npx tsx examples/tpch/index.ts --skip-generate
`)
      process.exit(0)
    }
  }

  try {
    await runTPCH(options)
  } catch (error) {
    console.error('Error:', error)
    process.exit(1)
  }
}

// Run if executed directly
const isMainModule = process.argv[1]?.endsWith('/tpch/index.ts') ||
  process.argv[1]?.endsWith('/examples/tpch/index.ts')

if (isMainModule) {
  main().catch(console.error)
}
