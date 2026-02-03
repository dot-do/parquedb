#!/usr/bin/env bun

/**
 * Check Dataset Availability
 *
 * Verifies all configured datasets have their Parquet files accessible
 * in the production R2 bucket by making HTTP requests to the API.
 *
 * Usage:
 *   bun scripts/check-datasets.ts
 *   bun scripts/check-datasets.ts --verbose
 *   bun scripts/check-datasets.ts --base-url https://staging.example.com
 *
 * Exit codes:
 *   0 - All datasets available
 *   1 - One or more datasets failed
 */

// =============================================================================
// Types
// =============================================================================

interface Options {
  verbose: boolean
  baseUrl: string
}

interface DatasetConfig {
  name: string
  description: string
  collections: string[]
  prefix: string
}

interface CheckResult {
  success: boolean
  status: number
  error?: string
  elapsed: number
  itemCount?: number
  dataset?: string
  collection?: string
}

interface ApiResponse {
  items?: unknown[]
  api?: {
    returned?: number
  }
}

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_BASE_URL = 'https://api.parquedb.com'
const REQUEST_TIMEOUT_MS = 30000

// Dataset configuration (mirrored from src/worker/datasets.ts)
const DATASETS: Record<string, DatasetConfig> = {
  imdb: {
    name: 'IMDB',
    description: 'Internet Movie Database - Sample titles and names',
    collections: ['titles', 'names'],
    prefix: 'imdb',
  },
  'onet-graph': {
    name: 'O*NET',
    description: 'Occupational Information Network - 1,016 occupations with skills, abilities, knowledge relationships',
    collections: ['occupations', 'skills', 'abilities', 'knowledge'],
    prefix: 'onet-graph',
  },
  unspsc: {
    name: 'UNSPSC',
    description: 'United Nations Standard Products and Services Code - Product taxonomy',
    collections: ['segments', 'families', 'classes', 'commodities'],
    prefix: 'unspsc',
  },
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Parse command line arguments
 */
function parseArgs(): Options {
  const args = process.argv.slice(2)
  const options: Options = {
    verbose: false,
    baseUrl: DEFAULT_BASE_URL,
  }

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === '--verbose' || arg === '-v') {
      options.verbose = true
    } else if (arg === '--base-url' && args[i + 1]) {
      options.baseUrl = args[++i]
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
Usage: bun scripts/check-datasets.ts [options]

Options:
  --verbose, -v       Show detailed output
  --base-url URL      Use custom base URL (default: ${DEFAULT_BASE_URL})
  --help, -h          Show this help message

Exit codes:
  0 - All datasets available
  1 - One or more datasets failed
`)
      process.exit(0)
    }
  }

  return options
}

/**
 * Fetch with timeout
 */
async function fetchWithTimeout(url: string, timeoutMs = REQUEST_TIMEOUT_MS): Promise<Response> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'ParqueDB-Dataset-Checker/1.0',
      },
    })
    return response
  } finally {
    clearTimeout(timeoutId)
  }
}

// =============================================================================
// Check Functions
// =============================================================================

/**
 * Check a single collection endpoint
 */
async function checkCollection(baseUrl: string, datasetId: string, collectionId: string, _verbose: boolean): Promise<CheckResult> {
  const url = `${baseUrl}/datasets/${datasetId}/${collectionId}?limit=1`
  const startTime = Date.now()

  try {
    const response = await fetchWithTimeout(url)
    const elapsed = Date.now() - startTime

    if (!response.ok) {
      return {
        success: false,
        status: response.status,
        error: `HTTP ${response.status}: ${response.statusText}`,
        elapsed,
      }
    }

    const data = await response.json() as ApiResponse

    // Verify response has data
    const itemCount = data.items?.length ?? 0
    const hasData = itemCount > 0 || (data.api?.returned ?? 0) > 0

    if (!hasData) {
      return {
        success: false,
        status: response.status,
        error: 'No data returned (empty collection or missing Parquet file)',
        elapsed,
      }
    }

    return {
      success: true,
      status: response.status,
      itemCount,
      elapsed,
    }
  } catch (error) {
    const elapsed = Date.now() - startTime
    const err = error as Error & { name?: string }
    const errorMessage = err.name === 'AbortError'
      ? `Timeout after ${REQUEST_TIMEOUT_MS}ms`
      : err.message

    return {
      success: false,
      status: 0,
      error: errorMessage,
      elapsed,
    }
  }
}

/**
 * Check all collections for a dataset
 */
async function checkDataset(baseUrl: string, datasetId: string, dataset: DatasetConfig, verbose: boolean): Promise<CheckResult[]> {
  const results: CheckResult[] = []

  for (const collection of dataset.collections) {
    const result = await checkCollection(baseUrl, datasetId, collection, verbose)
    results.push({
      dataset: datasetId,
      collection,
      ...result,
    })
  }

  return results
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const options = parseArgs()
  const { verbose, baseUrl } = options

  console.log('='.repeat(60))
  console.log('ParqueDB Dataset Availability Check')
  console.log('='.repeat(60))
  console.log(`Base URL: ${baseUrl}`)
  console.log(`Datasets: ${Object.keys(DATASETS).length}`)
  console.log(`Collections: ${Object.values(DATASETS).reduce((sum, d) => sum + d.collections.length, 0)}`)
  console.log('')

  const allResults: CheckResult[] = []
  let totalPassed = 0
  let totalFailed = 0

  for (const [datasetId, dataset] of Object.entries(DATASETS)) {
    console.log(`\nChecking ${dataset.name} (${datasetId})...`)

    const results = await checkDataset(baseUrl, datasetId, dataset, verbose)
    allResults.push(...results)

    for (const result of results) {
      if (result.success) {
        totalPassed++
        if (verbose) {
          console.log(`  [OK]  ${result.collection} (${result.elapsed}ms, ${result.itemCount} items)`)
        } else {
          console.log(`  [OK]  ${result.collection}`)
        }
      } else {
        totalFailed++
        console.log(`  [FAIL] ${result.collection}: ${result.error}`)
      }
    }
  }

  // Summary
  console.log('')
  console.log('='.repeat(60))
  console.log('Summary')
  console.log('='.repeat(60))
  console.log(`Passed: ${totalPassed}`)
  console.log(`Failed: ${totalFailed}`)
  console.log(`Total:  ${totalPassed + totalFailed}`)

  if (totalFailed > 0) {
    console.log('')
    console.log('Failed collections:')
    for (const result of allResults) {
      if (!result.success) {
        console.log(`  - ${result.dataset}/${result.collection}: ${result.error}`)
      }
    }
    console.log('')
    console.log('RESULT: FAILED')
    process.exit(1)
  } else {
    console.log('')
    console.log('RESULT: PASSED')
    process.exit(0)
  }
}

main().catch(error => {
  console.error('Unexpected error:', error)
  process.exit(1)
})
