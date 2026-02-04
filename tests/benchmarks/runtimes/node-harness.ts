#!/usr/bin/env tsx
/**
 * Node.js Benchmark Harness
 *
 * Runs ParqueDB benchmarks in Node.js environment using native fetch (undici).
 * Supports CDN, FS, and R2 backends.
 *
 * Usage:
 *   npx tsx tests/benchmarks/runtimes/node-harness.ts [options]
 *
 * Options:
 *   --backends=cdn,fs,r2       Comma-separated backends to test (default: all valid)
 *   --datasets=imdb,onet,...   Comma-separated datasets to test (default: all valid)
 *   --iterations=N             Number of test iterations (default: 10)
 *   --warmup=N                 Warmup iterations (default: 2)
 *   --output=table|json|md     Output format (default: table)
 *   --verbose                  Enable verbose logging
 *   --timeout=N                Request timeout in ms (default: 30000)
 *   --data-dir=PATH            Data directory for FS backend (default: ./data)
 *   --compare                  Compare with previous results if available
 *   --save=PATH                Save results to JSON file
 *
 * Examples:
 *   npx tsx tests/benchmarks/runtimes/node-harness.ts
 *   npx tsx tests/benchmarks/runtimes/node-harness.ts --backends=cdn --datasets=blog,ecommerce
 *   npx tsx tests/benchmarks/runtimes/node-harness.ts --output=json --save=results.json
 *
 * @module runtimes/node-harness
 */

import * as fs from 'node:fs'
import * as path from 'node:path'

import {
  type RuntimeName,
  type BackendName,
  type DatasetName,
  runtimeConfigs,
  getValidCombinations,
  getValidDatasets,
  getValidBackends,
} from '../runtime-configs'

import {
  type BenchmarkSuiteResult,
  type BenchmarkOptions,
  type StorageBackendType,
  runStorageBenchmark,
  formatMs,
  formatBytes,
  printTable,
  printMarkdown,
  printJson,
  DEFAULT_OPTIONS,
} from '../storage-benchmark-runner'

import {
  type UnifiedBenchmarkResult,
  collectRuntimeMetadata,
  formatResultsForConsole,
  formatResultsAsJson,
} from './index'

// =============================================================================
// Types
// =============================================================================

interface NodeHarnessConfig {
  backends: BackendName[]
  datasets: DatasetName[]
  options: BenchmarkOptions
  compare: boolean
  savePath?: string
}

interface PreviousResults {
  timestamp: string
  runtime: RuntimeName
  results: BenchmarkSuiteResult[]
}

// =============================================================================
// CLI Argument Parsing
// =============================================================================

function parseArgs(): NodeHarnessConfig {
  const args = process.argv.slice(2)
  const nodeConfig = runtimeConfigs.find((c) => c.name === 'node')!

  let backends: BackendName[] = nodeConfig.supportedBackends
  let datasets: DatasetName[] = nodeConfig.supportedDatasets
  const options: BenchmarkOptions = { ...DEFAULT_OPTIONS }
  let compare = false
  let savePath: string | undefined

  for (const arg of args) {
    if (arg.startsWith('--backends=')) {
      backends = arg.slice(11).split(',').filter(Boolean) as BackendName[]
    } else if (arg.startsWith('--datasets=')) {
      datasets = arg.slice(11).split(',').filter(Boolean) as DatasetName[]
    } else if (arg.startsWith('--iterations=')) {
      options.iterations = parseInt(arg.slice(13), 10)
    } else if (arg.startsWith('--warmup=')) {
      options.warmup = parseInt(arg.slice(9), 10)
    } else if (arg.startsWith('--output=')) {
      const value = arg.slice(9)
      if (value === 'md') {
        options.output = 'markdown'
      } else if (['table', 'json', 'markdown'].includes(value)) {
        options.output = value as 'table' | 'json' | 'markdown'
      }
    } else if (arg === '--verbose') {
      options.verbose = true
    } else if (arg.startsWith('--timeout=')) {
      options.timeout = parseInt(arg.slice(10), 10)
    } else if (arg.startsWith('--data-dir=')) {
      options.dataDir = arg.slice(11)
    } else if (arg === '--compare') {
      compare = true
    } else if (arg.startsWith('--save=')) {
      savePath = arg.slice(7)
    } else if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }
  }

  // Validate backends
  for (const backend of backends) {
    if (!nodeConfig.supportedBackends.includes(backend)) {
      console.error(
        `Error: Backend "${backend}" not supported in Node.js. Valid: ${nodeConfig.supportedBackends.join(', ')}`
      )
      process.exit(1)
    }
  }

  // Validate datasets
  for (const dataset of datasets) {
    if (!nodeConfig.supportedDatasets.includes(dataset)) {
      console.error(
        `Error: Dataset "${dataset}" not supported in Node.js. Valid: ${nodeConfig.supportedDatasets.join(', ')}`
      )
      process.exit(1)
    }
  }

  return { backends, datasets, options, compare, savePath }
}

function printHelp(): void {
  console.log(`
Node.js Benchmark Harness

Runs ParqueDB benchmarks in Node.js environment using native fetch.

USAGE:
  npx tsx tests/benchmarks/runtimes/node-harness.ts [OPTIONS]

OPTIONS:
  --backends=LIST       Comma-separated backends: cdn, fs, r2 (default: cdn,fs,r2)
  --datasets=LIST       Comma-separated datasets to test (default: onet,unspsc,blog,ecommerce)
  --iterations=N        Number of measured iterations (default: 10)
  --warmup=N            Number of warmup iterations (default: 2)
  --output=FORMAT       Output format: table, json, md (default: table)
  --timeout=MS          Request timeout in milliseconds (default: 30000)
  --data-dir=PATH       Data directory for FS backend (default: ./data)
  --verbose             Enable verbose output
  --compare             Compare with previous results
  --save=PATH           Save results to JSON file
  --help, -h            Show this help message

SUPPORTED IN NODE.JS:
  Backends: cdn, fs, r2
  Datasets: onet, unspsc, blog, ecommerce (IMDB excluded - too large)
  Fetch: Native undici

EXAMPLES:
  # Run all valid combinations
  npx tsx tests/benchmarks/runtimes/node-harness.ts

  # Run only CDN backend
  npx tsx tests/benchmarks/runtimes/node-harness.ts --backends=cdn

  # Run specific datasets with verbose output
  npx tsx tests/benchmarks/runtimes/node-harness.ts --datasets=blog,ecommerce --verbose

  # Save results for comparison
  npx tsx tests/benchmarks/runtimes/node-harness.ts --save=node-results.json

  # Compare with previous run
  npx tsx tests/benchmarks/runtimes/node-harness.ts --compare
`)
}

// =============================================================================
// Benchmark Execution
// =============================================================================

/**
 * Map BackendName to StorageBackendType
 */
function mapBackend(backend: BackendName): StorageBackendType | null {
  switch (backend) {
    case 'cdn':
      return 'cdn'
    case 'fs':
      return 'fs'
    case 'r2':
      return 'r2'
    default:
      return null
  }
}

/**
 * Run benchmarks for all configured backend/dataset combinations
 */
async function runBenchmarks(config: NodeHarnessConfig): Promise<UnifiedBenchmarkResult[]> {
  const results: UnifiedBenchmarkResult[] = []
  const runtimeMetadata = collectRuntimeMetadata()
  const userAgent = `Node.js/${process.versions.node} (${process.platform}; ${process.arch})`

  console.log('='.repeat(70))
  console.log('NODE.JS BENCHMARK HARNESS')
  console.log('='.repeat(70))
  console.log(`Node Version: ${process.versions.node}`)
  console.log(`V8 Version: ${process.versions.v8}`)
  console.log(`Platform: ${process.platform} ${process.arch}`)
  console.log(`Backends: ${config.backends.join(', ')}`)
  console.log(`Datasets: ${config.datasets.join(', ')}`)
  console.log(`Iterations: ${config.options.iterations}`)
  console.log(`Warmup: ${config.options.warmup}`)
  console.log('='.repeat(70))

  for (const backend of config.backends) {
    const storageBackend = mapBackend(backend)
    if (!storageBackend) {
      console.warn(`\nSkipping backend "${backend}" - not implemented`)
      continue
    }

    // Get valid datasets for this backend
    const validDatasets = getValidDatasets('node', backend)
    const datasets = config.datasets.filter((d) => validDatasets.includes(d))

    if (datasets.length === 0) {
      console.warn(`\nSkipping backend "${backend}" - no valid datasets`)
      continue
    }

    console.log(`\n--- Backend: ${backend.toUpperCase()} ---`)
    console.log(`Datasets: ${datasets.join(', ')}`)

    try {
      const suiteResult = await runStorageBenchmark(
        storageBackend,
        datasets,
        config.options
      )

      results.push({
        runtime: 'node',
        userAgent,
        suiteResult,
        runtimeMetadata,
      })
    } catch (error) {
      console.error(`Error running benchmark for backend "${backend}":`, error)
    }
  }

  return results
}

// =============================================================================
// Result Comparison
// =============================================================================

/**
 * Load previous results for comparison
 */
function loadPreviousResults(savePath?: string): PreviousResults | null {
  const resultsPath = savePath ?? './node-benchmark-results.json'

  try {
    if (fs.existsSync(resultsPath)) {
      const data = fs.readFileSync(resultsPath, 'utf-8')
      return JSON.parse(data) as PreviousResults
    }
  } catch (error) {
    console.warn('Could not load previous results:', error)
  }

  return null
}

/**
 * Save results for future comparison
 */
function saveResults(results: UnifiedBenchmarkResult[], savePath: string): void {
  const data: PreviousResults = {
    timestamp: new Date().toISOString(),
    runtime: 'node',
    results: results.map((r) => r.suiteResult),
  }

  fs.writeFileSync(savePath, JSON.stringify(data, null, 2))
  console.log(`\nResults saved to: ${savePath}`)
}

/**
 * Compare current results with previous results
 */
function compareResults(
  current: UnifiedBenchmarkResult[],
  previous: PreviousResults
): void {
  console.log('\n' + '='.repeat(70))
  console.log('COMPARISON WITH PREVIOUS RESULTS')
  console.log('='.repeat(70))
  console.log(`Previous run: ${previous.timestamp}`)
  console.log('')

  for (const currentResult of current) {
    const currentSummary = currentResult.suiteResult.summary

    // Find matching previous result by backend
    const prevSuite = previous.results.find(
      (r) => r.config.backend === currentResult.suiteResult.config.backend
    )

    if (!prevSuite) {
      console.log(`Backend ${currentResult.suiteResult.config.backend}: No previous data`)
      continue
    }

    const prevSummary = prevSuite.summary

    const avgLatencyDiff = currentSummary.avgLatencyMs - prevSummary.avgLatencyMs
    const avgLatencyPct = (avgLatencyDiff / prevSummary.avgLatencyMs) * 100

    const p95LatencyDiff = currentSummary.p95LatencyMs - prevSummary.p95LatencyMs
    const p95LatencyPct = (p95LatencyDiff / prevSummary.p95LatencyMs) * 100

    const passRateCurrent =
      currentSummary.passedPatterns / currentSummary.totalPatterns
    const passRatePrev = prevSummary.passedPatterns / prevSummary.totalPatterns
    const passRateDiff = (passRateCurrent - passRatePrev) * 100

    const backend = currentResult.suiteResult.config.backend.toUpperCase()
    console.log(`${backend}:`)
    console.log(
      `  Avg Latency: ${formatMs(currentSummary.avgLatencyMs)} (${avgLatencyDiff > 0 ? '+' : ''}${avgLatencyPct.toFixed(1)}%)`
    )
    console.log(
      `  P95 Latency: ${formatMs(currentSummary.p95LatencyMs)} (${p95LatencyDiff > 0 ? '+' : ''}${p95LatencyPct.toFixed(1)}%)`
    )
    console.log(
      `  Pass Rate: ${(passRateCurrent * 100).toFixed(1)}% (${passRateDiff > 0 ? '+' : ''}${passRateDiff.toFixed(1)}%)`
    )
    console.log('')
  }
}

// =============================================================================
// Output Formatting
// =============================================================================

/**
 * Print results in the configured format
 */
function printResults(
  results: UnifiedBenchmarkResult[],
  format: 'table' | 'json' | 'markdown'
): void {
  if (format === 'json') {
    console.log(formatResultsAsJson(results))
    return
  }

  for (const result of results) {
    if (format === 'markdown') {
      printMarkdown(result.suiteResult)
    } else {
      printTable(result.suiteResult)
    }
  }
}

// =============================================================================
// Summary Output
// =============================================================================

/**
 * Print summary of all benchmark runs
 */
function printSummary(results: UnifiedBenchmarkResult[]): void {
  console.log('\n' + '='.repeat(70))
  console.log('NODE.JS BENCHMARK SUMMARY')
  console.log('='.repeat(70))

  let totalPatterns = 0
  let totalPassed = 0
  let totalFailed = 0
  let overallAvgLatency = 0
  let overallBytesRead = 0

  for (const result of results) {
    const summary = result.suiteResult.summary
    totalPatterns += summary.totalPatterns
    totalPassed += summary.passedPatterns
    totalFailed += summary.failedPatterns
    overallAvgLatency += summary.avgLatencyMs
    overallBytesRead += summary.totalBytesRead

    console.log(`\n${result.suiteResult.config.backend.toUpperCase()}:`)
    console.log(`  Patterns: ${summary.totalPatterns}`)
    console.log(`  Passed: ${summary.passedPatterns}`)
    console.log(`  Failed: ${summary.failedPatterns}`)
    console.log(`  Avg Latency: ${formatMs(summary.avgLatencyMs)}`)
    console.log(`  P95 Latency: ${formatMs(summary.p95LatencyMs)}`)
    console.log(`  Bytes Read: ${formatBytes(summary.totalBytesRead)}`)
  }

  if (results.length > 1) {
    console.log('\n' + '-'.repeat(70))
    console.log('OVERALL:')
    console.log(`  Total Patterns: ${totalPatterns}`)
    console.log(`  Total Passed: ${totalPassed}`)
    console.log(`  Total Failed: ${totalFailed}`)
    console.log(`  Pass Rate: ${((totalPassed / totalPatterns) * 100).toFixed(1)}%`)
    console.log(`  Avg Latency (all backends): ${formatMs(overallAvgLatency / results.length)}`)
    console.log(`  Total Bytes Read: ${formatBytes(overallBytesRead)}`)
  }

  // Memory usage
  if (process.memoryUsage) {
    const mem = process.memoryUsage()
    console.log('\nMemory Usage:')
    console.log(`  Heap Used: ${formatBytes(mem.heapUsed)}`)
    console.log(`  Heap Total: ${formatBytes(mem.heapTotal)}`)
    console.log(`  External: ${formatBytes(mem.external)}`)
  }
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()

  // Check if running in Node.js
  if (typeof process === 'undefined' || !process.versions?.node) {
    console.error('Error: This harness must be run in Node.js')
    process.exit(1)
  }

  try {
    const results = await runBenchmarks(config)

    // Print results in requested format
    printResults(results, config.options.output)

    // Print summary
    if (config.options.output === 'table') {
      printSummary(results)
    }

    // Compare with previous results if requested
    if (config.compare) {
      const previous = loadPreviousResults(config.savePath)
      if (previous) {
        compareResults(results, previous)
      } else {
        console.log('\nNo previous results found for comparison.')
      }
    }

    // Save results if requested
    if (config.savePath) {
      saveResults(results, config.savePath)
    }

    // Exit with error if any patterns failed
    const totalFailed = results.reduce(
      (sum, r) => sum + r.suiteResult.summary.failedPatterns,
      0
    )
    if (totalFailed > 0) {
      console.error(`\nWARNING: ${totalFailed} pattern(s) failed to meet target latency`)
      process.exit(1)
    }
  } catch (error) {
    console.error('Benchmark failed:', error)
    process.exit(1)
  }
}

// Run if executed directly
const scriptPath = process.argv[1]
if (scriptPath && (scriptPath.endsWith('node-harness.ts') || scriptPath.endsWith('node-harness.js'))) {
  main()
}

// =============================================================================
// Exports
// =============================================================================

export { runBenchmarks, compareResults, saveResults, loadPreviousResults }
export type { NodeHarnessConfig, PreviousResults }
