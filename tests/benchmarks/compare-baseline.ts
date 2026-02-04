#!/usr/bin/env tsx
/**
 * Compare current benchmark results against baseline
 *
 * Detects regressions based on configurable thresholds:
 * - P50 latency: > 20% increase triggers regression
 * - P95 latency: > 25% increase triggers regression
 * - P99 latency: > 30% increase triggers regression
 * - Throughput: > 15% decrease triggers regression
 *
 * Exit codes:
 * - 0: No regression detected
 * - 1: Regression detected or error occurred
 *
 * Usage:
 *   npx tsx tests/benchmarks/compare-baseline.ts [options]
 *
 * Options:
 *   --results=PATH      Path to current benchmark results JSON
 *   --baseline=PATH     Path to baseline benchmark results JSON
 *   --output=FORMAT     Output format: json, table, markdown (default: table)
 *   --thresholds=JSON   Custom threshold JSON (optional)
 *   --verbose           Enable verbose output
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import type { BenchmarkSuiteResult, StorageBenchmarkResult } from './storage-benchmark-runner'

// =============================================================================
// Types
// =============================================================================

/**
 * Regression thresholds (percentage change that triggers regression)
 * Positive = increase is bad, Negative = decrease is bad
 */
export interface RegressionThresholds {
  latencyP50: number
  latencyP95: number
  latencyP99: number
  throughput: number
  passRate: number
}

/**
 * Single metric comparison result
 */
export interface MetricComparison {
  name: string
  baseline: number
  current: number
  change: number
  changePercent: number
  threshold: number
  isRegression: boolean
}

/**
 * Pattern-level comparison result
 */
export interface PatternComparison {
  dataset: string
  pattern: string
  category: string
  metrics: MetricComparison[]
  hasRegression: boolean
}

/**
 * Overall comparison result
 */
export interface ComparisonResult {
  baselineTimestamp: string
  currentTimestamp: string
  baselineVersion: string
  currentVersion: string
  thresholds: RegressionThresholds
  patterns: PatternComparison[]
  summary: {
    totalPatterns: number
    regressedPatterns: number
    improvedPatterns: number
    unchangedPatterns: number
  }
  metrics: MetricComparison[]
  hasRegression: boolean
  severity: 'none' | 'minor' | 'moderate' | 'severe'
  message: string
}

// =============================================================================
// Default Configuration
// =============================================================================

export const DEFAULT_THRESHOLDS: RegressionThresholds = {
  latencyP50: 20, // 20% increase in P50 latency
  latencyP95: 25, // 25% increase in P95 latency
  latencyP99: 30, // 30% increase in P99 latency
  throughput: -15, // 15% decrease in throughput (negative = decrease is bad)
  passRate: -10, // 10% decrease in pass rate
}

// =============================================================================
// Comparison Logic
// =============================================================================

/**
 * Compare a single metric
 */
function compareMetric(
  name: string,
  baseline: number,
  current: number,
  threshold: number
): MetricComparison {
  const change = current - baseline
  const changePercent = baseline !== 0 ? (change / baseline) * 100 : current > 0 ? 100 : 0

  // For negative thresholds (like throughput), decrease is bad
  // For positive thresholds (like latency), increase is bad
  const isRegression =
    threshold < 0 ? changePercent < threshold : changePercent > threshold

  return {
    name,
    baseline,
    current,
    change,
    changePercent,
    threshold,
    isRegression,
  }
}

/**
 * Compare two pattern results
 */
function comparePattern(
  baseline: StorageBenchmarkResult,
  current: StorageBenchmarkResult,
  thresholds: RegressionThresholds
): PatternComparison {
  const metrics: MetricComparison[] = []

  // Compare P50 latency
  metrics.push(
    compareMetric(
      'P50 Latency',
      baseline.latencyMs.p50,
      current.latencyMs.p50,
      thresholds.latencyP50
    )
  )

  // Compare P95 latency
  metrics.push(
    compareMetric(
      'P95 Latency',
      baseline.latencyMs.p95,
      current.latencyMs.p95,
      thresholds.latencyP95
    )
  )

  // Compare P99 latency
  metrics.push(
    compareMetric(
      'P99 Latency',
      baseline.latencyMs.p99,
      current.latencyMs.p99,
      thresholds.latencyP99
    )
  )

  const hasRegression = metrics.some((m) => m.isRegression)

  return {
    dataset: current.dataset,
    pattern: current.pattern,
    category: current.category,
    metrics,
    hasRegression,
  }
}

/**
 * Compare two benchmark suite results
 */
export function compareBenchmarks(
  baseline: BenchmarkSuiteResult,
  current: BenchmarkSuiteResult,
  thresholds: RegressionThresholds = DEFAULT_THRESHOLDS
): ComparisonResult {
  const patterns: PatternComparison[] = []

  // Build lookup map for baseline results
  const baselineMap = new Map<string, StorageBenchmarkResult>()
  for (const result of baseline.results) {
    const key = `${result.dataset}:${result.pattern}`
    baselineMap.set(key, result)
  }

  // Compare each current pattern against baseline
  for (const currentResult of current.results) {
    const key = `${currentResult.dataset}:${currentResult.pattern}`
    const baselineResult = baselineMap.get(key)

    if (baselineResult) {
      patterns.push(comparePattern(baselineResult, currentResult, thresholds))
    }
  }

  // Calculate summary metrics
  const regressedPatterns = patterns.filter((p) => p.hasRegression).length
  const improvedPatterns = patterns.filter(
    (p) => !p.hasRegression && p.metrics.some((m) => m.changePercent < -10)
  ).length
  const unchangedPatterns = patterns.length - regressedPatterns - improvedPatterns

  // Compare overall summary metrics
  const overallMetrics: MetricComparison[] = []

  // Average latency comparison
  overallMetrics.push(
    compareMetric(
      'Avg Latency (P50)',
      baseline.summary.avgLatencyMs,
      current.summary.avgLatencyMs,
      thresholds.latencyP50
    )
  )

  // P95 latency comparison
  overallMetrics.push(
    compareMetric(
      'P95 Latency',
      baseline.summary.p95LatencyMs,
      current.summary.p95LatencyMs,
      thresholds.latencyP95
    )
  )

  // Pass rate comparison
  const baselinePassRate =
    baseline.summary.passedPatterns / baseline.summary.totalPatterns
  const currentPassRate =
    current.summary.passedPatterns / current.summary.totalPatterns
  overallMetrics.push(
    compareMetric('Pass Rate', baselinePassRate * 100, currentPassRate * 100, thresholds.passRate)
  )

  // Determine overall regression status
  const hasRegression = regressedPatterns > 0 || overallMetrics.some((m) => m.isRegression)

  let severity: ComparisonResult['severity'] = 'none'
  const regressionCount = regressedPatterns + overallMetrics.filter((m) => m.isRegression).length
  if (regressionCount >= 5) severity = 'severe'
  else if (regressionCount >= 3) severity = 'moderate'
  else if (regressionCount >= 1) severity = 'minor'

  const message = hasRegression
    ? `${regressedPatterns} pattern(s) and ${overallMetrics.filter((m) => m.isRegression).length} summary metric(s) show regression`
    : 'All metrics within acceptable thresholds'

  return {
    baselineTimestamp: baseline.metadata.completedAt,
    currentTimestamp: current.metadata.completedAt,
    baselineVersion: baseline.metadata.runnerVersion,
    currentVersion: current.metadata.runnerVersion,
    thresholds,
    patterns,
    summary: {
      totalPatterns: patterns.length,
      regressedPatterns,
      improvedPatterns,
      unchangedPatterns,
    },
    metrics: overallMetrics,
    hasRegression,
    severity,
    message,
  }
}

// =============================================================================
// Output Formatters
// =============================================================================

/**
 * Format milliseconds with appropriate precision
 */
function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`
  if (ms < 10) return `${ms.toFixed(2)}ms`
  if (ms < 100) return `${ms.toFixed(1)}ms`
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

/**
 * Format change as string with sign
 */
function formatChange(changePercent: number): string {
  const sign = changePercent >= 0 ? '+' : ''
  return `${sign}${changePercent.toFixed(1)}%`
}

/**
 * Print comparison as table
 */
function printTable(result: ComparisonResult): void {
  console.log('\n' + '='.repeat(80))
  console.log('BENCHMARK COMPARISON')
  console.log('='.repeat(80))

  console.log(`\nBaseline: ${result.baselineTimestamp}`)
  console.log(`Current:  ${result.currentTimestamp}`)
  console.log(`Status:   ${result.hasRegression ? 'REGRESSION DETECTED' : 'No regression'}`)
  console.log(`Severity: ${result.severity}`)

  // Overall metrics
  console.log('\n--- Summary Metrics ---')
  const metricWidths = [20, 12, 12, 10, 10]
  console.log(
    ['Metric', 'Baseline', 'Current', 'Change', 'Status']
      .map((h, i) => h.padEnd(metricWidths[i]!))
      .join(' ')
  )
  console.log(metricWidths.map((w) => '-'.repeat(w)).join(' '))

  for (const m of result.metrics) {
    const status = m.isRegression ? 'REGRESS' : m.changePercent < -5 ? 'IMPROVE' : 'OK'
    const row = [
      m.name,
      m.name.includes('Rate') ? `${m.baseline.toFixed(1)}%` : formatMs(m.baseline),
      m.name.includes('Rate') ? `${m.current.toFixed(1)}%` : formatMs(m.current),
      formatChange(m.changePercent),
      status,
    ]
    console.log(row.map((c, i) => c.padEnd(metricWidths[i]!)).join(' '))
  }

  // Pattern-level regressions
  const regressions = result.patterns.filter((p) => p.hasRegression)
  if (regressions.length > 0) {
    console.log('\n--- Regressed Patterns ---')
    const patternWidths = [15, 30, 15, 10, 10]
    console.log(
      ['Dataset', 'Pattern', 'Metric', 'Change', 'Threshold']
        .map((h, i) => h.padEnd(patternWidths[i]!))
        .join(' ')
    )
    console.log(patternWidths.map((w) => '-'.repeat(w)).join(' '))

    for (const p of regressions) {
      for (const m of p.metrics.filter((m) => m.isRegression)) {
        const row = [
          p.dataset.slice(0, 14),
          p.pattern.slice(0, 29),
          m.name,
          formatChange(m.changePercent),
          `>${m.threshold}%`,
        ]
        console.log(row.map((c, i) => c.padEnd(patternWidths[i]!)).join(' '))
      }
    }
  }

  // Summary
  console.log('\n--- Summary ---')
  console.log(`Total Patterns:    ${result.summary.totalPatterns}`)
  console.log(`Regressed:         ${result.summary.regressedPatterns}`)
  console.log(`Improved:          ${result.summary.improvedPatterns}`)
  console.log(`Unchanged:         ${result.summary.unchangedPatterns}`)
  console.log(`\nMessage: ${result.message}`)
  console.log('='.repeat(80))
}

/**
 * Print comparison as markdown
 */
function printMarkdown(result: ComparisonResult): void {
  console.log('## Benchmark Comparison\n')
  console.log(`**Baseline:** ${result.baselineTimestamp}`)
  console.log(`**Current:** ${result.currentTimestamp}`)
  console.log(`**Status:** ${result.hasRegression ? ':x: REGRESSION DETECTED' : ':white_check_mark: No regression'}`)
  console.log(`**Severity:** ${result.severity}\n`)

  console.log('### Summary Metrics\n')
  console.log('| Metric | Baseline | Current | Change | Status |')
  console.log('|--------|----------|---------|--------|--------|')

  for (const m of result.metrics) {
    const status = m.isRegression ? ':x: Regress' : m.changePercent < -5 ? ':chart_with_upwards_trend: Improve' : ':white_check_mark: OK'
    const baseVal = m.name.includes('Rate') ? `${m.baseline.toFixed(1)}%` : formatMs(m.baseline)
    const currVal = m.name.includes('Rate') ? `${m.current.toFixed(1)}%` : formatMs(m.current)
    console.log(`| ${m.name} | ${baseVal} | ${currVal} | ${formatChange(m.changePercent)} | ${status} |`)
  }

  const regressions = result.patterns.filter((p) => p.hasRegression)
  if (regressions.length > 0) {
    console.log('\n### Regressed Patterns\n')
    console.log('| Dataset | Pattern | Metric | Change | Threshold |')
    console.log('|---------|---------|--------|--------|-----------|')

    for (const p of regressions) {
      for (const m of p.metrics.filter((m) => m.isRegression)) {
        console.log(`| ${p.dataset} | ${p.pattern} | ${m.name} | ${formatChange(m.changePercent)} | >${m.threshold}% |`)
      }
    }
  }

  console.log('\n### Summary\n')
  console.log(`- Total Patterns: ${result.summary.totalPatterns}`)
  console.log(`- Regressed: ${result.summary.regressedPatterns}`)
  console.log(`- Improved: ${result.summary.improvedPatterns}`)
  console.log(`- Unchanged: ${result.summary.unchangedPatterns}`)
  console.log(`\n**${result.message}**`)
}

/**
 * Print comparison as JSON
 */
function printJson(result: ComparisonResult): void {
  console.log(JSON.stringify(result, null, 2))
}

// =============================================================================
// CLI
// =============================================================================

interface CLIArgs {
  resultsPath: string
  baselinePath: string
  output: 'json' | 'table' | 'markdown'
  thresholds: RegressionThresholds
  verbose: boolean
}

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2)
  let resultsPath = ''
  let baselinePath = ''
  let output: 'json' | 'table' | 'markdown' = 'table'
  let thresholds = DEFAULT_THRESHOLDS
  let verbose = false

  for (const arg of args) {
    if (arg.startsWith('--results=')) {
      resultsPath = arg.slice(10)
    } else if (arg.startsWith('--baseline=')) {
      baselinePath = arg.slice(11)
    } else if (arg.startsWith('--output=')) {
      const value = arg.slice(9)
      if (value === 'json' || value === 'table' || value === 'markdown') {
        output = value
      }
    } else if (arg.startsWith('--thresholds=')) {
      try {
        thresholds = { ...DEFAULT_THRESHOLDS, ...JSON.parse(arg.slice(13)) }
      } catch {
        console.error('Invalid thresholds JSON')
        process.exit(1)
      }
    } else if (arg === '--verbose') {
      verbose = true
    } else if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }
  }

  // Default paths
  if (!resultsPath) {
    resultsPath = './benchmark-results.json'
  }
  if (!baselinePath) {
    baselinePath = path.join(__dirname, 'baseline', 'production.json')
  }

  return { resultsPath, baselinePath, output, thresholds, verbose }
}

function printHelp(): void {
  console.log(`
Compare Baseline Script

Compares current benchmark results against a baseline and detects regressions.

USAGE:
  npx tsx tests/benchmarks/compare-baseline.ts [OPTIONS]

OPTIONS:
  --results=PATH      Path to current benchmark results JSON (default: ./benchmark-results.json)
  --baseline=PATH     Path to baseline benchmark results JSON (default: ./tests/benchmarks/baseline/production.json)
  --output=FORMAT     Output format: json, table, markdown (default: table)
  --thresholds=JSON   Custom thresholds JSON (optional)
  --verbose           Enable verbose output
  --help, -h          Show this help message

EXIT CODES:
  0 - No regression detected
  1 - Regression detected or error occurred

THRESHOLDS:
  Default thresholds (percentage change that triggers regression):
  - latencyP50: 20%   (increase)
  - latencyP95: 25%   (increase)
  - latencyP99: 30%   (increase)
  - throughput: -15%  (decrease)
  - passRate: -10%    (decrease)

EXAMPLES:
  # Compare with default baseline
  npx tsx tests/benchmarks/compare-baseline.ts --results=./results.json

  # Compare with custom baseline and output markdown
  npx tsx tests/benchmarks/compare-baseline.ts --results=./results.json --baseline=./old-baseline.json --output=markdown

  # Use custom thresholds
  npx tsx tests/benchmarks/compare-baseline.ts --thresholds='{"latencyP50": 10, "latencyP95": 15}'
`)
}

async function main(): Promise<void> {
  const { resultsPath, baselinePath, output, thresholds, verbose } = parseArgs()

  if (verbose) {
    console.error(`Results path: ${resultsPath}`)
    console.error(`Baseline path: ${baselinePath}`)
  }

  // Load current results
  if (!fs.existsSync(resultsPath)) {
    console.error(`Results file not found: ${resultsPath}`)
    process.exit(1)
  }

  let current: BenchmarkSuiteResult
  try {
    current = JSON.parse(fs.readFileSync(resultsPath, 'utf8'))
  } catch (error) {
    console.error(`Failed to parse results file: ${error}`)
    process.exit(1)
  }

  // Load baseline
  if (!fs.existsSync(baselinePath)) {
    console.error(`Baseline file not found: ${baselinePath}`)
    console.error('Run benchmarks with --update-baseline to create initial baseline')
    // No baseline = no regression comparison possible
    // Output current results as-is
    if (output === 'json') {
      console.log(JSON.stringify({ hasRegression: false, message: 'No baseline available for comparison', current }))
    } else {
      console.log('No baseline available for comparison')
    }
    process.exit(0)
  }

  let baseline: BenchmarkSuiteResult
  try {
    baseline = JSON.parse(fs.readFileSync(baselinePath, 'utf8'))
  } catch (error) {
    console.error(`Failed to parse baseline file: ${error}`)
    process.exit(1)
  }

  // Compare
  const result = compareBenchmarks(baseline, current, thresholds)

  // Output
  switch (output) {
    case 'json':
      printJson(result)
      break
    case 'markdown':
      printMarkdown(result)
      break
    default:
      printTable(result)
  }

  // Exit with appropriate code
  if (result.hasRegression) {
    process.exit(1)
  }
}

// Run if executed directly
if (process.argv[1]?.includes('compare-baseline')) {
  main()
}

// Export for programmatic use
export { compareBenchmarks as detectRegressions }
