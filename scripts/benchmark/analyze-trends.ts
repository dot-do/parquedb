#!/usr/bin/env npx tsx
/**
 * Benchmark Trend Analysis Script
 *
 * Downloads recent benchmark results from R2 or local storage,
 * calculates trends (improving/degrading), and generates reports.
 *
 * Usage:
 *   npx tsx scripts/benchmark/analyze-trends.ts [options]
 *
 * Options:
 *   --environment=<name>     Environment to analyze (default: production)
 *   --days=<n>               Number of days to analyze (default: 7)
 *   --limit=<n>              Maximum results to fetch (default: 100)
 *   --output=<format>        Output format: markdown, json, csv (default: markdown)
 *   --output-file=<path>     Write output to file instead of stdout
 *   --r2-url=<url>           R2 base URL for fetching results
 *   --base-path=<path>       Local base path for file-based storage
 *   --branch=<name>          Filter by git branch
 *   --show-data-points       Show individual data points in output
 *   --threshold=<n>          Percentage threshold for trend detection (default: 5)
 *
 * Environment Variables:
 *   R2_BASE_URL              Base URL for R2 bucket
 *   R2_API_TOKEN             API token for R2 access
 *   BENCHMARK_BASE_PATH      Local base path for results
 *
 * Examples:
 *   npx tsx scripts/benchmark/analyze-trends.ts --environment=staging --days=14
 *   npx tsx scripts/benchmark/analyze-trends.ts --output=json --output-file=report.json
 *   npx tsx scripts/benchmark/analyze-trends.ts --r2-url=https://benchmarks.example.com
 */

import {
  createTrendStore,
  buildTrendSummary,
  type TrendStore,
  type TrendStoreConfig,
  type TrendSummary,
  type TrendSeries,
  type TrendDirection,
  type StoredBenchmarkResult,
} from '../../src/observability/benchmarks/trend-store'

// =============================================================================
// Configuration
// =============================================================================

interface AnalyzeConfig {
  environment: string
  days: number
  limit: number
  output: 'markdown' | 'json' | 'csv'
  outputFile?: string | undefined
  r2Url?: string | undefined
  r2ApiToken?: string | undefined
  basePath?: string | undefined
  branch?: string | undefined
  showDataPoints: boolean
  threshold: number
}

const DEFAULT_CONFIG: AnalyzeConfig = {
  environment: 'production',
  days: 7,
  limit: 100,
  output: 'markdown',
  showDataPoints: false,
  threshold: 5,
}

// =============================================================================
// CLI Argument Parsing
// =============================================================================

function parseArgs(): AnalyzeConfig {
  const args = process.argv.slice(2)
  const config: AnalyzeConfig = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg.startsWith('--environment=')) {
      config.environment = arg.slice(14)
    } else if (arg.startsWith('--days=')) {
      config.days = parseInt(arg.slice(7), 10)
    } else if (arg.startsWith('--limit=')) {
      config.limit = parseInt(arg.slice(8), 10)
    } else if (arg.startsWith('--output=')) {
      config.output = arg.slice(9) as 'markdown' | 'json' | 'csv'
    } else if (arg.startsWith('--output-file=')) {
      config.outputFile = arg.slice(14)
    } else if (arg.startsWith('--r2-url=')) {
      config.r2Url = arg.slice(9)
    } else if (arg.startsWith('--base-path=')) {
      config.basePath = arg.slice(12)
    } else if (arg.startsWith('--branch=')) {
      config.branch = arg.slice(9)
    } else if (arg === '--show-data-points') {
      config.showDataPoints = true
    } else if (arg.startsWith('--threshold=')) {
      config.threshold = parseInt(arg.slice(12), 10)
    } else if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }
  }

  // Check environment variables
  if (!config.r2Url && process.env.R2_BASE_URL) {
    config.r2Url = process.env.R2_BASE_URL
  }
  if (!config.r2ApiToken && process.env.R2_API_TOKEN) {
    config.r2ApiToken = process.env.R2_API_TOKEN
  }
  if (!config.basePath && process.env.BENCHMARK_BASE_PATH) {
    config.basePath = process.env.BENCHMARK_BASE_PATH
  }

  return config
}

function printHelp(): void {
  console.log(`
Benchmark Trend Analysis

Usage:
  npx tsx scripts/benchmark/analyze-trends.ts [options]

Options:
  --environment=<name>     Environment to analyze (default: production)
  --days=<n>               Number of days to analyze (default: 7)
  --limit=<n>              Maximum results to fetch (default: 100)
  --output=<format>        Output format: markdown, json, csv (default: markdown)
  --output-file=<path>     Write output to file instead of stdout
  --r2-url=<url>           R2 base URL for fetching results
  --base-path=<path>       Local base path for file-based storage
  --branch=<name>          Filter by git branch
  --show-data-points       Show individual data points in output
  --threshold=<n>          Percentage threshold for trend detection (default: 5)
  --help, -h               Show this help message

Environment Variables:
  R2_BASE_URL              Base URL for R2 bucket
  R2_API_TOKEN             API token for R2 access
  BENCHMARK_BASE_PATH      Local base path for results

Examples:
  npx tsx scripts/benchmark/analyze-trends.ts --environment=staging --days=14
  npx tsx scripts/benchmark/analyze-trends.ts --output=json --output-file=report.json
  npx tsx scripts/benchmark/analyze-trends.ts --r2-url=https://benchmarks.example.com
`)
}

// =============================================================================
// Formatting Helpers
// =============================================================================

function formatMs(ms: number): string {
  return `${ms.toFixed(1)}ms`
}

function formatPercent(value: number): string {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(1)}%`
}

function formatDirection(direction: TrendDirection): string {
  switch (direction) {
    case 'improving':
      return 'Improving'
    case 'degrading':
      return 'Degrading'
    case 'stable':
      return 'Stable'
  }
}

function formatDirectionEmoji(direction: TrendDirection): string {
  switch (direction) {
    case 'improving':
      return 'Improving'
    case 'degrading':
      return 'Degrading'
    case 'stable':
      return 'Stable'
  }
}

function formatRiskLevel(risk: 'low' | 'medium' | 'high'): string {
  switch (risk) {
    case 'low':
      return 'Low'
    case 'medium':
      return 'Medium'
    case 'high':
      return 'High'
  }
}

// =============================================================================
// Report Generators
// =============================================================================

function generateMarkdownReport(
  summary: TrendSummary,
  results: StoredBenchmarkResult[],
  config: AnalyzeConfig
): string {
  const lines: string[] = []

  // Header
  lines.push('# Benchmark Trend Report')
  lines.push('')
  lines.push(`**Environment:** ${summary.environment}`)
  lines.push(`**Period:** ${new Date(summary.period.start).toLocaleDateString()} - ${new Date(summary.period.end).toLocaleDateString()}`)
  lines.push(`**Data Points:** ${summary.period.dataPointCount}`)
  lines.push(`**Generated:** ${new Date().toISOString()}`)
  lines.push('')

  // Overall Status
  lines.push('## Overall Status')
  lines.push('')
  lines.push(`| Metric | Value |`)
  lines.push(`|--------|-------|`)
  lines.push(`| Overall Direction | ${formatDirectionEmoji(summary.overallDirection)} |`)
  lines.push(`| Regression Risk | ${formatRiskLevel(summary.regressionRisk)} |`)
  lines.push('')

  // Metric Trends
  lines.push('## Metric Trends')
  lines.push('')
  lines.push('| Metric | Current | Previous | Change | Direction |')
  lines.push('|--------|---------|----------|--------|-----------|')

  const metrics = [
    { name: 'Latency P50', series: summary.metrics.latencyP50, unit: 'ms' },
    { name: 'Latency P95', series: summary.metrics.latencyP95, unit: 'ms' },
    { name: 'Latency P99', series: summary.metrics.latencyP99, unit: 'ms' },
    { name: 'Throughput', series: summary.metrics.throughput, unit: 'ops/s' },
    { name: 'Cold Start Overhead', series: summary.metrics.coldStartOverhead, unit: '%' },
    { name: 'Cache Speedup', series: summary.metrics.cacheSpeedup, unit: 'x' },
  ]

  for (const { name, series, unit } of metrics) {
    if (!series) {
      lines.push(`| ${name} | N/A | N/A | N/A | N/A |`)
      continue
    }

    const currentStr = unit === 'ms' ? formatMs(series.stats.current) :
                       unit === '%' ? `${series.stats.current.toFixed(1)}%` :
                       unit === 'x' ? `${series.stats.current.toFixed(2)}x` :
                       series.stats.current.toFixed(1)
    const previousStr = unit === 'ms' ? formatMs(series.stats.previous) :
                        unit === '%' ? `${series.stats.previous.toFixed(1)}%` :
                        unit === 'x' ? `${series.stats.previous.toFixed(2)}x` :
                        series.stats.previous.toFixed(1)

    lines.push(`| ${name} | ${currentStr} | ${previousStr} | ${formatPercent(series.changePercent)} | ${formatDirection(series.direction)} |`)
  }
  lines.push('')

  // Statistics Summary
  lines.push('## Statistics Summary')
  lines.push('')
  lines.push('| Metric | Min | Max | Mean | Std Dev |')
  lines.push('|--------|-----|-----|------|---------|')

  for (const { name, series, unit } of metrics) {
    if (!series) continue

    const formatValue = (v: number) =>
      unit === 'ms' ? formatMs(v) :
      unit === '%' ? `${v.toFixed(1)}%` :
      unit === 'x' ? `${v.toFixed(2)}x` :
      v.toFixed(1)

    lines.push(`| ${name} | ${formatValue(series.stats.min)} | ${formatValue(series.stats.max)} | ${formatValue(series.stats.mean)} | ${formatValue(series.stats.stdDev)} |`)
  }
  lines.push('')

  // Recent Runs
  lines.push('## Recent Runs')
  lines.push('')
  lines.push('| Date | Run ID | Commit | Branch | P50 | P95 |')
  lines.push('|------|--------|--------|--------|-----|-----|')

  const recentRuns = results.slice(0, 10)
  for (const run of recentRuns) {
    const date = new Date(run.timestamp).toLocaleDateString()
    const commit = run.commitSha?.substring(0, 7) ?? 'N/A'
    const branch = run.branch ?? 'N/A'
    const p50 = formatMs(run.results.summary.avgLatencyMs)
    const p95 = formatMs(run.results.summary.p95LatencyMs)
    lines.push(`| ${date} | ${run.runId.substring(0, 12)} | ${commit} | ${branch} | ${p50} | ${p95} |`)
  }
  lines.push('')

  // Data Points (if requested)
  if (config.showDataPoints && summary.metrics.latencyP50) {
    lines.push('## Latency P50 Data Points')
    lines.push('')
    lines.push('| Timestamp | Value | Commit |')
    lines.push('|-----------|-------|--------|')

    for (const dp of summary.metrics.latencyP50.dataPoints) {
      const ts = new Date(dp.timestamp).toISOString()
      const commit = dp.commitSha?.substring(0, 7) ?? 'N/A'
      lines.push(`| ${ts} | ${formatMs(dp.value)} | ${commit} |`)
    }
    lines.push('')
  }

  // Recommendations
  lines.push('## Recommendations')
  lines.push('')

  if (summary.regressionRisk === 'high') {
    lines.push('**Action Required:** High regression risk detected.')
    lines.push('')
    lines.push('- Investigate recent commits that may have caused performance degradation')
    lines.push('- Run detailed profiling on affected code paths')
    lines.push('- Consider reverting problematic changes')
  } else if (summary.regressionRisk === 'medium') {
    lines.push('**Monitoring Recommended:** Medium regression risk detected.')
    lines.push('')
    lines.push('- Continue monitoring trends over the next few days')
    lines.push('- Review any recent infrastructure or dependency changes')
  } else if (summary.overallDirection === 'improving') {
    lines.push('**Good Progress:** Performance is improving.')
    lines.push('')
    lines.push('- Consider updating baseline with recent results')
    lines.push('- Document optimization techniques that worked')
  } else {
    lines.push('**Stable Performance:** No significant changes detected.')
    lines.push('')
    lines.push('- Continue regular monitoring')
    lines.push('- Current performance is within expected ranges')
  }
  lines.push('')

  // Footer
  lines.push('---')
  lines.push('*Generated by ParqueDB Benchmark Trend Analysis*')

  return lines.join('\n')
}

function generateJsonReport(
  summary: TrendSummary,
  results: StoredBenchmarkResult[],
  _config: AnalyzeConfig
): string {
  const report = {
    generatedAt: new Date().toISOString(),
    summary,
    recentRuns: results.slice(0, 20).map(r => ({
      runId: r.runId,
      timestamp: r.timestamp,
      commitSha: r.commitSha,
      branch: r.branch,
      summary: {
        avgLatencyMs: r.results.summary.avgLatencyMs,
        p95LatencyMs: r.results.summary.p95LatencyMs,
        throughput: r.results.summary.overallThroughput,
        passedTests: r.results.summary.passedTests,
        failedTests: r.results.summary.failedTests,
      },
    })),
  }

  return JSON.stringify(report, null, 2)
}

function generateCsvReport(
  summary: TrendSummary,
  results: StoredBenchmarkResult[],
  _config: AnalyzeConfig
): string {
  const lines: string[] = []

  // Header
  lines.push('timestamp,runId,commitSha,branch,avgLatencyMs,p95LatencyMs,throughput,passedTests,failedTests,coldStartOverheadMs,cacheSpeedup')

  // Data rows
  for (const result of results) {
    const row = [
      result.timestamp,
      result.runId,
      result.commitSha ?? '',
      result.branch ?? '',
      result.results.summary.avgLatencyMs.toFixed(2),
      result.results.summary.p95LatencyMs.toFixed(2),
      result.results.summary.overallThroughput.toFixed(2),
      result.results.summary.passedTests.toString(),
      result.results.summary.failedTests.toString(),
      result.results.coldStart?.overheadMs.toFixed(2) ?? '',
      result.results.cachePerformance?.speedup.toFixed(2) ?? result.results.summary.cacheSpeedup?.toFixed(2) ?? '',
    ]
    lines.push(row.join(','))
  }

  return lines.join('\n')
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()

  console.error(`Analyzing benchmark trends for environment: ${config.environment}`)
  console.error(`Period: last ${config.days} days, limit: ${config.limit}`)

  // Create trend store
  const storeConfig: TrendStoreConfig = {}
  if (config.r2Url) {
    storeConfig.r2BaseUrl = config.r2Url
    if (config.r2ApiToken) {
      // HttpTrendStore will use this in headers
    }
  } else if (config.basePath) {
    storeConfig.basePath = config.basePath
  } else {
    // Default to local file storage
    storeConfig.basePath = './benchmark-results'
  }

  const store = createTrendStore(storeConfig)

  // Fetch results
  console.error('Fetching benchmark results...')
  let results = await store.getRecentResults(config.environment, config.days, config.limit)

  if (config.branch) {
    results = results.filter(r => r.branch === config.branch)
    console.error(`Filtered to ${results.length} results for branch: ${config.branch}`)
  }

  if (results.length === 0) {
    console.error('No benchmark results found for the specified criteria.')
    console.error('')
    console.error('Tips:')
    console.error('  - Check that the environment name is correct')
    console.error('  - Try increasing --days to search a longer period')
    console.error('  - Ensure results have been uploaded to the storage location')
    process.exit(1)
  }

  console.error(`Found ${results.length} benchmark results`)

  // Build trend summary
  console.error('Calculating trends...')
  const granularity = config.days <= 7 ? 'daily' : config.days <= 30 ? 'weekly' : 'monthly'
  const summary = buildTrendSummary(results, config.environment, granularity)

  if (!summary) {
    console.error('Failed to build trend summary - insufficient data')
    process.exit(1)
  }

  // Generate report
  let output: string
  switch (config.output) {
    case 'json':
      output = generateJsonReport(summary, results, config)
      break
    case 'csv':
      output = generateCsvReport(summary, results, config)
      break
    case 'markdown':
    default:
      output = generateMarkdownReport(summary, results, config)
  }

  // Write output
  if (config.outputFile) {
    const fs = await import('fs/promises')
    const dir = config.outputFile.substring(0, config.outputFile.lastIndexOf('/'))
    if (dir) {
      await fs.mkdir(dir, { recursive: true })
    }
    await fs.writeFile(config.outputFile, output)
    console.error(`Report written to: ${config.outputFile}`)
  } else {
    console.log(output)
  }

  // Exit with error code if high regression risk
  if (summary.regressionRisk === 'high') {
    console.error('\nWARNING: High regression risk detected!')
    process.exit(1)
  }
}

// Run if executed directly
main().catch(error => {
  console.error('Error:', error)
  process.exit(1)
})
