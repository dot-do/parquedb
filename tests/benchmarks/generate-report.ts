#!/usr/bin/env tsx
/**
 * Generate benchmark reports in multiple formats
 *
 * Formats supported:
 * - Markdown: For GitHub PR comments and documentation
 * - HTML: For dashboard display
 * - JSON: For programmatic access and storage
 *
 * Usage:
 *   npx tsx tests/benchmarks/generate-report.ts [options]
 *
 * Options:
 *   --results=PATH      Path to benchmark results JSON
 *   --comparison=PATH   Path to comparison results JSON (optional)
 *   --format=FORMAT     Output format: markdown, html, json (default: markdown)
 *   --title=TITLE       Report title (default: "Benchmark Report")
 *   --output=PATH       Output file path (default: stdout)
 */

import * as fs from 'node:fs'
import type { BenchmarkSuiteResult, StorageBenchmarkResult } from './storage-benchmark-runner'
import type { ComparisonResult, MetricComparison } from './compare-baseline'

// =============================================================================
// Types
// =============================================================================

export interface ReportOptions {
  title: string
  includeRawData: boolean
  includeCharts: boolean
}

export interface BenchmarkReport {
  title: string
  timestamp: string
  results: BenchmarkSuiteResult
  comparison?: ComparisonResult
}

// =============================================================================
// Utility Functions
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
 * Format bytes with appropriate unit
 */
function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex++
  }

  return `${value.toFixed(2)} ${units[unitIndex]}`
}

/**
 * Format percentage change with sign
 */
function formatChange(changePercent: number): string {
  const sign = changePercent >= 0 ? '+' : ''
  return `${sign}${changePercent.toFixed(1)}%`
}

/**
 * Get status emoji for markdown
 */
function getStatusEmoji(passed: boolean): string {
  return passed ? ':white_check_mark:' : ':x:'
}

/**
 * Get regression status emoji
 */
function getRegressionEmoji(metric: MetricComparison): string {
  if (metric.isRegression) return ':x:'
  if (metric.changePercent < -5) return ':chart_with_upwards_trend:'
  return ':white_check_mark:'
}

// =============================================================================
// Markdown Report Generator
// =============================================================================

/**
 * Generate a markdown report from benchmark results
 */
export function generateMarkdownReport(
  results: BenchmarkSuiteResult,
  comparison?: ComparisonResult,
  options: Partial<ReportOptions> = {}
): string {
  const title = options.title || 'Benchmark Report'
  const lines: string[] = []

  // Header
  lines.push(`# ${title}`)
  lines.push('')
  lines.push(`**Date:** ${results.metadata.completedAt}`)
  lines.push(`**Duration:** ${formatMs(results.metadata.durationMs)}`)
  lines.push(`**Backend:** ${results.config.backend}`)
  lines.push(`**Datasets:** ${results.config.datasets.join(', ')}`)
  lines.push('')

  // Regression Status (if comparison available)
  if (comparison) {
    const statusIcon = comparison.hasRegression ? ':x:' : ':white_check_mark:'
    lines.push('## Regression Status')
    lines.push('')
    lines.push(`${statusIcon} **${comparison.hasRegression ? 'Regression Detected' : 'No Regression'}**`)
    lines.push(`- Severity: ${comparison.severity}`)
    lines.push(`- ${comparison.message}`)
    lines.push('')

    // Comparison metrics
    lines.push('### Comparison with Baseline')
    lines.push('')
    lines.push('| Metric | Baseline | Current | Change | Status |')
    lines.push('|--------|----------|---------|--------|--------|')

    for (const m of comparison.metrics) {
      const baseVal = m.name.includes('Rate')
        ? `${m.baseline.toFixed(1)}%`
        : formatMs(m.baseline)
      const currVal = m.name.includes('Rate')
        ? `${m.current.toFixed(1)}%`
        : formatMs(m.current)
      const status = getRegressionEmoji(m)
      lines.push(`| ${m.name} | ${baseVal} | ${currVal} | ${formatChange(m.changePercent)} | ${status} |`)
    }
    lines.push('')
  }

  // Summary Section
  lines.push('## Summary')
  lines.push('')
  lines.push('| Metric | Value |')
  lines.push('|--------|-------|')
  lines.push(`| Total Patterns | ${results.summary.totalPatterns} |`)
  lines.push(`| Passed | ${results.summary.passedPatterns} |`)
  lines.push(`| Failed | ${results.summary.failedPatterns} |`)
  lines.push(`| Pass Rate | ${((results.summary.passedPatterns / results.summary.totalPatterns) * 100).toFixed(1)}% |`)
  lines.push(`| Avg Latency (P50) | ${formatMs(results.summary.avgLatencyMs)} |`)
  lines.push(`| P95 Latency | ${formatMs(results.summary.p95LatencyMs)} |`)
  lines.push(`| Total Bytes Read | ${formatBytes(results.summary.totalBytesRead)} |`)
  lines.push(`| Total Range Requests | ${results.summary.totalRangeRequests} |`)
  lines.push('')

  // Results by Dataset
  lines.push('## Results by Dataset')
  lines.push('')
  lines.push('| Dataset | Passed | Failed | Pass Rate | Avg Latency |')
  lines.push('|---------|--------|--------|-----------|-------------|')

  for (const [dataset, stats] of Object.entries(results.summary.byDataset)) {
    const total = stats.passed + stats.failed
    const passRate = total > 0 ? ((stats.passed / total) * 100).toFixed(1) : '0'
    const status = stats.failed === 0 ? ':white_check_mark:' : ':warning:'
    lines.push(`| ${status} ${dataset} | ${stats.passed} | ${stats.failed} | ${passRate}% | ${formatMs(stats.avgLatencyMs)} |`)
  }
  lines.push('')

  // Results by Category
  lines.push('## Results by Category')
  lines.push('')
  lines.push('| Category | Passed | Failed | Pass Rate | Avg Latency |')
  lines.push('|----------|--------|--------|-----------|-------------|')

  for (const [category, stats] of Object.entries(results.summary.byCategory)) {
    const total = stats.passed + stats.failed
    const passRate = total > 0 ? ((stats.passed / total) * 100).toFixed(1) : '0'
    lines.push(`| ${category} | ${stats.passed} | ${stats.failed} | ${passRate}% | ${formatMs(stats.avgLatencyMs)} |`)
  }
  lines.push('')

  // Detailed Results
  lines.push('## Detailed Results')
  lines.push('')
  lines.push('<details>')
  lines.push('<summary>Click to expand all pattern results</summary>')
  lines.push('')
  lines.push('| Dataset | Pattern | Category | Target | P50 | P95 | P99 | Status |')
  lines.push('|---------|---------|----------|--------|-----|-----|-----|--------|')

  for (const r of results.results) {
    const status = r.passedTarget ? ':white_check_mark:' : ':x:'
    lines.push(
      `| ${r.dataset} | ${r.pattern} | ${r.category} | ${r.targetMs}ms | ${formatMs(r.latencyMs.p50)} | ${formatMs(r.latencyMs.p95)} | ${formatMs(r.latencyMs.p99)} | ${status} |`
    )
  }

  lines.push('')
  lines.push('</details>')
  lines.push('')

  // Failed Patterns (if any)
  const failedPatterns = results.results.filter((r) => !r.passedTarget)
  if (failedPatterns.length > 0) {
    lines.push('## Failed Patterns')
    lines.push('')
    lines.push('| Dataset | Pattern | Target | Actual (P50) | Difference |')
    lines.push('|---------|---------|--------|--------------|------------|')

    for (const r of failedPatterns) {
      const diff = r.latencyMs.p50 - r.targetMs
      const diffPercent = ((diff / r.targetMs) * 100).toFixed(1)
      lines.push(
        `| ${r.dataset} | ${r.pattern} | ${r.targetMs}ms | ${formatMs(r.latencyMs.p50)} | +${formatMs(diff)} (+${diffPercent}%) |`
      )
    }
    lines.push('')
  }

  // Regressed patterns (if comparison available)
  if (comparison) {
    const regressions = comparison.patterns.filter((p) => p.hasRegression)
    if (regressions.length > 0) {
      lines.push('## Regressed Patterns')
      lines.push('')
      lines.push('| Dataset | Pattern | Metric | Change | Threshold |')
      lines.push('|---------|---------|--------|--------|-----------|')

      for (const p of regressions) {
        for (const m of p.metrics.filter((m) => m.isRegression)) {
          lines.push(
            `| ${p.dataset} | ${p.pattern} | ${m.name} | ${formatChange(m.changePercent)} | >${m.threshold}% |`
          )
        }
      }
      lines.push('')
    }
  }

  // Footer
  lines.push('---')
  lines.push('')
  lines.push(`Generated by ParqueDB Benchmark Suite v${results.metadata.runnerVersion}`)

  return lines.join('\n')
}

// =============================================================================
// HTML Report Generator
// =============================================================================

/**
 * Generate an HTML report from benchmark results
 */
export function generateHtmlReport(
  results: BenchmarkSuiteResult,
  comparison?: ComparisonResult,
  options: Partial<ReportOptions> = {}
): string {
  const title = options.title || 'Benchmark Report'

  const regressionStatus = comparison
    ? comparison.hasRegression
      ? '<span class="status-fail">Regression Detected</span>'
      : '<span class="status-pass">No Regression</span>'
    : ''

  const summaryRows = `
    <tr><td>Total Patterns</td><td>${results.summary.totalPatterns}</td></tr>
    <tr><td>Passed</td><td>${results.summary.passedPatterns}</td></tr>
    <tr><td>Failed</td><td>${results.summary.failedPatterns}</td></tr>
    <tr><td>Pass Rate</td><td>${((results.summary.passedPatterns / results.summary.totalPatterns) * 100).toFixed(1)}%</td></tr>
    <tr><td>Avg Latency (P50)</td><td>${formatMs(results.summary.avgLatencyMs)}</td></tr>
    <tr><td>P95 Latency</td><td>${formatMs(results.summary.p95LatencyMs)}</td></tr>
    <tr><td>Total Bytes Read</td><td>${formatBytes(results.summary.totalBytesRead)}</td></tr>
  `

  const datasetRows = Object.entries(results.summary.byDataset)
    .map(([dataset, stats]) => {
      const total = stats.passed + stats.failed
      const passRate = total > 0 ? ((stats.passed / total) * 100).toFixed(1) : '0'
      return `<tr>
        <td>${dataset}</td>
        <td>${stats.passed}</td>
        <td>${stats.failed}</td>
        <td>${passRate}%</td>
        <td>${formatMs(stats.avgLatencyMs)}</td>
      </tr>`
    })
    .join('\n')

  const patternRows = results.results
    .map((r) => {
      const statusClass = r.passedTarget ? 'status-pass' : 'status-fail'
      const status = r.passedTarget ? 'PASS' : 'FAIL'
      return `<tr>
        <td>${r.dataset}</td>
        <td>${r.pattern}</td>
        <td>${r.category}</td>
        <td>${r.targetMs}ms</td>
        <td>${formatMs(r.latencyMs.p50)}</td>
        <td>${formatMs(r.latencyMs.p95)}</td>
        <td>${formatMs(r.latencyMs.p99)}</td>
        <td class="${statusClass}">${status}</td>
      </tr>`
    })
    .join('\n')

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${title}</title>
  <style>
    :root {
      --bg-color: #ffffff;
      --text-color: #1a1a1a;
      --border-color: #e0e0e0;
      --header-bg: #f5f5f5;
      --pass-color: #22c55e;
      --fail-color: #ef4444;
      --warn-color: #f59e0b;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --bg-color: #1a1a1a;
        --text-color: #e0e0e0;
        --border-color: #333333;
        --header-bg: #2a2a2a;
      }
    }
    * { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
      background: var(--bg-color);
      color: var(--text-color);
      max-width: 1200px;
      margin: 0 auto;
      padding: 2rem;
      line-height: 1.6;
    }
    h1, h2, h3 { margin-top: 2rem; margin-bottom: 1rem; }
    h1 { border-bottom: 2px solid var(--border-color); padding-bottom: 0.5rem; }
    table {
      width: 100%;
      border-collapse: collapse;
      margin: 1rem 0;
    }
    th, td {
      padding: 0.75rem;
      text-align: left;
      border: 1px solid var(--border-color);
    }
    th { background: var(--header-bg); font-weight: 600; }
    tr:hover { background: var(--header-bg); }
    .meta { color: #666; font-size: 0.9rem; margin-bottom: 2rem; }
    .status-pass { color: var(--pass-color); font-weight: 600; }
    .status-fail { color: var(--fail-color); font-weight: 600; }
    .status-warn { color: var(--warn-color); font-weight: 600; }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 1rem;
      margin: 1rem 0;
    }
    .summary-card {
      background: var(--header-bg);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 1rem;
    }
    .summary-card h3 { margin-top: 0; font-size: 0.9rem; color: #666; }
    .summary-card .value { font-size: 2rem; font-weight: 600; }
    .footer {
      margin-top: 3rem;
      padding-top: 1rem;
      border-top: 1px solid var(--border-color);
      color: #666;
      font-size: 0.8rem;
    }
  </style>
</head>
<body>
  <h1>${title}</h1>

  <div class="meta">
    <p>
      <strong>Date:</strong> ${results.metadata.completedAt} |
      <strong>Duration:</strong> ${formatMs(results.metadata.durationMs)} |
      <strong>Backend:</strong> ${results.config.backend}
    </p>
    ${comparison ? `<p><strong>Regression Status:</strong> ${regressionStatus}</p>` : ''}
  </div>

  <h2>Summary</h2>
  <div class="summary-grid">
    <div class="summary-card">
      <h3>Pass Rate</h3>
      <div class="value">${((results.summary.passedPatterns / results.summary.totalPatterns) * 100).toFixed(1)}%</div>
    </div>
    <div class="summary-card">
      <h3>Avg Latency (P50)</h3>
      <div class="value">${formatMs(results.summary.avgLatencyMs)}</div>
    </div>
    <div class="summary-card">
      <h3>P95 Latency</h3>
      <div class="value">${formatMs(results.summary.p95LatencyMs)}</div>
    </div>
    <div class="summary-card">
      <h3>Total Bytes Read</h3>
      <div class="value">${formatBytes(results.summary.totalBytesRead)}</div>
    </div>
  </div>

  <table>
    <thead>
      <tr><th>Metric</th><th>Value</th></tr>
    </thead>
    <tbody>
      ${summaryRows}
    </tbody>
  </table>

  <h2>Results by Dataset</h2>
  <table>
    <thead>
      <tr>
        <th>Dataset</th>
        <th>Passed</th>
        <th>Failed</th>
        <th>Pass Rate</th>
        <th>Avg Latency</th>
      </tr>
    </thead>
    <tbody>
      ${datasetRows}
    </tbody>
  </table>

  <h2>Detailed Results</h2>
  <table>
    <thead>
      <tr>
        <th>Dataset</th>
        <th>Pattern</th>
        <th>Category</th>
        <th>Target</th>
        <th>P50</th>
        <th>P95</th>
        <th>P99</th>
        <th>Status</th>
      </tr>
    </thead>
    <tbody>
      ${patternRows}
    </tbody>
  </table>

  <div class="footer">
    Generated by ParqueDB Benchmark Suite v${results.metadata.runnerVersion}
  </div>
</body>
</html>`
}

// =============================================================================
// JSON Report Generator
// =============================================================================

/**
 * Generate a JSON report from benchmark results
 */
export function generateJsonReport(
  results: BenchmarkSuiteResult,
  comparison?: ComparisonResult,
  options: Partial<ReportOptions> = {}
): object {
  return {
    title: options.title || 'Benchmark Report',
    generatedAt: new Date().toISOString(),
    results: {
      config: results.config,
      metadata: results.metadata,
      summary: results.summary,
      patterns: results.results.map((r) => ({
        dataset: r.dataset,
        pattern: r.pattern,
        category: r.category,
        targetMs: r.targetMs,
        latency: {
          p50: r.latencyMs.p50,
          p95: r.latencyMs.p95,
          p99: r.latencyMs.p99,
          mean: r.latencyMs.mean,
          min: r.latencyMs.min,
          max: r.latencyMs.max,
        },
        passed: r.passedTarget,
        successCount: r.successCount,
        failureCount: r.failureCount,
      })),
    },
    comparison: comparison
      ? {
          hasRegression: comparison.hasRegression,
          severity: comparison.severity,
          message: comparison.message,
          metrics: comparison.metrics.map((m) => ({
            name: m.name,
            baseline: m.baseline,
            current: m.current,
            changePercent: m.changePercent,
            isRegression: m.isRegression,
          })),
          regressedPatterns: comparison.patterns
            .filter((p) => p.hasRegression)
            .map((p) => ({
              dataset: p.dataset,
              pattern: p.pattern,
              regressions: p.metrics
                .filter((m) => m.isRegression)
                .map((m) => ({
                  metric: m.name,
                  changePercent: m.changePercent,
                  threshold: m.threshold,
                })),
            })),
        }
      : undefined,
  }
}

// =============================================================================
// CLI
// =============================================================================

interface CLIArgs {
  resultsPath: string
  comparisonPath?: string
  format: 'markdown' | 'html' | 'json'
  title: string
  outputPath?: string
}

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2)
  let resultsPath = ''
  let comparisonPath: string | undefined
  let format: 'markdown' | 'html' | 'json' = 'markdown'
  let title = 'Benchmark Report'
  let outputPath: string | undefined

  for (const arg of args) {
    if (arg.startsWith('--results=')) {
      resultsPath = arg.slice(10)
    } else if (arg.startsWith('--comparison=')) {
      comparisonPath = arg.slice(13)
    } else if (arg.startsWith('--format=')) {
      const value = arg.slice(9)
      if (value === 'markdown' || value === 'html' || value === 'json') {
        format = value
      }
    } else if (arg.startsWith('--title=')) {
      title = arg.slice(8)
    } else if (arg.startsWith('--output=')) {
      outputPath = arg.slice(9)
    } else if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }
  }

  if (!resultsPath) {
    resultsPath = './benchmark-results.json'
  }

  return { resultsPath, comparisonPath, format, title, outputPath }
}

function printHelp(): void {
  console.log(`
Generate Report Script

Generates benchmark reports in multiple formats.

USAGE:
  npx tsx tests/benchmarks/generate-report.ts [OPTIONS]

OPTIONS:
  --results=PATH      Path to benchmark results JSON (required)
  --comparison=PATH   Path to comparison results JSON (optional)
  --format=FORMAT     Output format: markdown, html, json (default: markdown)
  --title=TITLE       Report title (default: "Benchmark Report")
  --output=PATH       Output file path (default: stdout)
  --help, -h          Show this help message

EXAMPLES:
  # Generate markdown report to stdout
  npx tsx tests/benchmarks/generate-report.ts --results=./results.json

  # Generate HTML report to file
  npx tsx tests/benchmarks/generate-report.ts --results=./results.json --format=html --output=report.html

  # Include comparison data
  npx tsx tests/benchmarks/generate-report.ts --results=./results.json --comparison=./comparison.json
`)
}

async function main(): Promise<void> {
  const { resultsPath, comparisonPath, format, title, outputPath } = parseArgs()

  // Load results
  if (!fs.existsSync(resultsPath)) {
    console.error(`Results file not found: ${resultsPath}`)
    process.exit(1)
  }

  let results: BenchmarkSuiteResult
  try {
    results = JSON.parse(fs.readFileSync(resultsPath, 'utf8'))
  } catch (error) {
    console.error(`Failed to parse results file: ${error}`)
    process.exit(1)
  }

  // Load comparison (optional)
  let comparison: ComparisonResult | undefined
  if (comparisonPath && fs.existsSync(comparisonPath)) {
    try {
      comparison = JSON.parse(fs.readFileSync(comparisonPath, 'utf8'))
    } catch (error) {
      console.error(`Warning: Failed to parse comparison file: ${error}`)
    }
  }

  // Generate report
  let output: string
  const options = { title }

  switch (format) {
    case 'html':
      output = generateHtmlReport(results, comparison, options)
      break
    case 'json':
      output = JSON.stringify(generateJsonReport(results, comparison, options), null, 2)
      break
    default:
      output = generateMarkdownReport(results, comparison, options)
  }

  // Output
  if (outputPath) {
    fs.writeFileSync(outputPath, output)
    console.error(`Report written to: ${outputPath}`)
  } else {
    console.log(output)
  }
}

// Run if executed directly
if (process.argv[1]?.includes('generate-report')) {
  main()
}
