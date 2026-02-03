#!/usr/bin/env npx tsx
/**
 * E2E Benchmark CLI
 *
 * Usage:
 *   npx tsx benchmarks/run.ts [options]
 *
 * Options:
 *   --url, -u         Base URL of the deployed worker (default: https://parque.do)
 *   --cold-iterations Number of cold start measurements (default: 5)
 *   --cold-delay      Seconds to wait between cold start tests (default: 65)
 *   --warm-iterations Number of warm request measurements (default: 20)
 *   --output, -o      Output file path for JSON results
 *   --verbose, -v     Enable verbose logging
 *   --help, -h        Show help message
 *
 * Examples:
 *   npx tsx benchmarks/run.ts
 *   npx tsx benchmarks/run.ts --url https://parque.do --output results.json
 *   npx tsx benchmarks/run.ts -c 3 -w 50 --verbose
 */

import { writeFileSync } from 'node:fs'
import { runBenchmarks, parseArgs } from './runner'

function showHelp(): void {
  console.log(`
ParqueDB E2E Benchmark Suite

Runs comprehensive benchmarks against a deployed ParqueDB worker to measure:
- Cold start latency (time for first request after isolate teardown)
- Warm request latency (subsequent request times)
- Cache hit vs miss performance

Usage:
  npx tsx benchmarks/run.ts [options]

Options:
  --url, -u <url>         Base URL of the deployed worker
                          Default: https://parque.do

  --cold-iterations <n>   Number of cold start measurements
                          Default: 5

  --cold-delay <seconds>  Seconds to wait between cold start tests
                          Default: 65 (to ensure isolate teardown)

  --warm-iterations <n>   Number of warm request measurements
                          Default: 20

  --output, -o <path>     Output file path for JSON results
                          If not specified, results are only printed

  --verbose, -v           Enable verbose logging

  --help, -h              Show this help message

Examples:
  # Run with defaults against parque.do
  npx tsx benchmarks/run.ts

  # Run against a custom URL with JSON output
  npx tsx benchmarks/run.ts --url https://my-worker.example.com -o results.json

  # Quick test with fewer iterations
  npx tsx benchmarks/run.ts -c 2 -w 10 --cold-delay 30

  # Full verbose benchmark
  npx tsx benchmarks/run.ts -c 10 -w 100 --verbose -o full-results.json
`)
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  // Check for help flag
  if (args.includes('--help') || args.includes('-h')) {
    showHelp()
    process.exit(0)
  }

  try {
    const config = parseArgs(args)
    const report = await runBenchmarks(config)

    // Write output file if specified
    if (config.outputPath) {
      writeFileSync(config.outputPath, JSON.stringify(report, null, 2))
      console.log(`\nResults written to: ${config.outputPath}`)
    }

    // Exit with error code if there were errors
    if (report.errors.length > 0) {
      console.log('\nErrors encountered:')
      for (const error of report.errors) {
        console.log(`  [${error.phase}] ${error.endpoint}: ${error.error}`)
      }
      process.exit(1)
    }

    process.exit(0)
  } catch (error) {
    console.error('\nFatal error:', error instanceof Error ? error.message : error)
    process.exit(1)
  }
}

main()
