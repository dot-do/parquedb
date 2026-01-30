#!/usr/bin/env npx tsx
/**
 * Load All Datasets
 *
 * Loads all available datasets sequentially.
 *
 * Usage:
 *   npx tsx scripts/load-all.ts [--output-dir ./data]
 */

import { spawn } from 'node:child_process'
import { parseArgs } from 'node:util'

interface DatasetInfo {
  name: string
  outputDir: string
  skip?: boolean
  skipReason?: string
}

const DATASETS: DatasetInfo[] = [
  { name: 'onet', outputDir: './data/onet' },
  { name: 'imdb', outputDir: './data/imdb' },
  { name: 'wiktionary', outputDir: './data/wiktionary', skip: true, skipReason: 'Not yet implemented' },
  { name: 'unspsc', outputDir: './data/unspsc', skip: true, skipReason: 'Requires manual download' },
  { name: 'wikidata', outputDir: './data/wikidata', skip: true, skipReason: 'Very large (~100GB)' },
  { name: 'commoncrawl', outputDir: './data/commoncrawl', skip: true, skipReason: 'Very large (TBs)' },
]

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  const minutes = Math.floor(ms / 60000)
  const seconds = ((ms % 60000) / 1000).toFixed(0)
  return `${minutes}m ${seconds}s`
}

async function runLoader(dataset: string, outputDir: string): Promise<{ success: boolean; duration: number }> {
  const startTime = Date.now()

  return new Promise((resolve) => {
    const child = spawn('npx', ['tsx', 'scripts/load-data.ts', '--dataset', dataset, '--output', outputDir], {
      stdio: 'inherit',
      shell: true,
    })

    child.on('close', (code) => {
      resolve({
        success: code === 0,
        duration: Date.now() - startTime,
      })
    })

    child.on('error', () => {
      resolve({
        success: false,
        duration: Date.now() - startTime,
      })
    })
  })
}

async function main() {
  const { values } = parseArgs({
    options: {
      'output-dir': {
        type: 'string',
        default: './data',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
      'include-large': {
        type: 'boolean',
        default: false,
      },
    },
  })

  if (values.help) {
    console.log(`
Load All Datasets

Usage:
  npx tsx scripts/load-all.ts [options]

Options:
  --output-dir     Base output directory (default: ./data)
  --include-large  Include large datasets (wikidata, commoncrawl)
  -h, --help       Show this help

The following datasets will be loaded:
${DATASETS.map(d => `  - ${d.name}${d.skip ? ` (skipped: ${d.skipReason})` : ''}`).join('\n')}
`)
    return
  }

  const baseOutputDir = values['output-dir'] || './data'
  const includeLarge = values['include-large']
  const startTime = Date.now()

  console.log(`
================================================================================
Loading All Datasets
================================================================================
Base output directory: ${baseOutputDir}
Include large datasets: ${includeLarge ? 'yes' : 'no'}
================================================================================
`)

  const results: Array<{ name: string; success: boolean; duration: number; skipped: boolean; skipReason?: string }> = []

  for (const dataset of DATASETS) {
    // Skip large datasets unless explicitly included
    if (dataset.skip && !includeLarge) {
      console.log(`\n[SKIP] ${dataset.name}: ${dataset.skipReason}`)
      results.push({
        name: dataset.name,
        success: false,
        duration: 0,
        skipped: true,
        skipReason: dataset.skipReason,
      })
      continue
    }

    console.log(`\n[LOADING] ${dataset.name}...`)
    const outputDir = dataset.outputDir.replace('./data', baseOutputDir)
    const result = await runLoader(dataset.name, outputDir)

    results.push({
      name: dataset.name,
      success: result.success,
      duration: result.duration,
      skipped: false,
    })

    if (result.success) {
      console.log(`[DONE] ${dataset.name} completed in ${formatDuration(result.duration)}`)
    } else {
      console.log(`[FAILED] ${dataset.name} failed after ${formatDuration(result.duration)}`)
    }
  }

  const totalDuration = Date.now() - startTime
  const successful = results.filter(r => r.success && !r.skipped).length
  const failed = results.filter(r => !r.success && !r.skipped).length
  const skipped = results.filter(r => r.skipped).length

  console.log(`
================================================================================
Summary
================================================================================
Total duration: ${formatDuration(totalDuration)}
Successful:     ${successful}
Failed:         ${failed}
Skipped:        ${skipped}

Results:
${results
  .map(r => {
    if (r.skipped) {
      return `  ${r.name.padEnd(15)} SKIPPED  (${r.skipReason})`
    }
    return `  ${r.name.padEnd(15)} ${r.success ? 'OK' : 'FAILED'}     ${formatDuration(r.duration)}`
  })
  .join('\n')}
================================================================================
`)

  // Exit with error if any non-skipped dataset failed
  if (failed > 0) {
    process.exit(1)
  }
}

main().catch(console.error)
