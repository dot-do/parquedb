#!/usr/bin/env npx tsx
/**
 * Benchmark Storage Modes
 *
 * Measures read performance across all 4 storage modes:
 * - Full scan (read all rows)
 * - Row reconstruction time (JSON.parse for data column)
 *
 * Usage:
 *   npx tsx scripts/benchmark-storage-modes.ts
 *   npx tsx scripts/benchmark-storage-modes.ts --dataset=imdb
 *   npx tsx scripts/benchmark-storage-modes.ts --iterations=10
 */

import { existsSync } from 'node:fs'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parquetRead, parquetMetadataAsync, toJson } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import type { StorageMode } from './lib/storage-modes'
import { formatBytes, formatNumber } from './lib/storage-modes'

// =============================================================================
// Configuration
// =============================================================================

const BASE_DIR = 'data-v3'
const ALL_MODES: StorageMode[] = ['columnar-only', 'columnar-row', 'row-only', 'row-index']
const DEFAULT_ITERATIONS = 3

interface BenchmarkResult {
  mode: StorageMode
  dataset: string
  collection: string
  operation: string
  avgMs: number
  minMs: number
  maxMs: number
  rowsProcessed: number
  rowsPerSec: number
  fileSize: number
}

// =============================================================================
// Utilities
// =============================================================================

async function readParquetFile(path: string): Promise<{ data: any[]; rowCount: number }> {
  const buffer = await fs.readFile(path)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const data: any[] = []

  await parquetRead({
    file: arrayBuffer,
    compressors,
    onComplete: (rows) => {
      const json = toJson(rows)
      for (const row of json) {
        data.push(row)
      }
    },
  })

  return { data, rowCount: data.length }
}

async function timeOperation<T>(
  iterations: number,
  fn: () => Promise<T>
): Promise<{ result: T; avgMs: number; minMs: number; maxMs: number }> {
  const times: number[] = []
  let result: T = undefined as T

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    result = await fn()
    times.push(performance.now() - start)
  }

  return {
    result,
    avgMs: times.reduce((a, b) => a + b, 0) / times.length,
    minMs: Math.min(...times),
    maxMs: Math.max(...times),
  }
}

// =============================================================================
// Benchmarks
// =============================================================================

async function benchmarkFullScan(
  path: string,
  mode: StorageMode,
  dataset: string,
  collection: string,
  iterations: number,
  fileSize: number
): Promise<BenchmarkResult> {
  const { result, avgMs, minMs, maxMs } = await timeOperation(
    iterations,
    async () => readParquetFile(path)
  )

  const rowsProcessed = result.rowCount

  return {
    mode,
    dataset,
    collection,
    operation: 'full_scan',
    avgMs: Math.round(avgMs * 100) / 100,
    minMs: Math.round(minMs * 100) / 100,
    maxMs: Math.round(maxMs * 100) / 100,
    rowsProcessed,
    rowsPerSec: Math.round(rowsProcessed / (avgMs / 1000)),
    fileSize,
  }
}

async function benchmarkRowReconstruction(
  data: any[],
  mode: StorageMode,
  dataset: string,
  collection: string,
  iterations: number,
  fileSize: number
): Promise<BenchmarkResult> {
  const { avgMs, minMs, maxMs } = await timeOperation(
    iterations,
    async () => {
      const reconstructed: any[] = []
      for (const row of data) {
        if (mode === 'columnar-only') {
          // Already have full row from columns
          reconstructed.push(row)
        } else {
          // Parse from data column
          if (row.data) {
            reconstructed.push(JSON.parse(row.data))
          } else {
            reconstructed.push(row)
          }
        }
      }
      return reconstructed
    }
  )

  return {
    mode,
    dataset,
    collection,
    operation: 'row_reconstruction',
    avgMs: Math.round(avgMs * 100) / 100,
    minMs: Math.round(minMs * 100) / 100,
    maxMs: Math.round(maxMs * 100) / 100,
    rowsProcessed: data.length,
    rowsPerSec: Math.round(data.length / (avgMs / 1000)),
    fileSize,
  }
}

// =============================================================================
// Main
// =============================================================================

interface DatasetConfig {
  collections: string[]
}

const DATASET_CONFIGS: Record<string, DatasetConfig> = {
  imdb: { collections: ['titles'] },
  onet: { collections: ['occupations', 'skills', 'abilities', 'knowledge'] },
  unspsc: { collections: ['segments', 'families', 'classes', 'commodities'] },
}

async function main() {
  const args = process.argv.slice(2)
  const datasetArg = args.find(a => a.startsWith('--dataset='))
  const iterArg = args.find(a => a.startsWith('--iterations='))

  const datasets = datasetArg
    ? [datasetArg.split('=')[1]!]
    : Object.keys(DATASET_CONFIGS)
  const iterations = iterArg ? parseInt(iterArg.split('=')[1]!) : DEFAULT_ITERATIONS

  console.log('=== Storage Mode Performance Benchmark ===')
  console.log(`Datasets: ${datasets.join(', ')}`)
  console.log(`Iterations: ${iterations}`)
  console.log('')

  const allResults: BenchmarkResult[] = []

  for (const dataset of datasets) {
    const config = DATASET_CONFIGS[dataset]
    if (!config) {
      console.log(`Unknown dataset: ${dataset}`)
      continue
    }

    console.log(`\n${'='.repeat(70)}`)
    console.log(`Dataset: ${dataset.toUpperCase()}`)
    console.log('='.repeat(70))

    for (const collection of config.collections) {
      console.log(`\nCollection: ${collection}`)
      console.log('-'.repeat(50))

      const modeResults: Map<StorageMode, { scan: BenchmarkResult; recon: BenchmarkResult }> = new Map()

      for (const mode of ALL_MODES) {
        const dir = join(BASE_DIR, `${dataset}-${mode}`)
        const path = join(dir, `${collection}.parquet`)

        if (!existsSync(path)) {
          console.log(`  ${mode}: [NOT FOUND]`)
          continue
        }

        const stats = await fs.stat(path)
        process.stdout.write(`  ${mode} (${formatBytes(stats.size)}): `)

        // Full scan
        const scanResult = await benchmarkFullScan(path, mode, dataset, collection, iterations, stats.size)
        allResults.push(scanResult)

        // Row reconstruction (reuse loaded data)
        const { data } = await readParquetFile(path)
        const reconResult = await benchmarkRowReconstruction(data, mode, dataset, collection, iterations, stats.size)
        allResults.push(reconResult)

        modeResults.set(mode, { scan: scanResult, recon: reconResult })

        console.log(`scan=${scanResult.avgMs.toFixed(0)}ms (${formatNumber(scanResult.rowsPerSec)}/s), recon=${reconResult.avgMs.toFixed(0)}ms (${formatNumber(reconResult.rowsPerSec)}/s)`)
      }

      // Show relative performance
      const baseline = modeResults.get('columnar-only')
      if (baseline && modeResults.size > 1) {
        console.log(`\n  Relative to columnar-only:`)
        for (const [mode, results] of modeResults) {
          if (mode === 'columnar-only') continue
          const scanRatio = (results.scan.avgMs / baseline.scan.avgMs).toFixed(2)
          const reconRatio = (results.recon.avgMs / baseline.recon.avgMs).toFixed(2)
          const sizeRatio = ((results.scan.fileSize / baseline.scan.fileSize - 1) * 100).toFixed(0)
          console.log(`    ${mode}: scan=${scanRatio}x, recon=${reconRatio}x, size=+${sizeRatio}%`)
        }
      }
    }
  }

  // Summary tables
  console.log('\n' + '='.repeat(90))
  console.log('SUMMARY: Full Scan Performance')
  console.log('='.repeat(90))
  console.log('Dataset/Collection   | Rows      | columnar-only    | columnar-row     | row-only         | row-index')
  console.log('---------------------|-----------|------------------|------------------|------------------|------------------')

  for (const dataset of datasets) {
    const config = DATASET_CONFIGS[dataset]
    if (!config) continue

    for (const collection of config.collections) {
      const baseResult = allResults.find(r =>
        r.dataset === dataset &&
        r.collection === collection &&
        r.mode === 'columnar-only' &&
        r.operation === 'full_scan'
      )
      const rows = baseResult?.rowsProcessed ?? 0

      const cols = [`${dataset}/${collection}`.padEnd(20), formatNumber(rows).padStart(9)]

      for (const mode of ALL_MODES) {
        const result = allResults.find(r =>
          r.dataset === dataset &&
          r.collection === collection &&
          r.mode === mode &&
          r.operation === 'full_scan'
        )
        if (result) {
          cols.push(`${result.avgMs.toFixed(0)}ms ${formatNumber(result.rowsPerSec)}/s`.padStart(16))
        } else {
          cols.push('N/A'.padStart(16))
        }
      }
      console.log(cols.join(' | '))
    }
  }

  console.log('\n' + '='.repeat(90))
  console.log('SUMMARY: Row Reconstruction Performance (includes JSON.parse for data column)')
  console.log('='.repeat(90))
  console.log('Dataset/Collection   | Rows      | columnar-only    | columnar-row     | row-only         | row-index')
  console.log('---------------------|-----------|------------------|------------------|------------------|------------------')

  for (const dataset of datasets) {
    const config = DATASET_CONFIGS[dataset]
    if (!config) continue

    for (const collection of config.collections) {
      const baseResult = allResults.find(r =>
        r.dataset === dataset &&
        r.collection === collection &&
        r.mode === 'columnar-only' &&
        r.operation === 'row_reconstruction'
      )
      const rows = baseResult?.rowsProcessed ?? 0

      const cols = [`${dataset}/${collection}`.padEnd(20), formatNumber(rows).padStart(9)]

      for (const mode of ALL_MODES) {
        const result = allResults.find(r =>
          r.dataset === dataset &&
          r.collection === collection &&
          r.mode === mode &&
          r.operation === 'row_reconstruction'
        )
        if (result) {
          cols.push(`${result.avgMs.toFixed(0)}ms ${formatNumber(result.rowsPerSec)}/s`.padStart(16))
        } else {
          cols.push('N/A'.padStart(16))
        }
      }
      console.log(cols.join(' | '))
    }
  }

  console.log('\nDone!')
}

main().catch(console.error)
