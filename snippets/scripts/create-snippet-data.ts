#!/usr/bin/env bun
/**
 * Create Snippet-Compatible Parquet Files
 *
 * Creates simple, uncompressed parquet files that work with parquet-tiny.
 * These have minimal metadata and no complex features.
 *
 * Usage:
 *   bun scripts/create-snippet-data.ts
 */

import { parquetRead, parquetMetadataAsync } from 'hyparquet'
import { parquetWriteFile } from 'hyparquet-writer'
import { compressors } from 'hyparquet-compressors'
import { readFileSync, mkdirSync, existsSync, writeFileSync } from 'fs'
import { resolve, dirname } from 'path'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = resolve(import.meta.dirname, '../../data')
const OUTPUT_DIR = resolve(import.meta.dirname, '../../data/snippets')

// =============================================================================
// Helper Functions
// =============================================================================

async function readParquet(filePath: string): Promise<unknown[][]> {
  const buffer = readFileSync(filePath)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const rows: unknown[][] = []
  await parquetRead({
    file: arrayBuffer,
    compressors,
    onComplete: (data) => {
      rows.push(...(data as unknown[][]))
    }
  })

  return rows
}

async function writeSimpleParquet(
  filePath: string,
  columnNames: string[],
  rows: unknown[][]
): Promise<void> {
  const columnData = columnNames.map((name, idx) => ({
    name,
    data: rows.map(row => String(row[idx] ?? '')),
  }))

  await parquetWriteFile({
    filename: filePath,
    columnData,
    codec: 'UNCOMPRESSED',
  })
}

// =============================================================================
// Dataset Converters
// =============================================================================

async function createOnetOccupations() {
  console.log('\n=== Creating O*NET Occupations ===')

  const sourcePath = resolve(DATA_DIR, 'onet/Occupation Data.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'onet-occupations.parquet')

  console.log(`Reading: ${sourcePath}`)
  const rows = await readParquet(sourcePath)
  console.log(`Rows: ${rows.length}`)

  console.log(`Writing: ${outputPath}`)
  await writeSimpleParquet(outputPath, ['code', 'title', 'description'], rows)

  console.log('Done!')
}

async function createUnspsc() {
  console.log('\n=== Creating UNSPSC Reference ===')

  const sourcePath = resolve(DATA_DIR, 'onet/UNSPSC Reference.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'unspsc.parquet')

  console.log(`Reading: ${sourcePath}`)
  const rows = await readParquet(sourcePath)
  console.log(`Rows: ${rows.length}`)

  const columnNames = [
    'commodityCode', 'commodityTitle',
    'classCode', 'classTitle',
    'familyCode', 'familyTitle',
    'segmentCode', 'segmentTitle'
  ]

  console.log(`Writing: ${outputPath}`)
  await writeSimpleParquet(outputPath, columnNames, rows)

  console.log('Done!')
}

async function createImdbSubset() {
  console.log('\n=== Creating IMDB Subset ===')

  const sourcePath = resolve(DATA_DIR, 'imdb/title.basics.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'imdb-titles.parquet')

  console.log(`Reading: ${sourcePath}`)
  const buffer = readFileSync(sourcePath)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const rows: unknown[][] = []
  const MAX_ROWS = 10000
  const MIN_YEAR = 2000  // Include more years

  await parquetRead({
    file: arrayBuffer,
    compressors,
    onComplete: (data) => {
      for (const row of data as unknown[][]) {
        if (rows.length >= MAX_ROWS) break

        // Filter: year >= 2000, non-adult, has title type (movies, tvSeries, etc)
        const year = parseInt(row[5] as string, 10)
        if (isNaN(year) || year < MIN_YEAR) continue
        if (row[4] === '1') continue

        rows.push(row)
      }
    }
  })

  console.log(`Filtered rows: ${rows.length}`)

  const columnNames = [
    'tconst', 'titleType', 'primaryTitle', 'originalTitle',
    'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres'
  ]

  console.log(`Writing: ${outputPath}`)
  await writeSimpleParquet(outputPath, columnNames, rows)

  console.log('Done!')
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('Creating Snippet-Compatible Parquet Files')
  console.log('=========================================')

  // Ensure output directory exists
  if (!existsSync(OUTPUT_DIR)) {
    mkdirSync(OUTPUT_DIR, { recursive: true })
  }

  await createOnetOccupations()
  await createUnspsc()
  await createImdbSubset()

  // Show file sizes
  console.log('\n=== Output File Sizes ===')
  const files = ['onet-occupations.parquet', 'unspsc.parquet', 'imdb-titles.parquet']
  for (const file of files) {
    const path = resolve(OUTPUT_DIR, file)
    if (existsSync(path)) {
      const stats = Bun.file(path).size
      console.log(`  ${file}: ${(stats / 1024).toFixed(1)} KB`)
    }
  }

  console.log('\n=== Upload Commands ===')
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/onet-occupations.parquet \\')
  console.log(`  --file="${OUTPUT_DIR}/onet-occupations.parquet" --remote`)
  console.log()
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/unspsc.parquet \\')
  console.log(`  --file="${OUTPUT_DIR}/unspsc.parquet" --remote`)
  console.log()
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/imdb-titles.parquet \\')
  console.log(`  --file="${OUTPUT_DIR}/imdb-titles.parquet" --remote`)
}

main().catch(console.error)
