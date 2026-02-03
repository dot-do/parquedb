#!/usr/bin/env bun
/**
 * Create IMDB Subset for Snippets
 *
 * The full IMDB dataset is too large for Cloudflare Snippets' memory limits.
 * This script creates a smaller subset suitable for edge deployment.
 *
 * Usage:
 *   bun scripts/create-imdb-subset.ts
 *
 * Output:
 *   ../data/imdb-subset/title-basics-subset.parquet
 */

import { parquetRead } from 'hyparquet'
import { parquetWriteFile } from 'hyparquet-writer'
import { compressors } from 'hyparquet-compressors'
import { readFileSync, mkdirSync, existsSync } from 'fs'
import { resolve, dirname } from 'path'

// =============================================================================
// Configuration
// =============================================================================

const SOURCE_FILE = resolve(import.meta.dirname, '../../data/imdb/title.basics.parquet')
const OUTPUT_DIR = resolve(import.meta.dirname, '../../data/imdb-subset')
const OUTPUT_FILE = resolve(OUTPUT_DIR, 'title-basics-subset.parquet')

// Subset parameters
const MAX_ROWS = 50000
const MIN_YEAR = 2015  // Only include recent titles

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('Creating IMDB subset for Snippets...\n')

  // Read source file
  console.log(`Reading: ${SOURCE_FILE}`)
  const buffer = readFileSync(SOURCE_FILE)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const allRows: unknown[][] = []

  await parquetRead({
    file: arrayBuffer,
    compressors,
    onComplete: (rows) => {
      for (const row of rows as unknown[][]) {
        // Filter by year
        const year = parseInt(row[5] as string, 10)
        if (isNaN(year) || year < MIN_YEAR) continue

        // Skip adult content
        if (row[4] === '1') continue

        allRows.push(row)

        if (allRows.length >= MAX_ROWS) break
      }
    }
  })

  console.log(`Filtered ${allRows.length} rows (from year ${MIN_YEAR}+, non-adult)`)

  // Ensure output directory exists
  if (!existsSync(OUTPUT_DIR)) {
    mkdirSync(OUTPUT_DIR, { recursive: true })
  }

  // Convert rows to column data format
  const columnNames = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
  const columnData = columnNames.map((name, idx) => ({
    name,
    data: allRows.map(row => row[idx] as string),
  }))

  // Write subset
  console.log(`Writing: ${OUTPUT_FILE}`)

  await parquetWriteFile({
    filename: OUTPUT_FILE,
    columnData,
    codec: 'UNCOMPRESSED',  // Required for tiny parquet reader
  })

  console.log('\nDone!')
  console.log(`Output: ${OUTPUT_FILE}`)
  console.log(`Rows: ${allRows.length}`)

  // Show sample
  console.log('\nSample rows:')
  for (let i = 0; i < Math.min(3, allRows.length); i++) {
    const row = allRows[i]
    console.log(`  ${row[0]}: ${row[2]} (${row[5]})`)
  }

  console.log('\nNext steps:')
  console.log('1. Upload to R2:')
  console.log(`   wrangler r2 object put cdn/parquedb-benchmarks/imdb/title-basics-subset.parquet \\`)
  console.log(`     --file="${OUTPUT_FILE}" --remote`)
}

main().catch(console.error)
