#!/usr/bin/env bun
/**
 * Create JSON Data Files for Snippets
 *
 * Creates JSON files that can be easily parsed by snippets.
 * This is a simpler alternative to Parquet when the tiny reader has issues.
 *
 * Usage:
 *   bun scripts/create-json-data.ts
 */

import { parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { readFileSync, mkdirSync, existsSync, writeFileSync } from 'fs'
import { resolve } from 'path'

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

// =============================================================================
// Dataset Converters
// =============================================================================

async function createOnetJson() {
  console.log('\n=== Creating O*NET JSON ===')

  const sourcePath = resolve(DATA_DIR, 'onet/Occupation Data.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'onet-occupations.json')

  console.log(`Reading: ${sourcePath}`)
  const rows = await readParquet(sourcePath)
  console.log(`Rows: ${rows.length}`)

  const data = rows.map(row => ({
    code: row[0],
    title: row[1],
    description: row[2],
  }))

  console.log(`Writing: ${outputPath}`)
  writeFileSync(outputPath, JSON.stringify(data))

  const size = Bun.file(outputPath).size
  console.log(`Size: ${(size / 1024).toFixed(1)} KB`)
}

async function createUnspscJson() {
  console.log('\n=== Creating UNSPSC JSON ===')

  const sourcePath = resolve(DATA_DIR, 'onet/UNSPSC Reference.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'unspsc.json')

  console.log(`Reading: ${sourcePath}`)
  const rows = await readParquet(sourcePath)
  console.log(`Rows: ${rows.length}`)

  const data = rows.map(row => ({
    commodityCode: row[0],
    commodityTitle: row[1],
    classCode: row[2],
    classTitle: row[3],
    familyCode: row[4],
    familyTitle: row[5],
    segmentCode: row[6],
    segmentTitle: row[7],
  }))

  console.log(`Writing: ${outputPath}`)
  writeFileSync(outputPath, JSON.stringify(data))

  const size = Bun.file(outputPath).size
  console.log(`Size: ${(size / 1024).toFixed(1)} KB`)
}

async function createImdbJson() {
  console.log('\n=== Creating IMDB JSON ===')

  const sourcePath = resolve(DATA_DIR, 'imdb/title.basics.parquet')
  const outputPath = resolve(OUTPUT_DIR, 'imdb-titles.json')

  console.log(`Reading: ${sourcePath}`)
  const buffer = readFileSync(sourcePath)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const rows: unknown[][] = []
  const MAX_ROWS = 10000
  const MIN_YEAR = 2000

  await parquetRead({
    file: arrayBuffer,
    compressors,
    onComplete: (data) => {
      for (const row of data as unknown[][]) {
        if (rows.length >= MAX_ROWS) break

        const year = parseInt(row[5] as string, 10)
        if (isNaN(year) || year < MIN_YEAR) continue
        if (row[4] === '1') continue

        rows.push(row)
      }
    }
  })

  console.log(`Filtered rows: ${rows.length}`)

  const data = rows.map(row => ({
    tconst: row[0],
    titleType: row[1],
    primaryTitle: row[2],
    originalTitle: row[3],
    isAdult: row[4] === '1',
    startYear: parseInt(row[5] as string, 10) || null,
    endYear: row[6] !== '\\N' ? parseInt(row[6] as string, 10) : null,
    runtimeMinutes: row[7] !== '\\N' ? parseInt(row[7] as string, 10) : null,
    genres: row[8] && row[8] !== '\\N' ? (row[8] as string).split(',') : [],
  }))

  console.log(`Writing: ${outputPath}`)
  writeFileSync(outputPath, JSON.stringify(data))

  const size = Bun.file(outputPath).size
  console.log(`Size: ${(size / 1024).toFixed(1)} KB`)
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('Creating JSON Data Files for Snippets')
  console.log('=====================================')

  if (!existsSync(OUTPUT_DIR)) {
    mkdirSync(OUTPUT_DIR, { recursive: true })
  }

  await createOnetJson()
  await createUnspscJson()
  await createImdbJson()

  console.log('\n=== Upload Commands ===')
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/onet-occupations.json \\')
  console.log(`  --file="${OUTPUT_DIR}/onet-occupations.json" --remote`)
  console.log()
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/unspsc.json \\')
  console.log(`  --file="${OUTPUT_DIR}/unspsc.json" --remote`)
  console.log()
  console.log('wrangler r2 object put cdn/parquedb-benchmarks/snippets/imdb-titles.json \\')
  console.log(`  --file="${OUTPUT_DIR}/imdb-titles.json" --remote`)
}

main().catch(console.error)
