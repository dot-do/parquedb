#!/usr/bin/env npx tsx
/**
 * Generate IMDB Dataset in All Storage Modes
 *
 * Creates 4 variants for benchmarking:
 * - columnar-only: Native columns, no row store
 * - columnar-row:  Native columns + $data JSON blob
 * - row-only:      Just $id, $type, name, $data
 * - row-index:     $data + $index_* shredded columns
 *
 * Usage:
 *   npx tsx scripts/generate-imdb-modes.ts                    # All modes, 100K rows
 *   npx tsx scripts/generate-imdb-modes.ts --mode=columnar-only
 *   npx tsx scripts/generate-imdb-modes.ts --rows=1000000     # 1M rows
 *   npx tsx scripts/generate-imdb-modes.ts --real             # Use real IMDB data
 */

import { createReadStream, existsSync } from 'node:fs'
import { promises as fs } from 'node:fs'
import { createGunzip } from 'node:zlib'
import { pipeline } from 'node:stream/promises'
import { createInterface } from 'node:readline'
import { join } from 'node:path'
import {
  type StorageMode,
  type EntitySchema,
  generateColumns,
  writeParquetFile,
  getOutputDir,
  formatBytes,
  formatNumber,
} from './lib/storage-modes'

// =============================================================================
// Configuration
// =============================================================================

const BASE_DIR = 'data-v3'
const CACHE_DIR = 'data-v3/imdb-1m/.cache'
const DEFAULT_ROWS = 100_000
const ALL_MODES: StorageMode[] = ['columnar-only', 'columnar-row', 'row-only', 'row-index']

// IMDB Title schema
const TITLE_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'tconst', type: 'STRING', indexed: true },
    { name: 'titleType', type: 'STRING', indexed: true },
    { name: 'startYear', type: 'INT32', indexed: true },
    { name: 'endYear', type: 'INT32' },
    { name: 'runtimeMinutes', type: 'INT32', indexed: true },
    { name: 'isAdult', type: 'BOOLEAN', indexed: true },
    { name: 'genres', type: 'STRING' },
    { name: 'originalTitle', type: 'STRING' },
    { name: 'averageRating', type: 'DOUBLE', indexed: true },
    { name: 'numVotes', type: 'INT32', indexed: true },
  ],
}

// =============================================================================
// Types
// =============================================================================

interface ImdbTitle {
  tconst: string
  titleType: string
  primaryTitle: string
  originalTitle: string | null
  isAdult: boolean
  startYear: number | null
  endYear: number | null
  runtimeMinutes: number | null
  genres: string | null
  averageRating?: number
  numVotes?: number
}

interface GenerationStats {
  mode: StorageMode
  file: string
  rows: number
  size: number
  bytesPerRow: number
}

// =============================================================================
// Data Loading
// =============================================================================

async function downloadIfNeeded(): Promise<string> {
  const gzPath = join(CACHE_DIR, 'title.basics.tsv.gz')
  const tsvPath = join(CACHE_DIR, 'title.basics.tsv')

  if (!existsSync(gzPath)) {
    console.log('Downloading title.basics.tsv.gz...')
    const url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    const res = await fetch(url)
    if (!res.ok) throw new Error(`Download failed: ${res.status}`)

    await fs.mkdir(CACHE_DIR, { recursive: true })
    const data = await res.arrayBuffer()
    await fs.writeFile(gzPath, Buffer.from(data))
    console.log(`Downloaded ${formatBytes(data.byteLength)}`)
  }

  if (!existsSync(tsvPath)) {
    console.log('Decompressing...')
    const gunzip = createGunzip()
    const source = createReadStream(gzPath)
    const dest = await fs.open(tsvPath, 'w')
    await pipeline(source, gunzip, dest.createWriteStream())
    await dest.close()
    console.log('Decompressed')
  }

  return tsvPath
}

async function* parseTsv(path: string): AsyncGenerator<Record<string, string>> {
  const stream = createReadStream(path)
  const rl = createInterface({ input: stream, crlfDelay: Infinity })

  let headers: string[] = []
  let isFirst = true

  for await (const line of rl) {
    if (isFirst) {
      headers = line.split('\t')
      isFirst = false
      continue
    }

    const values = line.split('\t')
    const row: Record<string, string> = {}
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]!] = values[i] ?? ''
    }
    yield row
  }
}

async function loadRealImdbData(limit: number): Promise<ImdbTitle[]> {
  const tsvPath = await downloadIfNeeded()
  console.log(`\nParsing real IMDB data (limit: ${formatNumber(limit)})...`)

  const titles: ImdbTitle[] = []
  let count = 0

  for await (const row of parseTsv(tsvPath)) {
    if (count >= limit) break
    if (!row.tconst || row.tconst === '\\N') continue

    titles.push({
      tconst: row.tconst,
      titleType: row.titleType !== '\\N' ? row.titleType : 'unknown',
      primaryTitle: row.primaryTitle !== '\\N' ? row.primaryTitle : row.originalTitle !== '\\N' ? row.originalTitle : `Title ${row.tconst}`,
      originalTitle: row.originalTitle !== '\\N' ? row.originalTitle : null,
      isAdult: row.isAdult === '1',
      startYear: row.startYear !== '\\N' ? parseInt(row.startYear) : null,
      endYear: row.endYear !== '\\N' ? parseInt(row.endYear) : null,
      runtimeMinutes: row.runtimeMinutes !== '\\N' ? parseInt(row.runtimeMinutes) : null,
      genres: row.genres !== '\\N' ? row.genres : null,
    })
    count++

    if (count % 50000 === 0) {
      console.log(`  Loaded ${formatNumber(count)} titles...`)
    }
  }

  console.log(`Loaded ${formatNumber(titles.length)} real titles`)
  return titles
}

function generateSyntheticData(count: number): ImdbTitle[] {
  console.log(`\nGenerating ${formatNumber(count)} synthetic titles...`)

  const titleTypes = ['movie', 'tvSeries', 'tvEpisode', 'short', 'tvMovie', 'video']
  const genreList = ['Drama', 'Comedy', 'Action', 'Thriller', 'Horror', 'Romance', 'Sci-Fi', 'Documentary']
  const titles: ImdbTitle[] = []

  for (let i = 0; i < count; i++) {
    const typeIdx = i % titleTypes.length
    const year = 1920 + (i % 105)
    const rating = 1 + (i % 90) / 10
    const votes = 100 + (i * 7) % 1000000

    titles.push({
      tconst: `tt${String(i).padStart(7, '0')}`,
      titleType: titleTypes[typeIdx]!,
      primaryTitle: `Title ${i}`,
      originalTitle: i % 3 === 0 ? `Original Title ${i}` : null,
      isAdult: i % 20 === 0,
      startYear: year,
      endYear: titleTypes[typeIdx] === 'tvSeries' ? year + (i % 10) : null,
      runtimeMinutes: 60 + (i % 180),
      genres: `${genreList[i % genreList.length]},${genreList[(i + 3) % genreList.length]}`,
      averageRating: Math.round(rating * 10) / 10,
      numVotes: votes,
    })

    if ((i + 1) % 100000 === 0) {
      console.log(`  Generated ${formatNumber(i + 1)} titles...`)
    }
  }

  return titles
}

// =============================================================================
// Generation
// =============================================================================

async function generateMode(
  titles: ImdbTitle[],
  mode: StorageMode
): Promise<GenerationStats> {
  const outputDir = getOutputDir(BASE_DIR, 'imdb', mode)
  const outputPath = join(outputDir, 'titles.parquet')

  console.log(`\nGenerating ${mode}...`)

  // Sort by titleType for better row group statistics
  titles.sort((a, b) => a.titleType.localeCompare(b.titleType))

  const columns = generateColumns(
    titles,
    TITLE_SCHEMA,
    mode,
    (t) => `title:${t.tconst}`,
    () => 'Title',
    (t) => t.primaryTitle
  )

  const result = await writeParquetFile(outputPath, columns, 10000)

  const stats: GenerationStats = {
    mode,
    file: outputPath,
    rows: result.rows,
    size: result.size,
    bytesPerRow: Math.round(result.size / result.rows),
  }

  console.log(`  Wrote ${formatBytes(result.size)} (${stats.bytesPerRow} bytes/row)`)
  console.log(`  Columns: ${columns.map(c => c.name).join(', ')}`)

  return stats
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)
  const useReal = args.includes('--real')
  const rowsArg = args.find(a => a.startsWith('--rows='))
  const modeArg = args.find(a => a.startsWith('--mode='))

  const rows = rowsArg ? parseInt(rowsArg.split('=')[1]!) : DEFAULT_ROWS
  const modes: StorageMode[] = modeArg
    ? [modeArg.split('=')[1] as StorageMode]
    : ALL_MODES

  console.log('=== IMDB Storage Mode Generator ===')
  console.log(`Rows: ${formatNumber(rows)}`)
  console.log(`Modes: ${modes.join(', ')}`)
  console.log(`Source: ${useReal ? 'Real IMDB data' : 'Synthetic'}`)

  // Load or generate data
  const titles = useReal
    ? await loadRealImdbData(rows)
    : generateSyntheticData(rows)

  // Generate each mode
  const allStats: GenerationStats[] = []
  for (const mode of modes) {
    const stats = await generateMode(titles, mode)
    allStats.push(stats)
  }

  // Print summary
  console.log('\n=== Summary ===')
  console.log('Mode              | Rows      | Size       | Bytes/Row')
  console.log('------------------|-----------|------------|----------')
  for (const stats of allStats) {
    const mode = stats.mode.padEnd(17)
    const rowsStr = formatNumber(stats.rows).padStart(9)
    const sizeStr = formatBytes(stats.size).padStart(10)
    const bprStr = String(stats.bytesPerRow).padStart(9)
    console.log(`${mode} | ${rowsStr} | ${sizeStr} | ${bprStr}`)
  }

  // Calculate overhead
  const columnarOnly = allStats.find(s => s.mode === 'columnar-only')
  if (columnarOnly) {
    console.log('\nStorage overhead vs columnar-only:')
    for (const stats of allStats) {
      if (stats.mode === 'columnar-only') continue
      const overhead = ((stats.size / columnarOnly.size - 1) * 100).toFixed(1)
      console.log(`  ${stats.mode}: +${overhead}%`)
    }
  }

  console.log('\nDone!')
}

main().catch(console.error)
