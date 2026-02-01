/**
 * Variant Shredding Benchmark for ParqueDB
 *
 * Compares query performance across different data storage strategies:
 * - V1: JSON column (no predicate pushdown)
 * - V2: Native columns (full predicate pushdown)
 * - V3: Dual Variant ($index shredded + $data full) - target architecture
 *
 * Tests predicate pushdown effectiveness on IMDB-like data.
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetQuery, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import {
  Timer,
  startTimer,
  formatBytes,
  randomInt,
} from './setup'

// =============================================================================
// Configuration
// =============================================================================

const BENCHMARK_DIR = '/tmp/parquedb-variant-benchmark'
const ROW_COUNTS = [10_000, 100_000, 500_000]
const ROWS_PER_GROUP = 50_000

// Title types with realistic distribution
const TITLE_TYPES = ['movie', 'tvSeries', 'short', 'tvEpisode', 'tvMovie', 'video']
const GENRES = ['Action', 'Drama', 'Comedy', 'Thriller', 'Horror', 'Romance', 'Sci-Fi', 'Documentary']

// =============================================================================
// Data Generation
// =============================================================================

interface TitleRecord {
  id: string
  titleType: string
  primaryTitle: string
  startYear: number
  genres: string[]
  averageRating: number
  numVotes: number
}

function generateTitleRecords(count: number): TitleRecord[] {
  const records: TitleRecord[] = []

  for (let i = 0; i < count; i++) {
    // Movie is ~35% of data, distributed across row groups
    const titleType = i % 3 === 0 ? 'movie' : TITLE_TYPES[i % TITLE_TYPES.length]
    const startYear = 1900 + (i % 125) // 1900-2024
    const numGenres = 1 + (i % 3)
    const genres = Array.from({ length: numGenres }, (_, j) =>
      GENRES[(i + j) % GENRES.length]
    )

    records.push({
      id: `tt${String(i).padStart(7, '0')}`,
      titleType,
      primaryTitle: `Title ${i}`,
      startYear,
      genres,
      averageRating: 5 + (i % 50) / 10, // 5.0-10.0
      numVotes: 100 + i * 10,
    })
  }

  return records
}

// =============================================================================
// File Writers
// =============================================================================

/**
 * V1: JSON column - no predicate pushdown possible
 */
async function writeV1JsonFile(records: TitleRecord[], path: string): Promise<number> {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: records.map(r => r.id) },
      { name: '$data', type: 'STRING', data: records.map(r => JSON.stringify(r)) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'LZ4_RAW',
  })

  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

/**
 * V2: Native columns - full predicate pushdown
 */
async function writeV2NativeFile(records: TitleRecord[], path: string): Promise<number> {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: records.map(r => r.id) },
      { name: 'titleType', type: 'STRING', data: records.map(r => r.titleType) },
      { name: 'primaryTitle', type: 'STRING', data: records.map(r => r.primaryTitle) },
      { name: 'startYear', type: 'INT32', data: records.map(r => r.startYear) },
      { name: 'genres', type: 'STRING', data: records.map(r => JSON.stringify(r.genres)) },
      { name: 'averageRating', type: 'DOUBLE', data: records.map(r => r.averageRating) },
      { name: 'numVotes', type: 'INT32', data: records.map(r => r.numVotes) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'LZ4_RAW',
  })

  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

/**
 * V3: Dual storage - $index with native shredded columns + $data as JSON
 * This approximates the target architecture - index fields for pushdown, data for return
 */
async function writeV3DualVariantFile(records: TitleRecord[], path: string): Promise<number> {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: records.map(r => r.id) },
      // $index fields as native columns (shredded equivalent)
      { name: '$index_titleType', type: 'STRING', data: records.map(r => r.titleType) },
      { name: '$index_startYear', type: 'INT32', data: records.map(r => r.startYear) },
      // $data as JSON string (will be Variant in production)
      { name: '$data', type: 'STRING', data: records.map(r => JSON.stringify(r)) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'LZ4_RAW',
  })

  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

// =============================================================================
// Query Benchmarks
// =============================================================================

interface QueryResult {
  rowsReturned: number
  queryTimeMs: number
}

/**
 * Query V1 JSON file - must scan all rows, filter in memory
 */
async function queryV1Json(path: string, targetType: string): Promise<QueryResult> {
  const timer = startTimer()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (start: number, end?: number) => fileBuffer.slice(start, end).buffer,
  }

  // Read all rows (no predicate pushdown possible)
  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data'],
    compressors,
  })

  // Filter in memory
  const filtered = rows.filter((row: any) => {
    const data = JSON.parse(row.$data)
    return data.titleType === targetType
  })

  return {
    rowsReturned: filtered.length,
    queryTimeMs: timer.stop().elapsed(),
  }
}

/**
 * Query V2 Native file - full predicate pushdown
 */
async function queryV2Native(path: string, targetType: string): Promise<QueryResult> {
  const timer = startTimer()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (start: number, end?: number) => fileBuffer.slice(start, end).buffer,
  }

  // Query with predicate pushdown
  const rows = await parquetQuery({
    file,
    filter: { titleType: targetType },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: timer.stop().elapsed(),
  }
}

/**
 * Query V3 Dual storage file - predicate pushdown on $index columns
 */
async function queryV3DualVariant(path: string, targetType: string): Promise<QueryResult> {
  const timer = startTimer()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (start: number, end?: number) => fileBuffer.slice(start, end).buffer,
  }

  // Query with predicate pushdown on $index column, return $data
  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data'],
    filter: { '$index_titleType': targetType },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: timer.stop().elapsed(),
  }
}

// =============================================================================
// Main Benchmark Runner
// =============================================================================

async function runBenchmark() {
  console.log('='.repeat(80))
  console.log('Variant Shredding Benchmark')
  console.log('='.repeat(80))
  console.log()

  // Setup
  await fs.mkdir(BENCHMARK_DIR, { recursive: true })

  for (const rowCount of ROW_COUNTS) {
    console.log(`\n${'─'.repeat(80)}`)
    console.log(`Dataset: ${rowCount.toLocaleString()} records`)
    console.log('─'.repeat(80))

    // Generate data
    console.log('Generating test data...')
    const timer = startTimer()
    const records = generateTitleRecords(rowCount)
    console.log(`  Generated in ${timer.stop().elapsed().toFixed(0)}ms`)

    // Count movies for filter selectivity
    const movieCount = records.filter(r => r.titleType === 'movie').length
    const selectivity = (movieCount / rowCount * 100).toFixed(1)
    console.log(`  Movies: ${movieCount.toLocaleString()} (${selectivity}% selectivity)`)

    // Write files
    const v1Path = join(BENCHMARK_DIR, `v1-json-${rowCount}.parquet`)
    const v2Path = join(BENCHMARK_DIR, `v2-native-${rowCount}.parquet`)
    const v3Path = join(BENCHMARK_DIR, `v3-dual-${rowCount}.parquet`)

    console.log('\nWriting files...')

    timer.reset().start()
    const v1Size = await writeV1JsonFile(records, v1Path)
    console.log(`  V1 (JSON):         ${formatBytes(v1Size).padStart(10)} in ${timer.stop().elapsed().toFixed(0)}ms`)

    timer.reset().start()
    const v2Size = await writeV2NativeFile(records, v2Path)
    console.log(`  V2 (Native):       ${formatBytes(v2Size).padStart(10)} in ${timer.stop().elapsed().toFixed(0)}ms`)

    timer.reset().start()
    const v3Size = await writeV3DualVariantFile(records, v3Path)
    console.log(`  V3 (Dual Variant): ${formatBytes(v3Size).padStart(10)} in ${timer.stop().elapsed().toFixed(0)}ms`)

    const v3Overhead = ((v3Size - v1Size) / v1Size * 100).toFixed(1)
    console.log(`  Storage overhead (V3 vs V1): ${v3Overhead}%`)

    // Run queries
    console.log('\nQuerying: { titleType: "movie" }')
    console.log()

    const iterations = 5

    // V1 benchmarks
    const v1Times: number[] = []
    let v1Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV1Json(v1Path, 'movie')
      v1Times.push(result.queryTimeMs)
      v1Rows = result.rowsReturned
    }

    // V2 benchmarks
    const v2Times: number[] = []
    let v2Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV2Native(v2Path, 'movie')
      v2Times.push(result.queryTimeMs)
      v2Rows = result.rowsReturned
    }

    // V3 benchmarks
    const v3Times: number[] = []
    let v3Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV3DualVariant(v3Path, 'movie')
      v3Times.push(result.queryTimeMs)
      v3Rows = result.rowsReturned
    }

    // Calculate averages
    const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length
    const min = (arr: number[]) => Math.min(...arr)

    const v1Avg = avg(v1Times)
    const v2Avg = avg(v2Times)
    const v3Avg = avg(v3Times)

    console.log('  V1 (JSON - no pushdown):')
    console.log(`    Avg: ${v1Avg.toFixed(1)}ms  Min: ${min(v1Times).toFixed(1)}ms  Rows: ${v1Rows.toLocaleString()}`)

    console.log('  V2 (Native - full pushdown):')
    console.log(`    Avg: ${v2Avg.toFixed(1)}ms  Min: ${min(v2Times).toFixed(1)}ms  Rows: ${v2Rows.toLocaleString()}`)

    console.log('  V3 (Dual Variant - $index pushdown):')
    console.log(`    Avg: ${v3Avg.toFixed(1)}ms  Min: ${min(v3Times).toFixed(1)}ms  Rows: ${v3Rows.toLocaleString()}`)

    console.log()
    console.log(`  SPEEDUP V2 vs V1: ${(v1Avg / v2Avg).toFixed(1)}x faster`)
    console.log(`  SPEEDUP V3 vs V1: ${(v1Avg / v3Avg).toFixed(1)}x faster`)
    console.log(`  V3 vs V2 overhead: ${((v3Avg / v2Avg - 1) * 100).toFixed(0)}%`)

    // Range query on startYear
    console.log('\nQuerying: { startYear > 2020 } (range query)')

    const v2RangeTimes: number[] = []
    const v3RangeTimes: number[] = []

    for (let i = 0; i < iterations; i++) {
      const fileBuffer = await fs.readFile(v2Path)
      const file = {
        byteLength: fileBuffer.byteLength,
        slice: (start: number, end?: number) => fileBuffer.slice(start, end).buffer,
      }
      const t = startTimer()
      await parquetQuery({
        file,
        filter: { startYear: { $gt: 2020 } },
        compressors,
      })
      v2RangeTimes.push(t.stop().elapsed())
    }

    for (let i = 0; i < iterations; i++) {
      const fileBuffer = await fs.readFile(v3Path)
      const file = {
        byteLength: fileBuffer.byteLength,
        slice: (start: number, end?: number) => fileBuffer.slice(start, end).buffer,
      }
      const t = startTimer()
      await parquetQuery({
        file,
        filter: { '$index_startYear': { $gt: 2020 } },
        compressors,
      })
      v3RangeTimes.push(t.stop().elapsed())
    }

    console.log(`  V2 (Native): ${avg(v2RangeTimes).toFixed(1)}ms`)
    console.log(`  V3 (Dual):   ${avg(v3RangeTimes).toFixed(1)}ms`)
  }

  // Cleanup
  console.log('\n' + '='.repeat(80))
  console.log('Benchmark complete!')
  console.log('='.repeat(80))
}

// Run if executed directly
runBenchmark().catch(console.error)
