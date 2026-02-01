/**
 * Variant Shredding Benchmark for ParqueDB
 *
 * Standalone benchmark that tests predicate pushdown performance.
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetQuery } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const BENCHMARK_DIR = '/tmp/parquedb-variant-benchmark'
const ROW_COUNTS = [10_000, 100_000, 500_000]
const ROWS_PER_GROUP = 50_000

const TITLE_TYPES = ['movie', 'tvSeries', 'short', 'tvEpisode', 'tvMovie', 'video']
const GENRES = ['Action', 'Drama', 'Comedy', 'Thriller', 'Horror', 'Romance', 'Sci-Fi', 'Documentary']

// =============================================================================
// Utilities
// =============================================================================

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

// =============================================================================
// Data Generation
// =============================================================================

function generateTitleRecords(count) {
  const records = []

  for (let i = 0; i < count; i++) {
    const titleType = i % 3 === 0 ? 'movie' : TITLE_TYPES[i % TITLE_TYPES.length]
    const startYear = 1900 + (i % 125)
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
      averageRating: 5 + (i % 50) / 10,
      numVotes: 100 + i * 10,
    })
  }

  return records
}

// =============================================================================
// File Writers
// =============================================================================

async function writeV1JsonFile(records, path) {
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

async function writeV2NativeFile(records, path) {
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

async function writeV3DualFile(records, path) {
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: records.map(r => r.id) },
      { name: '$index_titleType', type: 'STRING', data: records.map(r => r.titleType) },
      { name: '$index_startYear', type: 'INT32', data: records.map(r => r.startYear) },
      { name: '$data', type: 'STRING', data: records.map(r => JSON.stringify(r)) },
    ],
    rowGroupSize: ROWS_PER_GROUP,
    compression: 'LZ4_RAW',
  })

  await fs.writeFile(path, Buffer.from(buffer))
  return buffer.byteLength
}

// =============================================================================
// Query Functions
// =============================================================================

async function queryV1Json(path, targetType) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data'],
    compressors,
  })

  const filtered = rows.filter(row => {
    const data = JSON.parse(row.$data)
    return data.titleType === targetType
  })

  return {
    rowsReturned: filtered.length,
    queryTimeMs: performance.now() - start,
  }
}

async function queryV2Native(path, targetType) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    filter: { titleType: targetType },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: performance.now() - start,
  }
}

async function queryV3Dual(path, targetType) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data', '$index_titleType'],
    filter: { '$index_titleType': targetType },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: performance.now() - start,
  }
}

// Range query functions (for startYear range)
async function queryV1JsonRange(path, minYear, maxYear) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data'],
    compressors,
  })

  const filtered = rows.filter(row => {
    const data = JSON.parse(row.$data)
    return data.startYear >= minYear && data.startYear <= maxYear
  })

  return {
    rowsReturned: filtered.length,
    queryTimeMs: performance.now() - start,
  }
}

async function queryV2NativeRange(path, minYear, maxYear) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    filter: { startYear: { $gte: minYear, $lte: maxYear } },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: performance.now() - start,
  }
}

async function queryV3DualRange(path, minYear, maxYear) {
  const start = performance.now()
  const fileBuffer = await fs.readFile(path)
  const file = {
    byteLength: fileBuffer.byteLength,
    slice: (s, e) => {
      const buf = fileBuffer.slice(s, e)
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
    },
  }

  const rows = await parquetQuery({
    file,
    columns: ['$id', '$data', '$index_startYear'],
    filter: { '$index_startYear': { $gte: minYear, $lte: maxYear } },
    compressors,
  })

  return {
    rowsReturned: rows.length,
    queryTimeMs: performance.now() - start,
  }
}

// =============================================================================
// Main
// =============================================================================

async function runBenchmark() {
  console.log('='.repeat(80))
  console.log('Variant Shredding Benchmark')
  console.log('='.repeat(80))
  console.log()

  await fs.mkdir(BENCHMARK_DIR, { recursive: true })

  for (const rowCount of ROW_COUNTS) {
    console.log(`\n${'─'.repeat(80)}`)
    console.log(`Dataset: ${rowCount.toLocaleString()} records`)
    console.log('─'.repeat(80))

    console.log('Generating test data...')
    const genStart = performance.now()
    const records = generateTitleRecords(rowCount)
    console.log(`  Generated in ${(performance.now() - genStart).toFixed(0)}ms`)

    const movieCount = records.filter(r => r.titleType === 'movie').length
    console.log(`  Movies: ${movieCount.toLocaleString()} (${(movieCount / rowCount * 100).toFixed(1)}% selectivity)`)

    const v1Path = join(BENCHMARK_DIR, `v1-json-${rowCount}.parquet`)
    const v2Path = join(BENCHMARK_DIR, `v2-native-${rowCount}.parquet`)
    const v3Path = join(BENCHMARK_DIR, `v3-dual-${rowCount}.parquet`)

    console.log('\nWriting files...')

    let start = performance.now()
    const v1Size = await writeV1JsonFile(records, v1Path)
    console.log(`  V1 (JSON):  ${formatBytes(v1Size).padStart(12)} in ${(performance.now() - start).toFixed(0)}ms`)

    start = performance.now()
    const v2Size = await writeV2NativeFile(records, v2Path)
    console.log(`  V2 (Native): ${formatBytes(v2Size).padStart(11)} in ${(performance.now() - start).toFixed(0)}ms`)

    start = performance.now()
    const v3Size = await writeV3DualFile(records, v3Path)
    console.log(`  V3 (Dual):   ${formatBytes(v3Size).padStart(11)} in ${(performance.now() - start).toFixed(0)}ms`)

    console.log(`  Storage overhead (V3 vs V1): ${((v3Size - v1Size) / v1Size * 100).toFixed(1)}%`)

    console.log('\nQuerying: { titleType: "movie" }')
    console.log()

    const iterations = 5
    const avg = arr => arr.reduce((a, b) => a + b, 0) / arr.length
    const min = arr => Math.min(...arr)

    // V1 benchmarks
    const v1Times = []
    let v1Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV1Json(v1Path, 'movie')
      v1Times.push(result.queryTimeMs)
      v1Rows = result.rowsReturned
    }

    // V2 benchmarks
    const v2Times = []
    let v2Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV2Native(v2Path, 'movie')
      v2Times.push(result.queryTimeMs)
      v2Rows = result.rowsReturned
    }

    // V3 benchmarks
    const v3Times = []
    let v3Rows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV3Dual(v3Path, 'movie')
      v3Times.push(result.queryTimeMs)
      v3Rows = result.rowsReturned
    }

    const v1Avg = avg(v1Times)
    const v2Avg = avg(v2Times)
    const v3Avg = avg(v3Times)

    console.log('  V1 (JSON - no pushdown):')
    console.log(`    Avg: ${v1Avg.toFixed(1)}ms  Min: ${min(v1Times).toFixed(1)}ms  Rows: ${v1Rows.toLocaleString()}`)

    console.log('  V2 (Native - full pushdown):')
    console.log(`    Avg: ${v2Avg.toFixed(1)}ms  Min: ${min(v2Times).toFixed(1)}ms  Rows: ${v2Rows.toLocaleString()}`)

    console.log('  V3 (Dual - $index pushdown):')
    console.log(`    Avg: ${v3Avg.toFixed(1)}ms  Min: ${min(v3Times).toFixed(1)}ms  Rows: ${v3Rows.toLocaleString()}`)

    console.log()
    console.log(`  SPEEDUP V2 vs V1: ${(v1Avg / v2Avg).toFixed(1)}x faster`)
    console.log(`  SPEEDUP V3 vs V1: ${(v1Avg / v3Avg).toFixed(1)}x faster`)
    console.log(`  V3 vs V2 overhead: ${((v3Avg / v2Avg - 1) * 100).toFixed(0)}%`)

    // Range query test (low selectivity - should benefit from row group statistics)
    const targetYearMin = 2020
    const targetYearMax = 2024
    const yearCount = records.filter(r => r.startYear >= targetYearMin && r.startYear <= targetYearMax).length

    console.log(`\nQuerying: { startYear: { $gte: ${targetYearMin}, $lte: ${targetYearMax} } }`)
    console.log(`  Expected rows: ${yearCount.toLocaleString()} (${(yearCount / rowCount * 100).toFixed(1)}% selectivity)`)
    console.log()

    // V1 range benchmarks
    const v1RangeTimes = []
    let v1RangeRows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV1JsonRange(v1Path, targetYearMin, targetYearMax)
      v1RangeTimes.push(result.queryTimeMs)
      v1RangeRows = result.rowsReturned
    }

    // V2 range benchmarks
    const v2RangeTimes = []
    let v2RangeRows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV2NativeRange(v2Path, targetYearMin, targetYearMax)
      v2RangeTimes.push(result.queryTimeMs)
      v2RangeRows = result.rowsReturned
    }

    // V3 range benchmarks
    const v3RangeTimes = []
    let v3RangeRows = 0
    for (let i = 0; i < iterations; i++) {
      const result = await queryV3DualRange(v3Path, targetYearMin, targetYearMax)
      v3RangeTimes.push(result.queryTimeMs)
      v3RangeRows = result.rowsReturned
    }

    const v1RangeAvg = avg(v1RangeTimes)
    const v2RangeAvg = avg(v2RangeTimes)
    const v3RangeAvg = avg(v3RangeTimes)

    console.log('  V1 (JSON - no pushdown):')
    console.log(`    Avg: ${v1RangeAvg.toFixed(1)}ms  Min: ${min(v1RangeTimes).toFixed(1)}ms  Rows: ${v1RangeRows.toLocaleString()}`)

    console.log('  V2 (Native - row group stats):')
    console.log(`    Avg: ${v2RangeAvg.toFixed(1)}ms  Min: ${min(v2RangeTimes).toFixed(1)}ms  Rows: ${v2RangeRows.toLocaleString()}`)

    console.log('  V3 (Dual - $index row group stats):')
    console.log(`    Avg: ${v3RangeAvg.toFixed(1)}ms  Min: ${min(v3RangeTimes).toFixed(1)}ms  Rows: ${v3RangeRows.toLocaleString()}`)

    console.log()
    console.log(`  SPEEDUP V2 vs V1: ${(v1RangeAvg / v2RangeAvg).toFixed(1)}x faster`)
    console.log(`  SPEEDUP V3 vs V1: ${(v1RangeAvg / v3RangeAvg).toFixed(1)}x faster`)
    console.log(`  V3 vs V2 overhead: ${((v3RangeAvg / v2RangeAvg - 1) * 100).toFixed(0)}%`)
  }

  console.log('\n' + '='.repeat(80))
  console.log('Benchmark complete!')
  console.log('='.repeat(80))
}

runBenchmark().catch(console.error)
