/**
 * Benchmark: Cloudflare Snippet Constraints
 *
 * Target constraints:
 * - Script: < 32KB
 * - Memory: < 32MB
 * - CPU: < 5ms
 * - Fetches: ≤ 5
 * - File size: < 25MB (free static assets)
 */

import { parquetMetadataAsync, parquetRead } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile, stat } from 'node:fs/promises'

// Simulate range request by slicing buffer
class SimulatedR2File {
  private buffer: ArrayBuffer
  private fetchCount = 0
  private bytesRead = 0

  constructor(buffer: ArrayBuffer) {
    this.buffer = buffer
  }

  get byteLength() { return this.buffer.byteLength }

  async slice(start: number, end?: number): Promise<ArrayBuffer> {
    this.fetchCount++
    const slice = this.buffer.slice(start, end)
    this.bytesRead += slice.byteLength
    return slice
  }

  getStats() {
    return {
      fetchCount: this.fetchCount,
      bytesRead: this.bytesRead,
      bytesReadKB: (this.bytesRead / 1024).toFixed(1),
    }
  }
}

interface BenchmarkResult {
  query: string
  cpuMs: number
  memoryMB: number
  fetches: number
  bytesReadKB: number
  rowsScanned: number
  rowsReturned: number
  withinLimits: boolean
}

async function benchmarkPointLookup(file: SimulatedR2File, targetId: string): Promise<BenchmarkResult> {
  const startMem = process.memoryUsage().heapUsed
  const startCpu = performance.now()

  // 1. Get metadata (simulates 1-2 range requests for footer)
  const metadata = await parquetMetadataAsync(file)

  // 2. Find row groups that might contain our ID using statistics
  // In real implementation, we'd check min/max stats for $id column
  const matchingRowGroups: number[] = []
  for (let i = 0; i < metadata.row_groups.length; i++) {
    // For now, assume we need to check all (no stats available in this test file)
    // In production, parquet files would have min/max for $id column
    matchingRowGroups.push(i)
  }

  // 3. Read only matching row groups
  let found: any = null
  let rowsScanned = 0

  for (const rgIndex of matchingRowGroups) {
    if (found) break

    await parquetRead({
      file,
      compressors,
      rowStart: rgIndex > 0 ? metadata.row_groups.slice(0, rgIndex).reduce((sum, rg) => sum + Number(rg.num_rows), 0) : 0,
      rowEnd: metadata.row_groups.slice(0, rgIndex + 1).reduce((sum, rg) => sum + Number(rg.num_rows), 0),
      onComplete: (rows: any[][]) => {
        // rows is array of columns, each column is array of values
        const idCol = rows[0] // Assuming first column is ID
        rowsScanned += idCol.length

        for (let i = 0; i < idCol.length; i++) {
          if (idCol[i] === targetId) {
            found = {}
            for (let c = 0; c < rows.length; c++) {
              found[`col${c}`] = rows[c][i]
            }
            break
          }
        }
      }
    })
  }

  const cpuMs = performance.now() - startCpu
  const memoryMB = (process.memoryUsage().heapUsed - startMem) / 1024 / 1024
  const stats = file.getStats()

  return {
    query: `lookup(${targetId})`,
    cpuMs,
    memoryMB,
    fetches: stats.fetchCount,
    bytesReadKB: parseFloat(stats.bytesReadKB),
    rowsScanned,
    rowsReturned: found ? 1 : 0,
    withinLimits: cpuMs < 5 && memoryMB < 32 && stats.fetchCount <= 5
  }
}

async function benchmarkRangeQuery(
  file: SimulatedR2File,
  column: string,
  minVal: any,
  maxVal: any
): Promise<BenchmarkResult> {
  const startMem = process.memoryUsage().heapUsed
  const startCpu = performance.now()

  const metadata = await parquetMetadataAsync(file)

  const results: any[] = []
  let rowsScanned = 0

  // In production, we'd use row group statistics to skip non-matching groups
  await parquetRead({
    file,
    compressors,
    columns: [column],
    onComplete: (rows: any[][]) => {
      const col = rows[0]
      rowsScanned += col.length

      for (let i = 0; i < col.length; i++) {
        if (col[i] >= minVal && col[i] <= maxVal) {
          results.push(col[i])
        }
      }
    }
  })

  const cpuMs = performance.now() - startCpu
  const memoryMB = (process.memoryUsage().heapUsed - startMem) / 1024 / 1024
  const stats = file.getStats()

  return {
    query: `range(${column}, ${minVal}, ${maxVal})`,
    cpuMs,
    memoryMB,
    fetches: stats.fetchCount,
    bytesReadKB: parseFloat(stats.bytesReadKB),
    rowsScanned,
    rowsReturned: results.length,
    withinLimits: cpuMs < 5 && memoryMB < 32 && stats.fetchCount <= 5
  }
}

async function measureLibrarySize() {
  // Measure the size of the query library
  const files = [
    'node_modules/hyparquet/src/index.js',
    'node_modules/hyparquet/src/read.js',
    'node_modules/hyparquet/src/metadata.js',
    'node_modules/hyparquet/src/column.js',
    'node_modules/hyparquet/src/decode.js',
  ]

  let totalSize = 0
  for (const file of files) {
    try {
      const s = await stat(file)
      totalSize += s.size
    } catch {
      // File might not exist
    }
  }

  return totalSize
}

async function main() {
  console.log('=== Cloudflare Snippet Constraint Benchmark ===\n')

  // Check library size
  const libSize = await measureLibrarySize()
  console.log(`hyparquet core size: ~${(libSize / 1024).toFixed(1)}KB`)
  console.log(`Target: < 32KB script size\n`)

  // Test with small file (simulating <25MB partition)
  const testFile = 'data/tpch/lineitem.parquet' // 19MB, within 25MB limit
  const fileStat = await stat(testFile)
  console.log(`Test file: ${testFile}`)
  console.log(`File size: ${(fileStat.size / 1024 / 1024).toFixed(1)}MB (limit: 25MB)\n`)

  const buffer = await readFile(testFile)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Get metadata to understand structure
  const metadata = await parquetMetadataAsync(arrayBuffer)
  const numRows = Number(metadata.num_rows)
  console.log(`Rows: ${numRows.toLocaleString()}`)
  console.log(`Row groups: ${metadata.row_groups.length}`)
  console.log(`Avg rows per group: ${Math.round(numRows / metadata.row_groups.length).toLocaleString()}\n`)

  console.log('=== Constraint Limits ===')
  console.log('| Metric | Limit | Status |')
  console.log('|--------|-------|--------|')
  console.log(`| Script | < 32KB | ${libSize < 32000 ? '✅' : '❌'} ${(libSize / 1024).toFixed(1)}KB |`)
  console.log(`| File | < 25MB | ${fileStat.size < 25 * 1024 * 1024 ? '✅' : '❌'} ${(fileStat.size / 1024 / 1024).toFixed(1)}MB |`)

  console.log('\n=== Query Benchmarks ===')
  console.log('(Note: These are LOCAL benchmarks. R2 range requests add ~5-10ms network latency)\n')

  // Simulate different query patterns
  console.log('Testing single row group read...')

  // Read just first row group to measure minimal overhead
  const singleRgFile = new SimulatedR2File(arrayBuffer)
  const singleRgStart = performance.now()
  const singleRgMeta = await parquetMetadataAsync(singleRgFile)

  let singleRgRows = 0
  await parquetRead({
    file: singleRgFile,
    compressors,
    columns: ['l_orderkey', 'l_quantity'],
    rowEnd: Number(singleRgMeta.row_groups[0].num_rows),
    onComplete: (rows: any[][]) => {
      singleRgRows = rows[0].length
    }
  })
  const singleRgTime = performance.now() - singleRgStart
  const singleRgStats = singleRgFile.getStats()

  console.log(`\nSingle row group (${singleRgRows.toLocaleString()} rows, 2 columns):`)
  console.log(`  CPU: ${singleRgTime.toFixed(2)}ms ${singleRgTime < 5 ? '✅' : '❌'}`)
  console.log(`  Fetches: ${singleRgStats.fetchCount} ${singleRgStats.fetchCount <= 5 ? '✅' : '❌'}`)
  console.log(`  Bytes read: ${singleRgStats.bytesReadKB}KB`)

  // Test with metadata-only (for routing decisions)
  console.log('\nMetadata-only fetch:')
  const metaOnlyFile = new SimulatedR2File(arrayBuffer)
  const metaStart = performance.now()
  await parquetMetadataAsync(metaOnlyFile)
  const metaTime = performance.now() - metaStart
  const metaStats = metaOnlyFile.getStats()
  console.log(`  CPU: ${metaTime.toFixed(2)}ms ${metaTime < 5 ? '✅' : '❌'}`)
  console.log(`  Fetches: ${metaStats.fetchCount} ${metaStats.fetchCount <= 5 ? '✅' : '❌'}`)
  console.log(`  Bytes read: ${metaStats.bytesReadKB}KB`)

  // Memory baseline
  console.log('\n=== Memory Analysis ===')
  global.gc?.() // Force GC if available
  const baselineMem = process.memoryUsage()
  console.log(`Baseline heap: ${(baselineMem.heapUsed / 1024 / 1024).toFixed(1)}MB`)

  // Estimate per-row memory
  const rowSize = 8 * 16 // ~8 bytes per field, ~16 fields = 128 bytes/row
  const maxRowsIn32MB = (32 * 1024 * 1024) / rowSize
  console.log(`Estimated max rows in 32MB: ${maxRowsIn32MB.toLocaleString()}`)
  console.log(`Row group size: ${Number(singleRgMeta.row_groups[0].num_rows).toLocaleString()} rows`)
  console.log(`Can fit ~${Math.floor(maxRowsIn32MB / Number(singleRgMeta.row_groups[0].num_rows))} row groups in memory`)

  console.log('\n=== Recommendations ===')
  console.log('1. Partition files to < 25MB for free static hosting')
  console.log('2. Sort partitions by query column for effective row group skipping')
  console.log('3. Keep row groups small (~10K-50K rows) for granular filtering')
  console.log('4. Pre-compute metadata index for multi-file queries')
  console.log('5. Use streaming decode, process row group at a time')
}

main().catch(console.error)
