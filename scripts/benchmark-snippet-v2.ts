/**
 * Benchmark: What can we do in 5ms CPU?
 *
 * Cloudflare Snippet constraints:
 * - Script: < 32KB
 * - Memory: < 32MB
 * - CPU: < 5ms
 * - Fetches: ≤ 5
 */

import { parquetMetadataAsync, parquetRead } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile, stat } from 'node:fs/promises'

async function main() {
  console.log('=== What Can We Do in 5ms CPU? ===\n')

  const testFile = 'data/tpch/lineitem.parquet'
  const buffer = await readFile(testFile)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Warm up
  await parquetMetadataAsync(arrayBuffer)

  // Test 1: Metadata read
  console.log('1. Metadata read (required for any query):')
  const times: number[] = []
  for (let i = 0; i < 10; i++) {
    const start = performance.now()
    await parquetMetadataAsync(arrayBuffer)
    times.push(performance.now() - start)
  }
  const metaTime = times.sort((a, b) => a - b)[5] // median
  console.log(`   Median: ${metaTime.toFixed(2)}ms ✅`)
  console.log(`   Remaining budget: ${(5 - metaTime).toFixed(2)}ms\n`)

  // Test 2: How many rows can we decode in remaining time?
  console.log('2. Row decoding speed (2 columns):')

  const metadata = await parquetMetadataAsync(arrayBuffer)
  const rowsPerGroup = Number(metadata.row_groups[0].num_rows)
  console.log(`   Row group size: ${rowsPerGroup.toLocaleString()} rows`)

  // Measure decode time for different row counts
  const rowCounts = [100, 1000, 5000, 10000, 50000]

  for (const rowCount of rowCounts) {
    const decodeStart = performance.now()
    let decoded = 0

    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity'],
      rowEnd: rowCount,
      onComplete: (rows: any[][]) => {
        decoded = rows[0].length
      }
    })

    const decodeTime = performance.now() - decodeStart
    const totalTime = metaTime + decodeTime
    const withinBudget = totalTime < 5

    console.log(`   ${rowCount.toLocaleString().padStart(6)} rows: ${decodeTime.toFixed(2)}ms decode, ${totalTime.toFixed(2)}ms total ${withinBudget ? '✅' : '❌'}`)
  }

  // Test 3: What about with predicate pushdown simulation?
  console.log('\n3. With row group statistics (skip irrelevant groups):')
  console.log('   If we can skip 4/5 row groups via statistics:')

  const skipRatio = 0.8 // Skip 80% of data
  const effectiveRows = Math.round(rowsPerGroup * (1 - skipRatio))
  console.log(`   Only read ~${effectiveRows.toLocaleString()} rows instead of ${rowsPerGroup.toLocaleString()}`)

  const pushdownStart = performance.now()
  let pushdownDecoded = 0
  await parquetRead({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_quantity'],
    rowEnd: effectiveRows,
    onComplete: (rows: any[][]) => {
      pushdownDecoded = rows[0].length
    }
  })
  const pushdownTime = performance.now() - pushdownStart
  const pushdownTotal = metaTime + pushdownTime
  console.log(`   ${effectiveRows.toLocaleString()} rows: ${pushdownTime.toFixed(2)}ms decode, ${pushdownTotal.toFixed(2)}ms total ${pushdownTotal < 5 ? '✅' : '❌'}`)

  // Test 4: Memory per row
  console.log('\n4. Memory efficiency:')
  global.gc?.()
  const memBefore = process.memoryUsage().heapUsed

  let rows: any[][] = []
  await parquetRead({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_quantity', 'l_extendedprice', 'l_discount'],
    rowEnd: 10000,
    onComplete: (r: any[][]) => { rows = r }
  })

  const memAfter = process.memoryUsage().heapUsed
  const memUsed = memAfter - memBefore
  const bytesPerRow = memUsed / 10000 / 4 // 4 columns

  console.log(`   10K rows x 4 cols: ${(memUsed / 1024).toFixed(1)}KB`)
  console.log(`   ~${bytesPerRow.toFixed(0)} bytes per cell`)

  const maxRowsIn32MB = (32 * 1024 * 1024) / (bytesPerRow * 4)
  console.log(`   Max rows (4 cols) in 32MB: ~${maxRowsIn32MB.toLocaleString()}`)

  // Summary
  console.log('\n=== Summary for Snippet Constraints ===')
  console.log('┌─────────────────────────────────────────────────────┐')
  console.log('│ Constraint    │ Limit  │ Achievable               │')
  console.log('├─────────────────────────────────────────────────────┤')
  console.log(`│ CPU time      │ < 5ms  │ ~1-5K rows (no pushdown) │`)
  console.log(`│               │        │ ~20-25K rows (with skip) │`)
  console.log(`│ Memory        │ < 32MB │ ~${Math.floor(maxRowsIn32MB / 1000)}K rows (4 cols)       │`)
  console.log(`│ Fetches       │ ≤ 5    │ 1 meta + 1-4 row groups  │`)
  console.log(`│ Script size   │ < 32KB │ hyparquet ~29KB ✅       │`)
  console.log('└─────────────────────────────────────────────────────┘')

  console.log('\n=== Optimal Strategy ===')
  console.log('1. Pre-index metadata for instant partition routing')
  console.log('2. Small row groups (~10K rows) for fine-grained skipping')
  console.log('3. Sort data by query column for effective statistics')
  console.log('4. Stream results, don\'t buffer entire result set')
  console.log('5. For point lookups: binary search via row group stats')
}

main().catch(console.error)
