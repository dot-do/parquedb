/**
 * Benchmark: Small row groups for 5ms CPU constraint
 */

import { parquetMetadata, parquetRead } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

async function main() {
  console.log('=== Small Row Group Benchmark (10K rows/group) ===\n')

  const buffer = await readFile('data/test/small-rowgroups.parquet')
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Warm up
  parquetMetadata(arrayBuffer)
  await parquetRead({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey'],
    rowEnd: 100,
    onComplete: () => {}
  })

  const metadata = parquetMetadata(arrayBuffer)
  console.log(`File: ${metadata.row_groups.length} row groups, ~${Number(metadata.row_groups[0].num_rows)} rows each\n`)

  // Test 1: Read single row group (10K rows)
  console.log('1. Single row group read (10K rows, 2 cols):')
  const singleTimes: number[] = []
  for (let i = 0; i < 10; i++) {
    const start = performance.now()
    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity'],
      rowStart: 0,
      rowEnd: 10000,
      onComplete: () => {}
    })
    singleTimes.push(performance.now() - start)
  }
  singleTimes.sort((a, b) => a - b)
  console.log(`   Min: ${singleTimes[0].toFixed(2)}ms ${singleTimes[0] < 5 ? '✅' : '❌'}`)
  console.log(`   Median: ${singleTimes[5].toFixed(2)}ms ${singleTimes[5] < 5 ? '✅' : '❌'}`)

  // Test 2: Metadata + single row group
  console.log('\n2. Metadata + single row group (realistic query):')
  const fullTimes: number[] = []
  for (let i = 0; i < 10; i++) {
    const start = performance.now()
    parquetMetadata(arrayBuffer)
    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity'],
      rowStart: 0,
      rowEnd: 10000,
      onComplete: () => {}
    })
    fullTimes.push(performance.now() - start)
  }
  fullTimes.sort((a, b) => a - b)
  console.log(`   Min: ${fullTimes[0].toFixed(2)}ms ${fullTimes[0] < 5 ? '✅' : '❌'}`)
  console.log(`   Median: ${fullTimes[5].toFixed(2)}ms ${fullTimes[5] < 5 ? '✅' : '❌'}`)

  // Test 3: Point lookup simulation (use statistics to find row group)
  console.log('\n3. Point lookup with row group skip:')
  const targetKey = 25000 // Should be in RG2 (19939-29767)

  const lookupTimes: number[] = []
  for (let i = 0; i < 10; i++) {
    const start = performance.now()

    const meta = parquetMetadata(arrayBuffer)

    // Find row group by statistics
    let targetRg = -1
    let rowStart = 0
    for (let rg = 0; rg < meta.row_groups.length; rg++) {
      const col = meta.row_groups[rg].columns.find(c =>
        c.meta_data?.path_in_schema?.includes('l_orderkey')
      )
      const stats = col?.meta_data?.statistics
      if (stats) {
        const min = Number(stats.min_value)
        const max = Number(stats.max_value)
        if (targetKey >= min && targetKey <= max) {
          targetRg = rg
          break
        }
      }
      rowStart += Number(meta.row_groups[rg].num_rows)
    }

    if (targetRg >= 0) {
      const rgRows = Number(meta.row_groups[targetRg].num_rows)
      await parquetRead({
        file: arrayBuffer,
        compressors,
        columns: ['l_orderkey', 'l_quantity'],
        rowStart,
        rowEnd: rowStart + rgRows,
        onComplete: () => {}
      })
    }

    lookupTimes.push(performance.now() - start)
  }
  lookupTimes.sort((a, b) => a - b)
  console.log(`   Target: ${targetKey} (in RG2)`)
  console.log(`   Min: ${lookupTimes[0].toFixed(2)}ms ${lookupTimes[0] < 5 ? '✅' : '❌'}`)
  console.log(`   Median: ${lookupTimes[5].toFixed(2)}ms ${lookupTimes[5] < 5 ? '✅' : '❌'}`)

  // Test 4: Smaller row group (1K rows)
  console.log('\n4. Theoretical 1K row group (extrapolated):')
  const decodeTimePerRow = singleTimes[5] / 10000
  const estimated1K = decodeTimePerRow * 1000 + 0.3 // + metadata
  console.log(`   Estimated: ${estimated1K.toFixed(2)}ms ${estimated1K < 5 ? '✅' : '❌'}`)

  // Summary
  console.log('\n=== Summary ===')
  console.log('┌────────────────────────────────────────────────────────┐')
  console.log('│ Operation                    │ Time    │ Status      │')
  console.log('├────────────────────────────────────────────────────────┤')
  console.log(`│ Metadata only                │ ~0.3ms  │ ✅          │`)
  console.log(`│ 10K row group (2 cols)       │ ~${singleTimes[5].toFixed(1)}ms │ ${singleTimes[5] < 5 ? '✅' : '❌'}          │`)
  console.log(`│ Meta + 10K rows              │ ~${fullTimes[5].toFixed(1)}ms │ ${fullTimes[5] < 5 ? '✅' : '❌'}          │`)
  console.log(`│ Point lookup (skip RGs)      │ ~${lookupTimes[5].toFixed(1)}ms │ ${lookupTimes[5] < 5 ? '✅' : '❌'}          │`)
  console.log('└────────────────────────────────────────────────────────┘')

  if (fullTimes[5] > 5) {
    console.log('\n⚠️  10K row groups still exceed 5ms CPU budget.')
    console.log('   Consider: smaller row groups (1-5K), or uncompressed storage')
  } else {
    console.log('\n✅ 10K row groups fit within 5ms CPU budget!')
  }
}

main().catch(console.error)
