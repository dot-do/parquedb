/**
 * Benchmark: Raw hyparquet vs ParqueDB wrapper
 *
 * Identify if our wrapping is adding overhead
 */

import { parquetMetadataAsync, parquetRead, parquetReadObjects, parquetQuery } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

const TEST_FILE = 'data/tpch/lineitem.parquet'

async function main() {
  console.log('=== Raw hyparquet vs Wrapper Performance ===\n')

  const buffer = await readFile(TEST_FILE)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Check data format first
  console.log('1. Data format check:')
  const sample = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_shipdate', 'l_quantity'],
    rowEnd: 3,
  })
  console.log('   Sample rows:')
  for (const row of sample) {
    console.log(`     l_orderkey: ${row.l_orderkey} (${typeof row.l_orderkey})`)
    console.log(`     l_shipdate: ${row.l_shipdate} (${typeof row.l_shipdate}, instanceof Date: ${row.l_shipdate instanceof Date})`)
    console.log(`     l_quantity: ${row.l_quantity} (${typeof row.l_quantity})`)
    console.log('')
  }

  // Warm up
  await parquetReadObjects({ file: arrayBuffer, compressors, rowEnd: 100 })

  const iterations = 5

  // ==========================================================================
  // Test 1: parquetReadObjects (row objects)
  // ==========================================================================
  console.log('2. parquetReadObjects (full file, 3 columns):')
  const readObjTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const rows = await parquetReadObjects({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
    })
    readObjTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rows.length.toLocaleString()}`)
  }
  readObjTimes.sort((a, b) => a - b)
  console.log(`   Median: ${readObjTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 2: parquetRead (columnar)
  // ==========================================================================
  console.log('3. parquetRead (columnar, 3 columns):')
  const readColTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    let rowCount = 0
    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
      onComplete: (cols: unknown[][]) => {
        rowCount = cols[0].length
      }
    })
    readColTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rowCount.toLocaleString()}`)
  }
  readColTimes.sort((a, b) => a - b)
  console.log(`   Median: ${readColTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 3: parquetQuery with filter
  // ==========================================================================
  console.log('4. parquetQuery with filter (l_quantity < 10):')
  const queryTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const rows = await parquetQuery({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
      filter: { l_quantity: { '<': 10 } },
    })
    queryTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Matching rows: ${rows.length.toLocaleString()}`)
  }
  queryTimes.sort((a, b) => a - b)
  console.log(`   Median: ${queryTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 4: parquetQuery with orderkey filter (point-ish lookup)
  // ==========================================================================
  console.log('5. parquetQuery point lookup (l_orderkey = 100000):')
  const pointTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const rows = await parquetQuery({
      file: arrayBuffer,
      compressors,
      filter: { l_orderkey: 100000 },
    })
    pointTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Matching rows: ${rows.length.toLocaleString()}`)
  }
  pointTimes.sort((a, b) => a - b)
  console.log(`   Median: ${pointTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 5: Manual row group skipping
  // ==========================================================================
  console.log('6. Manual row group skip (find l_orderkey = 100000):')
  const metadata = await parquetMetadataAsync(arrayBuffer)

  const manualTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()

    // Check row group statistics
    let targetRg = -1
    let rowStart = 0
    for (let rg = 0; rg < metadata.row_groups.length; rg++) {
      const col = metadata.row_groups[rg].columns.find(c =>
        c.meta_data?.path_in_schema?.includes('l_orderkey')
      )
      const stats = col?.meta_data?.statistics
      if (stats) {
        // Check if target could be in this row group
        // Note: stats values might be encoded differently
        targetRg = rg // For now, just scan first matching
        break
      }
      rowStart += Number(metadata.row_groups[rg].num_rows)
    }

    let found: unknown[] = []
    if (targetRg >= 0) {
      const rgRows = Number(metadata.row_groups[targetRg].num_rows)
      await parquetRead({
        file: arrayBuffer,
        compressors,
        rowStart,
        rowEnd: rowStart + rgRows,
        columns: ['l_orderkey', 'l_quantity'],
        onComplete: (cols: unknown[][]) => {
          const orderKeys = cols[0] as (number | bigint)[]
          for (let j = 0; j < orderKeys.length; j++) {
            if (Number(orderKeys[j]) === 100000) {
              found.push({ l_orderkey: orderKeys[j], l_quantity: cols[1][j] })
            }
          }
        }
      })
    }

    manualTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Found: ${found.length} rows in RG ${targetRg}`)
  }
  manualTimes.sort((a, b) => a - b)
  console.log(`   Median: ${manualTimes[2].toFixed(2)}ms (single row group)\n`)

  // ==========================================================================
  // Summary
  // ==========================================================================
  console.log('=== Summary ===\n')
  console.log('| Method | Median Time | Notes |')
  console.log('|--------|-------------|-------|')
  console.log(`| parquetReadObjects | ${readObjTimes[2].toFixed(0)}ms | Full file, row objects |`)
  console.log(`| parquetRead | ${readColTimes[2].toFixed(0)}ms | Full file, columnar |`)
  console.log(`| parquetQuery (filter) | ${queryTimes[2].toFixed(0)}ms | With predicate pushdown |`)
  console.log(`| parquetQuery (point) | ${pointTimes[2].toFixed(0)}ms | Equality filter |`)
  console.log(`| Manual RG skip | ${manualTimes[2].toFixed(0)}ms | Single row group read |`)

  console.log('\n### Analysis:')
  console.log(`Row object overhead: ${((readObjTimes[2] / readColTimes[2] - 1) * 100).toFixed(0)}% slower than columnar`)
  console.log(`parquetQuery vs full: ${(readObjTimes[2] / queryTimes[2]).toFixed(1)}x faster with pushdown`)
  console.log(`Manual RG skip: ${(readObjTimes[2] / manualTimes[2]).toFixed(1)}x faster than full scan`)
}

main().catch(console.error)
