/**
 * Benchmark hyparquet internals to find the bottleneck
 *
 * parquetQuery should be FASTER than parquetReadObjects with filters,
 * but our tests show it's 2-3x SLOWER. Let's find out why.
 */

import {
  parquetMetadataAsync,
  parquetRead,
  parquetReadObjects,
  parquetQuery,
} from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

const TEST_FILE = 'data/tpch/lineitem.parquet'

async function main() {
  console.log('=== hyparquet Internal Benchmark ===\n')

  const buffer = await readFile(TEST_FILE)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const metadata = await parquetMetadataAsync(arrayBuffer)
  console.log(`File: ${Number(metadata.num_rows).toLocaleString()} rows, ${metadata.row_groups.length} row groups\n`)

  // Warm up
  await parquetReadObjects({ file: arrayBuffer, compressors, rowEnd: 100 })

  // ==========================================================================
  // Test 1: Baseline - parquetReadObjects (full file)
  // ==========================================================================
  console.log('1. parquetReadObjects (full file, 3 cols):')
  let t1 = performance.now()
  const objRows = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${objRows.length.toLocaleString()}\n`)

  // ==========================================================================
  // Test 2: parquetQuery NO filter (should be same as parquetReadObjects)
  // ==========================================================================
  console.log('2. parquetQuery (NO filter, same cols):')
  t1 = performance.now()
  const queryNoFilter = await parquetQuery({
    file: arrayBuffer,
    compressors,
    metadata, // Pass cached metadata
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${queryNoFilter.length.toLocaleString()}\n`)

  // ==========================================================================
  // Test 3: parquetQuery WITH filter that matches ALL rows
  // ==========================================================================
  console.log('3. parquetQuery (filter matches ALL rows):')
  t1 = performance.now()
  const queryAllMatch = await parquetQuery({
    file: arrayBuffer,
    compressors,
    metadata,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
    filter: { l_quantity: { '>': 0 } }, // All rows have quantity > 0
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${queryAllMatch.length.toLocaleString()}\n`)

  // ==========================================================================
  // Test 4: parquetQuery WITH filter that matches ~10% of rows
  // ==========================================================================
  console.log('4. parquetQuery (filter matches ~10%):')
  t1 = performance.now()
  const query10pct = await parquetQuery({
    file: arrayBuffer,
    compressors,
    metadata,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
    filter: { l_quantity: { '<': 5 } }, // ~10% of rows
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${query10pct.length.toLocaleString()}\n`)

  // ==========================================================================
  // Test 5: parquetQuery WITH limit (early exit)
  // ==========================================================================
  console.log('5. parquetQuery with limit=100:')
  t1 = performance.now()
  const queryLimit = await parquetQuery({
    file: arrayBuffer,
    compressors,
    metadata,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
    limit: 100,
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${queryLimit.length}\n`)

  // ==========================================================================
  // Test 6: parquetQuery with filter AND limit
  // ==========================================================================
  console.log('6. parquetQuery (filter + limit=100):')
  t1 = performance.now()
  const queryFilterLimit = await parquetQuery({
    file: arrayBuffer,
    compressors,
    metadata,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
    filter: { l_quantity: { '<': 5 } },
    limit: 100,
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${queryFilterLimit.length}\n`)

  // ==========================================================================
  // Test 7: Manual single row group read (best case)
  // ==========================================================================
  console.log('7. Manual single row group read:')
  t1 = performance.now()
  let singleRgRows = 0
  await parquetRead({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_quantity', 'l_discount'],
    rowEnd: Number(metadata.row_groups[0].num_rows),
    onComplete: (cols: unknown[][]) => {
      singleRgRows = cols[0].length
    }
  })
  console.log(`   Time: ${(performance.now() - t1).toFixed(2)}ms, Rows: ${singleRgRows.toLocaleString()}\n`)

  // ==========================================================================
  // Test 8: Check if row group skipping works
  // ==========================================================================
  console.log('8. Row group statistics check:')
  for (let i = 0; i < metadata.row_groups.length; i++) {
    const rg = metadata.row_groups[i]
    const qtyCol = rg.columns.find(c => c.meta_data?.path_in_schema?.includes('l_quantity'))
    const stats = qtyCol?.meta_data?.statistics
    if (stats) {
      console.log(`   RG${i}: l_quantity min=${stats.min_value}, max=${stats.max_value}`)
    } else {
      console.log(`   RG${i}: NO STATISTICS`)
    }
  }

  // ==========================================================================
  // Summary
  // ==========================================================================
  console.log('\n=== Summary ===')
  console.log('If parquetQuery is slower than parquetReadObjects, the bottleneck is:')
  console.log('1. Filter evaluation overhead (matchesFilter called per row)')
  console.log('2. Row group iteration overhead')
  console.log('3. Project/slice operations')
  console.log('4. No effective row group skipping (stats not used or not present)')
}

main().catch(console.error)
