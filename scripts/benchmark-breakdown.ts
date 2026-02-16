/**
 * Performance breakdown: Where is the time spent in hyparquet?
 */

import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

const SF1_LINEITEM = 'data/tpch/lineitem-sf1.parquet'

async function main() {
  console.log('=== Performance Breakdown ===\n')

  // 1. File read time
  const fileStart = performance.now()
  const buffer = await readFile(SF1_LINEITEM)
  const fileTime = performance.now() - fileStart
  console.log(`1. File read: ${fileTime.toFixed(0)}ms (${(buffer.length / 1024 / 1024).toFixed(1)}MB)`)

  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // 2. Metadata parse time
  const metaStart = performance.now()
  const metadata = await parquetMetadataAsync(arrayBuffer)
  const metaTime = performance.now() - metaStart
  console.log(`2. Metadata parse: ${metaTime.toFixed(0)}ms (${metadata.num_rows.toLocaleString()} rows, ${metadata.row_groups.length} row groups)`)

  // 3. Full parquet read time
  console.log('\n3. Full parquet decode timing breakdown:')

  // 3a. Just 4 columns (Q6 case)
  const q6Start = performance.now()
  const q6Data = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']
  }) as any[]
  const q6Time = performance.now() - q6Start
  console.log(`   - 4 columns (Q6): ${q6Time.toFixed(0)}ms for ${q6Data.length.toLocaleString()} rows`)

  // 3b. 6 columns (Q1 case)
  const q1Start = performance.now()
  const q1Data = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_shipdate']
  }) as any[]
  const q1Time = performance.now() - q1Start
  console.log(`   - 6 columns (Q1): ${q1Time.toFixed(0)}ms for ${q1Data.length.toLocaleString()} rows`)

  // 4. JS processing time (filter + aggregate)
  console.log('\n4. JavaScript processing time:')

  const filterStart = performance.now()
  const cutoff = new Date('1998-09-02')
  const filtered = q1Data.filter(r => r.l_shipdate <= cutoff)
  const filterTime = performance.now() - filterStart
  console.log(`   - Filter: ${filterTime.toFixed(0)}ms (${filtered.length.toLocaleString()} rows passed)`)

  const aggStart = performance.now()
  const groups = new Map<string, { sum_qty: number; count: number }>()
  for (const row of filtered) {
    const key = `${row.l_returnflag}|${row.l_linestatus}`
    let g = groups.get(key)
    if (!g) { g = { sum_qty: 0, count: 0 }; groups.set(key, g) }
    g.sum_qty += row.l_quantity
    g.count++
  }
  const aggTime = performance.now() - aggStart
  console.log(`   - Aggregate: ${aggTime.toFixed(0)}ms (${groups.size} groups)`)

  // Summary
  console.log('\n=== Summary ===')
  console.log(`Total Q1 time: ${(q1Time + filterTime + aggTime).toFixed(0)}ms`)
  console.log(`  - Parquet decode: ${((q1Time / (q1Time + filterTime + aggTime)) * 100).toFixed(1)}%`)
  console.log(`  - JS processing: ${(((filterTime + aggTime) / (q1Time + filterTime + aggTime)) * 100).toFixed(1)}%`)

  // Memory usage
  const used = process.memoryUsage()
  console.log('\nMemory usage:')
  console.log(`  - Heap used: ${(used.heapUsed / 1024 / 1024).toFixed(1)}MB`)
  console.log(`  - Heap total: ${(used.heapTotal / 1024 / 1024).toFixed(1)}MB`)
  console.log(`  - RSS: ${(used.rss / 1024 / 1024).toFixed(1)}MB`)
}

main().catch(console.error)
