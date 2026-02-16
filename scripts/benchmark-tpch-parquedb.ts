/**
 * TPC-H Benchmark for ParqueDB
 *
 * Compares ParqueDB performance against DuckDB baseline
 */

import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

interface LineItem {
  l_returnflag: string
  l_linestatus: string
  l_quantity: number
  l_extendedprice: number
  l_discount: number
  l_shipdate: Date
}

// Load parquet file and run aggregation
async function runQ1ParqueDB(): Promise<{ result: unknown; timeMs: number }> {
  const start = performance.now()

  // Read lineitem parquet
  const buffer = await readFile('data/tpch/lineitem.parquet')
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Get metadata first
  const metadata = await parquetMetadataAsync(arrayBuffer)
  console.log(`Lineitem: ${metadata.num_rows} rows, ${metadata.row_groups.length} row groups`)

  // Read all data using parquetReadObjects (returns array of row objects)
  const rows = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_shipdate'],
  }) as LineItem[]

  const readTime = performance.now() - start
  console.log(`Read ${rows.length} rows in ${readTime.toFixed(1)}ms`)

  // Filter: l_shipdate <= '1998-09-02'
  const cutoffDate = new Date('1998-09-02')
  const filtered = rows.filter(r => {
    const shipdate = r.l_shipdate as Date
    return shipdate <= cutoffDate
  })

  const filterTime = performance.now() - start
  console.log(`Filtered to ${filtered.length} rows in ${(filterTime - readTime).toFixed(1)}ms`)

  // Group by l_returnflag, l_linestatus and aggregate
  const groups = new Map<string, {
    sum_qty: number
    sum_base_price: number
    sum_disc_price: number
    count: number
  }>()

  for (const row of filtered) {
    const key = `${row.l_returnflag}|${row.l_linestatus}`
    let group = groups.get(key)
    if (!group) {
      group = { sum_qty: 0, sum_base_price: 0, sum_disc_price: 0, count: 0 }
      groups.set(key, group)
    }
    const qty = row.l_quantity as number
    const price = row.l_extendedprice as number
    const discount = row.l_discount as number

    group.sum_qty += qty
    group.sum_base_price += price
    group.sum_disc_price += price * (1 - discount)
    group.count++
  }

  const aggTime = performance.now() - start
  console.log(`Aggregated in ${(aggTime - filterTime).toFixed(1)}ms`)

  // Format results
  const results = Array.from(groups.entries())
    .map(([key, g]) => {
      const [returnflag, linestatus] = key.split('|')
      return {
        l_returnflag: returnflag,
        l_linestatus: linestatus,
        sum_qty: g.sum_qty,
        sum_base_price: g.sum_base_price,
        sum_disc_price: g.sum_disc_price,
        avg_qty: g.sum_qty / g.count,
        count_order: g.count,
      }
    })
    .sort((a, b) => a.l_returnflag.localeCompare(b.l_returnflag) || a.l_linestatus.localeCompare(b.l_linestatus))

  const totalTime = performance.now() - start

  return { result: results, timeMs: totalTime }
}

// Run multiple iterations
async function benchmark(iterations = 5) {
  console.log('=== ParqueDB TPC-H Q1 Benchmark ===\n')

  const times: number[] = []

  for (let i = 0; i < iterations; i++) {
    console.log(`\n--- Iteration ${i + 1} ---`)
    const { result, timeMs } = await runQ1ParqueDB()
    times.push(timeMs)

    if (i === 0) {
      console.log('\nResults:')
      console.table(result)
    }
  }

  // Statistics
  times.sort((a, b) => a - b)
  const min = times[0]
  const max = times[times.length - 1]
  const median = times[Math.floor(times.length / 2)]
  const avg = times.reduce((a, b) => a + b, 0) / times.length

  console.log('\n=== Summary ===')
  console.log(`Iterations: ${iterations}`)
  console.log(`Min:    ${min.toFixed(1)}ms`)
  console.log(`Max:    ${max.toFixed(1)}ms`)
  console.log(`Median: ${median.toFixed(1)}ms`)
  console.log(`Avg:    ${avg.toFixed(1)}ms`)
  console.log(`\nDuckDB baseline: ~26ms`)
  console.log(`ParqueDB/DuckDB ratio: ${(median / 26).toFixed(1)}x slower`)
}

benchmark().catch(console.error)
