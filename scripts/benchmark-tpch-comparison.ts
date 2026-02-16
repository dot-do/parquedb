/**
 * TPC-H Benchmark Comparison: DuckDB vs ParqueDB (hyparquet)
 *
 * Runs key TPC-H queries against both systems to understand ParqueDB constraints
 */

import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'
import { execSync } from 'node:child_process'

const ITERATIONS = 3

// ============================================================================
// DuckDB Queries
// ============================================================================

function runDuckDB(query: string): { timeMs: number; rows: number } {
  const start = performance.now()
  const result = execSync(`duckdb -c "${query}"`, { encoding: 'utf-8' })
  const timeMs = performance.now() - start
  // Count result rows (rough estimate from output)
  const lines = result.trim().split('\n').filter(l => l.startsWith('│'))
  return { timeMs, rows: lines.length }
}

// ============================================================================
// ParqueDB/hyparquet Queries
// ============================================================================

async function loadParquet<T>(path: string, columns?: string[]): Promise<T[]> {
  const buffer = await readFile(path)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
  return await parquetReadObjects({ file: arrayBuffer, compressors, columns }) as T[]
}

// Q1: Pricing Summary Report
async function runQ1Parquet(): Promise<{ timeMs: number; rows: number }> {
  const start = performance.now()

  const lineitem = await loadParquet<any>('data/tpch/lineitem.parquet', [
    'l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_shipdate'
  ])

  const cutoff = new Date('1998-09-02')
  const filtered = lineitem.filter(r => r.l_shipdate <= cutoff)

  const groups = new Map<string, { sum_qty: number; count: number }>()
  for (const row of filtered) {
    const key = `${row.l_returnflag}|${row.l_linestatus}`
    let g = groups.get(key)
    if (!g) { g = { sum_qty: 0, count: 0 }; groups.set(key, g) }
    g.sum_qty += row.l_quantity
    g.count++
  }

  return { timeMs: performance.now() - start, rows: groups.size }
}

// Q6: Forecasting Revenue Change (simple aggregation)
async function runQ6Parquet(): Promise<{ timeMs: number; rows: number }> {
  const start = performance.now()

  const lineitem = await loadParquet<any>('data/tpch/lineitem.parquet', [
    'l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice'
  ])

  const startDate = new Date('1994-01-01')
  const endDate = new Date('1995-01-01')

  let revenue = 0
  for (const row of lineitem) {
    if (row.l_shipdate >= startDate && row.l_shipdate < endDate &&
        row.l_discount >= 0.05 && row.l_discount <= 0.07 &&
        row.l_quantity < 24) {
      revenue += row.l_extendedprice * row.l_discount
    }
  }

  return { timeMs: performance.now() - start, rows: 1 }
}

// Q3: Shipping Priority (3-way join)
async function runQ3Parquet(): Promise<{ timeMs: number; rows: number }> {
  const start = performance.now()

  // Load all tables
  const [customer, orders, lineitem] = await Promise.all([
    loadParquet<any>('data/tpch/customer.parquet', ['c_custkey', 'c_mktsegment']),
    loadParquet<any>('data/tpch/orders.parquet', ['o_orderkey', 'o_custkey', 'o_orderdate', 'o_shippriority']),
    loadParquet<any>('data/tpch/lineitem.parquet', ['l_orderkey', 'l_extendedprice', 'l_discount', 'l_shipdate'])
  ])

  // Build indexes
  const buildingCustomers = new Set(
    customer.filter(c => c.c_mktsegment === 'BUILDING').map(c => c.c_custkey)
  )

  const cutoffDate = new Date('1995-03-15')
  const validOrders = new Map<number, any>()
  for (const o of orders) {
    if (buildingCustomers.has(o.o_custkey) && o.o_orderdate < cutoffDate) {
      validOrders.set(o.o_orderkey, o)
    }
  }

  // Aggregate
  const results = new Map<string, number>()
  for (const li of lineitem) {
    if (li.l_shipdate > cutoffDate && validOrders.has(li.l_orderkey)) {
      const o = validOrders.get(li.l_orderkey)
      const key = `${li.l_orderkey}|${o.o_orderdate}|${o.o_shippriority}`
      const revenue = li.l_extendedprice * (1 - li.l_discount)
      results.set(key, (results.get(key) || 0) + revenue)
    }
  }

  return { timeMs: performance.now() - start, rows: Math.min(results.size, 10) }
}

// Q12: Shipping Modes (2-way join)
async function runQ12Parquet(): Promise<{ timeMs: number; rows: number }> {
  const start = performance.now()

  const [orders, lineitem] = await Promise.all([
    loadParquet<any>('data/tpch/orders.parquet', ['o_orderkey', 'o_orderpriority']),
    loadParquet<any>('data/tpch/lineitem.parquet', ['l_orderkey', 'l_shipmode', 'l_commitdate', 'l_receiptdate', 'l_shipdate'])
  ])

  // Build order priority lookup
  const orderPriority = new Map<number, string>()
  for (const o of orders) {
    orderPriority.set(o.o_orderkey, o.o_orderpriority)
  }

  const startDate = new Date('1994-01-01')
  const endDate = new Date('1995-01-01')
  const modes = new Set(['MAIL', 'SHIP'])

  const results = new Map<string, { high: number; low: number }>()
  for (const li of lineitem) {
    if (modes.has(li.l_shipmode) &&
        li.l_commitdate < li.l_receiptdate &&
        li.l_shipdate < li.l_commitdate &&
        li.l_receiptdate >= startDate && li.l_receiptdate < endDate) {
      const priority = orderPriority.get(li.l_orderkey)
      if (priority) {
        let r = results.get(li.l_shipmode)
        if (!r) { r = { high: 0, low: 0 }; results.set(li.l_shipmode, r) }
        if (priority === '1-URGENT' || priority === '2-HIGH') {
          r.high++
        } else {
          r.low++
        }
      }
    }
  }

  return { timeMs: performance.now() - start, rows: results.size }
}

// ============================================================================
// Benchmark Runner
// ============================================================================

interface BenchmarkResult {
  query: string
  description: string
  duckdb_ms: number
  parquet_ms: number
  ratio: number
}

async function runBenchmarks(): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []

  // Q1: Pricing Summary Report
  console.log('\nRunning Q1: Pricing Summary Report...')
  let duckTimes: number[] = []
  let parquetTimes: number[] = []
  for (let i = 0; i < ITERATIONS; i++) {
    duckTimes.push(runDuckDB(`
      SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*)
      FROM 'data/tpch/lineitem.parquet'
      WHERE l_shipdate <= DATE '1998-09-02'
      GROUP BY 1,2 ORDER BY 1,2
    `).timeMs)
    parquetTimes.push((await runQ1Parquet()).timeMs)
  }
  results.push({
    query: 'Q1',
    description: 'Pricing Summary (aggregation)',
    duckdb_ms: median(duckTimes),
    parquet_ms: median(parquetTimes),
    ratio: median(parquetTimes) / median(duckTimes)
  })

  // Q6: Forecasting Revenue Change
  console.log('Running Q6: Forecasting Revenue Change...')
  duckTimes = []; parquetTimes = []
  for (let i = 0; i < ITERATIONS; i++) {
    duckTimes.push(runDuckDB(`
      SELECT sum(l_extendedprice * l_discount) as revenue
      FROM 'data/tpch/lineitem.parquet'
      WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
        AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24
    `).timeMs)
    parquetTimes.push((await runQ6Parquet()).timeMs)
  }
  results.push({
    query: 'Q6',
    description: 'Revenue Forecast (scan + filter)',
    duckdb_ms: median(duckTimes),
    parquet_ms: median(parquetTimes),
    ratio: median(parquetTimes) / median(duckTimes)
  })

  // Q3: Shipping Priority
  console.log('Running Q3: Shipping Priority...')
  duckTimes = []; parquetTimes = []
  for (let i = 0; i < ITERATIONS; i++) {
    duckTimes.push(runDuckDB(`
      SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
      FROM 'data/tpch/customer.parquet' c, 'data/tpch/orders.parquet' o, 'data/tpch/lineitem.parquet' l
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
      GROUP BY l_orderkey, o_orderdate, o_shippriority
      ORDER BY revenue DESC, o_orderdate LIMIT 10
    `).timeMs)
    parquetTimes.push((await runQ3Parquet()).timeMs)
  }
  results.push({
    query: 'Q3',
    description: 'Shipping Priority (3-way join)',
    duckdb_ms: median(duckTimes),
    parquet_ms: median(parquetTimes),
    ratio: median(parquetTimes) / median(duckTimes)
  })

  // Q12: Shipping Modes
  console.log('Running Q12: Shipping Modes...')
  duckTimes = []; parquetTimes = []
  for (let i = 0; i < ITERATIONS; i++) {
    duckTimes.push(runDuckDB(`
      SELECT l_shipmode,
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
      FROM 'data/tpch/orders.parquet' o, 'data/tpch/lineitem.parquet' l
      WHERE o_orderkey = l_orderkey AND l_shipmode in ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
        AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'
      GROUP BY l_shipmode ORDER BY l_shipmode
    `).timeMs)
    parquetTimes.push((await runQ12Parquet()).timeMs)
  }
  results.push({
    query: 'Q12',
    description: 'Shipping Modes (2-way join)',
    duckdb_ms: median(duckTimes),
    parquet_ms: median(parquetTimes),
    ratio: median(parquetTimes) / median(duckTimes)
  })

  return results
}

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

async function main() {
  console.log('=================================================')
  console.log('TPC-H Benchmark: DuckDB vs ParqueDB (hyparquet)')
  console.log('Dataset: SF0.1 (600K lineitem rows)')
  console.log('=================================================')

  const results = await runBenchmarks()

  console.log('\n=== Results ===\n')
  console.log('| Query | Description | DuckDB | ParqueDB | Ratio |')
  console.log('|-------|-------------|--------|----------|-------|')
  for (const r of results) {
    console.log(`| ${r.query} | ${r.description} | ${r.duckdb_ms.toFixed(0)}ms | ${r.parquet_ms.toFixed(0)}ms | ${r.ratio.toFixed(1)}x |`)
  }

  const avgRatio = results.reduce((a, r) => a + r.ratio, 0) / results.length
  console.log(`\n**Average: ParqueDB is ${avgRatio.toFixed(1)}x slower than DuckDB**`)

  console.log('\n=== Analysis ===')
  console.log('- DuckDB uses vectorized C++ execution with columnar processing')
  console.log('- ParqueDB (hyparquet) is pure JavaScript')
  console.log('- Main overhead: Parquet decompression and row materialization')
  console.log('- Join performance depends on hash table implementation')
}

main().catch(console.error)
