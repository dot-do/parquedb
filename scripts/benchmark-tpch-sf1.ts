/**
 * TPC-H Benchmark at SF1 (6M lineitem rows)
 *
 * Compares DuckDB vs ParqueDB (hyparquet) at scale
 */

import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'
import { execSync } from 'node:child_process'

const ITERATIONS = 3
const SF1_LINEITEM = 'data/tpch/lineitem-sf1.parquet'

// ============================================================================
// DuckDB Queries
// ============================================================================

function runDuckDB(query: string): { timeMs: number; rows: number } {
  const start = performance.now()
  const result = execSync(`duckdb -c "${query}"`, { encoding: 'utf-8' })
  const timeMs = performance.now() - start
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

  const lineitem = await loadParquet<any>(SF1_LINEITEM, [
    'l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_shipdate'
  ])

  const readTime = performance.now() - start
  console.log(`  Read ${lineitem.length.toLocaleString()} rows in ${readTime.toFixed(0)}ms`)

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

  const lineitem = await loadParquet<any>(SF1_LINEITEM, [
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

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

async function runBenchmarks(): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []

  // Get row count
  const metadata = await parquetMetadataAsync(
    (await readFile(SF1_LINEITEM)).buffer as ArrayBuffer
  )
  console.log(`\nSF1 Lineitem: ${metadata.num_rows.toLocaleString()} rows\n`)

  // Q1: Pricing Summary Report
  console.log('Running Q1: Pricing Summary Report...')
  let duckTimes: number[] = []
  let parquetTimes: number[] = []
  for (let i = 0; i < ITERATIONS; i++) {
    duckTimes.push(runDuckDB(`
      SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*)
      FROM '${SF1_LINEITEM}'
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
      FROM '${SF1_LINEITEM}'
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

  return results
}

async function main() {
  console.log('=================================================')
  console.log('TPC-H Benchmark: DuckDB vs ParqueDB (hyparquet)')
  console.log('Dataset: SF1 (~6M lineitem rows)')
  console.log('=================================================')

  const results = await runBenchmarks()

  console.log('\n=== Results ===\n')
  console.log('| Query | Description | DuckDB | ParqueDB | Ratio |')
  console.log('|-------|-------------|--------|----------|-------|')
  for (const r of results) {
    console.log(`| ${r.query} | ${r.description} | ${r.duckdb_ms.toFixed(0)}ms | ${r.parquet_ms.toFixed(0)}ms | ${r.ratio.toFixed(1)}x |`)
  }

  const avgRatio = results.reduce((a, r) => a + r.ratio, 0) / results.length
  console.log(`\n**Average: ParqueDB is ${avgRatio.toFixed(1)}x slower than DuckDB at SF1**`)

  // Compare to SF0.1
  console.log('\n=== Scale Comparison ===')
  console.log('SF0.1 (600K rows): ~8x slower')
  console.log(`SF1 (6M rows): ~${avgRatio.toFixed(1)}x slower`)
  console.log('\nThis shows how performance scales with data size.')
}

main().catch(console.error)
