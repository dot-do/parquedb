/**
 * Overhead Analysis: Raw hyparquet vs ParqueDB patterns
 *
 * Identifies where time is spent in our query path
 */

import { parquetMetadataAsync, parquetRead, parquetReadObjects, parquetQuery } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile, stat } from 'node:fs/promises'

const TEST_FILE = 'data/tpch/lineitem.parquet'

// Simulate QueryExecutor's createAsyncBuffer pattern
async function createAsyncBufferWithStat(path: string) {
  // This is what QueryExecutor does: stat + create async buffer
  const fileStat = await stat(path)
  const buffer = await readFile(path)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  return {
    byteLength: fileStat.size,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      // Copy like QueryExecutor does
      const slice = arrayBuffer.slice(start, end)
      const copy = new ArrayBuffer(slice.byteLength)
      new Uint8Array(copy).set(new Uint8Array(slice))
      return copy
    }
  }
}

// Simpler approach: just use ArrayBuffer directly
async function createDirectBuffer(path: string) {
  const buffer = await readFile(path)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

async function main() {
  console.log('=== Overhead Analysis ===\n')

  // Warm up
  const warmBuffer = await createDirectBuffer(TEST_FILE)
  await parquetReadObjects({ file: warmBuffer, compressors, rowEnd: 100 })

  const iterations = 5

  // ==========================================================================
  // Test 1: Measure stat() overhead
  // ==========================================================================
  console.log('1. stat() call overhead:')
  const statTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await stat(TEST_FILE)
    statTimes.push(performance.now() - start)
  }
  statTimes.sort((a, b) => a - b)
  console.log(`   Median: ${statTimes[2].toFixed(3)}ms\n`)

  // ==========================================================================
  // Test 2: Direct ArrayBuffer read
  // ==========================================================================
  console.log('2. Direct ArrayBuffer read (3 cols):')
  const directTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const arrayBuffer = await createDirectBuffer(TEST_FILE)
    const rows = await parquetReadObjects({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
    })
    directTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rows.length.toLocaleString()}`)
  }
  directTimes.sort((a, b) => a - b)
  console.log(`   Median: ${directTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 3: With stat + async buffer (QueryExecutor pattern)
  // ==========================================================================
  console.log('3. With stat + AsyncBuffer pattern:')
  const asyncBufTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const asyncBuffer = await createAsyncBufferWithStat(TEST_FILE)
    const rows = await parquetReadObjects({
      file: asyncBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
    })
    asyncBufTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rows.length.toLocaleString()}`)
  }
  asyncBufTimes.sort((a, b) => a - b)
  console.log(`   Median: ${asyncBufTimes[2].toFixed(2)}ms`)
  console.log(`   Overhead vs direct: +${(asyncBufTimes[2] - directTimes[2]).toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 4: parquetQuery performance
  // ==========================================================================
  console.log('4. parquetQuery (no filter):')
  const queryNoFilterTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const arrayBuffer = await createDirectBuffer(TEST_FILE)
    const rows = await parquetQuery({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
    })
    queryNoFilterTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rows.length.toLocaleString()}`)
  }
  queryNoFilterTimes.sort((a, b) => a - b)
  console.log(`   Median: ${queryNoFilterTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 5: parquetQuery with filter
  // ==========================================================================
  console.log('5. parquetQuery with filter (l_quantity < 10):')
  const queryFilterTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const arrayBuffer = await createDirectBuffer(TEST_FILE)
    const rows = await parquetQuery({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
      filter: { l_quantity: { '<': 10 } },
    })
    queryFilterTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rows.length.toLocaleString()}`)
  }
  queryFilterTimes.sort((a, b) => a - b)
  console.log(`   Median: ${queryFilterTimes[2].toFixed(2)}ms\n`)

  // ==========================================================================
  // Test 6: parquetRead (columnar) vs parquetReadObjects
  // ==========================================================================
  console.log('6. parquetRead (columnar, same columns):')
  const columnarTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const arrayBuffer = await createDirectBuffer(TEST_FILE)
    let rowCount = 0
    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity', 'l_extendedprice'],
      onComplete: (cols: unknown[][]) => {
        rowCount = cols[0].length
      }
    })
    columnarTimes.push(performance.now() - start)
    if (i === 0) console.log(`   Rows: ${rowCount.toLocaleString()}`)
  }
  columnarTimes.sort((a, b) => a - b)
  console.log(`   Median: ${columnarTimes[2].toFixed(2)}ms`)
  console.log(`   Row object overhead: ${((directTimes[2] / columnarTimes[2] - 1) * 100).toFixed(0)}%\n`)

  // ==========================================================================
  // Test 7: JSON parsing overhead (simulating $data unpacking)
  // ==========================================================================
  console.log('7. JSON parse overhead (100K parses):')
  const sampleJson = JSON.stringify({ name: 'Test', value: 123, nested: { a: 1 } })
  const jsonParseTimes: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    for (let j = 0; j < 100000; j++) {
      JSON.parse(sampleJson)
    }
    jsonParseTimes.push(performance.now() - start)
  }
  jsonParseTimes.sort((a, b) => a - b)
  console.log(`   100K parses: ${jsonParseTimes[2].toFixed(2)}ms`)
  console.log(`   Per parse: ${(jsonParseTimes[2] / 100000 * 1000).toFixed(3)}µs\n`)

  // ==========================================================================
  // Summary
  // ==========================================================================
  console.log('=== Summary ===\n')
  console.log('| Operation | Time |')
  console.log('|-----------|------|')
  console.log(`| stat() | ${statTimes[2].toFixed(2)}ms |`)
  console.log(`| Direct ArrayBuffer read | ${directTimes[2].toFixed(0)}ms |`)
  console.log(`| AsyncBuffer pattern | ${asyncBufTimes[2].toFixed(0)}ms |`)
  console.log(`| parquetQuery (no filter) | ${queryNoFilterTimes[2].toFixed(0)}ms |`)
  console.log(`| parquetQuery (with filter) | ${queryFilterTimes[2].toFixed(0)}ms |`)
  console.log(`| parquetRead (columnar) | ${columnarTimes[2].toFixed(0)}ms |`)

  console.log('\n### Overhead Breakdown:')
  console.log(`- stat() call: ${statTimes[2].toFixed(2)}ms per query`)
  console.log(`- AsyncBuffer copy: ${(asyncBufTimes[2] - directTimes[2] - statTimes[2]).toFixed(2)}ms`)
  console.log(`- Row object creation: ${((directTimes[2] / columnarTimes[2] - 1) * 100).toFixed(0)}%`)
  console.log(`- parquetQuery vs parquetReadObjects: ${(queryNoFilterTimes[2] / directTimes[2]).toFixed(2)}x`)

  if (queryNoFilterTimes[2] > directTimes[2]) {
    console.log('\n⚠️  parquetQuery is SLOWER than parquetReadObjects!')
    console.log('   This suggests parquetQuery has internal overhead beyond just filtering.')
  }
}

main().catch(console.error)
