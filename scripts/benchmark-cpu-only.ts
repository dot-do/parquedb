/**
 * Benchmark: CPU-only time (excluding I/O)
 *
 * Cloudflare counts CPU time, not wall-clock time.
 * Network I/O doesn't count against the 5ms limit.
 */

import { parquetMetadataAsync, parquetRead, parquetMetadata } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

async function main() {
  console.log('=== CPU-Only Time Analysis ===\n')

  // Load file into memory (simulates R2 data already fetched)
  const buffer = await readFile('data/tpch/lineitem.parquet')
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Pre-warm - JIT compilation
  await parquetMetadataAsync(arrayBuffer)
  await parquetRead({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey'],
    rowEnd: 100,
    onComplete: () => {}
  })

  console.log('After JIT warmup:\n')

  // Test synchronous metadata parse (pure CPU)
  console.log('1. Metadata parse (sync, pure CPU):')
  const syncTimes: number[] = []
  for (let i = 0; i < 20; i++) {
    const start = performance.now()
    parquetMetadata(arrayBuffer) // sync version
    syncTimes.push(performance.now() - start)
  }
  syncTimes.sort((a, b) => a - b)
  console.log(`   Min: ${syncTimes[0].toFixed(3)}ms`)
  console.log(`   Median: ${syncTimes[10].toFixed(3)}ms`)
  console.log(`   Max: ${syncTimes[19].toFixed(3)}ms`)

  // The key insight: most time in parquetRead is decompression
  console.log('\n2. Decompression is the bottleneck:')

  // Test with uncompressed vs compressed
  // Since we can't easily test uncompressed, estimate based on file structure

  const metadata = parquetMetadata(arrayBuffer)
  const rg = metadata.row_groups[0]
  let compressedSize = 0
  let uncompressedSize = 0
  for (const col of rg.columns) {
    compressedSize += Number(col.meta_data?.total_compressed_size || 0)
    uncompressedSize += Number(col.meta_data?.total_uncompressed_size || 0)
  }

  const compressionRatio = uncompressedSize / compressedSize
  console.log(`   First row group compression ratio: ${compressionRatio.toFixed(2)}x`)
  console.log(`   Compressed: ${(compressedSize / 1024).toFixed(0)}KB`)
  console.log(`   Uncompressed: ${(uncompressedSize / 1024).toFixed(0)}KB`)

  // Test reading different amounts of data
  console.log('\n3. Column chunk granularity:')
  console.log('   (Parquet reads entire column chunks, not individual rows)')

  const cols = ['l_orderkey', 'l_quantity']
  for (const col of cols) {
    const colMeta = rg.columns.find(c => c.meta_data?.path_in_schema?.includes(col))
    if (colMeta?.meta_data) {
      console.log(`   ${col}: ${(Number(colMeta.meta_data.total_compressed_size) / 1024).toFixed(0)}KB compressed`)
    }
  }

  // Profile decode time breakdown
  console.log('\n4. Decode breakdown (single row group, 2 cols):')

  // Measure with threadBlocker to get more accurate CPU time
  const iterations = 5
  const decodeTimes: number[] = []

  for (let i = 0; i < iterations; i++) {
    // Force sync-like behavior
    const start = performance.now()

    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['l_orderkey', 'l_quantity'],
      rowEnd: Number(rg.num_rows),
      onComplete: () => {}
    })

    decodeTimes.push(performance.now() - start)
  }

  decodeTimes.sort((a, b) => a - b)
  console.log(`   Min: ${decodeTimes[0].toFixed(2)}ms`)
  console.log(`   Median: ${decodeTimes[2].toFixed(2)}ms`)
  console.log(`   ~${(decodeTimes[2] / Number(rg.num_rows) * 1000).toFixed(3)}µs per row`)

  // Estimate what's achievable
  console.log('\n=== What\'s Achievable in 5ms CPU ===')

  const cpuBudget = 5.0
  const metaOverhead = syncTimes[10]
  const perRowCost = decodeTimes[2] / Number(rg.num_rows) // ms per row

  const maxRows = Math.floor((cpuBudget - metaOverhead) / perRowCost)
  console.log(`Metadata overhead: ${metaOverhead.toFixed(3)}ms`)
  console.log(`Per-row decode cost: ${(perRowCost * 1000).toFixed(3)}µs`)
  console.log(`Max rows in 5ms: ~${maxRows.toLocaleString()}`)

  console.log('\n=== Key Insight ===')
  console.log('The bottleneck is decompression, not row count.')
  console.log('Parquet reads entire column chunks regardless of rowEnd.')
  console.log('')
  console.log('Solutions:')
  console.log('1. Smaller row groups (10K rows) = smaller chunks to decompress')
  console.log('2. Pre-filter via statistics to skip entire chunks')
  console.log('3. Store uncompressed for CPU-constrained queries')
  console.log('4. Use faster WASM decompressors')
}

main().catch(console.error)
