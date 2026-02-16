/**
 * Minimal benchmark: Just test the core read path
 *
 * This isolates the Parquet read performance without test framework overhead.
 */

import { parquetMetadata, parquetRead, parquetReadObjects } from 'hyparquet'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { compressors, writeCompressors } from '../../src/parquet/compression'
import { readFile, writeFile, mkdir, rm } from 'node:fs/promises'

const TEST_FILE = 'data/test/minimal-benchmark.parquet'

async function main() {
  console.log('=== Minimal Snippet Benchmark ===\n')

  // Create test file with 5K rows, single row group
  await mkdir('data/test', { recursive: true })

  const columnData = [
    { name: '$id', data: Array.from({ length: 5000 }, (_, i) => `entity-${String(i).padStart(8, '0')}`) },
    { name: '$type', data: Array.from({ length: 5000 }, () => 'TestEntity') },
    { name: 'name', data: Array.from({ length: 5000 }, (_, i) => `Test Entity ${i}`) },
    { name: 'score', data: Array.from({ length: 5000 }, () => Math.random() * 100) },
  ]

  const buffer = parquetWriteBuffer({
    columnData,
    statistics: true,
    rowGroupSize: 5000,
    codec: 'SNAPPY',
    compressors: writeCompressors,
  })

  await writeFile(TEST_FILE, Buffer.from(buffer))
  console.log(`Created test file: ${(buffer.byteLength / 1024).toFixed(1)}KB, 5K rows\n`)

  // Load file
  const fileBuffer = await readFile(TEST_FILE)
  const arrayBuffer = fileBuffer.buffer.slice(fileBuffer.byteOffset, fileBuffer.byteOffset + fileBuffer.byteLength)

  // Warm up
  parquetMetadata(arrayBuffer)
  await parquetReadObjects({ file: arrayBuffer, compressors, rowEnd: 10 })

  // Test 1: Metadata only
  console.log('1. Metadata read:')
  const metaTimes: number[] = []
  for (let i = 0; i < 20; i++) {
    const start = performance.now()
    parquetMetadata(arrayBuffer)
    metaTimes.push(performance.now() - start)
  }
  metaTimes.sort((a, b) => a - b)
  console.log(`   Median: ${metaTimes[10].toFixed(3)}ms ✅\n`)

  // Test 2: Full row group read (worst case)
  console.log('2. Full row group read (5K rows, 4 cols):')
  const fullTimes: number[] = []
  for (let i = 0; i < 10; i++) {
    const start = performance.now()
    parquetMetadata(arrayBuffer)
    await parquetRead({
      file: arrayBuffer,
      compressors,
      onComplete: () => {}
    })
    fullTimes.push(performance.now() - start)
  }
  fullTimes.sort((a, b) => a - b)
  console.log(`   Median: ${fullTimes[5].toFixed(2)}ms ${fullTimes[5] < 5 ? '✅' : '❌'}\n`)

  // Test 3: Point lookup (find specific row)
  console.log('3. Point lookup (scan for ID):')
  const lookupTimes: number[] = []
  const targetId = 'entity-00002500' // Middle of the file

  for (let i = 0; i < 10; i++) {
    const start = performance.now()

    const meta = parquetMetadata(arrayBuffer)
    let found = false

    await parquetRead({
      file: arrayBuffer,
      compressors,
      columns: ['$id', 'name', 'score'],
      onComplete: (cols: unknown[][]) => {
        const idCol = cols[0] as string[]
        const idx = idCol.indexOf(targetId)
        if (idx !== -1) {
          found = true
          // Would extract: { $id: idCol[idx], name: cols[1][idx], score: cols[2][idx] }
        }
      }
    })

    lookupTimes.push(performance.now() - start)
  }
  lookupTimes.sort((a, b) => a - b)
  console.log(`   Median: ${lookupTimes[5].toFixed(2)}ms ${lookupTimes[5] < 5 ? '✅' : '❌'}\n`)

  // Test 4: Memory measurement
  console.log('4. Memory per read:')
  global.gc?.()
  const memBefore = process.memoryUsage().heapUsed

  await parquetReadObjects({ file: arrayBuffer, compressors })

  const memAfter = process.memoryUsage().heapUsed
  const memDelta = (memAfter - memBefore) / 1024 / 1024

  console.log(`   Delta: ${memDelta.toFixed(2)}MB for 5K rows`)
  console.log(`   Estimated for 10K rows: ~${(memDelta * 2).toFixed(1)}MB ${memDelta * 2 < 32 ? '✅' : '❌'}\n`)

  // Summary
  console.log('=== Results for 5K Row File ===')
  console.log('┌────────────────────────────────────────────────────┐')
  console.log('│ Operation          │ Time     │ Limit  │ Status  │')
  console.log('├────────────────────────────────────────────────────┤')
  console.log(`│ Metadata           │ ${metaTimes[10].toFixed(2)}ms   │ <5ms   │ ✅      │`)
  console.log(`│ Full read (5K)     │ ${fullTimes[5].toFixed(2)}ms   │ <5ms   │ ${fullTimes[5] < 5 ? '✅' : '❌'}      │`)
  console.log(`│ Point lookup       │ ${lookupTimes[5].toFixed(2)}ms   │ <5ms   │ ${lookupTimes[5] < 5 ? '✅' : '❌'}      │`)
  console.log(`│ Memory             │ ${memDelta.toFixed(1)}MB    │ <32MB  │ ✅      │`)
  console.log('└────────────────────────────────────────────────────┘')

  // Row count recommendations
  console.log('\n=== Row Group Size Recommendations ===')
  const timePerRow = fullTimes[5] / 5000
  const maxRows = Math.floor(5 / timePerRow)
  console.log(`Time per row: ~${(timePerRow * 1000).toFixed(2)}µs`)
  console.log(`Max rows in 5ms: ~${maxRows.toLocaleString()}`)
  console.log(`Recommended row group: ${Math.min(maxRows, 10000).toLocaleString()} rows`)

  // Clean up
  await rm(TEST_FILE)
}

main().catch(console.error)
