/**
 * Benchmark: Simulate Cloudflare Snippet constraints locally
 *
 * This validates that queries fit within:
 * - <5ms CPU time
 * - <32MB memory
 * - ≤5 fetch subrequests
 */

import { parquetMetadata, parquetRead, parquetReadObjects } from 'hyparquet'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { compressors, writeCompressors } from '../../src/parquet/compression'
import { readFile, writeFile, mkdir, rm } from 'node:fs/promises'
import { partitionById, type PartitionManifest } from './partition'
import { getById, createAsyncFile, type AsyncFile } from './query'
// Simple entity type for benchmarking (avoids branded type complexity)
interface TestEntity {
  $id: string
  $type: string
  name: string
  email: string
  score: number
  createdAt: Date
  updatedAt: Date
  createdBy: string
  updatedBy: string
  [key: string]: unknown
}

const TEST_DIR = 'data/test/snippet-benchmark'

interface BenchmarkResult {
  operation: string
  cpuTimeMs: number
  memoryMB: number
  fetchCount: number
  rowGroupsScanned: number
  rowGroupsSkipped: number
  withinLimits: boolean
}

async function main() {
  console.log('=== Snippet Constraint Benchmark ===\n')

  // Clean up previous test data
  await rm(TEST_DIR, { recursive: true, force: true })
  await mkdir(TEST_DIR, { recursive: true })

  // Generate test data
  console.log('1. Generating test data...')
  const entities = generateTestEntities(50_000)
  console.log(`   Generated ${entities.length.toLocaleString()} entities\n`)

  // Partition data with SMALL files (target <1MB for fast reads)
  console.log('2. Partitioning data...')
  const partitionResult = partitionById(entities, {
    rowGroupSize: 5_000,  // 5K rows per row group
    maxFileSize: 1 * 1024 * 1024,  // 1MB files for faster reads
  })

  // Write partitions
  for (const file of partitionResult.files) {
    await writeFile(`${TEST_DIR}/${file.name}`, Buffer.from(file.buffer))
  }
  await writeFile(
    `${TEST_DIR}/manifest.json`,
    JSON.stringify(partitionResult.manifest, null, 2)
  )

  console.log(`   Created ${partitionResult.files.length} partition files`)
  console.log(`   Manifest: ${JSON.stringify(partitionResult.manifest.partitions.length)} partitions\n`)

  // Load manifest
  const manifest: PartitionManifest = partitionResult.manifest

  // Run benchmarks
  const results: BenchmarkResult[] = []

  // Warm up
  console.log('3. Warming up JIT...')
  const warmupFile = await readFile(`${TEST_DIR}/${partitionResult.files[0].name}`)
  const warmupBuffer = warmupFile.buffer.slice(warmupFile.byteOffset, warmupFile.byteOffset + warmupFile.byteLength)
  parquetMetadata(warmupBuffer)
  await parquetRead({
    file: warmupBuffer,
    compressors,
    rowEnd: 100,
    onComplete: () => {}
  })
  console.log('   Done\n')

  // Benchmark: Point lookup (existing ID)
  console.log('4. Benchmarking point lookups...\n')

  // Pick random IDs to look up
  const testIds = [
    entities[0].$id,                           // First
    entities[Math.floor(entities.length / 2)].$id,  // Middle
    entities[entities.length - 1].$id,         // Last
    `entity-99999999`,                          // Non-existent
  ]

  for (const testId of testIds) {
    const result = await benchmarkPointLookup(TEST_DIR, manifest, testId)
    results.push(result)

    const status = result.withinLimits ? '✅' : '❌'
    console.log(`   ${result.operation}`)
    console.log(`     CPU: ${result.cpuTimeMs.toFixed(2)}ms ${result.cpuTimeMs < 5 ? '✅' : '❌'}`)
    console.log(`     Fetches: ${result.fetchCount} ${result.fetchCount <= 5 ? '✅' : '❌'}`)
    console.log(`     Row groups: ${result.rowGroupsScanned} scanned, ${result.rowGroupsSkipped} skipped`)
    console.log('')
  }

  // Benchmark: Multiple lookups (simulate realistic workload)
  console.log('5. Benchmarking batch lookups (10 sequential)...')
  const batchStart = performance.now()
  const batchMemBefore = process.memoryUsage().heapUsed

  for (let i = 0; i < 10; i++) {
    const randomId = entities[Math.floor(Math.random() * entities.length)].$id
    await benchmarkPointLookup(TEST_DIR, manifest, randomId)
  }

  const batchTime = performance.now() - batchStart
  const batchMem = (process.memoryUsage().heapUsed - batchMemBefore) / 1024 / 1024

  console.log(`   Total time: ${batchTime.toFixed(2)}ms`)
  console.log(`   Avg per lookup: ${(batchTime / 10).toFixed(2)}ms`)
  console.log(`   Memory delta: ${batchMem.toFixed(2)}MB\n`)

  // Summary
  console.log('=== Summary ===\n')
  console.log('Snippet Constraints:')
  console.log('┌────────────────────────────────────────────────────────┐')
  console.log('│ Constraint    │ Limit  │ Actual        │ Status      │')
  console.log('├────────────────────────────────────────────────────────┤')

  const avgCpu = results.reduce((sum, r) => sum + r.cpuTimeMs, 0) / results.length
  const maxFetches = Math.max(...results.map(r => r.fetchCount))
  const memUsage = process.memoryUsage().heapUsed / 1024 / 1024

  console.log(`│ CPU time      │ <5ms   │ ~${avgCpu.toFixed(1)}ms avg    │ ${avgCpu < 5 ? '✅' : '❌'}          │`)
  console.log(`│ Memory        │ <32MB  │ ~${memUsage.toFixed(1)}MB       │ ${memUsage < 32 ? '✅' : '❌'}          │`)
  console.log(`│ Fetches       │ ≤5     │ ${maxFetches} max        │ ${maxFetches <= 5 ? '✅' : '❌'}          │`)
  console.log('└────────────────────────────────────────────────────────┘')

  console.log('\nRow Group Statistics:')
  const avgSkipped = results.reduce((sum, r) => sum + r.rowGroupsSkipped, 0) / results.length
  const avgScanned = results.reduce((sum, r) => sum + r.rowGroupsScanned, 0) / results.length
  console.log(`  Avg skipped: ${avgSkipped.toFixed(1)} row groups (predicate pushdown)`)
  console.log(`  Avg scanned: ${avgScanned.toFixed(1)} row groups`)

  if (avgCpu < 5 && memUsage < 32 && maxFetches <= 5) {
    console.log('\n✅ All constraints satisfied! Ready for Cloudflare Snippets.')
  } else {
    console.log('\n⚠️  Some constraints exceeded. Consider:')
    if (avgCpu >= 5) console.log('   - Smaller row groups')
    if (memUsage >= 32) console.log('   - Stream processing instead of buffering')
    if (maxFetches > 5) console.log('   - Better partition routing')
  }

  // Clean up
  await rm(TEST_DIR, { recursive: true, force: true })
}

/**
 * Generate test entities
 */
function generateTestEntities(count: number): TestEntity[] {
  const entities: TestEntity[] = []

  for (let i = 0; i < count; i++) {
    const id = `entity-${String(i).padStart(8, '0')}`
    entities.push({
      $id: id,
      $type: 'TestEntity',
      name: `Test Entity ${i}`,
      email: `user${i}@example.com`,
      score: Math.random() * 100,
      createdAt: new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000),
      updatedAt: new Date(),
      createdBy: 'benchmark',
      updatedBy: 'benchmark',
    } as TestEntity)
  }

  return entities
}

/**
 * Benchmark a single point lookup
 */
async function benchmarkPointLookup(
  dataDir: string,
  manifest: PartitionManifest,
  id: string
): Promise<BenchmarkResult> {
  const memBefore = process.memoryUsage().heapUsed
  const startTime = performance.now()

  // Simulate AsyncFile with fetch tracking
  let fetchCount = 0
  let rowGroupsScanned = 0
  let rowGroupsSkipped = 0

  const getFile = async (fileName: string): Promise<AsyncFile> => {
    const buffer = await readFile(`${dataDir}/${fileName}`)
    const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

    return {
      byteLength: arrayBuffer.byteLength,
      async slice(start: number, end?: number): Promise<ArrayBuffer> {
        fetchCount++
        return arrayBuffer.slice(start, end)
      },
    }
  }

  const result = await getById(id, manifest, getFile)

  const cpuTimeMs = performance.now() - startTime
  const memoryMB = (process.memoryUsage().heapUsed - memBefore) / 1024 / 1024

  return {
    operation: `get("${id.slice(0, 20)}...")`,
    cpuTimeMs,
    memoryMB,
    fetchCount: result.metrics.fetchCount,
    rowGroupsScanned: result.metrics.rowGroupsScanned,
    rowGroupsSkipped: result.metrics.rowGroupsSkipped,
    withinLimits: cpuTimeMs < 5 && memoryMB < 32 && result.metrics.fetchCount <= 5,
  }
}

main().catch(console.error)
