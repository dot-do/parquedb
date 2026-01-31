/**
 * Remote R2 Benchmarks via S3-compatible API
 *
 * Measures actual network I/O latency from Node.js to Cloudflare R2.
 *
 * Setup:
 * 1. Create .env with R2 credentials:
 *    R2_ACCESS_KEY_ID=your_access_key
 *    R2_SECRET_ACCESS_KEY=your_secret_key
 *    R2_URL=https://your-account-id.r2.cloudflarestorage.com
 *    R2_BUCKET=your-bucket-name (defaults to 'parquedb')
 *
 * 2. Run with: npx vitest bench tests/benchmarks/r2-remote.bench.ts
 *
 * Measures:
 * - Single file read latency
 * - Range read latency (for Parquet row groups)
 * - List operations
 * - Concurrent reads
 *
 * Reports p50, p95, p99 latencies.
 */

import { describe, bench, beforeAll, afterAll } from 'vitest'
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3'
import { config } from 'dotenv'

// Load environment variables from .env file
config()

// =============================================================================
// Configuration
// =============================================================================

const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_URL = process.env.R2_URL
const R2_BUCKET = process.env.R2_BUCKET ?? 'parquedb'

const hasCredentials = R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY && R2_URL

// Generate unique test prefix
const testPrefix = `bench-r2-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}/`

// Test data sizes (in bytes)
const SIZES = {
  small: 1024,              // 1 KB
  medium: 64 * 1024,        // 64 KB (typical Parquet metadata)
  large: 1024 * 1024,       // 1 MB (small Parquet row group)
  xlarge: 8 * 1024 * 1024,  // 8 MB (medium Parquet row group)
}

// Range sizes for testing
const RANGE_SIZES = {
  footer: 8,               // Parquet magic bytes
  metadata: 4096,          // Parquet file metadata
  rowGroupSmall: 64 * 1024,// Small row group
  rowGroupLarge: 512 * 1024,// Larger row group
}

// Number of files for list/concurrent tests
const FILE_COUNT = 100

// =============================================================================
// S3 Client Setup
// =============================================================================

let client: S3Client

function createClient(): S3Client {
  if (!hasCredentials) {
    throw new Error('R2 credentials not configured')
  }

  return new S3Client({
    region: 'auto',
    endpoint: R2_URL!,
    credentials: {
      accessKeyId: R2_ACCESS_KEY_ID!,
      secretAccessKey: R2_SECRET_ACCESS_KEY!,
    },
  })
}

// =============================================================================
// Latency Tracking
// =============================================================================

interface LatencyStats {
  samples: number[]
  add(duration: number): void
  reset(): void
  getStats(): { min: number; max: number; mean: number; p50: number; p95: number; p99: number }
}

function createLatencyTracker(): LatencyStats {
  return {
    samples: [],
    add(duration: number) {
      this.samples.push(duration)
    },
    reset() {
      this.samples = []
    },
    getStats() {
      if (this.samples.length === 0) {
        return { min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0 }
      }
      const sorted = [...this.samples].sort((a, b) => a - b)
      const n = sorted.length
      const percentile = (p: number) => {
        const idx = Math.ceil((p / 100) * n) - 1
        return sorted[Math.max(0, Math.min(idx, n - 1))]
      }
      return {
        min: sorted[0],
        max: sorted[n - 1],
        mean: sorted.reduce((a, b) => a + b, 0) / n,
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
      }
    },
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function generateRandomData(size: number): Uint8Array {
  const data = new Uint8Array(size)
  for (let i = 0; i < size; i++) {
    data[i] = Math.floor(Math.random() * 256)
  }
  return data
}

async function uploadTestFile(key: string, size: number): Promise<void> {
  const data = generateRandomData(size)
  const command = new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: testPrefix + key,
    Body: data,
  })
  await client.send(command)
}

async function readFile(key: string): Promise<Uint8Array> {
  const command = new GetObjectCommand({
    Bucket: R2_BUCKET,
    Key: testPrefix + key,
  })
  const response = await client.send(command)
  if (!response.Body) throw new Error('No body')

  const chunks: Uint8Array[] = []
  for await (const chunk of response.Body as AsyncIterable<Uint8Array>) {
    chunks.push(chunk)
  }
  return new Uint8Array(Buffer.concat(chunks))
}

async function readRange(key: string, start: number, end: number): Promise<Uint8Array> {
  const command = new GetObjectCommand({
    Bucket: R2_BUCKET,
    Key: testPrefix + key,
    Range: `bytes=${start}-${end}`,
  })
  const response = await client.send(command)
  if (!response.Body) throw new Error('No body')

  const chunks: Uint8Array[] = []
  for await (const chunk of response.Body as AsyncIterable<Uint8Array>) {
    chunks.push(chunk)
  }
  return new Uint8Array(Buffer.concat(chunks))
}

async function listFiles(prefix: string, limit?: number): Promise<string[]> {
  const command = new ListObjectsV2Command({
    Bucket: R2_BUCKET,
    Prefix: testPrefix + prefix,
    MaxKeys: limit,
  })
  const response = await client.send(command)
  return (response.Contents ?? []).map(obj => obj.Key ?? '')
}

async function headFile(key: string): Promise<{ size: number; etag: string }> {
  const command = new HeadObjectCommand({
    Bucket: R2_BUCKET,
    Key: testPrefix + key,
  })
  const response = await client.send(command)
  return {
    size: response.ContentLength ?? 0,
    etag: response.ETag ?? '',
  }
}

async function cleanupTestData(): Promise<void> {
  let cursor: string | undefined

  do {
    const listCommand = new ListObjectsV2Command({
      Bucket: R2_BUCKET,
      Prefix: testPrefix,
      ContinuationToken: cursor,
    })
    const listResponse = await client.send(listCommand)

    if (listResponse.Contents && listResponse.Contents.length > 0) {
      const deleteCommand = new DeleteObjectsCommand({
        Bucket: R2_BUCKET,
        Delete: {
          Objects: listResponse.Contents.map(obj => ({ Key: obj.Key })),
        },
      })
      await client.send(deleteCommand)
    }

    cursor = listResponse.IsTruncated ? listResponse.NextContinuationToken : undefined
  } while (cursor)
}

// =============================================================================
// Benchmarks
// =============================================================================

describe.skipIf(!hasCredentials)('R2 Remote Latency Benchmarks', () => {
  const latencyRead = createLatencyTracker()
  const latencyRange = createLatencyTracker()
  const latencyList = createLatencyTracker()
  const latencyHead = createLatencyTracker()

  beforeAll(async () => {
    if (!hasCredentials) return

    client = createClient()

    console.log('\n=== R2 Remote Benchmark Setup ===')
    console.log(`Endpoint: ${R2_URL}`)
    console.log(`Bucket: ${R2_BUCKET}`)
    console.log(`Test prefix: ${testPrefix}`)
    console.log('')

    // Upload test files
    console.log('Uploading test files...')

    // Upload files of various sizes
    await uploadTestFile('small.bin', SIZES.small)
    await uploadTestFile('medium.bin', SIZES.medium)
    await uploadTestFile('large.bin', SIZES.large)
    await uploadTestFile('xlarge.bin', SIZES.xlarge)

    // Upload files for list/concurrent tests
    const uploadPromises = []
    for (let i = 0; i < FILE_COUNT; i++) {
      uploadPromises.push(uploadTestFile(`batch/file-${i.toString().padStart(4, '0')}.bin`, SIZES.medium))
    }
    await Promise.all(uploadPromises)

    console.log(`Uploaded ${4 + FILE_COUNT} test files`)
    console.log('')
  }, 300000) // 5 minute timeout for setup

  afterAll(async () => {
    if (!hasCredentials) return

    // Print latency statistics
    console.log('\n=== R2 Remote Latency Statistics ===\n')

    const printStats = (name: string, tracker: LatencyStats) => {
      const stats = tracker.getStats()
      console.log(`${name}:`)
      console.log(`  Samples: ${tracker.samples.length}`)
      console.log(`  Min:     ${stats.min.toFixed(2)}ms`)
      console.log(`  Mean:    ${stats.mean.toFixed(2)}ms`)
      console.log(`  P50:     ${stats.p50.toFixed(2)}ms`)
      console.log(`  P95:     ${stats.p95.toFixed(2)}ms`)
      console.log(`  P99:     ${stats.p99.toFixed(2)}ms`)
      console.log(`  Max:     ${stats.max.toFixed(2)}ms`)
      console.log('')
    }

    printStats('Single File Read', latencyRead)
    printStats('Range Read', latencyRange)
    printStats('List Operations', latencyList)
    printStats('HEAD Operations', latencyHead)

    // Cleanup
    console.log('Cleaning up test data...')
    await cleanupTestData()
    console.log('Cleanup complete')
  }, 120000)

  // ===========================================================================
  // Single File Read Benchmarks
  // ===========================================================================

  describe('Single File Read', () => {
    bench('[R2] read small file (1KB)', async () => {
      const start = performance.now()
      await readFile('small.bin')
      latencyRead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] read medium file (64KB)', async () => {
      const start = performance.now()
      await readFile('medium.bin')
      latencyRead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] read large file (1MB)', async () => {
      const start = performance.now()
      await readFile('large.bin')
      latencyRead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] read xlarge file (8MB)', async () => {
      const start = performance.now()
      await readFile('xlarge.bin')
      latencyRead.add(performance.now() - start)
    }, { iterations: 5, warmupIterations: 1 })
  })

  // ===========================================================================
  // Range Read Benchmarks (Parquet Row Groups)
  // ===========================================================================

  describe('Range Read (Parquet Row Groups)', () => {
    bench('[R2] range read: footer (8 bytes at end)', async () => {
      // Read last 8 bytes (Parquet magic)
      const start = performance.now()
      const fileSize = SIZES.large
      await readRange('large.bin', fileSize - 8, fileSize - 1)
      latencyRange.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] range read: metadata (4KB at end)', async () => {
      // Read last 4KB (typical Parquet footer)
      const start = performance.now()
      const fileSize = SIZES.large
      await readRange('large.bin', fileSize - RANGE_SIZES.metadata, fileSize - 1)
      latencyRange.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] range read: small row group (64KB)', async () => {
      const start = performance.now()
      await readRange('large.bin', 0, RANGE_SIZES.rowGroupSmall - 1)
      latencyRange.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] range read: large row group (512KB)', async () => {
      const start = performance.now()
      await readRange('large.bin', 0, RANGE_SIZES.rowGroupLarge - 1)
      latencyRange.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] range read: middle of xlarge file (1MB)', async () => {
      // Read 1MB from middle of 8MB file
      const start = performance.now()
      const offset = 3 * 1024 * 1024 // 3MB offset
      await readRange('xlarge.bin', offset, offset + SIZES.large - 1)
      latencyRange.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })
  })

  // ===========================================================================
  // List Operations Benchmarks
  // ===========================================================================

  describe('List Operations', () => {
    bench('[R2] list 10 files', async () => {
      const start = performance.now()
      await listFiles('batch/', 10)
      latencyList.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] list 50 files', async () => {
      const start = performance.now()
      await listFiles('batch/', 50)
      latencyList.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] list 100 files', async () => {
      const start = performance.now()
      await listFiles('batch/', 100)
      latencyList.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] list all files (no limit)', async () => {
      const start = performance.now()
      await listFiles('batch/')
      latencyList.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })
  })

  // ===========================================================================
  // HEAD Operations Benchmarks
  // ===========================================================================

  describe('HEAD Operations (Metadata Only)', () => {
    bench('[R2] HEAD small file', async () => {
      const start = performance.now()
      await headFile('small.bin')
      latencyHead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] HEAD large file', async () => {
      const start = performance.now()
      await headFile('large.bin')
      latencyHead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })

    bench('[R2] HEAD xlarge file', async () => {
      const start = performance.now()
      await headFile('xlarge.bin')
      latencyHead.add(performance.now() - start)
    }, { iterations: 10, warmupIterations: 2 })
  })

  // ===========================================================================
  // Concurrent Operations Benchmarks
  // ===========================================================================

  describe('Concurrent Operations', () => {
    bench('[R2] concurrent read: 5 files', async () => {
      const start = performance.now()
      await Promise.all([
        readFile('batch/file-0000.bin'),
        readFile('batch/file-0001.bin'),
        readFile('batch/file-0002.bin'),
        readFile('batch/file-0003.bin'),
        readFile('batch/file-0004.bin'),
      ])
      const duration = performance.now() - start
      latencyRead.add(duration / 5) // Average per file
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] concurrent read: 10 files', async () => {
      const start = performance.now()
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          readFile(`batch/file-${i.toString().padStart(4, '0')}.bin`)
        )
      )
      const duration = performance.now() - start
      latencyRead.add(duration / 10) // Average per file
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] concurrent read: 20 files', async () => {
      const start = performance.now()
      await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          readFile(`batch/file-${i.toString().padStart(4, '0')}.bin`)
        )
      )
      const duration = performance.now() - start
      latencyRead.add(duration / 20) // Average per file
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] concurrent range read: 5 row groups', async () => {
      const start = performance.now()
      await Promise.all([
        readRange('xlarge.bin', 0, RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', SIZES.large, SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', 2 * SIZES.large, 2 * SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', 3 * SIZES.large, 3 * SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', 4 * SIZES.large, 4 * SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
      ])
      const duration = performance.now() - start
      latencyRange.add(duration / 5) // Average per range
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] concurrent HEAD: 10 files', async () => {
      const start = performance.now()
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          headFile(`batch/file-${i.toString().padStart(4, '0')}.bin`)
        )
      )
      const duration = performance.now() - start
      latencyHead.add(duration / 10) // Average per HEAD
    }, { iterations: 5, warmupIterations: 1 })
  })

  // ===========================================================================
  // Realistic Parquet Read Pattern
  // ===========================================================================

  describe('Realistic Parquet Read Pattern', () => {
    bench('[R2] Parquet read: footer + metadata + 1 row group', async () => {
      const start = performance.now()
      const fileSize = SIZES.xlarge

      // 1. Read footer (last 8 bytes to get metadata size)
      await readRange('xlarge.bin', fileSize - 8, fileSize - 1)

      // 2. Read metadata (assume 4KB)
      await readRange('xlarge.bin', fileSize - 4096, fileSize - 1)

      // 3. Read one row group (64KB)
      await readRange('xlarge.bin', 0, RANGE_SIZES.rowGroupSmall - 1)

      latencyRead.add(performance.now() - start)
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] Parquet read: footer + metadata + 3 row groups', async () => {
      const start = performance.now()
      const fileSize = SIZES.xlarge

      // 1. Read footer
      await readRange('xlarge.bin', fileSize - 8, fileSize - 1)

      // 2. Read metadata
      await readRange('xlarge.bin', fileSize - 4096, fileSize - 1)

      // 3. Read 3 row groups in parallel
      await Promise.all([
        readRange('xlarge.bin', 0, RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', SIZES.large, SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
        readRange('xlarge.bin', 2 * SIZES.large, 2 * SIZES.large + RANGE_SIZES.rowGroupSmall - 1),
      ])

      latencyRead.add(performance.now() - start)
    }, { iterations: 5, warmupIterations: 1 })

    bench('[R2] Parquet scan: list + HEAD + read pattern', async () => {
      const start = performance.now()

      // 1. List files to find Parquet files
      const files = await listFiles('batch/', 10)

      // 2. HEAD one file to get size
      if (files.length > 0) {
        const key = files[0].replace(testPrefix, '')
        await headFile(key)

        // 3. Read the file
        await readFile(key)
      }

      latencyRead.add(performance.now() - start)
    }, { iterations: 5, warmupIterations: 1 })
  })
})
