/**
 * Backend Format Comparison Benchmarks
 *
 * Compares performance across different backend formats:
 * - Native: Simple Parquet files (baseline, fastest for simple use cases)
 * - Iceberg: Apache Iceberg format (DuckDB, Spark, Snowflake compatible)
 * - Delta Lake: Delta Lake format
 *
 * Tests:
 * - Single entity CRUD operations
 * - Batch create performance at various sizes (10, 100, 500, 1000)
 * - Find/query operations with filters (equality, comparison, compound)
 * - Time travel performance (Iceberg & Delta only)
 * - Metadata overhead comparison
 * - Storage efficiency
 * - Read-heavy vs write-heavy workloads
 *
 * Run with: pnpm bench tests/benchmarks/backend-formats.bench.ts
 */

import { describe, bench, beforeAll, afterAll } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { FsBackend } from '../../src/storage/FsBackend'
import { IcebergBackend, createIcebergBackend } from '../../src/backends/iceberg'
import { DeltaBackend, createDeltaBackend } from '../../src/backends/delta'
import { NativeBackend, createNativeBackend } from '../../src/backends/native'
import type { EntityBackend } from '../../src/backends/types'
import type { Entity, EntityId } from '../../src/types/entity'
import {
  generateTestData,
  randomElement,
  randomInt,
} from './setup'

// =============================================================================
// Test Configuration
// =============================================================================

/** Data sizes for scalability testing */
const DATA_SIZES = {
  small: 100,
  medium: 500,
  large: 1000,
}

/** Number of entities for batch operations */
const BATCH_SIZES = [10, 100, 500]

/** Test namespace */
const TEST_NS = 'benchmark-entities'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create test entities for seeding backends
 */
function createTestEntities(count: number): Array<{ $type: string; name: string; [key: string]: unknown }> {
  return generateTestData(count, 'Post') as Array<{ $type: string; name: string; [key: string]: unknown }>
}

/**
 * Seed a backend with test data using bulk operations when available
 */
async function seedBackend(backend: EntityBackend, ns: string, count: number): Promise<string[]> {
  const testData = createTestEntities(count)
  const ids: string[] = []

  // Use bulkCreate for efficiency when possible
  const batchSize = 100
  for (let i = 0; i < testData.length; i += batchSize) {
    const batch = testData.slice(i, i + batchSize)
    const entities = await backend.bulkCreate(ns, batch)
    for (const entity of entities) {
      ids.push(entity.$id)
    }
  }

  return ids
}

/**
 * Create a fresh backend instance for isolated testing
 */
function createTestBackend(
  type: 'native' | 'iceberg' | 'delta',
  storage: MemoryBackend | FsBackend,
  location: string
): NativeBackend | IcebergBackend | DeltaBackend {
  switch (type) {
    case 'native':
      return createNativeBackend({ type: 'native', storage, location })
    case 'iceberg':
      return createIcebergBackend({ type: 'iceberg', storage, warehouse: location })
    case 'delta':
      return createDeltaBackend({ type: 'delta', storage, location })
  }
}

// =============================================================================
// Backend Format Comparison Benchmarks
// =============================================================================

describe('Backend Format Comparison', () => {
  // ===========================================================================
  // Memory-backed comparison (isolates format overhead from I/O)
  // ===========================================================================

  describe('Memory-backed Backends (Small Dataset: 100 entities)', () => {
    let icebergBackend: IcebergBackend
    let deltaBackend: DeltaBackend
    let nativeBackend: NativeBackend

    let icebergMemory: MemoryBackend
    let deltaMemory: MemoryBackend
    let nativeMemory: MemoryBackend

    let icebergIds: string[] = []
    let deltaIds: string[] = []
    let nativeIds: string[] = []

    const SEED_COUNT = DATA_SIZES.small

    beforeAll(async () => {
      // Create memory backends
      icebergMemory = new MemoryBackend()
      deltaMemory = new MemoryBackend()
      nativeMemory = new MemoryBackend()

      // Create entity backends using real implementations
      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergMemory,
        warehouse: 'warehouse',
      })

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaMemory,
        location: 'warehouse',
      })

      nativeBackend = createNativeBackend({
        type: 'native',
        storage: nativeMemory,
        location: 'warehouse',
      })

      // Initialize all backends
      await icebergBackend.initialize()
      await deltaBackend.initialize()
      await nativeBackend.initialize()

      // Seed data for read benchmarks
      console.log(`\nSeeding ${SEED_COUNT} entities per backend...`)
      const startTime = Date.now()

      icebergIds = await seedBackend(icebergBackend, TEST_NS, SEED_COUNT)
      deltaIds = await seedBackend(deltaBackend, TEST_NS, SEED_COUNT)
      nativeIds = await seedBackend(nativeBackend, TEST_NS, SEED_COUNT)

      console.log(`Seeding complete in ${Date.now() - startTime}ms`)
    }, 120000)

    afterAll(async () => {
      await icebergBackend.close()
      await deltaBackend.close()
      await nativeBackend.close()
    })

    // =========================================================================
    // Single Create Operations
    // =========================================================================

    describe('Single Create', () => {
      bench('[Native] create single entity', async () => {
        await nativeBackend.create(`create-native-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content for benchmarking',
          status: 'draft',
          views: 0,
        })
      })

      bench('[Delta] create single entity', async () => {
        await deltaBackend.create(`create-delta-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content for benchmarking',
          status: 'draft',
          views: 0,
        })
      })

      bench('[Iceberg] create single entity', async () => {
        await icebergBackend.create(`create-iceberg-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content for benchmarking',
          status: 'draft',
          views: 0,
        })
      })
    })

    // =========================================================================
    // Batch Create Operations
    // =========================================================================

    describe('Batch Create (10 entities)', () => {
      bench('[Native] bulk create 10 entities', async () => {
        const ns = `batch10-native-${Date.now()}`
        await nativeBackend.bulkCreate(ns, createTestEntities(10))
      })

      bench('[Delta] bulk create 10 entities', async () => {
        const ns = `batch10-delta-${Date.now()}`
        await deltaBackend.bulkCreate(ns, createTestEntities(10))
      })

      bench('[Iceberg] bulk create 10 entities', async () => {
        const ns = `batch10-iceberg-${Date.now()}`
        await icebergBackend.bulkCreate(ns, createTestEntities(10))
      })
    })

    describe('Batch Create (100 entities)', () => {
      bench('[Native] bulk create 100 entities', async () => {
        const ns = `batch100-native-${Date.now()}`
        await nativeBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 10 })

      bench('[Delta] bulk create 100 entities', async () => {
        const ns = `batch100-delta-${Date.now()}`
        await deltaBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 10 })

      bench('[Iceberg] bulk create 100 entities', async () => {
        const ns = `batch100-iceberg-${Date.now()}`
        await icebergBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 10 })
    })

    // =========================================================================
    // Get by ID Operations
    // =========================================================================

    describe('Get by ID', () => {
      bench('[Native] get by ID', async () => {
        const id = randomElement(nativeIds)
        await nativeBackend.get(TEST_NS, id)
      })

      bench('[Delta] get by ID', async () => {
        const id = randomElement(deltaIds)
        await deltaBackend.get(TEST_NS, id)
      })

      bench('[Iceberg] get by ID', async () => {
        const id = randomElement(icebergIds)
        await icebergBackend.get(TEST_NS, id)
      })
    })

    // =========================================================================
    // Find Operations
    // =========================================================================

    describe('Find All (no filter)', () => {
      bench('[Native] find all', async () => {
        await nativeBackend.find(TEST_NS)
      })

      bench('[Delta] find all', async () => {
        await deltaBackend.find(TEST_NS)
      })

      bench('[Iceberg] find all', async () => {
        await icebergBackend.find(TEST_NS)
      })
    })

    describe('Find with Filter', () => {
      bench('[Native] find with equality filter', async () => {
        await nativeBackend.find(TEST_NS, { status: 'published' })
      })

      bench('[Delta] find with equality filter', async () => {
        await deltaBackend.find(TEST_NS, { status: 'published' })
      })

      bench('[Iceberg] find with equality filter', async () => {
        await icebergBackend.find(TEST_NS, { status: 'published' })
      })
    })

    describe('Find with Comparison Filter', () => {
      bench('[Native] find with $gt filter', async () => {
        await nativeBackend.find(TEST_NS, { views: { $gt: 50000 } })
      })

      bench('[Delta] find with $gt filter', async () => {
        await deltaBackend.find(TEST_NS, { views: { $gt: 50000 } })
      })

      bench('[Iceberg] find with $gt filter', async () => {
        await icebergBackend.find(TEST_NS, { views: { $gt: 50000 } })
      })
    })

    describe('Find with Limit', () => {
      bench('[Native] find with limit 10', async () => {
        await nativeBackend.find(TEST_NS, {}, { limit: 10 })
      })

      bench('[Delta] find with limit 10', async () => {
        await deltaBackend.find(TEST_NS, {}, { limit: 10 })
      })

      bench('[Iceberg] find with limit 10', async () => {
        await icebergBackend.find(TEST_NS, {}, { limit: 10 })
      })
    })

    describe('Find with Sort', () => {
      bench('[Native] find with sort', async () => {
        await nativeBackend.find(TEST_NS, {}, { sort: { views: -1 } })
      })

      bench('[Delta] find with sort', async () => {
        await deltaBackend.find(TEST_NS, {}, { sort: { views: -1 } })
      })

      bench('[Iceberg] find with sort', async () => {
        await icebergBackend.find(TEST_NS, {}, { sort: { views: -1 } })
      })
    })

    // =========================================================================
    // Update Operations
    // =========================================================================

    describe('Update Operations', () => {
      bench('[Native] update single field', async () => {
        const id = randomElement(nativeIds)
        await nativeBackend.update(TEST_NS, id, {
          $set: { title: `Updated ${Date.now()}` },
        })
      })

      bench('[Delta] update single field', async () => {
        const id = randomElement(deltaIds)
        await deltaBackend.update(TEST_NS, id, {
          $set: { title: `Updated ${Date.now()}` },
        })
      })

      bench('[Iceberg] update single field', async () => {
        const id = randomElement(icebergIds)
        await icebergBackend.update(TEST_NS, id, {
          $set: { title: `Updated ${Date.now()}` },
        })
      })
    })

    describe('Update with $inc', () => {
      bench('[Native] update with $inc', async () => {
        const id = randomElement(nativeIds)
        await nativeBackend.update(TEST_NS, id, {
          $inc: { views: 1 },
        })
      })

      bench('[Delta] update with $inc', async () => {
        const id = randomElement(deltaIds)
        await deltaBackend.update(TEST_NS, id, {
          $inc: { views: 1 },
        })
      })

      bench('[Iceberg] update with $inc', async () => {
        const id = randomElement(icebergIds)
        await icebergBackend.update(TEST_NS, id, {
          $inc: { views: 1 },
        })
      })
    })

    // =========================================================================
    // Count Operations
    // =========================================================================

    describe('Count Operations', () => {
      bench('[Native] count all', async () => {
        await nativeBackend.count(TEST_NS)
      })

      bench('[Delta] count all', async () => {
        await deltaBackend.count(TEST_NS)
      })

      bench('[Iceberg] count all', async () => {
        await icebergBackend.count(TEST_NS)
      })
    })

    describe('Count with Filter', () => {
      bench('[Native] count with filter', async () => {
        await nativeBackend.count(TEST_NS, { status: 'published' })
      })

      bench('[Delta] count with filter', async () => {
        await deltaBackend.count(TEST_NS, { status: 'published' })
      })

      bench('[Iceberg] count with filter', async () => {
        await icebergBackend.count(TEST_NS, { status: 'published' })
      })
    })
  })

  // ===========================================================================
  // Filesystem-backed comparison (measures real I/O)
  // ===========================================================================

  describe('Filesystem-backed Backends', () => {
    let icebergBackend: IcebergBackend
    let deltaBackend: DeltaBackend
    let nativeBackend: NativeBackend

    let icebergFs: FsBackend
    let deltaFs: FsBackend
    let nativeFs: FsBackend

    let testDir: string

    let icebergIds: string[] = []
    let deltaIds: string[] = []
    let nativeIds: string[] = []

    const FS_SEED_COUNT = 200 // Smaller for FS to avoid long test times

    beforeAll(async () => {
      // Create temp directories
      testDir = join(tmpdir(), `parquedb-backend-bench-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`)
      await fs.mkdir(testDir, { recursive: true })

      const icebergDir = join(testDir, 'iceberg')
      const deltaDir = join(testDir, 'delta')
      const nativeDir = join(testDir, 'native')

      await fs.mkdir(icebergDir, { recursive: true })
      await fs.mkdir(deltaDir, { recursive: true })
      await fs.mkdir(nativeDir, { recursive: true })

      // Create FS backends
      icebergFs = new FsBackend(icebergDir)
      deltaFs = new FsBackend(deltaDir)
      nativeFs = new FsBackend(nativeDir)

      // Create entity backends
      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergFs,
        warehouse: 'warehouse',
      })

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaFs,
        location: 'warehouse',
      })

      nativeBackend = new NativeBackend({
        storage: nativeFs,
        location: 'warehouse',
      })

      // Initialize all backends
      await icebergBackend.initialize()
      await deltaBackend.initialize()
      await nativeBackend.initialize()

      // Seed data
      console.log(`\nSeeding ${FS_SEED_COUNT} entities per FS backend...`)
      const startTime = Date.now()

      icebergIds = await seedBackend(icebergBackend, TEST_NS, FS_SEED_COUNT)
      deltaIds = await seedBackend(deltaBackend, TEST_NS, FS_SEED_COUNT)
      nativeIds = await seedBackend(nativeBackend, TEST_NS, FS_SEED_COUNT)

      console.log(`FS seeding complete in ${Date.now() - startTime}ms`)
    }, 180000)

    afterAll(async () => {
      await icebergBackend.close()
      await deltaBackend.close()
      await nativeBackend.close()

      // Cleanup temp directory
      try {
        await fs.rm(testDir, { recursive: true, force: true })
      } catch {
        // Ignore cleanup errors
      }
    })

    // =========================================================================
    // FS-backed Create Operations
    // =========================================================================

    describe('FS Create', () => {
      bench('[Native/FS] create single entity', async () => {
        await nativeBackend.create(`fs-create-native-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content',
          status: 'draft',
          views: 0,
        })
      })

      bench('[Delta/FS] create single entity', async () => {
        await deltaBackend.create(`fs-create-delta-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content',
          status: 'draft',
          views: 0,
        })
      })

      bench('[Iceberg/FS] create single entity', async () => {
        await icebergBackend.create(`fs-create-iceberg-${Date.now()}`, {
          $type: 'Post',
          name: `Post ${Date.now()}`,
          title: 'Benchmark Post',
          content: 'Content',
          status: 'draft',
          views: 0,
        })
      })
    })

    // =========================================================================
    // FS-backed Read Operations
    // =========================================================================

    describe('FS Get by ID', () => {
      bench('[Native/FS] get by ID', async () => {
        const id = randomElement(nativeIds)
        await nativeBackend.get(TEST_NS, id)
      })

      bench('[Delta/FS] get by ID', async () => {
        const id = randomElement(deltaIds)
        await deltaBackend.get(TEST_NS, id)
      })

      bench('[Iceberg/FS] get by ID', async () => {
        const id = randomElement(icebergIds)
        await icebergBackend.get(TEST_NS, id)
      })
    })

    describe('FS Find All', () => {
      bench('[Native/FS] find all', async () => {
        await nativeBackend.find(TEST_NS)
      })

      bench('[Delta/FS] find all', async () => {
        await deltaBackend.find(TEST_NS)
      })

      bench('[Iceberg/FS] find all', async () => {
        await icebergBackend.find(TEST_NS)
      })
    })

    describe('FS Find with Filter', () => {
      bench('[Native/FS] find with filter', async () => {
        await nativeBackend.find(TEST_NS, { status: 'published' })
      })

      bench('[Delta/FS] find with filter', async () => {
        await deltaBackend.find(TEST_NS, { status: 'published' })
      })

      bench('[Iceberg/FS] find with filter', async () => {
        await icebergBackend.find(TEST_NS, { status: 'published' })
      })
    })
  })

  // ===========================================================================
  // Time Travel Comparison (Iceberg vs Delta)
  // ===========================================================================

  describe('Time Travel Performance', () => {
    let icebergBackend: IcebergBackend
    let deltaBackend: DeltaBackend
    let icebergMemory: MemoryBackend
    let deltaMemory: MemoryBackend

    beforeAll(async () => {
      icebergMemory = new MemoryBackend()
      deltaMemory = new MemoryBackend()

      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergMemory,
        warehouse: 'timetravel-warehouse',
      })

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaMemory,
        location: 'timetravel-warehouse',
      })

      await icebergBackend.initialize()
      await deltaBackend.initialize()

      // Create multiple snapshots by doing sequential writes
      const ns = 'timetravel-ns'
      for (let i = 0; i < 10; i++) {
        await icebergBackend.create(ns, {
          $type: 'Post',
          name: `Snapshot Post ${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
        })
        await deltaBackend.create(ns, {
          $type: 'Post',
          name: `Snapshot Post ${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
        })
      }
    }, 60000)

    afterAll(async () => {
      await icebergBackend.close()
      await deltaBackend.close()
    })

    describe('List Snapshots', () => {
      bench('[Delta] list snapshots', async () => {
        await deltaBackend.listSnapshots?.('timetravel-ns')
      })

      bench('[Iceberg] list snapshots', async () => {
        await icebergBackend.listSnapshots?.('timetravel-ns')
      })
    })
  })

  // ===========================================================================
  // Metadata Overhead Comparison
  // ===========================================================================

  describe('Metadata & Transaction Log Overhead', () => {
    let deltaBackend: DeltaBackend
    let deltaMemory: MemoryBackend
    let icebergBackend: IcebergBackend
    let icebergMemory: MemoryBackend

    beforeAll(async () => {
      deltaMemory = new MemoryBackend()
      icebergMemory = new MemoryBackend()

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaMemory,
        location: 'overhead-warehouse',
      })

      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergMemory,
        warehouse: 'overhead-warehouse',
      })

      await deltaBackend.initialize()
      await icebergBackend.initialize()
    })

    afterAll(async () => {
      await deltaBackend.close()
      await icebergBackend.close()
    })

    describe('Transaction Log Performance', () => {
      bench('[Delta] create with transaction log', async () => {
        const ns = `txlog-delta-${Date.now()}`
        // Delta writes commit file for each transaction
        await deltaBackend.create(ns, {
          $type: 'Post',
          name: 'TX Log Test',
          title: 'Testing transaction log overhead',
        })
      })

      bench('[Iceberg] create with metadata', async () => {
        const ns = `txlog-iceberg-${Date.now()}`
        // Iceberg writes manifest + manifest list + metadata
        await icebergBackend.create(ns, {
          $type: 'Post',
          name: 'Metadata Test',
          title: 'Testing metadata overhead',
        })
      })
    })

    describe('Sequential Writes (commits)', () => {
      bench('[Delta] 10 sequential commits', async () => {
        const ns = `seq-delta-${Date.now()}`
        for (let i = 0; i < 10; i++) {
          await deltaBackend.create(ns, {
            $type: 'Post',
            name: `Seq Post ${i}`,
            title: `Sequential Post ${i}`,
          })
        }
      }, { iterations: 5 })

      bench('[Iceberg] 10 sequential commits', async () => {
        const ns = `seq-iceberg-${Date.now()}`
        for (let i = 0; i < 10; i++) {
          await icebergBackend.create(ns, {
            $type: 'Post',
            name: `Seq Post ${i}`,
            title: `Sequential Post ${i}`,
          })
        }
      }, { iterations: 5 })
    })
  })

  // ===========================================================================
  // Mixed Workload Comparison
  // ===========================================================================

  describe('Mixed Workload (Read-Heavy)', () => {
    let nativeBackend: NativeBackend
    let deltaBackend: DeltaBackend
    let icebergBackend: IcebergBackend

    let nativeIds: string[] = []
    let deltaIds: string[] = []
    let icebergIds: string[] = []

    const MIXED_NS = 'mixed-workload'

    beforeAll(async () => {
      const nativeMemory = new MemoryBackend()
      const deltaMemory = new MemoryBackend()
      const icebergMemory = new MemoryBackend()

      nativeBackend = createNativeBackend({ type: 'native', storage: nativeMemory, location: 'mixed' })
      deltaBackend = createDeltaBackend({ type: 'delta', storage: deltaMemory, location: 'mixed' })
      icebergBackend = createIcebergBackend({ type: 'iceberg', storage: icebergMemory, warehouse: 'mixed' })

      await nativeBackend.initialize()
      await deltaBackend.initialize()
      await icebergBackend.initialize()

      // Seed 100 entities for mixed workload
      nativeIds = await seedBackend(nativeBackend, MIXED_NS, 100)
      deltaIds = await seedBackend(deltaBackend, MIXED_NS, 100)
      icebergIds = await seedBackend(icebergBackend, MIXED_NS, 100)
    }, 60000)

    afterAll(async () => {
      await nativeBackend.close()
      await deltaBackend.close()
      await icebergBackend.close()
    })

    bench('[Native] mixed workload (80% read, 20% write)', async () => {
      for (let i = 0; i < 10; i++) {
        if (i < 8) {
          // Read operations
          await nativeBackend.find(MIXED_NS, { status: 'published' }, { limit: 10 })
        } else {
          // Write operation
          await nativeBackend.update(MIXED_NS, randomElement(nativeIds), {
            $inc: { views: 1 },
          })
        }
      }
    })

    bench('[Delta] mixed workload (80% read, 20% write)', async () => {
      for (let i = 0; i < 10; i++) {
        if (i < 8) {
          await deltaBackend.find(MIXED_NS, { status: 'published' }, { limit: 10 })
        } else {
          await deltaBackend.update(MIXED_NS, randomElement(deltaIds), {
            $inc: { views: 1 },
          })
        }
      }
    })

    bench('[Iceberg] mixed workload (80% read, 20% write)', async () => {
      for (let i = 0; i < 10; i++) {
        if (i < 8) {
          await icebergBackend.find(MIXED_NS, { status: 'published' }, { limit: 10 })
        } else {
          await icebergBackend.update(MIXED_NS, randomElement(icebergIds), {
            $inc: { views: 1 },
          })
        }
      }
    })
  })

  // ===========================================================================
  // Medium Dataset Comparison (500 entities)
  // ===========================================================================

  describe('Memory-backed Backends (Medium Dataset: 500 entities)', () => {
    let icebergBackend: IcebergBackend
    let deltaBackend: DeltaBackend
    let nativeBackend: NativeBackend

    let icebergIds: string[] = []
    let deltaIds: string[] = []
    let nativeIds: string[] = []

    const SEED_COUNT = DATA_SIZES.medium
    const MEDIUM_NS = 'medium-dataset'

    beforeAll(async () => {
      const icebergMemory = new MemoryBackend()
      const deltaMemory = new MemoryBackend()
      const nativeMemory = new MemoryBackend()

      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergMemory,
        warehouse: 'medium-warehouse',
      })

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaMemory,
        location: 'medium-warehouse',
      })

      nativeBackend = createNativeBackend({
        type: 'native',
        storage: nativeMemory,
        location: 'medium-warehouse',
      })

      await icebergBackend.initialize()
      await deltaBackend.initialize()
      await nativeBackend.initialize()

      console.log(`\nSeeding ${SEED_COUNT} entities per backend (medium)...`)
      const startTime = Date.now()

      icebergIds = await seedBackend(icebergBackend, MEDIUM_NS, SEED_COUNT)
      deltaIds = await seedBackend(deltaBackend, MEDIUM_NS, SEED_COUNT)
      nativeIds = await seedBackend(nativeBackend, MEDIUM_NS, SEED_COUNT)

      console.log(`Medium seeding complete in ${Date.now() - startTime}ms`)
    }, 180000)

    afterAll(async () => {
      await icebergBackend.close()
      await deltaBackend.close()
      await nativeBackend.close()
    })

    describe('Full Scan Performance (500 entities)', () => {
      bench('[Native] full scan 500 entities', async () => {
        await nativeBackend.find(MEDIUM_NS)
      })

      bench('[Delta] full scan 500 entities', async () => {
        await deltaBackend.find(MEDIUM_NS)
      })

      bench('[Iceberg] full scan 500 entities', async () => {
        await icebergBackend.find(MEDIUM_NS)
      })
    })

    describe('Filtered Query Performance (500 entities)', () => {
      bench('[Native] filtered query on 500 entities', async () => {
        await nativeBackend.find(MEDIUM_NS, { status: 'published', views: { $gt: 50000 } }, { limit: 50 })
      })

      bench('[Delta] filtered query on 500 entities', async () => {
        await deltaBackend.find(MEDIUM_NS, { status: 'published', views: { $gt: 50000 } }, { limit: 50 })
      })

      bench('[Iceberg] filtered query on 500 entities', async () => {
        await icebergBackend.find(MEDIUM_NS, { status: 'published', views: { $gt: 50000 } }, { limit: 50 })
      })
    })

    describe('Batch Update Performance (500 entities)', () => {
      bench('[Native] batch update 50 entities', async () => {
        await nativeBackend.bulkUpdate(MEDIUM_NS, { status: 'draft' }, { $set: { status: 'archived' } })
      }, { iterations: 3 })

      bench('[Delta] batch update 50 entities', async () => {
        await deltaBackend.bulkUpdate(MEDIUM_NS, { status: 'draft' }, { $set: { status: 'archived' } })
      }, { iterations: 3 })

      bench('[Iceberg] batch update 50 entities', async () => {
        await icebergBackend.bulkUpdate(MEDIUM_NS, { status: 'draft' }, { $set: { status: 'archived' } })
      }, { iterations: 3 })
    })
  })

  // ===========================================================================
  // Large Dataset Comparison (1000 entities)
  // ===========================================================================

  describe('Memory-backed Backends (Large Dataset: 1000 entities)', () => {
    let icebergBackend: IcebergBackend
    let deltaBackend: DeltaBackend
    let nativeBackend: NativeBackend

    let icebergIds: string[] = []
    let deltaIds: string[] = []
    let nativeIds: string[] = []

    const SEED_COUNT = DATA_SIZES.large
    const LARGE_NS = 'large-dataset'

    beforeAll(async () => {
      const icebergMemory = new MemoryBackend()
      const deltaMemory = new MemoryBackend()
      const nativeMemory = new MemoryBackend()

      icebergBackend = createIcebergBackend({
        type: 'iceberg',
        storage: icebergMemory,
        warehouse: 'large-warehouse',
      })

      deltaBackend = createDeltaBackend({
        type: 'delta',
        storage: deltaMemory,
        location: 'large-warehouse',
      })

      nativeBackend = createNativeBackend({
        type: 'native',
        storage: nativeMemory,
        location: 'large-warehouse',
      })

      await icebergBackend.initialize()
      await deltaBackend.initialize()
      await nativeBackend.initialize()

      console.log(`\nSeeding ${SEED_COUNT} entities per backend (large)...`)
      const startTime = Date.now()

      icebergIds = await seedBackend(icebergBackend, LARGE_NS, SEED_COUNT)
      deltaIds = await seedBackend(deltaBackend, LARGE_NS, SEED_COUNT)
      nativeIds = await seedBackend(nativeBackend, LARGE_NS, SEED_COUNT)

      console.log(`Large seeding complete in ${Date.now() - startTime}ms`)
    }, 300000)

    afterAll(async () => {
      await icebergBackend.close()
      await deltaBackend.close()
      await nativeBackend.close()
    })

    describe('Full Scan Performance (1000 entities)', () => {
      bench('[Native] full scan 1000 entities', async () => {
        await nativeBackend.find(LARGE_NS)
      }, { iterations: 5 })

      bench('[Delta] full scan 1000 entities', async () => {
        await deltaBackend.find(LARGE_NS)
      }, { iterations: 5 })

      bench('[Iceberg] full scan 1000 entities', async () => {
        await icebergBackend.find(LARGE_NS)
      }, { iterations: 5 })
    })

    describe('Point Read Performance (1000 entities)', () => {
      bench('[Native] random point read from 1000 entities', async () => {
        await nativeBackend.get(LARGE_NS, randomElement(nativeIds))
      })

      bench('[Delta] random point read from 1000 entities', async () => {
        await deltaBackend.get(LARGE_NS, randomElement(deltaIds))
      })

      bench('[Iceberg] random point read from 1000 entities', async () => {
        await icebergBackend.get(LARGE_NS, randomElement(icebergIds))
      })
    })

    describe('Count Performance (1000 entities)', () => {
      bench('[Native] count all 1000 entities', async () => {
        await nativeBackend.count(LARGE_NS)
      })

      bench('[Delta] count all 1000 entities', async () => {
        await deltaBackend.count(LARGE_NS)
      })

      bench('[Iceberg] count all 1000 entities', async () => {
        await icebergBackend.count(LARGE_NS)
      })
    })

    describe('Count with Filter (1000 entities)', () => {
      bench('[Native] count with filter on 1000 entities', async () => {
        await nativeBackend.count(LARGE_NS, { status: 'published' })
      })

      bench('[Delta] count with filter on 1000 entities', async () => {
        await deltaBackend.count(LARGE_NS, { status: 'published' })
      })

      bench('[Iceberg] count with filter on 1000 entities', async () => {
        await icebergBackend.count(LARGE_NS, { status: 'published' })
      })
    })

    describe('Sorted Query Performance (1000 entities)', () => {
      bench('[Native] sorted query on 1000 entities', async () => {
        await nativeBackend.find(LARGE_NS, {}, { sort: { views: -1 }, limit: 100 })
      })

      bench('[Delta] sorted query on 1000 entities', async () => {
        await deltaBackend.find(LARGE_NS, {}, { sort: { views: -1 }, limit: 100 })
      })

      bench('[Iceberg] sorted query on 1000 entities', async () => {
        await icebergBackend.find(LARGE_NS, {}, { sort: { views: -1 }, limit: 100 })
      })
    })
  })

  // ===========================================================================
  // Write-Heavy Workload Comparison
  // ===========================================================================

  describe('Write-Heavy Workload', () => {
    let nativeBackend: NativeBackend
    let deltaBackend: DeltaBackend
    let icebergBackend: IcebergBackend

    const WRITE_NS = 'write-heavy'

    beforeAll(async () => {
      const nativeMemory = new MemoryBackend()
      const deltaMemory = new MemoryBackend()
      const icebergMemory = new MemoryBackend()

      nativeBackend = createNativeBackend({ type: 'native', storage: nativeMemory, location: 'write' })
      deltaBackend = createDeltaBackend({ type: 'delta', storage: deltaMemory, location: 'write' })
      icebergBackend = createIcebergBackend({ type: 'iceberg', storage: icebergMemory, warehouse: 'write' })

      await nativeBackend.initialize()
      await deltaBackend.initialize()
      await icebergBackend.initialize()
    }, 30000)

    afterAll(async () => {
      await nativeBackend.close()
      await deltaBackend.close()
      await icebergBackend.close()
    })

    describe('Bulk Create Scalability', () => {
      bench('[Native] bulk create 100 entities', async () => {
        const ns = `bulk-native-${Date.now()}`
        await nativeBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 5 })

      bench('[Delta] bulk create 100 entities', async () => {
        const ns = `bulk-delta-${Date.now()}`
        await deltaBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 5 })

      bench('[Iceberg] bulk create 100 entities', async () => {
        const ns = `bulk-iceberg-${Date.now()}`
        await icebergBackend.bulkCreate(ns, createTestEntities(100))
      }, { iterations: 5 })
    })

    describe('Sequential Create Performance', () => {
      bench('[Native] 20 sequential creates', async () => {
        const ns = `seq-native-${Date.now()}`
        for (let i = 0; i < 20; i++) {
          await nativeBackend.create(ns, {
            $type: 'Post',
            name: `Post ${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: i * 100,
          })
        }
      }, { iterations: 3 })

      bench('[Delta] 20 sequential creates', async () => {
        const ns = `seq-delta-${Date.now()}`
        for (let i = 0; i < 20; i++) {
          await deltaBackend.create(ns, {
            $type: 'Post',
            name: `Post ${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: i * 100,
          })
        }
      }, { iterations: 3 })

      bench('[Iceberg] 20 sequential creates', async () => {
        const ns = `seq-iceberg-${Date.now()}`
        for (let i = 0; i < 20; i++) {
          await icebergBackend.create(ns, {
            $type: 'Post',
            name: `Post ${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: i * 100,
          })
        }
      }, { iterations: 3 })
    })
  })

  // ===========================================================================
  // Native Backend Advantage Tests
  // ===========================================================================

  describe('Native Backend Performance Advantage', () => {
    let nativeBackend: NativeBackend
    let deltaBackend: DeltaBackend
    let icebergBackend: IcebergBackend

    let nativeIds: string[] = []
    let deltaIds: string[] = []
    let icebergIds: string[] = []

    const PERF_NS = 'perf-comparison'

    beforeAll(async () => {
      const nativeMemory = new MemoryBackend()
      const deltaMemory = new MemoryBackend()
      const icebergMemory = new MemoryBackend()

      nativeBackend = createNativeBackend({ type: 'native', storage: nativeMemory, location: 'perf' })
      deltaBackend = createDeltaBackend({ type: 'delta', storage: deltaMemory, location: 'perf' })
      icebergBackend = createIcebergBackend({ type: 'iceberg', storage: icebergMemory, warehouse: 'perf' })

      await nativeBackend.initialize()
      await deltaBackend.initialize()
      await icebergBackend.initialize()

      // Seed 200 entities
      nativeIds = await seedBackend(nativeBackend, PERF_NS, 200)
      deltaIds = await seedBackend(deltaBackend, PERF_NS, 200)
      icebergIds = await seedBackend(icebergBackend, PERF_NS, 200)
    }, 90000)

    afterAll(async () => {
      await nativeBackend.close()
      await deltaBackend.close()
      await icebergBackend.close()
    })

    describe('Simple Use Case - No Time Travel Needed', () => {
      bench('[Native] simple CRUD cycle', async () => {
        // Create
        const entity = await nativeBackend.create(PERF_NS, {
          $type: 'Post',
          name: 'Test Post',
          title: 'Test',
          status: 'draft',
        })
        // Read
        await nativeBackend.get(PERF_NS, entity.$id)
        // Update
        await nativeBackend.update(PERF_NS, entity.$id, { $set: { status: 'published' } })
        // Delete
        await nativeBackend.delete(PERF_NS, entity.$id)
      })

      bench('[Delta] simple CRUD cycle', async () => {
        const entity = await deltaBackend.create(PERF_NS, {
          $type: 'Post',
          name: 'Test Post',
          title: 'Test',
          status: 'draft',
        })
        await deltaBackend.get(PERF_NS, entity.$id)
        await deltaBackend.update(PERF_NS, entity.$id, { $set: { status: 'published' } })
        await deltaBackend.delete(PERF_NS, entity.$id)
      })

      bench('[Iceberg] simple CRUD cycle', async () => {
        const entity = await icebergBackend.create(PERF_NS, {
          $type: 'Post',
          name: 'Test Post',
          title: 'Test',
          status: 'draft',
        })
        await icebergBackend.get(PERF_NS, entity.$id)
        await icebergBackend.update(PERF_NS, entity.$id, { $set: { status: 'published' } })
        await icebergBackend.delete(PERF_NS, entity.$id)
      })
    })

    describe('Read-Dominated Workload', () => {
      bench('[Native] 95% reads, 5% writes', async () => {
        for (let i = 0; i < 20; i++) {
          if (i < 19) {
            await nativeBackend.find(PERF_NS, { status: 'published' }, { limit: 10 })
          } else {
            await nativeBackend.create(PERF_NS, { $type: 'Post', name: 'New', title: 'New' })
          }
        }
      })

      bench('[Delta] 95% reads, 5% writes', async () => {
        for (let i = 0; i < 20; i++) {
          if (i < 19) {
            await deltaBackend.find(PERF_NS, { status: 'published' }, { limit: 10 })
          } else {
            await deltaBackend.create(PERF_NS, { $type: 'Post', name: 'New', title: 'New' })
          }
        }
      })

      bench('[Iceberg] 95% reads, 5% writes', async () => {
        for (let i = 0; i < 20; i++) {
          if (i < 19) {
            await icebergBackend.find(PERF_NS, { status: 'published' }, { limit: 10 })
          } else {
            await icebergBackend.create(PERF_NS, { $type: 'Post', name: 'New', title: 'New' })
          }
        }
      })
    })
  })
})
