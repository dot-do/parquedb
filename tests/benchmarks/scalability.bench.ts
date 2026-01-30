/**
 * Scalability Benchmarks for ParqueDB
 *
 * Tests performance across different data scales:
 * - 100, 1K, 10K, 100K entities
 * - Query performance vs dataset size
 * - Memory usage tracking
 * - Index effectiveness
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
import {
  randomElement,
  randomInt,
  randomString,
  randomSubset,
  randomDate,
  generateTestData,
  calculateStats,
  formatStats,
  getMemoryUsage,
  formatBytes,
  Timer,
  startTimer,
} from './setup'

// =============================================================================
// Types for Scalability Testing
// =============================================================================

interface ScalableEntity {
  code: string
  category: string
  subcategory: string
  status: 'active' | 'inactive' | 'pending' | 'archived'
  priority: number
  score: number
  value: number
  tags: string[]
  metadata: {
    source: string
    version: number
    flags: string[]
  }
  createdDate: Date
  updatedDate: Date
}

// =============================================================================
// Data Generators
// =============================================================================

const categories = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
const subcategories = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota', 'kappa']
const statuses: ScalableEntity['status'][] = ['active', 'inactive', 'pending', 'archived']
const sources = ['api', 'import', 'manual', 'migration', 'sync']
const tagPool = Array.from({ length: 50 }, (_, i) => `tag-${i}`)

function generateScalableEntity(index: number): ScalableEntity & { $type: string; name: string } {
  return {
    $type: 'ScalableEntity',
    name: `Entity ${index}`,
    code: `ENT-${index.toString().padStart(8, '0')}`,
    category: categories[index % categories.length],
    subcategory: subcategories[Math.floor(index / 10) % subcategories.length],
    status: statuses[index % statuses.length],
    priority: randomInt(1, 10),
    score: randomInt(0, 10000) / 100,
    value: randomInt(100, 1000000) / 100,
    tags: randomSubset(tagPool, randomInt(1, 5)),
    metadata: {
      source: randomElement(sources),
      version: randomInt(1, 100),
      flags: randomSubset(['verified', 'reviewed', 'processed', 'exported'], randomInt(0, 3)),
    },
    createdDate: randomDate(new Date(2020, 0, 1), new Date(2024, 0, 1)),
    updatedDate: randomDate(new Date(2024, 0, 1), new Date()),
  }
}

// =============================================================================
// Scalability Test Suite
// =============================================================================

describe('Scalability Benchmarks', () => {
  // ===========================================================================
  // Scale: 100 Entities
  // ===========================================================================

  describe('Scale: 100 Entities', () => {
    let collection: Collection<ScalableEntity>
    let entityIds: string[] = []
    let memoryBefore: ReturnType<typeof getMemoryUsage>

    beforeAll(async () => {
      memoryBefore = getMemoryUsage()
      const suffix = Date.now()
      collection = new Collection<ScalableEntity>(`scale-100-${suffix}`)

      for (let i = 0; i < 100; i++) {
        const entity = await collection.create(generateScalableEntity(i))
        entityIds.push(entity.$id as string)
      }
    })

    bench('[100] find all', async () => {
      await collection.find()
    })

    bench('[100] find with equality filter', async () => {
      await collection.find({ status: 'active' })
    })

    bench('[100] find with range filter', async () => {
      await collection.find({ score: { $gte: 50, $lt: 80 } })
    })

    bench('[100] find with complex filter', async () => {
      await collection.find({
        $and: [
          { status: { $in: ['active', 'pending'] } },
          { priority: { $gte: 5 } },
          { score: { $gte: 30 } },
        ],
      })
    })

    bench('[100] find with sort', async () => {
      await collection.find({}, { sort: { score: -1 } })
    })

    bench('[100] find with sort + limit', async () => {
      await collection.find({}, { sort: { score: -1 }, limit: 10 })
    })

    bench('[100] count', async () => {
      await collection.count()
    })

    bench('[100] count with filter', async () => {
      await collection.count({ status: 'active' })
    })

    bench('[100] get by id', async () => {
      const id = randomElement(entityIds).split('/')[1]
      await collection.get(id)
    })

    bench('[100] aggregate group by', async () => {
      await collection.aggregate([
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ])
    })
  })

  // ===========================================================================
  // Scale: 1,000 Entities
  // ===========================================================================

  describe('Scale: 1,000 Entities', () => {
    let collection: Collection<ScalableEntity>
    let entityIds: string[] = []
    let memoryBefore: ReturnType<typeof getMemoryUsage>

    beforeAll(async () => {
      memoryBefore = getMemoryUsage()
      const suffix = Date.now()
      collection = new Collection<ScalableEntity>(`scale-1k-${suffix}`)

      for (let i = 0; i < 1000; i++) {
        const entity = await collection.create(generateScalableEntity(i))
        entityIds.push(entity.$id as string)
      }
    })

    bench('[1K] find all', async () => {
      await collection.find()
    })

    bench('[1K] find with equality filter', async () => {
      await collection.find({ status: 'active' })
    })

    bench('[1K] find with range filter', async () => {
      await collection.find({ score: { $gte: 50, $lt: 80 } })
    })

    bench('[1K] find with complex filter', async () => {
      await collection.find({
        $and: [
          { status: { $in: ['active', 'pending'] } },
          { priority: { $gte: 5 } },
          { score: { $gte: 30 } },
        ],
      })
    })

    bench('[1K] find with sort', async () => {
      await collection.find({}, { sort: { score: -1 } })
    })

    bench('[1K] find with sort + limit', async () => {
      await collection.find({}, { sort: { score: -1 }, limit: 10 })
    })

    bench('[1K] find with filter + sort + limit', async () => {
      await collection.find(
        { status: 'active' },
        { sort: { score: -1 }, limit: 20 }
      )
    })

    bench('[1K] count', async () => {
      await collection.count()
    })

    bench('[1K] count with filter', async () => {
      await collection.count({ status: 'active' })
    })

    bench('[1K] get by id', async () => {
      const id = randomElement(entityIds).split('/')[1]
      await collection.get(id)
    })

    bench('[1K] aggregate group by', async () => {
      await collection.aggregate([
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ])
    })

    bench('[1K] aggregate with match + group', async () => {
      await collection.aggregate([
        { $match: { status: 'active' } },
        { $group: { _id: '$category', count: { $sum: 1 }, avgScore: { $avg: '$score' } } },
      ])
    })

    bench('[1K] pagination: page 1', async () => {
      await collection.find({}, { limit: 20, skip: 0, sort: { code: 1 } })
    })

    bench('[1K] pagination: page 10', async () => {
      await collection.find({}, { limit: 20, skip: 180, sort: { code: 1 } })
    })

    bench('[1K] pagination: page 50', async () => {
      await collection.find({}, { limit: 20, skip: 980, sort: { code: 1 } })
    })
  })

  // ===========================================================================
  // Scale: 10,000 Entities
  // ===========================================================================

  describe('Scale: 10,000 Entities', () => {
    let collection: Collection<ScalableEntity>
    let entityIds: string[] = []
    let memoryBefore: ReturnType<typeof getMemoryUsage>

    beforeAll(async () => {
      memoryBefore = getMemoryUsage()
      const suffix = Date.now()
      collection = new Collection<ScalableEntity>(`scale-10k-${suffix}`)

      // Batch creation for faster setup
      for (let i = 0; i < 10000; i++) {
        const entity = await collection.create(generateScalableEntity(i))
        entityIds.push(entity.$id as string)
      }
    }, 60000) // 60 second timeout for setup

    bench('[10K] find all', async () => {
      await collection.find()
    }, { iterations: 5 })

    bench('[10K] find with equality filter', async () => {
      await collection.find({ status: 'active' })
    })

    bench('[10K] find with range filter', async () => {
      await collection.find({ score: { $gte: 50, $lt: 80 } })
    })

    bench('[10K] find with complex filter', async () => {
      await collection.find({
        $and: [
          { status: { $in: ['active', 'pending'] } },
          { priority: { $gte: 5 } },
          { score: { $gte: 30 } },
        ],
      })
    })

    bench('[10K] find with sort', async () => {
      await collection.find({}, { sort: { score: -1 } })
    }, { iterations: 10 })

    bench('[10K] find with sort + limit (top 10)', async () => {
      await collection.find({}, { sort: { score: -1 }, limit: 10 })
    })

    bench('[10K] find with sort + limit (top 100)', async () => {
      await collection.find({}, { sort: { score: -1 }, limit: 100 })
    })

    bench('[10K] find with filter + sort + limit', async () => {
      await collection.find(
        { status: 'active' },
        { sort: { score: -1 }, limit: 20 }
      )
    })

    bench('[10K] find with projection', async () => {
      await collection.find(
        { status: 'active' },
        { project: { code: 1, score: 1, status: 1 }, limit: 100 }
      )
    })

    bench('[10K] count', async () => {
      await collection.count()
    })

    bench('[10K] count with filter', async () => {
      await collection.count({ status: 'active' })
    })

    bench('[10K] count with complex filter', async () => {
      await collection.count({
        $and: [
          { status: { $in: ['active', 'pending'] } },
          { priority: { $gte: 5 } },
        ],
      })
    })

    bench('[10K] get by id', async () => {
      const id = randomElement(entityIds).split('/')[1]
      await collection.get(id)
    })

    bench('[10K] aggregate group by category', async () => {
      await collection.aggregate([
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ])
    })

    bench('[10K] aggregate group by status', async () => {
      await collection.aggregate([
        { $group: { _id: '$status', count: { $sum: 1 }, avgScore: { $avg: '$score' } } },
      ])
    })

    bench('[10K] aggregate complex pipeline', async () => {
      await collection.aggregate([
        { $match: { status: { $in: ['active', 'pending'] } } },
        { $group: { _id: '$category', count: { $sum: 1 }, avgScore: { $avg: '$score' }, totalValue: { $sum: '$value' } } },
        { $sort: { totalValue: -1 } },
        { $limit: 5 },
      ])
    })

    bench('[10K] pagination: deep page (page 400)', async () => {
      await collection.find({}, { limit: 20, skip: 7980, sort: { code: 1 } })
    })

    bench('[10K] regex search', async () => {
      await collection.find({ code: { $regex: '^ENT-0001' } }, { limit: 20 })
    })
  })

  // ===========================================================================
  // Scale: 100,000 Entities (if feasible)
  // ===========================================================================

  describe('Scale: 100,000 Entities', () => {
    let collection: Collection<ScalableEntity>
    let entityIds: string[] = []
    let memoryBefore: ReturnType<typeof getMemoryUsage>
    let setupComplete = false

    beforeAll(async () => {
      memoryBefore = getMemoryUsage()
      const suffix = Date.now()
      collection = new Collection<ScalableEntity>(`scale-100k-${suffix}`)

      // Batch creation
      for (let i = 0; i < 100000; i++) {
        const entity = await collection.create(generateScalableEntity(i))
        if (i % 10000 === 0) {
          entityIds.push(entity.$id as string)
        }
      }
      setupComplete = true
    }, 300000) // 5 minute timeout for large dataset setup

    bench('[100K] find with filter + limit', async () => {
      await collection.find({ status: 'active' }, { limit: 20 })
    }, { iterations: 10 })

    bench('[100K] find with complex filter + limit', async () => {
      await collection.find(
        {
          $and: [
            { status: 'active' },
            { category: 'A' },
            { score: { $gte: 50 } },
          ],
        },
        { limit: 20 }
      )
    }, { iterations: 10 })

    bench('[100K] find with sort + limit (top 10)', async () => {
      await collection.find({}, { sort: { score: -1 }, limit: 10 })
    }, { iterations: 5 })

    bench('[100K] count', async () => {
      await collection.count()
    }, { iterations: 10 })

    bench('[100K] count with filter', async () => {
      await collection.count({ status: 'active' })
    }, { iterations: 10 })

    bench('[100K] get by id', async () => {
      const id = randomElement(entityIds).split('/')[1]
      await collection.get(id)
    })

    bench('[100K] aggregate group by category', async () => {
      await collection.aggregate([
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ])
    }, { iterations: 5 })

    bench('[100K] aggregate with match + group', async () => {
      await collection.aggregate([
        { $match: { status: 'active' } },
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ])
    }, { iterations: 5 })
  })

  // ===========================================================================
  // Query Performance vs Dataset Size Comparison
  // ===========================================================================

  describe('Query Performance Comparison', () => {
    const scales = [100, 500, 1000, 2000, 5000]
    const collections: Map<number, Collection<ScalableEntity>> = new Map()

    beforeAll(async () => {
      for (const scale of scales) {
        const suffix = `compare-${scale}-${Date.now()}`
        const collection = new Collection<ScalableEntity>(suffix)

        for (let i = 0; i < scale; i++) {
          await collection.create(generateScalableEntity(i))
        }

        collections.set(scale, collection)
      }
    }, 120000)

    // Run same query across different scales
    for (const scale of scales) {
      bench(`[${scale}] equality filter`, async () => {
        const collection = collections.get(scale)!
        await collection.find({ status: 'active' })
      })
    }

    for (const scale of scales) {
      bench(`[${scale}] range + sort + limit`, async () => {
        const collection = collections.get(scale)!
        await collection.find(
          { score: { $gte: 30, $lt: 70 } },
          { sort: { score: -1 }, limit: 10 }
        )
      })
    }

    for (const scale of scales) {
      bench(`[${scale}] aggregate group`, async () => {
        const collection = collections.get(scale)!
        await collection.aggregate([
          { $group: { _id: '$category', count: { $sum: 1 } } },
        ])
      })
    }
  })

  // ===========================================================================
  // Memory Usage Tracking
  // ===========================================================================

  describe('Memory Usage Tracking', () => {
    bench('memory: create 1000 small entities', async () => {
      const before = getMemoryUsage()
      const collection = new Collection<ScalableEntity>(`mem-small-${Date.now()}`)

      for (let i = 0; i < 1000; i++) {
        await collection.create(generateScalableEntity(i))
      }

      const after = getMemoryUsage()
      if (before && after) {
        // Heap growth per entity
        const heapPerEntity = (after.heapUsed - before.heapUsed) / 1000
        // console.log(`Heap per small entity: ${formatBytes(heapPerEntity)}`)
      }
    }, { iterations: 3 })

    bench('memory: create 1000 large entities (5KB content)', async () => {
      const before = getMemoryUsage()
      const collection = new Collection<ScalableEntity>(`mem-large-${Date.now()}`)

      for (let i = 0; i < 1000; i++) {
        const entity = generateScalableEntity(i)
        // Add large content
        ;(entity as Record<string, unknown>).content = randomString(5000)
        await collection.create(entity)
      }

      const after = getMemoryUsage()
      if (before && after) {
        const heapPerEntity = (after.heapUsed - before.heapUsed) / 1000
        // console.log(`Heap per large entity: ${formatBytes(heapPerEntity)}`)
      }
    }, { iterations: 3 })

    bench('memory: query result size (1000 entities)', async () => {
      const collection = new Collection<ScalableEntity>(`mem-query-${Date.now()}`)

      for (let i = 0; i < 1000; i++) {
        await collection.create(generateScalableEntity(i))
      }

      const before = getMemoryUsage()
      const results = await collection.find()
      const after = getMemoryUsage()

      if (before && after) {
        const resultMemory = after.heapUsed - before.heapUsed
        // console.log(`Memory for 1000 result entities: ${formatBytes(resultMemory)}`)
      }
    }, { iterations: 3 })

    bench('memory: projection reduces result size', async () => {
      const collection = new Collection<ScalableEntity>(`mem-project-${Date.now()}`)

      for (let i = 0; i < 1000; i++) {
        await collection.create(generateScalableEntity(i))
      }

      // Full results
      const beforeFull = getMemoryUsage()
      const fullResults = await collection.find({}, { limit: 500 })
      const afterFull = getMemoryUsage()

      // Projected results
      const beforeProjected = getMemoryUsage()
      const projectedResults = await collection.find(
        {},
        { limit: 500, project: { code: 1, status: 1 } }
      )
      const afterProjected = getMemoryUsage()

      // Compare sizes (in actual use, projection should use less memory)
    }, { iterations: 3 })
  })

  // ===========================================================================
  // Index Effectiveness
  // ===========================================================================

  describe('Index Effectiveness', () => {
    let indexCollection: Collection<ScalableEntity>
    let entityCodes: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      indexCollection = new Collection<ScalableEntity>(`index-test-${suffix}`)

      // Create 5000 entities
      for (let i = 0; i < 5000; i++) {
        const entity = await indexCollection.create(generateScalableEntity(i))
        entityCodes.push(entity.code as string)
      }
    }, 60000)

    bench('indexed lookup: exact match on code (unique)', async () => {
      const code = randomElement(entityCodes)
      await indexCollection.find({ code }, { limit: 1 })
    })

    bench('indexed lookup: equality on category', async () => {
      await indexCollection.find({ category: 'A' })
    })

    bench('indexed lookup: equality on status', async () => {
      await indexCollection.find({ status: 'active' })
    })

    bench('unindexed lookup: equality on priority', async () => {
      await indexCollection.find({ priority: 5 })
    })

    bench('unindexed lookup: range on score', async () => {
      await indexCollection.find({ score: { $gte: 40, $lt: 60 } })
    })

    bench('compound: indexed + unindexed fields', async () => {
      await indexCollection.find({
        status: 'active',
        priority: { $gte: 5 },
      })
    })

    bench('compound: multiple indexed fields', async () => {
      await indexCollection.find({
        category: 'A',
        status: 'active',
      })
    })

    bench('selectivity: high selectivity filter (1%)', async () => {
      // code is unique, so very selective
      const code = randomElement(entityCodes)
      await indexCollection.find({ code })
    })

    bench('selectivity: low selectivity filter (25%)', async () => {
      // status has 4 values, ~25% each
      await indexCollection.find({ status: 'active' })
    })

    bench('selectivity: medium selectivity filter (10%)', async () => {
      // category has 10 values, ~10% each
      await indexCollection.find({ category: 'A' })
    })

    bench('full scan: no index usable', async () => {
      // Complex condition that can't use index
      await indexCollection.find({
        $or: [
          { priority: 7 },
          { score: { $gte: 90 } },
        ],
      })
    })

    bench('index + sort: sorted by indexed field', async () => {
      await indexCollection.find(
        { status: 'active' },
        { sort: { code: 1 }, limit: 20 }
      )
    })

    bench('index + sort: sorted by non-indexed field', async () => {
      await indexCollection.find(
        { status: 'active' },
        { sort: { score: -1 }, limit: 20 }
      )
    })
  })

  // ===========================================================================
  // Insertion Performance at Scale
  // ===========================================================================

  describe('Insertion Performance at Scale', () => {
    bench('insert into empty collection (100 entities)', async () => {
      const collection = new Collection<ScalableEntity>(`insert-empty-${Date.now()}`)
      for (let i = 0; i < 100; i++) {
        await collection.create(generateScalableEntity(i))
      }
    })

    bench('insert into 1K collection (100 more entities)', async () => {
      const collection = new Collection<ScalableEntity>(`insert-1k-${Date.now()}`)
      // Seed 1K
      for (let i = 0; i < 1000; i++) {
        await collection.create(generateScalableEntity(i))
      }
      // Insert 100 more
      for (let i = 1000; i < 1100; i++) {
        await collection.create(generateScalableEntity(i))
      }
    }, { iterations: 5 })

    bench('insert into 5K collection (100 more entities)', async () => {
      const collection = new Collection<ScalableEntity>(`insert-5k-${Date.now()}`)
      // Seed 5K
      for (let i = 0; i < 5000; i++) {
        await collection.create(generateScalableEntity(i))
      }
      // Insert 100 more
      for (let i = 5000; i < 5100; i++) {
        await collection.create(generateScalableEntity(i))
      }
    }, { iterations: 3 })

    bench('update in 5K collection (100 updates)', async () => {
      const collection = new Collection<ScalableEntity>(`update-5k-${Date.now()}`)
      const entityIds: string[] = []

      // Seed 5K
      for (let i = 0; i < 5000; i++) {
        const entity = await collection.create(generateScalableEntity(i))
        entityIds.push(entity.$id as string)
      }

      // Update 100
      for (let i = 0; i < 100; i++) {
        const id = randomElement(entityIds).split('/')[1]
        await collection.update(id, {
          $set: { status: 'active' },
          $inc: { score: 1 },
        })
      }
    }, { iterations: 3 })
  })
})
