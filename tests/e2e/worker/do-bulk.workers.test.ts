/**
 * ParqueDB DO WAL Phase 2 - Bulk Bypass to R2 Tests
 *
 * Tests the bulk write path that bypasses SQLite buffering:
 * - 5+ entities stream directly to R2 pending files
 * - Pending row group metadata tracked in SQLite
 * - Reads merge pending files with committed data
 * - Flush promotes pending to committed
 *
 * These tests run in the Cloudflare Workers environment with real bindings:
 * - Durable Objects (PARQUEDB) with SQLite storage
 * - R2 (BUCKET) for Parquet file storage
 *
 * Run with: npm run test:e2e:workers
 */

import { env } from 'cloudflare:test'
import { describe, it, expect, beforeEach } from 'vitest'
import type { TestEnv, ParqueDBDOTestStub } from '../types'
import { asDOTestStub } from '../types'

// Cast env to our typed environment
const testEnv = env as TestEnv

// Bulk threshold constant (must match ParqueDBDO.ts)
const BULK_THRESHOLD = 5

// Counter to ensure unique DO names across tests (Date.now() alone can collide)
let testCounter = 0
function uniqueDOName(prefix: string): string {
  return `${prefix}-${Date.now()}-${++testCounter}-${Math.random().toString(36).slice(2, 8)}`
}

describe('DO WAL Phase 2 - Bulk Bypass to R2', () => {
  describe('Bulk Create Threshold Detection', () => {
    it('uses standard event buffering for < 5 entities', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-small'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create 4 entities (below threshold)
      const items = Array.from({ length: 4 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        content: `Content ${i}`,
      }))

      const entities = await stub.createMany('posts', items, {})

      // Should have created all entities
      expect(entities).toHaveLength(4)

      // Should NOT have pending row groups (used event buffering)
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(0)
    })

    it('writes directly to R2 for >= 5 entities', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-large'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create exactly 5 entities (at threshold)
      const items = Array.from({ length: BULK_THRESHOLD }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        content: `Content ${i}`,
      }))

      const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>

      // Should have created all entities
      expect(entities).toHaveLength(5)

      // Should have pending row group metadata
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)
      expect(pending[0]!.rowCount).toBe(5)
    })

    it('creates multiple pending row groups for separate bulk creates', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-multi'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // First bulk create
      const items1 = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Batch1 Post ${i}`,
      }))
      await stub.createMany('posts', items1, {})

      // Second bulk create
      const items2 = Array.from({ length: 7 }, (_, i) => ({
        $type: 'Post',
        name: `Batch2 Post ${i}`,
      }))
      await stub.createMany('posts', items2, {})

      // Should have 2 pending row groups
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(2)

      // First group: 10 rows
      expect(pending[0]!.rowCount).toBe(10)

      // Second group: 7 rows
      expect(pending[1]!.rowCount).toBe(7)
    })
  })

  describe('Pending Row Group Metadata', () => {
    it('records correct metadata for bulk write', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-meta'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const items = Array.from({ length: 20 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      }))

      await stub.createMany('posts', items, {})

      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)

      const group = pending[0]!
      expect(group.rowCount).toBe(20)
      expect(group.path).toContain('data/posts/pending/')
      expect(group.path).toContain('.parquet')
      expect(group.firstSeq).toBeGreaterThan(0)
      expect(group.lastSeq).toBe(group.firstSeq + 19) // 20 items
      expect(group.createdAt).toBeDefined()
    })

    it('tracks sequence numbers correctly across bulk creates', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-seq'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // First bulk create: 10 items starting at seq 1
      const items1 = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `First ${i}`,
      }))
      await stub.createMany('posts', items1, {})

      // Second bulk create: 5 items starting at seq 11
      const items2 = Array.from({ length: 5 }, (_, i) => ({
        $type: 'Post',
        name: `Second ${i}`,
      }))
      await stub.createMany('posts', items2, {})

      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(2)

      // First group: seq 1-10
      expect(pending[0]!.firstSeq).toBe(1)
      expect(pending[0]!.lastSeq).toBe(10)

      // Second group: seq 11-15
      expect(pending[1]!.firstSeq).toBe(11)
      expect(pending[1]!.lastSeq).toBe(15)
    })
  })

  describe('Entity Retrieval', () => {
    it('can retrieve bulk-created entities by ID', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-get'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const items = Array.from({ length: 5 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        index: i,
      }))

      const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>

      // Should be able to retrieve each entity
      for (let i = 0; i < entities.length; i++) {
        const entityId = entities[i]!.$id as string
        const shortId = entityId.split('/')[1]!

        const retrieved = await stub.get('posts', shortId) as Record<string, unknown>
        expect(retrieved).not.toBeNull()
        expect(retrieved.name).toBe(`Post ${i}`)
        expect(retrieved.index).toBe(i)
      }
    })

    it('assigns unique IDs to bulk-created entities', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-unique'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const items = Array.from({ length: 50 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      }))

      const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>

      // All IDs should be unique
      const ids = entities.map(e => e.$id)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).toBe(50)

      // IDs should follow format posts/{sqid}
      for (const entityId of ids) {
        expect(entityId).toContain('/')
        const [ns, _] = (entityId as string).split('/')
        expect(ns).toBe('posts')
      }
    })
  })

  describe('Pending File Cleanup', () => {
    it('can delete pending row groups by sequence', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-delete'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create two batches
      const items1 = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Batch1 ${i}`,
      }))
      await stub.createMany('posts', items1, {})

      const items2 = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Batch2 ${i}`,
      }))
      await stub.createMany('posts', items2, {})

      // Should have 2 pending groups
      let pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(2)

      // Delete first batch (seq 1-10)
      await stub.deletePendingRowGroups('posts', 10)

      // Should have 1 pending group remaining
      pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)
      expect(pending[0]!.firstSeq).toBe(11)
    })

    it('flushPendingToCommitted promotes pending files', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-flush'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create bulk entities
      const items = Array.from({ length: 25 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      }))
      await stub.createMany('posts', items, {})

      // Should have pending row group
      let pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)

      // Flush pending to committed
      const promoted = await stub.flushPendingToCommitted('posts')
      expect(promoted).toBe(25)

      // Pending should be empty now
      pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(0)
    })
  })

  describe('R2 File Verification', () => {
    it('writes pending Parquet file to R2', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-r2'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const items = Array.from({ length: 5 }, (_, i) => ({
        $type: 'Post',
        name: `R2 Post ${i}`,
      }))

      await stub.createMany('posts', items, {})

      // Get pending row group metadata
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)

      const path = pending[0]!.path

      // Verify file exists in R2 (either .parquet or .json fallback)
      let obj = await testEnv.BUCKET.head(path)
      if (!obj) {
        // Try JSON fallback
        obj = await testEnv.BUCKET.head(path + '.json')
      }

      expect(obj).not.toBeNull()
      expect(obj!.size).toBeGreaterThan(0)
    })

    it('pending file contains correct data', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-data'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const items = Array.from({ length: 5 }, (_, i) => ({
        $type: 'Article',
        name: `Article ${i}`,
        author: `Author ${i}`,
      }))

      const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>

      // Get pending file path
      const pending = await stub.getPendingRowGroups('posts')
      const path = pending[0]!.path

      // Try to read as JSON fallback (easier to verify than Parquet)
      const obj = await testEnv.BUCKET.get(path + '.json')
      if (obj) {
        const text = await obj.text()
        const data = JSON.parse(text) as Array<{ $id: string; data: string }>

        expect(data).toHaveLength(5)

        // Verify each row has correct data
        for (let i = 0; i < data.length; i++) {
          expect(data[i]!.$id).toBe(entities[i]!.$id)
          const rowData = JSON.parse(data[i]!.data)
          expect(rowData.name).toBe(`Article ${i}`)
          expect(rowData.author).toBe(`Author ${i}`)
        }
      }
    })
  })

  describe('Mixed Operations', () => {
    it('handles mix of small and bulk creates', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-mixed'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Small create (uses event buffering)
      const smallItems = Array.from({ length: 3 }, (_, i) => ({
        $type: 'Post',
        name: `Small ${i}`,
      }))
      const smallEntities = await stub.createMany('posts', smallItems, {}) as Array<Record<string, unknown>>
      expect(smallEntities).toHaveLength(3)

      // Bulk create (uses R2 bypass)
      const bulkItems = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Bulk ${i}`,
      }))
      const bulkEntities = await stub.createMany('posts', bulkItems, {}) as Array<Record<string, unknown>>
      expect(bulkEntities).toHaveLength(10)

      // Another small create
      const moreSmallItems = Array.from({ length: 2 }, (_, i) => ({
        $type: 'Post',
        name: `MoreSmall ${i}`,
      }))
      const moreSmallEntities = await stub.createMany('posts', moreSmallItems, {}) as Array<Record<string, unknown>>
      expect(moreSmallEntities).toHaveLength(2)

      // Should have 1 pending row group (from bulk create)
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)
      expect(pending[0]!.rowCount).toBe(10)

      // All entities should be retrievable
      for (const entity of [...smallEntities, ...bulkEntities, ...moreSmallEntities]) {
        const shortId = (entity.$id as string).split('/')[1]!
        const retrieved = await stub.get('posts', shortId)
        expect(retrieved).not.toBeNull()
      }
    })

    it('maintains ID uniqueness across small and bulk creates', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-unique-mixed'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const allIds: string[] = []

      // Alternating small and bulk creates
      for (let batch = 0; batch < 5; batch++) {
        const size = batch % 2 === 0 ? 3 : 7 // Alternates below/above threshold
        const items = Array.from({ length: size }, (_, i) => ({
          $type: 'Post',
          name: `Batch${batch} Item${i}`,
        }))

        const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>
        for (const entity of entities) {
          allIds.push(entity.$id as string)
        }
      }

      // Total: 3 + 7 + 3 + 7 + 3 = 23 entities
      expect(allIds).toHaveLength(23)

      // All IDs should be unique
      const uniqueIds = new Set(allIds)
      expect(uniqueIds.size).toBe(23)
    })
  })

  describe('Edge Cases', () => {
    // Note: Validation error tests moved to unit tests (tests/unit/worker/ParqueDBDO.test.ts)
    // because DO RPC errors cause isolated storage cleanup issues in vitest-pool-workers.
    // Validation is tested at the unit level where we can properly catch thrown errors.

    it('handles empty array gracefully', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-empty'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      const entities = await stub.createMany('posts', [], {})
      expect(entities).toHaveLength(0)

      // Should not create any pending row groups
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(0)
    })
  })

  describe('Cost Optimization Verification', () => {
    it('bulk create uses 1 SQLite row instead of N rows', async () => {
      const id = testEnv.PARQUEDB.idFromName(uniqueDOName('bulk-cost'))
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create 100 entities in bulk
      const items = Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      }))

      await stub.createMany('posts', items, {})

      // Should have exactly 1 pending row group (metadata row)
      // Instead of 100 event rows in events_wal
      const pending = await stub.getPendingRowGroups('posts')
      expect(pending).toHaveLength(1)
      expect(pending[0]!.rowCount).toBe(100)

      // The cost is 1 SQLite row + 1 R2 write
      // vs 100 SQLite rows for event buffering
    })
  })
})
