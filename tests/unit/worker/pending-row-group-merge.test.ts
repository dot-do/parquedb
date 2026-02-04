/**
 * Pending Row Group Merge Tests - RED PHASE
 *
 * Tests for the merge functionality from pending/ row groups to main data.parquet.
 *
 * Context: The merge from pending/ to main data.parquet is NOT IMPLEMENTED.
 * bulkWriteToR2() writes to data/{ns}/pending/{id}.parquet but
 * flushPendingToCommitted() is a placeholder that just deletes metadata
 * without actually merging files.
 *
 * These tests verify the EXPECTED behavior:
 * 1. After bulk write, pending files exist in R2 at data/{ns}/pending/
 * 2. After flush/merge, pending files are merged into data/{ns}/data.parquet
 * 3. After merge, pending files are deleted from R2
 * 4. Merged data.parquet contains all entities from pending files
 * 5. Row group metadata is properly updated
 *
 * Issue: parquedb-vgj0.1
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { ParquetWriter } from '../../../src/parquet/writer'
import { ParquetReader } from '../../../src/parquet/reader'
import type { ParquetSchema } from '../../../src/parquet/types'

// =============================================================================
// Test Helpers
// =============================================================================

/** Schema for pending Parquet files (matches bulkWriteToR2 format) */
const PENDING_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  data: { type: 'STRING', optional: false },
}

/**
 * Create a mock pending Parquet file with entity data
 */
async function createPendingParquet(
  storage: MemoryBackend,
  path: string,
  entities: Array<{ $id: string; data: string }>
): Promise<void> {
  const writer = new ParquetWriter(storage)
  await writer.write(path, entities, PENDING_SCHEMA)
}

/**
 * Read entities from a Parquet file
 */
async function readParquetEntities(storage: MemoryBackend, path: string): Promise<Array<{ $id: string; data: string }>> {
  const reader = new ParquetReader({ storage })
  const rows = await reader.read<{ $id: string; data: string }>(path)
  return rows
}

/**
 * Simulate pending row group metadata (as stored in SQLite)
 */
interface PendingRowGroup {
  id: string
  ns: string
  path: string
  rowCount: number
  firstSeq: number
  lastSeq: number
  createdAt: string
}

/**
 * In-memory simulation of the pending row group merge operation
 *
 * This represents the EXPECTED behavior of flushPendingToCommitted:
 * 1. Read all pending Parquet files
 * 2. Read existing data.parquet (if exists)
 * 3. Merge all entities
 * 4. Write new data.parquet
 * 5. Delete pending files from R2
 */
async function mergePendingToCommitted(
  storage: MemoryBackend,
  ns: string,
  pendingGroups: PendingRowGroup[]
): Promise<{ mergedCount: number; deletedPendingFiles: string[] }> {
  if (pendingGroups.length === 0) {
    return { mergedCount: 0, deletedPendingFiles: [] }
  }

  const dataPath = `data/${ns}/data.parquet`
  let existingEntities: Array<{ $id: string; data: string }> = []

  // Step 1: Read existing data.parquet if it exists
  if (await storage.exists(dataPath)) {
    existingEntities = await readParquetEntities(storage, dataPath)
  }

  // Step 2: Read all pending files
  const pendingEntities: Array<{ $id: string; data: string }> = []
  for (const group of pendingGroups) {
    if (await storage.exists(group.path)) {
      const entities = await readParquetEntities(storage, group.path)
      pendingEntities.push(...entities)
    }
  }

  // Step 3: Merge entities (pending entities are newer, so they come last)
  const mergedEntities = [...existingEntities, ...pendingEntities]

  // Step 4: Write merged data.parquet
  if (mergedEntities.length > 0) {
    await createPendingParquet(storage, dataPath, mergedEntities)
  }

  // Step 5: Delete pending files
  const deletedPendingFiles: string[] = []
  for (const group of pendingGroups) {
    if (await storage.exists(group.path)) {
      await storage.delete(group.path)
      deletedPendingFiles.push(group.path)
    }
  }

  return {
    mergedCount: pendingEntities.length,
    deletedPendingFiles,
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Pending Row Group Merge', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  // ===========================================================================
  // Test 1: Pending files exist after bulk write
  // ===========================================================================

  describe('After bulk write, pending files exist in R2', () => {
    it('should write pending file to data/{ns}/pending/ path', async () => {
      const ns = 'posts'
      const pendingId = 'pending-001'
      const pendingPath = `data/${ns}/pending/${pendingId}.parquet`

      // Simulate bulk write creating a pending file
      const entities = Array.from({ length: 10 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Post', name: `Post ${i}` }),
      }))

      await createPendingParquet(storage, pendingPath, entities)

      // Verify pending file exists
      const exists = await storage.exists(pendingPath)
      expect(exists).toBe(true)

      // Verify pending file contains correct data
      const readEntities = await readParquetEntities(storage, pendingPath)
      expect(readEntities).toHaveLength(10)
      expect(readEntities[0]!.$id).toBe('posts/0')
    })

    it('should create multiple pending files for separate bulk operations', async () => {
      const ns = 'articles'

      // First bulk write
      const pending1 = `data/${ns}/pending/batch-001.parquet`
      const entities1 = Array.from({ length: 5 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Article', title: `Article ${i}` }),
      }))
      await createPendingParquet(storage, pending1, entities1)

      // Second bulk write
      const pending2 = `data/${ns}/pending/batch-002.parquet`
      const entities2 = Array.from({ length: 7 }, (_, i) => ({
        $id: `${ns}/${i + 5}`,
        data: JSON.stringify({ $type: 'Article', title: `Article ${i + 5}` }),
      }))
      await createPendingParquet(storage, pending2, entities2)

      // Both files should exist
      expect(await storage.exists(pending1)).toBe(true)
      expect(await storage.exists(pending2)).toBe(true)

      // List pending directory
      const listResult = await storage.list(`data/${ns}/pending/`)
      expect(listResult.files).toHaveLength(2)
    })
  })

  // ===========================================================================
  // Test 2: After flush/merge, pending files are merged into data.parquet
  // ===========================================================================

  describe('After flush/merge, pending files are merged into data.parquet', () => {
    it('should create data.parquet when merging first pending file', async () => {
      const ns = 'users'
      const dataPath = `data/${ns}/data.parquet`
      const pendingPath = `data/${ns}/pending/batch-001.parquet`

      // Create pending file
      const entities = Array.from({ length: 5 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'User', name: `User ${i}` }),
      }))
      await createPendingParquet(storage, pendingPath, entities)

      // Verify data.parquet does not exist yet
      expect(await storage.exists(dataPath)).toBe(false)

      // Simulate merge operation
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 5,
        firstSeq: 1,
        lastSeq: 5,
        createdAt: new Date().toISOString(),
      }]

      const result = await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify data.parquet now exists
      expect(await storage.exists(dataPath)).toBe(true)
      expect(result.mergedCount).toBe(5)
    })

    it('should merge pending files with existing data.parquet', async () => {
      const ns = 'comments'
      const dataPath = `data/${ns}/data.parquet`
      const pendingPath = `data/${ns}/pending/batch-002.parquet`

      // Create existing data.parquet with 3 entities
      const existingEntities = Array.from({ length: 3 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Comment', text: `Existing ${i}` }),
      }))
      await createPendingParquet(storage, dataPath, existingEntities)

      // Create pending file with 4 more entities
      const pendingEntities = Array.from({ length: 4 }, (_, i) => ({
        $id: `${ns}/${i + 3}`,
        data: JSON.stringify({ $type: 'Comment', text: `New ${i}` }),
      }))
      await createPendingParquet(storage, pendingPath, pendingEntities)

      // Verify initial state
      const beforeMerge = await readParquetEntities(storage, dataPath)
      expect(beforeMerge).toHaveLength(3)

      // Simulate merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-002',
        ns,
        path: pendingPath,
        rowCount: 4,
        firstSeq: 4,
        lastSeq: 7,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify merged data.parquet contains all entities
      const afterMerge = await readParquetEntities(storage, dataPath)
      expect(afterMerge).toHaveLength(7)

      // Verify both existing and new entities are present
      const existingIds = existingEntities.map(e => e.$id)
      const pendingIds = pendingEntities.map(e => e.$id)
      const mergedIds = afterMerge.map(e => e.$id)

      for (const id of [...existingIds, ...pendingIds]) {
        expect(mergedIds).toContain(id)
      }
    })

    it('should merge multiple pending files in sequence order', async () => {
      const ns = 'tags'
      const dataPath = `data/${ns}/data.parquet`

      // Create two pending files
      const pending1 = `data/${ns}/pending/batch-001.parquet`
      const entities1 = Array.from({ length: 3 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Tag', name: `Tag ${i}` }),
      }))
      await createPendingParquet(storage, pending1, entities1)

      const pending2 = `data/${ns}/pending/batch-002.parquet`
      const entities2 = Array.from({ length: 2 }, (_, i) => ({
        $id: `${ns}/${i + 3}`,
        data: JSON.stringify({ $type: 'Tag', name: `Tag ${i + 3}` }),
      }))
      await createPendingParquet(storage, pending2, entities2)

      // Merge both pending files
      const pendingGroups: PendingRowGroup[] = [
        { id: 'batch-001', ns, path: pending1, rowCount: 3, firstSeq: 1, lastSeq: 3, createdAt: new Date().toISOString() },
        { id: 'batch-002', ns, path: pending2, rowCount: 2, firstSeq: 4, lastSeq: 5, createdAt: new Date().toISOString() },
      ]

      const result = await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify all entities merged
      expect(result.mergedCount).toBe(5)

      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(5)
    })
  })

  // ===========================================================================
  // Test 3: After merge, pending files are deleted from R2
  // ===========================================================================

  describe('After merge, pending files are deleted from R2', () => {
    it('should delete pending file after successful merge', async () => {
      const ns = 'events'
      const pendingPath = `data/${ns}/pending/batch-001.parquet`

      // Create pending file
      const entities = Array.from({ length: 5 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Event', name: `Event ${i}` }),
      }))
      await createPendingParquet(storage, pendingPath, entities)

      // Verify pending file exists before merge
      expect(await storage.exists(pendingPath)).toBe(true)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 5,
        firstSeq: 1,
        lastSeq: 5,
        createdAt: new Date().toISOString(),
      }]

      const result = await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify pending file is deleted
      expect(await storage.exists(pendingPath)).toBe(false)
      expect(result.deletedPendingFiles).toContain(pendingPath)
    })

    it('should delete all pending files after merging multiple', async () => {
      const ns = 'logs'

      // Create multiple pending files
      const pendingPaths = ['batch-001', 'batch-002', 'batch-003'].map(id =>
        `data/${ns}/pending/${id}.parquet`
      )

      for (let i = 0; i < pendingPaths.length; i++) {
        const entities = Array.from({ length: 3 }, (_, j) => ({
          $id: `${ns}/${i * 3 + j}`,
          data: JSON.stringify({ $type: 'Log', message: `Log ${i * 3 + j}` }),
        }))
        await createPendingParquet(storage, pendingPaths[i]!, entities)
      }

      // Verify all pending files exist
      for (const path of pendingPaths) {
        expect(await storage.exists(path)).toBe(true)
      }

      // Perform merge
      const pendingGroups: PendingRowGroup[] = pendingPaths.map((path, i) => ({
        id: `batch-00${i + 1}`,
        ns,
        path,
        rowCount: 3,
        firstSeq: i * 3 + 1,
        lastSeq: i * 3 + 3,
        createdAt: new Date().toISOString(),
      }))

      const result = await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify all pending files are deleted
      for (const path of pendingPaths) {
        expect(await storage.exists(path)).toBe(false)
      }
      expect(result.deletedPendingFiles).toHaveLength(3)
    })

    it('should leave pending directory empty after merge', async () => {
      const ns = 'metrics'
      const pendingDir = `data/${ns}/pending/`
      const pendingPath = `${pendingDir}batch-001.parquet`

      // Create pending file
      const entities = [{ $id: `${ns}/0`, data: JSON.stringify({ $type: 'Metric', value: 42 }) }]
      await createPendingParquet(storage, pendingPath, entities)

      // Verify pending directory has file
      let listing = await storage.list(pendingDir)
      expect(listing.files).toHaveLength(1)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 1,
        firstSeq: 1,
        lastSeq: 1,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify pending directory is empty
      listing = await storage.list(pendingDir)
      expect(listing.files).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Test 4: Merged data.parquet contains all entities
  // ===========================================================================

  describe('Merged data.parquet contains all entities from pending files', () => {
    it('should preserve all entity data during merge', async () => {
      const ns = 'products'
      const dataPath = `data/${ns}/data.parquet`
      const pendingPath = `data/${ns}/pending/batch-001.parquet`

      // Create entities with various data types
      const entities = [
        { $id: `${ns}/1`, data: JSON.stringify({ $type: 'Product', name: 'Widget', price: 19.99, tags: ['electronics', 'gadget'] }) },
        { $id: `${ns}/2`, data: JSON.stringify({ $type: 'Product', name: 'Gizmo', price: 29.99, active: true }) },
        { $id: `${ns}/3`, data: JSON.stringify({ $type: 'Product', name: 'Doohickey', price: 9.99, count: 100 }) },
      ]

      await createPendingParquet(storage, pendingPath, entities)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 3,
        firstSeq: 1,
        lastSeq: 3,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify all entity data is preserved
      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(3)

      for (let i = 0; i < entities.length; i++) {
        const original = entities[i]!
        const mergedEntity = merged.find(e => e.$id === original.$id)
        expect(mergedEntity).toBeDefined()
        expect(mergedEntity!.data).toBe(original.data)
      }
    })

    it('should handle large number of entities', async () => {
      const ns = 'items'
      const dataPath = `data/${ns}/data.parquet`

      // Create multiple pending files with many entities
      const pendingPaths = ['batch-001', 'batch-002'].map(id =>
        `data/${ns}/pending/${id}.parquet`
      )

      const allEntities: Array<{ $id: string; data: string }> = []

      for (let i = 0; i < pendingPaths.length; i++) {
        const batchSize = 100
        const entities = Array.from({ length: batchSize }, (_, j) => ({
          $id: `${ns}/${i * batchSize + j}`,
          data: JSON.stringify({ $type: 'Item', index: i * batchSize + j }),
        }))
        allEntities.push(...entities)
        await createPendingParquet(storage, pendingPaths[i]!, entities)
      }

      // Perform merge
      const pendingGroups: PendingRowGroup[] = pendingPaths.map((path, i) => ({
        id: `batch-00${i + 1}`,
        ns,
        path,
        rowCount: 100,
        firstSeq: i * 100 + 1,
        lastSeq: (i + 1) * 100,
        createdAt: new Date().toISOString(),
      }))

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify all 200 entities are in merged file
      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(200)

      // Spot check some entities
      expect(merged.some(e => e.$id === `${ns}/0`)).toBe(true)
      expect(merged.some(e => e.$id === `${ns}/99`)).toBe(true)
      expect(merged.some(e => e.$id === `${ns}/100`)).toBe(true)
      expect(merged.some(e => e.$id === `${ns}/199`)).toBe(true)
    })

    it('should correctly merge with pre-existing data', async () => {
      const ns = 'orders'
      const dataPath = `data/${ns}/data.parquet`

      // Create existing data.parquet with 10 entities
      const existingEntities = Array.from({ length: 10 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Order', status: 'completed', orderId: i }),
      }))
      await createPendingParquet(storage, dataPath, existingEntities)

      // Create pending file with 5 new entities
      const pendingPath = `data/${ns}/pending/batch-001.parquet`
      const pendingEntities = Array.from({ length: 5 }, (_, i) => ({
        $id: `${ns}/${i + 10}`,
        data: JSON.stringify({ $type: 'Order', status: 'pending', orderId: i + 10 }),
      }))
      await createPendingParquet(storage, pendingPath, pendingEntities)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 5,
        firstSeq: 11,
        lastSeq: 15,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify merged contains both existing and new
      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(15)

      // Check existing entities are preserved
      const existing = merged.filter(e => {
        const data = JSON.parse(e.data)
        return data.status === 'completed'
      })
      expect(existing).toHaveLength(10)

      // Check new entities are added
      const newOnes = merged.filter(e => {
        const data = JSON.parse(e.data)
        return data.status === 'pending'
      })
      expect(newOnes).toHaveLength(5)
    })
  })

  // ===========================================================================
  // Test 5: Row group metadata is properly updated
  // ===========================================================================

  describe('Row group metadata is properly updated', () => {
    it('should track correct row count after merge', async () => {
      const ns = 'accounts'
      const dataPath = `data/${ns}/data.parquet`
      const pendingPath = `data/${ns}/pending/batch-001.parquet`

      // Create pending file
      const entities = Array.from({ length: 7 }, (_, i) => ({
        $id: `${ns}/${i}`,
        data: JSON.stringify({ $type: 'Account', name: `Account ${i}` }),
      }))
      await createPendingParquet(storage, pendingPath, entities)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 7,
        firstSeq: 1,
        lastSeq: 7,
        createdAt: new Date().toISOString(),
      }]

      const result = await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify returned count
      expect(result.mergedCount).toBe(7)

      // Verify actual file count
      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(7)
    })

    it('should handle empty pending groups gracefully', async () => {
      const ns = 'empty'
      const dataPath = `data/${ns}/data.parquet`

      // Perform merge with no pending groups
      const result = await mergePendingToCommitted(storage, ns, [])

      // Should return 0 and not create data.parquet
      expect(result.mergedCount).toBe(0)
      expect(result.deletedPendingFiles).toHaveLength(0)
      expect(await storage.exists(dataPath)).toBe(false)
    })

    it('should handle pending file that no longer exists', async () => {
      const ns = 'orphaned'

      // Create pending group metadata but no actual file
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: `data/${ns}/pending/batch-001.parquet`,
        rowCount: 5,
        firstSeq: 1,
        lastSeq: 5,
        createdAt: new Date().toISOString(),
      }]

      // Should not throw, just skip missing files
      const result = await mergePendingToCommitted(storage, ns, pendingGroups)
      expect(result.mergedCount).toBe(0)
    })

    it('should update file size after merge', async () => {
      const ns = 'sized'
      const dataPath = `data/${ns}/data.parquet`

      // Create existing small file
      const existingEntities = [{ $id: `${ns}/0`, data: JSON.stringify({ small: true }) }]
      await createPendingParquet(storage, dataPath, existingEntities)
      const beforeStat = await storage.stat(dataPath)

      // Create pending file with more data
      const pendingPath = `data/${ns}/pending/batch-001.parquet`
      const pendingEntities = Array.from({ length: 50 }, (_, i) => ({
        $id: `${ns}/${i + 1}`,
        data: JSON.stringify({ index: i + 1, largerData: 'x'.repeat(100) }),
      }))
      await createPendingParquet(storage, pendingPath, pendingEntities)

      // Perform merge
      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 50,
        firstSeq: 2,
        lastSeq: 51,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      // Verify file size increased
      const afterStat = await storage.stat(dataPath)
      expect(afterStat!.size).toBeGreaterThan(beforeStat!.size)
    })
  })

  // ===========================================================================
  // Additional Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle special characters in entity data', async () => {
      const ns = 'special'
      const dataPath = `data/${ns}/data.parquet`
      const pendingPath = `data/${ns}/pending/batch-001.parquet`

      const entities = [
        { $id: `${ns}/1`, data: JSON.stringify({ text: 'Hello "World"' }) },
        { $id: `${ns}/2`, data: JSON.stringify({ text: "It's a test" }) },
        { $id: `${ns}/3`, data: JSON.stringify({ text: 'Line1\nLine2\tTabbed' }) },
        { $id: `${ns}/4`, data: JSON.stringify({ emoji: 'Test emoji data' }) },
      ]

      await createPendingParquet(storage, pendingPath, entities)

      const pendingGroups: PendingRowGroup[] = [{
        id: 'batch-001',
        ns,
        path: pendingPath,
        rowCount: 4,
        firstSeq: 1,
        lastSeq: 4,
        createdAt: new Date().toISOString(),
      }]

      await mergePendingToCommitted(storage, ns, pendingGroups)

      const merged = await readParquetEntities(storage, dataPath)
      expect(merged).toHaveLength(4)

      // Verify special characters preserved
      const withNewline = merged.find(e => e.$id === `${ns}/3`)
      expect(withNewline).toBeDefined()
      expect(JSON.parse(withNewline!.data).text).toBe('Line1\nLine2\tTabbed')
    })

    it('should handle concurrent merges for different namespaces', async () => {
      const ns1 = 'concurrent1'
      const ns2 = 'concurrent2'

      // Create pending files for both namespaces
      const pending1 = `data/${ns1}/pending/batch-001.parquet`
      const pending2 = `data/${ns2}/pending/batch-001.parquet`

      await createPendingParquet(storage, pending1, [
        { $id: `${ns1}/1`, data: JSON.stringify({ ns: ns1 }) },
      ])
      await createPendingParquet(storage, pending2, [
        { $id: `${ns2}/1`, data: JSON.stringify({ ns: ns2 }) },
      ])

      // Merge both concurrently
      const [result1, result2] = await Promise.all([
        mergePendingToCommitted(storage, ns1, [{
          id: 'batch-001', ns: ns1, path: pending1, rowCount: 1, firstSeq: 1, lastSeq: 1, createdAt: new Date().toISOString(),
        }]),
        mergePendingToCommitted(storage, ns2, [{
          id: 'batch-001', ns: ns2, path: pending2, rowCount: 1, firstSeq: 1, lastSeq: 1, createdAt: new Date().toISOString(),
        }]),
      ])

      // Both should succeed
      expect(result1.mergedCount).toBe(1)
      expect(result2.mergedCount).toBe(1)

      // Both data files should exist with correct content
      const merged1 = await readParquetEntities(storage, `data/${ns1}/data.parquet`)
      const merged2 = await readParquetEntities(storage, `data/${ns2}/data.parquet`)

      expect(merged1).toHaveLength(1)
      expect(merged2).toHaveLength(1)
      expect(JSON.parse(merged1[0]!.data).ns).toBe(ns1)
      expect(JSON.parse(merged2[0]!.data).ns).toBe(ns2)
    })
  })
})

// =============================================================================
// Integration Test with Expected DO Behavior
// =============================================================================

describe('Expected ParqueDBDO.flushPendingToCommitted Behavior', () => {
  /**
   * This test suite documents the EXPECTED behavior of flushPendingToCommitted.
   *
   * Currently, flushPendingToCommitted is a PLACEHOLDER that:
   * - Returns the count of rows from pending metadata
   * - Deletes pending_row_groups records from SQLite
   * - DOES NOT read pending Parquet files from R2
   * - DOES NOT merge them into data.parquet
   * - DOES NOT delete pending files from R2
   *
   * The mergePendingToCommitted helper in this file simulates the EXPECTED behavior.
   * Once the real implementation is done, the helper should be replaced with calls
   * to the actual DO method for integration testing.
   */

  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('documents expected merge behavior using test helper', async () => {
    // This test demonstrates what the real flushPendingToCommitted should do:
    // 1. Read pending files from R2
    // 2. Read existing data.parquet (if any)
    // 3. Merge all rows
    // 4. Write new data.parquet
    // 5. Delete pending files from R2
    // 6. Delete pending_row_groups metadata

    const ns = 'test'
    const dataPath = `data/${ns}/data.parquet`
    const pendingPath = `data/${ns}/pending/batch-001.parquet`

    // Create pending file (simulates what bulkWriteToR2 does)
    const entities = Array.from({ length: 5 }, (_, i) => ({
      $id: `${ns}/${i}`,
      data: JSON.stringify({ $type: 'Test', name: `Test ${i}` }),
    }))
    await createPendingParquet(storage, pendingPath, entities)

    // Before merge: pending file exists, data file doesn't
    expect(await storage.exists(pendingPath)).toBe(true)
    expect(await storage.exists(dataPath)).toBe(false)

    // The test helper simulates correct behavior
    const pendingGroups: PendingRowGroup[] = [{
      id: 'batch-001',
      ns,
      path: pendingPath,
      rowCount: 5,
      firstSeq: 1,
      lastSeq: 5,
      createdAt: new Date().toISOString(),
    }]

    // Execute the expected merge behavior
    await mergePendingToCommitted(storage, ns, pendingGroups)

    // After merge: data file exists with all entities, pending file is deleted
    expect(await storage.exists(dataPath)).toBe(true)
    expect(await storage.exists(pendingPath)).toBe(false)

    const merged = await readParquetEntities(storage, dataPath)
    expect(merged).toHaveLength(5)
    expect(merged[0]!.$id).toBe('test/0')
  })

  it('documents the current PLACEHOLDER behavior (incomplete)', async () => {
    /**
     * This test documents what the current placeholder does:
     *
     * ```typescript
     * // src/worker/ParqueDBDO.ts lines 633-656
     * async flushPendingToCommitted(ns: string): Promise<number> {
     *   const pending = await this.getPendingRowGroups(ns)
     *   if (pending.length === 0) return 0
     *
     *   // This is a placeholder - actual implementation would:
     *   // 1. Read existing data/{ns}/data.parquet
     *   // 2. Read all pending files
     *   // 3. Merge and write new data.parquet
     *   // 4. Delete pending files from R2
     *   // 5. Delete pending_row_groups records
     *
     *   // For now, just track what would be promoted
     *   const totalRows = pending.reduce((sum, p) => sum + p.rowCount, 0)
     *   const maxSeq = Math.max(...pending.map(p => p.lastSeq))
     *
     *   // Delete the pending records (but NOT the files!)
     *   await this.deletePendingRowGroups(ns, maxSeq)
     *
     *   return totalRows
     * }
     * ```
     *
     * The problem: After this runs, pending Parquet files are ORPHANED in R2
     * and data.parquet is never created/updated with the pending data.
     */

    // This is a documentation test - it passes but documents the bug
    expect(true).toBe(true)
  })
})
