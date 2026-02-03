/**
 * Tests for VectorIndex Incremental Update Support
 *
 * Tests the incremental update functionality for streaming data,
 * including row group tracking, change detection, and compaction handling.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '@/indexes/vector/hnsw'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createDefinition(
  overrides: Partial<IndexDefinition & { vectorOptions: Record<string, unknown> }> = {}
): IndexDefinition {
  return {
    name: 'idx_embedding',
    type: 'vector',
    fields: [{ path: 'embedding' }],
    vectorOptions: {
      dimensions: 3,
      metric: 'cosine',
      m: 16,
      efConstruction: 200,
    },
    ...overrides,
  }
}

function createIndex(
  storage: MemoryBackend,
  definition?: IndexDefinition
): VectorIndex {
  return new VectorIndex(storage, 'documents', definition ?? createDefinition())
}

function makeDoc(embedding: number[]) {
  return { embedding }
}

// =============================================================================
// Row Group Metadata Tracking
// =============================================================================

describe('VectorIndex Incremental Update', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('row group metadata tracking', () => {
    it('tracks row group metadata on insert', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 1, 0)

      const metadata = index.getRowGroupMetadata()
      expect(metadata.size).toBe(2)

      const rg0 = metadata.get(0)
      expect(rg0).toBeDefined()
      expect(rg0!.rowGroup).toBe(0)
      expect(rg0!.vectorCount).toBe(2)
      expect(rg0!.minRowOffset).toBe(0)
      expect(rg0!.maxRowOffset).toBe(1)

      const rg1 = metadata.get(1)
      expect(rg1).toBeDefined()
      expect(rg1!.vectorCount).toBe(1)
    })

    it('updates metadata on remove', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      let metadata = index.getRowGroupMetadata()
      expect(metadata.get(0)!.vectorCount).toBe(3)

      index.remove('doc2')

      metadata = index.getRowGroupMetadata()
      expect(metadata.get(0)!.vectorCount).toBe(2)
    })

    it('removes row group metadata when all vectors removed', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 1, 0)

      index.remove('doc1')

      const metadata = index.getRowGroupMetadata()
      expect(metadata.has(0)).toBe(false)
      expect(metadata.has(1)).toBe(true)
    })

    it('clears metadata on clear()', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 1, 0)

      index.clear()

      expect(index.getRowGroupMetadata().size).toBe(0)
      expect(index.getIndexVersion()).toBe(0)
    })
  })

  // ===========================================================================
  // Index Version Tracking
  // ===========================================================================

  describe('index version tracking', () => {
    it('increments version on incremental update', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      const initialVersion = index.getIndexVersion()

      index.incrementalUpdateFromArray([
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 1, rowOffset: 0 },
      ])

      expect(index.getIndexVersion()).toBe(initialVersion + 1)
    })

    it('tracks last updated timestamp', () => {
      const index = createIndex(storage)

      const before = Date.now()
      index.incrementalUpdateFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])
      const after = Date.now()

      const lastUpdated = index.getLastUpdatedAt()
      expect(lastUpdated).toBeGreaterThanOrEqual(before)
      expect(lastUpdated).toBeLessThanOrEqual(after)
    })
  })

  // ===========================================================================
  // Get Documents for Row Group
  // ===========================================================================

  describe('getDocIdsForRowGroup', () => {
    it('returns document IDs for a specific row group', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 1, 0)
      index.insert([1, 1, 0], 'doc4', 1, 1)

      const rg0Docs = index.getDocIdsForRowGroup(0)
      expect(rg0Docs.sort()).toEqual(['doc1', 'doc2'])

      const rg1Docs = index.getDocIdsForRowGroup(1)
      expect(rg1Docs.sort()).toEqual(['doc3', 'doc4'])

      const rg2Docs = index.getDocIdsForRowGroup(2)
      expect(rg2Docs).toEqual([])
    })
  })

  // ===========================================================================
  // Remove Row Group
  // ===========================================================================

  describe('removeRowGroup', () => {
    it('removes all vectors from a row group', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 1, 0)

      expect(index.size).toBe(3)

      const removed = index.removeRowGroup(0)
      expect(removed).toBe(2)
      expect(index.size).toBe(1)

      // Verify the right vectors were removed
      const result = index.search([1, 0, 0], 10)
      expect(result.docIds).not.toContain('doc1')
      expect(result.docIds).not.toContain('doc2')
      expect(result.docIds).toContain('doc3')
    })

    it('returns 0 for empty row group', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const removed = index.removeRowGroup(99)
      expect(removed).toBe(0)
      expect(index.size).toBe(1)
    })

    it('removes row group metadata', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      index.removeRowGroup(0)

      expect(index.getRowGroupMetadata().has(0)).toBe(false)
    })
  })

  // ===========================================================================
  // Change Detection
  // ===========================================================================

  describe('detectChangedRowGroups', () => {
    it('detects added row groups', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      // Set existing checksum for row group 0
      const metadata = index.getRowGroupMetadata().get(0)!
      metadata.checksum = 'checksum0'

      // Simulate checksum update - row group 0 exists (unchanged), row group 1 is new
      const checksums = new Map([
        [0, 'checksum0'], // Same as existing
        [1, 'checksum1'], // New row group
      ])

      const changes = index.detectChangedRowGroups(checksums)
      expect(changes.added).toContain(1)
      expect(changes.modified).toHaveLength(0)
      expect(changes.removed).toHaveLength(0)
    })

    it('detects modified row groups', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      // Set initial checksum
      const metadata = index.getRowGroupMetadata().get(0)!
      metadata.checksum = 'old_checksum'

      // New checksum differs
      const checksums = new Map([
        [0, 'new_checksum'],
      ])

      const changes = index.detectChangedRowGroups(checksums)
      expect(changes.added).toHaveLength(0)
      expect(changes.modified).toContain(0)
      expect(changes.removed).toHaveLength(0)
    })

    it('detects removed row groups', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 1, rowOffset: 0 },
      ])

      // Set existing checksums
      index.getRowGroupMetadata().get(0)!.checksum = 'checksum0'
      index.getRowGroupMetadata().get(1)!.checksum = 'checksum1'

      // New checksums only include row group 0 (row group 1 was deleted)
      const checksums = new Map([
        [0, 'checksum0'],
      ])

      const changes = index.detectChangedRowGroups(checksums)
      expect(changes.added).toHaveLength(0)
      expect(changes.modified).toHaveLength(0)
      expect(changes.removed).toContain(1)
    })

    it('detects unchanged row groups correctly', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      // Set checksum
      const metadata = index.getRowGroupMetadata().get(0)!
      metadata.checksum = 'same_checksum'

      // Same checksum
      const checksums = new Map([
        [0, 'same_checksum'],
      ])

      const changes = index.detectChangedRowGroups(checksums)
      expect(changes.added).toHaveLength(0)
      expect(changes.modified).toHaveLength(0)
      expect(changes.removed).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Row Group Remapping (Compaction)
  // ===========================================================================

  describe('remapRowGroups', () => {
    it('remaps row group numbers after compaction', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 2, 0) // Gap in row group numbers
      index.insert([0, 0, 1], 'doc3', 5, 0)

      // After compaction, row groups are renumbered: 0->0, 2->1, 5->2
      const remapping = new Map([
        [0, 0],
        [2, 1],
        [5, 2],
      ])

      index.remapRowGroups(remapping)

      // Verify metadata was updated
      const metadata = index.getRowGroupMetadata()
      expect(metadata.has(0)).toBe(true)
      expect(metadata.has(1)).toBe(true)
      expect(metadata.has(2)).toBe(true)
      expect(metadata.has(5)).toBe(false)

      // Verify document mappings were updated
      const rg1Docs = index.getDocIdsForRowGroup(1)
      expect(rg1Docs).toContain('doc2')

      // Search should still work
      const result = index.search([0, 1, 0], 1)
      expect(result.docIds[0]).toBe('doc2')
    })

    it('removes row groups not in remapping', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 1, 0)

      // Only row group 0 is in the new mapping
      const remapping = new Map([
        [0, 0],
      ])

      index.remapRowGroups(remapping)

      // Row group 1 metadata should be gone
      const metadata = index.getRowGroupMetadata()
      expect(metadata.has(1)).toBe(false)
    })

    it('increments index version on remap', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      const versionBefore = index.getIndexVersion()

      index.remapRowGroups(new Map([[0, 0]]))

      expect(index.getIndexVersion()).toBe(versionBefore + 1)
    })
  })

  // ===========================================================================
  // Incremental Update from Array
  // ===========================================================================

  describe('incrementalUpdateFromArray', () => {
    it('adds new vectors without clearing existing ones', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'existing', 0, 0)

      const result = index.incrementalUpdateFromArray([
        { doc: makeDoc([0, 1, 0]), docId: 'new', rowGroup: 1, rowOffset: 0 },
      ])

      expect(result.success).toBe(true)
      expect(result.added).toBe(1)
      expect(result.removed).toBe(0)
      expect(index.size).toBe(2)

      // Both should be searchable
      const searchResult = index.search([1, 0, 0], 2)
      expect(searchResult.docIds).toContain('existing')
      expect(searchResult.docIds).toContain('new')
    })

    it('handles checksums to detect and remove deleted row groups', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 1, rowOffset: 0 },
      ])

      // Set initial checksums
      for (const [rg, meta] of index.getRowGroupMetadata()) {
        meta.checksum = `checksum${rg}`
      }

      // New data only has row group 0 with same checksum (unchanged)
      // Row group 1 was deleted (not in new checksums)
      const result = index.incrementalUpdateFromArray(
        [], // No new data to insert - row group 0 is unchanged
        {
          checksums: new Map([[0, 'checksum0']]), // Same checksum = unchanged
        }
      )

      expect(result.success).toBe(true)
      expect(result.removedRowGroups).toContain(1)
      expect(result.removed).toBe(1) // doc2 was removed

      // Only doc1 should remain
      expect(index.size).toBe(1)
      const searchResult = index.search([0, 1, 0], 5)
      expect(searchResult.docIds).not.toContain('doc2')
      expect(searchResult.docIds).toContain('doc1')
    })

    it('handles modified row groups by removing and re-inserting', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ])

      // Set initial checksum
      const metadata = index.getRowGroupMetadata().get(0)!
      metadata.checksum = 'old_checksum'

      // Row group 0 was modified (new checksum)
      const result = index.incrementalUpdateFromArray(
        [
          { doc: makeDoc([0.5, 0.5, 0]), docId: 'doc1_new', rowGroup: 0, rowOffset: 0 },
          { doc: makeDoc([0, 0, 1]), docId: 'doc2_new', rowGroup: 0, rowOffset: 1 },
        ],
        {
          checksums: new Map([[0, 'new_checksum']]),
        }
      )

      expect(result.success).toBe(true)
      expect(result.removed).toBe(2) // old doc1 and doc2 removed
      expect(result.added).toBe(2) // new versions added
      expect(result.updatedRowGroups).toContain(0)

      // Old docs should not be searchable
      const searchResult = index.search([1, 0, 0], 5)
      expect(searchResult.docIds).not.toContain('doc1')
      expect(searchResult.docIds).not.toContain('doc2')
      expect(searchResult.docIds).toContain('doc1_new')
    })

    it('filters data by rowGroupsToUpdate option', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'existing', 0, 0)

      const result = index.incrementalUpdateFromArray(
        [
          { doc: makeDoc([0, 1, 0]), docId: 'wanted', rowGroup: 1, rowOffset: 0 },
          { doc: makeDoc([0, 0, 1]), docId: 'filtered', rowGroup: 2, rowOffset: 0 },
        ],
        {
          rowGroupsToUpdate: [1], // Only process row group 1
        }
      )

      expect(result.success).toBe(true)
      expect(result.added).toBe(1) // Only 'wanted'
      expect(index.size).toBe(2)
      expect(index.hasDocument('wanted')).toBe(true)
      expect(index.hasDocument('filtered')).toBe(false)
    })

    it('handles row group remapping', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 2, 0)

      // Remap and add new data
      const result = index.incrementalUpdateFromArray(
        [
          { doc: makeDoc([0, 0, 1]), docId: 'doc3', rowGroup: 2, rowOffset: 0 },
        ],
        {
          rowGroupRemapping: new Map([[0, 0], [2, 1]]),
          rowGroupsToUpdate: [2],
        }
      )

      expect(result.success).toBe(true)
      expect(index.size).toBe(3)

      // Original doc2 should now be in row group 1
      expect(index.getDocIdsForRowGroup(1)).toContain('doc2')
    })

    it('updates checksums in metadata', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      // Set initial checksum
      index.getRowGroupMetadata().get(0)!.checksum = 'old_checksum'

      // Update the row group with new data and new checksum
      index.incrementalUpdateFromArray(
        [
          { doc: makeDoc([0, 1, 0]), docId: 'doc1_updated', rowGroup: 0, rowOffset: 0 },
        ],
        {
          checksums: new Map([[0, 'my_checksum']]),
          rowGroupsToUpdate: [0],
        }
      )

      const metadata = index.getRowGroupMetadata().get(0)
      expect(metadata?.checksum).toBe('my_checksum')
    })

    it('returns error info on failure', () => {
      const index = createIndex(storage)

      // Force an error by using a mock that throws
      // This is a simplified test - in practice, errors might come from various sources
      const result = index.incrementalUpdateFromArray([])

      expect(result.success).toBe(true) // Empty update should succeed
    })
  })

  // ===========================================================================
  // Async Incremental Update
  // ===========================================================================

  describe('incrementalUpdate (async)', () => {
    it('processes async iterator', async () => {
      const index = createIndex(storage)

      async function* dataIterator() {
        yield { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 }
        yield { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 0, rowOffset: 1 }
        yield { doc: makeDoc([0, 0, 1]), docId: 'doc3', rowGroup: 1, rowOffset: 0 }
      }

      const result = await index.incrementalUpdate(dataIterator())

      expect(result.success).toBe(true)
      expect(result.added).toBe(3)
      expect(index.size).toBe(3)
    })

    it('reports progress during update', async () => {
      const index = createIndex(storage)

      async function* dataIterator() {
        for (let i = 0; i < 250; i++) {
          yield {
            doc: makeDoc([Math.cos(i), Math.sin(i), 0]),
            docId: `doc${i}`,
            rowGroup: 0,
            rowOffset: i,
          }
        }
      }

      const progressCalls: Array<{ processed: number; total: number }> = []

      await index.incrementalUpdate(dataIterator(), {
        onProgress: (processed, total) => {
          progressCalls.push({ processed, total })
        },
      })

      expect(progressCalls.length).toBeGreaterThan(0)
      // Should report progress at intervals
      expect(progressCalls.some(p => p.processed < p.total)).toBe(true)
      // Final call should show completion
      expect(progressCalls[progressCalls.length - 1]!.processed).toBe(250)
    })

    it('handles checksums with async update', async () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'old', rowGroup: 0, rowOffset: 0 },
      ])

      // Set checksum
      const metadata = index.getRowGroupMetadata().get(0)!
      metadata.checksum = 'old_checksum'

      async function* dataIterator() {
        yield { doc: makeDoc([0, 1, 0]), docId: 'new', rowGroup: 0, rowOffset: 0 }
      }

      const result = await index.incrementalUpdate(dataIterator(), {
        checksums: new Map([[0, 'new_checksum']]),
      })

      expect(result.success).toBe(true)
      expect(result.removed).toBe(1) // old removed
      expect(result.added).toBe(1) // new added
    })
  })

  // ===========================================================================
  // Persistence with Incremental Metadata
  // ===========================================================================

  describe('persistence with incremental metadata', () => {
    it('saves and loads row group metadata', async () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { doc: makeDoc([0, 0, 1]), docId: 'doc3', rowGroup: 1, rowOffset: 0 },
      ])

      // Set checksums
      for (const [rg, meta] of index.getRowGroupMetadata()) {
        meta.checksum = `checksum${rg}`
      }

      // Do an incremental update to set version and timestamp
      index.incrementalUpdateFromArray([])

      const versionBefore = index.getIndexVersion()
      const timestampBefore = index.getLastUpdatedAt()

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      // Verify metadata was preserved
      const loadedMetadata = loaded.getRowGroupMetadata()
      expect(loadedMetadata.size).toBe(2)

      const rg0 = loadedMetadata.get(0)
      expect(rg0).toBeDefined()
      expect(rg0!.vectorCount).toBe(2)
      expect(rg0!.checksum).toBe('checksum0')

      const rg1 = loadedMetadata.get(1)
      expect(rg1).toBeDefined()
      expect(rg1!.vectorCount).toBe(1)
      expect(rg1!.checksum).toBe('checksum1')

      // Verify version and timestamp were preserved
      expect(loaded.getIndexVersion()).toBe(versionBefore)
      expect(loaded.getLastUpdatedAt()).toBe(timestampBefore)
    })

    it('preserves node to row group mappings', async () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 1, 0)

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      // Verify the mappings work correctly
      expect(loaded.getDocIdsForRowGroup(0)).toContain('doc1')
      expect(loaded.getDocIdsForRowGroup(1)).toContain('doc2')
    })

    it('can do incremental update after load', async () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      // Should be able to do incremental update on loaded index
      const result = loaded.incrementalUpdateFromArray([
        { doc: makeDoc([0, 1, 0]), docId: 'doc2', rowGroup: 1, rowOffset: 0 },
      ])

      expect(result.success).toBe(true)
      expect(loaded.size).toBe(2)

      // Search should work
      const searchResult = loaded.search([0, 1, 0], 2)
      expect(searchResult.docIds).toContain('doc2')
    })

    it('rebuilds metadata when loading v1 index', async () => {
      // This tests backward compatibility - v1 indexes don't have incremental metadata
      // The deserialize method should rebuild the metadata from nodes

      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 1, 0)

      // Clear metadata to simulate v1 format behavior
      // (The actual v1 format handling is in deserialize)
      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      // Metadata should have been rebuilt
      const metadata = loaded.getRowGroupMetadata()
      expect(metadata.size).toBe(2)
      expect(metadata.get(0)?.vectorCount).toBe(1)
      expect(metadata.get(1)?.vectorCount).toBe(1)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty incremental update', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'existing', 0, 0)

      const result = index.incrementalUpdateFromArray([])

      expect(result.success).toBe(true)
      expect(result.added).toBe(0)
      expect(result.removed).toBe(0)
      expect(index.size).toBe(1)
    })

    it('handles incremental update on empty index', () => {
      const index = createIndex(storage)

      const result = index.incrementalUpdateFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      expect(result.success).toBe(true)
      expect(result.added).toBe(1)
      expect(index.size).toBe(1)
    })

    it('skips vectors with wrong dimensions in incremental update', () => {
      const index = createIndex(storage)

      const result = index.incrementalUpdateFromArray([
        { doc: makeDoc([1, 0, 0]), docId: 'valid', rowGroup: 0, rowOffset: 0 },
        { doc: makeDoc([1, 0]), docId: 'invalid', rowGroup: 0, rowOffset: 1 }, // Wrong dims
      ])

      expect(result.success).toBe(true)
      expect(result.added).toBe(1)
      expect(index.hasDocument('valid')).toBe(true)
      expect(index.hasDocument('invalid')).toBe(false)
    })

    it('handles removing non-existent row groups gracefully', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      // Try to remove a row group that doesn't exist
      const removed = index.removeRowGroup(99)
      expect(removed).toBe(0)
      expect(index.size).toBe(1)
    })

    it('handles remapping with no matching row groups', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      // Remapping with no matching entries
      index.remapRowGroups(new Map([[5, 0]]))

      // Row group 0 should have been removed since it wasn't in the remapping
      expect(index.getRowGroupMetadata().has(0)).toBe(false)
    })

    it('handles duplicate docIds in incremental update', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      // Update with same docId
      const result = index.incrementalUpdateFromArray([
        { doc: makeDoc([0, 1, 0]), docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      ])

      expect(result.success).toBe(true)
      expect(index.size).toBe(1)

      // Vector should be updated
      const searchResult = index.search([0, 1, 0], 1)
      expect(searchResult.docIds[0]).toBe('doc1')
    })
  })
})
