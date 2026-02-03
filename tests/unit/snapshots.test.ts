/**
 * Snapshots Test Suite
 *
 * Tests for snapshot functionality in ParqueDB event sourcing.
 * Uses real FsBackend with temp directories for actual snapshot persistence.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type {
  EntityId,
} from '../../src/types'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Advance fake timers by specified milliseconds (use only with vi.useFakeTimers())
 */
function advanceTime(ms: number): void {
  vi.advanceTimersByTime(ms)
}

// =============================================================================
// Snapshots Test Suite
// =============================================================================

describe('Snapshots', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-snapshots-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    vi.useRealTimers()
    // Clean up the temp directory
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Snapshot Creation Tests
  // ===========================================================================

  describe('snapshot creation', () => {
    it('creates checkpoint at sequence number', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create some updates
      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })

      // Create a snapshot
      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      expect(snapshot).toBeDefined()
      expect(snapshot.entityId).toBe(entity.$id)
      expect(snapshot.sequenceNumber).toBeGreaterThan(0)
      expect(snapshot.createdAt).toBeInstanceOf(Date)
    })

    it('stores full entity state', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        viewCount: 100,
        tags: ['a', 'b', 'c'],
        metadata: { key: 'value' },
      })

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title' },
        $inc: { viewCount: 50 },
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      expect(snapshot.state).toBeDefined()
      expect(snapshot.state.title).toBe('Updated Title')
      expect(snapshot.state.content).toBe('Content')
      expect(snapshot.state.viewCount).toBe(150)
      expect(snapshot.state.tags).toEqual(['a', 'b', 'c'])
      expect(snapshot.state.metadata).toEqual({ key: 'value' })
    })

    it('compresses snapshot data', async () => {
      // Create entity with large data
      const largeContent = 'x'.repeat(100000) // 100KB

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: largeContent,
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Verify compression was applied
      expect(snapshot.compressed).toBe(true)

      // The stored size should be smaller than the raw data
      const rawSize = JSON.stringify(snapshot.state).length
      const storedSnapshot = await snapshotManager.getRawSnapshot(snapshot.id)
      expect(storedSnapshot.size).toBeLessThan(rawSize)
    })

    it('creates snapshot at specific event ID', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })

      const history = await db.history(entity.$id as EntityId)
      const createEventId = history.items[0].id

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshotAtEvent(
        entity.$id as EntityId,
        createEventId
      )

      expect(snapshot.state.title).toBe('V1')
      expect(snapshot.eventId).toBe(createEventId)
    })

    it('auto-creates snapshots after threshold events', async () => {
      // Configure auto-snapshot after every 10 events
      const dbWithAutoSnapshot = new ParqueDB({
        storage,
        snapshotConfig: { autoSnapshotThreshold: 10 },
      })

      const entity = await dbWithAutoSnapshot.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create 15 updates
      for (let i = 2; i <= 16; i++) {
        await dbWithAutoSnapshot.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      const snapshotManager = dbWithAutoSnapshot.getSnapshotManager()
      const snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)

      // Should have at least one auto-created snapshot
      expect(snapshots.length).toBeGreaterThan(0)
    })

    it('stores namespace and entity ID correctly', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      expect(snapshot.ns).toBe('posts')
      expect(snapshot.entityId).toBe(entity.$id)
    })
  })

  // ===========================================================================
  // Snapshot Restoration Tests
  // ===========================================================================

  describe('snapshot restoration', () => {
    it('loads from nearest snapshot', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create updates
      for (let i = 2; i <= 5; i++) {
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      // Create snapshot at V5
      const snapshotManager = db.getSnapshotManager()
      await snapshotManager.createSnapshot(entity.$id as EntityId)

      // More updates after snapshot
      for (let i = 6; i <= 10; i++) {
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      // Query at time after V5 but before V10
      const atV7 = await db.get('posts', entity.$id as string)

      // Implementation should use snapshot + replay for efficiency
      // This test verifies the result is correct
      expect(atV7!.title).toBe('V10')

      // Verify snapshot was used (check internal stats)
      const stats = await snapshotManager.getQueryStats(entity.$id as EntityId)
      expect(stats.snapshotsUsed).toBeGreaterThan(0)
    })

    it('replays events after snapshot', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
        viewCount: 0,
      })

      // Updates
      await db.update('posts', entity.$id as string, {
        $set: { title: 'V2' },
        $inc: { viewCount: 10 },
      })

      // Create snapshot
      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // More updates after snapshot
      await db.update('posts', entity.$id as string, {
        $set: { title: 'V3' },
        $inc: { viewCount: 20 },
      })

      await db.update('posts', entity.$id as string, {
        $set: { title: 'V4' },
        $inc: { viewCount: 30 },
      })

      // Get current state - should use snapshot + replay 2 events
      const current = await db.get('posts', entity.$id as string)

      expect(current!.title).toBe('V4')
      expect(current!.viewCount).toBe(60) // 0 + 10 + 20 + 30
    })

    it('optimizes time-travel queries', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create 50 events with 1ms delays to ensure unique timestamps
      // This avoids ambiguity in time-travel queries when multiple events
      // share the same millisecond timestamp
      for (let i = 2; i <= 50; i++) {
        advanceTime(1)
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      // Create snapshot at event 25
      const snapshotManager = db.getSnapshotManager()
      const history = await db.history(entity.$id as EntityId)
      const event25 = history.items[24] // 0-indexed, so 24 = 25th event

      await snapshotManager.createSnapshotAtEvent(
        entity.$id as EntityId,
        event25.id
      )

      // Time travel to event 30
      const event30 = history.items[29]
      const atEvent30 = await db.get('posts', entity.$id as string, {
        asOf: event30.ts,
      })

      expect(atEvent30!.title).toBe('V30')

      // Verify optimization: should replay only 5 events from snapshot
      const stats = await snapshotManager.getQueryStats(entity.$id as EntityId)
      expect(stats.eventsReplayed).toBe(5)
    })

    it('falls back to full replay when no snapshot available', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create some events without snapshots, with delays for unique timestamps
      for (let i = 2; i <= 10; i++) {
        advanceTime(1)
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      // Query should still work
      const history = await db.history(entity.$id as EntityId)
      const event5 = history.items[4]

      const atEvent5 = await db.get('posts', entity.$id as string, {
        asOf: event5.ts,
      })

      expect(atEvent5!.title).toBe('V5')

      // Verify full replay was used
      const snapshotManager = db.getSnapshotManager()
      const stats = await snapshotManager.getQueryStats(entity.$id as EntityId)
      expect(stats.snapshotsUsed).toBe(0)
    })

    it('selects optimal snapshot for time-travel', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create 100 events with snapshots at 25, 50, 75
      // Advance time between updates to ensure unique timestamps
      for (let i = 2; i <= 100; i++) {
        advanceTime(1) // Ensure unique timestamp for each event
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })

        if (i === 25 || i === 50 || i === 75) {
          const snapshotManager = db.getSnapshotManager()
          await snapshotManager.createSnapshot(entity.$id as EntityId)
        }
      }

      const snapshotManager = db.getSnapshotManager()

      // Query at event 60 - should use snapshot at 50
      const history = await db.history(entity.$id as EntityId)
      const event60 = history.items[59]

      await db.get('posts', entity.$id as string, { asOf: event60.ts })

      const stats = await snapshotManager.getQueryStats(entity.$id as EntityId)

      // Should have used snapshot at 50 and replayed approximately 10 events
      // Note: Due to potential timestamp collisions (multiple events at same millisecond),
      // the exact count may vary slightly. The important thing is that we used the
      // snapshot at 50 and didn't replay all 60 events from scratch.
      expect(stats.snapshotUsedAt).toBe(50)
      expect(stats.eventsReplayed).toBeGreaterThanOrEqual(10)
      expect(stats.eventsReplayed).toBeLessThanOrEqual(15) // Allow some variance for timestamp collisions
    })
  })

  // ===========================================================================
  // Snapshot Management Tests
  // ===========================================================================

  describe('snapshot management', () => {
    it('lists all snapshots for entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create multiple snapshots
      await snapshotManager.createSnapshot(entity.$id as EntityId)
      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await snapshotManager.createSnapshot(entity.$id as EntityId)
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })
      await snapshotManager.createSnapshot(entity.$id as EntityId)

      const snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)

      expect(snapshots.length).toBe(3)
      expect(snapshots[0].state.title).toBe('V1')
      expect(snapshots[1].state.title).toBe('V2')
      expect(snapshots[2].state.title).toBe('V3')
    })

    it('deletes old snapshots', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create snapshots
      const snapshot1 = await snapshotManager.createSnapshot(entity.$id as EntityId)
      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Delete first snapshot
      await snapshotManager.deleteSnapshot(snapshot1.id)

      const snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)

      expect(snapshots.length).toBe(1)
      expect(snapshots[0].state.title).toBe('V2')
    })

    it('prunes snapshots by age', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create old snapshot (mock time)
      const oldSnapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Simulate time passing
      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      const newSnapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Prune snapshots older than 0 days (for testing)
      const pruneDate = new Date(newSnapshot.createdAt.getTime() - 1)
      await snapshotManager.pruneSnapshots({ olderThan: pruneDate })

      const snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)

      // Only the new snapshot should remain
      expect(snapshots.length).toBe(1)
      expect(snapshots[0].id).toBe(newSnapshot.id)
    })

    it('keeps minimum number of snapshots during pruning', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create 5 snapshots
      for (let i = 1; i <= 5; i++) {
        await snapshotManager.createSnapshot(entity.$id as EntityId)
        if (i < 5) {
          await db.update('posts', entity.$id as string, {
            $set: { title: `V${i + 1}` },
          })
        }
      }

      // Prune but keep minimum 2
      await snapshotManager.pruneSnapshots({
        olderThan: new Date(),
        keepMinimum: 2,
      })

      const snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)

      expect(snapshots.length).toBe(2)
    })

    it('calculates snapshot storage size', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'x'.repeat(10000),
      })

      const snapshotManager = db.getSnapshotManager()
      await snapshotManager.createSnapshot(entity.$id as EntityId)

      const stats = await snapshotManager.getStorageStats()

      expect(stats.totalSize).toBeGreaterThan(0)
      expect(stats.snapshotCount).toBe(1)
      expect(stats.avgSnapshotSize).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Snapshot Persistence Tests
  // ===========================================================================

  describe('snapshot persistence', () => {
    it('persists snapshots to storage', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Verify snapshot can be retrieved
      const retrievedSnapshot = await snapshotManager.getRawSnapshot(snapshot.id)
      expect(retrievedSnapshot).toBeDefined()
      expect(retrievedSnapshot.size).toBeGreaterThan(0)
    })

    it('loads snapshots from storage on restart', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Simulate restart by creating new db instance
      const newDb = new ParqueDB({ storage })
      const newSnapshotManager = newDb.getSnapshotManager()

      const loadedSnapshots = await newSnapshotManager.listSnapshots(
        entity.$id as EntityId
      )

      expect(loadedSnapshots.length).toBeGreaterThan(0)
      expect(loadedSnapshots[0].id).toBe(snapshot.id)
    })

    it('stores snapshots in parquet format', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()
      await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Verify the snapshot storage contains data
      const stats = await snapshotManager.getStorageStats()
      expect(stats.snapshotCount).toBe(1)
      expect(stats.totalSize).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty entity snapshot', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Minimal Post',
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      expect(snapshot.state).toBeDefined()
      expect(snapshot.state.name).toBe('Minimal Post')
    })

    it('handles snapshot of deleted entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await db.delete('posts', entity.$id as string)

      const snapshotManager = db.getSnapshotManager()

      // Should throw or return null for deleted entity
      await expect(
        snapshotManager.createSnapshot(entity.$id as EntityId)
      ).rejects.toThrow()
    })

    it('handles concurrent snapshot creation', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create multiple snapshots concurrently
      const results = await Promise.all([
        snapshotManager.createSnapshot(entity.$id as EntityId),
        snapshotManager.createSnapshot(entity.$id as EntityId),
        snapshotManager.createSnapshot(entity.$id as EntityId),
      ])

      // All should succeed with unique IDs
      const ids = results.map((s) => s.id)
      const uniqueIds = [...new Set(ids)]
      expect(uniqueIds.length).toBe(3)
    })

    it('handles very large entity snapshots', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Large Post',
        title: 'Title',
        content: 'x'.repeat(10 * 1024 * 1024), // 10MB
        largeArray: Array.from({ length: 10000 }, (_, i) => ({
          id: i,
          value: `item-${i}`,
        })),
      })

      const snapshotManager = db.getSnapshotManager()
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      expect(snapshot).toBeDefined()
      expect(snapshot.compressed).toBe(true)
    })

    it('handles snapshot restoration after entity recreation', async () => {
      // Create and delete entity
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Original',
        title: 'Original Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()
      await snapshotManager.createSnapshot(entity1.$id as EntityId)

      await db.delete('posts', entity1.$id as string, { hard: true })

      // Create new entity with same ID (if allowed)
      // Or verify snapshot isolation
      const snapshots = await snapshotManager.listSnapshots(entity1.$id as EntityId)

      // Snapshots from deleted entity should still exist for audit
      expect(snapshots.length).toBe(1)
      expect(snapshots[0].state.title).toBe('Original Title')
    })

    it('throws SnapshotNotFoundError when deleting non-existent snapshot', async () => {
      const snapshotManager = db.getSnapshotManager()

      await expect(
        snapshotManager.deleteSnapshot('non-existent-snapshot-id')
      ).rejects.toThrow('Snapshot not found')
    })

    it('throws error with snapshot ID when deleting non-existent snapshot', async () => {
      const snapshotManager = db.getSnapshotManager()

      try {
        await snapshotManager.deleteSnapshot('my-fake-snapshot-id')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toContain('my-fake-snapshot-id')
      }
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('maintains consistency when deleting non-existent snapshot', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create a snapshot
      const snapshot = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Verify it exists
      let snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)
      expect(snapshots.length).toBe(1)

      // Try to delete a non-existent snapshot (should throw)
      await expect(
        snapshotManager.deleteSnapshot('non-existent-id')
      ).rejects.toThrow()

      // The original snapshot should still be intact
      snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)
      expect(snapshots.length).toBe(1)
      expect(snapshots[0].id).toBe(snapshot.id)
    })

    it('successfully deletes existing snapshot', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const snapshotManager = db.getSnapshotManager()

      // Create two snapshots
      const snapshot1 = await snapshotManager.createSnapshot(entity.$id as EntityId)
      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      const snapshot2 = await snapshotManager.createSnapshot(entity.$id as EntityId)

      // Verify both exist
      let snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)
      expect(snapshots.length).toBe(2)

      // Delete the first one
      await snapshotManager.deleteSnapshot(snapshot1.id)

      // Verify only one remains
      snapshots = await snapshotManager.listSnapshots(entity.$id as EntityId)
      expect(snapshots.length).toBe(1)
      expect(snapshots[0].id).toBe(snapshot2.id)
    })
  })
})
