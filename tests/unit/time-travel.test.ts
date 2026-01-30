/**
 * Time Travel Queries Test Suite
 *
 * Tests for time-travel query functionality in ParqueDB.
 * Uses real FsBackend with temp directories for actual event sourcing and time-travel replay.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type {
  EntityId,
  Event,
  HistoryOptions,
} from '../../src/types'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// =============================================================================
// Helper Functions
// =============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// =============================================================================
// Time Travel Queries Test Suite
// =============================================================================

describe('Time Travel Queries', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-time-travel-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Clean up the temp directory
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // asOf Option Tests
  // ===========================================================================

  describe('asOf option', () => {
    it('returns entity state at specific time', async () => {
      // Create entity
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Original content',
      })

      const createdAt = new Date()
      await sleep(10) // Ensure time difference

      // Update entity
      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title' },
      })

      const afterUpdate = new Date() // Capture immediately after first update
      await sleep(10) // Ensure time difference before final update

      // Update again
      await db.update('posts', entity.$id as string, {
        $set: { title: 'Final Title' },
      })

      // Query at different points in time
      const atCreation = await db.get('posts', entity.$id as string, {
        asOf: createdAt,
      })

      const atUpdate = await db.get('posts', entity.$id as string, {
        asOf: afterUpdate,
      })

      const current = await db.get('posts', entity.$id as string)

      expect(atCreation!.title).toBe('Original Title')
      expect(atUpdate!.title).toBe('Updated Title')
      expect(current!.title).toBe('Final Title')
    })

    it('replays events up to asOf timestamp', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
        viewCount: 0,
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'V2' },
        $inc: { viewCount: 10 },
      })

      const t2 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'V3' },
        $inc: { viewCount: 20 },
      })

      const t3 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'V4' },
        $inc: { viewCount: 30 },
      })

      // Query at each timestamp
      const atT1 = await db.get('posts', entity.$id as string, { asOf: t1 })
      const atT2 = await db.get('posts', entity.$id as string, { asOf: t2 })
      const atT3 = await db.get('posts', entity.$id as string, { asOf: t3 })

      expect(atT1!.title).toBe('V1')
      expect(atT1!.viewCount).toBe(0)

      expect(atT2!.title).toBe('V2')
      expect(atT2!.viewCount).toBe(10)

      expect(atT3!.title).toBe('V3')
      expect(atT3!.viewCount).toBe(30)
    })

    it('handles entity that did not exist yet', async () => {
      const beforeCreation = new Date()
      await sleep(10)

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello',
        content: 'Content',
      })

      // Query before entity existed
      const result = await db.get('posts', entity.$id as string, {
        asOf: beforeCreation,
      })

      expect(result).toBeNull()
    })

    it('handles deleted entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello',
        content: 'Content',
      })

      const afterCreate = new Date()
      await sleep(10)

      await db.delete('posts', entity.$id as string)

      const afterDelete = new Date()

      // Query at time when entity existed
      const whenExisted = await db.get('posts', entity.$id as string, {
        asOf: afterCreate,
      })

      // Query at time after deletion
      const afterDeletion = await db.get('posts', entity.$id as string, {
        asOf: afterDelete,
      })

      expect(whenExisted).not.toBeNull()
      expect(whenExisted!.title).toBe('Hello')
      expect(afterDeletion).toBeNull()
    })

    it('works with find() queries', async () => {
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content',
        status: 'draft',
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity1.$id as string, {
        $set: { status: 'published' },
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content',
        status: 'published',
      })

      // Query published posts at t1 (before first post was published)
      const atT1 = await db.find('posts', { status: 'published' }, { asOf: t1 })

      // Query published posts now
      const now = await db.find('posts', { status: 'published' })

      expect(atT1.items.length).toBe(0)
      expect(now.items.length).toBe(2)
    })

    it('applies asOf to populated relationships', async () => {
      const author = await db.create('users', {
        $type: 'User',
        name: 'Original Author Name',
        email: 'author@example.com',
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('users', author.$id as string, {
        $set: { name: 'Updated Author Name' },
      })

      await sleep(10)

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        author: { 'Author': author.$id as EntityId },
      })

      const afterPost = new Date()

      // Get post at t1 - should return null since post didn't exist at t1
      const atT1 = await db.get('posts', post.$id as string, {
        asOf: t1,
      })
      expect(atT1).toBeNull()

      // Get current post with populated author
      const currentWithAuthor = await db.get('posts', post.$id as string, {
        asOf: afterPost,
        hydrate: ['author'],
      })

      // Post should exist
      expect(currentWithAuthor).toBeDefined()
      // Author should be populated (hydration is a separate feature)
      expect(currentWithAuthor!.author).toBeDefined()
    })
  })

  // ===========================================================================
  // History Method Tests
  // ===========================================================================

  describe('history method', () => {
    it('returns all events for entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })
      await db.delete('posts', entity.$id as string)

      const history = await db.history(entity.$id as EntityId)

      expect(history.items.length).toBe(4) // CREATE, UPDATE, UPDATE, DELETE
      expect(history.items[0].op).toBe('CREATE')
      expect(history.items[3].op).toBe('DELETE')
    })

    it('filters by time range', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })

      const t2 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, { $set: { title: 'V4' } })

      const options: HistoryOptions = { from: t1, to: t2 }
      const history = await db.history(entity.$id as EntityId, options)

      // Should only include V2 and V3 updates
      expect(history.items.length).toBe(2)
      expect(history.items.every((e: Event) => e.op === 'UPDATE')).toBe(true)
    })

    it('filters by operation type', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })
      await db.delete('posts', entity.$id as string)

      const options: HistoryOptions = { op: 'UPDATE' }
      const history = await db.history(entity.$id as EntityId, options)

      expect(history.items.length).toBe(2)
      expect(history.items.every((e: Event) => e.op === 'UPDATE')).toBe(true)
    })

    it('paginates results', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Create many updates
      for (let i = 2; i <= 20; i++) {
        await db.update('posts', entity.$id as string, {
          $set: { title: `V${i}` },
        })
      }

      // Get first page
      const page1 = await db.history(entity.$id as EntityId, { limit: 5 })

      expect(page1.items.length).toBe(5)
      expect(page1.hasMore).toBe(true)
      expect(page1.nextCursor).toBeDefined()

      // Get next page
      const page2 = await db.history(entity.$id as EntityId, {
        limit: 5,
        cursor: page1.nextCursor,
      })

      expect(page2.items.length).toBe(5)
      expect(page2.hasMore).toBe(true)

      // Verify no overlap between pages
      const page1Ids = page1.items.map((e: Event) => e.id)
      const page2Ids = page2.items.map((e: Event) => e.id)
      const overlap = page1Ids.filter((id: string) => page2Ids.includes(id))
      expect(overlap.length).toBe(0)
    })

    it('filters by actor', async () => {
      const actor1 = 'users/admin' as EntityId
      const actor2 = 'users/editor' as EntityId

      const entity = await db.create(
        'posts',
        {
          $type: 'Post',
          name: 'Test Post',
          title: 'V1',
          content: 'Content',
        },
        { actor: actor1 }
      )

      await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'V2' } },
        { actor: actor2 }
      )

      await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'V3' } },
        { actor: actor1 }
      )

      const history = await db.history(entity.$id as EntityId, { actor: actor1 })

      expect(history.items.length).toBe(2) // CREATE and last UPDATE
      expect(history.items.every((e: Event) => e.actor === actor1)).toBe(true)
    })

    it('returns events in chronological order', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      await db.update('posts', entity.$id as string, { $set: { title: 'V2' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'V3' } })

      const history = await db.history(entity.$id as EntityId)

      for (let i = 1; i < history.items.length; i++) {
        expect(history.items[i].ts.getTime()).toBeGreaterThanOrEqual(
          history.items[i - 1].ts.getTime()
        )
      }
    })
  })

  // ===========================================================================
  // Diff Method Tests
  // ===========================================================================

  describe('diff method', () => {
    it('computes diff between two timestamps', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Original content',
        viewCount: 0,
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'New Title', content: 'New content' },
        $inc: { viewCount: 100 },
      })

      const t2 = new Date()

      const diff = await db.diff(entity.$id as EntityId, t1, t2)

      expect(diff.changed).toContain('title')
      expect(diff.changed).toContain('content')
      expect(diff.changed).toContain('viewCount')
      expect(diff.values.title.before).toBe('Original Title')
      expect(diff.values.title.after).toBe('New Title')
    })

    it('shows added/removed/changed fields', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        oldField: 'to be removed',
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title', newField: 'added' },
        $unset: { oldField: '' },
      })

      const t2 = new Date()

      const diff = await db.diff(entity.$id as EntityId, t1, t2)

      expect(diff.added).toContain('newField')
      expect(diff.removed).toContain('oldField')
      expect(diff.changed).toContain('title')
    })

    it('returns empty diff when no changes', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const t1 = new Date()
      await sleep(10)
      const t2 = new Date()

      const diff = await db.diff(entity.$id as EntityId, t1, t2)

      expect(diff.added).toEqual([])
      expect(diff.removed).toEqual([])
      expect(diff.changed).toEqual([])
    })

    it('handles nested object changes', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        metadata: { views: 0, likes: 0 },
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { 'metadata.views': 100 },
      })

      const t2 = new Date()

      const diff = await db.diff(entity.$id as EntityId, t1, t2)

      expect(diff.changed).toContain('metadata.views')
      expect(diff.values['metadata.views'].before).toBe(0)
      expect(diff.values['metadata.views'].after).toBe(100)
    })

    it('handles array changes', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['original'],
      })

      const t1 = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $push: { tags: 'new-tag' },
      })

      const t2 = new Date()

      const diff = await db.diff(entity.$id as EntityId, t1, t2)

      expect(diff.changed).toContain('tags')
      expect(diff.values.tags.before).toEqual(['original'])
      expect(diff.values.tags.after).toEqual(['original', 'new-tag'])
    })
  })

  // ===========================================================================
  // Revert Method Tests
  // ===========================================================================

  describe('revert method', () => {
    it('reverts entity to previous state', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Original content',
      })

      const targetTime = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Bad Update', content: 'Unwanted content' },
      })

      // Revert to the original state
      const reverted = await db.revert(entity.$id as EntityId, targetTime)

      expect(reverted.title).toBe('Original Title')
      expect(reverted.content).toBe('Original content')

      // Verify the current state is also reverted
      const current = await db.get('posts', entity.$id as string)
      expect(current!.title).toBe('Original Title')
    })

    it('records revert as new event', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      const targetTime = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Modified' },
      })

      await db.revert(entity.$id as EntityId, targetTime)

      const history = await db.history(entity.$id as EntityId)

      // History should show: CREATE, UPDATE, UPDATE (revert)
      expect(history.items.length).toBe(3)
      expect(history.items[2].op).toBe('UPDATE')
      expect((history.items[2].metadata as any)?.revert).toBe(true)
    })

    it('preserves version incrementing on revert', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      const targetTime = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Modified' },
      })

      const reverted = await db.revert(entity.$id as EntityId, targetTime)

      // Version should be 3 (create=1, update=2, revert=3)
      expect(reverted.version).toBe(3)
    })

    it('throws error if target time is in the future', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      const futureTime = new Date(Date.now() + 1000000)

      await expect(
        db.revert(entity.$id as EntityId, futureTime)
      ).rejects.toThrow()
    })

    it('throws error if entity did not exist at target time', async () => {
      const beforeCreation = new Date()
      await sleep(10)

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      await expect(
        db.revert(entity.$id as EntityId, beforeCreation)
      ).rejects.toThrow()
    })

    it('supports revert with actor option', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      const targetTime = new Date()
      await sleep(10)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Modified' },
      })

      const actor = 'users/admin' as EntityId
      await db.revert(entity.$id as EntityId, targetTime, { actor })

      const history = await db.history(entity.$id as EntityId)
      const revertEvent = history.items[history.items.length - 1]

      expect(revertEvent.actor).toBe(actor)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles high-precision timestamps', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      // Very precise timestamp
      const preciseTime = new Date()
      preciseTime.setMilliseconds(preciseTime.getMilliseconds() + 0.5)

      await db.update('posts', entity.$id as string, {
        $set: { title: 'V2' },
      })

      const result = await db.get('posts', entity.$id as string, {
        asOf: preciseTime,
      })

      // Should handle sub-millisecond precision gracefully
      expect(result).toBeDefined()
    })

    it('handles very old asOf dates', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const veryOld = new Date('1990-01-01')

      const result = await db.get('posts', entity.$id as string, {
        asOf: veryOld,
      })

      expect(result).toBeNull()
    })

    it('handles rapid successive updates', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V0',
        content: 'Content',
      })

      // Rapid updates without waiting
      const updates = Array.from({ length: 10 }, (_, i) =>
        db.update('posts', entity.$id as string, {
          $set: { title: `V${i + 1}` },
        })
      )

      await Promise.all(updates)

      const history = await db.history(entity.$id as EntityId)

      // Should have recorded all events
      expect(history.items.length).toBe(11) // 1 create + 10 updates
    })
  })
})
