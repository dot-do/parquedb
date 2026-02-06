import { describe, it, expect, beforeEach } from 'vitest'
import { RelationshipBuffer } from '@/engine/rel-buffer'
import type { RelLine } from '@/engine/types'

/**
 * RelationshipBuffer Test Suite
 *
 * Tests the in-memory buffer for relationships in the MergeTree engine.
 * The buffer maintains:
 * - A store keyed by composite 'f:p:t' holding RelLine entries (including tombstones)
 * - A forward index: fromId -> predicate -> Set<toId>
 * - A reverse index: toId -> reverseName -> Set<fromId>
 *
 * Key invariants:
 * - Unlinks are stored as tombstones ($op: 'u') for merge-on-read with Parquet
 * - Forward/reverse indexes only contain LIVE relationships (not tombstones)
 * - Duplicate links are idempotent (update timestamp, no duplicate entries)
 */

describe('RelationshipBuffer', () => {
  let buffer: RelationshipBuffer

  beforeEach(() => {
    buffer = new RelationshipBuffer()
  })

  // ===========================================================================
  // Link operations
  // ===========================================================================
  describe('link()', () => {
    it('stores a relationship', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(true)
    })

    it('populates forward index after link', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.getForward('user1', 'author')).toEqual(['post1'])
    })

    it('populates reverse index after link', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.getReverse('post1', 'posts')).toEqual(['user1'])
    })

    it('multiple links from same entity accumulate in forward index', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })

      const forward = buffer.getForward('user1', 'author')
      expect(forward).toHaveLength(2)
      expect(forward).toContain('post1')
      expect(forward).toContain('post2')
    })

    it('multiple links to same entity accumulate in reverse index', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post1', $ts: 1001 })

      const reverse = buffer.getReverse('post1', 'posts')
      expect(reverse).toHaveLength(2)
      expect(reverse).toContain('user1')
      expect(reverse).toContain('user2')
    })

    it('duplicate link is idempotent — no duplicate entries, updates timestamp', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 2000 })

      // Should still be one relationship
      expect(buffer.getForward('user1', 'author')).toEqual(['post1'])
      expect(buffer.getReverse('post1', 'posts')).toEqual(['user1'])
      expect(buffer.size).toBe(1)

      // Timestamp should be updated
      const all = buffer.getAll()
      expect(all).toHaveLength(1)
      expect(all[0].$ts).toBe(2000)
    })
  })

  // ===========================================================================
  // Unlink operations
  // ===========================================================================
  describe('unlink()', () => {
    it('removes relationship from forward index', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.getForward('user1', 'author')).toEqual([])
    })

    it('removes relationship from reverse index', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.getReverse('post1', 'posts')).toEqual([])
    })

    it('hasLink returns false after unlink', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(false)
    })

    it('unlink for non-existent relationship is a no-op', () => {
      // Should not throw
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.getForward('user1', 'author')).toEqual([])
    })

    it('unlink stores as tombstone internally for merge-on-read', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(true)

      // Tombstone should be accessible via entries()
      const entries = [...buffer.entries()]
      expect(entries).toHaveLength(1)
      const [key, line] = entries[0]
      expect(line.$op).toBe('u')
      expect(line.$ts).toBe(2000)
    })
  })

  // ===========================================================================
  // Query operations
  // ===========================================================================
  describe('getForward()', () => {
    it('returns target IDs for a given fromId and predicate', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })

      const targets = buffer.getForward('user1', 'author')
      expect(targets).toHaveLength(2)
      expect(targets).toContain('post1')
      expect(targets).toContain('post2')
    })

    it('returns empty array for unknown entity', () => {
      expect(buffer.getForward('nonexistent', 'author')).toEqual([])
    })

    it('returns empty array for unknown predicate on known entity', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.getForward('user1', 'editor')).toEqual([])
    })
  })

  describe('getReverse()', () => {
    it('returns source IDs for a given toId and reverseName', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post1', $ts: 1001 })

      const sources = buffer.getReverse('post1', 'posts')
      expect(sources).toHaveLength(2)
      expect(sources).toContain('user1')
      expect(sources).toContain('user2')
    })

    it('returns empty array for unknown entity', () => {
      expect(buffer.getReverse('nonexistent', 'posts')).toEqual([])
    })

    it('returns empty array for unknown reverse name on known entity', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.getReverse('post1', 'comments')).toEqual([])
    })
  })

  describe('hasLink()', () => {
    it('returns true for existing live link', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(true)
    })

    it('returns false for non-existent link', () => {
      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(false)
    })

    it('returns false for tombstoned link', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(false)
    })
  })

  describe('isUnlinked()', () => {
    it('returns true only for tombstoned relationships', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(true)
    })

    it('returns false for live relationships', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })

      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(false)
    })

    it('returns false for non-existent relationships', () => {
      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(false)
    })
  })

  // ===========================================================================
  // Bulk operations
  // ===========================================================================
  describe('getAll()', () => {
    it('returns all live relationships as RelLine[]', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })

      const all = buffer.getAll()
      expect(all).toHaveLength(2)
      expect(all.every(r => r.$op === 'l')).toBe(true)
    })

    it('excludes tombstoned relationships', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      const all = buffer.getAll()
      expect(all).toHaveLength(1)
      expect(all[0].f).toBe('user2')
      expect(all[0].t).toBe('post2')
    })

    it('returns empty array when buffer is empty', () => {
      expect(buffer.getAll()).toEqual([])
    })
  })

  describe('clear()', () => {
    it('removes all entries', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })

      buffer.clear()

      expect(buffer.size).toBe(0)
      expect(buffer.getForward('user1', 'author')).toEqual([])
      expect(buffer.getReverse('post1', 'posts')).toEqual([])
      expect(buffer.getAll()).toEqual([])
    })
  })

  describe('size', () => {
    it('returns count of live relationships', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })

      expect(buffer.size).toBe(2)
    })

    it('does not count tombstoned relationships', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.size).toBe(1)
    })

    it('returns 0 for empty buffer', () => {
      expect(buffer.size).toBe(0)
    })
  })

  describe('entries()', () => {
    it('iterates all entries including tombstones', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user2', p: 'author', r: 'posts', t: 'post2', $ts: 1001 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      const entries = [...buffer.entries()]
      expect(entries).toHaveLength(2)

      const ops = entries.map(([_, line]) => line.$op)
      expect(ops).toContain('l') // live
      expect(ops).toContain('u') // tombstone
    })

    it('returns empty iterator for empty buffer', () => {
      const entries = [...buffer.entries()]
      expect(entries).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Composite key behavior
  // ===========================================================================
  describe('composite key (f:p:t)', () => {
    it('different predicates between same entities are independent', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user1', p: 'editor', r: 'editedPosts', t: 'post1', $ts: 1001 })

      expect(buffer.size).toBe(2)
      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(true)
      expect(buffer.hasLink('user1', 'editor', 'post1')).toBe(true)

      // Forward indexes are separate
      expect(buffer.getForward('user1', 'author')).toEqual(['post1'])
      expect(buffer.getForward('user1', 'editor')).toEqual(['post1'])

      // Reverse indexes are separate
      expect(buffer.getReverse('post1', 'posts')).toEqual(['user1'])
      expect(buffer.getReverse('post1', 'editedPosts')).toEqual(['user1'])
    })

    it('unlinking one predicate does not affect another', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.link({ f: 'user1', p: 'editor', r: 'editedPosts', t: 'post1', $ts: 1001 })

      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(false)
      expect(buffer.hasLink('user1', 'editor', 'post1')).toBe(true)
      expect(buffer.size).toBe(1)
    })
  })

  // ===========================================================================
  // Re-link after unlink
  // ===========================================================================
  describe('re-link after unlink', () => {
    it('re-linking after unlink restores the relationship', () => {
      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 1000 })
      buffer.unlink({ f: 'user1', p: 'author', t: 'post1', $ts: 2000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(false)
      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(true)

      buffer.link({ f: 'user1', p: 'author', r: 'posts', t: 'post1', $ts: 3000 })

      expect(buffer.hasLink('user1', 'author', 'post1')).toBe(true)
      expect(buffer.isUnlinked('user1', 'author', 'post1')).toBe(false)
      expect(buffer.getForward('user1', 'author')).toEqual(['post1'])
      expect(buffer.getReverse('post1', 'posts')).toEqual(['user1'])
      expect(buffer.size).toBe(1)
    })
  })
})
