import { describe, it, expect } from 'vitest'
import { mergeRelationships, relKey, relComparator } from '@/engine/merge-rels'
import { mergeEvents } from '@/engine/merge-events'
import type { RelLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

/** Helper to create a RelLine with sensible defaults */
function makeRel(overrides: Partial<RelLine> & { f: string; p: string; t: string }): RelLine {
  return {
    $op: 'l',
    $ts: Date.now(),
    r: 'reverse',
    ...overrides,
  }
}

// =============================================================================
// relKey
// =============================================================================

describe('relKey', () => {
  it('returns f:p:t composite key', () => {
    const rel = makeRel({ f: 'user-1', p: 'posts', t: 'post-1' })
    expect(relKey(rel)).toBe('user-1:posts:post-1')
  })

  it('handles empty strings', () => {
    const rel = makeRel({ f: '', p: '', t: '' })
    expect(relKey(rel)).toBe('::')
  })

  it('handles strings containing colons', () => {
    const rel = makeRel({ f: 'a:b', p: 'c:d', t: 'e:f' })
    expect(relKey(rel)).toBe('a:b:c:d:e:f')
  })
})

// =============================================================================
// relComparator
// =============================================================================

describe('relComparator', () => {
  it('sorts by f first', () => {
    const a = makeRel({ f: 'alpha', p: 'z', t: 'z' })
    const b = makeRel({ f: 'beta', p: 'a', t: 'a' })
    expect(relComparator(a, b)).toBeLessThan(0)
    expect(relComparator(b, a)).toBeGreaterThan(0)
  })

  it('sorts by p second when f is equal', () => {
    const a = makeRel({ f: 'same', p: 'alpha', t: 'z' })
    const b = makeRel({ f: 'same', p: 'beta', t: 'a' })
    expect(relComparator(a, b)).toBeLessThan(0)
    expect(relComparator(b, a)).toBeGreaterThan(0)
  })

  it('sorts by t third when f and p are equal', () => {
    const a = makeRel({ f: 'same', p: 'same', t: 'alpha' })
    const b = makeRel({ f: 'same', p: 'same', t: 'beta' })
    expect(relComparator(a, b)).toBeLessThan(0)
    expect(relComparator(b, a)).toBeGreaterThan(0)
  })

  it('returns 0 for identical f, p, t', () => {
    const a = makeRel({ f: 'x', p: 'y', t: 'z' })
    const b = makeRel({ f: 'x', p: 'y', t: 'z' })
    expect(relComparator(a, b)).toBe(0)
  })
})

// =============================================================================
// mergeRelationships
// =============================================================================

describe('mergeRelationships', () => {
  // ---------------------------------------------------------------------------
  // Basic merge
  // ---------------------------------------------------------------------------

  describe('basic merge', () => {
    it('returns empty array when both inputs are empty', () => {
      const result = mergeRelationships([], [])
      expect(result).toEqual([])
    })

    it('returns base as-is when overlay is empty', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p2', $ts: 200 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result).toHaveLength(2)
    })

    it('returns overlay as-is when base is empty', () => {
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u2', p: 'posts', t: 'p3', $ts: 200 }),
      ]
      const result = mergeRelationships([], overlay)
      expect(result).toHaveLength(2)
    })

    it('merges disjoint base and overlay', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
      ]
      const overlay = [
        makeRel({ f: 'u2', p: 'posts', t: 'p2', $ts: 200 }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(2)
    })
  })

  // ---------------------------------------------------------------------------
  // Overlay overwrite
  // ---------------------------------------------------------------------------

  describe('overlay overwrites base', () => {
    it('overlay with higher $ts wins for same f:p:t', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100, r: 'author' }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 200, r: 'writer' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('writer')
      expect(result[0].$ts).toBe(200)
    })

    it('base with higher $ts wins over overlay', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 300, r: 'author' }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100, r: 'writer' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('author')
      expect(result[0].$ts).toBe(300)
    })
  })

  // ---------------------------------------------------------------------------
  // $ts conflict resolution
  // ---------------------------------------------------------------------------

  describe('$ts conflict resolution', () => {
    it('higher $ts wins regardless of array position', () => {
      const base = [
        makeRel({ f: 'u1', p: 'likes', t: 'p1', $ts: 500, r: 'base-reverse' }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'likes', t: 'p1', $ts: 300, r: 'overlay-reverse' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('base-reverse')
    })

    it('on equal $ts, overlay wins (tie-break)', () => {
      const base = [
        makeRel({ f: 'u1', p: 'likes', t: 'p1', $ts: 100, r: 'base-reverse' }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'likes', t: 'p1', $ts: 100, r: 'overlay-reverse' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('overlay-reverse')
    })
  })

  // ---------------------------------------------------------------------------
  // Tombstone filtering ($op='u' means unlink)
  // ---------------------------------------------------------------------------

  describe('tombstone filtering', () => {
    it('filters out unlinks ($op=u) from base', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'u', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p2', $op: 'l', $ts: 100 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result).toHaveLength(1)
      expect(result[0].t).toBe('p2')
    })

    it('overlay unlink removes base link', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'l', $ts: 100 }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'u', $ts: 200 }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(0)
    })

    it('base unlink with higher $ts overrides overlay link', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'u', $ts: 300 }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'l', $ts: 100 }),
      ]
      const result = mergeRelationships(base, overlay)
      // base has higher $ts and $op='u', so the unlink wins => filtered out
      expect(result).toHaveLength(0)
    })

    it('overlay re-link after base unlink (overlay has higher $ts)', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'u', $ts: 100 }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'l', $ts: 200 }),
      ]
      const result = mergeRelationships(base, overlay)
      // overlay has higher $ts and $op='l', so re-link wins
      expect(result).toHaveLength(1)
      expect(result[0].$op).toBe('l')
    })

    it('all tombstones results in empty array', () => {
      const base = [
        makeRel({ f: 'u1', p: 'a', t: 'p1', $op: 'u', $ts: 100 }),
        makeRel({ f: 'u1', p: 'b', t: 'p2', $op: 'u', $ts: 100 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result).toEqual([])
    })
  })

  // ---------------------------------------------------------------------------
  // Sort order verification
  // ---------------------------------------------------------------------------

  describe('sort order', () => {
    it('results sorted by (f, p, t)', () => {
      const base = [
        makeRel({ f: 'u3', p: 'posts', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u2', p: 'posts', t: 'p1', $ts: 100 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result.map(r => r.f)).toEqual(['u1', 'u2', 'u3'])
    })

    it('sorts by p when f is tied', () => {
      const base = [
        makeRel({ f: 'u1', p: 'likes', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u1', p: 'follows', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result.map(r => r.p)).toEqual(['follows', 'likes', 'posts'])
    })

    it('sorts by t when f and p are tied', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p3', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p2', $ts: 100 }),
      ]
      const result = mergeRelationships(base, [])
      expect(result.map(r => r.t)).toEqual(['p1', 'p2', 'p3'])
    })
  })

  // ---------------------------------------------------------------------------
  // Mixed scenarios
  // ---------------------------------------------------------------------------

  describe('mixed scenarios', () => {
    it('complex merge: links, unlinks, overwrites, and new entries', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'l', $ts: 100 }),
        makeRel({ f: 'u1', p: 'posts', t: 'p2', $op: 'l', $ts: 100 }),
        makeRel({ f: 'u1', p: 'likes', t: 'p3', $op: 'l', $ts: 100 }),
        makeRel({ f: 'u2', p: 'posts', t: 'p4', $op: 'l', $ts: 100 }),
      ]
      const overlay = [
        // Unlink u1->posts->p1
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $op: 'u', $ts: 200 }),
        // New link
        makeRel({ f: 'u1', p: 'posts', t: 'p5', $op: 'l', $ts: 200 }),
        // Overwrite u2->posts->p4 with updated reverse name
        makeRel({ f: 'u2', p: 'posts', t: 'p4', $op: 'l', $ts: 200, r: 'updated-r' }),
      ]
      const result = mergeRelationships(base, overlay)

      // u1:posts:p1 unlinked -> removed
      // u1:posts:p2 unchanged
      // u1:likes:p3 unchanged
      // u2:posts:p4 overwritten (higher $ts)
      // u1:posts:p5 new
      expect(result).toHaveLength(4)

      const keys = result.map(r => `${r.f}:${r.p}:${r.t}`)
      expect(keys).not.toContain('u1:posts:p1')
      expect(keys).toContain('u1:likes:p3')
      expect(keys).toContain('u1:posts:p2')
      expect(keys).toContain('u1:posts:p5')
      expect(keys).toContain('u2:posts:p4')

      // Verify overwritten entry has updated reverse name
      const u2p4 = result.find(r => r.f === 'u2' && r.t === 'p4')!
      expect(u2p4.r).toBe('updated-r')
    })

    it('duplicate keys with same $ts: overlay wins', () => {
      const base = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100, r: 'base-r' }),
      ]
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100, r: 'overlay-r' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('overlay-r')
    })

    it('multiple overlay entries for same key: last one wins (highest $ts)', () => {
      const base: RelLine[] = []
      const overlay = [
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 100, r: 'first' }),
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 300, r: 'third' }),
        makeRel({ f: 'u1', p: 'posts', t: 'p1', $ts: 200, r: 'second' }),
      ]
      const result = mergeRelationships(base, overlay)
      expect(result).toHaveLength(1)
      expect(result[0].r).toBe('third')
      expect(result[0].$ts).toBe(300)
    })
  })
})

// =============================================================================
// mergeEvents
// =============================================================================

describe('mergeEvents', () => {
  it('returns empty array when both inputs are empty', () => {
    const result = mergeEvents([], [])
    expect(result).toEqual([])
  })

  it('returns base as-is when overlay is empty', () => {
    const base = [
      { id: 'e1', ts: 100, op: 'c' },
      { id: 'e2', ts: 200, op: 'u' },
    ]
    const result = mergeEvents(base, [])
    expect(result).toHaveLength(2)
    expect(result[0].id).toBe('e1')
    expect(result[1].id).toBe('e2')
  })

  it('returns overlay as-is when base is empty', () => {
    const overlay = [
      { id: 'e3', ts: 300, op: 'c' },
    ]
    const result = mergeEvents([], overlay)
    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('e3')
  })

  it('concatenates and sorts by ts', () => {
    const base = [
      { id: 'e1', ts: 100, op: 'c' },
      { id: 'e3', ts: 300, op: 'u' },
    ]
    const overlay = [
      { id: 'e2', ts: 200, op: 'c' },
      { id: 'e4', ts: 50, op: 'd' },
    ]
    const result = mergeEvents(base, overlay)
    expect(result).toHaveLength(4)
    expect(result.map(r => r.id)).toEqual(['e4', 'e1', 'e2', 'e3'])
  })

  it('sorts events by ts field', () => {
    const base: Array<{ id: string; ts: number; op: string; ns: string; eid: string }> = [
      { id: 'e1', ts: 200, op: 'c', ns: 'users', eid: 'u1' },
    ]
    const overlay: Array<{ id: string; ts: number; op: string; ns: string; eid: string }> = [
      { id: 'e2', ts: 100, op: 'u', ns: 'users', eid: 'u2' },
    ]
    const result = mergeEvents(base, overlay)
    expect(result).toHaveLength(2)
    expect(result[0].id).toBe('e2')
    expect(result[1].id).toBe('e1')
  })

  it('handles events with missing ts (defaults to 0)', () => {
    const base = [
      { id: 'e1', op: 'c' },
    ]
    const overlay = [
      { id: 'e2', ts: 100, op: 'u' },
    ]
    const result = mergeEvents(base, overlay)
    expect(result).toHaveLength(2)
    expect(result[0].id).toBe('e1') // ts=0 sorts first
    expect(result[1].id).toBe('e2')
  })

  it('preserves all event fields', () => {
    const base = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', actor: 'admin', after: { name: 'Alice' } },
    ]
    const overlay = [
      { id: 'e2', ts: 200, op: 'u', ns: 'users', eid: 'u1', before: { name: 'Alice' }, after: { name: 'Bob' } },
    ]
    const result = mergeEvents(base, overlay)
    expect(result).toHaveLength(2)
    expect(result[0]).toEqual(base[0])
    expect(result[1]).toEqual(overlay[0])
  })

  it('does not deduplicate events (append-only)', () => {
    const base = [
      { id: 'e1', ts: 100, op: 'c' },
    ]
    const overlay = [
      { id: 'e1', ts: 100, op: 'c' }, // Same event ID
    ]
    const result = mergeEvents(base, overlay)
    // Events are append-only, no dedup
    expect(result).toHaveLength(2)
  })

  it('stable sort preserves order for equal timestamps', () => {
    const base = [
      { id: 'e1', ts: 100, op: 'c' },
      { id: 'e2', ts: 100, op: 'u' },
    ]
    const overlay = [
      { id: 'e3', ts: 100, op: 'd' },
    ]
    const result = mergeEvents(base, overlay)
    expect(result).toHaveLength(3)
    // All have same ts, order should be base first then overlay (stable sort)
    expect(result[0].id).toBe('e1')
    expect(result[1].id).toBe('e2')
    expect(result[2].id).toBe('e3')
  })
})
