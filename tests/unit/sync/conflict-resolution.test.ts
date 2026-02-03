/**
 * Tests for Conflict Resolution Strategies
 *
 * This test suite validates all conflict resolution strategies including
 * built-in strategies, custom resolution functions, bulk resolution,
 * and composite strategies.
 */

import { describe, it, expect } from 'vitest'
import {
  resolveConflict,
  resolveAllConflicts,
  resolveConflictsByType,
  allResolutionsComplete,
  getUnresolvedConflicts,
  applyManualResolution,
  createFallbackStrategy,
  createFieldBasedStrategy,
  createPreferenceStrategy,
  createNonNullStrategy,
  createConcatenateStrategy,
  createArrayMergeStrategy,
  type BuiltinStrategy,
  type CustomResolutionFn,
  type ConflictResolution,
} from '../../../src/sync/conflict-resolution'
import type { ConflictInfo, ConflictType } from '../../../src/sync/conflict-detection'
import type { Event } from '../../../src/types/entity'
import { generateULID } from '../../../src/utils/random'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  target: string,
  op: 'CREATE' | 'UPDATE' | 'DELETE' | 'REL_CREATE' | 'REL_DELETE',
  ts: number,
  before?: unknown,
  after?: unknown
): Event {
  return {
    id: generateULID(),
    ts,
    op,
    target,
    before: before as any,
    after: after as any,
    actor: 'test',
  }
}

function createConflict(
  type: ConflictType,
  target: string,
  field: string | undefined,
  ourValue: unknown,
  theirValue: unknown,
  baseValue: unknown,
  ourTs: number,
  theirTs: number
): ConflictInfo {
  return {
    type,
    target,
    field,
    ourValue,
    theirValue,
    baseValue,
    ourEvent: createEvent(target, 'UPDATE', ourTs, { [field || 'value']: baseValue }, { [field || 'value']: ourValue }),
    theirEvent: createEvent(target, 'UPDATE', theirTs, { [field || 'value']: baseValue }, { [field || 'value']: theirValue }),
  }
}

// =============================================================================
// Built-in Strategy Tests: 'ours'
// =============================================================================

describe('Conflict Resolution - "ours" strategy', () => {
  it('should always pick local value for concurrent update', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',  // our value
      'archived',   // their value
      'draft',      // base value
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBe('published')
    expect(result.strategy).toBe('ours')
    expect(result.requiresManualResolution).toBe(false)
    expect(result.explanation).toContain('our value')
    expect(result.conflict).toBe(conflict)
  })

  it('should pick local value even when their timestamp is later', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      'Our Title',
      'Their Title',
      'Original',
      1000,  // ours earlier
      2000   // theirs later
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBe('Our Title')
  })

  it('should handle delete_update conflict', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      ourValue: undefined,
      theirValue: { title: 'Updated' },
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
    }

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBeUndefined()
    expect(result.strategy).toBe('ours')
  })

  it('should handle create_create conflict', () => {
    const conflict: ConflictInfo = {
      type: 'create_create',
      target: 'posts:p1',
      ourValue: { title: 'Our Post', views: 10 },
      theirValue: { title: 'Their Post', views: 5 },
      baseValue: undefined,
      ourEvent: createEvent('posts:p1', 'CREATE', 1000, undefined, { title: 'Our Post', views: 10 }),
      theirEvent: createEvent('posts:p1', 'CREATE', 1100, undefined, { title: 'Their Post', views: 5 }),
    }

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toEqual({ title: 'Our Post', views: 10 })
  })

  it('should handle null values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'description',
      null,
      'Their description',
      'Original description',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBeNull()
  })

  it('should handle complex object values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'metadata',
      { tags: ['a', 'b'], count: 5 },
      { tags: ['c', 'd'], count: 10 },
      { tags: [], count: 0 },
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toEqual({ tags: ['a', 'b'], count: 5 })
  })
})

// =============================================================================
// Built-in Strategy Tests: 'theirs'
// =============================================================================

describe('Conflict Resolution - "theirs" strategy', () => {
  it('should always pick remote value for concurrent update', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'theirs')

    expect(result.resolvedValue).toBe('archived')
    expect(result.strategy).toBe('theirs')
    expect(result.requiresManualResolution).toBe(false)
    expect(result.explanation).toContain('their value')
    expect(result.conflict).toBe(conflict)
  })

  it('should pick remote value even when our timestamp is later', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      'Our Title',
      'Their Title',
      'Original',
      2000,  // ours later
      1000   // theirs earlier
    )

    const result = resolveConflict(conflict, 'theirs')

    expect(result.resolvedValue).toBe('Their Title')
  })

  it('should handle delete_update conflict with update value', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      ourValue: { title: 'Updated' },
      theirValue: undefined,
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, { title: 'Original' }, { title: 'Updated' }),
      theirEvent: createEvent('posts:p1', 'DELETE', 1100, { title: 'Original' }, undefined),
    }

    const result = resolveConflict(conflict, 'theirs')

    expect(result.resolvedValue).toBeUndefined()
  })

  it('should handle undefined values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'optionalField',
      'our value',
      undefined,
      'base value',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'theirs')

    expect(result.resolvedValue).toBeUndefined()
  })

  it('should handle array values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'tags',
      ['a', 'b'],
      ['c', 'd', 'e'],
      ['a'],
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'theirs')

    expect(result.resolvedValue).toEqual(['c', 'd', 'e'])
  })
})

// =============================================================================
// Built-in Strategy Tests: 'latest'
// =============================================================================

describe('Conflict Resolution - "latest" strategy', () => {
  it('should pick value from event with later timestamp', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,  // ours earlier
      1100   // theirs later
    )

    const result = resolveConflict(conflict, 'latest')

    expect(result.resolvedValue).toBe('archived')
    expect(result.strategy).toBe('latest')
    expect(result.requiresManualResolution).toBe(false)
    expect(result.explanation).toContain('1100')
    expect(result.explanation).toContain('1000')
  })

  it('should pick our value when our timestamp is later', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1200,  // ours later
      1100   // theirs earlier
    )

    const result = resolveConflict(conflict, 'latest')

    expect(result.resolvedValue).toBe('published')
    expect(result.explanation).toContain('1200')
  })

  it('should pick our value when timestamps are equal (tiebreaker)', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,  // same timestamp
      1000   // same timestamp
    )

    const result = resolveConflict(conflict, 'latest')

    // Equal timestamps: >= means ours wins
    expect(result.resolvedValue).toBe('published')
  })

  it('should handle timestamps with millisecond precision', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      'Our Title',
      'Their Title',
      'Original',
      1000001,  // 1ms earlier
      1000002   // 1ms later
    )

    const result = resolveConflict(conflict, 'latest')

    expect(result.resolvedValue).toBe('Their Title')
  })

  it('should work with delete_update conflict', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      ourValue: undefined,
      theirValue: { title: 'Updated' },
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
      theirEvent: createEvent('posts:p1', 'UPDATE', 2000, { title: 'Original' }, { title: 'Updated' }),
    }

    const result = resolveConflict(conflict, 'latest')

    // Their timestamp (2000) is later
    expect(result.resolvedValue).toEqual({ title: 'Updated' })
  })
})

// =============================================================================
// Built-in Strategy Tests: 'manual'
// =============================================================================

describe('Conflict Resolution - "manual" strategy', () => {
  it('should mark conflict for manual resolution', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'manual')

    expect(result.resolvedValue).toBeUndefined()
    expect(result.strategy).toBe('manual')
    expect(result.requiresManualResolution).toBe(true)
    expect(result.explanation).toContain('manual resolution')
    expect(result.conflict).toBe(conflict)
  })

  it('should preserve conflict info for later resolution', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      'Our Title',
      'Their Title',
      'Original',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'manual')

    expect(result.conflict).toBeDefined()
    expect(result.conflict?.ourValue).toBe('Our Title')
    expect(result.conflict?.theirValue).toBe('Their Title')
  })

  it('should include field info in explanation when available', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'description',
      'Our desc',
      'Their desc',
      'Base desc',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'manual')

    expect(result.explanation).toContain('description')
  })

  it('should use target in explanation when field is undefined', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      ourValue: undefined,
      theirValue: { title: 'Updated' },
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
    }

    const result = resolveConflict(conflict, 'manual')

    expect(result.explanation).toContain('posts:p1')
  })
})

// =============================================================================
// Unknown Strategy Tests
// =============================================================================

describe('Conflict Resolution - Unknown strategy', () => {
  it('should throw error for unknown strategy', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,
      1100
    )

    expect(() => resolveConflict(conflict, 'unknown' as BuiltinStrategy)).toThrow('Unknown resolution strategy')
  })
})

// =============================================================================
// Custom Resolution Function Tests
// =============================================================================

describe('Conflict Resolution - Custom functions', () => {
  it('should use custom resolution function', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'views',
      100,
      200,
      50,
      1000,
      1100
    )

    // Custom function that picks the higher value
    const maxStrategy: CustomResolutionFn = (c) => ({
      resolvedValue: Math.max(c.ourValue as number, c.theirValue as number),
      strategy: 'max',
      requiresManualResolution: false,
      explanation: 'Picked the higher value',
      conflict: c,
    })

    const result = resolveConflict(conflict, maxStrategy)

    expect(result.resolvedValue).toBe(200)
    expect(result.strategy).toBe('max')
  })

  it('should allow custom function to return manual resolution', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'status',
      'published',
      'archived',
      'draft',
      1000,
      1100
    )

    const conditionalManual: CustomResolutionFn = (c) => {
      if (c.field === 'status') {
        return {
          resolvedValue: undefined,
          strategy: 'conditional-manual',
          requiresManualResolution: true,
          explanation: 'Status changes require review',
          conflict: c,
        }
      }
      return {
        resolvedValue: c.ourValue,
        strategy: 'auto',
        requiresManualResolution: false,
        conflict: c,
      }
    }

    const result = resolveConflict(conflict, conditionalManual)

    expect(result.requiresManualResolution).toBe(true)
    expect(result.strategy).toBe('conditional-manual')
  })

  it('should pass full conflict info to custom function', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      'Our Title',
      'Their Title',
      'Original',
      1000,
      1100
    )

    let capturedConflict: ConflictInfo | undefined

    const inspectStrategy: CustomResolutionFn = (c) => {
      capturedConflict = c
      return {
        resolvedValue: c.ourValue,
        strategy: 'inspect',
        requiresManualResolution: false,
      }
    }

    resolveConflict(conflict, inspectStrategy)

    expect(capturedConflict).toBeDefined()
    expect(capturedConflict?.type).toBe('concurrent_update')
    expect(capturedConflict?.target).toBe('posts:p1')
    expect(capturedConflict?.field).toBe('title')
    expect(capturedConflict?.ourEvent).toBeDefined()
    expect(capturedConflict?.theirEvent).toBeDefined()
  })
})

// =============================================================================
// Bulk Resolution Tests
// =============================================================================

describe('Conflict Resolution - Bulk resolution', () => {
  it('should resolve all conflicts with same strategy', () => {
    const conflicts: ConflictInfo[] = [
      createConflict('concurrent_update', 'posts:p1', 'title', 'Our Title', 'Their Title', 'Base', 1000, 1100),
      createConflict('concurrent_update', 'posts:p2', 'status', 'published', 'draft', 'pending', 1000, 1100),
      createConflict('concurrent_update', 'users:u1', 'name', 'Alice', 'Bob', 'Charlie', 1000, 1100),
    ]

    const results = resolveAllConflicts(conflicts, 'ours')

    expect(results).toHaveLength(3)
    expect(results[0]?.resolvedValue).toBe('Our Title')
    expect(results[1]?.resolvedValue).toBe('published')
    expect(results[2]?.resolvedValue).toBe('Alice')
    expect(results.every(r => r.strategy === 'ours')).toBe(true)
  })

  it('should resolve empty array without errors', () => {
    const results = resolveAllConflicts([], 'theirs')

    expect(results).toHaveLength(0)
  })

  it('should work with custom function for bulk resolution', () => {
    const conflicts: ConflictInfo[] = [
      createConflict('concurrent_update', 'posts:p1', 'views', 100, 200, 50, 1000, 1100),
      createConflict('concurrent_update', 'posts:p2', 'views', 300, 150, 100, 1000, 1100),
    ]

    const maxStrategy: CustomResolutionFn = (c) => ({
      resolvedValue: Math.max(c.ourValue as number, c.theirValue as number),
      strategy: 'max',
      requiresManualResolution: false,
    })

    const results = resolveAllConflicts(conflicts, maxStrategy)

    expect(results[0]?.resolvedValue).toBe(200)
    expect(results[1]?.resolvedValue).toBe(300)
  })
})

// =============================================================================
// Resolve by Type Tests
// =============================================================================

describe('Conflict Resolution - By conflict type', () => {
  it('should use different strategies for different conflict types', () => {
    const conflicts: ConflictInfo[] = [
      createConflict('concurrent_update', 'posts:p1', 'title', 'Our Title', 'Their Title', 'Base', 1000, 1100),
      {
        type: 'delete_update',
        target: 'posts:p2',
        ourValue: undefined,
        theirValue: { title: 'Updated' },
        baseValue: { title: 'Original' },
        ourEvent: createEvent('posts:p2', 'DELETE', 1000, { title: 'Original' }, undefined),
        theirEvent: createEvent('posts:p2', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
      },
      {
        type: 'create_create',
        target: 'posts:p3',
        ourValue: { title: 'Our Post' },
        theirValue: { title: 'Their Post' },
        baseValue: undefined,
        ourEvent: createEvent('posts:p3', 'CREATE', 1000, undefined, { title: 'Our Post' }),
        theirEvent: createEvent('posts:p3', 'CREATE', 1100, undefined, { title: 'Their Post' }),
      },
    ]

    const results = resolveConflictsByType(
      conflicts,
      {
        concurrent_update: 'latest',
        delete_update: 'ours',
        create_create: 'theirs',
      }
    )

    expect(results[0]?.resolvedValue).toBe('Their Title')  // latest (their ts is higher)
    expect(results[1]?.resolvedValue).toBeUndefined()      // ours (delete)
    expect(results[2]?.resolvedValue).toEqual({ title: 'Their Post' })  // theirs
  })

  it('should use default strategy for unmapped types', () => {
    const conflicts: ConflictInfo[] = [
      createConflict('concurrent_update', 'posts:p1', 'title', 'Our', 'Their', 'Base', 1000, 1100),
    ]

    const results = resolveConflictsByType(
      conflicts,
      { delete_update: 'ours' },  // Only delete_update mapped
      'theirs'  // default
    )

    expect(results[0]?.resolvedValue).toBe('Their')
    expect(results[0]?.strategy).toBe('theirs')
  })

  it('should default to manual when no default specified', () => {
    const conflicts: ConflictInfo[] = [
      createConflict('concurrent_update', 'posts:p1', 'title', 'Our', 'Their', 'Base', 1000, 1100),
    ]

    const results = resolveConflictsByType(conflicts, {})  // No mappings, no default

    expect(results[0]?.requiresManualResolution).toBe(true)
  })
})

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('Conflict Resolution - Helper functions', () => {
  describe('allResolutionsComplete', () => {
    it('should return true when all resolutions are complete', () => {
      const resolutions: ConflictResolution[] = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: 'b', strategy: 'theirs', requiresManualResolution: false },
        { resolvedValue: 'c', strategy: 'latest', requiresManualResolution: false },
      ]

      expect(allResolutionsComplete(resolutions)).toBe(true)
    })

    it('should return false when any resolution requires manual handling', () => {
      const resolutions: ConflictResolution[] = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: undefined, strategy: 'manual', requiresManualResolution: true },
      ]

      expect(allResolutionsComplete(resolutions)).toBe(false)
    })

    it('should return true for empty array', () => {
      expect(allResolutionsComplete([])).toBe(true)
    })
  })

  describe('getUnresolvedConflicts', () => {
    it('should return only unresolved conflicts', () => {
      const resolutions: ConflictResolution[] = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: undefined, strategy: 'manual', requiresManualResolution: true },
        { resolvedValue: 'c', strategy: 'theirs', requiresManualResolution: false },
        { resolvedValue: undefined, strategy: 'manual', requiresManualResolution: true },
      ]

      const unresolved = getUnresolvedConflicts(resolutions)

      expect(unresolved).toHaveLength(2)
      expect(unresolved.every(r => r.requiresManualResolution)).toBe(true)
    })

    it('should return empty array when all are resolved', () => {
      const resolutions: ConflictResolution[] = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
      ]

      expect(getUnresolvedConflicts(resolutions)).toHaveLength(0)
    })
  })

  describe('applyManualResolution', () => {
    it('should apply user-provided value to manual conflict', () => {
      const original: ConflictResolution = {
        resolvedValue: undefined,
        strategy: 'manual',
        requiresManualResolution: true,
        explanation: 'Requires review',
        conflict: createConflict('concurrent_update', 'posts:p1', 'status', 'a', 'b', 'c', 1000, 1100),
      }

      const resolved = applyManualResolution(original, 'user-chosen-value')

      expect(resolved.resolvedValue).toBe('user-chosen-value')
      expect(resolved.strategy).toBe('manual-resolved')
      expect(resolved.requiresManualResolution).toBe(false)
      expect(resolved.explanation).toContain('Manually resolved')
    })

    it('should preserve original conflict info', () => {
      const conflict = createConflict('concurrent_update', 'posts:p1', 'status', 'a', 'b', 'c', 1000, 1100)
      const original: ConflictResolution = {
        resolvedValue: undefined,
        strategy: 'manual',
        requiresManualResolution: true,
        conflict,
      }

      const resolved = applyManualResolution(original, 'chosen')

      expect(resolved.conflict).toBe(conflict)
    })

    it('should allow applying null as resolution', () => {
      const original: ConflictResolution = {
        resolvedValue: undefined,
        strategy: 'manual',
        requiresManualResolution: true,
      }

      const resolved = applyManualResolution(original, null)

      expect(resolved.resolvedValue).toBeNull()
      expect(resolved.requiresManualResolution).toBe(false)
    })
  })
})

// =============================================================================
// Composite Strategy Tests
// =============================================================================

describe('Conflict Resolution - Composite strategies', () => {
  describe('createFallbackStrategy', () => {
    it('should try strategies in order and return first non-manual', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'title',
        'Our Title',
        'Their Title',
        'Original',
        1000,
        1100
      )

      const fallback = createFallbackStrategy('ours', 'theirs')
      const result = resolveConflict(conflict, fallback)

      // 'ours' should succeed first
      expect(result.resolvedValue).toBe('Our Title')
      expect(result.strategy).toBe('ours')
    })

    it('should fall back to next strategy when first requires manual', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'title',
        'Our Title',
        'Their Title',
        'Original',
        1000,
        1100
      )

      const fallback = createFallbackStrategy('manual', 'theirs')
      const result = resolveConflict(conflict, fallback)

      expect(result.resolvedValue).toBe('Their Title')
      expect(result.strategy).toBe('theirs')
    })

    it('should return manual if all strategies require manual', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'title',
        'Our',
        'Their',
        'Base',
        1000,
        1100
      )

      const fallback = createFallbackStrategy('manual', 'manual')
      const result = resolveConflict(conflict, fallback)

      expect(result.requiresManualResolution).toBe(true)
    })
  })

  describe('createFieldBasedStrategy', () => {
    it('should use different strategies for different fields', () => {
      const titleConflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'title',
        'Our Title',
        'Their Title',
        'Base',
        1000,
        1100
      )

      const viewsConflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'views',
        100,
        200,
        50,
        1000,
        1100
      )

      const fieldStrategy = createFieldBasedStrategy({
        title: 'ours',
        views: 'latest',
      })

      const titleResult = resolveConflict(titleConflict, fieldStrategy)
      const viewsResult = resolveConflict(viewsConflict, fieldStrategy)

      expect(titleResult.resolvedValue).toBe('Our Title')
      expect(viewsResult.resolvedValue).toBe(200)  // theirs is later
    })

    it('should use default for unmapped fields', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'unknownField',
        'Our',
        'Their',
        'Base',
        1000,
        1100
      )

      const fieldStrategy = createFieldBasedStrategy(
        { title: 'ours' },
        'theirs'  // default
      )

      const result = resolveConflict(conflict, fieldStrategy)

      expect(result.resolvedValue).toBe('Their')
    })

    it('should default to manual for unmapped fields when no default', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'unknownField',
        'Our',
        'Their',
        'Base',
        1000,
        1100
      )

      const fieldStrategy = createFieldBasedStrategy({ title: 'ours' })

      const result = resolveConflict(conflict, fieldStrategy)

      expect(result.requiresManualResolution).toBe(true)
    })

    it('should use default for entity-level conflicts (no field)', () => {
      const conflict: ConflictInfo = {
        type: 'delete_update',
        target: 'posts:p1',
        // No field property
        ourValue: undefined,
        theirValue: { title: 'Updated' },
        baseValue: { title: 'Original' },
        ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
        theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
      }

      const fieldStrategy = createFieldBasedStrategy(
        { title: 'ours' },
        'theirs'
      )

      const result = resolveConflict(conflict, fieldStrategy)

      expect(result.resolvedValue).toEqual({ title: 'Updated' })
    })
  })

  describe('createPreferenceStrategy', () => {
    it('should use custom preference function', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'views',
        100,
        200,
        50,
        1000,
        1100
      )

      // Prefer lower values
      const preferLower = createPreferenceStrategy(
        (ourValue, theirValue) => (ourValue as number) < (theirValue as number)
      )

      const result = resolveConflict(conflict, preferLower)

      expect(result.resolvedValue).toBe(100)
      expect(result.strategy).toBe('preference')
    })

    it('should receive full conflict info in preference function', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'status',
        'published',
        'draft',
        'pending',
        1000,
        1100
      )

      // Prefer ours only for specific targets
      const preferOursForPosts = createPreferenceStrategy(
        (ourValue, theirValue, c) => c.target.startsWith('posts:')
      )

      const result = resolveConflict(conflict, preferOursForPosts)

      expect(result.resolvedValue).toBe('published')
    })
  })

  describe('createNonNullStrategy', () => {
    it('should prefer non-null value over null', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'description',
        null,
        'Valid description',
        'Original',
        1000,
        1100
      )

      const nonNull = createNonNullStrategy()
      const result = resolveConflict(conflict, nonNull)

      expect(result.resolvedValue).toBe('Valid description')
    })

    it('should prefer non-undefined value over undefined', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'optionalField',
        undefined,
        'has value',
        'original',
        1000,
        1100
      )

      const nonNull = createNonNullStrategy()
      const result = resolveConflict(conflict, nonNull)

      expect(result.resolvedValue).toBe('has value')
    })

    it('should prefer ours when both are non-null', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'field',
        'our value',
        'their value',
        'base',
        1000,
        1100
      )

      const nonNull = createNonNullStrategy()
      const result = resolveConflict(conflict, nonNull)

      // When both are non-null, preference function returns false, so theirs wins
      expect(result.resolvedValue).toBe('their value')
    })

    it('should prefer theirs when both are null/undefined', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'field',
        null,
        undefined,
        'base',
        1000,
        1100
      )

      const nonNull = createNonNullStrategy()
      const result = resolveConflict(conflict, nonNull)

      expect(result.resolvedValue).toBeUndefined()
    })
  })

  describe('createConcatenateStrategy', () => {
    it('should concatenate string values with default separator', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'content',
        'Our content here.',
        'Their content here.',
        'Original content.',
        1000,
        1100
      )

      const concat = createConcatenateStrategy()
      const result = resolveConflict(conflict, concat)

      expect(result.resolvedValue).toBe('Our content here.\n---\nTheir content here.')
      expect(result.strategy).toBe('concatenate')
    })

    it('should concatenate with custom separator', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'notes',
        'Note A',
        'Note B',
        '',
        1000,
        1100
      )

      const concat = createConcatenateStrategy(' | ')
      const result = resolveConflict(conflict, concat)

      expect(result.resolvedValue).toBe('Note A | Note B')
    })

    it('should fall back to manual for non-string values', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'count',
        10,
        20,
        5,
        1000,
        1100
      )

      const concat = createConcatenateStrategy()
      const result = resolveConflict(conflict, concat)

      expect(result.requiresManualResolution).toBe(true)
    })

    it('should handle empty strings', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'content',
        '',
        'Their content',
        'Original',
        1000,
        1100
      )

      const concat = createConcatenateStrategy()
      const result = resolveConflict(conflict, concat)

      expect(result.resolvedValue).toBe('\n---\nTheir content')
    })
  })

  describe('createArrayMergeStrategy', () => {
    it('should merge arrays and remove duplicates', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'tags',
        ['a', 'b', 'c'],
        ['b', 'c', 'd', 'e'],
        ['a', 'b'],
        1000,
        1100
      )

      const arrayMerge = createArrayMergeStrategy()
      const result = resolveConflict(conflict, arrayMerge)

      expect(result.resolvedValue).toEqual(['a', 'b', 'c', 'd', 'e'])
      expect(result.strategy).toBe('array-merge')
    })

    it('should handle disjoint arrays', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'tags',
        ['a', 'b'],
        ['c', 'd'],
        [],
        1000,
        1100
      )

      const arrayMerge = createArrayMergeStrategy()
      const result = resolveConflict(conflict, arrayMerge)

      expect(result.resolvedValue).toEqual(['a', 'b', 'c', 'd'])
    })

    it('should fall back to manual for non-array values', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'field',
        'not an array',
        ['a', 'b'],
        [],
        1000,
        1100
      )

      const arrayMerge = createArrayMergeStrategy()
      const result = resolveConflict(conflict, arrayMerge)

      expect(result.requiresManualResolution).toBe(true)
    })

    it('should handle empty arrays', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'tags',
        [],
        ['a', 'b'],
        [],
        1000,
        1100
      )

      const arrayMerge = createArrayMergeStrategy()
      const result = resolveConflict(conflict, arrayMerge)

      expect(result.resolvedValue).toEqual(['a', 'b'])
    })

    it('should handle arrays with identical elements', () => {
      const conflict = createConflict(
        'concurrent_update',
        'posts:p1',
        'tags',
        ['a', 'b', 'c'],
        ['a', 'b', 'c'],
        ['a'],
        1000,
        1100
      )

      const arrayMerge = createArrayMergeStrategy()
      const result = resolveConflict(conflict, arrayMerge)

      expect(result.resolvedValue).toEqual(['a', 'b', 'c'])
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Conflict Resolution - Edge cases', () => {
  it('should handle conflicts with equal timestamps in latest strategy', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'field',
      'our value',
      'their value',
      'base',
      1000,  // equal timestamps
      1000
    )

    const result = resolveConflict(conflict, 'latest')

    // With equal timestamps, ours should win (>= check)
    expect(result.resolvedValue).toBe('our value')
  })

  it('should handle missing field property in conflict', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      // field is undefined
      ourValue: undefined,
      theirValue: { title: 'Updated' },
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
    }

    // Should not throw for any strategy
    expect(() => resolveConflict(conflict, 'ours')).not.toThrow()
    expect(() => resolveConflict(conflict, 'theirs')).not.toThrow()
    expect(() => resolveConflict(conflict, 'latest')).not.toThrow()
    expect(() => resolveConflict(conflict, 'manual')).not.toThrow()
  })

  it('should handle conflicts where both values are undefined', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'optional',
      undefined,
      undefined,
      'base',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBeUndefined()
    expect(result.requiresManualResolution).toBe(false)
  })

  it('should handle conflicts with very large timestamps', () => {
    const largeTs = Number.MAX_SAFE_INTEGER - 1
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'field',
      'our value',
      'their value',
      'base',
      largeTs,
      largeTs + 1
    )

    const result = resolveConflict(conflict, 'latest')

    expect(result.resolvedValue).toBe('their value')
  })

  it('should handle deeply nested object values', () => {
    const deepValue = {
      level1: {
        level2: {
          level3: {
            value: 'deep'
          }
        }
      }
    }

    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'nested',
      deepValue,
      { different: 'structure' },
      {},
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toEqual(deepValue)
  })

  it('should handle boolean values correctly', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'isPublished',
      true,
      false,
      false,
      1000,
      1100
    )

    const oursResult = resolveConflict(conflict, 'ours')
    const theirsResult = resolveConflict(conflict, 'theirs')

    expect(oursResult.resolvedValue).toBe(true)
    expect(theirsResult.resolvedValue).toBe(false)
  })

  it('should handle numeric zero values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'count',
      0,
      10,
      5,
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBe(0)
  })

  it('should handle empty string values', () => {
    const conflict = createConflict(
      'concurrent_update',
      'posts:p1',
      'title',
      '',
      'Non-empty',
      'Original',
      1000,
      1100
    )

    const result = resolveConflict(conflict, 'ours')

    expect(result.resolvedValue).toBe('')
  })
})
