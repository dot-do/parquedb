/**
 * Sync Conflict Resolution Tests
 *
 * Tests for commutative operations, conflict detection, and resolution strategies.
 */

import { describe, it, expect } from 'vitest'
import {
  isCommutative,
  combineOperations,
  getAffectedFields,
  extractOperations,
  type UpdateOps,
} from '../../../src/sync/commutative-ops'
import {
  detectConflicts,
  isDeleteEvent,
  isUpdateEvent,
  isCreateEvent,
  type ConflictInfo,
} from '../../../src/sync/conflict-detection'
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
  type ResolutionStrategy,
} from '../../../src/sync/conflict-resolution'
import type { Event } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a test event with default values
 */
function createEvent(partial: Partial<Event> & { id: string; target: string; op: Event['op'] }): Event {
  return {
    ts: Date.now(),
    ...partial,
  }
}

/**
 * Create a conflict info for testing
 */
function createConflictInfo(
  partial: Partial<ConflictInfo> & { target: string; type: ConflictInfo['type'] }
): ConflictInfo {
  return {
    ourValue: 'our-value',
    theirValue: 'their-value',
    baseValue: 'base-value',
    ourEvent: createEvent({ id: 'our-event', target: partial.target, op: 'UPDATE', ts: 1000 }),
    theirEvent: createEvent({ id: 'their-event', target: partial.target, op: 'UPDATE', ts: 2000 }),
    ...partial,
  }
}

// =============================================================================
// Commutative Operations Tests
// =============================================================================

describe('Commutative Operations', () => {
  describe('isCommutative', () => {
    describe('$inc operations', () => {
      it('should recognize $inc on same field as commutative', () => {
        const op1: UpdateOps = { $inc: { count: 1 } }
        const op2: UpdateOps = { $inc: { count: 5 } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $inc on different fields as commutative', () => {
        const op1: UpdateOps = { $inc: { views: 1 } }
        const op2: UpdateOps = { $inc: { likes: 1 } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $inc and $set on different fields as commutative', () => {
        const op1: UpdateOps = { $inc: { count: 1 } }
        const op2: UpdateOps = { $set: { name: 'test' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $inc and $set on same field as non-commutative', () => {
        const op1: UpdateOps = { $inc: { count: 1 } }
        const op2: UpdateOps = { $set: { count: 100 } }

        expect(isCommutative(op1, op2)).toBe(false)
      })
    })

    describe('$set operations', () => {
      it('should recognize $set on different fields as commutative', () => {
        const op1: UpdateOps = { $set: { name: 'Alice' } }
        const op2: UpdateOps = { $set: { email: 'alice@test.com' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $set on same field as non-commutative (conflict)', () => {
        const op1: UpdateOps = { $set: { name: 'Alice' } }
        const op2: UpdateOps = { $set: { name: 'Bob' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })

      it('should handle multiple fields in $set', () => {
        const op1: UpdateOps = { $set: { name: 'Alice', age: 25 } }
        const op2: UpdateOps = { $set: { email: 'alice@test.com', phone: '123' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should detect conflict with partial field overlap', () => {
        const op1: UpdateOps = { $set: { name: 'Alice', age: 25 } }
        const op2: UpdateOps = { $set: { age: 30, email: 'alice@test.com' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })
    })

    describe('$unset operations', () => {
      it('should recognize $unset on different fields as commutative', () => {
        const op1: UpdateOps = { $unset: { oldField: '' } }
        const op2: UpdateOps = { $unset: { anotherOldField: '' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $unset on same field as non-commutative', () => {
        const op1: UpdateOps = { $unset: { field: '' } }
        const op2: UpdateOps = { $unset: { field: '' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })

      it('should recognize $set and $unset on different fields as commutative', () => {
        const op1: UpdateOps = { $set: { name: 'test' } }
        const op2: UpdateOps = { $unset: { oldName: '' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $set and $unset on same field as non-commutative', () => {
        const op1: UpdateOps = { $set: { name: 'test' } }
        const op2: UpdateOps = { $unset: { name: '' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })
    })

    describe('$addToSet operations', () => {
      it('should recognize $addToSet on same field as commutative', () => {
        const op1: UpdateOps = { $addToSet: { tags: 'typescript' } }
        const op2: UpdateOps = { $addToSet: { tags: 'javascript' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $addToSet on different fields as commutative', () => {
        const op1: UpdateOps = { $addToSet: { tags: 'typescript' } }
        const op2: UpdateOps = { $addToSet: { categories: 'tech' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })
    })

    describe('$push operations', () => {
      it('should recognize $push as never commutative (order matters)', () => {
        const op1: UpdateOps = { $push: { items: 'first' } }
        const op2: UpdateOps = { $push: { items: 'second' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })

      it('should recognize $push with $set on different fields as non-commutative', () => {
        const op1: UpdateOps = { $push: { items: 'item' } }
        const op2: UpdateOps = { $set: { name: 'test' } }

        expect(isCommutative(op1, op2)).toBe(false)
      })
    })

    describe('$min and $max operations', () => {
      it('should recognize $min on different fields as commutative', () => {
        const op1: UpdateOps = { $min: { minPrice: 10 } }
        const op2: UpdateOps = { $min: { minQuantity: 5 } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $max on different fields as commutative', () => {
        const op1: UpdateOps = { $max: { maxPrice: 100 } }
        const op2: UpdateOps = { $max: { maxQuantity: 50 } }

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize $min and $max on same field as non-commutative', () => {
        const op1: UpdateOps = { $min: { value: 10 } }
        const op2: UpdateOps = { $max: { value: 100 } }

        expect(isCommutative(op1, op2)).toBe(false)
      })
    })

    describe('empty operations', () => {
      it('should recognize empty operations as commutative', () => {
        const op1: UpdateOps = {}
        const op2: UpdateOps = {}

        expect(isCommutative(op1, op2)).toBe(true)
      })

      it('should recognize empty operation with non-empty as commutative', () => {
        const op1: UpdateOps = {}
        const op2: UpdateOps = { $set: { name: 'test' } }

        expect(isCommutative(op1, op2)).toBe(true)
      })
    })
  })

  describe('combineOperations', () => {
    it('should combine $inc operations by summing values', () => {
      const op1: UpdateOps = { $inc: { count: 1 } }
      const op2: UpdateOps = { $inc: { count: 5 } }

      const combined = combineOperations(op1, op2)

      expect(combined.$inc).toEqual({ count: 6 })
    })

    it('should combine $inc with different fields', () => {
      const op1: UpdateOps = { $inc: { views: 1 } }
      const op2: UpdateOps = { $inc: { likes: 2 } }

      const combined = combineOperations(op1, op2)

      expect(combined.$inc).toEqual({ views: 1, likes: 2 })
    })

    it('should combine $set operations on different fields', () => {
      const op1: UpdateOps = { $set: { name: 'Alice' } }
      const op2: UpdateOps = { $set: { email: 'alice@test.com' } }

      const combined = combineOperations(op1, op2)

      expect(combined.$set).toEqual({ name: 'Alice', email: 'alice@test.com' })
    })

    it('should combine $unset operations', () => {
      const op1: UpdateOps = { $unset: { oldField: '' } }
      const op2: UpdateOps = { $unset: { anotherOldField: '' } }

      const combined = combineOperations(op1, op2)

      expect(combined.$unset).toEqual({ oldField: '', anotherOldField: '' })
    })

    it('should combine $addToSet operations using set union', () => {
      const op1: UpdateOps = { $addToSet: { tags: { $each: ['typescript'] } } }
      const op2: UpdateOps = { $addToSet: { tags: { $each: ['javascript'] } } }

      const combined = combineOperations(op1, op2)

      expect(combined.$addToSet).toEqual({ tags: { $each: ['typescript', 'javascript'] } })
    })

    it('should deduplicate $addToSet values', () => {
      const op1: UpdateOps = { $addToSet: { tags: { $each: ['typescript', 'javascript'] } } }
      const op2: UpdateOps = { $addToSet: { tags: { $each: ['javascript', 'rust'] } } }

      const combined = combineOperations(op1, op2)

      expect(combined.$addToSet).toEqual({ tags: { $each: ['typescript', 'javascript', 'rust'] } })
    })

    it('should combine $min by taking minimum', () => {
      const op1: UpdateOps = { $min: { value: 10 } }
      const op2: UpdateOps = { $min: { value: 5 } }

      const combined = combineOperations(op1, op2)

      expect(combined.$min).toEqual({ value: 5 })
    })

    it('should combine $max by taking maximum', () => {
      const op1: UpdateOps = { $max: { value: 10 } }
      const op2: UpdateOps = { $max: { value: 15 } }

      const combined = combineOperations(op1, op2)

      expect(combined.$max).toEqual({ value: 15 })
    })

    it('should combine multiple operation types', () => {
      const op1: UpdateOps = { $inc: { count: 1 }, $set: { name: 'test' } }
      const op2: UpdateOps = { $inc: { views: 1 }, $set: { email: 'test@test.com' } }

      const combined = combineOperations(op1, op2)

      expect(combined.$inc).toEqual({ count: 1, views: 1 })
      expect(combined.$set).toEqual({ name: 'test', email: 'test@test.com' })
    })
  })

  describe('getAffectedFields', () => {
    it('should extract fields from $set', () => {
      const ops: UpdateOps = { $set: { name: 'test', email: 'test@test.com' } }
      const fields = getAffectedFields(ops)

      expect(fields).toContain('name')
      expect(fields).toContain('email')
      expect(fields.size).toBe(2)
    })

    it('should extract fields from multiple operators', () => {
      const ops: UpdateOps = {
        $set: { name: 'test' },
        $inc: { count: 1 },
        $unset: { oldField: '' },
      }
      const fields = getAffectedFields(ops)

      expect(fields).toContain('name')
      expect(fields).toContain('count')
      expect(fields).toContain('oldField')
      expect(fields.size).toBe(3)
    })

    it('should return empty set for empty operations', () => {
      const ops: UpdateOps = {}
      const fields = getAffectedFields(ops)

      expect(fields.size).toBe(0)
    })
  })

  describe('extractOperations', () => {
    it('should extract _ops from event after state', () => {
      const after = {
        name: 'test',
        _ops: { $set: { name: 'test' } },
      }
      const ops = extractOperations(after)

      expect(ops).toEqual({ $set: { name: 'test' } })
    })

    it('should return empty object when no _ops present', () => {
      const after = { name: 'test' }
      const ops = extractOperations(after)

      expect(ops).toEqual({})
    })

    it('should return empty object for undefined input', () => {
      const ops = extractOperations(undefined)

      expect(ops).toEqual({})
    })
  })
})

// =============================================================================
// Conflict Detection Tests
// =============================================================================

describe('Conflict Detection', () => {
  describe('detectConflicts', () => {
    describe('concurrent modifications to same entity', () => {
      it('should detect conflict when same field modified with different values', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'Original' },
            after: { name: 'OurChange' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { name: 'Original' },
            after: { name: 'TheirChange' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.type).toBe('concurrent_update')
        expect(conflicts[0]!.target).toBe('users:u1')
        expect(conflicts[0]!.field).toBe('name')
        expect(conflicts[0]!.ourValue).toBe('OurChange')
        expect(conflicts[0]!.theirValue).toBe('TheirChange')
      })

      it('should not detect conflict when same field modified with same value', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'Original' },
            after: { name: 'SameValue' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { name: 'Original' },
            after: { name: 'SameValue' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })

      it('should not detect conflict when different fields modified', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'Original', email: 'old@test.com' },
            after: { name: 'NewName', email: 'old@test.com' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { name: 'Original', email: 'old@test.com' },
            after: { name: 'Original', email: 'new@test.com' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })

      it('should detect conflicts with embedded _ops and non-commutative operations', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { count: 0 },
            after: { count: 10, _ops: { $set: { count: 10 } } },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { count: 0 },
            after: { count: 20, _ops: { $set: { count: 20 } } },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.type).toBe('concurrent_update')
      })

      it('should not detect conflict with commutative $inc operations', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { count: 0 },
            after: { count: 5, _ops: { $inc: { count: 5 } } },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { count: 0 },
            after: { count: 3, _ops: { $inc: { count: 3 } } },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })
    })

    describe('modifications to different entities (no conflict)', () => {
      it('should not detect conflict when different entities modified', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'User1' },
            after: { name: 'User1Updated' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u2',
            op: 'UPDATE',
            ts: 1001,
            before: { name: 'User2' },
            after: { name: 'User2Updated' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })

      it('should not detect conflict with events on different namespaces', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'User' },
            after: { name: 'UserUpdated' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'posts:p1',
            op: 'UPDATE',
            ts: 1001,
            before: { title: 'Post' },
            after: { title: 'PostUpdated' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })
    })

    describe('delete vs update conflict', () => {
      it('should detect delete_update conflict when we delete and they update', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'DELETE',
            ts: 1000,
            before: { name: 'User' },
            after: undefined,
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1001,
            before: { name: 'User' },
            after: { name: 'UpdatedUser' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.type).toBe('delete_update')
        expect(conflicts[0]!.target).toBe('users:u1')
        expect(conflicts[0]!.ourValue).toBe(undefined)
        expect(conflicts[0]!.theirValue).toEqual({ name: 'UpdatedUser' })
      })

      it('should detect delete_update conflict when they delete and we update', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'User' },
            after: { name: 'UpdatedUser' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'DELETE',
            ts: 1001,
            before: { name: 'User' },
            after: undefined,
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.type).toBe('delete_update')
        expect(conflicts[0]!.target).toBe('users:u1')
        expect(conflicts[0]!.ourValue).toEqual({ name: 'UpdatedUser' })
        expect(conflicts[0]!.theirValue).toBe(undefined)
      })
    })

    describe('create_create conflict', () => {
      it('should detect create_create conflict when same entity created in both streams', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'CREATE',
            ts: 1000,
            before: undefined,
            after: { name: 'OurUser' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'CREATE',
            ts: 1001,
            before: undefined,
            after: { name: 'TheirUser' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.type).toBe('create_create')
        expect(conflicts[0]!.target).toBe('users:u1')
      })

      it('should not detect conflict when same entity created with same values', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'CREATE',
            ts: 1000,
            before: undefined,
            after: { name: 'SameUser', email: 'same@test.com' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'CREATE',
            ts: 1001,
            before: undefined,
            after: { name: 'SameUser', email: 'same@test.com' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(0)
      })
    })

    describe('edge cases', () => {
      it('should handle empty event arrays', () => {
        const conflicts = detectConflicts([], [])

        expect(conflicts.length).toBe(0)
      })

      it('should handle one empty event array', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'User' },
            after: { name: 'Updated' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, [])

        expect(conflicts.length).toBe(0)
      })

      it('should use latest event when multiple events for same target', () => {
        const ourEvents: Event[] = [
          createEvent({
            id: 'e1',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 1000,
            before: { name: 'Original' },
            after: { name: 'First' },
          }),
          createEvent({
            id: 'e3',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 3000,
            before: { name: 'First' },
            after: { name: 'OurLatest' },
          }),
        ]

        const theirEvents: Event[] = [
          createEvent({
            id: 'e2',
            target: 'users:u1',
            op: 'UPDATE',
            ts: 2000,
            before: { name: 'Original' },
            after: { name: 'TheirLatest' },
          }),
        ]

        const conflicts = detectConflicts(ourEvents, theirEvents)

        expect(conflicts.length).toBe(1)
        expect(conflicts[0]!.ourValue).toBe('OurLatest')
        expect(conflicts[0]!.theirValue).toBe('TheirLatest')
      })
    })
  })

  describe('event type helpers', () => {
    it('isDeleteEvent should identify DELETE events', () => {
      expect(isDeleteEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'DELETE' }))).toBe(true)
      expect(isDeleteEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'REL_DELETE' }))).toBe(true)
      expect(isDeleteEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE' }))).toBe(false)
      expect(isDeleteEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'CREATE' }))).toBe(false)
    })

    it('isUpdateEvent should identify UPDATE events', () => {
      expect(isUpdateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE' }))).toBe(true)
      expect(isUpdateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'DELETE' }))).toBe(false)
      expect(isUpdateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'CREATE' }))).toBe(false)
    })

    it('isCreateEvent should identify CREATE events', () => {
      expect(isCreateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'CREATE' }))).toBe(true)
      expect(isCreateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'REL_CREATE' }))).toBe(true)
      expect(isCreateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE' }))).toBe(false)
      expect(isCreateEvent(createEvent({ id: 'e1', target: 'users:u1', op: 'DELETE' }))).toBe(false)
    })
  })
})

// =============================================================================
// Conflict Resolution Tests
// =============================================================================

describe('Conflict Resolution', () => {
  describe('resolveConflict', () => {
    describe('ours strategy (local-wins)', () => {
      it('should resolve in favor of local value', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Local Value',
          theirValue: 'Remote Value',
        })

        const resolution = resolveConflict(conflict, 'ours')

        expect(resolution.resolvedValue).toBe('Local Value')
        expect(resolution.strategy).toBe('ours')
        expect(resolution.requiresManualResolution).toBe(false)
      })
    })

    describe('theirs strategy (remote-wins)', () => {
      it('should resolve in favor of remote value', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Local Value',
          theirValue: 'Remote Value',
        })

        const resolution = resolveConflict(conflict, 'theirs')

        expect(resolution.resolvedValue).toBe('Remote Value')
        expect(resolution.strategy).toBe('theirs')
        expect(resolution.requiresManualResolution).toBe(false)
      })
    })

    describe('latest strategy (newest-wins)', () => {
      it('should resolve in favor of value with newer timestamp', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Older Value',
          theirValue: 'Newer Value',
          ourEvent: createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE', ts: 1000 }),
          theirEvent: createEvent({ id: 'e2', target: 'users:u1', op: 'UPDATE', ts: 2000 }),
        })

        const resolution = resolveConflict(conflict, 'latest')

        expect(resolution.resolvedValue).toBe('Newer Value')
        expect(resolution.strategy).toBe('latest')
        expect(resolution.requiresManualResolution).toBe(false)
      })

      it('should resolve in favor of local when timestamps are equal', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Our Value',
          theirValue: 'Their Value',
          ourEvent: createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE', ts: 1000 }),
          theirEvent: createEvent({ id: 'e2', target: 'users:u1', op: 'UPDATE', ts: 1000 }),
        })

        const resolution = resolveConflict(conflict, 'latest')

        expect(resolution.resolvedValue).toBe('Our Value')
        expect(resolution.strategy).toBe('latest')
      })

      it('should resolve in favor of local when local is newer', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Newer Local',
          theirValue: 'Older Remote',
          ourEvent: createEvent({ id: 'e1', target: 'users:u1', op: 'UPDATE', ts: 2000 }),
          theirEvent: createEvent({ id: 'e2', target: 'users:u1', op: 'UPDATE', ts: 1000 }),
        })

        const resolution = resolveConflict(conflict, 'latest')

        expect(resolution.resolvedValue).toBe('Newer Local')
      })
    })

    describe('manual strategy', () => {
      it('should leave conflict pending for manual resolution', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
        })

        const resolution = resolveConflict(conflict, 'manual')

        expect(resolution.resolvedValue).toBe(undefined)
        expect(resolution.strategy).toBe('manual')
        expect(resolution.requiresManualResolution).toBe(true)
        expect(resolution.conflict).toBe(conflict)
      })
    })

    describe('custom strategy function', () => {
      it('should use custom resolution function', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
          ourValue: 'Local',
          theirValue: 'Remote',
        })

        const customStrategy: ResolutionStrategy = (c) => ({
          resolvedValue: `${c.ourValue} + ${c.theirValue}`,
          strategy: 'custom',
          requiresManualResolution: false,
        })

        const resolution = resolveConflict(conflict, customStrategy)

        expect(resolution.resolvedValue).toBe('Local + Remote')
        expect(resolution.strategy).toBe('custom')
      })
    })

    describe('unknown strategy', () => {
      it('should throw error for unknown strategy', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
        })

        expect(() => resolveConflict(conflict, 'unknown' as ResolutionStrategy)).toThrow(
          'Unknown resolution strategy: unknown'
        )
      })
    })
  })

  describe('resolveAllConflicts', () => {
    it('should resolve multiple conflicts with same strategy', () => {
      const conflicts = [
        createConflictInfo({ target: 'users:u1', type: 'concurrent_update', field: 'name' }),
        createConflictInfo({ target: 'users:u2', type: 'concurrent_update', field: 'email' }),
      ]

      const resolutions = resolveAllConflicts(conflicts, 'ours')

      expect(resolutions.length).toBe(2)
      expect(resolutions[0]!.strategy).toBe('ours')
      expect(resolutions[1]!.strategy).toBe('ours')
    })
  })

  describe('resolveConflictsByType', () => {
    it('should use different strategies for different conflict types', () => {
      const conflicts = [
        createConflictInfo({ target: 'users:u1', type: 'concurrent_update' }),
        createConflictInfo({ target: 'users:u2', type: 'delete_update' }),
        createConflictInfo({ target: 'users:u3', type: 'create_create' }),
      ]

      const resolutions = resolveConflictsByType(conflicts, {
        concurrent_update: 'latest',
        delete_update: 'ours',
      })

      expect(resolutions[0]!.strategy).toBe('latest')
      expect(resolutions[1]!.strategy).toBe('ours')
      expect(resolutions[2]!.requiresManualResolution).toBe(true) // default manual
    })
  })

  describe('resolution helpers', () => {
    it('allResolutionsComplete should return true when all resolved', () => {
      const resolutions = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: 'b', strategy: 'theirs', requiresManualResolution: false },
      ]

      expect(allResolutionsComplete(resolutions)).toBe(true)
    })

    it('allResolutionsComplete should return false when any manual', () => {
      const resolutions = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: undefined, strategy: 'manual', requiresManualResolution: true },
      ]

      expect(allResolutionsComplete(resolutions)).toBe(false)
    })

    it('getUnresolvedConflicts should return only manual resolutions', () => {
      const resolutions = [
        { resolvedValue: 'a', strategy: 'ours', requiresManualResolution: false },
        { resolvedValue: undefined, strategy: 'manual', requiresManualResolution: true },
        { resolvedValue: 'c', strategy: 'latest', requiresManualResolution: false },
      ]

      const unresolved = getUnresolvedConflicts(resolutions)

      expect(unresolved.length).toBe(1)
      expect(unresolved[0]!.strategy).toBe('manual')
    })

    it('applyManualResolution should update resolution', () => {
      const resolution = {
        resolvedValue: undefined,
        strategy: 'manual',
        requiresManualResolution: true,
      }

      const updated = applyManualResolution(resolution, 'user-chosen-value')

      expect(updated.resolvedValue).toBe('user-chosen-value')
      expect(updated.strategy).toBe('manual-resolved')
      expect(updated.requiresManualResolution).toBe(false)
    })
  })

  describe('strategy composition', () => {
    describe('createFallbackStrategy', () => {
      it('should try strategies in order and return first non-manual', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
        })

        const fallbackStrategy = createFallbackStrategy('manual', 'ours')
        const resolution = fallbackStrategy(conflict)

        expect(resolution.strategy).toBe('ours')
        expect(resolution.requiresManualResolution).toBe(false)
      })

      it('should return last result if all require manual resolution', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
        })

        const fallbackStrategy = createFallbackStrategy('manual', 'manual')
        const resolution = fallbackStrategy(conflict)

        expect(resolution.requiresManualResolution).toBe(true)
      })
    })

    describe('createFieldBasedStrategy', () => {
      it('should use different strategies for different fields', () => {
        const nameConflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'name',
        })
        const emailConflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'email',
        })
        const otherConflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          field: 'other',
        })

        const fieldStrategy = createFieldBasedStrategy(
          { name: 'ours', email: 'theirs' },
          'manual'
        )

        const nameResolution = fieldStrategy(nameConflict)
        const emailResolution = fieldStrategy(emailConflict)
        const otherResolution = fieldStrategy(otherConflict)

        expect(nameResolution.strategy).toBe('ours')
        expect(emailResolution.strategy).toBe('theirs')
        expect(otherResolution.requiresManualResolution).toBe(true)
      })
    })

    describe('createPreferenceStrategy', () => {
      it('should use preference function to decide', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: 10,
          theirValue: 5,
        })

        // Prefer larger values
        const preferLarger = createPreferenceStrategy(
          (ourValue, theirValue) =>
            typeof ourValue === 'number' &&
            typeof theirValue === 'number' &&
            ourValue > theirValue
        )

        const resolution = preferLarger(conflict)

        expect(resolution.resolvedValue).toBe(10)
        expect(resolution.strategy).toBe('preference')
      })
    })

    describe('createNonNullStrategy', () => {
      it('should prefer non-null values', () => {
        const nonNullStrategy = createNonNullStrategy()

        // Our value is non-null, theirs is null
        const conflict1 = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: 'value',
          theirValue: null,
        })
        const resolution1 = nonNullStrategy(conflict1)
        expect(resolution1.resolvedValue).toBe('value')

        // Our value is null, theirs is non-null
        const conflict2 = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: null,
          theirValue: 'value',
        })
        const resolution2 = nonNullStrategy(conflict2)
        expect(resolution2.resolvedValue).toBe('value')
      })
    })

    describe('createConcatenateStrategy', () => {
      it('should concatenate string values', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: 'Hello',
          theirValue: 'World',
        })

        const concatStrategy = createConcatenateStrategy(' | ')
        const resolution = concatStrategy(conflict)

        expect(resolution.resolvedValue).toBe('Hello | World')
        expect(resolution.strategy).toBe('concatenate')
      })

      it('should fall back to manual for non-strings', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: 123,
          theirValue: 456,
        })

        const concatStrategy = createConcatenateStrategy()
        const resolution = concatStrategy(conflict)

        expect(resolution.requiresManualResolution).toBe(true)
      })
    })

    describe('createArrayMergeStrategy', () => {
      it('should merge and deduplicate array values', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: ['a', 'b', 'c'],
          theirValue: ['b', 'c', 'd'],
        })

        const mergeStrategy = createArrayMergeStrategy()
        const resolution = mergeStrategy(conflict)

        expect(resolution.resolvedValue).toEqual(['a', 'b', 'c', 'd'])
        expect(resolution.strategy).toBe('array-merge')
      })

      it('should fall back to manual for non-arrays', () => {
        const conflict = createConflictInfo({
          target: 'users:u1',
          type: 'concurrent_update',
          ourValue: 'not an array',
          theirValue: 'also not an array',
        })

        const mergeStrategy = createArrayMergeStrategy()
        const resolution = mergeStrategy(conflict)

        expect(resolution.requiresManualResolution).toBe(true)
      })
    })
  })
})
