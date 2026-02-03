/**
 * Update Operator Type Guards Tests
 *
 * Tests for individual update operator type guard functions.
 * These type guards provide runtime type checking for MongoDB-style update operators.
 */

import { describe, it, expect } from 'vitest'
import {
  is$Set,
  is$Unset,
  is$Rename,
  is$SetOnInsert,
  is$Inc,
  is$Mul,
  is$Min,
  is$Max,
  is$Push,
  is$Pull,
  is$PullAll,
  is$AddToSet,
  is$Pop,
  is$CurrentDate,
  is$Link,
  is$Unlink,
  is$Bit,
  is$Embed,
  hasFieldOperators,
  hasNumericOperators,
  hasArrayOperators,
  hasRelationshipOperators,
  hasEmbeddingOperators,
  getUpdateOperatorTypes,
  type UpdateInput,
} from '../../../src/types/update'

// =============================================================================
// Field Operator Type Guards
// =============================================================================

describe('Field Operator Type Guards', () => {
  describe('is$Set', () => {
    it('returns true when update contains $set', () => {
      const update: UpdateInput = { $set: { name: 'test' } }
      expect(is$Set(update)).toBe(true)
    })

    it('returns false when update does not contain $set', () => {
      const update: UpdateInput = { $inc: { count: 1 } }
      expect(is$Set(update)).toBe(false)
    })

    it('returns false for empty update', () => {
      const update: UpdateInput = {}
      expect(is$Set(update)).toBe(false)
    })

    it('returns false when $set is undefined', () => {
      const update = { $set: undefined } as unknown as UpdateInput
      expect(is$Set(update)).toBe(false)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $set: { title: 'Hello' } }
      if (is$Set(update)) {
        // TypeScript should know $set is defined here
        expect(update.$set.title).toBe('Hello')
      }
    })
  })

  describe('is$Unset', () => {
    it('returns true when update contains $unset', () => {
      const update: UpdateInput = { $unset: { oldField: '' } }
      expect(is$Unset(update)).toBe(true)
    })

    it('returns false when update does not contain $unset', () => {
      const update: UpdateInput = { $set: { name: 'test' } }
      expect(is$Unset(update)).toBe(false)
    })

    it('accepts various unset value formats', () => {
      expect(is$Unset({ $unset: { field: '' } })).toBe(true)
      expect(is$Unset({ $unset: { field: 1 } })).toBe(true)
      expect(is$Unset({ $unset: { field: true } })).toBe(true)
    })
  })

  describe('is$Rename', () => {
    it('returns true when update contains $rename', () => {
      const update: UpdateInput = { $rename: { oldName: 'newName' } }
      expect(is$Rename(update)).toBe(true)
    })

    it('returns false when update does not contain $rename', () => {
      const update: UpdateInput = { $set: { name: 'test' } }
      expect(is$Rename(update)).toBe(false)
    })
  })

  describe('is$SetOnInsert', () => {
    it('returns true when update contains $setOnInsert', () => {
      const update: UpdateInput = { $setOnInsert: { createdAt: new Date() } }
      expect(is$SetOnInsert(update)).toBe(true)
    })

    it('returns false when update does not contain $setOnInsert', () => {
      const update: UpdateInput = { $set: { name: 'test' } }
      expect(is$SetOnInsert(update)).toBe(false)
    })
  })
})

// =============================================================================
// Numeric Operator Type Guards
// =============================================================================

describe('Numeric Operator Type Guards', () => {
  describe('is$Inc', () => {
    it('returns true when update contains $inc', () => {
      const update: UpdateInput = { $inc: { count: 1 } }
      expect(is$Inc(update)).toBe(true)
    })

    it('returns false when update does not contain $inc', () => {
      const update: UpdateInput = { $set: { count: 10 } }
      expect(is$Inc(update)).toBe(false)
    })

    it('works with negative increments', () => {
      const update: UpdateInput = { $inc: { count: -5 } }
      expect(is$Inc(update)).toBe(true)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $inc: { views: 1 } }
      if (is$Inc(update)) {
        expect(update.$inc.views).toBe(1)
      }
    })
  })

  describe('is$Mul', () => {
    it('returns true when update contains $mul', () => {
      const update: UpdateInput = { $mul: { price: 1.1 } }
      expect(is$Mul(update)).toBe(true)
    })

    it('returns false when update does not contain $mul', () => {
      const update: UpdateInput = { $inc: { count: 1 } }
      expect(is$Mul(update)).toBe(false)
    })
  })

  describe('is$Min', () => {
    it('returns true when update contains $min', () => {
      const update: UpdateInput = { $min: { lowScore: 50 } }
      expect(is$Min(update)).toBe(true)
    })

    it('returns false when update does not contain $min', () => {
      const update: UpdateInput = { $max: { highScore: 100 } }
      expect(is$Min(update)).toBe(false)
    })
  })

  describe('is$Max', () => {
    it('returns true when update contains $max', () => {
      const update: UpdateInput = { $max: { highScore: 100 } }
      expect(is$Max(update)).toBe(true)
    })

    it('returns false when update does not contain $max', () => {
      const update: UpdateInput = { $min: { lowScore: 50 } }
      expect(is$Max(update)).toBe(false)
    })
  })
})

// =============================================================================
// Array Operator Type Guards
// =============================================================================

describe('Array Operator Type Guards', () => {
  describe('is$Push', () => {
    it('returns true when update contains $push', () => {
      const update: UpdateInput = { $push: { tags: 'new-tag' } }
      expect(is$Push(update)).toBe(true)
    })

    it('returns false when update does not contain $push', () => {
      const update: UpdateInput = { $set: { tags: ['a', 'b'] } }
      expect(is$Push(update)).toBe(false)
    })

    it('works with $each modifier', () => {
      const update: UpdateInput = {
        $push: { tags: { $each: ['tag1', 'tag2'] } },
      }
      expect(is$Push(update)).toBe(true)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $push: { items: 'item' } }
      if (is$Push(update)) {
        expect(update.$push.items).toBe('item')
      }
    })
  })

  describe('is$Pull', () => {
    it('returns true when update contains $pull', () => {
      const update: UpdateInput = { $pull: { tags: 'remove-me' } }
      expect(is$Pull(update)).toBe(true)
    })

    it('returns false when update does not contain $pull', () => {
      const update: UpdateInput = { $push: { tags: 'add-me' } }
      expect(is$Pull(update)).toBe(false)
    })

    it('works with filter conditions', () => {
      const update: UpdateInput = { $pull: { scores: { $lt: 50 } } }
      expect(is$Pull(update)).toBe(true)
    })
  })

  describe('is$PullAll', () => {
    it('returns true when update contains $pullAll', () => {
      const update: UpdateInput = { $pullAll: { tags: ['a', 'b'] } }
      expect(is$PullAll(update)).toBe(true)
    })

    it('returns false when update does not contain $pullAll', () => {
      const update: UpdateInput = { $pull: { tags: 'a' } }
      expect(is$PullAll(update)).toBe(false)
    })
  })

  describe('is$AddToSet', () => {
    it('returns true when update contains $addToSet', () => {
      const update: UpdateInput = { $addToSet: { tags: 'unique' } }
      expect(is$AddToSet(update)).toBe(true)
    })

    it('returns false when update does not contain $addToSet', () => {
      const update: UpdateInput = { $push: { tags: 'duplicate-ok' } }
      expect(is$AddToSet(update)).toBe(false)
    })

    it('works with $each modifier', () => {
      const update: UpdateInput = {
        $addToSet: { tags: { $each: ['a', 'b'] } },
      }
      expect(is$AddToSet(update)).toBe(true)
    })
  })

  describe('is$Pop', () => {
    it('returns true when update contains $pop', () => {
      const update: UpdateInput = { $pop: { queue: 1 } }
      expect(is$Pop(update)).toBe(true)
    })

    it('returns false when update does not contain $pop', () => {
      const update: UpdateInput = { $push: { queue: 'item' } }
      expect(is$Pop(update)).toBe(false)
    })

    it('works with -1 (shift) direction', () => {
      const update: UpdateInput = { $pop: { queue: -1 } }
      expect(is$Pop(update)).toBe(true)
    })
  })
})

// =============================================================================
// Date Operator Type Guards
// =============================================================================

describe('Date Operator Type Guards', () => {
  describe('is$CurrentDate', () => {
    it('returns true when update contains $currentDate', () => {
      const update: UpdateInput = { $currentDate: { updatedAt: true } }
      expect(is$CurrentDate(update)).toBe(true)
    })

    it('returns false when update does not contain $currentDate', () => {
      const update: UpdateInput = { $set: { updatedAt: new Date() } }
      expect(is$CurrentDate(update)).toBe(false)
    })

    it('works with $type specification', () => {
      const update: UpdateInput = {
        $currentDate: { lastModified: { $type: 'timestamp' } },
      }
      expect(is$CurrentDate(update)).toBe(true)
    })
  })
})

// =============================================================================
// Relationship Operator Type Guards
// =============================================================================

describe('Relationship Operator Type Guards', () => {
  describe('is$Link', () => {
    it('returns true when update contains $link', () => {
      const update: UpdateInput = { $link: { author: 'users/123' } }
      expect(is$Link(update)).toBe(true)
    })

    it('returns false when update does not contain $link', () => {
      const update: UpdateInput = { $set: { authorId: 'users/123' } }
      expect(is$Link(update)).toBe(false)
    })

    it('works with array of targets', () => {
      const update: UpdateInput = {
        $link: { tags: ['tags/a', 'tags/b'] },
      }
      expect(is$Link(update)).toBe(true)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $link: { category: 'categories/tech' } }
      if (is$Link(update)) {
        expect(update.$link.category).toBe('categories/tech')
      }
    })
  })

  describe('is$Unlink', () => {
    it('returns true when update contains $unlink', () => {
      const update: UpdateInput = { $unlink: { author: 'users/123' } }
      expect(is$Unlink(update)).toBe(true)
    })

    it('returns false when update does not contain $unlink', () => {
      const update: UpdateInput = { $link: { author: 'users/456' } }
      expect(is$Unlink(update)).toBe(false)
    })

    it('works with $all special value', () => {
      const update: UpdateInput = { $unlink: { tags: '$all' } }
      expect(is$Unlink(update)).toBe(true)
    })
  })
})

// =============================================================================
// Bitwise Operator Type Guards
// =============================================================================

describe('Bitwise Operator Type Guards', () => {
  describe('is$Bit', () => {
    it('returns true when update contains $bit', () => {
      const update: UpdateInput = { $bit: { flags: { or: 4 } } }
      expect(is$Bit(update)).toBe(true)
    })

    it('returns false when update does not contain $bit', () => {
      const update: UpdateInput = { $inc: { flags: 4 } }
      expect(is$Bit(update)).toBe(false)
    })

    it('works with and operation', () => {
      const update: UpdateInput = { $bit: { permissions: { and: 0b1111 } } }
      expect(is$Bit(update)).toBe(true)
    })

    it('works with xor operation', () => {
      const update: UpdateInput = { $bit: { state: { xor: 0b0001 } } }
      expect(is$Bit(update)).toBe(true)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $bit: { mask: { or: 8 } } }
      if (is$Bit(update)) {
        expect(update.$bit.mask).toEqual({ or: 8 })
      }
    })
  })
})

// =============================================================================
// Embedding Operator Type Guards
// =============================================================================

describe('Embedding Operator Type Guards', () => {
  describe('is$Embed', () => {
    it('returns true when update contains $embed', () => {
      const update: UpdateInput = { $embed: { description: 'embedding' } }
      expect(is$Embed(update)).toBe(true)
    })

    it('returns false when update does not contain $embed', () => {
      const update: UpdateInput = { $set: { embedding: [0.1, 0.2, 0.3] } }
      expect(is$Embed(update)).toBe(false)
    })

    it('works with options object', () => {
      const update: UpdateInput = {
        $embed: {
          content: { field: 'contentVector', model: '@cf/baai/bge-small-en-v1.5' },
        },
      }
      expect(is$Embed(update)).toBe(true)
    })

    it('narrows type correctly', () => {
      const update: UpdateInput = { $embed: { title: 'titleEmbedding' } }
      if (is$Embed(update)) {
        expect(update.$embed.title).toBe('titleEmbedding')
      }
    })
  })
})

// =============================================================================
// Category Helper Functions
// =============================================================================

describe('Category Helper Functions', () => {
  describe('hasFieldOperators', () => {
    it('returns true for $set', () => {
      expect(hasFieldOperators({ $set: { name: 'test' } })).toBe(true)
    })

    it('returns true for $unset', () => {
      expect(hasFieldOperators({ $unset: { field: '' } })).toBe(true)
    })

    it('returns true for $rename', () => {
      expect(hasFieldOperators({ $rename: { old: 'new' } })).toBe(true)
    })

    it('returns false for non-field operators', () => {
      expect(hasFieldOperators({ $inc: { count: 1 } })).toBe(false)
      expect(hasFieldOperators({ $push: { items: 'x' } })).toBe(false)
    })

    it('returns false for empty update', () => {
      expect(hasFieldOperators({})).toBe(false)
    })
  })

  describe('hasNumericOperators', () => {
    it('returns true for $inc', () => {
      expect(hasNumericOperators({ $inc: { count: 1 } })).toBe(true)
    })

    it('returns true for $mul', () => {
      expect(hasNumericOperators({ $mul: { price: 1.5 } })).toBe(true)
    })

    it('returns true for $min', () => {
      expect(hasNumericOperators({ $min: { low: 0 } })).toBe(true)
    })

    it('returns true for $max', () => {
      expect(hasNumericOperators({ $max: { high: 100 } })).toBe(true)
    })

    it('returns false for non-numeric operators', () => {
      expect(hasNumericOperators({ $set: { count: 10 } })).toBe(false)
      expect(hasNumericOperators({ $push: { items: 1 } })).toBe(false)
    })
  })

  describe('hasArrayOperators', () => {
    it('returns true for $push', () => {
      expect(hasArrayOperators({ $push: { items: 'x' } })).toBe(true)
    })

    it('returns true for $pull', () => {
      expect(hasArrayOperators({ $pull: { items: 'x' } })).toBe(true)
    })

    it('returns true for $pullAll', () => {
      expect(hasArrayOperators({ $pullAll: { items: ['x'] } })).toBe(true)
    })

    it('returns true for $addToSet', () => {
      expect(hasArrayOperators({ $addToSet: { items: 'x' } })).toBe(true)
    })

    it('returns true for $pop', () => {
      expect(hasArrayOperators({ $pop: { items: 1 } })).toBe(true)
    })

    it('returns false for non-array operators', () => {
      expect(hasArrayOperators({ $set: { items: ['x'] } })).toBe(false)
      expect(hasArrayOperators({ $inc: { count: 1 } })).toBe(false)
    })
  })

  describe('hasRelationshipOperators', () => {
    it('returns true for $link', () => {
      expect(hasRelationshipOperators({ $link: { author: 'users/1' } })).toBe(true)
    })

    it('returns true for $unlink', () => {
      expect(hasRelationshipOperators({ $unlink: { author: 'users/1' } })).toBe(true)
    })

    it('returns false for non-relationship operators', () => {
      expect(hasRelationshipOperators({ $set: { authorId: 'users/1' } })).toBe(false)
    })
  })

  describe('hasEmbeddingOperators', () => {
    it('returns true for $embed', () => {
      expect(hasEmbeddingOperators({ $embed: { text: 'vector' } })).toBe(true)
    })

    it('returns false for non-embedding operators', () => {
      expect(hasEmbeddingOperators({ $set: { vector: [0.1] } })).toBe(false)
    })
  })

  describe('getUpdateOperatorTypes', () => {
    it('returns empty array for empty update', () => {
      expect(getUpdateOperatorTypes({})).toEqual([])
    })

    it('returns single operator', () => {
      expect(getUpdateOperatorTypes({ $set: { name: 'test' } })).toEqual(['$set'])
    })

    it('returns multiple operators', () => {
      const update: UpdateInput = {
        $set: { status: 'active' },
        $inc: { count: 1 },
        $push: { tags: 'new' },
      }
      const operators = getUpdateOperatorTypes(update)
      expect(operators).toHaveLength(3)
      expect(operators).toContain('$set')
      expect(operators).toContain('$inc')
      expect(operators).toContain('$push')
    })

    it('only includes $-prefixed keys', () => {
      // Non-standard update object with non-operator keys
      const update = { $set: { x: 1 }, notAnOperator: 'ignored' } as unknown as UpdateInput
      expect(getUpdateOperatorTypes(update)).toEqual(['$set'])
    })
  })
})

// =============================================================================
// Combined Operators
// =============================================================================

describe('Combined Operators', () => {
  it('can detect multiple operators in a single update', () => {
    const update: UpdateInput = {
      $set: { status: 'published' },
      $inc: { version: 1 },
      $currentDate: { updatedAt: true },
      $push: { tags: 'featured' },
      $link: { categories: 'categories/tech' },
    }

    expect(is$Set(update)).toBe(true)
    expect(is$Inc(update)).toBe(true)
    expect(is$CurrentDate(update)).toBe(true)
    expect(is$Push(update)).toBe(true)
    expect(is$Link(update)).toBe(true)

    // These should be false
    expect(is$Unset(update)).toBe(false)
    expect(is$Pull(update)).toBe(false)
    expect(is$Embed(update)).toBe(false)
  })

  it('type guards work together with category helpers', () => {
    const update: UpdateInput = {
      $set: { name: 'test' },
      $inc: { count: 1 },
      $push: { items: 'x' },
    }

    expect(hasFieldOperators(update)).toBe(true)
    expect(hasNumericOperators(update)).toBe(true)
    expect(hasArrayOperators(update)).toBe(true)
    expect(hasRelationshipOperators(update)).toBe(false)
    expect(hasEmbeddingOperators(update)).toBe(false)
  })
})
