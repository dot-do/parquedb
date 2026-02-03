/**
 * Field-Level $not Operator Tests
 *
 * Tests for the field-level $not operator which allows negating specific
 * operators on a single field, as opposed to the top-level $not which
 * negates entire filter documents.
 *
 * MongoDB-style field-level $not:
 *   { field: { $not: { $operator: value } } }
 *
 * This is different from top-level $not:
 *   { $not: { field: value } }
 */

import { describe, it, expect } from 'vitest'
import { matchesFilter, createPredicate } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// Basic Field-Level $not Tests
// =============================================================================

describe('field-level $not operator', () => {
  describe('with comparison operators', () => {
    it('negates $gt operator', () => {
      const doc = { score: 50 }
      // NOT greater than 75 = less than or equal to 75
      expect(matchesFilter(doc, { score: { $not: { $gt: 75 } } })).toBe(true)
      // NOT greater than 25 = less than or equal to 25 (50 > 25, so negated is false)
      expect(matchesFilter(doc, { score: { $not: { $gt: 25 } } })).toBe(false)
    })

    it('negates $gte operator', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { score: { $not: { $gte: 75 } } })).toBe(true)
      expect(matchesFilter(doc, { score: { $not: { $gte: 50 } } })).toBe(false)
      expect(matchesFilter(doc, { score: { $not: { $gte: 25 } } })).toBe(false)
    })

    it('negates $lt operator', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { score: { $not: { $lt: 25 } } })).toBe(true)
      expect(matchesFilter(doc, { score: { $not: { $lt: 75 } } })).toBe(false)
    })

    it('negates $lte operator', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { score: { $not: { $lte: 25 } } })).toBe(true)
      expect(matchesFilter(doc, { score: { $not: { $lte: 50 } } })).toBe(false)
      expect(matchesFilter(doc, { score: { $not: { $lte: 75 } } })).toBe(false)
    })

    it('negates $eq operator', () => {
      const doc = { status: 'active' }
      expect(matchesFilter(doc, { status: { $not: { $eq: 'inactive' } } })).toBe(true)
      expect(matchesFilter(doc, { status: { $not: { $eq: 'active' } } })).toBe(false)
    })

    it('negates $ne operator', () => {
      const doc = { status: 'active' }
      // NOT not-equal-to 'active' = equal to 'active'
      expect(matchesFilter(doc, { status: { $not: { $ne: 'active' } } })).toBe(true)
      // NOT not-equal-to 'inactive' = equal to 'inactive' (status is 'active', so false)
      expect(matchesFilter(doc, { status: { $not: { $ne: 'inactive' } } })).toBe(false)
    })

    it('negates $in operator', () => {
      const doc = { category: 'electronics' }
      expect(matchesFilter(doc, { category: { $not: { $in: ['books', 'clothing'] } } })).toBe(true)
      expect(matchesFilter(doc, { category: { $not: { $in: ['electronics', 'books'] } } })).toBe(false)
    })

    it('negates $nin operator', () => {
      const doc = { category: 'electronics' }
      // NOT not-in ['books', 'clothing'] = in ['books', 'clothing'] (electronics not in list, so false)
      expect(matchesFilter(doc, { category: { $not: { $nin: ['books', 'clothing'] } } })).toBe(false)
      // NOT not-in ['electronics', 'books'] = in ['electronics', 'books'] (electronics in list, so true)
      expect(matchesFilter(doc, { category: { $not: { $nin: ['electronics', 'books'] } } })).toBe(true)
    })
  })

  describe('with string operators', () => {
    it('negates $regex operator', () => {
      const doc = { name: 'admin_user' }
      expect(matchesFilter(doc, { name: { $not: { $regex: '^guest' } } })).toBe(true)
      expect(matchesFilter(doc, { name: { $not: { $regex: '^admin' } } })).toBe(false)
    })

    it('negates $regex with $options', () => {
      const doc = { name: 'Admin_User' }
      // Case-insensitive match
      expect(matchesFilter(doc, { name: { $not: { $regex: '^admin', $options: 'i' } } })).toBe(false)
      expect(matchesFilter(doc, { name: { $not: { $regex: '^guest', $options: 'i' } } })).toBe(true)
    })

    it('negates $regex with RegExp object', () => {
      const doc = { email: 'user@example.com' }
      expect(matchesFilter(doc, { email: { $not: { $regex: /@other\.com$/ } } })).toBe(true)
      expect(matchesFilter(doc, { email: { $not: { $regex: /@example\.com$/ } } })).toBe(false)
    })

    it('negates $startsWith operator', () => {
      const doc = { url: 'https://example.com' }
      expect(matchesFilter(doc, { url: { $not: { $startsWith: 'http://' } } })).toBe(true)
      expect(matchesFilter(doc, { url: { $not: { $startsWith: 'https://' } } })).toBe(false)
    })

    it('negates $endsWith operator', () => {
      const doc = { filename: 'document.pdf' }
      expect(matchesFilter(doc, { filename: { $not: { $endsWith: '.txt' } } })).toBe(true)
      expect(matchesFilter(doc, { filename: { $not: { $endsWith: '.pdf' } } })).toBe(false)
    })

    it('negates $contains operator', () => {
      const doc = { description: 'A powerful database engine' }
      expect(matchesFilter(doc, { description: { $not: { $contains: 'spreadsheet' } } })).toBe(true)
      expect(matchesFilter(doc, { description: { $not: { $contains: 'database' } } })).toBe(false)
    })
  })

  describe('with array operators', () => {
    it('negates $all operator', () => {
      const doc = { tags: ['javascript', 'typescript', 'nodejs'] }
      expect(matchesFilter(doc, { tags: { $not: { $all: ['python', 'ruby'] } } })).toBe(true)
      expect(matchesFilter(doc, { tags: { $not: { $all: ['javascript', 'typescript'] } } })).toBe(false)
    })

    it('negates $size operator', () => {
      const doc = { items: [1, 2, 3] }
      expect(matchesFilter(doc, { items: { $not: { $size: 5 } } })).toBe(true)
      expect(matchesFilter(doc, { items: { $not: { $size: 3 } } })).toBe(false)
    })

    it('negates $elemMatch operator', () => {
      const doc = {
        products: [
          { name: 'laptop', price: 999 },
          { name: 'mouse', price: 29 },
        ],
      }
      expect(matchesFilter(doc, { products: { $not: { $elemMatch: { price: { $gt: 1000 } } } } })).toBe(true)
      expect(matchesFilter(doc, { products: { $not: { $elemMatch: { price: { $lt: 100 } } } } })).toBe(false)
    })
  })

  describe('with existence operators', () => {
    it('negates $exists operator', () => {
      const doc = { name: 'test' }
      // NOT exists:true on 'name' -> exists:false behavior (name exists, so false)
      expect(matchesFilter(doc, { name: { $not: { $exists: true } } })).toBe(false)
      // NOT exists:false on 'name' -> exists:true behavior (name exists, so true)
      expect(matchesFilter(doc, { name: { $not: { $exists: false } } })).toBe(true)
      // NOT exists:true on 'missing' -> exists:false behavior (missing doesn't exist, so true)
      expect(matchesFilter(doc, { missing: { $not: { $exists: true } } })).toBe(true)
      // NOT exists:false on 'missing' -> exists:true behavior (missing doesn't exist, so false)
      expect(matchesFilter(doc, { missing: { $not: { $exists: false } } })).toBe(false)
    })

    it('negates $type operator', () => {
      const doc = { value: 42, name: 'test' }
      expect(matchesFilter(doc, { value: { $not: { $type: 'string' } } })).toBe(true)
      expect(matchesFilter(doc, { value: { $not: { $type: 'number' } } })).toBe(false)
      expect(matchesFilter(doc, { name: { $not: { $type: 'number' } } })).toBe(true)
      expect(matchesFilter(doc, { name: { $not: { $type: 'string' } } })).toBe(false)
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('field-level $not edge cases', () => {
  describe('null and undefined handling', () => {
    it('handles null field values', () => {
      const doc = { value: null }
      // null is NOT greater than 0 -> true (because null fails $gt comparison)
      expect(matchesFilter(doc, { value: { $not: { $gt: 0 } } })).toBe(true)
      // null is NOT equal to null -> false
      expect(matchesFilter(doc, { value: { $not: { $eq: null } } })).toBe(false)
    })

    it('handles undefined/missing field values', () => {
      const doc = { other: 'value' }
      // missing field is NOT greater than 0 -> true
      expect(matchesFilter(doc, { value: { $not: { $gt: 0 } } })).toBe(true)
      // missing field is NOT equal to null -> false (undefined == null for equality)
      expect(matchesFilter(doc, { value: { $not: { $eq: null } } })).toBe(false)
    })
  })

  describe('nested field paths', () => {
    it('works with dot notation paths', () => {
      const doc = { user: { profile: { age: 25 } } }
      expect(matchesFilter(doc, { 'user.profile.age': { $not: { $gte: 30 } } })).toBe(true)
      expect(matchesFilter(doc, { 'user.profile.age': { $not: { $gte: 20 } } })).toBe(false)
    })

    it('handles missing nested paths', () => {
      const doc = { user: { name: 'test' } }
      expect(matchesFilter(doc, { 'user.profile.age': { $not: { $gt: 0 } } })).toBe(true)
    })
  })

  describe('combined with other operators at field level', () => {
    it('should not allow mixing $not with other operators at same level', () => {
      // This tests the expected behavior - field-level $not should be the sole operator
      // when used, similar to MongoDB's behavior
      const doc = { score: 50 }
      // When $not is present with other operators, the behavior depends on implementation
      // MongoDB treats { $not: ..., $gt: ... } as invalid or ignores extra operators
      // Our implementation should evaluate $not and ignore non-$not operators at same level
      const filter = { score: { $not: { $gt: 75 }, $lt: 100 } } as Filter
      // If we evaluate both: score NOT > 75 AND score < 100
      // 50 NOT > 75 = true, 50 < 100 = true, so result is true
      expect(matchesFilter(doc, filter)).toBe(true)
    })
  })

  describe('empty operator object in $not', () => {
    it('handles empty $not object (should match everything negated)', () => {
      const doc = { value: 42 }
      // Empty operator object matches everything, so $not negates to false
      expect(matchesFilter(doc, { value: { $not: {} } })).toBe(false)
    })
  })
})

// =============================================================================
// Comparison with Top-Level $not
// =============================================================================

describe('field-level vs top-level $not', () => {
  it('field-level $not targets specific operator', () => {
    const doc = { score: 50, status: 'active' }

    // Field-level: only affects the specific operator on 'score'
    const fieldLevelFilter: Filter = { score: { $not: { $gt: 75 } } }
    expect(matchesFilter(doc, fieldLevelFilter)).toBe(true)

    // Can combine with other field conditions
    const combinedFilter: Filter = {
      score: { $not: { $gt: 75 } },
      status: 'active',
    }
    expect(matchesFilter(doc, combinedFilter)).toBe(true)
  })

  it('top-level $not negates entire sub-filter', () => {
    const doc = { score: 50, status: 'active' }

    // Top-level: negates the entire filter { score: { $gt: 75 } }
    const topLevelFilter: Filter = { $not: { score: { $gt: 75 } } }
    expect(matchesFilter(doc, topLevelFilter)).toBe(true)

    // Top-level negating multiple fields at once
    const topLevelMultiFilter: Filter = {
      $not: { score: { $gt: 75 }, status: 'inactive' },
    }
    expect(matchesFilter(doc, topLevelMultiFilter)).toBe(true)
  })

  it('produces equivalent results for single field conditions', () => {
    const doc = { score: 50 }

    // These should produce the same result
    const fieldLevel: Filter = { score: { $not: { $gt: 75 } } }
    const topLevel: Filter = { $not: { score: { $gt: 75 } } }

    expect(matchesFilter(doc, fieldLevel)).toBe(matchesFilter(doc, topLevel))
  })

  it('produces different results for multi-field conditions', () => {
    const doc = { a: 1, b: 2 }

    // Field-level on 'a', separate condition on 'b'
    const fieldLevel: Filter = { a: { $not: { $eq: 5 } }, b: 2 }
    // a NOT = 5 AND b = 2 -> true AND true = true
    expect(matchesFilter(doc, fieldLevel)).toBe(true)

    // Top-level negates both conditions together
    const topLevel: Filter = { $not: { a: 5, b: 2 } }
    // NOT (a = 5 AND b = 2) -> NOT (false AND true) = NOT false = true
    expect(matchesFilter(doc, topLevel)).toBe(true)

    // Different scenario where they differ
    const doc2 = { a: 5, b: 2 }
    const fieldLevel2: Filter = { a: { $not: { $eq: 5 } }, b: 2 }
    // a NOT = 5 AND b = 2 -> false AND true = false
    expect(matchesFilter(doc2, fieldLevel2)).toBe(false)

    const topLevel2: Filter = { $not: { a: 5, b: 2 } }
    // NOT (a = 5 AND b = 2) -> NOT (true AND true) = NOT true = false
    expect(matchesFilter(doc2, topLevel2)).toBe(false)
  })
})

// =============================================================================
// Integration with Logical Operators
// =============================================================================

describe('field-level $not with logical operators', () => {
  it('works inside $and', () => {
    const doc = { score: 50, category: 'tech' }
    const filter: Filter = {
      $and: [
        { score: { $not: { $lt: 25 } } },
        { category: 'tech' },
      ],
    }
    expect(matchesFilter(doc, filter)).toBe(true)
  })

  it('works inside $or', () => {
    const doc = { score: 50, category: 'tech' }
    const filter: Filter = {
      $or: [
        { score: { $not: { $gt: 100 } } },  // true
        { category: 'science' },             // false
      ],
    }
    expect(matchesFilter(doc, filter)).toBe(true)
  })

  it('works inside $nor', () => {
    const doc = { score: 50 }
    const filter: Filter = {
      $nor: [
        { score: { $not: { $gt: 100 } } },  // true (score NOT > 100)
      ],
    }
    // $nor requires ALL conditions to be false, but the condition is true
    expect(matchesFilter(doc, filter)).toBe(false)
  })

  it('can be combined with top-level $not', () => {
    const doc = { a: 50, b: 'active' }
    const filter: Filter = {
      $not: {
        a: { $not: { $gt: 100 } },  // a NOT > 100 = true
        b: 'active',                 // true
      },
    }
    // Inner: a NOT > 100 AND b = 'active' -> true AND true = true
    // Outer $not: NOT true = false
    expect(matchesFilter(doc, filter)).toBe(false)
  })
})

// =============================================================================
// Real-World Use Cases
// =============================================================================

describe('field-level $not real-world scenarios', () => {
  describe('username validation', () => {
    it('finds usernames that do not start with admin', () => {
      const users = [
        { username: 'admin_system', role: 'admin' },
        { username: 'user_john', role: 'user' },
        { username: 'admin_backup', role: 'admin' },
        { username: 'moderator_jane', role: 'mod' },
      ]

      const filter: Filter = { username: { $not: { $regex: '^admin' } } }
      const filtered = users.filter(u => matchesFilter(u, filter))
      expect(filtered.map(u => u.username)).toEqual(['user_john', 'moderator_jane'])
    })
  })

  describe('price range exclusion', () => {
    it('finds products not in expensive range', () => {
      const products = [
        { name: 'Basic', price: 10 },
        { name: 'Standard', price: 50 },
        { name: 'Premium', price: 100 },
        { name: 'Enterprise', price: 500 },
      ]

      const filter: Filter = { price: { $not: { $gte: 100 } } }
      const filtered = products.filter(p => matchesFilter(p, filter))
      expect(filtered.map(p => p.name)).toEqual(['Basic', 'Standard'])
    })
  })

  describe('tag exclusion', () => {
    it('finds posts that do not have all specified tags', () => {
      const posts = [
        { title: 'Post 1', tags: ['javascript', 'react', 'tutorial'] },
        { title: 'Post 2', tags: ['javascript', 'vue'] },
        { title: 'Post 3', tags: ['python', 'django'] },
        { title: 'Post 4', tags: ['javascript', 'react'] },
      ]

      // Find posts that do NOT have both 'javascript' AND 'react'
      const filter: Filter = { tags: { $not: { $all: ['javascript', 'react'] } } }
      const filtered = posts.filter(p => matchesFilter(p, filter))
      expect(filtered.map(p => p.title)).toEqual(['Post 2', 'Post 3'])
    })
  })

  describe('email domain filtering', () => {
    it('finds users with non-internal email addresses', () => {
      const users = [
        { name: 'Alice', email: 'alice@company.internal' },
        { name: 'Bob', email: 'bob@gmail.com' },
        { name: 'Charlie', email: 'charlie@company.internal' },
        { name: 'Diana', email: 'diana@outlook.com' },
      ]

      const filter: Filter = { email: { $not: { $endsWith: '@company.internal' } } }
      const filtered = users.filter(u => matchesFilter(u, filter))
      expect(filtered.map(u => u.name)).toEqual(['Bob', 'Diana'])
    })
  })
})

// =============================================================================
// createPredicate with Field-Level $not
// =============================================================================

describe('createPredicate with field-level $not', () => {
  it('creates reusable predicate with field-level $not', () => {
    const isNotExpensive = createPredicate({ price: { $not: { $gte: 100 } } })

    expect(isNotExpensive({ price: 50 })).toBe(true)
    expect(isNotExpensive({ price: 100 })).toBe(false)
    expect(isNotExpensive({ price: 150 })).toBe(false)
  })

  it('creates complex predicate combining field-level $not', () => {
    const isAccessible = createPredicate({
      status: 'published',
      visibility: { $not: { $in: ['private', 'draft'] } },
      'author.banned': { $not: { $eq: true } },
    })

    expect(isAccessible({
      status: 'published',
      visibility: 'public',
      author: { banned: false },
    })).toBe(true)

    expect(isAccessible({
      status: 'published',
      visibility: 'private',
      author: { banned: false },
    })).toBe(false)

    expect(isAccessible({
      status: 'published',
      visibility: 'public',
      author: { banned: true },
    })).toBe(false)
  })
})
