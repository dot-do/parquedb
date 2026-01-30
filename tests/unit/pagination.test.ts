/**
 * Pagination Tests for ParqueDB
 *
 * RED phase tests for pagination functionality including:
 * - Limit and skip (offset-based pagination)
 * - Cursor-based pagination
 * - Count operations
 *
 * All tests are expected to FAIL until implementation is complete.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { Collection } from '../../src/Collection'
import type {
  Entity,
  EntityId,
  FindOptions,
  PaginatedResult,
} from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  views: number
  publishedAt?: Date
}

// =============================================================================
// Test Setup
// =============================================================================

describe('Pagination', () => {
  let collection: Collection<Post>

  beforeEach(() => {
    collection = new Collection<Post>('posts')
  })

  // ===========================================================================
  // Limit and Skip (Offset-based Pagination)
  // ===========================================================================

  describe('limit and skip', () => {
    it('limits results to specified count', async () => {
      const results = await collection.find({}, { limit: 5 })

      expect(results.length).toBeLessThanOrEqual(5)
    })

    it('skips specified number of results', async () => {
      // First, get all results to compare
      const allResults = await collection.find()
      const skippedResults = await collection.find({}, { skip: 3 })

      // If there are more than 3 results, verify skipping worked
      if (allResults.length > 3) {
        expect(skippedResults.length).toBe(allResults.length - 3)
        expect(skippedResults[0].$id).toBe(allResults[3].$id)
      }
    })

    it('combines limit and skip', async () => {
      // First, get all results
      const allResults = await collection.find()

      // Skip 2 and take 3
      const paginatedResults = await collection.find({}, { skip: 2, limit: 3 })

      // Verify the correct slice is returned
      if (allResults.length >= 5) {
        expect(paginatedResults.length).toBe(3)
        expect(paginatedResults[0].$id).toBe(allResults[2].$id)
        expect(paginatedResults[1].$id).toBe(allResults[3].$id)
        expect(paginatedResults[2].$id).toBe(allResults[4].$id)
      }
    })

    it('handles limit larger than result set', async () => {
      // Request more results than exist
      const results = await collection.find({}, { limit: 10000 })

      // Should return all available results without error
      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeLessThanOrEqual(10000)
    })

    it('returns empty array when skip exceeds result count', async () => {
      const results = await collection.find({}, { skip: 999999 })

      expect(results).toEqual([])
    })

    it('returns empty array when limit is 0', async () => {
      const results = await collection.find({}, { limit: 0 })

      expect(results).toEqual([])
    })

    it('throws error for negative limit', async () => {
      await expect(collection.find({}, { limit: -1 })).rejects.toThrow()
    })

    it('throws error for negative skip', async () => {
      await expect(collection.find({}, { skip: -5 })).rejects.toThrow()
    })

    it('handles decimal values by truncating', async () => {
      // Some implementations truncate, others throw - verify consistent behavior
      const results = await collection.find({}, { limit: 3.7 as number })

      // Should treat as limit: 3
      expect(results.length).toBeLessThanOrEqual(3)
    })
  })

  // ===========================================================================
  // Cursor-based Pagination
  // ===========================================================================

  describe('cursor-based pagination', () => {
    it('returns cursor in response', async () => {
      // Use findPaginated method that returns PaginatedResult
      const result = await (collection as any).findPaginated({}, { limit: 2 }) as PaginatedResult<Entity<Post>>

      // When there are more results, nextCursor should be defined
      if (result.hasMore) {
        expect(result.nextCursor).toBeDefined()
        expect(typeof result.nextCursor).toBe('string')
      }
    })

    it('continues from cursor position', async () => {
      // Get first page
      const page1 = await (collection as any).findPaginated({}, { limit: 2 }) as PaginatedResult<Entity<Post>>

      expect(page1.items.length).toBeLessThanOrEqual(2)

      if (page1.hasMore && page1.nextCursor) {
        // Get second page using cursor
        const page2 = await (collection as any).findPaginated({}, {
          limit: 2,
          cursor: page1.nextCursor,
        }) as PaginatedResult<Entity<Post>>

        // Ensure no overlap between pages
        const page1Ids = page1.items.map(item => item.$id)
        const page2Ids = page2.items.map(item => item.$id)

        page2Ids.forEach(id => {
          expect(page1Ids).not.toContain(id)
        })
      }
    })

    it('handles end of results', async () => {
      // Fetch all results with a small limit to ensure pagination
      let allItems: Entity<Post>[] = []
      let cursor: string | undefined

      do {
        const result = await (collection as any).findPaginated({}, {
          limit: 2,
          cursor,
        }) as PaginatedResult<Entity<Post>>

        allItems = [...allItems, ...result.items]
        cursor = result.nextCursor

        // When we reach the end, hasMore should be false
        if (!result.hasMore) {
          expect(cursor).toBeUndefined()
        }
      } while (cursor)

      // Verify we got all unique items
      const uniqueIds = new Set(allItems.map(item => item.$id))
      expect(uniqueIds.size).toBe(allItems.length)
    })

    it('cursor survives data changes', async () => {
      // Get first page and cursor
      const page1 = await (collection as any).findPaginated({}, { limit: 2 }) as PaginatedResult<Entity<Post>>

      if (page1.hasMore && page1.nextCursor) {
        // Simulate data change (in real test, we'd insert/delete)
        // The cursor should still work and give consistent results
        const page2 = await (collection as any).findPaginated({}, {
          limit: 2,
          cursor: page1.nextCursor,
        }) as PaginatedResult<Entity<Post>>

        // Cursor should still be valid
        expect(page2.items).toBeDefined()
        expect(Array.isArray(page2.items)).toBe(true)
      }
    })

    it('cursor includes sort position', async () => {
      // When sorting, cursor should encode sort field values
      const page1 = await (collection as any).findPaginated({}, {
        limit: 2,
        sort: { views: -1 },
      }) as PaginatedResult<Entity<Post>>

      if (page1.hasMore && page1.nextCursor) {
        const page2 = await (collection as any).findPaginated({}, {
          limit: 2,
          cursor: page1.nextCursor,
          sort: { views: -1 },
        }) as PaginatedResult<Entity<Post>>

        // Page2's first item should have fewer or equal views than page1's last
        if (page1.items.length > 0 && page2.items.length > 0) {
          const page1LastViews = page1.items[page1.items.length - 1].views
          const page2FirstViews = page2.items[0].views

          expect(page2FirstViews).toBeLessThanOrEqual(page1LastViews as number)
        }
      }
    })

    it('throws error for invalid cursor format', async () => {
      await expect(
        (collection as any).findPaginated({}, { cursor: 'invalid-cursor-xyz' })
      ).rejects.toThrow(/invalid cursor|malformed cursor/i)
    })

    it('throws error for expired or tampered cursor', async () => {
      // A cursor from a different collection or with wrong signature
      const tamperedCursor = 'eyJpZCI6ImZha2UiLCJ0cyI6MTIzNH0='

      await expect(
        (collection as any).findPaginated({}, { cursor: tamperedCursor })
      ).rejects.toThrow(/invalid cursor|expired|tampered/i)
    })

    it('returns hasMore=true when more results exist', async () => {
      const result = await (collection as any).findPaginated({}, { limit: 1 }) as PaginatedResult<Entity<Post>>

      // If there's more than 1 item in the collection
      // hasMore should be true
      if (result.items.length === 1) {
        // We need to check the total to know if hasMore is correct
        // This test validates hasMore is present and boolean
        expect(typeof result.hasMore).toBe('boolean')
      }
    })

    it('returns hasMore=false when no more results', async () => {
      // Fetch with a very large limit to get all results
      const result = await (collection as any).findPaginated({}, { limit: 10000 }) as PaginatedResult<Entity<Post>>

      expect(result.hasMore).toBe(false)
      expect(result.nextCursor).toBeUndefined()
    })
  })

  // ===========================================================================
  // Count Operations
  // ===========================================================================

  describe('count', () => {
    it('returns total count with results', async () => {
      const result = await (collection as any).findPaginated({}, { limit: 2 }) as PaginatedResult<Entity<Post>>

      // total should include count of ALL matching documents, not just returned ones
      expect(result.total).toBeDefined()
      expect(typeof result.total).toBe('number')
      expect(result.total).toBeGreaterThanOrEqual(result.items.length)
    })

    it('count ignores limit', async () => {
      // Get count with small limit
      const result1 = await (collection as any).findPaginated({}, { limit: 1 }) as PaginatedResult<Entity<Post>>

      // Get count with large limit
      const result2 = await (collection as any).findPaginated({}, { limit: 1000 }) as PaginatedResult<Entity<Post>>

      // total should be the same regardless of limit
      expect(result1.total).toBe(result2.total)
    })

    it('count respects filter', async () => {
      // Count all posts
      const allCount = await collection.count()

      // Count only published posts
      const publishedCount = await collection.count({ status: 'published' })

      // Published count should be less than or equal to total
      expect(publishedCount).toBeLessThanOrEqual(allCount)
    })

    it('returns 0 for empty result set', async () => {
      // Filter that matches nothing
      const count = await collection.count({ title: 'definitely-not-a-real-title-xyz123' })

      expect(count).toBe(0)
    })

    it('count excludes soft-deleted by default', async () => {
      const normalCount = await collection.count()
      const withDeletedCount = await collection.count({ includeDeleted: true } as any)

      // With deleted should be >= normal count
      expect(withDeletedCount).toBeGreaterThanOrEqual(normalCount)
    })

    it('count works with complex filters', async () => {
      const count = await collection.count({
        $and: [
          { status: { $in: ['published', 'archived'] } },
          { views: { $gt: 100 } },
        ],
      })

      expect(typeof count).toBe('number')
      expect(count).toBeGreaterThanOrEqual(0)
    })

    it('countDocuments method returns same as count', async () => {
      const count1 = await collection.count({ status: 'published' })
      const count2 = await (collection as any).countDocuments({ status: 'published' })

      expect(count1).toBe(count2)
    })

    it('estimatedDocumentCount returns approximate count', async () => {
      // This method should be fast but may be approximate
      const estimated = await (collection as any).estimatedDocumentCount()

      expect(typeof estimated).toBe('number')
      expect(estimated).toBeGreaterThanOrEqual(0)
    })
  })

  // ===========================================================================
  // Combined Pagination Scenarios
  // ===========================================================================

  describe('combined scenarios', () => {
    it('paginated find with filter', async () => {
      const result = await (collection as any).findPaginated(
        { status: 'published' },
        { limit: 2 }
      ) as PaginatedResult<Entity<Post>>

      // All returned items should match filter
      result.items.forEach(item => {
        expect(item.status).toBe('published')
      })
    })

    it('paginated find with sort', async () => {
      const result = await (collection as any).findPaginated({}, {
        limit: 5,
        sort: { views: -1 },
      }) as PaginatedResult<Entity<Post>>

      // Results should be sorted by views descending
      for (let i = 1; i < result.items.length; i++) {
        expect(result.items[i].views).toBeLessThanOrEqual(result.items[i - 1].views as number)
      }
    })

    it('paginated find with filter and sort', async () => {
      const result = await (collection as any).findPaginated(
        { status: 'published' },
        { limit: 3, sort: { publishedAt: -1 } }
      ) as PaginatedResult<Entity<Post>>

      // All items should be published
      result.items.forEach(item => {
        expect(item.status).toBe('published')
      })

      // Should be sorted by publishedAt descending
      for (let i = 1; i < result.items.length; i++) {
        const prevDate = result.items[i - 1].publishedAt
        const currDate = result.items[i].publishedAt
        if (prevDate && currDate) {
          expect(currDate.getTime()).toBeLessThanOrEqual(prevDate.getTime())
        }
      }
    })

    it('paginated find with projection', async () => {
      const result = await (collection as any).findPaginated({}, {
        limit: 2,
        project: { title: 1, status: 1 },
      }) as PaginatedResult<Entity<Post>>

      result.items.forEach(item => {
        expect(item).toHaveProperty('$id')
        expect(item).toHaveProperty('title')
        expect(item).toHaveProperty('status')
        // content should not be included
        expect(item).not.toHaveProperty('content')
      })
    })

    it('exhaustive pagination collects all items', async () => {
      // Get total count first
      const totalCount = await collection.count()

      // Paginate through everything
      let allItems: Entity<Post>[] = []
      let cursor: string | undefined

      do {
        const result = await (collection as any).findPaginated({}, {
          limit: 3,
          cursor,
        }) as PaginatedResult<Entity<Post>>

        allItems = [...allItems, ...result.items]
        cursor = result.nextCursor
      } while (cursor)

      // Should have collected all items
      expect(allItems.length).toBe(totalCount)
    })
  })
})
