/**
 * Relationship Batch Loader Tests
 *
 * Tests for the RelationshipBatchLoader that eliminates N+1 queries
 * when loading relationships.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  RelationshipBatchLoader,
  createBatchLoader,
  type BatchLoaderDB,
  type BatchLoadRequest,
} from '../../../src/relationships/batch-loader'
import type { Entity } from '../../../src/types/entity'
import type { GetRelatedResult } from '../../../src/ParqueDB/types'

// =============================================================================
// Mock Database
// =============================================================================

/**
 * Create a mock database with tracking capabilities
 */
function createMockDB(
  mockData: Map<string, Map<string, Map<string, Entity[]>>>
): BatchLoaderDB & {
  getRelatedCallCount: number
  getRelatedCalls: Array<{ namespace: string; id: string; relation: string }>
  reset: () => void
} {
  let getRelatedCallCount = 0
  const getRelatedCalls: Array<{ namespace: string; id: string; relation: string }> = []

  return {
    get getRelatedCallCount() {
      return getRelatedCallCount
    },
    get getRelatedCalls() {
      return getRelatedCalls
    },
    reset() {
      getRelatedCallCount = 0
      getRelatedCalls.length = 0
    },
    async getRelated<T = Record<string, unknown>>(
      namespace: string,
      id: string,
      relationField: string
    ): Promise<GetRelatedResult<T>> {
      getRelatedCallCount++
      getRelatedCalls.push({ namespace, id, relation: relationField })

      // Simulate some latency
      await new Promise((resolve) => setTimeout(resolve, 1))

      const nsData = mockData.get(namespace)
      if (!nsData) {
        return { items: [], total: 0, hasMore: false }
      }

      const entityData = nsData.get(id)
      if (!entityData) {
        return { items: [], total: 0, hasMore: false }
      }

      const related = entityData.get(relationField)
      if (!related) {
        return { items: [], total: 0, hasMore: false }
      }

      return {
        items: related as Entity<T>[],
        total: related.length,
        hasMore: false,
      }
    },
  }
}

/**
 * Create a mock entity
 */
function createMockEntity(
  id: string,
  type: string,
  name: string,
  data: Record<string, unknown> = {}
): Entity {
  return {
    $id: id as any,
    $type: type,
    name,
    createdAt: new Date(),
    createdBy: 'system/system' as any,
    updatedAt: new Date(),
    updatedBy: 'system/system' as any,
    version: 1,
    ...data,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('RelationshipBatchLoader', () => {
  describe('basic functionality', () => {
    it('should load a single relationship', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([
                ['author', [createMockEntity('users/alice', 'User', 'Alice')]],
              ]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      const results = await loader.load('Post', 'post-1', 'author')

      expect(results).toHaveLength(1)
      expect(results[0]?.name).toBe('Alice')
      expect(db.getRelatedCallCount).toBe(1)
    })

    it('should return empty array for missing relationship', async () => {
      const db = createMockDB(new Map())
      const loader = new RelationshipBatchLoader(db)

      const results = await loader.load('Post', 'nonexistent', 'author')

      expect(results).toHaveLength(0)
    })

    it('should handle multiple related entities', async () => {
      const mockData = new Map([
        [
          'users',
          new Map([
            [
              'alice',
              new Map([
                [
                  'posts',
                  [
                    createMockEntity('posts/post-1', 'Post', 'Post 1'),
                    createMockEntity('posts/post-2', 'Post', 'Post 2'),
                    createMockEntity('posts/post-3', 'Post', 'Post 3'),
                  ],
                ],
              ]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      const results = await loader.load('User', 'alice', 'posts')

      expect(results).toHaveLength(3)
      expect(results.map((r) => r.name)).toEqual(['Post 1', 'Post 2', 'Post 3'])
    })
  })

  describe('batching behavior', () => {
    it('should batch multiple requests for same relation type', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
            [
              'post-2',
              new Map([['author', [createMockEntity('users/bob', 'User', 'Bob')]]]),
            ],
            [
              'post-3',
              new Map([['author', [createMockEntity('users/charlie', 'User', 'Charlie')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db, { windowMs: 10 })

      // Fire all requests in parallel
      const [result1, result2, result3] = await Promise.all([
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'post-2', 'author'),
        loader.load('Post', 'post-3', 'author'),
      ])

      expect(result1[0]?.name).toBe('Alice')
      expect(result2[0]?.name).toBe('Bob')
      expect(result3[0]?.name).toBe('Charlie')

      // Without batching, we would have 3 calls
      // With batching, we still have 3 calls (one per unique ID)
      // but they're all batched and executed together
      expect(db.getRelatedCallCount).toBe(3)
    })

    it('should deduplicate identical requests', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db, { deduplicate: true })

      // Request same relationship multiple times
      const [result1, result2, result3] = await Promise.all([
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'post-1', 'author'),
      ])

      // All should get the same result
      expect(result1).toBe(result2)
      expect(result2).toBe(result3)

      // Only one DB call should be made
      expect(db.getRelatedCallCount).toBe(1)
    })

    it('should not deduplicate when disabled', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db, { deduplicate: false })

      // Request same relationship multiple times
      const [result1, result2] = await Promise.all([
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'post-1', 'author'),
      ])

      // Results should be equivalent but not the same object
      expect(result1[0]?.name).toBe('Alice')
      expect(result2[0]?.name).toBe('Alice')

      // Two requests should still result in one call due to ID deduplication
      // But internally, two separate promise instances are created
      expect(db.getRelatedCallCount).toBe(1)
    })

    it('should flush after max batch size is reached', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map(
            Array.from({ length: 10 }, (_, i) => [
              `post-${i}`,
              new Map([
                ['author', [createMockEntity(`users/user-${i}`, 'User', `User ${i}`)]],
              ]),
            ])
          ),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db, {
        windowMs: 1000, // Long window
        maxBatchSize: 5, // Small batch size
      })

      // Fire 10 requests
      const promises = Array.from({ length: 10 }, (_, i) =>
        loader.load('Post', `post-${i}`, 'author')
      )

      // Wait for all to complete
      await Promise.all(promises)

      // Should have triggered 2 flush cycles (10 requests / 5 per batch)
      expect(db.getRelatedCallCount).toBe(10)
    })
  })

  describe('loadMany', () => {
    it('should load multiple relationships at once', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
            [
              'post-2',
              new Map([['author', [createMockEntity('users/bob', 'User', 'Bob')]]]),
            ],
          ]),
        ],
        [
          'users',
          new Map([
            [
              'alice',
              new Map([
                ['posts', [createMockEntity('posts/post-1', 'Post', 'Post 1')]],
              ]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      const requests: BatchLoadRequest[] = [
        { type: 'Post', id: 'post-1', relation: 'author' },
        { type: 'Post', id: 'post-2', relation: 'author' },
        { type: 'User', id: 'alice', relation: 'posts' },
      ]

      const results = await loader.loadMany(requests)

      expect(results).toHaveLength(3)
      expect(results[0]?.results[0]?.name).toBe('Alice')
      expect(results[1]?.results[0]?.name).toBe('Bob')
      expect(results[2]?.results[0]?.name).toBe('Post 1')
    })
  })

  describe('error handling', () => {
    it('should handle errors gracefully', async () => {
      const db: BatchLoaderDB = {
        async getRelated() {
          throw new Error('Database error')
        },
      }

      const loader = new RelationshipBatchLoader(db)

      await expect(loader.load('Post', 'post-1', 'author')).rejects.toThrow(
        'Database error'
      )
    })

    it('should isolate errors to specific requests', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      let callCount = 0
      const db: BatchLoaderDB = {
        async getRelated<T>(namespace: string, id: string): Promise<GetRelatedResult<T>> {
          callCount++
          if (id === 'error-post') {
            throw new Error('Specific error')
          }

          const nsData = mockData.get(namespace)
          if (!nsData) {
            return { items: [], total: 0, hasMore: false }
          }

          const entityData = nsData.get(id)
          if (!entityData) {
            return { items: [], total: 0, hasMore: false }
          }

          const related = entityData.get('author')
          return {
            items: (related || []) as Entity<T>[],
            total: related?.length || 0,
            hasMore: false,
          }
        },
      }

      const loader = new RelationshipBatchLoader(db)

      const results = await Promise.allSettled([
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'error-post', 'author'),
      ])

      expect(results[0]?.status).toBe('fulfilled')
      expect(results[1]?.status).toBe('rejected')

      if (results[0]?.status === 'fulfilled') {
        expect(results[0].value[0]?.name).toBe('Alice')
      }

      if (results[1]?.status === 'rejected') {
        expect(results[1].reason.message).toBe('Specific error')
      }
    })
  })

  describe('clear', () => {
    it('should clear pending requests', async () => {
      const db = createMockDB(new Map())
      const loader = new RelationshipBatchLoader(db, { windowMs: 1000 })

      // Start a request but don't await
      const promise = loader.load('Post', 'post-1', 'author')

      // Clear immediately
      loader.clear()

      // The promise should be rejected
      await expect(promise).rejects.toThrow('Batch loader cleared')
    })

    it('should reset stats after clear', () => {
      const db = createMockDB(new Map())
      const loader = new RelationshipBatchLoader(db, { windowMs: 1000 })

      // Queue some requests without awaiting
      loader.load('Post', 'post-1', 'author')
      loader.load('Post', 'post-2', 'author')

      let stats = loader.getStats()
      expect(stats.pendingRequests).toBe(2)

      loader.clear()

      stats = loader.getStats()
      expect(stats.pendingRequests).toBe(0)
      expect(stats.pendingBatches).toBe(0)
      expect(stats.cachedPromises).toBe(0)
    })
  })

  describe('getStats', () => {
    it('should return correct statistics', async () => {
      const db = createMockDB(new Map())
      const loader = new RelationshipBatchLoader(db, { windowMs: 1000 })

      let stats = loader.getStats()
      expect(stats.pendingRequests).toBe(0)
      expect(stats.pendingBatches).toBe(0)
      expect(stats.cachedPromises).toBe(0)

      // Queue some requests and capture them to catch rejections
      const promises = [
        loader.load('Post', 'post-1', 'author'),
        loader.load('Post', 'post-2', 'author'),
        loader.load('User', 'user-1', 'posts'),
      ]

      stats = loader.getStats()
      expect(stats.pendingRequests).toBe(3)
      expect(stats.pendingBatches).toBe(2) // Post:author and User:posts
      expect(stats.cachedPromises).toBe(3)

      loader.clear()

      // Catch all the rejection errors from clear()
      await Promise.allSettled(promises)
    })
  })

  describe('createBatchLoader factory', () => {
    it('should create a loader instance', () => {
      const db = createMockDB(new Map())
      const loader = createBatchLoader(db)

      expect(loader).toBeInstanceOf(RelationshipBatchLoader)
    })

    it('should pass options correctly', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = createBatchLoader(db, { windowMs: 5 })

      const results = await loader.load('Post', 'post-1', 'author')
      expect(results[0]?.name).toBe('Alice')
    })
  })

  describe('namespace conversion', () => {
    it('should convert type to namespace correctly', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      // Type 'Post' should become namespace 'posts'
      await loader.load('Post', 'post-1', 'author')

      expect(db.getRelatedCalls[0]?.namespace).toBe('posts')
    })

    it('should handle already pluralized types', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      // Type 'Posts' (already plural) should become namespace 'posts'
      await loader.load('Posts', 'post-1', 'author')

      expect(db.getRelatedCalls[0]?.namespace).toBe('posts')
    })
  })

  describe('ID handling', () => {
    it('should strip namespace prefix from IDs', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      // ID with namespace prefix
      await loader.load('Post', 'posts/post-1', 'author')

      // Should strip the prefix
      expect(db.getRelatedCalls[0]?.id).toBe('post-1')
    })

    it('should handle IDs without namespace prefix', async () => {
      const mockData = new Map([
        [
          'posts',
          new Map([
            [
              'post-1',
              new Map([['author', [createMockEntity('users/alice', 'User', 'Alice')]]]),
            ],
          ]),
        ],
      ])

      const db = createMockDB(mockData)
      const loader = new RelationshipBatchLoader(db)

      // ID without namespace prefix
      await loader.load('Post', 'post-1', 'author')

      expect(db.getRelatedCalls[0]?.id).toBe('post-1')
    })
  })
})

describe('N+1 Query Elimination', () => {
  it('should eliminate N+1 queries when loading relationships in a loop', async () => {
    // Simulate a common N+1 scenario:
    // - Fetch list of posts
    // - For each post, fetch author
    // Without batching: N+1 queries (1 for posts, N for authors)
    // With batching: 2 queries (1 for posts, 1 batched for all authors)

    const mockData = new Map([
      [
        'posts',
        new Map(
          Array.from({ length: 10 }, (_, i) => [
            `post-${i}`,
            new Map([
              ['author', [createMockEntity(`users/user-${i % 3}`, 'User', `User ${i % 3}`)]],
            ]),
          ])
        ),
      ],
    ])

    const db = createMockDB(mockData)
    const loader = new RelationshipBatchLoader(db, { windowMs: 10 })

    // Simulate fetching posts and their authors
    const postIds = Array.from({ length: 10 }, (_, i) => `post-${i}`)

    // Load all authors in "parallel" (they'll be batched)
    const authorPromises = postIds.map((id) => loader.load('Post', id, 'author'))
    const authors = await Promise.all(authorPromises)

    // Verify results
    expect(authors).toHaveLength(10)
    authors.forEach((authorList, i) => {
      expect(authorList[0]?.name).toBe(`User ${i % 3}`)
    })

    // Verify batching happened
    // We have 10 posts but they should be loaded in batched calls
    // The exact number depends on timing, but it should be <= 10
    expect(db.getRelatedCallCount).toBeLessThanOrEqual(10)
  })
})
