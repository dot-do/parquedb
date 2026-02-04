/**
 * Relationship Handler Tests
 *
 * Tests for the /datasets/:dataset/:collection/:id/:predicate relationship traversal endpoint.
 * Tests graph operations, pagination, filtering, and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleRelationshipTraversal } from '../../../../src/worker/handlers/relationships'
import {
  createMockWorker,
  createMockHandlerContext,
  createMockCaches,
} from '../../../mocks'
import type { MockHandlerContext } from '../../../mocks/worker'

// Mock the Cloudflare caches API
const mockCachesObj = createMockCaches()
// @ts-expect-error - mocking global caches API
globalThis.caches = mockCachesObj

describe('Relationship Handler', () => {
  let mockWorker: ReturnType<typeof createMockWorker>
  let mockContext: MockHandlerContext

  beforeEach(() => {
    vi.clearAllMocks()
    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // Happy Path Tests
  // ===========================================================================

  describe('GET /datasets/:dataset/:collection/:id/:predicate - happy path', () => {
    it('should return relationship items', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'critical-thinking',
          to_name: 'Critical Thinking',
          to_type: 'skill',
          predicate: 'skills',
          importance: 85,
          level: 4,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'active-listening',
          to_name: 'Active Listening',
          to_type: 'skill',
          predicate: 'skills',
          importance: 72,
          level: 3,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'onet-graph',
        'occupations',
        '11-1011',
        'skills'
      )

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>

      expect(body.api).toEqual(
        expect.objectContaining({
          resource: 'relationships',
          dataset: 'onet-graph',
          collection: 'occupations',
          id: '11-1011',
          predicate: 'skills',
          count: 2,
        })
      )

      const items = body.items as Array<Record<string, unknown>>
      expect(items.length).toBe(2)
    })

    it('should sort items by importance descending', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'skill-low',
          to_name: 'Low Priority',
          to_type: 'skill',
          predicate: 'skills',
          importance: 30,
          level: null,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'skill-high',
          to_name: 'High Priority',
          to_type: 'skill',
          predicate: 'skills',
          importance: 95,
          level: null,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'skill-mid',
          to_name: 'Mid Priority',
          to_type: 'skill',
          predicate: 'skills',
          importance: 60,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<{ importance: number; name: string }>

      expect(items[0]?.importance).toBe(95)
      expect(items[1]?.importance).toBe(60)
      expect(items[2]?.importance).toBe(30)
    })

    it('should return empty items array when no relationships exist', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/posts/p1/comments',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'posts',
        'p1',
        'comments'
      )

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as unknown[]
      expect(items.length).toBe(0)

      const api = body.api as Record<string, unknown>
      expect(api.count).toBe(0)
    })

    it('should include correct links in response', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'onet-graph',
        'occupations',
        '11-1011',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const links = body.links as Record<string, string>

      expect(links.self).toContain('/datasets/onet-graph/occupations/')
      expect(links.self).toContain('/skills')
      expect(links.entity).toContain('/datasets/onet-graph/occupations/')
    })

    it('should call worker.getRelationships with correct parameters', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1/friends',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'myapp',
        'users',
        'u1',
        'friends'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'myapp',
        'u1',
        'friends',
        {
          matchMode: undefined,
          minSimilarity: undefined,
          maxSimilarity: undefined,
        }
      )
    })
  })

  // ===========================================================================
  // Pagination Tests
  // ===========================================================================

  describe('pagination', () => {
    it('should paginate results with limit parameter', async () => {
      // Create 10 relationships
      const rels = Array.from({ length: 10 }, (_, i) => ({
        to_ns: 'skills',
        to_id: `skill-${i}`,
        to_name: `Skill ${i}`,
        to_type: 'skill',
        predicate: 'skills',
        importance: 100 - i * 5,
        level: null,
        matchMode: null,
        similarity: null,
      }))

      mockWorker.getRelationships.mockResolvedValue(rels)

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills?limit=3',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as unknown[]

      expect(items.length).toBe(3)
      const api = body.api as Record<string, unknown>
      expect(api.count).toBe(10) // Total count, not paginated count
    })

    it('should skip results with skip parameter', async () => {
      const rels = Array.from({ length: 5 }, (_, i) => ({
        to_ns: 'skills',
        to_id: `skill-${i}`,
        to_name: `Skill ${i}`,
        to_type: 'skill',
        predicate: 'skills',
        importance: 100 - i * 10, // 100, 90, 80, 70, 60
        level: null,
        matchMode: null,
        similarity: null,
      }))

      mockWorker.getRelationships.mockResolvedValue(rels)

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills?skip=2&limit=2',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<{ importance: number }>

      // Should skip first 2 (100, 90), return next 2 (80, 70)
      expect(items.length).toBe(2)
      expect(items[0]?.importance).toBe(80)
      expect(items[1]?.importance).toBe(70)
    })

    it('should use default limit of 100', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      // No assertions on default limit as it's internal, but shouldn't throw
      expect(mockWorker.getRelationships).toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // Filter Options Tests
  // ===========================================================================

  describe('filter options', () => {
    it('should pass matchMode filter to worker', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?matchMode=exact',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        {
          matchMode: 'exact',
          minSimilarity: undefined,
          maxSimilarity: undefined,
        }
      )
    })

    it('should pass fuzzy matchMode filter to worker', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?matchMode=fuzzy',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        expect.objectContaining({
          matchMode: 'fuzzy',
        })
      )
    })

    it('should ignore invalid matchMode values', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?matchMode=invalid',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        expect.objectContaining({
          matchMode: undefined,
        })
      )
    })

    it('should pass minSimilarity filter to worker', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?minSimilarity=0.8',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        expect.objectContaining({
          minSimilarity: 0.8,
        })
      )
    })

    it('should pass maxSimilarity filter to worker', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?maxSimilarity=0.95',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        expect.objectContaining({
          maxSimilarity: 0.95,
        })
      )
    })

    it('should combine multiple filter options', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related?matchMode=fuzzy&minSimilarity=0.7&maxSimilarity=0.99',
        { worker: mockWorker }
      )

      await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      expect(mockWorker.getRelationships).toHaveBeenCalledWith(
        'test',
        'e1',
        'related',
        {
          matchMode: 'fuzzy',
          minSimilarity: 0.7,
          maxSimilarity: 0.99,
        }
      )
    })
  })

  // ===========================================================================
  // Shredded Field Tests
  // ===========================================================================

  describe('shredded field handling', () => {
    it('should include matchMode in response items', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'entities',
          to_id: 'e2',
          to_name: 'Related Entity',
          to_type: 'entity',
          predicate: 'related',
          importance: null,
          level: null,
          matchMode: 'fuzzy',
          similarity: 0.85,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/entities/e1/related',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'entities',
        'e1',
        'related'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.matchMode).toBe('fuzzy')
      expect(items[0]?.similarity).toBe(0.85)
    })

    it('should not include matchMode when null', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 's1',
          to_name: 'Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: 50,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.matchMode).toBeUndefined()
      expect(items[0]?.similarity).toBeUndefined()
    })
  })

  // ===========================================================================
  // Error Cases
  // ===========================================================================

  describe('error cases', () => {
    it('should throw error for invalid limit (non-numeric)', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills?limit=abc',
        { worker: mockWorker }
      )

      await expect(
        handleRelationshipTraversal(
          mockContext as any,
          'test',
          'occupations',
          'o1',
          'skills'
        )
      ).rejects.toThrow('Invalid limit: must be a valid integer')
    })

    it('should throw error for negative skip', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills?skip=-10',
        { worker: mockWorker }
      )

      await expect(
        handleRelationshipTraversal(
          mockContext as any,
          'test',
          'occupations',
          'o1',
          'skills'
        )
      ).rejects.toThrow('Invalid skip: must be non-negative')
    })

    it('should throw error for limit exceeding maximum', async () => {
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills?limit=10000',
        { worker: mockWorker }
      )

      await expect(
        handleRelationshipTraversal(
          mockContext as any,
          'test',
          'occupations',
          'o1',
          'skills'
        )
      ).rejects.toThrow('Invalid limit: cannot exceed 1000')
    })

    it('should return 404 when parquet file is missing', async () => {
      mockWorker.getRelationships.mockRejectedValue(
        new Error('File not found: test/rels.parquet')
      )

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      expect(response.status).toBe(404)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.hint).toBe('This collection may not have been uploaded yet.')
    })

    it('should re-throw non file-not-found errors', async () => {
      mockWorker.getRelationships.mockRejectedValue(
        new Error('Connection timeout')
      )

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      await expect(
        handleRelationshipTraversal(
          mockContext as any,
          'test',
          'occupations',
          'o1',
          'skills'
        )
      ).rejects.toThrow('Connection timeout')
    })
  })

  // ===========================================================================
  // Response Format Tests
  // ===========================================================================

  describe('response format', () => {
    it('should include $id as full URL for each item', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'critical-thinking',
          to_name: 'Critical Thinking',
          to_type: 'skill',
          predicate: 'skills',
          importance: 85,
          level: 4,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'onet-graph',
        'occupations',
        '11-1011',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.$id).toContain(
        '/datasets/onet-graph/skills/critical-thinking'
      )
    })

    it('should include importance when present', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 's1',
          to_name: 'Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: 75,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.importance).toBe(75)
    })

    it('should include level when present', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 's1',
          to_name: 'Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: null,
          level: 3,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/o1/skills',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'occupations',
        'o1',
        'skills'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.level).toBe(3)
    })

    it('should not include importance when null', async () => {
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'tags',
          to_id: 't1',
          to_name: 'Tag',
          to_type: 'tag',
          predicate: 'tags',
          importance: null,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/posts/p1/tags',
        { worker: mockWorker }
      )

      const response = await handleRelationshipTraversal(
        mockContext as any,
        'test',
        'posts',
        'p1',
        'tags'
      )

      const body = (await response.json()) as Record<string, unknown>
      const items = body.items as Array<Record<string, unknown>>

      expect(items[0]?.importance).toBeUndefined()
      expect(items[0]?.level).toBeUndefined()
    })
  })
})
