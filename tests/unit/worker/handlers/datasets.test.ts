/**
 * Datasets Handler Tests
 *
 * Tests for /datasets/* route handlers.
 * Tests dataset listing, detail view, collection queries, and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  handleDatasetsList,
  handleDatasetDetail,
  handleCollectionList,
  handleFileNotFoundError,
} from '../../../../src/worker/handlers/datasets'
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

describe('Datasets Handlers', () => {
  let mockWorker: ReturnType<typeof createMockWorker>
  let mockContext: MockHandlerContext

  beforeEach(() => {
    vi.clearAllMocks()
    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // handleFileNotFoundError
  // ===========================================================================

  describe('handleFileNotFoundError', () => {
    it('should return 404 for File not found errors', () => {
      const error = new Error('File not found: data/test.parquet')
      const request = new Request('https://api.parquedb.com/datasets/test')

      const response = handleFileNotFoundError(error, request, performance.now(), 'test.parquet')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(404)
    })

    it('should extract file path from error message', async () => {
      const error = new Error('File not found: data/users.parquet')
      const request = new Request('https://api.parquedb.com/datasets/test')

      const response = handleFileNotFoundError(error, request, performance.now(), 'default.parquet')

      expect(response).not.toBeNull()
      const body = (await response!.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.message).toContain('data/users.parquet')
    })

    it('should return null for non-file-not-found errors', () => {
      const error = new Error('Connection timeout')
      const request = new Request('https://api.parquedb.com/datasets/test')

      const response = handleFileNotFoundError(error, request, performance.now(), 'test.parquet')

      expect(response).toBeNull()
    })

    it('should handle non-Error objects', () => {
      const request = new Request('https://api.parquedb.com/datasets/test')

      const response = handleFileNotFoundError('File not found: x.parquet', request, performance.now(), 'x.parquet')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(404)
    })

    it('should include DATASET_NOT_FOUND code', async () => {
      const error = new Error('File not found: test.parquet')
      const request = new Request('https://api.parquedb.com/datasets/test')

      const response = handleFileNotFoundError(error, request, performance.now(), 'test.parquet')

      const body = (await response!.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.hint).toContain('may not have been uploaded')
    })
  })

  // ===========================================================================
  // handleDatasetsList
  // ===========================================================================

  describe('handleDatasetsList', () => {
    it('should return 200 with list of datasets', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets',
        { worker: mockWorker }
      )

      const response = handleDatasetsList(mockContext as any)

      expect(response.status).toBe(200)
    })

    it('should include api metadata in response', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets',
        { worker: mockWorker }
      )

      const response = handleDatasetsList(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('datasets')
      expect(api.description).toContain('example datasets')
      expect(typeof api.count).toBe('number')
      expect(api.count).toBeGreaterThan(0)
    })

    it('should include items with dataset info', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets',
        { worker: mockWorker }
      )

      const response = handleDatasetsList(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const items = body.items as Array<Record<string, unknown>>
      expect(items.length).toBeGreaterThan(0)

      const firstItem = items[0]!
      expect(firstItem.id).toBeDefined()
      expect(firstItem.name).toBeDefined()
      expect(firstItem.href).toBeDefined()
      expect(firstItem.href).toContain('/datasets/')
    })

    it('should include links with self and dataset URLs', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets',
        { worker: mockWorker }
      )

      const response = handleDatasetsList(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toContain('/datasets')
      expect(links.home).toBeDefined()
    })

    it('should be synchronous', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets',
        { worker: mockWorker }
      )

      const response = handleDatasetsList(mockContext as any)

      expect(response).toBeInstanceOf(Response)
    })
  })

  // ===========================================================================
  // handleDatasetDetail
  // ===========================================================================

  describe('handleDatasetDetail', () => {
    it('should return 200 for existing dataset', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'imdb')

      expect(response.status).toBe(200)
    })

    it('should return 404 for non-existent dataset', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/nonexistent',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'nonexistent')

      expect(response.status).toBe(404)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
    })

    it('should include dataset metadata in response', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'imdb')
      const body = (await response.json()) as Record<string, unknown>

      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('dataset')
      expect(api.id).toBe('imdb')
      expect(api.name).toBe('IMDB')
    })

    it('should include collection links', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'imdb')
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toContain('/datasets/imdb')
      expect(links.home).toBeDefined()
      expect(links.datasets).toContain('/datasets')
    })

    it('should include collections data', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'imdb')
      const body = (await response.json()) as Record<string, unknown>

      const data = body.data as Record<string, unknown>
      expect(data.collections).toBeDefined()
    })

    it('should return array format collections when ?arrays is set', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb?arrays',
        { worker: mockWorker }
      )

      const response = handleDatasetDetail(mockContext as any, 'imdb')
      const body = (await response.json()) as Record<string, unknown>

      const data = body.data as Record<string, unknown>
      expect(Array.isArray(data.collections)).toBe(true)
    })
  })

  // ===========================================================================
  // handleCollectionList
  // ===========================================================================

  describe('handleCollectionList', () => {
    it('should return 404 for non-existent dataset', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/nonexistent/items',
        { worker: mockWorker }
      )

      const response = await handleCollectionList(mockContext as any, 'nonexistent', 'items')

      expect(response.status).toBe(404)
    })

    it('should query worker with correct namespace', async () => {
      mockWorker.find.mockResolvedValue({
        items: [],
        hasMore: false,
        stats: {},
      })

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      await handleCollectionList(mockContext as any, 'imdb', 'titles')

      expect(mockWorker.find).toHaveBeenCalledWith(
        'imdb/titles',
        expect.any(Object),
        expect.objectContaining({ limit: 20 })
      )
    })

    it('should return items from worker query', async () => {
      mockWorker.find.mockResolvedValue({
        items: [
          { $id: 'titles/tt001', $type: 'title', name: 'Movie 1' },
          { $id: 'titles/tt002', $type: 'title', name: 'Movie 2' },
        ],
        hasMore: false,
        stats: {},
      })

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      const response = await handleCollectionList(mockContext as any, 'imdb', 'titles')

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('collection')
      expect(api.returned).toBe(2)
    })

    it('should handle File not found errors with 404', async () => {
      mockWorker.find.mockRejectedValue(new Error('File not found: imdb/titles.parquet'))

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      const response = await handleCollectionList(mockContext as any, 'imdb', 'titles')

      expect(response.status).toBe(404)
    })

    it('should re-throw non file-not-found errors', async () => {
      mockWorker.find.mockRejectedValue(new Error('Connection failed'))

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      await expect(
        handleCollectionList(mockContext as any, 'imdb', 'titles')
      ).rejects.toThrow('Connection failed')
    })

    it('should use default limit of 20', async () => {
      mockWorker.find.mockResolvedValue({
        items: [],
        hasMore: false,
        stats: {},
      })

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      await handleCollectionList(mockContext as any, 'imdb', 'titles')

      expect(mockWorker.find).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Object),
        expect.objectContaining({ limit: 20 })
      )
    })

    it('should include pagination links when hasMore is true', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'titles/tt001', $type: 'title', name: 'Movie' }],
        hasMore: true,
        stats: {},
      })

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/imdb/titles',
        { worker: mockWorker }
      )

      const response = await handleCollectionList(mockContext as any, 'imdb', 'titles')
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.next).toBeDefined()
    })
  })
})
