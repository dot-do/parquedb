/**
 * Vector Text Query Tests for ParqueDB
 *
 * Tests query-time embedding generation for vector similarity searches.
 * This feature allows users to pass text strings in $vector queries
 * which are automatically converted to embeddings using a configured provider.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  normalizeVectorFilter,
  normalizeVectorFilterBatch,
  extractVectorQuery,
  isTextVectorQuery,
} from '../../../src/query/vector-query'
import {
  QueryEmbeddingCache,
  CachingEmbeddingProvider,
  withQueryCache,
} from '../../../src/embeddings/provider'
import type { EmbeddingProvider } from '../../../src/embeddings/provider'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// Mock Embedding Provider
// =============================================================================

function createMockProvider(dimensions = 128): EmbeddingProvider {
  return {
    dimensions,
    model: 'mock-model',
    embed: vi.fn().mockImplementation(async (text: string) => {
      // Generate deterministic "embedding" based on text hash
      const hash = text.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
      return Array.from({ length: dimensions }, (_, i) => (hash + i) / 1000)
    }),
    embedBatch: vi.fn().mockImplementation(async (texts: string[]) => {
      return texts.map(text => {
        const hash = text.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
        return Array.from({ length: dimensions }, (_, i) => (hash + i) / 1000)
      })
    }),
  }
}

// =============================================================================
// extractVectorQuery Tests
// =============================================================================

describe('extractVectorQuery', () => {
  describe('new format (query/field/topK)', () => {
    it('extracts vector query with array query', () => {
      const filter: Filter = {
        $vector: {
          query: [0.1, 0.2, 0.3],
          field: 'embedding',
          topK: 10,
        },
      }
      const result = extractVectorQuery(filter)
      expect(result).toEqual({
        query: [0.1, 0.2, 0.3],
        field: 'embedding',
        topK: 10,
        minScore: undefined,
        efSearch: undefined,
      })
    })

    it('extracts vector query with text query', () => {
      const filter: Filter = {
        $vector: {
          query: 'machine learning tutorials',
          field: 'embedding',
          topK: 5,
          minScore: 0.8,
        },
      }
      const result = extractVectorQuery(filter)
      expect(result).toEqual({
        query: 'machine learning tutorials',
        field: 'embedding',
        topK: 5,
        minScore: 0.8,
        efSearch: undefined,
      })
    })

    it('includes efSearch when provided', () => {
      const filter: Filter = {
        $vector: {
          query: 'test',
          field: 'embedding',
          topK: 10,
          efSearch: 100,
        },
      }
      const result = extractVectorQuery(filter)
      expect(result?.efSearch).toBe(100)
    })
  })

  describe('legacy format ($near/$k/$field)', () => {
    it('extracts legacy vector query', () => {
      const filter: Filter = {
        $vector: {
          $near: [0.1, 0.2, 0.3],
          $k: 10,
          $field: 'embedding',
          $minScore: 0.7,
        } as Filter['$vector'],
      }
      const result = extractVectorQuery(filter)
      expect(result).toEqual({
        query: [0.1, 0.2, 0.3],
        field: 'embedding',
        topK: 10,
        minScore: 0.7,
        efSearch: undefined,
      })
    })

    it('uses defaults for missing legacy fields', () => {
      const filter: Filter = {
        $vector: {
          $near: [0.1, 0.2, 0.3],
        } as Filter['$vector'],
      }
      const result = extractVectorQuery(filter)
      expect(result).toEqual({
        query: [0.1, 0.2, 0.3],
        field: 'embedding', // default
        topK: 10, // default
        minScore: undefined,
        efSearch: undefined,
      })
    })
  })

  it('returns null for non-vector filters', () => {
    expect(extractVectorQuery({})).toBeNull()
    expect(extractVectorQuery({ name: 'test' })).toBeNull()
    expect(extractVectorQuery({ $text: { $search: 'hello' } })).toBeNull()
  })
})

// =============================================================================
// isTextVectorQuery Tests
// =============================================================================

describe('isTextVectorQuery', () => {
  it('returns true for text query', () => {
    const filter: Filter = {
      $vector: {
        query: 'machine learning',
        field: 'embedding',
        topK: 10,
      },
    }
    expect(isTextVectorQuery(filter)).toBe(true)
  })

  it('returns false for array query', () => {
    const filter: Filter = {
      $vector: {
        query: [0.1, 0.2, 0.3],
        field: 'embedding',
        topK: 10,
      },
    }
    expect(isTextVectorQuery(filter)).toBe(false)
  })

  it('returns false for non-vector filter', () => {
    expect(isTextVectorQuery({})).toBe(false)
    expect(isTextVectorQuery({ name: 'test' })).toBe(false)
  })
})

// =============================================================================
// normalizeVectorFilter Tests
// =============================================================================

describe('normalizeVectorFilter', () => {
  it('returns filter unchanged for array query', async () => {
    const filter: Filter = {
      $vector: {
        query: [0.1, 0.2, 0.3],
        field: 'embedding',
        topK: 10,
      },
    }
    const result = await normalizeVectorFilter(filter)
    expect(result.filter).toBe(filter)
    expect(result.textEmbedded).toBe(false)
    expect(result.vectorQuery?.vector).toEqual([0.1, 0.2, 0.3])
  })

  it('returns filter unchanged for non-vector filters', async () => {
    const filter: Filter = { name: 'test' }
    const result = await normalizeVectorFilter(filter)
    expect(result.filter).toBe(filter)
    expect(result.textEmbedded).toBe(false)
    expect(result.vectorQuery).toBeUndefined()
  })

  it('throws error for text query without provider', async () => {
    const filter: Filter = {
      $vector: {
        query: 'machine learning',
        field: 'embedding',
        topK: 10,
      },
    }
    await expect(normalizeVectorFilter(filter)).rejects.toThrow(
      /no embedding provider is configured/
    )
  })

  it('converts text query to embedding with provider', async () => {
    const provider = createMockProvider()
    const filter: Filter = {
      $vector: {
        query: 'machine learning tutorials',
        field: 'embedding',
        topK: 10,
        minScore: 0.7,
      },
    }

    const result = await normalizeVectorFilter(filter, provider)

    expect(result.textEmbedded).toBe(true)
    expect(result.vectorQuery).toBeDefined()
    expect(result.vectorQuery?.vector).toHaveLength(128)
    expect(result.vectorQuery?.field).toBe('embedding')
    expect(result.vectorQuery?.topK).toBe(10)
    expect(result.vectorQuery?.minScore).toBe(0.7)

    // Check the filter was updated
    expect(Array.isArray(result.filter.$vector?.query)).toBe(true)
    expect(result.filter.$vector?.query).toHaveLength(128)

    // Check legacy format is also set
    expect(result.filter.$vector?.$near).toEqual(result.filter.$vector?.query)
    expect(result.filter.$vector?.$k).toBe(10)
    expect(result.filter.$vector?.$field).toBe('embedding')

    // Check provider was called with isQuery flag
    expect(provider.embed).toHaveBeenCalledWith('machine learning tutorials', { isQuery: true })
  })

  it('preserves other filter conditions', async () => {
    const provider = createMockProvider()
    const filter: Filter = {
      status: 'published',
      $vector: {
        query: 'test',
        field: 'embedding',
        topK: 5,
      },
    }

    const result = await normalizeVectorFilter(filter, provider)

    expect(result.filter.status).toBe('published')
    expect(result.filter.$vector).toBeDefined()
  })
})

// =============================================================================
// normalizeVectorFilterBatch Tests
// =============================================================================

describe('normalizeVectorFilterBatch', () => {
  it('handles mixed text and vector queries efficiently', async () => {
    const provider = createMockProvider()
    const filters: Filter[] = [
      { $vector: { query: 'query one', field: 'embedding', topK: 10 } },
      { $vector: { query: [0.1, 0.2], field: 'embedding', topK: 5 } },
      { $vector: { query: 'query two', field: 'embedding', topK: 10 } },
      { name: 'no vector' },
    ]

    const results = await normalizeVectorFilterBatch(filters, provider)

    expect(results).toHaveLength(4)
    expect(results[0]!.textEmbedded).toBe(true)
    expect(results[1]!.textEmbedded).toBe(false)
    expect(results[2]!.textEmbedded).toBe(true)
    expect(results[3]!.textEmbedded).toBe(false)

    // Should batch the text queries
    expect(provider.embedBatch).toHaveBeenCalledTimes(1)
    expect(provider.embedBatch).toHaveBeenCalledWith(
      ['query one', 'query two'],
      { isQuery: true }
    )
  })

  it('returns immediately for all-vector filters without API call', async () => {
    const provider = createMockProvider()
    const filters: Filter[] = [
      { $vector: { query: [0.1, 0.2], field: 'embedding', topK: 10 } },
      { name: 'test' },
    ]

    const results = await normalizeVectorFilterBatch(filters, provider)

    expect(results).toHaveLength(2)
    expect(provider.embed).not.toHaveBeenCalled()
    expect(provider.embedBatch).not.toHaveBeenCalled()
  })

  it('throws error for text queries without provider', async () => {
    const filters: Filter[] = [
      { $vector: { query: 'text query', field: 'embedding', topK: 10 } },
    ]

    await expect(normalizeVectorFilterBatch(filters)).rejects.toThrow(
      /no embedding provider is configured/
    )
  })
})

// =============================================================================
// QueryEmbeddingCache Tests
// =============================================================================

describe('QueryEmbeddingCache', () => {
  let cache: QueryEmbeddingCache

  beforeEach(() => {
    cache = new QueryEmbeddingCache({ maxSize: 3, ttlMs: 100 })
  })

  it('stores and retrieves embeddings', () => {
    const vector = [0.1, 0.2, 0.3]
    cache.set('test query', vector)
    expect(cache.get('test query')).toEqual(vector)
  })

  it('returns undefined for missing entries', () => {
    expect(cache.get('missing')).toBeUndefined()
  })

  it('respects model in cache key', () => {
    const vector1 = [0.1, 0.2]
    const vector2 = [0.3, 0.4]
    cache.set('query', vector1, 'model1')
    cache.set('query', vector2, 'model2')
    expect(cache.get('query', 'model1')).toEqual(vector1)
    expect(cache.get('query', 'model2')).toEqual(vector2)
  })

  it('evicts oldest entries when at capacity', () => {
    cache.set('a', [1])
    cache.set('b', [2])
    cache.set('c', [3])
    cache.set('d', [4]) // Should evict 'a'

    expect(cache.get('a')).toBeUndefined()
    expect(cache.get('b')).toEqual([2])
    expect(cache.get('c')).toEqual([3])
    expect(cache.get('d')).toEqual([4])
  })

  it('expires entries after TTL', async () => {
    cache.set('test', [0.1])
    expect(cache.get('test')).toEqual([0.1])

    // Wait for TTL to expire
    await new Promise(resolve => setTimeout(resolve, 150))

    expect(cache.get('test')).toBeUndefined()
  })

  it('clears all entries', () => {
    cache.set('a', [1])
    cache.set('b', [2])
    cache.clear()
    expect(cache.size).toBe(0)
    expect(cache.get('a')).toBeUndefined()
  })

  it('prunes expired entries', async () => {
    cache.set('a', [1])
    cache.set('b', [2])
    expect(cache.size).toBe(2)

    await new Promise(resolve => setTimeout(resolve, 150))

    const pruned = cache.prune()
    expect(pruned).toBe(2)
    expect(cache.size).toBe(0)
  })
})

// =============================================================================
// CachingEmbeddingProvider Tests
// =============================================================================

describe('CachingEmbeddingProvider', () => {
  let baseProvider: EmbeddingProvider
  let cachingProvider: CachingEmbeddingProvider

  beforeEach(() => {
    baseProvider = createMockProvider()
    cachingProvider = new CachingEmbeddingProvider(baseProvider, {
      cache: { maxSize: 100, ttlMs: 60000 },
    })
  })

  it('caches query embeddings', async () => {
    const text = 'test query'
    const result1 = await cachingProvider.embed(text, { isQuery: true })
    const result2 = await cachingProvider.embed(text, { isQuery: true })

    expect(result1).toEqual(result2)
    expect(baseProvider.embed).toHaveBeenCalledTimes(1)
  })

  it('does not cache non-query embeddings', async () => {
    const text = 'document text'
    await cachingProvider.embed(text)
    await cachingProvider.embed(text)

    expect(baseProvider.embed).toHaveBeenCalledTimes(2)
  })

  it('caches batch query embeddings', async () => {
    const texts = ['query1', 'query2', 'query3']
    const result1 = await cachingProvider.embedBatch(texts, { isQuery: true })

    // Second call should use cache
    const result2 = await cachingProvider.embedBatch(texts, { isQuery: true })

    expect(result1).toEqual(result2)
    expect(baseProvider.embedBatch).toHaveBeenCalledTimes(1)
  })

  it('only embeds uncached texts in batch', async () => {
    // Cache one text
    await cachingProvider.embed('cached', { isQuery: true })
    vi.mocked(baseProvider.embed).mockClear()

    // Now batch embed with mix of cached and uncached
    await cachingProvider.embedBatch(['cached', 'new1', 'new2'], { isQuery: true })

    // Should only call batch for uncached texts
    expect(baseProvider.embedBatch).toHaveBeenCalledWith(
      ['new1', 'new2'],
      { isQuery: true }
    )
  })

  it('exposes underlying cache', () => {
    expect(cachingProvider.cache).toBeInstanceOf(QueryEmbeddingCache)
  })

  it('passes through dimensions and model from base provider', () => {
    expect(cachingProvider.dimensions).toBe(128)
    expect(cachingProvider.model).toBe('mock-model')
  })
})

// =============================================================================
// withQueryCache Helper Tests
// =============================================================================

describe('withQueryCache', () => {
  it('wraps provider with caching', async () => {
    const baseProvider = createMockProvider()
    const cached = withQueryCache(baseProvider)

    await cached.embed('test', { isQuery: true })
    await cached.embed('test', { isQuery: true })

    expect(baseProvider.embed).toHaveBeenCalledTimes(1)
  })

  it('accepts custom cache options', async () => {
    const baseProvider = createMockProvider()
    const cached = withQueryCache(baseProvider, {
      cache: { maxSize: 5, ttlMs: 10 },
    })

    await cached.embed('test', { isQuery: true })

    // Wait for TTL
    await new Promise(resolve => setTimeout(resolve, 20))

    await cached.embed('test', { isQuery: true })

    expect(baseProvider.embed).toHaveBeenCalledTimes(2)
  })
})
