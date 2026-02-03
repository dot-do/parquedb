/**
 * Tests for Query-Time Embedding Generation
 *
 * Tests the ability to convert text queries to vector embeddings
 * at query time for vector similarity search.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  QueryEmbeddingGenerator,
  createQueryEmbeddingGenerator,
  type QueryEmbeddingGeneratorOptions,
} from '@/indexes/vector/query-embeddings'
import type { EmbeddingProvider, EmbedTextOptions } from '@/embeddings/provider'

// =============================================================================
// Mock Embedding Provider
// =============================================================================

/**
 * Create a mock embedding provider for testing
 */
function createMockProvider(options?: {
  dimensions?: number
  failOnText?: string
}): EmbeddingProvider {
  const dimensions = options?.dimensions ?? 128

  const embed = async (text: string, opts?: EmbedTextOptions): Promise<number[]> => {
    if (options?.failOnText && text.includes(options.failOnText)) {
      throw new Error(`Embedding failed for text containing "${options.failOnText}"`)
    }

    // Generate deterministic embedding based on text hash
    return generateDeterministicEmbedding(text, dimensions)
  }

  const embedBatch = async (texts: string[], opts?: EmbedTextOptions): Promise<number[][]> => {
    if (options?.failOnText) {
      const failText = options.failOnText
      if (texts.some(t => t.includes(failText))) {
        throw new Error(`Embedding failed for text containing "${failText}"`)
      }
    }

    return texts.map(text => generateDeterministicEmbedding(text, dimensions))
  }

  return {
    embed,
    embedBatch,
    dimensions,
    model: 'mock-model',
  }
}

/**
 * Generate a deterministic embedding from text for testing
 */
function generateDeterministicEmbedding(text: string, dimensions: number): number[] {
  const vector: number[] = []

  // Simple hash-based approach for deterministic embeddings
  let hash = 0
  for (let i = 0; i < text.length; i++) {
    const char = text.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash
  }

  // Generate vector values based on hash
  const seed = Math.abs(hash)
  for (let i = 0; i < dimensions; i++) {
    const x = Math.sin(seed * (i + 1)) * 10000
    vector.push(x - Math.floor(x))
  }

  // Normalize to unit vector
  const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
  return vector.map(v => v / magnitude)
}

// =============================================================================
// Tests
// =============================================================================

describe('QueryEmbeddingGenerator', () => {
  let mockProvider: EmbeddingProvider

  beforeEach(() => {
    mockProvider = createMockProvider()
  })

  describe('constructor', () => {
    it('creates instance with embedding provider', () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      expect(generator.provider).toBe(mockProvider)
      expect(generator.dimensions).toBe(128)
    })

    it('creates instance with options', () => {
      const generator = new QueryEmbeddingGenerator(mockProvider, {
        cacheEnabled: true,
        maxCacheSize: 500,
        cacheTtlMs: 60000,
      })

      expect(generator.cacheEnabled).toBe(true)
    })
  })

  describe('embed()', () => {
    it('generates embedding for text query', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vector = await generator.embed('What is machine learning?')

      expect(vector).toBeInstanceOf(Array)
      expect(vector.length).toBe(128)
      expect(vector.every(v => typeof v === 'number')).toBe(true)
    })

    it('generates normalized vectors', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vector = await generator.embed('Test query')

      // Check that vector is approximately unit length
      const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
      expect(magnitude).toBeCloseTo(1.0, 5)
    })

    it('passes isQuery option to provider', async () => {
      const embedSpy = vi.fn().mockResolvedValue(new Array(128).fill(0.1))
      const spyProvider: EmbeddingProvider = {
        embed: embedSpy,
        embedBatch: vi.fn(),
        dimensions: 128,
        model: 'spy-model',
      }

      const generator = new QueryEmbeddingGenerator(spyProvider)
      await generator.embed('test query')

      expect(embedSpy).toHaveBeenCalledWith('test query', { isQuery: true })
    })

    it('generates consistent embeddings for same query', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vector1 = await generator.embed('Same query')
      const vector2 = await generator.embed('Same query')

      expect(vector1).toEqual(vector2)
    })

    it('generates different embeddings for different queries', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vector1 = await generator.embed('First query')
      const vector2 = await generator.embed('Second query')

      expect(vector1).not.toEqual(vector2)
    })

    it('handles empty string query', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vector = await generator.embed('')

      expect(vector).toBeInstanceOf(Array)
      expect(vector.length).toBe(128)
    })

    it('propagates provider errors', async () => {
      const failingProvider = createMockProvider({ failOnText: 'ERROR' })
      const generator = new QueryEmbeddingGenerator(failingProvider)

      await expect(generator.embed('This contains ERROR')).rejects.toThrow(
        'Embedding failed for text containing "ERROR"'
      )
    })
  })

  describe('embedBatch()', () => {
    it('generates embeddings for multiple queries', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vectors = await generator.embedBatch([
        'What is AI?',
        'How does ML work?',
        'Explain neural networks',
      ])

      expect(vectors.length).toBe(3)
      expect(vectors.every(v => v.length === 128)).toBe(true)
    })

    it('returns empty array for empty input', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const vectors = await generator.embedBatch([])

      expect(vectors).toEqual([])
    })

    it('maintains order of results', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)
      const queries = ['Alpha', 'Beta', 'Gamma']

      const vectors = await generator.embedBatch(queries)

      // Each vector should match individual embed() call
      for (let i = 0; i < queries.length; i++) {
        const individual = await generator.embed(queries[i])
        expect(vectors[i]).toEqual(individual)
      }
    })

    it('passes isQuery option for batch queries', async () => {
      const embedBatchSpy = vi.fn().mockResolvedValue([
        new Array(128).fill(0.1),
        new Array(128).fill(0.2),
      ])
      const spyProvider: EmbeddingProvider = {
        embed: vi.fn(),
        embedBatch: embedBatchSpy,
        dimensions: 128,
        model: 'spy-model',
      }

      const generator = new QueryEmbeddingGenerator(spyProvider)
      await generator.embedBatch(['query1', 'query2'])

      expect(embedBatchSpy).toHaveBeenCalledWith(
        ['query1', 'query2'],
        { isQuery: true }
      )
    })
  })

  describe('caching', () => {
    it('caches query embeddings when enabled', async () => {
      const embedSpy = vi.fn().mockImplementation((text: string) =>
        Promise.resolve(generateDeterministicEmbedding(text, 128))
      )
      const spyProvider: EmbeddingProvider = {
        embed: embedSpy,
        embedBatch: vi.fn(),
        dimensions: 128,
        model: 'spy-model',
      }

      const generator = new QueryEmbeddingGenerator(spyProvider, {
        cacheEnabled: true,
      })

      // First call - should hit provider
      await generator.embed('cached query')
      expect(embedSpy).toHaveBeenCalledTimes(1)

      // Second call - should use cache
      await generator.embed('cached query')
      expect(embedSpy).toHaveBeenCalledTimes(1) // Still 1

      // Different query - should hit provider
      await generator.embed('different query')
      expect(embedSpy).toHaveBeenCalledTimes(2)
    })

    it('does not cache when disabled', async () => {
      const embedSpy = vi.fn().mockImplementation((text: string) =>
        Promise.resolve(generateDeterministicEmbedding(text, 128))
      )
      const spyProvider: EmbeddingProvider = {
        embed: embedSpy,
        embedBatch: vi.fn(),
        dimensions: 128,
        model: 'spy-model',
      }

      const generator = new QueryEmbeddingGenerator(spyProvider, {
        cacheEnabled: false,
      })

      await generator.embed('query')
      await generator.embed('query')

      expect(embedSpy).toHaveBeenCalledTimes(2)
    })

    it('respects cache size limit', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider, {
        cacheEnabled: true,
        maxCacheSize: 2,
      })

      await generator.embed('query1')
      await generator.embed('query2')
      await generator.embed('query3') // Should evict query1

      expect(generator.cacheSize).toBeLessThanOrEqual(2)
    })

    it('clearCache() removes all cached entries', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider, {
        cacheEnabled: true,
      })

      await generator.embed('query1')
      await generator.embed('query2')

      expect(generator.cacheSize).toBeGreaterThan(0)

      generator.clearCache()

      expect(generator.cacheSize).toBe(0)
    })

    it('getCacheStats() returns cache statistics', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider, {
        cacheEnabled: true,
      })

      await generator.embed('query1')
      await generator.embed('query2')
      await generator.embed('query1') // Cache hit

      const stats = generator.getCacheStats()

      expect(stats.size).toBe(2)
      expect(stats.hits).toBe(1)
      expect(stats.misses).toBe(2)
      expect(stats.hitRate).toBeCloseTo(1 / 3, 2)
    })
  })

  describe('embedOrPassthrough()', () => {
    it('generates embedding when query is string', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const result = await generator.embedOrPassthrough('text query')

      expect(result).toBeInstanceOf(Array)
      expect(result.length).toBe(128)
    })

    it('passes through when query is already a vector', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)
      const existingVector = [0.1, 0.2, 0.3]

      const result = await generator.embedOrPassthrough(existingVector)

      expect(result).toBe(existingVector)
    })

    it('validates vector dimensions when passing through', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)
      const wrongDimensions = [0.1, 0.2] // Only 2 dimensions, expected 128

      await expect(generator.embedOrPassthrough(wrongDimensions)).rejects.toThrow(
        /dimension mismatch/i
      )
    })

    it('skips validation when validateDimensions is false', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider, {
        validateDimensions: false,
      })
      const anyVector = [0.1, 0.2]

      const result = await generator.embedOrPassthrough(anyVector)

      expect(result).toEqual([0.1, 0.2])
    })
  })

  describe('prepareVectorQuery()', () => {
    it('prepares vector query from text', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const prepared = await generator.prepareVectorQuery({
        query: 'semantic search query',
        field: 'embedding',
        topK: 10,
      })

      expect(prepared.vector).toBeInstanceOf(Array)
      expect(prepared.vector.length).toBe(128)
      expect(prepared.field).toBe('embedding')
      expect(prepared.topK).toBe(10)
      expect(prepared.textEmbedded).toBe(true)
      expect(prepared.originalQuery).toBe('semantic search query')
    })

    it('prepares vector query from existing vector', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)
      const existingVector = new Array(128).fill(0.1)

      const prepared = await generator.prepareVectorQuery({
        query: existingVector,
        field: 'embedding',
        topK: 5,
      })

      expect(prepared.vector).toBe(existingVector)
      expect(prepared.textEmbedded).toBe(false)
      expect(prepared.originalQuery).toBe(existingVector)
    })

    it('preserves optional search parameters', async () => {
      const generator = new QueryEmbeddingGenerator(mockProvider)

      const prepared = await generator.prepareVectorQuery({
        query: 'test',
        field: 'vectors.semantic',
        topK: 20,
        minScore: 0.8,
        efSearch: 100,
      })

      expect(prepared.minScore).toBe(0.8)
      expect(prepared.efSearch).toBe(100)
    })
  })
})

describe('createQueryEmbeddingGenerator()', () => {
  it('creates generator with provider', () => {
    const mockProvider = createMockProvider()

    const generator = createQueryEmbeddingGenerator(mockProvider)

    expect(generator).toBeInstanceOf(QueryEmbeddingGenerator)
    expect(generator.dimensions).toBe(128)
  })

  it('creates generator with options', () => {
    const mockProvider = createMockProvider()

    const generator = createQueryEmbeddingGenerator(mockProvider, {
      cacheEnabled: true,
      maxCacheSize: 100,
    })

    expect(generator.cacheEnabled).toBe(true)
  })
})

describe('Integration with VectorIndex-compatible queries', () => {
  let generator: QueryEmbeddingGenerator

  beforeEach(() => {
    const provider = createMockProvider({ dimensions: 3 })
    generator = new QueryEmbeddingGenerator(provider)
  })

  it('generates embeddings suitable for vector search', async () => {
    const vector = await generator.embed('Find similar documents')

    // Vector should be suitable for HNSW search
    expect(vector.length).toBe(3)
    expect(vector.every(v => typeof v === 'number' && !isNaN(v))).toBe(true)

    // Should be normalized (unit length)
    const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
    expect(magnitude).toBeCloseTo(1.0, 5)
  })

  it('supports semantic search workflow', async () => {
    // Simulate semantic search preparation
    const queries = [
      'machine learning basics',
      'deep neural networks',
      'reinforcement learning',
    ]

    const vectors = await generator.embedBatch(queries)

    // All vectors should be valid for search
    expect(vectors.length).toBe(3)
    vectors.forEach((v, i) => {
      expect(v.length).toBe(3)
      expect(v.every(val => typeof val === 'number' && !isNaN(val))).toBe(true)
    })
  })
})
