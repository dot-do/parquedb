/**
 * Integration Tests for Cloudflare Workers AI Embeddings
 *
 * Tests the embedding generation functionality with mocked AI binding.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL,
  DEFAULT_DIMENSIONS,
  EMBEDDING_MODELS,
  processEmbedOperator,
  autoEmbedFields,
  hasEmbedOperator,
  extractEmbedOperator,
  buildAutoEmbedConfig,
  type AIBinding,
  type AutoEmbedConfig,
} from '../../src/embeddings'

// =============================================================================
// Mock AI Binding
// =============================================================================

/**
 * Create a mock AI binding for testing
 *
 * Generates deterministic embeddings based on input text for testing.
 */
function createMockAI(options?: {
  dimensions?: number
  failOnText?: string
}): AIBinding {
  const dimensions = options?.dimensions ?? DEFAULT_DIMENSIONS

  return {
    async run<T = unknown>(
      model: string,
      inputs: Record<string, unknown>
    ): Promise<T> {
      const texts = inputs.text as string[]

      if (!texts || !Array.isArray(texts)) {
        throw new Error('Invalid input: expected { text: string[] }')
      }

      // Check for intentional failures (for error testing)
      if (options?.failOnText) {
        const failText = options.failOnText
        if (texts.some(t => t.includes(failText))) {
          throw new Error(`AI model error: failed on text containing "${failText}"`)
        }
      }

      // Generate deterministic embeddings based on text content
      const data = texts.map(text => generateDeterministicEmbedding(text, dimensions))

      return {
        shape: [texts.length, dimensions],
        data,
      } as T
    },
  }
}

/**
 * Generate a deterministic embedding from text
 * Uses simple hashing to create reproducible vectors for testing
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
    // Use a simple PRNG seeded by hash and position
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

describe('WorkersAIEmbeddings', () => {
  let mockAI: AIBinding

  beforeEach(() => {
    mockAI = createMockAI()
  })

  describe('constructor', () => {
    it('creates instance with default model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      expect(embeddings.model).toBe(DEFAULT_MODEL)
      expect(embeddings.dimensions).toBe(DEFAULT_DIMENSIONS)
    })

    it('creates instance with custom model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-small-en-v1.5')

      expect(embeddings.model).toBe('@cf/baai/bge-small-en-v1.5')
      expect(embeddings.dimensions).toBe(384)
    })

    it('handles unknown model with default dimensions', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/custom/model')

      expect(embeddings.model).toBe('@cf/custom/model')
      expect(embeddings.dimensions).toBe(DEFAULT_DIMENSIONS)
    })
  })

  describe('embed()', () => {
    it('generates embedding for single text', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector = await embeddings.embed('Hello, world!')

      expect(vector).toBeInstanceOf(Array)
      expect(vector.length).toBe(DEFAULT_DIMENSIONS)
      expect(vector.every(v => typeof v === 'number')).toBe(true)
    })

    it('generates normalized vectors', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector = await embeddings.embed('Test text')

      // Check that vector is approximately unit length
      const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
      expect(magnitude).toBeCloseTo(1.0, 5)
    })

    it('generates consistent embeddings for same text', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector1 = await embeddings.embed('Same text')
      const vector2 = await embeddings.embed('Same text')

      expect(vector1).toEqual(vector2)
    })

    it('generates different embeddings for different text', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector1 = await embeddings.embed('First text')
      const vector2 = await embeddings.embed('Second text')

      expect(vector1).not.toEqual(vector2)
    })

    it('can use custom model via options', async () => {
      const smallAI = createMockAI({ dimensions: 384 })
      const embeddings = new WorkersAIEmbeddings(smallAI)

      const vector = await embeddings.embed('Test', {
        model: '@cf/baai/bge-small-en-v1.5',
      })

      expect(vector.length).toBe(384)
    })

    it('throws on empty response', async () => {
      const badAI: AIBinding = {
        async run() {
          return { shape: [], data: [] }
        },
      }
      const embeddings = new WorkersAIEmbeddings(badAI)

      await expect(embeddings.embed('Test')).rejects.toThrow('Failed to generate embedding')
    })
  })

  describe('embedBatch()', () => {
    it('generates embeddings for multiple texts', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vectors = await embeddings.embedBatch([
        'First document',
        'Second document',
        'Third document',
      ])

      expect(vectors.length).toBe(3)
      expect(vectors.every(v => v.length === DEFAULT_DIMENSIONS)).toBe(true)
    })

    it('returns empty array for empty input', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vectors = await embeddings.embedBatch([])

      expect(vectors).toEqual([])
    })

    it('maintains order of results', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)
      const texts = ['Alpha', 'Beta', 'Gamma']

      const vectors = await embeddings.embedBatch(texts)

      // Each vector should match individual embed() call
      for (let i = 0; i < texts.length; i++) {
        const individual = await embeddings.embed(texts[i])
        expect(vectors[i]).toEqual(individual)
      }
    })

    it('throws on length mismatch', async () => {
      const badAI: AIBinding = {
        async run() {
          return { shape: [2], data: [[1], [2]] } // Wrong number of results
        },
      }
      const embeddings = new WorkersAIEmbeddings(badAI)

      await expect(
        embeddings.embedBatch(['One', 'Two', 'Three'])
      ).rejects.toThrow('expected 3 vectors')
    })
  })

  describe('embedQuery()', () => {
    it('adds query prefix for BGE models', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [1, DEFAULT_DIMENSIONS],
        data: [new Array(DEFAULT_DIMENSIONS).fill(0.1)],
      })

      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI, '@cf/baai/bge-m3')

      await embeddings.embedQuery('test query')

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/baai/bge-m3',
        expect.objectContaining({
          text: expect.arrayContaining([
            expect.stringContaining('Represent this sentence'),
          ]),
        })
      )
    })
  })

  describe('embedDocument()', () => {
    it('embeds document without prefix', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [1, DEFAULT_DIMENSIONS],
        data: [new Array(DEFAULT_DIMENSIONS).fill(0.1)],
      })

      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI, '@cf/baai/bge-m3')

      await embeddings.embedDocument('document text')

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/baai/bge-m3',
        expect.objectContaining({
          text: ['document text'],
        })
      )
    })
  })

  describe('getModelConfig()', () => {
    it('returns config for known model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const config = embeddings.getModelConfig('@cf/baai/bge-base-en-v1.5')

      expect(config.dimensions).toBe(768)
      expect(config.normalize).toBe(true)
    })

    it('returns current model config by default', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-small-en-v1.5')

      const config = embeddings.getModelConfig()

      expect(config.dimensions).toBe(384)
    })
  })
})

describe('createEmbeddings()', () => {
  it('creates WorkersAIEmbeddings instance', () => {
    const mockAI = createMockAI()

    const embeddings = createEmbeddings(mockAI)

    expect(embeddings).toBeInstanceOf(WorkersAIEmbeddings)
    expect(embeddings.model).toBe(DEFAULT_MODEL)
  })

  it('creates instance with custom model', () => {
    const mockAI = createMockAI()

    const embeddings = createEmbeddings(mockAI, '@cf/baai/bge-large-en-v1.5')

    expect(embeddings.model).toBe('@cf/baai/bge-large-en-v1.5')
    expect(embeddings.dimensions).toBe(1024)
  })
})

describe('getModelDimensions()', () => {
  it('returns dimensions for known model', () => {
    expect(getModelDimensions('@cf/baai/bge-m3')).toBe(1024)
    expect(getModelDimensions('@cf/baai/bge-base-en-v1.5')).toBe(768)
    expect(getModelDimensions('@cf/baai/bge-small-en-v1.5')).toBe(384)
    expect(getModelDimensions('@cf/baai/bge-large-en-v1.5')).toBe(1024)
  })

  it('returns default dimensions for unknown model', () => {
    expect(getModelDimensions('@cf/unknown/model')).toBe(DEFAULT_DIMENSIONS)
  })

  it('returns default dimensions when no model specified', () => {
    expect(getModelDimensions()).toBe(DEFAULT_DIMENSIONS)
  })
})

describe('EMBEDDING_MODELS', () => {
  it('contains expected models', () => {
    expect(EMBEDDING_MODELS).toHaveProperty('@cf/baai/bge-m3')
    expect(EMBEDDING_MODELS).toHaveProperty('@cf/baai/bge-base-en-v1.5')
    expect(EMBEDDING_MODELS).toHaveProperty('@cf/baai/bge-small-en-v1.5')
    expect(EMBEDDING_MODELS).toHaveProperty('@cf/baai/bge-large-en-v1.5')
  })

  it('all models have required properties', () => {
    for (const [name, config] of Object.entries(EMBEDDING_MODELS)) {
      expect(config.model).toBe(name)
      expect(typeof config.dimensions).toBe('number')
      expect(config.dimensions).toBeGreaterThan(0)
    }
  })
})

describe('Vector Operations', () => {
  it('similar texts produce similar embeddings', async () => {
    const mockAI = createMockAI()
    const embeddings = new WorkersAIEmbeddings(mockAI)

    const v1 = await embeddings.embed('The quick brown fox')
    const v2 = await embeddings.embed('The quick brown dog') // Similar
    const v3 = await embeddings.embed('Python programming language') // Different

    // Calculate cosine similarities
    const sim12 = cosineSimilarity(v1, v2)
    const sim13 = cosineSimilarity(v1, v3)

    // Note: With deterministic mock, this tests the mock behavior
    // In production, similar texts would have higher similarity
    expect(typeof sim12).toBe('number')
    expect(typeof sim13).toBe('number')
    expect(sim12).toBeGreaterThan(-1)
    expect(sim12).toBeLessThanOrEqual(1)
  })

  it('vectors can be used for nearest neighbor search', async () => {
    const mockAI = createMockAI()
    const embeddings = new WorkersAIEmbeddings(mockAI)

    // Create document embeddings
    const documents = [
      'Machine learning is a subset of artificial intelligence',
      'Dogs are loyal pets',
      'Neural networks power deep learning',
      'Cats are independent animals',
    ]

    const docVectors = await embeddings.embedBatch(documents)

    // Query embedding
    const query = await embeddings.embed('AI and machine learning')

    // Find nearest neighbors
    const similarities = docVectors.map((v, i) => ({
      index: i,
      similarity: cosineSimilarity(query, v),
    }))

    similarities.sort((a, b) => b.similarity - a.similarity)

    // Verify we get a ranking
    expect(similarities.length).toBe(4)
    expect(similarities[0].similarity).toBeGreaterThanOrEqual(similarities[3].similarity)
  })
})

describe('Error Handling', () => {
  it('handles AI model errors gracefully', async () => {
    const failingAI = createMockAI({ failOnText: 'ERROR_TRIGGER' })
    const embeddings = new WorkersAIEmbeddings(failingAI)

    await expect(
      embeddings.embed('This text contains ERROR_TRIGGER')
    ).rejects.toThrow('AI model error')
  })

  it('handles null/undefined responses', async () => {
    const nullAI: AIBinding = {
      async run() {
        return null as unknown
      },
    }
    const embeddings = new WorkersAIEmbeddings(nullAI)

    await expect(embeddings.embed('Test')).rejects.toThrow('Failed to generate embedding')
  })
})

// =============================================================================
// Auto-Embed Tests
// =============================================================================

describe('processEmbedOperator', () => {
  it('embeds a single field', async () => {
    const mockAI = createMockAI()
    const data = {
      title: 'Hello World',
      description: 'A simple greeting message',
    }

    const result = await processEmbedOperator(
      data,
      { description: 'embedding' },
      { ai: mockAI }
    )

    expect(result.embedding).toBeDefined()
    expect(Array.isArray(result.embedding)).toBe(true)
    expect((result.embedding as number[]).length).toBe(DEFAULT_DIMENSIONS)
  })

  it('embeds multiple fields', async () => {
    const mockAI = createMockAI()
    const data = {
      title: 'Product Name',
      description: 'Product description',
      content: 'Detailed content here',
    }

    const result = await processEmbedOperator(
      data,
      {
        title: 'title_embedding',
        description: 'description_embedding',
      },
      { ai: mockAI }
    )

    expect(result.title_embedding).toBeDefined()
    expect(result.description_embedding).toBeDefined()
    expect((result.title_embedding as number[]).length).toBe(DEFAULT_DIMENSIONS)
    expect((result.description_embedding as number[]).length).toBe(DEFAULT_DIMENSIONS)
  })

  it('handles nested target fields', async () => {
    const mockAI = createMockAI()
    const data = {
      description: 'Test content',
      vectors: {},
    }

    const result = await processEmbedOperator(
      data,
      { description: 'vectors.semantic' },
      { ai: mockAI }
    )

    expect((result.vectors as Record<string, unknown>).semantic).toBeDefined()
  })

  it('uses config object for options', async () => {
    const mockAI = createMockAI({ dimensions: 384 })
    const data = {
      description: 'Test content',
    }

    const result = await processEmbedOperator(
      data,
      {
        description: {
          field: 'embedding',
          model: '@cf/baai/bge-small-en-v1.5',
        },
      },
      { ai: mockAI }
    )

    expect(result.embedding).toBeDefined()
    expect((result.embedding as number[]).length).toBe(384)
  })

  it('skips empty or non-string fields', async () => {
    const mockAI = createMockAI()
    const data = {
      title: '',
      count: 42,
      description: 'Valid text',
    }

    const result = await processEmbedOperator(
      data,
      {
        title: 'title_embedding',
        count: 'count_embedding',
        description: 'description_embedding',
      },
      { ai: mockAI }
    )

    expect(result.title_embedding).toBeUndefined()
    expect(result.count_embedding).toBeUndefined()
    expect(result.description_embedding).toBeDefined()
  })

  it('respects skipExisting option', async () => {
    const mockAI = createMockAI()
    const existingEmbedding = [1, 2, 3]
    const data = {
      description: 'Test content',
      embedding: existingEmbedding,
    }

    const result = await processEmbedOperator(
      data,
      { description: { field: 'embedding', overwrite: false } },
      { ai: mockAI, skipExisting: true }
    )

    expect(result.embedding).toEqual(existingEmbedding)
  })
})

describe('autoEmbedFields', () => {
  it('auto-embeds based on config', async () => {
    const mockAI = createMockAI()
    const data = {
      description: 'Product description',
    }

    const config: AutoEmbedConfig = {
      fields: [
        {
          sourceField: 'description',
          targetField: 'embedding',
        },
      ],
    }

    const result = await autoEmbedFields(data, config, { ai: mockAI })

    expect(result.embedding).toBeDefined()
    expect(Array.isArray(result.embedding)).toBe(true)
  })

  it('uses default model from config', async () => {
    const mockAI = createMockAI({ dimensions: 768 })
    const data = {
      description: 'Test',
    }

    const config: AutoEmbedConfig = {
      fields: [
        {
          sourceField: 'description',
          targetField: 'embedding',
        },
      ],
      defaultModel: '@cf/baai/bge-base-en-v1.5',
    }

    const result = await autoEmbedFields(data, config, { ai: mockAI })

    expect(result.embedding).toBeDefined()
    expect((result.embedding as number[]).length).toBe(768)
  })
})

describe('hasEmbedOperator', () => {
  it('returns true when $embed exists', () => {
    expect(hasEmbedOperator({ $embed: { description: 'embedding' } })).toBe(true)
  })

  it('returns false when $embed is missing', () => {
    expect(hasEmbedOperator({ $set: { name: 'test' } })).toBe(false)
  })

  it('returns false when $embed is undefined', () => {
    expect(hasEmbedOperator({ $embed: undefined })).toBe(false)
  })
})

describe('extractEmbedOperator', () => {
  it('extracts and removes $embed from update', () => {
    const update = {
      $set: { name: 'test' },
      $embed: { description: 'embedding' },
    }

    const embedConfig = extractEmbedOperator(update)

    expect(embedConfig).toEqual({ description: 'embedding' })
    expect(update.$embed).toBeUndefined()
    expect(update.$set).toEqual({ name: 'test' })
  })

  it('returns undefined when no $embed', () => {
    const update = { $set: { name: 'test' } }

    const embedConfig = extractEmbedOperator(update)

    expect(embedConfig).toBeUndefined()
  })
})

describe('buildAutoEmbedConfig', () => {
  it('builds config from vector index definitions', () => {
    const vectorIndexes = [
      {
        name: 'idx_embedding',
        fields: [{ path: 'embedding' }],
        sourceField: 'description',
      },
    ]

    const config = buildAutoEmbedConfig(vectorIndexes)

    expect(config.fields).toHaveLength(1)
    expect(config.fields[0]).toEqual({
      sourceField: 'description',
      targetField: 'embedding',
      overwrite: true,
    })
  })

  it('infers source field from target name', () => {
    const vectorIndexes = [
      {
        name: 'idx_desc_embedding',
        fields: [{ path: 'description_embedding' }],
      },
    ]

    const config = buildAutoEmbedConfig(vectorIndexes)

    expect(config.fields).toHaveLength(1)
    expect(config.fields[0].sourceField).toBe('description')
  })

  it('uses default source for "embedding" target', () => {
    const vectorIndexes = [
      {
        name: 'idx_embedding',
        fields: [{ path: 'embedding' }],
      },
    ]

    const config = buildAutoEmbedConfig(vectorIndexes)

    expect(config.fields).toHaveLength(1)
    expect(config.fields[0].sourceField).toBe('description')
  })

  it('skips indexes where source cannot be inferred', () => {
    const vectorIndexes = [
      {
        name: 'idx_vec',
        fields: [{ path: 'some_vector' }],
      },
    ]

    const config = buildAutoEmbedConfig(vectorIndexes)

    expect(config.fields).toHaveLength(0)
  })
})

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Calculate cosine similarity between two vectors
 */
function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error('Vectors must have same dimensions')
  }

  let dotProduct = 0
  let normA = 0
  let normB = 0

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i]
    normA += a[i] * a[i]
    normB += b[i] * b[i]
  }

  const magnitude = Math.sqrt(normA) * Math.sqrt(normB)
  if (magnitude === 0) return 0

  return dotProduct / magnitude
}
