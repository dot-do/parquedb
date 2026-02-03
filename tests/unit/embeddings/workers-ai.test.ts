/**
 * Unit Tests for Workers AI Embeddings Module
 *
 * Tests the WorkersAIEmbeddings class, factory functions, and model configurations.
 * Uses mock AI bindings to test the embedding generation logic.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL,
  DEFAULT_DIMENSIONS,
  EMBEDDING_MODELS,
  type AIBinding,
} from '../../../src/embeddings/workers-ai'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock AI binding for unit testing
 */
function createMockAI(options?: {
  dimensions?: number
  throwError?: boolean
  errorMessage?: string
  returnEmpty?: boolean
  returnWrongCount?: boolean
}): AIBinding {
  const dimensions = options?.dimensions ?? DEFAULT_DIMENSIONS

  return {
    async run<T = unknown>(
      _model: string,
      inputs: Record<string, unknown>
    ): Promise<T> {
      if (options?.throwError) {
        throw new Error(options.errorMessage ?? 'Mock AI error')
      }

      const texts = inputs.text as string[]

      if (options?.returnEmpty) {
        return { shape: [], data: [] } as T
      }

      if (options?.returnWrongCount) {
        // Return fewer vectors than requested
        return {
          shape: [1, dimensions],
          data: [new Array(dimensions).fill(0.1)],
        } as T
      }

      // Generate simple mock vectors
      const data = texts.map((text, idx) => {
        const vector = new Array(dimensions).fill(0)
        // Add some variety based on text and index
        for (let i = 0; i < dimensions; i++) {
          vector[i] = Math.sin((text.charCodeAt(0) ?? 65) + idx + i) * 0.5
        }
        return vector
      })

      return {
        shape: [texts.length, dimensions],
        data,
      } as T
    },
  }
}

/**
 * Helper to calculate vector magnitude
 */
function vectorMagnitude(vector: number[]): number {
  return Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
}

// =============================================================================
// WorkersAIEmbeddings Tests
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

    it('creates instance with custom known model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-small-en-v1.5')

      expect(embeddings.model).toBe('@cf/baai/bge-small-en-v1.5')
      expect(embeddings.dimensions).toBe(384)
    })

    it('creates instance with custom unknown model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/custom/model')

      expect(embeddings.model).toBe('@cf/custom/model')
      // Unknown models get default dimensions
      expect(embeddings.dimensions).toBe(DEFAULT_DIMENSIONS)
    })

    it('stores AI binding internally', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      // Verify it works by making a call
      await expect(embeddings.embed('test')).resolves.toBeDefined()
    })
  })

  describe('embed()', () => {
    it('generates embedding for text', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector = await embeddings.embed('Hello, world!')

      expect(vector).toBeInstanceOf(Array)
      expect(vector.length).toBe(DEFAULT_DIMENSIONS)
      expect(vector.every(v => typeof v === 'number')).toBe(true)
    })

    it('generates normalized vectors by default', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vector = await embeddings.embed('Test text')

      // Normalized vectors have magnitude close to 1
      const magnitude = vectorMagnitude(vector)
      expect(magnitude).toBeCloseTo(1.0, 5)
    })

    it('can return raw vectors when requested', async () => {
      // Create mock that returns non-normalized vectors
      const rawMockAI: AIBinding = {
        async run<T = unknown>(): Promise<T> {
          // Return a vector that's not normalized
          const vector = new Array(DEFAULT_DIMENSIONS).fill(2.0)
          return {
            shape: [1, DEFAULT_DIMENSIONS],
            data: [vector],
          } as T
        },
      }
      const embeddings = new WorkersAIEmbeddings(rawMockAI)

      const rawVector = await embeddings.embed('Test', { raw: true })

      // Should not be normalized
      const magnitude = vectorMagnitude(rawVector)
      expect(magnitude).not.toBeCloseTo(1.0, 1)
    })

    it('uses custom model from options', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [1, DEFAULT_DIMENSIONS],
        data: [new Array(DEFAULT_DIMENSIONS).fill(0.1)],
      })
      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI)

      await embeddings.embed('Test', { model: '@cf/baai/bge-small-en-v1.5' })

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/baai/bge-small-en-v1.5',
        expect.objectContaining({ text: ['Test'] })
      )
    })

    it('throws on empty response', async () => {
      const emptyAI = createMockAI({ returnEmpty: true })
      const embeddings = new WorkersAIEmbeddings(emptyAI)

      await expect(embeddings.embed('Test')).rejects.toThrow('Failed to generate embedding')
    })

    it('propagates AI errors', async () => {
      const errorAI = createMockAI({
        throwError: true,
        errorMessage: 'Model unavailable',
      })
      const embeddings = new WorkersAIEmbeddings(errorAI)

      await expect(embeddings.embed('Test')).rejects.toThrow('Model unavailable')
    })
  })

  describe('embedBatch()', () => {
    it('generates embeddings for multiple texts', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vectors = await embeddings.embedBatch(['First', 'Second', 'Third'])

      expect(vectors.length).toBe(3)
      expect(vectors.every(v => v.length === DEFAULT_DIMENSIONS)).toBe(true)
    })

    it('returns empty array for empty input', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vectors = await embeddings.embedBatch([])

      expect(vectors).toEqual([])
    })

    it('generates normalized vectors', async () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const vectors = await embeddings.embedBatch(['One', 'Two'])

      for (const vector of vectors) {
        const magnitude = vectorMagnitude(vector)
        expect(magnitude).toBeCloseTo(1.0, 5)
      }
    })

    it('throws on count mismatch', async () => {
      const mismatchAI = createMockAI({ returnWrongCount: true })
      const embeddings = new WorkersAIEmbeddings(mismatchAI)

      await expect(
        embeddings.embedBatch(['One', 'Two', 'Three'])
      ).rejects.toThrow('expected 3 vectors')
    })

    it('uses custom model from options', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [2, DEFAULT_DIMENSIONS],
        data: [
          new Array(DEFAULT_DIMENSIONS).fill(0.1),
          new Array(DEFAULT_DIMENSIONS).fill(0.2),
        ],
      })
      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI)

      await embeddings.embedBatch(['A', 'B'], { model: '@cf/custom/model' })

      expect(runSpy).toHaveBeenCalledWith('@cf/custom/model', expect.anything())
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

      await embeddings.embedQuery('search query')

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/baai/bge-m3',
        expect.objectContaining({
          text: [expect.stringContaining('Represent this sentence')],
        })
      )
    })

    it('includes original query in prefixed text', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [1, DEFAULT_DIMENSIONS],
        data: [new Array(DEFAULT_DIMENSIONS).fill(0.1)],
      })
      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI, '@cf/baai/bge-base-en-v1.5')

      await embeddings.embedQuery('my search query')

      const callArgs = runSpy.mock.calls[0]
      const textArg = callArgs[1].text[0]
      expect(textArg).toContain('my search query')
    })

    it('does not add prefix for non-BGE models', async () => {
      const runSpy = vi.fn().mockResolvedValue({
        shape: [1, DEFAULT_DIMENSIONS],
        data: [new Array(DEFAULT_DIMENSIONS).fill(0.1)],
      })
      const spyAI: AIBinding = { run: runSpy }
      const embeddings = new WorkersAIEmbeddings(spyAI, '@cf/custom/model')

      await embeddings.embedQuery('search query')

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/custom/model',
        expect.objectContaining({
          text: ['search query'],
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

      await embeddings.embedDocument('document content')

      expect(runSpy).toHaveBeenCalledWith(
        '@cf/baai/bge-m3',
        expect.objectContaining({
          text: ['document content'],
        })
      )
    })
  })

  describe('getModelConfig()', () => {
    it('returns config for known model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI)

      const config = embeddings.getModelConfig('@cf/baai/bge-base-en-v1.5')

      expect(config.model).toBe('@cf/baai/bge-base-en-v1.5')
      expect(config.dimensions).toBe(768)
      expect(config.normalize).toBe(true)
    })

    it('returns current model config when no argument', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-small-en-v1.5')

      const config = embeddings.getModelConfig()

      expect(config.model).toBe('@cf/baai/bge-small-en-v1.5')
      expect(config.dimensions).toBe(384)
    })

    it('returns default config for unknown model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/custom/model')

      const config = embeddings.getModelConfig()

      expect(config.dimensions).toBe(DEFAULT_DIMENSIONS)
    })
  })

  describe('property accessors', () => {
    it('model getter returns current model', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-large-en-v1.5')

      expect(embeddings.model).toBe('@cf/baai/bge-large-en-v1.5')
    })

    it('dimensions getter returns model dimensions', () => {
      const embeddings = new WorkersAIEmbeddings(mockAI, '@cf/baai/bge-small-en-v1.5')

      expect(embeddings.dimensions).toBe(384)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createEmbeddings()', () => {
  it('creates WorkersAIEmbeddings instance', () => {
    const mockAI = createMockAI()

    const embeddings = createEmbeddings(mockAI)

    expect(embeddings).toBeInstanceOf(WorkersAIEmbeddings)
  })

  it('uses default model', () => {
    const mockAI = createMockAI()

    const embeddings = createEmbeddings(mockAI)

    expect(embeddings.model).toBe(DEFAULT_MODEL)
  })

  it('accepts custom model', () => {
    const mockAI = createMockAI()

    const embeddings = createEmbeddings(mockAI, '@cf/baai/bge-large-en-v1.5')

    expect(embeddings.model).toBe('@cf/baai/bge-large-en-v1.5')
  })
})

describe('getModelDimensions()', () => {
  it('returns dimensions for bge-m3', () => {
    expect(getModelDimensions('@cf/baai/bge-m3')).toBe(1024)
  })

  it('returns dimensions for bge-base', () => {
    expect(getModelDimensions('@cf/baai/bge-base-en-v1.5')).toBe(768)
  })

  it('returns dimensions for bge-small', () => {
    expect(getModelDimensions('@cf/baai/bge-small-en-v1.5')).toBe(384)
  })

  it('returns dimensions for bge-large', () => {
    expect(getModelDimensions('@cf/baai/bge-large-en-v1.5')).toBe(1024)
  })

  it('returns default for unknown model', () => {
    expect(getModelDimensions('@cf/unknown/model')).toBe(DEFAULT_DIMENSIONS)
  })

  it('returns default when no model specified', () => {
    expect(getModelDimensions()).toBe(DEFAULT_DIMENSIONS)
  })
})

// =============================================================================
// Model Configuration Tests
// =============================================================================

describe('EMBEDDING_MODELS', () => {
  it('contains all expected models', () => {
    const expectedModels = [
      '@cf/baai/bge-m3',
      '@cf/baai/bge-base-en-v1.5',
      '@cf/baai/bge-small-en-v1.5',
      '@cf/baai/bge-large-en-v1.5',
    ]

    for (const model of expectedModels) {
      expect(EMBEDDING_MODELS).toHaveProperty(model)
    }
  })

  it('all models have required properties', () => {
    for (const [name, config] of Object.entries(EMBEDDING_MODELS)) {
      expect(config.model).toBe(name)
      expect(typeof config.dimensions).toBe('number')
      expect(config.dimensions).toBeGreaterThan(0)
      expect(typeof config.normalize).toBe('boolean')
    }
  })

  it('models have valid maxTokens', () => {
    for (const config of Object.values(EMBEDDING_MODELS)) {
      if (config.maxTokens !== undefined) {
        expect(config.maxTokens).toBeGreaterThan(0)
      }
    }
  })

  describe('specific model configurations', () => {
    it('bge-m3 has correct config', () => {
      const config = EMBEDDING_MODELS['@cf/baai/bge-m3']

      expect(config.dimensions).toBe(1024)
      expect(config.maxTokens).toBe(8192)
      expect(config.normalize).toBe(true)
    })

    it('bge-small has smallest dimensions', () => {
      const small = EMBEDDING_MODELS['@cf/baai/bge-small-en-v1.5']
      const others = Object.values(EMBEDDING_MODELS).filter(
        c => c.model !== '@cf/baai/bge-small-en-v1.5'
      )

      for (const other of others) {
        expect(small.dimensions).toBeLessThan(other.dimensions)
      }
    })
  })
})

// =============================================================================
// Constants Tests
// =============================================================================

describe('constants', () => {
  it('DEFAULT_MODEL is bge-m3', () => {
    expect(DEFAULT_MODEL).toBe('@cf/baai/bge-m3')
  })

  it('DEFAULT_DIMENSIONS matches bge-m3', () => {
    expect(DEFAULT_DIMENSIONS).toBe(1024)
    expect(DEFAULT_DIMENSIONS).toBe(EMBEDDING_MODELS['@cf/baai/bge-m3'].dimensions)
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('error handling', () => {
  it('handles null response from AI', async () => {
    const nullAI: AIBinding = {
      async run() {
        return null as unknown
      },
    }
    const embeddings = new WorkersAIEmbeddings(nullAI)

    await expect(embeddings.embed('Test')).rejects.toThrow()
  })

  it('handles undefined data in response', async () => {
    const undefinedAI: AIBinding = {
      async run() {
        return { shape: [1, 1024], data: undefined }
      },
    }
    const embeddings = new WorkersAIEmbeddings(undefinedAI)

    await expect(embeddings.embed('Test')).rejects.toThrow()
  })

  it('handles empty data array', async () => {
    const emptyAI: AIBinding = {
      async run() {
        return { shape: [0, 1024], data: [] }
      },
    }
    const embeddings = new WorkersAIEmbeddings(emptyAI)

    await expect(embeddings.embed('Test')).rejects.toThrow('Failed to generate embedding')
  })
})

// =============================================================================
// Vector Normalization Tests
// =============================================================================

describe('vector normalization', () => {
  it('normalizes vectors to unit length', async () => {
    // Create AI that returns non-normalized vectors
    const nonNormalizedAI: AIBinding = {
      async run<T = unknown>(): Promise<T> {
        const vector = [3, 4] // Magnitude = 5
        // Pad to full dimensions
        const fullVector = new Array(DEFAULT_DIMENSIONS).fill(0)
        fullVector[0] = 3
        fullVector[1] = 4
        return {
          shape: [1, DEFAULT_DIMENSIONS],
          data: [fullVector],
        } as T
      },
    }
    const embeddings = new WorkersAIEmbeddings(nonNormalizedAI)

    const vector = await embeddings.embed('Test')

    const magnitude = vectorMagnitude(vector)
    expect(magnitude).toBeCloseTo(1.0, 5)
  })

  it('handles zero vector gracefully', async () => {
    const zeroAI: AIBinding = {
      async run<T = unknown>(): Promise<T> {
        return {
          shape: [1, DEFAULT_DIMENSIONS],
          data: [new Array(DEFAULT_DIMENSIONS).fill(0)],
        } as T
      },
    }
    const embeddings = new WorkersAIEmbeddings(zeroAI)

    // Should not throw, returns zero vector as-is
    const vector = await embeddings.embed('Test')
    expect(vector.every(v => v === 0)).toBe(true)
  })
})
