/**
 * Unit Tests for Vercel AI SDK Embeddings
 *
 * Tests the AI SDK embedding wrapper with mocked providers.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  AISDKEmbeddings,
  createAISDKEmbeddings,
  getAISDKModelDimensions,
  listAISDKModels,
  AI_SDK_MODELS,
  DEFAULT_AI_SDK_DIMENSIONS,
  type EmbeddingProvider,
  type AISDKProvider,
  type AISDKEmbeddingsConfig,
} from '../../../src/embeddings/ai-sdk'

// =============================================================================
// Mock AI SDK
// =============================================================================

// Mock the AI SDK modules
vi.mock('ai', () => ({
  embedMany: vi.fn(),
}))

vi.mock('@ai-sdk/openai', () => ({
  openai: vi.fn(),
  createOpenAI: vi.fn(),
}))

vi.mock('@ai-sdk/cohere', () => ({
  cohere: vi.fn(),
}))

vi.mock('@ai-sdk/mistral', () => ({
  mistral: vi.fn(),
}))

vi.mock('@ai-sdk/google', () => ({
  google: vi.fn(),
}))

vi.mock('@ai-sdk/amazon-bedrock', () => ({
  amazon: vi.fn(),
}))

/**
 * Create mock embedMany response
 */
function createMockEmbedMany(dimensions: number = 1536) {
  return async ({ values }: { values: string[] }) => ({
    embeddings: values.map(() => generateDeterministicEmbedding(dimensions)),
  })
}

/**
 * Create mock provider with textEmbeddingModel
 */
function createMockProvider(dimensions: number = 1536) {
  return () => ({
    textEmbeddingModel: () => ({
      modelId: 'test-model',
    }),
  })
}

/**
 * Generate deterministic embedding for testing
 */
function generateDeterministicEmbedding(dimensions: number): number[] {
  const vector: number[] = []
  for (let i = 0; i < dimensions; i++) {
    vector.push(Math.sin(i) * 0.5 + 0.5)
  }
  return vector
}

/**
 * Generate hash-based deterministic embedding
 */
function generateHashEmbedding(text: string, dimensions: number): number[] {
  let hash = 0
  for (let i = 0; i < text.length; i++) {
    const char = text.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash
  }

  const vector: number[] = []
  const seed = Math.abs(hash)
  for (let i = 0; i < dimensions; i++) {
    const x = Math.sin(seed * (i + 1)) * 10000
    vector.push(x - Math.floor(x))
  }

  return vector
}

// =============================================================================
// Tests
// =============================================================================

describe('AISDKEmbeddings', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('constructor', () => {
    it('creates instance with OpenAI provider', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'openai',
        model: 'text-embedding-3-small',
        apiKey: 'test-key',
      })

      expect(embeddings.provider).toBe('openai')
      expect(embeddings.model).toBe('text-embedding-3-small')
      expect(embeddings.dimensions).toBe(1536)
    })

    it('creates instance with Cohere provider', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'cohere',
        model: 'embed-english-v3.0',
        apiKey: 'test-key',
      })

      expect(embeddings.provider).toBe('cohere')
      expect(embeddings.model).toBe('embed-english-v3.0')
      expect(embeddings.dimensions).toBe(1024)
    })

    it('uses default dimensions for unknown model', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'openai',
        model: 'unknown-model',
        apiKey: 'test-key',
      })

      expect(embeddings.dimensions).toBe(DEFAULT_AI_SDK_DIMENSIONS)
    })

    it('allows dimension override', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'openai',
        model: 'text-embedding-3-small',
        apiKey: 'test-key',
        dimensions: 512,
      })

      expect(embeddings.dimensions).toBe(512)
    })

    it('normalizes vectors by default', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'openai',
        model: 'text-embedding-3-small',
        apiKey: 'test-key',
      })

      // Normalization is an internal config, tested via output
      expect(embeddings).toBeInstanceOf(AISDKEmbeddings)
    })
  })

  describe('custom provider', () => {
    it('works with custom embedding function', async () => {
      const customDimensions = 768
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(customDimensions))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'my-model',
        dimensions: customDimensions,
        customEmbedFn,
      })

      const vector = await embeddings.embed('Hello, world!')

      expect(customEmbedFn).toHaveBeenCalledWith(['Hello, world!'])
      expect(vector.length).toBe(customDimensions)
    })

    it('handles batch with custom function', async () => {
      const customDimensions = 768
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(customDimensions))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'my-model',
        dimensions: customDimensions,
        customEmbedFn,
      })

      const texts = ['First', 'Second', 'Third']
      const vectors = await embeddings.embedBatch(texts)

      expect(customEmbedFn).toHaveBeenCalledWith(texts)
      expect(vectors.length).toBe(3)
      expect(vectors.every(v => v.length === customDimensions)).toBe(true)
    })

    it('throws error without customEmbedFn', async () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'my-model',
        dimensions: 768,
      })

      await expect(embeddings.embed('test')).rejects.toThrow('Custom provider requires customEmbedFn')
    })
  })

  describe('embed()', () => {
    it('returns single embedding vector', async () => {
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(768))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 768,
        customEmbedFn,
      })

      const vector = await embeddings.embed('Test text')

      expect(Array.isArray(vector)).toBe(true)
      expect(vector.length).toBe(768)
      expect(vector.every(v => typeof v === 'number')).toBe(true)
    })

    it('normalizes vectors by default', async () => {
      // Create non-normalized vectors
      const rawVector = [1, 2, 3, 4, 5]
      const customEmbedFn = vi.fn().mockResolvedValue([rawVector])

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 5,
        normalize: true,
        customEmbedFn,
      })

      const vector = await embeddings.embed('Test')

      // Check normalization - magnitude should be ~1
      const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
      expect(magnitude).toBeCloseTo(1.0, 5)
    })

    it('returns raw vectors when requested', async () => {
      const rawVector = [1, 2, 3, 4, 5]
      const customEmbedFn = vi.fn().mockResolvedValue([rawVector])

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 5,
        normalize: true,
        customEmbedFn,
      })

      const vector = await embeddings.embed('Test', { raw: true })

      expect(vector).toEqual(rawVector)
    })
  })

  describe('embedBatch()', () => {
    it('returns multiple embedding vectors', async () => {
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(768))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 768,
        customEmbedFn,
      })

      const texts = ['First', 'Second', 'Third']
      const vectors = await embeddings.embedBatch(texts)

      expect(vectors.length).toBe(3)
      expect(vectors.every(v => v.length === 768)).toBe(true)
    })

    it('returns empty array for empty input', async () => {
      const customEmbedFn = vi.fn()

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 768,
        customEmbedFn,
      })

      const vectors = await embeddings.embedBatch([])

      expect(vectors).toEqual([])
      expect(customEmbedFn).not.toHaveBeenCalled()
    })
  })

  describe('embedQuery() and embedDocument()', () => {
    it('embedQuery works like embed', async () => {
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(768))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 768,
        customEmbedFn,
      })

      const vector = await embeddings.embedQuery('search query')

      expect(customEmbedFn).toHaveBeenCalledWith(['search query'])
      expect(vector.length).toBe(768)
    })

    it('embedDocument works like embed', async () => {
      const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
        texts.map(() => generateDeterministicEmbedding(768))
      )

      const embeddings = new AISDKEmbeddings({
        provider: 'custom',
        model: 'test',
        dimensions: 768,
        customEmbedFn,
      })

      const vector = await embeddings.embedDocument('document content')

      expect(customEmbedFn).toHaveBeenCalledWith(['document content'])
      expect(vector.length).toBe(768)
    })
  })

  describe('getModelConfig()', () => {
    it('returns model configuration', () => {
      const embeddings = new AISDKEmbeddings({
        provider: 'openai',
        model: 'text-embedding-3-small',
        apiKey: 'test-key',
      })

      const config = embeddings.getModelConfig()

      expect(config.dimensions).toBe(1536)
      expect(config.maxBatchSize).toBe(2048)
    })
  })
})

describe('createAISDKEmbeddings()', () => {
  it('creates AISDKEmbeddings instance', () => {
    const embeddings = createAISDKEmbeddings({
      provider: 'openai',
      model: 'text-embedding-3-small',
      apiKey: 'test-key',
    })

    expect(embeddings).toBeInstanceOf(AISDKEmbeddings)
  })

  it('passes configuration to constructor', () => {
    const embeddings = createAISDKEmbeddings({
      provider: 'cohere',
      model: 'embed-english-v3.0',
      apiKey: 'test-key',
      dimensions: 512,
    })

    expect(embeddings.provider).toBe('cohere')
    expect(embeddings.dimensions).toBe(512)
  })
})

describe('getAISDKModelDimensions()', () => {
  it('returns dimensions for OpenAI models', () => {
    expect(getAISDKModelDimensions('openai', 'text-embedding-3-small')).toBe(1536)
    expect(getAISDKModelDimensions('openai', 'text-embedding-3-large')).toBe(3072)
    expect(getAISDKModelDimensions('openai', 'text-embedding-ada-002')).toBe(1536)
  })

  it('returns dimensions for Cohere models', () => {
    expect(getAISDKModelDimensions('cohere', 'embed-english-v3.0')).toBe(1024)
    expect(getAISDKModelDimensions('cohere', 'embed-english-light-v3.0')).toBe(384)
  })

  it('returns dimensions for Mistral models', () => {
    expect(getAISDKModelDimensions('mistral', 'mistral-embed')).toBe(1024)
  })

  it('returns dimensions for Google models', () => {
    expect(getAISDKModelDimensions('google', 'text-embedding-004')).toBe(768)
  })

  it('returns dimensions for Voyage models', () => {
    expect(getAISDKModelDimensions('voyage', 'voyage-3')).toBe(1024)
    expect(getAISDKModelDimensions('voyage', 'voyage-3-lite')).toBe(512)
  })

  it('returns default dimensions for unknown model', () => {
    expect(getAISDKModelDimensions('openai', 'unknown')).toBe(DEFAULT_AI_SDK_DIMENSIONS)
  })

  it('returns default dimensions for unknown provider', () => {
    expect(getAISDKModelDimensions('unknown' as AISDKProvider, 'model')).toBe(DEFAULT_AI_SDK_DIMENSIONS)
  })
})

describe('listAISDKModels()', () => {
  it('lists OpenAI models', () => {
    const models = listAISDKModels('openai')

    expect(models).toContain('text-embedding-3-small')
    expect(models).toContain('text-embedding-3-large')
    expect(models).toContain('text-embedding-ada-002')
  })

  it('lists Cohere models', () => {
    const models = listAISDKModels('cohere')

    expect(models).toContain('embed-english-v3.0')
    expect(models).toContain('embed-multilingual-v3.0')
  })

  it('returns empty array for unknown provider', () => {
    const models = listAISDKModels('unknown' as AISDKProvider)

    expect(models).toEqual([])
  })
})

describe('AI_SDK_MODELS', () => {
  it('contains OpenAI models', () => {
    expect(AI_SDK_MODELS.openai).toBeDefined()
    expect(AI_SDK_MODELS.openai['text-embedding-3-small']).toBeDefined()
  })

  it('contains Cohere models', () => {
    expect(AI_SDK_MODELS.cohere).toBeDefined()
    expect(AI_SDK_MODELS.cohere['embed-english-v3.0']).toBeDefined()
  })

  it('all models have required properties', () => {
    for (const [providerName, models] of Object.entries(AI_SDK_MODELS)) {
      for (const [modelName, config] of Object.entries(models)) {
        expect(typeof config.dimensions).toBe('number')
        expect(config.dimensions).toBeGreaterThan(0)
      }
    }
  })
})

describe('EmbeddingProvider interface', () => {
  it('AISDKEmbeddings implements EmbeddingProvider', () => {
    const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
      texts.map(() => generateDeterministicEmbedding(768))
    )

    const embeddings: EmbeddingProvider = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
    })

    // Check interface methods exist
    expect(typeof embeddings.embed).toBe('function')
    expect(typeof embeddings.embedBatch).toBe('function')
    expect(typeof embeddings.dimensions).toBe('number')
  })

  it('can be used polymorphically', async () => {
    const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
      texts.map(() => generateDeterministicEmbedding(768))
    )

    async function useEmbedder(embedder: EmbeddingProvider, text: string): Promise<number[]> {
      return embedder.embed(text)
    }

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
    })

    const vector = await useEmbedder(embeddings, 'test')
    expect(vector.length).toBe(768)
  })
})

describe('Vector Operations', () => {
  it('generates consistent embeddings for same input', async () => {
    const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
      texts.map(text => generateHashEmbedding(text, 768))
    )

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
      normalize: false, // Disable normalization to test raw consistency
    })

    const v1 = await embeddings.embed('Same text')
    const v2 = await embeddings.embed('Same text')

    expect(v1).toEqual(v2)
  })

  it('generates different embeddings for different input', async () => {
    const customEmbedFn = vi.fn().mockImplementation(async (texts: string[]) =>
      texts.map(text => generateHashEmbedding(text, 768))
    )

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
      normalize: false,
    })

    const v1 = await embeddings.embed('First text')
    const v2 = await embeddings.embed('Different text')

    expect(v1).not.toEqual(v2)
  })
})

describe('Error Handling', () => {
  it('handles empty response', async () => {
    const customEmbedFn = vi.fn().mockResolvedValue([])

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
    })

    await expect(embeddings.embed('test')).rejects.toThrow('Failed to generate embedding')
  })

  it('propagates errors from custom function', async () => {
    const customEmbedFn = vi.fn().mockRejectedValue(new Error('Custom error'))

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 768,
      customEmbedFn,
    })

    await expect(embeddings.embed('test')).rejects.toThrow('Custom error')
  })

  it('throws for unsupported provider', async () => {
    const embeddings = new AISDKEmbeddings({
      provider: 'unsupported' as AISDKProvider,
      model: 'test',
      apiKey: 'test-key',
    })

    await expect(embeddings.embed('test')).rejects.toThrow('Unsupported provider')
  })
})

describe('Configuration Options', () => {
  it('accepts baseURL override', () => {
    const embeddings = new AISDKEmbeddings({
      provider: 'openai',
      model: 'text-embedding-3-small',
      apiKey: 'test-key',
      baseURL: 'https://custom.api.com/v1',
    })

    expect(embeddings).toBeInstanceOf(AISDKEmbeddings)
  })

  it('can disable normalization', async () => {
    const rawVector = [1, 2, 3, 4, 5]
    const customEmbedFn = vi.fn().mockResolvedValue([rawVector])

    const embeddings = new AISDKEmbeddings({
      provider: 'custom',
      model: 'test',
      dimensions: 5,
      normalize: false,
      customEmbedFn,
    })

    const vector = await embeddings.embed('test')

    expect(vector).toEqual(rawVector)
  })
})
