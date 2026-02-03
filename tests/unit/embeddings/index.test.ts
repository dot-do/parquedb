/**
 * Unit Tests for Embeddings Module Exports
 *
 * Verifies that all expected exports are available from the embeddings module.
 */

import { describe, it, expect } from 'vitest'
import * as embeddings from '../../../src/embeddings'

// =============================================================================
// Module Export Tests
// =============================================================================

describe('embeddings module exports', () => {
  describe('workers-ai exports', () => {
    it('exports WorkersAIEmbeddings class', () => {
      expect(embeddings.WorkersAIEmbeddings).toBeDefined()
      expect(typeof embeddings.WorkersAIEmbeddings).toBe('function')
    })

    it('exports createEmbeddings factory function', () => {
      expect(embeddings.createEmbeddings).toBeDefined()
      expect(typeof embeddings.createEmbeddings).toBe('function')
    })

    it('exports getModelDimensions function', () => {
      expect(embeddings.getModelDimensions).toBeDefined()
      expect(typeof embeddings.getModelDimensions).toBe('function')
    })

    it('exports DEFAULT_MODEL constant', () => {
      expect(embeddings.DEFAULT_MODEL).toBeDefined()
      expect(typeof embeddings.DEFAULT_MODEL).toBe('string')
      expect(embeddings.DEFAULT_MODEL).toBe('@cf/baai/bge-m3')
    })

    it('exports DEFAULT_DIMENSIONS constant', () => {
      expect(embeddings.DEFAULT_DIMENSIONS).toBeDefined()
      expect(typeof embeddings.DEFAULT_DIMENSIONS).toBe('number')
      expect(embeddings.DEFAULT_DIMENSIONS).toBe(1024)
    })

    it('exports EMBEDDING_MODELS configuration', () => {
      expect(embeddings.EMBEDDING_MODELS).toBeDefined()
      expect(typeof embeddings.EMBEDDING_MODELS).toBe('object')
      expect(Object.keys(embeddings.EMBEDDING_MODELS).length).toBeGreaterThan(0)
    })
  })

  describe('auto-embed exports', () => {
    it('exports processEmbedOperator function', () => {
      expect(embeddings.processEmbedOperator).toBeDefined()
      expect(typeof embeddings.processEmbedOperator).toBe('function')
    })

    it('exports autoEmbedFields function', () => {
      expect(embeddings.autoEmbedFields).toBeDefined()
      expect(typeof embeddings.autoEmbedFields).toBe('function')
    })

    it('exports hasEmbedOperator function', () => {
      expect(embeddings.hasEmbedOperator).toBeDefined()
      expect(typeof embeddings.hasEmbedOperator).toBe('function')
    })

    it('exports extractEmbedOperator function', () => {
      expect(embeddings.extractEmbedOperator).toBeDefined()
      expect(typeof embeddings.extractEmbedOperator).toBe('function')
    })

    it('exports buildAutoEmbedConfig function', () => {
      expect(embeddings.buildAutoEmbedConfig).toBeDefined()
      expect(typeof embeddings.buildAutoEmbedConfig).toBe('function')
    })

    it('exports getNestedValue function', () => {
      expect(embeddings.getNestedValue).toBeDefined()
      expect(typeof embeddings.getNestedValue).toBe('function')
    })

    it('exports setNestedValue function', () => {
      expect(embeddings.setNestedValue).toBeDefined()
      expect(typeof embeddings.setNestedValue).toBe('function')
    })
  })
})

// =============================================================================
// Type Export Verification
// =============================================================================

describe('type exports usage', () => {
  it('AIBinding type can be used', () => {
    // Type-only test - if this compiles, the type is exported correctly
    const mockAI: embeddings.AIBinding = {
      run: async () => ({ shape: [], data: [] }),
    }
    expect(mockAI).toBeDefined()
  })

  it('EmbeddingModelConfig type structure', () => {
    // Verify the structure matches expected shape
    const config: embeddings.EmbeddingModelConfig = {
      model: 'test',
      dimensions: 512,
      maxTokens: 1000,
      normalize: true,
    }
    expect(config.model).toBe('test')
    expect(config.dimensions).toBe(512)
  })

  it('EmbedOptions type structure', () => {
    const options: embeddings.EmbedOptions = {
      model: '@cf/baai/bge-small-en-v1.5',
      raw: true,
    }
    expect(options.model).toBe('@cf/baai/bge-small-en-v1.5')
    expect(options.raw).toBe(true)
  })

  it('AutoEmbedFieldConfig type structure', () => {
    const config: embeddings.AutoEmbedFieldConfig = {
      sourceField: 'description',
      targetField: 'embedding',
      model: '@cf/baai/bge-m3',
      overwrite: true,
    }
    expect(config.sourceField).toBe('description')
    expect(config.targetField).toBe('embedding')
  })

  it('AutoEmbedConfig type structure', () => {
    const config: embeddings.AutoEmbedConfig = {
      fields: [
        {
          sourceField: 'description',
          targetField: 'embedding',
        },
      ],
      defaultModel: '@cf/baai/bge-m3',
    }
    expect(config.fields.length).toBe(1)
    expect(config.defaultModel).toBe('@cf/baai/bge-m3')
  })

  it('ProcessEmbeddingsOptions type structure', () => {
    const mockAI: embeddings.AIBinding = {
      run: async () => ({ shape: [], data: [] }),
    }
    const options: embeddings.ProcessEmbeddingsOptions = {
      ai: mockAI,
      model: '@cf/baai/bge-m3',
      skipExisting: true,
    }
    expect(options.ai).toBeDefined()
    expect(options.model).toBe('@cf/baai/bge-m3')
    expect(options.skipExisting).toBe(true)
  })
})

// =============================================================================
// Functional Export Verification
// =============================================================================

describe('exported functions are callable', () => {
  it('createEmbeddings creates an instance', () => {
    const mockAI: embeddings.AIBinding = {
      run: async () => ({ shape: [1, 1024], data: [[]] }),
    }

    const instance = embeddings.createEmbeddings(mockAI)

    expect(instance).toBeInstanceOf(embeddings.WorkersAIEmbeddings)
  })

  it('getModelDimensions returns number', () => {
    const dims = embeddings.getModelDimensions()

    expect(typeof dims).toBe('number')
    expect(dims).toBeGreaterThan(0)
  })

  it('hasEmbedOperator works correctly', () => {
    expect(embeddings.hasEmbedOperator({ $embed: {} })).toBe(true)
    expect(embeddings.hasEmbedOperator({ $set: {} })).toBe(false)
  })

  it('extractEmbedOperator works correctly', () => {
    const update = { $embed: { field: 'target' }, $set: {} }
    const extracted = embeddings.extractEmbedOperator(update)

    expect(extracted).toEqual({ field: 'target' })
    expect(update.$embed).toBeUndefined()
  })

  it('buildAutoEmbedConfig returns config object', () => {
    const config = embeddings.buildAutoEmbedConfig([])

    expect(config).toHaveProperty('fields')
    expect(Array.isArray(config.fields)).toBe(true)
  })

  it('getNestedValue retrieves values', () => {
    const obj = { a: { b: 'value' } }

    expect(embeddings.getNestedValue(obj, 'a.b')).toBe('value')
  })

  it('setNestedValue sets values', () => {
    const obj: Record<string, unknown> = {}

    embeddings.setNestedValue(obj, 'a.b', 'value')

    expect(embeddings.getNestedValue(obj, 'a.b')).toBe('value')
  })
})
