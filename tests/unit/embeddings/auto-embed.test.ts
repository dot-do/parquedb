/**
 * Unit Tests for Auto-Embed Utilities
 *
 * Tests the pure utility functions from the auto-embed module.
 * These tests do not require AI binding mocks - they test the logic
 * for nested value access, operator detection, and configuration building.
 */

import { describe, it, expect } from 'vitest'
import {
  getNestedValue,
  setNestedValue,
  hasEmbedOperator,
  extractEmbedOperator,
  buildAutoEmbedConfig,
} from '../../../src/embeddings/auto-embed'

// =============================================================================
// getNestedValue Tests
// =============================================================================

describe('getNestedValue', () => {
  describe('simple paths', () => {
    it('returns value for top-level key', () => {
      const obj = { name: 'test', value: 42 }

      expect(getNestedValue(obj, 'name')).toBe('test')
      expect(getNestedValue(obj, 'value')).toBe(42)
    })

    it('returns undefined for missing key', () => {
      const obj = { name: 'test' }

      expect(getNestedValue(obj, 'missing')).toBeUndefined()
    })

    it('returns undefined for empty object', () => {
      expect(getNestedValue({}, 'any')).toBeUndefined()
    })
  })

  describe('nested paths', () => {
    it('returns value for nested object', () => {
      const obj = {
        user: {
          name: 'Alice',
          profile: {
            bio: 'Developer',
          },
        },
      }

      expect(getNestedValue(obj, 'user.name')).toBe('Alice')
      expect(getNestedValue(obj, 'user.profile.bio')).toBe('Developer')
    })

    it('returns undefined for partially missing path', () => {
      const obj = {
        user: {
          name: 'Alice',
        },
      }

      expect(getNestedValue(obj, 'user.profile.bio')).toBeUndefined()
    })

    it('returns undefined when intermediate value is null', () => {
      const obj = { user: null }

      expect(getNestedValue(obj as Record<string, unknown>, 'user.name')).toBeUndefined()
    })

    it('returns undefined when intermediate value is primitive', () => {
      const obj = { value: 42 }

      expect(getNestedValue(obj, 'value.nested')).toBeUndefined()
    })
  })

  describe('edge cases', () => {
    it('handles array values', () => {
      const obj = { items: [1, 2, 3] }

      expect(getNestedValue(obj, 'items')).toEqual([1, 2, 3])
    })

    it('handles boolean values', () => {
      const obj = { enabled: false, active: true }

      expect(getNestedValue(obj, 'enabled')).toBe(false)
      expect(getNestedValue(obj, 'active')).toBe(true)
    })

    it('handles null values', () => {
      const obj = { value: null }

      expect(getNestedValue(obj as Record<string, unknown>, 'value')).toBeNull()
    })

    it('handles empty string keys', () => {
      const obj = { '': 'empty key' }

      expect(getNestedValue(obj, '')).toBe('empty key')
    })

    it('handles deep nesting', () => {
      const obj = { a: { b: { c: { d: { e: 'deep' } } } } }

      expect(getNestedValue(obj, 'a.b.c.d.e')).toBe('deep')
    })
  })
})

// =============================================================================
// setNestedValue Tests
// =============================================================================

describe('setNestedValue', () => {
  describe('simple paths', () => {
    it('sets top-level key', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'name', 'test')

      expect(obj.name).toBe('test')
    })

    it('overwrites existing top-level key', () => {
      const obj: Record<string, unknown> = { name: 'old' }

      setNestedValue(obj, 'name', 'new')

      expect(obj.name).toBe('new')
    })
  })

  describe('nested paths', () => {
    it('sets value in nested object', () => {
      const obj: Record<string, unknown> = {
        user: { name: 'Alice' },
      }

      setNestedValue(obj, 'user.email', 'alice@example.com')

      expect((obj.user as Record<string, unknown>).email).toBe('alice@example.com')
    })

    it('creates intermediate objects when missing', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'user.profile.bio', 'Developer')

      expect(obj.user).toBeDefined()
      expect((obj.user as Record<string, unknown>).profile).toBeDefined()
      expect(
        ((obj.user as Record<string, unknown>).profile as Record<string, unknown>).bio
      ).toBe('Developer')
    })

    it('overwrites null intermediate values', () => {
      const obj: Record<string, unknown> = { user: null }

      setNestedValue(obj, 'user.name', 'Alice')

      expect((obj.user as Record<string, unknown>).name).toBe('Alice')
    })

    it('overwrites undefined intermediate values', () => {
      const obj: Record<string, unknown> = { user: undefined }

      setNestedValue(obj, 'user.name', 'Alice')

      expect((obj.user as Record<string, unknown>).name).toBe('Alice')
    })
  })

  describe('value types', () => {
    it('sets array values', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'items', [1, 2, 3])

      expect(obj.items).toEqual([1, 2, 3])
    })

    it('sets number values', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'count', 42)
      setNestedValue(obj, 'rate', 3.14)

      expect(obj.count).toBe(42)
      expect(obj.rate).toBe(3.14)
    })

    it('sets boolean values', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'enabled', true)
      setNestedValue(obj, 'disabled', false)

      expect(obj.enabled).toBe(true)
      expect(obj.disabled).toBe(false)
    })

    it('sets null values', () => {
      const obj: Record<string, unknown> = { value: 'something' }

      setNestedValue(obj, 'value', null)

      expect(obj.value).toBeNull()
    })

    it('sets object values', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'config', { key: 'value' })

      expect(obj.config).toEqual({ key: 'value' })
    })
  })

  describe('edge cases', () => {
    it('handles deep nesting', () => {
      const obj: Record<string, unknown> = {}

      setNestedValue(obj, 'a.b.c.d.e', 'deep')

      expect(getNestedValue(obj, 'a.b.c.d.e')).toBe('deep')
    })

    it('preserves existing sibling values', () => {
      const obj: Record<string, unknown> = {
        user: {
          name: 'Alice',
          email: 'alice@example.com',
        },
      }

      setNestedValue(obj, 'user.bio', 'Developer')

      expect((obj.user as Record<string, unknown>).name).toBe('Alice')
      expect((obj.user as Record<string, unknown>).email).toBe('alice@example.com')
      expect((obj.user as Record<string, unknown>).bio).toBe('Developer')
    })
  })
})

// =============================================================================
// hasEmbedOperator Tests
// =============================================================================

describe('hasEmbedOperator', () => {
  describe('positive cases', () => {
    it('returns true when $embed is present with string config', () => {
      const update = { $embed: { description: 'embedding' } }

      expect(hasEmbedOperator(update)).toBe(true)
    })

    it('returns true when $embed is present with object config', () => {
      const update = {
        $embed: {
          description: { field: 'embedding', model: '@cf/baai/bge-m3' },
        },
      }

      expect(hasEmbedOperator(update)).toBe(true)
    })

    it('returns true when $embed is present alongside other operators', () => {
      const update = {
        $set: { name: 'test' },
        $embed: { description: 'embedding' },
        $inc: { count: 1 },
      }

      expect(hasEmbedOperator(update)).toBe(true)
    })

    it('returns true when $embed is empty object', () => {
      const update = { $embed: {} }

      expect(hasEmbedOperator(update)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('returns false when $embed is missing', () => {
      const update = { $set: { name: 'test' } }

      expect(hasEmbedOperator(update)).toBe(false)
    })

    it('returns false when $embed is undefined', () => {
      const update = { $embed: undefined }

      expect(hasEmbedOperator(update)).toBe(false)
    })

    it('returns false for empty update object', () => {
      expect(hasEmbedOperator({})).toBe(false)
    })

    it('returns false when similar property name exists', () => {
      const update = { embed: { description: 'embedding' } } // Missing $

      expect(hasEmbedOperator(update)).toBe(false)
    })
  })
})

// =============================================================================
// extractEmbedOperator Tests
// =============================================================================

describe('extractEmbedOperator', () => {
  describe('extraction', () => {
    it('extracts $embed and removes it from update', () => {
      const update: Record<string, unknown> = {
        $set: { name: 'test' },
        $embed: { description: 'embedding' },
      }

      const embedConfig = extractEmbedOperator(update)

      expect(embedConfig).toEqual({ description: 'embedding' })
      expect(update.$embed).toBeUndefined()
      expect(update.$set).toEqual({ name: 'test' })
    })

    it('extracts complex $embed config', () => {
      const update: Record<string, unknown> = {
        $embed: {
          description: { field: 'embedding', model: '@cf/baai/bge-m3' },
          title: 'title_embedding',
        },
      }

      const embedConfig = extractEmbedOperator(update)

      expect(embedConfig).toEqual({
        description: { field: 'embedding', model: '@cf/baai/bge-m3' },
        title: 'title_embedding',
      })
    })

    it('returns extracted config unchanged', () => {
      const originalConfig = { description: 'embedding' }
      const update: Record<string, unknown> = { $embed: originalConfig }

      const embedConfig = extractEmbedOperator(update)

      expect(embedConfig).toBe(originalConfig) // Same reference
    })
  })

  describe('no extraction', () => {
    it('returns undefined when $embed is missing', () => {
      const update: Record<string, unknown> = { $set: { name: 'test' } }

      const embedConfig = extractEmbedOperator(update)

      expect(embedConfig).toBeUndefined()
    })

    it('returns undefined when $embed is undefined', () => {
      const update: Record<string, unknown> = { $embed: undefined }

      const embedConfig = extractEmbedOperator(update)

      expect(embedConfig).toBeUndefined()
    })

    it('does not modify update when $embed is missing', () => {
      const update: Record<string, unknown> = { $set: { name: 'test' } }
      const originalKeys = Object.keys(update)

      extractEmbedOperator(update)

      expect(Object.keys(update)).toEqual(originalKeys)
    })
  })
})

// =============================================================================
// buildAutoEmbedConfig Tests
// =============================================================================

describe('buildAutoEmbedConfig', () => {
  describe('explicit source field', () => {
    it('builds config from vector index with sourceField', () => {
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

    it('builds config from multiple indexes', () => {
      const vectorIndexes = [
        {
          name: 'idx_desc_embedding',
          fields: [{ path: 'desc_embedding' }],
          sourceField: 'description',
        },
        {
          name: 'idx_title_embedding',
          fields: [{ path: 'title_embedding' }],
          sourceField: 'title',
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(2)
      expect(config.fields[0]).toEqual({
        sourceField: 'description',
        targetField: 'desc_embedding',
        overwrite: true,
      })
      expect(config.fields[1]).toEqual({
        sourceField: 'title',
        targetField: 'title_embedding',
        overwrite: true,
      })
    })
  })

  describe('inferred source field', () => {
    it('infers source from _embedding suffix', () => {
      const vectorIndexes = [
        {
          name: 'idx_desc',
          fields: [{ path: 'description_embedding' }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(1)
      expect(config.fields[0].sourceField).toBe('description')
      expect(config.fields[0].targetField).toBe('description_embedding')
    })

    it('uses "description" as default for "embedding" target', () => {
      const vectorIndexes = [
        {
          name: 'idx_embedding',
          fields: [{ path: 'embedding' }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(1)
      expect(config.fields[0].sourceField).toBe('description')
      expect(config.fields[0].targetField).toBe('embedding')
    })

    it('infers from complex _embedding suffix', () => {
      const vectorIndexes = [
        {
          name: 'idx_content',
          fields: [{ path: 'body_content_embedding' }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(1)
      expect(config.fields[0].sourceField).toBe('body_content')
    })
  })

  describe('skipping non-inferable indexes', () => {
    it('skips indexes where source cannot be inferred', () => {
      const vectorIndexes = [
        {
          name: 'idx_vec',
          fields: [{ path: 'some_vector' }],
        },
        {
          name: 'idx_data',
          fields: [{ path: 'vector_data' }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(0)
    })

    it('skips indexes without fields', () => {
      const vectorIndexes = [
        {
          name: 'idx_empty',
          fields: [],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(0)
    })

    it('includes only inferable indexes from mixed input', () => {
      const vectorIndexes = [
        {
          name: 'idx_valid',
          fields: [{ path: 'embedding' }],
        },
        {
          name: 'idx_invalid',
          fields: [{ path: 'some_vector' }],
        },
        {
          name: 'idx_explicit',
          fields: [{ path: 'custom_vec' }],
          sourceField: 'content',
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(2)
      expect(config.fields.map(f => f.targetField)).toEqual(['embedding', 'custom_vec'])
    })
  })

  describe('edge cases', () => {
    it('returns empty config for empty input', () => {
      const config = buildAutoEmbedConfig([])

      expect(config.fields).toEqual([])
    })

    it('handles undefined fields in index', () => {
      const vectorIndexes = [
        {
          name: 'idx_test',
          fields: [{ path: undefined as unknown as string }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields).toHaveLength(0)
    })

    it('sets overwrite to true by default', () => {
      const vectorIndexes = [
        {
          name: 'idx_test',
          fields: [{ path: 'embedding' }],
        },
      ]

      const config = buildAutoEmbedConfig(vectorIndexes)

      expect(config.fields[0].overwrite).toBe(true)
    })
  })
})

// =============================================================================
// Integration of utility functions
// =============================================================================

describe('utility function integration', () => {
  it('getNestedValue and setNestedValue work together', () => {
    const obj: Record<string, unknown> = {}

    setNestedValue(obj, 'deeply.nested.value', 'test')

    expect(getNestedValue(obj, 'deeply.nested.value')).toBe('test')
  })

  it('can update existing nested values', () => {
    const obj: Record<string, unknown> = {
      config: {
        settings: {
          theme: 'dark',
        },
      },
    }

    setNestedValue(obj, 'config.settings.theme', 'light')

    expect(getNestedValue(obj, 'config.settings.theme')).toBe('light')
  })

  it('extract and has operators work together', () => {
    const update: Record<string, unknown> = {
      $set: { name: 'test' },
      $embed: { description: 'embedding' },
    }

    expect(hasEmbedOperator(update)).toBe(true)

    const embedConfig = extractEmbedOperator(update)

    expect(embedConfig).toBeDefined()
    expect(hasEmbedOperator(update)).toBe(false)
  })
})
