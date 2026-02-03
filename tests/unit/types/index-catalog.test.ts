/**
 * Tests for asIndexCatalog validation function
 *
 * Tests that the asIndexCatalog function properly validates
 * external data before returning it as a typed IndexCatalog.
 */

import { describe, it, expect } from 'vitest'
import { asIndexCatalog } from '../../../src/types/cast'

describe('asIndexCatalog()', () => {
  const validCatalog = {
    version: 1,
    indexes: {
      posts: [
        {
          definition: {
            name: 'idx_content_fts',
            type: 'fts',
            fields: [{ path: 'content' }],
          },
          metadata: {
            createdAt: '2024-01-15T10:00:00Z',
            updatedAt: '2024-01-15T10:00:00Z',
          },
        },
      ],
    },
  }

  it('should return valid IndexCatalog', () => {
    const result = asIndexCatalog(validCatalog)
    expect(result.version).toBe(1)
    expect(result.indexes.posts).toHaveLength(1)
    expect(result.indexes.posts[0].definition.name).toBe('idx_content_fts')
  })

  it('should accept empty indexes', () => {
    const catalog = { version: 1, indexes: {} }
    const result = asIndexCatalog(catalog)
    expect(result.version).toBe(1)
    expect(Object.keys(result.indexes)).toHaveLength(0)
  })

  it('should accept multiple namespaces', () => {
    const catalog = {
      version: 2,
      indexes: {
        posts: [
          {
            definition: { name: 'idx_fts', type: 'fts', fields: [{ path: 'content' }] },
            metadata: {},
          },
        ],
        users: [
          {
            definition: { name: 'idx_vector', type: 'vector', fields: [{ path: 'embedding' }] },
            metadata: {},
          },
        ],
      },
    }
    const result = asIndexCatalog(catalog)
    expect(Object.keys(result.indexes)).toHaveLength(2)
  })

  describe('version validation', () => {
    it('should throw for missing version', () => {
      const catalog = { indexes: {} }
      expect(() => asIndexCatalog(catalog)).toThrow("'version' must be a number")
    })

    it('should throw for non-number version', () => {
      expect(() => asIndexCatalog({ version: '1', indexes: {} })).toThrow(
        "'version' must be a number, got string"
      )
      expect(() => asIndexCatalog({ version: null, indexes: {} })).toThrow(
        "'version' must be a number"
      )
      expect(() => asIndexCatalog({ version: {}, indexes: {} })).toThrow(
        "'version' must be a number, got object"
      )
    })
  })

  describe('indexes validation', () => {
    it('should throw for missing indexes', () => {
      expect(() => asIndexCatalog({ version: 1 })).toThrow("'indexes' is required")
    })

    it('should throw for null indexes', () => {
      expect(() => asIndexCatalog({ version: 1, indexes: null })).toThrow(
        "'indexes' is required"
      )
    })

    it('should throw for array indexes (must be record)', () => {
      expect(() => asIndexCatalog({ version: 1, indexes: [] })).toThrow(
        "'indexes' must be a record object, got array"
      )
    })

    it('should throw for non-object indexes', () => {
      expect(() => asIndexCatalog({ version: 1, indexes: 'invalid' })).toThrow(
        "'indexes' must be a record object, got string"
      )
    })

    it('should throw for non-array namespace entries', () => {
      expect(() => asIndexCatalog({ version: 1, indexes: { posts: 'invalid' } })).toThrow(
        "indexes['posts'] must be an array"
      )
      expect(() => asIndexCatalog({ version: 1, indexes: { posts: {} } })).toThrow(
        "indexes['posts'] must be an array"
      )
    })
  })

  describe('entry validation', () => {
    it('should throw for non-object entry', () => {
      expect(() => asIndexCatalog({ version: 1, indexes: { posts: ['invalid'] } })).toThrow(
        "indexes['posts'][0] must be an object"
      )
      expect(() => asIndexCatalog({ version: 1, indexes: { posts: [null] } })).toThrow(
        "indexes['posts'][0] must be an object"
      )
    })

    it('should throw for missing definition', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: { posts: [{ metadata: {} }] },
      })).toThrow("indexes['posts'][0].definition must be an object")
    })

    it('should throw for invalid definition type', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: { posts: [{ definition: 'invalid', metadata: {} }] },
      })).toThrow("indexes['posts'][0].definition must be an object")
    })

    it('should throw for missing definition.name', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { type: 'fts', fields: [] },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.name must be a string")
    })

    it('should throw for non-string definition.name', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 123, type: 'fts', fields: [] },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.name must be a string")
    })

    it('should throw for missing definition.type', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', fields: [] },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.type must be a string")
    })

    it('should throw for non-string definition.type', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', type: 123, fields: [] },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.type must be a string")
    })

    it('should throw for missing definition.fields', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', type: 'fts' },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.fields must be an array")
    })

    it('should throw for non-array definition.fields', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', type: 'fts', fields: {} },
            metadata: {},
          }],
        },
      })).toThrow("indexes['posts'][0].definition.fields must be an array")
    })

    it('should throw for missing metadata', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', type: 'fts', fields: [] },
          }],
        },
      })).toThrow("indexes['posts'][0].metadata must be an object")
    })

    it('should throw for invalid metadata type', () => {
      expect(() => asIndexCatalog({
        version: 1,
        indexes: {
          posts: [{
            definition: { name: 'idx', type: 'fts', fields: [] },
            metadata: 'invalid',
          }],
        },
      })).toThrow("indexes['posts'][0].metadata must be an object")
    })
  })

  it('should validate multiple entries in a namespace', () => {
    const catalog = {
      version: 1,
      indexes: {
        posts: [
          {
            definition: { name: 'idx1', type: 'fts', fields: [] },
            metadata: {},
          },
          {
            definition: { name: 'idx2', type: 'vector' },  // missing fields
            metadata: {},
          },
        ],
      },
    }
    expect(() => asIndexCatalog(catalog)).toThrow(
      "indexes['posts'][1].definition.fields must be an array"
    )
  })
})
