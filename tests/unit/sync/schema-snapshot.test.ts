/**
 * Schema Snapshot Tests
 *
 * Tests for capturing and comparing schema snapshots
 */

import { describe, it, expect } from 'vitest'
import type { ParqueDBConfig } from '../../../src/config/loader'
import {
  captureSchema,
  diffSchemas,
  type SchemaSnapshot,
  type CollectionSchemaSnapshot
} from '../../../src/sync/schema-snapshot'

describe('Schema Snapshot', () => {
  describe('captureSchema', () => {
    it('should capture schema from config', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#',
            name: 'string',
            age: 'int?'
          },
          Post: {
            title: 'string!',
            content: 'text',
            author: '-> User'
          }
        }
      }

      const snapshot = await captureSchema(config)

      expect(snapshot).toBeDefined()
      expect(snapshot.hash).toBeDefined()
      expect(snapshot.configHash).toBeDefined()
      expect(snapshot.capturedAt).toBeGreaterThan(0)
      expect(Object.keys(snapshot.collections)).toHaveLength(2)
    })

    it('should handle empty schema', async () => {
      const config: ParqueDBConfig = {}

      const snapshot = await captureSchema(config)

      expect(snapshot.collections).toEqual({})
      expect(snapshot.hash).toBeDefined()
    })

    it('should skip flexible collections', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#'
          },
          Logs: 'flexible'
        }
      }

      const snapshot = await captureSchema(config)

      expect(Object.keys(snapshot.collections)).toHaveLength(1)
      expect(snapshot.collections.User).toBeDefined()
      expect(snapshot.collections.Logs).toBeUndefined()
    })

    it('should parse field definitions correctly', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#',     // required + indexed
            name: 'string?',       // optional
            verified: 'boolean!',  // required
            tags: 'string[]',      // array
            profile: '-> Profile'  // relationship
          }
        }
      }

      const snapshot = await captureSchema(config)
      const userFields = snapshot.collections.User?.fields

      expect(userFields).toBeDefined()
      expect(userFields).toHaveLength(5)

      const emailField = userFields?.find(f => f.name === 'email')
      expect(emailField?.required).toBe(true)
      expect(emailField?.indexed).toBe(true)

      const nameField = userFields?.find(f => f.name === 'name')
      expect(nameField?.required).toBe(false)

      const tagsField = userFields?.find(f => f.name === 'tags')
      expect(tagsField?.array).toBe(true)

      const profileField = userFields?.find(f => f.name === 'profile')
      expect(profileField?.relationship).toBeDefined()
      expect(profileField?.relationship?.target).toBe('Profile')
      expect(profileField?.relationship?.direction).toBe('outbound')
    })

    it('should parse reverse relationships', async () => {
      const config: ParqueDBConfig = {
        schema: {
          Post: {
            comments: '<- Comment.post'
          }
        }
      }

      const snapshot = await captureSchema(config)
      const commentsField = snapshot.collections.Post?.fields[0]

      expect(commentsField?.relationship).toBeDefined()
      expect(commentsField?.relationship?.target).toBe('Comment')
      expect(commentsField?.relationship?.reverse).toBe('post')
      expect(commentsField?.relationship?.direction).toBe('inbound')
    })

    it('should include collection options', async () => {
      const config: ParqueDBConfig = {
        schema: {
          Logs: {
            $options: { includeDataVariant: false },
            level: 'string',
            message: 'text'
          }
        }
      }

      const snapshot = await captureSchema(config)

      expect(snapshot.collections.Logs?.options).toBeDefined()
      expect(snapshot.collections.Logs?.options?.includeDataVariant).toBe(false)
    })
  })

  describe('diffSchemas', () => {
    it('should detect no changes when schemas are identical', () => {
      const schema1: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const schema2: SchemaSnapshot = {
        ...schema1,
        hash: 'abc',
        capturedAt: Date.now()
      }

      const diff = diffSchemas(schema1, schema2)

      expect(diff.changes).toHaveLength(0)
      expect(diff.compatible).toBe(true)
      expect(diff.summary).toBe('No schema changes')
    })

    it('should detect added collections', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {}
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      expect(diff.changes).toHaveLength(1)
      expect(diff.changes[0]?.type).toBe('ADD_COLLECTION')
      expect(diff.changes[0]?.collection).toBe('User')
      expect(diff.changes[0]?.breaking).toBe(false)
      expect(diff.compatible).toBe(true)
    })

    it('should detect removed collections as breaking', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {}
      }

      const diff = diffSchemas(before, after)

      expect(diff.changes).toHaveLength(1)
      expect(diff.changes[0]?.type).toBe('DROP_COLLECTION')
      expect(diff.changes[0]?.breaking).toBe(true)
      expect(diff.breakingChanges).toHaveLength(1)
      expect(diff.compatible).toBe(false)
    })

    it('should detect added fields', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false },
            { name: 'name', type: 'string', required: false, indexed: false, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      expect(diff.changes.length).toBeGreaterThan(0)
      const addField = diff.changes.find(c => c.type === 'ADD_FIELD')
      expect(addField).toBeDefined()
      expect(addField?.field).toBe('name')
      expect(addField?.breaking).toBe(false)
    })

    it('should detect required field addition as breaking', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false },
            { name: 'name', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      const addField = diff.changes.find(c => c.type === 'ADD_FIELD')
      expect(addField?.breaking).toBe(true)
      expect(diff.compatible).toBe(false)
    })

    it('should detect removed fields as breaking', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false },
            { name: 'name', type: 'string', required: false, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      const removeField = diff.changes.find(c => c.type === 'REMOVE_FIELD')
      expect(removeField).toBeDefined()
      expect(removeField?.breaking).toBe(true)
      expect(diff.compatible).toBe(false)
    })

    it('should detect type changes as breaking', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'age', type: 'string', required: false, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'age', type: 'int', required: false, indexed: false, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      const typeChange = diff.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
      expect(typeChange?.breaking).toBe(true)
      expect(diff.compatible).toBe(false)
    })

    it('should detect index additions as non-breaking', () => {
      const before: SchemaSnapshot = {
        hash: 'abc',
        configHash: 'def',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: false, unique: false, array: false }
          ])
        }
      }

      const after: SchemaSnapshot = {
        hash: 'xyz',
        configHash: 'uvw',
        capturedAt: Date.now(),
        collections: {
          User: createTestCollection('User', [
            { name: 'email', type: 'string!', required: true, indexed: true, unique: false, array: false }
          ])
        }
      }

      const diff = diffSchemas(before, after)

      const indexChange = diff.changes.find(c => c.type === 'ADD_INDEX')
      expect(indexChange).toBeDefined()
      expect(indexChange?.breaking).toBe(false)
      expect(diff.compatible).toBe(true)
    })
  })
})

/**
 * Helper to create test collection
 */
function createTestCollection(
  name: string,
  fields: Array<{
    name: string
    type: string
    required: boolean
    indexed: boolean
    unique: boolean
    array: boolean
  }>
): CollectionSchemaSnapshot {
  // Generate unique hash based on fields to make diffs work
  const fieldStr = JSON.stringify(fields)
  const hash = `hash-${name}-${fieldStr.length}`

  return {
    name,
    hash,
    version: 1,
    fields: fields.map(f => ({
      ...f,
      relationship: undefined
    }))
  }
}
