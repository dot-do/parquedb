/**
 * TypeScript Code Generator Tests
 */

import { describe, it, expect } from 'vitest'
import { generateTypeScript, mapType } from '../../../src/codegen/typescript'
import type { SchemaSnapshot } from '../../../src/sync/schema-snapshot'

describe('TypeScript Code Generator', () => {
  describe('mapType', () => {
    it('should map string types', () => {
      expect(mapType('string')).toBe('string')
      expect(mapType('string!')).toBe('string')
      expect(mapType('string?')).toBe('string')
      expect(mapType('text')).toBe('string')
    })

    it('should map numeric types', () => {
      expect(mapType('int')).toBe('number')
      expect(mapType('integer')).toBe('number')
      expect(mapType('float')).toBe('number')
      expect(mapType('double')).toBe('number')
      expect(mapType('number')).toBe('number')
    })

    it('should map boolean types', () => {
      expect(mapType('boolean')).toBe('boolean')
      expect(mapType('bool')).toBe('boolean')
    })

    it('should map date types', () => {
      expect(mapType('date')).toBe('Date')
      expect(mapType('datetime')).toBe('Date')
      expect(mapType('timestamp')).toBe('Date')
    })

    it('should map unknown types', () => {
      expect(mapType('json')).toBe('unknown')
      expect(mapType('variant')).toBe('unknown')
      expect(mapType('any')).toBe('unknown')
    })

    it('should handle modifiers', () => {
      expect(mapType('string!#')).toBe('string')
      expect(mapType('int?')).toBe('number')
      expect(mapType('boolean!@')).toBe('boolean')
    })
  })

  describe('generateTypeScript', () => {
    it('should generate types for simple schema', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {
          User: {
            name: 'User',
            hash: 'user123',
            version: 1,
            fields: [
              {
                name: 'email',
                type: 'string!',
                required: true,
                indexed: false,
                unique: false,
                array: false
              },
              {
                name: 'name',
                type: 'string',
                required: false,
                indexed: false,
                unique: false,
                array: false
              }
            ]
          }
        }
      }

      const code = generateTypeScript(schema)

      expect(code).toContain('export interface UserEntity extends Entity')
      expect(code).toContain('export interface UserInput')
      expect(code).toContain('email: string')
      expect(code).toContain('name?: string')
      expect(code).toContain('export interface Database')
      expect(code).toContain('User: UserCollection')
    })

    it('should generate array types', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {
          Post: {
            name: 'Post',
            hash: 'post123',
            version: 1,
            fields: [
              {
                name: 'tags',
                type: 'string[]',
                required: false,
                indexed: false,
                unique: false,
                array: true
              }
            ]
          }
        }
      }

      const code = generateTypeScript(schema)

      expect(code).toContain('tags?: string[]')
    })

    it('should generate relationship types', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {
          Post: {
            name: 'Post',
            hash: 'post123',
            version: 1,
            fields: [
              {
                name: 'author',
                type: '-> User',
                required: true,
                indexed: false,
                unique: false,
                array: false,
                relationship: {
                  target: 'User',
                  direction: 'outbound'
                }
              }
            ]
          }
        }
      }

      const code = generateTypeScript(schema)

      expect(code).toContain('author: EntityRef<UserEntity>')
    })

    it('should include metadata when requested', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: 1234567890000,
        commitHash: 'commit123',
        collections: {}
      }

      const code = generateTypeScript(schema, { includeMetadata: true })

      expect(code).toContain('SCHEMA_METADATA')
      expect(code).toContain('abc123')
      expect(code).toContain('commit123')
    })

    it('should wrap in namespace when requested', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {}
      }

      const code = generateTypeScript(schema, { namespace: 'DB' })

      expect(code).toContain('export namespace DB {')
      expect(code).toContain('}')
    })

    it('should include imports by default', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {}
      }

      const code = generateTypeScript(schema)

      expect(code).toContain("import type {")
      expect(code).toContain("Entity")
      expect(code).toContain("EntityRef")
      expect(code).toContain("Filter")
    })

    it('should skip imports when requested', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {}
      }

      const code = generateTypeScript(schema, { includeImports: false })

      expect(code).not.toContain("import type")
    })

    it('should generate collection interface with typed methods', () => {
      const schema: SchemaSnapshot = {
        hash: 'abc123',
        configHash: 'def456',
        capturedAt: Date.now(),
        collections: {
          User: {
            name: 'User',
            hash: 'user123',
            version: 1,
            fields: [
              {
                name: 'email',
                type: 'string!',
                required: true,
                indexed: false,
                unique: false,
                array: false
              }
            ]
          }
        }
      }

      const code = generateTypeScript(schema)

      expect(code).toContain('export interface UserCollection {')
      expect(code).toContain('create(input: UserInput): Promise<UserEntity>')
      expect(code).toContain('get(id: string, options?: GetOptions): Promise<UserEntity | null>')
      expect(code).toContain('find(filter?: Filter<UserEntity>, options?: FindOptions): Promise<UserEntity[]>')
      expect(code).toContain('update(id: string, update: UpdateOperators<UserEntity>')
      expect(code).toContain('delete(id: string): Promise<boolean>')
    })
  })
})
