/**
 * Tests for DB() factory function
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  DB,
  db,
  extractCollectionOptions,
  getFieldsWithoutOptions,
  extractAllCollectionOptions,
  DEFAULT_COLLECTION_OPTIONS,
  type CollectionOptions,
  type CollectionSchema,
  type DBSchema,
} from '../../src/db'
import { clearGlobalStorage } from '../../src/Collection'

describe('DB factory', () => {
  beforeEach(() => {
    clearGlobalStorage()
  })

  describe('flexible mode', () => {
    it('creates a ParqueDB instance with flexible schema', () => {
      const database = DB({ schema: 'flexible' })
      expect(database).toBeDefined()
    })

    it('allows creating entities without schema', async () => {
      const database = DB({ schema: 'flexible' })
      const entity = await database.Posts.create({
        title: 'Hello World',
        content: 'This is a test post',
        tags: ['test', 'hello'],
      })
      expect(entity.$id).toBeDefined()
      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.$type).toBe('Post')  // auto-derived from 'posts' collection
      expect(entity.name).toBe('Hello World')  // auto-derived from title
      expect(entity.title).toBe('Hello World')
    })

    it('exports a default db instance', () => {
      expect(db).toBeDefined()
    })
  })

  describe('typed mode', () => {
    it('creates a ParqueDB instance with typed schema', () => {
      const database = DB({
        User: {
          email: 'string!',
          name: 'string',
        },
      })
      expect(database).toBeDefined()
    })

    it('allows creating entities with typed schema', async () => {
      const database = DB({
        User: {
          email: 'string!',
          name: 'string',
          age: 'int?',
        },
      })

      const user = await database.User.create({
        email: 'alice@example.com',
        label: 'Alice',  // will be used as name
        age: 30,
      })

      expect(user.$id).toBeDefined()
      expect(user.$type).toBe('User')  // auto-derived
      expect(user.email).toBe('alice@example.com')
      expect(user.name).toBe('Alice')  // derived from label
      expect(user.age).toBe(30)
    })

    it('supports relationships in schema', async () => {
      const database = DB({
        User: {
          email: 'string!',
          name: 'string',
          posts: '<- Post.author[]',
        },
        Post: {
          title: 'string!',
          content: 'string',
          author: '-> User',
        },
      })

      const user = await database.User.create({
        email: 'bob@example.com',
        label: 'Bob',
      })

      expect(user.$id).toBeDefined()
      expect(user.$type).toBe('User')
      expect(user.email).toBe('bob@example.com')
    })

    it('supports indexed fields with # modifier', async () => {
      const database = DB({
        Product: {
          sku: 'string!#',
          title: 'string!',
          price: 'float',
        },
      })

      const product = await database.Product.create({
        sku: 'WIDGET-001',
        title: 'Widget',
        price: 9.99,
      })

      expect(product.$id).toBeDefined()
      expect(product.$type).toBe('Product')
      expect(product.name).toBe('Widget')  // derived from title
      expect(product.sku).toBe('WIDGET-001')
    })
  })

  describe('mixed mode', () => {
    it('supports both typed and flexible collections', async () => {
      const database = DB({
        User: {
          email: 'string!',
          name: 'string',
        },
        Logs: 'flexible',
      })

      // Typed collection
      const user = await database.User.create({
        email: 'charlie@example.com',
        label: 'Charlie',
      })
      expect(user.$type).toBe('User')
      expect(user.email).toBe('charlie@example.com')

      // Flexible collection
      const log = await database.Logs.create({
        level: 'info',
        message: 'User created',
        metadata: { userId: user.$id },
      })
      expect(log.$type).toBe('Log')  // auto-derived from 'logs' -> 'Log'
      expect(log.level).toBe('info')
    })
  })

  describe('default instance', () => {
    it('db singleton works in flexible mode', async () => {
      const post = await db.Posts.create({
        title: 'Test Post',
        body: 'Test body',
      })
      expect(post.$id).toBeDefined()
      expect(post.$type).toBe('Post')
      expect(post.name).toBe('Test Post')  // derived from title
      expect(post.title).toBe('Test Post')
    })
  })

  describe('$options support', () => {
    describe('extractCollectionOptions', () => {
      it('returns default options for flexible schema', () => {
        const options = extractCollectionOptions('flexible')
        expect(options).toEqual(DEFAULT_COLLECTION_OPTIONS)
        expect(options.includeDataVariant).toBe(true)
      })

      it('returns default options when $options is not specified', () => {
        const schema: CollectionSchema = {
          name: 'string!',
          email: 'string!',
        }
        const options = extractCollectionOptions(schema)
        expect(options.includeDataVariant).toBe(true)
      })

      it('extracts $options from schema', () => {
        const schema: CollectionSchema = {
          $options: { includeDataVariant: false },
          name: 'string!',
          level: 'string',
        }
        const options = extractCollectionOptions(schema)
        expect(options.includeDataVariant).toBe(false)
      })

      it('merges custom options with defaults', () => {
        const schema: CollectionSchema = {
          $options: { includeDataVariant: false },
          name: 'string!',
        }
        const options = extractCollectionOptions(schema)
        // Should have all default keys with custom override
        expect(options.includeDataVariant).toBe(false)
      })

      it('preserves default values for unspecified options', () => {
        const schema: CollectionSchema = {
          $options: {},  // Empty options object
          name: 'string!',
        }
        const options = extractCollectionOptions(schema)
        expect(options.includeDataVariant).toBe(true)  // Default value
      })
    })

    describe('getFieldsWithoutOptions', () => {
      it('returns empty object for flexible schema', () => {
        const fields = getFieldsWithoutOptions('flexible')
        expect(fields).toEqual({})
      })

      it('returns all fields when no $-prefixed keys', () => {
        const schema: CollectionSchema = {
          name: 'string!',
          email: 'email!',
          age: 'int?',
        }
        const fields = getFieldsWithoutOptions(schema)
        expect(fields).toEqual({
          name: 'string!',
          email: 'email!',
          age: 'int?',
        })
      })

      it('excludes $options from fields', () => {
        const schema: CollectionSchema = {
          $options: { includeDataVariant: false },
          name: 'string!',
          level: 'string',
        }
        const fields = getFieldsWithoutOptions(schema)
        expect(fields).toEqual({
          name: 'string!',
          level: 'string',
        })
        expect('$options' in fields).toBe(false)
      })

      it('excludes all $-prefixed keys ($options, $layout, $studio, $sidebar)', () => {
        const schema: CollectionSchema = {
          $options: { includeDataVariant: true },
          $layout: [['title', 'slug'], 'content'],
          $sidebar: ['$id', 'status'],
          $studio: { label: 'Blog Posts' },
          title: 'string!',
          slug: 'string!',
          content: 'text',
          status: 'string',
        }
        const fields = getFieldsWithoutOptions(schema)
        expect(fields).toEqual({
          title: 'string!',
          slug: 'string!',
          content: 'text',
          status: 'string',
        })
        expect(Object.keys(fields).some(k => k.startsWith('$'))).toBe(false)
      })
    })

    describe('extractAllCollectionOptions', () => {
      it('extracts options from all collections', () => {
        const schema: DBSchema = {
          User: {
            $options: { includeDataVariant: true },
            email: 'string!',
            name: 'string',
          },
          Logs: {
            $options: { includeDataVariant: false },
            level: 'string',
            message: 'string',
          },
          Posts: 'flexible',
        }
        const optionsMap = extractAllCollectionOptions(schema)

        expect(optionsMap.size).toBe(3)
        expect(optionsMap.get('User')?.includeDataVariant).toBe(true)
        expect(optionsMap.get('Logs')?.includeDataVariant).toBe(false)
        expect(optionsMap.get('Posts')?.includeDataVariant).toBe(true)  // default for flexible
      })

      it('uses defaults for collections without $options', () => {
        const schema: DBSchema = {
          User: {
            email: 'string!',
            name: 'string',
          },
          Post: {
            title: 'string!',
            content: 'text',
          },
        }
        const optionsMap = extractAllCollectionOptions(schema)

        expect(optionsMap.get('User')?.includeDataVariant).toBe(true)
        expect(optionsMap.get('Post')?.includeDataVariant).toBe(true)
      })
    })

    describe('DB() integration with $options', () => {
      it('creates database with $options in schema', async () => {
        const database = DB({
          Occupation: {
            $options: { includeDataVariant: true },
            name: 'string!',
            socCode: 'string!',
          },
        })
        expect(database).toBeDefined()

        // Verify collection works normally
        const occupation = await database.Occupation.create({
          name: 'Software Developer',
          socCode: '15-1252',
        })
        expect(occupation.$id).toBeDefined()
        expect(occupation.$type).toBe('Occupation')
        expect(occupation.name).toBe('Software Developer')
        expect(occupation.socCode).toBe('15-1252')
      })

      it('creates database with includeDataVariant: false', async () => {
        const database = DB({
          Logs: {
            $options: { includeDataVariant: false },
            level: 'string',
            message: 'string',
          },
        })
        expect(database).toBeDefined()

        const log = await database.Logs.create({
          level: 'info',
          message: 'Test message',
        })
        expect(log.$id).toBeDefined()
        expect(log.$type).toBe('Log')
        expect(log.level).toBe('info')
      })

      it('supports mixed collections with different $options', async () => {
        const database = DB({
          User: {
            $options: { includeDataVariant: true },
            email: 'string!',
            name: 'string',
          },
          AuditLog: {
            $options: { includeDataVariant: false },
            action: 'string!',
            timestamp: 'datetime',
          },
          Notes: 'flexible',
        })

        const user = await database.User.create({
          email: 'test@example.com',
          label: 'Test User',
        })
        expect(user.$type).toBe('User')

        const log = await database.AuditLog.create({
          action: 'user.created',
          timestamp: new Date().toISOString(),
          label: 'User Created',
        })
        expect(log.$type).toBe('Auditlog')  // derived from collection 'auditlog' -> 'Auditlog'

        const note = await database.Notes.create({
          title: 'My Note',
          content: 'Note content',
        })
        expect(note.$type).toBe('Note')
      })
    })
  })
})
