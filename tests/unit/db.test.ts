/**
 * Tests for DB() factory function
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DB, db } from '../../src/db'
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
})
