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
        $type: 'Post',
        name: 'Hello World',
        title: 'Hello World',
        content: 'This is a test post',
        tags: ['test', 'hello'],
      })
      expect(entity.$id).toBeDefined()
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
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })

      expect(user.$id).toBeDefined()
      expect(user.email).toBe('alice@example.com')
      expect(user.name).toBe('Alice')
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
        $type: 'User',
        name: 'Bob',
        email: 'bob@example.com',
      })

      expect(user.$id).toBeDefined()
      expect(user.email).toBe('bob@example.com')
    })

    it('supports indexed fields with # modifier', async () => {
      const database = DB({
        Product: {
          sku: 'string!#',
          name: 'string!',
          price: 'float',
        },
      })

      const product = await database.Product.create({
        $type: 'Product',
        name: 'Widget',
        sku: 'WIDGET-001',
        price: 9.99,
      })

      expect(product.$id).toBeDefined()
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
        $type: 'User',
        name: 'Charlie',
        email: 'charlie@example.com',
      })
      expect(user.email).toBe('charlie@example.com')

      // Flexible collection
      const log = await database.Logs.create({
        $type: 'Log',
        name: 'User created log',
        level: 'info',
        message: 'User created',
        metadata: { userId: user.$id },
      })
      expect(log.level).toBe('info')
    })
  })

  describe('default instance', () => {
    it('db singleton works in flexible mode', async () => {
      const post = await db.Posts.create({
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Post',
        body: 'Test body',
      })
      expect(post.$id).toBeDefined()
      expect(post.title).toBe('Test Post')
    })
  })
})
