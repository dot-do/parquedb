/**
 * StorageRouter Integration Tests
 *
 * Tests that the StorageRouter is properly wired into the DB() factory
 * and ParqueDB, and that storage paths and modes are correctly determined.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DB } from '../../src/db'
import { MemoryBackend } from '../../src/storage'

// =============================================================================
// Test Suite
// =============================================================================

describe('StorageRouter Integration', () => {
  // ===========================================================================
  // Basic wiring tests
  // ===========================================================================

  describe('DB() factory creates StorageRouter', () => {
    it('should create StorageRouter for typed schema', () => {
      const db = DB({
        User: { name: 'string!', email: 'string!' },
        Post: { title: 'string!', content: 'text' },
      })

      // Verify router is accessible
      const router = db.getStorageRouter()
      expect(router).not.toBeNull()
    })

    it('should not create StorageRouter for pure flexible mode', () => {
      const db = DB({ schema: 'flexible' })

      // No router in flexible mode
      const router = db.getStorageRouter()
      expect(router).toBeNull()
    })

    it('should create StorageRouter for mixed schema', () => {
      const db = DB({
        User: { name: 'string!', email: 'string!' },
        Events: 'flexible',
        Logs: 'flexible',
      })

      const router = db.getStorageRouter()
      expect(router).not.toBeNull()
    })
  })

  // ===========================================================================
  // Storage mode detection
  // ===========================================================================

  describe('getStorageMode', () => {
    it('should return "typed" for typed collections', () => {
      const db = DB({
        User: { name: 'string!', email: 'string!' },
        Post: { title: 'string!', content: 'text' },
      })

      expect(db.getStorageMode('user')).toBe('typed')
      expect(db.getStorageMode('post')).toBe('typed')
      expect(db.getStorageMode('User')).toBe('typed')
      expect(db.getStorageMode('Post')).toBe('typed')
    })

    it('should return "flexible" for flexible collections', () => {
      const db = DB({
        User: { name: 'string!' },
        Events: 'flexible',
        Logs: 'flexible',
      })

      expect(db.getStorageMode('events')).toBe('flexible')
      expect(db.getStorageMode('logs')).toBe('flexible')
      expect(db.getStorageMode('Events')).toBe('flexible')
      expect(db.getStorageMode('Logs')).toBe('flexible')
    })

    it('should return "flexible" for unknown collections', () => {
      const db = DB({
        User: { name: 'string!' },
      })

      expect(db.getStorageMode('unknown')).toBe('flexible')
      expect(db.getStorageMode('random')).toBe('flexible')
    })

    it('should return "flexible" when no router (pure flexible mode)', () => {
      const db = DB({ schema: 'flexible' })

      expect(db.getStorageMode('anything')).toBe('flexible')
    })
  })

  // ===========================================================================
  // Data path generation
  // ===========================================================================

  describe('getDataPath', () => {
    it('should return typed path for typed collections', () => {
      const db = DB({
        User: { name: 'string!', email: 'string!' },
        Occupation: { name: 'string!', socCode: 'string!' },
      })

      expect(db.getDataPath('user')).toBe('data/user.parquet')
      expect(db.getDataPath('occupation')).toBe('data/occupation.parquet')
      expect(db.getDataPath('User')).toBe('data/user.parquet')
      expect(db.getDataPath('Occupation')).toBe('data/occupation.parquet')
    })

    it('should return flexible path for flexible collections', () => {
      const db = DB({
        User: { name: 'string!' },
        Events: 'flexible',
        Logs: 'flexible',
      })

      expect(db.getDataPath('events')).toBe('data/events/data.parquet')
      expect(db.getDataPath('logs')).toBe('data/logs/data.parquet')
      expect(db.getDataPath('Events')).toBe('data/events/data.parquet')
    })

    it('should return flexible path for unknown collections', () => {
      const db = DB({
        User: { name: 'string!' },
      })

      expect(db.getDataPath('unknown')).toBe('data/unknown/data.parquet')
    })

    it('should return flexible path when no router', () => {
      const db = DB({ schema: 'flexible' })

      expect(db.getDataPath('posts')).toBe('data/posts/data.parquet')
    })
  })

  // ===========================================================================
  // hasTypedSchema
  // ===========================================================================

  describe('hasTypedSchema', () => {
    it('should return true for typed collections', () => {
      const db = DB({
        User: { name: 'string!' },
        Post: { title: 'string!' },
      })

      expect(db.hasTypedSchema('user')).toBe(true)
      expect(db.hasTypedSchema('post')).toBe(true)
      expect(db.hasTypedSchema('User')).toBe(true)
      expect(db.hasTypedSchema('Post')).toBe(true)
    })

    it('should return false for flexible collections', () => {
      const db = DB({
        User: { name: 'string!' },
        Events: 'flexible',
      })

      expect(db.hasTypedSchema('events')).toBe(false)
      expect(db.hasTypedSchema('Events')).toBe(false)
    })

    it('should return false for unknown collections', () => {
      const db = DB({
        User: { name: 'string!' },
      })

      expect(db.hasTypedSchema('unknown')).toBe(false)
    })

    it('should return false when no router', () => {
      const db = DB({ schema: 'flexible' })

      expect(db.hasTypedSchema('anything')).toBe(false)
    })
  })

  // ===========================================================================
  // Collection options
  // ===========================================================================

  describe('getCollectionOptions', () => {
    it('should return options for collections with $options', () => {
      const db = DB({
        User: {
          $options: { includeDataVariant: false },
          name: 'string!',
          email: 'string!',
        },
        Post: {
          $options: { includeDataVariant: true },
          title: 'string!',
        },
      })

      const userOptions = db.getCollectionOptions('user')
      expect(userOptions).toBeDefined()
      expect(userOptions?.includeDataVariant).toBe(false)

      const postOptions = db.getCollectionOptions('post')
      expect(postOptions).toBeDefined()
      expect(postOptions?.includeDataVariant).toBe(true)
    })

    it('should return default options for collections without $options', () => {
      const db = DB({
        User: { name: 'string!' },
      })

      const options = db.getCollectionOptions('user')
      expect(options).toBeDefined()
      expect(options?.includeDataVariant).toBe(true) // Default
    })

    it('should return options for flexible collections', () => {
      const db = DB({
        Events: 'flexible',
      })

      const options = db.getCollectionOptions('events')
      expect(options).toBeDefined()
      expect(options?.includeDataVariant).toBe(true) // Default
    })

    it('should return undefined for unknown collections when router exists', () => {
      const db = DB({
        User: { name: 'string!' },
      })

      // Unknown collection - collectionOptions Map doesn't have this key
      const options = db.getCollectionOptions('unknown')
      expect(options).toBeUndefined()
    })

    it('should return undefined when no router (flexible mode)', () => {
      const db = DB({ schema: 'flexible' })

      const options = db.getCollectionOptions('anything')
      expect(options).toBeUndefined()
    })
  })

  // ===========================================================================
  // Mixed schema with $layout and $studio
  // ===========================================================================

  describe('handles $-prefixed config fields correctly', () => {
    it('should ignore $layout and $studio when determining storage mode', () => {
      const db = DB({
        User: {
          $layout: [['name', 'email']],
          $studio: { label: 'Users' },
          $options: { includeDataVariant: true },
          name: 'string!',
          email: 'string!',
        },
      })

      // Should still be typed (has field definitions)
      expect(db.getStorageMode('user')).toBe('typed')
      expect(db.hasTypedSchema('user')).toBe(true)
      expect(db.getDataPath('user')).toBe('data/user.parquet')
    })

    it('should extract only field definitions for router schema', () => {
      const db = DB({
        Post: {
          $layout: { Main: [['title', 'content']], Meta: ['status'] },
          $sidebar: ['$id', 'status'],
          $studio: { label: 'Blog Posts' },
          title: 'string!',
          content: 'text',
          status: 'string',
        },
      })

      expect(db.getStorageMode('post')).toBe('typed')
      expect(db.hasTypedSchema('post')).toBe(true)
    })
  })

  // ===========================================================================
  // Real-world schema patterns
  // ===========================================================================

  describe('real-world schema patterns', () => {
    it('should handle O*NET-style schema with typed and flexible collections', () => {
      const db = DB({
        Occupation: {
          name: 'string!',
          socCode: 'string!',
          jobZone: 'int',
        },
        Skill: {
          name: 'string!',
          category: 'string',
        },
        // User content is flexible
        Posts: 'flexible',
        Comments: 'flexible',
      })

      // Typed collections
      expect(db.getStorageMode('occupation')).toBe('typed')
      expect(db.getStorageMode('skill')).toBe('typed')
      expect(db.getDataPath('occupation')).toBe('data/occupation.parquet')
      expect(db.getDataPath('skill')).toBe('data/skill.parquet')

      // Flexible collections
      expect(db.getStorageMode('posts')).toBe('flexible')
      expect(db.getStorageMode('comments')).toBe('flexible')
      expect(db.getDataPath('posts')).toBe('data/posts/data.parquet')
      expect(db.getDataPath('comments')).toBe('data/comments/data.parquet')
    })

    it('should handle CMS-style schema', () => {
      const db = DB({
        Page: {
          $options: { includeDataVariant: true },
          title: 'string!',
          slug: 'string!',
          content: 'text',
          publishedAt: 'datetime',
        },
        Media: {
          $options: { includeDataVariant: false },
          filename: 'string!',
          mimeType: 'string!',
          size: 'int',
        },
        // Audit logs should be flexible
        AuditLog: 'flexible',
      })

      expect(db.getStorageMode('page')).toBe('typed')
      expect(db.getStorageMode('media')).toBe('typed')
      expect(db.getStorageMode('auditlog')).toBe('flexible')

      // Check options
      expect(db.getCollectionOptions('page')?.includeDataVariant).toBe(true)
      expect(db.getCollectionOptions('media')?.includeDataVariant).toBe(false)
    })
  })

  // ===========================================================================
  // CRUD operations with storage mode awareness
  // ===========================================================================

  describe('CRUD operations respect storage mode', () => {
    it('should allow CRUD on typed collections', async () => {
      const db = DB(
        {
          User: { name: 'string!', email: 'string!' },
        },
        { storage: new MemoryBackend() }
      )

      // Create
      const user = await db.User.create({ name: 'Alice', email: 'alice@example.com' })
      expect(user.$id).toContain('user/')
      expect(user.name).toBe('Alice')

      // Read
      const found = await db.User.get(user.$id)
      expect(found).not.toBeNull()
      expect(found?.name).toBe('Alice')

      // Update
      const updated = await db.User.update(user.$id, { $set: { name: 'Alicia' } })
      expect(updated?.name).toBe('Alicia')

      // Delete
      const deleteResult = await db.User.delete(user.$id)
      expect(deleteResult.deletedCount).toBe(1)
    })

    it('should allow CRUD on flexible collections', async () => {
      const db = DB(
        {
          User: { name: 'string!' },
          Events: 'flexible',
        },
        { storage: new MemoryBackend() }
      )

      // Create event with arbitrary data
      const event = await db.Events.create({
        type: 'user.signup',
        userId: 'user/123',
        metadata: { browser: 'Chrome', ip: '192.168.1.1' },
      })
      expect(event.$id).toContain('events/')

      // Read
      const found = await db.Events.get(event.$id)
      expect(found).not.toBeNull()
      expect((found as Record<string, unknown>).type).toBe('user.signup')
    })

    it('should allow CRUD on unknown collections (default flexible)', async () => {
      const db = DB(
        {
          User: { name: 'string!' },
        },
        { storage: new MemoryBackend() }
      )

      // Unknown collection should still work (defaults to flexible)
      const log = await db.Logs.create({ level: 'info', message: 'Test' })
      expect(log.$id).toContain('logs/')

      const found = await db.Logs.get(log.$id)
      expect(found).not.toBeNull()
    })
  })
})
