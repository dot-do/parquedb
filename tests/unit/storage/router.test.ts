/**
 * StorageRouter Tests
 *
 * Tests for the StorageRouter class that routes storage operations
 * based on collection storage mode (typed vs flexible).
 */

import { describe, it, expect } from 'vitest'
import { StorageRouter, type RouterSchema } from '../../../src/storage/router'

describe('StorageRouter', () => {
  // ===========================================================================
  // Constructor
  // ===========================================================================

  describe('constructor', () => {
    it('should create router with no schema', () => {
      const router = new StorageRouter()

      expect(router).toBeInstanceOf(StorageRouter)
    })

    it('should create router with typed collections', () => {
      const schema: RouterSchema = {
        User: { name: 'string!' },
        Post: { title: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('user')).toBe(true)
      expect(router.hasTypedSchema('post')).toBe(true)
    })

    it('should create router with flexible collections', () => {
      const schema: RouterSchema = {
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('events')).toBe(false)
    })

    it('should create router with mixed collections', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Post: { title: 'string!' },
        Events: 'flexible',
        Logs: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('user')).toBe(true)
      expect(router.hasTypedSchema('post')).toBe(true)
      expect(router.hasTypedSchema('events')).toBe(false)
      expect(router.hasTypedSchema('logs')).toBe(false)
    })

    it('should accept custom default mode', () => {
      const router = new StorageRouter({}, { defaultMode: 'typed' })

      // Unknown collection should use custom default
      expect(router.getStorageMode('unknown')).toBe('typed')
    })
  })

  // ===========================================================================
  // getStorageMode
  // ===========================================================================

  describe('getStorageMode', () => {
    it('should return "typed" for typed collections', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Occupation: { name: 'string!', socCode: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.getStorageMode('user')).toBe('typed')
      expect(router.getStorageMode('occupation')).toBe('typed')
    })

    it('should return "flexible" for flexible collections', () => {
      const schema: RouterSchema = {
        Posts: 'flexible',
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getStorageMode('posts')).toBe('flexible')
      expect(router.getStorageMode('events')).toBe('flexible')
    })

    it('should return "flexible" for unknown collections by default', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.getStorageMode('unknown')).toBe('flexible')
      expect(router.getStorageMode('random')).toBe('flexible')
    })

    it('should handle case insensitivity', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Posts: 'flexible',
      }
      const router = new StorageRouter(schema)

      // Typed - various cases
      expect(router.getStorageMode('user')).toBe('typed')
      expect(router.getStorageMode('User')).toBe('typed')
      expect(router.getStorageMode('USER')).toBe('typed')
      expect(router.getStorageMode('uSeR')).toBe('typed')

      // Flexible - various cases
      expect(router.getStorageMode('posts')).toBe('flexible')
      expect(router.getStorageMode('Posts')).toBe('flexible')
      expect(router.getStorageMode('POSTS')).toBe('flexible')
    })

    it('should prioritize explicit flexible over typed', () => {
      // Edge case: if somehow same name appears both places
      // explicit flexible should win
      const router = new StorageRouter({ Events: 'flexible' })

      expect(router.getStorageMode('events')).toBe('flexible')
    })

    it('should work with empty schema', () => {
      const router = new StorageRouter({})

      expect(router.getStorageMode('anything')).toBe('flexible')
    })
  })

  // ===========================================================================
  // getDataPath
  // ===========================================================================

  describe('getDataPath', () => {
    it('should return correct path for typed collections', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Occupation: { name: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.getDataPath('user')).toBe('data/user.parquet')
      expect(router.getDataPath('occupation')).toBe('data/occupation.parquet')
    })

    it('should return correct path for flexible collections', () => {
      const schema: RouterSchema = {
        Posts: 'flexible',
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getDataPath('posts')).toBe('data/posts/data.parquet')
      expect(router.getDataPath('events')).toBe('data/events/data.parquet')
    })

    it('should return flexible path for unknown collections', () => {
      const router = new StorageRouter({})

      expect(router.getDataPath('unknown')).toBe('data/unknown/data.parquet')
    })

    it('should handle case insensitivity in path generation', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Posts: 'flexible',
      }
      const router = new StorageRouter(schema)

      // Typed - all cases should produce lowercase path
      expect(router.getDataPath('User')).toBe('data/user.parquet')
      expect(router.getDataPath('USER')).toBe('data/user.parquet')

      // Flexible - all cases should produce lowercase path
      expect(router.getDataPath('Posts')).toBe('data/posts/data.parquet')
      expect(router.getDataPath('POSTS')).toBe('data/posts/data.parquet')
    })

    it('should return correct paths for mixed schema', () => {
      const schema: RouterSchema = {
        Occupation: { name: 'string!', socCode: 'string!' },
        Skill: { name: 'string!' },
        Posts: 'flexible',
        Logs: 'flexible',
      }
      const router = new StorageRouter(schema)

      // Typed collections
      expect(router.getDataPath('occupation')).toBe('data/occupation.parquet')
      expect(router.getDataPath('skill')).toBe('data/skill.parquet')

      // Flexible collections
      expect(router.getDataPath('posts')).toBe('data/posts/data.parquet')
      expect(router.getDataPath('logs')).toBe('data/logs/data.parquet')

      // Unknown collection (defaults to flexible)
      expect(router.getDataPath('other')).toBe('data/other/data.parquet')
    })
  })

  // ===========================================================================
  // hasTypedSchema
  // ===========================================================================

  describe('hasTypedSchema', () => {
    it('should return true for typed collections', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Post: { title: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('user')).toBe(true)
      expect(router.hasTypedSchema('post')).toBe(true)
    })

    it('should return false for flexible collections', () => {
      const schema: RouterSchema = {
        Posts: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('posts')).toBe(false)
    })

    it('should return false for unknown collections', () => {
      const router = new StorageRouter({})

      expect(router.hasTypedSchema('unknown')).toBe(false)
    })

    it('should handle case insensitivity', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.hasTypedSchema('user')).toBe(true)
      expect(router.hasTypedSchema('User')).toBe(true)
      expect(router.hasTypedSchema('USER')).toBe(true)
    })
  })

  // ===========================================================================
  // getTypedCollections / getFlexibleCollections
  // ===========================================================================

  describe('getTypedCollections', () => {
    it('should return all typed collection names', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Post: { title: 'string!' },
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      const typed = router.getTypedCollections()
      expect(typed).toHaveLength(2)
      expect(typed).toContain('user')
      expect(typed).toContain('post')
    })

    it('should return empty array when no typed collections', () => {
      const schema: RouterSchema = {
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getTypedCollections()).toEqual([])
    })
  })

  describe('getFlexibleCollections', () => {
    it('should return all flexible collection names', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
        Events: 'flexible',
        Logs: 'flexible',
      }
      const router = new StorageRouter(schema)

      const flexible = router.getFlexibleCollections()
      expect(flexible).toHaveLength(2)
      expect(flexible).toContain('events')
      expect(flexible).toContain('logs')
    })

    it('should return empty array when no flexible collections', () => {
      const schema: RouterSchema = {
        User: { email: 'string!' },
      }
      const router = new StorageRouter(schema)

      expect(router.getFlexibleCollections()).toEqual([])
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle empty schema object', () => {
      const router = new StorageRouter({})

      expect(router.getStorageMode('any')).toBe('flexible')
      expect(router.getDataPath('any')).toBe('data/any/data.parquet')
      expect(router.hasTypedSchema('any')).toBe(false)
    })

    it('should handle schema with only flexible collections', () => {
      const schema: RouterSchema = {
        A: 'flexible',
        B: 'flexible',
        C: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getTypedCollections()).toEqual([])
      expect(router.getFlexibleCollections()).toHaveLength(3)
    })

    it('should handle schema with only typed collections', () => {
      const schema: RouterSchema = {
        A: { x: 'string' },
        B: { y: 'int' },
      }
      const router = new StorageRouter(schema)

      expect(router.getTypedCollections()).toHaveLength(2)
      expect(router.getFlexibleCollections()).toEqual([])
    })

    it('should handle collection names with numbers', () => {
      const schema: RouterSchema = {
        Table1: { col: 'string' },
        Table2: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getStorageMode('table1')).toBe('typed')
      expect(router.getStorageMode('table2')).toBe('flexible')
    })

    it('should handle collection names with underscores', () => {
      const schema: RouterSchema = {
        user_profiles: { name: 'string' },
        event_logs: 'flexible',
      }
      const router = new StorageRouter(schema)

      expect(router.getStorageMode('user_profiles')).toBe('typed')
      expect(router.getStorageMode('event_logs')).toBe('flexible')
      expect(router.getDataPath('user_profiles')).toBe('data/user_profiles.parquet')
      expect(router.getDataPath('event_logs')).toBe('data/event_logs/data.parquet')
    })

    it('should preserve collection name format in path (lowercase)', () => {
      const schema: RouterSchema = {
        UserProfile: { name: 'string' },
      }
      const router = new StorageRouter(schema)

      // Path should always be lowercase
      expect(router.getDataPath('UserProfile')).toBe('data/userprofile.parquet')
      expect(router.getDataPath('USERPROFILE')).toBe('data/userprofile.parquet')
    })
  })

  // ===========================================================================
  // Real-world Usage Patterns
  // ===========================================================================

  describe('real-world usage patterns', () => {
    it('should handle O*NET-style schema', () => {
      const schema: RouterSchema = {
        Occupation: {
          name: 'string!',
          socCode: 'string!',
          jobZone: 'int',
        },
        Skill: {
          name: 'string!',
          category: 'string',
        },
        // Flexible for user-generated content
        Posts: 'flexible',
        Comments: 'flexible',
      }
      const router = new StorageRouter(schema)

      // Typed data
      expect(router.getStorageMode('occupation')).toBe('typed')
      expect(router.getStorageMode('skill')).toBe('typed')
      expect(router.getDataPath('occupation')).toBe('data/occupation.parquet')
      expect(router.getDataPath('skill')).toBe('data/skill.parquet')

      // User content
      expect(router.getStorageMode('posts')).toBe('flexible')
      expect(router.getDataPath('posts')).toBe('data/posts/data.parquet')
    })

    it('should work with DB() factory schema format', () => {
      // This mimics the schema format used in db.ts
      const schema: RouterSchema = {
        User: {
          $layout: [['name', 'email']],
          $studio: { label: 'Users' },
          name: 'string!',
          email: 'string!',
        },
        Events: 'flexible',
      }
      const router = new StorageRouter(schema)

      // $-prefixed keys should be ignored (treated as typed because object)
      expect(router.getStorageMode('user')).toBe('typed')
      expect(router.getStorageMode('events')).toBe('flexible')
    })
  })
})
