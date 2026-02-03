/**
 * StorageRouter Tests
 *
 * Tests for the StorageRouter class that routes storage operations
 * based on collection storage mode (typed vs flexible) and namespace sharding.
 */

import { describe, it, expect } from 'vitest'
import {
  StorageRouter,
  type RouterSchema,
  type ShardingConfig,
  STORAGE_PATHS,
  NAMESPACE_FILES,
  formatTimePeriod,
  calculateHashShard,
  DEFAULT_SHARDING_THRESHOLDS,
} from '../../../src/storage/router'

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

// =============================================================================
// Storage Path Helpers Tests
// =============================================================================

describe('STORAGE_PATHS', () => {
  describe('namespaceData', () => {
    it('should return correct namespace data path', () => {
      expect(STORAGE_PATHS.namespaceData('users')).toBe('users/data.parquet')
      expect(STORAGE_PATHS.namespaceData('tenant-a')).toBe('tenant-a/data.parquet')
    })
  })

  describe('typeShardData', () => {
    it('should return correct type shard path', () => {
      expect(STORAGE_PATHS.typeShardData('orders', 'purchase')).toBe(
        'orders/_shards/type=purchase/data.parquet'
      )
      expect(STORAGE_PATHS.typeShardData('events', 'click')).toBe(
        'events/_shards/type=click/data.parquet'
      )
    })
  })

  describe('timeShardData', () => {
    it('should return correct time shard path', () => {
      expect(STORAGE_PATHS.timeShardData('events', '2024-01')).toBe(
        'events/_shards/period=2024-01/data.parquet'
      )
      expect(STORAGE_PATHS.timeShardData('logs', '2024-W05')).toBe(
        'logs/_shards/period=2024-W05/data.parquet'
      )
    })
  })

  describe('hashShardData', () => {
    it('should return correct hash shard path', () => {
      expect(STORAGE_PATHS.hashShardData('users', 0)).toBe(
        'users/_shards/shard=0/data.parquet'
      )
      expect(STORAGE_PATHS.hashShardData('users', 15)).toBe(
        'users/_shards/shard=15/data.parquet'
      )
    })
  })

  describe('shardsPrefix', () => {
    it('should return correct shards prefix', () => {
      expect(STORAGE_PATHS.shardsPrefix('users')).toBe('users/_shards/')
    })
  })
})

describe('NAMESPACE_FILES', () => {
  it('should have correct file names', () => {
    expect(NAMESPACE_FILES.DATA).toBe('data.parquet')
    expect(NAMESPACE_FILES.EDGES).toBe('edges.parquet')
    expect(NAMESPACE_FILES.EVENTS).toBe('events.parquet')
    expect(NAMESPACE_FILES.SCHEMA).toBe('_schema.parquet')
    expect(NAMESPACE_FILES.META).toBe('_meta.parquet')
    expect(NAMESPACE_FILES.SHARDS_DIR).toBe('_shards')
  })
})

// =============================================================================
// formatTimePeriod Tests
// =============================================================================

describe('formatTimePeriod', () => {
  // Use a known date: 2024-03-15T14:30:00Z (Friday, March 15, 2024)
  const testDate = new Date(Date.UTC(2024, 2, 15, 14, 30, 0))
  const testTimestamp = testDate.getTime()

  describe('hour bucket', () => {
    it('should format to year-month-day-hour', () => {
      expect(formatTimePeriod(testTimestamp, 'hour')).toBe('2024-03-15T14')
    })

    it('should pad hours with zeros', () => {
      const earlyDate = new Date(Date.UTC(2024, 0, 1, 5, 0, 0))
      expect(formatTimePeriod(earlyDate.getTime(), 'hour')).toBe('2024-01-01T05')
    })
  })

  describe('day bucket', () => {
    it('should format to year-month-day', () => {
      expect(formatTimePeriod(testTimestamp, 'day')).toBe('2024-03-15')
    })

    it('should pad month and day with zeros', () => {
      const earlyDate = new Date(Date.UTC(2024, 0, 5, 0, 0, 0))
      expect(formatTimePeriod(earlyDate.getTime(), 'day')).toBe('2024-01-05')
    })
  })

  describe('week bucket', () => {
    it('should format to year-week', () => {
      // March 15, 2024 is in ISO week 11
      expect(formatTimePeriod(testTimestamp, 'week')).toBe('2024-W11')
    })

    it('should pad week number', () => {
      const earlyDate = new Date(Date.UTC(2024, 0, 5, 0, 0, 0))
      // January 5, 2024 is in ISO week 1
      expect(formatTimePeriod(earlyDate.getTime(), 'week')).toBe('2024-W01')
    })
  })

  describe('month bucket', () => {
    it('should format to year-month', () => {
      expect(formatTimePeriod(testTimestamp, 'month')).toBe('2024-03')
    })

    it('should pad month with zeros', () => {
      const earlyDate = new Date(Date.UTC(2024, 0, 15, 0, 0, 0))
      expect(formatTimePeriod(earlyDate.getTime(), 'month')).toBe('2024-01')
    })
  })

  describe('year bucket', () => {
    it('should format to year only', () => {
      expect(formatTimePeriod(testTimestamp, 'year')).toBe('2024')
    })
  })

  describe('Date input', () => {
    it('should accept Date objects', () => {
      expect(formatTimePeriod(testDate, 'day')).toBe('2024-03-15')
    })
  })
})

// =============================================================================
// calculateHashShard Tests
// =============================================================================

describe('calculateHashShard', () => {
  it('should return consistent shard for same ID', () => {
    const shard1 = calculateHashShard('user-123', 16)
    const shard2 = calculateHashShard('user-123', 16)
    expect(shard1).toBe(shard2)
  })

  it('should return value within shard count range', () => {
    for (let i = 0; i < 100; i++) {
      const shard = calculateHashShard(`user-${i}`, 16)
      expect(shard).toBeGreaterThanOrEqual(0)
      expect(shard).toBeLessThan(16)
    }
  })

  it('should distribute IDs across shards', () => {
    const shardCounts = new Map<number, number>()
    const shardCount = 8

    // Generate many IDs and count distribution
    for (let i = 0; i < 1000; i++) {
      const shard = calculateHashShard(`entity-${i}`, shardCount)
      shardCounts.set(shard, (shardCounts.get(shard) ?? 0) + 1)
    }

    // All shards should be used
    expect(shardCounts.size).toBe(shardCount)

    // Distribution should be somewhat even (no shard has more than 25% of total)
    for (const count of shardCounts.values()) {
      expect(count).toBeLessThan(250) // Less than 25% of 1000
    }
  })

  it('should handle empty string', () => {
    const shard = calculateHashShard('', 16)
    expect(shard).toBeGreaterThanOrEqual(0)
    expect(shard).toBeLessThan(16)
  })

  it('should handle single character', () => {
    const shard = calculateHashShard('a', 16)
    expect(shard).toBeGreaterThanOrEqual(0)
    expect(shard).toBeLessThan(16)
  })

  it('should work with shardCount of 1', () => {
    const shard = calculateHashShard('any-id', 1)
    expect(shard).toBe(0)
  })
})

// =============================================================================
// StorageRouter Sharding Tests
// =============================================================================

describe('StorageRouter - Sharding', () => {
  describe('getShardStrategy', () => {
    it('should return "none" for non-sharded namespaces', () => {
      const router = new StorageRouter({})
      expect(router.getShardStrategy('users')).toBe('none')
    })

    it('should return correct strategy for type sharding', () => {
      const router = new StorageRouter({}, {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' }
        }
      })
      expect(router.getShardStrategy('orders')).toBe('type')
    })

    it('should return correct strategy for time sharding', () => {
      const router = new StorageRouter({}, {
        sharding: {
          events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
        }
      })
      expect(router.getShardStrategy('events')).toBe('time')
    })

    it('should return correct strategy for hash sharding', () => {
      const router = new StorageRouter({}, {
        sharding: {
          users: { strategy: 'hash', shardCount: 16 }
        }
      })
      expect(router.getShardStrategy('users')).toBe('hash')
    })

    it('should be case insensitive', () => {
      const router = new StorageRouter({}, {
        sharding: {
          Orders: { strategy: 'type', typeField: 'orderType' }
        }
      })
      expect(router.getShardStrategy('orders')).toBe('type')
      expect(router.getShardStrategy('ORDERS')).toBe('type')
    })
  })

  describe('getShardConfig', () => {
    it('should return undefined for non-sharded namespaces', () => {
      const router = new StorageRouter({})
      expect(router.getShardConfig('users')).toBeUndefined()
    })

    it('should return shard config', () => {
      const config = { strategy: 'type' as const, typeField: 'orderType' }
      const router = new StorageRouter({}, {
        sharding: { orders: config }
      })
      expect(router.getShardConfig('orders')).toEqual(config)
    })
  })

  describe('isSharded', () => {
    it('should return false for non-sharded namespaces', () => {
      const router = new StorageRouter({})
      expect(router.isSharded('users')).toBe(false)
    })

    it('should return true for sharded namespaces', () => {
      const router = new StorageRouter({}, {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' }
        }
      })
      expect(router.isSharded('orders')).toBe(true)
      expect(router.isSharded('users')).toBe(false)
    })
  })

  describe('getShardPath', () => {
    describe('type-based sharding', () => {
      it('should return type shard path', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const path = router.getShardPath('orders', { orderType: 'purchase', id: '123' })
        expect(path).toBe('orders/_shards/type=purchase/data.parquet')
      })

      it('should sanitize type value', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const path = router.getShardPath('orders', { orderType: 'Special Order!', id: '123' })
        expect(path).toBe('orders/_shards/type=special_order_/data.parquet')
      })

      it('should fall back to base path if type field is missing', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const path = router.getShardPath('orders', { id: '123' })
        expect(path).toBe('data/orders/data.parquet')
      })

      it('should convert type value to lowercase', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const path = router.getShardPath('orders', { orderType: 'PURCHASE', id: '123' })
        expect(path).toBe('orders/_shards/type=purchase/data.parquet')
      })
    })

    describe('time-based sharding', () => {
      it('should return time shard path', () => {
        const router = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
          }
        })

        // March 15, 2024
        const path = router.getShardPath('events', { createdAt: Date.UTC(2024, 2, 15), id: '123' })
        expect(path).toBe('events/_shards/period=2024-03/data.parquet')
      })

      it('should fall back to base path if time field is missing', () => {
        const router = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
          }
        })

        const path = router.getShardPath('events', { id: '123' })
        expect(path).toBe('data/events/data.parquet')
      })

      it('should handle string date values', () => {
        const router = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'day' }
          }
        })

        const path = router.getShardPath('events', { createdAt: '2024-03-15T14:30:00Z', id: '123' })
        expect(path).toBe('events/_shards/period=2024-03-15/data.parquet')
      })

      it('should handle different bucket sizes', () => {
        const hourRouter = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'ts', bucketSize: 'hour' }
          }
        })

        const ts = Date.UTC(2024, 2, 15, 14, 30, 0)
        expect(hourRouter.getShardPath('events', { ts })).toBe('events/_shards/period=2024-03-15T14/data.parquet')
      })
    })

    describe('hash-based sharding', () => {
      it('should return hash shard path', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 16 }
          }
        })

        const path1 = router.getShardPath('users', { id: 'user-123' })
        const shardNum = calculateHashShard('user-123', 16)
        expect(path1).toBe(`users/_shards/shard=${shardNum}/data.parquet`)
      })

      it('should use $id if id is not present', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 16 }
          }
        })

        const path = router.getShardPath('users', { $id: 'user-456' })
        const shardNum = calculateHashShard('user-456', 16)
        expect(path).toBe(`users/_shards/shard=${shardNum}/data.parquet`)
      })

      it('should fall back to shard 0 if no id', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 16 }
          }
        })

        const path = router.getShardPath('users', { name: 'John' })
        expect(path).toBe('users/_shards/shard=0/data.parquet')
      })
    })

    describe('non-sharded namespace', () => {
      it('should return base data path', () => {
        const router = new StorageRouter({})
        const path = router.getShardPath('users', { id: '123' })
        expect(path).toBe('data/users/data.parquet')
      })
    })
  })

  describe('listShardPaths', () => {
    it('should return base path for non-sharded namespace', () => {
      const router = new StorageRouter({})
      expect(router.listShardPaths('users')).toEqual(['data/users/data.parquet'])
    })

    it('should return all hash shard paths', () => {
      const router = new StorageRouter({}, {
        sharding: {
          users: { strategy: 'hash', shardCount: 4 }
        }
      })

      const paths = router.listShardPaths('users')
      expect(paths).toHaveLength(4)
      expect(paths).toContain('users/_shards/shard=0/data.parquet')
      expect(paths).toContain('users/_shards/shard=1/data.parquet')
      expect(paths).toContain('users/_shards/shard=2/data.parquet')
      expect(paths).toContain('users/_shards/shard=3/data.parquet')
    })

    it('should return base path for type sharding without known keys', () => {
      const router = new StorageRouter({}, {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' }
        }
      })

      expect(router.listShardPaths('orders')).toEqual(['data/orders/data.parquet'])
    })

    it('should return type shard paths when keys provided', () => {
      const router = new StorageRouter({}, {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' }
        }
      })

      const paths = router.listShardPaths('orders', ['purchase', 'refund', 'exchange'])
      expect(paths).toHaveLength(3)
      expect(paths).toContain('orders/_shards/type=purchase/data.parquet')
      expect(paths).toContain('orders/_shards/type=refund/data.parquet')
      expect(paths).toContain('orders/_shards/type=exchange/data.parquet')
    })

    it('should return time shard paths when periods provided', () => {
      const router = new StorageRouter({}, {
        sharding: {
          events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
        }
      })

      const paths = router.listShardPaths('events', ['2024-01', '2024-02', '2024-03'])
      expect(paths).toHaveLength(3)
      expect(paths).toContain('events/_shards/period=2024-01/data.parquet')
      expect(paths).toContain('events/_shards/period=2024-02/data.parquet')
      expect(paths).toContain('events/_shards/period=2024-03/data.parquet')
    })
  })

  describe('resolveDataPaths', () => {
    describe('non-sharded namespace', () => {
      it('should return base data path', () => {
        const router = new StorageRouter({})
        expect(router.resolveDataPaths('users')).toEqual(['data/users/data.parquet'])
      })
    })

    describe('type-based sharding', () => {
      it('should return single shard for direct type filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const paths = router.resolveDataPaths('orders', { orderType: 'purchase' })
        expect(paths).toEqual(['orders/_shards/type=purchase/data.parquet'])
      })

      it('should return single shard for $eq filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const paths = router.resolveDataPaths('orders', { orderType: { $eq: 'purchase' } })
        expect(paths).toEqual(['orders/_shards/type=purchase/data.parquet'])
      })

      it('should return multiple shards for $in filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const paths = router.resolveDataPaths('orders', { orderType: { $in: ['purchase', 'refund'] } })
        expect(paths).toHaveLength(2)
        expect(paths).toContain('orders/_shards/type=purchase/data.parquet')
        expect(paths).toContain('orders/_shards/type=refund/data.parquet')
      })

      it('should return base path when no type filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            orders: { strategy: 'type', typeField: 'orderType' }
          }
        })

        const paths = router.resolveDataPaths('orders', { status: 'pending' })
        expect(paths).toEqual(['data/orders/data.parquet'])
      })
    })

    describe('time-based sharding', () => {
      it('should return single shard for time filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
          }
        })

        const paths = router.resolveDataPaths('events', { createdAt: Date.UTC(2024, 2, 15) })
        expect(paths).toEqual(['events/_shards/period=2024-03/data.parquet'])
      })

      it('should return base path when no time filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            events: { strategy: 'time', timeField: 'createdAt', bucketSize: 'month' }
          }
        })

        const paths = router.resolveDataPaths('events', { type: 'click' })
        expect(paths).toEqual(['data/events/data.parquet'])
      })
    })

    describe('hash-based sharding', () => {
      it('should return single shard for id filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 16 }
          }
        })

        const paths = router.resolveDataPaths('users', { id: 'user-123' })
        const expectedShard = calculateHashShard('user-123', 16)
        expect(paths).toEqual([`users/_shards/shard=${expectedShard}/data.parquet`])
      })

      it('should return single shard for $id filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 16 }
          }
        })

        const paths = router.resolveDataPaths('users', { $id: 'user-456' })
        const expectedShard = calculateHashShard('user-456', 16)
        expect(paths).toEqual([`users/_shards/shard=${expectedShard}/data.parquet`])
      })

      it('should return all shards when no id filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 4 }
          }
        })

        const paths = router.resolveDataPaths('users', { name: 'John' })
        expect(paths).toHaveLength(4)
      })

      it('should return all shards when no filter', () => {
        const router = new StorageRouter({}, {
          sharding: {
            users: { strategy: 'hash', shardCount: 4 }
          }
        })

        const paths = router.resolveDataPaths('users')
        expect(paths).toHaveLength(4)
      })
    })
  })

  describe('setShardConfig / removeShardConfig', () => {
    it('should add shard config dynamically', () => {
      const router = new StorageRouter({})
      expect(router.isSharded('orders')).toBe(false)

      router.setShardConfig('orders', { strategy: 'type', typeField: 'orderType' })
      expect(router.isSharded('orders')).toBe(true)
      expect(router.getShardStrategy('orders')).toBe('type')
    })

    it('should remove shard config', () => {
      const router = new StorageRouter({}, {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' }
        }
      })

      expect(router.isSharded('orders')).toBe(true)
      router.removeShardConfig('orders')
      expect(router.isSharded('orders')).toBe(false)
    })
  })

  describe('shouldShard', () => {
    it('should return true when file size exceeds threshold', () => {
      const router = new StorageRouter({})
      expect(router.shouldShard({
        fileSize: 2 * 1024 * 1024 * 1024, // 2GB
        entityCount: 1000,
        rowGroupCount: 10
      })).toBe(true)
    })

    it('should return true when entity count exceeds threshold', () => {
      const router = new StorageRouter({})
      expect(router.shouldShard({
        fileSize: 100 * 1024 * 1024, // 100MB
        entityCount: 15_000_000,
        rowGroupCount: 10
      })).toBe(true)
    })

    it('should return true when row group count exceeds threshold', () => {
      const router = new StorageRouter({})
      expect(router.shouldShard({
        fileSize: 100 * 1024 * 1024,
        entityCount: 1000,
        rowGroupCount: 1500
      })).toBe(true)
    })

    it('should return false when under all thresholds', () => {
      const router = new StorageRouter({})
      expect(router.shouldShard({
        fileSize: 100 * 1024 * 1024,
        entityCount: 100_000,
        rowGroupCount: 50
      })).toBe(false)
    })

    it('should use custom thresholds', () => {
      const router = new StorageRouter({}, {
        shardingThresholds: {
          maxFileSize: 50 * 1024 * 1024, // 50MB
          maxEntityCount: 10_000,
          maxRowGroupCount: 100
        }
      })

      // Would be under default thresholds, but over custom ones
      expect(router.shouldShard({
        fileSize: 75 * 1024 * 1024, // 75MB
        entityCount: 5000,
        rowGroupCount: 50
      })).toBe(true)
    })
  })

  describe('getShardingThresholds', () => {
    it('should return default thresholds', () => {
      const router = new StorageRouter({})
      expect(router.getShardingThresholds()).toEqual(DEFAULT_SHARDING_THRESHOLDS)
    })

    it('should return custom thresholds', () => {
      const customThresholds = {
        maxFileSize: 500 * 1024 * 1024,
        maxEntityCount: 5_000_000,
        maxRowGroupCount: 500
      }
      const router = new StorageRouter({}, { shardingThresholds: customThresholds })
      expect(router.getShardingThresholds()).toEqual(customThresholds)
    })
  })

  describe('getShardsPrefix', () => {
    it('should return shards prefix for namespace', () => {
      const router = new StorageRouter({})
      expect(router.getShardsPrefix('orders')).toBe('orders/_shards/')
    })
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('StorageRouter - Integration', () => {
  it('should work with combined typed schema and sharding', () => {
    const router = new StorageRouter(
      {
        Orders: { id: 'string!', orderType: 'string!', total: 'number!' },
        Users: { id: 'string!', name: 'string!' },
        Events: 'flexible'
      },
      {
        sharding: {
          orders: { strategy: 'type', typeField: 'orderType' },
          users: { strategy: 'hash', shardCount: 8 }
        }
      }
    )

    // Schema detection
    expect(router.hasTypedSchema('orders')).toBe(true)
    expect(router.hasTypedSchema('users')).toBe(true)
    expect(router.hasTypedSchema('events')).toBe(false)

    // Sharding detection
    expect(router.isSharded('orders')).toBe(true)
    expect(router.isSharded('users')).toBe(true)
    expect(router.isSharded('events')).toBe(false)

    // Shard path resolution
    expect(router.getShardPath('orders', { orderType: 'purchase' })).toBe(
      'orders/_shards/type=purchase/data.parquet'
    )
  })

  it('should handle multi-tenant namespaces', () => {
    const router = new StorageRouter({}, {
      sharding: {
        'tenant-a/orders': { strategy: 'type', typeField: 'orderType' },
        'tenant-b/orders': { strategy: 'hash', shardCount: 4 }
      }
    })

    expect(router.isSharded('tenant-a/orders')).toBe(true)
    expect(router.getShardStrategy('tenant-a/orders')).toBe('type')
    expect(router.getShardStrategy('tenant-b/orders')).toBe('hash')
  })
})
