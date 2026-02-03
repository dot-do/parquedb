/**
 * Time Bucket Sharding Tests
 *
 * Tests for the time bucket sharding feature that distributes CompactionStateDO
 * instances across time buckets for extreme concurrency (>1000 writes/sec).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  calculateTimeBucket,
  getCompactionStateDOId,
  parseCompactionStateDOId,
  getRecentTimeBuckets,
  isTimeBucketExpired,
  groupUpdatesByDOId,
  shouldUseTimeBucketSharding,
  type TimeBucketShardingConfig,
} from '../../../src/workflows/compaction-queue-consumer'

// =============================================================================
// calculateTimeBucket Tests
// =============================================================================

describe('calculateTimeBucket', () => {
  it('should calculate bucket for timestamp with default bucket size (1 hour)', () => {
    const timestamp = 1700000000000 // Nov 14, 2023 22:13:20 UTC
    const bucket = calculateTimeBucket(timestamp)

    // 1 hour = 3600000 ms
    const expectedBucket = Math.floor(timestamp / 3600000)
    expect(bucket).toBe(expectedBucket)
  })

  it('should calculate bucket for custom bucket size', () => {
    const timestamp = 1700000000000
    const bucketSizeMs = 1800000 // 30 minutes

    const bucket = calculateTimeBucket(timestamp, bucketSizeMs)
    const expectedBucket = Math.floor(timestamp / bucketSizeMs)
    expect(bucket).toBe(expectedBucket)
  })

  it('should group timestamps within same hour to same bucket', () => {
    // Start at a bucket boundary to ensure all additions stay within the same bucket
    const bucketSizeMs = 3600000 // 1 hour
    const baseTimestamp = Math.floor(1700000000000 / bucketSizeMs) * bucketSizeMs // Align to bucket start
    const bucket1 = calculateTimeBucket(baseTimestamp)
    const bucket2 = calculateTimeBucket(baseTimestamp + 1000) // +1 second
    const bucket3 = calculateTimeBucket(baseTimestamp + 3599999) // +59:59.999 (just before bucket ends)

    expect(bucket1).toBe(bucket2)
    expect(bucket2).toBe(bucket3)
  })

  it('should separate timestamps in different hours to different buckets', () => {
    const baseTimestamp = 1700000000000
    const bucket1 = calculateTimeBucket(baseTimestamp)
    const bucket2 = calculateTimeBucket(baseTimestamp + 3600000) // +1 hour

    expect(bucket2).toBe(bucket1 + 1)
  })
})

// =============================================================================
// getCompactionStateDOId Tests
// =============================================================================

describe('getCompactionStateDOId', () => {
  it('should return namespace only when sharding is disabled', () => {
    const doId = getCompactionStateDOId('users', 12345, false)
    expect(doId).toBe('users')
  })

  it('should return namespace only when timeBucket is undefined', () => {
    const doId = getCompactionStateDOId('users', undefined, true)
    expect(doId).toBe('users')
  })

  it('should return namespace:timeBucket when sharding is enabled', () => {
    const doId = getCompactionStateDOId('users', 472222, true)
    expect(doId).toBe('users:472222')
  })

  it('should handle namespaces with special characters', () => {
    const doId = getCompactionStateDOId('org/users', 472222, true)
    expect(doId).toBe('org/users:472222')
  })

  it('should handle nested namespaces', () => {
    const doId = getCompactionStateDOId('tenant/org/users', 472222, true)
    expect(doId).toBe('tenant/org/users:472222')
  })
})

// =============================================================================
// parseCompactionStateDOId Tests
// =============================================================================

describe('parseCompactionStateDOId', () => {
  it('should parse simple namespace', () => {
    const result = parseCompactionStateDOId('users')
    expect(result).toEqual({ namespace: 'users' })
  })

  it('should parse namespace with time bucket', () => {
    const result = parseCompactionStateDOId('users:472222')
    expect(result).toEqual({ namespace: 'users', timeBucket: 472222 })
  })

  it('should parse nested namespace without bucket', () => {
    const result = parseCompactionStateDOId('org/users')
    expect(result).toEqual({ namespace: 'org/users' })
  })

  it('should parse nested namespace with bucket', () => {
    const result = parseCompactionStateDOId('org/users:472222')
    expect(result).toEqual({ namespace: 'org/users', timeBucket: 472222 })
  })

  it('should handle namespace containing colon without bucket', () => {
    // If the part after colon is not a number, treat it as part of namespace
    const result = parseCompactionStateDOId('ns:with:colons')
    // Last colon before 'colons' - 'colons' is not a number
    expect(result).toEqual({ namespace: 'ns:with:colons' })
  })

  it('should handle deeply nested namespace with bucket', () => {
    const result = parseCompactionStateDOId('tenant/org/namespace:472222')
    expect(result).toEqual({ namespace: 'tenant/org/namespace', timeBucket: 472222 })
  })
})

// =============================================================================
// getRecentTimeBuckets Tests
// =============================================================================

describe('getRecentTimeBuckets', () => {
  it('should return 25 buckets for default 24 hours (with current)', () => {
    const now = Date.now()
    const buckets = getRecentTimeBuckets(now)

    // 24 hours + current hour = up to 25 buckets
    expect(buckets.length).toBe(25)
  })

  it('should return buckets in descending order (newest first)', () => {
    const now = Date.now()
    const buckets = getRecentTimeBuckets(now)

    for (let i = 1; i < buckets.length; i++) {
      expect(buckets[i]).toBeLessThan(buckets[i - 1]!)
    }
  })

  it('should include current bucket as first element', () => {
    const now = Date.now()
    const buckets = getRecentTimeBuckets(now)
    const currentBucket = calculateTimeBucket(now)

    expect(buckets[0]).toBe(currentBucket)
  })

  it('should respect custom maxAgeHours', () => {
    const now = Date.now()
    const buckets = getRecentTimeBuckets(now, 6) // 6 hours

    // 6 hours = 6-7 buckets (depending on current position in hour)
    expect(buckets.length).toBe(7)
  })

  it('should respect custom bucketSizeMs', () => {
    const now = Date.now()
    const bucketSizeMs = 1800000 // 30 minutes
    const buckets = getRecentTimeBuckets(now, 6, bucketSizeMs)

    // 6 hours with 30 min buckets = 12 intervals + current = 13
    expect(buckets.length).toBe(13)
  })
})

// =============================================================================
// isTimeBucketExpired Tests
// =============================================================================

describe('isTimeBucketExpired', () => {
  it('should return false for current bucket', () => {
    const now = Date.now()
    const currentBucket = calculateTimeBucket(now)

    const expired = isTimeBucketExpired(currentBucket, now)
    expect(expired).toBe(false)
  })

  it('should return false for bucket within cleanup age', () => {
    const now = Date.now()
    const bucketSizeMs = 3600000 // 1 hour
    // Bucket from 24 hours ago (within 48 hour cleanup window)
    const bucket = calculateTimeBucket(now - 24 * 3600000)

    const expired = isTimeBucketExpired(bucket, now, bucketSizeMs)
    expect(expired).toBe(false)
  })

  it('should return true for bucket older than cleanup age', () => {
    const now = Date.now()
    const bucketSizeMs = 3600000 // 1 hour
    // Bucket from 72 hours ago (past 48 hour cleanup window)
    const bucket = calculateTimeBucket(now - 72 * 3600000)

    const expired = isTimeBucketExpired(bucket, now, bucketSizeMs)
    expect(expired).toBe(true)
  })

  it('should respect custom cleanup age', () => {
    const now = Date.now()
    const bucketSizeMs = 3600000 // 1 hour
    const cleanupAgeMs = 6 * 3600000 // 6 hours

    // Bucket from 8 hours ago (past 6 hour cleanup window)
    const bucket = calculateTimeBucket(now - 8 * 3600000)

    const expired = isTimeBucketExpired(bucket, now, bucketSizeMs, cleanupAgeMs)
    expect(expired).toBe(true)
  })
})

// =============================================================================
// shouldUseTimeBucketSharding Tests
// =============================================================================

describe('shouldUseTimeBucketSharding', () => {
  it('should return false when config is undefined', () => {
    expect(shouldUseTimeBucketSharding('users')).toBe(false)
  })

  it('should return false when enabled is false', () => {
    const config: TimeBucketShardingConfig = { enabled: false }
    expect(shouldUseTimeBucketSharding('users', config)).toBe(false)
  })

  it('should return true when enabled and no namespace filter', () => {
    const config: TimeBucketShardingConfig = { enabled: true }
    expect(shouldUseTimeBucketSharding('users', config)).toBe(true)
  })

  it('should return true when enabled with empty namespace filter', () => {
    const config: TimeBucketShardingConfig = {
      enabled: true,
      namespacesWithSharding: [],
    }
    expect(shouldUseTimeBucketSharding('users', config)).toBe(true)
  })

  it('should return true when namespace is in filter', () => {
    const config: TimeBucketShardingConfig = {
      enabled: true,
      namespacesWithSharding: ['users', 'events'],
    }
    expect(shouldUseTimeBucketSharding('users', config)).toBe(true)
    expect(shouldUseTimeBucketSharding('events', config)).toBe(true)
  })

  it('should return false when namespace is not in filter', () => {
    const config: TimeBucketShardingConfig = {
      enabled: true,
      namespacesWithSharding: ['users', 'events'],
    }
    expect(shouldUseTimeBucketSharding('posts', config)).toBe(false)
  })
})

// =============================================================================
// groupUpdatesByDOId Tests
// =============================================================================

describe('groupUpdatesByDOId', () => {
  const createUpdate = (namespace: string, timestamp: number) => ({
    namespace,
    writerId: 'writer1',
    file: `data/${namespace}/file.parquet`,
    timestamp,
    size: 1024,
  })

  it('should group by namespace only when sharding is disabled', () => {
    const updates = [
      createUpdate('users', 1700000000000),
      createUpdate('users', 1700000000001),
      createUpdate('posts', 1700000000000),
    ]

    const grouped = groupUpdatesByDOId(updates, false)

    expect(grouped.size).toBe(2)
    expect(grouped.get('users')?.length).toBe(2)
    expect(grouped.get('posts')?.length).toBe(1)
  })

  it('should group by namespace and bucket when sharding is enabled', () => {
    const baseTimestamp = 1700000000000
    const updates = [
      createUpdate('users', baseTimestamp),
      createUpdate('users', baseTimestamp + 1000), // Same hour
      createUpdate('users', baseTimestamp + 3600000), // Next hour
    ]

    const grouped = groupUpdatesByDOId(updates, true)

    expect(grouped.size).toBe(2) // Two different time buckets
  })

  it('should use custom bucket size', () => {
    const baseTimestamp = 1700000000000
    const bucketSizeMs = 1800000 // 30 minutes

    const updates = [
      createUpdate('users', baseTimestamp),
      createUpdate('users', baseTimestamp + 1800000), // 30 min later - different bucket
    ]

    const grouped = groupUpdatesByDOId(updates, true, bucketSizeMs)

    expect(grouped.size).toBe(2)
  })

  it('should separate namespaces even with same timestamps', () => {
    const timestamp = 1700000000000
    const updates = [
      createUpdate('users', timestamp),
      createUpdate('posts', timestamp),
    ]

    const grouped = groupUpdatesByDOId(updates, true)

    expect(grouped.size).toBe(2)
    // Each should have its own namespace:bucket key
  })

  it('should preserve all update fields', () => {
    const update = {
      namespace: 'users',
      writerId: 'writer-abc',
      file: 'data/users/123.parquet',
      timestamp: 1700000000000,
      size: 2048,
    }

    const grouped = groupUpdatesByDOId([update], false)
    const groupedUpdate = grouped.get('users')?.[0]

    expect(groupedUpdate).toEqual(update)
  })
})

// =============================================================================
// Integration: Round-trip Tests
// =============================================================================

describe('Time Bucket Sharding - Round-trip', () => {
  it('should correctly round-trip DO ID creation and parsing', () => {
    const namespace = 'org/users'
    const timestamp = 1700000000000
    const bucket = calculateTimeBucket(timestamp)

    // Create DO ID
    const doId = getCompactionStateDOId(namespace, bucket, true)

    // Parse it back
    const parsed = parseCompactionStateDOId(doId)

    expect(parsed.namespace).toBe(namespace)
    expect(parsed.timeBucket).toBe(bucket)
  })

  it('should handle updates grouped and then identified correctly', () => {
    const updates = [
      {
        namespace: 'users',
        writerId: 'w1',
        file: 'f1.parquet',
        timestamp: 1700000000000,
        size: 100,
      },
    ]

    // Group updates
    const grouped = groupUpdatesByDOId(updates, true)

    // Get the DO ID
    const doId = Array.from(grouped.keys())[0]!

    // Parse it
    const parsed = parseCompactionStateDOId(doId)

    expect(parsed.namespace).toBe('users')
    expect(parsed.timeBucket).toBe(calculateTimeBucket(1700000000000))
  })
})
