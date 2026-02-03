/**
 * Queue Partitioning Tests
 *
 * Tests for the horizontal queue partitioning module that enables
 * scaling compaction workloads across multiple queue consumers.
 */

import { describe, it, expect } from 'vitest'
import {
  fnv1a32,
  getPartitionForNamespace,
  createPartitionedQueueName,
  getAllQueueNames,
  analyzePartitionDistribution,
  getPartitionConfigFromEnv,
  validateMessagePartition,
  generateR2NotificationRules,
  DEFAULT_NUM_PARTITIONS,
  QUEUE_BASE_NAME,
} from '@/workflows/queue-partitioning'

// =============================================================================
// FNV-1a Hash Tests
// =============================================================================

describe('fnv1a32', () => {
  it('should produce consistent hashes for same input', () => {
    const hash1 = fnv1a32('users')
    const hash2 = fnv1a32('users')
    expect(hash1).toBe(hash2)
  })

  it('should produce different hashes for different inputs', () => {
    const hash1 = fnv1a32('users')
    const hash2 = fnv1a32('posts')
    const hash3 = fnv1a32('comments')

    expect(hash1).not.toBe(hash2)
    expect(hash2).not.toBe(hash3)
    expect(hash1).not.toBe(hash3)
  })

  it('should return unsigned 32-bit integers', () => {
    const inputs = ['users', 'posts', 'comments', 'likes', 'a', '', 'very-long-namespace-name']

    for (const input of inputs) {
      const hash = fnv1a32(input)
      expect(hash).toBeGreaterThanOrEqual(0)
      expect(hash).toBeLessThanOrEqual(0xFFFFFFFF)
      expect(Number.isInteger(hash)).toBe(true)
    }
  })

  it('should handle empty string', () => {
    const hash = fnv1a32('')
    expect(hash).toBeGreaterThanOrEqual(0)
    expect(Number.isInteger(hash)).toBe(true)
  })

  it('should handle unicode characters', () => {
    const hash = fnv1a32('usuarios')
    expect(hash).toBeGreaterThanOrEqual(0)
    expect(Number.isInteger(hash)).toBe(true)
  })

  it('should handle nested namespace paths', () => {
    const hash1 = fnv1a32('app/users')
    const hash2 = fnv1a32('app/posts')
    const hash3 = fnv1a32('other/users')

    expect(hash1).not.toBe(hash2)
    expect(hash1).not.toBe(hash3)
  })

  // Known FNV-1a test vectors (verified against reference implementation)
  it('should match known FNV-1a test vectors', () => {
    // Empty string: FNV offset basis
    expect(fnv1a32('')).toBe(0x811c9dc5)
  })
})

// =============================================================================
// Partition Routing Tests
// =============================================================================

describe('getPartitionForNamespace', () => {
  it('should return 0 for single partition', () => {
    expect(getPartitionForNamespace('users', 1)).toBe(0)
    expect(getPartitionForNamespace('posts', 1)).toBe(0)
    expect(getPartitionForNamespace('anything', 1)).toBe(0)
  })

  it('should return value in range [0, numPartitions)', () => {
    const namespaces = ['users', 'posts', 'comments', 'likes', 'shares', 'follows', 'messages']
    const numPartitions = 4

    for (const ns of namespaces) {
      const partition = getPartitionForNamespace(ns, numPartitions)
      expect(partition).toBeGreaterThanOrEqual(0)
      expect(partition).toBeLessThan(numPartitions)
    }
  })

  it('should be deterministic', () => {
    const numPartitions = 4

    for (let i = 0; i < 100; i++) {
      expect(getPartitionForNamespace('users', numPartitions))
        .toBe(getPartitionForNamespace('users', numPartitions))
    }
  })

  it('should distribute namespaces across partitions', () => {
    const namespaces = Array.from({ length: 100 }, (_, i) => `namespace-${i}`)
    const numPartitions = 4
    const counts: Record<number, number> = { 0: 0, 1: 0, 2: 0, 3: 0 }

    for (const ns of namespaces) {
      const partition = getPartitionForNamespace(ns, numPartitions)
      counts[partition]++
    }

    // Should have reasonable distribution (not all in one partition)
    expect(counts[0]).toBeGreaterThan(10)
    expect(counts[1]).toBeGreaterThan(10)
    expect(counts[2]).toBeGreaterThan(10)
    expect(counts[3]).toBeGreaterThan(10)
  })

  it('should use default partition count when not specified', () => {
    const partition = getPartitionForNamespace('users')
    expect(partition).toBe(0) // Default is 1 partition, so always 0
  })

  it('should throw for invalid partition count', () => {
    expect(() => getPartitionForNamespace('users', 0)).toThrow()
    expect(() => getPartitionForNamespace('users', -1)).toThrow()
  })

  it('should handle nested namespaces', () => {
    const partition1 = getPartitionForNamespace('app/users', 4)
    const partition2 = getPartitionForNamespace('app/posts', 4)

    // Should be deterministic
    expect(getPartitionForNamespace('app/users', 4)).toBe(partition1)
    expect(getPartitionForNamespace('app/posts', 4)).toBe(partition2)
  })
})

// =============================================================================
// Queue Name Generation Tests
// =============================================================================

describe('createPartitionedQueueName', () => {
  it('should return base name for single partition', () => {
    expect(createPartitionedQueueName('users', 1)).toBe(QUEUE_BASE_NAME)
    expect(createPartitionedQueueName('posts', 1)).toBe(QUEUE_BASE_NAME)
  })

  it('should return partitioned name for multiple partitions', () => {
    const name = createPartitionedQueueName('users', 4)
    expect(name).toMatch(/^parquedb-compaction-events-\d$/)
  })

  it('should be consistent with getPartitionForNamespace', () => {
    const numPartitions = 4
    const partition = getPartitionForNamespace('users', numPartitions)
    const queueName = createPartitionedQueueName('users', numPartitions)

    expect(queueName).toBe(`${QUEUE_BASE_NAME}-${partition}`)
  })

  it('should use default partition count when not specified', () => {
    expect(createPartitionedQueueName('users')).toBe(QUEUE_BASE_NAME)
  })
})

describe('getAllQueueNames', () => {
  it('should return single base name for single partition', () => {
    expect(getAllQueueNames(1)).toEqual([QUEUE_BASE_NAME])
  })

  it('should return partitioned names for multiple partitions', () => {
    const names = getAllQueueNames(4)
    expect(names).toEqual([
      'parquedb-compaction-events-0',
      'parquedb-compaction-events-1',
      'parquedb-compaction-events-2',
      'parquedb-compaction-events-3',
    ])
  })

  it('should return correct count of names', () => {
    expect(getAllQueueNames(1)).toHaveLength(1)
    expect(getAllQueueNames(2)).toHaveLength(2)
    expect(getAllQueueNames(8)).toHaveLength(8)
  })

  it('should use default partition count when not specified', () => {
    expect(getAllQueueNames()).toEqual([QUEUE_BASE_NAME])
  })
})

// =============================================================================
// Partition Analysis Tests
// =============================================================================

describe('analyzePartitionDistribution', () => {
  it('should analyze single namespace', () => {
    const analysis = analyzePartitionDistribution(['users'], 4)

    expect(Object.keys(analysis.partitionCounts)).toHaveLength(4)
    expect(analysis.minCount).toBe(0)
    expect(analysis.maxCount).toBe(1)
    expect(analysis.namespaceToPartition['users']).toBeDefined()
  })

  it('should analyze multiple namespaces', () => {
    const namespaces = ['users', 'posts', 'comments', 'likes', 'shares']
    const analysis = analyzePartitionDistribution(namespaces, 4)

    expect(analysis.minCount).toBeGreaterThanOrEqual(0)
    expect(analysis.maxCount).toBeLessThanOrEqual(namespaces.length)

    // All namespaces should be assigned
    for (const ns of namespaces) {
      expect(analysis.namespaceToPartition[ns]).toBeDefined()
      expect(analysis.namespaceToPartition[ns]).toBeGreaterThanOrEqual(0)
      expect(analysis.namespaceToPartition[ns]).toBeLessThan(4)
    }
  })

  it('should return correct partition counts', () => {
    const namespaces = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
    const analysis = analyzePartitionDistribution(namespaces, 2)

    const totalCount = Object.values(analysis.partitionCounts).reduce((a, b) => a + b, 0)
    expect(totalCount).toBe(namespaces.length)
  })

  it('should handle empty namespace list', () => {
    const analysis = analyzePartitionDistribution([], 4)

    expect(analysis.minCount).toBe(0)
    expect(analysis.maxCount).toBe(0)
    expect(Object.keys(analysis.namespaceToPartition)).toHaveLength(0)
  })

  it('should match partition assignments from getPartitionForNamespace', () => {
    const namespaces = ['users', 'posts', 'comments']
    const numPartitions = 4
    const analysis = analyzePartitionDistribution(namespaces, numPartitions)

    for (const ns of namespaces) {
      expect(analysis.namespaceToPartition[ns])
        .toBe(getPartitionForNamespace(ns, numPartitions))
    }
  })
})

// =============================================================================
// Environment Configuration Tests
// =============================================================================

describe('getPartitionConfigFromEnv', () => {
  it('should use defaults when env vars not set', () => {
    const config = getPartitionConfigFromEnv({})

    expect(config.numPartitions).toBe(1)
    expect(config.partitionIndex).toBe(0)
  })

  it('should parse NUM_PARTITIONS', () => {
    const config = getPartitionConfigFromEnv({ NUM_PARTITIONS: '4' })

    expect(config.numPartitions).toBe(4)
    expect(config.partitionIndex).toBe(0)
  })

  it('should parse PARTITION_INDEX', () => {
    const config = getPartitionConfigFromEnv({
      NUM_PARTITIONS: '4',
      PARTITION_INDEX: '2',
    })

    expect(config.numPartitions).toBe(4)
    expect(config.partitionIndex).toBe(2)
  })

  it('should throw for invalid NUM_PARTITIONS', () => {
    expect(() => getPartitionConfigFromEnv({ NUM_PARTITIONS: '0' })).toThrow()
    expect(() => getPartitionConfigFromEnv({ NUM_PARTITIONS: '-1' })).toThrow()
  })

  it('should throw for PARTITION_INDEX out of range', () => {
    expect(() => getPartitionConfigFromEnv({
      NUM_PARTITIONS: '4',
      PARTITION_INDEX: '4', // Should be 0-3
    })).toThrow()

    expect(() => getPartitionConfigFromEnv({
      NUM_PARTITIONS: '4',
      PARTITION_INDEX: '-1',
    })).toThrow()
  })

  it('should accept boundary partition indices', () => {
    // First partition
    const config1 = getPartitionConfigFromEnv({
      NUM_PARTITIONS: '4',
      PARTITION_INDEX: '0',
    })
    expect(config1.partitionIndex).toBe(0)

    // Last partition
    const config2 = getPartitionConfigFromEnv({
      NUM_PARTITIONS: '4',
      PARTITION_INDEX: '3',
    })
    expect(config2.partitionIndex).toBe(3)
  })
})

// =============================================================================
// Message Validation Tests
// =============================================================================

describe('validateMessagePartition', () => {
  it('should validate correctly routed message', () => {
    const numPartitions = 4
    const namespace = 'users'
    const expectedPartition = getPartitionForNamespace(namespace, numPartitions)

    expect(validateMessagePartition(namespace, numPartitions, expectedPartition)).toBe(true)
  })

  it('should reject incorrectly routed message', () => {
    const numPartitions = 4
    const namespace = 'users'
    const actualPartition = getPartitionForNamespace(namespace, numPartitions)
    const wrongPartition = (actualPartition + 1) % numPartitions

    expect(validateMessagePartition(namespace, numPartitions, wrongPartition)).toBe(false)
  })

  it('should always return true for single partition', () => {
    expect(validateMessagePartition('users', 1, 0)).toBe(true)
    expect(validateMessagePartition('posts', 1, 0)).toBe(true)
    expect(validateMessagePartition('anything', 1, 0)).toBe(true)
  })
})

// =============================================================================
// R2 Notification Rules Tests
// =============================================================================

describe('generateR2NotificationRules', () => {
  it('should generate rules for each namespace', () => {
    const namespaces = ['users', 'posts', 'comments']
    const rules = generateR2NotificationRules(namespaces, 4)

    expect(rules).toHaveLength(3)
    expect(rules.map(r => r.prefix)).toContain('data/users/')
    expect(rules.map(r => r.prefix)).toContain('data/posts/')
    expect(rules.map(r => r.prefix)).toContain('data/comments/')
  })

  it('should generate correct rule structure', () => {
    const rules = generateR2NotificationRules(['users'], 4)
    const rule = rules[0]

    expect(rule).toHaveProperty('prefix', 'data/users/')
    expect(rule).toHaveProperty('suffix', '.parquet')
    expect(rule).toHaveProperty('queue')
    expect(rule).toHaveProperty('eventTypes', ['object-create'])
  })

  it('should use correct queue names', () => {
    const namespaces = ['users', 'posts']
    const numPartitions = 4
    const rules = generateR2NotificationRules(namespaces, numPartitions)

    for (const rule of rules) {
      const ns = rule.prefix.replace('data/', '').replace('/', '')
      const expectedQueue = createPartitionedQueueName(ns, numPartitions)
      expect(rule.queue).toBe(expectedQueue)
    }
  })

  it('should use custom data prefix', () => {
    const rules = generateR2NotificationRules(['users'], 4, 'events/')
    expect(rules[0].prefix).toBe('events/users/')
  })

  it('should handle empty namespace list', () => {
    const rules = generateR2NotificationRules([], 4)
    expect(rules).toHaveLength(0)
  })
})

// =============================================================================
// Constants Tests
// =============================================================================

describe('Constants', () => {
  it('should have correct default values', () => {
    expect(DEFAULT_NUM_PARTITIONS).toBe(1)
    expect(QUEUE_BASE_NAME).toBe('parquedb-compaction-events')
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Partition Routing Integration', () => {
  it('should maintain consistent routing across functions', () => {
    const namespace = 'users'
    const numPartitions = 4

    const partition = getPartitionForNamespace(namespace, numPartitions)
    const queueName = createPartitionedQueueName(namespace, numPartitions)
    const analysis = analyzePartitionDistribution([namespace], numPartitions)

    // All should agree on the partition
    expect(queueName).toBe(`${QUEUE_BASE_NAME}-${partition}`)
    expect(analysis.namespaceToPartition[namespace]).toBe(partition)
  })

  it('should provide stable routing as partitions change', () => {
    // When increasing partitions, some namespaces will move
    // but the algorithm should be predictable
    const namespace = 'users'

    // Get hash once
    const hash = fnv1a32(namespace)

    // Verify modulo behavior
    expect(getPartitionForNamespace(namespace, 2)).toBe(hash % 2)
    expect(getPartitionForNamespace(namespace, 4)).toBe(hash % 4)
    expect(getPartitionForNamespace(namespace, 8)).toBe(hash % 8)
  })

  it('should work end-to-end for typical workflow', () => {
    // Simulate a typical setup
    const namespaces = ['users', 'posts', 'comments', 'likes']
    const numPartitions = 4

    // 1. Analyze distribution
    const analysis = analyzePartitionDistribution(namespaces, numPartitions)
    expect(analysis.maxCount).toBeLessThanOrEqual(namespaces.length)

    // 2. Generate R2 notification rules
    const rules = generateR2NotificationRules(namespaces, numPartitions)
    expect(rules).toHaveLength(namespaces.length)

    // 3. Get all queue names for wrangler config
    const queueNames = getAllQueueNames(numPartitions)
    expect(queueNames).toHaveLength(numPartitions)

    // 4. Validate routing for each namespace
    for (const ns of namespaces) {
      const partition = getPartitionForNamespace(ns, numPartitions)
      expect(validateMessagePartition(ns, numPartitions, partition)).toBe(true)
    }
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  it('should handle very long namespace names', () => {
    const longNamespace = 'a'.repeat(1000)
    const partition = getPartitionForNamespace(longNamespace, 4)

    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(4)
  })

  it('should handle special characters in namespace', () => {
    const namespaces = [
      'user-data',
      'user_data',
      'user.data',
      'user/nested/path',
      'user:special',
    ]

    for (const ns of namespaces) {
      const partition = getPartitionForNamespace(ns, 4)
      expect(partition).toBeGreaterThanOrEqual(0)
      expect(partition).toBeLessThan(4)
    }
  })

  it('should handle large number of partitions', () => {
    const partition = getPartitionForNamespace('users', 100)
    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(100)
  })

  it('should handle power-of-2 partition counts', () => {
    // Power-of-2 partitions are common for consistent hashing
    const powers = [2, 4, 8, 16, 32, 64]

    for (const n of powers) {
      const partition = getPartitionForNamespace('users', n)
      expect(partition).toBeGreaterThanOrEqual(0)
      expect(partition).toBeLessThan(n)
    }
  })

  it('should handle non-power-of-2 partition counts', () => {
    // Non-power-of-2 should also work
    const counts = [3, 5, 7, 10, 12]

    for (const n of counts) {
      const partition = getPartitionForNamespace('users', n)
      expect(partition).toBeGreaterThanOrEqual(0)
      expect(partition).toBeLessThan(n)
    }
  })
})
