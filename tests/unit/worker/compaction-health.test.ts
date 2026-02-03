/**
 * Tests for /compaction/health endpoint
 *
 * Tests the compaction health check endpoint that provides alerting metrics.
 */

import { describe, it, expect } from 'vitest'
import {
  evaluateNamespaceHealth,
  aggregateHealthStatus,
  isCompactionStatusResponse,
  DEFAULT_COMPACTION_HEALTH_CONFIG,
  type CompactionStatusResponse,
  type NamespaceHealth,
  type CompactionHealthConfig,
} from '../../../src/workflows/compaction-queue-consumer'

// =============================================================================
// evaluateNamespaceHealth Tests
// =============================================================================

describe('evaluateNamespaceHealth', () => {
  const defaultConfig: CompactionHealthConfig = DEFAULT_COMPACTION_HEALTH_CONFIG

  it('should return healthy for normal metrics', () => {
    const result = evaluateNamespaceHealth('users', {
      activeWindows: 2,
      oldestWindowAge: 3600000, // 1 hour
      totalPendingFiles: 20,
      windowsStuckInProcessing: 0,
    }, defaultConfig)

    expect(result.status).toBe('healthy')
    expect(result.issues).toHaveLength(0)
    expect(result.namespace).toBe('users')
  })

  it('should return unhealthy for stuck processing windows', () => {
    const result = evaluateNamespaceHealth('users', {
      activeWindows: 2,
      oldestWindowAge: 3600000,
      totalPendingFiles: 20,
      windowsStuckInProcessing: 1,
    }, defaultConfig)

    expect(result.status).toBe('unhealthy')
    expect(result.issues).toContain('1 window(s) stuck in processing')
  })

  it('should return unhealthy for multiple stuck windows', () => {
    const result = evaluateNamespaceHealth('posts', {
      activeWindows: 5,
      oldestWindowAge: 3600000,
      totalPendingFiles: 50,
      windowsStuckInProcessing: 3,
    }, defaultConfig)

    expect(result.status).toBe('unhealthy')
    expect(result.issues).toContain('3 window(s) stuck in processing')
  })

  it('should return degraded for too many pending windows', () => {
    const result = evaluateNamespaceHealth('users', {
      activeWindows: 15, // > 10 default threshold
      oldestWindowAge: 3600000,
      totalPendingFiles: 150,
      windowsStuckInProcessing: 0,
    }, defaultConfig)

    expect(result.status).toBe('degraded')
    expect(result.issues.some(i => i.includes('15 windows pending'))).toBe(true)
  })

  it('should return degraded for old windows', () => {
    const result = evaluateNamespaceHealth('users', {
      activeWindows: 2,
      oldestWindowAge: 3 * 3600000, // 3 hours > 2 hour default
      totalPendingFiles: 20,
      windowsStuckInProcessing: 0,
    }, defaultConfig)

    expect(result.status).toBe('degraded')
    expect(result.issues.some(i => i.includes('oldest window age'))).toBe(true)
  })

  it('should use custom thresholds', () => {
    const customConfig: CompactionHealthConfig = {
      maxPendingWindows: 5,
      maxWindowAgeHours: 1,
    }

    const result = evaluateNamespaceHealth('users', {
      activeWindows: 7, // > 5 custom threshold
      oldestWindowAge: 1.5 * 3600000, // 1.5 hours > 1 hour custom
      totalPendingFiles: 70,
      windowsStuckInProcessing: 0,
    }, customConfig)

    expect(result.status).toBe('degraded')
    expect(result.issues).toHaveLength(2)
  })

  it('should prioritize unhealthy over degraded', () => {
    const result = evaluateNamespaceHealth('users', {
      activeWindows: 15, // degraded condition
      oldestWindowAge: 5 * 3600000, // degraded condition
      totalPendingFiles: 150,
      windowsStuckInProcessing: 1, // unhealthy condition
    }, defaultConfig)

    expect(result.status).toBe('unhealthy')
    expect(result.issues).toHaveLength(3)
  })

  it('should include all metrics in response', () => {
    const input = {
      activeWindows: 5,
      oldestWindowAge: 7200000,
      totalPendingFiles: 50,
      windowsStuckInProcessing: 0,
    }

    const result = evaluateNamespaceHealth('test-ns', input, defaultConfig)

    expect(result.metrics).toEqual(input)
  })
})

// =============================================================================
// aggregateHealthStatus Tests
// =============================================================================

describe('aggregateHealthStatus', () => {
  it('should return healthy when all namespaces are healthy', () => {
    const namespaces: Record<string, NamespaceHealth> = {
      users: {
        namespace: 'users',
        status: 'healthy',
        metrics: { activeWindows: 2, oldestWindowAge: 3600000, totalPendingFiles: 20, windowsStuckInProcessing: 0 },
        issues: [],
      },
      posts: {
        namespace: 'posts',
        status: 'healthy',
        metrics: { activeWindows: 1, oldestWindowAge: 1800000, totalPendingFiles: 10, windowsStuckInProcessing: 0 },
        issues: [],
      },
    }

    const result = aggregateHealthStatus(namespaces)

    expect(result.status).toBe('healthy')
    expect(result.alerts).toHaveLength(0)
    expect(Object.keys(result.namespaces)).toHaveLength(2)
  })

  it('should return degraded when any namespace is degraded', () => {
    const namespaces: Record<string, NamespaceHealth> = {
      users: {
        namespace: 'users',
        status: 'healthy',
        metrics: { activeWindows: 2, oldestWindowAge: 3600000, totalPendingFiles: 20, windowsStuckInProcessing: 0 },
        issues: [],
      },
      posts: {
        namespace: 'posts',
        status: 'degraded',
        metrics: { activeWindows: 15, oldestWindowAge: 1800000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
        issues: ['15 windows pending (threshold: 10)'],
      },
    }

    const result = aggregateHealthStatus(namespaces)

    expect(result.status).toBe('degraded')
    expect(result.alerts).toContain('posts: 15 windows pending (threshold: 10)')
  })

  it('should return unhealthy when any namespace is unhealthy', () => {
    const namespaces: Record<string, NamespaceHealth> = {
      users: {
        namespace: 'users',
        status: 'degraded',
        metrics: { activeWindows: 15, oldestWindowAge: 3600000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
        issues: ['15 windows pending (threshold: 10)'],
      },
      posts: {
        namespace: 'posts',
        status: 'unhealthy',
        metrics: { activeWindows: 5, oldestWindowAge: 1800000, totalPendingFiles: 50, windowsStuckInProcessing: 2 },
        issues: ['2 window(s) stuck in processing'],
      },
    }

    const result = aggregateHealthStatus(namespaces)

    expect(result.status).toBe('unhealthy')
  })

  it('should aggregate alerts from all namespaces', () => {
    const namespaces: Record<string, NamespaceHealth> = {
      users: {
        namespace: 'users',
        status: 'degraded',
        metrics: { activeWindows: 15, oldestWindowAge: 10800000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
        issues: ['15 windows pending (threshold: 10)', 'oldest window age: 3h (threshold: 2h)'],
      },
      posts: {
        namespace: 'posts',
        status: 'unhealthy',
        metrics: { activeWindows: 5, oldestWindowAge: 1800000, totalPendingFiles: 50, windowsStuckInProcessing: 1 },
        issues: ['1 window(s) stuck in processing'],
      },
      comments: {
        namespace: 'comments',
        status: 'healthy',
        metrics: { activeWindows: 1, oldestWindowAge: 600000, totalPendingFiles: 10, windowsStuckInProcessing: 0 },
        issues: [],
      },
    }

    const result = aggregateHealthStatus(namespaces)

    expect(result.alerts).toHaveLength(3)
    expect(result.alerts).toContain('users: 15 windows pending (threshold: 10)')
    expect(result.alerts).toContain('users: oldest window age: 3h (threshold: 2h)')
    expect(result.alerts).toContain('posts: 1 window(s) stuck in processing')
  })

  it('should handle empty namespaces', () => {
    const result = aggregateHealthStatus({})

    expect(result.status).toBe('healthy')
    expect(result.alerts).toHaveLength(0)
    expect(Object.keys(result.namespaces)).toHaveLength(0)
  })
})

// =============================================================================
// isCompactionStatusResponse Tests
// =============================================================================

describe('isCompactionStatusResponse', () => {
  it('should return true for valid status response', () => {
    const validResponse: CompactionStatusResponse = {
      namespace: 'users',
      activeWindows: 2,
      knownWriters: ['writer1', 'writer2'],
      activeWriters: ['writer1'],
      oldestWindowAge: 3600000,
      totalPendingFiles: 20,
      windowsStuckInProcessing: 0,
      windows: [],
    }

    expect(isCompactionStatusResponse(validResponse)).toBe(true)
  })

  it('should return false for null', () => {
    expect(isCompactionStatusResponse(null)).toBe(false)
  })

  it('should return false for non-object', () => {
    expect(isCompactionStatusResponse('string')).toBe(false)
    expect(isCompactionStatusResponse(123)).toBe(false)
    expect(isCompactionStatusResponse([])).toBe(false)
  })

  it('should return false for missing required fields', () => {
    expect(isCompactionStatusResponse({ namespace: 'test' })).toBe(false)
    expect(isCompactionStatusResponse({ activeWindows: 5 })).toBe(false)
    expect(isCompactionStatusResponse({
      namespace: 'test',
      activeWindows: 5,
      // missing oldestWindowAge
    })).toBe(false)
  })

  it('should return false for wrong field types', () => {
    expect(isCompactionStatusResponse({
      namespace: 123, // should be string
      activeWindows: 5,
      oldestWindowAge: 1000,
      totalPendingFiles: 10,
      windowsStuckInProcessing: 0,
    })).toBe(false)

    expect(isCompactionStatusResponse({
      namespace: 'test',
      activeWindows: '5', // should be number
      oldestWindowAge: 1000,
      totalPendingFiles: 10,
      windowsStuckInProcessing: 0,
    })).toBe(false)
  })
})

// =============================================================================
// Response Format Tests
// =============================================================================

describe('CompactionHealthResponse format', () => {
  it('should produce valid JSON structure', () => {
    const health = evaluateNamespaceHealth('users', {
      activeWindows: 15,
      oldestWindowAge: 10800000,
      totalPendingFiles: 150,
      windowsStuckInProcessing: 1,
    }, DEFAULT_COMPACTION_HEALTH_CONFIG)

    const response = aggregateHealthStatus({ users: health })

    // Serialize and parse to ensure valid JSON
    const json = JSON.stringify(response, null, 2)
    const parsed = JSON.parse(json)

    expect(parsed).toHaveProperty('status')
    expect(parsed).toHaveProperty('namespaces')
    expect(parsed).toHaveProperty('alerts')
    expect(['healthy', 'degraded', 'unhealthy']).toContain(parsed.status)
  })

  it('should include namespace metrics', () => {
    const health = evaluateNamespaceHealth('users', {
      activeWindows: 5,
      oldestWindowAge: 3600000,
      totalPendingFiles: 50,
      windowsStuckInProcessing: 0,
    }, DEFAULT_COMPACTION_HEALTH_CONFIG)

    const response = aggregateHealthStatus({ users: health })

    expect(response.namespaces.users.metrics).toEqual({
      activeWindows: 5,
      oldestWindowAge: 3600000,
      totalPendingFiles: 50,
      windowsStuckInProcessing: 0,
    })
  })
})
