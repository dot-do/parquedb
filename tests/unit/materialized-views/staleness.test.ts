/**
 * Staleness Detection Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  StalenessDetector,
  InMemoryVersionProvider,
  createStalenessDetector,
  createInMemoryVersionProvider,
  createInitialLineage,
  updateLineageAfterRefresh,
  createNativeVersion,
  createIcebergVersion,
  createDeltaVersion,
  parseThresholdDuration,
  createThresholdsFromConfig,
  DEFAULT_STALENESS_THRESHOLDS,
  type MVLineage,
  type SourceVersion,
  type StalenessThresholds,
} from '@/materialized-views/staleness'
import { viewName } from '@/materialized-views/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createTestVersion(versionId: string, timestamp?: number): SourceVersion {
  return {
    versionId,
    timestamp: timestamp ?? Date.now(),
    backend: 'native',
  }
}

function createTestLineage(overrides: Partial<MVLineage> = {}): MVLineage {
  return {
    viewName: viewName('test-view'),
    sourceVersions: new Map(),
    definitionVersionId: 'def-v1',
    lastRefreshTime: Date.now(),
    lastRefreshDurationMs: 100,
    lastRefreshRecordCount: 50,
    lastRefreshType: 'full',
    ...overrides,
  }
}

// =============================================================================
// StalenessDetector Tests
// =============================================================================

describe('StalenessDetector', () => {
  let provider: InMemoryVersionProvider
  let detector: StalenessDetector

  beforeEach(() => {
    provider = createInMemoryVersionProvider()
    detector = createStalenessDetector(provider)
  })

  describe('isStale', () => {
    it('returns false when source version matches', async () => {
      const version = createTestVersion('v1')
      provider.setVersion('orders', version)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', version]]),
      })

      const result = await detector.isStale(lineage, ['orders'])
      expect(result).toBe(false)
    })

    it('returns true when source version differs', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
      })

      const result = await detector.isStale(lineage, ['orders'])
      expect(result).toBe(true)
    })

    it('returns true when source has no recorded version', async () => {
      const version = createTestVersion('v1')
      provider.setVersion('orders', version)

      const lineage = createTestLineage({
        sourceVersions: new Map(), // No versions recorded
      })

      const result = await detector.isStale(lineage, ['orders'])
      expect(result).toBe(true)
    })

    it('handles multiple sources', async () => {
      const ordersV1 = createTestVersion('orders-v1')
      const usersV1 = createTestVersion('users-v1')
      const usersV2 = createTestVersion('users-v2')

      provider.setVersion('orders', ordersV1)
      provider.setVersion('users', usersV2)

      const lineage = createTestLineage({
        sourceVersions: new Map([
          ['orders', ordersV1],
          ['users', usersV1], // Outdated
        ]),
      })

      const result = await detector.isStale(lineage, ['orders', 'users'])
      expect(result).toBe(true)
    })

    it('returns false when source does not exist', async () => {
      // No version set for 'orders'
      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', createTestVersion('v1')]]),
      })

      const result = await detector.isStale(lineage, ['orders'])
      expect(result).toBe(false) // Can't determine, assume fresh
    })
  })

  describe('isWithinGracePeriod', () => {
    it('returns true when within grace period', () => {
      const lineage = createTestLineage({
        lastRefreshTime: Date.now() - 1000, // 1 second ago
      })

      const result = detector.isWithinGracePeriod(lineage)
      expect(result).toBe(true)
    })

    it('returns false when outside grace period', () => {
      const lineage = createTestLineage({
        lastRefreshTime: Date.now() - (20 * 60 * 1000), // 20 minutes ago (default grace is 15)
      })

      const result = detector.isWithinGracePeriod(lineage)
      expect(result).toBe(false)
    })

    it('respects custom grace period', () => {
      detector = createStalenessDetector(provider, {
        gracePeriodMs: 5000, // 5 seconds
      })

      const lineage = createTestLineage({
        lastRefreshTime: Date.now() - 3000, // 3 seconds ago
      })

      expect(detector.isWithinGracePeriod(lineage)).toBe(true)

      const oldLineage = createTestLineage({
        lastRefreshTime: Date.now() - 6000, // 6 seconds ago
      })

      expect(detector.isWithinGracePeriod(oldLineage)).toBe(false)
    })
  })

  describe('needsImmediateRefresh', () => {
    it('returns false when within grace period even if stale', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - 1000, // 1 second ago - within grace
      })

      const result = await detector.needsImmediateRefresh(lineage, ['orders'])
      expect(result).toBe(false)
    })

    it('returns true when stale and outside grace period', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - (20 * 60 * 1000), // 20 minutes ago
      })

      const result = await detector.needsImmediateRefresh(lineage, ['orders'])
      expect(result).toBe(true)
    })

    it('returns false when fresh and outside grace period', async () => {
      const version = createTestVersion('v1')
      provider.setVersion('orders', version)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', version]]),
        lastRefreshTime: Date.now() - (20 * 60 * 1000), // 20 minutes ago
      })

      const result = await detector.needsImmediateRefresh(lineage, ['orders'])
      expect(result).toBe(false)
    })
  })

  describe('getState', () => {
    it('returns fresh when not stale and recently refreshed', async () => {
      const version = createTestVersion('v1')
      provider.setVersion('orders', version)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', version]]),
        lastRefreshTime: Date.now() - 1000, // 1 second ago
      })

      const result = await detector.getState(lineage, ['orders'])
      expect(result).toBe('fresh')
    })

    it('returns stale when source changed', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - 1000,
      })

      const result = await detector.getState(lineage, ['orders'])
      expect(result).toBe('stale')
    })

    it('returns invalid when critically stale', async () => {
      detector = createStalenessDetector(provider, {
        gracePeriodMs: 60000, // 1 minute
        criticalStalenessRatio: 2.0,
      })

      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - (60 * 60 * 1000), // 1 hour ago
      })

      const result = await detector.getState(lineage, ['orders'])
      expect(result).toBe('invalid')
    })
  })

  describe('getMetrics', () => {
    it('returns comprehensive metrics for fresh view', async () => {
      const version = createTestVersion('v1')
      provider.setVersion('orders', version)

      const lineage = createTestLineage({
        viewName: viewName('orders-view'),
        sourceVersions: new Map([['orders', version]]),
        lastRefreshTime: Date.now() - 1000,
      })

      const metrics = await detector.getMetrics(viewName('orders-view'), lineage, ['orders'])

      expect(metrics.viewName).toBe('orders-view')
      expect(metrics.state).toBe('fresh')
      expect(metrics.usable).toBe(true)
      expect(metrics.timeSinceRefresh).toBeLessThan(2000)
      expect(metrics.stalenessPercent).toBeLessThan(1)
      expect(metrics.withinGracePeriod).toBe(true)
      expect(metrics.gracePeriodRemainingMs).toBeGreaterThan(0)
      expect(metrics.sources).toHaveLength(1)
      expect(metrics.sources[0].isStale).toBe(false)
      expect(metrics.recommendedRefreshType).toBe('none')
      expect(metrics.reason).toBe('View is fresh')
    })

    it('returns metrics for stale view within grace period', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)
      provider.setChangeCount('orders', 50)

      const lineage = createTestLineage({
        viewName: viewName('orders-view'),
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - (5 * 60 * 1000), // 5 minutes ago
      })

      const metrics = await detector.getMetrics(viewName('orders-view'), lineage, ['orders'])

      expect(metrics.state).toBe('stale')
      expect(metrics.usable).toBe(true) // Still usable within grace
      expect(metrics.withinGracePeriod).toBe(true)
      expect(metrics.sources[0].isStale).toBe(true)
      expect(metrics.sources[0].estimatedChanges).toBe(50)
      expect(metrics.estimatedTotalChanges).toBe(50)
      expect(metrics.recommendedRefreshType).toBe('incremental')
      expect(metrics.reason).toContain('Sources changed')
    })

    it('returns metrics for stale view outside grace period', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)

      const lineage = createTestLineage({
        viewName: viewName('orders-view'),
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - (20 * 60 * 1000), // 20 minutes ago
      })

      const metrics = await detector.getMetrics(viewName('orders-view'), lineage, ['orders'])

      expect(metrics.state).toBe('stale')
      expect(metrics.usable).toBe(false) // Not usable outside grace
      expect(metrics.withinGracePeriod).toBe(false)
      expect(metrics.gracePeriodRemainingMs).toBeLessThan(0)
    })

    it('recommends full refresh when too many changes', async () => {
      const oldVersion = createTestVersion('v1')
      const newVersion = createTestVersion('v2')
      provider.setVersion('orders', newVersion)
      provider.setChangeCount('orders', 20000) // More than default threshold

      const lineage = createTestLineage({
        sourceVersions: new Map([['orders', oldVersion]]),
        lastRefreshTime: Date.now() - 1000,
      })

      const metrics = await detector.getMetrics(viewName('test-view'), lineage, ['orders'])

      expect(metrics.recommendedRefreshType).toBe('full')
    })

    it('handles multiple sources', async () => {
      const ordersV1 = createTestVersion('orders-v1')
      const ordersV2 = createTestVersion('orders-v2')
      const usersV1 = createTestVersion('users-v1')

      provider.setVersion('orders', ordersV2) // Changed
      provider.setVersion('users', usersV1) // Not changed
      provider.setChangeCount('orders', 100)

      const lineage = createTestLineage({
        sourceVersions: new Map([
          ['orders', ordersV1],
          ['users', usersV1],
        ]),
        lastRefreshTime: Date.now() - 1000,
      })

      const metrics = await detector.getMetrics(viewName('test-view'), lineage, ['orders', 'users'])

      expect(metrics.sources).toHaveLength(2)

      const ordersMetrics = metrics.sources.find(s => s.source === 'orders')
      const usersMetrics = metrics.sources.find(s => s.source === 'users')

      expect(ordersMetrics?.isStale).toBe(true)
      expect(usersMetrics?.isStale).toBe(false)
      expect(metrics.estimatedTotalChanges).toBe(100)
    })
  })

  describe('setThresholds', () => {
    it('updates thresholds', () => {
      detector.setThresholds({
        gracePeriodMs: 30000,
        warningThresholdMs: 10000,
      })

      const thresholds = detector.getThresholds()
      expect(thresholds.gracePeriodMs).toBe(30000)
      expect(thresholds.warningThresholdMs).toBe(10000)
      // Others should be defaults
      expect(thresholds.maxIncrementalChanges).toBe(DEFAULT_STALENESS_THRESHOLDS.maxIncrementalChanges)
    })
  })
})

// =============================================================================
// InMemoryVersionProvider Tests
// =============================================================================

describe('InMemoryVersionProvider', () => {
  let provider: InMemoryVersionProvider

  beforeEach(() => {
    provider = createInMemoryVersionProvider()
  })

  it('stores and retrieves versions', async () => {
    const version = createTestVersion('v1')
    provider.setVersion('orders', version)

    const result = await provider.getCurrentVersion('orders')
    expect(result).toEqual(version)
  })

  it('returns null for unknown sources', async () => {
    const result = await provider.getCurrentVersion('unknown')
    expect(result).toBeNull()
  })

  it('stores and retrieves change counts', async () => {
    provider.setChangeCount('orders', 100)

    const count = await provider.getChangeCount(
      'orders',
      createTestVersion('v1'),
      createTestVersion('v2')
    )
    expect(count).toBe(100)
  })

  it('removes sources', async () => {
    provider.setVersion('orders', createTestVersion('v1'))
    provider.setChangeCount('orders', 50)

    provider.removeSource('orders')

    expect(await provider.getCurrentVersion('orders')).toBeNull()
  })

  it('clears all data', async () => {
    provider.setVersion('orders', createTestVersion('v1'))
    provider.setVersion('users', createTestVersion('v2'))

    provider.clear()

    expect(await provider.getCurrentVersion('orders')).toBeNull()
    expect(await provider.getCurrentVersion('users')).toBeNull()
  })

  it('always returns true for isVersionValid', async () => {
    const result = await provider.isVersionValid('any', createTestVersion('v1'))
    expect(result).toBe(true)
  })
})

// =============================================================================
// Lineage Factory Functions Tests
// =============================================================================

describe('Lineage factory functions', () => {
  describe('createInitialLineage', () => {
    it('creates lineage with default values', () => {
      const lineage = createInitialLineage(viewName('my-view'), 'def-v1')

      expect(lineage.viewName).toBe('my-view')
      expect(lineage.definitionVersionId).toBe('def-v1')
      expect(lineage.sourceVersions.size).toBe(0)
      expect(lineage.lastRefreshTime).toBe(0)
      expect(lineage.lastRefreshDurationMs).toBe(0)
      expect(lineage.lastRefreshRecordCount).toBe(0)
      expect(lineage.lastRefreshType).toBe('full')
    })
  })

  describe('updateLineageAfterRefresh', () => {
    it('updates lineage with new source versions', () => {
      const initial = createInitialLineage(viewName('my-view'), 'def-v1')
      const newVersions = new Map([
        ['orders', createTestVersion('orders-v1')],
        ['users', createTestVersion('users-v1')],
      ])

      const updated = updateLineageAfterRefresh(
        initial,
        newVersions,
        'incremental',
        250,
        1000
      )

      expect(updated.sourceVersions.size).toBe(2)
      expect(updated.sourceVersions.get('orders')?.versionId).toBe('orders-v1')
      expect(updated.sourceVersions.get('users')?.versionId).toBe('users-v1')
      expect(updated.lastRefreshType).toBe('incremental')
      expect(updated.lastRefreshDurationMs).toBe(250)
      expect(updated.lastRefreshRecordCount).toBe(1000)
      expect(updated.lastRefreshTime).toBeGreaterThan(0)
    })

    it('merges with existing source versions', () => {
      const initial = createTestLineage({
        sourceVersions: new Map([
          ['orders', createTestVersion('orders-v1')],
        ]),
      })

      const newVersions = new Map([
        ['users', createTestVersion('users-v1')],
      ])

      const updated = updateLineageAfterRefresh(initial, newVersions, 'incremental', 100, 50)

      expect(updated.sourceVersions.size).toBe(2)
      expect(updated.sourceVersions.has('orders')).toBe(true)
      expect(updated.sourceVersions.has('users')).toBe(true)
    })
  })
})

// =============================================================================
// Version Factory Functions Tests
// =============================================================================

describe('Version factory functions', () => {
  describe('createNativeVersion', () => {
    it('creates native version from event sequence', () => {
      const version = createNativeVersion(42, 'evt-123', 1704067200000)

      expect(version.versionId).toBe('seq-42')
      expect(version.backend).toBe('native')
      expect(version.lastEventId).toBe('evt-123')
      expect(version.lastEventTs).toBe(1704067200000)
    })

    it('works without optional fields', () => {
      const version = createNativeVersion(42)

      expect(version.versionId).toBe('seq-42')
      expect(version.lastEventId).toBeUndefined()
      expect(version.lastEventTs).toBeUndefined()
    })
  })

  describe('createIcebergVersion', () => {
    it('creates Iceberg version from snapshot ID', () => {
      const version = createIcebergVersion('snap-abc123')

      expect(version.versionId).toBe('snap-abc123')
      expect(version.backend).toBe('iceberg')
      expect(version.timestamp).toBeGreaterThan(0)
    })
  })

  describe('createDeltaVersion', () => {
    it('creates Delta version from transaction number', () => {
      const version = createDeltaVersion(15)

      expect(version.versionId).toBe('txn-15')
      expect(version.backend).toBe('delta')
      expect(version.timestamp).toBeGreaterThan(0)
    })
  })
})

// =============================================================================
// Duration Parsing Tests
// =============================================================================

describe('parseThresholdDuration', () => {
  it('parses milliseconds', () => {
    expect(parseThresholdDuration('500ms')).toBe(500)
    expect(parseThresholdDuration('1000ms')).toBe(1000)
  })

  it('parses seconds', () => {
    expect(parseThresholdDuration('30s')).toBe(30000)
    expect(parseThresholdDuration('1s')).toBe(1000)
  })

  it('parses minutes', () => {
    expect(parseThresholdDuration('5m')).toBe(300000)
    expect(parseThresholdDuration('15m')).toBe(900000)
  })

  it('parses hours', () => {
    expect(parseThresholdDuration('1h')).toBe(3600000)
    expect(parseThresholdDuration('24h')).toBe(86400000)
  })

  it('parses days', () => {
    expect(parseThresholdDuration('1d')).toBe(86400000)
    expect(parseThresholdDuration('7d')).toBe(604800000)
  })

  it('throws on invalid format', () => {
    expect(() => parseThresholdDuration('invalid')).toThrow()
    expect(() => parseThresholdDuration('5')).toThrow()
    expect(() => parseThresholdDuration('5x')).toThrow()
    expect(() => parseThresholdDuration('')).toThrow()
  })
})

describe('createThresholdsFromConfig', () => {
  it('creates thresholds from config with duration strings', () => {
    const config = {
      gracePeriod: '10m',
      warningThreshold: '2m',
      maxIncrementalChanges: 5000,
      allowStaleReads: false,
    }

    const thresholds = createThresholdsFromConfig(config)

    expect(thresholds.gracePeriodMs).toBe(600000) // 10 minutes
    expect(thresholds.warningThresholdMs).toBe(120000) // 2 minutes
    expect(thresholds.maxIncrementalChanges).toBe(5000)
    expect(thresholds.allowStaleReads).toBe(false)
  })

  it('handles partial config', () => {
    const thresholds = createThresholdsFromConfig({
      gracePeriod: '5m',
    })

    expect(thresholds.gracePeriodMs).toBe(300000)
    expect(thresholds.warningThresholdMs).toBeUndefined()
    expect(thresholds.maxIncrementalChanges).toBeUndefined()
  })

  it('handles empty config', () => {
    const thresholds = createThresholdsFromConfig({})
    expect(thresholds).toEqual({})
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Edge cases', () => {
  let provider: InMemoryVersionProvider
  let detector: StalenessDetector

  beforeEach(() => {
    provider = createInMemoryVersionProvider()
    detector = createStalenessDetector(provider)
  })

  it('handles empty sources array', async () => {
    const lineage = createTestLineage()

    const isStale = await detector.isStale(lineage, [])
    expect(isStale).toBe(false)

    const metrics = await detector.getMetrics(viewName('test'), lineage, [])
    expect(metrics.sources).toHaveLength(0)
    expect(metrics.state).toBe('fresh')
  })

  it('handles very old refresh time', async () => {
    const version = createTestVersion('v1')
    provider.setVersion('orders', version)

    const lineage = createTestLineage({
      sourceVersions: new Map([['orders', version]]),
      lastRefreshTime: Date.now() - (365 * 24 * 60 * 60 * 1000), // 1 year ago
    })

    const metrics = await detector.getMetrics(viewName('test'), lineage, ['orders'])
    expect(metrics.stalenessPercent).toBe(100)
  })

  it('handles zero lastRefreshTime (never refreshed)', async () => {
    const version = createTestVersion('v1')
    provider.setVersion('orders', version)

    const lineage = createTestLineage({
      sourceVersions: new Map(),
      lastRefreshTime: 0,
    })

    const metrics = await detector.getMetrics(viewName('test'), lineage, ['orders'])
    expect(metrics.state).not.toBe('fresh')
  })

  it('handles allowStaleReads=false', async () => {
    detector = createStalenessDetector(provider, {
      allowStaleReads: false,
    })

    const oldVersion = createTestVersion('v1')
    const newVersion = createTestVersion('v2')
    provider.setVersion('orders', newVersion)

    const lineage = createTestLineage({
      sourceVersions: new Map([['orders', oldVersion]]),
      lastRefreshTime: Date.now() - 1000, // Very recent, within grace
    })

    const metrics = await detector.getMetrics(viewName('test'), lineage, ['orders'])
    expect(metrics.state).toBe('stale')
    expect(metrics.withinGracePeriod).toBe(true)
    expect(metrics.usable).toBe(false) // Not usable even within grace when allowStaleReads=false
  })
})
