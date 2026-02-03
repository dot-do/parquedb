/**
 * Compaction Dashboard Tests
 *
 * Tests for the dashboard HTML generation and health evaluation.
 */

import { describe, it, expect } from 'vitest'
import {
  evaluateHealth,
  evaluateAggregatedHealth,
  generateDashboardHtml,
  type CompactionMetrics,
  type DashboardConfig,
  DEFAULT_DASHBOARD_CONFIG,
} from '../../../../src/observability/compaction'

describe('Compaction Dashboard', () => {
  describe('evaluateHealth', () => {
    it('should return healthy for normal metrics', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 30 * 60 * 1000, // 30 minutes
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      expect(evaluateHealth(metrics)).toBe('healthy')
    })

    it('should return degraded for high pending windows', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 15, // Above threshold of 10
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 30 * 60 * 1000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      expect(evaluateHealth(metrics)).toBe('degraded')
    })

    it('should return degraded for old windows', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3 * 60 * 60 * 1000, // 3 hours (above 2 hour threshold)
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      expect(evaluateHealth(metrics)).toBe('degraded')
    })

    it('should return unhealthy for stuck windows', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 30 * 60 * 1000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 1, // Any stuck windows is unhealthy
      }

      expect(evaluateHealth(metrics)).toBe('unhealthy')
    })

    it('should return unhealthy for very high pending windows', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 60, // Above unhealthy threshold of 50
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 30 * 60 * 1000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      expect(evaluateHealth(metrics)).toBe('unhealthy')
    })

    it('should return unhealthy for very old windows', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 7 * 60 * 60 * 1000, // 7 hours (above 6 hour threshold)
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      expect(evaluateHealth(metrics)).toBe('unhealthy')
    })

    it('should use custom config thresholds', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 3,
        windows_processing: 0,
        windows_dispatched: 0,
        files_pending: 0,
        oldest_window_age_ms: 0,
        known_writers: 0,
        active_writers: 0,
        bytes_pending: 0,
        windows_stuck: 0,
      }

      const strictConfig: DashboardConfig = {
        ...DEFAULT_DASHBOARD_CONFIG,
        thresholds: {
          ...DEFAULT_DASHBOARD_CONFIG.thresholds,
          pendingWindowsDegraded: 2, // Lower threshold
        },
      }

      expect(evaluateHealth(metrics, strictConfig)).toBe('degraded')
    })
  })

  describe('evaluateAggregatedHealth', () => {
    it('should return healthy when all namespaces are healthy', () => {
      const metrics = new Map<string, CompactionMetrics>([
        ['posts', {
          namespace: 'posts',
          timestamp: Date.now(),
          windows_pending: 5,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 0,
        }],
        ['users', {
          namespace: 'users',
          timestamp: Date.now(),
          windows_pending: 3,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 0,
        }],
      ])

      expect(evaluateAggregatedHealth(metrics)).toBe('healthy')
    })

    it('should return degraded if any namespace is degraded', () => {
      const metrics = new Map<string, CompactionMetrics>([
        ['posts', {
          namespace: 'posts',
          timestamp: Date.now(),
          windows_pending: 15, // Degraded
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 0,
        }],
        ['users', {
          namespace: 'users',
          timestamp: Date.now(),
          windows_pending: 3,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 0,
        }],
      ])

      expect(evaluateAggregatedHealth(metrics)).toBe('degraded')
    })

    it('should return unhealthy if any namespace is unhealthy', () => {
      const metrics = new Map<string, CompactionMetrics>([
        ['posts', {
          namespace: 'posts',
          timestamp: Date.now(),
          windows_pending: 15, // Degraded
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 0,
        }],
        ['users', {
          namespace: 'users',
          timestamp: Date.now(),
          windows_pending: 3,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 0,
          active_writers: 0,
          bytes_pending: 0,
          windows_stuck: 1, // Unhealthy
        }],
      ])

      expect(evaluateAggregatedHealth(metrics)).toBe('unhealthy')
    })
  })

  describe('generateDashboardHtml', () => {
    it('should generate valid HTML', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts', 'users'])

      expect(html).toContain('<!DOCTYPE html>')
      expect(html).toContain('<html')
      expect(html).toContain('</html>')
    })

    it('should include Chart.js CDN', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts'])

      expect(html).toContain('cdn.jsdelivr.net/npm/chart.js')
    })

    it('should include the base URL for API calls', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts'])

      expect(html).toContain('https://api.example.com')
    })

    it('should include namespace configuration', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts', 'users', 'comments'])

      expect(html).toContain('"posts"')
      expect(html).toContain('"users"')
      expect(html).toContain('"comments"')
    })

    it('should include export links', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts'])

      expect(html).toContain('/compaction/metrics')
      expect(html).toContain('/compaction/metrics/json')
      expect(html).toContain('/compaction/health')
    })

    it('should use custom refresh interval', () => {
      const config: DashboardConfig = {
        ...DEFAULT_DASHBOARD_CONFIG,
        refreshIntervalSeconds: 60,
      }

      const html = generateDashboardHtml('https://api.example.com', ['posts'], config)

      expect(html).toContain('60000') // 60 seconds in ms
    })

    it('should include status indicators', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts'])

      expect(html).toContain('healthy')
      expect(html).toContain('degraded')
      expect(html).toContain('unhealthy')
    })

    it('should include summary cards', () => {
      const html = generateDashboardHtml('https://api.example.com', ['posts'])

      expect(html).toContain('Windows Pending')
      expect(html).toContain('Windows Processing')
      expect(html).toContain('Files Pending')
      expect(html).toContain('Oldest Window Age')
    })
  })
})
