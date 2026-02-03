/**
 * Compaction Metrics Tests
 *
 * Tests for the compaction metrics collection and export.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  emitCompactionMetrics,
  getLatestMetrics,
  getAllLatestMetrics,
  getMetricTimeSeries,
  getAggregatedMetrics,
  clearMetricsStore,
  exportPrometheusMetrics,
  exportJsonTimeSeries,
  type CompactionMetrics,
} from '../../../../src/observability/compaction'

describe('Compaction Metrics', () => {
  beforeEach(() => {
    clearMetricsStore()
  })

  describe('emitCompactionMetrics', () => {
    it('should store metrics for a namespace', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      emitCompactionMetrics(metrics)

      const latest = getLatestMetrics('posts')
      expect(latest).toBeDefined()
      expect(latest?.windows_pending).toBe(5)
      expect(latest?.windows_processing).toBe(2)
      expect(latest?.files_pending).toBe(50)
    })

    it('should overwrite previous metrics for same namespace', () => {
      const metrics1: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      const metrics2: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now() + 1000,
        windows_pending: 3,
        windows_processing: 1,
        windows_dispatched: 2,
        files_pending: 30,
        oldest_window_age_ms: 1800000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 512000,
        windows_stuck: 0,
      }

      emitCompactionMetrics(metrics1)
      emitCompactionMetrics(metrics2)

      const latest = getLatestMetrics('posts')
      expect(latest?.windows_pending).toBe(3)
      expect(latest?.files_pending).toBe(30)
    })

    it('should store metrics for multiple namespaces', () => {
      const postsMetrics: CompactionMetrics = {
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      }

      const usersMetrics: CompactionMetrics = {
        namespace: 'users',
        timestamp: Date.now(),
        windows_pending: 2,
        windows_processing: 1,
        windows_dispatched: 0,
        files_pending: 20,
        oldest_window_age_ms: 1800000,
        known_writers: 2,
        active_writers: 1,
        bytes_pending: 512000,
        windows_stuck: 0,
      }

      emitCompactionMetrics(postsMetrics)
      emitCompactionMetrics(usersMetrics)

      const allMetrics = getAllLatestMetrics()
      expect(allMetrics.size).toBe(2)
      expect(allMetrics.get('posts')?.windows_pending).toBe(5)
      expect(allMetrics.get('users')?.windows_pending).toBe(2)
    })
  })

  describe('getMetricTimeSeries', () => {
    it('should return time-series data for a metric', () => {
      const baseTime = Date.now()

      for (let i = 0; i < 5; i++) {
        emitCompactionMetrics({
          namespace: 'posts',
          timestamp: baseTime + (i * 1000),
          windows_pending: i + 1,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: (i + 1) * 10,
          oldest_window_age_ms: 0,
          known_writers: 1,
          active_writers: 1,
          bytes_pending: 0,
          windows_stuck: 0,
        })
      }

      const series = getMetricTimeSeries('posts', 'windows_pending')
      expect(series.metric).toBe('windows_pending')
      expect(series.namespace).toBe('posts')
      expect(series.data.length).toBe(5)
      expect(series.data[0].value).toBe(1)
      expect(series.data[4].value).toBe(5)
    })

    it('should filter by since timestamp', () => {
      const baseTime = Date.now()

      for (let i = 0; i < 5; i++) {
        emitCompactionMetrics({
          namespace: 'posts',
          timestamp: baseTime + (i * 1000),
          windows_pending: i + 1,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 1,
          active_writers: 1,
          bytes_pending: 0,
          windows_stuck: 0,
        })
      }

      const series = getMetricTimeSeries('posts', 'windows_pending', baseTime + 2000)
      expect(series.data.length).toBe(3) // Points at +2s, +3s, +4s
    })

    it('should respect limit parameter', () => {
      const baseTime = Date.now()

      for (let i = 0; i < 10; i++) {
        emitCompactionMetrics({
          namespace: 'posts',
          timestamp: baseTime + (i * 1000),
          windows_pending: i + 1,
          windows_processing: 0,
          windows_dispatched: 0,
          files_pending: 0,
          oldest_window_age_ms: 0,
          known_writers: 1,
          active_writers: 1,
          bytes_pending: 0,
          windows_stuck: 0,
        })
      }

      const series = getMetricTimeSeries('posts', 'windows_pending', undefined, 5)
      expect(series.data.length).toBe(5)
      // Should return most recent 5
      expect(series.data[4].value).toBe(10)
    })

    it('should return empty data for unknown namespace', () => {
      const series = getMetricTimeSeries('unknown', 'windows_pending')
      expect(series.data.length).toBe(0)
    })
  })

  describe('getAggregatedMetrics', () => {
    it('should aggregate metrics across namespaces', () => {
      emitCompactionMetrics({
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      })

      emitCompactionMetrics({
        namespace: 'users',
        timestamp: Date.now(),
        windows_pending: 3,
        windows_processing: 1,
        windows_dispatched: 2,
        files_pending: 30,
        oldest_window_age_ms: 1800000,
        known_writers: 2,
        active_writers: 1,
        bytes_pending: 512000,
        windows_stuck: 1,
      })

      const aggregated = getAggregatedMetrics()
      expect(aggregated.namespace_count).toBe(2)
      expect(aggregated.total_windows_pending).toBe(8)
      expect(aggregated.total_windows_processing).toBe(3)
      expect(aggregated.total_windows_dispatched).toBe(3)
      expect(aggregated.total_files_pending).toBe(80)
      expect(aggregated.total_bytes_pending).toBe(1536000)
    })
  })

  describe('exportPrometheusMetrics', () => {
    it('should export metrics in Prometheus format', () => {
      emitCompactionMetrics({
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      })

      const output = exportPrometheusMetrics()

      // Check for HELP and TYPE lines
      expect(output).toContain('# HELP parquedb_compaction_windows_pending')
      expect(output).toContain('# TYPE parquedb_compaction_windows_pending gauge')
      expect(output).toContain('parquedb_compaction_windows_pending{namespace="posts"} 5')
      expect(output).toContain('parquedb_compaction_files_pending{namespace="posts"} 50')
      // Oldest age should be in seconds
      expect(output).toContain('parquedb_compaction_oldest_window_age_seconds{namespace="posts"} 3600')
    })

    it('should filter by namespaces parameter', () => {
      emitCompactionMetrics({
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
      })

      emitCompactionMetrics({
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
      })

      const output = exportPrometheusMetrics(['posts'])
      expect(output).toContain('namespace="posts"')
      expect(output).not.toContain('namespace="users"')
    })
  })

  describe('exportJsonTimeSeries', () => {
    it('should export metrics as JSON time-series', () => {
      emitCompactionMetrics({
        namespace: 'posts',
        timestamp: Date.now(),
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        files_pending: 50,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
        bytes_pending: 1024000,
        windows_stuck: 0,
      })

      const output = exportJsonTimeSeries()

      expect(output.timestamp).toBeDefined()
      expect(output.namespaces['posts']).toBeDefined()
      expect(output.namespaces['posts'].latest?.windows_pending).toBe(5)
      expect(output.namespaces['posts'].timeSeries['windows_pending']).toBeDefined()
    })

    it('should filter by namespaces parameter', () => {
      emitCompactionMetrics({
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
      })

      emitCompactionMetrics({
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
      })

      const output = exportJsonTimeSeries(['posts'])
      expect(output.namespaces['posts']).toBeDefined()
      expect(output.namespaces['users']).toBeUndefined()
    })
  })
})
