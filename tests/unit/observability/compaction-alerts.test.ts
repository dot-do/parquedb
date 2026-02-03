/**
 * Compaction Alerts Tests
 *
 * Tests for the compaction alerting system:
 * - Webhook channel
 * - Slack channel
 * - PagerDuty channel
 * - Alert manager
 * - Threshold-based alerting
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  createWebhookChannel,
  createSlackChannel,
  createPagerDutyChannel,
  CompactionAlertManager,
  createAlertManagerFromEnv,
  DEFAULT_ALERT_THRESHOLDS,
  type AlertEvent,
  type AlertChannel,
  type CompactionMetrics,
} from '../../../src/observability/compaction/alerts'
import type { HealthIndicator } from '../../../src/observability/compaction/types'

// =============================================================================
// Mock Fetch
// =============================================================================

const mockFetch = vi.fn()
global.fetch = mockFetch

beforeEach(() => {
  mockFetch.mockReset()
})

afterEach(() => {
  vi.restoreAllMocks()
})

// =============================================================================
// Helper Functions
// =============================================================================

function createTestAlert(overrides: Partial<AlertEvent> = {}): AlertEvent {
  return {
    id: 'test_alert_123',
    type: 'stuck_window',
    severity: 'warning',
    title: 'Test Alert',
    description: 'This is a test alert',
    namespace: 'users',
    timestamp: Date.now(),
    ...overrides,
  }
}

function createTestMetrics(overrides: Partial<CompactionMetrics> = {}): CompactionMetrics {
  return {
    namespace: 'users',
    timestamp: Date.now(),
    windows_pending: 0,
    windows_processing: 0,
    windows_dispatched: 0,
    files_pending: 0,
    oldest_window_age_ms: 0,
    known_writers: 0,
    active_writers: 0,
    bytes_pending: 0,
    windows_stuck: 0,
    ...overrides,
  }
}

// =============================================================================
// Webhook Channel Tests
// =============================================================================

describe('createWebhookChannel', () => {
  it('should send alert to webhook URL', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ received: true }),
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
    })

    const alert = createTestAlert()
    const result = await channel.send(alert)

    expect(result.success).toBe(true)
    expect(result.channel).toBe('webhook')
    expect(mockFetch).toHaveBeenCalledTimes(1)

    const [url, options] = mockFetch.mock.calls[0]
    expect(url).toBe('https://example.com/webhook')
    expect(options.method).toBe('POST')
    expect(options.headers['Content-Type']).toBe('application/json')
    expect(options.headers['User-Agent']).toBe('ParqueDB-Alerts/1.0')
  })

  it('should include custom headers', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
      headers: {
        'X-Custom-Header': 'custom-value',
        'Authorization': 'Bearer token123',
      },
    })

    await channel.send(createTestAlert())

    const [_, options] = mockFetch.mock.calls[0]
    expect(options.headers['X-Custom-Header']).toBe('custom-value')
    expect(options.headers['Authorization']).toBe('Bearer token123')
  })

  it('should add HMAC signature when secret is provided', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
      secret: 'my-secret-key',
    })

    await channel.send(createTestAlert())

    const [_, options] = mockFetch.mock.calls[0]
    expect(options.headers['X-ParqueDB-Signature']).toMatch(/^sha256=[a-f0-9]+$/)
  })

  it('should include metrics when includeMetrics is true', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
      includeMetrics: true,
    })

    const alert = createTestAlert({
      metrics: { windows_pending: 5, windows_stuck: 2 } as Partial<CompactionMetrics>,
    })

    await channel.send(alert)

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)
    expect(body.metrics).toEqual({ windows_pending: 5, windows_stuck: 2 })
  })

  it('should handle HTTP errors', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: async () => 'Internal Server Error',
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
    })

    const result = await channel.send(createTestAlert())

    expect(result.success).toBe(false)
    expect(result.error).toContain('HTTP 500')
    expect(result.error).toContain('Internal Server Error')
  })

  it('should handle network errors', async () => {
    mockFetch.mockRejectedValueOnce(new Error('Network error'))

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
    })

    const result = await channel.send(createTestAlert())

    expect(result.success).toBe(false)
    expect(result.error).toBe('Network error')
  })

  it('should support PUT method', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    })

    const channel = createWebhookChannel({
      url: 'https://example.com/webhook',
      method: 'PUT',
    })

    await channel.send(createTestAlert())

    const [_, options] = mockFetch.mock.calls[0]
    expect(options.method).toBe('PUT')
  })
})

// =============================================================================
// Slack Channel Tests
// =============================================================================

describe('createSlackChannel', () => {
  it('should send alert to Slack webhook', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => 'ok',
    })

    const channel = createSlackChannel({
      webhookUrl: 'https://hooks.slack.com/services/xxx/yyy/zzz',
    })

    const result = await channel.send(createTestAlert())

    expect(result.success).toBe(true)
    expect(result.channel).toBe('slack')
    expect(mockFetch).toHaveBeenCalledTimes(1)

    const [url, options] = mockFetch.mock.calls[0]
    expect(url).toBe('https://hooks.slack.com/services/xxx/yyy/zzz')
  })

  it('should format message with blocks', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => 'ok',
    })

    const channel = createSlackChannel({
      webhookUrl: 'https://hooks.slack.com/services/xxx/yyy/zzz',
    })

    const alert = createTestAlert({
      severity: 'critical',
      metrics: { windows_pending: 10, windows_stuck: 3 } as Partial<CompactionMetrics>,
    })

    await channel.send(alert)

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.blocks).toBeDefined()
    expect(Array.isArray(body.blocks)).toBe(true)
    expect(body.blocks.length).toBeGreaterThan(0)
    expect(body.username).toBe('ParqueDB Alerts')
    expect(body.icon_emoji).toBe(':database:')
  })

  it('should include channel mention for critical alerts when configured', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => 'ok',
    })

    const channel = createSlackChannel({
      webhookUrl: 'https://hooks.slack.com/services/xxx/yyy/zzz',
      mentionChannelOnCritical: true,
    })

    const alert = createTestAlert({ severity: 'critical' })
    await channel.send(alert)

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)
    const headerBlock = body.blocks.find((b: { type: string }) => b.type === 'section')

    expect(headerBlock.text.text).toContain('<!channel>')
  })

  it('should include custom channel and username', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => 'ok',
    })

    const channel = createSlackChannel({
      webhookUrl: 'https://hooks.slack.com/services/xxx/yyy/zzz',
      channel: '#ops-alerts',
      username: 'Custom Bot',
      iconEmoji: ':robot:',
    })

    await channel.send(createTestAlert())

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.channel).toBe('#ops-alerts')
    expect(body.username).toBe('Custom Bot')
    expect(body.icon_emoji).toBe(':robot:')
  })

  it('should handle Slack API errors', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      text: async () => 'invalid_payload',
    })

    const channel = createSlackChannel({
      webhookUrl: 'https://hooks.slack.com/services/xxx/yyy/zzz',
    })

    const result = await channel.send(createTestAlert())

    expect(result.success).toBe(false)
    expect(result.error).toContain('HTTP 400')
  })
})

// =============================================================================
// PagerDuty Channel Tests
// =============================================================================

describe('createPagerDutyChannel', () => {
  it('should send alert to PagerDuty Events API', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success', dedup_key: 'abc123' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
    })

    const result = await channel.send(createTestAlert())

    expect(result.success).toBe(true)
    expect(result.channel).toBe('pagerduty')
    expect(mockFetch).toHaveBeenCalledTimes(1)

    const [url, options] = mockFetch.mock.calls[0]
    expect(url).toBe('https://events.pagerduty.com/v2/enqueue')
  })

  it('should trigger incident for non-recovery alerts', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
    })

    await channel.send(createTestAlert({ type: 'stuck_window' }))

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.event_action).toBe('trigger')
    expect(body.routing_key).toBe('pd-routing-key-123')
  })

  it('should resolve incident for recovery alerts', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
    })

    await channel.send(createTestAlert({ type: 'recovery' }))

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.event_action).toBe('resolve')
  })

  it('should generate dedup key from namespace and event type', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
      component: 'my-compaction',
    })

    await channel.send(createTestAlert({ namespace: 'users', type: 'stuck_window' }))

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.dedup_key).toBe('my-compaction:users:stuck_window')
  })

  it('should map severity correctly', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
    })

    await channel.send(createTestAlert({ severity: 'critical' }))

    const [_, options] = mockFetch.mock.calls[0]
    const body = JSON.parse(options.body)

    expect(body.payload.severity).toBe('critical')
  })

  it('should use custom API endpoint when provided', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const channel = createPagerDutyChannel({
      routingKey: 'pd-routing-key-123',
      apiEndpoint: 'https://custom.pagerduty.com/events',
    })

    await channel.send(createTestAlert())

    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe('https://custom.pagerduty.com/events')
  })
})

// =============================================================================
// Alert Manager Tests
// =============================================================================

describe('CompactionAlertManager', () => {
  let alertManager: CompactionAlertManager
  let mockChannel: AlertChannel

  beforeEach(() => {
    mockChannel = {
      name: 'mock',
      enabled: true,
      send: vi.fn().mockResolvedValue({ success: true, channel: 'mock' }),
    }

    alertManager = new CompactionAlertManager({
      channels: [mockChannel],
      logAlerts: false,
    })
  })

  describe('checkMetricsAndAlert', () => {
    it('should alert on stuck windows', async () => {
      const metrics = createTestMetrics({ windows_stuck: 2 })
      const results = await alertManager.checkMetricsAndAlert('users', metrics, 'unhealthy')

      expect(results.length).toBeGreaterThan(0)
      expect(mockChannel.send).toHaveBeenCalled()

      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('stuck_window')
      expect(sentAlert.title).toContain('stuck')
    })

    it('should use warning severity for low stuck window count', async () => {
      const metrics = createTestMetrics({ windows_stuck: 1 })
      await alertManager.checkMetricsAndAlert('users', metrics, 'degraded')

      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.severity).toBe('warning')
    })

    it('should use critical severity for high stuck window count', async () => {
      const metrics = createTestMetrics({
        windows_stuck: DEFAULT_ALERT_THRESHOLDS.stuckWindowsCritical,
      })
      await alertManager.checkMetricsAndAlert('users', metrics, 'unhealthy')

      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.severity).toBe('critical')
    })

    it('should alert on high pending windows', async () => {
      const metrics = createTestMetrics({
        windows_pending: DEFAULT_ALERT_THRESHOLDS.pendingWindowsWarning + 1,
      })
      await alertManager.checkMetricsAndAlert('users', metrics, 'degraded')

      expect(mockChannel.send).toHaveBeenCalled()
      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('capacity_warning')
    })

    it('should alert on old windows', async () => {
      const metrics = createTestMetrics({
        oldest_window_age_ms: (DEFAULT_ALERT_THRESHOLDS.windowAgeWarningHours + 1) * 60 * 60 * 1000,
      })
      await alertManager.checkMetricsAndAlert('users', metrics, 'degraded')

      expect(mockChannel.send).toHaveBeenCalled()
      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('threshold_exceeded')
    })

    it('should alert on health state change to unhealthy', async () => {
      const metrics = createTestMetrics()

      // First call sets the baseline
      const firstResults = await alertManager.checkMetricsAndAlert('users', metrics, 'healthy');
      (mockChannel.send as ReturnType<typeof vi.fn>).mockClear()

      // Second call with unhealthy state triggers alert
      const secondResults = await alertManager.checkMetricsAndAlert('users', metrics, 'unhealthy')

      expect(mockChannel.send).toHaveBeenCalled()
      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('health_unhealthy')
      expect(sentAlert.severity).toBe('critical')
    })

    it('should alert on health state change to degraded', async () => {
      const metrics = createTestMetrics()

      const firstResults = await alertManager.checkMetricsAndAlert('users', metrics, 'healthy');
      (mockChannel.send as ReturnType<typeof vi.fn>).mockClear()

      const secondResults = await alertManager.checkMetricsAndAlert('users', metrics, 'degraded')

      expect(mockChannel.send).toHaveBeenCalled()
      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('health_degraded')
    })

    it('should alert on recovery', async () => {
      const metrics = createTestMetrics()

      // Set initial unhealthy state
      const firstResults = await alertManager.checkMetricsAndAlert('users', metrics, 'unhealthy');
      (mockChannel.send as ReturnType<typeof vi.fn>).mockClear()

      // Recover to healthy
      const secondResults = await alertManager.checkMetricsAndAlert('users', metrics, 'healthy')

      expect(mockChannel.send).toHaveBeenCalled()
      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('recovery')
      expect(sentAlert.severity).toBe('info')
    })

    it('should not alert on healthy with no issues', async () => {
      const metrics = createTestMetrics()
      await alertManager.checkMetricsAndAlert('users', metrics, 'healthy')

      expect(mockChannel.send).not.toHaveBeenCalled()
    })
  })

  describe('alertCompactionFailure', () => {
    it('should send critical failure alert', async () => {
      const results = await alertManager.alertCompactionFailure(
        'users',
        '1700000000000',
        'Workflow timed out',
        { attemptCount: 3 }
      )

      expect(results.length).toBe(1)
      expect(results[0].success).toBe(true)

      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('compaction_failure')
      expect(sentAlert.severity).toBe('critical')
      expect(sentAlert.context?.windowKey).toBe('1700000000000')
      expect(sentAlert.context?.attemptCount).toBe(3)
    })
  })

  describe('alertRecovery', () => {
    it('should send recovery alert', async () => {
      const results = await alertManager.alertRecovery(
        'users',
        'All windows compacted successfully'
      )

      expect(results.length).toBe(1)
      expect(results[0].success).toBe(true)

      const sentAlert = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls[0][0] as AlertEvent
      expect(sentAlert.type).toBe('recovery')
      expect(sentAlert.severity).toBe('info')
    })
  })

  describe('deduplication', () => {
    it('should dedupe non-critical alerts within interval', async () => {
      const manager = new CompactionAlertManager({
        channels: [mockChannel],
        dedupeIntervalMs: 60000, // 1 minute
        logAlerts: false,
      })

      const metrics = createTestMetrics({ windows_stuck: 1 }) // warning severity

      await manager.checkMetricsAndAlert('users', metrics, 'degraded')
      await manager.checkMetricsAndAlert('users', metrics, 'degraded')
      await manager.checkMetricsAndAlert('users', metrics, 'degraded')

      // Should only send once due to deduplication
      const stuckAlerts = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls.filter(
        call => call[0].type === 'stuck_window'
      )
      expect(stuckAlerts.length).toBe(1)
    })

    it('should always send critical alerts', async () => {
      const manager = new CompactionAlertManager({
        channels: [mockChannel],
        dedupeIntervalMs: 60000,
        logAlerts: false,
      })

      const metrics = createTestMetrics({
        windows_stuck: DEFAULT_ALERT_THRESHOLDS.stuckWindowsCritical,
      })

      await manager.checkMetricsAndAlert('users', metrics, 'unhealthy')
      await manager.checkMetricsAndAlert('users', metrics, 'unhealthy')

      // Critical alerts should not be deduped
      const stuckAlerts = (mockChannel.send as ReturnType<typeof vi.fn>).mock.calls.filter(
        call => call[0].type === 'stuck_window'
      )
      expect(stuckAlerts.length).toBe(2)
    })

    it('should always send recovery alerts', async () => {
      const manager = new CompactionAlertManager({
        channels: [mockChannel],
        dedupeIntervalMs: 60000,
        logAlerts: false,
      })

      await manager.alertRecovery('users', 'Recovered')
      await manager.alertRecovery('users', 'Recovered again')

      expect(mockChannel.send).toHaveBeenCalledTimes(2)
    })
  })

  describe('channel management', () => {
    it('should add channel', async () => {
      const newChannel: AlertChannel = {
        name: 'new-channel',
        enabled: true,
        send: vi.fn().mockResolvedValue({ success: true, channel: 'new-channel' }),
      }

      alertManager.addChannel(newChannel)
      await alertManager.alertRecovery('users', 'test')

      // Both channels should be called
      expect(mockChannel.send).toHaveBeenCalled()
      expect(newChannel.send).toHaveBeenCalled()
    })

    it('should remove channel', async () => {
      alertManager.removeChannel('mock')
      await alertManager.alertRecovery('users', 'test')

      expect(mockChannel.send).not.toHaveBeenCalled()
    })

    it('should skip disabled channels', async () => {
      mockChannel.enabled = false
      await alertManager.alertRecovery('users', 'test')

      expect(mockChannel.send).not.toHaveBeenCalled()
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createAlertManagerFromEnv', () => {
  it('should create manager with no channels when env is empty', () => {
    const manager = createAlertManagerFromEnv({})

    // Manager should be created but have no channels
    expect(manager).toBeInstanceOf(CompactionAlertManager)
  })

  it('should create webhook channel from env', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    })

    const manager = createAlertManagerFromEnv({
      WEBHOOK_URL: 'https://example.com/webhook',
      WEBHOOK_SECRET: 'secret123',
    })

    await manager.alertRecovery('users', 'test')

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe('https://example.com/webhook')
  })

  it('should create Slack channel from env', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => 'ok',
    })

    const manager = createAlertManagerFromEnv({
      SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/xxx/yyy/zzz',
      SLACK_CHANNEL: '#alerts',
    })

    await manager.alertRecovery('users', 'test')

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe('https://hooks.slack.com/services/xxx/yyy/zzz')
  })

  it('should create PagerDuty channel from env', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' }),
    })

    const manager = createAlertManagerFromEnv({
      PAGERDUTY_ROUTING_KEY: 'pd-key-123',
    })

    await manager.alertRecovery('users', 'test')

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe('https://events.pagerduty.com/v2/enqueue')
  })

  it('should create multiple channels from env', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({}),
      text: async () => 'ok',
    })

    const manager = createAlertManagerFromEnv({
      WEBHOOK_URL: 'https://example.com/webhook',
      SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/xxx/yyy/zzz',
      PAGERDUTY_ROUTING_KEY: 'pd-key-123',
    })

    await manager.alertRecovery('users', 'test')

    // All three channels should be called
    expect(mockFetch).toHaveBeenCalledTimes(3)
  })

  it('should parse dedupe interval from env', () => {
    const manager = createAlertManagerFromEnv({
      ALERT_DEDUPE_INTERVAL_MS: '120000',
    })

    // Manager should be created with custom dedupe interval
    expect(manager).toBeInstanceOf(CompactionAlertManager)
  })
})

// =============================================================================
// Default Thresholds Tests
// =============================================================================

describe('DEFAULT_ALERT_THRESHOLDS', () => {
  it('should have sensible defaults', () => {
    expect(DEFAULT_ALERT_THRESHOLDS.stuckWindowsWarning).toBe(1)
    expect(DEFAULT_ALERT_THRESHOLDS.stuckWindowsCritical).toBe(3)
    expect(DEFAULT_ALERT_THRESHOLDS.pendingWindowsWarning).toBe(10)
    expect(DEFAULT_ALERT_THRESHOLDS.pendingWindowsCritical).toBe(50)
    expect(DEFAULT_ALERT_THRESHOLDS.windowAgeWarningHours).toBe(2)
    expect(DEFAULT_ALERT_THRESHOLDS.windowAgeCriticalHours).toBe(6)
    expect(DEFAULT_ALERT_THRESHOLDS.pendingFilesWarning).toBe(100)
    expect(DEFAULT_ALERT_THRESHOLDS.pendingFilesCritical).toBe(500)
  })

  it('should have warning thresholds lower than critical', () => {
    expect(DEFAULT_ALERT_THRESHOLDS.stuckWindowsWarning).toBeLessThan(
      DEFAULT_ALERT_THRESHOLDS.stuckWindowsCritical
    )
    expect(DEFAULT_ALERT_THRESHOLDS.pendingWindowsWarning).toBeLessThan(
      DEFAULT_ALERT_THRESHOLDS.pendingWindowsCritical
    )
    expect(DEFAULT_ALERT_THRESHOLDS.windowAgeWarningHours).toBeLessThan(
      DEFAULT_ALERT_THRESHOLDS.windowAgeCriticalHours
    )
  })
})
