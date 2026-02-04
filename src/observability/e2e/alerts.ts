/**
 * E2E Monitoring Alerts
 *
 * Alerting integration for E2E monitoring events:
 * - Health check failures
 * - Performance regressions
 * - Test failures
 * - Recovery notifications
 *
 * Supports Slack and PagerDuty integrations.
 */

import { logger } from '../../utils/logger'
import type { RegressionAnalysis } from './types'

// =============================================================================
// Types
// =============================================================================

/** E2E alert event types */
export type E2EAlertEventType =
  | 'health_check_failed'
  | 'regression_detected'
  | 'test_failure'
  | 'recovery'

/** Alert severity levels */
export type E2EAlertSeverity = 'critical' | 'warning' | 'info'

/** E2E alert event */
export interface E2EAlertEvent {
  /** Unique event ID */
  id: string
  /** Event type */
  type: E2EAlertEventType
  /** Alert severity */
  severity: E2EAlertSeverity
  /** Human-readable title */
  title: string
  /** Detailed description */
  description: string
  /** Environment (production, staging) */
  environment?: string | undefined
  /** Worker URL being monitored */
  workerUrl?: string | undefined
  /** Regression analysis if applicable */
  regression?: RegressionAnalysis | undefined
  /** Test results summary */
  testSummary?: {
    totalTests: number
    passedTests: number
    failedTests: number
  } | undefined
  /** Health check details if applicable */
  healthCheck?: {
    status: 'healthy' | 'degraded' | 'unhealthy'
    checks: Record<string, { success: boolean; latencyMs?: number; error?: string }>
  } | undefined
  /** Additional context */
  context?: Record<string, unknown> | undefined
  /** Timestamp when alert was generated */
  timestamp: number
}

/** Alert delivery result */
export interface E2EAlertDeliveryResult {
  success: boolean
  channel: string
  error?: string | undefined
  response?: unknown | undefined
}

/** Alert channel interface */
export interface E2EAlertChannel {
  name: string
  enabled: boolean
  send(event: E2EAlertEvent): Promise<E2EAlertDeliveryResult>
}

// =============================================================================
// Alert Thresholds
// =============================================================================

/** Threshold configuration for triggering alerts */
export interface E2EAlertThresholds {
  /** P50 latency regression threshold (percent) */
  latencyP50Regression: number
  /** P95 latency regression threshold (percent) */
  latencyP95Regression: number
  /** Number of consecutive failures before critical */
  consecutiveFailuresCritical: number
  /** Number of regression metrics to trigger critical */
  regressionMetricsCritical: number
}

/** Default alert thresholds */
export const DEFAULT_E2E_ALERT_THRESHOLDS: E2EAlertThresholds = {
  latencyP50Regression: 50, // 50% increase triggers critical
  latencyP95Regression: 30, // 30% increase triggers warning
  consecutiveFailuresCritical: 2,
  regressionMetricsCritical: 3,
}

// =============================================================================
// Slack Channel
// =============================================================================

/** Slack channel configuration */
export interface E2ESlackConfig {
  webhookUrl: string
  channel?: string | undefined
  username?: string | undefined
  iconEmoji?: string | undefined
  mentionChannelOnCritical?: boolean | undefined
  timeoutMs?: number | undefined
}

interface SlackBlock {
  type: string
  text?: { type: string; text: string; emoji?: boolean } | undefined
  fields?: Array<{ type: string; text: string }> | undefined
  elements?: Array<{ type: string; text: string }> | undefined
}

/**
 * Create a Slack alert channel for E2E monitoring
 */
export function createE2ESlackChannel(config: E2ESlackConfig): E2EAlertChannel {
  return {
    name: 'slack',
    enabled: true,

    async send(event: E2EAlertEvent): Promise<E2EAlertDeliveryResult> {
      const blocks = buildE2ESlackBlocks(event, config)

      const payload: Record<string, unknown> = {
        blocks,
        username: config.username ?? 'ParqueDB E2E Monitor',
        icon_emoji: config.iconEmoji ?? ':test_tube:',
      }

      if (config.channel) {
        payload.channel = config.channel
      }

      try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), config.timeoutMs ?? 10000)

        const response = await fetch(config.webhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: controller.signal,
        })

        clearTimeout(timeoutId)

        if (!response.ok) {
          const errorText = await response.text().catch(() => 'Unknown error')
          return {
            success: false,
            channel: 'slack',
            error: `HTTP ${response.status}: ${errorText}`,
          }
        }

        return { success: true, channel: 'slack', response: 'ok' }
      } catch (error) {
        return {
          success: false,
          channel: 'slack',
          error: error instanceof Error ? error.message : 'Unknown error',
        }
      }
    },
  }
}

function buildE2ESlackBlocks(event: E2EAlertEvent, config: E2ESlackConfig): SlackBlock[] {
  const severityEmoji = getE2ESeverityEmoji(event.severity)
  const eventEmoji = getE2EEventEmoji(event.type)
  const blocks: SlackBlock[] = []

  // Header
  let headerText = `${severityEmoji} ${eventEmoji} *${event.title}*`
  if (config.mentionChannelOnCritical && event.severity === 'critical') {
    headerText = `<!channel> ${headerText}`
  }

  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: headerText },
  })

  // Description
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: event.description },
  })

  // Context fields
  const fields: Array<{ type: string; text: string }> = []

  fields.push({ type: 'mrkdwn', text: `*Severity:* ${event.severity.toUpperCase()}` })
  fields.push({ type: 'mrkdwn', text: `*Type:* ${event.type.replace(/_/g, ' ')}` })

  if (event.environment) {
    fields.push({ type: 'mrkdwn', text: `*Environment:* ${event.environment}` })
  }

  if (event.workerUrl) {
    fields.push({ type: 'mrkdwn', text: `*Worker:* ${event.workerUrl}` })
  }

  blocks.push({ type: 'section', fields })

  // Test summary if available
  if (event.testSummary) {
    const { totalTests, passedTests, failedTests } = event.testSummary
    const passRate = totalTests > 0 ? ((passedTests / totalTests) * 100).toFixed(1) : '0'
    blocks.push({
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*Total Tests:* ${totalTests}` },
        { type: 'mrkdwn', text: `*Passed:* ${passedTests}` },
        { type: 'mrkdwn', text: `*Failed:* ${failedTests}` },
        { type: 'mrkdwn', text: `*Pass Rate:* ${passRate}%` },
      ],
    })
  }

  // Regression details if available
  if (event.regression && event.regression.hasRegression) {
    const regressionFields = event.regression.metrics
      .filter(m => m.isRegression)
      .slice(0, 4)
      .map(m => ({
        type: 'mrkdwn',
        text: `*${m.name}:* ${m.changePercent >= 0 ? '+' : ''}${m.changePercent.toFixed(1)}% (threshold: ${m.threshold}%)`,
      }))

    if (regressionFields.length > 0) {
      blocks.push({ type: 'section', fields: regressionFields })
    }
  }

  // Health check details if available
  if (event.healthCheck) {
    const healthFields = Object.entries(event.healthCheck.checks)
      .slice(0, 4)
      .map(([name, check]) => ({
        type: 'mrkdwn',
        text: `*${name}:* ${check.success ? ':white_check_mark:' : ':x:'} ${check.latencyMs ? `(${check.latencyMs}ms)` : ''} ${check.error ?? ''}`,
      }))

    if (healthFields.length > 0) {
      blocks.push({ type: 'section', fields: healthFields })
    }
  }

  // Divider and footer
  blocks.push({ type: 'divider' })
  blocks.push({
    type: 'context',
    elements: [
      { type: 'mrkdwn', text: `Alert ID: \`${event.id}\` | ${new Date(event.timestamp).toISOString()}` },
    ],
  })

  return blocks
}

function getE2ESeverityEmoji(severity: E2EAlertSeverity): string {
  switch (severity) {
    case 'critical': return ':rotating_light:'
    case 'warning': return ':warning:'
    case 'info': return ':information_source:'
    default: return ':grey_question:'
  }
}

function getE2EEventEmoji(type: E2EAlertEventType): string {
  switch (type) {
    case 'health_check_failed': return ':broken_heart:'
    case 'regression_detected': return ':chart_with_downwards_trend:'
    case 'test_failure': return ':x:'
    case 'recovery': return ':white_check_mark:'
    default: return ':bell:'
  }
}

// =============================================================================
// PagerDuty Channel
// =============================================================================

/** PagerDuty channel configuration */
export interface E2EPagerDutyConfig {
  routingKey: string
  apiEndpoint?: string | undefined
  component?: string | undefined
  service?: string | undefined
  timeoutMs?: number | undefined
}

type PagerDutyAction = 'trigger' | 'acknowledge' | 'resolve'
type PagerDutySeverity = 'critical' | 'error' | 'warning' | 'info'

/**
 * Create a PagerDuty alert channel for E2E monitoring
 */
export function createE2EPagerDutyChannel(config: E2EPagerDutyConfig): E2EAlertChannel {
  const endpoint = config.apiEndpoint ?? 'https://events.pagerduty.com/v2/enqueue'
  const component = config.component ?? 'parquedb-e2e'
  const service = config.service ?? 'ParqueDB'

  return {
    name: 'pagerduty',
    enabled: true,

    async send(event: E2EAlertEvent): Promise<E2EAlertDeliveryResult> {
      const action = getPagerDutyAction(event)
      const severity = mapToPagerDutySeverity(event.severity)
      const dedupKey = `${component}:${event.environment ?? 'global'}:${event.type}`

      const payload: Record<string, unknown> = {
        routing_key: config.routingKey,
        event_action: action,
        dedup_key: dedupKey,
        payload: {
          summary: `[${service}] ${event.title}`,
          severity,
          source: event.workerUrl ?? event.environment ?? 'global',
          component,
          group: 'e2e-monitoring',
          class: event.type,
          timestamp: new Date(event.timestamp).toISOString(),
          custom_details: {
            description: event.description,
            environment: event.environment,
            worker_url: event.workerUrl,
            event_id: event.id,
            ...(event.testSummary ?? {}),
            ...(event.regression ? { regression_severity: event.regression.severity } : {}),
            ...(event.context ?? {}),
          },
        },
      }

      try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), config.timeoutMs ?? 10000)

        const response = await fetch(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: controller.signal,
        })

        clearTimeout(timeoutId)

        const responseBody = await response.json().catch(() => ({})) as Record<string, unknown>

        if (!response.ok) {
          return {
            success: false,
            channel: 'pagerduty',
            error: `HTTP ${response.status}: ${responseBody.message ?? 'Unknown error'}`,
            response: responseBody,
          }
        }

        return { success: true, channel: 'pagerduty', response: responseBody }
      } catch (error) {
        return {
          success: false,
          channel: 'pagerduty',
          error: error instanceof Error ? error.message : 'Unknown error',
        }
      }
    },
  }
}

function getPagerDutyAction(event: E2EAlertEvent): PagerDutyAction {
  return event.type === 'recovery' ? 'resolve' : 'trigger'
}

function mapToPagerDutySeverity(severity: E2EAlertSeverity): PagerDutySeverity {
  switch (severity) {
    case 'critical': return 'critical'
    case 'warning': return 'warning'
    case 'info': return 'info'
    default: return 'info'
  }
}

// =============================================================================
// E2E Alert Manager
// =============================================================================

/** E2E alert manager configuration */
export interface E2EAlertManagerConfig {
  channels: E2EAlertChannel[]
  thresholds: E2EAlertThresholds
  dedupeIntervalMs?: number | undefined
  logAlerts?: boolean | undefined
}

/** Track alert state for deduplication */
interface E2EAlertState {
  lastAlertTime: Map<string, number>
  consecutiveFailures: Map<string, number>
}

/**
 * E2E Alert Manager for coordinating alert delivery
 */
export class E2EAlertManager {
  private config: E2EAlertManagerConfig
  private state: E2EAlertState

  constructor(config: Partial<E2EAlertManagerConfig> = {}) {
    this.config = {
      channels: config.channels ?? [],
      thresholds: { ...DEFAULT_E2E_ALERT_THRESHOLDS, ...config.thresholds },
      dedupeIntervalMs: config.dedupeIntervalMs ?? 5 * 60 * 1000,
      logAlerts: config.logAlerts ?? true,
    }
    this.state = {
      lastAlertTime: new Map(),
      consecutiveFailures: new Map(),
    }
  }

  /** Add an alert channel */
  addChannel(channel: E2EAlertChannel): void {
    this.config.channels.push(channel)
  }

  /** Remove an alert channel by name */
  removeChannel(name: string): void {
    this.config.channels = this.config.channels.filter(c => c.name !== name)
  }

  /**
   * Alert on health check failure
   */
  async alertHealthCheckFailed(
    environment: string,
    workerUrl: string,
    checks: Record<string, { success: boolean; latencyMs?: number; error?: string }>,
    overallStatus: 'degraded' | 'unhealthy'
  ): Promise<E2EAlertDeliveryResult[]> {
    const failedChecks = Object.entries(checks)
      .filter(([, c]) => !c.success)
      .map(([name]) => name)

    const severity: E2EAlertSeverity = overallStatus === 'unhealthy' ? 'critical' : 'warning'

    const alert = this.createAlert({
      type: 'health_check_failed',
      severity,
      title: `Health check ${overallStatus} for ${environment}`,
      description: `Worker health check failed. Failed checks: ${failedChecks.join(', ')}`,
      environment,
      workerUrl,
      healthCheck: { status: overallStatus, checks },
    })

    return this.sendAlert(alert)
  }

  /**
   * Alert on regression detection
   */
  async alertRegressionDetected(
    environment: string,
    workerUrl: string,
    regression: RegressionAnalysis,
    testSummary?: E2EAlertEvent['testSummary']
  ): Promise<E2EAlertDeliveryResult[]> {
    const severity: E2EAlertSeverity =
      regression.severity === 'severe' ? 'critical' :
      regression.severity === 'moderate' ? 'warning' : 'info'

    const regressedMetrics = regression.metrics
      .filter(m => m.isRegression)
      .map(m => m.name)
      .join(', ')

    const alert = this.createAlert({
      type: 'regression_detected',
      severity,
      title: `Performance regression in ${environment}`,
      description: `${regression.message}. Regressed metrics: ${regressedMetrics}`,
      environment,
      workerUrl,
      regression,
      testSummary,
    })

    return this.sendAlert(alert)
  }

  /**
   * Alert on test failure
   */
  async alertTestFailure(
    environment: string,
    workerUrl: string,
    testSummary: E2EAlertEvent['testSummary'],
    failureDetails?: string
  ): Promise<E2EAlertDeliveryResult[]> {
    const failKey = `${environment}:test_failure`
    const failures = (this.state.consecutiveFailures.get(failKey) ?? 0) + 1
    this.state.consecutiveFailures.set(failKey, failures)

    const severity: E2EAlertSeverity =
      failures >= this.config.thresholds.consecutiveFailuresCritical ? 'critical' : 'warning'

    const alert = this.createAlert({
      type: 'test_failure',
      severity,
      title: `E2E test failure in ${environment}`,
      description: failureDetails ?? `${testSummary?.failedTests ?? 0} tests failed out of ${testSummary?.totalTests ?? 0}`,
      environment,
      workerUrl,
      testSummary,
      context: { consecutiveFailures: failures },
    })

    return this.sendAlert(alert)
  }

  /**
   * Alert on recovery
   */
  async alertRecovery(
    environment: string,
    workerUrl: string,
    message: string
  ): Promise<E2EAlertDeliveryResult[]> {
    // Reset failure counter on recovery
    const failKey = `${environment}:test_failure`
    this.state.consecutiveFailures.set(failKey, 0)

    const alert = this.createAlert({
      type: 'recovery',
      severity: 'info',
      title: `E2E monitoring recovered for ${environment}`,
      description: message,
      environment,
      workerUrl,
    })

    return this.sendAlert(alert)
  }

  /** Create an alert event */
  private createAlert(params: Omit<E2EAlertEvent, 'id' | 'timestamp'>): E2EAlertEvent {
    return {
      id: generateAlertId(),
      timestamp: Date.now(),
      ...params,
    }
  }

  /** Check if alert should be sent (deduplication) */
  private shouldSendAlert(alert: E2EAlertEvent): boolean {
    const key = `${alert.type}:${alert.environment ?? 'global'}:${alert.severity}`
    const lastTime = this.state.lastAlertTime.get(key)
    const now = Date.now()

    // Always send recovery and critical alerts
    if (alert.type === 'recovery' || alert.severity === 'critical') {
      this.state.lastAlertTime.set(key, now)
      return true
    }

    // Dedupe non-critical alerts
    if (lastTime && now - lastTime < (this.config.dedupeIntervalMs ?? 300000)) {
      return false
    }

    this.state.lastAlertTime.set(key, now)
    return true
  }

  /** Send alert to all configured channels */
  private async sendAlert(alert: E2EAlertEvent): Promise<E2EAlertDeliveryResult[]> {
    if (!this.shouldSendAlert(alert)) {
      return []
    }

    if (this.config.logAlerts) {
      logger.info('e2e_alert', {
        alert_id: alert.id,
        type: alert.type,
        severity: alert.severity,
        title: alert.title,
        environment: alert.environment,
      })
    }

    const results: E2EAlertDeliveryResult[] = []

    for (const channel of this.config.channels) {
      if (!channel.enabled) continue

      try {
        const result = await channel.send(alert)
        results.push(result)

        if (!result.success) {
          logger.error('e2e_alert_delivery_failed', {
            channel: channel.name,
            alert_id: alert.id,
            error: result.error,
          })
        }
      } catch (error) {
        const result: E2EAlertDeliveryResult = {
          success: false,
          channel: channel.name,
          error: error instanceof Error ? error.message : 'Unknown error',
        }
        results.push(result)
        logger.error('e2e_alert_delivery_error', {
          channel: channel.name,
          alert_id: alert.id,
          error: result.error,
        })
      }
    }

    return results
  }

  /** Clear alert state (for testing) */
  clearState(): void {
    this.state.lastAlertTime.clear()
    this.state.consecutiveFailures.clear()
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

function generateAlertId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `e2e_alert_${timestamp}_${random}`
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an E2EAlertManager from environment configuration
 */
export function createE2EAlertManagerFromEnv(env: {
  E2E_SLACK_WEBHOOK_URL?: string | undefined
  E2E_SLACK_CHANNEL?: string | undefined
  E2E_PAGERDUTY_ROUTING_KEY?: string | undefined
  E2E_ALERT_DEDUPE_INTERVAL_MS?: string | undefined
}): E2EAlertManager {
  const channels: E2EAlertChannel[] = []

  if (env.E2E_SLACK_WEBHOOK_URL) {
    channels.push(createE2ESlackChannel({
      webhookUrl: env.E2E_SLACK_WEBHOOK_URL,
      channel: env.E2E_SLACK_CHANNEL,
      mentionChannelOnCritical: true,
    }))
  }

  if (env.E2E_PAGERDUTY_ROUTING_KEY) {
    channels.push(createE2EPagerDutyChannel({
      routingKey: env.E2E_PAGERDUTY_ROUTING_KEY,
    }))
  }

  const dedupeIntervalMs = env.E2E_ALERT_DEDUPE_INTERVAL_MS
    ? parseInt(env.E2E_ALERT_DEDUPE_INTERVAL_MS, 10)
    : undefined

  return new E2EAlertManager({ channels, dedupeIntervalMs })
}

/**
 * Determine alert routing based on event type and severity
 *
 * | Condition | Severity | Channel |
 * |-----------|----------|---------|
 * | Health check fails | Critical | PagerDuty + Slack |
 * | p50 latency >50% (2+ runs) | Critical | PagerDuty + Slack |
 * | p95/p99 >30% regression | Warning | Slack only |
 * | Any test failure | Warning | Slack only |
 */
export function shouldAlertPagerDuty(event: E2EAlertEvent): boolean {
  if (event.severity === 'critical') return true
  if (event.type === 'health_check_failed') return true
  return false
}
