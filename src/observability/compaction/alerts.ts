/**
 * Compaction Alerts
 *
 * Alerting integration for critical compaction events:
 * - Webhook notification support (generic HTTP)
 * - Slack integration
 * - PagerDuty integration
 * - Configurable alert thresholds
 * - Critical event types: stuck jobs, failures, capacity warnings
 */

import { logger } from '../../utils/logger'
import type { CompactionMetrics, HealthIndicator } from './types'

// =============================================================================
// Types
// =============================================================================

/** Severity levels for alerts */
export type AlertSeverity = 'info' | 'warning' | 'critical'

/** Alert event types */
export type AlertEventType =
  | 'stuck_window'
  | 'compaction_failure'
  | 'capacity_warning'
  | 'health_degraded'
  | 'health_unhealthy'
  | 'recovery'
  | 'threshold_exceeded'

/** Base alert event */
export interface AlertEvent {
  /** Unique event ID */
  id: string
  /** Event type */
  type: AlertEventType
  /** Alert severity */
  severity: AlertSeverity
  /** Human-readable title */
  title: string
  /** Detailed description */
  description: string
  /** Namespace affected (if applicable) */
  namespace?: string | undefined
  /** Relevant metrics at time of alert */
  metrics?: Partial<CompactionMetrics> | undefined
  /** Additional context data */
  context?: Record<string, unknown> | undefined
  /** Timestamp when alert was generated */
  timestamp: number
}

/** Alert threshold configuration */
export interface AlertThresholds {
  /** Number of stuck windows to trigger warning */
  stuckWindowsWarning: number
  /** Number of stuck windows to trigger critical */
  stuckWindowsCritical: number
  /** Max pending windows before warning */
  pendingWindowsWarning: number
  /** Max pending windows before critical */
  pendingWindowsCritical: number
  /** Max window age (hours) before warning */
  windowAgeWarningHours: number
  /** Max window age (hours) before critical */
  windowAgeCriticalHours: number
  /** Max pending files before warning */
  pendingFilesWarning: number
  /** Max pending files before critical */
  pendingFilesCritical: number
  /** Max pending bytes before warning */
  pendingBytesWarning: number
  /** Max pending bytes before critical */
  pendingBytesCritical: number
}

/** Default alert thresholds */
export const DEFAULT_ALERT_THRESHOLDS: AlertThresholds = {
  stuckWindowsWarning: 1,
  stuckWindowsCritical: 3,
  pendingWindowsWarning: 10,
  pendingWindowsCritical: 50,
  windowAgeWarningHours: 2,
  windowAgeCriticalHours: 6,
  pendingFilesWarning: 100,
  pendingFilesCritical: 500,
  pendingBytesWarning: 1024 * 1024 * 100, // 100MB
  pendingBytesCritical: 1024 * 1024 * 500, // 500MB
}

// =============================================================================
// Alert Channel Interfaces
// =============================================================================

/** Base alert channel interface */
export interface AlertChannel {
  /** Channel name for identification */
  name: string
  /** Whether the channel is enabled */
  enabled: boolean
  /** Send an alert through this channel */
  send(event: AlertEvent): Promise<AlertDeliveryResult>
}

/** Result of alert delivery attempt */
export interface AlertDeliveryResult {
  /** Whether delivery was successful */
  success: boolean
  /** Channel name */
  channel: string
  /** Error message if failed */
  error?: string | undefined
  /** Response from the channel (e.g., message ID) */
  response?: unknown | undefined
}

// =============================================================================
// Webhook Channel
// =============================================================================

/** Webhook channel configuration */
export interface WebhookChannelConfig {
  /** Webhook URL */
  url: string
  /** HTTP method (default: POST) */
  method?: 'POST' | 'PUT' | undefined
  /** Custom headers */
  headers?: Record<string, string> | undefined
  /** Secret for signing payloads (optional) */
  secret?: string | undefined
  /** Timeout in milliseconds (default: 10000) */
  timeoutMs?: number | undefined
  /** Whether to include full metrics in payload */
  includeMetrics?: boolean | undefined
}

/**
 * Create a generic webhook alert channel
 */
export function createWebhookChannel(config: WebhookChannelConfig): AlertChannel {
  return {
    name: 'webhook',
    enabled: true,

    async send(event: AlertEvent): Promise<AlertDeliveryResult> {
      const payload = {
        id: event.id,
        type: event.type,
        severity: event.severity,
        title: event.title,
        description: event.description,
        namespace: event.namespace,
        timestamp: event.timestamp,
        timestampIso: new Date(event.timestamp).toISOString(),
        context: event.context,
        ...(config.includeMetrics && event.metrics ? { metrics: event.metrics } : {}),
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'User-Agent': 'ParqueDB-Alerts/1.0',
        ...config.headers,
      }

      // Add HMAC signature if secret is provided
      if (config.secret) {
        const signature = await computeHmacSignature(JSON.stringify(payload), config.secret)
        headers['X-ParqueDB-Signature'] = signature
      }

      try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), config.timeoutMs ?? 10000)

        const response = await fetch(config.url, {
          method: config.method ?? 'POST',
          headers,
          body: JSON.stringify(payload),
          signal: controller.signal,
        })

        clearTimeout(timeoutId)

        if (!response.ok) {
          const errorText = await response.text().catch(() => 'Unknown error')
          return {
            success: false,
            channel: 'webhook',
            error: `HTTP ${response.status}: ${errorText}`,
          }
        }

        const responseBody = await response.json().catch(() => ({}))
        return {
          success: true,
          channel: 'webhook',
          response: responseBody,
        }
      } catch (error) {
        return {
          success: false,
          channel: 'webhook',
          error: error instanceof Error ? error.message : 'Unknown error',
        }
      }
    },
  }
}

// =============================================================================
// Slack Channel
// =============================================================================

/** Slack channel configuration */
export interface SlackChannelConfig {
  /** Slack webhook URL */
  webhookUrl: string
  /** Default channel (can be overridden by webhook config) */
  channel?: string | undefined
  /** Bot username (default: ParqueDB Alerts) */
  username?: string | undefined
  /** Bot icon emoji (default: :database:) */
  iconEmoji?: string | undefined
  /** Whether to mention @channel for critical alerts */
  mentionChannelOnCritical?: boolean | undefined
  /** Timeout in milliseconds (default: 10000) */
  timeoutMs?: number | undefined
}

/** Slack message block types */
interface SlackBlock {
  type: string
  text?: { type: string; text: string; emoji?: boolean | undefined } | undefined
  fields?: Array<{ type: string; text: string }> | undefined
  elements?: Array<{ type: string; text: string }> | undefined
}

/**
 * Create a Slack alert channel
 */
export function createSlackChannel(config: SlackChannelConfig): AlertChannel {
  return {
    name: 'slack',
    enabled: true,

    async send(event: AlertEvent): Promise<AlertDeliveryResult> {
      const blocks = buildSlackBlocks(event, config)

      const payload: Record<string, unknown> = {
        blocks,
        username: config.username ?? 'ParqueDB Alerts',
        icon_emoji: config.iconEmoji ?? ':database:',
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

        return {
          success: true,
          channel: 'slack',
          response: 'ok',
        }
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

/**
 * Build Slack message blocks from an alert event
 */
function buildSlackBlocks(event: AlertEvent, config: SlackChannelConfig): SlackBlock[] {
  const severityEmoji = getSeverityEmoji(event.severity)
  const eventTypeEmoji = getEventTypeEmoji(event.type)

  const blocks: SlackBlock[] = []

  // Header
  let headerText = `${severityEmoji} ${eventTypeEmoji} *${event.title}*`
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

  fields.push({
    type: 'mrkdwn',
    text: `*Severity:* ${event.severity.toUpperCase()}`,
  })

  fields.push({
    type: 'mrkdwn',
    text: `*Type:* ${event.type.replace(/_/g, ' ')}`,
  })

  if (event.namespace) {
    fields.push({
      type: 'mrkdwn',
      text: `*Namespace:* ${event.namespace}`,
    })
  }

  fields.push({
    type: 'mrkdwn',
    text: `*Time:* <!date^${Math.floor(event.timestamp / 1000)}^{date_short_pretty} {time}|${new Date(event.timestamp).toISOString()}>`,
  })

  blocks.push({
    type: 'section',
    fields,
  })

  // Metrics if available
  if (event.metrics) {
    const metricsFields: Array<{ type: string; text: string }> = []

    if (event.metrics.windows_pending !== undefined) {
      metricsFields.push({
        type: 'mrkdwn',
        text: `*Pending Windows:* ${event.metrics.windows_pending}`,
      })
    }
    if (event.metrics.windows_stuck !== undefined) {
      metricsFields.push({
        type: 'mrkdwn',
        text: `*Stuck Windows:* ${event.metrics.windows_stuck}`,
      })
    }
    if (event.metrics.files_pending !== undefined) {
      metricsFields.push({
        type: 'mrkdwn',
        text: `*Pending Files:* ${event.metrics.files_pending}`,
      })
    }
    if (event.metrics.oldest_window_age_ms !== undefined) {
      const ageHours = (event.metrics.oldest_window_age_ms / (1000 * 60 * 60)).toFixed(1)
      metricsFields.push({
        type: 'mrkdwn',
        text: `*Oldest Window Age:* ${ageHours}h`,
      })
    }

    if (metricsFields.length > 0) {
      blocks.push({
        type: 'section',
        fields: metricsFields,
      })
    }
  }

  // Divider
  blocks.push({ type: 'divider' })

  // Context footer
  blocks.push({
    type: 'context',
    elements: [
      { type: 'mrkdwn', text: `Alert ID: \`${event.id}\`` },
    ],
  })

  return blocks
}

function getSeverityEmoji(severity: AlertSeverity): string {
  switch (severity) {
    case 'critical': return ':rotating_light:'
    case 'warning': return ':warning:'
    case 'info': return ':information_source:'
    default: return ':grey_question:'
  }
}

function getEventTypeEmoji(type: AlertEventType): string {
  switch (type) {
    case 'stuck_window': return ':hourglass:'
    case 'compaction_failure': return ':x:'
    case 'capacity_warning': return ':chart_with_upwards_trend:'
    case 'health_degraded': return ':yellow_heart:'
    case 'health_unhealthy': return ':broken_heart:'
    case 'recovery': return ':white_check_mark:'
    case 'threshold_exceeded': return ':exclamation:'
    default: return ':bell:'
  }
}

// =============================================================================
// PagerDuty Channel
// =============================================================================

/** PagerDuty channel configuration */
export interface PagerDutyChannelConfig {
  /** PagerDuty routing key (integration key) */
  routingKey: string
  /** PagerDuty Events API endpoint (default: events.pagerduty.com) */
  apiEndpoint?: string | undefined
  /** Component name for dedup (default: parquedb-compaction) */
  component?: string | undefined
  /** Service name (default: ParqueDB) */
  service?: string | undefined
  /** Timeout in milliseconds (default: 10000) */
  timeoutMs?: number | undefined
}

/** PagerDuty event action */
type PagerDutyAction = 'trigger' | 'acknowledge' | 'resolve'

/** PagerDuty severity mapping */
type PagerDutySeverity = 'critical' | 'error' | 'warning' | 'info'

/**
 * Create a PagerDuty alert channel
 */
export function createPagerDutyChannel(config: PagerDutyChannelConfig): AlertChannel {
  const endpoint = config.apiEndpoint ?? 'https://events.pagerduty.com/v2/enqueue'
  const component = config.component ?? 'parquedb-compaction'
  const service = config.service ?? 'ParqueDB'

  return {
    name: 'pagerduty',
    enabled: true,

    async send(event: AlertEvent): Promise<AlertDeliveryResult> {
      const action = getpagerDutyAction(event)
      const severity = mapToPagerDutySeverity(event.severity)

      // Build dedup key from namespace and event type
      const dedupKey = event.namespace
        ? `${component}:${event.namespace}:${event.type}`
        : `${component}:${event.type}`

      const payload: Record<string, unknown> = {
        routing_key: config.routingKey,
        event_action: action,
        dedup_key: dedupKey,
        payload: {
          summary: `[${service}] ${event.title}`,
          severity,
          source: event.namespace ?? 'global',
          component,
          group: 'compaction',
          class: event.type,
          timestamp: new Date(event.timestamp).toISOString(),
          custom_details: {
            description: event.description,
            namespace: event.namespace,
            event_id: event.id,
            event_type: event.type,
            ...(event.metrics ?? {}),
            ...(event.context ?? {}),
          },
        },
      }

      // Add links for context
      if (event.context?.dashboardUrl) {
        payload.links = [
          {
            href: event.context.dashboardUrl,
            text: 'View Dashboard',
          },
        ]
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

        return {
          success: true,
          channel: 'pagerduty',
          response: responseBody,
        }
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

function getpagerDutyAction(event: AlertEvent): PagerDutyAction {
  if (event.type === 'recovery') {
    return 'resolve'
  }
  return 'trigger'
}

function mapToPagerDutySeverity(severity: AlertSeverity): PagerDutySeverity {
  switch (severity) {
    case 'critical': return 'critical'
    case 'warning': return 'warning'
    case 'info': return 'info'
    default: return 'info'
  }
}

// =============================================================================
// Alert Manager
// =============================================================================

/** Alert manager configuration */
export interface AlertManagerConfig {
  /** Alert channels to send to */
  channels: AlertChannel[]
  /** Alert thresholds */
  thresholds: AlertThresholds
  /** Minimum interval between alerts of same type (ms) */
  dedupeIntervalMs?: number | undefined
  /** Whether to log alerts to console */
  logAlerts?: boolean | undefined
}

/** Track last alert time per type/namespace for deduplication */
interface AlertState {
  lastAlertTime: Map<string, number>
  lastHealthState: Map<string, HealthIndicator>
}

/**
 * Alert manager for coordinating alert delivery
 */
export class CompactionAlertManager {
  private config: AlertManagerConfig
  private state: AlertState

  constructor(config: Partial<AlertManagerConfig> = {}) {
    this.config = {
      channels: config.channels ?? [],
      thresholds: { ...DEFAULT_ALERT_THRESHOLDS, ...config.thresholds },
      dedupeIntervalMs: config.dedupeIntervalMs ?? 5 * 60 * 1000, // 5 minutes default
      logAlerts: config.logAlerts ?? true,
    }

    this.state = {
      lastAlertTime: new Map(),
      lastHealthState: new Map(),
    }
  }

  /**
   * Add an alert channel
   */
  addChannel(channel: AlertChannel): void {
    this.config.channels.push(channel)
  }

  /**
   * Remove an alert channel by name
   */
  removeChannel(name: string): void {
    this.config.channels = this.config.channels.filter(c => c.name !== name)
  }

  /**
   * Check metrics and send alerts if thresholds exceeded
   */
  async checkMetricsAndAlert(
    namespace: string,
    metrics: CompactionMetrics,
    health: HealthIndicator
  ): Promise<AlertDeliveryResult[]> {
    const alerts: AlertEvent[] = []

    // Check for stuck windows
    if (metrics.windows_stuck > 0) {
      const severity = metrics.windows_stuck >= this.config.thresholds.stuckWindowsCritical
        ? 'critical'
        : metrics.windows_stuck >= this.config.thresholds.stuckWindowsWarning
        ? 'warning'
        : 'info'

      alerts.push(this.createAlert({
        type: 'stuck_window',
        severity,
        title: `${metrics.windows_stuck} stuck compaction window${metrics.windows_stuck > 1 ? 's' : ''} in ${namespace}`,
        description: `${metrics.windows_stuck} window${metrics.windows_stuck > 1 ? 's have' : ' has'} been in processing state longer than the timeout threshold. This may indicate a workflow failure or resource constraint.`,
        namespace,
        metrics,
      }))
    }

    // Check for pending windows threshold
    if (metrics.windows_pending >= this.config.thresholds.pendingWindowsCritical) {
      alerts.push(this.createAlert({
        type: 'capacity_warning',
        severity: 'critical',
        title: `High pending window count in ${namespace}`,
        description: `${metrics.windows_pending} windows pending compaction (critical threshold: ${this.config.thresholds.pendingWindowsCritical}). Compaction may be falling behind write throughput.`,
        namespace,
        metrics,
      }))
    } else if (metrics.windows_pending >= this.config.thresholds.pendingWindowsWarning) {
      alerts.push(this.createAlert({
        type: 'capacity_warning',
        severity: 'warning',
        title: `Elevated pending window count in ${namespace}`,
        description: `${metrics.windows_pending} windows pending compaction (warning threshold: ${this.config.thresholds.pendingWindowsWarning}). Consider increasing compaction throughput.`,
        namespace,
        metrics,
      }))
    }

    // Check for window age threshold
    const ageHours = metrics.oldest_window_age_ms / (1000 * 60 * 60)
    if (ageHours >= this.config.thresholds.windowAgeCriticalHours) {
      alerts.push(this.createAlert({
        type: 'threshold_exceeded',
        severity: 'critical',
        title: `Old uncompacted window in ${namespace}`,
        description: `Oldest window is ${ageHours.toFixed(1)} hours old (critical threshold: ${this.config.thresholds.windowAgeCriticalHours}h). Data freshness may be affected.`,
        namespace,
        metrics,
      }))
    } else if (ageHours >= this.config.thresholds.windowAgeWarningHours) {
      alerts.push(this.createAlert({
        type: 'threshold_exceeded',
        severity: 'warning',
        title: `Aging uncompacted window in ${namespace}`,
        description: `Oldest window is ${ageHours.toFixed(1)} hours old (warning threshold: ${this.config.thresholds.windowAgeWarningHours}h).`,
        namespace,
        metrics,
      }))
    }

    // Check for health state changes
    const previousHealth = this.state.lastHealthState.get(namespace)
    if (previousHealth !== health) {
      if (health === 'unhealthy') {
        alerts.push(this.createAlert({
          type: 'health_unhealthy',
          severity: 'critical',
          title: `Compaction health unhealthy for ${namespace}`,
          description: `Compaction system health has degraded to unhealthy state. Immediate attention required.`,
          namespace,
          metrics,
        }))
      } else if (health === 'degraded' && previousHealth !== 'unhealthy') {
        alerts.push(this.createAlert({
          type: 'health_degraded',
          severity: 'warning',
          title: `Compaction health degraded for ${namespace}`,
          description: `Compaction system health has degraded. Monitor closely for further deterioration.`,
          namespace,
          metrics,
        }))
      } else if (health === 'healthy' && previousHealth && previousHealth !== 'healthy') {
        alerts.push(this.createAlert({
          type: 'recovery',
          severity: 'info',
          title: `Compaction health recovered for ${namespace}`,
          description: `Compaction system has recovered from ${previousHealth} state to healthy.`,
          namespace,
          metrics,
        }))
      }
      this.state.lastHealthState.set(namespace, health)
    }

    // Send alerts (with deduplication)
    const results: AlertDeliveryResult[] = []
    for (const alert of alerts) {
      if (this.shouldSendAlert(alert)) {
        const alertResults = await this.sendAlert(alert)
        results.push(...alertResults)
      }
    }

    return results
  }

  /**
   * Send a compaction failure alert
   */
  async alertCompactionFailure(
    namespace: string,
    windowKey: string,
    error: string,
    context?: Record<string, unknown>
  ): Promise<AlertDeliveryResult[]> {
    const alert = this.createAlert({
      type: 'compaction_failure',
      severity: 'critical',
      title: `Compaction failed for window in ${namespace}`,
      description: `Compaction workflow failed for window ${windowKey}: ${error}`,
      namespace,
      context: { windowKey, error, ...context },
    })

    return this.sendAlert(alert)
  }

  /**
   * Send a recovery alert
   */
  async alertRecovery(
    namespace: string,
    message: string
  ): Promise<AlertDeliveryResult[]> {
    const alert = this.createAlert({
      type: 'recovery',
      severity: 'info',
      title: `Compaction recovered for ${namespace}`,
      description: message,
      namespace,
    })

    return this.sendAlert(alert)
  }

  /**
   * Create an alert event
   */
  private createAlert(params: Omit<AlertEvent, 'id' | 'timestamp'>): AlertEvent {
    return {
      id: generateAlertId(),
      timestamp: Date.now(),
      ...params,
    }
  }

  /**
   * Check if alert should be sent (deduplication)
   */
  private shouldSendAlert(alert: AlertEvent): boolean {
    const key = `${alert.type}:${alert.namespace ?? 'global'}:${alert.severity}`
    const lastTime = this.state.lastAlertTime.get(key)
    const now = Date.now()

    // Always send recovery alerts
    if (alert.type === 'recovery') {
      return true
    }

    // Always send critical alerts
    if (alert.severity === 'critical') {
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

  /**
   * Send an alert to all configured channels
   */
  private async sendAlert(alert: AlertEvent): Promise<AlertDeliveryResult[]> {
    if (this.config.logAlerts) {
      logger.info('compaction_alert', {
        alert_id: alert.id,
        type: alert.type,
        severity: alert.severity,
        title: alert.title,
        namespace: alert.namespace,
      })
    }

    const results: AlertDeliveryResult[] = []

    for (const channel of this.config.channels) {
      if (!channel.enabled) continue

      try {
        const result = await channel.send(alert)
        results.push(result)

        if (!result.success) {
          logger.error('alert_delivery_failed', {
            channel: channel.name,
            alert_id: alert.id,
            error: result.error,
          })
        }
      } catch (error) {
        const result: AlertDeliveryResult = {
          success: false,
          channel: channel.name,
          error: error instanceof Error ? error.message : 'Unknown error',
        }
        results.push(result)

        logger.error('alert_delivery_error', {
          channel: channel.name,
          alert_id: alert.id,
          error: result.error,
        })
      }
    }

    return results
  }

  /**
   * Clear alert state (for testing)
   */
  clearState(): void {
    this.state.lastAlertTime.clear()
    this.state.lastHealthState.clear()
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique alert ID
 */
function generateAlertId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `alert_${timestamp}_${random}`
}

/**
 * Compute HMAC-SHA256 signature for webhook payloads
 */
async function computeHmacSignature(payload: string, secret: string): Promise<string> {
  const encoder = new TextEncoder()
  const keyData = encoder.encode(secret)
  const messageData = encoder.encode(payload)

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )

  const signature = await crypto.subtle.sign('HMAC', cryptoKey, messageData)
  const signatureArray = Array.from(new Uint8Array(signature))
  return 'sha256=' + signatureArray.map(b => b.toString(16).padStart(2, '0')).join('')
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a CompactionAlertManager from environment configuration
 */
export function createAlertManagerFromEnv(env: {
  WEBHOOK_URL?: string | undefined
  WEBHOOK_SECRET?: string | undefined
  SLACK_WEBHOOK_URL?: string | undefined
  SLACK_CHANNEL?: string | undefined
  PAGERDUTY_ROUTING_KEY?: string | undefined
  ALERT_DEDUPE_INTERVAL_MS?: string | undefined
}): CompactionAlertManager {
  const channels: AlertChannel[] = []

  // Add webhook channel if configured
  if (env.WEBHOOK_URL) {
    channels.push(createWebhookChannel({
      url: env.WEBHOOK_URL,
      secret: env.WEBHOOK_SECRET,
      includeMetrics: true,
    }))
  }

  // Add Slack channel if configured
  if (env.SLACK_WEBHOOK_URL) {
    channels.push(createSlackChannel({
      webhookUrl: env.SLACK_WEBHOOK_URL,
      channel: env.SLACK_CHANNEL,
      mentionChannelOnCritical: true,
    }))
  }

  // Add PagerDuty channel if configured
  if (env.PAGERDUTY_ROUTING_KEY) {
    channels.push(createPagerDutyChannel({
      routingKey: env.PAGERDUTY_ROUTING_KEY,
    }))
  }

  const dedupeIntervalMs = env.ALERT_DEDUPE_INTERVAL_MS
    ? parseInt(env.ALERT_DEDUPE_INTERVAL_MS, 10)
    : undefined

  return new CompactionAlertManager({
    channels,
    dedupeIntervalMs,
  })
}
