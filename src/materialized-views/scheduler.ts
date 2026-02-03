/**
 * Materialized View Scheduler for ParqueDB
 *
 * Implements scheduled refresh using Cloudflare Durable Object alarms.
 * Supports cron-like scheduling, interval-based refresh, and manual triggers.
 *
 * @example
 * ```typescript
 * // In a Durable Object
 * const scheduler = new MVScheduler(this.ctx.storage, {
 *   onRefresh: async (viewName) => {
 *     await this.refreshView(viewName)
 *   }
 * })
 *
 * // Schedule a view for periodic refresh
 * await scheduler.scheduleView('daily_stats', {
 *   cron: '0 0 * * *', // Daily at midnight
 * })
 *
 * // In alarm handler
 * async alarm() {
 *   await scheduler.processAlarm()
 * }
 * ```
 */

import type {
  ViewName,
  ViewMetadata,
  ScheduleOptions,
  RefreshMode,
  ViewState,
} from './types'
import { viewName } from './types'

// =============================================================================
// Types
// =============================================================================

/**
 * Scheduled view entry stored in DO storage
 */
export interface ScheduledView {
  /** View name */
  name: ViewName

  /** Schedule configuration */
  schedule: ScheduleOptions

  /** When the view was scheduled */
  scheduledAt: number

  /** Next scheduled refresh time */
  nextRefreshAt: number

  /** Last refresh time */
  lastRefreshAt?: number | undefined

  /** Last refresh duration in milliseconds */
  lastRefreshDurationMs?: number | undefined

  /** Number of consecutive failures */
  consecutiveFailures: number

  /** Last error message if any */
  lastError?: string | undefined

  /** Whether the schedule is enabled */
  enabled: boolean

  /** Retry configuration */
  retryConfig?: RetryConfig | undefined
}

/**
 * Configuration for retry behavior
 */
export interface RetryConfig {
  /** Maximum retry attempts after failure */
  maxRetries: number

  /** Base delay between retries in milliseconds */
  baseDelayMs: number

  /** Maximum delay between retries in milliseconds */
  maxDelayMs: number

  /** Exponential backoff multiplier */
  backoffMultiplier: number
}

/**
 * Default retry configuration
 */
export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 1000,
  maxDelayMs: 300000, // 5 minutes
  backoffMultiplier: 2,
}

/**
 * Configuration for the MV Scheduler
 */
export interface MVSchedulerConfig {
  /**
   * Callback when a view needs to be refreshed
   */
  onRefresh: (viewName: ViewName) => Promise<void>

  /**
   * Optional callback when a refresh completes successfully
   */
  onRefreshComplete?: ((viewName: ViewName, durationMs: number) => Promise<void>) | undefined

  /**
   * Optional callback when a refresh fails
   */
  onRefreshError?: ((viewName: ViewName, error: Error) => Promise<void>) | undefined

  /**
   * Optional callback when a view is disabled due to too many failures
   */
  onViewDisabled?: ((viewName: ViewName, reason: string) => Promise<void>) | undefined

  /**
   * Default retry configuration for all views
   */
  defaultRetryConfig?: RetryConfig | undefined

  /**
   * Minimum interval between alarm checks (prevents excessive DO wakeups)
   * @default 1000 (1 second)
   */
  minAlarmIntervalMs?: number | undefined
}

/**
 * Result of processing an alarm
 */
export interface AlarmProcessingResult {
  /** Views that were refreshed */
  refreshed: ViewName[]

  /** Views that failed to refresh */
  failed: Array<{ name: ViewName; error: string }>

  /** Views that were skipped (already being processed) */
  skipped: ViewName[]

  /** Next alarm time (if any views remain scheduled) */
  nextAlarmAt?: number | undefined
}

/**
 * Statistics for the scheduler
 */
export interface SchedulerStats {
  /** Total scheduled views */
  totalViews: number

  /** Enabled views */
  enabledViews: number

  /** Disabled views */
  disabledViews: number

  /** Total refreshes performed */
  totalRefreshes: number

  /** Successful refreshes */
  successfulRefreshes: number

  /** Failed refreshes */
  failedRefreshes: number

  /** Next scheduled refresh */
  nextRefreshAt?: number | undefined
}

// =============================================================================
// Storage Keys
// =============================================================================

const STORAGE_PREFIX = 'mv_schedule:'
const STATS_KEY = 'mv_scheduler_stats'
const PROCESSING_KEY = 'mv_scheduler_processing'

// =============================================================================
// Cron Parser (Simplified)
// =============================================================================

/**
 * Simplified cron parser for common patterns
 * Supports: minute, hour, day of month, month, day of week
 * Special values: * (any), numbers, ranges (1-5), lists (1,3,5)
 */
export function parseCronExpression(cron: string): CronExpression {
  const parts = cron.trim().split(/\s+/)
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: expected 5 fields, got ${parts.length}`)
  }

  return {
    minute: parseCronField(parts[0]!, 0, 59, 'minute'),
    hour: parseCronField(parts[1]!, 0, 23, 'hour'),
    dayOfMonth: parseCronField(parts[2]!, 1, 31, 'day of month'),
    month: parseCronField(parts[3]!, 1, 12, 'month'),
    dayOfWeek: parseCronField(parts[4]!, 0, 6, 'day of week'),
  }
}

interface CronExpression {
  minute: number[]
  hour: number[]
  dayOfMonth: number[]
  month: number[]
  dayOfWeek: number[]
}

function parseCronField(field: string, min: number, max: number, fieldName: string): number[] {
  if (field === '*') {
    return Array.from({ length: max - min + 1 }, (_, i) => min + i)
  }

  const values: number[] = []

  for (const part of field.split(',')) {
    if (part.includes('-')) {
      const [startStr, endStr] = part.split('-')
      const start = Number(startStr)
      const end = Number(endStr)
      if (!Number.isInteger(start) || startStr!.includes('.')) {
        throw new Error(`Cron field '${fieldName}': '${startStr}' is not a valid integer`)
      }
      if (!Number.isInteger(end) || endStr!.includes('.')) {
        throw new Error(`Cron field '${fieldName}': '${endStr}' is not a valid integer`)
      }
      if (start > end) {
        throw new Error(`Cron field '${fieldName}': start (${start}) is greater than end (${end}) in range '${part}'`)
      }
      if (start < min || start > max) {
        throw new Error(`Cron field '${fieldName}': ${start} is out of range (${min}-${max})`)
      }
      if (end < min || end > max) {
        throw new Error(`Cron field '${fieldName}': ${end} is out of range (${min}-${max})`)
      }
      for (let i = start; i <= end; i++) {
        values.push(i)
      }
    } else if (part.includes('/')) {
      const [base, step] = part.split('/')
      const stepNum = Number(step)
      if (isNaN(stepNum) || stepNum <= 0 || !Number.isInteger(stepNum)) {
        throw new Error(`Cron field '${fieldName}': Invalid step value '${step}'`)
      }
      const startNum = base === '*' ? min : Number(base)
      if (base !== '*' && (!Number.isInteger(startNum) || base!.includes('.'))) {
        throw new Error(`Cron field '${fieldName}': '${base}' is not a valid integer`)
      }
      if (base !== '*' && (startNum < min || startNum > max)) {
        throw new Error(`Cron field '${fieldName}': ${startNum} is out of range (${min}-${max})`)
      }
      for (let i = startNum; i <= max; i += stepNum) {
        values.push(i)
      }
    } else {
      const num = Number(part)
      if (!Number.isInteger(num) || part.includes('.')) {
        throw new Error(`Cron field '${fieldName}': '${part}' is not a valid integer`)
      }
      if (num < min || num > max) {
        throw new Error(`Cron field '${fieldName}': ${num} is out of range (${min}-${max})`)
      }
      values.push(num)
    }
  }

  return [...new Set(values)].sort((a, b) => a - b)
}

/**
 * Calculate the next occurrence of a cron expression
 */
export function getNextCronTime(cron: CronExpression, afterTime: Date = new Date()): Date {
  const next = new Date(afterTime)
  next.setSeconds(0)
  next.setMilliseconds(0)
  next.setMinutes(next.getMinutes() + 1) // Start from next minute

  // Limit iterations to prevent infinite loops
  for (let i = 0; i < 366 * 24 * 60; i++) {
    // Check if current time matches cron expression
    if (
      cron.minute.includes(next.getMinutes()) &&
      cron.hour.includes(next.getHours()) &&
      cron.dayOfMonth.includes(next.getDate()) &&
      cron.month.includes(next.getMonth() + 1) &&
      cron.dayOfWeek.includes(next.getDay())
    ) {
      return next
    }

    // Advance by one minute
    next.setMinutes(next.getMinutes() + 1)
  }

  throw new Error('Could not calculate next cron time within one year')
}

// =============================================================================
// MV Scheduler Class
// =============================================================================

/**
 * Materialized View Scheduler using Durable Object Alarms
 *
 * Manages scheduled refreshes for materialized views using DO storage
 * and alarms for reliable, distributed scheduling.
 */
export class MVScheduler {
  /** Durable Object storage */
  private storage: DurableObjectStorage

  /** Scheduler configuration */
  private config: Required<MVSchedulerConfig>

  /** In-memory cache of scheduled views */
  private viewCache: Map<ViewName, ScheduledView> = new Map()

  /** Whether cache has been loaded from storage */
  private cacheLoaded = false

  /** Set of views currently being processed (prevents concurrent refreshes) */
  private processing: Set<ViewName> = new Set()

  /** Statistics */
  private stats: SchedulerStats = {
    totalViews: 0,
    enabledViews: 0,
    disabledViews: 0,
    totalRefreshes: 0,
    successfulRefreshes: 0,
    failedRefreshes: 0,
  }

  /**
   * Create a new MV Scheduler
   *
   * @param storage - Durable Object storage
   * @param config - Scheduler configuration
   */
  constructor(storage: DurableObjectStorage, config: MVSchedulerConfig) {
    this.storage = storage
    this.config = {
      ...config,
      defaultRetryConfig: config.defaultRetryConfig ?? DEFAULT_RETRY_CONFIG,
      minAlarmIntervalMs: config.minAlarmIntervalMs ?? 1000,
      onRefreshComplete: config.onRefreshComplete ?? (async () => {}),
      onRefreshError: config.onRefreshError ?? (async () => {}),
      onViewDisabled: config.onViewDisabled ?? (async () => {}),
    }
  }

  // ===========================================================================
  // View Scheduling
  // ===========================================================================

  /**
   * Schedule a view for periodic refresh
   *
   * @param name - View name
   * @param schedule - Schedule configuration
   * @param retryConfig - Optional retry configuration for this view
   */
  async scheduleView(
    name: string,
    schedule: ScheduleOptions,
    retryConfig?: RetryConfig
  ): Promise<ScheduledView> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const now = Date.now()

    // Calculate next refresh time
    const nextRefreshAt = this.calculateNextRefreshTime(schedule, now)

    const scheduledView: ScheduledView = {
      name: vn,
      schedule,
      scheduledAt: now,
      nextRefreshAt,
      consecutiveFailures: 0,
      enabled: true,
      retryConfig: retryConfig ?? this.config.defaultRetryConfig,
    }

    // Save to storage and cache
    await this.storage.put(this.getStorageKey(vn), scheduledView)
    this.viewCache.set(vn, scheduledView)

    // Update stats
    this.stats.totalViews++
    this.stats.enabledViews++

    // Ensure alarm is set for next refresh
    await this.ensureAlarmSet()

    return scheduledView
  }

  /**
   * Unschedule a view (remove from scheduler)
   *
   * @param name - View name
   * @returns true if the view was unscheduled
   */
  async unscheduleView(name: string): Promise<boolean> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const existing = this.viewCache.get(vn)
    if (!existing) return false

    await this.storage.delete(this.getStorageKey(vn))
    this.viewCache.delete(vn)

    // Update stats
    this.stats.totalViews--
    if (existing.enabled) {
      this.stats.enabledViews--
    } else {
      this.stats.disabledViews--
    }

    return true
  }

  /**
   * Enable a previously disabled view
   *
   * @param name - View name
   */
  async enableView(name: string): Promise<void> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const view = this.viewCache.get(vn)
    if (!view || view.enabled) return

    view.enabled = true
    view.consecutiveFailures = 0
    view.lastError = undefined
    view.nextRefreshAt = this.calculateNextRefreshTime(view.schedule, Date.now())

    await this.storage.put(this.getStorageKey(vn), view)

    // Update stats
    this.stats.enabledViews++
    this.stats.disabledViews--

    await this.ensureAlarmSet()
  }

  /**
   * Disable a view (stop scheduled refreshes)
   *
   * @param name - View name
   * @param reason - Optional reason for disabling
   */
  async disableView(name: string, reason?: string): Promise<void> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const view = this.viewCache.get(vn)
    if (!view || !view.enabled) return

    view.enabled = false
    if (reason) {
      view.lastError = reason
    }

    await this.storage.put(this.getStorageKey(vn), view)

    // Update stats
    this.stats.enabledViews--
    this.stats.disabledViews++
  }

  /**
   * Update a view's schedule
   *
   * @param name - View name
   * @param schedule - New schedule configuration
   */
  async updateSchedule(name: string, schedule: ScheduleOptions): Promise<void> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const view = this.viewCache.get(vn)
    if (!view) {
      throw new Error(`View '${name}' is not scheduled`)
    }

    view.schedule = schedule
    view.nextRefreshAt = this.calculateNextRefreshTime(schedule, Date.now())

    await this.storage.put(this.getStorageKey(vn), view)
    await this.ensureAlarmSet()
  }

  /**
   * Trigger an immediate refresh for a view
   *
   * @param name - View name
   * @returns true if refresh was triggered, false if already processing
   */
  async triggerRefresh(name: string): Promise<boolean> {
    await this.ensureCacheLoaded()

    const vn = viewName(name)
    const view = this.viewCache.get(vn)
    if (!view) {
      throw new Error(`View '${name}' is not scheduled`)
    }

    // Check if already processing
    if (this.processing.has(vn)) {
      return false
    }

    // Process immediately
    await this.refreshView(view)
    return true
  }

  // ===========================================================================
  // Alarm Processing
  // ===========================================================================

  /**
   * Process scheduled alarm
   *
   * This should be called from the DO's alarm() handler.
   * Processes all views due for refresh and schedules the next alarm.
   */
  async processAlarm(): Promise<AlarmProcessingResult> {
    await this.ensureCacheLoaded()

    const now = Date.now()
    const result: AlarmProcessingResult = {
      refreshed: [],
      failed: [],
      skipped: [],
    }

    // Find all views due for refresh
    const dueViews = Array.from(this.viewCache.values())
      .filter(v => v.enabled && v.nextRefreshAt <= now && !this.processing.has(v.name))

    // Process each due view
    for (const view of dueViews) {
      try {
        await this.refreshView(view)
        result.refreshed.push(view.name)
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error)
        result.failed.push({ name: view.name, error: errorMessage })
      }
    }

    // Record skipped views (still processing from previous alarm)
    for (const view of this.viewCache.values()) {
      if (view.enabled && view.nextRefreshAt <= now && this.processing.has(view.name)) {
        result.skipped.push(view.name)
      }
    }

    // Schedule next alarm
    result.nextAlarmAt = await this.ensureAlarmSet()

    return result
  }

  /**
   * Refresh a single view
   */
  private async refreshView(view: ScheduledView): Promise<void> {
    this.processing.add(view.name)
    const startTime = Date.now()

    try {
      // Call the refresh callback
      await this.config.onRefresh(view.name)

      // Update view state on success
      const duration = Date.now() - startTime
      view.lastRefreshAt = Date.now()
      view.lastRefreshDurationMs = duration
      view.consecutiveFailures = 0
      view.lastError = undefined
      view.nextRefreshAt = this.calculateNextRefreshTime(view.schedule, Date.now())

      await this.storage.put(this.getStorageKey(view.name), view)

      // Update stats
      this.stats.totalRefreshes++
      this.stats.successfulRefreshes++

      // Call completion callback
      await this.config.onRefreshComplete(view.name, duration)
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)

      // Update view state on failure
      view.consecutiveFailures++
      view.lastError = errorMessage

      // Check if view should be disabled
      const maxRetries = view.retryConfig?.maxRetries ?? this.config.defaultRetryConfig.maxRetries
      if (view.consecutiveFailures >= maxRetries) {
        view.enabled = false
        this.stats.enabledViews--
        this.stats.disabledViews++
        await this.config.onViewDisabled(view.name, `Disabled after ${view.consecutiveFailures} consecutive failures: ${errorMessage}`)
      } else {
        // Schedule retry with exponential backoff
        view.nextRefreshAt = this.calculateRetryTime(view)
      }

      await this.storage.put(this.getStorageKey(view.name), view)

      // Update stats
      this.stats.totalRefreshes++
      this.stats.failedRefreshes++

      // Call error callback
      await this.config.onRefreshError(view.name, error instanceof Error ? error : new Error(errorMessage))

      throw error
    } finally {
      this.processing.delete(view.name)
    }
  }

  // ===========================================================================
  // Query Methods
  // ===========================================================================

  /**
   * Get a scheduled view by name
   */
  async getView(name: string): Promise<ScheduledView | null> {
    await this.ensureCacheLoaded()
    return this.viewCache.get(viewName(name)) ?? null
  }

  /**
   * Get all scheduled views
   */
  async getViews(): Promise<ScheduledView[]> {
    await this.ensureCacheLoaded()
    return Array.from(this.viewCache.values())
  }

  /**
   * Get views due for refresh
   */
  async getDueViews(): Promise<ScheduledView[]> {
    await this.ensureCacheLoaded()
    const now = Date.now()
    return Array.from(this.viewCache.values())
      .filter(v => v.enabled && v.nextRefreshAt <= now)
  }

  /**
   * Get scheduler statistics
   */
  async getStats(): Promise<SchedulerStats> {
    await this.ensureCacheLoaded()

    // Calculate next refresh time
    const enabledViews = Array.from(this.viewCache.values()).filter(v => v.enabled)
    if (enabledViews.length > 0) {
      this.stats.nextRefreshAt = Math.min(...enabledViews.map(v => v.nextRefreshAt))
    } else {
      this.stats.nextRefreshAt = undefined
    }

    return { ...this.stats }
  }

  /**
   * Check if a view is currently being refreshed
   */
  isProcessing(name: string): boolean {
    return this.processing.has(viewName(name))
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Calculate the next refresh time based on schedule
   */
  private calculateNextRefreshTime(schedule: ScheduleOptions, afterTime: number): number {
    if (schedule.cron) {
      const cronExpr = parseCronExpression(schedule.cron)
      const nextTime = getNextCronTime(cronExpr, new Date(afterTime))
      return nextTime.getTime()
    }

    if (schedule.intervalMs) {
      return afterTime + schedule.intervalMs
    }

    // Default to 1 hour if no schedule specified
    return afterTime + 3600000
  }

  /**
   * Calculate retry time with exponential backoff
   */
  private calculateRetryTime(view: ScheduledView): number {
    const config = view.retryConfig ?? this.config.defaultRetryConfig
    const delay = Math.min(
      config.baseDelayMs * Math.pow(config.backoffMultiplier, view.consecutiveFailures - 1),
      config.maxDelayMs
    )
    return Date.now() + delay
  }

  /**
   * Ensure the cache is loaded from storage
   */
  private async ensureCacheLoaded(): Promise<void> {
    if (this.cacheLoaded) return

    const entries = await this.storage.list<ScheduledView>({
      prefix: STORAGE_PREFIX,
    })

    this.viewCache.clear()
    let enabled = 0
    let disabled = 0

    for (const [_, view] of entries) {
      this.viewCache.set(view.name, view)
      if (view.enabled) {
        enabled++
      } else {
        disabled++
      }
    }

    // Load stats
    const savedStats = await this.storage.get<SchedulerStats>(STATS_KEY)
    if (savedStats) {
      this.stats = savedStats
    }
    this.stats.totalViews = entries.size
    this.stats.enabledViews = enabled
    this.stats.disabledViews = disabled

    this.cacheLoaded = true
  }

  /**
   * Ensure an alarm is set for the next due view
   */
  private async ensureAlarmSet(): Promise<number | undefined> {
    const enabledViews = Array.from(this.viewCache.values()).filter(v => v.enabled)
    if (enabledViews.length === 0) return undefined

    // Find the earliest next refresh time
    const nextRefreshAt = Math.min(...enabledViews.map(v => v.nextRefreshAt))

    // Get current alarm
    const currentAlarm = await this.storage.getAlarm()

    // Determine when to set the alarm
    const now = Date.now()
    const alarmTime = Math.max(nextRefreshAt, now + this.config.minAlarmIntervalMs)

    // Only set alarm if no alarm exists or if new time is earlier
    if (!currentAlarm || alarmTime < currentAlarm) {
      await this.storage.setAlarm(alarmTime)
    }

    return alarmTime
  }

  /**
   * Get the storage key for a view
   */
  private getStorageKey(name: ViewName): string {
    return `${STORAGE_PREFIX}${name}`
  }

  /**
   * Save statistics to storage
   */
  async saveStats(): Promise<void> {
    await this.storage.put(STATS_KEY, this.stats)
  }

  /**
   * Clear all scheduled views and reset state
   *
   * WARNING: This is a destructive operation
   */
  async clear(): Promise<void> {
    const keys = Array.from(this.viewCache.keys()).map(k => this.getStorageKey(k))
    if (keys.length > 0) {
      await this.storage.delete(keys)
    }
    await this.storage.delete(STATS_KEY)
    await this.storage.deleteAlarm()

    this.viewCache.clear()
    this.processing.clear()
    this.stats = {
      totalViews: 0,
      enabledViews: 0,
      disabledViews: 0,
      totalRefreshes: 0,
      successfulRefreshes: 0,
      failedRefreshes: 0,
    }
    this.cacheLoaded = true
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new MV Scheduler
 */
export function createMVScheduler(
  storage: DurableObjectStorage,
  config: MVSchedulerConfig
): MVScheduler {
  return new MVScheduler(storage, config)
}

/**
 * Create a retry configuration with custom values
 */
export function createRetryConfig(overrides: Partial<RetryConfig>): RetryConfig {
  return { ...DEFAULT_RETRY_CONFIG, ...overrides }
}
