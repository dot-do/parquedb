/**
 * Materialized View Scheduler Tests
 *
 * Tests for DO alarm-based scheduled refresh of materialized views.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  MVScheduler,
  createMVScheduler,
  parseCronExpression,
  getNextCronTime,
  DEFAULT_RETRY_CONFIG,
  createRetryConfig,
  type ScheduledView,
  type AlarmProcessingResult,
} from '@/materialized-views/scheduler'
import { viewName } from '@/materialized-views/types'

// =============================================================================
// Mock Durable Object Storage
// =============================================================================

class MockDurableObjectStorage {
  private data: Map<string, unknown> = new Map()
  private alarm: number | null = null

  async get<T>(key: string): Promise<T | undefined>
  async get<T>(keys: string[]): Promise<Record<string, T>>
  async get<T>(key: string | string[]): Promise<T | undefined | Record<string, T>> {
    if (Array.isArray(key)) {
      const result: Record<string, T> = {}
      for (const k of key) {
        const value = this.data.get(k)
        if (value !== undefined) {
          result[k] = value as T
        }
      }
      return result
    }
    return this.data.get(key) as T | undefined
  }

  async put<T>(key: string, value: T): Promise<void>
  async put<T>(entries: Map<string, T>): Promise<void>
  async put<T>(key: string | Map<string, T>, value?: T): Promise<void> {
    if (typeof key === 'string') {
      this.data.set(key, value)
    } else {
      for (const [k, v] of key) {
        this.data.set(k, v)
      }
    }
  }

  async delete(key: string): Promise<boolean>
  async delete(keys: string[]): Promise<number>
  async delete(key: string | string[]): Promise<boolean | number> {
    if (Array.isArray(key)) {
      let count = 0
      for (const k of key) {
        if (this.data.delete(k)) count++
      }
      return count
    }
    return this.data.delete(key)
  }

  async list<T>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>> {
    const result = new Map<string, T>()
    for (const [key, value] of this.data) {
      if (!options?.prefix || key.startsWith(options.prefix)) {
        result.set(key, value as T)
        if (options?.limit && result.size >= options.limit) break
      }
    }
    return result
  }

  async setAlarm(time: number | Date): Promise<void> {
    this.alarm = typeof time === 'number' ? time : time.getTime()
  }

  async getAlarm(): Promise<number | null> {
    return this.alarm
  }

  async deleteAlarm(): Promise<void> {
    this.alarm = null
  }

  // Test helpers
  clear(): void {
    this.data.clear()
    this.alarm = null
  }

  getDataSize(): number {
    return this.data.size
  }
}

// =============================================================================
// Cron Parser Tests
// =============================================================================

describe('Cron Parser', () => {
  describe('parseCronExpression', () => {
    it('parses wildcard fields', () => {
      const cron = parseCronExpression('* * * * *')
      expect(cron.minute).toHaveLength(60) // 0-59
      expect(cron.hour).toHaveLength(24) // 0-23
      expect(cron.dayOfMonth).toHaveLength(31) // 1-31
      expect(cron.month).toHaveLength(12) // 1-12
      expect(cron.dayOfWeek).toHaveLength(7) // 0-6
    })

    it('parses specific values', () => {
      const cron = parseCronExpression('0 12 15 6 1')
      expect(cron.minute).toEqual([0])
      expect(cron.hour).toEqual([12])
      expect(cron.dayOfMonth).toEqual([15])
      expect(cron.month).toEqual([6])
      expect(cron.dayOfWeek).toEqual([1])
    })

    it('parses ranges', () => {
      const cron = parseCronExpression('0-5 9-17 * * 1-5')
      expect(cron.minute).toEqual([0, 1, 2, 3, 4, 5])
      expect(cron.hour).toEqual([9, 10, 11, 12, 13, 14, 15, 16, 17])
      expect(cron.dayOfWeek).toEqual([1, 2, 3, 4, 5])
    })

    it('parses lists', () => {
      const cron = parseCronExpression('0,15,30,45 * * * *')
      expect(cron.minute).toEqual([0, 15, 30, 45])
    })

    it('parses step values', () => {
      const cron = parseCronExpression('*/15 */2 * * *')
      expect(cron.minute).toEqual([0, 15, 30, 45])
      expect(cron.hour).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22])
    })

    it('throws for invalid expression with wrong number of parts', () => {
      expect(() => parseCronExpression('* * *')).toThrow('5 fields')
      expect(() => parseCronExpression('* * * * * *')).toThrow('5 fields')
    })

    it('throws for out-of-range minute values', () => {
      expect(() => parseCronExpression('60 * * * *')).toThrow('minute')
      expect(() => parseCronExpression('60 * * * *')).toThrow('60')
      expect(() => parseCronExpression('60 * * * *')).toThrow('out of range')
    })

    it('throws for out-of-range hour values', () => {
      expect(() => parseCronExpression('0 24 * * *')).toThrow('hour')
      expect(() => parseCronExpression('0 24 * * *')).toThrow('24')
      expect(() => parseCronExpression('0 25 * * *')).toThrow('out of range')
    })

    it('throws for out-of-range day of month values', () => {
      expect(() => parseCronExpression('0 0 0 * *')).toThrow('day of month')
      expect(() => parseCronExpression('0 0 32 * *')).toThrow('32')
      expect(() => parseCronExpression('0 0 32 * *')).toThrow('out of range')
    })

    it('throws for out-of-range month values', () => {
      expect(() => parseCronExpression('0 0 * 0 *')).toThrow('month')
      expect(() => parseCronExpression('0 0 * 13 *')).toThrow('13')
      expect(() => parseCronExpression('0 0 * 13 *')).toThrow('out of range')
    })

    it('throws for out-of-range day of week values', () => {
      expect(() => parseCronExpression('0 0 * * 7')).toThrow('day of week')
      expect(() => parseCronExpression('0 0 * * 7')).toThrow('7')
      expect(() => parseCronExpression('0 0 * * 8')).toThrow('out of range')
    })

    it('throws for invalid range expressions', () => {
      expect(() => parseCronExpression('5-3 * * * *')).toThrow('start (5)')
      expect(() => parseCronExpression('5-3 * * * *')).toThrow('greater than end')
    })

    it('throws for invalid step expressions', () => {
      expect(() => parseCronExpression('*/0 * * * *')).toThrow('step')
      expect(() => parseCronExpression('*/abc * * * *')).toThrow('step')
    })

    it('throws for non-integer values', () => {
      expect(() => parseCronExpression('1.5 * * * *')).toThrow('not a valid integer')
      expect(() => parseCronExpression('abc * * * *')).toThrow('not a valid integer')
    })

    it('parses common patterns', () => {
      // Every hour at minute 0
      const hourly = parseCronExpression('0 * * * *')
      expect(hourly.minute).toEqual([0])

      // Daily at midnight
      const daily = parseCronExpression('0 0 * * *')
      expect(daily.minute).toEqual([0])
      expect(daily.hour).toEqual([0])

      // Weekly on Sunday at midnight
      const weekly = parseCronExpression('0 0 * * 0')
      expect(weekly.dayOfWeek).toEqual([0])

      // Monthly on the 1st at midnight
      const monthly = parseCronExpression('0 0 1 * *')
      expect(monthly.dayOfMonth).toEqual([1])
    })
  })

  describe('getNextCronTime', () => {
    it('calculates next occurrence for hourly', () => {
      const cron = parseCronExpression('0 * * * *')
      const baseTime = new Date('2024-01-15T10:30:00Z')
      const next = getNextCronTime(cron, baseTime)

      expect(next.getUTCMinutes()).toBe(0)
      expect(next.getUTCHours()).toBe(11)
    })

    it('calculates next occurrence for daily', () => {
      const cron = parseCronExpression('0 9 * * *')
      const baseTime = new Date('2024-01-15T10:00:00Z')
      const next = getNextCronTime(cron, baseTime)

      // Next occurrence at 9:00 (could be same day if before 9am local, or next day)
      expect(next.getMinutes()).toBe(0)
      expect(next.getHours()).toBe(9)
      // Time should be after base time
      expect(next.getTime()).toBeGreaterThan(baseTime.getTime())
    })

    it('calculates next occurrence for specific weekday', () => {
      const cron = parseCronExpression('0 9 * * 1') // Monday at 9am
      const baseTime = new Date('2024-01-15T10:00:00Z') // Monday at 10am
      const next = getNextCronTime(cron, baseTime)

      // Next Monday at 9am
      expect(next.getDay()).toBe(1) // Monday
      expect(next.getHours()).toBe(9)
      // Should be a future Monday
      expect(next.getTime()).toBeGreaterThan(baseTime.getTime())
    })

    it('handles month boundaries', () => {
      const cron = parseCronExpression('0 0 1 * *') // 1st of month at midnight
      const baseTime = new Date('2024-01-15T00:00:00Z')
      const next = getNextCronTime(cron, baseTime)

      expect(next.getUTCDate()).toBe(1)
      expect(next.getUTCMonth()).toBe(1) // February
    })

    it('advances from exact match time', () => {
      const cron = parseCronExpression('30 10 * * *')
      const baseTime = new Date('2024-01-15T10:30:00Z')
      const next = getNextCronTime(cron, baseTime)

      // Should advance since we're at the match time
      // The function starts from afterTime + 1 minute
      expect(next.getMinutes()).toBe(30)
      expect(next.getHours()).toBe(10)
      expect(next.getTime()).toBeGreaterThan(baseTime.getTime())
    })
  })
})

// =============================================================================
// MVScheduler Tests
// =============================================================================

describe('MVScheduler', () => {
  let storage: MockDurableObjectStorage
  let onRefresh: ReturnType<typeof vi.fn>
  let onRefreshComplete: ReturnType<typeof vi.fn>
  let onRefreshError: ReturnType<typeof vi.fn>
  let onViewDisabled: ReturnType<typeof vi.fn>

  beforeEach(() => {
    storage = new MockDurableObjectStorage()
    onRefresh = vi.fn().mockResolvedValue(undefined)
    onRefreshComplete = vi.fn().mockResolvedValue(undefined)
    onRefreshError = vi.fn().mockResolvedValue(undefined)
    onViewDisabled = vi.fn().mockResolvedValue(undefined)
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T10:00:00Z'))
  })

  function createScheduler(): MVScheduler {
    return createMVScheduler(storage as unknown as DurableObjectStorage, {
      onRefresh,
      onRefreshComplete,
      onRefreshError,
      onViewDisabled,
    })
  }

  describe('scheduleView', () => {
    it('schedules a view with interval', async () => {
      const scheduler = createScheduler()

      const view = await scheduler.scheduleView('my_view', {
        intervalMs: 60000, // Every minute
      })

      expect(view.name).toBe('my_view')
      expect(view.enabled).toBe(true)
      expect(view.consecutiveFailures).toBe(0)
      expect(view.nextRefreshAt).toBe(Date.now() + 60000)
    })

    it('schedules a view with cron', async () => {
      const scheduler = createScheduler()

      const view = await scheduler.scheduleView('hourly_view', {
        cron: '0 * * * *', // Every hour at minute 0
      })

      expect(view.name).toBe('hourly_view')
      expect(view.enabled).toBe(true)
      // Next occurrence should be at 11:00
      const nextDate = new Date(view.nextRefreshAt)
      expect(nextDate.getUTCHours()).toBe(11)
      expect(nextDate.getUTCMinutes()).toBe(0)
    })

    it('sets an alarm for the view', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', {
        intervalMs: 60000,
      })

      const alarm = await storage.getAlarm()
      expect(alarm).not.toBeNull()
    })

    it('updates stats after scheduling', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 60000 })
      await scheduler.scheduleView('view2', { intervalMs: 120000 })

      const stats = await scheduler.getStats()
      expect(stats.totalViews).toBe(2)
      expect(stats.enabledViews).toBe(2)
      expect(stats.disabledViews).toBe(0)
    })

    it('persists view to storage', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('persistent_view', {
        intervalMs: 60000,
      })

      // Create new scheduler instance (simulates DO restart)
      const newScheduler = createScheduler()
      const view = await newScheduler.getView('persistent_view')

      expect(view).not.toBeNull()
      expect(view?.name).toBe('persistent_view')
    })
  })

  describe('unscheduleView', () => {
    it('removes a scheduled view', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('to_remove', { intervalMs: 60000 })
      const result = await scheduler.unscheduleView('to_remove')

      expect(result).toBe(true)

      const view = await scheduler.getView('to_remove')
      expect(view).toBeNull()
    })

    it('returns false for non-existent view', async () => {
      const scheduler = createScheduler()

      const result = await scheduler.unscheduleView('nonexistent')
      expect(result).toBe(false)
    })

    it('updates stats after unscheduling', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 60000 })
      await scheduler.unscheduleView('view1')

      const stats = await scheduler.getStats()
      expect(stats.totalViews).toBe(0)
    })
  })

  describe('enableView / disableView', () => {
    it('disables a view', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })
      await scheduler.disableView('my_view', 'Manual disable')

      const view = await scheduler.getView('my_view')
      expect(view?.enabled).toBe(false)
      expect(view?.lastError).toBe('Manual disable')
    })

    it('enables a disabled view', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })
      await scheduler.disableView('my_view')
      await scheduler.enableView('my_view')

      const view = await scheduler.getView('my_view')
      expect(view?.enabled).toBe(true)
      expect(view?.consecutiveFailures).toBe(0)
    })

    it('updates stats when enabling/disabling', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })

      await scheduler.disableView('my_view')
      let stats = await scheduler.getStats()
      expect(stats.enabledViews).toBe(0)
      expect(stats.disabledViews).toBe(1)

      await scheduler.enableView('my_view')
      stats = await scheduler.getStats()
      expect(stats.enabledViews).toBe(1)
      expect(stats.disabledViews).toBe(0)
    })
  })

  describe('updateSchedule', () => {
    it('updates the schedule for a view', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })
      await scheduler.updateSchedule('my_view', { intervalMs: 120000 })

      const view = await scheduler.getView('my_view')
      expect(view?.schedule.intervalMs).toBe(120000)
    })

    it('throws for non-existent view', async () => {
      const scheduler = createScheduler()

      await expect(
        scheduler.updateSchedule('nonexistent', { intervalMs: 60000 })
      ).rejects.toThrow('not scheduled')
    })

    it('recalculates next refresh time', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })
      const originalNext = (await scheduler.getView('my_view'))!.nextRefreshAt

      await scheduler.updateSchedule('my_view', { intervalMs: 30000 })
      const newNext = (await scheduler.getView('my_view'))!.nextRefreshAt

      expect(newNext).toBeLessThan(originalNext)
    })
  })

  describe('processAlarm', () => {
    it('refreshes due views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 1000 })

      // Advance time past the refresh interval
      vi.advanceTimersByTime(2000)

      const result = await scheduler.processAlarm()

      expect(result.refreshed).toContain('my_view')
      expect(onRefresh).toHaveBeenCalledWith('my_view')
    })

    it('does not refresh views not yet due', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })

      // Only advance 30 seconds
      vi.advanceTimersByTime(30000)

      const result = await scheduler.processAlarm()

      expect(result.refreshed).toHaveLength(0)
      expect(onRefresh).not.toHaveBeenCalled()
    })

    it('handles refresh errors', async () => {
      const error = new Error('Refresh failed')
      onRefresh.mockRejectedValueOnce(error)

      const scheduler = createScheduler()

      await scheduler.scheduleView('failing_view', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)

      const result = await scheduler.processAlarm()

      expect(result.failed).toHaveLength(1)
      expect(result.failed[0]).toMatchObject({
        name: 'failing_view',
        error: 'Refresh failed',
      })
      expect(onRefreshError).toHaveBeenCalledWith('failing_view', error)
    })

    it('increments failure count on error', async () => {
      onRefresh.mockRejectedValue(new Error('Refresh failed'))

      const scheduler = createScheduler()

      await scheduler.scheduleView('failing_view', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      const view = await scheduler.getView('failing_view')
      expect(view?.consecutiveFailures).toBe(1)
    })

    it('disables view after max retries', async () => {
      onRefresh.mockRejectedValue(new Error('Refresh failed'))

      const scheduler = createScheduler()

      await scheduler.scheduleView('failing_view', {
        intervalMs: 1000,
      }, createRetryConfig({ maxRetries: 3 }))

      // Trigger 3 failures
      for (let i = 0; i < 3; i++) {
        vi.advanceTimersByTime(2000)
        await scheduler.processAlarm()
      }

      const view = await scheduler.getView('failing_view')
      expect(view?.enabled).toBe(false)
      expect(onViewDisabled).toHaveBeenCalled()
    })

    it('schedules next alarm after processing', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })

      vi.advanceTimersByTime(61000)
      const result = await scheduler.processAlarm()

      expect(result.nextAlarmAt).toBeDefined()
      expect(result.nextAlarmAt).toBeGreaterThan(Date.now())
    })

    it('calls onRefreshComplete for successful refresh', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      expect(onRefreshComplete).toHaveBeenCalled()
      const call = onRefreshComplete.mock.calls[0]
      expect(call[0]).toBe('my_view')
      expect(typeof call[1]).toBe('number') // duration
    })

    it('updates view state after successful refresh', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      const view = await scheduler.getView('my_view')
      expect(view?.lastRefreshAt).toBeDefined()
      expect(view?.lastRefreshDurationMs).toBeDefined()
      expect(view?.consecutiveFailures).toBe(0)
    })

    it('processes multiple due views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 1000 })
      await scheduler.scheduleView('view2', { intervalMs: 1000 })
      await scheduler.scheduleView('view3', { intervalMs: 60000 }) // Not due

      vi.advanceTimersByTime(2000)
      const result = await scheduler.processAlarm()

      expect(result.refreshed).toContain('view1')
      expect(result.refreshed).toContain('view2')
      expect(result.refreshed).not.toContain('view3')
    })
  })

  describe('triggerRefresh', () => {
    it('triggers immediate refresh', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })
      const result = await scheduler.triggerRefresh('my_view')

      expect(result).toBe(true)
      expect(onRefresh).toHaveBeenCalledWith('my_view')
    })

    it('throws for non-existent view', async () => {
      const scheduler = createScheduler()

      await expect(scheduler.triggerRefresh('nonexistent')).rejects.toThrow('not scheduled')
    })
  })

  describe('getViews / getDueViews', () => {
    it('returns all scheduled views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 60000 })
      await scheduler.scheduleView('view2', { intervalMs: 120000 })

      const views = await scheduler.getViews()
      expect(views).toHaveLength(2)
    })

    it('returns only due views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('due_view', { intervalMs: 1000 })
      await scheduler.scheduleView('not_due_view', { intervalMs: 60000 })

      vi.advanceTimersByTime(2000)

      const dueViews = await scheduler.getDueViews()
      expect(dueViews).toHaveLength(1)
      expect(dueViews[0]!.name).toBe('due_view')
    })

    it('excludes disabled views from due views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 1000 })
      await scheduler.disableView('my_view')

      vi.advanceTimersByTime(2000)

      const dueViews = await scheduler.getDueViews()
      expect(dueViews).toHaveLength(0)
    })
  })

  describe('getStats', () => {
    it('returns accurate statistics', async () => {
      onRefresh
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('Failed'))

      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 1000 })
      await scheduler.scheduleView('view2', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      const stats = await scheduler.getStats()
      expect(stats.totalViews).toBe(2)
      expect(stats.totalRefreshes).toBe(2)
      expect(stats.successfulRefreshes).toBe(1)
      expect(stats.failedRefreshes).toBe(1)
    })

    it('includes next refresh time', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })

      const stats = await scheduler.getStats()
      expect(stats.nextRefreshAt).toBeDefined()
      expect(stats.nextRefreshAt).toBe(Date.now() + 60000)
    })
  })

  describe('isProcessing', () => {
    it('returns true during refresh', async () => {
      // Use real timers for this test since we need to track processing state
      vi.useRealTimers()

      let isProcessingDuringRefresh = false
      let resolveRefresh: () => void

      const refreshPromise = new Promise<void>(resolve => {
        resolveRefresh = resolve
      })

      const localOnRefresh = vi.fn().mockImplementation(async () => {
        isProcessingDuringRefresh = localScheduler.isProcessing('my_view')
        await refreshPromise
      })

      const localScheduler = createMVScheduler(storage as unknown as DurableObjectStorage, {
        onRefresh: localOnRefresh,
      })

      // Schedule view with immediate refresh (nextRefreshAt in the past)
      await localScheduler.scheduleView('my_view', { intervalMs: 1 })

      // Wait a small amount using real time for the view to become due
      await new Promise(resolve => setTimeout(resolve, 10))

      // Start processing (will be blocked by refreshPromise)
      const processPromise = localScheduler.processAlarm()

      // Give the processing a moment to start
      await new Promise(resolve => setTimeout(resolve, 10))

      // Now resolve the refresh to complete
      resolveRefresh!()
      await processPromise

      expect(isProcessingDuringRefresh).toBe(true)

      // Restore fake timers for other tests
      vi.useFakeTimers()
      vi.setSystemTime(new Date('2024-01-15T10:00:00Z'))
    })

    it('returns false when not processing', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('my_view', { intervalMs: 60000 })

      expect(scheduler.isProcessing('my_view')).toBe(false)
    })
  })

  describe('clear', () => {
    it('removes all scheduled views', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 60000 })
      await scheduler.scheduleView('view2', { intervalMs: 60000 })

      await scheduler.clear()

      const views = await scheduler.getViews()
      expect(views).toHaveLength(0)
    })

    it('resets statistics', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 1000 })
      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      await scheduler.clear()

      const stats = await scheduler.getStats()
      expect(stats.totalViews).toBe(0)
      expect(stats.totalRefreshes).toBe(0)
    })

    it('deletes alarm', async () => {
      const scheduler = createScheduler()

      await scheduler.scheduleView('view1', { intervalMs: 60000 })
      await scheduler.clear()

      const alarm = await storage.getAlarm()
      expect(alarm).toBeNull()
    })
  })

  describe('exponential backoff', () => {
    it('increases retry delay exponentially', async () => {
      onRefresh.mockRejectedValue(new Error('Refresh failed'))

      const scheduler = createScheduler()

      await scheduler.scheduleView('failing_view', {
        intervalMs: 1000,
      }, createRetryConfig({
        maxRetries: 5,
        baseDelayMs: 1000,
        backoffMultiplier: 2,
        maxDelayMs: 60000,
      }))

      // First failure
      vi.advanceTimersByTime(2000)
      await scheduler.processAlarm()

      let view = await scheduler.getView('failing_view')
      const firstRetry = view!.nextRefreshAt - Date.now()

      // Second failure
      vi.advanceTimersByTime(firstRetry + 100)
      await scheduler.processAlarm()

      view = await scheduler.getView('failing_view')
      const secondRetry = view!.nextRefreshAt - Date.now()

      // Second retry should be longer than first
      expect(secondRetry).toBeGreaterThan(firstRetry)
    })

    it('respects maxDelayMs', async () => {
      onRefresh.mockRejectedValue(new Error('Refresh failed'))

      const scheduler = createScheduler()
      const maxDelay = 5000

      await scheduler.scheduleView('failing_view', {
        intervalMs: 1000,
      }, createRetryConfig({
        maxRetries: 10,
        baseDelayMs: 1000,
        backoffMultiplier: 10, // Large multiplier to quickly exceed max
        maxDelayMs: maxDelay,
      }))

      // Trigger several failures
      for (let i = 0; i < 5; i++) {
        vi.advanceTimersByTime(maxDelay + 1000)
        await scheduler.processAlarm()
      }

      const view = await scheduler.getView('failing_view')
      const retryDelay = view!.nextRefreshAt - Date.now()

      expect(retryDelay).toBeLessThanOrEqual(maxDelay)
    })
  })

  describe('persistence and recovery', () => {
    it('restores views after scheduler restart', async () => {
      const scheduler1 = createScheduler()

      await scheduler1.scheduleView('persistent_view', { intervalMs: 60000 })

      // Simulate DO restart by creating new scheduler
      const scheduler2 = createScheduler()

      const views = await scheduler2.getViews()
      expect(views).toHaveLength(1)
      expect(views[0]!.name).toBe('persistent_view')
    })

    it('restores stats after scheduler restart', async () => {
      const scheduler1 = createScheduler()

      await scheduler1.scheduleView('my_view', { intervalMs: 1000 })

      vi.advanceTimersByTime(2000)
      await scheduler1.processAlarm()

      await scheduler1.saveStats()

      // Simulate DO restart
      const scheduler2 = createScheduler()
      await scheduler2.getViews() // Trigger cache load

      const stats = await scheduler2.getStats()
      expect(stats.totalRefreshes).toBe(1)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory functions', () => {
  it('createMVScheduler creates a scheduler', () => {
    const storage = new MockDurableObjectStorage()
    const scheduler = createMVScheduler(
      storage as unknown as DurableObjectStorage,
      { onRefresh: async () => {} }
    )
    expect(scheduler).toBeInstanceOf(MVScheduler)
  })

  it('createRetryConfig merges with defaults', () => {
    const config = createRetryConfig({ maxRetries: 5 })
    expect(config.maxRetries).toBe(5)
    expect(config.baseDelayMs).toBe(DEFAULT_RETRY_CONFIG.baseDelayMs)
  })
})

// =============================================================================
// ViewName Tests
// =============================================================================

describe('viewName', () => {
  it('creates a ViewName branded type', () => {
    const vn = viewName('test_view')
    expect(vn).toBe('test_view')
    // TypeScript ensures the brand is applied
  })
})
