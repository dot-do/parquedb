/**
 * RetentionManager - Data Retention and Compaction for Observability MVs
 *
 * Provides efficient batch cleanup and retention policy management for
 * materialized views. Replaces inefficient O(n) delete-one-by-one operations
 * with batch deletes.
 *
 * Features:
 * - Tiered retention policies by granularity (hourly, daily, monthly)
 * - Batch delete operations for efficiency
 * - Background cleanup scheduling
 * - Progress callbacks and logging
 *
 * @example
 * ```typescript
 * import { RetentionManager, createRetentionManager } from 'parquedb/observability/retention'
 *
 * const retentionManager = createRetentionManager(db, {
 *   collection: 'ai_requests',
 *   policies: {
 *     hourly: { maxAgeMs: 7 * 24 * 60 * 60 * 1000 },  // 7 days
 *     daily: { maxAgeMs: 90 * 24 * 60 * 60 * 1000 }, // 90 days
 *     monthly: { maxAgeMs: 365 * 24 * 60 * 60 * 1000 }, // 1 year
 *   }
 * })
 *
 * // Run cleanup
 * const result = await retentionManager.cleanup()
 * console.log(`Deleted ${result.deletedCount} records`)
 *
 * // Schedule background cleanup
 * const scheduler = retentionManager.scheduleCleanup({
 *   intervalMs: 60 * 60 * 1000, // Every hour
 *   onProgress: (progress) => console.log(`Cleanup ${progress.percentage}% complete`)
 * })
 * ```
 *
 * @module observability/retention/RetentionManager
 */

import type { ParqueDB } from '../../ParqueDB'
import type { TimeGranularity } from '../ai/types'
import { MAX_BATCH_SIZE, MS_PER_HOUR } from '../../constants'
import { logger } from '../../utils/logger'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE
const DEFAULT_HOURLY_MAX_AGE_MS = 7 * 24 * MS_PER_HOUR    // 7 days
const DEFAULT_DAILY_MAX_AGE_MS = 90 * 24 * MS_PER_HOUR   // 90 days
const DEFAULT_MONTHLY_MAX_AGE_MS = 365 * 24 * MS_PER_HOUR // 1 year
const DEFAULT_CLEANUP_INTERVAL_MS = MS_PER_HOUR          // 1 hour

// =============================================================================
// Types
// =============================================================================

/**
 * Retention policy for a specific granularity
 */
export interface RetentionPolicy {
  /** Maximum age in milliseconds for data of this granularity */
  maxAgeMs: number
  /** Whether this granularity is enabled for cleanup */
  enabled?: boolean | undefined
}

/**
 * Tiered retention policies by granularity
 */
export interface TieredRetentionPolicies {
  /** Retention for hourly granularity data */
  hourly?: RetentionPolicy | undefined
  /** Retention for daily granularity data */
  daily?: RetentionPolicy | undefined
  /** Retention for weekly granularity data */
  weekly?: RetentionPolicy | undefined
  /** Retention for monthly granularity data */
  monthly?: RetentionPolicy | undefined
  /** Default retention for records without granularity */
  default?: RetentionPolicy | undefined
}

/**
 * Configuration for RetentionManager
 */
export interface RetentionManagerConfig {
  /** Collection name to manage retention for */
  collection: string
  /** Tiered retention policies */
  policies?: TieredRetentionPolicies | undefined
  /** Simple max age (applies to all records if policies not set) */
  maxAgeMs?: number | undefined
  /** Batch size for delete operations */
  batchSize?: number | undefined
  /** Field name containing the timestamp (default: 'timestamp') */
  timestampField?: string | undefined
  /** Field name containing the granularity (default: 'granularity') */
  granularityField?: string | undefined
  /** Enable debug logging */
  debug?: boolean | undefined
}

/**
 * Resolved configuration with defaults
 */
export interface ResolvedRetentionConfig {
  collection: string
  policies: TieredRetentionPolicies
  batchSize: number
  timestampField: string
  granularityField: string
  debug: boolean
}

/**
 * Progress information for cleanup operations
 */
export interface CleanupProgress {
  /** Current phase of cleanup */
  phase: 'scanning' | 'deleting' | 'complete'
  /** Total records found for deletion */
  totalToDelete: number
  /** Records deleted so far */
  deletedSoFar: number
  /** Percentage complete (0-100) */
  percentage: number
  /** Current granularity being processed */
  currentGranularity?: TimeGranularity | 'default' | undefined
  /** Elapsed time in milliseconds */
  elapsedMs: number
}

/**
 * Result of a cleanup operation
 */
export interface CleanupResult {
  /** Whether cleanup completed successfully */
  success: boolean
  /** Total records deleted */
  deletedCount: number
  /** Breakdown by granularity */
  byGranularity: Record<string, number>
  /** Duration of cleanup in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string | undefined
}

/**
 * Options for scheduling cleanup
 */
export interface ScheduleOptions {
  /** Interval between cleanups in milliseconds */
  intervalMs?: number | undefined
  /** Progress callback */
  onProgress?: ((progress: CleanupProgress) => void) | undefined
  /** Completion callback */
  onComplete?: ((result: CleanupResult) => void) | undefined
  /** Error callback */
  onError?: ((error: Error) => void) | undefined
  /** Run immediately on start */
  runImmediately?: boolean | undefined
}

/**
 * Cleanup scheduler handle
 */
export interface CleanupScheduler {
  /** Stop the scheduler */
  stop(): void
  /** Pause the scheduler */
  pause(): void
  /** Resume the scheduler */
  resume(): void
  /** Manually trigger a cleanup */
  trigger(): Promise<CleanupResult>
  /** Check if scheduler is running */
  isRunning(): boolean
  /** Get next scheduled run time */
  nextRunAt(): Date | null
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: RetentionManagerConfig): ResolvedRetentionConfig {
  const policies: TieredRetentionPolicies = config.policies ?? {}

  // If no policies but maxAgeMs is set, use it as default
  if (!config.policies && config.maxAgeMs) {
    policies.default = { maxAgeMs: config.maxAgeMs, enabled: true }
  }

  // Set default policies if not provided
  if (!policies.hourly) {
    policies.hourly = { maxAgeMs: DEFAULT_HOURLY_MAX_AGE_MS, enabled: true }
  }
  if (!policies.daily) {
    policies.daily = { maxAgeMs: DEFAULT_DAILY_MAX_AGE_MS, enabled: true }
  }
  if (!policies.monthly) {
    policies.monthly = { maxAgeMs: DEFAULT_MONTHLY_MAX_AGE_MS, enabled: true }
  }
  if (!policies.default) {
    policies.default = { maxAgeMs: DEFAULT_DAILY_MAX_AGE_MS, enabled: true }
  }

  return {
    collection: config.collection,
    policies,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    timestampField: config.timestampField ?? 'timestamp',
    granularityField: config.granularityField ?? 'granularity',
    debug: config.debug ?? false,
  }
}

/**
 * Get retention policy for a specific granularity
 */
function _getPolicyForGranularity(
  policies: TieredRetentionPolicies,
  granularity: TimeGranularity | undefined
): RetentionPolicy | null {
  if (!granularity) {
    return policies.default ?? null
  }

  const policy = policies[granularity]
  if (policy && policy.enabled !== false) {
    return policy
  }

  return policies.default ?? null
}

// =============================================================================
// RetentionManager Class
// =============================================================================

/**
 * RetentionManager - Handles data retention and compaction for observability MVs
 *
 * Provides efficient batch cleanup operations with tiered retention policies.
 */
export class RetentionManager {
  private readonly db: ParqueDB
  private readonly config: ResolvedRetentionConfig

  /**
   * Create a new RetentionManager instance
   *
   * @param db - ParqueDB instance
   * @param config - Configuration options
   */
  constructor(db: ParqueDB, config: RetentionManagerConfig) {
    this.db = db
    this.config = resolveConfig(config)
  }

  /**
   * Run cleanup with progress tracking
   *
   * @param onProgress - Optional progress callback
   * @returns Cleanup result
   */
  async cleanup(onProgress?: (progress: CleanupProgress) => void): Promise<CleanupResult> {
    const startTime = Date.now()
    const collection = this.db.collection(this.config.collection)
    const byGranularity: Record<string, number> = {}
    let totalDeleted = 0

    try {
      const now = new Date()

      // Report initial progress
      onProgress?.({
        phase: 'scanning',
        totalToDelete: 0,
        deletedSoFar: 0,
        percentage: 0,
        elapsedMs: Date.now() - startTime,
      })

      // Process each granularity
      const granularities: (TimeGranularity | 'default')[] = ['hour', 'day', 'week', 'month', 'default']

      for (const granularity of granularities) {
        const policy = granularity === 'default'
          ? this.config.policies.default
          : this.config.policies[granularity]

        if (!policy || policy.enabled === false) {
          continue
        }

        const cutoffDate = new Date(now.getTime() - policy.maxAgeMs)

        // Build filter for this granularity
        const filter: Record<string, unknown> = {
          [this.config.timestampField]: { $lt: cutoffDate },
        }

        // Add granularity filter (skip for 'default' which handles records without granularity)
        if (granularity !== 'default') {
          filter[this.config.granularityField] = granularity
        } else {
          // For default, match records without a granularity field or with null/undefined
          filter[this.config.granularityField] = { $exists: false }
        }

        // Report scanning progress
        onProgress?.({
          phase: 'scanning',
          totalToDelete: totalDeleted,
          deletedSoFar: totalDeleted,
          percentage: 0,
          currentGranularity: granularity,
          elapsedMs: Date.now() - startTime,
        })

        // Count total to delete for this granularity
        const countResult = await collection.count(filter)

        if (countResult === 0) {
          byGranularity[granularity] = 0
          continue
        }

        if (this.config.debug) {
          logger.info(`[RetentionManager] Found ${countResult} ${granularity} records to delete`)
        }

        // Delete in batches
        let deletedForGranularity = 0
        let hasMore = true

        while (hasMore) {
          // Find batch of records to delete
          const batch = await collection.find(filter, {
            limit: this.config.batchSize,
          })

          if (batch.length === 0) {
            hasMore = false
            break
          }

          // Use deleteMany with filter for efficient batch deletion
          const deleteResult = await collection.deleteMany(filter, { hard: true })
          deletedForGranularity += deleteResult.deletedCount
          totalDeleted += deleteResult.deletedCount

          // Report progress
          const percentage = Math.min(100, Math.round((totalDeleted / (totalDeleted + batch.length)) * 100))
          onProgress?.({
            phase: 'deleting',
            totalToDelete: totalDeleted + batch.length,
            deletedSoFar: totalDeleted,
            percentage,
            currentGranularity: granularity,
            elapsedMs: Date.now() - startTime,
          })

          // If we deleted less than batch size, no more records
          if (deleteResult.deletedCount < this.config.batchSize) {
            hasMore = false
          }
        }

        byGranularity[granularity] = deletedForGranularity
      }

      // Report completion
      onProgress?.({
        phase: 'complete',
        totalToDelete: totalDeleted,
        deletedSoFar: totalDeleted,
        percentage: 100,
        elapsedMs: Date.now() - startTime,
      })

      return {
        success: true,
        deletedCount: totalDeleted,
        byGranularity,
        durationMs: Date.now() - startTime,
      }
    } catch (error) {
      return {
        success: false,
        deletedCount: totalDeleted,
        byGranularity,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Clean up records older than a specific cutoff date (simple cleanup)
   *
   * @param cutoffDate - Delete records older than this date
   * @returns Number of deleted records
   */
  async cleanupBefore(cutoffDate: Date): Promise<number> {
    const collection = this.db.collection(this.config.collection)

    const filter = {
      [this.config.timestampField]: { $lt: cutoffDate },
    }

    const result = await collection.deleteMany(filter, { hard: true })
    return result.deletedCount
  }

  /**
   * Schedule periodic cleanup
   *
   * @param options - Scheduling options
   * @returns Cleanup scheduler handle
   */
  scheduleCleanup(options: ScheduleOptions = {}): CleanupScheduler {
    const intervalMs = options.intervalMs ?? DEFAULT_CLEANUP_INTERVAL_MS
    let timer: NodeJS.Timeout | null = null
    let isPaused = false
    let isRunning = true
    let nextRun: Date | null = null

    const runCleanup = async () => {
      if (isPaused || !isRunning) {
        return { success: false, deletedCount: 0, byGranularity: {}, durationMs: 0, error: 'Scheduler paused' }
      }

      try {
        const result = await this.cleanup(options.onProgress)
        options.onComplete?.(result)
        return result
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error))
        options.onError?.(err)
        return { success: false, deletedCount: 0, byGranularity: {}, durationMs: 0, error: err.message }
      }
    }

    const scheduleNext = () => {
      if (!isRunning) return

      nextRun = new Date(Date.now() + intervalMs)
      timer = setTimeout(async () => {
        await runCleanup()
        scheduleNext()
      }, intervalMs)
    }

    // Run immediately if requested
    if (options.runImmediately) {
      runCleanup().then(() => scheduleNext())
    } else {
      scheduleNext()
    }

    return {
      stop() {
        isRunning = false
        if (timer) {
          clearTimeout(timer)
          timer = null
        }
        nextRun = null
      },
      pause() {
        isPaused = true
      },
      resume() {
        isPaused = false
      },
      async trigger() {
        return runCleanup()
      },
      isRunning() {
        return isRunning && !isPaused
      },
      nextRunAt() {
        return nextRun
      },
    }
  }

  /**
   * Get current configuration
   */
  getConfig(): ResolvedRetentionConfig {
    return { ...this.config }
  }

  /**
   * Get retention statistics
   *
   * @returns Statistics about data eligible for cleanup
   */
  async getRetentionStats(): Promise<{
    collection: string
    byGranularity: Record<string, { count: number; oldestTimestamp: Date | null; eligibleForDeletion: number }>
    totalRecords: number
    totalEligibleForDeletion: number
  }> {
    const collection = this.db.collection(this.config.collection)
    const now = new Date()
    const byGranularity: Record<string, { count: number; oldestTimestamp: Date | null; eligibleForDeletion: number }> = {}

    const granularities: (TimeGranularity | 'default')[] = ['hour', 'day', 'week', 'month', 'default']
    let totalRecords = 0
    let totalEligibleForDeletion = 0

    for (const granularity of granularities) {
      const policy = granularity === 'default'
        ? this.config.policies.default
        : this.config.policies[granularity]

      // Build filter for this granularity
      const baseFilter: Record<string, unknown> = {}
      if (granularity !== 'default') {
        baseFilter[this.config.granularityField] = granularity
      } else {
        baseFilter[this.config.granularityField] = { $exists: false }
      }

      // Get count
      const count = await collection.count(baseFilter)
      totalRecords += count

      // Get oldest timestamp
      const oldest = await collection.find(baseFilter, {
        limit: 1,
        sort: { [this.config.timestampField]: 1 },
      })
      const oldestTimestamp = oldest.length > 0
        ? new Date((oldest[0] as Record<string, unknown>)[this.config.timestampField] as string | Date)
        : null

      // Count eligible for deletion
      let eligibleForDeletion = 0
      if (policy && policy.enabled !== false) {
        const cutoffDate = new Date(now.getTime() - policy.maxAgeMs)
        const eligibleFilter = {
          ...baseFilter,
          [this.config.timestampField]: { $lt: cutoffDate },
        }
        eligibleForDeletion = await collection.count(eligibleFilter)
        totalEligibleForDeletion += eligibleForDeletion
      }

      byGranularity[granularity] = {
        count,
        oldestTimestamp,
        eligibleForDeletion,
      }
    }

    return {
      collection: this.config.collection,
      byGranularity,
      totalRecords,
      totalEligibleForDeletion,
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a RetentionManager instance
 *
 * @param db - ParqueDB instance
 * @param config - Configuration options
 * @returns RetentionManager instance
 *
 * @example
 * ```typescript
 * const retentionManager = createRetentionManager(db, {
 *   collection: 'ai_requests',
 *   policies: {
 *     hourly: { maxAgeMs: 7 * 24 * 60 * 60 * 1000 },
 *     daily: { maxAgeMs: 90 * 24 * 60 * 60 * 1000 },
 *   }
 * })
 *
 * const result = await retentionManager.cleanup()
 * ```
 */
export function createRetentionManager(db: ParqueDB, config: RetentionManagerConfig): RetentionManager {
  return new RetentionManager(db, config)
}
