/**
 * Staleness Detection for Materialized Views
 *
 * Tracks and detects when materialized views become stale relative to their
 * source data. Uses event sequence IDs/snapshot IDs to determine if source
 * data has changed since the last refresh.
 *
 * Key concepts:
 * - Source Version: Identifies the state of source data (event seq, snapshot ID, timestamp)
 * - Staleness State: fresh | stale | invalid
 * - Staleness Metrics: Detailed information about how stale a view is
 */

import type { ViewName, ViewMetadata, RefreshMode } from './types'

// =============================================================================
// Source Version Types
// =============================================================================

/**
 * Identifies the version/state of source data at a point in time.
 *
 * Different backends use different versioning:
 * - Native: Event sequence IDs
 * - Iceberg: Snapshot IDs
 * - Delta Lake: Transaction log versions
 */
export interface SourceVersion {
  /**
   * Primary version identifier (event seq, snapshot ID, or transaction version)
   */
  versionId: string

  /**
   * Timestamp when this version was captured
   */
  timestamp: number

  /**
   * Optional: Last event ID processed (for event-sourced data)
   */
  lastEventId?: string | undefined

  /**
   * Optional: Last event timestamp processed
   */
  lastEventTs?: number | undefined

  /**
   * Backend type for this version
   */
  backend: 'native' | 'iceberg' | 'delta'
}

/**
 * Tracks source versions for all sources of a materialized view
 */
export interface MVLineage {
  /**
   * View name
   */
  viewName?: ViewName | undefined

  /**
   * Source versions at last refresh, keyed by source collection name
   */
  sourceVersions: Map<string, SourceVersion>

  /**
   * Version ID of the MV definition when last refreshed
   * (to detect definition changes that require full refresh)
   */
  definitionVersionId: string

  /**
   * Timestamp of last successful refresh
   */
  lastRefreshTime: number

  /**
   * Duration of last refresh in milliseconds
   */
  lastRefreshDurationMs?: number | undefined

  /**
   * Number of records processed in last refresh
   */
  lastRefreshRecordCount?: number | undefined

  /**
   * Whether last refresh was incremental or full
   */
  lastRefreshType?: 'incremental' | 'full' | undefined

  /**
   * Last event IDs per source for incremental tracking
   */
  lastEventIds?: Map<string, string> | undefined

  /**
   * Source snapshots per source collection
   */
  sourceSnapshots?: Map<string, string> | undefined

  /**
   * Count of events processed in last refresh
   */
  lastEventCount?: number | undefined
}

// =============================================================================
// Staleness State
// =============================================================================

/**
 * Staleness state of a materialized view
 *
 * - 'fresh': View is up-to-date with source data
 * - 'stale': Source data has changed since last refresh
 * - 'invalid': View definition changed, requires full rebuild
 */
export type StalenessState = 'fresh' | 'stale' | 'invalid'

/**
 * Detailed staleness information for a single source
 */
export interface SourceStaleness {
  /**
   * Source collection name
   */
  source: string

  /**
   * Version at last refresh
   */
  refreshedVersion?: SourceVersion | undefined

  /**
   * Current version of source
   */
  currentVersion?: SourceVersion | undefined

  /**
   * Whether this source has changed
   */
  isStale: boolean

  /**
   * Estimated number of changes since last refresh
   */
  estimatedChanges?: number | undefined

  /**
   * Time since source last changed (milliseconds)
   */
  timeSinceChange?: number | undefined
}

/**
 * Comprehensive staleness metrics for a materialized view
 */
export interface StalenessMetrics {
  /**
   * View name
   */
  viewName: ViewName

  /**
   * Overall staleness state
   */
  state: StalenessState

  /**
   * Whether the view is considered usable
   * (may be stale but within grace period)
   */
  usable: boolean

  /**
   * Time since last refresh (milliseconds)
   */
  timeSinceRefresh: number

  /**
   * Staleness percentage (0-100)
   * Ratio of time since refresh to staleness threshold
   */
  stalenessPercent: number

  /**
   * Whether within configured grace period
   */
  withinGracePeriod: boolean

  /**
   * Time until grace period expires (milliseconds, negative if expired)
   */
  gracePeriodRemainingMs: number

  /**
   * Staleness details for each source
   */
  sources: SourceStaleness[]

  /**
   * Estimated total changes across all sources
   */
  estimatedTotalChanges: number

  /**
   * Reason for staleness (human readable)
   */
  reason?: string | undefined

  /**
   * Recommendation for refresh type
   */
  recommendedRefreshType: 'none' | 'incremental' | 'full'

  /**
   * Timestamp when metrics were computed
   */
  computedAt: number
}

// =============================================================================
// Staleness Threshold Configuration
// =============================================================================

/**
 * Configuration for staleness thresholds
 */
export interface StalenessThresholds {
  /**
   * Maximum time a view can be stale and still be considered usable (milliseconds)
   * After this, queries should fall back to source or fail
   * @default 900000 (15 minutes)
   */
  gracePeriodMs: number

  /**
   * Time after which a view is considered "warning" level stale (milliseconds)
   * Triggers alerts but view is still usable
   * @default 300000 (5 minutes)
   */
  warningThresholdMs: number

  /**
   * Maximum number of source changes before recommending full refresh
   * @default 10000
   */
  maxIncrementalChanges: number

  /**
   * Whether to allow stale reads (with staleness metadata)
   * @default true
   */
  allowStaleReads: boolean

  /**
   * Maximum staleness ratio (time since refresh / expected refresh interval)
   * Beyond this, the view is considered critically stale
   * @default 2.0
   */
  criticalStalenessRatio: number
}

/**
 * Default staleness thresholds
 */
export const DEFAULT_STALENESS_THRESHOLDS: StalenessThresholds = {
  gracePeriodMs: 15 * 60 * 1000, // 15 minutes
  warningThresholdMs: 5 * 60 * 1000, // 5 minutes
  maxIncrementalChanges: 10000,
  allowStaleReads: true,
  criticalStalenessRatio: 2.0,
}

// =============================================================================
// Source Version Provider Interface
// =============================================================================

/**
 * Interface for getting current source versions from different backends
 */
export interface SourceVersionProvider {
  /**
   * Get the current version of a source collection
   */
  getCurrentVersion(source: string): Promise<SourceVersion | null>

  /**
   * Get the number of changes between two versions
   */
  getChangeCount(source: string, fromVersion: SourceVersion, toVersion: SourceVersion): Promise<number>

  /**
   * Check if a version is still valid (not compacted away)
   */
  isVersionValid(source: string, version: SourceVersion): Promise<boolean>
}

// =============================================================================
// StalenessDetector Class
// =============================================================================

/**
 * Detects and tracks staleness for materialized views
 *
 * @example
 * ```typescript
 * const detector = new StalenessDetector(versionProvider, {
 *   gracePeriodMs: 60000, // 1 minute grace period
 * })
 *
 * // Get staleness metrics
 * const metrics = await detector.getMetrics(viewName, lineage, ['orders'])
 *
 * if (metrics.state === 'stale' && !metrics.withinGracePeriod) {
 *   await refreshView(viewName)
 * }
 * ```
 */
export class StalenessDetector {
  private versionProvider: SourceVersionProvider
  private thresholds: StalenessThresholds

  constructor(
    versionProvider: SourceVersionProvider,
    thresholds: Partial<StalenessThresholds> = {}
  ) {
    this.versionProvider = versionProvider
    this.thresholds = { ...DEFAULT_STALENESS_THRESHOLDS, ...thresholds }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Get comprehensive staleness metrics for a view
   */
  async getMetrics(
    viewName: ViewName,
    lineage: MVLineage,
    sources: string[]
  ): Promise<StalenessMetrics> {
    const now = Date.now()
    const timeSinceRefresh = now - lineage.lastRefreshTime

    // Check each source for staleness
    const sourceMetrics: SourceStaleness[] = []
    let estimatedTotalChanges = 0
    let anyStale = false

    for (const source of sources) {
      const metrics = await this.checkSourceStaleness(source, lineage)
      sourceMetrics.push(metrics)

      if (metrics.isStale) {
        anyStale = true
        estimatedTotalChanges += metrics.estimatedChanges ?? 0
      }
    }

    // Calculate staleness state
    const state = this.calculateState(anyStale, timeSinceRefresh)

    // Calculate grace period status
    const withinGracePeriod = timeSinceRefresh <= this.thresholds.gracePeriodMs
    const gracePeriodRemainingMs = this.thresholds.gracePeriodMs - timeSinceRefresh

    // Calculate staleness percentage
    const stalenessPercent = Math.min(100, (timeSinceRefresh / this.thresholds.gracePeriodMs) * 100)

    // Determine usability
    const usable = state === 'fresh' || (state === 'stale' && withinGracePeriod && this.thresholds.allowStaleReads)

    // Determine recommended refresh type
    const recommendedRefreshType = this.getRecommendedRefreshType(
      state,
      estimatedTotalChanges,
      timeSinceRefresh
    )

    // Build reason string
    const reason = this.buildReason(state, anyStale, timeSinceRefresh, sourceMetrics)

    return {
      viewName,
      state,
      usable,
      timeSinceRefresh,
      stalenessPercent,
      withinGracePeriod,
      gracePeriodRemainingMs,
      sources: sourceMetrics,
      estimatedTotalChanges,
      reason,
      recommendedRefreshType,
      computedAt: now,
    }
  }

  /**
   * Quick check if a view is stale (without detailed metrics)
   */
  async isStale(
    lineage: MVLineage,
    sources: string[]
  ): Promise<boolean> {
    for (const source of sources) {
      const refreshedVersion = lineage.sourceVersions.get(source)
      if (!refreshedVersion) {
        return true // No version recorded = stale
      }

      const currentVersion = await this.versionProvider.getCurrentVersion(source)
      if (!currentVersion) {
        continue // Source doesn't exist or can't get version
      }

      if (refreshedVersion.versionId !== currentVersion.versionId) {
        return true
      }
    }

    return false
  }

  /**
   * Check if a view is within its grace period
   */
  isWithinGracePeriod(lineage: MVLineage): boolean {
    const timeSinceRefresh = Date.now() - lineage.lastRefreshTime
    return timeSinceRefresh <= this.thresholds.gracePeriodMs
  }

  /**
   * Check if a view needs immediate refresh
   * (stale and outside grace period)
   */
  async needsImmediateRefresh(
    lineage: MVLineage,
    sources: string[]
  ): Promise<boolean> {
    if (!this.isWithinGracePeriod(lineage)) {
      return await this.isStale(lineage, sources)
    }
    return false
  }

  /**
   * Get the staleness state
   */
  async getState(
    lineage: MVLineage,
    sources: string[]
  ): Promise<StalenessState> {
    const timeSinceRefresh = Date.now() - lineage.lastRefreshTime
    const isStale = await this.isStale(lineage, sources)
    return this.calculateState(isStale, timeSinceRefresh)
  }

  /**
   * Update thresholds
   */
  setThresholds(thresholds: Partial<StalenessThresholds>): void {
    this.thresholds = { ...this.thresholds, ...thresholds }
  }

  /**
   * Get current thresholds
   */
  getThresholds(): StalenessThresholds {
    return { ...this.thresholds }
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Check staleness for a single source
   */
  private async checkSourceStaleness(
    source: string,
    lineage: MVLineage
  ): Promise<SourceStaleness> {
    const refreshedVersion = lineage.sourceVersions.get(source)
    const currentVersion = await this.versionProvider.getCurrentVersion(source)

    // No refreshed version means never refreshed from this source
    if (!refreshedVersion) {
      return {
        source,
        currentVersion: currentVersion ?? undefined,
        isStale: true,
        reason: 'Never refreshed from this source',
      } as SourceStaleness & { reason: string }
    }

    // Can't determine current version
    if (!currentVersion) {
      return {
        source,
        refreshedVersion,
        isStale: false, // Assume fresh if we can't check
      }
    }

    // Compare versions
    const isStale = refreshedVersion.versionId !== currentVersion.versionId

    // Get change count if stale
    let estimatedChanges: number | undefined
    let timeSinceChange: number | undefined

    if (isStale) {
      estimatedChanges = await this.versionProvider.getChangeCount(
        source,
        refreshedVersion,
        currentVersion
      )
      timeSinceChange = Date.now() - currentVersion.timestamp
    }

    return {
      source,
      refreshedVersion,
      currentVersion,
      isStale,
      estimatedChanges,
      timeSinceChange,
    }
  }

  /**
   * Calculate overall staleness state
   */
  private calculateState(isStale: boolean, timeSinceRefresh: number): StalenessState {
    if (!isStale && timeSinceRefresh <= this.thresholds.warningThresholdMs) {
      return 'fresh'
    }

    // Check for critical staleness (possible data corruption or definition change)
    const expectedInterval = this.thresholds.gracePeriodMs / this.thresholds.criticalStalenessRatio
    if (timeSinceRefresh > expectedInterval * this.thresholds.criticalStalenessRatio * 2) {
      return 'invalid' // Extremely stale, may need full rebuild
    }

    return isStale ? 'stale' : 'fresh'
  }

  /**
   * Determine recommended refresh type
   */
  private getRecommendedRefreshType(
    state: StalenessState,
    estimatedChanges: number,
    timeSinceRefresh: number
  ): 'none' | 'incremental' | 'full' {
    if (state === 'fresh') {
      return 'none'
    }

    if (state === 'invalid') {
      return 'full'
    }

    // If too many changes, recommend full refresh
    if (estimatedChanges > this.thresholds.maxIncrementalChanges) {
      return 'full'
    }

    // If been a very long time, recommend full refresh
    if (timeSinceRefresh > this.thresholds.gracePeriodMs * 10) {
      return 'full'
    }

    return 'incremental'
  }

  /**
   * Build human-readable reason string
   */
  private buildReason(
    state: StalenessState,
    anyStale: boolean,
    timeSinceRefresh: number,
    sources: SourceStaleness[]
  ): string {
    if (state === 'fresh') {
      return 'View is fresh'
    }

    if (state === 'invalid') {
      return 'View is critically stale and may require full rebuild'
    }

    const staleSources = sources.filter(s => s.isStale).map(s => s.source)
    const timePart = this.formatDuration(timeSinceRefresh)

    if (staleSources.length > 0) {
      return `Sources changed: ${staleSources.join(', ')} (${timePart} since refresh)`
    }

    return `Time-based staleness: ${timePart} since last refresh`
  }

  /**
   * Format duration for human readability
   */
  private formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`
    if (ms < 60000) return `${Math.round(ms / 1000)}s`
    if (ms < 3600000) return `${Math.round(ms / 60000)}m`
    if (ms < 86400000) return `${Math.round(ms / 3600000)}h`
    return `${Math.round(ms / 86400000)}d`
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a StalenessDetector instance
 */
export function createStalenessDetector(
  versionProvider: SourceVersionProvider,
  thresholds?: Partial<StalenessThresholds>
): StalenessDetector {
  return new StalenessDetector(versionProvider, thresholds)
}

/**
 * Create initial lineage for a new view
 */
export function createInitialLineage(
  viewName: ViewName,
  definitionVersionId: string
): MVLineage {
  return {
    viewName,
    sourceVersions: new Map(),
    definitionVersionId,
    lastRefreshTime: 0,
    lastRefreshDurationMs: 0,
    lastRefreshRecordCount: 0,
    lastRefreshType: 'full',
  }
}

/**
 * Update lineage after a refresh
 */
export function updateLineageAfterRefresh(
  lineage: MVLineage,
  sourceVersions: Map<string, SourceVersion>,
  refreshType: 'incremental' | 'full',
  durationMs: number,
  recordCount: number
): MVLineage {
  // Merge maps without using spread syntax to avoid downlevelIteration issues
  const mergedVersions = new Map<string, SourceVersion>()
  lineage.sourceVersions.forEach((v, k) => mergedVersions.set(k, v))
  sourceVersions.forEach((v, k) => mergedVersions.set(k, v))

  return {
    ...lineage,
    sourceVersions: mergedVersions,
    lastRefreshTime: Date.now(),
    lastRefreshDurationMs: durationMs,
    lastRefreshRecordCount: recordCount,
    lastRefreshType: refreshType,
  }
}

// =============================================================================
// In-Memory Version Provider (for testing)
// =============================================================================

/**
 * Simple in-memory version provider for testing
 */
export class InMemoryVersionProvider implements SourceVersionProvider {
  private versions: Map<string, SourceVersion> = new Map()
  private changeCounts: Map<string, number> = new Map()

  /**
   * Set the current version for a source
   */
  setVersion(source: string, version: SourceVersion): void {
    this.versions.set(source, version)
  }

  /**
   * Set the change count between versions
   */
  setChangeCount(source: string, count: number): void {
    this.changeCounts.set(source, count)
  }

  /**
   * Remove a source
   */
  removeSource(source: string): void {
    this.versions.delete(source)
    this.changeCounts.delete(source)
  }

  /**
   * Clear all data
   */
  clear(): void {
    this.versions.clear()
    this.changeCounts.clear()
  }

  // SourceVersionProvider implementation

  async getCurrentVersion(source: string): Promise<SourceVersion | null> {
    return this.versions.get(source) ?? null
  }

  async getChangeCount(
    source: string,
    _fromVersion: SourceVersion,
    _toVersion: SourceVersion
  ): Promise<number> {
    return this.changeCounts.get(source) ?? 0
  }

  async isVersionValid(_source: string, _version: SourceVersion): Promise<boolean> {
    return true
  }
}

/**
 * Create an InMemoryVersionProvider instance
 */
export function createInMemoryVersionProvider(): InMemoryVersionProvider {
  return new InMemoryVersionProvider()
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a native backend source version
 */
export function createNativeVersion(
  eventSeq: number,
  eventId?: string,
  eventTs?: number
): SourceVersion {
  return {
    versionId: `seq-${eventSeq}`,
    timestamp: Date.now(),
    lastEventId: eventId,
    lastEventTs: eventTs,
    backend: 'native',
  }
}

/**
 * Create an Iceberg backend source version
 */
export function createIcebergVersion(snapshotId: string): SourceVersion {
  return {
    versionId: snapshotId,
    timestamp: Date.now(),
    backend: 'iceberg',
  }
}

/**
 * Create a Delta Lake backend source version
 */
export function createDeltaVersion(transactionVersion: number): SourceVersion {
  return {
    versionId: `txn-${transactionVersion}`,
    timestamp: Date.now(),
    backend: 'delta',
  }
}

/**
 * Parse threshold duration string (e.g., '5m', '1h', '30s')
 */
export function parseThresholdDuration(duration: string): number {
  const match = duration.match(/^(\d+)(ms|s|m|h|d)$/)
  if (!match) {
    throw new Error(`Invalid duration format: ${duration}. Use format like '5m', '1h', '30s'`)
  }

  const value = parseInt(match[1]!, 10)
  const unit = match[2]!

  switch (unit) {
    case 'ms': return value
    case 's': return value * 1000
    case 'm': return value * 60 * 1000
    case 'h': return value * 60 * 60 * 1000
    case 'd': return value * 24 * 60 * 60 * 1000
    default: return value
  }
}

/**
 * Create staleness thresholds from a config object with duration strings
 */
export function createThresholdsFromConfig(config: {
  gracePeriod?: string | undefined
  warningThreshold?: string | undefined
  maxIncrementalChanges?: number | undefined
  allowStaleReads?: boolean | undefined
  criticalStalenessRatio?: number | undefined
}): Partial<StalenessThresholds> {
  return {
    ...(config.gracePeriod && { gracePeriodMs: parseThresholdDuration(config.gracePeriod) }),
    ...(config.warningThreshold && { warningThresholdMs: parseThresholdDuration(config.warningThreshold) }),
    ...(config.maxIncrementalChanges !== undefined && { maxIncrementalChanges: config.maxIncrementalChanges }),
    ...(config.allowStaleReads !== undefined && { allowStaleReads: config.allowStaleReads }),
    ...(config.criticalStalenessRatio !== undefined && { criticalStalenessRatio: config.criticalStalenessRatio }),
  }
}

// =============================================================================
// Lineage Utilities
// =============================================================================

/**
 * Create an empty MVLineage with default values
 */
export function createEmptyLineage(): MVLineage {
  return {
    definitionVersionId: '',
    lastRefreshTime: 0,
    sourceVersions: new Map(),
    lastEventIds: new Map(),
    sourceSnapshots: new Map(),
    lastEventCount: 0,
  }
}

/**
 * Serialize an MVLineage to a JSON string for storage
 */
export function serializeLineage(lineage: MVLineage): string {
  return JSON.stringify({
    viewName: lineage.viewName,
    definitionVersionId: lineage.definitionVersionId,
    lastRefreshTime: lineage.lastRefreshTime,
    lastRefreshDurationMs: lineage.lastRefreshDurationMs,
    lastRefreshRecordCount: lineage.lastRefreshRecordCount,
    lastRefreshType: lineage.lastRefreshType,
    sourceVersions: Array.from(lineage.sourceVersions.entries()),
    lastEventIds: lineage.lastEventIds ? Array.from(lineage.lastEventIds.entries()) : [],
    sourceSnapshots: lineage.sourceSnapshots ? Array.from(lineage.sourceSnapshots.entries()) : [],
    lastEventCount: lineage.lastEventCount,
  })
}

/**
 * Deserialize an MVLineage from a JSON string
 */
export function deserializeLineage(data: string): MVLineage {
  const parsed = JSON.parse(data)
  return {
    viewName: parsed.viewName,
    definitionVersionId: parsed.definitionVersionId ?? '',
    lastRefreshTime: parsed.lastRefreshTime ?? 0,
    lastRefreshDurationMs: parsed.lastRefreshDurationMs,
    lastRefreshRecordCount: parsed.lastRefreshRecordCount,
    lastRefreshType: parsed.lastRefreshType,
    sourceVersions: new Map(parsed.sourceVersions ?? []),
    lastEventIds: new Map(parsed.lastEventIds ?? []),
    sourceSnapshots: new Map(parsed.sourceSnapshots ?? []),
    lastEventCount: parsed.lastEventCount ?? 0,
  }
}
