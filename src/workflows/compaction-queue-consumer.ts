/**
 * Compaction Queue Consumer
 *
 * Receives R2 event notifications and batches them into time windows.
 * Tracks writers and triggers compaction workflows when windows are complete.
 *
 * Architecture:
 * ```
 * R2 Write → Event Notification → Queue → This Consumer
 *                                              ↓
 *                                    Track writers per window
 *                                              ↓
 *                                    Window complete? → Workflow
 * ```
 *
 * Writer-Aware Batching:
 * - Groups files by time window (e.g., 1 hour)
 * - Tracks all writers that have written in each window
 * - Waits for "quorum" or timeout before triggering compaction
 * - Ensures efficient merge-sort (each writer's data is pre-sorted)
 *
 * Namespace Sharding:
 * - Each namespace gets its own CompactionStateDO instance
 * - Eliminates single-DO bottleneck for high-throughput scenarios
 * - Updates are grouped by namespace and dispatched to sharded DOs in parallel
 *
 * Two-Phase Commit:
 * - Windows are marked as "processing" before workflow.create()
 * - On success, windows are marked as "dispatched" with workflow ID
 * - On failure, windows are reset to "pending" for retry
 * - Stuck "processing" windows are cleaned up via timeout
 */

import { DurableObject } from 'cloudflare:workers'
import { logger } from '../utils/logger'
import type { BackendType } from '../backends'
import type { CompactionQueueEnv as Env } from './types'
import {
  emitCompactionMetrics,
  type CompactionMetrics,
} from '../observability/compaction'
import {
  BackpressureManager,
  createDefaultBackpressureManager,
  type BackpressureConfig,
} from './backpressure'
import { asWorkflowBinding } from '../types/cast'

// =============================================================================
// Types
// =============================================================================

/** Namespace priority levels: 0 (critical) to 3 (background) */
export type NamespacePriority = 0 | 1 | 2 | 3

/** Backpressure levels */
export type BackpressureLevel = 'none' | 'normal' | 'severe'

/** Priority-specific max wait times in milliseconds */
export const PRIORITY_WAIT_TIMES: Record<NamespacePriority, number> = {
  0: 1 * 60 * 1000,    // P0 (critical): 1 minute
  1: 5 * 60 * 1000,    // P1 (high): 5 minutes
  2: 15 * 60 * 1000,   // P2 (medium): 15 minutes
  3: 60 * 60 * 1000,   // P3 (background): 1 hour
}

/** Backpressure thresholds */
export const BACKPRESSURE_THRESHOLD = 10  // Windows pending before backpressure kicks in
export const SEVERE_BACKPRESSURE_THRESHOLD = 20  // Windows pending before severe backpressure

/** R2 Event Notification message from queue */
export interface R2EventMessage {
  account: string
  bucket: string
  object: {
    key: string
    size: number
    eTag: string
  }
  /** R2 uses 'action' field with values like 'PutObject', 'DeleteObject' */
  action: 'PutObject' | 'CopyObject' | 'CompleteMultipartUpload' | 'DeleteObject' | 'LifecycleDeletion'
  eventTime: string
}

/** Configuration for the queue consumer */
export interface CompactionConsumerConfig {
  /** Time window size in milliseconds (default: 1 hour) */
  windowSizeMs?: number | undefined

  /** Minimum files before compaction is considered (default: 10) */
  minFilesToCompact?: number | undefined

  /** Maximum time to wait for late writers (default: 5 minutes) */
  maxWaitTimeMs?: number | undefined

  /** Target format for compacted files */
  targetFormat?: BackendType | undefined

  /** Namespace prefix to watch (default: 'data/') */
  namespacePrefix?: string | undefined

  /**
   * Time after which a writer is considered inactive (default: 30 minutes)
   * Writers that haven't written within this threshold won't delay compaction
   */
  writerInactiveThresholdMs?: number | undefined

  /**
   * Time after which a processing window is considered stuck (default: 5 minutes)
   * Stuck windows will be recovered via /get-stuck-windows endpoint
   */
  processingTimeoutMs?: number | undefined

  /**
   * Age after which bucket DOs can be cleaned up (default: 48 hours)
   * Only applies when time bucket sharding is enabled
   */
  bucketCleanupAgeMs?: number | undefined

  /**
   * Time bucket sharding configuration
   * When enabled, DOs are sharded by namespace + time bucket for extreme concurrency (>1000 writes/sec)
   */
  timeBucketSharding?: TimeBucketShardingConfig | undefined

  /**
   * Backpressure configuration for overload protection
   * Controls rate limiting, circuit breaker, and backpressure detection
   */
  backpressure?: BackpressureConfig | undefined
}

/**
 * Configuration for time bucket sharding
 * Only enable for high-throughput namespaces (>1000 writes/sec)
 */
export interface TimeBucketShardingConfig {
  /** Enable time bucket sharding (default: false for backwards compatibility) */
  enabled: boolean

  /** Namespaces that should use time bucket sharding (empty = all namespaces if enabled) */
  namespacesWithSharding?: string[] | undefined

  /** Time bucket size in milliseconds (default: 1 hour / 3600000) */
  bucketSizeMs?: number | undefined

  /** Maximum age of buckets to query for status aggregation in hours (default: 24) */
  maxBucketAgeHours?: number | undefined
}

/**
 * Check if a namespace should use time bucket sharding
 * @param namespace - Namespace name
 * @param config - Time bucket sharding configuration
 * @returns True if time bucket sharding should be used
 */
export function shouldUseTimeBucketSharding(
  namespace: string,
  config?: TimeBucketShardingConfig
): boolean {
  if (!config?.enabled) return false

  // If no specific namespaces are configured, enable for all
  if (!config.namespacesWithSharding || config.namespacesWithSharding.length === 0) {
    return true
  }

  return config.namespacesWithSharding.includes(namespace)
}

/** Processing status for two-phase commit */
type WindowProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number; provisionalWorkflowId: string }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

// =============================================================================
// State Machine Definition
// =============================================================================

/** Window state names for state machine */
export type WindowStateName = 'pending' | 'processing' | 'dispatched' | 'deleted'

/**
 * Window Processing State Machine
 *
 * Defines all valid state transitions for compaction window processing.
 * This ensures consistency and prevents invalid state changes.
 *
 * State Diagram:
 * ```
 *                    ┌─────────────────────────────────────┐
 *                    │                                     │
 *                    ▼                                     │
 *   [NEW] ──► (pending) ──► (processing) ──► (dispatched) ─┴─► [DELETED]
 *                 ▲              │                │
 *                 │              │                │
 *                 │   rollback/  │    failure     │
 *                 │    timeout   │                │
 *                 └──────────────┴────────────────┘
 * ```
 *
 * Transitions:
 * - pending → processing: Window is ready, picked up for workflow dispatch
 * - processing → dispatched: Workflow successfully created
 * - processing → pending: Workflow creation failed (rollback) or stuck timeout
 * - dispatched → deleted: Workflow completed successfully
 * - dispatched → pending: Workflow failed, retry
 */
export const WINDOW_STATE_TRANSITIONS: Record<WindowStateName, WindowStateName[]> = {
  pending: ['processing'],
  processing: ['dispatched', 'pending'],
  dispatched: ['deleted', 'pending'],
  deleted: [], // Terminal state, no transitions out
}

/**
 * Descriptions of each state transition for logging and documentation
 */
export const TRANSITION_DESCRIPTIONS: Record<string, string> = {
  'pending→processing': 'Window ready for compaction, starting workflow dispatch',
  'processing→dispatched': 'Workflow successfully created, awaiting completion',
  'processing→pending': 'Workflow dispatch failed or timed out, will retry',
  'dispatched→deleted': 'Workflow completed successfully, window cleaned up',
  'dispatched→pending': 'Workflow failed, resetting for retry',
}

/**
 * Error class for invalid state transitions
 */
export class InvalidStateTransitionError extends Error {
  constructor(
    public readonly fromState: WindowStateName,
    public readonly toState: WindowStateName,
    public readonly windowKey: string,
    public readonly reason?: string
  ) {
    const validTransitions = WINDOW_STATE_TRANSITIONS[fromState]
    const message = reason
      ? `Invalid state transition for window '${windowKey}': ${fromState} → ${toState}. ${reason}. Valid transitions from '${fromState}': [${validTransitions.join(', ')}]`
      : `Invalid state transition for window '${windowKey}': ${fromState} → ${toState}. Valid transitions from '${fromState}': [${validTransitions.join(', ')}]`
    super(message)
    this.name = 'InvalidStateTransitionError'
  }
}

/**
 * Validate that a state transition is allowed
 * @param fromState - Current window state
 * @param toState - Target window state
 * @returns true if the transition is valid
 */
export function isValidStateTransition(
  fromState: WindowStateName,
  toState: WindowStateName
): boolean {
  const validTransitions = WINDOW_STATE_TRANSITIONS[fromState]
  return validTransitions.includes(toState)
}

/**
 * Validate a state transition and throw if invalid
 * @param fromState - Current window state
 * @param toState - Target window state
 * @param windowKey - Window identifier for error messages
 * @param reason - Optional additional context for error message
 * @throws InvalidStateTransitionError if transition is invalid
 */
export function validateStateTransition(
  fromState: WindowStateName,
  toState: WindowStateName,
  windowKey: string,
  reason?: string
): void {
  if (!isValidStateTransition(fromState, toState)) {
    throw new InvalidStateTransitionError(fromState, toState, windowKey, reason)
  }
}

/**
 * Get the state name from a WindowProcessingStatus
 * @param status - The processing status object
 * @returns The state name
 */
export function getStateName(status: WindowProcessingStatus): WindowStateName {
  return status.state
}

/**
 * Get the transition description for logging
 * @param fromState - Current window state
 * @param toState - Target window state
 * @returns Description of the transition
 */
export function getTransitionDescription(
  fromState: WindowStateName,
  toState: WindowStateName
): string {
  const key = `${fromState}→${toState}`
  return TRANSITION_DESCRIPTIONS[key] ?? `${fromState} → ${toState}`
}

/** Tracked state for a time window */
interface WindowState {
  /** Window start timestamp */
  windowStart: number

  /** Window end timestamp */
  windowEnd: number

  /** Files in this window, grouped by writer */
  filesByWriter: Map<string, string[]>

  /** All writers seen in this window */
  writers: Set<string>

  /** Last time we received a file for this window */
  lastActivityAt: number

  /** Total size of files in this window */
  totalSize: number

  /** Processing status for two-phase commit (default: pending) */
  processingStatus: WindowProcessingStatus
}

// =============================================================================
// Constants
// =============================================================================

/** Default window size: 1 hour */
const DEFAULT_WINDOW_SIZE_MS = 60 * 60 * 1000

/** Default minimum files to trigger compaction */
const DEFAULT_MIN_FILES = 10

/** Default max wait time for late writers: 5 minutes */
const DEFAULT_MAX_WAIT_MS = 5 * 60 * 1000

/** Default time after which a writer is considered inactive: 30 minutes */
const DEFAULT_WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000

/** Default time after which a processing window is considered stuck: 5 minutes */
const DEFAULT_PROCESSING_TIMEOUT_MS = 5 * 60 * 1000

/** Time bucket size for DO sharding: 1 hour */
const TIME_BUCKET_SIZE_MS = 60 * 60 * 1000

/** Maximum age of time buckets to query for status aggregation: 24 hours */
const MAX_BUCKET_AGE_HOURS = 24

/** Default age after which bucket DOs can be cleaned up: 48 hours */
const DEFAULT_BUCKET_CLEANUP_AGE_MS = 48 * 60 * 60 * 1000

// =============================================================================
// Time Bucket Sharding Helpers
// =============================================================================

/**
 * Calculate time bucket for a given timestamp
 * @param timestamp - Unix timestamp in milliseconds
 * @param bucketSizeMs - Bucket size in milliseconds (default: 1 hour)
 * @returns Time bucket as a number (floor of timestamp / bucket size)
 */
export function calculateTimeBucket(timestamp: number, bucketSizeMs: number = TIME_BUCKET_SIZE_MS): number {
  return Math.floor(timestamp / bucketSizeMs)
}

/**
 * Get DO ID for a namespace with optional time bucket sharding
 * @param namespace - Namespace name
 * @param timeBucket - Optional time bucket for sharding
 * @param useTimeBucketSharding - Whether to use time bucket sharding
 * @returns DO ID string
 */
export function getCompactionStateDOId(
  namespace: string,
  timeBucket?: number,
  useTimeBucketSharding: boolean = false
): string {
  if (useTimeBucketSharding && timeBucket !== undefined) {
    return `${namespace}:${timeBucket}`
  }
  return namespace
}

/**
 * Parse DO ID to extract namespace and optional time bucket
 * @param doId - DO ID string
 * @returns Object with namespace and optional timeBucket
 */
export function parseCompactionStateDOId(doId: string): { namespace: string; timeBucket?: number | undefined } {
  const colonIndex = doId.lastIndexOf(':')
  if (colonIndex !== -1) {
    const possibleBucket = doId.slice(colonIndex + 1)
    const bucket = parseInt(possibleBucket, 10)
    if (!isNaN(bucket)) {
      return {
        namespace: doId.slice(0, colonIndex),
        timeBucket: bucket,
      }
    }
  }
  return { namespace: doId }
}

/**
 * Get list of time buckets to query for status aggregation
 * @param now - Current timestamp in milliseconds
 * @param maxAgeHours - Maximum age of buckets to include
 * @param bucketSizeMs - Bucket size in milliseconds
 * @returns Array of time bucket numbers
 */
export function getRecentTimeBuckets(
  now: number = Date.now(),
  maxAgeHours: number = MAX_BUCKET_AGE_HOURS,
  bucketSizeMs: number = TIME_BUCKET_SIZE_MS
): number[] {
  const currentBucket = calculateTimeBucket(now, bucketSizeMs)
  const bucketsToInclude = Math.ceil((maxAgeHours * 60 * 60 * 1000) / bucketSizeMs)
  const buckets: number[] = []

  for (let i = 0; i <= bucketsToInclude; i++) {
    buckets.push(currentBucket - i)
  }

  return buckets
}

/**
 * Check if a time bucket is old enough for cleanup
 * @param timeBucket - Time bucket number
 * @param now - Current timestamp in milliseconds
 * @param bucketSizeMs - Bucket size in milliseconds
 * @param cleanupAgeMs - Age after which buckets can be cleaned up
 * @returns True if bucket is eligible for cleanup
 */
export function isTimeBucketExpired(
  timeBucket: number,
  now: number = Date.now(),
  bucketSizeMs: number = TIME_BUCKET_SIZE_MS,
  cleanupAgeMs: number = DEFAULT_BUCKET_CLEANUP_AGE_MS
): boolean {
  const bucketEndTimestamp = (timeBucket + 1) * bucketSizeMs
  return (now - bucketEndTimestamp) > cleanupAgeMs
}

/**
 * Group updates by namespace and time bucket for sharded dispatch
 * @param updates - Array of file updates
 * @param useTimeBucketSharding - Whether to use time bucket sharding
 * @param bucketSizeMs - Bucket size in milliseconds
 * @returns Map of DO IDs to their updates
 */
export function groupUpdatesByDOId(
  updates: Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>,
  useTimeBucketSharding: boolean = false,
  bucketSizeMs: number = TIME_BUCKET_SIZE_MS
): Map<string, Array<{
  namespace: string
  writerId: string
  file: string
  timestamp: number
  size: number
}>> {
  const grouped = new Map<string, Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>>()

  for (const update of updates) {
    const timeBucket = useTimeBucketSharding
      ? calculateTimeBucket(update.timestamp, bucketSizeMs)
      : undefined
    const doId = getCompactionStateDOId(update.namespace, timeBucket, useTimeBucketSharding)

    const existing = grouped.get(doId) ?? []
    existing.push(update)
    grouped.set(doId, existing)
  }

  return grouped
}

// =============================================================================
// Queue Consumer Handler
// =============================================================================

/** Window entry structure */
export interface WindowReadyEntry {
  namespace: string
  windowKey: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
  /** Namespace priority for workflow queue routing */
  priority?: NamespacePriority | undefined
  /** Provisional workflow ID generated before workflow.create() for crash recovery */
  provisionalWorkflowId: string
}

/** Response from CompactionStateDO */
export interface WindowsReadyResponse {
  windowsReady: WindowReadyEntry[]
}

/** Status response from CompactionStateDO /status endpoint */
export interface CompactionStatusResponse {
  namespace: string
  activeWindows: number
  knownWriters: string[]
  activeWriters: string[]
  oldestWindowAge: number
  totalPendingFiles: number
  windowsStuckInProcessing: number
  windows: Array<{
    key: string
    windowStart: string
    windowEnd: string
    writers: string[]
    fileCount: number
    totalSize: number
    processingStatus: { state: 'pending' } | { state: 'processing'; startedAt: number; provisionalWorkflowId: string } | { state: 'dispatched'; workflowId: string; dispatchedAt: number }
  }>
}

/** Configuration for compaction health checks */
export interface CompactionHealthConfig {
  /** Maximum pending windows before degraded (default: 10) */
  maxPendingWindows: number
  /** Maximum window age in hours before degraded (default: 2) */
  maxWindowAgeHours: number
}

/** Health status for a single namespace */
export interface NamespaceHealth {
  namespace: string
  status: 'healthy' | 'degraded' | 'unhealthy'
  metrics: {
    activeWindows: number
    oldestWindowAge: number
    totalPendingFiles: number
    windowsStuckInProcessing: number
  }
  issues: string[]
}

/** Health check response for /compaction/health endpoint */
export interface CompactionHealthResponse {
  status: 'healthy' | 'degraded' | 'unhealthy'
  namespaces: Record<string, NamespaceHealth>
  alerts: string[]
}

/** Default health check configuration */
export const DEFAULT_COMPACTION_HEALTH_CONFIG: CompactionHealthConfig = {
  maxPendingWindows: 10,
  maxWindowAgeHours: 2,
}

/**
 * Evaluate health for a single namespace based on status response
 */
export function evaluateNamespaceHealth(
  namespace: string,
  statusData: Pick<CompactionStatusResponse, 'activeWindows' | 'oldestWindowAge' | 'totalPendingFiles' | 'windowsStuckInProcessing'>,
  config: CompactionHealthConfig
): NamespaceHealth {
  const issues: string[] = []
  let status: 'healthy' | 'degraded' | 'unhealthy' = 'healthy'

  // Check for windows stuck in processing (always unhealthy)
  if (statusData.windowsStuckInProcessing > 0) {
    issues.push(`${statusData.windowsStuckInProcessing} window(s) stuck in processing`)
    status = 'unhealthy'
  }

  // Check for too many pending windows
  if (statusData.activeWindows > config.maxPendingWindows) {
    issues.push(`${statusData.activeWindows} windows pending (threshold: ${config.maxPendingWindows})`)
    if (status !== 'unhealthy') status = 'degraded'
  }

  // Check for oldest window age
  const maxAgeMs = config.maxWindowAgeHours * 60 * 60 * 1000
  if (statusData.oldestWindowAge > maxAgeMs) {
    const ageHours = Math.round(statusData.oldestWindowAge / (60 * 60 * 1000) * 10) / 10
    issues.push(`oldest window age: ${ageHours}h (threshold: ${config.maxWindowAgeHours}h)`)
    if (status !== 'unhealthy') status = 'degraded'
  }

  return {
    namespace,
    status,
    metrics: {
      activeWindows: statusData.activeWindows,
      oldestWindowAge: statusData.oldestWindowAge,
      totalPendingFiles: statusData.totalPendingFiles,
      windowsStuckInProcessing: statusData.windowsStuckInProcessing,
    },
    issues,
  }
}

/**
 * Aggregate health status across multiple namespaces
 */
export function aggregateHealthStatus(
  namespaces: Record<string, NamespaceHealth>
): CompactionHealthResponse {
  const alerts: string[] = []
  let overallStatus: 'healthy' | 'degraded' | 'unhealthy' = 'healthy'

  for (const [ns, health] of Object.entries(namespaces)) {
    if (health.status === 'unhealthy') {
      overallStatus = 'unhealthy'
    } else if (health.status === 'degraded' && overallStatus !== 'unhealthy') {
      overallStatus = 'degraded'
    }

    for (const issue of health.issues) {
      alerts.push(`${ns}: ${issue}`)
    }
  }

  return {
    status: overallStatus,
    namespaces,
    alerts,
  }
}

/**
 * Aggregated status response for time-bucket sharded namespaces
 * Combines status from multiple bucket DOs into a single namespace view
 */
export interface AggregatedCompactionStatusResponse {
  namespace: string
  /** Number of time bucket DOs queried */
  bucketDOsQueried: number
  /** Number of time bucket DOs that responded successfully */
  bucketDOsResponded: number
  /** Combined active windows across all buckets */
  activeWindows: number
  knownWriters: string[]
  activeWriters: string[]
  oldestWindowAge: number
  totalPendingFiles: number
  windowsStuckInProcessing: number
  /** Windows from all buckets */
  windows: Array<{
    bucketDoId: string
    key: string
    windowStart: string
    windowEnd: string
    writers: string[]
    fileCount: number
    totalSize: number
    processingStatus: { state: 'pending' } | { state: 'processing'; startedAt: number; provisionalWorkflowId: string } | { state: 'dispatched'; workflowId: string; dispatchedAt: number }
  }>
}

/**
 * Query compaction status for a namespace across all relevant time bucket DOs
 * This is used when time bucket sharding is enabled to aggregate status from multiple DOs
 *
 * @param namespace - Namespace to query status for
 * @param env - Environment with COMPACTION_STATE binding
 * @param config - Time bucket sharding configuration
 * @returns Aggregated status from all relevant bucket DOs
 */
export async function getAggregatedCompactionStatus(
  namespace: string,
  env: { COMPACTION_STATE: DurableObjectNamespace },
  config?: TimeBucketShardingConfig
): Promise<AggregatedCompactionStatusResponse> {
  const useSharding = shouldUseTimeBucketSharding(namespace, config)

  if (!useSharding) {
    // For non-sharded namespaces, query single DO via RPC
    const stateId = env.COMPACTION_STATE.idFromName(namespace)
    const stateDO = env.COMPACTION_STATE.get(stateId) as unknown as CompactionStateDO
    const data = await stateDO.getCompactionStatus()

    return {
      namespace: data.namespace,
      bucketDOsQueried: 1,
      bucketDOsResponded: 1,
      activeWindows: data.activeWindows,
      knownWriters: data.knownWriters,
      activeWriters: data.activeWriters,
      oldestWindowAge: data.oldestWindowAge,
      totalPendingFiles: data.totalPendingFiles,
      windowsStuckInProcessing: data.windowsStuckInProcessing,
      windows: data.windows.map(w => ({ ...w, bucketDoId: namespace })),
    }
  }

  // For sharded namespaces, query all recent time buckets
  const bucketSizeMs = config?.bucketSizeMs ?? TIME_BUCKET_SIZE_MS
  const maxAgeHours = config?.maxBucketAgeHours ?? MAX_BUCKET_AGE_HOURS
  const timeBuckets = getRecentTimeBuckets(Date.now(), maxAgeHours, bucketSizeMs)

  const results: Array<{ doId: string; status: CompactionStatusResponse | null }> = []

  // Query all bucket DOs in parallel
  await Promise.all(
    timeBuckets.map(async (bucket) => {
      const doId = getCompactionStateDOId(namespace, bucket, true)
      const stateId = env.COMPACTION_STATE.idFromName(doId)
      const stateDO = env.COMPACTION_STATE.get(stateId)

      try {
        const data = await (stateDO as unknown as CompactionStateDO).getCompactionStatus()
        results.push({ doId, status: data })
      } catch {
        // DO might not exist yet if no data was written in that bucket
        results.push({ doId, status: null })
      }
    })
  )

  // Aggregate results
  const aggregated: AggregatedCompactionStatusResponse = {
    namespace,
    bucketDOsQueried: timeBuckets.length,
    bucketDOsResponded: 0,
    activeWindows: 0,
    knownWriters: [],
    activeWriters: [],
    oldestWindowAge: 0,
    totalPendingFiles: 0,
    windowsStuckInProcessing: 0,
    windows: [],
  }

  const allKnownWriters = new Set<string>()
  const allActiveWriters = new Set<string>()

  for (const { doId, status } of results) {
    if (!status) continue

    aggregated.bucketDOsResponded++
    aggregated.activeWindows += status.activeWindows
    aggregated.totalPendingFiles += status.totalPendingFiles
    aggregated.windowsStuckInProcessing += status.windowsStuckInProcessing

    if (status.oldestWindowAge > aggregated.oldestWindowAge) {
      aggregated.oldestWindowAge = status.oldestWindowAge
    }

    for (const writer of status.knownWriters) {
      allKnownWriters.add(writer)
    }
    for (const writer of status.activeWriters) {
      allActiveWriters.add(writer)
    }

    for (const window of status.windows) {
      aggregated.windows.push({ ...window, bucketDoId: doId })
    }
  }

  aggregated.knownWriters = Array.from(allKnownWriters)
  aggregated.activeWriters = Array.from(allActiveWriters)

  return aggregated
}

/**
 * Type guard for CompactionStatusResponse
 */
export function isCompactionStatusResponse(data: unknown): data is CompactionStatusResponse {
  if (typeof data !== 'object' || data === null) {
    return false
  }
  const d = data as Record<string, unknown>
  return (
    typeof d.namespace === 'string' &&
    typeof d.activeWindows === 'number' &&
    typeof d.oldestWindowAge === 'number' &&
    typeof d.totalPendingFiles === 'number' &&
    typeof d.windowsStuckInProcessing === 'number'
  )
}

/**
 * Type guard for WindowsReadyResponse
 */
export function isWindowsReadyResponse(data: unknown): data is WindowsReadyResponse {
  if (typeof data !== 'object' || data === null) {
    return false
  }
  if (!('windowsReady' in data)) {
    return false
  }
  const { windowsReady } = data as { windowsReady: unknown }
  if (!Array.isArray(windowsReady)) {
    return false
  }
  // Validate each entry has required fields
  for (const entry of windowsReady) {
    if (typeof entry !== 'object' || entry === null) {
      return false
    }
    const e = entry as Record<string, unknown>
    if (
      typeof e.namespace !== 'string' ||
      typeof e.windowKey !== 'string' ||
      typeof e.windowStart !== 'number' ||
      typeof e.windowEnd !== 'number' ||
      !Array.isArray(e.files) ||
      !Array.isArray(e.writers)
    ) {
      return false
    }
  }
  return true
}

/**
 * Handle a batch of R2 event notifications
 *
 * ARCHITECTURE: Namespace Sharding + Time Bucket Sharding
 * Each namespace gets its own CompactionStateDO instance for scalability.
 * For high-throughput namespaces (>1000 writes/sec), time bucket sharding
 * further distributes load: idFromName(namespace + ':' + timeBucket).
 *
 * Updates are grouped by DO ID (namespace or namespace:timeBucket) and sent
 * to the appropriate DOs in parallel.
 *
 * TWO-PHASE COMMIT:
 * 1. DO marks windows as "processing" and returns them
 * 2. Queue consumer attempts workflow.create()
 * 3. On success: DO marks window as "dispatched" with workflow ID
 * 4. On failure: DO resets window to "pending" for retry
 */
export async function handleCompactionQueue(
  batch: MessageBatch<R2EventMessage>,
  env: Env,
  config: CompactionConsumerConfig = {}
): Promise<void> {
  logger.debug('Received batch', {
    messageCount: batch.messages.length,
    queue: batch.queue,
  })

  const {
    windowSizeMs = DEFAULT_WINDOW_SIZE_MS,
    minFilesToCompact = DEFAULT_MIN_FILES,
    maxWaitTimeMs = DEFAULT_MAX_WAIT_MS,
    targetFormat = 'native',
    namespacePrefix = 'data/',
    writerInactiveThresholdMs = DEFAULT_WRITER_INACTIVE_THRESHOLD_MS,
    processingTimeoutMs = DEFAULT_PROCESSING_TIMEOUT_MS,
    bucketCleanupAgeMs = DEFAULT_BUCKET_CLEANUP_AGE_MS,
    timeBucketSharding,
    backpressure: backpressureConfig,
  } = config

  // Initialize backpressure manager for overload protection
  // Provides rate limiting, circuit breaker, and backpressure detection
  const backpressureManager = backpressureConfig
    ? new BackpressureManager(backpressureConfig)
    : createDefaultBackpressureManager()

  // Collect all updates first
  const allUpdates: Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }> = []

  for (const message of batch.messages) {
    const event = message.body
    logger.debug('Event', { event })

    // Only process object-create events for Parquet files
    // R2 uses 'action' field: PutObject, CopyObject, CompleteMultipartUpload for creates
    const isCreateAction = event.action === 'PutObject' || event.action === 'CopyObject' || event.action === 'CompleteMultipartUpload'
    if (!isCreateAction) {
      logger.debug('Skipping non-create action', { action: event.action })
      continue
    }
    if (!event.object.key.endsWith('.parquet')) {
      logger.debug('Skipping non-parquet file', { key: event.object.key })
      continue
    }
    if (!event.object.key.startsWith(namespacePrefix)) {
      logger.debug('Skipping file outside prefix', { key: event.object.key, prefix: namespacePrefix })
      continue
    }

    // Parse file info: data/{namespace}/{timestamp}-{writerId}-{seq}.parquet
    const keyWithoutPrefix = event.object.key.slice(namespacePrefix.length)
    const parts = keyWithoutPrefix.split('/')
    const namespace = parts.slice(0, -1).join('/')
    const filename = parts[parts.length - 1] ?? ''

    logger.debug('Parsed', { namespace, filename })

    const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
    if (!match) {
      logger.debug('Skipping file with unexpected format', { key: event.object.key, filename })
      message.ack()
      continue
    }

    const [, timestampStr, writerId] = match
    // Filename timestamps are in seconds, convert to milliseconds
    const timestamp = parseInt(timestampStr ?? '0', 10) * 1000

    allUpdates.push({
      namespace,
      writerId: writerId ?? 'unknown',
      file: event.object.key,
      timestamp,
      size: event.object.size,
    })

    message.ack()
  }

  if (allUpdates.length === 0) {
    logger.debug('No matching updates to process')
    return
  }

  // Group updates by DO ID (with optional time bucket sharding)
  // For namespaces with time bucket sharding enabled, group by namespace:timeBucket
  const bucketSizeMs = timeBucketSharding?.bucketSizeMs ?? TIME_BUCKET_SIZE_MS
  const updatesByDOId = new Map<string, Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>>()

  for (const update of allUpdates) {
    const useSharding = shouldUseTimeBucketSharding(update.namespace, timeBucketSharding)
    const timeBucket = useSharding ? calculateTimeBucket(update.timestamp, bucketSizeMs) : undefined
    const doId = getCompactionStateDOId(update.namespace, timeBucket, useSharding)

    const existing = updatesByDOId.get(doId) ?? []
    existing.push(update)
    updatesByDOId.set(doId, existing)
  }

  logger.info('Processing updates', {
    doCount: updatesByDOId.size,
    totalUpdates: allUpdates.length,
    timeBucketShardingEnabled: timeBucketSharding?.enabled ?? false,
  })

  // Send updates to sharded DOs in parallel
  // Phase 1: Mark windows as "processing" and get ready windows
  const allWindowsReady: Array<WindowReadyEntry & { doId: string }> = []

  await Promise.all(
    Array.from(updatesByDOId.entries()).map(async ([doId, updates]) => {
      if (!env.COMPACTION_STATE) {
        logger.error('COMPACTION_STATE binding not available')
        return
      }
      // Get DO instance using the (possibly time-bucket-sharded) ID
      const stateId = env.COMPACTION_STATE.idFromName(doId)
      const stateDO = env.COMPACTION_STATE.get(stateId) as unknown as CompactionStateDO

      // Extract namespace from first update (all updates in this group have same namespace)
      const namespace = updates[0]?.namespace ?? ''

      const data = await stateDO.updateCompaction({
        namespace,
        updates,
        config: {
          windowSizeMs,
          minFilesToCompact,
          maxWaitTimeMs,
          targetFormat,
          writerInactiveThresholdMs,
          processingTimeoutMs,
          bucketCleanupAgeMs,
        },
      })

      // Track doId with each ready window for Phase 2
      for (const window of data.windowsReady) {
        allWindowsReady.push({ ...window, doId })
      }
    })
  )

  // Phase 2: Trigger workflows for ready windows with two-phase commit
  // Use BackpressureManager for rate limiting, circuit breaker, and overload protection
  for (const window of allWindowsReady) {
    if (!env.COMPACTION_STATE) {
      logger.error('COMPACTION_STATE binding not available')
      continue
    }
    // Use the doId that was tracked with the ready window
    const stateId = env.COMPACTION_STATE.idFromName(window.doId)
    const stateDO = env.COMPACTION_STATE.get(stateId) as unknown as CompactionStateDO

    // Check if we can dispatch (rate limit, circuit breaker, backpressure)
    // High-priority namespaces can bypass backpressure but still respect rate limits
    if (!backpressureManager.canDispatch(window.namespace)) {
      const status = backpressureManager.getStatus()
      logger.warn('Workflow dispatch blocked by backpressure manager', {
        namespace: window.namespace,
        doId: window.doId,
        windowKey: window.windowKey,
        rateLimitActive: status.rateLimitActive,
        circuitBreakerOpen: status.circuitBreakerOpen,
        backpressureActive: status.backpressureSignalActive,
      })

      // Rollback to pending state so window can be retried later
      await stateDO.rollbackProcessing({ windowKey: window.windowKey })
      continue
    }

    logger.info('Triggering compaction workflow', {
      namespace: window.namespace,
      doId: window.doId,
      windowKey: window.windowKey,
      windowStart: new Date(window.windowStart).toISOString(),
      files: window.files.length,
      writers: window.writers.length,
    })

    // Execute workflow creation with backpressure protection
    // This handles rate limiting, circuit breaker state, and records success/failure
    const result = await backpressureManager.executeWorkflowCreate(
      () => env.COMPACTION_WORKFLOW.create({
        params: {
          namespace: window.namespace,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: window.files,
          writers: window.writers,
          targetFormat,
          // Include doId so workflow can notify correct DO on completion
          doId: window.doId,
        },
      }),
      window.namespace
    )

    if (result.success && result.result) {
      logger.info('Workflow started', { workflowId: result.result.id, doId: window.doId })

      // Phase 2a: Confirm dispatch success - mark as dispatched
      await stateDO.confirmDispatch({
        windowKey: window.windowKey,
        workflowId: result.result.id,
      })
    } else if (result.skipped) {
      // Skipped due to rate limiting, circuit breaker, or backpressure
      logger.warn('Workflow dispatch skipped', {
        reason: result.reason,
        namespace: window.namespace,
        doId: window.doId,
        windowKey: window.windowKey,
      })

      // Rollback to pending state so window can be retried later
      await stateDO.rollbackProcessing({ windowKey: window.windowKey })
    } else {
      // Workflow creation failed - circuit breaker will track this
      logger.error('Failed to start compaction workflow', {
        error: result.error?.message ?? 'Unknown',
        namespace: window.namespace,
        doId: window.doId,
        windowKey: window.windowKey,
      })

      // Phase 2b: Rollback on failure - reset to pending
      await stateDO.rollbackProcessing({ windowKey: window.windowKey })
    }
  }

  // Log backpressure status at end of batch for observability
  const finalStatus = backpressureManager.getStatus()
  if (finalStatus.rateLimitActive || finalStatus.circuitBreakerOpen || finalStatus.backpressureSignalActive) {
    logger.info('Backpressure manager status after batch', {
      rateLimitActive: finalStatus.rateLimitActive,
      tokensRemaining: finalStatus.tokensRemaining,
      circuitBreakerOpen: finalStatus.circuitBreakerOpen,
      circuitBreakerResetMs: finalStatus.circuitBreakerResetMs,
      consecutiveFailures: finalStatus.consecutiveFailures,
      backpressureActive: finalStatus.backpressureSignalActive,
      dispatchesInWindow: finalStatus.dispatchesInWindow,
    })
  }
}

// =============================================================================
// Stuck Window Recovery
// =============================================================================

/** Stuck window info returned from /get-stuck-windows */
export interface StuckWindowInfo {
  windowKey: string
  provisionalWorkflowId: string
  startedAt: number
  stuckDurationMs: number
}

/** Response from /get-stuck-windows endpoint */
export interface StuckWindowsResponse {
  namespace: string
  stuckWindows: StuckWindowInfo[]
}

/**
 * Recover stuck windows by checking workflow status and either confirming or rolling back
 *
 * This function should be called periodically (e.g., by a cron trigger or at the start
 * of each queue batch) to recover windows that got stuck in "processing" state due to
 * crashes between workflow.create() and confirm-dispatch.
 *
 * The recovery process:
 * 1. Get stuck windows from each DO via /get-stuck-windows
 * 2. Check workflow status using the provisional workflow ID
 * 3. If workflow is running/complete: call /confirm-dispatch (workflow was created successfully)
 * 4. If workflow doesn't exist or errored: call /rollback-processing (safe to retry)
 *
 * @param doIds - Array of DO IDs to check for stuck windows
 * @param env - Environment with COMPACTION_STATE and COMPACTION_WORKFLOW bindings
 */
export async function recoverStuckWindows(
  doIds: string[],
  env: Env
): Promise<{ recovered: number; failed: number }> {
  let recovered = 0
  let failed = 0

  if (!env.COMPACTION_STATE) {
    logger.error('COMPACTION_STATE binding not available')
    return { recovered, failed }
  }

  for (const doId of doIds) {
    try {
      const stateId = env.COMPACTION_STATE.idFromName(doId)
      const stateDO = env.COMPACTION_STATE.get(stateId) as unknown as CompactionStateDO

      // Get stuck windows via RPC
      const data = await stateDO.getStuckWindows()

      for (const stuckWindow of data.stuckWindows) {
        try {
          // Check workflow status using the provisional ID
          let workflowExists = false
          let workflowRunning = false

          try {
            if (!env.COMPACTION_WORKFLOW) {
              throw new Error('COMPACTION_WORKFLOW binding not available')
            }
            // Use type assertion since the Workflow type definition is incomplete
            const workflowBinding = asWorkflowBinding<{
              get(id: string): { status(): Promise<{ status: string }> }
            }>(env.COMPACTION_WORKFLOW)
            const workflow = workflowBinding.get(stuckWindow.provisionalWorkflowId)
            const status = await workflow.status()
            workflowExists = true
            workflowRunning = status.status === 'queued' || status.status === 'running' || status.status === 'complete'
          } catch {
            // Workflow doesn't exist - this means workflow.create() failed or was never called
            workflowExists = false
          }

          if (workflowExists && workflowRunning) {
            // Workflow was created successfully - confirm the dispatch
            logger.info('Recovering stuck window: workflow exists, confirming dispatch', {
              namespace: data.namespace,
              windowKey: stuckWindow.windowKey,
              provisionalWorkflowId: stuckWindow.provisionalWorkflowId,
            })

            await stateDO.confirmDispatch({
              windowKey: stuckWindow.windowKey,
              workflowId: stuckWindow.provisionalWorkflowId,
            })
            recovered++
          } else {
            // Workflow doesn't exist or errored - safe to rollback and retry
            logger.info('Recovering stuck window: workflow does not exist or errored, rolling back', {
              namespace: data.namespace,
              windowKey: stuckWindow.windowKey,
              provisionalWorkflowId: stuckWindow.provisionalWorkflowId,
              workflowExists,
            })

            await stateDO.rollbackProcessing({ windowKey: stuckWindow.windowKey })
            recovered++
          }
        } catch (err) {
          logger.error('Failed to recover stuck window', {
            namespace: data.namespace,
            windowKey: stuckWindow.windowKey,
            error: err instanceof Error ? err.message : 'Unknown',
          })
          failed++
        }
      }
    } catch (err) {
      logger.error('Failed to get stuck windows from DO', {
        doId,
        error: err instanceof Error ? err.message : 'Unknown',
      })
      failed++
    }
  }

  return { recovered, failed }
}

// =============================================================================
// State Durable Object
// =============================================================================

/** Serializable processing status for storage */
type StoredProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number; provisionalWorkflowId: string }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

/** Serializable window state for storage */
interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
  processingStatus?: StoredProcessingStatus | undefined
}

/** Stored state structure (namespace-sharded - each DO handles one namespace) */
interface StoredState {
  /** The namespace this DO instance handles */
  namespace: string
  /** Windows keyed by windowStart timestamp */
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
  /** Namespace priority: 0 (critical) to 3 (background). Default: 2 */
  priority?: NamespacePriority | undefined
}

/**
 * Per-window storage format for 128KB limit fix
 * Metadata is stored in 'metadata' key, each window in 'window:{windowStart}' key
 * This prevents exceeding the 128KB per-key limit in Durable Object storage
 */
interface StoredMetadata {
  namespace: string
  knownWriters: string[]
  writerLastSeen: Record<string, number>
  priority?: NamespacePriority | undefined
}

/**
 * Durable Object for tracking compaction state across queue messages
 *
 * ARCHITECTURE: Namespace Sharding
 * Each instance handles a single namespace (determined by idFromName(namespace)).
 * This eliminates the scalability bottleneck of a single global instance.
 * Windows are keyed by windowStart timestamp since namespace is implicit.
 *
 * TWO-PHASE COMMIT:
 * Windows go through states: pending → processing → dispatched
 * - pending: Ready to be picked up for compaction
 * - processing: workflow.create() in progress (with timeout for stuck windows)
 * - dispatched: Workflow successfully created, window can be deleted after completion
 */
export class CompactionStateDO extends DurableObject {
  private doState: DurableObjectState
  /** The namespace this DO instance handles (set on first update) */
  private namespace: string = ''
  /** Windows keyed by windowStart timestamp */
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()
  private initialized = false
  /** Namespace priority: 0 (critical) to 3 (background). Default: 2 */
  private priority: NamespacePriority = 2
  /** External backpressure level (set by queue consumer) */
  private backpressureLevel: BackpressureLevel = 'none'
  /** Configurable timeout: time after which a writer is considered inactive */
  private writerInactiveThresholdMs: number = DEFAULT_WRITER_INACTIVE_THRESHOLD_MS
  /** Configurable timeout: time after which a processing window is considered stuck */
  private processingTimeoutMs: number = DEFAULT_PROCESSING_TIMEOUT_MS

  constructor(state: DurableObjectState, env: Record<string, unknown>) {
    super(state, env)
    this.doState = state
  }

  /**
   * Load state from storage
   * Supports both new per-window storage format and legacy single-key format
   *
   * Migration Strategy:
   * 1. Try to load from new per-window format ('metadata' + 'window:*' keys)
   * 2. If not found, load from legacy single-key format ('compactionState')
   * 3. After loading from legacy format, migrate to new format and delete legacy key
   *
   * This ensures:
   * - Existing DOs with legacy data are automatically migrated
   * - New DOs start with the per-window format
   * - The 128KB per-key limit is never exceeded
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Try new per-window storage format first (128KB limit fix)
    const metadata = await this.doState.storage.get<StoredMetadata>('metadata')
    if (metadata) {
      this.namespace = metadata.namespace ?? ''
      this.priority = metadata.priority ?? 2
      this.knownWriters = new Set(metadata.knownWriters)
      this.writerLastSeen = new Map(Object.entries(metadata.writerLastSeen))

      // Load windows from per-window keys
      const windowEntries = await this.doState.storage.list({ prefix: 'window:' })
      for (const [key, value] of windowEntries) {
        const sw = value as StoredWindowState
        const windowKey = key.replace('window:', '')
        this.windows.set(windowKey, {
          windowStart: sw.windowStart,
          windowEnd: sw.windowEnd,
          filesByWriter: new Map(Object.entries(sw.filesByWriter)),
          writers: new Set(sw.writers),
          lastActivityAt: sw.lastActivityAt,
          totalSize: sw.totalSize,
          processingStatus: sw.processingStatus ?? { state: 'pending' },
        })
      }
    } else {
      // Fall back to legacy single-key format for backwards compatibility
      const stored = await this.doState.storage.get<StoredState>('compactionState')
      if (stored) {
        logger.info('Migrating from legacy compactionState format to per-window storage', {
          namespace: stored.namespace,
          windowCount: Object.keys(stored.windows).length,
        })

        // Restore namespace
        this.namespace = stored.namespace ?? ''
        // Restore priority
        this.priority = stored.priority ?? 2
        // Restore windows (keyed by windowStart timestamp)
        for (const [key, sw] of Object.entries(stored.windows)) {
          this.windows.set(key, {
            windowStart: sw.windowStart,
            windowEnd: sw.windowEnd,
            filesByWriter: new Map(Object.entries(sw.filesByWriter)),
            writers: new Set(sw.writers),
            lastActivityAt: sw.lastActivityAt,
            totalSize: sw.totalSize,
            processingStatus: sw.processingStatus ?? { state: 'pending' },
          })
        }
        // Restore writers
        this.knownWriters = new Set(stored.knownWriters)
        this.writerLastSeen = new Map(Object.entries(stored.writerLastSeen))

        // Migrate to new format by saving with per-window keys
        // This will create 'metadata' and 'window:*' keys
        await this.saveState()

        // Delete legacy key to complete migration
        await this.doState.storage.delete('compactionState')

        logger.info('Migration to per-window storage complete', {
          namespace: this.namespace,
          windowCount: this.windows.size,
        })
      }
    }

    this.initialized = true
  }

  /**
   * Save state to storage using per-window keys to avoid 128KB limit
   * Each window is stored in its own key: `window:{windowStart}`
   * Metadata (namespace, writers, priority) is stored in 'metadata' key
   *
   * IMPORTANT: This method no longer writes to the legacy 'compactionState' key.
   * Migration from legacy format happens in ensureInitialized() - once migrated,
   * all subsequent writes use only the per-window format.
   */
  private async saveState(): Promise<void> {
    // Save metadata separately (small, fixed size)
    const metadata: StoredMetadata = {
      namespace: this.namespace,
      knownWriters: Array.from(this.knownWriters),
      writerLastSeen: Object.fromEntries(this.writerLastSeen),
      priority: this.priority,
    }
    await this.doState.storage.put('metadata', metadata)

    // Save each window in its own key (prevents 128KB limit issues)
    for (const [key, window] of this.windows) {
      const storedWindow: StoredWindowState = {
        windowStart: window.windowStart,
        windowEnd: window.windowEnd,
        filesByWriter: Object.fromEntries(window.filesByWriter),
        writers: Array.from(window.writers),
        lastActivityAt: window.lastActivityAt,
        totalSize: window.totalSize,
        processingStatus: window.processingStatus,
      }
      await this.doState.storage.put(`window:${key}`, storedWindow)
    }
  }

  // ===========================================================================
  // Priority and Backpressure Helpers
  // ===========================================================================

  /**
   * Get the effective max wait time based on priority
   */
  private getEffectiveMaxWaitTimeMs(): number {
    return PRIORITY_WAIT_TIMES[this.priority]
  }

  /**
   * Check if this namespace should be skipped due to backpressure
   */
  private shouldSkipDueToBackpressure(): boolean {
    // P0 always processes
    if (this.priority === 0) return false

    // P1 processes under normal backpressure, skipped under severe
    if (this.priority === 1) return this.backpressureLevel === 'severe'

    // P2 skipped under severe backpressure
    if (this.priority === 2) return this.backpressureLevel === 'severe'

    // P3 skipped under any backpressure
    return this.backpressureLevel !== 'none'
  }

  /**
   * Calculate current backpressure level based on pending windows
   */
  private calculateBackpressureLevel(): BackpressureLevel {
    let pendingCount = 0
    for (const window of this.windows.values()) {
      if (window.processingStatus.state === 'pending') {
        pendingCount++
      }
    }

    if (pendingCount >= SEVERE_BACKPRESSURE_THRESHOLD) return 'severe'
    if (pendingCount >= BACKPRESSURE_THRESHOLD) return 'normal'
    return 'none'
  }

  // ===========================================================================
  // Public RPC Methods
  // ===========================================================================

  /**
   * Get compaction status (RPC)
   */
  async getCompactionStatus(): Promise<CompactionStatusResponse & {
    priority: NamespacePriority
    effectiveMaxWaitTimeMs: number
    backpressure: BackpressureLevel
    queueMetrics: { pendingWindows: number; processingWindows: number; dispatchedWindows: number }
    health: { status: string; issues: string[] }
  }> {
    await this.ensureInitialized()
    return this.buildStatusResponse()
  }

  /**
   * Update compaction state with new file writes (RPC)
   */
  async updateCompaction(params: {
    namespace: string
    updates: Array<{
      namespace: string
      writerId: string
      file: string
      timestamp: number
      size: number
    }>
    config: {
      windowSizeMs: number
      minFilesToCompact: number
      maxWaitTimeMs: number
      targetFormat: string
      writerInactiveThresholdMs?: number | undefined
      processingTimeoutMs?: number | undefined
      bucketCleanupAgeMs?: number | undefined
    }
  }): Promise<{ windowsReady: WindowReadyEntry[]; skippedDueToBackpressure?: boolean }> {
    await this.ensureInitialized()
    return this.processUpdate(params)
  }

  /**
   * Configure namespace priority (RPC)
   */
  async setConfig(params: { priority?: number | undefined }): Promise<{ success: boolean; priority: NamespacePriority }> {
    await this.ensureInitialized()

    if (params.priority !== undefined) {
      if (typeof params.priority !== 'number' || params.priority < 0 || params.priority > 3) {
        throw new Error('Priority must be 0, 1, 2, or 3')
      }
      this.priority = params.priority as NamespacePriority
    }

    await this.saveState()

    logger.info('Namespace priority configured', {
      namespace: this.namespace,
      priority: this.priority,
    })

    return { success: true, priority: this.priority }
  }

  /**
   * Set external backpressure level (RPC)
   */
  async setBackpressureLevel(level: BackpressureLevel): Promise<{ success: boolean; backpressure: BackpressureLevel }> {
    await this.ensureInitialized()

    if (!['none', 'normal', 'severe'].includes(level)) {
      throw new Error('Invalid backpressure level')
    }

    this.backpressureLevel = level

    logger.info('Backpressure level set', {
      namespace: this.namespace,
      backpressure: this.backpressureLevel,
    })

    return { success: true, backpressure: this.backpressureLevel }
  }

  /**
   * Confirm successful workflow dispatch (RPC)
   */
  async confirmDispatch(params: { windowKey: string; workflowId: string }): Promise<{ success: boolean }> {
    await this.ensureInitialized()
    return this.processConfirmDispatch(params)
  }

  /**
   * Rollback failed workflow dispatch (RPC)
   */
  async rollbackProcessing(params: { windowKey: string }): Promise<{ success: boolean }> {
    await this.ensureInitialized()
    return this.processRollbackProcessing(params)
  }

  /**
   * Notify workflow completion (RPC)
   */
  async workflowComplete(params: { windowKey: string; workflowId: string; success: boolean }): Promise<{ success: boolean; alreadyDeleted?: boolean }> {
    await this.ensureInitialized()
    return this.processWorkflowComplete(params)
  }

  /**
   * Get windows stuck in processing state (RPC)
   */
  async getStuckWindows(): Promise<StuckWindowsResponse> {
    await this.ensureInitialized()
    return this.buildStuckWindowsResponse()
  }

  // ===========================================================================
  // Legacy fetch() handler (kept for backwards compatibility)
  // ===========================================================================

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    if (url.pathname === '/update' && request.method === 'POST') {
      return this.handleUpdate(request)
    }

    if (url.pathname === '/config' && request.method === 'POST') {
      return this.handleConfig(request)
    }

    if (url.pathname === '/set-backpressure' && request.method === 'POST') {
      return this.handleSetBackpressure(request)
    }

    if (url.pathname === '/confirm-dispatch' && request.method === 'POST') {
      return this.handleConfirmDispatch(request)
    }

    if (url.pathname === '/rollback-processing' && request.method === 'POST') {
      return this.handleRollbackProcessing(request)
    }

    if (url.pathname === '/workflow-complete' && request.method === 'POST') {
      return this.handleWorkflowComplete(request)
    }

    if (url.pathname === '/get-stuck-windows' && request.method === 'GET') {
      return this.handleGetStuckWindows()
    }

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  /**
   * Handle /config endpoint - configure namespace priority (legacy fetch)
   */
  private async handleConfig(request: Request): Promise<Response> {
    const body = await request.json() as { priority?: number | undefined }
    try {
      const result = await this.setConfig(body)
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
    } catch (err) {
      return new Response(JSON.stringify({ error: err instanceof Error ? err.message : 'Unknown error' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Handle /set-backpressure endpoint (legacy fetch)
   */
  private async handleSetBackpressure(request: Request): Promise<Response> {
    const body = await request.json() as { level: BackpressureLevel }
    try {
      const result = await this.setBackpressureLevel(body.level)
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
    } catch (err) {
      return new Response(JSON.stringify({ error: err instanceof Error ? err.message : 'Unknown error' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  private async handleUpdate(request: Request): Promise<Response> {
    const body = await request.json() as Parameters<CompactionStateDO['updateCompaction']>[0]
    const result = await this.processUpdate(body)
    return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
  }

  /**
   * Shared update logic used by both RPC and fetch handlers
   */
  private async processUpdate(params: {
    namespace: string
    updates: Array<{
      namespace: string
      writerId: string
      file: string
      timestamp: number
      size: number
    }>
    config: {
      windowSizeMs: number
      minFilesToCompact: number
      maxWaitTimeMs: number
      targetFormat: string
      writerInactiveThresholdMs?: number | undefined
      processingTimeoutMs?: number | undefined
      bucketCleanupAgeMs?: number | undefined
    }
  }): Promise<{ windowsReady: WindowReadyEntry[]; skippedDueToBackpressure?: boolean }> {
    const { namespace, updates, config } = params
    const now = Date.now()
    const windowsReady: WindowReadyEntry[] = []

    // Update configurable timeout values from config (use instance defaults if not provided)
    if (config.writerInactiveThresholdMs !== undefined) {
      this.writerInactiveThresholdMs = config.writerInactiveThresholdMs
    }
    if (config.processingTimeoutMs !== undefined) {
      this.processingTimeoutMs = config.processingTimeoutMs
    }

    // Set namespace on first update (each DO instance handles one namespace)
    if (!this.namespace) {
      this.namespace = namespace
    }

    // Clean up stuck processing windows (timeout recovery)
    this.cleanupStuckProcessingWindows(now)

    // Process updates
    for (const update of updates) {
      const { writerId, file, timestamp, size } = update

      // Track writer
      this.knownWriters.add(writerId)
      this.writerLastSeen.set(writerId, now)

      // Calculate window - key by windowStart only since namespace is implicit
      const windowStart = Math.floor(timestamp / config.windowSizeMs) * config.windowSizeMs
      const windowEnd = windowStart + config.windowSizeMs
      const windowKey = String(windowStart)

      // Get or create window
      let window = this.windows.get(windowKey)
      if (!window) {
        window = {
          windowStart,
          windowEnd,
          filesByWriter: new Map(),
          writers: new Set(),
          lastActivityAt: now,
          totalSize: 0,
          processingStatus: { state: 'pending' },
        }
        this.windows.set(windowKey, window)
      }

      // Only add files to pending windows (don't modify processing/dispatched windows)
      if (window.processingStatus.state === 'pending') {
        const writerFiles = window.filesByWriter.get(writerId) ?? []
        writerFiles.push(file)
        window.filesByWriter.set(writerId, writerFiles)
        window.writers.add(writerId)
        window.lastActivityAt = now
        window.totalSize += size
      }
    }

    // Check if we should skip processing due to backpressure
    const skippedDueToBackpressure = this.shouldSkipDueToBackpressure()
    if (skippedDueToBackpressure) {
      await this.saveState()
      this.emitMetrics(now)
      logger.info('Skipping window processing due to backpressure', {
        namespace: this.namespace,
        priority: this.priority,
        backpressure: this.backpressureLevel,
      })
      return { windowsReady: [], skippedDueToBackpressure: true }
    }

    // Use priority-based max wait time
    const effectiveMaxWaitTimeMs = this.getEffectiveMaxWaitTimeMs()

    // Check for windows ready for compaction (only pending windows)
    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
      // Skip non-pending windows
      if (window.processingStatus.state !== 'pending') continue

      // Skip if window is too recent (still filling) - use priority-based wait time
      if (now < window.windowEnd + effectiveMaxWaitTimeMs) continue

      // Count total files
      let totalFiles = 0
      for (const files of window.filesByWriter.values()) {
        totalFiles += files.length
      }

      // Skip if not enough files
      if (totalFiles < config.minFilesToCompact) continue

      // Check if we've heard from all active writers (or waited long enough)
      const missingWriters = activeWriters.filter(w => !window.writers.has(w))
      const waitedLongEnough = (now - window.lastActivityAt) > effectiveMaxWaitTimeMs

      if (missingWriters.length === 0 || waitedLongEnough) {
        // Window is ready! Generate provisional workflow ID and mark as processing
        // The provisional ID is stored so we can check workflow status on timeout recovery
        const provisionalWorkflowId = `compaction-${this.namespace}-${windowKey}-${now}`
        window.processingStatus = { state: 'processing', startedAt: now, provisionalWorkflowId }

        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace: this.namespace,
          windowKey,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: allFiles.sort(),
          writers: Array.from(window.writers),
          priority: this.priority,
          provisionalWorkflowId,
        })
      }
    }

    // Persist state
    await this.saveState()

    // Emit metrics for observability dashboard
    this.emitMetrics(now)

    return { windowsReady, skippedDueToBackpressure: false }
  }

  /**
   * Collect and emit compaction metrics for monitoring
   * Called after each update to track system health
   */
  private emitMetrics(now: number): void {
    // Count windows by state
    let pendingWindows = 0
    let processingWindows = 0
    let dispatchedWindows = 0
    let stuckWindows = 0
    let totalPendingFiles = 0
    let totalPendingBytes = 0
    let oldestWindowAge = 0

    for (const window of this.windows.values()) {
      const windowAge = now - window.windowEnd
      if (windowAge > oldestWindowAge) {
        oldestWindowAge = windowAge
      }

      switch (window.processingStatus.state) {
        case 'pending':
          pendingWindows++
          for (const files of window.filesByWriter.values()) {
            totalPendingFiles += files.length
          }
          totalPendingBytes += window.totalSize
          break
        case 'processing':
          processingWindows++
          // Check if stuck (using configurable processing timeout)
          if (now - window.processingStatus.startedAt > this.processingTimeoutMs) {
            stuckWindows++
          }
          break
        case 'dispatched':
          dispatchedWindows++
          break
      }
    }

    const metrics: CompactionMetrics = {
      namespace: this.namespace,
      timestamp: now,
      windows_pending: pendingWindows,
      windows_processing: processingWindows,
      windows_dispatched: dispatchedWindows,
      files_pending: totalPendingFiles,
      oldest_window_age_ms: oldestWindowAge,
      known_writers: this.knownWriters.size,
      active_writers: this.getActiveWriters(now).length,
      bytes_pending: totalPendingBytes,
      windows_stuck: stuckWindows,
    }

    emitCompactionMetrics(metrics)
  }

  /**
   * Phase 2a: Confirm successful workflow dispatch (legacy fetch)
   */
  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string }
    try {
      const result = await this.processConfirmDispatch(body)
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
    } catch (err) {
      const status = (err as { status?: number }).status ?? 409
      return new Response(JSON.stringify({ error: err instanceof Error ? err.message : 'Unknown error' }), {
        status,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Phase 2b: Rollback failed workflow dispatch (legacy fetch)
   */
  private async handleRollbackProcessing(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string }
    try {
      const result = await this.processRollbackProcessing(body)
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
    } catch (err) {
      const status = (err as { status?: number }).status ?? 409
      return new Response(JSON.stringify({ error: err instanceof Error ? err.message : 'Unknown error' }), {
        status,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Called by workflow on completion (legacy fetch)
   */
  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string; success: boolean }
    try {
      const result = await this.processWorkflowComplete(body)
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
    } catch (err) {
      const status = (err as { status?: number }).status ?? 409
      return new Response(JSON.stringify({ error: err instanceof Error ? err.message : 'Unknown error' }), {
        status,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Get stuck windows (legacy fetch)
   */
  private handleGetStuckWindows(): Response {
    const result = this.buildStuckWindowsResponse()
    return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } })
  }

  // ===========================================================================
  // Shared Logic Methods (used by both RPC and fetch handlers)
  // ===========================================================================

  private async processConfirmDispatch(params: { windowKey: string; workflowId: string }): Promise<{ success: boolean }> {
    const { windowKey, workflowId } = params
    const window = this.windows.get(windowKey)

    if (!window) {
      throw Object.assign(new Error('Window not found'), { status: 404 })
    }

    const currentState = getStateName(window.processingStatus)
    const targetState: WindowStateName = 'dispatched'

    if (!isValidStateTransition(currentState, targetState)) {
      throw Object.assign(
        new Error(`Invalid state transition: ${currentState} -> ${targetState}. ${getTransitionDescription(currentState, targetState)}`),
        { status: 409 }
      )
    }

    window.processingStatus = {
      state: 'dispatched',
      workflowId,
      dispatchedAt: Date.now(),
    }

    await this.saveState()

    logger.info('Window dispatch confirmed', {
      namespace: this.namespace,
      windowKey,
      workflowId,
      transition: getTransitionDescription(currentState, targetState),
    })

    return { success: true }
  }

  private async processRollbackProcessing(params: { windowKey: string }): Promise<{ success: boolean }> {
    const { windowKey } = params
    const window = this.windows.get(windowKey)

    if (!window) {
      throw Object.assign(new Error('Window not found'), { status: 404 })
    }

    const currentState = getStateName(window.processingStatus)
    const targetState: WindowStateName = 'pending'

    if (!isValidStateTransition(currentState, targetState)) {
      throw Object.assign(
        new Error(`Invalid state transition: ${currentState} -> ${targetState}. ${getTransitionDescription(currentState, targetState)}`),
        { status: 409 }
      )
    }

    window.processingStatus = { state: 'pending' }

    await this.saveState()

    logger.info('Window processing rolled back', {
      namespace: this.namespace,
      windowKey,
      transition: getTransitionDescription(currentState, targetState),
    })

    return { success: true }
  }

  private async processWorkflowComplete(params: { windowKey: string; workflowId: string; success: boolean }): Promise<{ success: boolean; alreadyDeleted?: boolean }> {
    const { windowKey, workflowId, success } = params
    const window = this.windows.get(windowKey)

    if (!window) {
      return { success: true, alreadyDeleted: true }
    }

    const currentState = getStateName(window.processingStatus)
    const targetState: WindowStateName = success ? 'deleted' : 'pending'

    if (!isValidStateTransition(currentState, targetState)) {
      throw Object.assign(
        new Error(`Invalid state transition: ${currentState} -> ${targetState}. ${getTransitionDescription(currentState, targetState)}`),
        { status: 409 }
      )
    }

    if (window.processingStatus.state === 'dispatched' && window.processingStatus.workflowId !== workflowId) {
      throw Object.assign(
        new Error(`Workflow ID mismatch: expected ${window.processingStatus.workflowId}, received ${workflowId}`),
        { status: 409 }
      )
    }

    if (success) {
      this.windows.delete(windowKey)
      await this.doState.storage.delete(`window:${windowKey}`)
      logger.info('Window completed and deleted', {
        namespace: this.namespace,
        windowKey,
        workflowId,
        transition: getTransitionDescription(currentState, targetState),
      })
    } else {
      window.processingStatus = { state: 'pending' }
      logger.warn('Workflow failed, window reset to pending', {
        namespace: this.namespace,
        windowKey,
        workflowId,
        transition: getTransitionDescription(currentState, targetState),
      })
    }

    await this.saveState()

    return { success: true }
  }

  private buildStuckWindowsResponse(): StuckWindowsResponse {
    const now = Date.now()
    const stuckWindows: StuckWindowInfo[] = []

    for (const [windowKey, window] of this.windows) {
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > this.processingTimeoutMs
      ) {
        stuckWindows.push({
          windowKey,
          provisionalWorkflowId: window.processingStatus.provisionalWorkflowId,
          startedAt: window.processingStatus.startedAt,
          stuckDurationMs: now - window.processingStatus.startedAt,
        })
      }
    }

    return { namespace: this.namespace, stuckWindows }
  }

  /**
   * Clean up windows stuck in "processing" state due to crashes or timeouts
   *
   * NOTE: This method does NOT check workflow status before resetting.
   * For proper crash recovery with duplicate prevention, use /get-stuck-windows
   * and check workflow status externally before calling /confirm-dispatch or
   * /rollback-processing.
   *
   * This method is kept for backwards compatibility and as a fallback safety net
   * if the queue consumer doesn't implement proper recovery. Windows will be
   * reset to pending and potentially cause duplicate processing.
   *
   * @deprecated Use /get-stuck-windows + workflow status check instead
   */
  private cleanupStuckProcessingWindows(_now: number): void {
    // IMPORTANT: This is now a no-op to prevent race conditions.
    // Stuck window recovery is handled by the queue consumer via
    // /get-stuck-windows endpoint which includes the provisional workflow ID
    // for status checking before deciding to confirm or rollback.
    //
    // The queue consumer should:
    // 1. Call /get-stuck-windows to get stuck windows with provisionalWorkflowId
    // 2. Check workflow status via env.COMPACTION_WORKFLOW.get(id).status()
    // 3. If workflow is running/complete: call /confirm-dispatch
    // 4. If workflow doesn't exist or errored: call /rollback-processing
    //
    // This prevents duplicate compaction workflows when a crash occurs between
    // workflow.create() succeeding and confirm-dispatch being called.
  }

  private handleStatus(): Response {
    const status = this.buildStatusResponse()
    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private buildStatusResponse() {
    const now = Date.now()

    // Calculate queue metrics
    let pendingWindows = 0
    let processingWindows = 0
    let dispatchedWindows = 0
    let oldestWindowAge = 0
    let totalPendingFiles = 0
    let windowsStuckInProcessing = 0

    for (const window of this.windows.values()) {
      const windowAge = now - window.windowEnd
      if (windowAge > oldestWindowAge) {
        oldestWindowAge = windowAge
      }

      switch (window.processingStatus.state) {
        case 'pending':
          pendingWindows++
          for (const files of window.filesByWriter.values()) {
            totalPendingFiles += files.length
          }
          break
        case 'processing':
          processingWindows++
          if (now - window.processingStatus.startedAt > this.processingTimeoutMs) {
            windowsStuckInProcessing++
          }
          break
        case 'dispatched':
          dispatchedWindows++
          break
      }
    }

    const effectiveMaxWaitTimeMs = this.getEffectiveMaxWaitTimeMs()
    const healthThresholdMs = effectiveMaxWaitTimeMs * 2
    let healthStatus: 'healthy' | 'degraded' | 'unhealthy' = 'healthy'
    const healthIssues: string[] = []

    if (oldestWindowAge > healthThresholdMs) {
      healthStatus = 'degraded'
      healthIssues.push(`Oldest window age (${Math.round(oldestWindowAge / 60000)}m) exceeds threshold (${Math.round(healthThresholdMs / 60000)}m)`)
    }

    if (windowsStuckInProcessing > 0) {
      healthStatus = 'unhealthy'
      healthIssues.push(`${windowsStuckInProcessing} window(s) stuck in processing`)
    }

    return {
      namespace: this.namespace,
      priority: this.priority,
      effectiveMaxWaitTimeMs: this.getEffectiveMaxWaitTimeMs(),
      backpressure: this.calculateBackpressureLevel(),
      activeWindows: this.windows.size,
      queueMetrics: {
        pendingWindows,
        processingWindows,
        dispatchedWindows,
      },
      health: {
        status: healthStatus,
        issues: healthIssues,
      },
      knownWriters: Array.from(this.knownWriters),
      activeWriters: this.getActiveWriters(now),
      oldestWindowAge,
      totalPendingFiles,
      windowsStuckInProcessing,
      windows: Array.from(this.windows.entries()).map(([key, w]) => ({
        key,
        windowStart: new Date(w.windowStart).toISOString(),
        windowEnd: new Date(w.windowEnd).toISOString(),
        writers: Array.from(w.writers),
        fileCount: Array.from(w.filesByWriter.values()).reduce((sum, f) => sum + f.length, 0),
        totalSize: w.totalSize,
        processingStatus: w.processingStatus,
      })),
    }
  }

  private getActiveWriters(now: number): string[] {
    const active: string[] = []
    for (const [writerId, lastSeen] of this.writerLastSeen) {
      if (now - lastSeen < this.writerInactiveThresholdMs) {
        active.push(writerId)
      }
    }
    return active
  }
}

export default { handleCompactionQueue, CompactionStateDO, recoverStuckWindows }
