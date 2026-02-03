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

import { logger } from '../utils/logger'
import type { BackendType } from '../backends'
import type { CompactionQueueEnv as Env } from './types'

// =============================================================================
// Types
// =============================================================================

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
  windowSizeMs?: number

  /** Minimum files before compaction is considered (default: 10) */
  minFilesToCompact?: number

  /** Maximum time to wait for late writers (default: 5 minutes) */
  maxWaitTimeMs?: number

  /** Target format for compacted files */
  targetFormat?: BackendType

  /** Namespace prefix to watch (default: 'data/') */
  namespacePrefix?: string
}

/** Processing status for two-phase commit */
type WindowProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

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

/** Time after which a writer is considered inactive: 30 minutes */
const WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000

/** Time after which a processing window is considered stuck: 5 minutes */
const PROCESSING_TIMEOUT_MS = 5 * 60 * 1000

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
    processingStatus: { state: 'pending' } | { state: 'processing'; startedAt: number } | { state: 'dispatched'; workflowId: string; dispatchedAt: number }
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
 * ARCHITECTURE: Namespace Sharding
 * Each namespace gets its own CompactionStateDO instance for scalability.
 * Updates are grouped by namespace and sent to namespace-specific DOs.
 * This eliminates the bottleneck of a single global DO instance.
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
  } = config

  // Process each message and group by namespace
  const updatesByNamespace = new Map<string, Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>>()

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

    const update = {
      namespace,
      writerId: writerId ?? 'unknown',
      file: event.object.key,
      timestamp,
      size: event.object.size,
    }

    // Group updates by namespace for sharded DO dispatch
    const namespaceUpdates = updatesByNamespace.get(namespace) ?? []
    namespaceUpdates.push(update)
    updatesByNamespace.set(namespace, namespaceUpdates)

    message.ack()
  }

  if (updatesByNamespace.size === 0) {
    logger.debug('No matching updates to process')
    return
  }

  logger.info('Processing updates', {
    namespaceCount: updatesByNamespace.size,
    totalUpdates: Array.from(updatesByNamespace.values()).reduce((sum, u) => sum + u.length, 0),
  })

  // Send updates to namespace-sharded DOs in parallel
  // Phase 1: Mark windows as "processing" and get ready windows
  const allWindowsReady: WindowReadyEntry[] = []

  await Promise.all(
    Array.from(updatesByNamespace.entries()).map(async ([namespace, updates]) => {
      // Get namespace-specific DO instance for scalability
      const stateId = env.COMPACTION_STATE.idFromName(namespace)
      const stateDO = env.COMPACTION_STATE.get(stateId)

      const response = await stateDO.fetch('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace, // Include namespace for DO to know its identity
          updates,
          config: {
            windowSizeMs,
            minFilesToCompact,
            maxWaitTimeMs,
            targetFormat,
          },
        }),
      })

      const data = await response.json()
      if (!isWindowsReadyResponse(data)) {
        throw new Error(`Invalid response from CompactionStateDO for namespace '${namespace}': expected { windowsReady: Array<WindowReadyEntry> }`)
      }

      allWindowsReady.push(...data.windowsReady)
    })
  )

  // Phase 2: Trigger workflows for ready windows with two-phase commit
  for (const window of allWindowsReady) {
    const stateId = env.COMPACTION_STATE.idFromName(window.namespace)
    const stateDO = env.COMPACTION_STATE.get(stateId)

    logger.info('Triggering compaction workflow', {
      namespace: window.namespace,
      windowKey: window.windowKey,
      windowStart: new Date(window.windowStart).toISOString(),
      files: window.files.length,
      writers: window.writers.length,
    })

    try {
      const instance = await env.COMPACTION_WORKFLOW.create({
        params: {
          namespace: window.namespace,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: window.files,
          writers: window.writers,
          targetFormat,
        },
      })

      logger.info('Workflow started', { workflowId: instance.id })

      // Phase 2a: Confirm dispatch success - mark as dispatched
      await stateDO.fetch('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          windowKey: window.windowKey,
          workflowId: instance.id,
        }),
      })
    } catch (err) {
      logger.error('Failed to start compaction workflow', {
        error: err instanceof Error ? err.message : 'Unknown',
        namespace: window.namespace,
        windowKey: window.windowKey,
      })

      // Phase 2b: Rollback on failure - reset to pending
      await stateDO.fetch('http://internal/rollback-processing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          windowKey: window.windowKey,
        }),
      })
    }
  }
}

// =============================================================================
// State Durable Object
// =============================================================================

/** Serializable processing status for storage */
type StoredProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

/** Serializable window state for storage */
interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
  processingStatus?: StoredProcessingStatus
}

/** Stored state structure (namespace-sharded - each DO handles one namespace) */
interface StoredState {
  /** The namespace this DO instance handles */
  namespace: string
  /** Windows keyed by windowStart timestamp */
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
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
export class CompactionStateDO {
  private state: DurableObjectState
  /** The namespace this DO instance handles (set on first update) */
  private namespace: string = ''
  /** Windows keyed by windowStart timestamp */
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()
  private initialized = false

  constructor(state: DurableObjectState) {
    this.state = state
  }

  /** Load state from storage */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('compactionState')
    if (stored) {
      // Restore namespace
      this.namespace = stored.namespace ?? ''
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
    }

    this.initialized = true
  }

  /** Save state to storage */
  private async saveState(): Promise<void> {
    const stored: StoredState = {
      namespace: this.namespace,
      windows: {},
      knownWriters: Array.from(this.knownWriters),
      writerLastSeen: Object.fromEntries(this.writerLastSeen),
    }

    for (const [key, window] of this.windows) {
      stored.windows[key] = {
        windowStart: window.windowStart,
        windowEnd: window.windowEnd,
        filesByWriter: Object.fromEntries(window.filesByWriter),
        writers: Array.from(window.writers),
        lastActivityAt: window.lastActivityAt,
        totalSize: window.totalSize,
        processingStatus: window.processingStatus,
      }
    }

    await this.state.storage.put('compactionState', stored)
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    if (url.pathname === '/update' && request.method === 'POST') {
      return this.handleUpdate(request)
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

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  private async handleUpdate(request: Request): Promise<Response> {
    const body = await request.json() as {
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
      }
    }

    const { namespace, updates, config } = body
    const now = Date.now()
    const windowsReady: Array<{
      namespace: string
      windowKey: string
      windowStart: number
      windowEnd: number
      files: string[]
      writers: string[]
    }> = []

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

    // Check for windows ready for compaction (only pending windows)
    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
      // Skip non-pending windows
      if (window.processingStatus.state !== 'pending') continue

      // Skip if window is too recent (still filling)
      if (now < window.windowEnd + config.maxWaitTimeMs) continue

      // Count total files
      let totalFiles = 0
      for (const files of window.filesByWriter.values()) {
        totalFiles += files.length
      }

      // Skip if not enough files
      if (totalFiles < config.minFilesToCompact) continue

      // Check if we've heard from all active writers (or waited long enough)
      const missingWriters = activeWriters.filter(w => !window.writers.has(w))
      const waitedLongEnough = (now - window.lastActivityAt) > config.maxWaitTimeMs

      if (missingWriters.length === 0 || waitedLongEnough) {
        // Window is ready! Mark as processing (Phase 1 of two-phase commit)
        window.processingStatus = { state: 'processing', startedAt: now }

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
        })
      }
    }

    // Persist state
    await this.saveState()

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Phase 2a: Confirm successful workflow dispatch
   * Marks window as "dispatched" with the workflow ID
   */
  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as {
      windowKey: string
      workflowId: string
    }

    const { windowKey, workflowId } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      return new Response(JSON.stringify({ error: 'Window not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'processing') {
      return new Response(JSON.stringify({
        error: 'Window not in processing state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Mark as dispatched
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
    })

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Phase 2b: Rollback failed workflow dispatch
   * Resets window to "pending" state so it can be retried
   */
  private async handleRollbackProcessing(request: Request): Promise<Response> {
    const body = await request.json() as {
      windowKey: string
    }

    const { windowKey } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      return new Response(JSON.stringify({ error: 'Window not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'processing') {
      return new Response(JSON.stringify({
        error: 'Window not in processing state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Reset to pending for retry
    window.processingStatus = { state: 'pending' }

    await this.saveState()

    logger.info('Window processing rolled back', {
      namespace: this.namespace,
      windowKey,
    })

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Called by workflow on completion to clean up the window
   * This is the final step - window can now be safely deleted
   */
  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as {
      windowKey: string
      workflowId: string
      success: boolean
    }

    const { windowKey, workflowId, success } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      // Window already cleaned up, that's fine
      return new Response(JSON.stringify({ success: true, alreadyDeleted: true }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'dispatched') {
      return new Response(JSON.stringify({
        error: 'Window not in dispatched state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.workflowId !== workflowId) {
      return new Response(JSON.stringify({
        error: 'Workflow ID mismatch',
        expected: window.processingStatus.workflowId,
        received: workflowId,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (success) {
      // Workflow completed successfully - delete the window
      this.windows.delete(windowKey)
      logger.info('Window completed and deleted', {
        namespace: this.namespace,
        windowKey,
        workflowId,
      })
    } else {
      // Workflow failed - reset to pending for retry
      window.processingStatus = { state: 'pending' }
      logger.warn('Workflow failed, window reset to pending', {
        namespace: this.namespace,
        windowKey,
        workflowId,
      })
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Clean up windows stuck in "processing" state due to crashes or timeouts
   */
  private cleanupStuckProcessingWindows(now: number): void {
    for (const [windowKey, window] of this.windows) {
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > PROCESSING_TIMEOUT_MS
      ) {
        logger.warn('Resetting stuck processing window', {
          namespace: this.namespace,
          windowKey,
          stuckSince: new Date(window.processingStatus.startedAt).toISOString(),
        })
        window.processingStatus = { state: 'pending' }
      }
    }
  }

  private handleStatus(): Response {
    const now = Date.now()

    // Calculate alerting metrics
    let oldestWindowAge = 0
    let totalPendingFiles = 0
    let windowsStuckInProcessing = 0

    for (const window of this.windows.values()) {
      // Calculate age from windowEnd (when the window closed)
      const windowAge = now - window.windowEnd
      if (windowAge > oldestWindowAge) {
        oldestWindowAge = windowAge
      }

      // Count pending files (only count pending windows, not processing/dispatched)
      if (window.processingStatus.state === 'pending') {
        for (const files of window.filesByWriter.values()) {
          totalPendingFiles += files.length
        }
      }

      // Count stuck processing windows (> 5 minutes in processing state)
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > PROCESSING_TIMEOUT_MS
      ) {
        windowsStuckInProcessing++
      }
    }

    const status = {
      namespace: this.namespace,
      activeWindows: this.windows.size,
      knownWriters: Array.from(this.knownWriters),
      activeWriters: this.getActiveWriters(now),
      // Alerting metrics for health monitoring
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

    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private getActiveWriters(now: number): string[] {
    const active: string[] = []
    for (const [writerId, lastSeen] of this.writerLastSeen) {
      if (now - lastSeen < WRITER_INACTIVE_THRESHOLD_MS) {
        active.push(writerId)
      }
    }
    return active
  }
}

export default { handleCompactionQueue, CompactionStateDO }
