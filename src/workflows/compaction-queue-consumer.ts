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
 */

import { logger } from '../utils/logger'
import type { BackendType } from '../backends'

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
}

/** State stored in Durable Object */
interface ConsumerState {
  /** Active windows being tracked */
  windows: Map<string, WindowState>

  /** Known writers (for quorum calculation) */
  knownWriters: Set<string>

  /** Last time each writer was active */
  writerLastSeen: Map<string, number>
}

interface Env {
  BUCKET: R2Bucket
  COMPACTION_WORKFLOW: {
    create(options: { params: unknown }): Promise<{ id: string }>
  }
  COMPACTION_STATE: DurableObjectNamespace
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

// =============================================================================
// Queue Consumer Handler
// =============================================================================

/** Window entry structure */
interface WindowReadyEntry {
  namespace: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
}

/** Response from CompactionStateDO */
interface WindowsReadyResponse {
  windowsReady: WindowReadyEntry[]
}

/**
 * Type guard for WindowsReadyResponse
 */
function isWindowsReadyResponse(data: unknown): data is WindowsReadyResponse {
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
 */
export async function handleCompactionQueue(
  batch: MessageBatch<R2EventMessage>,
  env: Env,
  config: CompactionConsumerConfig = {}
): Promise<void> {
  console.log('[CompactionQueue] Received batch', {
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
    console.log('[CompactionQueue] Event:', JSON.stringify(event))

    // Only process object-create events for Parquet files
    // R2 uses 'action' field: PutObject, CopyObject, CompleteMultipartUpload for creates
    const isCreateAction = event.action === 'PutObject' || event.action === 'CopyObject' || event.action === 'CompleteMultipartUpload'
    if (!isCreateAction) {
      console.log('[CompactionQueue] Skipping non-create action:', event.action)
      continue
    }
    if (!event.object.key.endsWith('.parquet')) {
      console.log('[CompactionQueue] Skipping non-parquet file:', event.object.key)
      continue
    }
    if (!event.object.key.startsWith(namespacePrefix)) {
      console.log('[CompactionQueue] Skipping file outside prefix:', event.object.key, 'prefix:', namespacePrefix)
      continue
    }

    // Parse file info: data/{namespace}/{timestamp}-{writerId}-{seq}.parquet
    const keyWithoutPrefix = event.object.key.slice(namespacePrefix.length)
    const parts = keyWithoutPrefix.split('/')
    const namespace = parts.slice(0, -1).join('/')
    const filename = parts[parts.length - 1] ?? ''

    console.log('[CompactionQueue] Parsed:', { namespace, filename })

    const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
    if (!match) {
      console.log('[CompactionQueue] Skipping file with unexpected format:', event.object.key, 'filename:', filename)
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
    console.log('[CompactionQueue] No matching updates to process')
    return
  }

  console.log('[CompactionQueue] Processing updates', {
    namespaceCount: updatesByNamespace.size,
    totalUpdates: Array.from(updatesByNamespace.values()).reduce((sum, u) => sum + u.length, 0),
  })

  // Send updates to namespace-sharded DOs in parallel
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

  // Trigger workflows for ready windows
  for (const window of allWindowsReady) {
    logger.info('Triggering compaction workflow', {
      namespace: window.namespace,
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
    } catch (err) {
      logger.error('Failed to start compaction workflow', {
        error: err instanceof Error ? err.message : 'Unknown',
        namespace: window.namespace,
      })
    }
  }
}

// =============================================================================
// State Durable Object
// =============================================================================

/** Serializable window state for storage */
interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
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
      windowStart: number
      windowEnd: number
      files: string[]
      writers: string[]
    }> = []

    // Set namespace on first update (each DO instance handles one namespace)
    if (!this.namespace) {
      this.namespace = namespace
    }

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
        }
        this.windows.set(windowKey, window)
      }

      // Add file to window
      const writerFiles = window.filesByWriter.get(writerId) ?? []
      writerFiles.push(file)
      window.filesByWriter.set(writerId, writerFiles)
      window.writers.add(writerId)
      window.lastActivityAt = now
      window.totalSize += size
    }

    // Check for windows ready for compaction
    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
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
        // Window is ready! Use this DO's namespace
        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace: this.namespace,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: allFiles.sort(),
          writers: Array.from(window.writers),
        })

        // Remove window from tracking
        this.windows.delete(windowKey)
      }
    }

    // Persist state
    await this.saveState()

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleStatus(): Response {
    const status = {
      namespace: this.namespace,
      activeWindows: this.windows.size,
      knownWriters: Array.from(this.knownWriters),
      activeWriters: this.getActiveWriters(Date.now()),
      windows: Array.from(this.windows.entries()).map(([key, w]) => ({
        key,
        windowStart: new Date(w.windowStart).toISOString(),
        windowEnd: new Date(w.windowEnd).toISOString(),
        writers: Array.from(w.writers),
        fileCount: Array.from(w.filesByWriter.values()).reduce((sum, f) => sum + f.length, 0),
        totalSize: w.totalSize,
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
