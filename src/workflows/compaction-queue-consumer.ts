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
  eventType: 'object-create' | 'object-delete'
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

/**
 * Handle a batch of R2 event notifications
 */
export async function handleCompactionQueue(
  batch: MessageBatch<R2EventMessage>,
  env: Env,
  config: CompactionConsumerConfig = {}
): Promise<void> {
  const {
    windowSizeMs = DEFAULT_WINDOW_SIZE_MS,
    minFilesToCompact = DEFAULT_MIN_FILES,
    maxWaitTimeMs = DEFAULT_MAX_WAIT_MS,
    targetFormat = 'native',
    namespacePrefix = 'data/',
  } = config

  // Get the state DO
  const stateId = env.COMPACTION_STATE.idFromName('default')
  const stateDO = env.COMPACTION_STATE.get(stateId)

  // Process each message
  const updates: Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }> = []

  for (const message of batch.messages) {
    const event = message.body

    // Only process object-create events for Parquet files
    if (event.eventType !== 'object-create') continue
    if (!event.object.key.endsWith('.parquet')) continue
    if (!event.object.key.startsWith(namespacePrefix)) continue

    // Parse file info: data/{namespace}/{timestamp}-{writerId}-{seq}.parquet
    const keyWithoutPrefix = event.object.key.slice(namespacePrefix.length)
    const parts = keyWithoutPrefix.split('/')
    const namespace = parts.slice(0, -1).join('/')
    const filename = parts[parts.length - 1] ?? ''

    const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
    if (!match) {
      logger.debug(`Skipping file with unexpected format: ${event.object.key}`)
      message.ack()
      continue
    }

    const [, timestampStr, writerId] = match
    const timestamp = parseInt(timestampStr ?? '0', 10)

    updates.push({
      namespace,
      writerId: writerId ?? 'unknown',
      file: event.object.key,
      timestamp,
      size: event.object.size,
    })

    message.ack()
  }

  if (updates.length === 0) return

  // Send updates to state DO and check for windows ready for compaction
  const response = await stateDO.fetch('http://internal/update', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      updates,
      config: {
        windowSizeMs,
        minFilesToCompact,
        maxWaitTimeMs,
        targetFormat,
      },
    }),
  })

  const result = await response.json() as {
    windowsReady: Array<{
      namespace: string
      windowStart: number
      windowEnd: number
      files: string[]
      writers: string[]
    }>
  }

  // Trigger workflows for ready windows
  for (const window of result.windowsReady) {
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

/**
 * Durable Object for tracking compaction state across queue messages
 */
export class CompactionStateDO {
  private state: DurableObjectState
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()

  constructor(state: DurableObjectState) {
    this.state = state
  }

  async fetch(request: Request): Promise<Response> {
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

    const { updates, config } = body
    const now = Date.now()
    const windowsReady: Array<{
      namespace: string
      windowStart: number
      windowEnd: number
      files: string[]
      writers: string[]
    }> = []

    // Process updates
    for (const update of updates) {
      const { namespace, writerId, file, timestamp, size } = update

      // Track writer
      this.knownWriters.add(writerId)
      this.writerLastSeen.set(writerId, now)

      // Calculate window
      const windowStart = Math.floor(timestamp / config.windowSizeMs) * config.windowSizeMs
      const windowEnd = windowStart + config.windowSizeMs
      const windowKey = `${namespace}:${windowStart}`

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
        // Window is ready!
        const namespace = windowKey.split(':')[0] ?? ''
        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace,
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
    await this.state.storage.put('windows', Array.from(this.windows.entries()))
    await this.state.storage.put('knownWriters', Array.from(this.knownWriters))
    await this.state.storage.put('writerLastSeen', Array.from(this.writerLastSeen.entries()))

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleStatus(): Response {
    const status = {
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
