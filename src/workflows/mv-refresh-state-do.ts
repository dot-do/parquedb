/**
 * Materialized View Refresh State Durable Object
 *
 * Tracks pending MV refreshes based on source data changes from R2 events.
 * Implements debounce and batching to avoid excessive refresh operations.
 *
 * Architecture:
 * ```
 * R2 Write → Event Notification → Queue → handleMVRefreshQueue
 *                                              ↓
 *                                    MVRefreshStateDO (this)
 *                                              ↓
 *                                    Ready MVs → MVRefreshWorkflow
 * ```
 *
 * Endpoints:
 * - POST /register-mv - Register MV definitions with source dependencies
 * - POST /notify-change - Notify of source data changes from R2 events
 * - POST /get-ready-mvs - Get MVs ready for refresh (after debounce)
 * - POST /confirm-dispatch - Confirm workflow was created successfully
 * - POST /workflow-complete - Mark workflow as complete (success or failure)
 * - GET /status - Get current state of pending MV refreshes
 */

import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * MV definition for tracking source dependencies
 */
export interface MVDefinitionEntry {
  name: string
  source: string
  refreshMode: 'streaming' | 'scheduled' | 'manual'
}

/**
 * Pending MV refresh entry
 */
export interface MVRefreshEntry {
  mvName: string
  source: string
  changedFiles: string[]
  lastChangeAt: number
  firstChangeAt: number
  status: 'pending' | 'processing' | 'dispatched'
  workflowId?: string | undefined
}

/**
 * Configuration for the MV refresh queue consumer
 */
export interface MVRefreshConsumerConfig {
  /** Debounce time before triggering refresh (default: 1000ms) */
  debounceMs?: number | undefined
  /** Maximum time to wait before forcing refresh (default: 5000ms) */
  maxWaitMs?: number | undefined
  /** Namespace prefix to watch (default: 'data/') */
  namespacePrefix?: string | undefined
  /** Whether to batch changes per MV (default: true) */
  batchChanges?: boolean | undefined
}

/**
 * Response from MVRefreshStateDO get-ready-mvs endpoint
 */
export interface MVRefreshResponse {
  mvsReady: MVRefreshEntry[]
}

/**
 * Status response from MVRefreshStateDO
 */
export interface MVRefreshStatusResponse {
  pendingMVs: number
  processingMVs: number
  dispatchedMVs: number
  mvs: MVRefreshEntry[]
}

// =============================================================================
// Internal Storage Types
// =============================================================================

interface StoredMVRefreshState {
  mvName: string
  source: string
  changedFiles: string[]
  lastChangeAt: number
  firstChangeAt: number
  status: 'pending' | 'processing' | 'dispatched'
  workflowId?: string | undefined
}

interface StoredState {
  mvDefinitions: Record<string, MVDefinitionEntry>
  pendingRefreshes: Record<string, StoredMVRefreshState>
}

// =============================================================================
// Durable Object Implementation
// =============================================================================

/**
 * Durable Object for tracking MV refresh state
 *
 * Each instance tracks all MVs across all namespaces. For higher scale,
 * consider sharding by namespace (similar to CompactionStateDO).
 */
export class MVRefreshStateDO {
  private state: DurableObjectState
  private mvDefinitions: Map<string, MVDefinitionEntry> = new Map()
  private pendingRefreshes: Map<string, StoredMVRefreshState> = new Map()
  private initialized = false

  constructor(state: DurableObjectState) {
    this.state = state
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('mvRefreshState')
    if (stored) {
      this.mvDefinitions = new Map(Object.entries(stored.mvDefinitions))
      this.pendingRefreshes = new Map(Object.entries(stored.pendingRefreshes))
    }

    this.initialized = true
  }

  private async saveState(): Promise<void> {
    const stored: StoredState = {
      mvDefinitions: Object.fromEntries(this.mvDefinitions),
      pendingRefreshes: Object.fromEntries(this.pendingRefreshes),
    }
    await this.state.storage.put('mvRefreshState', stored)
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    if (url.pathname === '/register-mv' && request.method === 'POST') {
      return this.handleRegisterMV(request)
    }

    if (url.pathname === '/notify-change' && request.method === 'POST') {
      return this.handleNotifyChange(request)
    }

    if (url.pathname === '/get-ready-mvs' && request.method === 'POST') {
      return this.handleGetReadyMVs(request)
    }

    if (url.pathname === '/confirm-dispatch' && request.method === 'POST') {
      return this.handleConfirmDispatch(request)
    }

    if (url.pathname === '/workflow-complete' && request.method === 'POST') {
      return this.handleWorkflowComplete(request)
    }

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  /**
   * Register an MV definition with its source dependency
   */
  private async handleRegisterMV(request: Request): Promise<Response> {
    const body = await request.json() as { mv: MVDefinitionEntry }
    const { mv } = body

    this.mvDefinitions.set(mv.name, mv)
    await this.saveState()

    logger.info('MV registered', {
      mvName: mv.name,
      source: mv.source,
      refreshMode: mv.refreshMode,
    })

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Notify of source data changes from R2 events
   * Creates or updates pending refresh entries for affected MVs
   */
  private async handleNotifyChange(request: Request): Promise<Response> {
    const body = await request.json() as {
      namespace: string
      files: string[]
      timestamp: number
    }
    const { namespace, files, timestamp } = body
    const now = Date.now()

    // Find all MVs that depend on this namespace
    for (const [mvName, mv] of this.mvDefinitions) {
      if (mv.source === namespace && mv.refreshMode === 'streaming') {
        const existing = this.pendingRefreshes.get(mvName)
        if (existing && existing.status === 'pending') {
          // Add to existing batch
          existing.changedFiles.push(...files)
          existing.lastChangeAt = now
        } else if (!existing || existing.status === 'dispatched') {
          // Create new pending refresh
          this.pendingRefreshes.set(mvName, {
            mvName,
            source: mv.source,
            changedFiles: [...files],
            firstChangeAt: timestamp,
            lastChangeAt: now,
            status: 'pending',
          })
        }
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Get MVs ready for refresh based on debounce and max wait times
   * Marks returned MVs as 'processing'
   */
  private async handleGetReadyMVs(request: Request): Promise<Response> {
    const body = await request.json() as {
      config: MVRefreshConsumerConfig
    }
    const { config } = body
    const now = Date.now()
    const debounceMs = config.debounceMs ?? 1000
    const maxWaitMs = config.maxWaitMs ?? 5000
    const mvsReady: MVRefreshEntry[] = []

    for (const [_mvName, pending] of this.pendingRefreshes) {
      if (pending.status !== 'pending') continue

      // Check if debounce period has passed OR max wait exceeded
      const timeSinceLastChange = now - pending.lastChangeAt
      const timeSinceFirstChange = now - pending.firstChangeAt
      const debounced = timeSinceLastChange >= debounceMs
      const maxWaitExceeded = timeSinceFirstChange >= maxWaitMs

      if (debounced || maxWaitExceeded) {
        // Mark as processing
        pending.status = 'processing'
        mvsReady.push(pending)
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ mvsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Confirm successful workflow dispatch
   * Marks MV as 'dispatched' with workflow ID
   */
  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as { mvName: string; workflowId: string }
    const { mvName, workflowId } = body

    const pending = this.pendingRefreshes.get(mvName)
    if (!pending) {
      return new Response(JSON.stringify({ error: 'MV refresh not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (pending.status !== 'processing') {
      return new Response(JSON.stringify({ error: 'MV not in processing state' }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    pending.status = 'dispatched'
    pending.workflowId = workflowId
    await this.saveState()

    logger.info('MV workflow dispatched', {
      mvName,
      workflowId,
    })

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Handle workflow completion
   * On success: removes pending refresh
   * On failure: resets to pending for retry
   */
  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as { mvName: string; workflowId: string; success: boolean }
    const { mvName, workflowId, success } = body

    const pending = this.pendingRefreshes.get(mvName)
    if (!pending) {
      return new Response(JSON.stringify({ success: true, alreadyDeleted: true }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (pending.workflowId !== workflowId) {
      return new Response(JSON.stringify({ error: 'Workflow ID mismatch' }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (success) {
      // Remove completed refresh
      this.pendingRefreshes.delete(mvName)
      logger.info('MV refresh completed', { mvName, workflowId })
    } else {
      // Reset to pending for retry
      pending.status = 'pending'
      pending.workflowId = undefined
      logger.warn('MV refresh failed, reset to pending', { mvName, workflowId })
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Get current status of all pending MV refreshes
   */
  private handleStatus(): Response {
    let pendingMVs = 0
    let processingMVs = 0
    let dispatchedMVs = 0

    for (const pending of this.pendingRefreshes.values()) {
      switch (pending.status) {
        case 'pending': pendingMVs++; break
        case 'processing': processingMVs++; break
        case 'dispatched': dispatchedMVs++; break
      }
    }

    const status: MVRefreshStatusResponse = {
      pendingMVs,
      processingMVs,
      dispatchedMVs,
      mvs: Array.from(this.pendingRefreshes.values()),
    }

    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

export default { MVRefreshStateDO }
