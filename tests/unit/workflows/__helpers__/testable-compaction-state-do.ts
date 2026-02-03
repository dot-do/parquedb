/**
 * Testable CompactionStateDO Helper
 *
 * Shared test utility that mirrors the production CompactionStateDO implementation.
 * Used for testing compaction workflow state management without deploying to Workers.
 */

// =============================================================================
// Types
// =============================================================================

export type WindowProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

export interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
  processingStatus: WindowProcessingStatus
}

export type StoredProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

export interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
  processingStatus?: StoredProcessingStatus
}

export interface StoredState {
  namespace: string
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

export interface UpdateRequest {
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

export interface WindowReadyEntry {
  namespace: string
  windowKey: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
}

// =============================================================================
// Mock Classes
// =============================================================================

export class MockDurableObjectStorage {
  private data: Map<string, unknown> = new Map()

  async get<T>(key: string): Promise<T | undefined> {
    return this.data.get(key) as T | undefined
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.data.set(key, value)
  }

  async delete(key: string): Promise<boolean> {
    return this.data.delete(key)
  }

  async list(): Promise<Map<string, unknown>> {
    return new Map(this.data)
  }

  // Test helpers
  clear(): void {
    this.data.clear()
  }

  getData(key: string): unknown {
    return this.data.get(key)
  }

  setData(key: string, value: unknown): void {
    this.data.set(key, value)
  }
}

export class MockDurableObjectState {
  storage: MockDurableObjectStorage

  constructor() {
    this.storage = new MockDurableObjectStorage()
  }

  clear(): void {
    this.storage.clear()
  }

  getData(key: string): unknown {
    return this.storage.getData(key)
  }

  setData(key: string, value: unknown): void {
    this.storage.setData(key, value)
  }
}

// =============================================================================
// TestableCompactionStateDO
// =============================================================================

/**
 * Testable CompactionStateDO implementation
 * Mirrors production implementation for testing with two-phase commit
 */
export class TestableCompactionStateDO {
  private state: MockDurableObjectState
  private namespace: string = ''
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()
  private initialized = false

  private static WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000
  private static PROCESSING_TIMEOUT_MS = 5 * 60 * 1000

  constructor(state: MockDurableObjectState) {
    this.state = state
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('compactionState')
    if (stored) {
      this.namespace = stored.namespace ?? ''
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
      this.knownWriters = new Set(stored.knownWriters)
      this.writerLastSeen = new Map(Object.entries(stored.writerLastSeen))
    }

    this.initialized = true
  }

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

  private cleanupStuckProcessingWindows(now: number): void {
    for (const [windowKey, window] of this.windows) {
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > TestableCompactionStateDO.PROCESSING_TIMEOUT_MS
      ) {
        window.processingStatus = { state: 'pending' }
      }
    }
  }

  private async handleUpdate(request: Request): Promise<Response> {
    const body = await request.json() as UpdateRequest

    const { namespace, updates, config } = body
    const now = Date.now()
    const windowsReady: WindowReadyEntry[] = []

    // Set namespace on first update
    if (!this.namespace) {
      this.namespace = namespace
    }

    // Clean up stuck processing windows
    this.cleanupStuckProcessingWindows(now)

    // Process updates
    for (const update of updates) {
      const { writerId, file, timestamp, size } = update

      this.knownWriters.add(writerId)
      this.writerLastSeen.set(writerId, now)

      const windowStart = Math.floor(timestamp / config.windowSizeMs) * config.windowSizeMs
      const windowEnd = windowStart + config.windowSizeMs
      const windowKey = String(windowStart)

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

      // Only add files to pending windows
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

      if (now < window.windowEnd + config.maxWaitTimeMs) continue

      let totalFiles = 0
      for (const files of window.filesByWriter.values()) {
        totalFiles += files.length
      }

      if (totalFiles < config.minFilesToCompact) continue

      const missingWriters = activeWriters.filter(w => !window.writers.has(w))
      const waitedLongEnough = (now - window.lastActivityAt) > config.maxWaitTimeMs

      if (missingWriters.length === 0 || waitedLongEnough) {
        // Mark as processing (Phase 1 of two-phase commit)
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

    await this.saveState()

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string }
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

    window.processingStatus = {
      state: 'dispatched',
      workflowId,
      dispatchedAt: Date.now(),
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleRollbackProcessing(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string }
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

    window.processingStatus = { state: 'pending' }
    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string; success: boolean }
    const { windowKey, workflowId, success } = body
    const window = this.windows.get(windowKey)

    if (!window) {
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
      this.windows.delete(windowKey)
    } else {
      window.processingStatus = { state: 'pending' }
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
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
      if (now - lastSeen < TestableCompactionStateDO.WRITER_INACTIVE_THRESHOLD_MS) {
        active.push(writerId)
      }
    }
    return active
  }

  // Test helpers
  getWindowCount(): number {
    return this.windows.size
  }

  getKnownWriters(): string[] {
    return Array.from(this.knownWriters)
  }

  getNamespace(): string {
    return this.namespace
  }

  getWindowProcessingStatus(windowKey: string): WindowProcessingStatus | undefined {
    return this.windows.get(windowKey)?.processingStatus
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

export function createUpdateRequest(overrides: Partial<UpdateRequest> = {}): UpdateRequest {
  return {
    namespace: 'users',
    updates: [],
    config: {
      windowSizeMs: 3600000, // 1 hour
      minFilesToCompact: 10,
      maxWaitTimeMs: 300000, // 5 minutes
      targetFormat: 'native',
    },
    ...overrides,
  }
}

export function createUpdate(overrides: Partial<UpdateRequest['updates'][0]> = {}) {
  return {
    namespace: 'users',
    writerId: 'writer1',
    file: 'data/users/1700001234-writer1-0.parquet',
    timestamp: 1700001234000,
    size: 1024,
    ...overrides,
  }
}
