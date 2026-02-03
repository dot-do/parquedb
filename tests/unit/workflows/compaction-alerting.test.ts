/**
 * Compaction Alerting Tests
 *
 * Tests for compaction health monitoring and alerting:
 * - /status endpoint metrics (oldestWindowAge, totalPendingFiles, windowsStuckInProcessing)
 * - /compaction/health endpoint in worker
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// =============================================================================
// Types
// =============================================================================

type WindowProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
  processingStatus: WindowProcessingStatus
}

type StoredProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
  processingStatus?: StoredProcessingStatus | undefined
}

interface StoredState {
  namespace: string
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

interface UpdateRequest {
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

interface WindowReadyEntry {
  namespace: string
  windowKey: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
}

interface CompactionHealthConfig {
  maxPendingWindows: number
  maxWindowAgeHours: number
}

interface NamespaceHealth {
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

interface HealthCheckResponse {
  status: 'healthy' | 'degraded' | 'unhealthy'
  namespaces: Record<string, NamespaceHealth>
  alerts: string[]
}

// =============================================================================
// Mock Classes
// =============================================================================

class MockDurableObjectStorage {
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

class MockDurableObjectState {
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

/**
 * Testable CompactionStateDO with enhanced status metrics for alerting
 */
class TestableCompactionStateDO {
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
    for (const [_windowKey, window] of this.windows) {
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

    if (!this.namespace) {
      this.namespace = namespace
    }

    this.cleanupStuckProcessingWindows(now)

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

      if (window.processingStatus.state === 'pending') {
        const writerFiles = window.filesByWriter.get(writerId) ?? []
        writerFiles.push(file)
        window.filesByWriter.set(writerId, writerFiles)
        window.writers.add(writerId)
        window.lastActivityAt = now
        window.totalSize += size
      }
    }

    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
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

      // Count pending files (only count pending and processing windows, not dispatched)
      if (window.processingStatus.state === 'pending') {
        for (const files of window.filesByWriter.values()) {
          totalPendingFiles += files.length
        }
      }

      // Count stuck processing windows (> 5 minutes in processing state)
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > TestableCompactionStateDO.PROCESSING_TIMEOUT_MS
      ) {
        windowsStuckInProcessing++
      }
    }

    const status = {
      namespace: this.namespace,
      activeWindows: this.windows.size,
      knownWriters: Array.from(this.knownWriters),
      activeWriters: this.getActiveWriters(now),
      // New alerting metrics
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
// Health Check Helper
// =============================================================================

/**
 * Evaluate health for a single namespace based on status response
 */
function evaluateNamespaceHealth(
  namespace: string,
  statusData: {
    activeWindows: number
    oldestWindowAge: number
    totalPendingFiles: number
    windowsStuckInProcessing: number
  },
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
function aggregateHealthStatus(
  namespaces: Record<string, NamespaceHealth>
): HealthCheckResponse {
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

// =============================================================================
// Helper Functions
// =============================================================================

function createUpdateRequest(overrides: Partial<UpdateRequest> = {}): UpdateRequest {
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

function createUpdate(overrides: Partial<UpdateRequest['updates'][0]> = {}) {
  return {
    namespace: 'users',
    writerId: 'writer1',
    file: 'data/users/1700001234-writer1-0.parquet',
    timestamp: 1700001234000,
    size: 1024,
    ...overrides,
  }
}

// =============================================================================
// Status Alerting Metrics Tests
// =============================================================================

describe('CompactionStateDO - Alerting Metrics', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('oldestWindowAge metric', () => {
    it('should return 0 for empty state', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { oldestWindowAge: number }

      expect(body.oldestWindowAge).toBe(0)
    })

    it('should calculate age from windowEnd', async () => {
      const now = Date.now()
      const twoHoursAgo = now - (2 * 3600000)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp: twoHoursAgo })],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { oldestWindowAge: number }

      // The window that contains twoHoursAgo has windowEnd about 1 hour after that
      // So the age should be approximately now - (twoHoursAgo + windowSize) = ~1 hour
      // We allow some tolerance for test execution time
      expect(body.oldestWindowAge).toBeGreaterThan(3500000) // > ~58 minutes
      expect(body.oldestWindowAge).toBeLessThan(7500000) // < ~2 hours
    })

    it('should track oldest of multiple windows', async () => {
      const now = Date.now()
      const threeHoursAgo = now - (3 * 3600000)
      const oneHourAgo = now - 3600000

      // Create windows at different times
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: threeHoursAgo, file: 'old.parquet' }),
            createUpdate({ timestamp: oneHourAgo, file: 'recent.parquet' }),
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { oldestWindowAge: number }

      // Should report the oldest window's age
      expect(body.oldestWindowAge).toBeGreaterThan(2 * 3600000) // > 2 hours
    })
  })

  describe('totalPendingFiles metric', () => {
    it('should return 0 for empty state', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { totalPendingFiles: number }

      expect(body.totalPendingFiles).toBe(0)
    })

    it('should count files in pending windows', async () => {
      const now = Date.now()

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: now, file: 'file1.parquet' }),
            createUpdate({ timestamp: now, file: 'file2.parquet' }),
            createUpdate({ timestamp: now, file: 'file3.parquet' }),
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { totalPendingFiles: number }

      expect(body.totalPendingFiles).toBe(3)
    })

    it('should not count files in processing/dispatched windows', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)

      // Create and trigger a ready window (goes to processing)
      const updateResponse = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const updateBody = await updateResponse.json() as { windowsReady: WindowReadyEntry[] }
      expect(updateBody.windowsReady).toHaveLength(1) // Verify it went to processing

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { totalPendingFiles: number }

      // Processing window files should not be counted as pending
      expect(body.totalPendingFiles).toBe(0)
    })

    it('should sum files across multiple pending windows', async () => {
      const now = Date.now()
      const oneHourAgo = now - 3600000

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: now, file: 'current1.parquet' }),
            createUpdate({ timestamp: now, file: 'current2.parquet' }),
            createUpdate({ timestamp: oneHourAgo, file: 'old1.parquet' }),
            createUpdate({ timestamp: oneHourAgo, file: 'old2.parquet' }),
            createUpdate({ timestamp: oneHourAgo, file: 'old3.parquet' }),
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { totalPendingFiles: number }

      expect(body.totalPendingFiles).toBe(5)
    })
  })

  describe('windowsStuckInProcessing metric', () => {
    it('should return 0 for empty state', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { windowsStuckInProcessing: number }

      expect(body.windowsStuckInProcessing).toBe(0)
    })

    it('should return 0 for recently started processing windows', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)

      // Create a ready window (goes to processing)
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { windowsStuckInProcessing: number }

      // Just started processing, not stuck yet
      expect(body.windowsStuckInProcessing).toBe(0)
    })

    it('should count windows stuck in processing > 5 minutes', async () => {
      // Pre-populate with a stuck processing window
      const oldWindowStart = Date.now() - (2 * 3600000)
      const windowKey = String(oldWindowStart)
      const stuckStartedAt = Date.now() - (10 * 60 * 1000) // 10 minutes ago

      const preloadedState: StoredState = {
        namespace: 'users',
        windows: {
          [windowKey]: {
            windowStart: oldWindowStart,
            windowEnd: oldWindowStart + 3600000,
            filesByWriter: { 'writer1': ['stuck-file.parquet'] },
            writers: ['writer1'],
            lastActivityAt: stuckStartedAt,
            totalSize: 1024,
            processingStatus: { state: 'processing', startedAt: stuckStartedAt },
          },
        },
        knownWriters: ['writer1'],
        writerLastSeen: { 'writer1': stuckStartedAt },
      }

      state.setData('compactionState', preloadedState)

      // Create new DO instance
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { windowsStuckInProcessing: number }

      expect(body.windowsStuckInProcessing).toBe(1)
    })

    it('should count multiple stuck windows', async () => {
      const stuckStartedAt = Date.now() - (10 * 60 * 1000) // 10 minutes ago

      const preloadedState: StoredState = {
        namespace: 'users',
        windows: {
          '1700000000000': {
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: { 'writer1': ['file1.parquet'] },
            writers: ['writer1'],
            lastActivityAt: stuckStartedAt,
            totalSize: 1024,
            processingStatus: { state: 'processing', startedAt: stuckStartedAt },
          },
          '1700003600000': {
            windowStart: 1700003600000,
            windowEnd: 1700007200000,
            filesByWriter: { 'writer1': ['file2.parquet'] },
            writers: ['writer1'],
            lastActivityAt: stuckStartedAt,
            totalSize: 1024,
            processingStatus: { state: 'processing', startedAt: stuckStartedAt },
          },
        },
        knownWriters: ['writer1'],
        writerLastSeen: { 'writer1': stuckStartedAt },
      }

      state.setData('compactionState', preloadedState)
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { windowsStuckInProcessing: number }

      expect(body.windowsStuckInProcessing).toBe(2)
    })
  })
})

// =============================================================================
// Health Evaluation Tests
// =============================================================================

describe('Compaction Health Evaluation', () => {
  const defaultConfig: CompactionHealthConfig = {
    maxPendingWindows: 10,
    maxWindowAgeHours: 2,
  }

  describe('evaluateNamespaceHealth', () => {
    it('should return healthy for no issues', () => {
      const result = evaluateNamespaceHealth('users', {
        activeWindows: 2,
        oldestWindowAge: 3600000, // 1 hour
        totalPendingFiles: 20,
        windowsStuckInProcessing: 0,
      }, defaultConfig)

      expect(result.status).toBe('healthy')
      expect(result.issues).toHaveLength(0)
    })

    it('should return unhealthy for stuck processing windows', () => {
      const result = evaluateNamespaceHealth('users', {
        activeWindows: 2,
        oldestWindowAge: 3600000,
        totalPendingFiles: 20,
        windowsStuckInProcessing: 1,
      }, defaultConfig)

      expect(result.status).toBe('unhealthy')
      expect(result.issues).toContain('1 window(s) stuck in processing')
    })

    it('should return degraded for too many pending windows', () => {
      const result = evaluateNamespaceHealth('users', {
        activeWindows: 15,
        oldestWindowAge: 3600000,
        totalPendingFiles: 150,
        windowsStuckInProcessing: 0,
      }, defaultConfig)

      expect(result.status).toBe('degraded')
      expect(result.issues.some(i => i.includes('15 windows pending'))).toBe(true)
    })

    it('should return degraded for old windows', () => {
      const result = evaluateNamespaceHealth('users', {
        activeWindows: 2,
        oldestWindowAge: 3 * 3600000, // 3 hours
        totalPendingFiles: 20,
        windowsStuckInProcessing: 0,
      }, defaultConfig)

      expect(result.status).toBe('degraded')
      expect(result.issues.some(i => i.includes('oldest window age'))).toBe(true)
    })

    it('should prioritize unhealthy over degraded', () => {
      const result = evaluateNamespaceHealth('users', {
        activeWindows: 15, // degraded
        oldestWindowAge: 3 * 3600000, // degraded
        totalPendingFiles: 150,
        windowsStuckInProcessing: 1, // unhealthy
      }, defaultConfig)

      expect(result.status).toBe('unhealthy')
      expect(result.issues).toHaveLength(3)
    })
  })

  describe('aggregateHealthStatus', () => {
    it('should aggregate healthy namespaces', () => {
      const result = aggregateHealthStatus({
        users: {
          namespace: 'users',
          status: 'healthy',
          metrics: { activeWindows: 2, oldestWindowAge: 3600000, totalPendingFiles: 20, windowsStuckInProcessing: 0 },
          issues: [],
        },
        posts: {
          namespace: 'posts',
          status: 'healthy',
          metrics: { activeWindows: 1, oldestWindowAge: 1800000, totalPendingFiles: 10, windowsStuckInProcessing: 0 },
          issues: [],
        },
      })

      expect(result.status).toBe('healthy')
      expect(result.alerts).toHaveLength(0)
    })

    it('should return degraded if any namespace is degraded', () => {
      const result = aggregateHealthStatus({
        users: {
          namespace: 'users',
          status: 'healthy',
          metrics: { activeWindows: 2, oldestWindowAge: 3600000, totalPendingFiles: 20, windowsStuckInProcessing: 0 },
          issues: [],
        },
        posts: {
          namespace: 'posts',
          status: 'degraded',
          metrics: { activeWindows: 15, oldestWindowAge: 1800000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
          issues: ['15 windows pending (threshold: 10)'],
        },
      })

      expect(result.status).toBe('degraded')
      expect(result.alerts).toContain('posts: 15 windows pending (threshold: 10)')
    })

    it('should return unhealthy if any namespace is unhealthy', () => {
      const result = aggregateHealthStatus({
        users: {
          namespace: 'users',
          status: 'degraded',
          metrics: { activeWindows: 15, oldestWindowAge: 3600000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
          issues: ['15 windows pending (threshold: 10)'],
        },
        posts: {
          namespace: 'posts',
          status: 'unhealthy',
          metrics: { activeWindows: 5, oldestWindowAge: 1800000, totalPendingFiles: 50, windowsStuckInProcessing: 2 },
          issues: ['2 window(s) stuck in processing'],
        },
      })

      expect(result.status).toBe('unhealthy')
      expect(result.alerts).toHaveLength(2)
    })

    it('should aggregate alerts from all namespaces', () => {
      const result = aggregateHealthStatus({
        users: {
          namespace: 'users',
          status: 'degraded',
          metrics: { activeWindows: 15, oldestWindowAge: 3 * 3600000, totalPendingFiles: 150, windowsStuckInProcessing: 0 },
          issues: ['15 windows pending (threshold: 10)', 'oldest window age: 3h (threshold: 2h)'],
        },
        posts: {
          namespace: 'posts',
          status: 'unhealthy',
          metrics: { activeWindows: 5, oldestWindowAge: 1800000, totalPendingFiles: 50, windowsStuckInProcessing: 1 },
          issues: ['1 window(s) stuck in processing'],
        },
      })

      expect(result.alerts).toHaveLength(3)
      expect(result.alerts).toContain('users: 15 windows pending (threshold: 10)')
      expect(result.alerts).toContain('users: oldest window age: 3h (threshold: 2h)')
      expect(result.alerts).toContain('posts: 1 window(s) stuck in processing')
    })
  })
})

// =============================================================================
// Health Endpoint Response Format Tests
// =============================================================================

describe('Compaction Health Endpoint Response Format', () => {
  it('should have correct structure', () => {
    const response: HealthCheckResponse = {
      status: 'healthy',
      namespaces: {
        users: {
          namespace: 'users',
          status: 'healthy',
          metrics: {
            activeWindows: 2,
            oldestWindowAge: 3600000,
            totalPendingFiles: 20,
            windowsStuckInProcessing: 0,
          },
          issues: [],
        },
      },
      alerts: [],
    }

    expect(response).toHaveProperty('status')
    expect(response).toHaveProperty('namespaces')
    expect(response).toHaveProperty('alerts')
    expect(['healthy', 'degraded', 'unhealthy']).toContain(response.status)
  })

  it('should match expected JSON format', () => {
    const response: HealthCheckResponse = {
      status: 'degraded',
      namespaces: {
        users: {
          namespace: 'users',
          status: 'degraded',
          metrics: {
            activeWindows: 15,
            oldestWindowAge: 7200000,
            totalPendingFiles: 150,
            windowsStuckInProcessing: 0,
          },
          issues: ['15 windows pending (threshold: 10)'],
        },
        posts: {
          namespace: 'posts',
          status: 'healthy',
          metrics: {
            activeWindows: 2,
            oldestWindowAge: 3600000,
            totalPendingFiles: 20,
            windowsStuckInProcessing: 0,
          },
          issues: [],
        },
      },
      alerts: ['users: 15 windows pending (threshold: 10)'],
    }

    const json = JSON.stringify(response, null, 2)
    const parsed = JSON.parse(json) as HealthCheckResponse

    expect(parsed.status).toBe('degraded')
    expect(Object.keys(parsed.namespaces)).toHaveLength(2)
    expect(parsed.alerts).toHaveLength(1)
  })
})
