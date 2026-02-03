/**
 * State Serialization Tests
 *
 * Tests for serializing and deserializing compaction workflow state.
 * Covers:
 * - WindowState serialization (Map -> Record, Set -> Array)
 * - Round-trip integrity (save -> load preserves data)
 * - Edge cases (empty state, large state, special characters)
 */

import { describe, it, expect } from 'vitest'

// =============================================================================
// Types (mirrors production types)
// =============================================================================

/** Runtime window state with JS collection types */
interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
}

/** Serializable window state for storage */
interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
}

/** Runtime consumer state with JS collection types */
interface ConsumerState {
  namespace: string
  windows: Map<string, WindowState>
  knownWriters: Set<string>
  writerLastSeen: Map<string, number>
}

/** Serializable consumer state for storage */
interface StoredState {
  namespace: string
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

// =============================================================================
// Serialization Functions (mirrors production implementation)
// =============================================================================

/**
 * Serialize runtime WindowState to storable format
 */
function serializeWindowState(window: WindowState): StoredWindowState {
  return {
    windowStart: window.windowStart,
    windowEnd: window.windowEnd,
    filesByWriter: Object.fromEntries(window.filesByWriter),
    writers: Array.from(window.writers),
    lastActivityAt: window.lastActivityAt,
    totalSize: window.totalSize,
  }
}

/**
 * Deserialize stored WindowState to runtime format
 */
function deserializeWindowState(stored: StoredWindowState): WindowState {
  return {
    windowStart: stored.windowStart,
    windowEnd: stored.windowEnd,
    filesByWriter: new Map(Object.entries(stored.filesByWriter)),
    writers: new Set(stored.writers),
    lastActivityAt: stored.lastActivityAt,
    totalSize: stored.totalSize,
  }
}

/**
 * Serialize runtime ConsumerState to storable format
 */
function serializeConsumerState(state: ConsumerState): StoredState {
  const windows: Record<string, StoredWindowState> = {}

  for (const [key, window] of state.windows) {
    windows[key] = serializeWindowState(window)
  }

  return {
    namespace: state.namespace,
    windows,
    knownWriters: Array.from(state.knownWriters),
    writerLastSeen: Object.fromEntries(state.writerLastSeen),
  }
}

/**
 * Deserialize stored ConsumerState to runtime format
 */
function deserializeConsumerState(stored: StoredState): ConsumerState {
  const windows = new Map<string, WindowState>()

  for (const [key, sw] of Object.entries(stored.windows)) {
    windows.set(key, deserializeWindowState(sw))
  }

  return {
    namespace: stored.namespace,
    windows,
    knownWriters: new Set(stored.knownWriters),
    writerLastSeen: new Map(Object.entries(stored.writerLastSeen)),
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a test WindowState
 */
function createWindowState(overrides: Partial<WindowState> = {}): WindowState {
  return {
    windowStart: 1700000000000,
    windowEnd: 1700003600000,
    filesByWriter: new Map(),
    writers: new Set(),
    lastActivityAt: 1700001234000,
    totalSize: 0,
    ...overrides,
  }
}

/**
 * Create a test ConsumerState
 */
function createConsumerState(overrides: Partial<ConsumerState> = {}): ConsumerState {
  return {
    namespace: 'users',
    windows: new Map(),
    knownWriters: new Set(),
    writerLastSeen: new Map(),
    ...overrides,
  }
}

/**
 * Deep equality check for WindowState
 */
function windowStatesEqual(a: WindowState, b: WindowState): boolean {
  if (a.windowStart !== b.windowStart) return false
  if (a.windowEnd !== b.windowEnd) return false
  if (a.lastActivityAt !== b.lastActivityAt) return false
  if (a.totalSize !== b.totalSize) return false

  // Compare writers sets
  if (a.writers.size !== b.writers.size) return false
  for (const writer of a.writers) {
    if (!b.writers.has(writer)) return false
  }

  // Compare filesByWriter maps
  if (a.filesByWriter.size !== b.filesByWriter.size) return false
  for (const [writer, files] of a.filesByWriter) {
    const bFiles = b.filesByWriter.get(writer)
    if (!bFiles) return false
    if (files.length !== bFiles.length) return false
    for (let i = 0; i < files.length; i++) {
      if (files[i] !== bFiles[i]) return false
    }
  }

  return true
}

// =============================================================================
// WindowState Serialization Tests
// =============================================================================

describe('State Serialization - WindowState', () => {
  describe('Map to Record conversion', () => {
    it('should convert filesByWriter Map to Record', () => {
      const window = createWindowState({
        filesByWriter: new Map([
          ['writer1', ['file1.parquet', 'file2.parquet']],
          ['writer2', ['file3.parquet']],
        ]),
      })

      const stored = serializeWindowState(window)

      expect(stored.filesByWriter).toBeTypeOf('object')
      expect(Array.isArray(stored.filesByWriter)).toBe(false)
      expect(stored.filesByWriter['writer1']).toEqual(['file1.parquet', 'file2.parquet'])
      expect(stored.filesByWriter['writer2']).toEqual(['file3.parquet'])
    })

    it('should handle empty Map', () => {
      const window = createWindowState({
        filesByWriter: new Map(),
      })

      const stored = serializeWindowState(window)

      expect(stored.filesByWriter).toEqual({})
      expect(Object.keys(stored.filesByWriter)).toHaveLength(0)
    })

    it('should handle Map with many entries', () => {
      const filesByWriter = new Map<string, string[]>()
      for (let i = 0; i < 100; i++) {
        filesByWriter.set(`writer${i}`, [`file${i}.parquet`])
      }

      const window = createWindowState({ filesByWriter })
      const stored = serializeWindowState(window)

      expect(Object.keys(stored.filesByWriter)).toHaveLength(100)
    })
  })

  describe('Set to Array conversion', () => {
    it('should convert writers Set to Array', () => {
      const window = createWindowState({
        writers: new Set(['writer1', 'writer2', 'writer3']),
      })

      const stored = serializeWindowState(window)

      expect(Array.isArray(stored.writers)).toBe(true)
      expect(stored.writers).toContain('writer1')
      expect(stored.writers).toContain('writer2')
      expect(stored.writers).toContain('writer3')
    })

    it('should handle empty Set', () => {
      const window = createWindowState({
        writers: new Set(),
      })

      const stored = serializeWindowState(window)

      expect(stored.writers).toEqual([])
    })

    it('should preserve unique values', () => {
      // Sets automatically deduplicate, verify this persists
      const window = createWindowState({
        writers: new Set(['writer1', 'writer2', 'writer1']), // Duplicate
      })

      const stored = serializeWindowState(window)

      expect(stored.writers).toHaveLength(2)
    })
  })

  describe('numeric fields', () => {
    it('should preserve timestamp values', () => {
      const window = createWindowState({
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        lastActivityAt: 1700001234567,
      })

      const stored = serializeWindowState(window)

      expect(stored.windowStart).toBe(1700000000000)
      expect(stored.windowEnd).toBe(1700003600000)
      expect(stored.lastActivityAt).toBe(1700001234567)
    })

    it('should preserve totalSize', () => {
      const window = createWindowState({
        totalSize: 12345678,
      })

      const stored = serializeWindowState(window)

      expect(stored.totalSize).toBe(12345678)
    })

    it('should handle zero values', () => {
      const window = createWindowState({
        windowStart: 0,
        windowEnd: 3600000,
        lastActivityAt: 0,
        totalSize: 0,
      })

      const stored = serializeWindowState(window)

      expect(stored.windowStart).toBe(0)
      expect(stored.totalSize).toBe(0)
    })

    it('should handle large numeric values', () => {
      const window = createWindowState({
        windowStart: Number.MAX_SAFE_INTEGER - 1000,
        totalSize: Number.MAX_SAFE_INTEGER - 1000,
      })

      const stored = serializeWindowState(window)

      expect(stored.windowStart).toBe(Number.MAX_SAFE_INTEGER - 1000)
      expect(stored.totalSize).toBe(Number.MAX_SAFE_INTEGER - 1000)
    })
  })
})

// =============================================================================
// WindowState Deserialization Tests
// =============================================================================

describe('State Serialization - WindowState Deserialization', () => {
  describe('Record to Map conversion', () => {
    it('should convert filesByWriter Record to Map', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {
          'writer1': ['file1.parquet', 'file2.parquet'],
          'writer2': ['file3.parquet'],
        },
        writers: ['writer1', 'writer2'],
        lastActivityAt: 1700001234000,
        totalSize: 3072,
      }

      const window = deserializeWindowState(stored)

      expect(window.filesByWriter instanceof Map).toBe(true)
      expect(window.filesByWriter.get('writer1')).toEqual(['file1.parquet', 'file2.parquet'])
      expect(window.filesByWriter.get('writer2')).toEqual(['file3.parquet'])
    })

    it('should handle empty Record', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {},
        writers: [],
        lastActivityAt: 1700001234000,
        totalSize: 0,
      }

      const window = deserializeWindowState(stored)

      expect(window.filesByWriter.size).toBe(0)
    })
  })

  describe('Array to Set conversion', () => {
    it('should convert writers Array to Set', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {},
        writers: ['writer1', 'writer2', 'writer3'],
        lastActivityAt: 1700001234000,
        totalSize: 0,
      }

      const window = deserializeWindowState(stored)

      expect(window.writers instanceof Set).toBe(true)
      expect(window.writers.has('writer1')).toBe(true)
      expect(window.writers.has('writer2')).toBe(true)
      expect(window.writers.has('writer3')).toBe(true)
    })

    it('should handle empty Array', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {},
        writers: [],
        lastActivityAt: 1700001234000,
        totalSize: 0,
      }

      const window = deserializeWindowState(stored)

      expect(window.writers.size).toBe(0)
    })

    it('should deduplicate Array values in Set', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {},
        writers: ['writer1', 'writer2', 'writer1'], // Duplicate
        lastActivityAt: 1700001234000,
        totalSize: 0,
      }

      const window = deserializeWindowState(stored)

      expect(window.writers.size).toBe(2)
    })
  })
})

// =============================================================================
// Round-Trip Integrity Tests
// =============================================================================

describe('State Serialization - Round-Trip Integrity', () => {
  describe('WindowState round-trip', () => {
    it('should preserve data through serialize/deserialize cycle', () => {
      const original = createWindowState({
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: new Map([
          ['writer1', ['file1.parquet', 'file2.parquet']],
          ['writer2', ['file3.parquet']],
        ]),
        writers: new Set(['writer1', 'writer2']),
        lastActivityAt: 1700001234000,
        totalSize: 3072,
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })

    it('should handle empty WindowState', () => {
      const original = createWindowState()

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })

    it('should handle WindowState with many writers', () => {
      const filesByWriter = new Map<string, string[]>()
      const writers = new Set<string>()

      for (let i = 0; i < 50; i++) {
        const writerId = `writer${i}`
        filesByWriter.set(writerId, Array.from({ length: 5 }, (_, j) => `${writerId}-file${j}.parquet`))
        writers.add(writerId)
      }

      const original = createWindowState({
        filesByWriter,
        writers,
        totalSize: 50 * 5 * 1024,
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })
  })

  describe('ConsumerState round-trip', () => {
    it('should preserve data through serialize/deserialize cycle', () => {
      const original = createConsumerState({
        namespace: 'users',
        windows: new Map([
          ['users:1700000000000', createWindowState({
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: new Map([['writer1', ['file1.parquet']]]),
            writers: new Set(['writer1']),
          })],
          ['users:1700003600000', createWindowState({
            windowStart: 1700003600000,
            windowEnd: 1700007200000,
            filesByWriter: new Map([['writer2', ['file2.parquet']]]),
            writers: new Set(['writer2']),
          })],
        ]),
        knownWriters: new Set(['writer1', 'writer2', 'writer3']),
        writerLastSeen: new Map([
          ['writer1', 1700001234000],
          ['writer2', 1700001235000],
          ['writer3', 1700001236000],
        ]),
      })

      const stored = serializeConsumerState(original)
      const restored = deserializeConsumerState(stored)

      expect(restored.namespace).toBe(original.namespace)
      expect(restored.windows.size).toBe(original.windows.size)
      expect(restored.knownWriters.size).toBe(original.knownWriters.size)
      expect(restored.writerLastSeen.size).toBe(original.writerLastSeen.size)

      // Verify window contents
      for (const [key, originalWindow] of original.windows) {
        const restoredWindow = restored.windows.get(key)
        expect(restoredWindow).toBeDefined()
        expect(windowStatesEqual(originalWindow, restoredWindow!)).toBe(true)
      }

      // Verify known writers
      for (const writer of original.knownWriters) {
        expect(restored.knownWriters.has(writer)).toBe(true)
      }

      // Verify writer last seen
      for (const [writer, timestamp] of original.writerLastSeen) {
        expect(restored.writerLastSeen.get(writer)).toBe(timestamp)
      }
    })

    it('should handle empty ConsumerState', () => {
      const original = createConsumerState()

      const stored = serializeConsumerState(original)
      const restored = deserializeConsumerState(stored)

      expect(restored.namespace).toBe(original.namespace)
      expect(restored.windows.size).toBe(0)
      expect(restored.knownWriters.size).toBe(0)
      expect(restored.writerLastSeen.size).toBe(0)
    })
  })

  describe('JSON round-trip', () => {
    it('should survive JSON.stringify/parse', () => {
      const original = createWindowState({
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: new Map([
          ['writer1', ['file1.parquet', 'file2.parquet']],
        ]),
        writers: new Set(['writer1']),
        lastActivityAt: 1700001234000,
        totalSize: 2048,
      })

      const stored = serializeWindowState(original)
      const json = JSON.stringify(stored)
      const parsed = JSON.parse(json) as StoredWindowState
      const restored = deserializeWindowState(parsed)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })

    it('should handle ConsumerState through JSON', () => {
      const original = createConsumerState({
        namespace: 'users',
        windows: new Map([
          ['users:1700000000000', createWindowState({
            filesByWriter: new Map([['writer1', ['file.parquet']]]),
            writers: new Set(['writer1']),
          })],
        ]),
        knownWriters: new Set(['writer1', 'writer2']),
        writerLastSeen: new Map([
          ['writer1', 1700001234000],
          ['writer2', 1700001235000],
        ]),
      })

      const stored = serializeConsumerState(original)
      const json = JSON.stringify(stored)
      const parsed = JSON.parse(json) as StoredState
      const restored = deserializeConsumerState(parsed)

      expect(restored.windows.size).toBe(1)
      expect(restored.knownWriters.size).toBe(2)
      expect(restored.writerLastSeen.get('writer1')).toBe(1700001234000)
    })
  })
})

// =============================================================================
// Edge Cases Tests
// =============================================================================

describe('State Serialization - Edge Cases', () => {
  describe('special characters in keys', () => {
    it('should handle writer IDs with special characters', () => {
      const original = createWindowState({
        filesByWriter: new Map([
          ['writer-with-dashes', ['file1.parquet']],
          ['writer_with_underscores', ['file2.parquet']],
          ['writer.with.dots', ['file3.parquet']],
        ]),
        writers: new Set(['writer-with-dashes', 'writer_with_underscores', 'writer.with.dots']),
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })

    it('should handle file paths with special characters', () => {
      const original = createWindowState({
        filesByWriter: new Map([
          ['writer1', [
            'data/users/1700001234-writer1-0.parquet',
            'data/app-v2/users/1700001234-writer1-0.parquet',
            'data/namespace_test/1700001234-writer1-0.parquet',
          ]],
        ]),
        writers: new Set(['writer1']),
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(windowStatesEqual(original, restored)).toBe(true)
    })

    it('should handle namespace with slashes', () => {
      const original = createConsumerState({
        namespace: 'org/team/project/data',
      })

      const stored = serializeConsumerState(original)
      const restored = deserializeConsumerState(stored)

      expect(restored.namespace).toBe('org/team/project/data')
    })
  })

  describe('empty and null-like values', () => {
    it('should handle empty file arrays', () => {
      const original = createWindowState({
        filesByWriter: new Map([
          ['writer1', []],
          ['writer2', ['file.parquet']],
        ]),
        writers: new Set(['writer1', 'writer2']),
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(restored.filesByWriter.get('writer1')).toEqual([])
      expect(restored.filesByWriter.get('writer2')).toEqual(['file.parquet'])
    })

    it('should handle zero timestamp', () => {
      const original = createWindowState({
        windowStart: 0,
        windowEnd: 3600000,
        lastActivityAt: 0,
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(restored.windowStart).toBe(0)
      expect(restored.lastActivityAt).toBe(0)
    })

    it('should handle zero totalSize', () => {
      const original = createWindowState({
        totalSize: 0,
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(restored.totalSize).toBe(0)
    })
  })

  describe('large state', () => {
    it('should handle many windows', () => {
      const windows = new Map<string, WindowState>()
      for (let i = 0; i < 100; i++) {
        const windowStart = 1700000000000 + (i * 3600000)
        windows.set(`users:${windowStart}`, createWindowState({
          windowStart,
          windowEnd: windowStart + 3600000,
        }))
      }

      const original = createConsumerState({ windows })
      const stored = serializeConsumerState(original)
      const restored = deserializeConsumerState(stored)

      expect(restored.windows.size).toBe(100)
    })

    it('should handle many writers', () => {
      const knownWriters = new Set<string>()
      const writerLastSeen = new Map<string, number>()
      for (let i = 0; i < 500; i++) {
        const writerId = `writer${i}`
        knownWriters.add(writerId)
        writerLastSeen.set(writerId, 1700001234000 + i)
      }

      const original = createConsumerState({
        knownWriters,
        writerLastSeen,
      })

      const stored = serializeConsumerState(original)
      const restored = deserializeConsumerState(stored)

      expect(restored.knownWriters.size).toBe(500)
      expect(restored.writerLastSeen.size).toBe(500)
    })

    it('should handle many files per writer', () => {
      const files = Array.from({ length: 1000 }, (_, i) => `file${i}.parquet`)
      const original = createWindowState({
        filesByWriter: new Map([['writer1', files]]),
        writers: new Set(['writer1']),
        totalSize: 1000 * 1024,
      })

      const stored = serializeWindowState(original)
      const restored = deserializeWindowState(stored)

      expect(restored.filesByWriter.get('writer1')).toHaveLength(1000)
    })
  })

  describe('type preservation', () => {
    it('should preserve Map type after deserialization', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: { writer1: ['file.parquet'] },
        writers: ['writer1'],
        lastActivityAt: 1700001234000,
        totalSize: 1024,
      }

      const window = deserializeWindowState(stored)

      expect(window.filesByWriter instanceof Map).toBe(true)
      expect(window.filesByWriter.get).toBeDefined()
      expect(window.filesByWriter.set).toBeDefined()
    })

    it('should preserve Set type after deserialization', () => {
      const stored: StoredWindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: {},
        writers: ['writer1'],
        lastActivityAt: 1700001234000,
        totalSize: 0,
      }

      const window = deserializeWindowState(stored)

      expect(window.writers instanceof Set).toBe(true)
      expect(window.writers.has).toBeDefined()
      expect(window.writers.add).toBeDefined()
    })
  })
})

// =============================================================================
// Durable Object Storage Simulation Tests
// =============================================================================

describe('State Serialization - DO Storage Simulation', () => {
  /**
   * Mock Durable Object storage
   */
  class MockStorage {
    private data: Map<string, unknown> = new Map()

    async put<T>(key: string, value: T): Promise<void> {
      this.data.set(key, JSON.parse(JSON.stringify(value)))
    }

    async get<T>(key: string): Promise<T | undefined> {
      const value = this.data.get(key)
      if (value === undefined) return undefined
      return JSON.parse(JSON.stringify(value)) as T
    }

    async delete(key: string): Promise<boolean> {
      return this.data.delete(key)
    }

    clear(): void {
      this.data.clear()
    }
  }

  it('should persist and restore ConsumerState through mock DO storage', async () => {
    const storage = new MockStorage()

    // Original state
    const original = createConsumerState({
      namespace: 'users',
      windows: new Map([
        ['users:1700000000000', createWindowState({
          windowStart: 1700000000000,
          windowEnd: 1700003600000,
          filesByWriter: new Map([
            ['writer1', ['file1.parquet', 'file2.parquet']],
            ['writer2', ['file3.parquet']],
          ]),
          writers: new Set(['writer1', 'writer2']),
          lastActivityAt: 1700001234000,
          totalSize: 3072,
        })],
      ]),
      knownWriters: new Set(['writer1', 'writer2']),
      writerLastSeen: new Map([
        ['writer1', 1700001234000],
        ['writer2', 1700001235000],
      ]),
    })

    // Save (like CompactionStateDO.saveState)
    const stored = serializeConsumerState(original)
    await storage.put('compactionState', stored)

    // Load (like CompactionStateDO.ensureInitialized)
    const loaded = await storage.get<StoredState>('compactionState')
    expect(loaded).toBeDefined()

    const restored = deserializeConsumerState(loaded!)

    // Verify
    expect(restored.namespace).toBe(original.namespace)
    expect(restored.windows.size).toBe(original.windows.size)

    const originalWindow = original.windows.get('users:1700000000000')!
    const restoredWindow = restored.windows.get('users:1700000000000')!
    expect(windowStatesEqual(originalWindow, restoredWindow)).toBe(true)
  })

  it('should handle initial empty state', async () => {
    const storage = new MockStorage()

    // First access - no stored data
    const loaded = await storage.get<StoredState>('compactionState')
    expect(loaded).toBeUndefined()

    // Initialize with empty state
    const initial = createConsumerState({ namespace: 'users' })
    const stored = serializeConsumerState(initial)
    await storage.put('compactionState', stored)

    // Verify it's saved
    const reloaded = await storage.get<StoredState>('compactionState')
    expect(reloaded).toBeDefined()
    expect(reloaded!.namespace).toBe('users')
    expect(Object.keys(reloaded!.windows)).toHaveLength(0)
  })

  it('should handle multiple save/load cycles', async () => {
    const storage = new MockStorage()
    let state = createConsumerState({ namespace: 'users' })

    // Cycle 1: Add first window
    state.windows.set('users:1700000000000', createWindowState({
      windowStart: 1700000000000,
      windowEnd: 1700003600000,
      filesByWriter: new Map([['writer1', ['file1.parquet']]]),
      writers: new Set(['writer1']),
    }))
    state.knownWriters.add('writer1')
    state.writerLastSeen.set('writer1', 1700001234000)

    await storage.put('compactionState', serializeConsumerState(state))
    let loaded = await storage.get<StoredState>('compactionState')
    state = deserializeConsumerState(loaded!)

    expect(state.windows.size).toBe(1)
    expect(state.knownWriters.size).toBe(1)

    // Cycle 2: Add second window
    state.windows.set('users:1700003600000', createWindowState({
      windowStart: 1700003600000,
      windowEnd: 1700007200000,
      filesByWriter: new Map([['writer2', ['file2.parquet']]]),
      writers: new Set(['writer2']),
    }))
    state.knownWriters.add('writer2')
    state.writerLastSeen.set('writer2', 1700004000000)

    await storage.put('compactionState', serializeConsumerState(state))
    loaded = await storage.get<StoredState>('compactionState')
    state = deserializeConsumerState(loaded!)

    expect(state.windows.size).toBe(2)
    expect(state.knownWriters.size).toBe(2)

    // Cycle 3: Remove first window (compacted)
    state.windows.delete('users:1700000000000')

    await storage.put('compactionState', serializeConsumerState(state))
    loaded = await storage.get<StoredState>('compactionState')
    state = deserializeConsumerState(loaded!)

    expect(state.windows.size).toBe(1)
    expect(state.windows.has('users:1700003600000')).toBe(true)
    // Writers should still be tracked
    expect(state.knownWriters.size).toBe(2)
  })
})
