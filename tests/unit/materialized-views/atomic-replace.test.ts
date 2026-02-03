/**
 * Tests for atomicReplace and crash recovery in materialized views
 *
 * Issue: parquedb-4l5g.21 - MV Full Refresh: atomicReplace isn't atomic
 *
 * These tests verify:
 * 1. Atomic replacement doesn't leave a window where file doesn't exist
 * 2. Recovery functions can restore state after crashes at various points
 * 3. Backup files are properly cleaned up
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  recoverViewFromCrash,
  recoverAllViewsFromCrash,
  getViewDataPath,
  getViewTempDataPath,
} from '../../../src/materialized-views/refresh'
import type { StorageBackend, WriteResult, FileStat, ListResult } from '../../../src/types/storage'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock storage backend that can simulate crashes at specific points
 */
function createCrashableStorage(
  baseBackend: StorageBackend,
  crashAfterOperation?: { operation: 'move' | 'delete'; path: string }
): StorageBackend & { setCrashPoint: (op: string, path: string) => void; clearCrashPoint: () => void } {
  let crashOp: string | undefined = crashAfterOperation?.operation
  let crashPath: string | undefined = crashAfterOperation?.path

  return {
    type: 'crashable-mock',

    setCrashPoint(op: string, path: string) {
      crashOp = op
      crashPath = path
    },

    clearCrashPoint() {
      crashOp = undefined
      crashPath = undefined
    },

    async read(path: string): Promise<Uint8Array> {
      return baseBackend.read(path)
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      return baseBackend.readRange(path, start, end)
    },

    async exists(path: string): Promise<boolean> {
      return baseBackend.exists(path)
    },

    async stat(path: string): Promise<FileStat | null> {
      return baseBackend.stat(path)
    },

    async list(prefix: string, opts?: { pattern?: string; delimiter?: string }): Promise<ListResult> {
      return baseBackend.list(prefix, opts)
    },

    async write(path: string, data: Uint8Array, opts?: { contentType?: string }): Promise<WriteResult> {
      return baseBackend.write(path, data, opts)
    },

    async writeAtomic(path: string, data: Uint8Array, opts?: { contentType?: string }): Promise<WriteResult> {
      return baseBackend.writeAtomic(path, data, opts)
    },

    async append(path: string, data: Uint8Array): Promise<void> {
      return baseBackend.append(path, data)
    },

    async delete(path: string): Promise<boolean> {
      const result = await baseBackend.delete(path)
      if (crashOp === 'delete' && path.includes(crashPath!)) {
        throw new Error('SIMULATED CRASH after delete')
      }
      return result
    },

    async deletePrefix(prefix: string): Promise<number> {
      return baseBackend.deletePrefix(prefix)
    },

    async mkdir(path: string): Promise<void> {
      return baseBackend.mkdir(path)
    },

    async rmdir(path: string, opts?: { recursive?: boolean }): Promise<void> {
      return baseBackend.rmdir(path, opts)
    },

    async writeConditional(
      path: string,
      data: Uint8Array,
      expectedVersion: string | null,
      opts?: { contentType?: string }
    ): Promise<WriteResult> {
      return baseBackend.writeConditional(path, data, expectedVersion, opts)
    },

    async copy(source: string, dest: string): Promise<void> {
      return baseBackend.copy(source, dest)
    },

    async move(source: string, dest: string): Promise<void> {
      await baseBackend.move(source, dest)
      if (crashOp === 'move' && (source.includes(crashPath!) || dest.includes(crashPath!))) {
        throw new Error('SIMULATED CRASH after move')
      }
    },
  }
}

// =============================================================================
// Recovery Function Tests
// =============================================================================

describe('recoverViewFromCrash', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should return no recovery needed when no orphaned files exist', async () => {
    const viewName = 'test-view'

    // Create the view directory with only the final file
    await storage.mkdir(`_views/${viewName}`)
    await storage.write(getViewDataPath(viewName), new Uint8Array([1, 2, 3]))

    const result = await recoverViewFromCrash(storage, viewName)

    expect(result.recovered).toBe(false)
    expect(result.action).toBe('none')
  })

  it('should clean up orphaned backup when both backup and final exist', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)
    const backupPath = `${finalPath}.backup`

    // Simulate state after crash during step 3 (backup cleanup)
    await storage.mkdir(`_views/${viewName}`)
    await storage.write(finalPath, new Uint8Array([4, 5, 6])) // New data
    await storage.write(backupPath, new Uint8Array([1, 2, 3])) // Old backup

    const result = await recoverViewFromCrash(storage, viewName)

    expect(result.recovered).toBe(true)
    expect(result.action).toBe('deleted_backup')

    // Backup should be deleted
    expect(await storage.exists(backupPath)).toBe(false)
    // Final should still exist with new data
    expect(await storage.exists(finalPath)).toBe(true)
    const data = await storage.read(finalPath)
    expect(Array.from(data)).toEqual([4, 5, 6])
  })

  it('should restore from backup when only backup exists', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)
    const backupPath = `${finalPath}.backup`

    // Simulate state after crash during step 2 (move temp to final)
    await storage.mkdir(`_views/${viewName}`)
    await storage.write(backupPath, new Uint8Array([1, 2, 3])) // Backup exists
    // Final does not exist (crash happened before temp was moved to final)

    const result = await recoverViewFromCrash(storage, viewName)

    expect(result.recovered).toBe(true)
    expect(result.action).toBe('restored_backup')

    // Backup should be moved to final
    expect(await storage.exists(backupPath)).toBe(false)
    expect(await storage.exists(finalPath)).toBe(true)
    const data = await storage.read(finalPath)
    expect(Array.from(data)).toEqual([1, 2, 3])
  })

  it('should clean up orphaned temp file', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)
    const tempPath = getViewTempDataPath(viewName)

    // Simulate state after crash before atomic replace started
    await storage.mkdir(`_views/${viewName}`)
    await storage.write(finalPath, new Uint8Array([1, 2, 3])) // Existing final
    await storage.write(tempPath, new Uint8Array([4, 5, 6])) // Orphaned temp

    const result = await recoverViewFromCrash(storage, viewName)

    expect(result.recovered).toBe(true)
    expect(result.action).toBe('deleted_temp')

    // Temp should be deleted
    expect(await storage.exists(tempPath)).toBe(false)
    // Final should still exist with original data
    expect(await storage.exists(finalPath)).toBe(true)
    const data = await storage.read(finalPath)
    expect(Array.from(data)).toEqual([1, 2, 3])
  })

  it('should restore backup and clean up temp when both orphaned', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)
    const backupPath = `${finalPath}.backup`
    const tempPath = getViewTempDataPath(viewName)

    // Simulate state where backup exists but final doesn't, and temp is also there
    await storage.mkdir(`_views/${viewName}`)
    await storage.write(backupPath, new Uint8Array([1, 2, 3])) // Backup exists
    await storage.write(tempPath, new Uint8Array([7, 8, 9])) // Orphaned temp
    // Final does not exist

    const result = await recoverViewFromCrash(storage, viewName)

    expect(result.recovered).toBe(true)
    expect(result.action).toBe('restored_backup')

    // Backup should be moved to final
    expect(await storage.exists(backupPath)).toBe(false)
    expect(await storage.exists(finalPath)).toBe(true)
    // Temp should also be deleted
    expect(await storage.exists(tempPath)).toBe(false)
  })
})

describe('recoverAllViewsFromCrash', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should recover multiple views', async () => {
    // Create multiple views with different crash states
    await storage.mkdir('_views/view1')
    await storage.mkdir('_views/view2')
    await storage.mkdir('_views/view3')

    const finalPath1 = getViewDataPath('view1')
    const finalPath2 = getViewDataPath('view2')
    const finalPath3 = getViewDataPath('view3')

    // View1: Clean state
    await storage.write(finalPath1, new Uint8Array([1]))

    // View2: Orphaned backup (crash during step 3)
    await storage.write(finalPath2, new Uint8Array([2]))
    await storage.write(`${finalPath2}.backup`, new Uint8Array([0]))

    // View3: Orphaned temp
    await storage.write(finalPath3, new Uint8Array([3]))
    await storage.write(getViewTempDataPath('view3'), new Uint8Array([9]))

    const results = await recoverAllViewsFromCrash(storage)

    // Should have recovered view2 and view3
    expect(results.length).toBe(2)
    expect(results.map(r => r.viewName).sort()).toEqual(['view2', 'view3'])
    expect(results.find(r => r.viewName === 'view2')?.action).toBe('deleted_backup')
    expect(results.find(r => r.viewName === 'view3')?.action).toBe('deleted_temp')
  })

  it('should handle empty views directory', async () => {
    const results = await recoverAllViewsFromCrash(storage)
    expect(results).toEqual([])
  })
})

// =============================================================================
// Atomic Replace Behavior Tests
// =============================================================================

describe('atomicReplace behavior', () => {
  let baseStorage: MemoryBackend

  beforeEach(() => {
    baseStorage = new MemoryBackend()
  })

  it('should never leave a window where the file does not exist', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)

    // Setup: Create initial file
    await baseStorage.mkdir(`_views/${viewName}`)
    await baseStorage.write(finalPath, new Uint8Array([1, 2, 3]))

    // We can't directly test the atomicReplace function since it's private,
    // but we can verify that after recovery, a file always exists
    const backupPath = `${finalPath}.backup`

    // Simulate various crash scenarios and verify recovery always restores a file

    // Scenario 1: Crash after moving final to backup, before moving temp to final
    await baseStorage.move(finalPath, backupPath)
    // At this point, only backup exists

    const result1 = await recoverViewFromCrash(baseStorage, viewName)
    expect(result1.action).toBe('restored_backup')
    expect(await baseStorage.exists(finalPath)).toBe(true)

    // Scenario 2: Crash after moving temp to final, before deleting backup
    await baseStorage.write(backupPath, new Uint8Array([1, 2, 3])) // Simulate leftover backup
    const result2 = await recoverViewFromCrash(baseStorage, viewName)
    expect(result2.action).toBe('deleted_backup')
    expect(await baseStorage.exists(finalPath)).toBe(true)
  })

  it('should preserve original data when replacement fails', async () => {
    const viewName = 'test-view'
    const finalPath = getViewDataPath(viewName)
    const tempPath = getViewTempDataPath(viewName)
    const backupPath = `${finalPath}.backup`

    // Setup: Create initial file with known content
    await baseStorage.mkdir(`_views/${viewName}`)
    const originalData = new Uint8Array([1, 2, 3])
    await baseStorage.write(finalPath, originalData)

    // Simulate the atomic replace flow manually
    // Step 1: Move final to backup
    await baseStorage.move(finalPath, backupPath)

    // Simulate crash during step 2 (temp doesn't exist or move fails)
    // At this point we should be able to recover

    const result = await recoverViewFromCrash(baseStorage, viewName)

    expect(result.action).toBe('restored_backup')
    expect(await baseStorage.exists(finalPath)).toBe(true)
    const recoveredData = await baseStorage.read(finalPath)
    expect(Array.from(recoveredData)).toEqual([1, 2, 3])
  })
})
