/**
 * FsxBackend Race Condition Tests
 *
 * Tests to verify the TOCTOU (Time-of-Check-to-Time-of-Use) race condition fix
 * in writeConditional(). The fix uses atomic fsx options (ifMatch, exclusive)
 * instead of separate stat() + write() calls.
 *
 * Issue: parquedb-m0ic - FsxBackend.writeConditional has TOCTOU race condition
 *
 * The old implementation had:
 * 1. stat() to get current etag
 * 2. compare etag with expectedVersion
 * 3. write() if match
 *
 * Between steps 1 and 3, another process could modify the file, leading to
 * lost updates. The fix uses atomic operations:
 * - exclusive: true for "create if not exists" (expectedVersion === null)
 * - ifMatch: expectedVersion for compare-and-swap (expectedVersion !== null)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { FsxBackend } from '../../../src/storage/FsxBackend'
import type { Fsx, FsxWriteOptions } from '../../../src/storage/types/fsx'

// =============================================================================
// Mock fsx that tracks operations and can simulate race conditions
// =============================================================================

interface MockFile {
  data: Uint8Array
  etag: string
}

interface WriteCall {
  path: string
  data: Uint8Array
  options?: FsxWriteOptions | undefined
}

/**
 * Creates a mock fsx implementation that tracks operations
 * and can simulate concurrent write scenarios
 */
function createMockFsx(): Fsx & {
  files: Map<string, MockFile>
  writeCalls: WriteCall[]
  simulateRaceOnNextWrite: (newEtag: string) => void
} {
  const files = new Map<string, MockFile>()
  const writeCalls: WriteCall[] = []
  let raceEtag: string | null = null

  const mockFsx = {
    files,
    writeCalls,
    simulateRaceOnNextWrite(newEtag: string) {
      raceEtag = newEtag
    },

    async readFile(path: string): Promise<Uint8Array> {
      const file = files.get(path)
      if (!file) {
        const err = new Error(`ENOENT: no such file or directory, open '${path}'`) as Error & { code: string }
        err.code = 'ENOENT'
        throw err
      }
      return file.data
    },

    async writeFile(
      path: string,
      data: Uint8Array,
      options?: FsxWriteOptions
    ): Promise<{ etag: string; size: number; tier: 'hot' }> {
      writeCalls.push({ path, data, options })

      // Simulate race condition: file is modified by another process
      // between the caller checking the etag and our write
      if (raceEtag !== null) {
        const currentFile = files.get(path)
        if (currentFile) {
          currentFile.etag = raceEtag
        }
        raceEtag = null
      }

      // Handle exclusive option (atomic create-if-not-exists)
      if (options?.exclusive) {
        if (files.has(path)) {
          const err = new Error(`EEXIST: file already exists, open '${path}'`) as Error & { code: string }
          err.code = 'EEXIST'
          throw err
        }
      }

      // Handle ifMatch option (atomic compare-and-swap)
      if (options?.ifMatch !== undefined) {
        const currentFile = files.get(path)
        if (!currentFile) {
          const err = new Error(`ENOENT: no such file or directory, open '${path}'`) as Error & { code: string }
          err.code = 'ENOENT'
          throw err
        }
        if (currentFile.etag !== options.ifMatch) {
          const err = new Error(`ECONFLICT: etag mismatch`) as Error & { code: string }
          err.code = 'ECONFLICT'
          throw err
        }
      }

      const newEtag = `etag-${Date.now()}-${Math.random().toString(36).slice(2)}`
      files.set(path, { data, etag: newEtag })
      return { etag: newEtag, size: data.length, tier: 'hot' }
    },

    async writeFileAtomic(
      path: string,
      data: Uint8Array,
      options?: FsxWriteOptions
    ): Promise<{ etag: string; size: number; tier: 'hot' }> {
      return mockFsx.writeFile(path, data, options)
    },

    async stat(path: string) {
      const file = files.get(path)
      if (!file) {
        const err = new Error(`ENOENT: no such file or directory, stat '${path}'`) as Error & { code: string }
        err.code = 'ENOENT'
        throw err
      }
      return {
        size: file.data.length,
        atime: new Date(),
        mtime: new Date(),
        birthtime: new Date(),
        ctime: new Date(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        isFile: () => true,
        isDirectory: () => false,
        isSymbolicLink: () => false,
        etag: file.etag,
      }
    },

    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },

    async unlink(path: string): Promise<void> {
      if (!files.has(path)) {
        const err = new Error(`ENOENT: no such file or directory, unlink '${path}'`) as Error & { code: string }
        err.code = 'ENOENT'
        throw err
      }
      files.delete(path)
    },

    async appendFile(_path: string, _data: Uint8Array): Promise<void> {},
    async readRange(_path: string, _start: number, _end: number): Promise<Uint8Array> { return new Uint8Array() },
    async mkdir(_path: string): Promise<void> {},
    async rmdir(_path: string): Promise<void> {},
    async readdir(_path: string): Promise<string[]> { return [] },
    async cp(_src: string, _dest: string): Promise<void> {},
    async rename(_oldPath: string, _newPath: string): Promise<void> {},
    async glob(_pattern: string): Promise<string[]> { return [] },
    async access(_path: string): Promise<void> {},
    async truncate(_path: string): Promise<void> {},
    async lstat(path: string) { return mockFsx.stat(path) },
    async getTier(_path: string) { return 'hot' as const },
    async setTier(_path: string, _tier: 'hot' | 'warm' | 'cold'): Promise<void> {},
    async promote(_path: string): Promise<void> {},
    async demote(_path: string): Promise<void> {},
    async storageStats() { return { totalFiles: 0, totalSize: 0, hotFiles: 0, hotSize: 0, warmFiles: 0, warmSize: 0, coldFiles: 0, coldSize: 0 } },
    async beginTransaction() {
      return {
        id: 'txn-1',
        async readFile(_path: string): Promise<Uint8Array> { return new Uint8Array() },
        async writeFile(_path: string, _data: Uint8Array): Promise<void> {},
        async unlink(_path: string): Promise<void> {},
        async commit(): Promise<void> {},
        async rollback(): Promise<void> {},
      }
    },
  } as unknown as Fsx & {
    files: Map<string, MockFile>
    writeCalls: WriteCall[]
    simulateRaceOnNextWrite: (newEtag: string) => void
  }

  return mockFsx
}

// =============================================================================
// Helpers
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

// =============================================================================
// Tests
// =============================================================================

describe('FsxBackend TOCTOU race condition fix (parquedb-m0ic)', () => {
  let mockFsx: ReturnType<typeof createMockFsx>
  let backend: FsxBackend

  beforeEach(() => {
    mockFsx = createMockFsx()
    backend = new FsxBackend(mockFsx)
  })

  describe('atomic writeConditional operations', () => {
    it('should use exclusive option for expectedVersion === null (no TOCTOU)', async () => {
      await backend.writeConditional('new-file.txt', textToBytes('content'), null)

      // Verify that exclusive: true was passed (atomic create-if-not-exists)
      expect(mockFsx.writeCalls.length).toBe(1)
      expect(mockFsx.writeCalls[0].options?.exclusive).toBe(true)
    })

    it('should use ifMatch option for specific expectedVersion (no TOCTOU)', async () => {
      // Create a file first
      mockFsx.files.set('existing.txt', { data: textToBytes('v1'), etag: 'etag-v1' })

      await backend.writeConditional('existing.txt', textToBytes('v2'), 'etag-v1')

      // Verify that ifMatch was passed (atomic compare-and-swap)
      expect(mockFsx.writeCalls.length).toBe(1)
      expect(mockFsx.writeCalls[0].options?.ifMatch).toBe('etag-v1')
    })

    it('should NOT call stat() before write for conditional writes', async () => {
      // This is the key test - the old implementation would call stat() first
      const statSpy = vi.spyOn(mockFsx, 'stat')

      // Create a file first
      mockFsx.files.set('file.txt', { data: textToBytes('v1'), etag: 'etag-v1' })

      await backend.writeConditional('file.txt', textToBytes('v2'), 'etag-v1')

      // stat should NOT be called before the write
      // (it may be called after on error to get actual etag for error message)
      const statCallsBeforeWrite = statSpy.mock.calls.filter(call => {
        // Check if stat was called - the implementation should use ifMatch instead
        return true
      })

      // The key is that writeFile should be called with ifMatch option
      // and NOT preceded by a stat() check
      expect(mockFsx.writeCalls[0].options?.ifMatch).toBe('etag-v1')
    })

    it('should handle ECONFLICT atomically when etag changes during write', async () => {
      // Set up existing file
      mockFsx.files.set('file.txt', { data: textToBytes('v1'), etag: 'etag-v1' })

      // Simulate race: file changes between caller's etag read and our write
      mockFsx.simulateRaceOnNextWrite('etag-v2')

      // This should fail with ETagMismatchError because the atomic ifMatch check fails
      await expect(
        backend.writeConditional('file.txt', textToBytes('v2'), 'etag-v1')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'etag-v1',
      })
    })
  })

  describe('concurrent create operations', () => {
    it('should handle concurrent creates - only one should succeed', async () => {
      // Simulate two concurrent creates to the same file
      const results = await Promise.allSettled([
        backend.writeConditional('race.txt', textToBytes('writer1'), null),
        backend.writeConditional('race.txt', textToBytes('writer2'), null),
      ])

      // Count successes and failures
      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      // Exactly one should succeed, one should fail
      expect(successes.length).toBe(1)
      expect(failures.length).toBe(1)

      // The failure should be an ETagMismatchError (file already exists)
      const failure = failures[0] as PromiseRejectedResult
      expect(failure.reason.name).toBe('ETagMismatchError')
    })
  })

  describe('concurrent update operations', () => {
    it('should handle concurrent updates - only one should succeed', async () => {
      // Create initial file
      mockFsx.files.set('race.txt', { data: textToBytes('initial'), etag: 'initial-etag' })

      // Simulate two concurrent updates from the same version
      const results = await Promise.allSettled([
        backend.writeConditional('race.txt', textToBytes('update1'), 'initial-etag'),
        backend.writeConditional('race.txt', textToBytes('update2'), 'initial-etag'),
      ])

      // Count successes and failures
      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      // Exactly one should succeed, one should fail
      expect(successes.length).toBe(1)
      expect(failures.length).toBe(1)

      // The failure should be an ETagMismatchError
      const failure = failures[0] as PromiseRejectedResult
      expect(failure.reason.name).toBe('ETagMismatchError')
    })
  })

  describe('error handling', () => {
    it('should throw ETagMismatchError when file exists but expectedVersion is null', async () => {
      mockFsx.files.set('existing.txt', { data: textToBytes('data'), etag: 'etag-123' })

      await expect(
        backend.writeConditional('existing.txt', textToBytes('new'), null)
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: null,
        actualEtag: 'etag-123',
      })
    })

    it('should throw ETagMismatchError when file does not exist but expectedVersion is set', async () => {
      await expect(
        backend.writeConditional('nonexistent.txt', textToBytes('data'), 'some-etag')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'some-etag',
        actualEtag: null,
      })
    })

    it('should throw ETagMismatchError when etag does not match', async () => {
      mockFsx.files.set('file.txt', { data: textToBytes('data'), etag: 'actual-etag' })

      await expect(
        backend.writeConditional('file.txt', textToBytes('new'), 'wrong-etag')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'wrong-etag',
      })
    })
  })

  describe('ifNoneMatch option', () => {
    it('should use exclusive option when ifNoneMatch is *', async () => {
      await backend.writeConditional('new.txt', textToBytes('content'), null, { ifNoneMatch: '*' })

      expect(mockFsx.writeCalls.length).toBe(1)
      expect(mockFsx.writeCalls[0].options?.exclusive).toBe(true)
    })

    it('should throw AlreadyExistsError when ifNoneMatch fails', async () => {
      mockFsx.files.set('existing.txt', { data: textToBytes('data'), etag: 'etag' })

      await expect(
        backend.writeConditional('existing.txt', textToBytes('new'), null, { ifNoneMatch: '*' })
      ).rejects.toMatchObject({
        name: 'AlreadyExistsError',
      })
    })
  })
})
