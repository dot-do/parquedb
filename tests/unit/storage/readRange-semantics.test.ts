/**
 * readRange End Position Semantics Tests
 *
 * This test file verifies that all storage backends implement consistent
 * readRange semantics: end position is EXCLUSIVE.
 *
 * For a file containing bytes [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
 *   readRange(path, 0, 5) should return [0, 1, 2, 3, 4] (5 bytes, NOT 6)
 *   readRange(path, 2, 6) should return [2, 3, 4, 5] (4 bytes)
 *
 * This matches:
 *   - JavaScript Array.slice() behavior
 *   - Python slice notation [start:end]
 *   - Many other programming conventions
 *
 * Note: HTTP Range headers use INCLUSIVE end (bytes=0-4 returns 5 bytes),
 * but our interface abstracts this away for consistency.
 *
 * @see StorageBackend.readRange() in types/storage.ts
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { FsBackend } from '../../../src/storage/FsBackend'
import { DOSqliteBackend, type SqlStorage, type SqlStatement } from '../../../src/storage/DOSqliteBackend'
import type { StorageBackend } from '../../../src/types/storage'
import * as os from 'node:os'
import * as path from 'node:path'
import * as fs from 'node:fs/promises'

/**
 * Test data: bytes 0-9 in sequence
 * This makes it easy to verify exact byte ranges
 */
function createSequentialTestData(): Uint8Array {
  return new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
}

/**
 * Create a mock SqlStorage for DOSqliteBackend testing
 */
function createMockSqlStorage(): SqlStorage {
  const data = new Map<string, { data: ArrayBuffer; size: number; etag: string; created_at: string; updated_at: string }>()
  let schemaInitialized = false

  return {
    exec(_query: string): void {
      schemaInitialized = true
    },
    prepare(query: string): SqlStatement {
      const createStatement = (boundParams: unknown[] = []): SqlStatement => ({
        bind(...params: unknown[]): SqlStatement {
          return createStatement(params)
        },
        first<T>(): T | null {
          if (query.includes('SELECT')) {
            const key = boundParams[0] as string
            const entry = data.get(key)
            if (!entry) return null
            if (query.includes('SELECT data FROM')) {
              return { data: entry.data } as T
            }
            if (query.includes('SELECT 1 FROM')) {
              return { '1': 1 } as T
            }
            if (query.includes('SELECT etag FROM')) {
              return { etag: entry.etag } as T
            }
            if (query.includes('SELECT created_at FROM')) {
              return { created_at: entry.created_at } as T
            }
            return entry as T
          }
          return null
        },
        all<T>(): { results: T[] } {
          if (query.includes('SELECT') && query.includes('LIKE')) {
            const pattern = (boundParams[0] as string).replace('%', '')
            const results: T[] = []
            for (const [key, entry] of data.entries()) {
              if (key.startsWith(pattern)) {
                results.push({ path: key, ...entry } as T)
              }
            }
            return { results }
          }
          return { results: [] }
        },
        run(): { changes: number } {
          if (query.includes('INSERT OR REPLACE')) {
            const [key, dataBlob, size, etag, created_at, updated_at] = boundParams as [string, Uint8Array, number, string, string, string]
            data.set(key, {
              data: dataBlob.buffer.slice(dataBlob.byteOffset, dataBlob.byteOffset + dataBlob.byteLength),
              size,
              etag,
              created_at,
              updated_at,
            })
            return { changes: 1 }
          }
          if (query.includes('DELETE')) {
            const key = boundParams[0] as string
            if (query.includes('LIKE')) {
              const pattern = key.replace('%', '')
              let changes = 0
              for (const k of data.keys()) {
                if (k.startsWith(pattern)) {
                  data.delete(k)
                  changes++
                }
              }
              return { changes }
            }
            const existed = data.has(key)
            data.delete(key)
            return { changes: existed ? 1 : 0 }
          }
          return { changes: 0 }
        },
      })
      return createStatement()
    },
  }
}

// =============================================================================
// Shared Test Suite for readRange Exclusive End Semantics
// =============================================================================

function runReadRangeExclusiveEndTests(
  backendName: string,
  createBackend: () => Promise<{ backend: StorageBackend; cleanup?: () => Promise<void> }>
) {
  describe(`${backendName} - readRange exclusive end semantics`, () => {
    let backend: StorageBackend
    let cleanup: (() => Promise<void>) | undefined
    const testPath = 'readrange-test/sequential.bin'

    beforeEach(async () => {
      const result = await createBackend()
      backend = result.backend
      cleanup = result.cleanup
      await backend.write(testPath, createSequentialTestData())
    })

    afterEach(async () => {
      if (cleanup) {
        await cleanup()
      }
    })

    // =========================================================================
    // Core Exclusive End Tests
    // =========================================================================

    describe('exclusive end position', () => {
      it('readRange(0, 5) should return exactly 5 bytes [0,1,2,3,4], NOT 6 bytes', async () => {
        const result = await backend.readRange(testPath, 0, 5)

        expect(result.length).toBe(5)
        expect(Array.from(result)).toEqual([0, 1, 2, 3, 4])
      })

      it('readRange(0, 10) should return all 10 bytes for 10-byte file', async () => {
        const result = await backend.readRange(testPath, 0, 10)

        expect(result.length).toBe(10)
        expect(Array.from(result)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      })

      it('readRange(2, 6) should return 4 bytes [2,3,4,5]', async () => {
        const result = await backend.readRange(testPath, 2, 6)

        expect(result.length).toBe(4)
        expect(Array.from(result)).toEqual([2, 3, 4, 5])
      })

      it('readRange(5, 6) should return single byte [5]', async () => {
        const result = await backend.readRange(testPath, 5, 6)

        expect(result.length).toBe(1)
        expect(Array.from(result)).toEqual([5])
      })

      it('readRange(9, 10) should return last byte [9]', async () => {
        const result = await backend.readRange(testPath, 9, 10)

        expect(result.length).toBe(1)
        expect(Array.from(result)).toEqual([9])
      })
    })

    // =========================================================================
    // Edge Cases
    // =========================================================================

    describe('edge cases with exclusive end', () => {
      it('readRange(5, 5) should return empty array (zero-length range)', async () => {
        const result = await backend.readRange(testPath, 5, 5)

        expect(result.length).toBe(0)
      })

      it('readRange(0, 0) should return empty array', async () => {
        const result = await backend.readRange(testPath, 0, 0)

        expect(result.length).toBe(0)
      })

      it('end beyond file size should clamp to file size', async () => {
        // File is 10 bytes, requesting 0-100 should return all 10 bytes
        const result = await backend.readRange(testPath, 0, 100)

        expect(result.length).toBe(10)
        expect(Array.from(result)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      })

      it('start at file boundary with end beyond should return remaining bytes', async () => {
        // File is 10 bytes, requesting 8-100 should return [8, 9]
        const result = await backend.readRange(testPath, 8, 100)

        expect(result.length).toBe(2)
        expect(Array.from(result)).toEqual([8, 9])
      })

      it('start beyond file size should return empty array', async () => {
        const result = await backend.readRange(testPath, 100, 200)

        expect(result.length).toBe(0)
      })
    })

    // =========================================================================
    // Consistency with slice() semantics
    // =========================================================================

    describe('consistency with Array.slice() semantics', () => {
      it('should match Array.slice behavior for various ranges', async () => {
        const original = Array.from(createSequentialTestData())

        // Test multiple range combinations
        const testCases = [
          [0, 5],
          [0, 10],
          [2, 8],
          [5, 5],
          [0, 1],
          [9, 10],
          [0, 100], // Beyond end
          [8, 12], // Partially beyond end
        ]

        for (const [start, end] of testCases) {
          const backendResult = await backend.readRange(testPath, start, end)
          const sliceResult = original.slice(start, end)

          expect(Array.from(backendResult)).toEqual(sliceResult)
        }
      })
    })
  })
}

// =============================================================================
// Run Tests for Each Backend
// =============================================================================

describe('readRange Exclusive End Semantics', () => {
  // MemoryBackend
  runReadRangeExclusiveEndTests('MemoryBackend', async () => ({
    backend: new MemoryBackend(),
  }))

  // FsBackend
  runReadRangeExclusiveEndTests('FsBackend', async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-test-'))
    return {
      backend: new FsBackend(tmpDir),
      cleanup: async () => {
        await fs.rm(tmpDir, { recursive: true, force: true })
      },
    }
  })

  // DOSqliteBackend
  runReadRangeExclusiveEndTests('DOSqliteBackend', async () => {
    const mockSql = createMockSqlStorage()
    return {
      backend: new DOSqliteBackend(mockSql),
    }
  })
})
