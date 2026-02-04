/**
 * E2E Tests: Studio Error Scenarios
 *
 * Tests error handling with REAL storage backends (no mocks):
 * - Storage errors (NotFoundError, AlreadyExistsError, etc.)
 * - Corrupted/invalid files
 * - Concurrent access handling
 * - Data integrity verification
 *
 * These tests verify that Studio handles errors gracefully.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  createTestBackend,
  cleanupBackend,
  writeCorruptedParquetFile,
  writeTruncatedParquetFile,
  writeEmptyFile,
  retry,
  type BackendType,
} from './setup'
import type { StorageBackend } from '../../../src/types/storage'
import { NotFoundError, ETagMismatchError, AlreadyExistsError } from '../../../src/storage/errors'

describe('E2E: Studio Error Scenarios', () => {
  const backends: BackendType[] = ['memory', 'fs']

  for (const backendType of backends) {
    describe(`with ${backendType} backend`, () => {
      let storage: StorageBackend

      beforeEach(async () => {
        storage = await createTestBackend(backendType)
      })

      afterEach(async () => {
        await cleanupBackend(storage)
      })

      // =========================================================================
      // File Not Found Errors
      // =========================================================================

      describe('File Not Found Errors', () => {
        it('throws NotFoundError when reading non-existent file', async () => {
          await expect(storage.read('does-not-exist.txt')).rejects.toThrow(NotFoundError)
        })

        it('throws NotFoundError when reading range from non-existent file', async () => {
          await expect(storage.readRange('does-not-exist.bin', 0, 100)).rejects.toThrow(NotFoundError)
        })

        it('returns null for stat on non-existent file', async () => {
          const stat = await storage.stat('does-not-exist.txt')
          expect(stat).toBeNull()
        })

        it('returns false for exists on non-existent file', async () => {
          const exists = await storage.exists('does-not-exist.txt')
          expect(exists).toBe(false)
        })

        it('returns false when deleting non-existent file', async () => {
          const result = await storage.delete('does-not-exist.txt')
          expect(result).toBe(false)
        })

        it('returns 0 when deleting non-existent prefix', async () => {
          const count = await storage.deletePrefix('nonexistent-prefix/')
          expect(count).toBe(0)
        })

        it('throws NotFoundError when copying non-existent file', async () => {
          await expect(storage.copy('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
        })

        it('throws NotFoundError when moving non-existent file', async () => {
          await expect(storage.move('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
        })
      })

      // =========================================================================
      // Conditional Write Errors
      // =========================================================================

      describe('Conditional Write Errors', () => {
        it('throws AlreadyExistsError when ifNoneMatch conflicts', async () => {
          await storage.write('existing.txt', new Uint8Array([1]))

          await expect(
            storage.write('existing.txt', new Uint8Array([2]), { ifNoneMatch: '*' })
          ).rejects.toThrow(AlreadyExistsError)
        })

        it('throws ETagMismatchError when ifMatch conflicts', async () => {
          await storage.write('versioned.txt', new Uint8Array([1]))

          await expect(
            storage.write('versioned.txt', new Uint8Array([2]), { ifMatch: 'wrong-etag' })
          ).rejects.toThrow(ETagMismatchError)
        })

        it('succeeds when ifMatch matches current etag', async () => {
          const result = await storage.write('versioned.txt', new Uint8Array([1]))

          await storage.write('versioned.txt', new Uint8Array([2]), {
            ifMatch: result.etag,
          })

          const content = await storage.read('versioned.txt')
          expect(content).toEqual(new Uint8Array([2]))
        })

        it('succeeds when ifNoneMatch and file does not exist', async () => {
          await storage.write('new-file.txt', new Uint8Array([1]), { ifNoneMatch: '*' })

          expect(await storage.exists('new-file.txt')).toBe(true)
        })
      })

      // =========================================================================
      // Corrupted File Handling
      // =========================================================================

      describe('Corrupted File Handling', () => {
        it('creates and reads corrupted parquet file', async () => {
          await writeCorruptedParquetFile(storage, 'corrupted.parquet')

          // File should exist and be readable
          expect(await storage.exists('corrupted.parquet')).toBe(true)

          const content = await storage.read('corrupted.parquet')
          expect(content.length).toBeGreaterThan(0)

          // But should not be valid parquet (doesn't start with PAR1)
          const magic = String.fromCharCode(...content.slice(0, 4))
          expect(magic).not.toBe('PAR1')
        })

        it('creates and reads truncated parquet file', async () => {
          await writeTruncatedParquetFile(storage, 'truncated.parquet')

          expect(await storage.exists('truncated.parquet')).toBe(true)

          const content = await storage.read('truncated.parquet')
          // Should start with PAR1 but be too short
          expect(String.fromCharCode(...content.slice(0, 4))).toBe('PAR1')
          expect(content.length).toBeLessThan(20)
        })

        it('creates and reads empty file', async () => {
          await writeEmptyFile(storage, 'empty.parquet')

          expect(await storage.exists('empty.parquet')).toBe(true)

          const content = await storage.read('empty.parquet')
          expect(content.length).toBe(0)
        })
      })

      // =========================================================================
      // Range Read Edge Cases
      // =========================================================================

      describe('Range Read Edge Cases', () => {
        it('returns empty for range past end of file', async () => {
          await storage.write('small.bin', new Uint8Array([1, 2, 3]))

          const range = await storage.readRange('small.bin', 100, 200)
          expect(range.length).toBe(0)
        })

        it('clamps range end to file size', async () => {
          await storage.write('data.bin', new Uint8Array([0, 1, 2, 3, 4]))

          const range = await storage.readRange('data.bin', 2, 100)
          expect(range).toEqual(new Uint8Array([2, 3, 4]))
        })

        it('returns correct slice for valid range', async () => {
          const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          await storage.write('data.bin', data)

          const range = await storage.readRange('data.bin', 3, 7)
          expect(range).toEqual(new Uint8Array([3, 4, 5, 6]))
        })
      })

      // =========================================================================
      // Concurrent Access
      // =========================================================================

      describe('Concurrent Access', () => {
        it('handles concurrent writes to different files', async () => {
          const writes = Array.from({ length: 10 }, (_, i) =>
            storage.write(`concurrent/file${i}.txt`, new Uint8Array([i]))
          )

          await Promise.all(writes)

          // All files should exist
          for (let i = 0; i < 10; i++) {
            expect(await storage.exists(`concurrent/file${i}.txt`)).toBe(true)
            const content = await storage.read(`concurrent/file${i}.txt`)
            expect(content).toEqual(new Uint8Array([i]))
          }
        })

        it('handles concurrent reads of same file', async () => {
          await storage.write('shared.txt', new TextEncoder().encode('shared content'))

          const reads = Array.from({ length: 10 }, () => storage.read('shared.txt'))

          const results = await Promise.all(reads)

          // All reads should return same content
          for (const result of results) {
            expect(new TextDecoder().decode(result)).toBe('shared content')
          }
        })

        it('handles concurrent appends', async () => {
          // Create initial file
          await storage.write('append.txt', new Uint8Array([]))

          // Concurrent appends
          const appends = Array.from({ length: 5 }, (_, i) =>
            storage.append('append.txt', new Uint8Array([i]))
          )

          await Promise.all(appends)

          // File should have all appended data (though order may vary)
          const content = await storage.read('append.txt')
          expect(content.length).toBe(5)
        })
      })

      // =========================================================================
      // Data Integrity
      // =========================================================================

      describe('Data Integrity', () => {
        it('preserves binary data exactly', async () => {
          // All possible byte values
          const allBytes = new Uint8Array(256)
          for (let i = 0; i < 256; i++) {
            allBytes[i] = i
          }

          await storage.write('binary.bin', allBytes)

          const result = await storage.read('binary.bin')

          expect(result.length).toBe(256)
          for (let i = 0; i < 256; i++) {
            expect(result[i]).toBe(i)
          }
        })

        it('preserves UTF-8 encoding', async () => {
          const text = 'Hello \u4e16\u754c! \u00e9\u00e0\u00fc \ud83d\ude00 \u0627\u0644\u0633\u0644\u0627\u0645'
          await storage.write('unicode.txt', new TextEncoder().encode(text))

          const result = await storage.read('unicode.txt')
          expect(new TextDecoder().decode(result)).toBe(text)
        })

        it('preserves JSON structure', async () => {
          const data = {
            string: 'hello',
            number: 42,
            float: 3.14,
            boolean: true,
            null: null,
            array: [1, 'two', false],
            nested: { a: { b: { c: 'deep' } } },
          }

          await storage.write('data.json', new TextEncoder().encode(JSON.stringify(data)))

          const result = await storage.read('data.json')
          const parsed = JSON.parse(new TextDecoder().decode(result))

          expect(parsed).toEqual(data)
        })

        it('handles large files', async () => {
          const size = 1024 * 1024 // 1MB
          const large = new Uint8Array(size)
          for (let i = 0; i < size; i++) {
            large[i] = i % 256
          }

          await storage.write('large.bin', large)

          const result = await storage.read('large.bin')
          expect(result.length).toBe(size)

          // Spot check
          expect(result[0]).toBe(0)
          expect(result[255]).toBe(255)
          expect(result[256]).toBe(0)
          expect(result[size - 1]).toBe((size - 1) % 256)
        })
      })

      // =========================================================================
      // Retry Logic
      // =========================================================================

      describe('Retry Logic', () => {
        it('retry succeeds on first attempt', async () => {
          let attempts = 0

          const result = await retry(async () => {
            attempts++
            return 'success'
          }, 3)

          expect(result).toBe('success')
          expect(attempts).toBe(1)
        })

        it('retry succeeds after failures', async () => {
          let attempts = 0

          const result = await retry(async () => {
            attempts++
            if (attempts < 3) {
              throw new Error('Simulated failure')
            }
            return 'success'
          }, 5, 10)

          expect(result).toBe('success')
          expect(attempts).toBe(3)
        })

        it('retry throws after max attempts', async () => {
          let attempts = 0

          await expect(
            retry(async () => {
              attempts++
              throw new Error('Always fails')
            }, 3, 10)
          ).rejects.toThrow('Always fails')

          expect(attempts).toBe(3)
        })
      })

      // =========================================================================
      // Edge Cases
      // =========================================================================

      describe('Edge Cases', () => {
        it('handles empty file operations', async () => {
          await storage.write('empty.txt', new Uint8Array(0))

          expect(await storage.exists('empty.txt')).toBe(true)

          const stat = await storage.stat('empty.txt')
          expect(stat!.size).toBe(0)

          const content = await storage.read('empty.txt')
          expect(content.length).toBe(0)
        })

        it('handles paths with special characters', async () => {
          // Note: Some characters may not be allowed in all backends
          await storage.write('special/file-with.dots.txt', new Uint8Array([1]))
          await storage.write('special/file_with_underscores.txt', new Uint8Array([2]))

          expect(await storage.exists('special/file-with.dots.txt')).toBe(true)
          expect(await storage.exists('special/file_with_underscores.txt')).toBe(true)
        })

        it('handles deeply nested paths', async () => {
          const deepPath = 'a/b/c/d/e/f/g/h/i/j/deep.txt'
          await storage.write(deepPath, new Uint8Array([1]))

          expect(await storage.exists(deepPath)).toBe(true)

          const content = await storage.read(deepPath)
          expect(content).toEqual(new Uint8Array([1]))
        })

        it('handles listing empty directory', async () => {
          await storage.mkdir('empty-dir')

          const result = await storage.list('empty-dir/', { delimiter: '/' })
          expect(result.files.length).toBe(0)
        })
      })
    })
  }
})
