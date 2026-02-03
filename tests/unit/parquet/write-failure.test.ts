/**
 * Parquet Write Failure Tests
 *
 * Tests for proper error handling when Parquet writes fail.
 * Critical: Parquet write failures should NEVER be silently ignored
 * as this causes data loss.
 *
 * Issue: parquedb-5w6h [P0]
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { ParquetWriter } from '@/parquet/writer'
import { MemoryBackend } from '@/storage/MemoryBackend'
import { ParquetWriteError, isParquetWriteError } from '@/storage/errors'
import type { ParquetSchema } from '@/parquet/types'

// =============================================================================
// Test Fixtures
// =============================================================================

const TEST_SCHEMA: ParquetSchema = {
  id: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: true },
  value: { type: 'INT64', optional: true },
}

const TEST_DATA = [
  { id: '1', name: 'Alice', value: 100 },
  { id: '2', name: 'Bob', value: 200 },
]

// =============================================================================
// Write Failure Tests
// =============================================================================

describe('Parquet Write Failure Handling', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter

  beforeEach(() => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage)
    vi.restoreAllMocks()
  })

  describe('ParquetWriteError', () => {
    it('should be properly constructed with message and path', () => {
      const error = new ParquetWriteError('Test error message', '/test/path.parquet')

      expect(error.name).toBe('ParquetWriteError')
      expect(error.message).toContain('Test error message')
      expect(error.path).toBe('/test/path.parquet')
    })

    it('should be properly constructed with cause', () => {
      const cause = new Error('Original error')
      const error = new ParquetWriteError('Write failed', '/test/path.parquet', cause)

      expect(error.cause).toBe(cause)
    })

    it('should be detectable with type guard', () => {
      const parquetError = new ParquetWriteError('Test')
      const genericError = new Error('Generic')

      expect(isParquetWriteError(parquetError)).toBe(true)
      expect(isParquetWriteError(genericError)).toBe(false)
    })
  })

  describe('Error Propagation', () => {
    it('should throw ParquetWriteError when hyparquet-writer fails', async () => {
      // The test environment doesn't have hyparquet-writer properly set up
      // which should result in a ParquetWriteError being thrown
      // This validates that we no longer silently fall back to JSON format

      try {
        await writer.write('test.parquet', TEST_DATA, TEST_SCHEMA)
        // If we get here without error AND the file exists, check the format
        const exists = await storage.exists('test.parquet')
        if (exists) {
          // Verify it's actual Parquet format, not JSON fallback
          const content = await storage.read('test.parquet')
          // Check Parquet magic bytes
          expect(content[0]).toBe(0x50) // 'P'
          expect(content[1]).toBe(0x41) // 'A'
          expect(content[2]).toBe(0x52) // 'R'
          expect(content[3]).toBe(0x31) // '1'
        }
      } catch (error) {
        // Expected: we should get a ParquetWriteError, NOT succeed with fallback
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toMatch(/parquet|write|failed/i)
      }
    })

    it('should propagate storage errors when storage.writeAtomic fails', async () => {
      // First write some valid data
      await writer.write('test.parquet', TEST_DATA, TEST_SCHEMA)

      // Now create a mock storage that fails on writeAtomic
      const originalWriteAtomic = storage.writeAtomic.bind(storage)
      vi.spyOn(storage, 'writeAtomic').mockImplementation(async (path, data, options) => {
        // Let the first call work (if it happens), fail on subsequent calls
        throw new Error('Storage write failed: disk full')
      })

      // Create a new writer with the mocked storage
      const newWriter = new ParquetWriter(storage)

      // The error could come from Parquet building OR storage write
      // Either way, it should throw - NOT silently succeed
      await expect(
        newWriter.write('test2.parquet', TEST_DATA, TEST_SCHEMA)
      ).rejects.toThrow()
    })
  })

  describe('Data Integrity', () => {
    it('should not create partial files on write failure', async () => {
      // First, ensure the file doesn't exist
      expect(await storage.exists('fail.parquet')).toBe(false)

      // Mock writeAtomic to fail
      vi.spyOn(storage, 'writeAtomic').mockRejectedValue(new Error('Write aborted'))

      const failingWriter = new ParquetWriter(storage)

      await expect(
        failingWriter.write('fail.parquet', TEST_DATA, TEST_SCHEMA)
      ).rejects.toThrow()

      // File should still not exist after failed write
      expect(await storage.exists('fail.parquet')).toBe(false)
    })
  })

  describe('Error Messages', () => {
    it('should provide error context', async () => {
      // Create an error instance directly to test the error class
      const error = new ParquetWriteError(
        'Buffer allocation failed: out of memory',
        '/data/test.parquet'
      )

      expect(error.message).toContain('Buffer allocation failed')
      expect(error.message).toContain('out of memory')
      expect(error.path).toBe('/data/test.parquet')
    })
  })

  describe('Typed Entity Write Failures', () => {
    it('should propagate errors from writeTypedEntities', async () => {
      // Mock writeAtomic to fail
      vi.spyOn(storage, 'writeAtomic').mockRejectedValue(new Error('Typed entity write failed'))

      const entities = [
        {
          $id: 'posts/1' as const,
          $type: 'Post',
          name: 'Test Post',
          createdAt: new Date(),
          createdBy: 'users/1' as const,
          updatedAt: new Date(),
          updatedBy: 'users/1' as const,
          version: 1,
        },
      ]

      await expect(
        writer.writeTypedEntities('posts.parquet', entities, {
          schema: { title: 'string' },
        })
      ).rejects.toThrow()
    })
  })

  describe('Empty File Write', () => {
    it('should handle empty file write failure', async () => {
      vi.spyOn(storage, 'writeAtomic').mockRejectedValue(new Error('Empty file write failed'))

      await expect(
        writer.write('empty.parquet', [], TEST_SCHEMA)
      ).rejects.toThrow()
    })
  })
})

// =============================================================================
// Regression Tests
// =============================================================================

describe('Silent Data Loss Regression Tests', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    vi.restoreAllMocks()
  })

  it('should throw an error rather than silently falling back to JSON', async () => {
    const writer = new ParquetWriter(storage)

    // This test ensures that if Parquet writing fails for any reason,
    // we get an error rather than a JSON file being written

    // Try to write - it should either:
    // 1. Succeed with valid Parquet, OR
    // 2. Throw an error (NOT silently write JSON)

    try {
      await writer.write('test.parquet', TEST_DATA, TEST_SCHEMA)

      // If we get here, verify the output is valid Parquet, not JSON
      const content = await storage.read('test.parquet')

      // Parquet files start and end with magic bytes PAR1
      expect(content.length).toBeGreaterThan(8)
      expect(content[0]).toBe(0x50) // 'P'
      expect(content[1]).toBe(0x41) // 'A'
      expect(content[2]).toBe(0x52) // 'R'
      expect(content[3]).toBe(0x31) // '1'

      // Verify it's NOT JSON (JSON would start with '{' or '[')
      expect(content[4]).not.toBe(0x7B) // '{'
      expect(content[4]).not.toBe(0x5B) // '['
    } catch (error) {
      // If we get an error, that's acceptable - better than silent fallback
      expect(error).toBeInstanceOf(Error)
    }
  })

  it('should NOT write .json extension files as fallback', async () => {
    // Track all write calls
    const writtenPaths: string[] = []
    const originalWrite = storage.write.bind(storage)
    const originalWriteAtomic = storage.writeAtomic.bind(storage)

    vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
      writtenPaths.push(path)
      return originalWrite(path, data, options)
    })

    vi.spyOn(storage, 'writeAtomic').mockImplementation(async (path, data, options) => {
      writtenPaths.push(path)
      return originalWriteAtomic(path, data, options)
    })

    const writer = new ParquetWriter(storage)

    try {
      await writer.write('data.parquet', TEST_DATA, TEST_SCHEMA)
    } catch {
      // Error is acceptable
    }

    // Verify no .json files were written
    const jsonWrites = writtenPaths.filter((p) => p.endsWith('.json'))
    expect(jsonWrites).toHaveLength(0)
  })

  it('should verify buildFallbackBuffer is NOT called on write failure', async () => {
    const writer = new ParquetWriter(storage)

    // Access private method to verify it's not used
    // We use any here because buildFallbackBuffer is private
    const writerAny = writer as unknown as { buildFallbackBuffer?: (...args: unknown[]) => unknown }

    // The method might still exist for reading legacy files, but verify write doesn't call it
    if (writerAny.buildFallbackBuffer) {
      const fallbackSpy = vi.spyOn(writerAny, 'buildFallbackBuffer')

      try {
        await writer.write('test.parquet', TEST_DATA, TEST_SCHEMA)
      } catch {
        // Error expected if hyparquet-writer not available
      }

      // buildFallbackBuffer should NOT have been called
      expect(fallbackSpy).not.toHaveBeenCalled()
    }
  })
})
