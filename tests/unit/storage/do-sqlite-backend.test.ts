/**
 * DOSqliteBackend Tests
 *
 * Tests the Durable Object SQLite storage backend using a mock SqlStorage
 * implementation. The DOSqliteBackend defines its own SqlStorage and SqlStatement
 * interfaces, so we can mock them without needing actual Cloudflare APIs.
 *
 * The mock uses a simple in-memory Map to simulate SQLite behavior.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  DOSqliteBackend,
  DOSqliteNotFoundError,
  DOSqliteETagMismatchError,
  DOSqliteFileExistsError,
  type SqlStorage,
  type SqlStatement,
} from '../../../src/storage/DOSqliteBackend'
import {
  StorageError,
  NotFoundError,
  ETagMismatchError,
  AlreadyExistsError,
  isStorageError,
  isNotFoundError,
  isETagMismatchError,
  isAlreadyExistsError,
} from '../../../src/storage/errors'

// =============================================================================
// Mock SqlStorage Implementation
// =============================================================================

interface MockRow {
  path: string
  data: ArrayBuffer
  size: number
  etag: string
  created_at: string
  updated_at: string
}

/**
 * In-memory mock of Cloudflare DO SqlStorage.
 * Implements basic SQL operations (SELECT, INSERT OR REPLACE, DELETE, CREATE TABLE/INDEX).
 */
function createMockSqlStorage(): SqlStorage {
  const rows = new Map<string, MockRow>()

  function createStatement(query: string): SqlStatement {
    let boundParams: unknown[] = []

    const stmt: SqlStatement = {
      bind(...params: unknown[]) {
        boundParams = params
        return stmt
      },
      first<T = Record<string, unknown>>(): T | null {
        const normalized = query.trim().replace(/\s+/g, ' ')

        // SELECT ... FROM parquet_blocks WHERE path = ?
        if (normalized.includes('SELECT') && normalized.includes('WHERE path = ?')) {
          const key = boundParams[0] as string
          const row = rows.get(key)
          if (!row) return null

          // Determine which columns to return
          if (normalized.includes('SELECT data FROM')) {
            return { data: row.data } as T
          }
          if (normalized.includes('SELECT 1 FROM')) {
            return { '1': 1 } as T
          }
          if (normalized.includes('SELECT etag FROM')) {
            return { etag: row.etag } as T
          }
          if (normalized.includes('SELECT created_at FROM')) {
            return { created_at: row.created_at } as T
          }
          if (normalized.includes('SELECT data, etag, created_at FROM')) {
            return { data: row.data, etag: row.etag, created_at: row.created_at } as T
          }
          // SELECT path, size, etag, created_at, updated_at
          return {
            path: row.path,
            size: row.size,
            etag: row.etag,
            created_at: row.created_at,
            updated_at: row.updated_at,
          } as T
        }

        // SELECT ... WHERE path LIKE ? AND path > ?
        if (normalized.includes('SELECT') && normalized.includes('WHERE path LIKE ?') && normalized.includes('path > ?')) {
          const pattern = boundParams[0] as string
          const cursor = boundParams[1] as string
          const prefix = pattern.replace(/%$/, '')
          const matchingRows: MockRow[] = []

          for (const [key, row] of rows) {
            if (key.startsWith(prefix) && key > cursor) {
              matchingRows.push(row)
            }
          }
          matchingRows.sort((a, b) => a.path.localeCompare(b.path))
          return matchingRows.length > 0 ? matchingRows[0] as unknown as T : null
        }

        // SELECT ... WHERE path LIKE ?
        if (normalized.includes('SELECT') && normalized.includes('WHERE path LIKE ?')) {
          const pattern = boundParams[0] as string
          const prefix = pattern.replace(/%$/, '')

          for (const [key, row] of rows) {
            if (key.startsWith(prefix)) {
              return {
                path: row.path,
                size: row.size,
                etag: row.etag,
                created_at: row.created_at,
                updated_at: row.updated_at,
              } as T
            }
          }
          return null
        }

        return null
      },
      all<T = Record<string, unknown>>(): { results: T[] } {
        const normalized = query.trim().replace(/\s+/g, ' ')

        // SELECT ... WHERE path LIKE ? AND path > ? ORDER BY path LIMIT ?
        if (normalized.includes('WHERE path LIKE ?') && normalized.includes('path > ?')) {
          const pattern = boundParams[0] as string
          const cursor = boundParams[1] as string
          const prefix = pattern.replace(/%$/, '')
          let limit = Infinity
          if (normalized.includes('LIMIT ?')) {
            limit = boundParams[2] as number
          }

          const matchingRows: MockRow[] = []
          for (const [key, row] of rows) {
            if (key.startsWith(prefix) && key > cursor) {
              matchingRows.push(row)
            }
          }
          matchingRows.sort((a, b) => a.path.localeCompare(b.path))
          const limited = matchingRows.slice(0, limit)

          return {
            results: limited.map(r => ({
              path: r.path,
              size: r.size,
              etag: r.etag,
              created_at: r.created_at,
              updated_at: r.updated_at,
            })) as T[],
          }
        }

        // SELECT ... WHERE path LIKE ? ORDER BY path LIMIT ?
        if (normalized.includes('WHERE path LIKE ?')) {
          const pattern = boundParams[0] as string
          const prefix = pattern.replace(/%$/, '')
          let limit = Infinity
          if (normalized.includes('LIMIT ?')) {
            limit = boundParams[boundParams.length - 1] as number
          }

          const matchingRows: MockRow[] = []
          for (const [key, row] of rows) {
            if (key.startsWith(prefix)) {
              matchingRows.push(row)
            }
          }
          matchingRows.sort((a, b) => a.path.localeCompare(b.path))
          const limited = matchingRows.slice(0, limit)

          return {
            results: limited.map(r => ({
              path: r.path,
              size: r.size,
              etag: r.etag,
              created_at: r.created_at,
              updated_at: r.updated_at,
            })) as T[],
          }
        }

        return { results: [] }
      },
      run(): { changes: number } {
        const normalized = query.trim().replace(/\s+/g, ' ')

        // INSERT OR REPLACE INTO parquet_blocks
        if (normalized.includes('INSERT OR REPLACE INTO parquet_blocks')) {
          const path = boundParams[0] as string
          const data = boundParams[1] as Uint8Array
          const size = boundParams[2] as number
          const etag = boundParams[3] as string
          const createdAt = boundParams[4] as string
          const updatedAt = boundParams[5] as string

          // Convert Uint8Array to ArrayBuffer for storage
          const buffer = data instanceof Uint8Array ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data as ArrayBuffer

          rows.set(path, {
            path,
            data: buffer,
            size,
            etag,
            created_at: createdAt,
            updated_at: updatedAt,
          })
          return { changes: 1 }
        }

        // DELETE FROM parquet_blocks WHERE path = ?
        if (normalized.includes('DELETE FROM parquet_blocks WHERE path = ?')) {
          const key = boundParams[0] as string
          const existed = rows.has(key)
          rows.delete(key)
          return { changes: existed ? 1 : 0 }
        }

        // DELETE FROM parquet_blocks WHERE path LIKE ?
        if (normalized.includes('DELETE FROM parquet_blocks WHERE path LIKE ?')) {
          const pattern = boundParams[0] as string
          const prefix = pattern.replace(/%$/, '')
          let count = 0
          for (const key of Array.from(rows.keys())) {
            if (key.startsWith(prefix)) {
              rows.delete(key)
              count++
            }
          }
          return { changes: count }
        }

        return { changes: 0 }
      },
    }

    return stmt
  }

  return {
    exec(_query: string): void {
      // Schema creation - no-op for mock
    },
    prepare(query: string): SqlStatement {
      return createStatement(query)
    },
  }
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
// Test Suite
// =============================================================================

describe('DOSqliteBackend', () => {
  let sql: SqlStorage
  let backend: DOSqliteBackend

  beforeEach(() => {
    sql = createMockSqlStorage()
    backend = new DOSqliteBackend(sql)
  })

  // ===========================================================================
  // Constructor & Type
  // ===========================================================================

  describe('constructor and type', () => {
    it('should have type "do-sqlite"', () => {
      expect(backend.type).toBe('do-sqlite')
    })

    it('should accept an optional prefix', () => {
      const prefixed = new DOSqliteBackend(sql, { prefix: 'tenant1' })
      expect(prefixed.type).toBe('do-sqlite')
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('read(path)', () => {
    it('should read a previously written file', async () => {
      await backend.write('test/file.txt', textToBytes('hello'))
      const result = await backend.read('test/file.txt')
      expect(bytesToText(result)).toBe('hello')
    })

    it('should throw DOSqliteNotFoundError for non-existent file', async () => {
      await expect(backend.read('nonexistent.txt')).rejects.toThrow(DOSqliteNotFoundError)
    })

    it('should include path in the error message', async () => {
      await expect(backend.read('missing/file.txt')).rejects.toThrow('missing/file.txt')
    })

    it('should read binary data correctly', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe])
      await backend.write('test/binary.bin', binaryData)
      const result = await backend.read('test/binary.bin')
      expect(result).toEqual(binaryData)
    })
  })

  describe('readRange(path, start, end)', () => {
    it('should read a byte range from file', async () => {
      await backend.write('test/range.txt', textToBytes('Hello, World!'))
      const result = await backend.readRange('test/range.txt', 7, 12)
      expect(bytesToText(result)).toBe('World')
    })

    it('should throw DOSqliteNotFoundError for non-existent file', async () => {
      await expect(backend.readRange('nonexistent.txt', 0, 10)).rejects.toThrow(DOSqliteNotFoundError)
    })

    it('should throw for negative start', async () => {
      await backend.write('test/file.txt', textToBytes('data'))
      await expect(backend.readRange('test/file.txt', -1, 10)).rejects.toThrow()
    })

    it('should throw when end < start', async () => {
      await backend.write('test/file.txt', textToBytes('data'))
      await expect(backend.readRange('test/file.txt', 10, 5)).rejects.toThrow()
    })

    it('should clamp end to file length', async () => {
      await backend.write('test/short.txt', textToBytes('ABC'))
      const result = await backend.readRange('test/short.txt', 0, 100)
      expect(bytesToText(result)).toBe('ABC')
    })
  })

  // ===========================================================================
  // exists() and stat()
  // ===========================================================================

  describe('exists(path)', () => {
    it('should return true for existing file', async () => {
      await backend.write('test/exists.txt', textToBytes('data'))
      expect(await backend.exists('test/exists.txt')).toBe(true)
    })

    it('should return false for non-existing file', async () => {
      expect(await backend.exists('nope.txt')).toBe(false)
    })

    it('should return false after file is deleted', async () => {
      await backend.write('test/del.txt', textToBytes('data'))
      await backend.delete('test/del.txt')
      expect(await backend.exists('test/del.txt')).toBe(false)
    })
  })

  describe('stat(path)', () => {
    it('should return FileStat for existing file', async () => {
      await backend.write('test/stat.txt', textToBytes('hello'))
      const stat = await backend.stat('test/stat.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(5)
      expect(stat!.isDirectory).toBe(false)
      expect(stat!.etag).toBeDefined()
      expect(stat!.path).toBe('test/stat.txt')
    })

    it('should return null for non-existent file', async () => {
      const stat = await backend.stat('nonexistent.txt')
      expect(stat).toBeNull()
    })
  })

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  describe('write(path, data, options)', () => {
    it('should write and return WriteResult with etag and size', async () => {
      const data = textToBytes('test content')
      const result = await backend.write('test/write.txt', data)
      expect(result.etag).toBeDefined()
      expect(typeof result.etag).toBe('string')
      expect(result.size).toBe(data.length)
    })

    it('should overwrite existing file', async () => {
      await backend.write('test/file.txt', textToBytes('first'))
      await backend.write('test/file.txt', textToBytes('second'))
      const result = await backend.read('test/file.txt')
      expect(bytesToText(result)).toBe('second')
    })

    it('should throw DOSqliteFileExistsError with ifNoneMatch: *', async () => {
      await backend.write('test/file.txt', textToBytes('existing'))
      await expect(
        backend.write('test/file.txt', textToBytes('new'), { ifNoneMatch: '*' })
      ).rejects.toThrow(DOSqliteFileExistsError)
    })

    it('should throw DOSqliteETagMismatchError with wrong ifMatch', async () => {
      await backend.write('test/file.txt', textToBytes('existing'))
      await expect(
        backend.write('test/file.txt', textToBytes('new'), { ifMatch: 'wrong-etag' })
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })

    it('should succeed with correct ifMatch etag', async () => {
      const writeResult = await backend.write('test/file.txt', textToBytes('existing'))
      const result = await backend.write('test/file.txt', textToBytes('updated'), {
        ifMatch: writeResult.etag,
      })
      expect(result.size).toBe(7) // 'updated'.length
    })

    it('should write empty data', async () => {
      const result = await backend.write('test/empty.txt', new Uint8Array(0))
      expect(result.size).toBe(0)
    })
  })

  describe('writeAtomic(path, data, options)', () => {
    it('should behave like write (SQLite is inherently atomic)', async () => {
      const data = textToBytes('atomic')
      const result = await backend.writeAtomic('test/atomic.txt', data)
      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)

      const readResult = await backend.read('test/atomic.txt')
      expect(bytesToText(readResult)).toBe('atomic')
    })
  })

  describe('append(path, data)', () => {
    it('should append to an existing file', async () => {
      await backend.write('test/append.txt', textToBytes('hello'))
      await backend.append('test/append.txt', textToBytes(' world'))
      const result = await backend.read('test/append.txt')
      expect(bytesToText(result)).toBe('hello world')
    })

    it('should create new file if it does not exist', async () => {
      await backend.append('test/new.txt', textToBytes('new content'))
      const result = await backend.read('test/new.txt')
      expect(bytesToText(result)).toBe('new content')
    })
  })

  // ===========================================================================
  // Delete Operations
  // ===========================================================================

  describe('delete(path)', () => {
    it('should return true when file existed', async () => {
      await backend.write('test/del.txt', textToBytes('data'))
      expect(await backend.delete('test/del.txt')).toBe(true)
    })

    it('should return false when file did not exist', async () => {
      expect(await backend.delete('nonexistent.txt')).toBe(false)
    })

    it('should actually remove the file', async () => {
      await backend.write('test/del.txt', textToBytes('data'))
      await backend.delete('test/del.txt')
      expect(await backend.exists('test/del.txt')).toBe(false)
    })
  })

  describe('deletePrefix(prefix)', () => {
    it('should delete all files with prefix and return count', async () => {
      await backend.write('prefix/a.txt', textToBytes('a'))
      await backend.write('prefix/b.txt', textToBytes('b'))
      await backend.write('other/c.txt', textToBytes('c'))

      const count = await backend.deletePrefix('prefix/')
      expect(count).toBe(2)
      expect(await backend.exists('prefix/a.txt')).toBe(false)
      expect(await backend.exists('other/c.txt')).toBe(true)
    })

    it('should return 0 when no files match', async () => {
      const count = await backend.deletePrefix('nonexistent/')
      expect(count).toBe(0)
    })
  })

  // ===========================================================================
  // Directory Operations
  // ===========================================================================

  describe('mkdir(path)', () => {
    it('should be a no-op (SQLite has no real directories)', async () => {
      // Should not throw
      await expect(backend.mkdir('some/dir')).resolves.toBeUndefined()
    })
  })

  describe('rmdir(path, options)', () => {
    it('should delete files recursively when recursive option is set', async () => {
      await backend.write('dir/a.txt', textToBytes('a'))
      await backend.write('dir/b.txt', textToBytes('b'))
      await backend.rmdir('dir', { recursive: true })
      expect(await backend.exists('dir/a.txt')).toBe(false)
      expect(await backend.exists('dir/b.txt')).toBe(false)
    })

    it('should be a no-op without recursive option', async () => {
      await backend.write('dir/a.txt', textToBytes('a'))
      // Non-recursive rmdir is a no-op for this backend
      await expect(backend.rmdir('dir')).resolves.toBeUndefined()
      // File should still exist
      expect(await backend.exists('dir/a.txt')).toBe(true)
    })
  })

  // ===========================================================================
  // Conditional / Atomic Operations
  // ===========================================================================

  describe('writeConditional(path, data, expectedVersion, options)', () => {
    it('should succeed when expectedVersion is null and file does not exist', async () => {
      const result = await backend.writeConditional('test/cond.txt', textToBytes('new'), null)
      expect(result.etag).toBeDefined()
    })

    it('should throw DOSqliteETagMismatchError when expectedVersion is null but file exists', async () => {
      await backend.write('test/cond.txt', textToBytes('existing'))
      await expect(
        backend.writeConditional('test/cond.txt', textToBytes('new'), null)
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })

    it('should succeed when expectedVersion matches current etag', async () => {
      const writeResult = await backend.write('test/cond.txt', textToBytes('v1'))
      const result = await backend.writeConditional(
        'test/cond.txt',
        textToBytes('v2'),
        writeResult.etag
      )
      expect(result.etag).toBeDefined()
      const data = await backend.read('test/cond.txt')
      expect(bytesToText(data)).toBe('v2')
    })

    it('should throw DOSqliteETagMismatchError when expected version does not match', async () => {
      await backend.write('test/cond.txt', textToBytes('v1'))
      await expect(
        backend.writeConditional('test/cond.txt', textToBytes('v2'), 'wrong-etag')
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })

    it('should throw DOSqliteETagMismatchError when file does not exist but version expected', async () => {
      await expect(
        backend.writeConditional('test/cond.txt', textToBytes('data'), 'some-etag')
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })
  })

  // ===========================================================================
  // Copy & Move
  // ===========================================================================

  describe('copy(source, dest)', () => {
    it('should copy file data to destination', async () => {
      await backend.write('test/src.txt', textToBytes('source data'))
      await backend.copy('test/src.txt', 'test/dst.txt')

      const result = await backend.read('test/dst.txt')
      expect(bytesToText(result)).toBe('source data')
      // Source should still exist
      expect(await backend.exists('test/src.txt')).toBe(true)
    })

    it('should throw DOSqliteNotFoundError when source does not exist', async () => {
      await expect(backend.copy('nonexistent.txt', 'dst.txt')).rejects.toThrow(DOSqliteNotFoundError)
    })
  })

  describe('move(source, dest)', () => {
    it('should move file data to destination and remove source', async () => {
      await backend.write('test/src.txt', textToBytes('move data'))
      await backend.move('test/src.txt', 'test/dst.txt')

      const result = await backend.read('test/dst.txt')
      expect(bytesToText(result)).toBe('move data')
      expect(await backend.exists('test/src.txt')).toBe(false)
    })

    it('should throw DOSqliteNotFoundError when source does not exist', async () => {
      await expect(backend.move('nonexistent.txt', 'dst.txt')).rejects.toThrow(DOSqliteNotFoundError)
    })
  })

  // ===========================================================================
  // List Operation
  // ===========================================================================

  describe('list(prefix, options)', () => {
    beforeEach(async () => {
      await backend.write('data/a.txt', textToBytes('a'))
      await backend.write('data/b.txt', textToBytes('b'))
      await backend.write('data/sub/c.txt', textToBytes('c'))
      await backend.write('other/d.txt', textToBytes('d'))
    })

    it('should list files with matching prefix', async () => {
      const result = await backend.list('data/')
      expect(result.files.length).toBeGreaterThanOrEqual(3)
      expect(result.hasMore).toBe(false)
    })

    it('should return empty result for non-matching prefix', async () => {
      const result = await backend.list('nonexistent/')
      expect(result.files).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Prefix Support
  // ===========================================================================

  describe('prefix option', () => {
    it('should prefix all paths transparently', async () => {
      const prefixedBackend = new DOSqliteBackend(sql, { prefix: 'tenant1' })
      await prefixedBackend.write('file.txt', textToBytes('tenant data'))

      const result = await prefixedBackend.read('file.txt')
      expect(bytesToText(result)).toBe('tenant data')

      // File should not be readable without prefix
      const unprefixedBackend = new DOSqliteBackend(sql)
      await expect(unprefixedBackend.read('file.txt')).rejects.toThrow(DOSqliteNotFoundError)
    })

    it('should strip prefix from stat results', async () => {
      const prefixedBackend = new DOSqliteBackend(sql, { prefix: 'tenant1' })
      await prefixedBackend.write('data/file.txt', textToBytes('data'))

      const stat = await prefixedBackend.stat('data/file.txt')
      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('data/file.txt')
    })
  })

  // ===========================================================================
  // Path normalization
  // ===========================================================================

  describe('path normalization', () => {
    it('should strip leading slashes', async () => {
      await backend.write('/leading-slash.txt', textToBytes('data'))
      const result = await backend.read('leading-slash.txt')
      expect(bytesToText(result)).toBe('data')
    })

    it('should strip trailing slashes', async () => {
      await backend.write('trailing-slash/', textToBytes('data'))
      // When normalized, the path becomes 'trailing-slash'
      const exists = await backend.exists('trailing-slash')
      expect(exists).toBe(true)
    })
  })

  // ===========================================================================
  // Error Classes
  // ===========================================================================

  describe('error classes', () => {
    it('DOSqliteNotFoundError should have correct name and path', () => {
      const err = new DOSqliteNotFoundError('test/path.txt')
      expect(err.name).toBe('DOSqliteNotFoundError')
      expect(err.path).toBe('test/path.txt')
      expect(err.message).toContain('test/path.txt')
      expect(err).toBeInstanceOf(Error)
    })

    it('DOSqliteETagMismatchError should have correct name and details', () => {
      const err = new DOSqliteETagMismatchError('test/path.txt', 'expected-etag', 'actual-etag')
      expect(err.name).toBe('DOSqliteETagMismatchError')
      expect(err.path).toBe('test/path.txt')
      expect(err.expectedEtag).toBe('expected-etag')
      expect(err.actualEtag).toBe('actual-etag')
      expect(err.message).toContain('expected-etag')
      expect(err.message).toContain('actual-etag')
      expect(err).toBeInstanceOf(Error)
    })

    it('DOSqliteFileExistsError should have correct name and path', () => {
      const err = new DOSqliteFileExistsError('test/path.txt')
      expect(err.name).toBe('DOSqliteFileExistsError')
      expect(err.path).toBe('test/path.txt')
      expect(err.message).toContain('test/path.txt')
      expect(err).toBeInstanceOf(Error)
    })

    // Test unified error hierarchy
    describe('unified error inheritance', () => {
      it('DOSqliteNotFoundError should extend NotFoundError and StorageError', () => {
        const err = new DOSqliteNotFoundError('test/path.txt')
        expect(err).toBeInstanceOf(NotFoundError)
        expect(err).toBeInstanceOf(StorageError)
        expect(err).toBeInstanceOf(Error)
      })

      it('DOSqliteETagMismatchError should extend ETagMismatchError and StorageError', () => {
        const err = new DOSqliteETagMismatchError('test/path.txt', 'expected', 'actual')
        expect(err).toBeInstanceOf(ETagMismatchError)
        expect(err).toBeInstanceOf(StorageError)
        expect(err).toBeInstanceOf(Error)
      })

      it('DOSqliteFileExistsError should extend AlreadyExistsError and StorageError', () => {
        const err = new DOSqliteFileExistsError('test/path.txt')
        expect(err).toBeInstanceOf(AlreadyExistsError)
        expect(err).toBeInstanceOf(StorageError)
        expect(err).toBeInstanceOf(Error)
      })

      it('unified type guards should work with DOSqlite errors', () => {
        const notFoundErr = new DOSqliteNotFoundError('test.txt')
        expect(isStorageError(notFoundErr)).toBe(true)
        expect(isNotFoundError(notFoundErr)).toBe(true)
        expect(isETagMismatchError(notFoundErr)).toBe(false)
        expect(isAlreadyExistsError(notFoundErr)).toBe(false)

        const mismatchErr = new DOSqliteETagMismatchError('test.txt', 'a', 'b')
        expect(isStorageError(mismatchErr)).toBe(true)
        expect(isNotFoundError(mismatchErr)).toBe(false)
        expect(isETagMismatchError(mismatchErr)).toBe(true)
        expect(isAlreadyExistsError(mismatchErr)).toBe(false)

        const existsErr = new DOSqliteFileExistsError('test.txt')
        expect(isStorageError(existsErr)).toBe(true)
        expect(isNotFoundError(existsErr)).toBe(false)
        expect(isETagMismatchError(existsErr)).toBe(false)
        expect(isAlreadyExistsError(existsErr)).toBe(true)
      })
    })
  })

  // ===========================================================================
  // Cross-method consistency
  // ===========================================================================

  describe('cross-method consistency', () => {
    it('should maintain consistency between write, read, and exists', async () => {
      const path = 'test/consistency.txt'
      const content = textToBytes('consistent')

      expect(await backend.exists(path)).toBe(false)

      await backend.write(path, content)
      expect(await backend.exists(path)).toBe(true)
      const data = await backend.read(path)
      expect(bytesToText(data)).toBe('consistent')

      await backend.delete(path)
      expect(await backend.exists(path)).toBe(false)
      await expect(backend.read(path)).rejects.toThrow(DOSqliteNotFoundError)
    })

    it('should read the same content via read and readRange(0, length)', async () => {
      const content = textToBytes('Hello, World!')
      await backend.write('test/full.txt', content)

      const fullRead = await backend.read('test/full.txt')
      const rangeRead = await backend.readRange('test/full.txt', 0, content.length)

      expect(fullRead).toEqual(rangeRead)
    })
  })
})
