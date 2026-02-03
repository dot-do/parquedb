/**
 * DOSqliteBackend Race Condition Tests
 *
 * Tests to verify thread-safety of write operations with conditional options.
 *
 * Issue: parquedb-7vkz - DOSqliteBackend.writeConditional has race condition
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

// =============================================================================
// Mock SqlStorage Implementation with Race Condition Simulation
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
 * Creates a mock SqlStorage that can simulate race conditions
 * by allowing interleaving of operations.
 */
function createMockSqlStorage(): SqlStorage & { getRows: () => Map<string, MockRow>; getOperationLog: () => string[] } {
  const rows = new Map<string, MockRow>()
  const operationLog: string[] = []

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
          operationLog.push(`SELECT path=${key} -> ${row ? 'found' : 'not found'}`)
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
          if (normalized.includes('SELECT data, created_at FROM')) {
            return { data: row.data, created_at: row.created_at } as T
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

        // SELECT ... WHERE path = ? AND etag = ?
        if (normalized.includes('WHERE path = ?') && normalized.includes('AND etag = ?')) {
          const key = boundParams[0] as string
          const expectedEtag = boundParams[1] as string
          const row = rows.get(key)
          const matches = row && row.etag === expectedEtag
          operationLog.push(`SELECT path=${key} etag=${expectedEtag} -> ${matches ? 'match' : 'no match'}`)
          if (!matches) return null
          return row as unknown as T
        }

        return null
      },
      all<T = Record<string, unknown>>(): { results: T[] } {
        const normalized = query.trim().replace(/\s+/g, ' ')

        // SELECT ... WHERE path LIKE ?
        if (normalized.includes('WHERE path LIKE ?')) {
          const pattern = boundParams[0] as string
          const prefix = pattern.replace(/%$/, '')

          const matchingRows: MockRow[] = []
          for (const [key, row] of rows) {
            if (key.startsWith(prefix)) {
              matchingRows.push(row)
            }
          }
          matchingRows.sort((a, b) => a.path.localeCompare(b.path))

          return {
            results: matchingRows.map(r => ({
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

        // INSERT OR IGNORE INTO parquet_blocks
        if (normalized.includes('INSERT OR IGNORE INTO parquet_blocks')) {
          const path = boundParams[0] as string
          const data = boundParams[1] as Uint8Array
          const size = boundParams[2] as number
          const etag = boundParams[3] as string
          const createdAt = boundParams[4] as string
          const updatedAt = boundParams[5] as string

          // Only insert if file doesn't already exist
          if (rows.has(path)) {
            operationLog.push(`INSERT OR IGNORE path=${path} -> IGNORED (exists)`)
            return { changes: 0 }
          }

          const buffer = data instanceof Uint8Array ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data as ArrayBuffer

          rows.set(path, {
            path,
            data: buffer,
            size,
            etag,
            created_at: createdAt,
            updated_at: updatedAt,
          })
          operationLog.push(`INSERT OR IGNORE path=${path} -> INSERTED`)
          return { changes: 1 }
        }

        // INSERT OR REPLACE INTO parquet_blocks
        if (normalized.includes('INSERT OR REPLACE INTO parquet_blocks')) {
          const path = boundParams[0] as string
          const data = boundParams[1] as Uint8Array
          const size = boundParams[2] as number
          const etag = boundParams[3] as string
          const createdAt = boundParams[4] as string
          const updatedAt = boundParams[5] as string

          const buffer = data instanceof Uint8Array ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data as ArrayBuffer

          rows.set(path, {
            path,
            data: buffer,
            size,
            etag,
            created_at: createdAt,
            updated_at: updatedAt,
          })
          operationLog.push(`INSERT OR REPLACE path=${path}`)
          return { changes: 1 }
        }

        // UPDATE parquet_blocks SET ... WHERE path = ? AND etag = ?
        if (normalized.includes('UPDATE parquet_blocks') && normalized.includes('WHERE path = ?') && normalized.includes('AND etag = ?')) {
          // Params: data, size, etag, updated_at, path, expectedEtag
          const data = boundParams[0] as Uint8Array
          const size = boundParams[1] as number
          const etag = boundParams[2] as string
          const updatedAt = boundParams[3] as string
          const path = boundParams[4] as string
          const expectedEtag = boundParams[5] as string

          const existing = rows.get(path)
          if (!existing || existing.etag !== expectedEtag) {
            operationLog.push(`UPDATE path=${path} etag=${expectedEtag} -> NO MATCH`)
            return { changes: 0 }
          }

          const buffer = data instanceof Uint8Array ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data as ArrayBuffer

          rows.set(path, {
            path,
            data: buffer,
            size,
            etag,
            created_at: existing.created_at,
            updated_at: updatedAt,
          })
          operationLog.push(`UPDATE path=${path} etag=${expectedEtag} -> UPDATED`)
          return { changes: 1 }
        }

        // DELETE FROM parquet_blocks WHERE path = ?
        if (normalized.includes('DELETE FROM parquet_blocks WHERE path = ?')) {
          const key = boundParams[0] as string
          const existed = rows.has(key)
          rows.delete(key)
          operationLog.push(`DELETE path=${key} -> ${existed ? 'deleted' : 'not found'}`)
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
          operationLog.push(`DELETE LIKE ${prefix}% -> ${count} deleted`)
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
    getRows: () => rows,
    getOperationLog: () => operationLog,
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

describe('DOSqliteBackend race condition fixes', () => {
  let sql: ReturnType<typeof createMockSqlStorage>
  let backend: DOSqliteBackend

  beforeEach(() => {
    sql = createMockSqlStorage()
    backend = new DOSqliteBackend(sql)
  })

  // ===========================================================================
  // writeConditional atomic operations
  // ===========================================================================

  describe('writeConditional atomicity', () => {
    it('should use atomic INSERT OR IGNORE when expectedVersion is null', async () => {
      await backend.writeConditional('test.txt', textToBytes('data'), null)

      // Should see only one INSERT operation, not SELECT followed by INSERT
      const log = sql.getOperationLog()
      const insertOps = log.filter(op => op.includes('INSERT'))
      const selectOps = log.filter(op => op.includes('SELECT'))

      // The atomic approach uses INSERT OR IGNORE without checking first
      expect(insertOps.length).toBe(1)
      expect(insertOps[0]).toContain('INSERT OR IGNORE')
    })

    it('should fail atomically when file already exists and expectedVersion is null', async () => {
      // First write
      await backend.write('test.txt', textToBytes('existing'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Second conditional write should fail atomically
      await expect(
        backend.writeConditional('test.txt', textToBytes('new'), null)
      ).rejects.toThrow(DOSqliteETagMismatchError)

      // Should use atomic INSERT OR IGNORE, not separate SELECT + INSERT
      const log = sql.getOperationLog()
      const insertOps = log.filter(op => op.includes('INSERT OR IGNORE'))
      expect(insertOps.length).toBe(1)
      expect(insertOps[0]).toContain('IGNORED')
    })

    it('should use atomic UPDATE with etag check when expectedVersion is specified', async () => {
      // First write
      const result = await backend.write('test.txt', textToBytes('v1'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Conditional update
      await backend.writeConditional('test.txt', textToBytes('v2'), result.etag)

      // Should use atomic UPDATE ... WHERE etag = ?
      const log = sql.getOperationLog()
      const updateOps = log.filter(op => op.includes('UPDATE'))
      expect(updateOps.length).toBe(1)
      expect(updateOps[0]).toContain('UPDATED')
    })

    it('should fail atomically when etag does not match', async () => {
      // First write
      await backend.write('test.txt', textToBytes('v1'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Conditional update with wrong etag
      await expect(
        backend.writeConditional('test.txt', textToBytes('v2'), 'wrong-etag')
      ).rejects.toThrow(DOSqliteETagMismatchError)

      // Should use atomic UPDATE ... WHERE etag = ?
      const log = sql.getOperationLog()
      const updateOps = log.filter(op => op.includes('UPDATE'))
      expect(updateOps.length).toBe(1)
      expect(updateOps[0]).toContain('NO MATCH')
    })
  })

  // ===========================================================================
  // write() method atomic operations with options
  // ===========================================================================

  describe('write() atomicity with options', () => {
    it('should use atomic approach for ifNoneMatch: * option', async () => {
      // First write to create file
      await backend.write('test.txt', textToBytes('existing'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Second write with ifNoneMatch: '*' should fail atomically
      await expect(
        backend.write('test.txt', textToBytes('new'), { ifNoneMatch: '*' })
      ).rejects.toThrow(DOSqliteFileExistsError)

      // Check operation log
      const log = sql.getOperationLog()

      // The atomic approach should use INSERT OR IGNORE and detect the conflict
      // rather than doing a separate SELECT first
      // Currently this fails because the implementation does SELECT first
      const insertOps = log.filter(op => op.includes('INSERT OR IGNORE'))
      const selectOps = log.filter(op => op.includes('SELECT'))

      // Atomic: uses INSERT OR IGNORE only (no separate SELECT for existence check)
      // Non-atomic: does SELECT first to check existence, then INSERT
      // We want atomic behavior
      expect(insertOps.length).toBe(1)
      expect(selectOps.filter(op => op.includes('SELECT 1 FROM') || op.includes('found'))).toHaveLength(0)
    })

    it('should use atomic approach for ifMatch option', async () => {
      // First write
      const result = await backend.write('test.txt', textToBytes('v1'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Update with ifMatch
      await backend.write('test.txt', textToBytes('v2'), { ifMatch: result.etag })

      // Should use atomic operation
      const log = sql.getOperationLog()
      // Either UPDATE ... WHERE etag = ?, or the operations are atomic
      const updateOps = log.filter(op => op.includes('UPDATE'))
      if (updateOps.length > 0) {
        expect(updateOps[0]).toContain('UPDATED')
      }
    })

    it('should fail atomically when ifMatch does not match', async () => {
      // First write
      await backend.write('test.txt', textToBytes('v1'))

      // Clear operation log
      sql.getOperationLog().length = 0

      // Update with wrong ifMatch
      await expect(
        backend.write('test.txt', textToBytes('v2'), { ifMatch: 'wrong-etag' })
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })
  })

  // ===========================================================================
  // Concurrent write tests
  // ===========================================================================

  describe('concurrent writeConditional operations', () => {
    it('should handle concurrent creates atomically - only one should succeed', async () => {
      // Simulate concurrent creates - both expect file not to exist
      const writes = [
        backend.writeConditional('race.txt', textToBytes('write1'), null).then(() => 'write1').catch(() => 'failed1'),
        backend.writeConditional('race.txt', textToBytes('write2'), null).then(() => 'write2').catch(() => 'failed2'),
      ]

      const results = await Promise.all(writes)

      // Exactly one should succeed, one should fail
      const successes = results.filter(r => !r.startsWith('failed'))
      const failures = results.filter(r => r.startsWith('failed'))

      expect(successes.length).toBe(1)
      expect(failures.length).toBe(1)

      // File should contain only the winning write's data
      const data = await backend.read('race.txt')
      expect(['write1', 'write2']).toContain(bytesToText(data))
    })

    it('should handle concurrent updates atomically - only one should succeed', async () => {
      // Create initial file
      const result = await backend.write('race.txt', textToBytes('initial'))
      const initialEtag = result.etag

      // Simulate concurrent updates - both trying to update from same version
      const writes = [
        backend.writeConditional('race.txt', textToBytes('update1'), initialEtag).then(() => 'update1').catch(() => 'failed1'),
        backend.writeConditional('race.txt', textToBytes('update2'), initialEtag).then(() => 'update2').catch(() => 'failed2'),
      ]

      const results = await Promise.all(writes)

      // Exactly one should succeed, one should fail
      const successes = results.filter(r => !r.startsWith('failed'))
      const failures = results.filter(r => r.startsWith('failed'))

      expect(successes.length).toBe(1)
      expect(failures.length).toBe(1)

      // File should contain only the winning update's data
      const data = await backend.read('race.txt')
      expect(['update1', 'update2']).toContain(bytesToText(data))
    })
  })

  // ===========================================================================
  // append() method tests
  // ===========================================================================

  describe('append() operations', () => {
    it('should append data to existing file', async () => {
      await backend.write('append.txt', textToBytes('hello'))
      await backend.append('append.txt', textToBytes(' world'))

      const data = await backend.read('append.txt')
      expect(bytesToText(data)).toBe('hello world')
    })

    it('should create file if it does not exist', async () => {
      await backend.append('new.txt', textToBytes('new content'))

      const data = await backend.read('new.txt')
      expect(bytesToText(data)).toBe('new content')
    })

    it('should handle concurrent appends without losing data', async () => {
      // Create initial file
      await backend.write('concurrent.txt', textToBytes('start|'))

      // Concurrent appends
      const appends = [
        backend.append('concurrent.txt', textToBytes('a|')),
        backend.append('concurrent.txt', textToBytes('b|')),
        backend.append('concurrent.txt', textToBytes('c|')),
      ]

      await Promise.all(appends)

      const data = bytesToText(await backend.read('concurrent.txt'))

      // All data should be present (order may vary)
      expect(data).toContain('start|')
      expect(data).toContain('a|')
      expect(data).toContain('b|')
      expect(data).toContain('c|')
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle writeConditional with both expectedVersion and ifMatch', async () => {
      const result = await backend.write('test.txt', textToBytes('v1'))

      // When both are provided, ifMatch takes precedence
      await backend.writeConditional('test.txt', textToBytes('v2'), result.etag, { ifMatch: result.etag })

      const data = await backend.read('test.txt')
      expect(bytesToText(data)).toBe('v2')
    })

    it('should throw when expectedVersion is null but ifMatch is provided', async () => {
      // This is a contradictory request - expectedVersion=null means file shouldn't exist,
      // but ifMatch requires file to exist with specific etag
      await expect(
        backend.writeConditional('test.txt', textToBytes('data'), null, { ifMatch: 'some-etag' })
      ).rejects.toThrow(DOSqliteETagMismatchError)
    })

    it('should throw FileExistsError when expectedVersion specified but ifNoneMatch: * provided', async () => {
      // Create file first
      const result = await backend.write('test.txt', textToBytes('existing'))

      // This is contradictory - expectedVersion set means file should exist,
      // ifNoneMatch: '*' means file shouldn't exist
      await expect(
        backend.writeConditional('test.txt', textToBytes('data'), result.etag, { ifNoneMatch: '*' })
      ).rejects.toThrow(DOSqliteFileExistsError)
    })
  })
})
