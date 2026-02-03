/**
 * TransactionalBackend Test Suite
 *
 * Comprehensive tests for the transactional storage backend wrapper.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  MemoryBackend,
  TransactionalBackend,
  TransactionError,
  TransactionCommitError,
  withTransactions,
  runInTransaction,
  NotFoundError,
  isTransactional,
} from '../../../src/storage'

// Helper to create test data
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

// Helper to decode test data
function decodeData(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

describe('TransactionalBackend', () => {
  let memoryBackend: MemoryBackend
  let backend: TransactionalBackend

  beforeEach(() => {
    memoryBackend = new MemoryBackend()
    backend = new TransactionalBackend(memoryBackend)
  })

  // ===========================================================================
  // Basic Properties
  // ===========================================================================

  describe('type property', () => {
    it('should have type prefixed with "transactional:"', () => {
      expect(backend.type).toBe('transactional:memory')
    })

    it('should expose inner backend', () => {
      expect(backend.inner).toBe(memoryBackend)
    })
  })

  describe('isTransactional type guard', () => {
    it('should return true for TransactionalBackend', () => {
      expect(isTransactional(backend)).toBe(true)
    })

    it('should return false for MemoryBackend', () => {
      expect(isTransactional(memoryBackend)).toBe(false)
    })
  })

  // ===========================================================================
  // Transaction Lifecycle
  // ===========================================================================

  describe('Transaction lifecycle', () => {
    it('should begin a transaction', async () => {
      const tx = await backend.beginTransaction()

      expect(tx).toBeDefined()
      expect(tx.id).toBeDefined()
      expect(typeof tx.id).toBe('string')
    })

    it('should generate unique transaction IDs', async () => {
      const tx1 = await backend.beginTransaction()
      const tx2 = await backend.beginTransaction()

      expect(tx1.id).not.toBe(tx2.id)
    })

    it('should track active transaction count', async () => {
      expect(backend.activeTransactionCount).toBe(0)

      const tx1 = await backend.beginTransaction()
      expect(backend.activeTransactionCount).toBe(1)

      const tx2 = await backend.beginTransaction()
      expect(backend.activeTransactionCount).toBe(2)

      await tx1.commit()
      expect(backend.activeTransactionCount).toBe(1)

      await tx2.rollback()
      expect(backend.activeTransactionCount).toBe(0)
    })
  })

  // ===========================================================================
  // Transaction Read Operations
  // ===========================================================================

  describe('Transaction read operations', () => {
    it('should read existing file from backend', async () => {
      await memoryBackend.write('existing.txt', createTestData('Hello'))

      const tx = await backend.beginTransaction()
      const data = await tx.read('existing.txt')

      expect(decodeData(data)).toBe('Hello')
    })

    it('should throw NotFoundError for non-existent file', async () => {
      const tx = await backend.beginTransaction()

      await expect(tx.read('nonexistent.txt')).rejects.toThrow(NotFoundError)
    })

    it('should read own pending writes', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('new.txt', createTestData('New content'))

      const data = await tx.read('new.txt')

      expect(decodeData(data)).toBe('New content')
    })

    it('should see pending deletes as not found', async () => {
      await memoryBackend.write('to-delete.txt', createTestData('Delete me'))

      const tx = await backend.beginTransaction()
      await tx.delete('to-delete.txt')

      await expect(tx.read('to-delete.txt')).rejects.toThrow(NotFoundError)
    })

    it('should see overwritten content', async () => {
      await memoryBackend.write('file.txt', createTestData('Original'))

      const tx = await backend.beginTransaction()
      await tx.write('file.txt', createTestData('Updated'))

      const data = await tx.read('file.txt')
      expect(decodeData(data)).toBe('Updated')
    })
  })

  // ===========================================================================
  // Transaction Write Operations
  // ===========================================================================

  describe('Transaction write operations', () => {
    it('should buffer writes without affecting backend', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('buffered.txt', createTestData('Buffered'))

      // Should not exist in backend yet
      expect(await memoryBackend.exists('buffered.txt')).toBe(false)
    })

    it('should buffer multiple writes', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('file1.txt', createTestData('Content 1'))
      await tx.write('file2.txt', createTestData('Content 2'))
      await tx.write('file3.txt', createTestData('Content 3'))

      // None should exist in backend yet
      expect(await memoryBackend.exists('file1.txt')).toBe(false)
      expect(await memoryBackend.exists('file2.txt')).toBe(false)
      expect(await memoryBackend.exists('file3.txt')).toBe(false)

      // But all should be readable in transaction
      expect(decodeData(await tx.read('file1.txt'))).toBe('Content 1')
      expect(decodeData(await tx.read('file2.txt'))).toBe('Content 2')
      expect(decodeData(await tx.read('file3.txt'))).toBe('Content 3')
    })

    it('should handle overwriting pending write', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('file.txt', createTestData('First'))
      await tx.write('file.txt', createTestData('Second'))

      const data = await tx.read('file.txt')
      expect(decodeData(data)).toBe('Second')
    })
  })

  // ===========================================================================
  // Transaction Delete Operations
  // ===========================================================================

  describe('Transaction delete operations', () => {
    it('should buffer deletes without affecting backend', async () => {
      await memoryBackend.write('existing.txt', createTestData('Content'))

      const tx = await backend.beginTransaction()
      await tx.delete('existing.txt')

      // Should still exist in backend
      expect(await memoryBackend.exists('existing.txt')).toBe(true)
    })

    it('should mark pending write as deleted', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('new.txt', createTestData('Content'))
      await tx.delete('new.txt')

      await expect(tx.read('new.txt')).rejects.toThrow(NotFoundError)
    })

    it('should handle write after delete', async () => {
      await memoryBackend.write('file.txt', createTestData('Original'))

      const tx = await backend.beginTransaction()
      await tx.delete('file.txt')
      await tx.write('file.txt', createTestData('New content'))

      const data = await tx.read('file.txt')
      expect(decodeData(data)).toBe('New content')
    })
  })

  // ===========================================================================
  // Transaction Commit
  // ===========================================================================

  describe('Transaction commit', () => {
    it('should apply writes on commit', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('new.txt', createTestData('Committed'))
      await tx.commit()

      const data = await memoryBackend.read('new.txt')
      expect(decodeData(data)).toBe('Committed')
    })

    it('should apply multiple writes on commit', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('file1.txt', createTestData('Content 1'))
      await tx.write('file2.txt', createTestData('Content 2'))
      await tx.write('file3.txt', createTestData('Content 3'))
      await tx.commit()

      expect(decodeData(await memoryBackend.read('file1.txt'))).toBe('Content 1')
      expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('Content 2')
      expect(decodeData(await memoryBackend.read('file3.txt'))).toBe('Content 3')
    })

    it('should apply deletes on commit', async () => {
      await memoryBackend.write('to-delete.txt', createTestData('Delete me'))

      const tx = await backend.beginTransaction()
      await tx.delete('to-delete.txt')
      await tx.commit()

      expect(await memoryBackend.exists('to-delete.txt')).toBe(false)
    })

    it('should handle mixed writes and deletes on commit', async () => {
      await memoryBackend.write('existing.txt', createTestData('Original'))
      await memoryBackend.write('to-delete.txt', createTestData('Delete me'))

      const tx = await backend.beginTransaction()
      await tx.write('existing.txt', createTestData('Updated'))
      await tx.write('new.txt', createTestData('New'))
      await tx.delete('to-delete.txt')
      await tx.commit()

      expect(decodeData(await memoryBackend.read('existing.txt'))).toBe('Updated')
      expect(decodeData(await memoryBackend.read('new.txt'))).toBe('New')
      expect(await memoryBackend.exists('to-delete.txt')).toBe(false)
    })

    it('should throw TransactionError when committing inactive transaction', async () => {
      const tx = await backend.beginTransaction()
      await tx.commit()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
    })

    it('should ignore not found errors when deleting non-existent file', async () => {
      const tx = await backend.beginTransaction()
      await tx.delete('nonexistent.txt')

      // Should not throw
      await expect(tx.commit()).resolves.toBeUndefined()
    })
  })

  // ===========================================================================
  // Transaction Rollback
  // ===========================================================================

  describe('Transaction rollback', () => {
    it('should discard writes on rollback', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('new.txt', createTestData('Discarded'))
      await tx.rollback()

      expect(await memoryBackend.exists('new.txt')).toBe(false)
    })

    it('should discard deletes on rollback', async () => {
      await memoryBackend.write('keep.txt', createTestData('Keep me'))

      const tx = await backend.beginTransaction()
      await tx.delete('keep.txt')
      await tx.rollback()

      expect(await memoryBackend.exists('keep.txt')).toBe(true)
    })

    it('should throw TransactionError when rolling back inactive transaction', async () => {
      const tx = await backend.beginTransaction()
      await tx.rollback()

      await expect(tx.rollback()).rejects.toThrow(TransactionError)
    })

    it('should throw TransactionError on operations after rollback', async () => {
      const tx = await backend.beginTransaction()
      await tx.rollback()

      await expect(tx.read('file.txt')).rejects.toThrow(TransactionError)
      await expect(tx.write('file.txt', createTestData('x'))).rejects.toThrow(TransactionError)
      await expect(tx.delete('file.txt')).rejects.toThrow(TransactionError)
    })
  })

  // ===========================================================================
  // Pass-through Operations
  // ===========================================================================

  describe('Pass-through operations', () => {
    it('should pass through direct read', async () => {
      await memoryBackend.write('file.txt', createTestData('Content'))

      const data = await backend.read('file.txt')
      expect(decodeData(data)).toBe('Content')
    })

    it('should pass through direct write', async () => {
      await backend.write('file.txt', createTestData('Direct write'))

      const data = await memoryBackend.read('file.txt')
      expect(decodeData(data)).toBe('Direct write')
    })

    it('should pass through exists', async () => {
      await memoryBackend.write('file.txt', createTestData('Content'))

      expect(await backend.exists('file.txt')).toBe(true)
      expect(await backend.exists('nonexistent.txt')).toBe(false)
    })

    it('should pass through stat', async () => {
      await memoryBackend.write('file.txt', createTestData('Content'))

      const stat = await backend.stat('file.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(7)
    })

    it('should pass through list', async () => {
      await memoryBackend.write('dir/file1.txt', createTestData('1'))
      await memoryBackend.write('dir/file2.txt', createTestData('2'))

      const result = await backend.list('dir/')
      expect(result.files).toHaveLength(2)
    })

    it('should pass through delete', async () => {
      await memoryBackend.write('file.txt', createTestData('Content'))

      const deleted = await backend.delete('file.txt')

      expect(deleted).toBe(true)
      expect(await memoryBackend.exists('file.txt')).toBe(false)
    })

    it('should pass through readRange', async () => {
      await memoryBackend.write('file.txt', createTestData('0123456789'))

      const data = await backend.readRange('file.txt', 2, 5)
      expect(decodeData(data)).toBe('234')
    })

    it('should pass through writeAtomic', async () => {
      const result = await backend.writeAtomic('file.txt', createTestData('Atomic'))

      expect(result.etag).toBeDefined()
      expect(decodeData(await memoryBackend.read('file.txt'))).toBe('Atomic')
    })

    it('should pass through append', async () => {
      await memoryBackend.write('log.txt', createTestData('Line 1\n'))
      await backend.append('log.txt', createTestData('Line 2\n'))

      expect(decodeData(await memoryBackend.read('log.txt'))).toBe('Line 1\nLine 2\n')
    })

    it('should pass through deletePrefix', async () => {
      await memoryBackend.write('data/a.txt', createTestData('a'))
      await memoryBackend.write('data/b.txt', createTestData('b'))
      await memoryBackend.write('other/c.txt', createTestData('c'))

      const count = await backend.deletePrefix('data/')

      expect(count).toBe(2)
      expect(await memoryBackend.exists('other/c.txt')).toBe(true)
    })

    it('should pass through mkdir', async () => {
      await backend.mkdir('newdir')

      const stat = await memoryBackend.stat('newdir')
      expect(stat?.isDirectory).toBe(true)
    })

    it('should pass through rmdir', async () => {
      await memoryBackend.mkdir('dir')
      await backend.rmdir('dir')

      const stat = await memoryBackend.stat('dir')
      expect(stat).toBeNull()
    })

    it('should pass through writeConditional', async () => {
      const result1 = await backend.write('file.txt', createTestData('v1'))
      const result2 = await backend.writeConditional(
        'file.txt',
        createTestData('v2'),
        result1.etag
      )

      expect(result2.etag).toBeDefined()
      expect(decodeData(await memoryBackend.read('file.txt'))).toBe('v2')
    })

    it('should pass through copy', async () => {
      await memoryBackend.write('source.txt', createTestData('Content'))
      await backend.copy('source.txt', 'dest.txt')

      expect(decodeData(await memoryBackend.read('dest.txt'))).toBe('Content')
    })

    it('should pass through move', async () => {
      await memoryBackend.write('old.txt', createTestData('Content'))
      await backend.move('old.txt', 'new.txt')

      expect(await memoryBackend.exists('old.txt')).toBe(false)
      expect(decodeData(await memoryBackend.read('new.txt'))).toBe('Content')
    })
  })

  // ===========================================================================
  // Helper Functions
  // ===========================================================================

  describe('withTransactions helper', () => {
    it('should wrap a backend with transaction support', () => {
      const wrapped = withTransactions(memoryBackend)

      expect(wrapped).toBeInstanceOf(TransactionalBackend)
      expect(isTransactional(wrapped)).toBe(true)
    })

    it('should not double-wrap', () => {
      const wrapped1 = withTransactions(memoryBackend)
      const wrapped2 = withTransactions(wrapped1)

      expect(wrapped1).toBe(wrapped2)
    })
  })

  describe('runInTransaction helper', () => {
    it('should auto-commit on success', async () => {
      const result = await runInTransaction(backend, async (tx) => {
        await tx.write('file.txt', createTestData('Content'))
        return 'done'
      })

      expect(result).toBe('done')
      expect(decodeData(await memoryBackend.read('file.txt'))).toBe('Content')
    })

    it('should auto-rollback on error', async () => {
      await expect(
        runInTransaction(backend, async (tx) => {
          await tx.write('file.txt', createTestData('Content'))
          throw new Error('Simulated error')
        })
      ).rejects.toThrow('Simulated error')

      expect(await memoryBackend.exists('file.txt')).toBe(false)
    })

    it('should return value from function', async () => {
      const result = await runInTransaction(backend, async (tx) => {
        await tx.write('file.txt', createTestData('42'))
        return 42
      })

      expect(result).toBe(42)
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('Error handling', () => {
    it('should include transaction ID in TransactionError', async () => {
      const tx = await backend.beginTransaction()
      await tx.commit()

      try {
        await tx.commit()
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).transactionId).toBe(tx.id)
        expect((error as TransactionError).message).toContain(tx.id)
      }
    })

    it('should handle concurrent transactions independently', async () => {
      const tx1 = await backend.beginTransaction()
      const tx2 = await backend.beginTransaction()

      await tx1.write('file1.txt', createTestData('From tx1'))
      await tx2.write('file2.txt', createTestData('From tx2'))

      // tx1 should not see tx2's writes
      await expect(tx1.read('file2.txt')).rejects.toThrow(NotFoundError)

      // tx2 should not see tx1's writes
      await expect(tx2.read('file1.txt')).rejects.toThrow(NotFoundError)

      await tx1.commit()
      await tx2.commit()

      // Both files should exist after commit
      expect(decodeData(await memoryBackend.read('file1.txt'))).toBe('From tx1')
      expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('From tx2')
    })
  })

  // ===========================================================================
  // Data Isolation
  // ===========================================================================

  describe('Data isolation', () => {
    it('should not mutate data passed to write', async () => {
      const tx = await backend.beginTransaction()
      const originalData = createTestData('Original')
      const dataCopy = new Uint8Array(originalData)

      await tx.write('file.txt', originalData)

      // Mutate original data
      originalData[0] = 0xff

      // Data in transaction should be unchanged
      const readData = await tx.read('file.txt')
      expect(readData).toEqual(dataCopy)
    })

    it('should not allow mutation of read data', async () => {
      const tx = await backend.beginTransaction()
      await tx.write('file.txt', createTestData('Original'))

      const data1 = await tx.read('file.txt')
      data1[0] = 0xff

      const data2 = await tx.read('file.txt')
      expect(decodeData(data2)).toBe('Original')
    })
  })
})
