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
  TransactionTooLargeError,
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

  // ===========================================================================
  // Transaction Size Limits
  // ===========================================================================

  describe('Transaction size limits', () => {
    describe('operation count limits', () => {
      it('should enforce maxTransactionOperations limit on writes', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 3,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('1'))
        await tx.write('file2.txt', createTestData('2'))
        await tx.write('file3.txt', createTestData('3'))

        // Fourth write should fail
        await expect(tx.write('file4.txt', createTestData('4'))).rejects.toThrow(
          TransactionTooLargeError
        )
      })

      it('should enforce maxTransactionOperations limit on deletes', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 2,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.delete('file1.txt')
        await tx.delete('file2.txt')

        // Third delete should fail
        await expect(tx.delete('file3.txt')).rejects.toThrow(TransactionTooLargeError)
      })

      it('should enforce limit across mixed operations', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 3,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('1'))
        await tx.delete('file2.txt')
        await tx.write('file3.txt', createTestData('3'))

        // Fourth operation (either write or delete) should fail
        await expect(tx.delete('file4.txt')).rejects.toThrow(TransactionTooLargeError)
      })

      it('should not count overwrites as new operations', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 2,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('v1'))
        await tx.write('file2.txt', createTestData('v1'))

        // Overwriting same path should not count as new operation
        await tx.write('file1.txt', createTestData('v2'))
        await tx.write('file2.txt', createTestData('v2'))

        // This should still fail since it's a new path
        await expect(tx.write('file3.txt', createTestData('v1'))).rejects.toThrow(
          TransactionTooLargeError
        )
      })

      it('should not count delete of pending write as new operation', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 2,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('1'))
        await tx.write('file2.txt', createTestData('2'))

        // Delete of existing pending write should not count as new operation
        await tx.delete('file1.txt')

        // Still at max, new path should fail
        await expect(tx.write('file3.txt', createTestData('3'))).rejects.toThrow(
          TransactionTooLargeError
        )
      })
    })

    describe('bytes limits', () => {
      it('should enforce maxTransactionBytes limit', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 100, // 100 bytes limit
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('x'.repeat(50))) // 50 bytes
        await tx.write('file2.txt', createTestData('y'.repeat(40))) // 40 bytes, total 90

        // This should exceed the limit (90 + 20 = 110 > 100)
        await expect(tx.write('file3.txt', createTestData('z'.repeat(20)))).rejects.toThrow(
          TransactionTooLargeError
        )
      })

      it('should allow operations up to exact limit', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 100,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('x'.repeat(50)))
        await tx.write('file2.txt', createTestData('y'.repeat(50)))

        // Exactly at limit, commit should work
        await tx.commit()

        expect(decodeData(await memoryBackend.read('file1.txt'))).toBe('x'.repeat(50))
        expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('y'.repeat(50))
      })

      it('should track bytes correctly when overwriting', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 100,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file.txt', createTestData('x'.repeat(80))) // 80 bytes

        // Overwriting with smaller data should free up space
        await tx.write('file.txt', createTestData('y'.repeat(30))) // Now only 30 bytes used

        // Should be able to write more since we freed space
        await tx.write('file2.txt', createTestData('z'.repeat(70))) // 30 + 70 = 100 (at limit)

        await tx.commit()
        expect(decodeData(await memoryBackend.read('file.txt'))).toBe('y'.repeat(30))
        expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('z'.repeat(70))
      })

      it('should reclaim bytes when write is deleted', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 100,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('x'.repeat(80))) // 80 bytes

        // Delete the write, should reclaim bytes
        await tx.delete('file1.txt')

        // Now we should have space for a large write again
        await tx.write('file2.txt', createTestData('y'.repeat(100)))

        await tx.commit()
        expect(await memoryBackend.exists('file1.txt')).toBe(false)
        expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('y'.repeat(100))
      })

      it('should reject single write exceeding limit', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 50,
        })

        const tx = await limitedBackend.beginTransaction()

        await expect(tx.write('big.txt', createTestData('x'.repeat(100)))).rejects.toThrow(
          TransactionTooLargeError
        )
      })
    })

    describe('error messages', () => {
      it('should provide clear error message for operation limit', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionOperations: 1,
        })

        const tx = await limitedBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('1'))

        try {
          await tx.write('file2.txt', createTestData('2'))
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(TransactionTooLargeError)
          const txError = error as TransactionTooLargeError
          expect(txError.limitType).toBe('operations')
          expect(txError.limit).toBe(1)
          expect(txError.actual).toBe(2)
          expect(txError.message).toContain('operation count limit')
        }
      })

      it('should provide clear error message for bytes limit', async () => {
        const limitedBackend = new TransactionalBackend(memoryBackend, {
          maxTransactionBytes: 10,
        })

        const tx = await limitedBackend.beginTransaction()

        try {
          await tx.write('big.txt', createTestData('x'.repeat(100)))
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(TransactionTooLargeError)
          const txError = error as TransactionTooLargeError
          expect(txError.limitType).toBe('bytes')
          expect(txError.limit).toBe(10)
          expect(txError.actual).toBe(100)
          expect(txError.message).toContain('size limit')
        }
      })
    })

    describe('default limits', () => {
      it('should use default maxTransactionOperations of 10000', async () => {
        // This tests that default limits are reasonable (not actually testing 10000 operations)
        const defaultBackend = new TransactionalBackend(memoryBackend)
        const tx = await defaultBackend.beginTransaction()

        // Should be able to do at least a few thousand operations
        for (let i = 0; i < 1000; i++) {
          await tx.write(`file${i}.txt`, createTestData(`${i}`))
        }

        // Clean up without committing (would be slow with 1000 files)
        await tx.rollback()
      })
    })

    describe('withTransactions helper with options', () => {
      it('should pass options to TransactionalBackend', async () => {
        const txBackend = withTransactions(memoryBackend, {
          maxTransactionOperations: 2,
        })

        const tx = await txBackend.beginTransaction()
        await tx.write('file1.txt', createTestData('1'))
        await tx.write('file2.txt', createTestData('2'))

        await expect(tx.write('file3.txt', createTestData('3'))).rejects.toThrow(
          TransactionTooLargeError
        )
      })
    })
  })

  // ===========================================================================
  // Commit Atomicity
  // ===========================================================================

  describe('Commit atomicity', () => {
    it('should rollback all changes when a write fails mid-commit', async () => {
      // Create a backend that fails on specific writes
      const failingBackend = new FailingBackend(memoryBackend, {
        failOnWrite: ['fail-this.txt'],
      })
      const txBackend = new TransactionalBackend(failingBackend)

      // Set up some existing files
      await memoryBackend.write('existing1.txt', createTestData('Original 1'))
      await memoryBackend.write('existing2.txt', createTestData('Original 2'))

      // Start transaction
      const tx = await txBackend.beginTransaction()
      await tx.write('new-file.txt', createTestData('New file'))
      await tx.write('existing1.txt', createTestData('Updated 1'))
      await tx.write('fail-this.txt', createTestData('This will fail'))
      await tx.write('existing2.txt', createTestData('Updated 2'))

      // Commit should fail
      await expect(tx.commit()).rejects.toThrow(TransactionCommitError)

      // Verify rollback: all files should be in their original state
      expect(await memoryBackend.exists('new-file.txt')).toBe(false)
      expect(decodeData(await memoryBackend.read('existing1.txt'))).toBe('Original 1')
      expect(decodeData(await memoryBackend.read('existing2.txt'))).toBe('Original 2')
      expect(await memoryBackend.exists('fail-this.txt')).toBe(false)
    })

    it('should rollback deletes when a later write fails', async () => {
      const failingBackend = new FailingBackend(memoryBackend, {
        failOnWrite: ['fail-this.txt'],
      })
      const txBackend = new TransactionalBackend(failingBackend)

      // Set up existing files
      await memoryBackend.write('to-delete.txt', createTestData('Delete me'))
      await memoryBackend.write('keep-me.txt', createTestData('Keep me'))

      // Start transaction with delete and write
      const tx = await txBackend.beginTransaction()
      await tx.delete('to-delete.txt')
      await tx.write('fail-this.txt', createTestData('This will fail'))

      // Commit should fail
      await expect(tx.commit()).rejects.toThrow(TransactionCommitError)

      // Verify rollback: deleted file should be restored
      expect(await memoryBackend.exists('to-delete.txt')).toBe(true)
      expect(decodeData(await memoryBackend.read('to-delete.txt'))).toBe('Delete me')
      expect(decodeData(await memoryBackend.read('keep-me.txt'))).toBe('Keep me')
    })

    it('should handle failure on first operation', async () => {
      const failingBackend = new FailingBackend(memoryBackend, {
        failOnDelete: ['fail-delete.txt'],
      })
      const txBackend = new TransactionalBackend(failingBackend)

      // Set up existing file
      await memoryBackend.write('fail-delete.txt', createTestData('Original'))
      await memoryBackend.write('other.txt', createTestData('Other'))

      // Start transaction
      const tx = await txBackend.beginTransaction()
      await tx.delete('fail-delete.txt')
      await tx.write('other.txt', createTestData('Updated'))

      // Commit should fail on delete
      await expect(tx.commit()).rejects.toThrow(TransactionCommitError)

      // Verify: file should still exist (delete failed, no rollback needed)
      expect(await memoryBackend.exists('fail-delete.txt')).toBe(true)
      expect(decodeData(await memoryBackend.read('fail-delete.txt'))).toBe('Original')
      // The write should not have been applied
      expect(decodeData(await memoryBackend.read('other.txt'))).toBe('Other')
    })

    it('should include commit error in TransactionCommitError', async () => {
      const failingBackend = new FailingBackend(memoryBackend, {
        failOnWrite: ['fail.txt'],
        errorMessage: 'Simulated disk full',
      })
      const txBackend = new TransactionalBackend(failingBackend)

      const tx = await txBackend.beginTransaction()
      await tx.write('fail.txt', createTestData('Will fail'))

      try {
        await tx.commit()
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCommitError)
        const commitError = error as TransactionCommitError
        expect(commitError.errors.length).toBeGreaterThanOrEqual(1)
        expect(commitError.errors[0].message).toContain('Simulated disk full')
      }
    })

    it('should maintain atomicity with mixed deletes and writes', async () => {
      const failingBackend = new FailingBackend(memoryBackend, {
        failOnWrite: ['fail.txt'],
      })
      const txBackend = new TransactionalBackend(failingBackend)

      // Setup: 3 existing files
      await memoryBackend.write('file1.txt', createTestData('File 1'))
      await memoryBackend.write('file2.txt', createTestData('File 2'))
      await memoryBackend.write('file3.txt', createTestData('File 3'))

      const tx = await txBackend.beginTransaction()
      // Order: delete file1, write new file, delete file2, update file3, then fail
      await tx.delete('file1.txt')
      await tx.write('new.txt', createTestData('New'))
      await tx.delete('file2.txt')
      await tx.write('file3.txt', createTestData('Updated 3'))
      await tx.write('fail.txt', createTestData('This fails'))

      await expect(tx.commit()).rejects.toThrow(TransactionCommitError)

      // All original files should be restored
      expect(await memoryBackend.exists('file1.txt')).toBe(true)
      expect(decodeData(await memoryBackend.read('file1.txt'))).toBe('File 1')
      expect(await memoryBackend.exists('file2.txt')).toBe(true)
      expect(decodeData(await memoryBackend.read('file2.txt'))).toBe('File 2')
      expect(decodeData(await memoryBackend.read('file3.txt'))).toBe('File 3')
      // New files should not exist
      expect(await memoryBackend.exists('new.txt')).toBe(false)
      expect(await memoryBackend.exists('fail.txt')).toBe(false)
    })

    it('should succeed when all operations complete', async () => {
      // No failures configured
      const failingBackend = new FailingBackend(memoryBackend, {})
      const txBackend = new TransactionalBackend(failingBackend)

      await memoryBackend.write('existing.txt', createTestData('Original'))

      const tx = await txBackend.beginTransaction()
      await tx.write('new.txt', createTestData('New'))
      await tx.write('existing.txt', createTestData('Updated'))
      await tx.delete('nonexistent.txt') // Should not fail

      await tx.commit()

      expect(decodeData(await memoryBackend.read('new.txt'))).toBe('New')
      expect(decodeData(await memoryBackend.read('existing.txt'))).toBe('Updated')
    })
  })
})

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * A wrapper backend that can simulate failures on specific operations
 */
class FailingBackend implements MemoryBackend {
  readonly type = 'failing'

  constructor(
    private readonly inner: MemoryBackend,
    private readonly config: {
      failOnWrite?: string[]
      failOnDelete?: string[]
      failOnRead?: string[]
      errorMessage?: string
    }
  ) {}

  private shouldFail(path: string, operation: 'write' | 'delete' | 'read'): boolean {
    const list =
      operation === 'write'
        ? this.config.failOnWrite
        : operation === 'delete'
          ? this.config.failOnDelete
          : this.config.failOnRead
    return list?.includes(path) ?? false
  }

  private throwError(path: string, operation: string): never {
    throw new Error(this.config.errorMessage || `Simulated ${operation} failure on ${path}`)
  }

  async read(path: string): Promise<Uint8Array> {
    if (this.shouldFail(path, 'read')) {
      this.throwError(path, 'read')
    }
    return this.inner.read(path)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    return this.inner.readRange(path, start, end)
  }

  async exists(path: string): Promise<boolean> {
    return this.inner.exists(path)
  }

  async stat(path: string): Promise<import('../../../src/types/storage').FileStat | null> {
    return this.inner.stat(path)
  }

  async list(
    prefix: string,
    options?: import('../../../src/types/storage').ListOptions
  ): Promise<import('../../../src/types/storage').ListResult> {
    return this.inner.list(prefix, options)
  }

  async write(
    path: string,
    data: Uint8Array,
    options?: import('../../../src/types/storage').WriteOptions
  ): Promise<import('../../../src/types/storage').WriteResult> {
    if (this.shouldFail(path, 'write')) {
      this.throwError(path, 'write')
    }
    return this.inner.write(path, data, options)
  }

  async writeAtomic(
    path: string,
    data: Uint8Array,
    options?: import('../../../src/types/storage').WriteOptions
  ): Promise<import('../../../src/types/storage').WriteResult> {
    return this.inner.writeAtomic(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.inner.append(path, data)
  }

  async delete(path: string): Promise<boolean> {
    if (this.shouldFail(path, 'delete')) {
      this.throwError(path, 'delete')
    }
    return this.inner.delete(path)
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.inner.deletePrefix(prefix)
  }

  async mkdir(path: string): Promise<void> {
    return this.inner.mkdir(path)
  }

  async rmdir(
    path: string,
    options?: import('../../../src/types/storage').RmdirOptions
  ): Promise<void> {
    return this.inner.rmdir(path, options)
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: import('../../../src/types/storage').WriteOptions
  ): Promise<import('../../../src/types/storage').WriteResult> {
    return this.inner.writeConditional(path, data, expectedVersion, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    return this.inner.copy(source, dest)
  }

  async move(source: string, dest: string): Promise<void> {
    return this.inner.move(source, dest)
  }
}
