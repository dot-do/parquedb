/**
 * Comprehensive Transaction System Tests
 *
 * This file expands on the transaction test suite with additional coverage for:
 * - Transaction commit and rollback (comprehensive scenarios)
 * - Concurrent transactions (stress tests and race conditions)
 * - Transaction isolation levels (behavior verification)
 * - Error handling and recovery (edge cases)
 *
 * Issue: parquedb-89pi - Expand transaction system tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  TransactionManager,
  TransactionError,
  withTransaction,
  withRetry,
  isTransaction,
  supportsSavepoints,
  type Transaction,
  type TransactionOptions,
  type TransactionStatus,
  type IsolationLevel,
} from '../../../src/transaction'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a delay promise for testing async behavior
 * Use vi.advanceTimersByTimeAsync when fake timers are active
 */
const delay = (ms: number) => vi.advanceTimersByTimeAsync(ms)

// =============================================================================
// Transaction Commit Tests - Comprehensive
// =============================================================================

describe('Transaction Commit - Comprehensive', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('commit atomicity', () => {
    it('commits empty transaction successfully', async () => {
      const tx = manager.begin('test-db')

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.getOperations()).toHaveLength(0)
    })

    it('commits transaction with single operation', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.getOperations()).toHaveLength(1)
    })

    it('commits transaction with many operations', async () => {
      const tx = manager.begin('test-db')

      for (let i = 0; i < 100; i++) {
        tx.recordOperation({
          type: i % 3 === 0 ? 'create' : i % 3 === 1 ? 'update' : 'delete',
          target: `namespace-\${i % 5}`,
          id: `entity-\${i}`,
        })
      }

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.getOperations()).toHaveLength(100)
    })

    it('commits do not affect manager after completion', async () => {
      const tx = manager.begin('test-db')
      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })

      expect(manager.getActiveCount()).toBe(1)

      await tx.commit()

      expect(manager.getActiveCount()).toBe(0)
      expect(manager.get(tx.id)).toBeUndefined()
    })

    it('commit preserves operation order', async () => {
      const tx = manager.begin('test-db')

      const operations = [
        { type: 'create' as const, target: 'a', id: '1' },
        { type: 'update' as const, target: 'b', id: '2' },
        { type: 'delete' as const, target: 'c', id: '3' },
        { type: 'create' as const, target: 'd', id: '4' },
      ]

      for (const op of operations) {
        tx.recordOperation(op)
      }

      await tx.commit()

      const recorded = tx.getOperations()
      expect(recorded.map(op => op.id)).toEqual(['1', '2', '3', '4'])
    })

    it('commit does not modify operations', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({
        type: 'update',
        target: 'users',
        id: 'u1',
        beforeState: { name: 'Alice' },
        afterState: { name: 'Bob' },
      })

      await tx.commit()

      const op = tx.getOperations()[0]
      expect(op?.beforeState).toEqual({ name: 'Alice' })
      expect(op?.afterState).toEqual({ name: 'Bob' })
    })
  })

  describe('commit idempotency and error handling', () => {
    it('double commit throws error', async () => {
      const tx = manager.begin('test-db')

      await tx.commit()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
      await expect(tx.commit()).rejects.toThrow("'committed'")
    })

    it('commit after failed transaction throws error', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
    })

    it('commit clears timeout handler', async () => {
      vi.useFakeTimers()

      const tx = manager.begin('test-db', { timeout: 1000 })
      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })

      await tx.commit()

      // Advance past timeout - should not change status
      await vi.advanceTimersByTimeAsync(2000)

      expect(tx.status).toBe('committed')

      vi.useRealTimers()
    })
  })
})

// =============================================================================
// Transaction Rollback Tests - Comprehensive
// =============================================================================

describe('Transaction Rollback - Comprehensive', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('rollback behavior', () => {
    it('rolls back empty transaction', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
      expect(tx.isActive()).toBe(false)
    })

    it('rolls back transaction preserving operation history', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      tx.recordOperation({ type: 'update', target: 'users', id: 'u1' })

      await tx.rollback()

      // Operations are still recorded for auditing
      expect(tx.getOperations()).toHaveLength(2)
      expect(tx.status).toBe('rolled_back')
    })

    it('rollback removes transaction from manager', async () => {
      const tx = manager.begin('test-db')

      expect(manager.getActiveCount()).toBe(1)

      await tx.rollback()

      expect(manager.getActiveCount()).toBe(0)
    })

    it('rollback clears timeout handler', async () => {
      vi.useFakeTimers()

      const tx = manager.begin('test-db', { timeout: 1000 })

      await tx.rollback()

      // Advance past timeout - should not affect already rolled back tx
      await vi.advanceTimersByTimeAsync(2000)

      expect(tx.status).toBe('rolled_back')

      vi.useRealTimers()
    })

    it('cannot record operations after rollback', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      expect(() => {
        tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      }).toThrow(TransactionError)
    })

    it('cannot create savepoint after rollback', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      await expect(tx.savepoint?.('sp1')).rejects.toThrow()
    })
  })

  describe('rollback with savepoints', () => {
    it('rollback to savepoint preserves earlier operations', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      tx.recordOperation({ type: 'create', target: 'users', id: 'u2' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u3' })
      tx.recordOperation({ type: 'create', target: 'users', id: 'u4' })

      await tx.rollbackToSavepoint?.('sp1')

      expect(tx.getOperations()).toHaveLength(2)
      expect(tx.getOperations().map(op => op.id)).toEqual(['u1', 'u2'])
    })

    it('rollback to savepoint allows continued operations', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u2' })
      await tx.rollbackToSavepoint?.('sp1')

      // Add new operation after rollback
      tx.recordOperation({ type: 'create', target: 'users', id: 'u3' })

      expect(tx.getOperations()).toHaveLength(2)
      expect(tx.getOperations()[1]?.id).toBe('u3')

      await tx.commit()
      expect(tx.status).toBe('committed')
    })

    it('handles multiple rollbacks to same savepoint', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      await tx.savepoint?.('sp1')

      // First batch
      tx.recordOperation({ type: 'create', target: 'users', id: 'batch1' })
      await tx.rollbackToSavepoint?.('sp1')

      // Second batch
      tx.recordOperation({ type: 'create', target: 'users', id: 'batch2' })
      await tx.rollbackToSavepoint?.('sp1')

      // Third batch
      tx.recordOperation({ type: 'create', target: 'users', id: 'batch3' })

      const ops = tx.getOperations()
      expect(ops).toHaveLength(2)
      expect(ops[1]?.id).toBe('batch3')
    })

    it('rollback removes nested savepoints', async () => {
      const tx = manager.begin('test-db')

      await tx.savepoint?.('sp1')
      tx.recordOperation({ type: 'create', target: 'a', id: '1' })

      await tx.savepoint?.('sp2')
      tx.recordOperation({ type: 'create', target: 'b', id: '2' })

      await tx.savepoint?.('sp3')
      tx.recordOperation({ type: 'create', target: 'c', id: '3' })

      // Rollback to sp1 should remove sp2 and sp3
      await tx.rollbackToSavepoint?.('sp1')

      await expect(tx.rollbackToSavepoint?.('sp2')).rejects.toThrow('not found')
      await expect(tx.rollbackToSavepoint?.('sp3')).rejects.toThrow('not found')
    })
  })
})

// =============================================================================
// Concurrent Transaction Tests
// =============================================================================

describe('Concurrent Transactions', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('basic concurrency', () => {
    it('supports multiple concurrent transactions', () => {
      const transactions: Transaction<string>[] = []

      for (let i = 0; i < 10; i++) {
        transactions.push(manager.begin(`context-\${i}`))
      }

      expect(manager.getActiveCount()).toBe(10)
      expect(transactions.every(tx => tx.isActive())).toBe(true)
    })

    it('each transaction has isolated operations', () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')
      const tx3 = manager.begin('ctx3')

      tx1.recordOperation({ type: 'create', target: 'users', id: 'from-tx1' })
      tx1.recordOperation({ type: 'update', target: 'users', id: 'from-tx1' })

      tx2.recordOperation({ type: 'delete', target: 'posts', id: 'from-tx2' })

      tx3.recordOperation({ type: 'create', target: 'comments', id: 'from-tx3' })
      tx3.recordOperation({ type: 'create', target: 'comments', id: 'from-tx3-2' })
      tx3.recordOperation({ type: 'create', target: 'comments', id: 'from-tx3-3' })

      expect(tx1.getOperations()).toHaveLength(2)
      expect(tx2.getOperations()).toHaveLength(1)
      expect(tx3.getOperations()).toHaveLength(3)

      // Operations don't cross-contaminate
      expect(tx1.getOperations().every(op => op.id?.includes('tx1'))).toBe(true)
      expect(tx2.getOperations().every(op => op.id?.includes('tx2'))).toBe(true)
      expect(tx3.getOperations().every(op => op.id?.includes('tx3'))).toBe(true)
    })

    it('concurrent commits do not interfere', async () => {
      const transactions = Array.from({ length: 5 }, (_, i) => {
        const tx = manager.begin(`ctx-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `item-\${i}` })
        return tx
      })

      // Commit all concurrently
      await Promise.all(transactions.map(tx => tx.commit()))

      expect(transactions.every(tx => tx.status === 'committed')).toBe(true)
      expect(manager.getActiveCount()).toBe(0)
    })

    it('concurrent rollbacks do not interfere', async () => {
      const transactions = Array.from({ length: 5 }, (_, i) => {
        const tx = manager.begin(`ctx-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `item-\${i}` })
        return tx
      })

      // Rollback all concurrently
      await Promise.all(transactions.map(tx => tx.rollback()))

      expect(transactions.every(tx => tx.status === 'rolled_back')).toBe(true)
      expect(manager.getActiveCount()).toBe(0)
    })

    it('mixed concurrent commits and rollbacks', async () => {
      const transactions = Array.from({ length: 10 }, (_, i) => {
        const tx = manager.begin(`ctx-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `item-\${i}` })
        return { tx, shouldCommit: i % 2 === 0 }
      })

      await Promise.all(
        transactions.map(({ tx, shouldCommit }) =>
          shouldCommit ? tx.commit() : tx.rollback()
        )
      )

      const committed = transactions.filter(t => t.tx.status === 'committed')
      const rolledBack = transactions.filter(t => t.tx.status === 'rolled_back')

      expect(committed).toHaveLength(5)
      expect(rolledBack).toHaveLength(5)
      expect(manager.getActiveCount()).toBe(0)
    })
  })

  describe('stress testing', () => {
    it('handles 100 concurrent transactions', async () => {
      const count = 100
      const transactions: Transaction<string>[] = []

      for (let i = 0; i < count; i++) {
        const tx = manager.begin(`ctx-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `\${i}` })
        transactions.push(tx)
      }

      expect(manager.getActiveCount()).toBe(count)

      // Commit half, rollback half in parallel
      await Promise.all([
        ...transactions.slice(0, count / 2).map(tx => tx.commit()),
        ...transactions.slice(count / 2).map(tx => tx.rollback()),
      ])

      expect(manager.getActiveCount()).toBe(0)
    })

    it('handles rapid transaction creation and completion', async () => {
      const iterations = 50

      for (let i = 0; i < iterations; i++) {
        const tx = manager.begin(`rapid-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `\${i}` })

        if (i % 2 === 0) {
          await tx.commit()
        } else {
          await tx.rollback()
        }
      }

      expect(manager.getActiveCount()).toBe(0)
    })

    it('handles interleaved operations across transactions', async () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')
      const tx3 = manager.begin('ctx3')

      // Interleave operations
      for (let i = 0; i < 10; i++) {
        tx1.recordOperation({ type: 'create', target: 'A', id: `a\${i}` })
        tx2.recordOperation({ type: 'update', target: 'B', id: `b\${i}` })
        tx3.recordOperation({ type: 'delete', target: 'C', id: `c\${i}` })
      }

      expect(tx1.getOperations()).toHaveLength(10)
      expect(tx2.getOperations()).toHaveLength(10)
      expect(tx3.getOperations()).toHaveLength(10)

      await Promise.all([tx1.commit(), tx2.rollback(), tx3.commit()])

      expect(tx1.status).toBe('committed')
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('committed')
    })
  })

  describe('unique transaction IDs', () => {
    it('generates unique IDs for all concurrent transactions', () => {
      const count = 100
      const ids = new Set<string>()

      for (let i = 0; i < count; i++) {
        const tx = manager.begin(`ctx-\${i}`)
        ids.add(tx.id)
      }

      expect(ids.size).toBe(count)
    })

    it('custom IDs are respected', () => {
      const tx1 = manager.begin('ctx', { id: 'custom-id-1' })
      const tx2 = manager.begin('ctx', { id: 'custom-id-2' })

      expect(tx1.id).toBe('custom-id-1')
      expect(tx2.id).toBe('custom-id-2')
    })
  })
})

// =============================================================================
// Transaction Isolation Level Tests
// =============================================================================

describe('Transaction Isolation Levels', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('isolation level configuration', () => {
    it('accepts read_uncommitted isolation level', () => {
      const tx = manager.begin('ctx', { isolation: 'read_uncommitted' })

      expect(tx.isActive()).toBe(true)
    })

    it('accepts read_committed isolation level (default)', () => {
      const tx = manager.begin('ctx', { isolation: 'read_committed' })

      expect(tx.isActive()).toBe(true)
    })

    it('accepts repeatable_read isolation level', () => {
      const tx = manager.begin('ctx', { isolation: 'repeatable_read' })

      expect(tx.isActive()).toBe(true)
    })

    it('accepts serializable isolation level', () => {
      const tx = manager.begin('ctx', { isolation: 'serializable' })

      expect(tx.isActive()).toBe(true)
    })

    it('uses read_committed as default', () => {
      expect(manager.defaults.isolation).toBe('read_committed')
    })

    it('creates transactions with all isolation levels concurrently', () => {
      const levels: IsolationLevel[] = [
        'read_uncommitted',
        'read_committed',
        'repeatable_read',
        'serializable',
      ]

      const transactions = levels.map(level =>
        manager.begin('ctx', { isolation: level })
      )

      expect(transactions.every(tx => tx.isActive())).toBe(true)
      expect(manager.getActiveCount()).toBe(4)
    })
  })

  describe('isolation behavior', () => {
    it('transactions do not see uncommitted operations from other transactions', () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')

      tx1.recordOperation({
        type: 'create',
        target: 'users',
        id: 'shared-user',
        afterState: { name: 'Alice' },
      })

      // tx2 should not see tx1's uncommitted operation
      expect(tx2.getOperations()).toHaveLength(0)
      expect(tx2.getOperations().find(op => op.id === 'shared-user')).toBeUndefined()
    })

    it('each transaction maintains its own view', () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')

      // Both work on "same" entity conceptually but in isolation
      tx1.recordOperation({
        type: 'update',
        target: 'accounts',
        id: 'acc-1',
        beforeState: { balance: 100 },
        afterState: { balance: 150 },
      })

      tx2.recordOperation({
        type: 'update',
        target: 'accounts',
        id: 'acc-1',
        beforeState: { balance: 100 },
        afterState: { balance: 75 },
      })

      // Each sees only its own version
      expect(tx1.getOperations()[0]?.afterState).toEqual({ balance: 150 })
      expect(tx2.getOperations()[0]?.afterState).toEqual({ balance: 75 })
    })

    it('savepoints are isolated per transaction', async () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')

      // Both can create savepoints with same name
      await tx1.savepoint?.('checkpoint')
      await tx2.savepoint?.('checkpoint')

      tx1.recordOperation({ type: 'create', target: 'A', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'B', id: '1' })

      // Rolling back tx1's savepoint doesn't affect tx2
      await tx1.rollbackToSavepoint?.('checkpoint')

      expect(tx1.getOperations()).toHaveLength(0)
      expect(tx2.getOperations()).toHaveLength(1)
    })
  })
})

// =============================================================================
// Error Handling and Recovery Tests
// =============================================================================

describe('Error Handling and Recovery', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('error codes', () => {
    it('INVALID_STATE on commit after commit', async () => {
      const tx = manager.begin('ctx')
      await tx.commit()

      try {
        await tx.commit()
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('INVALID_STATE')
      }
    })

    it('INVALID_STATE on rollback after commit', async () => {
      const tx = manager.begin('ctx')
      await tx.commit()

      try {
        await tx.rollback()
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('INVALID_STATE')
      }
    })

    it('INVALID_STATE on commit after rollback', async () => {
      const tx = manager.begin('ctx')
      await tx.rollback()

      try {
        await tx.commit()
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('INVALID_STATE')
      }
    })

    it('SAVEPOINT_EXISTS on duplicate savepoint', async () => {
      const tx = manager.begin('ctx')
      await tx.savepoint?.('sp1')

      try {
        await tx.savepoint?.('sp1')
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('SAVEPOINT_EXISTS')
      }
    })

    it('SAVEPOINT_NOT_FOUND on rollback to non-existent savepoint', async () => {
      const tx = manager.begin('ctx')

      try {
        await tx.rollbackToSavepoint?.('nonexistent')
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('SAVEPOINT_NOT_FOUND')
      }
    })

    it('INVALID_STATE on recording operation after commit', async () => {
      const tx = manager.begin('ctx')
      await tx.commit()

      try {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError)
        expect((error as TransactionError).code).toBe('INVALID_STATE')
      }
    })
  })

  describe('recovery scenarios', () => {
    it('new transaction can be started after commit', async () => {
      const tx1 = manager.begin('ctx')
      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx1.commit()

      const tx2 = manager.begin('ctx')
      tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx2.commit()

      expect(tx2.status).toBe('committed')
    })

    it('new transaction can be started after rollback', async () => {
      const tx1 = manager.begin('ctx')
      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx1.rollback()

      const tx2 = manager.begin('ctx')
      tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx2.commit()

      expect(tx2.status).toBe('committed')
    })

    it('manager recovers from multiple failed transactions', async () => {
      // Create and fail multiple transactions
      for (let i = 0; i < 5; i++) {
        const tx = manager.begin(`ctx-\${i}`)
        tx.recordOperation({ type: 'create', target: 'items', id: `\${i}` })
        await tx.rollback()
      }

      expect(manager.getActiveCount()).toBe(0)

      // Create new successful transaction
      const successTx = manager.begin('success')
      successTx.recordOperation({ type: 'create', target: 'success', id: '1' })
      await successTx.commit()

      expect(successTx.status).toBe('committed')
    })

    it('shutdown cleans up all active transactions', async () => {
      // Create several active transactions
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')
      const tx3 = manager.begin('ctx3')

      tx1.recordOperation({ type: 'create', target: 'a', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'b', id: '2' })
      tx3.recordOperation({ type: 'create', target: 'c', id: '3' })

      expect(manager.getActiveCount()).toBe(3)

      await manager.shutdown()

      expect(manager.getActiveCount()).toBe(0)
      expect(tx1.status).toBe('rolled_back')
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('rolled_back')
    })

    it('cleanup removes stale transactions', () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')

      // Simulate old transactions
      const oldTime = new Date(Date.now() - 10 * 60 * 1000) // 10 minutes ago
      ;(tx1 as { startedAt: Date }).startedAt = oldTime

      expect(manager.getActiveCount()).toBe(2)

      const cleaned = manager.cleanup(5 * 60 * 1000) // 5 minute threshold

      expect(cleaned).toBe(1)
      expect(manager.getActiveCount()).toBe(1)
    })
  })

  describe('withTransaction error handling', () => {
    it('automatically rolls back on sync error', async () => {
      let txRef: Transaction | null = null

      const provider = {
        beginTransaction: () => {
          txRef = manager.begin('ctx')
          return txRef
        },
      }

      await expect(
        withTransaction(provider, async () => {
          throw new Error('Sync error')
        })
      ).rejects.toThrow('Sync error')

      expect(txRef?.status).toBe('rolled_back')
    })

    it('automatically rolls back on async error', async () => {
      vi.useFakeTimers()
      let txRef: Transaction | null = null

      const provider = {
        beginTransaction: () => {
          txRef = manager.begin('ctx')
          return txRef
        },
      }

      await expect(
        withTransaction(provider, async () => {
          await delay(10)
          throw new Error('Async error')
        })
      ).rejects.toThrow('Async error')

      expect(txRef?.status).toBe('rolled_back')
      vi.useRealTimers()
    })

    it('preserves original error', async () => {
      class CustomError extends Error {
        code = 'CUSTOM_CODE'
        details = { foo: 'bar' }
      }

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      try {
        await withTransaction(provider, async () => {
          throw new CustomError('Custom error')
        })
        expect.fail('Should throw')
      } catch (error) {
        expect(error).toBeInstanceOf(CustomError)
        expect((error as CustomError).code).toBe('CUSTOM_CODE')
        expect((error as CustomError).details).toEqual({ foo: 'bar' })
      }
    })
  })

  describe('withRetry behavior', () => {
    it('retries on VERSION_CONFLICT', async () => {
      let attempts = 0

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      const result = await withRetry(
        provider,
        async () => {
          attempts++
          if (attempts < 3) {
            throw new TransactionError('Version conflict', 'VERSION_CONFLICT')
          }
          return 'success'
        },
        { maxRetries: 5, retryDelay: 10 }
      )

      expect(result).toBe('success')
      expect(attempts).toBe(3)
    })

    it('retries on COMMIT_CONFLICT', async () => {
      let attempts = 0

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      const result = await withRetry(
        provider,
        async () => {
          attempts++
          if (attempts < 2) {
            throw new TransactionError('Commit conflict', 'COMMIT_CONFLICT')
          }
          return 'done'
        },
        { maxRetries: 5, retryDelay: 10 }
      )

      expect(result).toBe('done')
      expect(attempts).toBe(2)
    })

    it('does not retry INVALID_STATE', async () => {
      let attempts = 0

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      await expect(
        withRetry(
          provider,
          async () => {
            attempts++
            throw new TransactionError('Invalid state', 'INVALID_STATE')
          },
          { maxRetries: 5, retryDelay: 10 }
        )
      ).rejects.toThrow('Invalid state')

      expect(attempts).toBe(1)
    })

    it('exhausts maxRetries', async () => {
      let attempts = 0

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      await expect(
        withRetry(
          provider,
          async () => {
            attempts++
            throw new TransactionError('Always fails', 'VERSION_CONFLICT')
          },
          { maxRetries: 3, retryDelay: 10 }
        )
      ).rejects.toThrow('Always fails')

      expect(attempts).toBe(4) // Initial + 3 retries
    })

    it('custom shouldRetry function', async () => {
      let attempts = 0

      const provider = {
        beginTransaction: () => manager.begin('ctx'),
      }

      const result = await withRetry(
        provider,
        async () => {
          attempts++
          if (attempts < 2) {
            throw new Error('Retryable: custom error')
          }
          return 'done'
        },
        {
          maxRetries: 5,
          retryDelay: 10,
          shouldRetry: (error) =>
            error instanceof Error && error.message.includes('Retryable'),
        }
      )

      expect(result).toBe('done')
      expect(attempts).toBe(2)
    })
  })
})

// =============================================================================
// Timeout Handling Tests
// =============================================================================

describe('Timeout Handling', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(async () => {
    vi.useRealTimers()
  })

  it('transaction auto-rollbacks after timeout', async () => {
    const manager = new TransactionManager<string>()
    const tx = manager.begin('ctx', { timeout: 1000 })

    expect(tx.isActive()).toBe(true)

    await vi.advanceTimersByTimeAsync(1500)

    expect(tx.isActive()).toBe(false)
    expect(tx.status).toBe('rolled_back')

    await manager.shutdown()
  })

  it('commit before timeout prevents auto-rollback', async () => {
    const manager = new TransactionManager<string>()
    const tx = manager.begin('ctx', { timeout: 1000 })

    await vi.advanceTimersByTimeAsync(500)
    await tx.commit()

    await vi.advanceTimersByTimeAsync(1000)

    expect(tx.status).toBe('committed')

    await manager.shutdown()
  })

  it('multiple transactions with different timeouts', async () => {
    const manager = new TransactionManager<string>()

    const tx1 = manager.begin('ctx1', { timeout: 100 })
    const tx2 = manager.begin('ctx2', { timeout: 200 })
    const tx3 = manager.begin('ctx3', { timeout: 300 })

    await vi.advanceTimersByTimeAsync(150)
    expect(tx1.status).toBe('rolled_back')
    expect(tx2.status).toBe('pending')
    expect(tx3.status).toBe('pending')

    await vi.advanceTimersByTimeAsync(100)
    expect(tx2.status).toBe('rolled_back')
    expect(tx3.status).toBe('pending')

    await vi.advanceTimersByTimeAsync(100)
    expect(tx3.status).toBe('rolled_back')

    await manager.shutdown()
  })

  it('zero timeout means no auto-rollback', async () => {
    const manager = new TransactionManager<string>()
    const tx = manager.begin('ctx', { timeout: 0 })

    await vi.advanceTimersByTimeAsync(100000)

    expect(tx.isActive()).toBe(true)
    expect(tx.status).toBe('pending')

    await manager.shutdown()
  })
})

// =============================================================================
// Transaction State Machine Tests
// =============================================================================

describe('Transaction State Machine', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('state transitions', () => {
    it('pending -> committed', async () => {
      const tx = manager.begin('ctx')
      expect(tx.status).toBe('pending')

      await tx.commit()
      expect(tx.status).toBe('committed')
    })

    it('pending -> rolled_back', async () => {
      const tx = manager.begin('ctx')
      expect(tx.status).toBe('pending')

      await tx.rollback()
      expect(tx.status).toBe('rolled_back')
    })

    it('committed is terminal', async () => {
      const tx = manager.begin('ctx')
      await tx.commit()

      await expect(tx.commit()).rejects.toThrow()
      await expect(tx.rollback()).rejects.toThrow()

      expect(tx.status).toBe('committed')
    })

    it('rolled_back is terminal', async () => {
      const tx = manager.begin('ctx')
      await tx.rollback()

      await expect(tx.commit()).rejects.toThrow()
      await expect(tx.rollback()).rejects.toThrow()

      expect(tx.status).toBe('rolled_back')
    })
  })

  describe('isActive behavior', () => {
    it('returns true for pending transaction', () => {
      const tx = manager.begin('ctx')
      expect(tx.isActive()).toBe(true)
    })

    it('returns false after commit', async () => {
      const tx = manager.begin('ctx')
      await tx.commit()
      expect(tx.isActive()).toBe(false)
    })

    it('returns false after rollback', async () => {
      const tx = manager.begin('ctx')
      await tx.rollback()
      expect(tx.isActive()).toBe(false)
    })
  })
})

// =============================================================================
// TransactionError Tests
// =============================================================================

describe('TransactionError', () => {
  describe('construction', () => {
    it('creates with message only', () => {
      const error = new TransactionError('Test message')

      expect(error.message).toBe('Test message')
      expect(error.code).toBe('UNKNOWN')
      expect(error.transactionId).toBeUndefined()
      expect(error.cause).toBeUndefined()
    })

    it('creates with all parameters', () => {
      const cause = new Error('Original')
      const error = new TransactionError('Test', 'COMMIT_FAILED', 'txn-123', cause)

      expect(error.message).toBe('Test')
      expect(error.code).toBe('COMMIT_FAILED')
      expect(error.transactionId).toBe('txn-123')
      expect(error.cause).toBe(cause)
    })

    it('fromError creates from existing error', () => {
      const original = new Error('Original error')
      original.stack = 'custom stack trace'

      const txError = TransactionError.fromError(original, 'ROLLBACK_FAILED', 'txn-456')

      expect(txError.message).toBe('Original error')
      expect(txError.code).toBe('ROLLBACK_FAILED')
      expect(txError.transactionId).toBe('txn-456')
      expect(txError.cause).toBe(original)
      expect(txError.stack).toBe('custom stack trace')
    })
  })

  describe('type guards', () => {
    it('isTransactionError identifies TransactionError', () => {
      const txError = new TransactionError('Test')
      const regularError = new Error('Test')

      expect(TransactionError.isTransactionError(txError)).toBe(true)
      expect(TransactionError.isTransactionError(regularError)).toBe(false)
      expect(TransactionError.isTransactionError(null)).toBe(false)
      expect(TransactionError.isTransactionError(undefined)).toBe(false)
      expect(TransactionError.isTransactionError('string')).toBe(false)
    })

    it('isCode checks specific error codes', () => {
      const error = new TransactionError('Test', 'VERSION_CONFLICT')

      expect(TransactionError.isCode(error, 'VERSION_CONFLICT')).toBe(true)
      expect(TransactionError.isCode(error, 'COMMIT_FAILED')).toBe(false)
      expect(TransactionError.isCode(new Error('Test'), 'VERSION_CONFLICT')).toBe(false)
    })
  })
})

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isTransaction', () => {
    it('returns true for valid transaction object', () => {
      const tx = {
        id: 'test-id',
        status: 'pending' as TransactionStatus,
        startedAt: new Date(),
        context: {},
        commit: async () => {},
        rollback: async () => {},
        isActive: () => true,
        getOperations: () => [],
        recordOperation: () => {},
      }

      expect(isTransaction(tx)).toBe(true)
    })

    it('returns false for incomplete objects', () => {
      expect(isTransaction({})).toBe(false)
      expect(isTransaction({ id: 'test' })).toBe(false)
      expect(isTransaction({ commit: () => {} })).toBe(false)
      expect(isTransaction({ id: 'test', commit: () => {}, rollback: () => {} })).toBe(false)
    })

    it('returns false for non-objects', () => {
      expect(isTransaction(null)).toBe(false)
      expect(isTransaction(undefined)).toBe(false)
      expect(isTransaction('string')).toBe(false)
      expect(isTransaction(123)).toBe(false)
      expect(isTransaction(true)).toBe(false)
    })
  })

  describe('supportsSavepoints', () => {
    it('returns true when savepoint methods exist', () => {
      const tx = {
        id: 'test',
        status: 'pending' as TransactionStatus,
        startedAt: new Date(),
        context: {},
        commit: async () => {},
        rollback: async () => {},
        isActive: () => true,
        getOperations: () => [],
        recordOperation: () => {},
        savepoint: async () => {},
        rollbackToSavepoint: async () => {},
      }

      expect(supportsSavepoints(tx)).toBe(true)
    })

    it('returns false when savepoint methods are missing', () => {
      const tx = {
        id: 'test',
        status: 'pending' as TransactionStatus,
        startedAt: new Date(),
        context: {},
        commit: async () => {},
        rollback: async () => {},
        isActive: () => true,
        getOperations: () => [],
        recordOperation: () => {},
      }

      expect(supportsSavepoints(tx)).toBe(false)
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let manager: TransactionManager<unknown>

  beforeEach(() => {
    manager = new TransactionManager<unknown>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('handles null context', () => {
    const tx = manager.begin(null)
    expect(tx.context).toBeNull()
    expect(tx.isActive()).toBe(true)
  })

  it('handles undefined context', () => {
    const tx = manager.begin(undefined)
    expect(tx.context).toBeUndefined()
    expect(tx.isActive()).toBe(true)
  })

  it('handles complex context objects', () => {
    const context = {
      database: 'test-db',
      tables: ['users', 'posts'],
      config: { nested: { deeply: { value: 42 } } },
    }

    const tx = manager.begin(context)
    expect(tx.context).toEqual(context)
  })

  it('handles operations without id', () => {
    const tx = manager.begin('ctx')

    tx.recordOperation({ type: 'custom', target: 'system' })

    const ops = tx.getOperations()
    expect(ops[0]?.id).toBeUndefined()
    expect(ops[0]?.type).toBe('custom')
  })

  it('handles operations with undefined beforeState', () => {
    const tx = manager.begin('ctx')

    tx.recordOperation({
      type: 'create',
      target: 'users',
      id: 'u1',
      afterState: { name: 'Alice' },
    })

    const ops = tx.getOperations()
    expect(ops[0]?.beforeState).toBeUndefined()
    expect(ops[0]?.afterState).toEqual({ name: 'Alice' })
  })

  it('handles large operations', () => {
    const tx = manager.begin('ctx')

    const largeData = { data: 'x'.repeat(10000) }

    tx.recordOperation({
      type: 'update',
      target: 'documents',
      id: 'doc-1',
      beforeState: largeData,
      afterState: { data: 'y'.repeat(10000) },
    })

    const ops = tx.getOperations()
    expect(ops[0]?.beforeState).toEqual(largeData)
  })

  it('handles many operations in single transaction', () => {
    const tx = manager.begin('ctx')

    const count = 1000
    for (let i = 0; i < count; i++) {
      tx.recordOperation({ type: 'create', target: 'items', id: `item-\${i}` })
    }

    expect(tx.getOperations()).toHaveLength(count)
    expect(tx.getOperations()[count - 1]?.sequence).toBe(count)
  })
})
