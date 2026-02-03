/**
 * Expanded Transaction System Tests
 *
 * Additional test coverage for the transaction system including:
 * - Multi-operation transactions
 * - Abort/rollback scenarios
 * - Nested transactions (savepoints)
 * - Concurrent transaction conflicts
 * - Transaction timeout handling
 * - Recovery after crash
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
} from '../../../src/transaction'

// =============================================================================
// Multi-Operation Transaction Tests
// =============================================================================

describe('Multi-Operation Transactions', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('operation batching', () => {
    it('records multiple operations in sequence', () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'create', target: 'posts', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '1' })
      tx.recordOperation({ type: 'delete', target: 'comments', id: '1' })
      tx.recordOperation({ type: 'create', target: 'posts', id: '2' })

      const ops = tx.getOperations()
      expect(ops.length).toBe(5)
      expect(ops.map(op => op.type)).toEqual(['create', 'create', 'update', 'delete', 'create'])
    })

    it('maintains operation order with correct sequence numbers', () => {
      const tx = manager.begin('test-db')

      for (let i = 0; i < 10; i++) {
        tx.recordOperation({ type: 'create', target: 'items', id: `${i}` })
      }

      const ops = tx.getOperations()
      for (let i = 0; i < 10; i++) {
        expect(ops[i]?.sequence).toBe(i + 1)
        expect(ops[i]?.id).toBe(`${i}`)
      }
    })

    it('tracks operations across multiple namespaces', () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
      tx.recordOperation({ type: 'create', target: 'posts', id: 'p1' })
      tx.recordOperation({ type: 'create', target: 'comments', id: 'c1' })
      tx.recordOperation({ type: 'update', target: 'users', id: 'u1' })
      tx.recordOperation({ type: 'create', target: 'likes', id: 'l1' })

      const ops = tx.getOperations()
      const targets = ops.map(op => op.target)
      expect(targets).toContain('users')
      expect(targets).toContain('posts')
      expect(targets).toContain('comments')
      expect(targets).toContain('likes')
    })

    it('handles large transaction with many operations', () => {
      const tx = manager.begin('test-db')

      const operationCount = 1000
      for (let i = 0; i < operationCount; i++) {
        tx.recordOperation({
          type: i % 3 === 0 ? 'create' : i % 3 === 1 ? 'update' : 'delete',
          target: `namespace-${i % 10}`,
          id: `entity-${i}`,
        })
      }

      const ops = tx.getOperations()
      expect(ops.length).toBe(operationCount)
      expect(ops[operationCount - 1]?.sequence).toBe(operationCount)
    })

    it('preserves operation metadata', () => {
      const tx = manager.begin('test-db')

      const beforeState = { name: 'Alice', age: 30 }
      const afterState = { name: 'Alice', age: 31 }

      tx.recordOperation({
        type: 'update',
        target: 'users',
        id: '1',
        beforeState,
        afterState,
      })

      tx.recordOperation({
        type: 'delete',
        target: 'users',
        id: '2',
        beforeState: { name: 'Bob' },
      })

      const ops = tx.getOperations()
      expect(ops[0]?.beforeState).toEqual(beforeState)
      expect(ops[0]?.afterState).toEqual(afterState)
      expect(ops[1]?.beforeState).toEqual({ name: 'Bob' })
      expect(ops[1]?.afterState).toBeUndefined()
    })
  })

  describe('commit with multiple operations', () => {
    it('commits all operations atomically', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'create', target: 'posts', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '1' })

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.getOperations().length).toBe(3)
    })

    it('preserves operations after commit', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '1' })

      await tx.commit()

      // Operations should still be accessible for logging/auditing
      const ops = tx.getOperations()
      expect(ops.length).toBe(2)
    })
  })
})

// =============================================================================
// Abort/Rollback Scenario Tests
// =============================================================================

describe('Abort/Rollback Scenarios', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('explicit rollback', () => {
    it('rolls back empty transaction', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
      expect(tx.isActive()).toBe(false)
    })

    it('rolls back transaction with single operation', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })

      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
      expect(tx.getOperations().length).toBe(1)
    })

    it('rolls back transaction with many operations', async () => {
      const tx = manager.begin('test-db')

      for (let i = 0; i < 100; i++) {
        tx.recordOperation({ type: 'create', target: 'items', id: `${i}` })
      }

      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
    })

    it('removes transaction from manager on rollback', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })

      expect(manager.getActiveCount()).toBe(1)

      await tx.rollback()

      expect(manager.getActiveCount()).toBe(0)
      expect(manager.get(tx.id)).toBeUndefined()
    })
  })

  describe('rollback state transitions', () => {
    it('cannot rollback already committed transaction', async () => {
      const tx = manager.begin('test-db')

      await tx.commit()

      await expect(tx.rollback()).rejects.toThrow(TransactionError)
      await expect(tx.rollback()).rejects.toThrow("'committed'")
    })

    it('cannot rollback already rolled back transaction', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      await expect(tx.rollback()).rejects.toThrow(TransactionError)
      await expect(tx.rollback()).rejects.toThrow("'rolled_back'")
    })

    it('cannot record operations after rollback', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      expect(() => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      }).toThrow(TransactionError)
    })

    it('cannot commit after rollback', async () => {
      const tx = manager.begin('test-db')

      await tx.rollback()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
    })
  })

  describe('abort scenarios', () => {
    it('handles rollback during operation recording', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })

      // Simulate an error condition that triggers abort
      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
      expect(manager.getActiveCount()).toBe(0)
    })

    it('maintains consistency across multiple rollbacks in separate transactions', async () => {
      const tx1 = manager.begin('test-db')
      const tx2 = manager.begin('test-db')
      const tx3 = manager.begin('test-db')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx3.recordOperation({ type: 'create', target: 'users', id: '3' })

      await tx1.rollback()
      await tx2.commit()
      await tx3.rollback()

      expect(tx1.status).toBe('rolled_back')
      expect(tx2.status).toBe('committed')
      expect(tx3.status).toBe('rolled_back')
      expect(manager.getActiveCount()).toBe(0)
    })
  })
})

// =============================================================================
// Nested Transaction (Savepoint) Tests
// =============================================================================

describe('Nested Transactions (Savepoints)', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('savepoint creation', () => {
    it('creates savepoint at current position', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })

      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '3' })

      expect(tx.getOperations().length).toBe(3)
    })

    it('creates multiple savepoints', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx.savepoint?.('sp2')

      tx.recordOperation({ type: 'create', target: 'users', id: '3' })
      await tx.savepoint?.('sp3')

      expect(tx.getOperations().length).toBe(3)
    })

    it('throws on duplicate savepoint name', async () => {
      const tx = manager.begin('test-db')

      await tx.savepoint?.('sp1')

      await expect(tx.savepoint?.('sp1')).rejects.toThrow('already exists')
    })

    it('cannot create savepoint on inactive transaction', async () => {
      const tx = manager.begin('test-db')
      await tx.commit()

      await expect(tx.savepoint?.('sp1')).rejects.toThrow('inactive')
    })
  })

  describe('rollback to savepoint', () => {
    it('rolls back to specific savepoint', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '3' })
      tx.recordOperation({ type: 'create', target: 'users', id: '4' })
      tx.recordOperation({ type: 'create', target: 'users', id: '5' })

      expect(tx.getOperations().length).toBe(5)

      await tx.rollbackToSavepoint?.('sp1')

      const ops = tx.getOperations()
      expect(ops.length).toBe(2)
      expect(ops.map(op => op.id)).toEqual(['1', '2'])
    })

    it('rolls back to intermediate savepoint', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx.savepoint?.('sp2')

      tx.recordOperation({ type: 'create', target: 'users', id: '3' })
      await tx.savepoint?.('sp3')

      tx.recordOperation({ type: 'create', target: 'users', id: '4' })

      await tx.rollbackToSavepoint?.('sp2')

      const ops = tx.getOperations()
      expect(ops.length).toBe(2)
      expect(ops.map(op => op.id)).toEqual(['1', '2'])
    })

    it('removes savepoints after rolled back point', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      await tx.savepoint?.('sp2')

      tx.recordOperation({ type: 'create', target: 'users', id: '3' })
      await tx.savepoint?.('sp3')

      await tx.rollbackToSavepoint?.('sp1')

      // sp2 and sp3 should no longer exist
      await expect(tx.rollbackToSavepoint?.('sp2')).rejects.toThrow('not found')
      await expect(tx.rollbackToSavepoint?.('sp3')).rejects.toThrow('not found')

      // But sp1 should still exist (can rollback to it again after more operations)
      tx.recordOperation({ type: 'create', target: 'users', id: 'new' })
      await tx.rollbackToSavepoint?.('sp1')
      expect(tx.getOperations().length).toBe(1)
    })

    it('throws on non-existent savepoint', async () => {
      const tx = manager.begin('test-db')

      await expect(tx.rollbackToSavepoint?.('nonexistent')).rejects.toThrow('not found')
    })

    it('cannot rollback to savepoint on inactive transaction', async () => {
      const tx = manager.begin('test-db')
      await tx.savepoint?.('sp1')
      await tx.commit()

      await expect(tx.rollbackToSavepoint?.('sp1')).rejects.toThrow('inactive')
    })
  })

  describe('release savepoint', () => {
    it('releases savepoint without rollback', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })

      await tx.releaseSavepoint?.('sp1')

      // Operations should remain
      expect(tx.getOperations().length).toBe(2)

      // But savepoint is gone
      await expect(tx.rollbackToSavepoint?.('sp1')).rejects.toThrow('not found')
    })

    it('release non-existent savepoint is idempotent', async () => {
      const tx = manager.begin('test-db')

      // Should not throw
      await tx.releaseSavepoint?.('nonexistent')
    })
  })

  describe('nested savepoint patterns', () => {
    it('handles deep nesting of savepoints', async () => {
      const tx = manager.begin('test-db')

      for (let i = 0; i < 10; i++) {
        tx.recordOperation({ type: 'create', target: 'items', id: `${i}` })
        await tx.savepoint?.(`sp${i}`)
      }

      expect(tx.getOperations().length).toBe(10)

      // Rollback to sp5 (should keep 6 operations)
      await tx.rollbackToSavepoint?.('sp5')
      expect(tx.getOperations().length).toBe(6)

      // Rollback to sp2 (should keep 3 operations)
      await tx.rollbackToSavepoint?.('sp2')
      expect(tx.getOperations().length).toBe(3)
    })

    it('supports savepoint with different operation types', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('after-create')

      tx.recordOperation({ type: 'update', target: 'users', id: '1' })
      await tx.savepoint?.('after-update')

      tx.recordOperation({ type: 'delete', target: 'users', id: '1' })

      // Rollback the delete
      await tx.rollbackToSavepoint?.('after-update')
      expect(tx.getOperations().length).toBe(2)
      expect(tx.getOperations()[1]?.type).toBe('update')

      // Rollback the update
      await tx.rollbackToSavepoint?.('after-create')
      expect(tx.getOperations().length).toBe(1)
      expect(tx.getOperations()[0]?.type).toBe('create')
    })

    it('allows commit after partial rollback', async () => {
      const tx = manager.begin('test-db')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')

      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx.recordOperation({ type: 'create', target: 'users', id: '3' })

      await tx.rollbackToSavepoint?.('sp1')
      tx.recordOperation({ type: 'create', target: 'users', id: 'final' })

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.getOperations().length).toBe(2)
    })
  })

  describe('supportsSavepoints type guard', () => {
    it('returns true for transaction with savepoint support', () => {
      const tx = manager.begin('test-db')
      expect(supportsSavepoints(tx)).toBe(true)
    })

    it('returns false for transaction without savepoint methods', () => {
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
        // No savepoint or rollbackToSavepoint
      }
      expect(supportsSavepoints(tx)).toBe(false)
    })
  })
})

// =============================================================================
// Concurrent Transaction Conflict Tests
// =============================================================================

describe('Concurrent Transaction Conflicts', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('multiple active transactions', () => {
    it('supports multiple concurrent transactions', () => {
      const tx1 = manager.begin('db1')
      const tx2 = manager.begin('db2')
      const tx3 = manager.begin('db3')

      expect(manager.getActiveCount()).toBe(3)
      expect(manager.getActiveTransactions()).toHaveLength(3)
    })

    it('maintains separate operation logs per transaction', () => {
      const tx1 = manager.begin('db1')
      const tx2 = manager.begin('db2')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx1.recordOperation({ type: 'create', target: 'users', id: '2' })

      tx2.recordOperation({ type: 'delete', target: 'posts', id: '1' })

      expect(tx1.getOperations().length).toBe(2)
      expect(tx2.getOperations().length).toBe(1)
    })

    it('commits and rollbacks are independent', async () => {
      const tx1 = manager.begin('db1')
      const tx2 = manager.begin('db2')
      const tx3 = manager.begin('db3')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx3.recordOperation({ type: 'create', target: 'users', id: '3' })

      await tx1.commit()
      expect(tx1.status).toBe('committed')
      expect(tx2.status).toBe('pending')
      expect(tx3.status).toBe('pending')

      await tx2.rollback()
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('pending')

      await tx3.commit()
      expect(tx3.status).toBe('committed')
    })
  })

  describe('transaction isolation', () => {
    it('transactions do not see each others operations', () => {
      const tx1 = manager.begin('db')
      const tx2 = manager.begin('db')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })

      // tx2 should not see tx1's operation
      expect(tx2.getOperations().length).toBe(0)

      tx2.recordOperation({ type: 'create', target: 'posts', id: '1' })

      // Each should only see their own
      expect(tx1.getOperations().length).toBe(1)
      expect(tx2.getOperations().length).toBe(1)
      expect(tx1.getOperations()[0]?.target).toBe('users')
      expect(tx2.getOperations()[0]?.target).toBe('posts')
    })

    it('interleaved operations maintain isolation', async () => {
      const tx1 = manager.begin('db')
      const tx2 = manager.begin('db')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'users', id: 'A' })
      tx1.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx2.recordOperation({ type: 'create', target: 'users', id: 'B' })
      tx1.recordOperation({ type: 'create', target: 'users', id: '3' })

      expect(tx1.getOperations().map(op => op.id)).toEqual(['1', '2', '3'])
      expect(tx2.getOperations().map(op => op.id)).toEqual(['A', 'B'])
    })
  })

  describe('conflict detection with version errors', () => {
    it('VERSION_CONFLICT error triggers retry with withRetry', async () => {
      let attempts = 0

      const mockProvider = {
        beginTransaction: () => {
          return manager.begin('mock')
        },
      }

      const result = await withRetry(
        mockProvider,
        async (tx) => {
          attempts++
          if (attempts < 3) {
            throw new TransactionError('Version conflict detected', 'VERSION_CONFLICT')
          }
          return 'success'
        },
        { maxRetries: 5, retryDelay: 10 }
      )

      expect(result).toBe('success')
      expect(attempts).toBe(3)
    })

    it('COMMIT_CONFLICT error triggers retry', async () => {
      let attempts = 0

      const mockProvider = {
        beginTransaction: () => manager.begin('mock'),
      }

      const result = await withRetry(
        mockProvider,
        async () => {
          attempts++
          if (attempts < 2) {
            throw new TransactionError('Commit conflict', 'COMMIT_CONFLICT')
          }
          return 'done'
        },
        { maxRetries: 3, retryDelay: 10 }
      )

      expect(result).toBe('done')
      expect(attempts).toBe(2)
    })

    it('non-retryable errors fail immediately', async () => {
      let attempts = 0

      const mockProvider = {
        beginTransaction: () => manager.begin('mock'),
      }

      await expect(
        withRetry(
          mockProvider,
          async () => {
            attempts++
            throw new TransactionError('Invalid state', 'INVALID_STATE')
          },
          { maxRetries: 3, retryDelay: 10 }
        )
      ).rejects.toThrow('Invalid state')

      expect(attempts).toBe(1)
    })
  })

  describe('unique transaction IDs', () => {
    it('generates unique IDs for concurrent transactions', () => {
      const transactions: Transaction<string>[] = []

      for (let i = 0; i < 100; i++) {
        transactions.push(manager.begin('test'))
      }

      const ids = transactions.map(tx => tx.id)
      const uniqueIds = new Set(ids)

      expect(uniqueIds.size).toBe(100)
    })

    it('allows custom IDs', () => {
      const tx1 = manager.begin('test', { id: 'custom-1' })
      const tx2 = manager.begin('test', { id: 'custom-2' })

      expect(tx1.id).toBe('custom-1')
      expect(tx2.id).toBe('custom-2')
    })
  })
})

// =============================================================================
// Transaction Timeout Handling Tests
// =============================================================================

describe('Transaction Timeout Handling', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
    vi.useFakeTimers()
  })

  afterEach(async () => {
    vi.useRealTimers()
    await manager.shutdown()
  })

  describe('automatic timeout', () => {
    it('auto-rollbacks transaction after timeout', async () => {
      const tx = manager.begin('test', { timeout: 1000 })

      expect(tx.isActive()).toBe(true)

      await vi.advanceTimersByTimeAsync(1100)

      expect(tx.isActive()).toBe(false)
      expect(tx.status).toBe('rolled_back')
    })

    it('removes transaction from manager after timeout', async () => {
      const tx = manager.begin('test', { timeout: 500 })

      expect(manager.getActiveCount()).toBe(1)

      await vi.advanceTimersByTimeAsync(600)

      expect(manager.getActiveCount()).toBe(0)
      expect(manager.get(tx.id)).toBeUndefined()
    })

    it('does not timeout if committed before timeout', async () => {
      const tx = manager.begin('test', { timeout: 1000 })

      await vi.advanceTimersByTimeAsync(500)
      await tx.commit()

      await vi.advanceTimersByTimeAsync(1000)

      expect(tx.status).toBe('committed')
    })

    it('does not timeout if rolled back before timeout', async () => {
      const tx = manager.begin('test', { timeout: 1000 })

      await vi.advanceTimersByTimeAsync(500)
      await tx.rollback()

      await vi.advanceTimersByTimeAsync(1000)

      expect(tx.status).toBe('rolled_back')
    })

    it('handles multiple transactions with different timeouts', async () => {
      const tx1 = manager.begin('test1', { timeout: 100 })
      const tx2 = manager.begin('test2', { timeout: 200 })
      const tx3 = manager.begin('test3', { timeout: 300 })

      expect(manager.getActiveCount()).toBe(3)

      await vi.advanceTimersByTimeAsync(150)
      expect(tx1.status).toBe('rolled_back')
      expect(tx2.status).toBe('pending')
      expect(tx3.status).toBe('pending')
      expect(manager.getActiveCount()).toBe(2)

      await vi.advanceTimersByTimeAsync(100)
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('pending')
      expect(manager.getActiveCount()).toBe(1)

      await vi.advanceTimersByTimeAsync(100)
      expect(tx3.status).toBe('rolled_back')
      expect(manager.getActiveCount()).toBe(0)
    })

    it('preserves operations after timeout rollback', async () => {
      const tx = manager.begin('test', { timeout: 100 })

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '1' })

      await vi.advanceTimersByTimeAsync(150)

      expect(tx.status).toBe('rolled_back')
      // Operations are still recorded for auditing/logging purposes
      expect(tx.getOperations().length).toBe(2)
    })
  })

  describe('timeout configuration', () => {
    it('uses default timeout when not specified', () => {
      const tx = manager.begin('test')

      // Default is 30000ms according to TransactionManager defaults
      expect(manager.defaults.timeout).toBe(30000)
    })

    it('respects custom timeout values', async () => {
      const tx = manager.begin('test', { timeout: 50 })

      await vi.advanceTimersByTimeAsync(40)
      expect(tx.isActive()).toBe(true)

      await vi.advanceTimersByTimeAsync(20)
      expect(tx.isActive()).toBe(false)
    })

    it('handles zero timeout (immediate)', async () => {
      const tx = manager.begin('test', { timeout: 0 })

      // With timeout 0, no timeout is set
      await vi.advanceTimersByTimeAsync(1000)

      // Transaction should still be active (no timeout was set)
      expect(tx.isActive()).toBe(true)
    })
  })

  describe('timeout edge cases', () => {
    it('cannot record operations after timeout', async () => {
      const tx = manager.begin('test', { timeout: 100 })

      await vi.advanceTimersByTimeAsync(150)

      expect(() => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      }).toThrow(TransactionError)
    })

    it('cannot create savepoint after timeout', async () => {
      const tx = manager.begin('test', { timeout: 100 })

      await vi.advanceTimersByTimeAsync(150)

      await expect(tx.savepoint?.('sp1')).rejects.toThrow('inactive')
    })

    it('timeout clears savepoints', async () => {
      const tx = manager.begin('test', { timeout: 100 })

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })

      await vi.advanceTimersByTimeAsync(150)

      expect(tx.status).toBe('rolled_back')
      // Cannot rollback to savepoint after timeout
      await expect(tx.rollbackToSavepoint?.('sp1')).rejects.toThrow('inactive')
    })
  })
})

// =============================================================================
// Recovery After Crash Tests
// =============================================================================

describe('Recovery After Crash', () => {
  describe('manager cleanup', () => {
    it('cleanup removes stale transactions', () => {
      const manager = new TransactionManager<string>()

      // Create transactions
      const tx1 = manager.begin('test1')
      const tx2 = manager.begin('test2')
      const tx3 = manager.begin('test3')

      // Manually set old timestamps to simulate stale transactions
      const oldTime = new Date(Date.now() - 10 * 60 * 1000)
      ;(tx1 as { startedAt: Date }).startedAt = oldTime
      ;(tx2 as { startedAt: Date }).startedAt = oldTime

      expect(manager.getActiveCount()).toBe(3)

      const cleaned = manager.cleanup(5 * 60 * 1000)

      expect(cleaned).toBe(2)
      expect(manager.getActiveCount()).toBe(1)
    })

    it('cleanup does not affect recent transactions', () => {
      const manager = new TransactionManager<string>()

      const tx1 = manager.begin('test1')
      const tx2 = manager.begin('test2')

      const cleaned = manager.cleanup(5 * 60 * 1000)

      expect(cleaned).toBe(0)
      expect(manager.getActiveCount()).toBe(2)
    })

    it('cleanup uses default max age', () => {
      const manager = new TransactionManager<string>()

      const tx = manager.begin('test')

      // Set to 6 minutes ago
      const oldTime = new Date(Date.now() - 6 * 60 * 1000)
      ;(tx as { startedAt: Date }).startedAt = oldTime

      // Default is 5 minutes
      const cleaned = manager.cleanup()

      expect(cleaned).toBe(1)
    })
  })

  describe('manager shutdown', () => {
    it('shutdown rolls back all active transactions', async () => {
      const manager = new TransactionManager<string>()

      const tx1 = manager.begin('test1')
      const tx2 = manager.begin('test2')
      const tx3 = manager.begin('test3')

      tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx3.recordOperation({ type: 'create', target: 'users', id: '3' })

      await manager.shutdown()

      expect(tx1.status).toBe('rolled_back')
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('rolled_back')
      expect(manager.getActiveCount()).toBe(0)
    })

    it('shutdown handles already committed transactions', async () => {
      const manager = new TransactionManager<string>()

      const tx1 = manager.begin('test1')
      const tx2 = manager.begin('test2')

      await tx1.commit()

      await manager.shutdown()

      expect(tx1.status).toBe('committed')
      expect(tx2.status).toBe('rolled_back')
    })

    it('shutdown is idempotent', async () => {
      const manager = new TransactionManager<string>()

      manager.begin('test1')
      manager.begin('test2')

      await manager.shutdown()
      await manager.shutdown()
      await manager.shutdown()

      expect(manager.getActiveCount()).toBe(0)
    })

    it('shutdown clears timeout handlers', async () => {
      vi.useFakeTimers()

      const manager = new TransactionManager<string>()

      const tx = manager.begin('test', { timeout: 1000 })

      await manager.shutdown()

      // Advance time past timeout
      await vi.advanceTimersByTimeAsync(2000)

      // Should already be rolled back from shutdown, not timeout
      expect(tx.status).toBe('rolled_back')

      vi.useRealTimers()
    })
  })

  describe('operation log recovery', () => {
    it('operations are preserved for recovery analysis', async () => {
      const manager = new TransactionManager<string>()

      const tx = manager.begin('test')

      tx.recordOperation({
        type: 'create',
        target: 'users',
        id: '1',
        afterState: { name: 'Alice' },
      })

      tx.recordOperation({
        type: 'update',
        target: 'users',
        id: '1',
        beforeState: { name: 'Alice' },
        afterState: { name: 'Alice Updated' },
      })

      // Simulate crash - just shutdown
      await manager.shutdown()

      // Operations can still be read for recovery
      const ops = tx.getOperations()
      expect(ops.length).toBe(2)
      expect(ops[0]?.afterState).toEqual({ name: 'Alice' })
      expect(ops[1]?.beforeState).toEqual({ name: 'Alice' })
    })

    it('before/after state allows undo during recovery', () => {
      const manager = new TransactionManager<string>()

      const tx = manager.begin('test')

      // Record operations with full state for recovery
      tx.recordOperation({
        type: 'update',
        target: 'accounts',
        id: 'acc1',
        beforeState: { balance: 100 },
        afterState: { balance: 150 },
      })

      tx.recordOperation({
        type: 'update',
        target: 'accounts',
        id: 'acc2',
        beforeState: { balance: 200 },
        afterState: { balance: 150 },
      })

      const ops = tx.getOperations()

      // Recovery system can use beforeState to undo
      const undoStates = ops.map(op => ({
        target: op.target,
        id: op.id,
        restoreTo: op.beforeState,
      }))

      expect(undoStates[0]).toEqual({
        target: 'accounts',
        id: 'acc1',
        restoreTo: { balance: 100 },
      })

      expect(undoStates[1]).toEqual({
        target: 'accounts',
        id: 'acc2',
        restoreTo: { balance: 200 },
      })
    })
  })
})

// =============================================================================
// withTransaction Helper Extended Tests
// =============================================================================

describe('withTransaction Extended Scenarios', () => {
  interface MockProvider {
    beginTransaction(options?: TransactionOptions): Transaction
  }

  describe('error handling', () => {
    it('rolls back on synchronous throw', async () => {
      let txRef: Transaction | null = null

      const provider: MockProvider = {
        beginTransaction: () => {
          const manager = new TransactionManager()
          txRef = manager.begin('test')
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

    it('rolls back on async rejection', async () => {
      let txRef: Transaction | null = null

      const provider: MockProvider = {
        beginTransaction: () => {
          const manager = new TransactionManager()
          txRef = manager.begin('test')
          return txRef
        },
      }

      await expect(
        withTransaction(provider, async () => {
          await Promise.reject(new Error('Async error'))
        })
      ).rejects.toThrow('Async error')

      expect(txRef?.status).toBe('rolled_back')
    })

    it('preserves error type through rollback', async () => {
      const provider: MockProvider = {
        beginTransaction: () => {
          const manager = new TransactionManager()
          return manager.begin('test')
        },
      }

      class CustomError extends Error {
        code = 'CUSTOM'
      }

      try {
        await withTransaction(provider, async () => {
          throw new CustomError('Custom error')
        })
      } catch (error) {
        expect(error).toBeInstanceOf(CustomError)
        expect((error as CustomError).code).toBe('CUSTOM')
      }
    })
  })

  describe('complex workflows', () => {
    it('supports nested function calls within transaction', async () => {
      const provider: MockProvider = {
        beginTransaction: () => {
          const manager = new TransactionManager()
          return manager.begin('test')
        },
      }

      const createUser = async (tx: Transaction) => {
        tx.recordOperation({ type: 'create', target: 'users', id: 'u1' })
        return { id: 'u1' }
      }

      const createPost = async (tx: Transaction, authorId: string) => {
        tx.recordOperation({ type: 'create', target: 'posts', id: 'p1' })
        return { id: 'p1', authorId }
      }

      const result = await withTransaction(provider, async (tx) => {
        const user = await createUser(tx)
        const post = await createPost(tx, user.id)
        return { user, post }
      })

      expect(result.user.id).toBe('u1')
      expect(result.post.authorId).toBe('u1')
    })

    it('handles conditional operations', async () => {
      let txRef: Transaction | null = null

      const provider: MockProvider = {
        beginTransaction: () => {
          const manager = new TransactionManager()
          txRef = manager.begin('test')
          return txRef
        },
      }

      const result = await withTransaction(provider, async (tx) => {
        tx.recordOperation({ type: 'read', target: 'users', id: 'u1' })

        const shouldCreate = true
        if (shouldCreate) {
          tx.recordOperation({ type: 'create', target: 'users', id: 'u2' })
        }

        return 'done'
      })

      expect(result).toBe('done')
      expect(txRef?.getOperations().length).toBe(2)
    })
  })
})

// =============================================================================
// TransactionError Extended Tests
// =============================================================================

describe('TransactionError Extended', () => {
  describe('error codes', () => {
    const errorCodes: Array<{
      code: Parameters<typeof TransactionError.isCode>[1]
      message: string
    }> = [
      { code: 'INVALID_STATE', message: 'Invalid state' },
      { code: 'COMMIT_FAILED', message: 'Commit failed' },
      { code: 'COMMIT_CONFLICT', message: 'Commit conflict' },
      { code: 'ROLLBACK_FAILED', message: 'Rollback failed' },
      { code: 'TIMEOUT', message: 'Timeout' },
      { code: 'SAVEPOINT_EXISTS', message: 'Savepoint exists' },
      { code: 'SAVEPOINT_NOT_FOUND', message: 'Savepoint not found' },
      { code: 'VERSION_CONFLICT', message: 'Version conflict' },
      { code: 'NESTED_NOT_ALLOWED', message: 'Nested not allowed' },
      { code: 'UNKNOWN', message: 'Unknown' },
    ]

    errorCodes.forEach(({ code, message }) => {
      it(`creates error with code ${code}`, () => {
        const error = new TransactionError(message, code)
        expect(error.code).toBe(code)
        expect(TransactionError.isCode(error, code)).toBe(true)
      })
    })
  })

  describe('error chain', () => {
    it('preserves cause chain', () => {
      const root = new Error('Root cause')
      const middle = new TransactionError('Middle', 'COMMIT_FAILED', 'txn-1', root)
      const top = new TransactionError('Top level', 'UNKNOWN', 'txn-2', middle)

      expect(top.cause).toBe(middle)
      expect((top.cause as TransactionError).cause).toBe(root)
    })

    it('fromError preserves original stack', () => {
      const original = new Error('Original')
      const captured = TransactionError.fromError(original, 'COMMIT_FAILED')

      expect(captured.stack).toBe(original.stack)
    })
  })
})

// =============================================================================
// Edge Cases and Stress Tests
// =============================================================================

describe('Edge Cases and Stress Tests', () => {
  describe('edge cases', () => {
    it('handles transaction with empty context', () => {
      const manager = new TransactionManager<null>()
      const tx = manager.begin(null)

      expect(tx.context).toBeNull()
      expect(tx.isActive()).toBe(true)
    })

    it('handles transaction with complex context', () => {
      interface ComplexContext {
        database: string
        schema: string
        tables: string[]
        config: { timeout: number; retries: number }
      }

      const manager = new TransactionManager<ComplexContext>()
      const context: ComplexContext = {
        database: 'test-db',
        schema: 'public',
        tables: ['users', 'posts', 'comments'],
        config: { timeout: 5000, retries: 3 },
      }

      const tx = manager.begin(context)

      expect(tx.context).toEqual(context)
    })

    it('handles rapid transaction creation and completion', async () => {
      const manager = new TransactionManager<string>()

      const promises: Promise<void>[] = []

      for (let i = 0; i < 100; i++) {
        const tx = manager.begin(`tx-${i}`)
        promises.push(
          (async () => {
            tx.recordOperation({ type: 'create', target: 'items', id: `${i}` })
            if (i % 2 === 0) {
              await tx.commit()
            } else {
              await tx.rollback()
            }
          })()
        )
      }

      await Promise.all(promises)

      expect(manager.getActiveCount()).toBe(0)
    })

    it('handles operations with undefined id', () => {
      const manager = new TransactionManager<string>()
      const tx = manager.begin('test')

      tx.recordOperation({ type: 'custom', target: 'system' })

      const ops = tx.getOperations()
      expect(ops[0]?.id).toBeUndefined()
    })
  })

  describe('isTransaction type guard', () => {
    it('validates all required properties', () => {
      const valid = {
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

      expect(isTransaction(valid)).toBe(true)
    })

    it('rejects missing id', () => {
      const invalid = {
        status: 'pending',
        commit: async () => {},
        rollback: async () => {},
        isActive: () => true,
      }

      expect(isTransaction(invalid)).toBe(false)
    })

    it('rejects missing commit', () => {
      const invalid = {
        id: 'test',
        status: 'pending',
        rollback: async () => {},
        isActive: () => true,
      }

      expect(isTransaction(invalid)).toBe(false)
    })

    it('rejects primitives', () => {
      expect(isTransaction('string')).toBe(false)
      expect(isTransaction(123)).toBe(false)
      expect(isTransaction(true)).toBe(false)
    })
  })
})
