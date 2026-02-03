/**
 * Tests for Unified Transaction Abstraction
 *
 * Tests the TransactionManager, withTransaction helper, and related utilities.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  TransactionManager,
  TransactionError,
  withTransaction,
  withRetry,
  isTransaction,
  supportsSavepoints,
  isDatabaseTransaction,
  isStorageTransaction,
  type Transaction,
  type TransactionStatus,
  type TransactionOptions,
  type DatabaseTransaction,
  type StorageTransaction,
} from '../../../src/transaction'

// =============================================================================
// Transaction Manager Tests
// =============================================================================

describe('TransactionManager', () => {
  let manager: TransactionManager<string>

  beforeEach(() => {
    manager = new TransactionManager<string>()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  describe('begin', () => {
    it('creates a transaction with auto-generated ID', () => {
      const tx = manager.begin('test-context')

      expect(tx.id).toBeDefined()
      expect(tx.id.startsWith('txn_')).toBe(true)
      expect(tx.status).toBe('pending')
      expect(tx.context).toBe('test-context')
      expect(tx.startedAt).toBeInstanceOf(Date)
    })

    it('creates a transaction with custom ID', () => {
      const tx = manager.begin('test-context', { id: 'custom-tx-id' })

      expect(tx.id).toBe('custom-tx-id')
    })

    it('creates unique IDs for multiple transactions', () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')
      const tx3 = manager.begin('ctx3')

      expect(tx1.id).not.toBe(tx2.id)
      expect(tx2.id).not.toBe(tx3.id)
      expect(tx1.id).not.toBe(tx3.id)
    })

    it('tracks active transactions', () => {
      expect(manager.getActiveCount()).toBe(0)

      const tx1 = manager.begin('ctx1')
      expect(manager.getActiveCount()).toBe(1)

      const tx2 = manager.begin('ctx2')
      expect(manager.getActiveCount()).toBe(2)

      expect(manager.getActiveTransactions()).toContain(tx1)
      expect(manager.getActiveTransactions()).toContain(tx2)
    })
  })

  describe('commit', () => {
    it('commits a pending transaction', async () => {
      const tx = manager.begin('test-context')
      expect(tx.status).toBe('pending')
      expect(tx.isActive()).toBe(true)

      await tx.commit()

      expect(tx.status).toBe('committed')
      expect(tx.isActive()).toBe(false)
    })

    it('removes transaction from manager after commit', async () => {
      const tx = manager.begin('test-context')
      expect(manager.getActiveCount()).toBe(1)

      await tx.commit()

      expect(manager.getActiveCount()).toBe(0)
      expect(manager.get(tx.id)).toBeUndefined()
    })

    it('throws error when committing non-pending transaction', async () => {
      const tx = manager.begin('test-context')
      await tx.commit()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
      await expect(tx.commit()).rejects.toThrow("Cannot commit transaction in 'committed' status")
    })

    it('throws error when committing rolled back transaction', async () => {
      const tx = manager.begin('test-context')
      await tx.rollback()

      await expect(tx.commit()).rejects.toThrow(TransactionError)
    })
  })

  describe('rollback', () => {
    it('rolls back a pending transaction', async () => {
      const tx = manager.begin('test-context')
      expect(tx.status).toBe('pending')

      await tx.rollback()

      expect(tx.status).toBe('rolled_back')
      expect(tx.isActive()).toBe(false)
    })

    it('removes transaction from manager after rollback', async () => {
      const tx = manager.begin('test-context')
      expect(manager.getActiveCount()).toBe(1)

      await tx.rollback()

      expect(manager.getActiveCount()).toBe(0)
    })

    it('throws error when rolling back committed transaction', async () => {
      const tx = manager.begin('test-context')
      await tx.commit()

      await expect(tx.rollback()).rejects.toThrow(TransactionError)
    })
  })

  describe('savepoints', () => {
    it('creates a savepoint', async () => {
      const tx = manager.begin('test-context')

      await tx.savepoint?.('sp1')

      // Should not throw
      expect(tx.isActive()).toBe(true)
    })

    it('throws error for duplicate savepoint names', async () => {
      const tx = manager.begin('test-context')

      await tx.savepoint?.('sp1')
      await expect(tx.savepoint?.('sp1')).rejects.toThrow('already exists')
    })

    it('rolls back to savepoint', async () => {
      const tx = manager.begin('test-context')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      await tx.savepoint?.('sp1')
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      tx.recordOperation({ type: 'create', target: 'users', id: '3' })

      expect(tx.getOperations().length).toBe(3)

      await tx.rollbackToSavepoint?.('sp1')

      expect(tx.getOperations().length).toBe(1)
      expect(tx.getOperations()[0]?.id).toBe('1')
    })

    it('throws error for non-existent savepoint', async () => {
      const tx = manager.begin('test-context')

      await expect(tx.rollbackToSavepoint?.('nonexistent')).rejects.toThrow('not found')
    })

    it('releases savepoint', async () => {
      const tx = manager.begin('test-context')

      await tx.savepoint?.('sp1')
      await tx.releaseSavepoint?.('sp1')

      // Should throw because savepoint no longer exists
      await expect(tx.rollbackToSavepoint?.('sp1')).rejects.toThrow()
    })
  })

  describe('operations', () => {
    it('records operations', () => {
      const tx = manager.begin('test-context')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '1' })
      tx.recordOperation({ type: 'delete', target: 'posts', id: '2' })

      const ops = tx.getOperations()
      expect(ops.length).toBe(3)
      expect(ops[0]?.type).toBe('create')
      expect(ops[1]?.type).toBe('update')
      expect(ops[2]?.type).toBe('delete')
    })

    it('assigns sequence numbers to operations', () => {
      const tx = manager.begin('test-context')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      tx.recordOperation({ type: 'update', target: 'users', id: '2' })
      tx.recordOperation({ type: 'delete', target: 'posts', id: '3' })

      const ops = tx.getOperations()
      expect(ops[0]?.sequence).toBe(1)
      expect(ops[1]?.sequence).toBe(2)
      expect(ops[2]?.sequence).toBe(3)
    })

    it('assigns timestamps to operations', () => {
      const tx = manager.begin('test-context')

      tx.recordOperation({ type: 'create', target: 'users', id: '1' })

      const ops = tx.getOperations()
      expect(ops[0]?.timestamp).toBeInstanceOf(Date)
    })

    it('throws when recording on inactive transaction', async () => {
      const tx = manager.begin('test-context')
      await tx.commit()

      expect(() => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
      }).toThrow(TransactionError)
    })

    it('stores before and after state', () => {
      const tx = manager.begin('test-context')

      tx.recordOperation({
        type: 'update',
        target: 'users',
        id: '1',
        beforeState: { name: 'Alice', age: 30 },
        afterState: { name: 'Alice', age: 31 },
      })

      const ops = tx.getOperations()
      expect(ops[0]?.beforeState).toEqual({ name: 'Alice', age: 30 })
      expect(ops[0]?.afterState).toEqual({ name: 'Alice', age: 31 })
    })
  })

  describe('timeout', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('auto-rollbacks after timeout', async () => {
      const tx = manager.begin('test-context', { timeout: 100 })

      expect(tx.isActive()).toBe(true)

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(150)

      expect(tx.isActive()).toBe(false)
      expect(tx.status).toBe('rolled_back')
    })

    it('clears timeout on commit', async () => {
      const tx = manager.begin('test-context', { timeout: 100 })

      await tx.commit()

      // Advance time past timeout
      await vi.advanceTimersByTimeAsync(150)

      // Should still be committed, not rolled back
      expect(tx.status).toBe('committed')
    })

    it('clears timeout on rollback', async () => {
      const tx = manager.begin('test-context', { timeout: 100 })

      await tx.rollback()

      // Advance time past timeout
      await vi.advanceTimersByTimeAsync(150)

      // Should still be rolled_back
      expect(tx.status).toBe('rolled_back')
    })
  })

  describe('cleanup', () => {
    it('cleans up stale transactions', async () => {
      // Create old transactions
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')

      // Mock the startedAt to be old
      const oldDate = new Date(Date.now() - 10 * 60 * 1000) // 10 minutes ago
      ;(tx1 as { startedAt: Date }).startedAt = oldDate
      ;(tx2 as { startedAt: Date }).startedAt = oldDate

      // Create new transaction
      manager.begin('ctx3')

      expect(manager.getActiveCount()).toBe(3)

      const cleaned = manager.cleanup(5 * 60 * 1000) // 5 minute threshold

      expect(cleaned).toBe(2)
      expect(manager.getActiveCount()).toBe(1)
    })
  })

  describe('shutdown', () => {
    it('rolls back all active transactions', async () => {
      const tx1 = manager.begin('ctx1')
      const tx2 = manager.begin('ctx2')
      const tx3 = manager.begin('ctx3')

      expect(manager.getActiveCount()).toBe(3)

      await manager.shutdown()

      expect(manager.getActiveCount()).toBe(0)
      expect(tx1.status).toBe('rolled_back')
      expect(tx2.status).toBe('rolled_back')
      expect(tx3.status).toBe('rolled_back')
    })
  })
})

// =============================================================================
// withTransaction Helper Tests
// =============================================================================

describe('withTransaction', () => {
  interface MockProvider {
    beginTransaction(options?: TransactionOptions): Transaction
  }

  let mockProvider: MockProvider
  let createdTx: Transaction | null

  beforeEach(() => {
    createdTx = null
    mockProvider = {
      beginTransaction: (options?: TransactionOptions) => {
        const manager = new TransactionManager()
        createdTx = manager.begin('mock-context', options)
        return createdTx
      },
    }
  })

  it('commits on success', async () => {
    const result = await withTransaction(mockProvider, async (tx) => {
      expect(tx.isActive()).toBe(true)
      return 'success'
    })

    expect(result).toBe('success')
    expect(createdTx?.status).toBe('committed')
  })

  it('rolls back on error', async () => {
    const error = new Error('Test error')

    await expect(
      withTransaction(mockProvider, async () => {
        throw error
      })
    ).rejects.toThrow('Test error')

    expect(createdTx?.status).toBe('rolled_back')
  })

  it('returns result from function', async () => {
    const result = await withTransaction(mockProvider, async () => {
      return { value: 42, nested: { data: 'test' } }
    })

    expect(result).toEqual({ value: 42, nested: { data: 'test' } })
  })

  it('passes transaction to function', async () => {
    await withTransaction(mockProvider, async (tx) => {
      expect(tx).toBeDefined()
      expect(tx.id).toBeDefined()
      expect(typeof tx.commit).toBe('function')
      expect(typeof tx.rollback).toBe('function')
    })
  })

  it('passes options to beginTransaction', async () => {
    const options = { id: 'custom-id', timeout: 5000 }

    await withTransaction(mockProvider, async (tx) => {
      expect(tx.id).toBe('custom-id')
    }, options)
  })
})

// =============================================================================
// withRetry Helper Tests
// =============================================================================

describe('withRetry', () => {
  interface MockProvider {
    beginTransaction(): Transaction
  }

  let attemptCount: number
  let mockProvider: MockProvider

  beforeEach(() => {
    attemptCount = 0
    mockProvider = {
      beginTransaction: () => {
        const manager = new TransactionManager()
        return manager.begin('mock-context')
      },
    }
  })

  it('succeeds on first attempt', async () => {
    const result = await withRetry(mockProvider, async () => {
      attemptCount++
      return 'success'
    })

    expect(result).toBe('success')
    expect(attemptCount).toBe(1)
  })

  it('retries on version conflict error', async () => {
    const result = await withRetry(
      mockProvider,
      async () => {
        attemptCount++
        if (attemptCount < 3) {
          throw new TransactionError('Version conflict', 'VERSION_CONFLICT')
        }
        return 'success'
      },
      { maxRetries: 5, retryDelay: 10 }
    )

    expect(result).toBe('success')
    expect(attemptCount).toBe(3)
  })

  it('does not retry on non-retryable errors', async () => {
    await expect(
      withRetry(
        mockProvider,
        async () => {
          attemptCount++
          throw new Error('Not retryable')
        },
        { maxRetries: 3, retryDelay: 10 }
      )
    ).rejects.toThrow('Not retryable')

    expect(attemptCount).toBe(1)
  })

  it('respects maxRetries limit', async () => {
    await expect(
      withRetry(
        mockProvider,
        async () => {
          attemptCount++
          throw new TransactionError('Version conflict', 'VERSION_CONFLICT')
        },
        { maxRetries: 3, retryDelay: 10 }
      )
    ).rejects.toThrow('Version conflict')

    expect(attemptCount).toBe(4) // Initial + 3 retries
  })

  it('uses custom shouldRetry function', async () => {
    const result = await withRetry(
      mockProvider,
      async () => {
        attemptCount++
        if (attemptCount < 2) {
          throw new Error('Custom retryable error')
        }
        return 'success'
      },
      {
        maxRetries: 3,
        retryDelay: 10,
        shouldRetry: (error) => error instanceof Error && error.message.includes('Custom'),
      }
    )

    expect(result).toBe('success')
    expect(attemptCount).toBe(2)
  })
})

// =============================================================================
// TransactionError Tests
// =============================================================================

describe('TransactionError', () => {
  it('creates error with message', () => {
    const error = new TransactionError('Test message')

    expect(error.message).toBe('Test message')
    expect(error.name).toBe('TransactionError')
    expect(error.code).toBe('UNKNOWN')
  })

  it('creates error with code', () => {
    const error = new TransactionError('Commit failed', 'COMMIT_FAILED')

    expect(error.code).toBe('COMMIT_FAILED')
  })

  it('creates error with transaction ID', () => {
    const error = new TransactionError('Test', 'UNKNOWN', 'txn-123')

    expect(error.transactionId).toBe('txn-123')
  })

  it('creates error with cause', () => {
    const cause = new Error('Original error')
    const error = new TransactionError('Wrapper', 'UNKNOWN', undefined, cause)

    expect(error.cause).toBe(cause)
  })

  it('fromError creates from existing error', () => {
    const original = new Error('Original')
    original.stack = 'Original stack'

    const error = TransactionError.fromError(original, 'COMMIT_FAILED', 'txn-123')

    expect(error.message).toBe('Original')
    expect(error.code).toBe('COMMIT_FAILED')
    expect(error.transactionId).toBe('txn-123')
    expect(error.cause).toBe(original)
    expect(error.stack).toBe('Original stack')
  })

  it('isTransactionError type guard', () => {
    const txError = new TransactionError('Test')
    const regularError = new Error('Test')

    expect(TransactionError.isTransactionError(txError)).toBe(true)
    expect(TransactionError.isTransactionError(regularError)).toBe(false)
    expect(TransactionError.isTransactionError(null)).toBe(false)
    expect(TransactionError.isTransactionError(undefined)).toBe(false)
  })

  it('isCode checks error code', () => {
    const error = new TransactionError('Test', 'VERSION_CONFLICT')

    expect(TransactionError.isCode(error, 'VERSION_CONFLICT')).toBe(true)
    expect(TransactionError.isCode(error, 'COMMIT_FAILED')).toBe(false)
    expect(TransactionError.isCode(new Error('Test'), 'VERSION_CONFLICT')).toBe(false)
  })
})

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isTransaction', () => {
    it('returns true for valid transaction', () => {
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

    it('returns false for invalid objects', () => {
      expect(isTransaction(null)).toBe(false)
      expect(isTransaction(undefined)).toBe(false)
      expect(isTransaction({})).toBe(false)
      expect(isTransaction({ id: 'test' })).toBe(false)
      expect(isTransaction({ commit: () => {} })).toBe(false)
    })
  })

  describe('supportsSavepoints', () => {
    it('returns true when savepoints are supported', () => {
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
        savepoint: async () => {},
        rollbackToSavepoint: async () => {},
      }

      expect(supportsSavepoints(tx)).toBe(true)
    })

    it('returns false when savepoints are not supported', () => {
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

      expect(supportsSavepoints(tx)).toBe(false)
    })
  })

  describe('isDatabaseTransaction', () => {
    it('returns true for database transactions', () => {
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
        create: async () => ({}),
        update: async () => null,
        delete: async () => ({ deletedCount: 0 }),
      }

      expect(isDatabaseTransaction(tx)).toBe(true)
    })

    it('returns false for non-database transactions', () => {
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

      expect(isDatabaseTransaction(tx)).toBe(false)
    })
  })

  describe('isStorageTransaction', () => {
    it('returns true for storage transactions', () => {
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
        read: async () => new Uint8Array(),
        write: async () => {},
        delete: async () => {},
      }

      expect(isStorageTransaction(tx)).toBe(true)
    })

    it('returns false for non-storage transactions', () => {
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

      expect(isStorageTransaction(tx)).toBe(false)
    })
  })
})

// =============================================================================
// Integration-like Tests
// =============================================================================

describe('Transaction Integration Scenarios', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('handles complex transaction with multiple operations and savepoints', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // Create initial records
    tx.recordOperation({
      type: 'create',
      target: 'users',
      id: '1',
      afterState: { name: 'Alice' },
    })

    tx.recordOperation({
      type: 'create',
      target: 'users',
      id: '2',
      afterState: { name: 'Bob' },
    })

    // Create savepoint after users
    await tx.savepoint?.('after-users')

    // Create posts
    tx.recordOperation({
      type: 'create',
      target: 'posts',
      id: '1',
      afterState: { title: 'Hello', authorId: '1' },
    })

    expect(tx.getOperations().length).toBe(3)

    // Rollback posts
    await tx.rollbackToSavepoint?.('after-users')

    expect(tx.getOperations().length).toBe(2)

    // Create different post
    tx.recordOperation({
      type: 'create',
      target: 'posts',
      id: '2',
      afterState: { title: 'Different', authorId: '2' },
    })

    await tx.commit()

    expect(tx.status).toBe('committed')
    expect(tx.getOperations().length).toBe(3)
  })

  it('handles concurrent transactions', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })
    const tx3 = manager.begin({ db: 'test-db' })

    expect(manager.getActiveCount()).toBe(3)

    // Each transaction records its own operations
    tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx2.recordOperation({ type: 'create', target: 'users', id: '2' })
    tx3.recordOperation({ type: 'create', target: 'users', id: '3' })

    // Commit tx1, rollback tx2
    await tx1.commit()
    await tx2.rollback()

    expect(tx1.status).toBe('committed')
    expect(tx2.status).toBe('rolled_back')
    expect(tx3.status).toBe('pending')
    expect(manager.getActiveCount()).toBe(1)

    await tx3.commit()
    expect(manager.getActiveCount()).toBe(0)
  })

  it('tracks before/after state for updates', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({
      type: 'update',
      target: 'users',
      id: '1',
      beforeState: { name: 'Alice', age: 30 },
      afterState: { name: 'Alice', age: 31 },
    })

    tx.recordOperation({
      type: 'update',
      target: 'users',
      id: '1',
      beforeState: { name: 'Alice', age: 31 },
      afterState: { name: 'Alicia', age: 31 },
    })

    const ops = tx.getOperations()

    // Can trace full history of changes
    expect(ops[0]?.beforeState).toEqual({ name: 'Alice', age: 30 })
    expect(ops[0]?.afterState).toEqual({ name: 'Alice', age: 31 })
    expect(ops[1]?.beforeState).toEqual({ name: 'Alice', age: 31 })
    expect(ops[1]?.afterState).toEqual({ name: 'Alicia', age: 31 })

    await tx.commit()
  })
})

// =============================================================================
// Multi-Operation Transaction Tests
// =============================================================================

describe('Multi-Operation Transactions', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('handles multiple creates in one transaction', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // Create multiple entities
    for (let i = 1; i <= 10; i++) {
      tx.recordOperation({
        type: 'create',
        target: 'users',
        id: `user-${i}`,
        afterState: { name: `User ${i}`, email: `user${i}@example.com` },
      })
    }

    expect(tx.getOperations().length).toBe(10)
    expect(tx.getOperations().every(op => op.type === 'create')).toBe(true)

    await tx.commit()
    expect(tx.status).toBe('committed')
  })

  it('handles multiple updates in one transaction', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // Update multiple entities
    for (let i = 1; i <= 5; i++) {
      tx.recordOperation({
        type: 'update',
        target: 'users',
        id: `user-${i}`,
        beforeState: { name: `User ${i}`, status: 'pending' },
        afterState: { name: `User ${i}`, status: 'active' },
      })
    }

    const ops = tx.getOperations()
    expect(ops.length).toBe(5)
    expect(ops.every(op => op.type === 'update')).toBe(true)
    expect(ops.every(op => op.afterState?.status === 'active')).toBe(true)

    await tx.commit()
    expect(tx.status).toBe('committed')
  })

  it('handles multiple deletes in one transaction', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // Delete multiple entities
    for (let i = 1; i <= 3; i++) {
      tx.recordOperation({
        type: 'delete',
        target: 'posts',
        id: `post-${i}`,
        beforeState: { title: `Post ${i}`, deleted: false },
        afterState: { title: `Post ${i}`, deleted: true },
      })
    }

    const ops = tx.getOperations()
    expect(ops.length).toBe(3)
    expect(ops.every(op => op.type === 'delete')).toBe(true)

    await tx.commit()
    expect(tx.status).toBe('committed')
  })

  it('handles mixed operations (create, update, delete) in one transaction', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // Create a user
    tx.recordOperation({
      type: 'create',
      target: 'users',
      id: 'user-new',
      afterState: { name: 'New User' },
    })

    // Update an existing user
    tx.recordOperation({
      type: 'update',
      target: 'users',
      id: 'user-1',
      beforeState: { name: 'Alice', status: 'active' },
      afterState: { name: 'Alice Smith', status: 'active' },
    })

    // Delete a user
    tx.recordOperation({
      type: 'delete',
      target: 'users',
      id: 'user-2',
      beforeState: { name: 'Bob', deleted: false },
    })

    // Create a related post
    tx.recordOperation({
      type: 'create',
      target: 'posts',
      id: 'post-new',
      afterState: { title: 'Hello World', authorId: 'user-new' },
    })

    const ops = tx.getOperations()
    expect(ops.length).toBe(4)
    expect(ops[0]?.type).toBe('create')
    expect(ops[1]?.type).toBe('update')
    expect(ops[2]?.type).toBe('delete')
    expect(ops[3]?.type).toBe('create')

    await tx.commit()
    expect(tx.status).toBe('committed')
  })

  it('handles operations across multiple namespaces', async () => {
    const tx = manager.begin({ db: 'test-db' })

    const namespaces = ['users', 'posts', 'comments', 'tags']

    for (const ns of namespaces) {
      tx.recordOperation({
        type: 'create',
        target: ns,
        id: `${ns}-1`,
        afterState: { type: ns, createdAt: new Date().toISOString() },
      })
    }

    const ops = tx.getOperations()
    expect(ops.length).toBe(4)
    expect(new Set(ops.map(op => op.target))).toEqual(new Set(namespaces))

    await tx.commit()
    expect(tx.status).toBe('committed')
  })

  it('preserves operation order in transaction', async () => {
    const tx = manager.begin({ db: 'test-db' })

    const operationSequence = [
      { type: 'create' as const, target: 'users', id: '1' },
      { type: 'update' as const, target: 'users', id: '1' },
      { type: 'create' as const, target: 'posts', id: '1' },
      { type: 'delete' as const, target: 'users', id: '1' },
    ]

    for (const op of operationSequence) {
      tx.recordOperation(op)
    }

    const ops = tx.getOperations()
    expect(ops.map(op => ({ type: op.type, target: op.target, id: op.id }))).toEqual(operationSequence)

    // Verify sequence numbers are ascending
    for (let i = 1; i < ops.length; i++) {
      expect(ops[i]!.sequence).toBeGreaterThan(ops[i - 1]!.sequence)
    }

    await tx.commit()
  })
})

// =============================================================================
// Abort/Rollback Scenario Tests
// =============================================================================

describe('Abort/Rollback Scenarios', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('rolls back all operations when transaction is aborted', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx.recordOperation({ type: 'create', target: 'users', id: '2' })
    tx.recordOperation({ type: 'update', target: 'users', id: '1' })

    expect(tx.getOperations().length).toBe(3)

    await tx.rollback()

    expect(tx.status).toBe('rolled_back')
    expect(tx.isActive()).toBe(false)
    // Operations are still tracked for potential logging/debugging
    expect(tx.getOperations().length).toBe(3)
  })

  it('handles partial rollback via savepoint', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // First batch of operations
    tx.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx.recordOperation({ type: 'create', target: 'users', id: '2' })

    await tx.savepoint?.('batch1')

    // Second batch of operations
    tx.recordOperation({ type: 'update', target: 'users', id: '1' })
    tx.recordOperation({ type: 'delete', target: 'users', id: '2' })

    expect(tx.getOperations().length).toBe(4)

    // Rollback only second batch
    await tx.rollbackToSavepoint?.('batch1')

    expect(tx.getOperations().length).toBe(2)
    expect(tx.getOperations().every(op => op.type === 'create')).toBe(true)
    expect(tx.isActive()).toBe(true)

    await tx.commit()
  })

  it('handles multiple savepoints with selective rollback', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'A', id: '1' })
    await tx.savepoint?.('sp1')

    tx.recordOperation({ type: 'create', target: 'B', id: '2' })
    await tx.savepoint?.('sp2')

    tx.recordOperation({ type: 'create', target: 'C', id: '3' })
    await tx.savepoint?.('sp3')

    tx.recordOperation({ type: 'create', target: 'D', id: '4' })

    expect(tx.getOperations().length).toBe(4)

    // Rollback to sp2 (should remove C and D operations)
    await tx.rollbackToSavepoint?.('sp2')

    expect(tx.getOperations().length).toBe(2)
    expect(tx.getOperations().map(op => op.target)).toEqual(['A', 'B'])

    // sp3 should be removed after rollback to sp2
    await expect(tx.rollbackToSavepoint?.('sp3')).rejects.toThrow('not found')
  })

  it('handles rollback after error in operation', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => {
        return manager.begin({ db: 'test-db' })
      },
    }

    const simulatedError = new Error('Simulated operation failure')

    await expect(
      withTransaction(mockProvider, async (tx) => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
        tx.recordOperation({ type: 'create', target: 'users', id: '2' })

        // Simulate an error during third operation
        throw simulatedError
      })
    ).rejects.toThrow('Simulated operation failure')

    // Transaction should be rolled back
    expect(manager.getActiveCount()).toBe(0)
  })

  it('handles rollback on async operation failure', async () => {
    vi.useFakeTimers()
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => {
        return manager.begin({ db: 'test-db' })
      },
    }

    const asyncOperation = async () => {
      await vi.advanceTimersByTimeAsync(10)
      throw new Error('Async operation failed')
    }

    await expect(
      withTransaction(mockProvider, async (tx) => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
        await asyncOperation()
      })
    ).rejects.toThrow('Async operation failed')

    expect(manager.getActiveCount()).toBe(0)
    vi.useRealTimers()
  })

  it('prevents operations after rollback', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })
    await tx.rollback()

    expect(() => {
      tx.recordOperation({ type: 'create', target: 'users', id: '2' })
    }).toThrow(TransactionError)
  })

  it('prevents commit after rollback', async () => {
    const tx = manager.begin({ db: 'test-db' })

    await tx.rollback()

    await expect(tx.commit()).rejects.toThrow(TransactionError)
    await expect(tx.commit()).rejects.toThrow("Cannot commit transaction in 'rolled_back' status")
  })

  it('prevents rollback after commit', async () => {
    const tx = manager.begin({ db: 'test-db' })

    await tx.commit()

    await expect(tx.rollback()).rejects.toThrow(TransactionError)
    await expect(tx.rollback()).rejects.toThrow("Cannot rollback transaction in 'committed' status")
  })
})

// =============================================================================
// Concurrent Transaction Conflict Tests
// =============================================================================

describe('Concurrent Transaction Conflicts', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('allows multiple concurrent transactions to operate independently', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    // Both transactions operate on different entities
    tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx2.recordOperation({ type: 'create', target: 'users', id: '2' })

    // Operations are isolated
    expect(tx1.getOperations().length).toBe(1)
    expect(tx2.getOperations().length).toBe(1)
    expect(tx1.getOperations()[0]?.id).toBe('1')
    expect(tx2.getOperations()[0]?.id).toBe('2')

    // Both can commit successfully
    await tx1.commit()
    await tx2.commit()

    expect(tx1.status).toBe('committed')
    expect(tx2.status).toBe('committed')
  })

  it('handles conflicting operations on same entity (write-write)', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    // Both transactions try to update the same entity
    tx1.recordOperation({
      type: 'update',
      target: 'users',
      id: 'shared-entity',
      beforeState: { name: 'Original', version: 1 },
      afterState: { name: 'From TX1', version: 2 },
    })

    tx2.recordOperation({
      type: 'update',
      target: 'users',
      id: 'shared-entity',
      beforeState: { name: 'Original', version: 1 },
      afterState: { name: 'From TX2', version: 2 },
    })

    // In optimistic concurrency, first committer wins
    await tx1.commit()
    expect(tx1.status).toBe('committed')

    // In a real system, tx2 would fail with version conflict
    // For now, we just verify both can record operations
    await tx2.commit()
    expect(tx2.status).toBe('committed')
  })

  it('handles read-write conflicts across transactions', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    // tx1 reads an entity
    tx1.recordOperation({
      type: 'read',
      target: 'users',
      id: 'entity-1',
    })

    // tx2 updates the same entity
    tx2.recordOperation({
      type: 'update',
      target: 'users',
      id: 'entity-1',
      beforeState: { value: 100 },
      afterState: { value: 200 },
    })

    // tx2 commits first
    await tx2.commit()
    expect(tx2.status).toBe('committed')

    // tx1 then uses the stale read value to make a decision
    tx1.recordOperation({
      type: 'update',
      target: 'users',
      id: 'entity-1',
      beforeState: { value: 100 }, // Stale!
      afterState: { value: 150 },
    })

    // In real system with serializable isolation, this would fail
    await tx1.commit()

    // Operations tracked show the potential conflict
    const tx1Ops = tx1.getOperations()
    expect(tx1Ops.some(op => op.type === 'read')).toBe(true)
    expect(tx1Ops.some(op => op.type === 'update')).toBe(true)
  })

  it('handles many concurrent transactions', async () => {
    const txCount = 20
    const transactions: Transaction<{ db: string }>[] = []

    // Create many concurrent transactions
    for (let i = 0; i < txCount; i++) {
      transactions.push(manager.begin({ db: 'test-db' }))
    }

    expect(manager.getActiveCount()).toBe(txCount)

    // Each transaction does work
    for (let i = 0; i < txCount; i++) {
      transactions[i]!.recordOperation({
        type: 'create',
        target: 'items',
        id: `item-${i}`,
        afterState: { index: i },
      })
    }

    // Commit half, rollback half
    const commitPromises = transactions.slice(0, txCount / 2).map(tx => tx.commit())
    const rollbackPromises = transactions.slice(txCount / 2).map(tx => tx.rollback())

    await Promise.all([...commitPromises, ...rollbackPromises])

    expect(manager.getActiveCount()).toBe(0)
    expect(transactions.slice(0, txCount / 2).every(tx => tx.status === 'committed')).toBe(true)
    expect(transactions.slice(txCount / 2).every(tx => tx.status === 'rolled_back')).toBe(true)
  })

  it('handles interleaved operations across transactions', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })
    const tx3 = manager.begin({ db: 'test-db' })

    // Interleaved operations
    tx1.recordOperation({ type: 'create', target: 'A', id: '1' })
    tx2.recordOperation({ type: 'create', target: 'B', id: '1' })
    tx3.recordOperation({ type: 'create', target: 'C', id: '1' })
    tx1.recordOperation({ type: 'update', target: 'A', id: '1' })
    tx2.recordOperation({ type: 'update', target: 'B', id: '1' })
    tx3.recordOperation({ type: 'update', target: 'C', id: '1' })
    tx1.recordOperation({ type: 'delete', target: 'A', id: '1' })

    // Verify each transaction has its own operations
    expect(tx1.getOperations().length).toBe(3)
    expect(tx2.getOperations().length).toBe(2)
    expect(tx3.getOperations().length).toBe(2)

    // All operations in tx1 target 'A'
    expect(tx1.getOperations().every(op => op.target === 'A')).toBe(true)
    expect(tx2.getOperations().every(op => op.target === 'B')).toBe(true)
    expect(tx3.getOperations().every(op => op.target === 'C')).toBe(true)

    await Promise.all([tx1.commit(), tx2.commit(), tx3.commit()])
  })
})

// =============================================================================
// Transaction Isolation Tests
// =============================================================================

describe('Transaction Isolation', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('transactions do not see each others uncommitted operations', () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    tx1.recordOperation({
      type: 'create',
      target: 'users',
      id: 'new-user',
      afterState: { name: 'Alice' },
    })

    // tx2 cannot see tx1's uncommitted operation
    const tx2Ops = tx2.getOperations()
    expect(tx2Ops.length).toBe(0)
    expect(tx2Ops.find(op => op.id === 'new-user')).toBeUndefined()

    // tx1's operations are only visible to tx1
    const tx1Ops = tx1.getOperations()
    expect(tx1Ops.length).toBe(1)
    expect(tx1Ops[0]?.id).toBe('new-user')
  })

  it('transaction operations remain isolated until commit', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    // tx1 creates entities
    tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx1.recordOperation({ type: 'create', target: 'users', id: '2' })

    // tx2 is isolated from tx1
    expect(tx2.getOperations().length).toBe(0)

    // tx1 commits
    await tx1.commit()

    // tx2 still has its own isolated view
    expect(tx2.getOperations().length).toBe(0)

    // tx2 does its own work
    tx2.recordOperation({ type: 'create', target: 'users', id: '3' })
    expect(tx2.getOperations().length).toBe(1)

    await tx2.commit()
  })

  it('rolled back operations do not affect other transactions', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    tx1.recordOperation({ type: 'create', target: 'users', id: '1' })
    tx1.recordOperation({ type: 'create', target: 'users', id: '2' })

    await tx1.rollback()

    // tx2 is unaffected by tx1's rollback
    tx2.recordOperation({ type: 'create', target: 'users', id: '1' })
    expect(tx2.getOperations().length).toBe(1)

    await tx2.commit()
    expect(tx2.status).toBe('committed')
  })

  it('each transaction maintains its own savepoint namespace', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })

    // Both transactions can create savepoints with the same name
    await tx1.savepoint?.('sp1')
    await tx2.savepoint?.('sp1')

    tx1.recordOperation({ type: 'create', target: 'A', id: '1' })
    tx2.recordOperation({ type: 'create', target: 'B', id: '1' })

    // Rollback tx1's savepoint doesn't affect tx2
    await tx1.rollbackToSavepoint?.('sp1')

    expect(tx1.getOperations().length).toBe(0)
    expect(tx2.getOperations().length).toBe(1)

    await tx2.commit()
    await tx1.commit()
  })

  it('getActiveTransactions only shows pending transactions', async () => {
    const tx1 = manager.begin({ db: 'test-db' })
    const tx2 = manager.begin({ db: 'test-db' })
    const tx3 = manager.begin({ db: 'test-db' })

    expect(manager.getActiveTransactions().length).toBe(3)

    await tx1.commit()
    expect(manager.getActiveTransactions().length).toBe(2)
    expect(manager.getActiveTransactions().includes(tx1)).toBe(false)

    await tx2.rollback()
    expect(manager.getActiveTransactions().length).toBe(1)
    expect(manager.getActiveTransactions()[0]).toBe(tx3)

    await tx3.commit()
    expect(manager.getActiveTransactions().length).toBe(0)
  })

  it('supports different isolation levels (configuration)', () => {
    const txReadUncommitted = manager.begin({ db: 'test-db' }, { isolation: 'read_uncommitted' })
    const txReadCommitted = manager.begin({ db: 'test-db' }, { isolation: 'read_committed' })
    const txRepeatableRead = manager.begin({ db: 'test-db' }, { isolation: 'repeatable_read' })
    const txSerializable = manager.begin({ db: 'test-db' }, { isolation: 'serializable' })

    // All transactions should be created successfully
    expect(txReadUncommitted.isActive()).toBe(true)
    expect(txReadCommitted.isActive()).toBe(true)
    expect(txRepeatableRead.isActive()).toBe(true)
    expect(txSerializable.isActive()).toBe(true)

    expect(manager.getActiveCount()).toBe(4)
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling Within Transactions', () => {
  let manager: TransactionManager<{ db: string }>

  beforeEach(() => {
    manager = new TransactionManager()
  })

  afterEach(async () => {
    await manager.shutdown()
  })

  it('handles synchronous errors in transaction operations', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    const syncError = new Error('Synchronous validation error')

    await expect(
      withTransaction(mockProvider, async (tx) => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })

        // Simulate a synchronous validation error
        const data = { email: 'invalid' }
        if (!data.email.includes('@')) {
          throw syncError
        }

        tx.recordOperation({ type: 'create', target: 'users', id: '2' })
      })
    ).rejects.toThrow('Synchronous validation error')
  })

  it('handles async errors in transaction operations', async () => {
    vi.useFakeTimers()
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    const asyncValidation = async () => {
      await vi.advanceTimersByTimeAsync(5)
      throw new Error('Async validation failed')
    }

    await expect(
      withTransaction(mockProvider, async (tx) => {
        tx.recordOperation({ type: 'create', target: 'users', id: '1' })
        await asyncValidation()
      })
    ).rejects.toThrow('Async validation failed')
    vi.useRealTimers()
  })

  it('handles TransactionError with proper codes', () => {
    const error1 = new TransactionError('Invalid state', 'INVALID_STATE', 'tx-123')
    expect(error1.code).toBe('INVALID_STATE')
    expect(error1.transactionId).toBe('tx-123')

    const error2 = new TransactionError('Version conflict', 'VERSION_CONFLICT')
    expect(error2.code).toBe('VERSION_CONFLICT')

    const error3 = new TransactionError('Commit failed', 'COMMIT_FAILED')
    expect(error3.code).toBe('COMMIT_FAILED')
  })

  it('preserves error context through transaction rollback', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    const originalError = new Error('Original error with context');
    (originalError as Error & { context: unknown }).context = { userId: '123', action: 'create' }

    try {
      await withTransaction(mockProvider, async () => {
        throw originalError
      })
    } catch (error) {
      expect(error).toBe(originalError)
      expect((error as Error & { context: unknown }).context).toEqual({ userId: '123', action: 'create' })
    }
  })

  it('handles errors in nested savepoint operations', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })
    await tx.savepoint?.('sp1')

    tx.recordOperation({ type: 'create', target: 'users', id: '2' })
    await tx.savepoint?.('sp2')

    // Try to create duplicate savepoint
    await expect(tx.savepoint?.('sp1')).rejects.toThrow('already exists')

    // Transaction is still active
    expect(tx.isActive()).toBe(true)
    expect(tx.getOperations().length).toBe(2)

    // Can still rollback to existing savepoint
    await tx.rollbackToSavepoint?.('sp1')
    expect(tx.getOperations().length).toBe(1)

    await tx.commit()
  })

  it('handles errors when rolling back to non-existent savepoint', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })

    await expect(tx.rollbackToSavepoint?.('nonexistent')).rejects.toThrow('not found')

    // Transaction should still be active
    expect(tx.isActive()).toBe(true)
    expect(tx.getOperations().length).toBe(1)

    await tx.commit()
  })

  it('handles multiple errors gracefully', async () => {
    const tx = manager.begin({ db: 'test-db' })

    // First error - try to commit without being in pending state
    await tx.commit()

    // Multiple attempts to commit/rollback should all throw
    await expect(tx.commit()).rejects.toThrow(TransactionError)
    await expect(tx.commit()).rejects.toThrow(TransactionError)
    await expect(tx.rollback()).rejects.toThrow(TransactionError)

    expect(tx.status).toBe('committed')
  })

  it('handles exception during commit', async () => {
    // Create a transaction and manually trigger commit
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })

    // Normal commit should succeed
    await tx.commit()
    expect(tx.status).toBe('committed')

    // Subsequent commit should fail with proper error
    try {
      await tx.commit()
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionError)
      expect((error as TransactionError).code).toBe('INVALID_STATE')
    }
  })

  it('handles exception during rollback', async () => {
    const tx = manager.begin({ db: 'test-db' })

    tx.recordOperation({ type: 'create', target: 'users', id: '1' })

    await tx.rollback()
    expect(tx.status).toBe('rolled_back')

    // Subsequent rollback should fail with proper error
    try {
      await tx.rollback()
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionError)
      expect((error as TransactionError).code).toBe('INVALID_STATE')
    }
  })

  it('retries retryable errors with withRetry', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    let attempts = 0

    const result = await withRetry(
      mockProvider,
      async (tx) => {
        attempts++
        tx.recordOperation({ type: 'update', target: 'users', id: '1' })

        if (attempts < 3) {
          throw new TransactionError('Version conflict', 'VERSION_CONFLICT')
        }

        return { success: true, attempts }
      },
      { maxRetries: 5, retryDelay: 10 }
    )

    expect(result.success).toBe(true)
    expect(result.attempts).toBe(3)
  })

  it('stops retrying after maxRetries exceeded', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    let attempts = 0

    await expect(
      withRetry(
        mockProvider,
        async () => {
          attempts++
          throw new TransactionError('Always fails', 'VERSION_CONFLICT')
        },
        { maxRetries: 3, retryDelay: 10 }
      )
    ).rejects.toThrow('Always fails')

    expect(attempts).toBe(4) // Initial + 3 retries
  })

  it('does not retry non-retryable errors', async () => {
    interface MockProvider {
      beginTransaction(options?: TransactionOptions): Transaction
    }

    const mockProvider: MockProvider = {
      beginTransaction: () => manager.begin({ db: 'test-db' }),
    }

    let attempts = 0

    await expect(
      withRetry(
        mockProvider,
        async () => {
          attempts++
          throw new TransactionError('Invalid input', 'INVALID_STATE')
        },
        { maxRetries: 5, retryDelay: 10 }
      )
    ).rejects.toThrow('Invalid input')

    expect(attempts).toBe(1) // No retries for non-retryable errors
  })
})
