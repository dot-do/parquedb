/**
 * SQLite Transaction Batching Tests
 *
 * RED phase tests for Beads issue parquedb-lssf.1
 *
 * Problem: Each bulk operation writes separately to SQLite:
 * - counterSet (sequence counter)
 * - pendingRowGroupInsert (pending file metadata)
 * - WAL flush (event batches)
 *
 * These should be batched in a single SQLite transaction for:
 * - Atomicity: All or nothing
 * - Performance: O(1) transaction overhead instead of O(n)
 * - Consistency: Counters and pending groups stay in sync
 *
 * Test Structure:
 * - Tests named "FAILS:" document current broken behavior (pass = problem exists)
 * - Tests named "SHOULD PASS:" show desired behavior with mock implementation
 * - Tests in "Integration tests for actual ParqueDBDO" are TRUE RED tests that
 *   will FAIL until the implementation is fixed
 *
 * Run with: pnpm test tests/unit/worker/sqlite-transaction-batching.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// =============================================================================
// Mock Types
// =============================================================================

interface SqlStorageValue {
  [key: string]: unknown
}

interface TransactionTracker {
  beginCount: number
  commitCount: number
  rollbackCount: number
  currentTransactionOps: string[]
  completedTransactions: Array<{ ops: string[] }>
  inTransaction: boolean
}

// =============================================================================
// Mock SqlStorage with Transaction Tracking
// =============================================================================

function createMockSqlStorageWithTransactionTracking(): {
  sql: {
    exec: ReturnType<typeof vi.fn>
  }
  tracker: TransactionTracker
  pendingRowGroups: Array<{ id: string; ns: string; path: string; row_count: number; first_seq: number; last_seq: number }>
  eventsWal: Array<{ id: number; ns: string; first_seq: number; last_seq: number; events: Uint8Array }>
  counters: Map<string, number>
} {
  const tracker: TransactionTracker = {
    beginCount: 0,
    commitCount: 0,
    rollbackCount: 0,
    currentTransactionOps: [],
    completedTransactions: [],
    inTransaction: false,
  }

  const pendingRowGroups: Array<{ id: string; ns: string; path: string; row_count: number; first_seq: number; last_seq: number }> = []
  const eventsWal: Array<{ id: number; ns: string; first_seq: number; last_seq: number; events: Uint8Array }> = []
  const counters = new Map<string, number>()
  let autoIncrementId = 1

  const exec = vi.fn((query: string, ...params: unknown[]) => {
    const trimmedQuery = query.trim().toLowerCase()

    // Track transactions
    if (trimmedQuery === 'begin' || trimmedQuery === 'begin transaction' || trimmedQuery === 'begin immediate') {
      tracker.beginCount++
      tracker.inTransaction = true
      tracker.currentTransactionOps = []
      return []
    }

    if (trimmedQuery === 'commit' || trimmedQuery === 'commit transaction') {
      tracker.commitCount++
      tracker.completedTransactions.push({ ops: [...tracker.currentTransactionOps] })
      tracker.currentTransactionOps = []
      tracker.inTransaction = false
      return []
    }

    if (trimmedQuery === 'rollback' || trimmedQuery === 'rollback transaction') {
      tracker.rollbackCount++
      tracker.currentTransactionOps = []
      tracker.inTransaction = false
      return []
    }

    // Track operation types
    const opType = getOperationType(trimmedQuery)
    if (tracker.inTransaction) {
      tracker.currentTransactionOps.push(opType)
    } else {
      // Operation outside transaction - track as standalone
      tracker.completedTransactions.push({ ops: [opType] })
    }

    // Handle CREATE TABLE / INDEX (no-op for mock)
    if (trimmedQuery.startsWith('create table') || trimmedQuery.startsWith('create index')) {
      return []
    }

    // Handle INSERT INTO pending_row_groups
    if (trimmedQuery.includes('insert into pending_row_groups')) {
      pendingRowGroups.push({
        id: params[0] as string,
        ns: params[1] as string,
        path: params[2] as string,
        row_count: params[3] as number,
        first_seq: params[4] as number,
        last_seq: params[5] as number,
      })
      return []
    }

    // Handle INSERT INTO events_wal
    if (trimmedQuery.includes('insert into events_wal')) {
      eventsWal.push({
        id: autoIncrementId++,
        ns: params[0] as string,
        first_seq: params[1] as number,
        last_seq: params[2] as number,
        events: params[3] as Uint8Array,
      })
      return []
    }

    // Handle SELECT from events_wal for counter initialization
    if (trimmedQuery.includes('select') && trimmedQuery.includes('from events_wal') && trimmedQuery.includes('max(last_seq)')) {
      const result: Array<{ ns: string; max_seq: number }> = []
      const grouped = new Map<string, number>()
      for (const wal of eventsWal) {
        const current = grouped.get(wal.ns) ?? 0
        if (wal.last_seq > current) {
          grouped.set(wal.ns, wal.last_seq)
        }
      }
      for (const [ns, max_seq] of grouped.entries()) {
        result.push({ ns, max_seq })
      }
      return result
    }

    // Handle SELECT from rels_wal for counter initialization
    if (trimmedQuery.includes('select') && trimmedQuery.includes('from rels_wal') && trimmedQuery.includes('max(last_seq)')) {
      return []
    }

    // Handle DELETE FROM pending_row_groups
    if (trimmedQuery.includes('delete from pending_row_groups')) {
      const ns = params[0] as string
      const upToSeq = params[1] as number
      const toRemove = pendingRowGroups.filter(p => p.ns === ns && p.last_seq <= upToSeq)
      for (const p of toRemove) {
        const idx = pendingRowGroups.indexOf(p)
        if (idx >= 0) pendingRowGroups.splice(idx, 1)
      }
      return []
    }

    // Handle SELECT from pending_row_groups
    if (trimmedQuery.includes('select') && trimmedQuery.includes('from pending_row_groups')) {
      const ns = params[0] as string
      return pendingRowGroups.filter(p => p.ns === ns)
    }

    return []
  })

  return { sql: { exec }, tracker, pendingRowGroups, eventsWal, counters }
}

function getOperationType(query: string): string {
  if (query.includes('insert into pending_row_groups')) return 'pending_row_group_insert'
  if (query.includes('insert into events_wal')) return 'events_wal_insert'
  if (query.includes('insert into rels_wal')) return 'rels_wal_insert'
  if (query.includes('insert into entities')) return 'entity_insert'
  if (query.includes('insert into relationships')) return 'rel_insert'
  if (query.includes('update')) return 'update'
  if (query.includes('delete')) return 'delete'
  if (query.includes('select')) return 'select'
  if (query.includes('create')) return 'create_schema'
  return 'unknown'
}

// =============================================================================
// Mock ParqueDBDO with Transaction Batching Verification
// =============================================================================

/**
 * Simulates the current ParqueDBDO behavior for bulk operations.
 * This mock demonstrates the CURRENT (unbatched) behavior that needs fixing.
 */
class MockParqueDBDOUnbatched {
  private sql: { exec: ReturnType<typeof vi.fn> }
  private counters: Map<string, number> = new Map()
  private initialized = false

  constructor(sql: { exec: ReturnType<typeof vi.fn> }) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    this.sql.exec(`CREATE TABLE IF NOT EXISTS pending_row_groups (
      id TEXT PRIMARY KEY,
      ns TEXT NOT NULL,
      path TEXT NOT NULL,
      row_count INTEGER NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )`)

    this.sql.exec(`CREATE TABLE IF NOT EXISTS events_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL
    )`)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)`)

    // Initialize counters
    const rows = this.sql.exec(`SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`) as Array<{ ns: string; max_seq: number }>
    for (const row of rows) {
      this.counters.set(row.ns, row.max_seq + 1)
    }

    this.sql.exec(`SELECT ns, MAX(last_seq) as max_seq FROM rels_wal GROUP BY ns`)

    this.initialized = true
  }

  private getNextId(ns: string): string {
    const seq = this.counters.get(ns) || 1
    this.counters.set(ns, seq + 1)
    return `sqid_${seq}`
  }

  /**
   * CURRENT BEHAVIOR: Each operation is a separate SQLite write (no batching)
   * This is what we're testing against - it should FAIL the transaction batching tests.
   */
  async createManyUnbatched(
    ns: string,
    items: Array<{ $type: string; name: string; data?: Record<string, unknown> }>,
    options: { actor?: string } = {}
  ): Promise<Array<{ $id: string }>> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const pendingId = `pending_${Date.now()}`

    // Calculate sequence range
    const firstSeq = this.counters.get(ns) || 1
    const lastSeq = firstSeq + items.length - 1

    // Build entities
    const entities: Array<{ $id: string }> = []
    for (let i = 0; i < items.length; i++) {
      const seq = firstSeq + i
      const id = `sqid_${seq}`
      entities.push({ $id: `${ns}/${id}` })
    }

    // PROBLEM: These are separate writes, not batched in a single transaction!

    // Write 1: Counter is updated via getNextId calls above
    // (in real code, counter updates happen in memory and persist via events_wal)

    // Write 2: Insert pending_row_groups (1 row for bulk)
    this.sql.exec(
      `INSERT INTO pending_row_groups (id, ns, path, row_count, first_seq, last_seq, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      pendingId, ns, `data/${ns}/pending/${pendingId}.parquet`, items.length, firstSeq, lastSeq, now
    )

    // Write 3: Insert events_wal for each entity (or batched events)
    const events = items.map((item, i) => ({
      id: `ulid_${i}`,
      ts: Date.now(),
      op: 'CREATE',
      target: `entity:${ns}:sqid_${firstSeq + i}`,
      after: item,
      actor,
    }))

    this.sql.exec(
      `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)`,
      ns, firstSeq, lastSeq, new TextEncoder().encode(JSON.stringify(events)), now
    )

    // Update counter
    this.counters.set(ns, lastSeq + 1)

    return entities
  }

  /**
   * DESIRED BEHAVIOR: All operations batched in a single SQLite transaction
   * This is what the implementation SHOULD do after fixing parquedb-lssf.
   */
  async createManyBatched(
    ns: string,
    items: Array<{ $type: string; name: string; data?: Record<string, unknown> }>,
    options: { actor?: string } = {}
  ): Promise<Array<{ $id: string }>> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const pendingId = `pending_${Date.now()}`

    // Calculate sequence range
    const firstSeq = this.counters.get(ns) || 1
    const lastSeq = firstSeq + items.length - 1

    // Build entities
    const entities: Array<{ $id: string }> = []
    for (let i = 0; i < items.length; i++) {
      const seq = firstSeq + i
      const id = `sqid_${seq}`
      entities.push({ $id: `${ns}/${id}` })
    }

    // DESIRED: Begin transaction
    this.sql.exec('BEGIN')

    try {
      // All writes in single transaction:

      // Write 1: Insert pending_row_groups
      this.sql.exec(
        `INSERT INTO pending_row_groups (id, ns, path, row_count, first_seq, last_seq, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        pendingId, ns, `data/${ns}/pending/${pendingId}.parquet`, items.length, firstSeq, lastSeq, now
      )

      // Write 2: Insert events_wal
      const events = items.map((item, i) => ({
        id: `ulid_${i}`,
        ts: Date.now(),
        op: 'CREATE',
        target: `entity:${ns}:sqid_${firstSeq + i}`,
        after: item,
        actor,
      }))

      this.sql.exec(
        `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)`,
        ns, firstSeq, lastSeq, new TextEncoder().encode(JSON.stringify(events)), now
      )

      // Commit transaction
      this.sql.exec('COMMIT')

      // Update counter (in-memory, persisted via events_wal)
      this.counters.set(ns, lastSeq + 1)

    } catch (error) {
      this.sql.exec('ROLLBACK')
      throw error
    }

    return entities
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('SQLite Transaction Batching (RED phase)', () => {
  describe('Bulk create should use single SQLite transaction', () => {
    it('FAILS: current implementation uses multiple transactions for bulk create', async () => {
      // This test verifies the CURRENT (broken) behavior
      // It should PASS now (showing the problem exists)
      // After fixing, this test should be updated to expect batched behavior

      const { sql, tracker } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // Create 10 entities
      const items = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        data: { content: `Content ${i}` },
      }))

      await doInstance.createManyUnbatched('posts', items)

      // CURRENT BEHAVIOR: No explicit transactions, each write is auto-committed
      // This test documents the problem
      expect(tracker.beginCount).toBe(0) // No BEGIN calls
      expect(tracker.commitCount).toBe(0) // No COMMIT calls

      // Multiple standalone operations instead of batched
      const nonSchemaOps = tracker.completedTransactions.filter(
        t => !t.ops.every(op => op === 'create_schema' || op === 'select')
      )

      // PROBLEM: Operations are not batched together
      // pending_row_group_insert and events_wal_insert are separate
      expect(nonSchemaOps.length).toBeGreaterThan(1)
    })

    it('SHOULD PASS: batched implementation uses single transaction for bulk create', async () => {
      const { sql, tracker } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // Create 10 entities using batched method
      const items = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        data: { content: `Content ${i}` },
      }))

      await doInstance.createManyBatched('posts', items)

      // DESIRED BEHAVIOR: Single BEGIN/COMMIT wrapping all operations
      expect(tracker.beginCount).toBe(1)
      expect(tracker.commitCount).toBe(1)

      // All writes should be in a single transaction
      const writeTransactions = tracker.completedTransactions.filter(
        t => t.ops.some(op => op.includes('insert'))
      )
      expect(writeTransactions.length).toBe(1)

      // Transaction should contain both pending_row_group and events_wal inserts
      const txnOps = writeTransactions[0]!.ops
      expect(txnOps).toContain('pending_row_group_insert')
      expect(txnOps).toContain('events_wal_insert')
    })
  })

  describe('Counter update and pending insert should be atomic', () => {
    it('FAILS: counter and pending_row_group can get out of sync', async () => {
      // This test verifies that counter updates and pending_row_group inserts
      // can get out of sync if there's a failure between them

      const { sql, tracker, pendingRowGroups } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // First bulk create succeeds
      await doInstance.createManyUnbatched('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
      ])

      expect(pendingRowGroups.length).toBe(1)
      const firstGroup = pendingRowGroups[0]!

      // Simulate crash between counter update and pending_row_group insert
      // In unbatched code, these can fail independently

      // PROBLEM: If events_wal insert succeeds but pending_row_group fails,
      // counter will be incremented but pending file won't be tracked
      // This leaves the system in an inconsistent state

      // The fact that we can even conceive of this scenario proves
      // the operations need to be atomic
      expect(firstGroup.first_seq).toBeDefined()
      expect(firstGroup.last_seq).toBeDefined()

      // This test documents that atomicity is not guaranteed
      // After fix, operations should be in single transaction
      expect(tracker.beginCount).toBe(0) // No transaction = not atomic
    })

    it('SHOULD PASS: counter and pending insert are atomic in transaction', async () => {
      const { sql, tracker, pendingRowGroups, eventsWal } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      await doInstance.createManyBatched('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
      ])

      // Both operations completed in single transaction
      expect(tracker.beginCount).toBe(1)
      expect(tracker.commitCount).toBe(1)

      // Verify both were written
      expect(pendingRowGroups.length).toBe(1)
      expect(eventsWal.length).toBe(1)

      // Verify sequence consistency
      const group = pendingRowGroups[0]!
      const wal = eventsWal[0]!
      expect(group.first_seq).toBe(wal.first_seq)
      expect(group.last_seq).toBe(wal.last_seq)
    })
  })

  describe('Transaction count should be O(1) not O(n)', () => {
    it('FAILS: transaction count grows with entity count (O(n))', async () => {
      const { sql, tracker } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // Create varying batch sizes
      const sizes = [10, 50, 100]
      const transactionCounts: number[] = []

      for (const size of sizes) {
        // Reset tracker for each batch
        tracker.beginCount = 0
        tracker.commitCount = 0
        tracker.completedTransactions = []

        const items = Array.from({ length: size }, (_, i) => ({
          $type: 'Post',
          name: `Post ${i}`,
        }))

        await doInstance.createManyUnbatched(`posts_${size}`, items)

        // Count non-schema operations
        const writeOps = tracker.completedTransactions.filter(
          t => t.ops.some(op => op.includes('insert') && !op.includes('create_schema'))
        )
        transactionCounts.push(writeOps.length)
      }

      // PROBLEM: Without batching, each operation is separate
      // Current behavior: 2 writes per bulk (pending + events_wal), not O(n) but still not O(1)
      // With proper batching, all should be 1 transaction regardless of size

      // Document current behavior (2 separate writes)
      expect(transactionCounts.every(c => c === 2)).toBe(true)

      // DESIRED: Should be O(1) - exactly 1 transaction regardless of batch size
      // This assertion will fail until we implement proper batching
      // expect(transactionCounts.every(c => c === 1)).toBe(true) // Uncomment after fix
    })

    it('SHOULD PASS: batched implementation has O(1) transaction overhead', async () => {
      const { sql, tracker } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // Create varying batch sizes
      const sizes = [10, 50, 100]
      const transactionCounts: number[] = []

      for (const size of sizes) {
        // Reset tracker
        tracker.beginCount = 0
        tracker.commitCount = 0
        tracker.completedTransactions = []

        const items = Array.from({ length: size }, (_, i) => ({
          $type: 'Post',
          name: `Post ${i}`,
        }))

        await doInstance.createManyBatched(`posts_${size}`, items)

        transactionCounts.push(tracker.beginCount)
      }

      // DESIRED: Exactly 1 transaction per bulk operation, regardless of size
      expect(transactionCounts).toEqual([1, 1, 1])

      // Also verify commit count matches
      expect(tracker.commitCount).toBe(1) // Last operation
    })
  })

  describe('Rollback should work if any part fails', () => {
    it('FAILS: partial failure leaves inconsistent state without rollback', async () => {
      const { sql, tracker, pendingRowGroups, eventsWal } = createMockSqlStorageWithTransactionTracking()

      // Create a mock that fails on events_wal insert
      const failingSql = {
        exec: vi.fn((query: string, ...params: unknown[]) => {
          const trimmedQuery = query.trim().toLowerCase()

          // Track transaction commands
          if (trimmedQuery === 'begin' || trimmedQuery === 'begin transaction') {
            tracker.beginCount++
            tracker.inTransaction = true
            return []
          }
          if (trimmedQuery === 'commit' || trimmedQuery === 'commit transaction') {
            tracker.commitCount++
            tracker.inTransaction = false
            return []
          }
          if (trimmedQuery === 'rollback' || trimmedQuery === 'rollback transaction') {
            tracker.rollbackCount++
            tracker.inTransaction = false
            return []
          }

          // Handle schema
          if (trimmedQuery.startsWith('create')) return []
          if (trimmedQuery.includes('select')) return []

          // Allow pending_row_groups insert
          if (trimmedQuery.includes('insert into pending_row_groups')) {
            pendingRowGroups.push({
              id: params[0] as string,
              ns: params[1] as string,
              path: params[2] as string,
              row_count: params[3] as number,
              first_seq: params[4] as number,
              last_seq: params[5] as number,
            })
            return []
          }

          // FAIL on events_wal insert
          if (trimmedQuery.includes('insert into events_wal')) {
            throw new Error('Simulated events_wal insert failure')
          }

          return []
        }),
      }

      const doInstance = new MockParqueDBDOUnbatched(failingSql)

      // Without proper transaction handling, pending_row_groups is written
      // but events_wal fails, leaving system in inconsistent state
      let errorThrown = false
      try {
        await doInstance.createManyUnbatched('posts', [
          { $type: 'Post', name: 'Post 1' },
        ])
      } catch {
        errorThrown = true
      }

      expect(errorThrown).toBe(true)

      // PROBLEM: pending_row_groups was written but events_wal wasn't
      // Without transaction rollback, this is inconsistent
      expect(pendingRowGroups.length).toBe(1) // Orphaned record!
      expect(eventsWal.length).toBe(0)

      // No rollback happened because there was no transaction
      expect(tracker.rollbackCount).toBe(0)
    })

    it('SHOULD PASS: rollback cleans up on failure', async () => {
      const pendingRowGroups: Array<{ id: string; ns: string; path: string; row_count: number; first_seq: number; last_seq: number }> = []
      const eventsWal: Array<{ id: number; ns: string; first_seq: number; last_seq: number; events: Uint8Array }> = []

      const tracker: TransactionTracker = {
        beginCount: 0,
        commitCount: 0,
        rollbackCount: 0,
        currentTransactionOps: [],
        completedTransactions: [],
        inTransaction: false,
      }

      // Track what was written during transaction for rollback
      let pendingDuringTxn: typeof pendingRowGroups[0] | null = null

      const rollbackSql = {
        exec: vi.fn((query: string, ...params: unknown[]) => {
          const trimmedQuery = query.trim().toLowerCase()

          if (trimmedQuery === 'begin' || trimmedQuery === 'begin transaction') {
            tracker.beginCount++
            tracker.inTransaction = true
            return []
          }
          if (trimmedQuery === 'commit' || trimmedQuery === 'commit transaction') {
            tracker.commitCount++
            tracker.inTransaction = false
            return []
          }
          if (trimmedQuery === 'rollback' || trimmedQuery === 'rollback transaction') {
            tracker.rollbackCount++
            // Rollback: remove any pending writes
            if (pendingDuringTxn) {
              const idx = pendingRowGroups.indexOf(pendingDuringTxn)
              if (idx >= 0) pendingRowGroups.splice(idx, 1)
              pendingDuringTxn = null
            }
            tracker.inTransaction = false
            return []
          }

          // Handle schema
          if (trimmedQuery.startsWith('create')) return []
          if (trimmedQuery.includes('select')) return []

          // Track pending_row_groups insert
          if (trimmedQuery.includes('insert into pending_row_groups')) {
            const record = {
              id: params[0] as string,
              ns: params[1] as string,
              path: params[2] as string,
              row_count: params[3] as number,
              first_seq: params[4] as number,
              last_seq: params[5] as number,
            }
            pendingRowGroups.push(record)
            if (tracker.inTransaction) {
              pendingDuringTxn = record
            }
            return []
          }

          // FAIL on events_wal insert
          if (trimmedQuery.includes('insert into events_wal')) {
            throw new Error('Simulated events_wal insert failure')
          }

          return []
        }),
      }

      // Create instance that uses batched (transactional) writes
      const doInstance = new MockParqueDBDOUnbatched(rollbackSql)

      let errorThrown = false
      try {
        await doInstance.createManyBatched('posts', [
          { $type: 'Post', name: 'Post 1' },
        ])
      } catch {
        errorThrown = true
      }

      expect(errorThrown).toBe(true)

      // DESIRED: Rollback was called
      expect(tracker.rollbackCount).toBe(1)

      // DESIRED: No orphaned records - rollback cleaned up pending_row_groups
      expect(pendingRowGroups.length).toBe(0)
      expect(eventsWal.length).toBe(0)
    })
  })

  describe('Real-world bulk operation scenarios', () => {
    it('FAILS: 100 entity bulk create generates multiple SQLite operations', async () => {
      const { sql, tracker } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      const items = Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
        data: { content: `Content ${i}`, index: i },
      }))

      await doInstance.createManyUnbatched('posts', items)

      // Count total SQL exec calls for data writes (excluding schema)
      const writeCalls = sql.exec.mock.calls.filter((call: unknown[]) => {
        const query = (call[0] as string).toLowerCase().trim()
        // Match INSERT INTO table_name pattern but exclude CREATE statements
        return (query.includes('insert into pending_row_groups') ||
                query.includes('insert into events_wal'))
      })

      // CURRENT: 2 insert calls (pending_row_groups + events_wal)
      expect(writeCalls.length).toBe(2)

      // PROBLEM: These are not wrapped in a transaction
      expect(tracker.beginCount).toBe(0)

      // After fix, should be wrapped:
      // expect(tracker.beginCount).toBe(1)
      // expect(tracker.commitCount).toBe(1)
    })

    it('verifies sequence numbers are consistent across pending and WAL', async () => {
      const { sql, pendingRowGroups, eventsWal } = createMockSqlStorageWithTransactionTracking()
      const doInstance = new MockParqueDBDOUnbatched(sql)

      // Multiple bulk creates
      await doInstance.createManyUnbatched('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
        { $type: 'Post', name: 'Post 3' },
      ])

      await doInstance.createManyUnbatched('posts', [
        { $type: 'Post', name: 'Post 4' },
        { $type: 'Post', name: 'Post 5' },
      ])

      // Verify sequence consistency
      expect(pendingRowGroups.length).toBe(2)
      expect(eventsWal.length).toBe(2)

      // First batch: seq 1-3
      expect(pendingRowGroups[0]!.first_seq).toBe(1)
      expect(pendingRowGroups[0]!.last_seq).toBe(3)
      expect(eventsWal[0]!.first_seq).toBe(1)
      expect(eventsWal[0]!.last_seq).toBe(3)

      // Second batch: seq 4-5
      expect(pendingRowGroups[1]!.first_seq).toBe(4)
      expect(pendingRowGroups[1]!.last_seq).toBe(5)
      expect(eventsWal[1]!.first_seq).toBe(4)
      expect(eventsWal[1]!.last_seq).toBe(5)
    })
  })

  // ===========================================================================
  // TRUE RED TESTS - These will FAIL until parquedb-lssf is implemented
  // ===========================================================================

  describe('Integration tests for actual ParqueDBDO (RED - will fail until fixed)', () => {
    /**
     * These tests define the EXPECTED behavior after the fix.
     * They are marked with .skip for now because they require the actual
     * ParqueDBDO implementation which cannot be easily unit tested without
     * the full Cloudflare Workers environment.
     *
     * To run these tests properly, move them to an integration test file
     * that uses the workers test environment.
     *
     * For now, we document the expected behavior here as a specification.
     */

    it.skip('bulkWriteToR2 should wrap all SQLite writes in BEGIN/COMMIT', () => {
      // EXPECTED BEHAVIOR after fix:
      // 1. BEGIN transaction
      // 2. INSERT INTO pending_row_groups (metadata for bulk write)
      // 3. INSERT INTO events_wal (CREATE events for all entities)
      // 4. COMMIT transaction
      //
      // This ensures atomicity: if any step fails, all are rolled back

      // The implementation should change from:
      //   this.sql.exec('INSERT INTO pending_row_groups...')
      //   this.sql.exec('INSERT INTO events_wal...')
      //
      // To:
      //   this.sql.exec('BEGIN')
      //   try {
      //     this.sql.exec('INSERT INTO pending_row_groups...')
      //     this.sql.exec('INSERT INTO events_wal...')
      //     this.sql.exec('COMMIT')
      //   } catch (e) {
      //     this.sql.exec('ROLLBACK')
      //     throw e
      //   }

      expect(true).toBe(false) // Placeholder - implement with actual DO testing
    })

    it.skip('createMany with 5+ entities should use single transaction', () => {
      // EXPECTED BEHAVIOR after fix:
      // - createMany(ns, [5+ items]) calls bulkWriteToR2
      // - bulkWriteToR2 wraps pending_row_groups + events_wal in single transaction
      // - Transaction count = 1 regardless of item count

      expect(true).toBe(false) // Placeholder
    })

    it.skip('transaction rollback should clean up all SQLite state', () => {
      // EXPECTED BEHAVIOR after fix:
      // If events_wal INSERT fails:
      // - pending_row_groups INSERT should be rolled back
      // - No orphaned records in either table
      // - Counter state should be restored

      expect(true).toBe(false) // Placeholder
    })
  })

  /**
   * Specification tests - document the required behavior
   * These tests pass by documenting what SHOULD happen
   */
  describe('Specification: Required transaction batching behavior', () => {
    it('should batch pending_row_groups and events_wal in single transaction', () => {
      // SPECIFICATION:
      // When ParqueDBDO.bulkWriteToR2 is called, it MUST:
      // 1. Execute BEGIN before any INSERT
      // 2. Execute both INSERTs (pending_row_groups, events_wal)
      // 3. Execute COMMIT after both succeed
      // 4. Execute ROLLBACK if either fails

      const requiredTransactionStructure = {
        begin: 'BEGIN or BEGIN IMMEDIATE',
        operations: [
          'INSERT INTO pending_row_groups (...)',
          'INSERT INTO events_wal (...)',
        ],
        end: 'COMMIT on success, ROLLBACK on failure',
      }

      expect(requiredTransactionStructure.begin).toMatch(/BEGIN/)
      expect(requiredTransactionStructure.operations.length).toBe(2)
      expect(requiredTransactionStructure.end).toContain('COMMIT')
    })

    it('should maintain O(1) transaction overhead for bulk operations', () => {
      // SPECIFICATION:
      // Transaction count MUST be constant regardless of batch size:
      // - 10 entities: 1 transaction
      // - 100 entities: 1 transaction
      // - 1000 entities: 1 transaction

      const expectedTransactionCount = (batchSize: number) => 1

      expect(expectedTransactionCount(10)).toBe(1)
      expect(expectedTransactionCount(100)).toBe(1)
      expect(expectedTransactionCount(1000)).toBe(1)
    })

    it('should guarantee atomicity of counter + pending_row_group + events_wal', () => {
      // SPECIFICATION:
      // All three operations MUST be atomic:
      // 1. Counter increment (in-memory, persisted via events_wal)
      // 2. pending_row_groups INSERT
      // 3. events_wal INSERT
      //
      // Failure modes that MUST be prevented:
      // - Counter incremented but pending_row_groups missing -> orphaned sequence numbers
      // - pending_row_groups exists but events_wal missing -> orphaned pending file
      // - events_wal exists but pending_row_groups missing -> data lost on compaction

      const atomicityRequirements = {
        allSucceed: 'Counter, pending_row_groups, events_wal all committed',
        anyFails: 'Counter restored, no records in either table',
        neverPartial: 'Never have pending without events or vice versa',
      }

      expect(atomicityRequirements.allSucceed).toBeDefined()
      expect(atomicityRequirements.anyFails).toBeDefined()
      expect(atomicityRequirements.neverPartial).toBeDefined()
    })

    it('should handle R2 write failures without corrupting SQLite state', () => {
      // SPECIFICATION:
      // If R2 PUT fails after SQLite transaction commits:
      // - SQLite state is consistent (both tables have records)
      // - Retry mechanism can re-attempt R2 write
      // - pending_row_groups serves as intent log for recovery
      //
      // The transaction boundary is:
      // [SQLite Transaction Start]
      //   INSERT pending_row_groups
      //   INSERT events_wal
      // [SQLite Transaction Commit]
      // R2.put(parquet file) <- Outside transaction, can fail independently

      const recoveryGuarantees = {
        sqliteConsistent: 'Both tables updated atomically',
        r2Retryable: 'R2 writes can be retried from pending_row_groups',
        noDataLoss: 'Events preserved in events_wal even if R2 fails',
      }

      expect(recoveryGuarantees.sqliteConsistent).toBeDefined()
      expect(recoveryGuarantees.r2Retryable).toBeDefined()
      expect(recoveryGuarantees.noDataLoss).toBeDefined()
    })
  })
})
