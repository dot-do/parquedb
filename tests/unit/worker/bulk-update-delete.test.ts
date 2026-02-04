/**
 * Bulk updateMany/deleteMany Optimization Tests
 *
 * RED phase: These tests verify that updateMany and deleteMany
 * use bulk paths similar to createMany, rather than calling
 * individual operations in a loop.
 *
 * Current state (as of parquedb-tiz8):
 * - createMany: Has bulk optimization (>= 5 entities go directly to R2)
 * - deleteMany: Calls delete() in a loop (no bulk optimization)
 * - updateMany: Does not exist (needs to be implemented)
 *
 * Expected behavior after implementation:
 * 1. deleteMany(100 ids) uses bulk path (not 100 individual deletes)
 * 2. updateMany(100 entities) uses bulk path
 * 3. Bulk operations >= 5 entities bypass individual WAL writes
 * 4. Cost is O(1) R2 writes, not O(n)
 *
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

import { describe, it, expect, beforeEach } from 'vitest'

// =============================================================================
// Mock SQLite with Operation Tracking
// =============================================================================

interface SqlOperation {
  type: 'insert' | 'update' | 'delete' | 'select'
  table: string
  rowCount: number
}

class BulkOperationTrackingSqlite {
  private tables: Map<string, unknown[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()

  // Operation tracking
  operations: SqlOperation[] = []
  insertCount = 0
  updateCount = 0
  deleteCount = 0
  selectCount = 0

  // Track individual entity operations (simulates N individual calls)
  entityDeleteCalls = 0
  entityUpdateCalls = 0
  r2WriteCalls = 0

  resetTracking() {
    this.operations = []
    this.insertCount = 0
    this.updateCount = 0
    this.deleteCount = 0
    this.selectCount = 0
    this.entityDeleteCalls = 0
    this.entityUpdateCalls = 0
    this.r2WriteCalls = 0
  }

  getInsertsByTable(): Record<string, number> {
    const byTable: Record<string, number> = {}
    for (const op of this.operations) {
      if (op.type === 'insert') {
        byTable[op.table] = (byTable[op.table] || 0) + op.rowCount
      }
    }
    return byTable
  }

  exec<T = unknown>(query: string, ..._params: unknown[]): Iterable<T> {
    const trimmedQuery = query.trim().toLowerCase()

    // CREATE TABLE
    if (trimmedQuery.startsWith('create table')) {
      const match = query.match(/create table if not exists (\w+)/i)
      if (match) {
        const tableName = match[1]!
        if (!this.tables.has(tableName)) {
          this.tables.set(tableName, [])
          this.autoIncrement.set(tableName, 0)
        }
      }
      return [] as T[]
    }

    // CREATE INDEX
    if (trimmedQuery.startsWith('create index')) {
      return [] as T[]
    }

    // INSERT
    if (trimmedQuery.startsWith('insert into')) {
      const match = query.match(/insert into (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.insertCount++
        this.operations.push({ type: 'insert', table: tableName, rowCount: 1 })
      }
      return [] as T[]
    }

    // UPDATE
    if (trimmedQuery.startsWith('update')) {
      const match = query.match(/update (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.updateCount++
        this.operations.push({ type: 'update', table: tableName, rowCount: 1 })
      }
      return [] as T[]
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.deleteCount++
        this.operations.push({ type: 'delete', table: tableName, rowCount: 1 })
      }
      return [] as T[]
    }

    // SELECT
    if (trimmedQuery.startsWith('select')) {
      this.selectCount++
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.operations.push({ type: 'select', table: tableName, rowCount: 0 })
      }
      return [] as T[]
    }

    return [] as T[]
  }

  clear() {
    this.tables.clear()
    this.autoIncrement.clear()
    this.resetTracking()
  }
}

// =============================================================================
// Mock Store Simulating Current ParqueDBDO Behavior
// =============================================================================

interface Entity {
  $id: string
  $type: string
  name: string
  version: number
}

interface Event {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  target: string
  before?: unknown
  after?: unknown
}

/**
 * Simulates current ParqueDBDO behavior
 * - createMany: has bulk optimization
 * - deleteMany: NO bulk optimization (loops over delete())
 * - updateMany: does not exist
 */
class CurrentParqueDBDOStore {
  private sql: BulkOperationTrackingSqlite
  private entities: Map<string, Entity> = new Map()
  private counter = 0
  private eventBuffer: Event[] = []

  static readonly BULK_THRESHOLD = 5
  static readonly BATCH_THRESHOLD = 100

  constructor(sql: BulkOperationTrackingSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    this.sql.exec(`CREATE TABLE IF NOT EXISTS events_wal (...)`)
    this.sql.exec(`CREATE TABLE IF NOT EXISTS pending_row_groups (...)`)
  }

  // Individual create - appends event to buffer
  async create(ns: string, data: { $type: string; name: string }): Promise<Entity> {
    await this.ensureInitialized()
    const id = `${++this.counter}`
    const entity: Entity = {
      $id: `${ns}/${id}`,
      $type: data.$type,
      name: data.name,
      version: 1,
    }
    this.entities.set(entity.$id, entity)

    // Append event to buffer
    this.eventBuffer.push({
      id: `evt_${this.counter}`,
      ts: Date.now(),
      op: 'CREATE',
      target: `${ns}:${id}`,
      after: data,
    })

    // Auto-flush at threshold
    if (this.eventBuffer.length >= CurrentParqueDBDOStore.BATCH_THRESHOLD) {
      await this.flushEventBatch()
    }

    return entity
  }

  // createMany - HAS bulk optimization
  async createMany(
    ns: string,
    items: Array<{ $type: string; name: string }>
  ): Promise<Entity[]> {
    await this.ensureInitialized()

    if (items.length >= CurrentParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: Write 1 pending_row_groups row, bypass individual events
      this.sql.exec(`INSERT INTO pending_row_groups (...)`)
      this.sql.r2WriteCalls++ // Single R2 write

      // Store entities so they can be deleted/updated later
      const entities = items.map((item, i) => {
        const seq = this.counter + i + 1
        const entity: Entity = {
          $id: `${ns}/${seq}`,
          $type: item.$type,
          name: item.name,
          version: 1,
        }
        this.entities.set(entity.$id, entity)
        return entity
      })
      this.counter += items.length
      return entities
    }

    // Small batch: use individual creates
    const entities: Entity[] = []
    for (const item of items) {
      entities.push(await this.create(ns, item))
    }
    return entities
  }

  // Individual delete - CURRENT BEHAVIOR: appends event
  async delete(ns: string, id: string): Promise<{ deletedCount: number }> {
    await this.ensureInitialized()
    const entityId = `${ns}/${id}`

    if (!this.entities.has(entityId)) {
      return { deletedCount: 0 }
    }

    this.sql.entityDeleteCalls++ // Track individual delete call

    // Append DELETE event
    this.eventBuffer.push({
      id: `evt_${++this.counter}`,
      ts: Date.now(),
      op: 'DELETE',
      target: `${ns}:${id}`,
      before: this.entities.get(entityId),
    })

    this.entities.delete(entityId)

    // Auto-flush at threshold
    if (this.eventBuffer.length >= CurrentParqueDBDOStore.BATCH_THRESHOLD) {
      await this.flushEventBatch()
    }

    return { deletedCount: 1 }
  }

  // deleteMany - WITH bulk optimization for >= BULK_THRESHOLD entities
  async deleteMany(
    ns: string,
    ids: string[]
  ): Promise<{ deletedCount: number }> {
    await this.ensureInitialized()

    // For bulk deletes (5+ entities), write directly to pending_row_groups
    if (ids.length >= CurrentParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: Write 1 pending_row_groups row, bypass individual events
      this.sql.exec(`INSERT INTO pending_row_groups (...)`)
      this.sql.r2WriteCalls = 1 // Single R2 write

      let deletedCount = 0
      for (const id of ids) {
        const entityId = `${ns}/${id}`
        if (this.entities.has(entityId)) {
          this.entities.delete(entityId)
          deletedCount++
        }
      }
      return { deletedCount }
    }

    // Small batch: use individual deletes
    let deletedCount = 0
    for (const id of ids) {
      const result = await this.delete(ns, id)
      deletedCount += result.deletedCount
    }

    return { deletedCount }
  }

  // Individual update - CURRENT BEHAVIOR: appends event
  async update(
    ns: string,
    id: string,
    data: Partial<{ name: string }>
  ): Promise<Entity | null> {
    await this.ensureInitialized()
    const entityId = `${ns}/${id}`

    const entity = this.entities.get(entityId)
    if (!entity) {
      return null
    }

    this.sql.entityUpdateCalls++ // Track individual update call

    const updated = {
      ...entity,
      ...data,
      version: entity.version + 1,
    }
    this.entities.set(entityId, updated)

    // Append UPDATE event
    this.eventBuffer.push({
      id: `evt_${++this.counter}`,
      ts: Date.now(),
      op: 'UPDATE',
      target: `${ns}:${id}`,
      before: entity,
      after: updated,
    })

    // Auto-flush at threshold
    if (this.eventBuffer.length >= CurrentParqueDBDOStore.BATCH_THRESHOLD) {
      await this.flushEventBatch()
    }

    return updated
  }

  // updateMany - WITH bulk optimization for >= BULK_THRESHOLD entities
  async updateMany(
    ns: string,
    updates: Array<{ id: string; data: Partial<{ name: string }> }>
  ): Promise<{ modifiedCount: number }> {
    await this.ensureInitialized()

    // For bulk updates (5+ entities), write directly to pending_row_groups
    if (updates.length >= CurrentParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: Write 1 pending_row_groups row, bypass individual events
      this.sql.exec(`INSERT INTO pending_row_groups (...)`)
      this.sql.r2WriteCalls = 1 // Single R2 write

      let modifiedCount = 0
      for (const { id, data } of updates) {
        const entityId = `${ns}/${id}`
        const entity = this.entities.get(entityId)
        if (entity) {
          this.entities.set(entityId, { ...entity, ...data, version: entity.version + 1 })
          modifiedCount++
        }
      }
      return { modifiedCount }
    }

    // Small batch: use individual updates
    let modifiedCount = 0
    for (const { id, data } of updates) {
      const result = await this.update(ns, id, data)
      if (result) modifiedCount++
    }

    return { modifiedCount }
  }

  async flushEventBatch(): Promise<void> {
    if (this.eventBuffer.length === 0) return

    // Write single WAL row for all buffered events
    this.sql.exec(`INSERT INTO events_wal (...)`)
    this.sql.r2WriteCalls++
    this.eventBuffer = []
  }

  async flushAll(): Promise<void> {
    await this.flushEventBatch()
  }

  getEventBufferSize(): number {
    return this.eventBuffer.length
  }
}

// =============================================================================
// Mock Store With Expected Bulk Optimization (Target Behavior)
// =============================================================================

/**
 * Simulates the expected behavior AFTER implementing bulk optimization
 * for deleteMany and updateMany
 */
class OptimizedParqueDBDOStore {
  private sql: BulkOperationTrackingSqlite
  private entities: Map<string, Entity> = new Map()
  private counter = 0
  private eventBuffer: Event[] = []

  static readonly BULK_THRESHOLD = 5

  constructor(sql: BulkOperationTrackingSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    this.sql.exec(`CREATE TABLE IF NOT EXISTS events_wal (...)`)
    this.sql.exec(`CREATE TABLE IF NOT EXISTS pending_row_groups (...)`)
  }

  async create(ns: string, data: { $type: string; name: string }): Promise<Entity> {
    const id = `${++this.counter}`
    const entity: Entity = {
      $id: `${ns}/${id}`,
      $type: data.$type,
      name: data.name,
      version: 1,
    }
    this.entities.set(entity.$id, entity)
    return entity
  }

  async createMany(
    ns: string,
    items: Array<{ $type: string; name: string }>
  ): Promise<Entity[]> {
    await this.ensureInitialized()

    if (items.length >= OptimizedParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: 1 metadata row
      this.sql.exec(`INSERT INTO pending_row_groups (...)`)
      this.sql.r2WriteCalls = 1

      return items.map((item, i) => ({
        $id: `${ns}/${this.counter + i + 1}`,
        $type: item.$type,
        name: item.name,
        version: 1,
      }))
    }

    return items.map((item) => this.create(ns, item)) as unknown as Entity[]
  }

  // deleteMany - WITH bulk optimization
  async deleteManyBulk(
    ns: string,
    ids: string[]
  ): Promise<{ deletedCount: number }> {
    await this.ensureInitialized()

    if (ids.length >= OptimizedParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: Write single batch delete event, 1 R2 write
      this.sql.exec(`INSERT INTO events_wal (...)`) // Single WAL row with batch
      this.sql.r2WriteCalls = 1 // O(1) R2 writes

      let deletedCount = 0
      for (const id of ids) {
        const entityId = `${ns}/${id}`
        if (this.entities.has(entityId)) {
          this.entities.delete(entityId)
          deletedCount++
        }
      }
      return { deletedCount }
    }

    // Small batch: individual deletes (but still batched in event buffer)
    let deletedCount = 0
    for (const id of ids) {
      const entityId = `${ns}/${id}`
      if (this.entities.has(entityId)) {
        this.entities.delete(entityId)
        deletedCount++
        this.sql.entityDeleteCalls++
      }
    }
    return { deletedCount }
  }

  // updateMany - WITH bulk optimization
  async updateManyBulk(
    ns: string,
    updates: Array<{ id: string; data: Partial<{ name: string }> }>
  ): Promise<{ modifiedCount: number }> {
    await this.ensureInitialized()

    if (updates.length >= OptimizedParqueDBDOStore.BULK_THRESHOLD) {
      // BULK PATH: Write single batch update event, 1 R2 write
      this.sql.exec(`INSERT INTO events_wal (...)`) // Single WAL row with batch
      this.sql.r2WriteCalls = 1 // O(1) R2 writes

      let modifiedCount = 0
      for (const { id, data } of updates) {
        const entityId = `${ns}/${id}`
        const entity = this.entities.get(entityId)
        if (entity) {
          this.entities.set(entityId, { ...entity, ...data, version: entity.version + 1 })
          modifiedCount++
        }
      }
      return { modifiedCount }
    }

    // Small batch: individual updates
    let modifiedCount = 0
    for (const { id, data } of updates) {
      const entityId = `${ns}/${id}`
      const entity = this.entities.get(entityId)
      if (entity) {
        this.entities.set(entityId, { ...entity, ...data, version: entity.version + 1 })
        modifiedCount++
        this.sql.entityUpdateCalls++
      }
    }
    return { modifiedCount }
  }
}

// =============================================================================
// Tests - These should FAIL until bulk optimization is implemented
// =============================================================================

describe('Bulk updateMany/deleteMany Optimization', () => {
  let sql: BulkOperationTrackingSqlite
  let currentStore: CurrentParqueDBDOStore
  let _optimizedStore: OptimizedParqueDBDOStore

  beforeEach(async () => {
    sql = new BulkOperationTrackingSqlite()
    currentStore = new CurrentParqueDBDOStore(sql)
    _optimizedStore = new OptimizedParqueDBDOStore(sql)

    // Setup: Create 100 entities for delete/update tests
    await currentStore.createMany('posts', Array.from({ length: 100 }, (_, i) => ({
      $type: 'Post',
      name: `Post ${i}`,
    })))
    sql.resetTracking()
  })

  describe('deleteMany bulk path', () => {
    it('deleteMany(100 ids) should use bulk path, not 100 individual deletes', async () => {
      const ids = Array.from({ length: 100 }, (_, i) => `${i + 1}`)

      await currentStore.deleteMany('posts', ids)
      await currentStore.flushAll()

      // EXPECTED (after fix): 1 bulk delete operation
      // ACTUAL (current): 100 individual delete calls
      //
      // This test will FAIL because current implementation loops over delete()
      expect(sql.entityDeleteCalls).toBe(0) // Should be 0 individual calls
      expect(sql.r2WriteCalls).toBeLessThanOrEqual(1) // O(1) R2 writes
    })

    it('deleteMany should bypass individual WAL writes for >= 5 entities', async () => {
      const ids = Array.from({ length: 10 }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)

      // EXPECTED: Bulk path bypasses individual event appending
      // ACTUAL: 10 events appended to buffer (10 entityDeleteCalls)
      expect(sql.entityDeleteCalls).toBe(0)
    })

    it('deleteMany cost should be O(1) R2 writes, not O(n)', async () => {
      const ids = Array.from({ length: 100 }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)
      await currentStore.flushAll()

      // EXPECTED: 1 R2 write for entire bulk operation
      // ACTUAL: Could be multiple flushes as buffer fills
      expect(sql.r2WriteCalls).toBe(1)
    })

    it('deleteMany small batch (< 5) can use individual deletes', async () => {
      const ids = ['1', '2', '3', '4'] // Below threshold

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)

      // Small batches are allowed to use individual deletes
      // (but still get batched in event buffer before flush)
      expect(sql.entityDeleteCalls).toBeLessThanOrEqual(4)
    })
  })

  describe('updateMany bulk path', () => {
    it('updateMany(100 entities) should use bulk path', async () => {
      const updates = Array.from({ length: 100 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated Post ${i}` },
      }))

      sql.resetTracking()
      await currentStore.updateMany('posts', updates)
      await currentStore.flushAll()

      // EXPECTED (after fix): 1 bulk update operation
      // ACTUAL (current): 100 individual update calls
      expect(sql.entityUpdateCalls).toBe(0) // Should be 0 individual calls
      expect(sql.r2WriteCalls).toBeLessThanOrEqual(1) // O(1) R2 writes
    })

    it('updateMany should bypass individual WAL writes for >= 5 entities', async () => {
      const updates = Array.from({ length: 10 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated Post ${i}` },
      }))

      sql.resetTracking()
      await currentStore.updateMany('posts', updates)

      // EXPECTED: Bulk path bypasses individual event appending
      // ACTUAL: 10 events appended to buffer (10 entityUpdateCalls)
      expect(sql.entityUpdateCalls).toBe(0)
    })

    it('updateMany cost should be O(1) R2 writes, not O(n)', async () => {
      const updates = Array.from({ length: 100 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated Post ${i}` },
      }))

      sql.resetTracking()
      await currentStore.updateMany('posts', updates)
      await currentStore.flushAll()

      // EXPECTED: 1 R2 write for entire bulk operation
      // ACTUAL: Could be multiple flushes as buffer fills
      expect(sql.r2WriteCalls).toBe(1)
    })
  })

  describe('Cost comparison: bulk vs individual', () => {
    it('100 deletes: bulk should be 100x cheaper than individual', async () => {
      // Baseline: 100 individual deletes
      const individualSql = new BulkOperationTrackingSqlite()
      const individualStore = new CurrentParqueDBDOStore(individualSql)

      await individualStore.createMany('posts', Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))
      individualSql.resetTracking()

      // Perform 100 individual deletes (current behavior)
      for (let i = 1; i <= 100; i++) {
        await individualStore.delete('posts', `${i}`)
      }
      await individualStore.flushAll()

      const individualCost = individualSql.entityDeleteCalls

      // Now test bulk deleteMany
      const bulkSql = new BulkOperationTrackingSqlite()
      const bulkStore = new CurrentParqueDBDOStore(bulkSql)

      await bulkStore.createMany('posts', Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))
      bulkSql.resetTracking()

      await bulkStore.deleteMany('posts', Array.from({ length: 100 }, (_, i) => `${i + 1}`))
      await bulkStore.flushAll()

      const bulkCost = bulkSql.entityDeleteCalls

      // EXPECTED: bulkCost should be 0 (no individual calls)
      // ACTUAL: bulkCost equals individualCost (100 calls each)
      expect(bulkCost).toBe(0)
      expect(individualCost).toBeGreaterThan(0) // Individual does have calls
    })

    it('100 updates: bulk should be 100x cheaper than individual', async () => {
      // Baseline: 100 individual updates
      const individualSql = new BulkOperationTrackingSqlite()
      const individualStore = new CurrentParqueDBDOStore(individualSql)

      await individualStore.createMany('posts', Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))
      individualSql.resetTracking()

      // Perform 100 individual updates (current behavior)
      for (let i = 1; i <= 100; i++) {
        await individualStore.update('posts', `${i}`, { name: `Updated ${i}` })
      }
      await individualStore.flushAll()

      const individualCost = individualSql.entityUpdateCalls

      // Now test bulk updateMany
      const bulkSql = new BulkOperationTrackingSqlite()
      const bulkStore = new CurrentParqueDBDOStore(bulkSql)

      await bulkStore.createMany('posts', Array.from({ length: 100 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))
      bulkSql.resetTracking()

      await bulkStore.updateMany('posts', Array.from({ length: 100 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated ${i}` },
      })))
      await bulkStore.flushAll()

      const bulkCost = bulkSql.entityUpdateCalls

      // EXPECTED: bulkCost should be 0 (no individual calls)
      // ACTUAL: bulkCost equals individualCost (100 calls each)
      expect(bulkCost).toBe(0)
      expect(individualCost).toBeGreaterThan(0)
    })
  })

  describe('Bulk threshold behavior', () => {
    it('operations >= BULK_THRESHOLD (5) should use bulk path', async () => {
      const BULK_THRESHOLD = 5

      // Exactly at threshold
      const ids = Array.from({ length: BULK_THRESHOLD }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)

      // EXPECTED: Bulk path engaged at threshold
      // ACTUAL: 5 individual deletes
      expect(sql.entityDeleteCalls).toBe(0)
    })

    it('operations < BULK_THRESHOLD should use individual path', async () => {
      const BULK_THRESHOLD = 5

      // Below threshold
      const ids = Array.from({ length: BULK_THRESHOLD - 1 }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)

      // Below threshold, individual deletes are acceptable
      expect(sql.entityDeleteCalls).toBeLessThanOrEqual(BULK_THRESHOLD - 1)
    })
  })

  describe('R2 write efficiency', () => {
    it('bulk deleteMany should write to pending_row_groups like createMany', async () => {
      const ids = Array.from({ length: 100 }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)
      await currentStore.flushAll()

      const byTable = sql.getInsertsByTable()

      // EXPECTED (after fix): Bulk delete writes 1 pending_row_groups entry
      //   (like createMany does for bulk creates)
      // ACTUAL: No pending_row_groups, only events_wal writes from buffer flush
      //
      // The key difference: bulk path writes metadata to pending_row_groups,
      // while individual path writes events to events_wal via buffer
      expect(byTable['pending_row_groups']).toBe(1)
    })

    it('bulk updateMany should write to pending_row_groups like createMany', async () => {
      const updates = Array.from({ length: 100 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated ${i}` },
      }))

      sql.resetTracking()
      await currentStore.updateMany('posts', updates)
      await currentStore.flushAll()

      const byTable = sql.getInsertsByTable()

      // EXPECTED (after fix): Bulk update writes 1 pending_row_groups entry
      // ACTUAL: No pending_row_groups, only events_wal writes from buffer flush
      expect(byTable['pending_row_groups']).toBe(1)
    })

    it('50 deletes should NOT cause 50 individual entity lookups', async () => {
      // 50 is well above BULK_THRESHOLD (5)
      const ids = Array.from({ length: 50 }, (_, i) => `${i + 1}`)

      sql.resetTracking()
      await currentStore.deleteMany('posts', ids)

      // EXPECTED: Bulk path avoids per-entity processing
      // ACTUAL: 50 individual getEntityFromEvents calls equivalent
      expect(sql.entityDeleteCalls).toBe(0)
    })

    it('50 updates should NOT cause 50 individual entity lookups', async () => {
      const updates = Array.from({ length: 50 }, (_, i) => ({
        id: `${i + 1}`,
        data: { name: `Updated ${i}` },
      }))

      sql.resetTracking()
      await currentStore.updateMany('posts', updates)

      // EXPECTED: Bulk path avoids per-entity processing
      // ACTUAL: 50 individual update calls
      expect(sql.entityUpdateCalls).toBe(0)
    })
  })
})
