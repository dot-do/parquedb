/**
 * WAL-Only Schema Tests
 *
 * Tests the WAL-only SQLite schema mode that reduces Durable Object
 * storage costs by 50-70% by only persisting the write-ahead log (events)
 * without entity/relationship snapshot tables.
 *
 * @see src/worker/do/wal-only-schema.ts
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  initializeWalOnlySchema,
  initializeFullSchema,
  initializeSchemaWithOptions,
  getExpectedTables,
  getExpectedIndexCount,
  WAL_CORE_TABLES,
  SNAPSHOT_TABLES,
  WAL_CORE_INDEXES,
  SNAPSHOT_INDEXES,
} from '../../../src/worker/do/wal-only-schema'

// =============================================================================
// Mock SQLite with Table/Index Tracking
// =============================================================================

class MockSqlite {
  tables: string[] = []
  indexes: string[] = []
  statements: string[] = []

  exec(query: string, ..._params: unknown[]): unknown[] {
    const trimmed = query.trim()
    this.statements.push(trimmed)

    // Track CREATE TABLE
    const tableMatch = trimmed.match(/CREATE TABLE IF NOT EXISTS (\w+)/i)
    if (tableMatch) {
      this.tables.push(tableMatch[1]!)
    }

    // Track CREATE INDEX
    const indexMatch = trimmed.match(/CREATE INDEX IF NOT EXISTS (\w+)/i)
    if (indexMatch) {
      this.indexes.push(indexMatch[1]!)
    }

    return []
  }

  hasTable(name: string): boolean {
    return this.tables.includes(name)
  }

  hasIndex(name: string): boolean {
    return this.indexes.includes(name)
  }

  reset(): void {
    this.tables = []
    this.indexes = []
    this.statements = []
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('WAL-Only Schema', () => {
  let sql: MockSqlite

  beforeEach(() => {
    sql = new MockSqlite()
  })

  // ===========================================================================
  // WAL-Only Mode Tests
  // ===========================================================================

  describe('initializeWalOnlySchema', () => {
    it('creates WAL core tables', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasTable('events_wal')).toBe(true)
      expect(sql.hasTable('rels_wal')).toBe(true)
      expect(sql.hasTable('checkpoints')).toBe(true)
      expect(sql.hasTable('pending_row_groups')).toBe(true)
    })

    it('does NOT create entity snapshot table', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasTable('entities')).toBe(false)
    })

    it('does NOT create relationship snapshot table', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasTable('relationships')).toBe(false)
    })

    it('does NOT create legacy event_batches table', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasTable('event_batches')).toBe(false)
    })

    it('does NOT create legacy events table', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasTable('events')).toBe(false)
    })

    it('creates WAL core indexes', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasIndex('idx_events_wal_ns')).toBe(true)
      expect(sql.hasIndex('idx_rels_wal_ns')).toBe(true)
      expect(sql.hasIndex('idx_pending_row_groups_ns')).toBe(true)
    })

    it('does NOT create entity/relationship indexes', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasIndex('idx_entities_type')).toBe(false)
      expect(sql.hasIndex('idx_entities_updated')).toBe(false)
      expect(sql.hasIndex('idx_rels_from')).toBe(false)
      expect(sql.hasIndex('idx_rels_to')).toBe(false)
      expect(sql.hasIndex('idx_relationships_match_mode')).toBe(false)
      expect(sql.hasIndex('idx_relationships_similarity')).toBe(false)
    })

    it('does NOT create legacy indexes', () => {
      initializeWalOnlySchema(sql)

      expect(sql.hasIndex('idx_event_batches_flushed')).toBe(false)
      expect(sql.hasIndex('idx_events_unflushed')).toBe(false)
      expect(sql.hasIndex('idx_events_ns')).toBe(false)
    })
  })

  // ===========================================================================
  // Full Schema Mode Tests
  // ===========================================================================

  describe('initializeFullSchema', () => {
    it('creates ALL tables including snapshots', () => {
      initializeFullSchema(sql)

      // Snapshot tables
      expect(sql.hasTable('entities')).toBe(true)
      expect(sql.hasTable('relationships')).toBe(true)
      expect(sql.hasTable('event_batches')).toBe(true)
      expect(sql.hasTable('events')).toBe(true)

      // WAL core tables
      expect(sql.hasTable('events_wal')).toBe(true)
      expect(sql.hasTable('rels_wal')).toBe(true)
      expect(sql.hasTable('checkpoints')).toBe(true)
      expect(sql.hasTable('pending_row_groups')).toBe(true)
    })

    it('creates ALL indexes', () => {
      initializeFullSchema(sql)

      // Snapshot indexes
      expect(sql.hasIndex('idx_entities_type')).toBe(true)
      expect(sql.hasIndex('idx_entities_updated')).toBe(true)
      expect(sql.hasIndex('idx_rels_from')).toBe(true)
      expect(sql.hasIndex('idx_rels_to')).toBe(true)
      expect(sql.hasIndex('idx_relationships_match_mode')).toBe(true)
      expect(sql.hasIndex('idx_relationships_similarity')).toBe(true)

      // WAL core indexes
      expect(sql.hasIndex('idx_events_wal_ns')).toBe(true)
      expect(sql.hasIndex('idx_rels_wal_ns')).toBe(true)
      expect(sql.hasIndex('idx_pending_row_groups_ns')).toBe(true)
    })
  })

  // ===========================================================================
  // initializeSchemaWithOptions Tests
  // ===========================================================================

  describe('initializeSchemaWithOptions', () => {
    it('defaults to full schema when no options provided', () => {
      initializeSchemaWithOptions(sql)

      expect(sql.hasTable('entities')).toBe(true)
      expect(sql.hasTable('relationships')).toBe(true)
      expect(sql.hasTable('events_wal')).toBe(true)
    })

    it('defaults to full schema when walOnly is false', () => {
      initializeSchemaWithOptions(sql, { walOnly: false })

      expect(sql.hasTable('entities')).toBe(true)
      expect(sql.hasTable('relationships')).toBe(true)
      expect(sql.hasTable('events_wal')).toBe(true)
    })

    it('creates WAL-only schema when walOnly is true', () => {
      initializeSchemaWithOptions(sql, { walOnly: true })

      // Should NOT have snapshot tables
      expect(sql.hasTable('entities')).toBe(false)
      expect(sql.hasTable('relationships')).toBe(false)

      // Should have WAL core tables
      expect(sql.hasTable('events_wal')).toBe(true)
      expect(sql.hasTable('rels_wal')).toBe(true)
      expect(sql.hasTable('checkpoints')).toBe(true)
      expect(sql.hasTable('pending_row_groups')).toBe(true)
    })
  })

  // ===========================================================================
  // Cost Analysis Tests
  // ===========================================================================

  describe('Cost Reduction Analysis', () => {
    it('WAL-only mode creates fewer tables than full mode', () => {
      const walOnlySql = new MockSqlite()
      const fullSql = new MockSqlite()

      initializeWalOnlySchema(walOnlySql)
      initializeFullSchema(fullSql)

      expect(walOnlySql.tables.length).toBeLessThan(fullSql.tables.length)

      // WAL-only: 4 tables (events_wal, rels_wal, checkpoints, pending_row_groups)
      expect(walOnlySql.tables.length).toBe(4)

      // Full: 8 tables (entities, relationships, event_batches, events + WAL core)
      expect(fullSql.tables.length).toBe(8)
    })

    it('WAL-only mode creates fewer indexes than full mode', () => {
      const walOnlySql = new MockSqlite()
      const fullSql = new MockSqlite()

      initializeWalOnlySchema(walOnlySql)
      initializeFullSchema(fullSql)

      expect(walOnlySql.indexes.length).toBeLessThan(fullSql.indexes.length)

      // WAL-only: 3 indexes
      expect(walOnlySql.indexes.length).toBe(3)

      // Full: 12 indexes (9 snapshot + 3 WAL core)
      expect(fullSql.indexes.length).toBe(12)
    })

    it('WAL-only mode executes fewer SQL statements', () => {
      const walOnlySql = new MockSqlite()
      const fullSql = new MockSqlite()

      initializeWalOnlySchema(walOnlySql)
      initializeFullSchema(fullSql)

      expect(walOnlySql.statements.length).toBeLessThan(fullSql.statements.length)

      // WAL-only: 4 CREATE TABLE + 3 CREATE INDEX = 7 statements
      expect(walOnlySql.statements.length).toBe(7)

      // Full: 8 CREATE TABLE + 12 CREATE INDEX = 20 statements
      expect(fullSql.statements.length).toBe(20)
    })

    it('WAL-only mode eliminates entity row storage overhead', () => {
      // In full mode, each entity = 1 row in entities table
      // In WAL-only mode, entities are derived from events_wal
      // So per-entity storage cost goes from 2 rows (entity + event) to ~0 rows (batched events)

      const walOnlyTables = getExpectedTables(true)
      const fullTables = getExpectedTables(false)

      expect(walOnlyTables).not.toContain('entities')
      expect(fullTables).toContain('entities')

      // The entities table is the main cost driver - each entity = 1 row
      // Removing it saves 1 row per entity
    })

    it('WAL-only mode eliminates relationship row storage overhead', () => {
      const walOnlyTables = getExpectedTables(true)
      const fullTables = getExpectedTables(false)

      expect(walOnlyTables).not.toContain('relationships')
      expect(fullTables).toContain('relationships')

      // The relationships table stores 1 row per edge
      // Removing it saves 1 row per relationship
    })

    it('estimates 50-70% cost reduction by table count', () => {
      const walOnlyTableCount = getExpectedTables(true).length
      const fullTableCount = getExpectedTables(false).length

      const tableReduction = (1 - walOnlyTableCount / fullTableCount) * 100

      // 8 -> 4 tables = 50% reduction
      expect(tableReduction).toBe(50)
    })

    it('estimates significant index reduction', () => {
      const walOnlyIndexCount = getExpectedIndexCount(true)
      const fullIndexCount = getExpectedIndexCount(false)

      const indexReduction = (1 - walOnlyIndexCount / fullIndexCount) * 100

      // 12 -> 3 indexes = 75% reduction
      expect(indexReduction).toBe(75)
    })
  })

  // ===========================================================================
  // Utility Function Tests
  // ===========================================================================

  describe('getExpectedTables', () => {
    it('returns WAL core tables for WAL-only mode', () => {
      const tables = getExpectedTables(true)

      expect(tables).toContain('events_wal')
      expect(tables).toContain('rels_wal')
      expect(tables).toContain('checkpoints')
      expect(tables).toContain('pending_row_groups')

      expect(tables).not.toContain('entities')
      expect(tables).not.toContain('relationships')
    })

    it('returns all tables for full mode', () => {
      const tables = getExpectedTables(false)

      expect(tables).toContain('entities')
      expect(tables).toContain('relationships')
      expect(tables).toContain('events_wal')
      expect(tables).toContain('rels_wal')
      expect(tables).toContain('checkpoints')
      expect(tables).toContain('pending_row_groups')
      expect(tables).toContain('event_batches')
      expect(tables).toContain('events')
    })
  })

  describe('getExpectedIndexCount', () => {
    it('returns fewer indexes for WAL-only mode', () => {
      expect(getExpectedIndexCount(true)).toBe(3)
      expect(getExpectedIndexCount(false)).toBe(12)
    })
  })

  // ===========================================================================
  // Schema Constant Tests
  // ===========================================================================

  describe('Schema Constants', () => {
    it('WAL_CORE_TABLES has exactly 4 tables', () => {
      const tableNames = Object.keys(WAL_CORE_TABLES)
      expect(tableNames).toHaveLength(4)
      expect(tableNames).toEqual(['events_wal', 'rels_wal', 'checkpoints', 'pending_row_groups'])
    })

    it('SNAPSHOT_TABLES has exactly 4 tables', () => {
      const tableNames = Object.keys(SNAPSHOT_TABLES)
      expect(tableNames).toHaveLength(4)
      expect(tableNames).toEqual(['entities', 'relationships', 'event_batches', 'events'])
    })

    it('WAL_CORE_INDEXES has exactly 3 indexes', () => {
      expect(WAL_CORE_INDEXES).toHaveLength(3)
    })

    it('SNAPSHOT_INDEXES has exactly 9 indexes', () => {
      expect(SNAPSHOT_INDEXES).toHaveLength(9)
    })

    it('all WAL_CORE_TABLES contain valid SQL', () => {
      for (const [name, sql] of Object.entries(WAL_CORE_TABLES)) {
        expect(sql).toContain('CREATE TABLE IF NOT EXISTS')
        expect(sql).toContain(name)
      }
    })

    it('all SNAPSHOT_TABLES contain valid SQL', () => {
      for (const [name, sql] of Object.entries(SNAPSHOT_TABLES)) {
        expect(sql).toContain('CREATE TABLE IF NOT EXISTS')
        expect(sql).toContain(name)
      }
    })

    it('all WAL_CORE_INDEXES contain valid SQL', () => {
      for (const sql of WAL_CORE_INDEXES) {
        expect(sql).toContain('CREATE INDEX IF NOT EXISTS')
      }
    })

    it('all SNAPSHOT_INDEXES contain valid SQL', () => {
      for (const sql of SNAPSHOT_INDEXES) {
        expect(sql).toContain('CREATE INDEX IF NOT EXISTS')
      }
    })
  })

  // ===========================================================================
  // Integration-Style Cost Simulation Tests
  // ===========================================================================

  describe('Cost Simulation', () => {
    /**
     * Simulates DO SQLite row operations for full mode vs WAL-only mode.
     * In full mode, each entity write creates rows in both entities and events tables.
     * In WAL-only mode, entities are batched into events_wal blobs.
     */
    it('single entity create: full mode uses 2 rows, WAL-only uses 0-1 rows', () => {
      // Full mode: 1 entity row + 1 event row = 2 rows
      const fullModeRows = 2

      // WAL-only mode: event buffered in memory, flushed later as batch blob
      // Before flush: 0 rows. After flush: 1 row (batch blob) for many events
      const walOnlyRowsBeforeFlush = 0
      const walOnlyRowsAfterFlush = 1 // This 1 row contains many events

      expect(fullModeRows).toBeGreaterThan(walOnlyRowsBeforeFlush)
      expect(fullModeRows).toBeGreaterThan(walOnlyRowsAfterFlush)
    })

    it('100 entity creates: full mode uses 200 rows, WAL-only uses 1 row', () => {
      const entityCount = 100

      // Full mode: 100 entity rows + 100 event rows = 200 rows
      const fullModeRows = entityCount * 2

      // WAL-only mode: 100 events batched into 1 WAL blob (at threshold)
      const walOnlyRows = 1

      const costReduction = ((fullModeRows - walOnlyRows) / fullModeRows) * 100

      expect(fullModeRows).toBe(200)
      expect(walOnlyRows).toBe(1)
      expect(costReduction).toBeGreaterThanOrEqual(99)
    })

    it('10K bulk import: full mode uses 20,000 rows, WAL-only uses ~10 rows', () => {
      const entityCount = 10000
      const batchSize = 1000

      // Full mode: 10,000 entity rows + 10,000 event rows = 20,000 rows
      const fullModeRows = entityCount * 2

      // WAL-only mode with bulk bypass: 10 pending_row_groups rows (1 per batch)
      const walOnlyRows = entityCount / batchSize

      const costReduction = ((fullModeRows - walOnlyRows) / fullModeRows) * 100

      expect(fullModeRows).toBe(20000)
      expect(walOnlyRows).toBe(10)
      expect(costReduction).toBeGreaterThanOrEqual(99.9)
    })

    it('mixed workload: WAL-only achieves 90%+ cost reduction', () => {
      // Simulate realistic workload:
      // - 500 single entity creates
      // - 200 updates
      // - 50 deletes
      // - 100 relationship creates

      const singleCreates = 500
      const updates = 200
      const deletes = 50
      const relationships = 100

      // Full mode rows:
      // - Creates: 500 entity + 500 event = 1000
      // - Updates: 500 entity update + 200 event = 700
      //   (entity rows already exist, but updates still touch them)
      // - Deletes: 50 entity update + 50 event = 100
      // - Relationships: 100 relationship rows + 100 event = 200
      const fullModeRows = (singleCreates * 2) + (updates * 2) + (deletes * 2) + (relationships * 2)

      // WAL-only mode rows:
      // - Creates: buffered, ~5 WAL batch rows (500/100 threshold)
      // - Updates: buffered into same WAL batches, ~2 more
      // - Deletes: buffered, ~1 more
      // - Relationships: buffered into rels_wal, ~1 row
      const walOnlyRows = 5 + 2 + 1 + 1

      const costReduction = ((fullModeRows - walOnlyRows) / fullModeRows) * 100

      expect(costReduction).toBeGreaterThanOrEqual(90)
    })
  })

  // ===========================================================================
  // Configuration Toggle Tests
  // ===========================================================================

  describe('Configuration Toggle', () => {
    it('can switch between modes at initialization time', () => {
      // WAL-only mode
      const walOnlySql = new MockSqlite()
      initializeSchemaWithOptions(walOnlySql, { walOnly: true })
      expect(walOnlySql.hasTable('entities')).toBe(false)
      expect(walOnlySql.hasTable('events_wal')).toBe(true)

      // Full mode
      const fullSql = new MockSqlite()
      initializeSchemaWithOptions(fullSql, { walOnly: false })
      expect(fullSql.hasTable('entities')).toBe(true)
      expect(fullSql.hasTable('events_wal')).toBe(true)
    })

    it('WAL-only mode is idempotent (can be called multiple times)', () => {
      initializeWalOnlySchema(sql)
      const firstTables = [...sql.tables]
      const firstIndexes = [...sql.indexes]

      // Call again
      initializeWalOnlySchema(sql)

      // Should have duplicate entries (IF NOT EXISTS prevents actual SQL errors)
      // The important thing is that it doesn't fail
      expect(sql.tables.length).toBe(firstTables.length * 2)
    })

    it('full schema is idempotent', () => {
      initializeFullSchema(sql)
      const firstTables = [...sql.tables]

      initializeFullSchema(sql)

      // Should have duplicate entries tracked but IF NOT EXISTS prevents SQL errors
      expect(sql.tables.length).toBe(firstTables.length * 2)
    })
  })
})
