/**
 * ParqueDBDO WAL Batching Unit Tests
 *
 * Tests for the event batching functionality that reduces SQLite row costs
 * by batching multiple events into single rows.
 *
 * Following TDD approach:
 * 1. RED: Write failing tests first
 * 2. GREEN: Implement minimum code to pass
 * 3. REFACTOR: Clean up while keeping tests green
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  entityTarget,
  relTarget,
  isRelationshipTarget,
  parseEntityTarget,
} from '@/types/entity'
import type { Event } from '@/types/entity'

// =============================================================================
// Mock SqlStorage Implementation with WAL Support
// =============================================================================

type SqlStorageValue = string | number | null | Uint8Array

/**
 * Mock SqlStorage that simulates SQLite operations in memory
 * Extended to support WAL (Write-Ahead Log) batching tables
 */
class MockSqlStorage {
  private tables: Map<string, Map<string, Record<string, SqlStorageValue>>> = new Map()
  private indexes: Set<string> = new Set()
  private autoIncrementCounters: Map<string, number> = new Map()

  /**
   * Execute a SQL statement
   */
  exec<T extends Record<string, SqlStorageValue>>(query: string, ...params: SqlStorageValue[]): Iterable<T> {
    const trimmedQuery = query.trim().toUpperCase()

    if (trimmedQuery.startsWith('CREATE TABLE')) {
      this.handleCreateTable(query)
      return [] as unknown as Iterable<T>
    }

    if (trimmedQuery.startsWith('CREATE INDEX')) {
      this.handleCreateIndex(query)
      return [] as unknown as Iterable<T>
    }

    if (trimmedQuery.startsWith('INSERT INTO')) {
      return this.handleInsert(query, params) as Iterable<T>
    }

    if (trimmedQuery.startsWith('UPDATE')) {
      this.handleUpdate(query, params)
      return [] as unknown as Iterable<T>
    }

    if (trimmedQuery.startsWith('DELETE FROM')) {
      this.handleDelete(query, params)
      return [] as unknown as Iterable<T>
    }

    if (trimmedQuery.startsWith('SELECT')) {
      return this.handleSelect<T>(query, params)
    }

    return [] as unknown as Iterable<T>
  }

  private handleCreateTable(query: string): void {
    const match = query.match(/CREATE TABLE IF NOT EXISTS (\w+)/i)
    if (match) {
      const tableName = match[1].toLowerCase()
      if (!this.tables.has(tableName)) {
        this.tables.set(tableName, new Map())
        this.autoIncrementCounters.set(tableName, 0)
      }
    }
  }

  private handleCreateIndex(query: string): void {
    const match = query.match(/CREATE INDEX IF NOT EXISTS (\w+)/i)
    if (match) {
      this.indexes.add(match[1])
    }
  }

  private handleInsert(query: string, params: SqlStorageValue[]): Iterable<{ id: number }> {
    const tableMatch = query.match(/INSERT INTO (\w+)\s*\(/i)
    if (!tableMatch) return []

    const tableName = tableMatch[1].toLowerCase()
    const table = this.tables.get(tableName)
    if (!table) return []

    const colsMatch = query.match(/\(([^)]+)\)\s*VALUES/i)
    if (!colsMatch) return []

    const columns = colsMatch[1].split(',').map((c) => c.trim().toLowerCase())

    // Parse VALUES clause to handle both ? and literal values
    const valuesMatch = query.match(/VALUES\s*\(([^)]+)\)/i)
    if (!valuesMatch) return []

    const valuePlaceholders = valuesMatch[1].split(',').map((v) => v.trim())

    const row: Record<string, SqlStorageValue> = {}
    let paramIndex = 0

    // Check for AUTOINCREMENT id column
    const hasAutoId = !columns.includes('id') && (
      tableName === 'events_wal' || tableName === 'event_batches'
    )

    if (hasAutoId) {
      const counter = (this.autoIncrementCounters.get(tableName) || 0) + 1
      this.autoIncrementCounters.set(tableName, counter)
      row['id'] = counter
    }

    for (let i = 0; i < columns.length && i < valuePlaceholders.length; i++) {
      const placeholder = valuePlaceholders[i]
      if (placeholder === '?') {
        row[columns[i]] = params[paramIndex]
        paramIndex++
      } else if (placeholder === '0') {
        row[columns[i]] = 0
      } else if (placeholder === '1') {
        row[columns[i]] = 1
      } else if (placeholder === 'NULL') {
        row[columns[i]] = null
      } else {
        // Try to parse as number
        const num = parseInt(placeholder, 10)
        if (!isNaN(num)) {
          row[columns[i]] = num
        } else {
          // String literal (with quotes)
          row[columns[i]] = placeholder.replace(/^['"]|['"]$/g, '')
        }
      }
    }

    const key = this.generateKey(tableName, row)
    table.set(key, row)

    // Return the inserted row ID for AUTOINCREMENT tables
    if (hasAutoId) {
      return [{ id: row['id'] as number }]
    }
    return []
  }

  private handleUpdate(query: string, params: SqlStorageValue[]): void {
    const tableMatch = query.match(/UPDATE (\w+)\s+SET/i)
    if (!tableMatch) return

    const tableName = tableMatch[1].toLowerCase()
    const table = this.tables.get(tableName)
    if (!table) return

    const setMatch = query.match(/SET\s+(.+?)\s+WHERE/i)
    if (!setMatch) return

    const setClause = setMatch[1]
    const setParts = setClause.split(',').map((p) => p.trim())
    const setColumns: Array<{ column: string; isVersionIncrement: boolean; isLiteralNull: boolean; literalValue?: number }> = []

    for (const part of setParts) {
      // Check for version = version + 1 pattern
      const versionIncrMatch = part.match(/(\w+)\s*=\s*\1\s*\+\s*1/i)
      if (versionIncrMatch) {
        setColumns.push({ column: versionIncrMatch[1].toLowerCase(), isVersionIncrement: true, isLiteralNull: false })
        continue
      }

      // Check for column = NULL pattern (literal NULL, not a placeholder)
      const nullMatch = part.match(/(\w+)\s*=\s*NULL/i)
      if (nullMatch) {
        setColumns.push({ column: nullMatch[1].toLowerCase(), isVersionIncrement: false, isLiteralNull: true })
        continue
      }

      // Check for column = literal number pattern (e.g., flushed = 1)
      const literalMatch = part.match(/(\w+)\s*=\s*(\d+)/i)
      if (literalMatch) {
        setColumns.push({
          column: literalMatch[1].toLowerCase(),
          isVersionIncrement: false,
          isLiteralNull: false,
          literalValue: parseInt(literalMatch[2], 10)
        })
        continue
      }

      // Regular column = ? pattern
      const colMatch = part.match(/(\w+)\s*=\s*\?/i)
      if (colMatch) {
        setColumns.push({ column: colMatch[1].toLowerCase(), isVersionIncrement: false, isLiteralNull: false })
      }
    }

    const whereMatch = query.match(/WHERE\s+(.+)$/i)
    if (!whereMatch) return

    // Check for IN clause
    const inMatch = whereMatch[1].match(/(\w+)\s+IN\s*\(([^)]+)\)/i)
    if (inMatch) {
      const inColumn = inMatch[1].toLowerCase()
      const placeholders = inMatch[2].split(',').map(s => s.trim())
      const inValues = placeholders.map((_, i) => params[i])

      for (const [key, row] of table.entries()) {
        // Handle type coercion for comparison (numbers stored as numbers, compared with numbers)
        const rowValue = row[inColumn]
        const matches = inValues.some(v => {
          // Compare with type coercion
          return v === rowValue || String(v) === String(rowValue)
        })
        if (matches) {
          for (const setCol of setColumns) {
            if (setCol.isVersionIncrement) {
              row[setCol.column] = ((row[setCol.column] as number) || 0) + 1
            } else if (setCol.isLiteralNull) {
              row[setCol.column] = null
            } else if (setCol.literalValue !== undefined) {
              row[setCol.column] = setCol.literalValue
            }
          }
        }
      }
      return
    }

    const whereConditions = this.parseWhereConditions(whereMatch[1])

    // Count how many params are needed for SET clause (only columns with ? placeholders)
    const setParamCount = setColumns.filter(c => !c.isVersionIncrement && !c.isLiteralNull && c.literalValue === undefined).length
    const setParams = params.slice(0, setParamCount)
    const whereParams = params.slice(setParamCount)

    for (const [key, row] of table.entries()) {
      if (this.matchesWhere(row, whereConditions, whereParams)) {
        let paramIndex = 0
        for (const setCol of setColumns) {
          if (setCol.isVersionIncrement) {
            // Increment the version
            row[setCol.column] = ((row[setCol.column] as number) || 0) + 1
          } else if (setCol.isLiteralNull) {
            // Set to literal null
            row[setCol.column] = null
          } else if (setCol.literalValue !== undefined) {
            row[setCol.column] = setCol.literalValue
          } else {
            row[setCol.column] = setParams[paramIndex]
            paramIndex++
          }
        }
      }
    }
  }

  private handleDelete(query: string, params: SqlStorageValue[]): void {
    const tableMatch = query.match(/DELETE FROM (\w+)/i)
    if (!tableMatch) return

    const tableName = tableMatch[1].toLowerCase()
    const table = this.tables.get(tableName)
    if (!table) return

    const whereMatch = query.match(/WHERE\s+(.+)$/i)
    if (!whereMatch) {
      table.clear()
      return
    }

    const whereConditions = this.parseWhereConditions(whereMatch[1])

    const keysToDelete: string[] = []
    for (const [key, row] of table.entries()) {
      if (this.matchesWhere(row, whereConditions, params)) {
        keysToDelete.push(key)
      }
    }

    for (const key of keysToDelete) {
      table.delete(key)
    }
  }

  private handleSelect<T extends Record<string, SqlStorageValue>>(
    query: string,
    params: SqlStorageValue[]
  ): Iterable<T> {
    // Handle last_insert_rowid()
    if (query.toUpperCase().includes('LAST_INSERT_ROWID()')) {
      // Return the last auto-increment value
      const lastId = Math.max(...Array.from(this.autoIncrementCounters.values()))
      return [{ id: lastId }] as unknown as Iterable<T>
    }

    // Handle SUM(count)
    if (query.toUpperCase().includes('SUM(COUNT)') || query.toUpperCase().includes('SUM(EVENT_COUNT)')) {
      const tableMatch = query.match(/FROM (\w+)/i)
      if (!tableMatch) return [{ total: 0 }] as unknown as Iterable<T>

      const tableName = tableMatch[1].toLowerCase()
      const table = this.tables.get(tableName)
      if (!table) return [{ total: 0 }] as unknown as Iterable<T>

      const whereMatch = query.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
      let total = 0
      const countColumn = query.toUpperCase().includes('SUM(EVENT_COUNT)') ? 'event_count' : 'count'

      if (whereMatch) {
        const whereConditions = this.parseWhereConditions(whereMatch[1])
        for (const row of table.values()) {
          if (this.matchesWhere(row, whereConditions, params)) {
            total += (row[countColumn] as number) || 0
          }
        }
      } else {
        for (const row of table.values()) {
          total += (row[countColumn] as number) || 0
        }
      }

      return [{ total }] as unknown as Iterable<T>
    }

    if (query.toUpperCase().includes('COUNT(*)')) {
      const tableMatch = query.match(/FROM (\w+)/i)
      if (!tableMatch) return [] as unknown as Iterable<T>

      const tableName = tableMatch[1].toLowerCase()
      const table = this.tables.get(tableName)
      if (!table) return [{ count: 0 }] as unknown as Iterable<T>

      const whereMatch = query.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
      let count = 0

      if (whereMatch) {
        const whereConditions = this.parseWhereConditions(whereMatch[1])
        for (const row of table.values()) {
          if (this.matchesWhere(row, whereConditions, params)) {
            count++
          }
        }
      } else {
        count = table.size
      }

      return [{ count }] as unknown as Iterable<T>
    }

    const tableMatch = query.match(/FROM (\w+)/i)
    if (!tableMatch) return [] as unknown as Iterable<T>

    const tableName = tableMatch[1].toLowerCase()
    const table = this.tables.get(tableName)
    if (!table) return [] as unknown as Iterable<T>

    const whereMatch = query.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
    const results: T[] = []

    if (whereMatch) {
      const whereConditions = this.parseWhereConditions(whereMatch[1])
      for (const row of table.values()) {
        if (this.matchesWhere(row, whereConditions, params)) {
          results.push({ ...row } as T)
        }
      }
    } else {
      for (const row of table.values()) {
        results.push({ ...row } as T)
      }
    }

    // Handle ORDER BY
    const orderMatch = query.match(/ORDER BY\s+(\w+)\s*(ASC|DESC)?/i)
    if (orderMatch) {
      const orderColumn = orderMatch[1].toLowerCase()
      const orderDir = (orderMatch[2]?.toUpperCase() || 'ASC') === 'DESC' ? -1 : 1
      results.sort((a, b) => {
        const aVal = a[orderColumn]
        const bVal = b[orderColumn]
        if (aVal === bVal) return 0
        if (aVal === null || aVal === undefined) return 1
        if (bVal === null || bVal === undefined) return -1
        return aVal < bVal ? -orderDir : orderDir
      })
    }

    const limitMatch = query.match(/LIMIT\s+(\d+)/i)
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10)
      return results.slice(0, limit) as Iterable<T>
    }

    return results as Iterable<T>
  }

  private parseWhereConditions(whereClause: string): Array<{ column: string; op: string; isNull?: boolean; literalValue?: number | string }> {
    const conditions: Array<{ column: string; op: string; isNull?: boolean; literalValue?: number | string }> = []

    // Normalize: remove newlines and extra whitespace
    const normalizedClause = whereClause.replace(/\s+/g, ' ').trim()

    // Split by AND (case insensitive)
    const parts = normalizedClause.split(/\s+AND\s+/i)

    for (const part of parts) {
      const trimmedPart = part.trim()

      // Handle IS NULL / IS NOT NULL
      const nullMatch = trimmedPart.match(/^(\w+)\s+IS\s+(NOT\s+)?NULL$/i)
      if (nullMatch) {
        conditions.push({
          column: nullMatch[1].toLowerCase(),
          op: nullMatch[2] ? 'IS NOT NULL' : 'IS NULL',
          isNull: !nullMatch[2],
        })
        continue
      }

      // Handle IN clause
      const inMatch = trimmedPart.match(/^(\w+)\s+IN\s*\(/i)
      if (inMatch) {
        conditions.push({
          column: inMatch[1].toLowerCase(),
          op: 'IN',
        })
        continue
      }

      // Handle = ? with possible surrounding parentheses
      const eqMatch = trimmedPart.match(/^\(?(\w+)\s*=\s*\?\)?$/i)
      if (eqMatch) {
        conditions.push({
          column: eqMatch[1].toLowerCase(),
          op: '=',
        })
        continue
      }

      // Handle column = literal number (e.g., flushed = 0)
      const literalNumMatch = trimmedPart.match(/(\w+)\s*=\s*(\d+)/i)
      if (literalNumMatch) {
        conditions.push({
          column: literalNumMatch[1].toLowerCase(),
          op: '=literal',
          literalValue: parseInt(literalNumMatch[2], 10),
        })
        continue
      }

      // Handle column = ? patterns within more complex expressions
      const simpleEqMatch = trimmedPart.match(/(\w+)\s*=\s*\?/i)
      if (simpleEqMatch) {
        conditions.push({
          column: simpleEqMatch[1].toLowerCase(),
          op: '=',
        })
      }
    }

    return conditions
  }

  private matchesWhere(
    row: Record<string, SqlStorageValue>,
    conditions: Array<{ column: string; op: string; isNull?: boolean; literalValue?: number | string }>,
    params: SqlStorageValue[]
  ): boolean {
    let paramIndex = 0

    for (const cond of conditions) {
      const value = row[cond.column]

      if (cond.op === 'IS NULL') {
        // Treat undefined and null as equivalent for IS NULL check
        if (value !== null && value !== undefined) return false
      } else if (cond.op === 'IS NOT NULL') {
        // Treat undefined and null as equivalent for IS NOT NULL check
        if (value === null || value === undefined) return false
      } else if (cond.op === '=') {
        if (value !== params[paramIndex]) return false
        paramIndex++
      } else if (cond.op === '=literal') {
        // Compare with literal value
        if (value !== cond.literalValue) return false
      } else if (cond.op === 'IN') {
        if (value !== params[paramIndex]) return false
        paramIndex++
      }
    }

    return true
  }

  private generateKey(tableName: string, row: Record<string, SqlStorageValue>): string {
    switch (tableName) {
      case 'entities':
        return `${row['ns']}:${row['id']}`
      case 'relationships':
        return `${row['from_ns']}:${row['from_id']}:${row['predicate']}:${row['to_ns']}:${row['to_id']}`
      case 'events':
        return row['id'] as string
      case 'checkpoints':
        return row['id'] as string
      case 'events_wal':
      case 'event_batches':
        return String(row['id'])
      default:
        return Math.random().toString(36)
    }
  }

  getTable(name: string): Map<string, Record<string, SqlStorageValue>> | undefined {
    return this.tables.get(name.toLowerCase())
  }

  getAutoIncrementCounter(tableName: string): number {
    return this.autoIncrementCounters.get(tableName.toLowerCase()) || 0
  }
}

// =============================================================================
// ULID Generation (simplified, for event IDs)
// =============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
let lastTime = 0
let lastRandom = 0

function generateULID(): string {
  let now = Date.now()
  if (now === lastTime) {
    lastRandom++
  } else {
    lastTime = now
    lastRandom = Math.floor(Math.random() * 0xffffffffffff)
  }

  let time = ''
  for (let i = 0; i < 10; i++) {
    time = ENCODING[now % 32] + time
    now = Math.floor(now / 32)
  }

  let random = ''
  let r = lastRandom
  for (let i = 0; i < 16; i++) {
    random = ENCODING[r % 32] + random
    r = Math.floor(r / 32)
  }

  return time + random
}

// =============================================================================
// WAL Batching Types and Constants
// =============================================================================

/** Event buffer threshold - number of events before flush */
const EVENT_BATCH_COUNT_THRESHOLD = 100

/** Event buffer threshold - bytes before flush */
const EVENT_BATCH_SIZE_THRESHOLD = 64 * 1024 // 64KB

interface EventBatchRow {
  id: number
  batch: Uint8Array
  min_ts: number
  max_ts: number
  event_count: number
  flushed: number
  created_at: string
}

// =============================================================================
// Mock ParqueDBDO with WAL Batching Implementation
// =============================================================================

interface Entity {
  $id: string
  $type: string
  name: string
  createdAt: Date
  createdBy: string
  updatedAt: Date
  updatedBy: string
  deletedAt?: Date
  deletedBy?: string
  version: number
  [key: string]: unknown
}

interface StoredEntity {
  [key: string]: SqlStorageValue
  ns: string
  id: string
  type: string
  name: string
  version: number
  created_at: string
  created_by: string
  updated_at: string
  updated_by: string
  deleted_at: string | null
  deleted_by: string | null
  data: string
}

interface CreateInput {
  $type: string
  name: string
  [key: string]: unknown
}

interface CreateOptions {
  actor?: string
}

/**
 * Mock ParqueDBDO with WAL batching support
 * Implements event batching to reduce SQLite row costs
 */
class MockParqueDBDOWithWal {
  private sql: MockSqlStorage
  private initialized = false

  // WAL batching state
  private eventBuffer: Event[] = []
  private eventBufferSize = 0

  constructor() {
    this.sql = new MockSqlStorage()
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Original tables
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        ns TEXT NOT NULL,
        id TEXT NOT NULL,
        type TEXT NOT NULL,
        name TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        updated_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        data TEXT NOT NULL,
        PRIMARY KEY (ns, id)
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS relationships (
        from_ns TEXT NOT NULL,
        from_id TEXT NOT NULL,
        predicate TEXT NOT NULL,
        to_ns TEXT NOT NULL,
        to_id TEXT NOT NULL,
        reverse TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        data TEXT,
        PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
      )
    `)

    // NEW: Event batches table (replaces per-event 'events' table)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS event_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch BLOB NOT NULL,
        min_ts INTEGER NOT NULL,
        max_ts INTEGER NOT NULL,
        event_count INTEGER NOT NULL,
        flushed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        event_count INTEGER NOT NULL,
        first_event_id TEXT NOT NULL,
        last_event_id TEXT NOT NULL,
        parquet_path TEXT NOT NULL
      )
    `)

    // Create indexes
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_event_batches_flushed ON event_batches(flushed, min_ts)')

    this.initialized = true
  }

  async create(ns: string, data: CreateInput, options: CreateOptions = {}): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const id = generateULID().toLowerCase()

    const { $type, name, ...rest } = data
    if (!$type) {
      throw new Error('Entity must have $type')
    }
    if (!name) {
      throw new Error('Entity must have name')
    }

    const dataWithoutLinks: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(rest)) {
      dataWithoutLinks[key] = value
    }

    const dataJson = JSON.stringify(dataWithoutLinks)

    this.sql.exec(
      `INSERT INTO entities (ns, id, type, name, version, created_at, created_by, updated_at, updated_by, data)
       VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
      ns, id, $type, name, now, actor, now, actor, dataJson
    )

    // Buffer the event instead of writing directly
    await this.bufferEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: entityTarget(ns, id),
      before: undefined,
      after: { ...dataWithoutLinks, $type, name },
      actor,
    })

    return this.toEntity({
      ns, id, type: $type, name, version: 1,
      created_at: now, created_by: actor,
      updated_at: now, updated_by: actor,
      deleted_at: null, deleted_by: null,
      data: dataJson,
    })
  }

  async get(ns: string, id: string): Promise<Entity | null> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec<StoredEntity>(
      'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL',
      ns, id
    )]

    if (rows.length === 0) {
      return null
    }

    return this.toEntity(rows[0])
  }

  // ===========================================================================
  // WAL Batching Methods
  // ===========================================================================

  /**
   * Buffer an event for batched writing
   * Flushes when threshold is reached
   */
  private async bufferEvent(event: Event): Promise<void> {
    this.eventBuffer.push(event)

    // Estimate size (rough approximation)
    const eventJson = JSON.stringify(event)
    this.eventBufferSize += eventJson.length

    // Check if we should flush
    if (this.eventBuffer.length >= EVENT_BATCH_COUNT_THRESHOLD ||
        this.eventBufferSize >= EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushEventBatch()
    }
  }

  /**
   * Flush buffered events as a single batch row
   */
  async flushEventBatch(): Promise<void> {
    if (this.eventBuffer.length === 0) return

    await this.ensureInitialized()

    const events = this.eventBuffer
    const minTs = Math.min(...events.map(e => e.ts))
    const maxTs = Math.max(...events.map(e => e.ts))

    // Serialize events to blob
    const json = JSON.stringify(events)
    const data = new TextEncoder().encode(json)

    this.sql.exec(
      `INSERT INTO event_batches (batch, min_ts, max_ts, event_count, flushed)
       VALUES (?, ?, ?, ?, 0)`,
      data,
      minTs,
      maxTs,
      events.length
    )

    // Clear buffer
    this.eventBuffer = []
    this.eventBufferSize = 0
  }

  /**
   * Get unflushed event count (sum across all batches + buffer)
   */
  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()

    // Count events in flushed batches
    const rows = [...this.sql.exec<{ total: number }>(
      'SELECT SUM(event_count) as total FROM event_batches WHERE flushed = 0'
    )]

    const batchCount = rows[0]?.total || 0

    // Add buffered events not yet written
    return batchCount + this.eventBuffer.length
  }

  /**
   * Get unflushed batch count
   */
  async getUnflushedBatchCount(): Promise<number> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM event_batches WHERE flushed = 0'
    )]

    return rows[0]?.count || 0
  }

  /**
   * Read all unflushed events (from batches + buffer)
   */
  async readUnflushedEvents(): Promise<Event[]> {
    await this.ensureInitialized()

    const allEvents: Event[] = []

    // Read from batches
    const rows = [...this.sql.exec<EventBatchRow>(
      `SELECT id, batch, min_ts, max_ts, event_count
       FROM event_batches
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    for (const row of rows) {
      const batchEvents = this.deserializeBatch(row.batch)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    allEvents.push(...this.eventBuffer)

    return allEvents
  }

  /**
   * Mark batches as flushed
   */
  async markBatchesFlushed(batchIds: number[]): Promise<void> {
    if (batchIds.length === 0) return

    await this.ensureInitialized()

    const placeholders = batchIds.map(() => '?').join(',')
    this.sql.exec(
      `UPDATE event_batches SET flushed = 1 WHERE id IN (${placeholders})`,
      ...batchIds
    )
  }

  /**
   * Get buffer state for testing
   */
  getBufferState(): { count: number; sizeBytes: number } {
    return {
      count: this.eventBuffer.length,
      sizeBytes: this.eventBufferSize,
    }
  }

  private deserializeBatch(batch: SqlStorageValue): Event[] {
    if (!batch) return []

    let data: Uint8Array
    if (batch instanceof Uint8Array) {
      data = batch
    } else if (batch instanceof ArrayBuffer) {
      data = new Uint8Array(batch)
    } else {
      // Assume it's already a buffer-like object
      data = new Uint8Array(batch as ArrayBuffer)
    }

    const json = new TextDecoder().decode(data)
    return JSON.parse(json)
  }

  private toEntity(stored: StoredEntity): Entity {
    const data = JSON.parse(stored.data) as Record<string, unknown>

    return {
      $id: `${stored.ns}/${stored.id}`,
      $type: stored.type,
      name: stored.name,
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by,
      updatedAt: new Date(stored.updated_at),
      updatedBy: stored.updated_by,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by || undefined,
      version: stored.version,
      ...data,
    }
  }

  // Expose SQL for testing
  getSql(): MockSqlStorage {
    return this.sql
  }
}

// =============================================================================
// Test Suites
// =============================================================================

describe('ParqueDBDO WAL Batching', () => {
  let dbo: MockParqueDBDOWithWal

  beforeEach(() => {
    dbo = new MockParqueDBDOWithWal()
  })

  // ===========================================================================
  // Event Batching Tests
  // ===========================================================================

  describe('Event Buffering', () => {
    it('should buffer events in memory before threshold', async () => {
      // Create a few entities (under threshold)
      for (let i = 0; i < 5; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Events should still be in buffer
      const bufferState = dbo.getBufferState()
      expect(bufferState.count).toBe(5)

      // No batches should be written yet
      const batchCount = await dbo.getUnflushedBatchCount()
      expect(batchCount).toBe(0)
    })

    it('should batch multiple events into single SQLite row', async () => {
      // Create enough entities to trigger flush
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Should have flushed to a batch
      const batchCount = await dbo.getUnflushedBatchCount()
      expect(batchCount).toBe(1)

      // Buffer should be empty after flush
      const bufferState = dbo.getBufferState()
      expect(bufferState.count).toBe(0)
    })

    it('should flush batch when count threshold reached (100 events)', async () => {
      // Create exactly threshold number of entities
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Verify batch was written
      const batchCount = await dbo.getUnflushedBatchCount()
      expect(batchCount).toBe(1)

      // Verify total event count
      const eventCount = await dbo.getUnflushedEventCount()
      expect(eventCount).toBe(EVENT_BATCH_COUNT_THRESHOLD)
    })

    it('should flush batch when size threshold reached (64KB)', async () => {
      // Create entities with large data to exceed size threshold
      const largeData = 'x'.repeat(10000) // ~10KB per entity

      // Should trigger flush before reaching count threshold
      for (let i = 0; i < 10; i++) {
        await dbo.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          content: largeData,
        })
      }

      // Should have flushed due to size
      const batchCount = await dbo.getUnflushedBatchCount()
      expect(batchCount).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Reading Batched Events', () => {
    it('should read back batched events correctly', async () => {
      // Create events and flush
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Read back all events
      const events = await dbo.readUnflushedEvents()

      expect(events.length).toBe(EVENT_BATCH_COUNT_THRESHOLD)
      expect(events[0].op).toBe('CREATE')
      expect(events[0].target).toMatch(/^posts:/)
    })

    it('should include buffered events in unflushed count', async () => {
      // Create some entities (under threshold)
      for (let i = 0; i < 50; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Should count buffered events
      const count = await dbo.getUnflushedEventCount()
      expect(count).toBe(50)
    })

    it('should combine batched and buffered events', async () => {
      // Create enough to trigger one batch
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Batch ${i}` })
      }

      // Create more that stay in buffer
      for (let i = 0; i < 25; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Buffer ${i}` })
      }

      // Should have both batch and buffer events
      const events = await dbo.readUnflushedEvents()
      expect(events.length).toBe(EVENT_BATCH_COUNT_THRESHOLD + 25)
    })
  })

  describe('Partial Batch Handling', () => {
    it('should handle partial batches on manual flush', async () => {
      // Create fewer than threshold
      for (let i = 0; i < 25; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Manually flush (simulates DO shutdown)
      await dbo.flushEventBatch()

      // Should have written a partial batch
      const batchCount = await dbo.getUnflushedBatchCount()
      expect(batchCount).toBe(1)

      // Buffer should be empty
      const bufferState = dbo.getBufferState()
      expect(bufferState.count).toBe(0)

      // Events should be readable
      const events = await dbo.readUnflushedEvents()
      expect(events.length).toBe(25)
    })

    it('should preserve event order across batches', async () => {
      // Create events across multiple batches
      const totalEvents = EVENT_BATCH_COUNT_THRESHOLD * 2 + 50

      for (let i = 0; i < totalEvents; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Flush remaining buffer
      await dbo.flushEventBatch()

      // Read all events
      const events = await dbo.readUnflushedEvents()

      // Verify order (timestamps should be non-decreasing)
      for (let i = 1; i < events.length; i++) {
        expect(events[i].ts).toBeGreaterThanOrEqual(events[i - 1].ts)
      }
    })
  })

  describe('Batch Flush Management', () => {
    it('should mark batches as flushed', async () => {
      // Create and flush events
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Get batch IDs
      const table = dbo.getSql().getTable('event_batches')
      const batchIds = Array.from(table?.values() || []).map(r => r['id'] as number)

      // Mark as flushed
      await dbo.markBatchesFlushed(batchIds)

      // Should show 0 unflushed batches
      const unflushedCount = await dbo.getUnflushedBatchCount()
      expect(unflushedCount).toBe(0)
    })

    it('should not count flushed batches in unflushed event count', async () => {
      // Create and flush events
      for (let i = 0; i < EVENT_BATCH_COUNT_THRESHOLD; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Get batch IDs and mark as flushed
      const table = dbo.getSql().getTable('event_batches')
      const batchIds = Array.from(table?.values() || []).map(r => r['id'] as number)
      await dbo.markBatchesFlushed(batchIds)

      // Create more events in buffer
      for (let i = 0; i < 10; i++) {
        await dbo.create('posts', { $type: 'Post', name: `New ${i}` })
      }

      // Should only count unflushed
      const count = await dbo.getUnflushedEventCount()
      expect(count).toBe(10)
    })
  })

  describe('Schema Migration', () => {
    it('should create event_batches table instead of events table', async () => {
      // Trigger initialization
      await dbo.create('posts', { $type: 'Post', name: 'Test' })

      // Check tables exist
      const eventBatchesTable = dbo.getSql().getTable('event_batches')
      expect(eventBatchesTable).toBeDefined()
    })
  })

  describe('Cost Optimization', () => {
    it('should reduce row count compared to per-event storage', async () => {
      // Create many events
      const totalEvents = 500

      for (let i = 0; i < totalEvents; i++) {
        await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Flush remaining
      await dbo.flushEventBatch()

      // Count rows in event_batches table
      const table = dbo.getSql().getTable('event_batches')
      const rowCount = table?.size || 0

      // With batching, we should have far fewer rows than events
      // 500 events / 100 per batch = 5 batches (or 6 with partial)
      expect(rowCount).toBeLessThanOrEqual(6)
      expect(rowCount).toBeGreaterThan(0)

      // Verify we still have all events
      const eventCount = await dbo.getUnflushedEventCount()
      expect(eventCount).toBe(totalEvents)
    })
  })
})
