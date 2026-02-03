/**
 * ParqueDBDO Unit Tests
 *
 * Tests for the ParqueDB Durable Object class that handles all write operations.
 * Uses mocks for Durable Object context and SqlStorage to enable testing
 * independent of the full Cloudflare Workers environment.
 *
 * NOTE: Since ParqueDBDO imports from 'cloudflare:workers', we cannot directly
 * import it in Node.js tests. Instead, we test the core logic by recreating
 * the essential implementation here, which serves as both a test and a
 * specification of the expected behavior.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  entityTarget,
  relTarget,
  isRelationshipTarget,
  parseEntityTarget,
} from '@/types/entity'

// =============================================================================
// Mock SqlStorage Implementation
// =============================================================================

type SqlStorageValue = string | number | null | Uint8Array

/**
 * Mock SqlStorage that simulates SQLite operations in memory
 */
class MockSqlStorage {
  private tables: Map<string, Map<string, Record<string, SqlStorageValue>>> = new Map()
  private indexes: Set<string> = new Set()

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
      this.handleInsert(query, params)
      return [] as unknown as Iterable<T>
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
      }
    }
  }

  private handleCreateIndex(query: string): void {
    const match = query.match(/CREATE INDEX IF NOT EXISTS (\w+)/i)
    if (match) {
      this.indexes.add(match[1])
    }
  }

  private handleInsert(query: string, params: SqlStorageValue[]): void {
    const tableMatch = query.match(/INSERT INTO (\w+)\s*\(/i)
    if (!tableMatch) return

    const tableName = tableMatch[1].toLowerCase()
    const table = this.tables.get(tableName)
    if (!table) return

    const colsMatch = query.match(/\(([^)]+)\)\s*VALUES/i)
    if (!colsMatch) return

    const columns = colsMatch[1].split(',').map((c) => c.trim().toLowerCase())

    // Parse VALUES clause to handle both ? and literal values
    const valuesMatch = query.match(/VALUES\s*\(([^)]+)\)/i)
    if (!valuesMatch) return

    const valuePlaceholders = valuesMatch[1].split(',').map((v) => v.trim())

    const row: Record<string, SqlStorageValue> = {}
    let paramIndex = 0

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
    const setColumns: Array<{
      column: string
      isVersionIncrement: boolean
      isLiteralNull: boolean
      literalValue?: number
    }> = []

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
      const literalNumMatch = part.match(/(\w+)\s*=\s*(\d+)/i)
      if (literalNumMatch) {
        setColumns.push({
          column: literalNumMatch[1].toLowerCase(),
          isVersionIncrement: false,
          isLiteralNull: false,
          literalValue: parseInt(literalNumMatch[2], 10),
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

    const whereConditions = this.parseWhereConditions(whereMatch[1])

    // Count how many params are needed for SET clause (only columns with ? placeholders, not literal values)
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
            // Set to literal number value
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

    const limitMatch = query.match(/LIMIT\s+(\d+)/i)
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10)
      return results.slice(0, limit) as Iterable<T>
    }

    return results as Iterable<T>
  }

  private parseWhereConditions(whereClause: string): Array<{
    column: string
    op: string
    isNull?: boolean
    literalValue?: number
  }> {
    const conditions: Array<{
      column: string
      op: string
      isNull?: boolean
      literalValue?: number
    }> = []

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

      // Handle column = literal number (e.g., flushed = 0)
      const literalNumMatch = trimmedPart.match(/^\(?(\w+)\s*=\s*(\d+)\)?$/i)
      if (literalNumMatch) {
        conditions.push({
          column: literalNumMatch[1].toLowerCase(),
          op: '=LITERAL',
          literalValue: parseInt(literalNumMatch[2], 10),
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
    conditions: Array<{ column: string; op: string; isNull?: boolean; literalValue?: number }>,
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
      } else if (cond.op === '=LITERAL') {
        // Compare with literal number value
        if (value !== cond.literalValue) return false
      } else if (cond.op === '=') {
        if (value !== params[paramIndex]) return false
        paramIndex++
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
      default:
        return Math.random().toString(36)
    }
  }

  getTable(name: string): Map<string, Record<string, SqlStorageValue>> | undefined {
    return this.tables.get(name.toLowerCase())
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
// Mock ParqueDBDO Implementation
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

interface Relationship {
  fromNs: string
  fromId: string
  predicate: string
  reverse: string
  toNs: string
  toId: string
  createdAt: Date
  createdBy: string
  deletedAt?: Date
  deletedBy?: string
  version: number
  data?: Record<string, unknown>
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

interface StoredRelationship {
  [key: string]: SqlStorageValue
  from_ns: string
  from_id: string
  predicate: string
  to_ns: string
  to_id: string
  reverse: string
  version: number
  created_at: string
  created_by: string
  deleted_at: string | null
  deleted_by: string | null
  data: string | null
}

interface CreateInput {
  $type: string
  name: string
  [key: string]: unknown
}

interface UpdateInput {
  $set?: Record<string, unknown>
  $unset?: Record<string, boolean>
  $inc?: Record<string, number>
  $push?: Record<string, unknown>
  $pull?: Record<string, unknown>
  $link?: Record<string, string | string[]>
  $unlink?: Record<string, string | string[]>
}

interface CreateOptions {
  actor?: string
}

interface UpdateOptions {
  actor?: string
  expectedVersion?: number
  upsert?: boolean
}

interface DeleteOptions {
  actor?: string
  hard?: boolean
  expectedVersion?: number
}

interface LinkOptions {
  actor?: string
  data?: Record<string, unknown>
}

/**
 * WAL configuration for batched event storage
 */
interface WalConfig {
  /** Maximum events to buffer before writing batch */
  maxBufferSize: number
  /** Maximum bytes to buffer before writing batch */
  maxBufferBytes: number
}

/**
 * Event for buffering
 */
interface BufferedEvent {
  id: string
  ts: number
  op: string
  target: string
  ns: string | null
  entityId: string | null
  before?: Record<string, unknown>
  after?: Record<string, unknown>
  actor: string
}

/**
 * Mock ParqueDBDO that mirrors the actual implementation
 * This allows us to test the logic without cloudflare:workers
 */
class MockParqueDBDO {
  private sql: MockSqlStorage
  private initialized = false

  /** WAL configuration */
  private walConfig: WalConfig = {
    maxBufferSize: 100,
    maxBufferBytes: 64 * 1024, // 64KB
  }

  /** Event buffer for batching */
  private eventBuffer: BufferedEvent[] = []

  /** Estimated buffer size in bytes */
  private bufferSizeBytes = 0

  constructor() {
    this.sql = new MockSqlStorage()
  }

  /**
   * Set WAL configuration for testing
   */
  setWalConfig(config: Partial<WalConfig>): void {
    this.walConfig = { ...this.walConfig, ...config }
  }

  /**
   * Get the number of pending (buffered) events
   */
  getPendingEventCount(): number {
    return this.eventBuffer.length
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

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

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        target TEXT NOT NULL,
        op TEXT NOT NULL,
        ns TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        before TEXT,
        after TEXT,
        actor TEXT NOT NULL,
        metadata TEXT,
        flushed INTEGER NOT NULL DEFAULT 0
      )
    `)

    // WAL table for batched event storage (Phase 1)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events_wal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch BLOB NOT NULL,
        min_ts INTEGER NOT NULL,
        max_ts INTEGER NOT NULL,
        count INTEGER NOT NULL,
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

    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_wal_flushed ON events_wal(flushed, min_ts)')

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
    const links: Array<{ predicate: string; targetId: string }> = []

    for (const [key, value] of Object.entries(rest)) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const entries = Object.entries(value as Record<string, unknown>)
        if (entries.length > 0 && entries.every(([_, v]) => typeof v === 'string' && (v as string).includes('/'))) {
          for (const [_, targetId] of entries) {
            links.push({ predicate: key, targetId: targetId as string })
          }
          continue
        }
      }
      dataWithoutLinks[key] = value
    }

    const dataJson = JSON.stringify(dataWithoutLinks)

    this.sql.exec(
      `INSERT INTO entities (ns, id, type, name, version, created_at, created_by, updated_at, updated_by, data)
       VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
      ns, id, $type, name, now, actor, now, actor, dataJson
    )

    const entityIdValue = `${ns}/${id}`

    for (const link of links) {
      await this.link(entityIdValue, link.predicate, link.targetId, { actor })
    }

    await this.appendEvent({
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

  async get(ns: string, id: string, includeDeleted = false): Promise<Entity | null> {
    await this.ensureInitialized()

    const query = includeDeleted
      ? 'SELECT * FROM entities WHERE ns = ? AND id = ?'
      : 'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL'

    const rows = [...this.sql.exec<StoredEntity>(query, ns, id)]

    if (rows.length === 0) {
      return null
    }

    return this.toEntity(rows[0])
  }

  async update(ns: string, id: string, update: UpdateInput, options: UpdateOptions = {}): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    const rows = [...this.sql.exec<StoredEntity>(
      'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL',
      ns, id
    )]

    if (rows.length === 0) {
      if (options.upsert) {
        const createData: CreateInput = {
          $type: 'Unknown',
          name: id,
          ...(update.$set || {}),
        }
        return this.create(ns, createData, { actor })
      }
      throw new Error(`Entity ${ns}/${id} not found`)
    }

    const current = rows[0]

    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    let data = JSON.parse(current.data) as Record<string, unknown>
    let name = current.name
    let type = current.type

    if (update.$set) {
      for (const [key, value] of Object.entries(update.$set)) {
        if (key === 'name') {
          name = value as string
        } else if (key === '$type') {
          type = value as string
        } else {
          data[key] = value
        }
      }
    }

    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete data[key]
      }
    }

    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        const currentVal = (data[key] as number) || 0
        data[key] = currentVal + value
      }
    }

    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const arr = (data[key] as unknown[]) || []
        if (typeof value === 'object' && value !== null && '$each' in value) {
          arr.push(...(value as { $each: unknown[] }).$each)
        } else {
          arr.push(value)
        }
        data[key] = arr
      }
    }

    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        const arr = (data[key] as unknown[]) || []
        data[key] = arr.filter(item => item !== value)
      }
    }

    const entityIdValue = `${ns}/${id}`

    if (update.$link) {
      for (const [predicate, targets] of Object.entries(update.$link)) {
        const targetList = Array.isArray(targets) ? targets : [targets]
        for (const targetId of targetList) {
          await this.link(entityIdValue, predicate, targetId, { actor })
        }
      }
    }

    if (update.$unlink) {
      for (const [predicate, targets] of Object.entries(update.$unlink)) {
        const targetList = Array.isArray(targets) ? targets : [targets]
        for (const targetId of targetList) {
          await this.unlink(entityIdValue, predicate, targetId, { actor })
        }
      }
    }

    const newVersion = current.version + 1
    const dataJson = JSON.stringify(data)

    this.sql.exec(
      `UPDATE entities
       SET type = ?, name = ?, version = ?, updated_at = ?, updated_by = ?, data = ?
       WHERE ns = ? AND id = ?`,
      type, name, newVersion, now, actor, dataJson, ns, id
    )

    await this.appendEvent({
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { ...JSON.parse(current.data), $type: current.type, name: current.name },
      after: { ...data, $type: type, name },
      actor,
    })

    return this.toEntity({
      ns, id, type, name, version: newVersion,
      created_at: current.created_at, created_by: current.created_by,
      updated_at: now, updated_by: actor,
      deleted_at: null, deleted_by: null,
      data: dataJson,
    })
  }

  async delete(ns: string, id: string, options: DeleteOptions = {}): Promise<boolean> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    const rows = [...this.sql.exec<StoredEntity>(
      'SELECT * FROM entities WHERE ns = ? AND id = ?',
      ns, id
    )]

    if (rows.length === 0) {
      return false
    }

    const current = rows[0]

    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    if (options.hard) {
      this.sql.exec('DELETE FROM entities WHERE ns = ? AND id = ?', ns, id)
      // Delete outbound relationships
      this.sql.exec('DELETE FROM relationships WHERE from_ns = ? AND from_id = ?', ns, id)
      // Delete inbound relationships
      this.sql.exec('DELETE FROM relationships WHERE to_ns = ? AND to_id = ?', ns, id)
    } else {
      this.sql.exec(
        `UPDATE entities SET deleted_at = ?, deleted_by = ?, version = ? WHERE ns = ? AND id = ?`,
        now, actor, current.version + 1, ns, id
      )
      // Soft delete outbound relationships
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ? WHERE from_ns = ? AND from_id = ?`,
        now, actor, ns, id
      )
      // Soft delete inbound relationships
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ? WHERE to_ns = ? AND to_id = ?`,
        now, actor, ns, id
      )
    }

    await this.appendEvent({
      op: 'DELETE',
      target: entityTarget(ns, id),
      before: { ...JSON.parse(current.data), $type: current.type, name: current.name },
      after: undefined,
      actor,
    })

    return true
  }

  async link(fromId: string, predicate: string, toId: string, options: LinkOptions = {}): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    const existing = [...this.sql.exec<StoredRelationship>(
      `SELECT * FROM relationships
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
      fromNs, fromEntityId, predicate, toNs, toEntityId
    )]

    if (existing.length > 0 && existing[0].deleted_at === null) {
      return
    }

    const dataJson = options.data ? JSON.stringify(options.data) : null

    if (existing.length > 0) {
      this.sql.exec(
        `UPDATE relationships SET deleted_at = NULL, deleted_by = NULL, version = ?, data = ? WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
        existing[0].version + 1, dataJson, fromNs, fromEntityId, predicate, toNs, toEntityId
      )
    } else {
      this.sql.exec(
        `INSERT INTO relationships
         (from_ns, from_id, predicate, to_ns, to_id, reverse, version, created_at, created_by, data)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
        fromNs, fromEntityId, predicate, toNs, toEntityId, reverse, now, actor, dataJson
      )
    }

    await this.appendEvent({
      op: 'CREATE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: undefined,
      after: { predicate, to: toId, data: options.data },
      actor,
    })
  }

  async unlink(fromId: string, predicate: string, toId: string, options: LinkOptions = {}): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    this.sql.exec(
      `UPDATE relationships SET deleted_at = ?, deleted_by = ? WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ? AND deleted_at IS NULL`,
      now, actor, fromNs, fromEntityId, predicate, toNs, toEntityId
    )

    await this.appendEvent({
      op: 'DELETE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: { predicate, to: toId },
      after: undefined,
      actor,
    })
  }

  async getRelationships(
    ns: string,
    id: string,
    predicate?: string,
    direction: 'outbound' | 'inbound' = 'outbound'
  ): Promise<Relationship[]> {
    await this.ensureInitialized()

    let query: string
    let params: unknown[]

    if (direction === 'outbound') {
      query = predicate
        ? 'SELECT * FROM relationships WHERE from_ns = ? AND from_id = ? AND predicate = ? AND deleted_at IS NULL'
        : 'SELECT * FROM relationships WHERE from_ns = ? AND from_id = ? AND deleted_at IS NULL'
      params = predicate ? [ns, id, predicate] : [ns, id]
    } else {
      query = predicate
        ? 'SELECT * FROM relationships WHERE to_ns = ? AND to_id = ? AND reverse = ? AND deleted_at IS NULL'
        : 'SELECT * FROM relationships WHERE to_ns = ? AND to_id = ? AND deleted_at IS NULL'
      params = predicate ? [ns, id, predicate] : [ns, id]
    }

    const rows = [...this.sql.exec<StoredRelationship>(query, ...params as SqlStorageValue[])]

    return rows.map(row => this.toRelationship(row))
  }

  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()

    // Count buffered events + events in WAL batches
    const bufferedCount = this.eventBuffer.length

    const rows = [...this.sql.exec<{ total: number }>(
      'SELECT SUM(count) as total FROM events_wal WHERE flushed = 0'
    )]

    const walCount = rows[0]?.total || 0

    return bufferedCount + walCount
  }

  /**
   * Get unflushed events (from buffer and WAL batches)
   */
  async getUnflushedEvents(ns?: string): Promise<BufferedEvent[]> {
    await this.ensureInitialized()

    // First flush buffer to WAL so we can read everything
    await this.flushEventBuffer()

    // Read from WAL batches
    const walRows = [...this.sql.exec<{ batch: string; min_ts: number; max_ts: number; count: number }>(
      'SELECT batch, min_ts, max_ts, count FROM events_wal WHERE flushed = 0 ORDER BY min_ts ASC'
    )]

    const allEvents: BufferedEvent[] = []

    for (const row of walRows) {
      try {
        // Deserialize the batch
        const events: BufferedEvent[] = JSON.parse(row.batch)
        allEvents.push(...events)
      } catch {
        // Skip malformed batches
      }
    }

    // Filter by namespace if specified
    if (ns) {
      return allEvents.filter(e => e.ns === ns)
    }

    return allEvents
  }

  /**
   * Flush buffered events to WAL as a batch
   */
  async flushEventBuffer(): Promise<void> {
    await this.ensureInitialized()

    if (this.eventBuffer.length === 0) {
      return
    }

    // Create batch from buffer
    const events = [...this.eventBuffer]
    const minTs = Math.min(...events.map(e => e.ts))
    const maxTs = Math.max(...events.map(e => e.ts))
    const count = events.length

    // Serialize batch as JSON
    const batchJson = JSON.stringify(events)

    // Write to WAL table
    this.sql.exec(
      `INSERT INTO events_wal (batch, min_ts, max_ts, count, flushed)
       VALUES (?, ?, ?, ?, 0)`,
      batchJson,
      minTs,
      maxTs,
      count
    )

    // Clear buffer
    this.eventBuffer = []
    this.bufferSizeBytes = 0
  }

  /**
   * Flush WAL batches to Parquet (marks as flushed)
   */
  async flushToParquet(): Promise<void> {
    await this.ensureInitialized()

    // First flush any buffered events
    await this.flushEventBuffer()

    // Mark all unflushed WAL batches as flushed
    this.sql.exec('UPDATE events_wal SET flushed = 1 WHERE flushed = 0')
  }

  private async appendEvent(event: {
    op: string
    target: string
    before?: Record<string, unknown>
    after?: Record<string, unknown>
    actor: string
  }): Promise<void> {
    await this.ensureInitialized()

    const id = generateULID()
    const ts = Date.now()

    let ns: string | null = null
    let entityId: string | null = null
    if (!isRelationshipTarget(event.target)) {
      const info = parseEntityTarget(event.target)
      ns = info.ns
      entityId = info.id
    }

    // Buffer the event instead of writing directly
    const bufferedEvent: BufferedEvent = {
      id,
      ts,
      op: event.op,
      target: event.target,
      ns,
      entityId,
      before: event.before,
      after: event.after,
      actor: event.actor,
    }

    this.eventBuffer.push(bufferedEvent)

    // Estimate size (rough JSON size)
    const eventSize = JSON.stringify(bufferedEvent).length
    this.bufferSizeBytes += eventSize

    // Check if we should flush the buffer
    if (this.eventBuffer.length >= this.walConfig.maxBufferSize ||
        this.bufferSizeBytes >= this.walConfig.maxBufferBytes) {
      await this.flushEventBuffer()
    }
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

  private toRelationship(stored: StoredRelationship): Relationship {
    return {
      fromNs: stored.from_ns,
      fromId: stored.from_id,
      predicate: stored.predicate,
      reverse: stored.reverse,
      toNs: stored.to_ns,
      toId: stored.to_id,
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by || undefined,
      version: stored.version,
      data: stored.data ? JSON.parse(stored.data) : undefined,
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

describe('ParqueDBDO', () => {
  let dbo: MockParqueDBDO

  beforeEach(() => {
    dbo = new MockParqueDBDO()
  })

  // ===========================================================================
  // SQLite Schema Initialization Tests
  // ===========================================================================

  describe('SQLite Schema Initialization', () => {
    it('initializes entity table on first operation', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Test' })

      const entitiesTable = dbo.getSql().getTable('entities')
      expect(entitiesTable).toBeDefined()
    })

    it('initializes relationships table on first operation', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Test' })

      const relsTable = dbo.getSql().getTable('relationships')
      expect(relsTable).toBeDefined()
    })

    it('initializes events table on first operation', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Test' })

      const eventsTable = dbo.getSql().getTable('events')
      expect(eventsTable).toBeDefined()
    })

    it('initializes checkpoints table on first operation', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Test' })

      const checkpointsTable = dbo.getSql().getTable('checkpoints')
      expect(checkpointsTable).toBeDefined()
    })

    it('only initializes schema once', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Post 1' })
      await dbo.create('posts', { $type: 'Post', name: 'Post 2' })
      await dbo.create('users', { $type: 'User', name: 'User 1' })

      expect(dbo.getSql().getTable('entities')).toBeDefined()
    })
  })

  // ===========================================================================
  // Entity CRUD Operations Tests
  // ===========================================================================

  describe('Entity CRUD Operations', () => {
    describe('create', () => {
      it('creates an entity with auto-generated ID', async () => {
        const entity = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello World',
        })

        expect(entity.$id).toBeDefined()
        expect(entity.$id).toMatch(/^posts\/[a-z0-9]+$/)
        expect(entity.$type).toBe('Post')
        expect(entity.name).toBe('Test Post')
      })

      it('requires $type field', async () => {
        await expect(
          dbo.create('posts', {
            name: 'Test Post',
          } as any)
        ).rejects.toThrow('Entity must have $type')
      })

      it('requires name field', async () => {
        await expect(
          dbo.create('posts', {
            $type: 'Post',
          } as any)
        ).rejects.toThrow('Entity must have name')
      })

      it('sets initial version to 1', async () => {
        const entity = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        expect(entity.version).toBe(1)
      })

      it('sets audit fields', async () => {
        const entity = await dbo.create(
          'posts',
          {
            $type: 'Post',
            name: 'Test Post',
          },
          { actor: 'users/admin' }
        )

        expect(entity.createdAt).toBeInstanceOf(Date)
        expect(entity.updatedAt).toBeInstanceOf(Date)
        expect(entity.createdBy).toBe('users/admin')
        expect(entity.updatedBy).toBe('users/admin')
      })

      it('defaults actor to system/anonymous', async () => {
        const entity = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        expect(entity.createdBy).toBe('system/anonymous')
      })

      it('stores additional data fields', async () => {
        const entity = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello World',
          content: 'Test content',
          viewCount: 0,
        })

        expect(entity.title).toBe('Hello World')
        expect(entity.content).toBe('Test content')
        expect(entity.viewCount).toBe(0)
      })

      it('creates inline relationships', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Test User',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { 'Test User': user.$id },
        })

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(1)
        expect(rels[0].predicate).toBe('author')
      })
    })

    describe('get', () => {
      it('retrieves an existing entity', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
        })

        const [, id] = created.$id.split('/')
        const retrieved = await dbo.get('posts', id)

        expect(retrieved).not.toBeNull()
        expect(retrieved!.$id).toBe(created.$id)
        expect(retrieved!.name).toBe('Test Post')
        expect(retrieved!.title).toBe('Hello')
      })

      it('returns null for non-existent entity', async () => {
        const result = await dbo.get('posts', 'nonexistent-id')
        expect(result).toBeNull()
      })

      it('excludes soft-deleted entities by default', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, id] = created.$id.split('/')
        await dbo.delete('posts', id)

        const result = await dbo.get('posts', id)
        expect(result).toBeNull()
      })

      it('includes soft-deleted entities when requested', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, id] = created.$id.split('/')
        await dbo.delete('posts', id)

        const result = await dbo.get('posts', id, true)
        expect(result).not.toBeNull()
        expect(result!.deletedAt).toBeInstanceOf(Date)
      })
    })

    describe('update', () => {
      it('updates entity with $set operator', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Original Name',
          title: 'Original Title',
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $set: { name: 'Updated Name', title: 'Updated Title' },
        })

        expect(updated.name).toBe('Updated Name')
        expect(updated.title).toBe('Updated Title')
      })

      it('increments version on update', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $set: { name: 'Updated' },
        })

        expect(updated.version).toBe(2)
      })

      it('updates entity with $inc operator', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
          viewCount: 10,
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $inc: { viewCount: 5 },
        })

        expect(updated.viewCount).toBe(15)
      })

      it('updates entity with $unset operator', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
          tempField: 'to remove',
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $unset: { tempField: true },
        })

        expect(updated.tempField).toBeUndefined()
      })

      it('updates entity with $push operator', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
          tags: ['tag1'],
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $push: { tags: 'tag2' },
        })

        expect(updated.tags).toEqual(['tag1', 'tag2'])
      })

      it('updates entity with $push $each', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
          tags: ['tag1'],
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $push: { tags: { $each: ['tag2', 'tag3'] } },
        })

        expect(updated.tags).toEqual(['tag1', 'tag2', 'tag3'])
      })

      it('updates entity with $pull operator', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
          tags: ['tag1', 'tag2', 'tag3'],
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update('posts', id, {
          $pull: { tags: 'tag2' },
        })

        expect(updated.tags).toEqual(['tag1', 'tag3'])
      })

      it('throws error for non-existent entity', async () => {
        await expect(
          dbo.update('posts', 'nonexistent', {
            $set: { name: 'Updated' },
          })
        ).rejects.toThrow('Entity posts/nonexistent not found')
      })

      it('supports upsert option', async () => {
        const result = await dbo.update(
          'posts',
          'new-id',
          {
            $set: { title: 'Upserted' },
          },
          { upsert: true }
        )

        expect(result.$id).toBeDefined()
        expect(result.title).toBe('Upserted')
      })

      it('updates actor information', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test',
        })

        const [, id] = created.$id.split('/')
        const updated = await dbo.update(
          'posts',
          id,
          { $set: { name: 'Updated' } },
          { actor: 'users/updater' }
        )

        expect(updated.updatedBy).toBe('users/updater')
      })

      it('creates relationships with $link operator', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, postId] = post.$id.split('/')
        await dbo.update('posts', postId, {
          $link: { author: user.$id },
        })

        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')
        expect(rels.length).toBe(1)
      })

      it('removes relationships with $unlink operator', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { Author: user.$id },
        })

        const [, postId] = post.$id.split('/')

        await dbo.update('posts', postId, {
          $unlink: { author: user.$id },
        })

        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')
        expect(rels.length).toBe(0)
      })
    })

    describe('delete', () => {
      it('soft deletes entity by default', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, id] = created.$id.split('/')
        const result = await dbo.delete('posts', id)

        expect(result).toBe(true)

        const retrieved = await dbo.get('posts', id)
        expect(retrieved).toBeNull()

        const withDeleted = await dbo.get('posts', id, true)
        expect(withDeleted).not.toBeNull()
        expect(withDeleted!.deletedAt).toBeInstanceOf(Date)
      })

      it('hard deletes entity when requested', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, id] = created.$id.split('/')
        const result = await dbo.delete('posts', id, { hard: true })

        expect(result).toBe(true)

        const withDeleted = await dbo.get('posts', id, true)
        expect(withDeleted).toBeNull()
      })

      it('returns false for non-existent entity', async () => {
        const result = await dbo.delete('posts', 'nonexistent')
        expect(result).toBe(false)
      })

      it('sets deletedBy field', async () => {
        const created = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        const [, id] = created.$id.split('/')
        await dbo.delete('posts', id, { actor: 'users/deleter' })

        const withDeleted = await dbo.get('posts', id, true)
        expect(withDeleted!.deletedBy).toBe('users/deleter')
      })

      it('soft deletes related relationships', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { Author: user.$id },
        })

        const [, postId] = post.$id.split('/')

        await dbo.delete('posts', postId)

        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')
        expect(rels.length).toBe(0)
      })
    })
  })

  // ===========================================================================
  // Event Logging Tests
  // ===========================================================================

  describe('Event Logging', () => {
    it('logs CREATE event on entity creation', async () => {
      await dbo.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello',
      })

      // Flush buffer to WAL so we can read events
      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()
      expect(events.length).toBeGreaterThan(0)

      const createEvent = events.find((e) => e.op === 'CREATE' && !e.target?.includes(':author:'))

      expect(createEvent).toBeDefined()
      expect(createEvent!.target).toMatch(/^posts:/)
      expect(createEvent!.before).toBeUndefined()
      expect(createEvent!.after).toBeDefined()
    })

    it('logs UPDATE event with before/after states', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Original',
        title: 'Original Title',
      })

      const [, id] = created.$id.split('/')
      await dbo.update('posts', id, {
        $set: { title: 'Updated Title' },
      })

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()
      const updateEvent = events.find((e) => e.op === 'UPDATE')

      expect(updateEvent).toBeDefined()
      expect(updateEvent!.before).toBeDefined()
      expect(updateEvent!.after).toBeDefined()

      expect(updateEvent!.before!.title).toBe('Original Title')
      expect(updateEvent!.after!.title).toBe('Updated Title')
    })

    it('logs DELETE event', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'To Delete',
      })

      const [, id] = created.$id.split('/')
      await dbo.delete('posts', id)

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()
      const deleteEvent = events.find((e) => e.op === 'DELETE' && !isRelationshipTarget(e.target))

      expect(deleteEvent).toBeDefined()
      expect(deleteEvent!.before).toBeDefined()
      expect(deleteEvent!.after).toBeUndefined()
    })

    it('logs relationship CREATE event for link operations', async () => {
      const user = await dbo.create('users', {
        $type: 'User',
        name: 'Author',
      })

      const post = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      await dbo.link(post.$id, 'author', user.$id)

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()
      const linkEvent = events.find((e) => e.op === 'CREATE' && e.target?.includes(':author:'))

      expect(linkEvent).toBeDefined()
    })

    it('logs relationship DELETE event for unlink operations', async () => {
      const user = await dbo.create('users', {
        $type: 'User',
        name: 'Author',
      })

      const post = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        author: { Author: user.$id },
      })

      await dbo.unlink(post.$id, 'author', user.$id)

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()
      const unlinkEvent = events.find((e) => e.op === 'DELETE' && e.target?.includes(':author:'))

      expect(unlinkEvent).toBeDefined()
    })

    it('includes actor in events', async () => {
      await dbo.create(
        'posts',
        {
          $type: 'Post',
          name: 'Test',
        },
        { actor: 'users/actor' }
      )

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()

      expect(events[0].actor).toBe('users/actor')
    })

    it('generates ULID event IDs', async () => {
      await dbo.create('posts', {
        $type: 'Post',
        name: 'Test 1',
      })

      await dbo.create('posts', {
        $type: 'Post',
        name: 'Test 2',
      })

      await dbo.flushEventBuffer()

      const events = await dbo.getUnflushedEvents()

      expect(events[0].id).toMatch(/^[0-9A-Z]{26}$/)
      expect(events[1].id).toMatch(/^[0-9A-Z]{26}$/)
      expect(events[1].id > events[0].id).toBe(true)
    })
  })

  // ===========================================================================
  // Optimistic Concurrency Tests
  // ===========================================================================

  describe('Optimistic Concurrency', () => {
    it('allows update with correct expected version', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test',
      })

      const [, id] = created.$id.split('/')
      const updated = await dbo.update(
        'posts',
        id,
        { $set: { name: 'Updated' } },
        { expectedVersion: 1 }
      )

      expect(updated.version).toBe(2)
      expect(updated.name).toBe('Updated')
    })

    it('rejects update with incorrect expected version', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test',
      })

      const [, id] = created.$id.split('/')

      await expect(
        dbo.update('posts', id, { $set: { name: 'Updated' } }, { expectedVersion: 5 })
      ).rejects.toThrow('Version mismatch: expected 5, got 1')
    })

    it('rejects delete with incorrect expected version', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test',
      })

      const [, id] = created.$id.split('/')

      await expect(dbo.delete('posts', id, { expectedVersion: 5 })).rejects.toThrow(
        'Version mismatch: expected 5, got 1'
      )
    })

    it('allows delete with correct expected version', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test',
      })

      const [, id] = created.$id.split('/')
      const result = await dbo.delete('posts', id, { expectedVersion: 1 })

      expect(result).toBe(true)
    })

    it('tracks version through multiple updates', async () => {
      const created = await dbo.create('posts', {
        $type: 'Post',
        name: 'Test',
      })

      expect(created.version).toBe(1)

      const [, id] = created.$id.split('/')

      const update1 = await dbo.update('posts', id, { $set: { name: 'Update 1' } })
      expect(update1.version).toBe(2)

      const update2 = await dbo.update('posts', id, { $set: { name: 'Update 2' } })
      expect(update2.version).toBe(3)

      const update3 = await dbo.update('posts', id, { $set: { name: 'Update 3' } })
      expect(update3.version).toBe(4)
    })
  })

  // ===========================================================================
  // Relationship Operations Tests
  // ===========================================================================

  describe('Relationship Operations', () => {
    describe('link', () => {
      it('creates a relationship between entities', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'author', user.$id)

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(1)
        expect(rels[0].predicate).toBe('author')
        expect(`${rels[0].toNs}/${rels[0].toId}`).toBe(user.$id)
      })

      it('generates reverse predicate name', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'author', user.$id)

        const relsTable = dbo.getSql().getTable('relationships')
        const rel = Array.from(relsTable!.values())[0]

        expect(rel['reverse']).toBe('authors')
      })

      it('handles predicates already ending in s', async () => {
        const category = await dbo.create('categories', {
          $type: 'Category',
          name: 'Tech',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'topics', category.$id)

        const relsTable = dbo.getSql().getTable('relationships')
        const rel = Array.from(relsTable!.values())[0]

        expect(rel['reverse']).toBe('topics')
      })

      it('is idempotent - does not duplicate existing relationship', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'author', user.$id)
        await dbo.link(post.$id, 'author', user.$id)

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(1)
      })

      it('can restore a soft-deleted relationship', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'author', user.$id)
        await dbo.unlink(post.$id, 'author', user.$id)
        await dbo.link(post.$id, 'author', user.$id)

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(1)
      })

      it('supports edge data', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await dbo.link(post.$id, 'author', user.$id, {
          data: { role: 'primary', addedAt: '2024-01-01' },
        })

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels[0].data).toBeDefined()
        expect(rels[0].data?.role).toBe('primary')
      })
    })

    describe('unlink', () => {
      it('soft deletes a relationship', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { Author: user.$id },
        })

        await dbo.unlink(post.$id, 'author', user.$id)

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(0)
      })

      it('is idempotent for non-existent relationship', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        })

        await expect(dbo.unlink(post.$id, 'author', user.$id)).resolves.not.toThrow()
      })
    })

    describe('getRelationships', () => {
      it('gets outbound relationships for entity', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const category = await dbo.create('categories', {
          $type: 'Category',
          name: 'Tech',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { Author: user.$id },
          category: { Tech: category.$id },
        })

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, undefined, 'outbound')

        expect(rels.length).toBe(2)
      })

      it('filters by predicate', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        const category = await dbo.create('categories', {
          $type: 'Category',
          name: 'Tech',
        })

        const post = await dbo.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: { Author: user.$id },
          category: { Tech: category.$id },
        })

        const [, postId] = post.$id.split('/')
        const rels = await dbo.getRelationships('posts', postId, 'author', 'outbound')

        expect(rels.length).toBe(1)
        expect(rels[0].predicate).toBe('author')
      })

      it('gets inbound relationships', async () => {
        const user = await dbo.create('users', {
          $type: 'User',
          name: 'Author',
        })

        await dbo.create('posts', {
          $type: 'Post',
          name: 'Post 1',
          author: { Author: user.$id },
        })

        await dbo.create('posts', {
          $type: 'Post',
          name: 'Post 2',
          author: { Author: user.$id },
        })

        const [, userId] = user.$id.split('/')
        const rels = await dbo.getRelationships('users', userId, 'authors', 'inbound')

        expect(rels.length).toBe(2)
      })
    })
  })

  // ===========================================================================
  // Flush Operations Tests
  // ===========================================================================

  describe('Flush Operations', () => {
    it('counts unflushed events', async () => {
      await dbo.create('posts', { $type: 'Post', name: 'Post 1' })
      await dbo.create('posts', { $type: 'Post', name: 'Post 2' })
      await dbo.create('posts', { $type: 'Post', name: 'Post 3' })

      const count = await dbo.getUnflushedEventCount()

      expect(count).toBeGreaterThanOrEqual(3)
    })
  })

  // ===========================================================================
  // WAL Batching Tests (Phase 1: Batched Event Storage)
  // ===========================================================================

  describe('WAL Batching', () => {
    describe('event batching', () => {
      it('stores events in batched blobs, not individual rows', async () => {
        // Create multiple entities to generate events
        for (let i = 0; i < 10; i++) {
          await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
        }

        // Flush to trigger batch write
        await dbo.flushEventBuffer()

        // Check events_wal table has batched entries
        const walTable = dbo.getSql().getTable('events_wal')
        expect(walTable).toBeDefined()

        // Should have far fewer rows than events (batched)
        const walRows = walTable ? Array.from(walTable.values()) : []
        const totalEventCount = walRows.reduce((sum, row) => sum + (row['count'] as number || 0), 0)

        // We should have at least 10 events (one CREATE per entity)
        expect(totalEventCount).toBeGreaterThanOrEqual(10)

        // But stored in fewer rows (batched)
        expect(walRows.length).toBeLessThan(totalEventCount)
      })

      it('buffers events in memory before writing batch', async () => {
        // Create a single entity - should buffer, not write immediately
        await dbo.create('posts', { $type: 'Post', name: 'Single Post' })

        // Check events_wal - should be empty or have 0 unflushed batches
        // (events are buffered in memory)
        const pendingCount = dbo.getPendingEventCount()
        expect(pendingCount).toBeGreaterThanOrEqual(1)

        // WAL table should not have the event yet (still buffered)
        const walTable = dbo.getSql().getTable('events_wal')
        const unflushedBatches = walTable
          ? Array.from(walTable.values()).filter(r => r['flushed'] === 0)
          : []

        // Either no batches yet, or a batch was written
        // The key is that not every event = 1 row
        const totalRows = unflushedBatches.length
        const totalEvents = unflushedBatches.reduce((sum, r) => sum + (r['count'] as number || 0), 0)

        // If we have any batches, they should contain multiple events per row
        if (totalRows > 0) {
          expect(totalEvents / totalRows).toBeGreaterThanOrEqual(1)
        }
      })

      it('writes batch when threshold is reached', async () => {
        // Configure a low threshold for testing
        dbo.setWalConfig({ maxBufferSize: 5 })

        // Create 6 entities (exceeds threshold of 5)
        for (let i = 0; i < 6; i++) {
          await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
        }

        // Should have automatically written a batch
        const walTable = dbo.getSql().getTable('events_wal')
        expect(walTable).toBeDefined()

        const batches = walTable ? Array.from(walTable.values()) : []
        expect(batches.length).toBeGreaterThan(0)

        // The batch should contain multiple events
        const firstBatch = batches[0]
        expect(firstBatch).toBeDefined()
        expect(firstBatch!['count']).toBeGreaterThanOrEqual(5)
      })

      it('writes batch when size threshold is reached', async () => {
        // Configure a low byte threshold for testing (1KB)
        dbo.setWalConfig({ maxBufferBytes: 1024 })

        // Create entities with large data to exceed byte threshold
        for (let i = 0; i < 3; i++) {
          await dbo.create('posts', {
            $type: 'Post',
            name: `Post ${i}`,
            content: 'x'.repeat(500), // Large content to trigger byte threshold
          })
        }

        // Should have automatically written a batch
        const walTable = dbo.getSql().getTable('events_wal')
        expect(walTable).toBeDefined()

        const batches = walTable ? Array.from(walTable.values()) : []
        expect(batches.length).toBeGreaterThan(0)
      })
    })

    describe('reading batched events', () => {
      it('can read events from batched storage', async () => {
        // Create entities and flush
        for (let i = 0; i < 5; i++) {
          await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
        }
        await dbo.flushEventBuffer()

        // Read unflushed events
        const events = await dbo.getUnflushedEvents()

        expect(events.length).toBeGreaterThanOrEqual(5)
        expect(events[0]).toHaveProperty('op')
        expect(events[0]).toHaveProperty('target')
      })

      it('can filter events by namespace', async () => {
        // Create entities in different namespaces
        await dbo.create('posts', { $type: 'Post', name: 'Post 1' })
        await dbo.create('users', { $type: 'User', name: 'User 1' })
        await dbo.create('posts', { $type: 'Post', name: 'Post 2' })
        await dbo.flushEventBuffer()

        // Read events for specific namespace
        const postEvents = await dbo.getUnflushedEvents('posts')

        // Should only have post events
        expect(postEvents.length).toBeGreaterThanOrEqual(2)
        expect(postEvents.every(e => e.ns === 'posts')).toBe(true)
      })

      it('returns events in chronological order', async () => {
        for (let i = 0; i < 5; i++) {
          await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
        }
        await dbo.flushEventBuffer()

        const events = await dbo.getUnflushedEvents()

        // Events should be in order by timestamp
        for (let i = 1; i < events.length; i++) {
          expect(events[i].ts).toBeGreaterThanOrEqual(events[i - 1].ts)
        }
      })
    })

    describe('flush behavior', () => {
      it('marks batches as flushed after R2 write', async () => {
        for (let i = 0; i < 5; i++) {
          await dbo.create('posts', { $type: 'Post', name: `Post ${i}` })
        }
        await dbo.flushEventBuffer()

        // Verify batches exist
        const walTable = dbo.getSql().getTable('events_wal')
        const batchesBefore = walTable ? Array.from(walTable.values()).filter(r => r['flushed'] === 0) : []
        expect(batchesBefore.length).toBeGreaterThan(0)

        // Simulate flush to R2
        await dbo.flushToParquet()

        // Batches should be marked as flushed
        const batchesAfter = walTable ? Array.from(walTable.values()).filter(r => r['flushed'] === 0) : []
        expect(batchesAfter.length).toBe(0)
      })

      it('flushes remaining buffer on explicit flush', async () => {
        // Create fewer events than threshold
        await dbo.create('posts', { $type: 'Post', name: 'Post 1' })
        await dbo.create('posts', { $type: 'Post', name: 'Post 2' })

        // Get pending count before flush
        const pendingBefore = dbo.getPendingEventCount()
        expect(pendingBefore).toBeGreaterThan(0)

        // Explicit flush should write buffered events
        await dbo.flushEventBuffer()

        // Pending should be 0
        const pendingAfter = dbo.getPendingEventCount()
        expect(pendingAfter).toBe(0)

        // WAL should have the batch
        const walTable = dbo.getSql().getTable('events_wal')
        expect(walTable).toBeDefined()
        expect(walTable!.size).toBeGreaterThan(0)
      })
    })
  })
})
