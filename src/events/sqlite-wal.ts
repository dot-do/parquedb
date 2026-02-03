/**
 * SQLite WAL (Write-Ahead Log) for Event Storage
 *
 * Stores event batches as blobs in SQLite for durability and cost optimization.
 * Each row contains a batch of events (up to 2MB) to minimize DO row charges.
 *
 * This module is designed for Cloudflare Durable Objects but can work with
 * any SQLite interface that supports the basic exec/query operations.
 */

import type { Event } from '../types/entity'
import type { EventBatch, SerializedBatch, WalRow } from './types'
import { isValidTableName } from '../utils/sql-security'

// =============================================================================
// Types
// =============================================================================

/**
 * SQLite interface compatible with DO SQLite
 */
export interface SqliteInterface {
  exec<T = unknown>(query: string, ...params: unknown[]): Iterable<T>
}

/**
 * Options for the SQLite WAL
 */
export interface SqliteWalOptions {
  /** Table name (default: 'events_wal') */
  tableName?: string | undefined
  /** Maximum blob size in bytes (default: 2MB) */
  maxBlobSize?: number | undefined
}

/**
 * Default options
 */
const DEFAULT_OPTIONS: Required<SqliteWalOptions> = {
  tableName: 'events_wal',
  maxBlobSize: 2 * 1024 * 1024, // 2MB
}

// =============================================================================
// SqliteWal Class
// =============================================================================

/**
 * Manages event storage in SQLite as compressed blobs.
 *
 * @example
 * ```typescript
 * const wal = new SqliteWal(this.sql)
 * await wal.ensureTable()
 *
 * // Write a batch
 * await wal.writeBatch(eventBatch)
 *
 * // Read all unflushed batches
 * const batches = await wal.readBatches()
 *
 * // Mark batches as flushed
 * await wal.markFlushed([1, 2, 3])
 * ```
 */
export class SqliteWal {
  private sql: SqliteInterface
  private options: Required<SqliteWalOptions>
  private tableCreated = false

  constructor(sql: SqliteInterface, options: SqliteWalOptions = {}) {
    this.sql = sql
    this.options = { ...DEFAULT_OPTIONS, ...options }

    // Validate table name to prevent SQL injection
    // Table names must be alphanumeric with underscores only
    if (!isValidTableName(this.options.tableName)) {
      throw new Error(
        `Invalid table name "${this.options.tableName}": must contain only alphanumeric characters and underscores, and start with a letter or underscore`
      )
    }
  }

  // ===========================================================================
  // Table Management
  // ===========================================================================

  /**
   * Create the WAL table if it doesn't exist
   */
  ensureTable(): void {
    if (this.tableCreated) return

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS ${this.options.tableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch BLOB NOT NULL,
        min_ts INTEGER NOT NULL,
        max_ts INTEGER NOT NULL,
        count INTEGER NOT NULL,
        flushed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `)

    // Index for reading unflushed batches
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_${this.options.tableName}_flushed
      ON ${this.options.tableName} (flushed, min_ts)
    `)

    this.tableCreated = true
  }

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  /**
   * Write an event batch to the WAL
   */
  writeBatch(batch: EventBatch): number {
    this.ensureTable()

    const serialized = this.serializeBatch(batch)

    if (serialized.data.length > this.options.maxBlobSize) {
      throw new Error(
        `Batch size ${serialized.data.length} exceeds max blob size ${this.options.maxBlobSize}`
      )
    }

    this.sql.exec(
      `INSERT INTO ${this.options.tableName} (batch, min_ts, max_ts, count)
       VALUES (?, ?, ?, ?)`,
      serialized.data,
      serialized.minTs,
      serialized.maxTs,
      serialized.count
    )

    // Get the inserted row ID
    const result = [...this.sql.exec<{ id: number }>(
      'SELECT last_insert_rowid() as id'
    )]
    return result[0]?.id ?? 0
  }

  /**
   * Write multiple batches (for bulk operations)
   */
  writeBatches(batches: EventBatch[]): number[] {
    return batches.map(batch => this.writeBatch(batch))
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Read all unflushed batches
   */
  readUnflushedBatches(): EventBatch[] {
    this.ensureTable()

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, batch, min_ts, max_ts, count
       FROM ${this.options.tableName}
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    return rows.map(row => this.deserializeBatch(row))
  }

  /**
   * Read batches within a time range
   */
  readBatchesByTimeRange(minTs: number, maxTs: number): EventBatch[] {
    this.ensureTable()

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, batch, min_ts, max_ts, count
       FROM ${this.options.tableName}
       WHERE max_ts >= ? AND min_ts <= ?
       ORDER BY min_ts ASC`,
      minTs,
      maxTs
    )]

    return rows.map(row => this.deserializeBatch(row))
  }

  /**
   * Get unflushed event count
   */
  getUnflushedCount(): number {
    this.ensureTable()

    const rows = [...this.sql.exec<{ total: number }>(
      `SELECT SUM(count) as total
       FROM ${this.options.tableName}
       WHERE flushed = 0`
    )]

    return rows[0]?.total ?? 0
  }

  /**
   * Get unflushed batch count
   */
  getUnflushedBatchCount(): number {
    this.ensureTable()

    const rows = [...this.sql.exec<{ count: number }>(
      `SELECT COUNT(*) as count
       FROM ${this.options.tableName}
       WHERE flushed = 0`
    )]

    return rows[0]?.count ?? 0
  }

  // ===========================================================================
  // Flush Management
  // ===========================================================================

  /**
   * Mark batches as flushed (after writing to R2)
   */
  markFlushed(batchIds: number[]): void {
    if (batchIds.length === 0) return

    this.ensureTable()

    // SQLite doesn't have a good way to do IN with parameters,
    // so we use a simple approach for small arrays
    const placeholders = batchIds.map(() => '?').join(',')
    this.sql.exec(
      `UPDATE ${this.options.tableName}
       SET flushed = 1
       WHERE id IN (${placeholders})`,
      ...batchIds
    )
  }

  /**
   * Delete flushed batches (for cleanup)
   */
  deleteFlushed(): number {
    this.ensureTable()

    const countResult = [...this.sql.exec<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${this.options.tableName} WHERE flushed = 1`
    )]
    const count = countResult[0]?.count ?? 0

    this.sql.exec(
      `DELETE FROM ${this.options.tableName} WHERE flushed = 1`
    )

    return count
  }

  /**
   * Delete all batches older than a timestamp
   */
  deleteOlderThan(timestamp: number): number {
    this.ensureTable()

    const countResult = [...this.sql.exec<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${this.options.tableName} WHERE max_ts < ?`,
      timestamp
    )]
    const count = countResult[0]?.count ?? 0

    this.sql.exec(
      `DELETE FROM ${this.options.tableName} WHERE max_ts < ?`,
      timestamp
    )

    return count
  }

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize an event batch to a blob
   * Uses JSON for now, can be upgraded to msgpack for better compression
   */
  private serializeBatch(batch: EventBatch): SerializedBatch {
    const json = JSON.stringify(batch.events)
    const data = new TextEncoder().encode(json)

    return {
      data,
      minTs: batch.minTs,
      maxTs: batch.maxTs,
      count: batch.count,
    }
  }

  /**
   * Deserialize a blob back to an event batch
   */
  private deserializeBatch(row: WalRow): EventBatch {
    // Handle various blob types that SQLite might return
    let data: Uint8Array
    const batch = row.batch as unknown

    if (batch instanceof Uint8Array) {
      data = batch
    } else if (batch instanceof ArrayBuffer) {
      data = new Uint8Array(batch)
    } else if (ArrayBuffer.isView(batch)) {
      data = new Uint8Array((batch as ArrayBufferView).buffer)
    } else {
      // Assume it's already a buffer-like object
      data = new Uint8Array(batch as ArrayBuffer)
    }

    const json = new TextDecoder().decode(data)
    let events: Event[]
    try {
      events = JSON.parse(json) as Event[]
    } catch {
      // Invalid JSON in event batch - return empty batch
      events = []
    }

    return {
      events,
      minTs: row.minTs,
      maxTs: row.maxTs,
      count: row.count,
      sizeBytes: data.length,
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a SqliteWal instance
 */
export function createSqliteWal(
  sql: SqliteInterface,
  options?: SqliteWalOptions
): SqliteWal {
  return new SqliteWal(sql, options)
}

/**
 * Create a flush handler for EventWriter that writes to SqliteWal
 */
export function createSqliteFlushHandler(wal: SqliteWal) {
  return async (batch: EventBatch): Promise<void> => {
    wal.writeBatch(batch)
  }
}

// Re-export for backward compatibility
export { isValidTableName } from '../utils/sql-security'
