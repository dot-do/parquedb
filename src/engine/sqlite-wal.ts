/**
 * SQLite WAL for MergeTree Engine (Cloudflare DO mode)
 *
 * Replaces JSONL rotation with SQLite-backed WAL for durable mutation buffering.
 * Each mutation is appended as a row. Rows are grouped into batches by table (kind).
 * Batches are flushed to Parquet during compaction, then cleaned up.
 *
 * Schema:
 *   wal(id INTEGER PK, ts INTEGER, kind TEXT, batch TEXT, row_count INTEGER, flushed INTEGER)
 *
 * - `kind` is the table/namespace name (e.g. "users", "posts", "rels")
 * - `batch` is a JSON array of line objects
 * - `flushed` tracks whether the batch has been compacted to Parquet
 */

// ---------------------------------------------------------------------------
// SqlStorage interface (matches Cloudflare DO ctx.storage.sql)
// ---------------------------------------------------------------------------

interface SqlStorageCursor {
  toArray(): unknown[]
}

interface SqlStorage {
  exec(query: string, ...bindings: unknown[]): SqlStorageCursor
}

// ---------------------------------------------------------------------------
// SqliteWal
// ---------------------------------------------------------------------------

export class SqliteWal {
  private sql: SqlStorage

  constructor(sql: SqlStorage) {
    this.sql = sql
    this.initSchema()
  }

  // -------------------------------------------------------------------------
  // Schema bootstrap
  // -------------------------------------------------------------------------

  private initSchema(): void {
    this.sql.exec(`CREATE TABLE IF NOT EXISTS wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      kind TEXT NOT NULL,
      batch TEXT NOT NULL,
      row_count INTEGER NOT NULL,
      flushed INTEGER NOT NULL DEFAULT 0
    )`)
  }

  // -------------------------------------------------------------------------
  // Append
  // -------------------------------------------------------------------------

  /** Append a single mutation line to the WAL for a given table. */
  append(table: string, line: Record<string, unknown>): void {
    const ts = Date.now()
    const batch = JSON.stringify([line])
    this.sql.exec(
      'INSERT INTO wal (ts, kind, batch, row_count, flushed) VALUES (?, ?, ?, ?, 0)',
      ts,
      table,
      batch,
      1,
    )
  }

  /** Append multiple lines as a single batch entry. */
  appendBatch(table: string, lines: Record<string, unknown>[]): void {
    if (lines.length === 0) return
    const ts = Date.now()
    const batch = JSON.stringify(lines)
    this.sql.exec(
      'INSERT INTO wal (ts, kind, batch, row_count, flushed) VALUES (?, ?, ?, ?, 0)',
      ts,
      table,
      batch,
      lines.length,
    )
  }

  // -------------------------------------------------------------------------
  // Read
  // -------------------------------------------------------------------------

  /** Get all unflushed batches for a specific table, ordered by insertion. */
  getBatches(table: string): Array<{ id: number; ts: number; batch: string; row_count: number }> {
    return this.sql
      .exec(
        'SELECT id, ts, batch, row_count FROM wal WHERE kind = ? AND flushed = 0 ORDER BY id',
        table,
      )
      .toArray() as Array<{ id: number; ts: number; batch: string; row_count: number }>
  }

  /** Get all unflushed batches across every table, ordered by insertion. */
  getAllBatches(): Array<{ id: number; ts: number; kind: string; batch: string; row_count: number }> {
    return this.sql
      .exec('SELECT id, ts, kind, batch, row_count FROM wal WHERE flushed = 0 ORDER BY id')
      .toArray() as Array<{ id: number; ts: number; kind: string; batch: string; row_count: number }>
  }

  // -------------------------------------------------------------------------
  // Flush / cleanup
  // -------------------------------------------------------------------------

  /** Mark specific batches as flushed (compacted to Parquet). */
  markFlushed(batchIds: number[]): void {
    if (batchIds.length === 0) return
    const BATCH_SIZE = 99 // Cloudflare DO SQLite max 100 params per statement
    for (let i = 0; i < batchIds.length; i += BATCH_SIZE) {
      const batch = batchIds.slice(i, i + BATCH_SIZE)
      const placeholders = batch.map(() => '?').join(',')
      this.sql.exec(`UPDATE wal SET flushed = 1 WHERE id IN (${placeholders})`, ...batch)
    }
  }

  /** Remove all flushed (already-compacted) batches from SQLite. */
  cleanup(): void {
    this.sql.exec('DELETE FROM wal WHERE flushed = 1')
  }

  // -------------------------------------------------------------------------
  // Counts / metadata
  // -------------------------------------------------------------------------

  /** Total unflushed row count across all tables. */
  getUnflushedCount(): number {
    const result = this.sql
      .exec('SELECT COALESCE(SUM(row_count), 0) as count FROM wal WHERE flushed = 0')
      .toArray() as Array<{ count: number }>
    return result[0]?.count ?? 0
  }

  /** Unflushed row count for a single table. */
  getUnflushedCountForTable(table: string): number {
    const result = this.sql
      .exec(
        'SELECT COALESCE(SUM(row_count), 0) as count FROM wal WHERE kind = ? AND flushed = 0',
        table,
      )
      .toArray() as Array<{ count: number }>
    return result[0]?.count ?? 0
  }

  /** List of tables that have unflushed data. */
  getUnflushedTables(): string[] {
    const result = this.sql
      .exec('SELECT DISTINCT kind FROM wal WHERE flushed = 0 ORDER BY kind')
      .toArray() as Array<{ kind: string }>
    return result.map((r) => r.kind)
  }

  // -------------------------------------------------------------------------
  // Replay (merge-on-read)
  // -------------------------------------------------------------------------

  /** Replay all unflushed lines for a table, in insertion order. */
  replayUnflushed<T extends Record<string, unknown> = Record<string, unknown>>(table: string): T[] {
    const batches = this.getBatches(table)
    const lines: T[] = []
    for (const batch of batches) {
      try {
        const parsed = JSON.parse(batch.batch) as T[]
        lines.push(...parsed)
      } catch {
        console.warn(`[sqlite-wal] Skipping corrupted WAL batch ${batch.id} for table '${table}': ${batch.batch.slice(0, 100)}`)
      }
    }
    return lines
  }
}
