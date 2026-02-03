/**
 * SQLite Schema Initialization
 *
 * Creates and maintains the SQLite schema for ParqueDB Durable Objects.
 */

/**
 * Initialize all SQLite tables and indexes
 *
 * Creates:
 * - entities: Entity storage table
 * - relationships: Relationship storage with shredded fields
 * - events_wal: Entity event WAL batching
 * - rels_wal: Relationship event WAL batching
 * - event_batches: Legacy event batching (kept for compatibility)
 * - events: Legacy events table (kept for migration)
 * - checkpoints: Parquet flush checkpoints
 * - pending_row_groups: Bulk write tracking
 *
 * @param sql - SqlStorage instance from DO context
 */
export function initializeSchema(sql: SqlStorage): void {
  // Create entities table
  sql.exec(`
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

  // Create relationships table with shredded fields
  sql.exec(`
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
      -- Shredded fields (top-level columns for efficient querying)
      match_mode TEXT,           -- 'exact' or 'fuzzy'
      similarity REAL,           -- 0.0 to 1.0 for fuzzy matches
      -- Remaining metadata in Variant
      data TEXT,
      PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
    )
  `)

  // Add indexes for shredded fields
  sql.exec(`
    CREATE INDEX IF NOT EXISTS idx_relationships_match_mode
    ON relationships(match_mode) WHERE match_mode IS NOT NULL
  `)

  sql.exec(`
    CREATE INDEX IF NOT EXISTS idx_relationships_similarity
    ON relationships(similarity) WHERE similarity IS NOT NULL
  `)

  // Events WAL table for namespace-based batching
  sql.exec(`
    CREATE TABLE IF NOT EXISTS events_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `)

  // Relationship WAL table for batching
  sql.exec(`
    CREATE TABLE IF NOT EXISTS rels_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `)

  // Legacy event_batches table - kept for backward compatibility
  sql.exec(`
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

  // Legacy events table - kept for backward compatibility during migration
  sql.exec(`
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      ts TEXT NOT NULL,
      target TEXT NOT NULL,
      op TEXT NOT NULL,
      ns TEXT,
      entity_id TEXT,
      before TEXT,
      after TEXT,
      actor TEXT NOT NULL,
      metadata TEXT,
      flushed INTEGER NOT NULL DEFAULT 0
    )
  `)

  // Checkpoints table
  sql.exec(`
    CREATE TABLE IF NOT EXISTS checkpoints (
      id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      event_count INTEGER NOT NULL,
      first_event_id TEXT NOT NULL,
      last_event_id TEXT NOT NULL,
      parquet_path TEXT NOT NULL
    )
  `)

  // Pending row groups table - tracks bulk writes to R2
  sql.exec(`
    CREATE TABLE IF NOT EXISTS pending_row_groups (
      id TEXT PRIMARY KEY,
      ns TEXT NOT NULL,
      path TEXT NOT NULL,
      row_count INTEGER NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
  `)

  // Create indexes
  sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_wal_ns ON rels_wal(ns, last_seq)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_event_batches_flushed ON event_batches(flushed, min_ts)')
  // Legacy indexes kept for backward compatibility
  sql.exec('CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)')
  sql.exec('CREATE INDEX IF NOT EXISTS idx_pending_row_groups_ns ON pending_row_groups(ns, created_at)')
}

/**
 * Initialize namespace counters from WAL tables
 *
 * On DO startup, loads the max sequence for each namespace from events_wal and rels_wal.
 *
 * @param sql - SqlStorage instance
 * @returns Map of namespace to next sequence number
 */
export function initializeCounters(sql: SqlStorage): Map<string, number> {
  const counters = new Map<string, number>()

  interface CounterRow {
    [key: string]: SqlStorageValue
    ns: string
    max_seq: number
  }

  // Get max sequence for each namespace from events_wal
  const rows = [...sql.exec<CounterRow>(
    `SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`
  )]

  for (const row of rows) {
    // Next ID starts after max_seq
    counters.set(row.ns, row.max_seq + 1)
  }

  // Also initialize from rels_wal for relationship sequences
  // Use separate counter namespace to avoid conflicts
  const relRows = [...sql.exec<CounterRow>(
    `SELECT ns, MAX(last_seq) as max_seq FROM rels_wal GROUP BY ns`
  )]

  for (const row of relRows) {
    // Store relationship counters with 'rel:' prefix to avoid conflicts
    const relCounterKey = `rel:${row.ns}`
    counters.set(relCounterKey, row.max_seq + 1)
  }

  return counters
}
