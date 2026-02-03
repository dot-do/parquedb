/**
 * WAL-Only SQLite Schema Initialization
 *
 * Provides a cost-optimized schema mode for ParqueDB Durable Objects.
 *
 * In WAL-only mode, entity and relationship snapshot tables are NOT created.
 * Entity state is derived entirely from event replay via the events_wal table.
 * This reduces DO SQLite storage costs by 50-70% by eliminating per-entity
 * and per-relationship row storage.
 *
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

// =============================================================================
// Configuration
// =============================================================================

/**
 * Schema initialization options
 */
export interface WalOnlySchemaOptions {
  /**
   * Enable WAL-only mode.
   *
   * When true, entity and relationship snapshot tables are NOT created.
   * Entity state is derived entirely from event replay (events_wal).
   * Relationship traversal queries use the relationship WAL (rels_wal).
   *
   * This reduces DO SQLite storage costs by 50-70% by eliminating
   * per-entity and per-relationship row storage.
   *
   * @default false
   */
  walOnly?: boolean
}

// =============================================================================
// Schema Definitions
// =============================================================================

/**
 * Tables that are always created (both full and WAL-only modes)
 */
export const WAL_CORE_TABLES = {
  /** WAL table for event batching with namespace-based counters */
  events_wal: `
    CREATE TABLE IF NOT EXISTS events_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `,

  /** WAL table for relationship event batching */
  rels_wal: `
    CREATE TABLE IF NOT EXISTS rels_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `,

  /** Flush checkpoints */
  checkpoints: `
    CREATE TABLE IF NOT EXISTS checkpoints (
      id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      event_count INTEGER NOT NULL,
      first_event_id TEXT NOT NULL,
      last_event_id TEXT NOT NULL,
      parquet_path TEXT NOT NULL
    )
  `,

  /** Pending row groups table - tracks bulk writes to R2 pending files */
  pending_row_groups: `
    CREATE TABLE IF NOT EXISTS pending_row_groups (
      id TEXT PRIMARY KEY,
      ns TEXT NOT NULL,
      path TEXT NOT NULL,
      row_count INTEGER NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
  `,
} as const

/**
 * Tables that are ONLY created in full mode (skipped in WAL-only)
 */
export const SNAPSHOT_TABLES = {
  /** Entity metadata table (snapshot) */
  entities: `
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
  `,

  /** Relationships table with shredded fields (snapshot) */
  relationships: `
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
      match_mode TEXT,
      similarity REAL,
      data TEXT,
      PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
    )
  `,

  /** Legacy event_batches table */
  event_batches: `
    CREATE TABLE IF NOT EXISTS event_batches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      batch BLOB NOT NULL,
      min_ts INTEGER NOT NULL,
      max_ts INTEGER NOT NULL,
      event_count INTEGER NOT NULL,
      flushed INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `,

  /** Legacy events table */
  events: `
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
  `,
} as const

/**
 * Indexes that are always created
 */
export const WAL_CORE_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)',
  'CREATE INDEX IF NOT EXISTS idx_rels_wal_ns ON rels_wal(ns, last_seq)',
  'CREATE INDEX IF NOT EXISTS idx_pending_row_groups_ns ON pending_row_groups(ns, created_at)',
] as const

/**
 * Indexes only created in full mode (skipped in WAL-only)
 */
export const SNAPSHOT_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)',
  'CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)',
  'CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)',
  'CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)',
  'CREATE INDEX IF NOT EXISTS idx_relationships_match_mode ON relationships(match_mode) WHERE match_mode IS NOT NULL',
  'CREATE INDEX IF NOT EXISTS idx_relationships_similarity ON relationships(similarity) WHERE similarity IS NOT NULL',
  'CREATE INDEX IF NOT EXISTS idx_event_batches_flushed ON event_batches(flushed, min_ts)',
  'CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)',
  'CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)',
] as const

// =============================================================================
// Schema Initialization
// =============================================================================

/**
 * SQL interface compatible with both SqlStorage and mock implementations
 */
export interface SqlExec {
  exec(query: string, ...params: unknown[]): unknown
}

/**
 * Initialize the WAL-only SQLite schema.
 *
 * Creates only the WAL tables (events_wal, rels_wal, checkpoints, pending_row_groups)
 * without entity/relationship snapshot tables.
 *
 * Cost savings vs full schema:
 * - No entity rows (saves 1 row per entity)
 * - No relationship rows (saves 1 row per relationship)
 * - No legacy event_batches/events tables
 * - Entity state derived entirely from event replay
 *
 * @param sql - SqlStorage or compatible SQL interface
 */
export function initializeWalOnlySchema(sql: SqlExec): void {
  // Create only WAL core tables
  for (const tableSql of Object.values(WAL_CORE_TABLES)) {
    sql.exec(tableSql)
  }

  // Create only WAL core indexes
  for (const indexSql of WAL_CORE_INDEXES) {
    sql.exec(indexSql)
  }
}

/**
 * Initialize the full SQLite schema (entities + WAL).
 *
 * Creates all tables including entity/relationship snapshots.
 * This is the default mode for backward compatibility.
 *
 * @param sql - SqlStorage or compatible SQL interface
 */
export function initializeFullSchema(sql: SqlExec): void {
  // Create snapshot tables
  for (const tableSql of Object.values(SNAPSHOT_TABLES)) {
    sql.exec(tableSql)
  }

  // Create WAL core tables
  for (const tableSql of Object.values(WAL_CORE_TABLES)) {
    sql.exec(tableSql)
  }

  // Create all indexes
  for (const indexSql of SNAPSHOT_INDEXES) {
    sql.exec(indexSql)
  }
  for (const indexSql of WAL_CORE_INDEXES) {
    sql.exec(indexSql)
  }
}

/**
 * Initialize SQLite schema with configurable mode.
 *
 * @param sql - SqlStorage or compatible SQL interface
 * @param options - Schema options including walOnly flag
 */
export function initializeSchemaWithOptions(sql: SqlExec, options?: WalOnlySchemaOptions): void {
  if (options?.walOnly) {
    initializeWalOnlySchema(sql)
  } else {
    initializeFullSchema(sql)
  }
}

/**
 * Get table names that would be created for a given mode.
 * Useful for testing and diagnostics.
 *
 * @param walOnly - Whether WAL-only mode is enabled
 * @returns Array of table names
 */
export function getExpectedTables(walOnly: boolean): string[] {
  const coreTables = Object.keys(WAL_CORE_TABLES)
  if (walOnly) {
    return coreTables
  }
  return [...Object.keys(SNAPSHOT_TABLES), ...coreTables]
}

/**
 * Get index count that would be created for a given mode.
 * Useful for cost analysis.
 *
 * @param walOnly - Whether WAL-only mode is enabled
 * @returns Number of indexes
 */
export function getExpectedIndexCount(walOnly: boolean): number {
  if (walOnly) {
    return WAL_CORE_INDEXES.length
  }
  return SNAPSHOT_INDEXES.length + WAL_CORE_INDEXES.length
}
