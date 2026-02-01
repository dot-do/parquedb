/**
 * Events module for ParqueDB
 *
 * Provides CDC (Change Data Capture) and time-travel functionality.
 * Events are the source of truth; data.parquet and rels.parquet are
 * materialized views that can be reconstructed from the events log.
 */

export * from './types'
export * from './writer'
export * from './sqlite-wal'
export * from './segment'
export * from './manifest'
export * from './replay'
export * from './compaction'
