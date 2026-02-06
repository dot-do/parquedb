/**
 * Storage Adapters for ParqueDB's Hybrid MergeTree Engine
 *
 * Provides pluggable storage backends for compacted data (Parquet/JSON files).
 * The hybrid architecture keeps JSONL writes local (fast appends) while routing
 * compacted data through a configurable storage adapter.
 *
 * Two adapters are provided:
 * - LocalStorageAdapter: reads/writes JSON files on local disk (default)
 * - MemoryStorageAdapter: in-memory Map (simulates R2/S3 for testing)
 *
 * FullStorageAdapter is the primary (canonical) interface for storage adapters.
 * The three narrower types (StorageAdapter, RelStorageAdapter, EventStorageAdapter)
 * are Pick<> aliases for backward compatibility, so callers that only need a
 * subset of the methods can still declare a narrow parameter type.
 */

import { readFile, writeFile, rename } from 'node:fs/promises'
import { join } from 'node:path'
import { rotate, cleanup } from './rotation'
import { replay } from './jsonl-reader'
import { mergeResults } from './merge'
import type { DataLine, RelLine } from './types'
import { mergeRelationships } from './merge-rels'
import { mergeEvents } from './merge-events'
import type { AnyEventLine } from './merge-events'

// =============================================================================
// FullStorageAdapter — unified interface for data, rels, and events
// =============================================================================

/**
 * A unified storage adapter that handles data, relationships, and events.
 *
 * This is the primary interface for all storage adapters. Individual compactors
 * accept Pick<> subsets of this interface so that callers can provide either a
 * full adapter or a narrow one with only the relevant methods.
 */
export interface FullStorageAdapter {
  /** Read entities from a data file */
  readData(path: string): Promise<DataLine[]>
  /** Write entities to a data file */
  writeData(path: string, data: DataLine[]): Promise<void>
  /** Read relationships from a compacted rels file */
  readRels(path: string): Promise<RelLine[]>
  /** Write relationships to a compacted rels file */
  writeRels(path: string, data: RelLine[]): Promise<void>
  /** Read events from a compacted events file */
  readEvents(path: string): Promise<AnyEventLine[]>
  /** Write events to a compacted events file */
  writeEvents(path: string, data: AnyEventLine[]): Promise<void>
  /**
   * Atomically rename a path (optional).
   * If provided, compaction uses this for the .tmp -> final rename.
   * If not provided, falls back to fs.rename (local disk).
   */
  rename?(fromPath: string, toPath: string): void | Promise<void>
}

// =============================================================================
// Narrow type aliases (Pick<> subsets of FullStorageAdapter)
// =============================================================================

/** Storage adapter for data compaction (readData + writeData). */
export type StorageAdapter = Pick<FullStorageAdapter, 'readData' | 'writeData'>

/** Storage adapter for relationship compaction (readRels + writeRels). */
export type RelStorageAdapter = Pick<FullStorageAdapter, 'readRels' | 'writeRels'>

/** Storage adapter for event compaction (readEvents + writeEvents). */
export type EventStorageAdapter = Pick<FullStorageAdapter, 'readEvents' | 'writeEvents'>

// =============================================================================
// LocalStorageAdapter — reads/writes JSON files on local disk
// =============================================================================

/**
 * Storage adapter that reads/writes JSON files on local disk.
 *
 * This is the default adapter used when no remote storage is configured.
 * Each method serializes data as JSON and writes to the specified path,
 * or deserializes from a JSON file at the specified path.
 *
 * Missing files return empty arrays (no errors), matching the behavior
 * expected by the compactors for first-time compaction.
 */
export class LocalStorageAdapter implements FullStorageAdapter {
  async readData(path: string): Promise<DataLine[]> {
    try {
      return JSON.parse(await readFile(path, 'utf-8'))
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
    }
  }

  async writeData(path: string, data: DataLine[]): Promise<void> {
    await writeFile(path, JSON.stringify(data))
  }

  async readRels(path: string): Promise<RelLine[]> {
    try {
      return JSON.parse(await readFile(path, 'utf-8'))
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
    }
  }

  async writeRels(path: string, data: RelLine[]): Promise<void> {
    await writeFile(path, JSON.stringify(data))
  }

  async readEvents(path: string): Promise<AnyEventLine[]> {
    try {
      return JSON.parse(await readFile(path, 'utf-8'))
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
    }
  }

  async writeEvents(path: string, data: AnyEventLine[]): Promise<void> {
    await writeFile(path, JSON.stringify(data))
  }
}

// =============================================================================
// MemoryStorageAdapter — in-memory Map (simulates R2/S3 for testing)
// =============================================================================

/**
 * In-memory storage adapter that simulates remote object storage (R2/S3).
 *
 * Data is stored in a Map keyed by path string. This adapter is intended
 * for testing the hybrid storage mode without requiring actual remote
 * storage infrastructure.
 *
 * Test helper methods:
 * - has(path): check if a path has stored data
 * - clear(): remove all stored data
 */
export class MemoryStorageAdapter implements FullStorageAdapter {
  private readonly store = new Map<string, unknown[]>()

  // --- Data operations ---

  async readData(path: string): Promise<DataLine[]> {
    return (this.store.get(path) as DataLine[] | undefined) ?? []
  }

  async writeData(path: string, data: DataLine[]): Promise<void> {
    this.store.set(path, data)
  }

  // --- Rel operations ---

  async readRels(path: string): Promise<RelLine[]> {
    return (this.store.get(path) as RelLine[] | undefined) ?? []
  }

  async writeRels(path: string, data: RelLine[]): Promise<void> {
    this.store.set(path, data)
  }

  // --- Event operations ---

  async readEvents(path: string): Promise<AnyEventLine[]> {
    return (this.store.get(path) as AnyEventLine[] | undefined) ?? []
  }

  async writeEvents(path: string, data: AnyEventLine[]): Promise<void> {
    this.store.set(path, data)
  }

  // --- Test helpers ---

  /**
   * Check if data exists at the given path.
   */
  has(path: string): boolean {
    return this.store.has(path)
  }

  /**
   * Remove all stored data.
   */
  clear(): void {
    this.store.clear()
  }

  /**
   * Atomically rename a path in the store (simulates file rename).
   * Used by hybrid compaction to handle the .tmp -> final rename step
   * that normally happens on local disk.
   */
  rename(fromPath: string, toPath: string): void {
    const data = this.store.get(fromPath)
    if (data !== undefined) {
      this.store.set(toPath, data)
      this.store.delete(fromPath)
    }
  }
}

// =============================================================================
// Hybrid Compaction Functions
// =============================================================================

// These functions perform the same compaction as the existing compactors,
// but write to a .tmp file first and then atomically rename, using the
// adapter's rename method if available, or falling back to fs.rename.

/**
 * Atomically rename a file using the adapter's rename if available,
 * otherwise fall back to fs.rename (local disk).
 */
async function atomicRename(
  storage: FullStorageAdapter,
  fromPath: string,
  toPath: string,
): Promise<void> {
  if (typeof storage.rename === 'function') {
    await storage.rename(fromPath, toPath)
  } else {
    await rename(fromPath, toPath)
  }
}

/**
 * Compact a data table using a hybrid-aware storage adapter.
 *
 * Writes to a temporary file first, then renames atomically to avoid
 * leaving a corrupted data file on failure. The .compacting file is
 * preserved on error for recovery.
 *
 * JSONL rotation and cleanup still happen on local disk (the JSONL layer
 * is always local in hybrid mode).
 *
 * @param dataDir - Local directory containing the JSONL files
 * @param table - Table name
 * @param storage - Storage adapter for reading/writing compacted data
 * @returns Count of entities in the compacted output, or null if nothing to compact
 */
export async function hybridCompactData(
  dataDir: string,
  table: string,
  storage: FullStorageAdapter,
): Promise<number | null> {
  const jsonlPath = join(dataDir, `${table}.jsonl`)
  const dataPath = join(dataDir, `${table}.parquet`)
  const tmpPath = dataPath + '.tmp'

  // Step 1: Rotate the JSONL file (local disk operation)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  try {
    // Step 2: Read existing data from remote storage
    const existing = await storage.readData(dataPath)

    // Step 3: Read the rotated JSONL file (local disk)
    const jsonlData = await replay<DataLine>(compactingPath)

    // Step 4: Merge using ReplacingMergeTree semantics
    const merged = mergeResults(existing, jsonlData)

    // Step 5: Write to a temporary file for atomicity
    await storage.writeData(tmpPath, merged)

    // Step 6: Atomic rename: .tmp -> .parquet
    await atomicRename(storage, tmpPath, dataPath)

    // Step 7: Cleanup the compacting file (local disk)
    await cleanup(compactingPath)

    return merged.length
  } catch (error) {
    // On failure, leave the .compacting file for recovery
    throw error
  }
}

/**
 * Compact relationships using a hybrid-aware storage adapter.
 *
 * Same as compactRelationships but writes directly via the adapter.
 *
 * @param dataDir - Local directory containing rels.jsonl
 * @param storage - Storage adapter for reading/writing compacted rels
 * @returns Count of live relationships, or null if nothing to compact
 */
export async function hybridCompactRels(
  dataDir: string,
  storage: FullStorageAdapter,
): Promise<number | null> {
  const jsonlPath = join(dataDir, 'rels.jsonl')
  const parquetPath = join(dataDir, 'rels.parquet')
  const tmpPath = parquetPath + '.tmp'

  // Rotate JSONL (local disk)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  try {
    // Read existing rels from remote storage
    const existing = await storage.readRels(parquetPath)

    // Read rotated JSONL (local disk)
    const mutations = await replay<RelLine>(compactingPath)

    if (mutations.length === 0) {
      await cleanup(compactingPath)
      return null
    }

    // Merge using shared logic: dedup by f:p:t, $ts wins, filter tombstones, sort
    const live = mergeRelationships(existing, mutations)

    // Write to tmp file, then atomic rename
    await storage.writeRels(tmpPath, live)
    await atomicRename(storage, tmpPath, parquetPath)

    // Cleanup
    await cleanup(compactingPath)

    return live.length
  } catch (error) {
    // On failure, leave the .compacting file for recovery
    throw error
  }
}

/**
 * Compact events using a hybrid-aware storage adapter.
 *
 * Same as compactEvents but writes directly via the adapter.
 *
 * @param dataDir - Local directory containing events.jsonl
 * @param storage - Storage adapter for reading/writing compacted events
 * @returns Total event count, or null if nothing to compact
 */
export async function hybridCompactEvents(
  dataDir: string,
  storage: FullStorageAdapter,
): Promise<number | null> {
  const jsonlPath = join(dataDir, 'events.jsonl')
  const compactedPath = join(dataDir, 'events.compacted')
  const tmpPath = compactedPath + '.tmp'

  // Rotate JSONL (local disk)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  try {
    // Read existing events from remote storage
    const existing = await storage.readEvents(compactedPath)

    // Read rotated JSONL (local disk)
    const newEvents = await replay<AnyEventLine>(compactingPath)

    if (newEvents.length === 0 && existing.length === 0) {
      await cleanup(compactingPath)
      return null
    }

    // Merge using shared logic: concatenate and sort by ts
    const all = mergeEvents(existing, newEvents)

    // Write to tmp file, then atomic rename
    await storage.writeEvents(tmpPath, all)
    await atomicRename(storage, tmpPath, compactedPath)

    // Cleanup
    await cleanup(compactingPath)

    return all.length
  } catch (error) {
    // On failure, leave the .compacting file for recovery
    throw error
  }
}

/**
 * Compact all tables (data, rels, events) using a hybrid-aware storage adapter.
 *
 * Convenience function that runs all three hybrid compaction steps.
 *
 * @param dataDir - Local directory containing JSONL files
 * @param tables - List of data table names to compact
 * @param storage - Storage adapter for reading/writing compacted files
 */
export async function hybridCompactAll(
  dataDir: string,
  tables: string[],
  storage: FullStorageAdapter,
): Promise<{
  data: Map<string, number | null>
  rels: number | null
  events: number | null
}> {
  const dataResults = new Map<string, number | null>()

  for (const table of tables) {
    const count = await hybridCompactData(dataDir, table, storage)
    dataResults.set(table, count)
  }

  const relsCount = await hybridCompactRels(dataDir, storage)
  const eventsCount = await hybridCompactEvents(dataDir, storage)

  return {
    data: dataResults,
    rels: relsCount,
    events: eventsCount,
  }
}
