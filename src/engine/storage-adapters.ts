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
 * The FullStorageAdapter interface unifies the three separate adapter interfaces
 * (StorageAdapter, RelStorageAdapter, EventStorageAdapter) used by the compactors,
 * so a single adapter instance can serve all compaction needs.
 */

import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { rotate, cleanup } from './rotation'
import { replay } from './jsonl-reader'
import { mergeResults } from './merge'
import type { DataLine, RelLine } from './types'

// Re-export the individual adapter interfaces from compactors for convenience
export type { StorageAdapter } from './compactor'
export type { RelStorageAdapter } from './compactor-rels'
export type { EventStorageAdapter } from './compactor-events'

// =============================================================================
// FullStorageAdapter — unified interface for data, rels, and events
// =============================================================================

/**
 * A unified storage adapter that handles data, relationships, and events.
 *
 * This interface combines the three separate adapter interfaces used by the
 * individual compactors (StorageAdapter, RelStorageAdapter, EventStorageAdapter)
 * into a single object, so one adapter can serve all compaction needs.
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
  readEvents(path: string): Promise<Record<string, unknown>[]>
  /** Write events to a compacted events file */
  writeEvents(path: string, data: Record<string, unknown>[]): Promise<void>
}

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
    } catch {
      return []
    }
  }

  async writeData(path: string, data: DataLine[]): Promise<void> {
    await writeFile(path, JSON.stringify(data))
  }

  async readRels(path: string): Promise<RelLine[]> {
    try {
      return JSON.parse(await readFile(path, 'utf-8'))
    } catch {
      return []
    }
  }

  async writeRels(path: string, data: RelLine[]): Promise<void> {
    await writeFile(path, JSON.stringify(data))
  }

  async readEvents(path: string): Promise<Record<string, unknown>[]> {
    try {
      return JSON.parse(await readFile(path, 'utf-8'))
    } catch {
      return []
    }
  }

  async writeEvents(path: string, data: Record<string, unknown>[]): Promise<void> {
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

  async readEvents(path: string): Promise<Record<string, unknown>[]> {
    return (this.store.get(path) as Record<string, unknown>[] | undefined) ?? []
  }

  async writeEvents(path: string, data: Record<string, unknown>[]): Promise<void> {
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
// but handle the .tmp -> final rename within the storage adapter instead
// of requiring a local filesystem rename. This makes them work with both
// local and remote (in-memory) storage adapters.

/**
 * Compact a data table using a hybrid-aware storage adapter.
 *
 * Unlike compactDataTable in compactor.ts which uses fs.rename for atomic swap,
 * this function writes directly to the final path via the adapter, avoiding
 * the need for local filesystem operations on the compacted data.
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

  // Step 1: Rotate the JSONL file (local disk operation)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  // Step 2: Read existing data from remote storage
  const existing = await storage.readData(dataPath)

  // Step 3: Read the rotated JSONL file (local disk)
  const jsonlData = await replay<DataLine>(compactingPath)

  // Step 4: Merge using ReplacingMergeTree semantics
  const merged = mergeResults(existing, jsonlData)

  // Step 5: Write directly to the final path (no .tmp rename needed)
  await storage.writeData(dataPath, merged)

  // Step 6: Cleanup the compacting file (local disk)
  await cleanup(compactingPath)

  return merged.length
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

  // Rotate JSONL (local disk)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  // Read existing rels from remote storage
  const existing = await storage.readRels(parquetPath)

  // Read rotated JSONL (local disk)
  const mutations = await replay<RelLine>(compactingPath)

  if (mutations.length === 0) {
    await cleanup(compactingPath)
    return null
  }

  // Build Map keyed by f:p:t, overlay mutations
  const merged = new Map<string, RelLine>()
  for (const rel of existing) {
    merged.set(`${rel.f}:${rel.p}:${rel.t}`, rel)
  }
  for (const mutation of mutations) {
    merged.set(`${mutation.f}:${mutation.p}:${mutation.t}`, mutation)
  }

  // Filter out tombstones, sort by (f, p, t)
  const live: RelLine[] = []
  for (const rel of merged.values()) {
    if (rel.$op === 'l') {
      live.push(rel)
    }
  }
  live.sort((a, b) => {
    if (a.f < b.f) return -1
    if (a.f > b.f) return 1
    if (a.p < b.p) return -1
    if (a.p > b.p) return 1
    if (a.t < b.t) return -1
    if (a.t > b.t) return 1
    return 0
  })

  // Write directly to final path (no .tmp rename)
  await storage.writeRels(parquetPath, live)

  // Cleanup
  await cleanup(compactingPath)

  return live.length
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

  // Rotate JSONL (local disk)
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  // Read existing events from remote storage
  const existing = await storage.readEvents(compactedPath)

  // Read rotated JSONL (local disk)
  const newEvents = await replay<Record<string, unknown>>(compactingPath)

  if (newEvents.length === 0 && existing.length === 0) {
    await cleanup(compactingPath)
    return null
  }

  // Concatenate and sort by ts
  const all = [...existing, ...newEvents]
  all.sort((a, b) => {
    const tsA = typeof a.ts === 'number' ? a.ts : 0
    const tsB = typeof b.ts === 'number' ? b.ts : 0
    return tsA - tsB
  })

  // Write directly to final path (no .tmp rename)
  await storage.writeEvents(compactedPath, all)

  // Cleanup
  await cleanup(compactingPath)

  return all.length
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
