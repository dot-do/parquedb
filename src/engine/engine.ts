/**
 * ParqueEngine - Core MergeTree Engine for ParqueDB
 *
 * Coordinates entity writes across three layers:
 * 1. JSONL append-only files ({dataDir}/{table}.jsonl) for durable storage
 * 2. Events JSONL ({dataDir}/events.jsonl) for CDC/audit trail
 * 3. In-memory TableBuffer per table for fast reads after writes
 *
 * Design decisions:
 * - Writers are lazily created per table (first write to a table creates the writer)
 * - One shared event writer across all tables
 * - IDs are ULID-like: time-sortable, unique, lexicographically ordered
 * - Updates store full entity state (not deltas) for merge-on-read simplicity
 * - Deletes write tombstone DataLines ($op: 'd') with no entity data fields
 */

import { join } from 'node:path'
import { readdir, unlink } from 'node:fs/promises'
import { TableBuffer } from './buffer'
import type { ScanFilter } from './buffer'
import { getNestedValue } from './filter'
import { JsonlWriter } from './jsonl-writer'
import { replay, replayInto } from './jsonl-reader'
import { compactDataTable, shouldCompact as shouldCompactData } from './compactor'
import type { StorageAdapter, CompactOptions } from './compactor'
import { needsRecovery, getCompactingPath } from './rotation'
import { mergeResults } from './merge'
import { ParquetStorageAdapter } from './parquet-adapter'
import type { DataLine, EventLine } from './types'
import { DATA_SYSTEM_FIELDS } from './utils'

// =============================================================================
// Configuration
// =============================================================================

export interface AutoCompactOptions {
  /** Compact when any table's JSONL exceeds this many lines */
  maxLines?: number
  /** Compact when any table's JSONL exceeds this many bytes */
  maxBytes?: number
}

export interface EngineConfig {
  dataDir: string
  /** Optional threshold-based auto-compaction */
  autoCompact?: AutoCompactOptions
}

// =============================================================================
// Update operators
// =============================================================================

export interface UpdateOps {
  $set?: Record<string, unknown>
  $inc?: Record<string, number>
  $unset?: Record<string, boolean>
}

// =============================================================================
// Find options
// =============================================================================

export interface FindOptions {
  limit?: number
  skip?: number
  sort?: Record<string, 1 | -1>
}

// =============================================================================
// System field prefix — used to separate system fields from entity data
// =============================================================================

/**
 * Extract only the user-data fields from a DataLine (exclude $id, $op, $v, $ts).
 */
function extractData(entity: DataLine): Record<string, unknown> {
  const data: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(entity)) {
    if (!DATA_SYSTEM_FIELDS.has(key)) {
      data[key] = value
    }
  }
  return data
}

// =============================================================================
// Sort helpers
// =============================================================================

/**
 * Sort entities by the given sort specification.
 *
 * The sort object maps field paths to 1 (ascending) or -1 (descending).
 * Multiple fields are used as tiebreakers in insertion order.
 */
function sortEntities(entities: DataLine[], sort: Record<string, 1 | -1>): DataLine[] {
  const sortKeys = Object.entries(sort)
  return [...entities].sort((a, b) => {
    for (const [field, direction] of sortKeys) {
      const aVal = getNestedValue(a as unknown as Record<string, unknown>, field)
      const bVal = getNestedValue(b as unknown as Record<string, unknown>, field)

      let cmp = 0
      if (aVal === bVal) {
        cmp = 0
      } else if (aVal === undefined || aVal === null) {
        cmp = -1
      } else if (bVal === undefined || bVal === null) {
        cmp = 1
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      } else {
        cmp = String(aVal) < String(bVal) ? -1 : 1
      }

      if (cmp !== 0) return cmp * direction
    }
    return 0
  })
}

// =============================================================================
// ParqueEngine
// =============================================================================

export class ParqueEngine {
  private readonly dataDir: string
  private readonly buffers: Map<string, TableBuffer> = new Map()
  private readonly dataWriters: Map<string, JsonlWriter> = new Map()
  private eventWriter: JsonlWriter | null = null
  private readonly autoCompact?: AutoCompactOptions

  /** Track write counts per table for auto-compaction threshold checks */
  private readonly writeCounters: Map<string, number> = new Map()
  /** Track known table names (discovered during init or writes) */
  private readonly knownTables: Set<string> = new Set()

  /** Last timestamp used in ID generation (for monotonic counter) */
  private lastIdTs = 0
  /** Monotonic counter within the same millisecond */
  private idCounter = 0

  constructor(config: EngineConfig) {
    this.dataDir = config.dataDir
    this.autoCompact = config.autoCompact
  }

  // ---------------------------------------------------------------------------
  // Create
  // ---------------------------------------------------------------------------

  /**
   * Create a single entity in the given table.
   *
   * If `data.$id` is provided, it is used as the entity ID; otherwise
   * a new time-sortable ID is generated.
   *
   * @returns The full DataLine written to disk and buffer.
   */
  async create(table: string, data: Record<string, unknown>): Promise<DataLine> {
    const id = typeof data.$id === 'string' ? data.$id : this.generateId()
    const ts = Date.now()

    // Build the DataLine (full entity state)
    const { $id: _discardId, ...rest } = data
    const dataLine: DataLine = {
      $id: id,
      $op: 'c',
      $v: 1,
      $ts: ts,
      ...rest,
    }

    // Build the EventLine
    const eventLine: EventLine = {
      id: this.generateId(),
      ts,
      op: 'c',
      ns: table,
      eid: id,
      after: extractData(dataLine),
    }

    // Write to JSONL files
    const dataWriter = this.getDataWriter(table)
    const evtWriter = this.getEventWriter()
    await dataWriter.append(dataLine as unknown as Record<string, unknown>)
    await evtWriter.append(eventLine as unknown as Record<string, unknown>)

    // Update in-memory buffer
    this.getBuffer(table).set(dataLine)

    // Track table and check auto-compact
    this.knownTables.add(table)
    await this.trackWrite(table, 1)

    return dataLine
  }

  // ---------------------------------------------------------------------------
  // createMany
  // ---------------------------------------------------------------------------

  /**
   * Create multiple entities in a single batch write.
   *
   * All DataLines are written in one appendBatch call, and all EventLines
   * in another appendBatch call, for efficiency and atomicity.
   */
  async createMany(table: string, items: Record<string, unknown>[]): Promise<DataLine[]> {
    const ts = Date.now()
    const dataLines: DataLine[] = []
    const eventLines: EventLine[] = []

    for (const data of items) {
      const id = typeof data.$id === 'string' ? data.$id : this.generateId()
      const { $id: _discardId, ...rest } = data

      const dataLine: DataLine = {
        $id: id,
        $op: 'c',
        $v: 1,
        $ts: ts,
        ...rest,
      }

      const eventLine: EventLine = {
        id: this.generateId(),
        ts,
        op: 'c',
        ns: table,
        eid: id,
        after: extractData(dataLine),
      }

      dataLines.push(dataLine)
      eventLines.push(eventLine)
    }

    // Batch writes
    const dataWriter = this.getDataWriter(table)
    const evtWriter = this.getEventWriter()
    await dataWriter.appendBatch(dataLines as unknown as Record<string, unknown>[])
    await evtWriter.appendBatch(eventLines as unknown as Record<string, unknown>[])

    // Update in-memory buffers
    const buffer = this.getBuffer(table)
    for (const dataLine of dataLines) {
      buffer.set(dataLine)
    }

    // Track table and check auto-compact
    this.knownTables.add(table)
    await this.trackWrite(table, dataLines.length)

    return dataLines
  }

  // ---------------------------------------------------------------------------
  // Update
  // ---------------------------------------------------------------------------

  /**
   * Update an existing entity by applying update operators.
   *
   * Supported operators:
   * - `$set`: merge fields into the entity
   * - `$inc`: increment numeric fields (defaults to 0 if missing)
   * - `$unset`: remove fields from the entity
   *
   * The updated DataLine contains the **full entity state** (not just the patch),
   * which simplifies merge-on-read.
   *
   * @throws Error if the entity does not exist or is a tombstone.
   */
  async update(table: string, id: string, ops: UpdateOps): Promise<DataLine> {
    const buffer = this.getBuffer(table)
    const existing = buffer.get(id)

    if (!existing || existing.$op === 'd') {
      throw new Error(`Entity not found: ${table}/${id}`)
    }

    const ts = Date.now()
    const beforeData = extractData(existing)

    // Apply update operators to produce new entity state
    const updated = this.applyUpdate(existing, ops)
    updated.$op = 'u'
    updated.$v = existing.$v + 1
    updated.$ts = ts

    const afterData = extractData(updated)

    // Build EventLine
    const eventLine: EventLine = {
      id: this.generateId(),
      ts,
      op: 'u',
      ns: table,
      eid: id,
      before: beforeData,
      after: afterData,
    }

    // Write to JSONL files
    const dataWriter = this.getDataWriter(table)
    const evtWriter = this.getEventWriter()
    await dataWriter.append(updated as unknown as Record<string, unknown>)
    await evtWriter.append(eventLine as unknown as Record<string, unknown>)

    // Update in-memory buffer
    buffer.set(updated)

    // Track auto-compact
    await this.trackWrite(table, 1)

    return updated
  }

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  /**
   * Delete an entity by writing a tombstone.
   *
   * The tombstone DataLine contains only system fields ($id, $op, $v, $ts),
   * no entity data fields.
   *
   * @throws Error if the entity does not exist or is already a tombstone.
   */
  async delete(table: string, id: string): Promise<void> {
    const buffer = this.getBuffer(table)
    const existing = buffer.get(id)

    if (!existing || existing.$op === 'd') {
      throw new Error(`Entity not found: ${table}/${id}`)
    }

    const ts = Date.now()
    const beforeData = extractData(existing)
    const newVersion = existing.$v + 1

    // Tombstone DataLine — only system fields
    const dataLine: DataLine = {
      $id: id,
      $op: 'd',
      $v: newVersion,
      $ts: ts,
    }

    // EventLine with before state
    const eventLine: EventLine = {
      id: this.generateId(),
      ts,
      op: 'd',
      ns: table,
      eid: id,
      before: beforeData,
    }

    // Write to JSONL files
    const dataWriter = this.getDataWriter(table)
    const evtWriter = this.getEventWriter()
    await dataWriter.append(dataLine as unknown as Record<string, unknown>)
    await evtWriter.append(eventLine as unknown as Record<string, unknown>)

    // Update in-memory buffer with tombstone
    buffer.delete(id, newVersion, ts)

    // Track auto-compact
    await this.trackWrite(table, 1)
  }

  // ---------------------------------------------------------------------------
  // Read operations
  // ---------------------------------------------------------------------------

  /**
   * Find entities in a table matching an optional filter, with sort/skip/limit.
   *
   * The filter supports all comparison operators from TableBuffer.scan(), plus
   * logical operators $or and $and:
   * - `{ $or: [filter1, filter2] }` — matches if any sub-filter matches
   * - `{ $and: [filter1, filter2] }` — matches if all sub-filters match
   *
   * @returns Matching entities (empty array if table doesn't exist or no matches).
   */
  async find(
    table: string,
    filter?: Record<string, unknown>,
    options?: FindOptions,
  ): Promise<DataLine[]> {
    const buffer = this.buffers.get(table)
    if (!buffer) return []

    // The shared matchesFilter handles all operators including $or/$and,
    // so we always pass the filter through to buffer.scan() which delegates
    // to the unified filter module.
    let results = buffer.scan(filter as ScanFilter | undefined)

    // Apply sort
    if (options?.sort) {
      results = sortEntities(results, options.sort)
    }

    // Apply skip
    if (options?.skip) {
      results = results.slice(options.skip)
    }

    // Apply limit
    if (options?.limit !== undefined) {
      results = results.slice(0, options.limit)
    }

    return results
  }

  /**
   * Find a single entity matching the filter.
   *
   * @returns The first matching entity, or null if none match.
   */
  async findOne(
    table: string,
    filter?: Record<string, unknown>,
  ): Promise<DataLine | null> {
    const results = await this.find(table, filter, { limit: 1 })
    return results.length > 0 ? results[0] : null
  }

  /**
   * Count entities in a table matching an optional filter.
   *
   * Optimized: counts matching entities directly in the buffer without
   * materializing them into an array. Skips sort/skip/limit since only
   * the total count of matching entities is needed.
   *
   * @returns The number of matching entities (0 if table doesn't exist).
   */
  async count(
    table: string,
    filter?: Record<string, unknown>,
  ): Promise<number> {
    const buffer = this.buffers.get(table)
    if (!buffer) return 0

    return buffer.count(filter as ScanFilter | undefined)
  }

  // ---------------------------------------------------------------------------
  // Buffer access
  // ---------------------------------------------------------------------------

  /**
   * Get the in-memory TableBuffer for a table, creating one if it doesn't exist.
   */
  getBuffer(table: string): TableBuffer {
    let buffer = this.buffers.get(table)
    if (!buffer) {
      buffer = new TableBuffer()
      this.buffers.set(table, buffer)
    }
    return buffer
  }

  // ---------------------------------------------------------------------------
  // Public Accessors
  // ---------------------------------------------------------------------------

  /** List of all known table names */
  get tables(): string[] {
    return Array.from(this.knownTables)
  }

  /** The data directory path */
  get dir(): string {
    return this.dataDir
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Close all writers. After close(), the engine should not be used.
   */
  async close(): Promise<void> {
    const closePromises: Promise<void>[] = []

    for (const writer of this.dataWriters.values()) {
      closePromises.push(writer.close())
    }
    if (this.eventWriter) {
      closePromises.push(this.eventWriter.close())
    }

    await Promise.all(closePromises)

    this.dataWriters.clear()
    this.eventWriter = null
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Get or create the JsonlWriter for a table's data file.
   */
  private getDataWriter(table: string): JsonlWriter {
    let writer = this.dataWriters.get(table)
    if (!writer) {
      writer = new JsonlWriter(join(this.dataDir, `${table}.jsonl`))
      this.dataWriters.set(table, writer)
    }
    return writer
  }

  /**
   * Get or create the shared events JsonlWriter.
   */
  private getEventWriter(): JsonlWriter {
    if (!this.eventWriter) {
      this.eventWriter = new JsonlWriter(join(this.dataDir, 'events.jsonl'))
    }
    return this.eventWriter
  }

  /**
   * Generate a time-sortable, unique ID (ULID-like).
   *
   * Format: {timestamp_base36_padded}{counter_base36_padded}{random_base36}
   * - Timestamp portion ensures lexicographic sort across milliseconds
   * - Counter portion ensures sort order within the same millisecond
   * - Random portion provides additional uniqueness
   */
  private generateId(): string {
    const now = Date.now()
    if (now === this.lastIdTs) {
      this.idCounter++
    } else {
      this.lastIdTs = now
      this.idCounter = 0
    }
    const ts = now.toString(36).padStart(9, '0')
    const counter = this.idCounter.toString(36).padStart(4, '0')
    const rand = Math.random().toString(36).substring(2, 6)
    return ts + counter + rand
  }

  /**
   * Apply update operators to an existing entity, producing a new DataLine.
   *
   * Operators:
   * - $set: shallow merge fields into entity
   * - $inc: add to numeric fields (default 0 if field missing)
   * - $unset: delete fields from entity
   */
  private applyUpdate(entity: DataLine, ops: UpdateOps): DataLine {
    // Clone the entity (shallow copy is sufficient for top-level fields)
    const result: DataLine = { ...entity }

    // $set: merge fields
    if (ops.$set) {
      for (const [key, value] of Object.entries(ops.$set)) {
        if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
        ;(result as Record<string, unknown>)[key] = value
      }
    }

    // $inc: increment numeric fields
    if (ops.$inc) {
      for (const [key, amount] of Object.entries(ops.$inc)) {
        if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
        const current = (result as Record<string, unknown>)[key]
        const base = typeof current === 'number' ? current : 0
        ;(result as Record<string, unknown>)[key] = base + amount
      }
    }

    // $unset: remove fields
    if (ops.$unset) {
      for (const key of Object.keys(ops.$unset)) {
        if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
        delete (result as Record<string, unknown>)[key]
      }
    }

    return result
  }

  // ===========================================================================
  // Get By ID
  // ===========================================================================

  /**
   * Get a single entity by ID from the given table.
   *
   * Fast path: O(1) lookup in the in-memory TableBuffer.
   * Returns null if the entity does not exist, the table is unknown,
   * or the entity has been deleted (tombstone).
   *
   * Async signature so that Parquet fallback can be added later
   * without changing the API.
   */
  async get(table: string, id: string): Promise<DataLine | null> {
    const buffer = this.buffers.get(table)
    if (!buffer) return null

    const entity = buffer.get(id)
    if (!entity) return null
    if (entity.$op === 'd') return null

    return entity
  }

  /**
   * Get multiple entities by ID from the given table.
   *
   * Returns an array in the same order as the requested IDs.
   * Missing or deleted entities are represented as null.
   *
   * Async signature so that Parquet fallback can be added later
   * without changing the API.
   */
  async getMany(table: string, ids: string[]): Promise<(DataLine | null)[]> {
    const buffer = this.buffers.get(table)
    if (!buffer) return ids.map(() => null)

    return ids.map(id => {
      const entity = buffer.get(id)
      if (!entity) return null
      if (entity.$op === 'd') return null
      return entity
    })
  }

  // ===========================================================================
  // Local Storage Lifecycle
  // ===========================================================================

  /**
   * Initialize the engine: replay JSONL files into buffers and recover any
   * interrupted compactions.
   *
   * Call this after constructing the engine to restore state from disk.
   * Safe to call multiple times (idempotent).
   *
   * Steps:
   * 1. Scan dataDir for *.jsonl files to discover tables
   * 2. Check for .compacting files (interrupted compactions) and recover them
   * 3. Read compacted data files (*.parquet) and load into buffers
   * 4. Replay JSONL files into buffers (overlay on compacted data)
   */
  async init(): Promise<void> {
    // 1. Scan dataDir for files
    let files: string[]
    try {
      files = await readdir(this.dataDir)
    } catch {
      // dataDir doesn't exist yet — nothing to replay
      return
    }

    // 1a. Clean up orphaned .tmp files from interrupted compactions
    for (const file of files) {
      if (file.endsWith('.tmp')) {
        try {
          await unlink(join(this.dataDir, file))
        } catch {
          // Ignore errors during cleanup (file may already be gone)
        }
      }
    }

    // 2. Discover table names from JSONL files
    const tables = new Set<string>()
    for (const file of files) {
      if (file.endsWith('.jsonl') && file !== 'events.jsonl' && file !== 'rels.jsonl') {
        tables.add(file.replace('.jsonl', ''))
      }
      // Also discover from .compacting files
      if (file.endsWith('.jsonl.compacting') && !file.startsWith('events.') && !file.startsWith('rels.')) {
        tables.add(file.replace('.jsonl.compacting', ''))
      }
      // Also discover from compacted data files (exclude system files)
      if (file.endsWith('.parquet') && file !== 'rels.parquet' && file !== 'events.parquet') {
        tables.add(file.replace('.parquet', ''))
      }
    }

    // 3. For each table: recover interrupted compactions, load data, replay JSONL
    const storage = this.createLocalStorageAdapter()
    for (const table of tables) {
      this.knownTables.add(table)
      const jsonlPath = join(this.dataDir, `${table}.jsonl`)

      // 3a. Check for interrupted compaction and recover
      if (await needsRecovery(jsonlPath)) {
        await this.recoverCompaction(table, storage)
      }

      // 3b. Load compacted data file into buffer
      const dataPath = join(this.dataDir, `${table}.parquet`)
      const compactedData = await storage.readData(dataPath)
      const buffer = this.getBuffer(table)
      for (const entity of compactedData) {
        buffer.set(entity)
      }

      // 3c. Replay JSONL file into buffer
      await replayInto<DataLine>(jsonlPath, (line) => {
        buffer.set(line)
      })
    }
  }

  /**
   * Compact a specific table's JSONL into its data file.
   *
   * If `options` are provided (maxBytes, maxLines), compaction only occurs
   * when the threshold is exceeded. Without options, compaction always runs.
   *
   * After compaction:
   * - The data file contains the merged result of old data + JSONL mutations
   * - The JSONL file is fresh (empty) for new writes
   * - The in-memory buffer is rebuilt from the compacted data + fresh JSONL
   *
   * @returns The number of entities in the compacted output, or null if skipped
   */
  async compact(table: string, options?: CompactOptions): Promise<number | null> {
    const jsonlPath = join(this.dataDir, `${table}.jsonl`)

    // If options include thresholds, check them first
    if (options && (options.maxBytes !== undefined || options.maxLines !== undefined)) {
      const should = await shouldCompactData(jsonlPath, options)
      if (!should) {
        return null
      }
    }

    // Close the existing writer for this table so all data is flushed
    const existingWriter = this.dataWriters.get(table)
    if (existingWriter) {
      await existingWriter.close()
      this.dataWriters.delete(table)
    }

    // Perform compaction
    const storage = this.createLocalStorageAdapter()
    const count = await compactDataTable(this.dataDir, table, storage)

    // Rebuild the buffer from compacted data + fresh JSONL
    if (count !== null) {
      const buffer = this.getBuffer(table)
      buffer.clear()

      // Load compacted data
      const dataPath = join(this.dataDir, `${table}.parquet`)
      const compactedData = await storage.readData(dataPath)
      for (const entity of compactedData) {
        buffer.set(entity)
      }

      // Replay any new JSONL lines (written after rotation)
      await replayInto<DataLine>(jsonlPath, (line) => {
        buffer.set(line)
      })

      // Reset write counter
      this.writeCounters.set(table, 0)
    }

    return count
  }

  /**
   * Compact all known tables, relationships, and events.
   *
   * This is a convenience method that compacts every table that has been
   * written to during this engine's lifecycle or discovered during init().
   */
  async compactAll(): Promise<void> {
    // Compact all data tables
    for (const table of this.knownTables) {
      await this.compact(table)
    }

    // Note: rels and events compaction would go here when the engine
    // manages relationship and event writers directly.
  }

  // ===========================================================================
  // Private: Local Storage Adapter
  // ===========================================================================

  /**
   * Create a Parquet-based StorageAdapter for local mode.
   *
   * Uses ParquetStorageAdapter so that compacted data files are real Parquet
   * format, enabling Parquet-native tools to read them directly.
   */
  private createLocalStorageAdapter(): StorageAdapter {
    return new ParquetStorageAdapter()
  }

  // ===========================================================================
  // Private: Recovery
  // ===========================================================================

  /**
   * Recover an interrupted compaction for a table.
   *
   * If a .compacting file exists, it means a previous compaction was interrupted.
   * We complete it by running the same merge logic: read existing data file,
   * read the .compacting JSONL, merge, and write the result.
   */
  private async recoverCompaction(table: string, storage: StorageAdapter): Promise<void> {
    const jsonlPath = join(this.dataDir, `${table}.jsonl`)
    const compactingPath = getCompactingPath(jsonlPath)
    const dataPath = join(this.dataDir, `${table}.parquet`)

    // Read existing data file
    const existing = await storage.readData(dataPath)

    // Read the compacting JSONL
    const compactingData = await replay<DataLine>(compactingPath)

    // Merge
    const merged = mergeResults(existing, compactingData)

    // Write the merged result
    await storage.writeData(dataPath, merged)

    // Remove the .compacting file
    try {
      await unlink(compactingPath)
    } catch {
      // Ignore if already gone
    }
  }

  // ===========================================================================
  // Private: Auto-Compact Tracking
  // ===========================================================================

  /**
   * Track a write operation for auto-compaction threshold checks.
   *
   * If autoCompact is configured and the write count for a table exceeds
   * the maxLines threshold, compaction is triggered automatically.
   */
  private async trackWrite(table: string, count: number): Promise<void> {
    if (!this.autoCompact) return

    const current = (this.writeCounters.get(table) ?? 0) + count
    this.writeCounters.set(table, current)

    if (this.autoCompact.maxLines !== undefined && current > this.autoCompact.maxLines) {
      await this.compact(table)
    }
  }
}
