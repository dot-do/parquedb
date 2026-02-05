/**
 * EventSourcedBackend - Unified event-sourced storage abstraction
 *
 * This module provides a unified storage interface that works identically
 * in both Node.js and Cloudflare Workers environments. It addresses the
 * dual storage architecture divergence by implementing event sourcing
 * consistently:
 *
 * ARCHITECTURE:
 * - Events are the single source of truth (not entities)
 * - Entity state is derived by replaying events
 * - Snapshots provide fast reconstruction checkpoints
 * - This interface wraps the underlying storage backend with event-sourcing semantics
 *
 * UNIFICATION BENEFITS:
 * - Same behavior in Node.js tests and Workers production
 * - No more globalEntityStore vs SQLite divergence
 * - Consistent event recording across all environments
 * - Shared reconstruction logic
 *
 * @see docs/architecture/entity-storage.md
 */

import type { StorageBackend } from '../types/storage'
import type { Entity, EntityId, Event, Variant } from '../types'
import { ParquetWriter } from '../parquet/writer'
import {
  buildEventParquetSchema,
  buildEntityParquetSchema,
  buildRelationshipParquetSchema,
  entityToRow,
  eventToRow,
  rowToEvent,
} from '../backends/parquet-utils'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Event batch stored in WAL
 */
export interface EventBatch {
  /** Batch ID */
  id: string
  /** Namespace */
  ns: string
  /** First sequence number in batch */
  firstSeq: number
  /** Last sequence number in batch */
  lastSeq: number
  /** Events in this batch */
  events: Event[]
  /** Batch creation timestamp */
  createdAt: string
}

/**
 * Entity snapshot for fast reconstruction
 */
export interface EntitySnapshot {
  /** Entity ID */
  entityId: string
  /** Namespace */
  ns: string
  /** Sequence number at time of snapshot */
  seq: number
  /** Full entity state at this point */
  state: Entity
  /** Snapshot creation timestamp */
  createdAt: string
}

/**
 * Configuration for event-sourced backend
 */
export interface EventSourcedConfig {
  /** Maximum events before auto-flush */
  maxBufferedEvents?: number
  /** Maximum bytes before auto-flush */
  maxBufferedBytes?: number
  /** Auto-snapshot after this many events per entity */
  autoSnapshotThreshold?: number
  /** Maximum cached entities */
  maxCachedEntities?: number
  /** Cache TTL in milliseconds */
  cacheTtlMs?: number
  /** Auto-compact when batch file count exceeds this (default: 10) */
  autoCompactFileThreshold?: number
  /** Auto-compact when total events exceed this (default: 1000) */
  autoCompactEventThreshold?: number
  /** Auto-compact after this many ms since last compaction (default: 60000 = 1 minute) */
  autoCompactIntervalMs?: number
}

/**
 * Event-sourced storage operations
 */
export interface EventSourcedOperations {
  /**
   * Append an event to the event log
   */
  appendEvent(event: Event): Promise<void>

  /**
   * Get all events for an entity (for reconstruction)
   */
  getEntityEvents(ns: string, id: string, afterSeq?: number): Promise<Event[]>

  /**
   * Get latest snapshot for an entity
   */
  getLatestSnapshot(ns: string, id: string): Promise<EntitySnapshot | null>

  /**
   * Create a snapshot for an entity
   */
  createSnapshot(ns: string, id: string, state: Entity, seq: number): Promise<void>

  /**
   * Flush buffered events to storage
   */
  flush(): Promise<void>

  /**
   * Get current sequence number for a namespace
   */
  getSequence(ns: string): number

  /**
   * Increment and return next sequence number
   */
  nextSequence(ns: string): number
}

/**
 * Internal event buffer
 */
interface EventBuffer {
  events: Event[]
  firstSeq: number
  lastSeq: number
  sizeBytes: number
}

/**
 * Batch file metadata
 */
interface BatchFileInfo {
  path: string
  seq: number
  eventCount: number
}

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG: Required<EventSourcedConfig> = {
  maxBufferedEvents: 100,
  maxBufferedBytes: 1024 * 1024, // 1MB
  autoSnapshotThreshold: 100,
  maxCachedEntities: 1000,
  cacheTtlMs: 5 * 60 * 1000, // 5 minutes
  autoCompactFileThreshold: 10, // Compact when >10 batch files
  autoCompactEventThreshold: 1000, // Compact when >1000 total events
  autoCompactIntervalMs: 60 * 1000, // Compact at most once per minute
}

// =============================================================================
// EventSourcedBackend
// =============================================================================

/**
 * EventSourcedBackend wraps a StorageBackend with event-sourcing semantics.
 *
 * This provides a unified interface that works the same way in:
 * - Node.js (with MemoryBackend, FsBackend)
 * - Workers (with R2Backend)
 *
 * The key principle is that events are the single source of truth.
 * Entity state is always derived by replaying events from the last snapshot.
 */
export class EventSourcedBackend implements EventSourcedOperations {
  private storage: StorageBackend
  private config: Required<EventSourcedConfig>

  /** Event buffers per namespace */
  private eventBuffers: Map<string, EventBuffer> = new Map()

  /** Sequence counters per namespace */
  private sequences: Map<string, number> = new Map()

  /** Entity state cache (derived from events) */
  private entityCache: Map<string, { entity: Entity | null; timestamp: number }> = new Map()

  /** Snapshot cache */
  private snapshotCache: Map<string, EntitySnapshot> = new Map()

  /** Current entity state (derived from events) - Map<fullId, Entity> */
  private entities: Map<string, Entity> = new Map()

  /** Relationship index: targetId -> Map<sourceField, Set<sourceId>> */
  private relIndex: Map<string, Map<string, Set<string>>> = new Map()

  /** Whether entities have been modified since last flush */
  private entitiesDirty = false

  /** Whether relationships have been modified since last flush */
  private relsDirty = false

  /** Whether sequences have been initialized from storage */
  private initialized = false

  /** Global batch sequence number for unique batch file names */
  private batchSeq = 0

  /** Cached batch file list */
  private batchFiles: BatchFileInfo[] = []

  /** Whether batch files have been scanned */
  private batchFilesScanned = false

  /** Timestamp of last compaction */
  private lastCompactedAt = 0

  /** Whether bulk operation is in progress (disables auto-compaction) */
  private bulkOperationInProgress = false

  /** Total events across all batch files (for threshold checking) */
  private totalEventCount = 0

  constructor(storage: StorageBackend, config?: EventSourcedConfig) {
    this.storage = storage
    this.config = { ...DEFAULT_CONFIG, ...config }
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize from storage (sequences can be derived from event count)
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Ensure events directory exists
    try {
      await this.storage.mkdir('events')
    } catch {
      // Directory may already exist
    }

    // Scan for existing batch files
    await this.scanBatchFiles()

    // Derive sequences from existing events
    await this.deriveSequencesFromEvents()

    this.initialized = true
  }

  /**
   * Derive sequence numbers from existing events
   */
  private async deriveSequencesFromEvents(): Promise<void> {
    const allEvents = await this.readAllStoredEvents()

    // Count events per namespace
    const eventCounts = new Map<string, number>()
    for (const event of allEvents) {
      const ns = this.parseNamespace(event.target)
      if (ns) {
        eventCounts.set(ns, (eventCounts.get(ns) || 0) + 1)
      }
    }

    // Set sequence for each namespace (events + 1 for next)
    for (const [ns, count] of eventCounts) {
      this.sequences.set(ns, count + 1)
    }
  }

  // ===========================================================================
  // EventSourcedOperations Implementation
  // ===========================================================================

  /**
   * Append an event to the event log
   */
  async appendEvent(event: Event): Promise<void> {
    await this.ensureInitialized()

    // Parse namespace from event target
    const ns = this.parseNamespace(event.target)
    if (!ns) {
      throw new Error(`Cannot parse namespace from event target: ${event.target}`)
    }

    // Get or create buffer for this namespace
    const buffer = this.getOrCreateBuffer(ns)

    // Add event to buffer
    buffer.events.push(event)
    buffer.lastSeq++
    buffer.sizeBytes += this.estimateEventSize(event)

    // Invalidate entity cache
    this.invalidateEntityCache(event.target)

    // Update entity state and relationships from events
    // Parse entity target - expected format is "ns:id"
    const parts = event.target.split(':')
    if (parts.length === 2) {
      // Standard entity target: "ns:id"
      const [entityNs, id] = parts
      const fullId = `${entityNs}/${id}`

      if (event.op === 'CREATE' || event.op === 'UPDATE') {
        if (event.after && typeof event.after === 'object') {
          const entity = this.variantToEntity(event.after as Variant, event.target)
          this.entities.set(fullId, entity)
          this.entitiesDirty = true
          this.updateRelationshipsFromEntity(entity, event.target)
        }
      } else if (event.op === 'DELETE') {
        this.entities.delete(fullId)
        this.entitiesDirty = true
      }
    } else if (parts.length > 2) {
      // Relationship target (e.g., "entity:ns:id:pred:target_ns:target_id")
      // These don't update entity state directly, which is expected
    } else {
      // Malformed target format - log warning
      logger.warn(
        `Event target "${event.target}" has unexpected format (expected "ns:id"). ` +
        `Entity state will not be updated. Event: ${event.op} ${event.id}`
      )
    }

    // Auto-flush if thresholds exceeded
    if (
      buffer.events.length >= this.config.maxBufferedEvents ||
      buffer.sizeBytes >= this.config.maxBufferedBytes
    ) {
      await this.flushNamespace(ns)
    }
  }

  /**
   * Get all events for an entity
   */
  async getEntityEvents(ns: string, id: string, afterSeq?: number): Promise<Event[]> {
    await this.ensureInitialized()

    const target = `${ns}:${id}`
    const events: Event[] = []

    // First, get events from storage
    const storedEvents = await this.readStoredEvents(ns)

    // Filter events for this entity
    for (const event of storedEvents) {
      if (event.target === target) {
        events.push(event)
      }
    }

    // Add buffered events
    const buffer = this.eventBuffers.get(ns)
    if (buffer) {
      for (const event of buffer.events) {
        if (event.target === target) {
          events.push(event)
        }
      }
    }

    // Filter by sequence if provided
    if (afterSeq !== undefined) {
      return events.slice(afterSeq)
    }

    return events
  }

  /**
   * Get latest snapshot for an entity
   */
  async getLatestSnapshot(ns: string, id: string): Promise<EntitySnapshot | null> {
    const cacheKey = `${ns}/${id}`

    // Check cache first
    const cached = this.snapshotCache.get(cacheKey)
    if (cached !== undefined) {
      return cached
    }

    // Read from storage
    try {
      const snapshotPath = `data/${ns}/snapshots/${id}.json`
      const exists = await this.storage.exists(snapshotPath)

      if (!exists) return null

      const data = await this.storage.read(snapshotPath)
      const snapshot = JSON.parse(new TextDecoder().decode(data)) as EntitySnapshot

      // Cache for future lookups
      this.snapshotCache.set(cacheKey, snapshot)

      return snapshot
    } catch {
      return null
    }
  }

  /**
   * Create a snapshot for an entity
   */
  async createSnapshot(ns: string, id: string, state: Entity, seq: number): Promise<void> {
    const snapshot: EntitySnapshot = {
      entityId: `${ns}/${id}`,
      ns,
      seq,
      state,
      createdAt: new Date().toISOString(),
    }

    const snapshotPath = `data/${ns}/snapshots/${id}.json`
    const data = new TextEncoder().encode(JSON.stringify(snapshot))

    await this.storage.write(snapshotPath, data)

    // Update cache
    this.snapshotCache.set(`${ns}/${id}`, snapshot)
  }

  /**
   * Flush all buffered events to storage
   */
  async flush(): Promise<void> {
    await this.ensureInitialized()

    const namespaces = [...this.eventBuffers.keys()]
    for (const ns of namespaces) {
      await this.flushNamespace(ns)
    }
  }

  /**
   * Get current sequence number for a namespace
   */
  getSequence(ns: string): number {
    return this.sequences.get(ns) || 1
  }

  /**
   * Increment and return next sequence number
   */
  nextSequence(ns: string): number {
    const current = this.getSequence(ns)
    this.sequences.set(ns, current + 1)
    return current
  }

  // ===========================================================================
  // Entity State Reconstruction
  // ===========================================================================

  /**
   * Reconstruct entity state from events
   *
   * This is the core event-sourcing operation:
   * 1. Load latest snapshot (if exists)
   * 2. Replay events since snapshot
   * 3. Return derived entity state
   */
  async reconstructEntity(ns: string, id: string): Promise<Entity | null> {
    const cacheKey = `${ns}/${id}`

    // Check in-memory entities first (populated by appendEvent)
    // This ensures get() works immediately after create() even before events are flushed
    const inMemoryEntity = this.entities.get(cacheKey)
    if (inMemoryEntity) {
      return inMemoryEntity
    }

    // Check cache second
    const cached = this.entityCache.get(cacheKey)
    if (cached && Date.now() - cached.timestamp < this.config.cacheTtlMs) {
      return cached.entity
    }

    // Get latest snapshot
    const snapshot = await this.getLatestSnapshot(ns, id)

    // Get events since snapshot
    const afterSeq = snapshot?.seq ?? 0
    const events = await this.getEntityEvents(ns, id, afterSeq > 0 ? afterSeq : undefined)

    // If no snapshot and no events, entity doesn't exist
    if (!snapshot && events.length === 0) {
      this.entityCache.set(cacheKey, { entity: null, timestamp: Date.now() })
      return null
    }

    // Start with snapshot state or empty
    let entity: Entity | null = snapshot?.state ?? null

    // Replay events to derive current state
    for (const event of events) {
      entity = this.applyEvent(entity, event)
    }

    // Cache result
    this.entityCache.set(cacheKey, { entity, timestamp: Date.now() })

    // Maybe create auto-snapshot
    if (
      entity &&
      this.config.autoSnapshotThreshold > 0 &&
      events.length >= this.config.autoSnapshotThreshold
    ) {
      const seq = this.getSequence(ns) - 1
      await this.createSnapshot(ns, id, entity, seq).catch(() => {
        // Ignore snapshot errors
      })
    }

    return entity
  }

  /**
   * Reconstruct all entities from stored events
   *
   * This method reads all events from storage and reconstructs the current
   * state of all entities. Used to populate the in-memory store on startup.
   *
   * @returns Map of entity ID (ns/id format) to reconstructed Entity
   */
  async reconstructAllEntities(): Promise<Map<string, Entity>> {
    await this.ensureInitialized()

    const entities = new Map<string, Entity>()

    try {
      // Read all events from storage
      const allEvents = await this.readAllStoredEvents()

      // Group events by entity target
      const eventsByEntity = new Map<string, Event[]>()
      for (const event of allEvents) {
        const target = event.target
        // Skip relationship events
        if (target.split(':').length > 2) continue

        const existing = eventsByEntity.get(target) || []
        existing.push(event)
        eventsByEntity.set(target, existing)
      }

      // Reconstruct each entity
      for (const [target, events] of eventsByEntity) {
        // Sort events by timestamp
        events.sort((a, b) => a.ts - b.ts)

        // Apply events in sequence
        let entity: Entity | null = null
        for (const event of events) {
          entity = this.applyEvent(entity, event)
        }

        // If entity exists and isn't deleted, add to result
        if (entity && !entity.deletedAt) {
          // Convert target (ns:id) to fullId (ns/id)
          const [ns, id] = target.split(':')
          const fullId = `${ns}/${id}`
          entities.set(fullId, entity)

          // Also store in internal entities map and index relationships
          this.entities.set(fullId, entity)
          this.updateRelationshipsFromEntity(entity, target)
        }
      }
    } catch {
      // Ignore read errors - return empty map
    }

    return entities
  }

  /**
   * Scan for batch files in the events directory
   */
  private async scanBatchFiles(): Promise<void> {
    if (this.batchFilesScanned) return

    const eventsDir = 'events'

    try {
      // List all batch files - don't rely on exists() for directories
      // because MemoryBackend returns false for directory paths
      const result = await this.storage.list(eventsDir)
      for (const file of result.files) {
        // Files may include the directory prefix or just the filename
        const fileName = file.includes('/') ? file.split('/').pop()! : file
        if (fileName.endsWith('.parquet') && fileName.includes('batch-')) {
          // Extract sequence from filename (batch-{timestamp}-{seq}.parquet)
          const match = fileName.match(/batch-\d+-(\d+)\.parquet$/)
          const seq = match?.[1] ? parseInt(match[1], 10) : 0

          // Construct the full path properly
          const fullPath = file.startsWith(eventsDir) ? file : `${eventsDir}/${file}`

          this.batchFiles.push({
            path: fullPath,
            seq,
            eventCount: 0, // Will be populated on read
          })

          // Track highest sequence seen
          if (seq > this.batchSeq) {
            this.batchSeq = seq
          }
        }
      }

      // Sort batch files by sequence
      this.batchFiles.sort((a, b) => a.seq - b.seq)
    } catch {
      // Directory may not exist yet - that's OK
    }

    this.batchFilesScanned = true

    // Also check for legacy events.parquet and migrate if exists
    await this.migrateLegacyEventsFile()

    // Count total events across all batch files (for threshold checking)
    // This is done lazily - eventCount is populated when files are read
    // For now, estimate based on file count until actual reads happen
    this.totalEventCount = this.batchFiles.reduce((sum, f) => sum + f.eventCount, 0)
  }

  /**
   * Migrate legacy events.parquet to batch file format
   */
  private async migrateLegacyEventsFile(): Promise<void> {
    try {
      const exists = await this.storage.exists('events.parquet')
      if (!exists) return

      // Read legacy file
      const events = await this.readEventsFromFile('events.parquet')
      if (events.length === 0) return

      // Write to a batch file
      const eventRows = events.map((event) => eventToRow(event))
      const eventSchema = buildEventParquetSchema()
      const writer = new ParquetWriter(this.storage, { compression: 'lz4' })

      const batchSeq = ++this.batchSeq
      const batchPath = `events/batch-${Date.now()}-${batchSeq}.parquet`

      await writer.write(batchPath, eventRows, eventSchema)

      this.batchFiles.push({
        path: batchPath,
        seq: batchSeq,
        eventCount: events.length,
      })

      // Delete legacy file
      await this.storage.delete('events.parquet').catch(() => {})
    } catch {
      // Ignore migration errors
    }
  }

  /**
   * Read events from a single Parquet file
   */
  private async readEventsFromFile(filePath: string): Promise<Event[]> {
    const events: Event[] = []

    try {
      const exists = await this.storage.exists(filePath)
      if (!exists) return events

      const data = await this.storage.read(filePath)

      // Parse Parquet file using hyparquet
      const { parquetRead, parquetMetadataAsync } = await import('hyparquet')
      const { compressors } = await import('../parquet/compression')

      const asyncBuffer = {
        byteLength: data.length,
        slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
          const sliced = data.slice(start, end ?? data.length)
          const buffer = new ArrayBuffer(sliced.byteLength)
          new Uint8Array(buffer).set(sliced)
          return buffer
        },
      }

      // Get metadata to extract column names
      const metadata = await parquetMetadataAsync(asyncBuffer)
      const schema = (metadata.schema || []) as Array<{ name?: string }>
      const columnNames: string[] = schema
        .filter((el) => el.name && el.name !== 'root')
        .map((el) => el.name!)

      // Use onComplete callback to get data
      let rows: unknown[][] = []
      await parquetRead({
        file: asyncBuffer,
        compressors,
        onComplete: (data: unknown[][]) => {
          rows = data
        },
      })

      // Convert rows back to Event objects
      for (const rowArray of rows) {
        const row: Record<string, unknown> = {}
        for (let i = 0; i < columnNames.length; i++) {
          const colName = columnNames[i]
          if (colName) {
            row[colName] = rowArray[i]
          }
        }
        const eventData = rowToEvent(row)
        events.push(eventData as Event)
      }
    } catch {
      // Ignore read errors
    }

    return events
  }

  /**
   * Read all stored events (not filtered by namespace)
   *
   * Scans all batch files and combines events.
   */
  private async readAllStoredEvents(): Promise<Event[]> {
    await this.scanBatchFiles()

    const events: Event[] = []
    let totalCount = 0

    // Read from all batch files
    for (const batchFile of this.batchFiles) {
      const batchEvents = await this.readEventsFromFile(batchFile.path)
      events.push(...batchEvents)

      // Update event count for this batch file
      batchFile.eventCount = batchEvents.length
      totalCount += batchEvents.length
    }

    // Update total event count
    this.totalEventCount = totalCount

    // Sort by timestamp
    events.sort((a, b) => a.ts - b.ts)

    return events
  }

  /**
   * Apply an event to entity state (event sourcing reducer)
   */
  private applyEvent(entity: Entity | null, event: Event): Entity | null {
    switch (event.op) {
      case 'CREATE':
        // Entity is created from the after state
        if (event.after && typeof event.after === 'object') {
          return this.variantToEntity(event.after as Variant, event.target)
        }
        return entity

      case 'UPDATE':
        // Merge after state into existing entity
        if (entity && event.after && typeof event.after === 'object') {
          return {
            ...entity,
            ...(event.after as object),
            version: (entity.version || 0) + 1,
            updatedAt: new Date(event.ts),
          } as Entity
        }
        return entity

      case 'DELETE':
        // Mark as deleted (soft delete) or return null (hard delete)
        if (entity) {
          return {
            ...entity,
            deletedAt: new Date(event.ts),
            deletedBy: event.actor as EntityId,
          }
        }
        return null

      default:
        return entity
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Get or create an event buffer for a namespace
   */
  private getOrCreateBuffer(ns: string): EventBuffer {
    const existing = this.eventBuffers.get(ns)
    if (existing) return existing

    const seq = this.getSequence(ns)
    const buffer: EventBuffer = {
      events: [],
      firstSeq: seq,
      lastSeq: seq - 1,
      sizeBytes: 0,
    }
    this.eventBuffers.set(ns, buffer)
    return buffer
  }

  /**
   * Parse namespace from event target
   */
  private parseNamespace(target: string): string | null {
    // Entity target: "ns:id"
    // Relationship target: "entity:ns:id:pred:target:ns:id"
    const parts = target.split(':')
    if (parts.length >= 2) {
      return parts[0] || null
    }
    return null
  }

  /**
   * Estimate event size in bytes
   */
  private estimateEventSize(event: Event): number {
    return JSON.stringify(event).length
  }

  /**
   * Invalidate entity cache for a target
   */
  private invalidateEntityCache(target: string): void {
    const parts = target.split(':')
    if (parts.length >= 2) {
      const ns = parts[0]
      const id = parts[1]
      this.entityCache.delete(`${ns}/${id}`)
    }
  }

  /**
   * Update relationship index from entity data
   */
  private updateRelationshipsFromEntity(entity: Entity, target: string): void {
    const parts = target.split(':')
    const ns = parts[0] || ''
    const id = parts[1] || ''
    const sourceId = `${ns}/${id}`

    // Look for relationship fields in entity data
    for (const [key, value] of Object.entries(entity)) {
      // Skip meta fields
      if (key.startsWith('$') || ['name', 'version', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'deletedAt', 'deletedBy'].includes(key)) {
        continue
      }

      // Check if value looks like entity references
      const refs = this.extractEntityRefs(value)
      for (const targetId of refs) {
        this.addRelationship(sourceId, key, targetId)
      }
    }
  }

  /**
   * Extract entity references from a value
   */
  private extractEntityRefs(value: unknown): string[] {
    const refs: string[] = []

    if (typeof value === 'string' && value.includes('/') && !value.startsWith('/')) {
      // Looks like an entity ID (ns/id format)
      refs.push(value)
    } else if (Array.isArray(value)) {
      for (const item of value) {
        refs.push(...this.extractEntityRefs(item))
      }
    } else if (value && typeof value === 'object') {
      // Check for $id field (entity reference object)
      const obj = value as Record<string, unknown>
      if (typeof obj.$id === 'string') {
        refs.push(obj.$id)
      }
    }

    return refs
  }

  /**
   * Add a relationship to the index
   */
  private addRelationship(sourceId: string, sourceField: string, targetId: string): void {
    let targetMap = this.relIndex.get(targetId)
    if (!targetMap) {
      targetMap = new Map()
      this.relIndex.set(targetId, targetMap)
    }

    let sourceSet = targetMap.get(sourceField)
    if (!sourceSet) {
      sourceSet = new Set()
      targetMap.set(sourceField, sourceSet)
    }

    if (!sourceSet.has(sourceId)) {
      sourceSet.add(sourceId)
      this.relsDirty = true
    }
  }

  /**
   * Flush events for a single namespace
   *
   * Uses append-only batch files for O(1) writes instead of O(n) rewrites.
   * Events are written to `events/batch-{seq}.parquet` files.
   *
   * Also writes data.parquet and rels.parquet for the current entity state.
   */
  private async flushNamespace(ns: string): Promise<void> {
    const buffer = this.eventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    // Convert events to Parquet rows
    const eventRows = buffer.events.map((event) => eventToRow(event))

    // Build Parquet schema and write to a NEW batch file (append-only pattern)
    const eventSchema = buildEventParquetSchema()
    const writer = new ParquetWriter(this.storage, { compression: 'lz4' })

    // Generate unique batch file name using timestamp + sequence
    const batchSeq = ++this.batchSeq
    const batchPath = `events/batch-${Date.now()}-${batchSeq}.parquet`

    // Write ONLY the new events to a new batch file (O(1) operation)
    await writer.write(batchPath, eventRows, eventSchema)

    // Track the new batch file
    const newEventCount = buffer.events.length
    this.batchFiles.push({
      path: batchPath,
      seq: batchSeq,
      eventCount: newEventCount,
    })
    this.totalEventCount += newEventCount

    // Write data.parquet (primary entity storage for deployment/queries)
    if (this.entitiesDirty && this.entities.size > 0) {
      await this.writeDataParquet(writer)
      this.entitiesDirty = false
    } else if (buffer.events.length > 0 && this.entities.size === 0) {
      // Log when events exist but no entities were tracked
      // This could indicate a target format issue
      logger.debug(
        `Skipping data.parquet write: ${buffer.events.length} events processed but no entities tracked. ` +
        `Check that event targets use "ns:id" format.`
      )
    }

    // Write rels.parquet (relationship index for queries)
    if (this.relsDirty) {
      await this.writeRelsParquet(writer)
      this.relsDirty = false
    }

    // Update sequence
    this.sequences.set(ns, buffer.lastSeq + 1)

    // Clear buffer
    this.eventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq + 1,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })

    // Check if auto-compaction should run
    await this.maybeAutoCompact()
  }

  /**
   * Read stored events for a namespace from batch files
   */
  private async readStoredEvents(ns: string): Promise<Event[]> {
    // Get all events and filter by namespace
    const allEvents = await this.readAllStoredEvents()

    const events: Event[] = []
    for (const event of allEvents) {
      const eventNs = this.parseNamespace(event.target)
      if (eventNs === ns) {
        events.push(event)
      }
    }

    return events
  }

  /**
   * Write entities to data.parquet (primary storage for deployment/queries)
   */
  private async writeDataParquet(writer: ParquetWriter): Promise<void> {
    const rows = Array.from(this.entities.values()).map(entity => entityToRow(entity))

    if (rows.length > 0) {
      const entitySchema = buildEntityParquetSchema()
      await writer.write('data.parquet', rows, entitySchema)
    }
  }

  /**
   * Write relationships to rels.parquet
   */
  private async writeRelsParquet(writer: ParquetWriter): Promise<void> {
    const rows: Array<{ sourceId: string; sourceField: string; targetId: string; createdAt: string }> = []
    const now = new Date().toISOString()

    for (const [targetId, sourceMap] of this.relIndex) {
      for (const [sourceField, sourceIds] of sourceMap) {
        for (const sourceId of sourceIds) {
          rows.push({
            sourceId,
            sourceField,
            targetId,
            createdAt: now,
          })
        }
      }
    }

    if (rows.length > 0) {
      const relSchema = buildRelationshipParquetSchema()
      await writer.write('rels.parquet', rows, relSchema)
    }
  }

  /**
   * Convert Variant to Entity
   */
  private variantToEntity(variant: Variant, target: string): Entity {
    const parts = target.split(':')
    const ns = parts[0] || ''
    const id = parts[1] || ''

    const base = variant as object

    return {
      $id: `${ns}/${id}` as EntityId,
      $type: (base as { $type?: string | undefined }).$type || 'Unknown',
      name: (base as { name?: string | undefined }).name || '',
      version: 1,
      createdAt: new Date(),
      createdBy: '' as EntityId,
      updatedAt: new Date(),
      updatedBy: '' as EntityId,
      ...base,
    } as Entity
  }

  // ===========================================================================
  // Bulk Operation Control
  // ===========================================================================

  /**
   * Begin a bulk operation (disables auto-compaction)
   *
   * Call this before bulk inserts/upserts to prevent compaction overhead.
   * Remember to call endBulkOperation() when done.
   */
  beginBulkOperation(): void {
    this.bulkOperationInProgress = true
  }

  /**
   * End a bulk operation and optionally trigger compaction
   *
   * @param compact - Whether to compact after the bulk operation (default: true)
   */
  async endBulkOperation(compact = true): Promise<void> {
    this.bulkOperationInProgress = false
    if (compact) {
      await this.compact()
    }
  }

  /**
   * Check if auto-compaction should run based on thresholds
   */
  private shouldAutoCompact(): boolean {
    // Skip if bulk operation in progress
    if (this.bulkOperationInProgress) return false

    // Skip if we just compacted recently
    const now = Date.now()
    if (now - this.lastCompactedAt < this.config.autoCompactIntervalMs) return false

    // Check file count threshold
    if (this.batchFiles.length > this.config.autoCompactFileThreshold) return true

    // Check event count threshold
    if (this.totalEventCount > this.config.autoCompactEventThreshold) return true

    return false
  }

  /**
   * Run auto-compaction if thresholds are met
   */
  private async maybeAutoCompact(): Promise<void> {
    if (this.shouldAutoCompact()) {
      await this.compact()
    }
  }

  // ===========================================================================
  // Resource Management
  // ===========================================================================

  /**
   * Compact all batch files into a single events file
   *
   * This merges all batch files into a single compacted file for
   * more efficient reads. Should be called after bulk operations complete.
   *
   * @returns Number of events compacted
   */
  async compact(): Promise<number> {
    await this.ensureInitialized()

    // Skip if only one batch file (already compacted)
    if (this.batchFiles.length <= 1) {
      this.lastCompactedAt = Date.now()
      return this.totalEventCount
    }

    // Read all events from batch files
    const allEvents = await this.readAllStoredEvents()
    if (allEvents.length === 0) {
      this.lastCompactedAt = Date.now()
      return 0
    }

    // Write compacted file
    const eventRows = allEvents.map((event) => eventToRow(event))
    const eventSchema = buildEventParquetSchema()
    const writer = new ParquetWriter(this.storage, { compression: 'lz4' })

    const compactedSeq = ++this.batchSeq
    const compactedPath = `events/batch-${Date.now()}-${compactedSeq}.parquet`

    await writer.write(compactedPath, eventRows, eventSchema)

    // Delete old batch files
    const oldBatchFiles = this.batchFiles
    for (const batchFile of oldBatchFiles) {
      try {
        await this.storage.delete(batchFile.path)
      } catch {
        // Ignore delete errors
      }
    }

    // Reset batch file tracking
    this.batchFiles = [{
      path: compactedPath,
      seq: compactedSeq,
      eventCount: allEvents.length,
    }]

    // Update tracking
    this.totalEventCount = allEvents.length
    this.lastCompactedAt = Date.now()

    return allEvents.length
  }

  /**
   * Clear all caches and buffers
   */
  clear(): void {
    this.eventBuffers.clear()
    this.entityCache.clear()
    this.snapshotCache.clear()
    this.entities.clear()
    this.relIndex.clear()
    this.entitiesDirty = false
    this.relsDirty = false
    this.batchFiles = []
    this.batchFilesScanned = false
    this.batchSeq = 0
    this.lastCompactedAt = 0
    this.bulkOperationInProgress = false
    this.totalEventCount = 0
  }

  /**
   * Dispose of resources
   */
  async dispose(): Promise<void> {
    // Flush any pending events
    await this.flush().catch(() => {})

    // Clear caches
    this.clear()

    // Reset state
    this.initialized = false
    this.sequences.clear()
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an event-sourced backend wrapper
 */
export function createEventSourcedBackend(
  storage: StorageBackend,
  config?: EventSourcedConfig
): EventSourcedBackend {
  return new EventSourcedBackend(storage, config)
}

/**
 * Wrap existing storage with event-sourcing semantics
 */
export function withEventSourcing(
  storage: StorageBackend,
  config?: EventSourcedConfig
): EventSourcedBackend {
  return new EventSourcedBackend(storage, config)
}
