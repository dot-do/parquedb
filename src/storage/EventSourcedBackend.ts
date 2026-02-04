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
  buildRelationshipParquetSchema,
  eventToRow,
  rowToEvent,
} from '../backends/parquet-utils'

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

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG: Required<EventSourcedConfig> = {
  maxBufferedEvents: 100,
  maxBufferedBytes: 1024 * 1024, // 1MB
  autoSnapshotThreshold: 100,
  maxCachedEntities: 1000,
  cacheTtlMs: 5 * 60 * 1000, // 5 minutes
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

  /** Relationship index: targetId -> Map<sourceField, Set<sourceId>> */
  private relIndex: Map<string, Map<string, Set<string>>> = new Map()

  /** Whether relationships have been modified since last flush */
  private relsDirty = false

  /** Whether sequences have been initialized from storage */
  private initialized = false

  constructor(storage: StorageBackend, config?: EventSourcedConfig) {
    this.storage = storage
    this.config = { ...DEFAULT_CONFIG, ...config }
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize sequence counters from storage
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    try {
      // Read sequence metadata from storage
      const metaPath = 'data/event-meta.json'
      const exists = await this.storage.exists(metaPath)

      if (exists) {
        const data = await this.storage.read(metaPath)
        const meta = JSON.parse(new TextDecoder().decode(data)) as {
          sequences: Record<string, number>
        }

        for (const [ns, seq] of Object.entries(meta.sequences)) {
          this.sequences.set(ns, seq)
        }
      }
    } catch {
      // Ignore read errors - start fresh
    }

    this.initialized = true
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

    // Track relationship changes from entity events
    if (event.op === 'CREATE' || event.op === 'UPDATE') {
      if (event.after && typeof event.after === 'object') {
        const afterEntity = event.after as Entity
        this.updateRelationshipsFromEntity(afterEntity, event.target)
      }
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

    // Persist sequence metadata
    await this.persistSequenceMetadata()
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

    // Check cache first
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

          // Also index relationships from this entity
          this.updateRelationshipsFromEntity(entity, target)
        }
      }
    } catch {
      // Ignore read errors - return empty map
    }

    return entities
  }

  /**
   * Read all stored events (not filtered by namespace)
   */
  private async readAllStoredEvents(): Promise<Event[]> {
    const events: Event[] = []

    try {
      // Read from flat events.parquet file
      const exists = await this.storage.exists('events.parquet')
      if (!exists) return events

      const data = await this.storage.read('events.parquet')

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

      // Use onComplete callback to get data (parquetRead returns void)
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

      // Sort by timestamp
      events.sort((a, b) => a.ts - b.ts)
    } catch (err) {
      // Log read errors for debugging
      console.error('[EventSourcedBackend] Error reading events.parquet:', err)
    }

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
   * Writes events to a single flat `events.parquet` file using Parquet format
   * with Variant-encoded binary JSON for efficiency.
   */
  private async flushNamespace(ns: string): Promise<void> {
    const buffer = this.eventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    // Read ALL existing events from storage (not just this namespace)
    // This is critical because we write to a single events.parquet file
    const existingEvents = await this.readAllStoredEvents()

    // Combine with new events from this namespace's buffer
    const allEvents = [...existingEvents, ...buffer.events]

    // Convert events to Parquet rows
    const eventRows = allEvents.map((event) => eventToRow(event))

    // Build Parquet schema and write
    const eventSchema = buildEventParquetSchema()
    const writer = new ParquetWriter(this.storage, { compression: 'lz4' })

    // Write to flat events.parquet file (not namespace subfolders)
    await writer.write('events.parquet', eventRows, eventSchema)

    // Write rels.parquet if relationships have changed
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

    // Also persist sequence metadata for consistency
    await this.persistSequenceMetadata()
  }

  /**
   * Read stored events for a namespace from flat events.parquet file
   */
  private async readStoredEvents(ns: string): Promise<Event[]> {
    const events: Event[] = []

    try {
      // Read from flat events.parquet file
      const exists = await this.storage.exists('events.parquet')
      if (!exists) return events

      const data = await this.storage.read('events.parquet')

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

      // Use onComplete callback to get data (parquetRead returns void)
      let rows: unknown[][] = []
      await parquetRead({
        file: asyncBuffer,
        compressors,
        onComplete: (data: unknown[][]) => {
          rows = data
        },
      })

      // Convert rows back to Event objects, filtering by namespace
      for (const rowArray of rows) {
        const row: Record<string, unknown> = {}
        for (let i = 0; i < columnNames.length; i++) {
          const colName = columnNames[i]
          if (colName) {
            row[colName] = rowArray[i]
          }
        }
        const eventData = rowToEvent(row)
        // Cast to Event type (rowToEvent returns compatible structure)
        const event = eventData as Event
        // Filter by namespace if needed
        const eventNs = this.parseNamespace(event.target)
        if (eventNs === ns) {
          events.push(event)
        }
      }

      // Sort by timestamp
      events.sort((a, b) => a.ts - b.ts)
    } catch {
      // Ignore read errors - file may not exist yet
    }

    return events
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
   * Persist sequence metadata to storage
   */
  private async persistSequenceMetadata(): Promise<void> {
    const meta = {
      sequences: Object.fromEntries(this.sequences),
      updatedAt: new Date().toISOString(),
    }

    const metaPath = 'data/event-meta.json'
    const data = new TextEncoder().encode(JSON.stringify(meta))
    await this.storage.write(metaPath, data)
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
  // Resource Management
  // ===========================================================================

  /**
   * Clear all caches and buffers
   */
  clear(): void {
    this.eventBuffers.clear()
    this.entityCache.clear()
    this.snapshotCache.clear()
    this.relIndex.clear()
    this.relsDirty = false
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
