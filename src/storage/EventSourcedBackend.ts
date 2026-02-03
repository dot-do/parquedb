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

import type { StorageBackend, WriteResult, WriteOptions } from '../types/storage'
import type { Entity, EntityId, Event, EventOp, Variant, CreateInput, UpdateInput } from '../types'
import { generateId } from '../utils'

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
  maxBufferedEvents?: number | undefined
  /** Maximum bytes before auto-flush */
  maxBufferedBytes?: number | undefined
  /** Auto-snapshot after this many events per entity */
  autoSnapshotThreshold?: number | undefined
  /** Maximum cached entities */
  maxCachedEntities?: number | undefined
  /** Cache TTL in milliseconds */
  cacheTtlMs?: number | undefined
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
    let buffer = this.eventBuffers.get(ns)
    if (!buffer) {
      const seq = this.getSequence(ns)
      buffer = {
        events: [],
        firstSeq: seq,
        lastSeq: seq - 1,
        sizeBytes: 0,
      }
      this.eventBuffers.set(ns, buffer)
    }

    // Add event to buffer
    buffer.events.push(event)
    buffer.lastSeq++
    buffer.sizeBytes += this.estimateEventSize(event)

    // Invalidate entity cache
    this.invalidateEntityCache(event.target)

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
    if (this.snapshotCache.has(cacheKey)) {
      return this.snapshotCache.get(cacheKey)!
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
   * Flush events for a single namespace
   */
  private async flushNamespace(ns: string): Promise<void> {
    const buffer = this.eventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    // Create event batch
    const batch: EventBatch = {
      id: generateId(),
      ns,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      events: buffer.events,
      createdAt: new Date().toISOString(),
    }

    // Write to storage
    const batchPath = `data/${ns}/events/${batch.id}.json`
    const data = new TextEncoder().encode(JSON.stringify(batch))
    await this.storage.write(batchPath, data)

    // Update sequence
    this.sequences.set(ns, buffer.lastSeq + 1)

    // Clear buffer
    this.eventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq + 1,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  /**
   * Read stored events for a namespace
   */
  private async readStoredEvents(ns: string): Promise<Event[]> {
    const events: Event[] = []

    try {
      const eventsPrefix = `data/${ns}/events/`
      const result = await this.storage.list(eventsPrefix)

      for (const file of result.files) {
        const data = await this.storage.read(file)
        const batch = JSON.parse(new TextDecoder().decode(data)) as EventBatch
        events.push(...batch.events)
      }

      // Sort by timestamp
      events.sort((a, b) => a.ts - b.ts)
    } catch {
      // Ignore read errors
    }

    return events
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
