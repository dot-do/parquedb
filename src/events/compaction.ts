/**
 * Event Compaction
 *
 * Compacts events into materialized state files:
 * 1. Read events from R2 segments up to compaction timestamp
 * 2. Replay to build current state per entity/relationship
 * 3. Write new data.parquet + rels.parquet
 * 4. Create snapshot: snapshots/{ts}/data.parquet, rels.parquet
 * 5. Update compactedThrough watermark in manifest
 * 6. Delete compacted event segments
 *
 * This module provides:
 * - EventCompactor: Main compaction engine
 * - StateCollector: Collects final state from replayed events
 */

import type { Event, Variant } from '../types/entity'
import { isRelationshipTarget, parseEntityTarget, parseRelTarget } from '../types/entity'
import type { EventBatch, EventSegment, CompactionConfig } from './types'
import type { SegmentStorage } from './segment'
import type { ManifestManager } from './manifest'
import { EventReplayer as _EventReplayer, BatchEventSource as _BatchEventSource } from './replay'
import {
  DEFAULT_COMPACTION_MIN_EVENTS,
  DEFAULT_COMPACTION_MAX_SEGMENT_AGE,
  DEFAULT_COMPACTION_RETENTION,
} from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for the compactor
 */
export interface CompactorOptions {
  /** Dataset name */
  dataset: string
  /** Storage backend */
  storage: SegmentStorage
  /** Manifest manager */
  manifest: ManifestManager
  /** Compaction configuration */
  config?: CompactionConfig | undefined
}

/**
 * Result of a compaction run
 */
export interface CompactionResult {
  /** Timestamp through which events were compacted */
  compactedThrough: number
  /** Number of events processed */
  eventsProcessed: number
  /** Number of entities in final state */
  entityCount: number
  /** Number of relationships in final state */
  relationshipCount: number
  /** Segments that were compacted */
  segmentsCompacted: EventSegment[]
  /** Snapshot path (if created) */
  snapshotPath?: string | undefined
  /** Duration in milliseconds */
  durationMs: number
}

/**
 * Entity state after compaction
 */
export interface EntityState {
  /** Entity target (ns:id) */
  target: string
  /** Namespace */
  ns: string
  /** Entity ID */
  id: string
  /** Final state (null if deleted) */
  state: Variant | null
  /** Last event timestamp */
  lastEventTs: number
  /** Whether the entity currently exists */
  exists: boolean
}

/**
 * Relationship state after compaction
 */
export interface RelationshipState {
  /** Relationship target (from:pred:to) */
  target: string
  /** From entity (ns:id) */
  from: string
  /** Predicate */
  predicate: string
  /** To entity (ns:id) */
  to: string
  /** Relationship data (null if deleted) */
  data: Variant | null
  /** Last event timestamp */
  lastEventTs: number
  /** Whether the relationship currently exists */
  exists: boolean
}

/**
 * Callback for writing compacted state
 */
export interface StateWriter {
  /** Write entity states to data files */
  writeEntities(entities: EntityState[]): Promise<void>
  /** Write relationship states to rels files */
  writeRelationships(relationships: RelationshipState[]): Promise<void>
  /** Create a snapshot at the given timestamp */
  createSnapshot?(timestamp: number, entities: EntityState[], relationships: RelationshipState[]): Promise<string>
}

/**
 * Default compaction config
 */
const DEFAULT_COMPACTION_CONFIG: CompactionConfig = {
  minEvents: DEFAULT_COMPACTION_MIN_EVENTS,
  maxSegmentAge: DEFAULT_COMPACTION_MAX_SEGMENT_AGE,
  retention: DEFAULT_COMPACTION_RETENTION,
}

// =============================================================================
// StateCollector Class
// =============================================================================

/**
 * Collects final state by replaying events.
 *
 * Groups events by target and tracks the final state of each entity
 * and relationship.
 */
export class StateCollector {
  private entityStates = new Map<string, EntityState>()
  private relationshipStates = new Map<string, RelationshipState>()

  /**
   * Process a batch of events
   */
  processBatch(batch: EventBatch): void {
    for (const event of batch.events) {
      this.processEvent(event)
    }
  }

  /**
   * Process a single event
   */
  processEvent(event: Event): void {
    if (isRelationshipTarget(event.target)) {
      this.processRelationshipEvent(event)
    } else {
      this.processEntityEvent(event)
    }
  }

  /**
   * Process an entity event
   */
  private processEntityEvent(event: Event): void {
    const parsed = parseEntityTarget(event.target)
    if (!parsed) return

    const { ns, id } = parsed
    const existing = this.entityStates.get(event.target)

    // Only update if this is the latest event for this entity
    if (existing && existing.lastEventTs > event.ts) {
      return
    }

    const state: EntityState = {
      target: event.target,
      ns,
      id,
      state: this.getStateAfterEvent(event),
      lastEventTs: event.ts,
      exists: event.op !== 'DELETE',
    }

    this.entityStates.set(event.target, state)
  }

  /**
   * Process a relationship event
   */
  private processRelationshipEvent(event: Event): void {
    const parsed = parseRelTarget(event.target)
    if (!parsed) return

    const { from, predicate, to } = parsed
    const existing = this.relationshipStates.get(event.target)

    // Only update if this is the latest event for this relationship
    if (existing && existing.lastEventTs > event.ts) {
      return
    }

    const state: RelationshipState = {
      target: event.target,
      from,
      predicate,
      to,
      data: this.getStateAfterEvent(event),
      lastEventTs: event.ts,
      exists: event.op !== 'DELETE',
    }

    this.relationshipStates.set(event.target, state)
  }

  /**
   * Get the state after applying an event
   */
  private getStateAfterEvent(event: Event): Variant | null {
    switch (event.op) {
      case 'CREATE':
      case 'UPDATE':
        return event.after ?? null
      case 'DELETE':
        return null
      default:
        return null
    }
  }

  /**
   * Get all entity states (including deleted)
   */
  getEntityStates(): EntityState[] {
    return Array.from(this.entityStates.values())
  }

  /**
   * Get only existing entities (not deleted)
   */
  getExistingEntities(): EntityState[] {
    return this.getEntityStates().filter(e => e.exists)
  }

  /**
   * Get all relationship states (including deleted)
   */
  getRelationshipStates(): RelationshipState[] {
    return Array.from(this.relationshipStates.values())
  }

  /**
   * Get only existing relationships (not deleted)
   */
  getExistingRelationships(): RelationshipState[] {
    return this.getRelationshipStates().filter(r => r.exists)
  }

  /**
   * Clear all collected state
   */
  clear(): void {
    this.entityStates.clear()
    this.relationshipStates.clear()
  }

  /**
   * Get statistics
   */
  getStats(): { entityCount: number; relationshipCount: number; existingEntities: number; existingRelationships: number } {
    return {
      entityCount: this.entityStates.size,
      relationshipCount: this.relationshipStates.size,
      existingEntities: this.getExistingEntities().length,
      existingRelationships: this.getExistingRelationships().length,
    }
  }
}

// =============================================================================
// EventCompactor Class
// =============================================================================

/**
 * Compacts events into materialized state files.
 *
 * @example
 * ```typescript
 * const compactor = new EventCompactor({
 *   dataset: 'my-app',
 *   storage,
 *   manifest,
 * })
 *
 * // Run compaction up to a specific timestamp
 * const result = await compactor.compact({
 *   throughTimestamp: Date.now() - 3600000, // 1 hour ago
 *   stateWriter,
 * })
 *
 * console.log(`Compacted ${result.eventsProcessed} events`)
 * ```
 */
export class EventCompactor {
  private options: CompactorOptions
  private config: CompactionConfig

  constructor(options: CompactorOptions) {
    this.options = options
    this.config = {
      ...DEFAULT_COMPACTION_CONFIG,
      ...options.config,
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Run compaction up to a timestamp
   */
  async compact(options: {
    /** Compact events up to (and including) this timestamp */
    throughTimestamp: number
    /** Writer for outputting compacted state */
    stateWriter: StateWriter
    /** Create a snapshot (default: true) */
    createSnapshot?: boolean | undefined
    /** Delete compacted segments (default: false - requires manual cleanup) */
    deleteSegments?: boolean | undefined
  }): Promise<CompactionResult> {
    const startTime = Date.now()
    const { throughTimestamp, stateWriter, createSnapshot = true, deleteSegments = false } = options

    // Get segments to compact
    const segments = await this.getCompactableSegments(throughTimestamp)
    if (segments.length === 0) {
      return {
        compactedThrough: throughTimestamp,
        eventsProcessed: 0,
        entityCount: 0,
        relationshipCount: 0,
        segmentsCompacted: [],
        durationMs: Date.now() - startTime,
      }
    }

    // Collect state from all segments
    const collector = new StateCollector()
    let eventsProcessed = 0

    for (const segment of segments) {
      const batch = await this.readSegment(segment)
      if (batch) {
        // Only process events up to throughTimestamp
        const filteredBatch: EventBatch = {
          ...batch,
          events: batch.events.filter(e => e.ts <= throughTimestamp),
          count: 0,
        }
        filteredBatch.count = filteredBatch.events.length
        collector.processBatch(filteredBatch)
        eventsProcessed += filteredBatch.count
      }
    }

    // Get final states
    const entities = collector.getExistingEntities()
    const relationships = collector.getExistingRelationships()

    // Write state files
    await stateWriter.writeEntities(entities)
    await stateWriter.writeRelationships(relationships)

    // Create snapshot if requested
    let snapshotPath: string | undefined
    if (createSnapshot && stateWriter.createSnapshot) {
      snapshotPath = await stateWriter.createSnapshot(throughTimestamp, entities, relationships)
    }

    // Update manifest
    await this.options.manifest.setCompactedThrough(throughTimestamp)
    await this.options.manifest.save()

    // Delete segments if requested
    if (deleteSegments) {
      const fullyCompactedSegments = segments.filter(s => s.maxTs <= throughTimestamp)
      const seqs = fullyCompactedSegments.map(s => s.seq)
      if (seqs.length > 0) {
        await this.deleteSegments(seqs)
        await this.options.manifest.removeSegments(seqs)
        await this.options.manifest.save()
      }
    }

    return {
      compactedThrough: throughTimestamp,
      eventsProcessed,
      entityCount: entities.length,
      relationshipCount: relationships.length,
      segmentsCompacted: segments,
      snapshotPath,
      durationMs: Date.now() - startTime,
    }
  }

  /**
   * Check if compaction is needed
   */
  async needsCompaction(): Promise<{ needed: boolean; reason?: string | undefined }> {
    await this.options.manifest.load() // Ensure manifest is loaded
    const summary = await this.options.manifest.getSummary()

    // Check minimum events threshold
    const minEvents = this.config.minEvents ?? 10000
    if (summary.totalEvents >= minEvents) {
      return { needed: true, reason: `Event count (${summary.totalEvents}) exceeds threshold (${minEvents})` }
    }

    // Check segment age
    if (this.config.maxSegmentAge) {
      const maxAgeMs = this.parseInterval(this.config.maxSegmentAge)
      const oldestSegmentAge = summary.minTs ? Date.now() - summary.minTs : 0

      if (oldestSegmentAge > maxAgeMs) {
        return { needed: true, reason: `Oldest segment age exceeds ${this.config.maxSegmentAge}` }
      }
    }

    return { needed: false }
  }

  /**
   * Get the suggested compaction timestamp
   */
  async getSuggestedCompactionTimestamp(): Promise<number> {
    const summary = await this.options.manifest.getSummary()

    // Default: compact everything up to 1 hour ago
    const oneHourAgo = Date.now() - 3600000

    // But don't compact past the max timestamp
    if (summary.maxTs && summary.maxTs < oneHourAgo) {
      return summary.maxTs
    }

    return oneHourAgo
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Get segments that can be compacted
   */
  private async getCompactableSegments(throughTimestamp: number): Promise<EventSegment[]> {
    // Get all segments that have events before or at the throughTimestamp
    const segments = await this.options.manifest.getSegmentsInRange(0, throughTimestamp)
    return segments
  }

  /**
   * Read a segment from storage
   */
  private async readSegment(segment: EventSegment): Promise<EventBatch | null> {
    const data = await this.options.storage.get(segment.path)
    if (!data) return null

    // Deserialize (same format as SegmentWriter)
    const json = new TextDecoder().decode(data)
    const lines = json.split('\n').filter(line => line.trim())
    const events: Event[] = []
    for (const line of lines) {
      try {
        events.push(JSON.parse(line) as Event)
      } catch {
        // Skip invalid JSON lines in event segments
        continue
      }
    }

    return {
      events,
      minTs: segment.minTs,
      maxTs: segment.maxTs,
      count: segment.count,
      sizeBytes: data.length,
    }
  }

  /**
   * Delete segments by sequence numbers
   */
  private async deleteSegments(seqs: number[]): Promise<void> {
    const { dataset } = this.options
    const prefix = 'events'

    for (const seq of seqs) {
      const paddedSeq = seq.toString().padStart(4, '0')
      const path = `${dataset}/${prefix}/seg-${paddedSeq}.parquet`
      await this.options.storage.delete(path)
    }
  }

  /**
   * Parse an interval string (e.g., '1h', '30d') to milliseconds
   */
  private parseInterval(interval: string): number {
    const match = interval.match(/^(\d+)(s|m|h|d)$/)
    if (!match) return 0

    const value = parseInt(match[1]!, 10) // match[1] is guaranteed to exist after successful regex
    const unit = match[2]!

    switch (unit) {
      case 's': return value * 1000
      case 'm': return value * 60 * 1000
      case 'h': return value * 60 * 60 * 1000
      case 'd': return value * 24 * 60 * 60 * 1000
      default: return 0
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an EventCompactor instance
 */
export function createEventCompactor(options: CompactorOptions): EventCompactor {
  return new EventCompactor(options)
}

/**
 * Create a StateCollector instance
 */
export function createStateCollector(): StateCollector {
  return new StateCollector()
}

// =============================================================================
// In-Memory State Writer (for testing)
// =============================================================================

/**
 * Simple in-memory state writer for testing
 */
export class InMemoryStateWriter implements StateWriter {
  public entities: EntityState[] = []
  public relationships: RelationshipState[] = []
  public snapshots: Map<number, { entities: EntityState[]; relationships: RelationshipState[] }> = new Map()

  async writeEntities(entities: EntityState[]): Promise<void> {
    this.entities = entities
  }

  async writeRelationships(relationships: RelationshipState[]): Promise<void> {
    this.relationships = relationships
  }

  async createSnapshot(
    timestamp: number,
    entities: EntityState[],
    relationships: RelationshipState[]
  ): Promise<string> {
    this.snapshots.set(timestamp, { entities, relationships })
    return `snapshots/${timestamp}`
  }

  clear(): void {
    this.entities = []
    this.relationships = []
    this.snapshots.clear()
  }
}

/**
 * Create an InMemoryStateWriter instance
 */
export function createInMemoryStateWriter(): InMemoryStateWriter {
  return new InMemoryStateWriter()
}
