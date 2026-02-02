/**
 * Event system types for ParqueDB
 *
 * The events log is the source of truth. data.parquet and rels.parquet are
 * materialized views that can be reconstructed by replaying events.
 */

import type { Event, Variant } from '../types/entity'
import {
  DEFAULT_EVENT_BUFFER_SIZE,
  DEFAULT_EVENT_BUFFER_BYTES,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_R2_THRESHOLD_BYTES,
} from '../constants'

// Re-export core Event type
export type { Event } from '../types/entity'
export {
  isRelationshipTarget,
  parseEntityTarget,
  parseRelTarget,
  entityTarget,
  relTarget,
} from '../types/entity'

// =============================================================================
// Event Batch Types
// =============================================================================

/**
 * A batch of events ready for persistence
 */
export interface EventBatch {
  /** Events in the batch (ordered by id/ts) */
  events: Event[]
  /** Minimum timestamp in batch */
  minTs: number
  /** Maximum timestamp in batch */
  maxTs: number
  /** Number of events */
  count: number
  /** Serialized size in bytes (after encoding) */
  sizeBytes?: number
}

/**
 * Serialized event batch for SQLite blob storage
 */
export interface SerializedBatch {
  /** MessagePack or CBOR encoded events */
  data: Uint8Array
  /** Minimum timestamp */
  minTs: number
  /** Maximum timestamp */
  maxTs: number
  /** Event count */
  count: number
}

// =============================================================================
// Event Segment Types (R2 Storage)
// =============================================================================

/**
 * Metadata for an event segment file in R2
 */
export interface EventSegment {
  /** Segment sequence number */
  seq: number
  /** Path in R2: events/seg-{seq}.parquet */
  path: string
  /** Minimum timestamp in segment */
  minTs: number
  /** Maximum timestamp in segment */
  maxTs: number
  /** Number of events in segment */
  count: number
  /** File size in bytes */
  sizeBytes: number
  /** When the segment was created */
  createdAt: number
}

/**
 * Events manifest tracking all segments
 */
export interface EventManifest {
  /** Manifest version */
  version: 1
  /** Dataset identifier */
  dataset: string
  /** Ordered list of segments (oldest first) */
  segments: EventSegment[]
  /** Last compaction timestamp (events before this are in data/rels.parquet) */
  compactedThrough?: number
  /** Next segment sequence number */
  nextSeq: number
  /** Total event count across all segments */
  totalEvents: number
  /** When manifest was last updated */
  updatedAt: number
}

// =============================================================================
// SQLite WAL Types
// =============================================================================

/**
 * Row in the events_wal SQLite table
 */
export interface WalRow {
  /** Auto-increment ID */
  id: number
  /** Serialized batch (blob - may be Uint8Array, ArrayBuffer, or Buffer depending on SQLite impl) */
  batch: Uint8Array | ArrayBuffer
  /** Minimum timestamp in batch */
  minTs: number
  /** Maximum timestamp in batch */
  maxTs: number
  /** Event count in batch */
  count: number
}

// =============================================================================
// Writer Configuration
// =============================================================================

/**
 * Configuration for the event writer
 */
export interface EventWriterConfig {
  /** Maximum events to buffer before flush (default: 1000) */
  maxBufferSize?: number
  /** Maximum bytes to buffer before flush (default: 1MB) */
  maxBufferBytes?: number
  /** Maximum time to buffer before flush in ms (default: 5000) */
  flushIntervalMs?: number
  /** Size threshold for writing directly to R2 vs SQLite (default: 512KB) */
  r2ThresholdBytes?: number
}

/**
 * Default writer configuration
 */
export const DEFAULT_WRITER_CONFIG: Required<EventWriterConfig> = {
  maxBufferSize: DEFAULT_EVENT_BUFFER_SIZE,
  maxBufferBytes: DEFAULT_EVENT_BUFFER_BYTES,
  flushIntervalMs: DEFAULT_FLUSH_INTERVAL_MS,
  r2ThresholdBytes: DEFAULT_R2_THRESHOLD_BYTES,
}

// =============================================================================
// Compaction Configuration
// =============================================================================

/**
 * Configuration for event compaction
 */
export interface CompactionConfig {
  /** How often to run compaction (default: '1h') */
  interval?: string
  /** How long to retain events after compaction (default: '30d') */
  retention?: string
  /** Minimum events before compaction triggers (default: 10000) */
  minEvents?: number
  /** Maximum segment age before compaction (default: '24h') */
  maxSegmentAge?: string
}

// =============================================================================
// Dataset Configuration
// =============================================================================

/**
 * Dataset configuration for events mode
 */
export interface DatasetConfig {
  /** Enable events (WAL + time-travel). Default: false (read-only snapshot) */
  events?: boolean
  /** Compaction settings (only used if events: true) */
  compaction?: CompactionConfig
}

/**
 * Default dataset configuration
 */
export const DEFAULT_DATASET_CONFIG: DatasetConfig = {
  events: false,
}

// =============================================================================
// Time-Travel Query Types
// =============================================================================

/**
 * Options for time-travel queries
 */
export interface TimeTravelOptions {
  /** Query state at this timestamp (ms since epoch) */
  at?: number
}

/**
 * Result of replaying events to a point in time
 */
export interface ReplayResult<T = Variant> {
  /** Reconstructed state at the requested timestamp */
  state: T | null
  /** Number of events replayed */
  eventsReplayed: number
  /** Whether the entity existed at the requested timestamp */
  existed: boolean
}

// =============================================================================
// Snapshot Types
// =============================================================================

/**
 * A snapshot of entity state at a point in time.
 * Used to speed up event replay by starting from a known state.
 */
export interface SnapshotRecord {
  /** Target this snapshot is for (ns:id) */
  target: string
  /** Timestamp when snapshot was taken */
  ts: number
  /** State at snapshot time (Variant) */
  state: Variant | null
  /** Number of events that were replayed to create this snapshot */
  eventCount: number
  /** Event ID of the last event included in this snapshot */
  lastEventId: string
  /** When this snapshot was created */
  createdAt: number
}

/**
 * Manifest for tracking snapshots per entity
 */
export interface SnapshotManifest {
  /** Manifest version */
  version: 1
  /** Target this manifest is for (ns:id) */
  target: string
  /** List of snapshots (newest first) */
  snapshots: SnapshotRecord[]
  /** Maximum number of snapshots to retain */
  maxSnapshots: number
  /** When manifest was last updated */
  updatedAt: number
}

// =============================================================================
// Event Versioning Types
// =============================================================================

/**
 * Metadata for event schema versioning
 */
export interface EventVersionMetadata {
  /** Schema version when this event was created */
  schemaVersion: number
  /** If event was upgraded, the original version */
  upgradedFrom?: number
}

/**
 * Configuration for event versioning and schema evolution
 */
export interface EventVersioningConfig {
  /** Current event schema version */
  currentVersion: number
  /** Minimum supported version for reading */
  minVersion?: number
  /** Whether to fail on unknown/unsupported versions */
  strict?: boolean
}

// =============================================================================
// Event Sourcing Configuration
// =============================================================================

/**
 * Configuration for event sourcing mode
 */
export interface EventSourcingConfig {
  /** Enable snapshot creation during replay (default: true) */
  enableSnapshots?: boolean
  /** Number of events before creating a snapshot (default: 100) */
  snapshotThreshold?: number
  /** Maximum snapshots to retain per entity (default: 3) */
  maxSnapshotsPerEntity?: number
  /** Event versioning configuration */
  versioning?: EventVersioningConfig
}
