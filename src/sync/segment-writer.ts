/**
 * Event Segment Writer
 *
 * Writes events to Parquet segment files with automatic partitioning
 * when segments exceed size limits. Computes checksums for content
 * addressing to enable deduplication across branches.
 */

import { parquetWriteBuffer } from 'hyparquet-writer'
import type { Event } from '../types/entity'
import type { StorageBackend } from '../types/storage'
import type { EventSegment } from './event-manifest'
import { createSegment } from './event-manifest'

// =============================================================================
// Configuration
// =============================================================================

/** Default maximum events per segment */
const DEFAULT_MAX_EVENTS_PER_SEGMENT = 10_000

/** Default segment file name prefix */
const DEFAULT_SEGMENT_PREFIX = 'seg-'

/** Directory for event segments */
const EVENTS_DIR = 'events'

// =============================================================================
// Options
// =============================================================================

export interface SegmentWriterOptions {
  /** Maximum events per segment (default: 10000) */
  maxEventsPerSegment?: number

  /** Segment file name prefix (default: 'seg-') */
  segmentPrefix?: string

  /** Compression codec (default: 'SNAPPY') */
  compression?: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'ZSTD' | 'LZ4'

  /** Base directory for segments (default: 'events') */
  baseDir?: string
}

// =============================================================================
// Single Segment Writer
// =============================================================================

/**
 * Write events to a new Parquet segment
 *
 * Creates a single segment file containing the provided events.
 * Computes metadata including checksums for content addressing.
 *
 * @param storage - Storage backend to write to
 * @param events - Events to write (must be sorted by timestamp/id)
 * @param opts - Writer options
 * @returns Segment metadata
 *
 * @example
 * ```typescript
 * const events = [event1, event2, event3]
 * const segment = await writeEventsToSegment(storage, events, {
 *   maxEventsPerSegment: 5000,
 *   compression: 'SNAPPY'
 * })
 * console.log(`Wrote ${segment.count} events to ${segment.file}`)
 * ```
 */
export async function writeEventsToSegment(
  storage: StorageBackend,
  events: Event[],
  opts: SegmentWriterOptions = {}
): Promise<EventSegment> {
  const {
    segmentPrefix = DEFAULT_SEGMENT_PREFIX,
    compression: _compression = 'SNAPPY',
    baseDir = EVENTS_DIR,
  } = opts

  if (events.length === 0) {
    throw new Error('Cannot write empty segment')
  }

  // Sort events by timestamp then ID for deterministic ordering
  const sortedEvents = [...events].sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts
    return a.id.localeCompare(b.id)
  })

  // Extract metadata
  const minId = sortedEvents[0]!.id
  const maxId = sortedEvents[sortedEvents.length - 1]!.id
  const minTs = sortedEvents[0]!.ts
  const maxTs = sortedEvents[sortedEvents.length - 1]!.ts
  const count = sortedEvents.length

  // Generate unique filename using timestamp and ID range
  const filename = `${segmentPrefix}${minTs}-${minId.slice(0, 6)}.parquet`
  const filepath = `${baseDir}/${filename}`

  // Convert events to column data for hyparquet-writer
  const columnData = [
    { name: 'id', data: sortedEvents.map(e => e.id) },
    { name: 'ts', data: sortedEvents.map(e => e.ts) },
    { name: 'op', data: sortedEvents.map(e => e.op) },
    { name: 'target', data: sortedEvents.map(e => e.target) },
    { name: 'before', data: sortedEvents.map(e => e.before) },
    { name: 'after', data: sortedEvents.map(e => e.after) },
    { name: 'actor', data: sortedEvents.map(e => e.actor) },
    { name: 'metadata', data: sortedEvents.map(e => e.metadata) },
  ]

  // Write events to Parquet using hyparquet-writer
  const parquetBuffer = parquetWriteBuffer({ columnData })
  const parquetData = new Uint8Array(parquetBuffer)

  // Write file to storage
  await storage.write(filepath, parquetData)

  // Create segment metadata with checksum
  return createSegment(
    filepath,
    minId,
    maxId,
    minTs,
    maxTs,
    count,
    parquetData
  )
}

// =============================================================================
// Multi-Segment Writer
// =============================================================================

/**
 * Write events across multiple segments if needed
 *
 * Automatically partitions events into segments based on size limits.
 * Events within each segment are sorted by timestamp/id.
 *
 * @param storage - Storage backend to write to
 * @param events - Events to write (will be sorted)
 * @param opts - Writer options
 * @returns Array of segment metadata (one per segment created)
 *
 * @example
 * ```typescript
 * // Write 25000 events, automatically creating 3 segments of ~8333 events each
 * const segments = await writeEvents(storage, allEvents, {
 *   maxEventsPerSegment: 10000
 * })
 * console.log(`Created ${segments.length} segments`)
 * ```
 */
export async function writeEvents(
  storage: StorageBackend,
  events: Event[],
  opts: SegmentWriterOptions = {}
): Promise<EventSegment[]> {
  const {
    maxEventsPerSegment = DEFAULT_MAX_EVENTS_PER_SEGMENT,
  } = opts

  if (events.length === 0) {
    return []
  }

  // Sort all events by timestamp then ID
  const sortedEvents = [...events].sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts
    return a.id.localeCompare(b.id)
  })

  // Split into chunks
  const segments: EventSegment[] = []
  for (let i = 0; i < sortedEvents.length; i += maxEventsPerSegment) {
    const chunk = sortedEvents.slice(i, i + maxEventsPerSegment)
    const segment = await writeEventsToSegment(storage, chunk, opts)
    segments.push(segment)
  }

  return segments
}

// =============================================================================
// Append Writer
// =============================================================================

/**
 * Append events to existing segments
 *
 * If the last segment is below the size threshold, appends to it.
 * Otherwise creates new segments.
 *
 * @param storage - Storage backend
 * @param events - New events to append
 * @param existingSegments - Current segments
 * @param opts - Writer options
 * @returns Updated list of segments (includes existing + new)
 */
export async function appendEvents(
  storage: StorageBackend,
  events: Event[],
  existingSegments: EventSegment[],
  opts: SegmentWriterOptions = {}
): Promise<EventSegment[]> {
  const {
    maxEventsPerSegment = DEFAULT_MAX_EVENTS_PER_SEGMENT,
  } = opts

  if (events.length === 0) {
    return existingSegments
  }

  // Check if we can append to the last segment
  const lastSegment = existingSegments[existingSegments.length - 1]
  const canAppend = lastSegment && lastSegment.count < maxEventsPerSegment

  if (canAppend) {
    // Read existing events from last segment
    const { readSegmentEvents } = await import('./segment-reader')
    const existingEvents: Event[] = []
    for await (const event of readSegmentEvents(storage, lastSegment)) {
      existingEvents.push(event)
    }

    // Combine with new events
    const combinedEvents = [...existingEvents, ...events]

    // If still under threshold, rewrite as single segment
    if (combinedEvents.length <= maxEventsPerSegment) {
      const newSegment = await writeEventsToSegment(storage, combinedEvents, opts)

      // Delete old segment
      await storage.delete(lastSegment.file)

      return [
        ...existingSegments.slice(0, -1),
        newSegment,
      ]
    }

    // Too big - write new events as separate segments
    const newSegments = await writeEvents(storage, events, opts)
    return [...existingSegments, ...newSegments]
  }

  // Can't append - write new segments
  const newSegments = await writeEvents(storage, events, opts)
  return [...existingSegments, ...newSegments]
}

// =============================================================================
// Batch Writer
// =============================================================================

/**
 * Write events incrementally in batches
 *
 * Useful for streaming large numbers of events without loading
 * them all into memory at once.
 *
 * @param storage - Storage backend
 * @param opts - Writer options
 * @returns Writer interface with add() and flush() methods
 *
 * @example
 * ```typescript
 * const writer = createBatchWriter(storage)
 * for await (const event of eventStream) {
 *   await writer.add(event)
 * }
 * const segments = await writer.flush()
 * ```
 */
export function createBatchWriter(
  storage: StorageBackend,
  opts: SegmentWriterOptions = {}
) {
  const {
    maxEventsPerSegment = DEFAULT_MAX_EVENTS_PER_SEGMENT,
  } = opts

  let buffer: Event[] = []
  const segments: EventSegment[] = []

  return {
    /**
     * Add an event to the buffer
     * Automatically flushes if buffer reaches max size
     */
    async add(event: Event): Promise<void> {
      buffer.push(event)

      if (buffer.length >= maxEventsPerSegment) {
        await this.flush()
      }
    },

    /**
     * Add multiple events to the buffer
     */
    async addMany(events: Event[]): Promise<void> {
      buffer.push(...events)

      while (buffer.length >= maxEventsPerSegment) {
        const chunk = buffer.splice(0, maxEventsPerSegment)
        const segment = await writeEventsToSegment(storage, chunk, opts)
        segments.push(segment)
      }
    },

    /**
     * Flush remaining events to storage
     * @returns All segments written so far
     */
    async flush(): Promise<EventSegment[]> {
      if (buffer.length > 0) {
        const segment = await writeEventsToSegment(storage, buffer, opts)
        segments.push(segment)
        buffer = []
      }

      return segments
    },

    /**
     * Get current buffer size
     */
    get bufferSize(): number {
      return buffer.length
    },

    /**
     * Get number of segments written
     */
    get segmentCount(): number {
      return segments.length
    },
  }
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate that events are properly sorted
 *
 * @param events - Events to validate
 * @returns True if sorted correctly
 */
export function validateEventOrder(events: Event[]): boolean {
  for (let i = 1; i < events.length; i++) {
    const prev = events[i - 1]!
    const curr = events[i]!

    if (curr.ts < prev.ts) return false
    if (curr.ts === prev.ts && curr.id < prev.id) return false
  }

  return true
}

/**
 * Deduplicate events by ID
 *
 * Keeps the first occurrence of each event ID
 *
 * @param events - Events to deduplicate
 * @returns Deduplicated events
 */
export function deduplicateEvents(events: Event[]): Event[] {
  const seen = new Set<string>()
  const deduplicated: Event[] = []

  for (const event of events) {
    if (!seen.has(event.id)) {
      seen.add(event.id)
      deduplicated.push(event)
    }
  }

  return deduplicated
}
