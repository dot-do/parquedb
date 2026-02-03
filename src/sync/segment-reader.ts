/**
 * Event Segment Reader
 *
 * Streams events from Parquet segment files. Uses async generators
 * to avoid loading entire segments into memory, enabling efficient
 * processing of large event logs.
 */

import type { Event } from '../types/entity'
import type { StorageBackend } from '../types/storage'
import type { EventSegment } from './event-manifest'
import { ParquetReader } from '../parquet/reader'

// =============================================================================
// Single Segment Reader
// =============================================================================

/**
 * Read events from a single Parquet segment file
 *
 * Streams events to avoid loading entire segment into memory.
 * Events are returned in the order they appear in the file.
 *
 * @param storage - Storage backend to read from
 * @param segment - Segment metadata
 * @yields Events from the segment
 *
 * @example
 * ```typescript
 * for await (const event of readSegmentEvents(storage, segment)) {
 *   console.log(event.id, event.op, event.target)
 * }
 * ```
 */
export async function* readSegmentEvents(
  storage: StorageBackend,
  segment: EventSegment
): AsyncGenerator<Event> {
  // Create a reader for this storage backend
  const reader = new ParquetReader({ storage })

  // Stream events using ParquetReader
  for await (const row of reader.stream(segment.file)) {
    yield normalizeEvent(row)
  }
}

/**
 * Normalize a Parquet row into an Event
 *
 * Handles type conversions and field mapping from Parquet storage
 * to the Event interface.
 *
 * @param row - Raw row from Parquet file
 * @returns Typed Event object
 */
function normalizeEvent(row: Record<string, unknown>): Event {
  return {
    id: row.id as string,
    ts: row.ts as number,
    op: row.op as Event['op'],
    target: row.target as string,
    before: row.before as Event['before'],
    after: row.after as Event['after'],
    actor: row.actor as string | undefined,
    metadata: row.metadata as Event['metadata'],
  }
}

// =============================================================================
// Multiple Segment Reader
// =============================================================================

/**
 * Read events from multiple segments in order
 *
 * Assumes segments are ordered chronologically (oldest first).
 * Streams events from each segment in sequence.
 *
 * @param storage - Storage backend to read from
 * @param segments - Ordered list of segments
 * @yields Events from all segments in order
 *
 * @example
 * ```typescript
 * const segments = manifest.segments
 * for await (const event of readEventsFromSegments(storage, segments)) {
 *   // Process events chronologically
 * }
 * ```
 */
export async function* readEventsFromSegments(
  storage: StorageBackend,
  segments: EventSegment[]
): AsyncGenerator<Event> {
  for (const segment of segments) {
    yield* readSegmentEvents(storage, segment)
  }
}

// =============================================================================
// Range Reader
// =============================================================================

/**
 * Read events within a timestamp range
 *
 * Only reads from segments that overlap the specified time range.
 * Filters individual events to ensure they fall within the range.
 *
 * @param storage - Storage backend to read from
 * @param segments - All available segments (will be filtered)
 * @param minTs - Minimum timestamp (inclusive, ms since epoch)
 * @param maxTs - Maximum timestamp (inclusive, ms since epoch)
 * @yields Events within the time range
 *
 * @example
 * ```typescript
 * // Read events from last hour
 * const now = Date.now()
 * const oneHourAgo = now - 3600_000
 * for await (const event of readEventsInRange(storage, segments, oneHourAgo, now)) {
 *   // Process recent events
 * }
 * ```
 */
export async function* readEventsInRange(
  storage: StorageBackend,
  segments: EventSegment[],
  minTs: number,
  maxTs: number
): AsyncGenerator<Event> {
  // Filter to segments that overlap the time range
  const relevantSegments = segments.filter(seg => {
    // Segment overlaps if:
    // - Segment starts before range ends AND
    // - Segment ends after range starts
    return seg.minTs <= maxTs && seg.maxTs >= minTs
  })

  // Read events from relevant segments
  for (const segment of relevantSegments) {
    for await (const event of readSegmentEvents(storage, segment)) {
      // Filter events that fall within the requested range
      if (event.ts >= minTs && event.ts <= maxTs) {
        yield event
      }
    }
  }
}

// =============================================================================
// Batch Reader
// =============================================================================

/**
 * Read events from segments in batches
 *
 * Useful for processing events in chunks rather than one at a time.
 *
 * @param storage - Storage backend to read from
 * @param segments - Segments to read from
 * @param batchSize - Number of events per batch
 * @yields Batches of events
 *
 * @example
 * ```typescript
 * for await (const batch of readEventBatches(storage, segments, 1000)) {
 *   await processBatch(batch) // Process 1000 events at a time
 * }
 * ```
 */
export async function* readEventBatches(
  storage: StorageBackend,
  segments: EventSegment[],
  batchSize: number
): AsyncGenerator<Event[]> {
  let batch: Event[] = []

  for await (const event of readEventsFromSegments(storage, segments)) {
    batch.push(event)

    if (batch.length >= batchSize) {
      yield batch
      batch = []
    }
  }

  // Yield remaining events
  if (batch.length > 0) {
    yield batch
  }
}

// =============================================================================
// Event ID Range Reader
// =============================================================================

/**
 * Read events within an ID range (using ULID ordering)
 *
 * ULIDs are lexicographically sortable, so we can filter by ID range
 * to get events between two points.
 *
 * @param storage - Storage backend to read from
 * @param segments - All available segments (will be filtered)
 * @param minId - Minimum event ID (inclusive)
 * @param maxId - Maximum event ID (inclusive)
 * @yields Events within the ID range
 *
 * @example
 * ```typescript
 * // Read events between two checkpoints
 * for await (const event of readEventsInIdRange(
 *   storage,
 *   segments,
 *   lastProcessedId,
 *   currentId
 * )) {
 *   // Process new events
 * }
 * ```
 */
export async function* readEventsInIdRange(
  storage: StorageBackend,
  segments: EventSegment[],
  minId: string,
  maxId: string
): AsyncGenerator<Event> {
  // Filter to segments that overlap the ID range
  const relevantSegments = segments.filter(seg => {
    // Segment overlaps if:
    // - Segment starts before range ends AND
    // - Segment ends after range starts
    return seg.minId <= maxId && seg.maxId >= minId
  })

  // Read events from relevant segments
  for (const segment of relevantSegments) {
    for await (const event of readSegmentEvents(storage, segment)) {
      // Filter events that fall within the requested range
      if (event.id >= minId && event.id <= maxId) {
        yield event
      }
    }
  }
}

// =============================================================================
// Statistics
// =============================================================================

/**
 * Count events in segments
 *
 * Fast path: if segments have accurate counts, sum them.
 * Slow path: actually read and count events.
 *
 * @param storage - Storage backend to read from
 * @param segments - Segments to count
 * @param accurate - If true, reads and counts events; if false, uses segment metadata
 * @returns Total event count
 */
export async function countEvents(
  storage: StorageBackend,
  segments: EventSegment[],
  accurate = false
): Promise<number> {
  if (!accurate) {
    // Fast path: sum segment counts
    return segments.reduce((total, seg) => total + seg.count, 0)
  }

  // Slow path: actually count events
  let count = 0
  for await (const _event of readEventsFromSegments(storage, segments)) {
    count++
  }
  return count
}

/**
 * Get statistics about segments
 *
 * @param segments - Segments to analyze
 * @returns Summary statistics
 */
export function getSegmentStats(segments: EventSegment[]): {
  totalSegments: number
  totalEvents: number
  totalSize: number
  oldestEvent: number
  newestEvent: number
  avgEventsPerSegment: number
} {
  if (segments.length === 0) {
    return {
      totalSegments: 0,
      totalEvents: 0,
      totalSize: 0,
      oldestEvent: 0,
      newestEvent: 0,
      avgEventsPerSegment: 0,
    }
  }

  const totalEvents = segments.reduce((sum, seg) => sum + seg.count, 0)
  const oldestEvent = Math.min(...segments.map(seg => seg.minTs))
  const newestEvent = Math.max(...segments.map(seg => seg.maxTs))

  return {
    totalSegments: segments.length,
    totalEvents,
    totalSize: 0, // Not tracked in current EventSegment interface
    oldestEvent,
    newestEvent,
    avgEventsPerSegment: totalEvents / segments.length,
  }
}
