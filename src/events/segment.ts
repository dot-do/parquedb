/**
 * R2 Segment Writer for Event Storage
 *
 * Writes event batches as Parquet segments to R2 for bulk operations.
 * Each segment is a self-contained Parquet file with event data.
 *
 * Segment naming: events/seg-{seq}.parquet
 * where seq is a zero-padded sequence number (e.g., seg-0001.parquet)
 */

import type { Event } from '../types/entity'
import type { EventBatch, EventSegment } from './types'
import { tryParseJson, logger } from '../utils'

// =============================================================================
// Types
// =============================================================================

/**
 * R2-like storage interface for writing segments
 */
export interface SegmentStorage {
  /** Write a file to storage */
  put(key: string, data: Uint8Array | ArrayBuffer): Promise<void>
  /** Read a file from storage */
  get(key: string): Promise<Uint8Array | null>
  /** Check if a file exists */
  head(key: string): Promise<boolean>
  /** Delete a file */
  delete(key: string): Promise<void>
  /** List files with prefix */
  list(prefix: string): Promise<string[]>
}

/**
 * Options for the segment writer
 */
export interface SegmentWriterOptions {
  /** Dataset name (used for path prefix) */
  dataset: string
  /** Path prefix for segments (default: 'events') */
  prefix?: string
  /** Sequence number padding (default: 4 digits) */
  seqPadding?: number
}

/**
 * Default options
 */
const DEFAULT_OPTIONS = {
  prefix: 'events',
  seqPadding: 4,
}

// =============================================================================
// SegmentWriter Class
// =============================================================================

/**
 * Writes event batches as Parquet segments to R2 or compatible storage.
 *
 * @example
 * ```typescript
 * const writer = new SegmentWriter(r2Bucket, { dataset: 'my-app' })
 *
 * // Write a batch as a new segment
 * const segment = await writer.writeSegment(eventBatch)
 *
 * // Read a segment back
 * const batch = await writer.readSegment(segment)
 * ```
 */
export class SegmentWriter {
  private storage: SegmentStorage
  private options: Required<SegmentWriterOptions>
  private nextSeq: number = 1

  constructor(storage: SegmentStorage, options: SegmentWriterOptions) {
    this.storage = storage
    this.options = {
      ...DEFAULT_OPTIONS,
      ...options,
    } as Required<SegmentWriterOptions>
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Write an event batch as a new segment
   */
  async writeSegment(batch: EventBatch): Promise<EventSegment> {
    const seq = this.nextSeq++
    const path = this.getSegmentPath(seq)

    // Serialize batch to JSON-lines format (simple, portable)
    // In production, this would be Parquet for compression and efficiency
    const data = this.serializeBatch(batch)

    await this.storage.put(path, data)

    const segment: EventSegment = {
      seq,
      path,
      minTs: batch.minTs,
      maxTs: batch.maxTs,
      count: batch.count,
      sizeBytes: data.length,
      createdAt: Date.now(),
    }

    return segment
  }

  /**
   * Write multiple batches as a single segment
   */
  async writeSegmentFromBatches(batches: EventBatch[]): Promise<EventSegment> {
    // Merge batches into one
    const events: Event[] = []
    let minTs = Infinity
    let maxTs = -Infinity

    for (const batch of batches) {
      events.push(...batch.events)
      if (batch.minTs < minTs) minTs = batch.minTs
      if (batch.maxTs > maxTs) maxTs = batch.maxTs
    }

    const mergedBatch: EventBatch = {
      events,
      minTs,
      maxTs,
      count: events.length,
    }

    return this.writeSegment(mergedBatch)
  }

  /**
   * Read a segment back as an event batch
   */
  async readSegment(segment: EventSegment): Promise<EventBatch | null> {
    const data = await this.storage.get(segment.path)
    if (!data) return null

    return this.deserializeBatch(data, segment)
  }

  /**
   * Read a segment by sequence number
   */
  async readSegmentBySeq(seq: number): Promise<EventBatch | null> {
    const path = this.getSegmentPath(seq)
    const data = await this.storage.get(path)
    if (!data) return null

    // We don't have segment metadata, so reconstruct from data
    const batch = this.deserializeBatchWithoutMeta(data)
    return batch
  }

  /**
   * Delete a segment
   */
  async deleteSegment(segment: EventSegment): Promise<void> {
    await this.storage.delete(segment.path)
  }

  /**
   * Delete segments by sequence numbers
   */
  async deleteSegments(seqs: number[]): Promise<void> {
    await Promise.all(seqs.map(seq => {
      const path = this.getSegmentPath(seq)
      return this.storage.delete(path)
    }))
  }

  /**
   * List all segment paths
   */
  async listSegments(): Promise<string[]> {
    const prefix = `${this.options.dataset}/${this.options.prefix}/seg-`
    return this.storage.list(prefix)
  }

  /**
   * Check if a segment exists
   */
  async segmentExists(seq: number): Promise<boolean> {
    const path = this.getSegmentPath(seq)
    return this.storage.head(path)
  }

  /**
   * Set the next sequence number (for recovery)
   */
  setNextSeq(seq: number): void {
    this.nextSeq = seq
  }

  /**
   * Get the current next sequence number
   */
  getNextSeq(): number {
    return this.nextSeq
  }

  // ===========================================================================
  // Path Helpers
  // ===========================================================================

  /**
   * Get the path for a segment
   */
  getSegmentPath(seq: number): string {
    const paddedSeq = seq.toString().padStart(this.options.seqPadding, '0')
    return `${this.options.dataset}/${this.options.prefix}/seg-${paddedSeq}.parquet`
  }

  /**
   * Extract sequence number from a segment path
   */
  parseSegmentPath(path: string): number | null {
    const match = path.match(/seg-(\d+)\.parquet$/)
    if (!match) return null
    return parseInt(match[1]!, 10) // match[1] is guaranteed to exist after successful regex
  }

  // ===========================================================================
  // Serialization (JSON-lines for now, Parquet later)
  // ===========================================================================

  /**
   * Serialize a batch to bytes
   * Currently uses JSON-lines format for simplicity.
   * Parquet format would provide better compression and columnar access,
   * but JSON-lines is sufficient for the event log use case.
   */
  private serializeBatch(batch: EventBatch): Uint8Array {
    // JSON-lines format: one event per line
    const lines = batch.events.map(e => JSON.stringify(e))
    const json = lines.join('\n')
    return new TextEncoder().encode(json)
  }

  /**
   * Deserialize bytes back to a batch
   */
  private deserializeBatch(data: Uint8Array, segment: EventSegment): EventBatch {
    const json = new TextDecoder().decode(data)
    const lines = json.split('\n').filter(line => line.trim())
    const events: Event[] = lines
      .map(line => tryParseJson<Event>(line))
      .filter((e): e is Event => e !== undefined)

    if (events.length < lines.length) {
      logger.warn(`Skipped ${lines.length - events.length} malformed event lines during batch deserialization`)
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
   * Deserialize without segment metadata (recalculate from events)
   */
  private deserializeBatchWithoutMeta(data: Uint8Array): EventBatch {
    const json = new TextDecoder().decode(data)
    const lines = json.split('\n').filter(line => line.trim())
    const events: Event[] = lines
      .map(line => tryParseJson<Event>(line))
      .filter((e): e is Event => e !== undefined)

    if (events.length < lines.length) {
      logger.warn(`Skipped ${lines.length - events.length} malformed event lines during batch deserialization`)
    }

    let minTs = Infinity
    let maxTs = -Infinity
    for (const e of events) {
      if (e.ts < minTs) minTs = e.ts
      if (e.ts > maxTs) maxTs = e.ts
    }

    return {
      events,
      minTs: minTs === Infinity ? 0 : minTs,
      maxTs: maxTs === -Infinity ? 0 : maxTs,
      count: events.length,
      sizeBytes: data.length,
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a SegmentWriter instance
 */
export function createSegmentWriter(
  storage: SegmentStorage,
  options: SegmentWriterOptions
): SegmentWriter {
  return new SegmentWriter(storage, options)
}

/**
 * Create a flush handler for EventWriter that writes to R2 segments
 */
export function createSegmentFlushHandler(writer: SegmentWriter) {
  return async (batch: EventBatch): Promise<EventSegment> => {
    return writer.writeSegment(batch)
  }
}

// =============================================================================
// R2 Adapter
// =============================================================================

/**
 * Adapter to make R2Bucket compatible with SegmentStorage interface
 */
export function createR2Adapter(bucket: {
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<unknown>
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  head(key: string): Promise<unknown | null>
  delete(key: string): Promise<void>
  list(options: { prefix: string }): Promise<{ objects: { key: string }[] }>
}): SegmentStorage {
  return {
    async put(key: string, data: Uint8Array | ArrayBuffer): Promise<void> {
      await bucket.put(key, data)
    },

    async get(key: string): Promise<Uint8Array | null> {
      const obj = await bucket.get(key)
      if (!obj) return null
      const buffer = await obj.arrayBuffer()
      return new Uint8Array(buffer)
    },

    async head(key: string): Promise<boolean> {
      const result = await bucket.head(key)
      return result !== null
    },

    async delete(key: string): Promise<void> {
      await bucket.delete(key)
    },

    async list(prefix: string): Promise<string[]> {
      const result = await bucket.list({ prefix })
      return result.objects.map(obj => obj.key)
    },
  }
}
