/**
 * Event Manifest Manager
 *
 * Tracks event segments with a manifest file that maintains:
 * - Ordered list of segments (oldest first)
 * - Min/max timestamps per segment
 * - Event counts
 * - Compaction watermark
 *
 * Manifest path: {dataset}/events/_manifest.json
 */

import type { EventManifest, EventSegment } from './types'
import type { SegmentStorage } from './segment'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for the manifest manager
 */
export interface ManifestManagerOptions {
  /** Dataset name */
  dataset: string
  /** Path prefix for events (default: 'events') */
  prefix?: string | undefined
}

/**
 * Default options
 */
const DEFAULT_OPTIONS = {
  prefix: 'events',
}

/**
 * Summary of manifest state
 */
export interface ManifestSummary {
  /** Total segments */
  segmentCount: number
  /** Total events across all segments */
  totalEvents: number
  /** Earliest event timestamp */
  minTs: number | null
  /** Latest event timestamp */
  maxTs: number | null
  /** Compaction watermark (events before this are in data/rels.parquet) */
  compactedThrough: number | null
  /** Size of all segments in bytes */
  totalSizeBytes: number
}

// =============================================================================
// ManifestManager Class
// =============================================================================

/**
 * Manages the events manifest file.
 *
 * @example
 * ```typescript
 * const manager = new ManifestManager(storage, { dataset: 'my-app' })
 *
 * // Load or create manifest
 * const manifest = await manager.load()
 *
 * // Add a new segment
 * await manager.addSegment(segment)
 *
 * // Get segments in time range
 * const segments = manager.getSegmentsInRange(minTs, maxTs)
 * ```
 */
export class ManifestManager {
  private storage: SegmentStorage
  private options: Required<ManifestManagerOptions>
  private manifest: EventManifest | null = null
  private dirty = false

  constructor(storage: SegmentStorage, options: ManifestManagerOptions) {
    this.storage = storage
    this.options = {
      ...DEFAULT_OPTIONS,
      ...options,
    } as Required<ManifestManagerOptions>
  }

  // ===========================================================================
  // Load / Save
  // ===========================================================================

  /**
   * Get the manifest path
   */
  getManifestPath(): string {
    return `${this.options.dataset}/${this.options.prefix}/_manifest.json`
  }

  /**
   * Load the manifest from storage, or create a new one if it doesn't exist
   */
  async load(): Promise<EventManifest> {
    if (this.manifest) {
      return this.manifest
    }

    const path = this.getManifestPath()
    const data = await this.storage.get(path)

    if (data) {
      const json = new TextDecoder().decode(data)
      try {
        this.manifest = JSON.parse(json) as EventManifest
      } catch {
        // Invalid manifest JSON - create new empty manifest
        this.manifest = this.createEmptyManifest()
      }
    } else {
      this.manifest = this.createEmptyManifest()
    }

    this.dirty = false
    return this.manifest
  }

  /**
   * Save the manifest to storage
   */
  async save(): Promise<void> {
    if (!this.manifest) {
      throw new Error('No manifest loaded')
    }

    this.manifest.updatedAt = Date.now()

    const path = this.getManifestPath()
    const json = JSON.stringify(this.manifest, null, 2)
    const data = new TextEncoder().encode(json)

    await this.storage.put(path, data)
    this.dirty = false
  }

  /**
   * Save only if there are unsaved changes
   */
  async saveIfDirty(): Promise<void> {
    if (this.dirty) {
      await this.save()
    }
  }

  /**
   * Check if there are unsaved changes
   */
  isDirty(): boolean {
    return this.dirty
  }

  // ===========================================================================
  // Segment Management
  // ===========================================================================

  /**
   * Add a segment to the manifest
   */
  async addSegment(segment: EventSegment): Promise<void> {
    const manifest = await this.load()

    // Find the right position to insert (maintain order by minTs)
    let insertIndex = manifest.segments.length
    for (let i = 0; i < manifest.segments.length; i++) {
      const seg = manifest.segments[i]
      if (seg && segment.minTs < seg.minTs) {
        insertIndex = i
        break
      }
    }

    manifest.segments.splice(insertIndex, 0, segment)
    manifest.totalEvents += segment.count

    // Update nextSeq if needed
    if (segment.seq >= manifest.nextSeq) {
      manifest.nextSeq = segment.seq + 1
    }

    this.dirty = true
  }

  /**
   * Remove a segment from the manifest
   */
  async removeSegment(seq: number): Promise<EventSegment | null> {
    const manifest = await this.load()

    const index = manifest.segments.findIndex(s => s.seq === seq)
    if (index === -1) return null

    const [removed] = manifest.segments.splice(index, 1)
    if (!removed) return null // splice returned empty array
    manifest.totalEvents -= removed.count

    this.dirty = true
    return removed
  }

  /**
   * Remove multiple segments
   */
  async removeSegments(seqs: number[]): Promise<EventSegment[]> {
    const manifest = await this.load()
    const removed: EventSegment[] = []

    for (const seq of seqs) {
      const index = manifest.segments.findIndex(s => s.seq === seq)
      if (index !== -1) {
        const [segment] = manifest.segments.splice(index, 1)
        if (segment) {
          manifest.totalEvents -= segment.count
          removed.push(segment)
        }
      }
    }

    if (removed.length > 0) {
      this.dirty = true
    }

    return removed
  }

  /**
   * Get a segment by sequence number
   */
  async getSegment(seq: number): Promise<EventSegment | null> {
    const manifest = await this.load()
    return manifest.segments.find(s => s.seq === seq) ?? null
  }

  /**
   * Get all segments
   */
  async getSegments(): Promise<EventSegment[]> {
    const manifest = await this.load()
    return [...manifest.segments]
  }

  /**
   * Get segments that overlap with a time range
   */
  async getSegmentsInRange(minTs: number, maxTs: number): Promise<EventSegment[]> {
    const manifest = await this.load()

    return manifest.segments.filter(s =>
      // Segment overlaps if its range intersects with query range
      s.maxTs >= minTs && s.minTs <= maxTs
    )
  }

  /**
   * Get segments after a timestamp (for replay from a point)
   */
  async getSegmentsAfter(timestamp: number): Promise<EventSegment[]> {
    const manifest = await this.load()

    return manifest.segments.filter(s => s.maxTs >= timestamp)
  }

  /**
   * Get segments before a timestamp (for cleanup)
   */
  async getSegmentsBefore(timestamp: number): Promise<EventSegment[]> {
    const manifest = await this.load()

    return manifest.segments.filter(s => s.maxTs < timestamp)
  }

  // ===========================================================================
  // Compaction
  // ===========================================================================

  /**
   * Update the compaction watermark
   */
  async setCompactedThrough(timestamp: number): Promise<void> {
    const manifest = await this.load()
    manifest.compactedThrough = timestamp
    this.dirty = true
  }

  /**
   * Get the compaction watermark
   */
  async getCompactedThrough(): Promise<number | null> {
    const manifest = await this.load()
    return manifest.compactedThrough ?? null
  }

  /**
   * Get segments that can be compacted (before the watermark)
   */
  async getCompactableSegments(): Promise<EventSegment[]> {
    const manifest = await this.load()

    if (!manifest.compactedThrough) {
      return []
    }

    return manifest.segments.filter(s => s.maxTs <= manifest.compactedThrough!)
  }

  // ===========================================================================
  // Sequence Management
  // ===========================================================================

  /**
   * Get the next sequence number
   */
  async getNextSeq(): Promise<number> {
    const manifest = await this.load()
    return manifest.nextSeq
  }

  /**
   * Reserve a sequence number (increment and return the reserved number)
   */
  async reserveSeq(): Promise<number> {
    const manifest = await this.load()
    const seq = manifest.nextSeq++
    this.dirty = true
    return seq
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get a summary of the manifest state
   */
  async getSummary(): Promise<ManifestSummary> {
    const manifest = await this.load()

    let minTs: number | null = null
    let maxTs: number | null = null
    let totalSizeBytes = 0

    for (const segment of manifest.segments) {
      if (minTs === null || segment.minTs < minTs) minTs = segment.minTs
      if (maxTs === null || segment.maxTs > maxTs) maxTs = segment.maxTs
      totalSizeBytes += segment.sizeBytes
    }

    return {
      segmentCount: manifest.segments.length,
      totalEvents: manifest.totalEvents,
      minTs,
      maxTs,
      compactedThrough: manifest.compactedThrough ?? null,
      totalSizeBytes,
    }
  }

  // ===========================================================================
  // Internal
  // ===========================================================================

  /**
   * Create an empty manifest
   */
  private createEmptyManifest(): EventManifest {
    return {
      version: 1,
      dataset: this.options.dataset,
      segments: [],
      nextSeq: 1,
      totalEvents: 0,
      updatedAt: Date.now(),
    }
  }

  /**
   * Get the loaded manifest (for testing)
   */
  getLoadedManifest(): EventManifest | null {
    return this.manifest
  }

  /**
   * Clear the cached manifest (for testing)
   */
  clearCache(): void {
    this.manifest = null
    this.dirty = false
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a ManifestManager instance
 */
export function createManifestManager(
  storage: SegmentStorage,
  options: ManifestManagerOptions
): ManifestManager {
  return new ManifestManager(storage, options)
}
