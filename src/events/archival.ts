/**
 * Event Archival
 *
 * Moves old event segments to cold storage for long-term retention.
 * Unlike compaction, archival preserves the original events without
 * replaying them.
 *
 * Archive structure:
 *   {dataset}/events/archive/{year}/{month}/seg-{seq}.parquet
 *
 * This module provides:
 * - EventArchiver: Main archival engine
 * - Archival policy configuration
 * - Restore functionality
 */

import type { EventSegment } from './types'
import type { SegmentStorage } from './segment'
import type { ManifestManager } from './manifest'
import { logger as _logger } from '../utils/logger'
import { DEFAULT_ARCHIVE_AFTER_DAYS, DEFAULT_RETENTION_DAYS } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Policy for event archival
 */
export interface ArchivalPolicy {
  /** Days after which segments are eligible for archival (default: 7) */
  archiveAfterDays?: number
  /** Days to retain archived segments before purging (default: 365) */
  retentionDays?: number
}

/**
 * Resolved archival policy with all values defined
 */
interface ResolvedArchivalPolicy {
  archiveAfterDays: number
  retentionDays: number
}

/**
 * Options for the archiver
 */
export interface ArchiverOptions {
  /** Dataset name */
  dataset: string
  /** Storage backend */
  storage: SegmentStorage
  /** Manifest manager */
  manifest: ManifestManager
  /** Archival policy */
  policy?: ArchivalPolicy | undefined
  /** Path prefix for events (default: 'events') */
  prefix?: string | undefined
}

/**
 * Options for archive operation
 */
export interface ArchiveOptions {
  /** Maximum number of segments to archive in one operation */
  maxSegments?: number | undefined
  /** If true, only report what would be archived without making changes */
  dryRun?: boolean | undefined
}

/**
 * Result of an archival operation
 */
export interface ArchivalResult {
  /** Number of segments successfully archived */
  segmentsArchived: number
  /** Number of segments that failed to archive */
  segmentsFailed: number
  /** Paths of archived segments */
  archivedPaths: string[]
  /** Errors encountered during archival */
  errors: Array<{ segment: EventSegment; error: string }>
  /** Duration in milliseconds */
  durationMs: number
  /** Whether this was a dry run */
  dryRun?: boolean | undefined
}

/**
 * Options for listing archived segments
 */
export interface ListArchivedOptions {
  /** Filter by year */
  year?: number | undefined
  /** Filter by month (1-12) */
  month?: number | undefined
}

/**
 * Result of a purge operation
 */
export interface PurgeResult {
  /** Number of archived segments purged */
  purgedCount: number
  /** Paths that were purged */
  purgedPaths: string[]
  /** Duration in milliseconds */
  durationMs: number
  /** Whether this was a dry run */
  dryRun?: boolean | undefined
}

/**
 * Default archival policy
 */
const DEFAULT_POLICY: Required<ArchivalPolicy> = {
  archiveAfterDays: DEFAULT_ARCHIVE_AFTER_DAYS,
  retentionDays: DEFAULT_RETENTION_DAYS,
}

/**
 * Default options
 */
const DEFAULT_OPTIONS = {
  prefix: 'events',
}

// =============================================================================
// EventArchiver Class
// =============================================================================

/**
 * Archives old event segments to cold storage.
 *
 * @example
 * ```typescript
 * const archiver = new EventArchiver({
 *   dataset: 'my-app',
 *   storage,
 *   manifest,
 *   policy: { archiveAfterDays: 7, retentionDays: 365 },
 * })
 *
 * // Archive old segments
 * const result = await archiver.archive()
 * console.log(`Archived ${result.segmentsArchived} segments`)
 *
 * // List archived segments
 * const archived = await archiver.listArchived({ year: 2024 })
 *
 * // Restore a segment
 * await archiver.restore('my-app/events/archive/2024/01/seg-0001.parquet')
 * ```
 */
export class EventArchiver {
  private dataset: string
  private storage: SegmentStorage
  private manifest: ManifestManager
  private policy: ResolvedArchivalPolicy
  private prefix: string

  constructor(options: ArchiverOptions) {
    this.dataset = options.dataset
    this.storage = options.storage
    this.manifest = options.manifest
    this.policy = {
      archiveAfterDays: options.policy?.archiveAfterDays ?? DEFAULT_POLICY.archiveAfterDays,
      retentionDays: options.policy?.retentionDays ?? DEFAULT_POLICY.retentionDays,
    }
    this.prefix = options.prefix ?? DEFAULT_OPTIONS.prefix
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Get segments eligible for archival
   */
  async getArchivableSegments(): Promise<EventSegment[]> {
    const segments = await this.manifest.getSegments()
    const threshold = Date.now() - this.policy.archiveAfterDays * 24 * 60 * 60 * 1000

    return segments.filter(s => s.maxTs < threshold)
  }

  /**
   * Get the archive path for a segment
   */
  getArchivePath(segment: EventSegment): string {
    const date = new Date(segment.minTs)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const seqPadded = segment.seq.toString().padStart(4, '0')

    return `${this.dataset}/${this.prefix}/archive/${year}/${month}/seg-${seqPadded}.parquet`
  }

  /**
   * Archive eligible segments to cold storage
   */
  async archive(options: ArchiveOptions = {}): Promise<ArchivalResult> {
    const startTime = Date.now()
    const { maxSegments, dryRun = false } = options

    let archivableSegments = await this.getArchivableSegments()

    if (maxSegments !== undefined && archivableSegments.length > maxSegments) {
      archivableSegments = archivableSegments.slice(0, maxSegments)
    }

    const result: ArchivalResult = {
      segmentsArchived: 0,
      segmentsFailed: 0,
      archivedPaths: [],
      errors: [],
      durationMs: 0,
      dryRun,
    }

    for (const segment of archivableSegments) {
      try {
        const archivePath = this.getArchivePath(segment)

        if (!dryRun) {
          // Read segment data
          const data = await this.storage.get(segment.path)
          if (!data) {
            throw new Error(`Segment data not found: ${segment.path}`)
          }

          // Write to archive path
          await this.storage.put(archivePath, data)

          // Delete original
          await this.storage.delete(segment.path)

          // Remove from manifest
          await this.manifest.removeSegment(segment.seq)
          await this.manifest.save()
        }

        result.segmentsArchived++
        result.archivedPaths.push(archivePath)
      } catch (err) {
        result.segmentsFailed++
        result.errors.push({
          segment,
          error: err instanceof Error ? err.message : String(err),
        })
      }
    }

    result.durationMs = Date.now() - startTime
    return result
  }

  /**
   * List archived segment paths
   */
  async listArchived(options: ListArchivedOptions = {}): Promise<string[]> {
    const { year, month } = options

    let prefix = `${this.dataset}/${this.prefix}/archive/`

    if (year !== undefined) {
      prefix += `${year}/`
      if (month !== undefined) {
        prefix += `${String(month).padStart(2, '0')}/`
      }
    }

    return this.storage.list(prefix)
  }

  /**
   * Restore an archived segment to active storage
   */
  async restore(archivePath: string): Promise<void> {
    // Validate archive path
    if (!archivePath.includes('/archive/')) {
      throw new Error(`Invalid archive path: ${archivePath}`)
    }

    // Read archived data
    const data = await this.storage.get(archivePath)
    if (!data) {
      throw new Error(`Archive not found: ${archivePath}`)
    }

    // Extract sequence number from path
    const match = archivePath.match(/seg-(\d+)\.parquet$/)
    if (!match) {
      throw new Error(`Invalid archive path format: ${archivePath}`)
    }
    const seq = parseInt(match[1]!, 10)
    const seqPadded = seq.toString().padStart(4, '0')
    const activePath = `${this.dataset}/${this.prefix}/seg-${seqPadded}.parquet`

    // Write to active path
    await this.storage.put(activePath, data)

    // Delete from archive
    await this.storage.delete(archivePath)

    // Reconstruct segment metadata from data
    const segmentInfo = this.parseSegmentData(data, seq, activePath)

    // Add to manifest
    await this.manifest.addSegment(segmentInfo)
    await this.manifest.save()
  }

  /**
   * Purge archived segments older than retention period
   */
  async purgeOldArchives(options: { dryRun?: boolean | undefined } = {}): Promise<PurgeResult> {
    const startTime = Date.now()
    const { dryRun = false } = options

    const result: PurgeResult = {
      purgedCount: 0,
      purgedPaths: [],
      durationMs: 0,
      dryRun,
    }

    const threshold = Date.now() - this.policy.retentionDays * 24 * 60 * 60 * 1000
    const thresholdDate = new Date(threshold)
    const thresholdYear = thresholdDate.getUTCFullYear()
    const thresholdMonth = thresholdDate.getUTCMonth() + 1

    // List all archived segments
    const allArchived = await this.listArchived()

    for (const path of allArchived) {
      // Parse year/month from path
      const match = path.match(/\/archive\/(\d{4})\/(\d{2})\//)
      if (!match) continue

      const archiveYear = parseInt(match[1]!, 10)
      const archiveMonth = parseInt(match[2]!, 10)

      // Check if this archive is older than retention
      if (
        archiveYear < thresholdYear ||
        (archiveYear === thresholdYear && archiveMonth < thresholdMonth)
      ) {
        if (!dryRun) {
          await this.storage.delete(path)
        }
        result.purgedCount++
        result.purgedPaths.push(path)
      }
    }

    result.durationMs = Date.now() - startTime
    return result
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Parse segment data to extract metadata
   */
  private parseSegmentData(
    data: Uint8Array,
    seq: number,
    path: string
  ): EventSegment {
    const json = new TextDecoder().decode(data)
    const lines = json.split('\n').filter(line => line.trim())

    let minTs = Infinity
    let maxTs = -Infinity

    for (const line of lines) {
      try {
        const event = JSON.parse(line)
        if (typeof event.ts === 'number') {
          if (event.ts < minTs) minTs = event.ts
          if (event.ts > maxTs) maxTs = event.ts
        }
      } catch {
        // Intentionally ignored: invalid JSON lines in event segments are skipped during timestamp parsing
      }
    }

    return {
      seq,
      path,
      minTs: minTs === Infinity ? Date.now() : minTs,
      maxTs: maxTs === -Infinity ? Date.now() : maxTs,
      count: lines.length,
      sizeBytes: data.length,
      createdAt: Date.now(),
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an EventArchiver instance
 */
export function createEventArchiver(options: ArchiverOptions): EventArchiver {
  return new EventArchiver(options)
}
