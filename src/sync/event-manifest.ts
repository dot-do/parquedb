/**
 * Event Manifest Management
 *
 * Tracks Parquet segment files for event storage, including metadata
 * for efficient querying and merge operations. Segments are content-addressed
 * by SHA256 checksum to enable deduplication across branches.
 */

import type { StorageBackend } from '../types/storage'
import { sha256 } from './hash'

// =============================================================================
// Types
// =============================================================================

/**
 * Metadata for a single event segment (Parquet file)
 */
export interface EventSegment {
  /** Segment file path in storage (e.g., "events/seg-001.parquet") */
  file: string

  /** First event ULID in segment (for range queries) */
  minId: string

  /** Last event ULID in segment (for range queries) */
  maxId: string

  /** Minimum timestamp in segment (ms since epoch) */
  minTs: number

  /** Maximum timestamp in segment (ms since epoch) */
  maxTs: number

  /** Number of events in segment */
  count: number

  /** SHA256 checksum for content addressing (enables deduplication) */
  checksum: string
}

/**
 * Manifest tracking all event segments for a branch
 */
export interface EventManifest {
  /** Manifest version (for schema evolution) */
  version: 1

  /** Ordered list of segments (oldest first) */
  segments: EventSegment[]

  /** Current branch name */
  branch: string

  /** Current HEAD commit ID */
  headCommit: string

  /** ID of last event in manifest (for append detection) */
  lastEventId: string

  /** Timestamp of last event in manifest */
  lastEventTs: number

  /** Tracking of previous merges (for three-way merge) */
  mergedFrom?: Array<{
    /** Branch that was merged in */
    branch: string
    /** Commit ID that was merged */
    commit: string
    /** Last segment from that branch that was included */
    upToSegment: string
  }>
}

// =============================================================================
// Constants
// =============================================================================

/** Default manifest file path */
export const MANIFEST_PATH = 'events/manifest.json'

/** Current manifest schema version */
const CURRENT_VERSION = 1

// =============================================================================
// Manifest Operations
// =============================================================================

/**
 * Load event manifest from storage
 *
 * @param storage - Storage backend to read from
 * @param path - Path to manifest file (default: events/manifest.json)
 * @returns Parsed manifest or empty manifest if not found
 */
export async function loadManifest(
  storage: StorageBackend,
  path: string = MANIFEST_PATH
): Promise<EventManifest> {
  try {
    const data = await storage.read(path)
    const text = new TextDecoder().decode(data)
    const manifest = JSON.parse(text) as EventManifest

    // Validate version
    if (manifest.version !== CURRENT_VERSION) {
      throw new Error(
        `Unsupported manifest version ${manifest.version}. Expected ${CURRENT_VERSION}`
      )
    }

    return manifest
  } catch (err: unknown) {
    // If file doesn't exist, return empty manifest
    if (err && typeof err === 'object' && 'code' in err) {
      if ((err as { code?: string }).code === 'ENOENT') {
        return createEmptyManifest('main')
      }
    }

    throw err
  }
}

/**
 * Save event manifest to storage
 *
 * @param storage - Storage backend to write to
 * @param manifest - Manifest to save
 * @param path - Path to manifest file (default: events/manifest.json)
 */
export async function saveManifest(
  storage: StorageBackend,
  manifest: EventManifest,
  path: string = MANIFEST_PATH
): Promise<void> {
  // Sort segments by minTs for consistency
  const sortedManifest: EventManifest = {
    ...manifest,
    segments: [...manifest.segments].sort((a, b) => {
      if (a.minTs !== b.minTs) return a.minTs - b.minTs
      return a.minId.localeCompare(b.minId)
    }),
  }

  const text = JSON.stringify(sortedManifest, null, 2)
  const data = new TextEncoder().encode(text)

  // Ensure events directory exists
  await storage.mkdir('events').catch(() => {
    // Ignore if already exists
  })

  // Write atomically if supported
  if ('writeAtomic' in storage && typeof storage.writeAtomic === 'function') {
    await storage.writeAtomic(path, data)
  } else {
    await storage.write(path, data)
  }
}

/**
 * Create an empty manifest for a new branch
 *
 * @param branch - Branch name (e.g., "main", "feature-x")
 * @returns Empty manifest
 */
export function createEmptyManifest(branch: string): EventManifest {
  return {
    version: CURRENT_VERSION,
    segments: [],
    branch,
    headCommit: '',
    lastEventId: '',
    lastEventTs: 0,
  }
}

// =============================================================================
// Segment Operations
// =============================================================================

/**
 * Add a segment to the manifest
 *
 * @param manifest - Manifest to update
 * @param segment - Segment to add
 * @returns Updated manifest
 */
export function addSegment(
  manifest: EventManifest,
  segment: EventSegment
): EventManifest {
  return {
    ...manifest,
    segments: [...manifest.segments, segment],
    lastEventId: segment.maxId,
    lastEventTs: segment.maxTs,
  }
}

/**
 * Find segments in a time range
 *
 * @param manifest - Manifest to search
 * @param minTs - Minimum timestamp (inclusive)
 * @param maxTs - Maximum timestamp (inclusive)
 * @returns Segments overlapping the time range
 */
export function findSegmentsInRange(
  manifest: EventManifest,
  minTs: number,
  maxTs: number
): EventSegment[] {
  return manifest.segments.filter(seg => {
    // Segment overlaps if:
    // - Segment starts before range ends AND
    // - Segment ends after range starts
    return seg.minTs <= maxTs && seg.maxTs >= minTs
  })
}

/**
 * Find segment containing a specific event ID
 *
 * @param manifest - Manifest to search
 * @param eventId - Event ULID to find
 * @returns Segment containing the event, or undefined if not found
 */
export function findSegmentForEvent(
  manifest: EventManifest,
  eventId: string
): EventSegment | undefined {
  return manifest.segments.find(seg => {
    // Check if eventId is within the segment's ID range
    // ULIDs are lexicographically sortable
    return eventId >= seg.minId && eventId <= seg.maxId
  })
}

/**
 * Compute checksum for segment content
 *
 * Content-addressed storage: same events = same checksum
 * This enables deduplication across branches
 *
 * @param data - Parquet file content
 * @returns SHA256 hex digest
 */
export function computeSegmentChecksum(data: Uint8Array): string {
  return sha256(data)
}

/**
 * Create segment metadata from Parquet file
 *
 * @param file - File path
 * @param minId - First event ULID
 * @param maxId - Last event ULID
 * @param minTs - Minimum timestamp
 * @param maxTs - Maximum timestamp
 * @param count - Event count
 * @param data - Parquet file content (for checksum)
 * @returns Segment metadata
 */
export function createSegment(
  file: string,
  minId: string,
  maxId: string,
  minTs: number,
  maxTs: number,
  count: number,
  data: Uint8Array
): EventSegment {
  return {
    file,
    minId,
    maxId,
    minTs,
    maxTs,
    count,
    checksum: computeSegmentChecksum(data),
  }
}

// =============================================================================
// Merge Tracking
// =============================================================================

/**
 * Record a merge operation in the manifest
 *
 * @param manifest - Manifest to update
 * @param branch - Branch that was merged
 * @param commit - Commit ID that was merged
 * @param upToSegment - Last segment from merged branch
 * @returns Updated manifest
 */
export function recordMerge(
  manifest: EventManifest,
  branch: string,
  commit: string,
  upToSegment: string
): EventManifest {
  const mergedFrom = manifest.mergedFrom || []

  return {
    ...manifest,
    mergedFrom: [
      ...mergedFrom,
      {
        branch,
        commit,
        upToSegment,
      },
    ],
  }
}

/**
 * Get the last merge from a specific branch
 *
 * @param manifest - Manifest to search
 * @param branch - Branch name
 * @returns Merge record, or undefined if never merged
 */
export function getLastMergeFrom(
  manifest: EventManifest,
  branch: string
): { branch: string; commit: string; upToSegment: string } | undefined {
  if (!manifest.mergedFrom) return undefined

  // Find last merge from this branch (array is ordered chronologically)
  for (let i = manifest.mergedFrom.length - 1; i >= 0; i--) {
    const merge = manifest.mergedFrom[i]!
    if (merge.branch === branch) {
      return merge
    }
  }

  return undefined
}
