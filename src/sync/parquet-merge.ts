/**
 * Parquet-Based Event Merge
 *
 * Merges event Parquet files from different branches using content-addressed
 * storage (checksum-based deduplication) and the event merge engine.
 *
 * Algorithm:
 * 1. Find common segments (by checksum - content addressed!)
 * 2. Identify new segments in each branch since common ancestor
 * 3. Read events from new segments
 * 4. Merge event streams using event-merge.ts
 * 5. Write merged events to new segments
 * 6. Create updated manifest
 */

import type { StorageBackend } from '../types/storage'
import type { Event } from '../types/entity'
import type {
  EventManifest,
  EventSegment,
} from './event-manifest'
import {
  loadManifest,
  saveManifest as _saveManifest,
  addSegment as _addSegment,
  recordMerge,
} from './event-manifest'
import {
  readEventsFromSegments,
  readSegmentEvents as _readSegmentEvents,
} from './segment-reader'
import { writeEvents } from './segment-writer'
import {
  mergeEventStreams,
  type EventMergeResult,
  type MergeOptions,
  type MergeConflict,
} from './event-merge'

// =============================================================================
// Types
// =============================================================================

/**
 * Result of Parquet merge operation
 */
export interface ParquetMergeResult {
  /** Whether merge succeeded (no unresolved conflicts) */
  readonly success: boolean

  /** Updated manifest (only if success) */
  readonly manifest?: EventManifest | undefined

  /** Conflicts detected during merge */
  readonly conflicts: readonly MergeConflict[]

  /** Statistics about the merge */
  stats: {
    /** Number of segments in base (common ancestor) */
    baseSegments: number

    /** Number of new segments from our branch */
    ourNewSegments: number

    /** Number of new segments from their branch */
    theirNewSegments: number

    /** Number of segments in merged result */
    mergedSegments: number

    /** Total events in merged result */
    totalEvents: number

    /** Number of segments shared (deduplicated) */
    sharedSegments: number

    /** Events that were auto-merged */
    autoMerged: number

    /** Conflicts that were resolved */
    resolved: number
  }

  /** Detailed merge result from event-merge engine */
  mergeResult?: EventMergeResult | undefined
}

// =============================================================================
// Main Merge Function
// =============================================================================

/**
 * Merge event Parquet files from two branches
 *
 * This implements a three-way merge using content-addressed segments:
 * - Base: Common ancestor segments (shared checksums)
 * - Ours: Segments added in our branch since base
 * - Theirs: Segments added in their branch since base
 *
 * Shared segments (same checksum) are automatically deduplicated,
 * making this efficient for branches with mostly identical history.
 *
 * @param storage - Storage backend for reading/writing
 * @param baseManifest - Common ancestor manifest
 * @param ourManifest - Our branch manifest
 * @param theirManifest - Their branch manifest
 * @param options - Merge options (conflict resolution, etc.)
 * @returns Merge result with conflicts and statistics
 *
 * @example
 * ```typescript
 * const result = await mergeEventParquets(
 *   storage,
 *   baseManifest,
 *   mainManifest,
 *   featureManifest,
 *   { resolutionStrategy: 'ours' }
 * )
 *
 * if (result.success) {
 *   await saveManifest(storage, result.manifest!)
 * } else {
 *   console.error('Merge conflicts:', result.conflicts)
 * }
 * ```
 */
export async function mergeEventParquets(
  storage: StorageBackend,
  baseManifest: EventManifest,
  ourManifest: EventManifest,
  theirManifest: EventManifest,
  options: MergeOptions = {}
): Promise<ParquetMergeResult> {
  // Step 1: Find common segments (content-addressed by checksum)
  const baseChecksums = new Set(baseManifest.segments.map(s => s.checksum))
  const ourChecksums = new Set(ourManifest.segments.map(s => s.checksum))
  const theirChecksums = new Set(theirManifest.segments.map(s => s.checksum))

  // Segments that exist in all three = common ancestor
  const commonChecksums = new Set<string>()
  for (const checksum of baseChecksums) {
    if (ourChecksums.has(checksum) && theirChecksums.has(checksum)) {
      commonChecksums.add(checksum)
    }
  }

  // Step 2: Identify new segments in each branch
  const ourNewSegments = getNewSegments(ourManifest, commonChecksums)
  const theirNewSegments = getNewSegments(theirManifest, commonChecksums)

  // Step 3: Fast path - if no overlapping new segments, just concatenate
  if (ourNewSegments.length === 0 && theirNewSegments.length === 0) {
    // No changes in either branch - return base
    return {
      success: true,
      manifest: baseManifest,
      conflicts: [],
      stats: {
        baseSegments: baseManifest.segments.length,
        ourNewSegments: 0,
        theirNewSegments: 0,
        mergedSegments: baseManifest.segments.length,
        totalEvents: baseManifest.segments.reduce((sum, s) => sum + s.count, 0),
        sharedSegments: commonChecksums.size,
        autoMerged: 0,
        resolved: 0,
      },
    }
  }

  if (ourNewSegments.length === 0) {
    // Only their branch has changes - use their manifest
    return {
      success: true,
      manifest: theirManifest,
      conflicts: [],
      stats: {
        baseSegments: baseManifest.segments.length,
        ourNewSegments: 0,
        theirNewSegments: theirNewSegments.length,
        mergedSegments: theirManifest.segments.length,
        totalEvents: theirManifest.segments.reduce((sum, s) => sum + s.count, 0),
        sharedSegments: commonChecksums.size,
        autoMerged: 0,
        resolved: 0,
      },
    }
  }

  if (theirNewSegments.length === 0) {
    // Only our branch has changes - use our manifest
    return {
      success: true,
      manifest: ourManifest,
      conflicts: [],
      stats: {
        baseSegments: baseManifest.segments.length,
        ourNewSegments: ourNewSegments.length,
        theirNewSegments: 0,
        mergedSegments: ourManifest.segments.length,
        totalEvents: ourManifest.segments.reduce((sum, s) => sum + s.count, 0),
        sharedSegments: commonChecksums.size,
        autoMerged: 0,
        resolved: 0,
      },
    }
  }

  // Step 4: Both branches have changes - need to merge events
  const baseEvents = await readAllEvents(storage, baseManifest.segments)
  const ourNewEvents = await readAllEvents(storage, ourNewSegments)
  const theirNewEvents = await readAllEvents(storage, theirNewSegments)

  // Step 5: Merge event streams using event-merge engine (two-way merge)
  // Note: baseEvents are used for context but the merge is between our and their new events
  const mergeResult = await mergeEventStreams(
    [...baseEvents, ...ourNewEvents],
    theirNewEvents,
    options
  )

  if (!mergeResult.success) {
    // Merge failed due to conflicts
    return {
      success: false,
      conflicts: mergeResult.conflicts,
      stats: {
        baseSegments: baseManifest.segments.length,
        ourNewSegments: ourNewSegments.length,
        theirNewSegments: theirNewSegments.length,
        mergedSegments: 0,
        totalEvents: 0,
        sharedSegments: commonChecksums.size,
        autoMerged: mergeResult.autoMerged.length,
        resolved: mergeResult.resolved.length,
      },
      mergeResult,
    }
  }

  // Step 6: Write merged events to new segments
  const commonSegments = getCommonSegments(ourManifest, commonChecksums)
  const mergedNewSegments = await writeEvents(
    storage,
    mergeResult.mergedEvents,
    {
      maxEventsPerSegment: 10_000,
      compression: 'SNAPPY',
    }
  )

  // Step 7: Create new manifest
  let newManifest: EventManifest = {
    version: 1,
    segments: [...commonSegments, ...mergedNewSegments],
    branch: ourManifest.branch,
    headCommit: '', // Caller should set this
    lastEventId: mergedNewSegments[mergedNewSegments.length - 1]?.maxId || commonSegments[commonSegments.length - 1]?.maxId || '',
    lastEventTs: mergedNewSegments[mergedNewSegments.length - 1]?.maxTs || commonSegments[commonSegments.length - 1]?.maxTs || 0,
  }

  // Record merge in manifest
  if (theirManifest.branch !== ourManifest.branch) {
    newManifest = recordMerge(
      newManifest,
      theirManifest.branch,
      theirManifest.headCommit,
      theirNewSegments[theirNewSegments.length - 1]?.file || ''
    )
  }

  return {
    success: true,
    manifest: newManifest,
    conflicts: [],
    stats: {
      baseSegments: baseManifest.segments.length,
      ourNewSegments: ourNewSegments.length,
      theirNewSegments: theirNewSegments.length,
      mergedSegments: newManifest.segments.length,
      totalEvents: newManifest.segments.reduce((sum, s) => sum + s.count, 0),
      sharedSegments: commonChecksums.size,
      autoMerged: mergeResult.autoMerged.length,
      resolved: mergeResult.resolved.length,
    },
    mergeResult,
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

// Note: findCommonSegments is currently implemented inline in mergeEventParquets
// This helper is kept for potential future use in incremental merge scenarios

/**
 * Get segments added after common ancestor
 *
 * Returns segments whose checksums are NOT in the common set.
 *
 * @param manifest - Manifest to search
 * @param commonChecksums - Set of common checksums
 * @returns Segments not in common set
 */
function getNewSegments(
  manifest: EventManifest,
  commonChecksums: Set<string>
): EventSegment[] {
  return manifest.segments.filter(seg => !commonChecksums.has(seg.checksum))
}

/**
 * Get segments that are in the common set
 *
 * @param manifest - Manifest to search
 * @param commonChecksums - Set of common checksums
 * @returns Segments in common set
 */
function getCommonSegments(
  manifest: EventManifest,
  commonChecksums: Set<string>
): EventSegment[] {
  return manifest.segments.filter(seg => commonChecksums.has(seg.checksum))
}

/**
 * Read all events from segments
 *
 * @param storage - Storage backend
 * @param segments - Segments to read
 * @returns All events from segments
 */
async function readAllEvents(
  storage: StorageBackend,
  segments: EventSegment[]
): Promise<Event[]> {
  const events: Event[] = []

  for await (const event of readEventsFromSegments(storage, segments)) {
    events.push(event)
  }

  return events
}

// =============================================================================
// Merge with Manifest Loading
// =============================================================================

/**
 * Merge events from two branches, loading manifests from storage
 *
 * Convenience wrapper that loads manifests and performs merge.
 *
 * @param storage - Storage backend
 * @param baseBranch - Base branch name
 * @param ourBranch - Our branch name
 * @param theirBranch - Their branch name
 * @param options - Merge options
 * @returns Merge result
 */
export async function mergeEventBranches(
  storage: StorageBackend,
  baseBranch: string,
  ourBranch: string,
  theirBranch: string,
  options: MergeOptions = {}
): Promise<ParquetMergeResult> {
  // Load manifests
  const baseManifest = await loadManifest(storage, `events/${baseBranch}/manifest.json`)
  const ourManifest = await loadManifest(storage, `events/${ourBranch}/manifest.json`)
  const theirManifest = await loadManifest(storage, `events/${theirBranch}/manifest.json`)

  // Perform merge
  return mergeEventParquets(
    storage,
    baseManifest,
    ourManifest,
    theirManifest,
    options
  )
}

// =============================================================================
// Incremental Merge
// =============================================================================

/**
 * Perform incremental merge (pull changes from another branch)
 *
 * Only merges segments that are new since the last merge from this branch.
 *
 * @param storage - Storage backend
 * @param ourManifest - Our current manifest
 * @param theirManifest - Their manifest to pull from
 * @param options - Merge options
 * @returns Merge result
 */
export async function incrementalMerge(
  storage: StorageBackend,
  ourManifest: EventManifest,
  theirManifest: EventManifest,
  options: MergeOptions = {}
): Promise<ParquetMergeResult> {
  // Find what we've already merged from their branch
  const lastMerge = ourManifest.mergedFrom?.find(
    m => m.branch === theirManifest.branch
  )

  let theirNewSegments: EventSegment[]

  if (!lastMerge) {
    // Never merged from their branch - all segments are new
    theirNewSegments = theirManifest.segments
  } else {
    // Find segments added since last merge
    const lastMergedSegmentIdx = theirManifest.segments.findIndex(
      s => s.file === lastMerge.upToSegment
    )

    if (lastMergedSegmentIdx === -1) {
      // Can't find last merged segment - merge all
      theirNewSegments = theirManifest.segments
    } else {
      // Only merge segments after the last merged one
      theirNewSegments = theirManifest.segments.slice(lastMergedSegmentIdx + 1)
    }
  }

  if (theirNewSegments.length === 0) {
    // Nothing new to merge
    return {
      success: true,
      manifest: ourManifest,
      conflicts: [],
      stats: {
        baseSegments: ourManifest.segments.length,
        ourNewSegments: 0,
        theirNewSegments: 0,
        mergedSegments: ourManifest.segments.length,
        totalEvents: ourManifest.segments.reduce((sum, s) => sum + s.count, 0),
        sharedSegments: 0,
        autoMerged: 0,
        resolved: 0,
      },
    }
  }

  // Read their new events
  const theirNewEvents = await readAllEvents(storage, theirNewSegments)

  // Merge with our current state
  // For incremental merge, we treat it as a two-way merge between
  // our current state (no new events) and their new events
  const ourNewEvents: Event[] = [] // No new events on our side

  const mergeResult = await mergeEventStreams(
    ourNewEvents,
    theirNewEvents,
    options
  )

  if (!mergeResult.success) {
    return {
      success: false,
      conflicts: mergeResult.conflicts,
      stats: {
        baseSegments: 0,
        ourNewSegments: 0,
        theirNewSegments: theirNewSegments.length,
        mergedSegments: 0,
        totalEvents: 0,
        sharedSegments: 0,
        autoMerged: mergeResult.autoMerged.length,
        resolved: mergeResult.resolved.length,
      },
      mergeResult,
    }
  }

  // Append merged events to our segments
  const newSegments = await writeEvents(
    storage,
    mergeResult.mergedEvents,
    {
      maxEventsPerSegment: 10_000,
      compression: 'SNAPPY',
    }
  )

  let newManifest: EventManifest = {
    ...ourManifest,
    segments: [...ourManifest.segments, ...newSegments],
    lastEventId: newSegments[newSegments.length - 1]?.maxId || ourManifest.lastEventId,
    lastEventTs: newSegments[newSegments.length - 1]?.maxTs || ourManifest.lastEventTs,
  }

  // Record merge
  newManifest = recordMerge(
    newManifest,
    theirManifest.branch,
    theirManifest.headCommit,
    theirNewSegments[theirNewSegments.length - 1]?.file || ''
  )

  return {
    success: true,
    manifest: newManifest,
    conflicts: [],
    stats: {
      baseSegments: 0,
      ourNewSegments: 0,
      theirNewSegments: theirNewSegments.length,
      mergedSegments: newManifest.segments.length,
      totalEvents: newManifest.segments.reduce((sum, s) => sum + s.count, 0),
      sharedSegments: 0,
      autoMerged: mergeResult.autoMerged.length,
      resolved: mergeResult.resolved.length,
    },
    mergeResult,
  }
}
