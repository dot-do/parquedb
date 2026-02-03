/**
 * Sync Manifest Types
 *
 * Tracks file state for push/pull/sync operations between
 * local filesystem and remote R2 storage.
 */

import type { Visibility } from '../types/visibility'

// =============================================================================
// Sync Manifest
// =============================================================================

/**
 * Sync manifest for tracking database state
 * Stored at _meta/manifest.json in both local and remote
 */
export interface SyncManifest {
  /** Schema version for manifest format */
  version: 1

  /** Unique database identifier */
  databaseId: string

  /** Human-readable database name */
  name: string

  /** Owner username (for public URL) */
  owner?: string | undefined

  /** URL-friendly slug (for public URL) */
  slug?: string | undefined

  /** Database visibility level */
  visibility: Visibility

  /** ISO timestamp of last sync */
  lastSyncedAt: string

  /** Sync source identifier (e.g., 'local', 'r2:bucket/prefix') */
  syncedFrom?: string | undefined

  /** All tracked files */
  files: Record<string, SyncFileEntry>

  /** Optional database metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Individual file entry in the manifest
 */
export interface SyncFileEntry {
  /** File path relative to database root */
  path: string

  /** File size in bytes */
  size: number

  /** MD5 or SHA-256 hash of file contents */
  hash: string

  /** Hash algorithm used */
  hashAlgorithm: 'md5' | 'sha256'

  /** ISO timestamp of last modification */
  modifiedAt: string

  /** ETag from storage (for conditional updates) */
  etag?: string | undefined

  /** Content type */
  contentType?: string | undefined
}

// =============================================================================
// Sync Operations
// =============================================================================

/**
 * Result of comparing two manifests
 */
export interface SyncDiff {
  /** Files that exist locally but not remotely */
  toUpload: SyncFileEntry[]

  /** Files that exist remotely but not locally */
  toDownload: SyncFileEntry[]

  /** Files that differ between local and remote */
  conflicts: SyncConflict[]

  /** Files that are identical */
  unchanged: string[]

  /** Files that were deleted locally */
  deletedLocally: string[]

  /** Files that were deleted remotely */
  deletedRemotely: string[]
}

/**
 * A conflict between local and remote versions of a file
 */
export interface SyncConflict {
  /** File path */
  path: string

  /** Local file entry */
  local: SyncFileEntry

  /** Remote file entry */
  remote: SyncFileEntry

  /** Suggested resolution based on timestamps */
  suggestedResolution: 'keep-local' | 'keep-remote' | 'manual'
}

/**
 * Strategy for resolving conflicts
 */
export type ConflictStrategy =
  | 'local-wins'   // Always use local version
  | 'remote-wins'  // Always use remote version
  | 'newest'       // Use the most recently modified
  | 'manual'       // Require manual resolution

/**
 * Options for sync operations
 */
export interface SyncOptions {
  /** Conflict resolution strategy */
  conflictStrategy?: ConflictStrategy | undefined

  /** Whether to perform a dry run (no actual changes) */
  dryRun?: boolean | undefined

  /** Callback for progress updates */
  onProgress?: ((progress: SyncProgress) => void) | undefined

  /** Files to include (glob patterns) */
  include?: string[] | undefined

  /** Files to exclude (glob patterns) */
  exclude?: string[] | undefined
}

/**
 * Progress update during sync
 */
export interface SyncProgress {
  /** Current operation */
  operation: 'comparing' | 'uploading' | 'downloading' | 'deleting'

  /** Current file being processed */
  currentFile?: string | undefined

  /** Files processed so far */
  processed: number

  /** Total files to process */
  total: number

  /** Bytes transferred so far */
  bytesTransferred: number

  /** Total bytes to transfer */
  bytesTotal: number
}

/**
 * Result of a sync operation
 */
export interface SyncResult {
  /** Whether sync was successful */
  success: boolean

  /** Files uploaded to remote */
  uploaded: string[]

  /** Files downloaded from remote */
  downloaded: string[]

  /** Files deleted */
  deleted: string[]

  /** Conflicts that were resolved */
  conflictsResolved: SyncConflict[]

  /** Conflicts that require manual resolution */
  conflictsPending: SyncConflict[]

  /** Any errors that occurred */
  errors: SyncError[]

  /** Updated manifest */
  manifest: SyncManifest
}

/**
 * Error during sync operation
 */
export interface SyncError {
  /** File path that caused the error */
  path: string

  /** Operation that failed */
  operation: 'upload' | 'download' | 'delete' | 'hash'

  /** Error message */
  message: string

  /** Original error */
  cause?: Error | undefined
}

// =============================================================================
// Manifest Helpers
// =============================================================================

/**
 * Create an empty manifest for a new database
 */
export function createManifest(
  databaseId: string,
  name: string,
  options?: {
    owner?: string | undefined
    slug?: string | undefined
    visibility?: Visibility | undefined
    metadata?: Record<string, unknown> | undefined
  }
): SyncManifest {
  return {
    version: 1,
    databaseId,
    name,
    owner: options?.owner,
    slug: options?.slug,
    visibility: options?.visibility ?? 'private',
    lastSyncedAt: new Date().toISOString(),
    files: {},
    metadata: options?.metadata,
  }
}

/**
 * Compare two manifests and produce a diff
 */
export function diffManifests(
  local: SyncManifest | null,
  remote: SyncManifest | null
): SyncDiff {
  const localFiles = local?.files ?? {}
  const remoteFiles = remote?.files ?? {}

  const toUpload: SyncFileEntry[] = []
  const toDownload: SyncFileEntry[] = []
  const conflicts: SyncConflict[] = []
  const unchanged: string[] = []
  const deletedLocally: string[] = []
  const deletedRemotely: string[] = []

  // Find files to upload (exist locally, not remotely)
  for (const [path, localFile] of Object.entries(localFiles)) {
    const remoteFile = remoteFiles[path]

    if (!remoteFile) {
      // New local file
      toUpload.push(localFile)
    } else if (localFile.hash === remoteFile.hash) {
      // Files are identical
      unchanged.push(path)
    } else {
      // Files differ - check timestamps for conflict resolution
      const localTime = new Date(localFile.modifiedAt).getTime()
      const remoteTime = new Date(remoteFile.modifiedAt).getTime()

      let suggestedResolution: SyncConflict['suggestedResolution']
      if (Math.abs(localTime - remoteTime) < 1000) {
        // Within 1 second - manual resolution needed
        suggestedResolution = 'manual'
      } else if (localTime > remoteTime) {
        suggestedResolution = 'keep-local'
      } else {
        suggestedResolution = 'keep-remote'
      }

      conflicts.push({
        path,
        local: localFile,
        remote: remoteFile,
        suggestedResolution,
      })
    }
  }

  // Find files to download (exist remotely, not locally)
  for (const [path, remoteFile] of Object.entries(remoteFiles)) {
    if (!localFiles[path]) {
      toDownload.push(remoteFile)
    }
  }

  // Note: For deletion tracking, we would need to compare against
  // a previous known state. This basic implementation doesn't track
  // deletions across syncs.

  return {
    toUpload,
    toDownload,
    conflicts,
    unchanged,
    deletedLocally,
    deletedRemotely,
  }
}

/**
 * Resolve conflicts based on strategy
 */
export function resolveConflicts(
  conflicts: SyncConflict[],
  strategy: ConflictStrategy
): { upload: SyncFileEntry[]; download: SyncFileEntry[]; manual: SyncConflict[] } {
  const upload: SyncFileEntry[] = []
  const download: SyncFileEntry[] = []
  const manual: SyncConflict[] = []

  for (const conflict of conflicts) {
    switch (strategy) {
      case 'local-wins':
        upload.push(conflict.local)
        break
      case 'remote-wins':
        download.push(conflict.remote)
        break
      case 'newest': {
        const localTime = new Date(conflict.local.modifiedAt).getTime()
        const remoteTime = new Date(conflict.remote.modifiedAt).getTime()
        if (localTime >= remoteTime) {
          upload.push(conflict.local)
        } else {
          download.push(conflict.remote)
        }
        break
      }
      case 'manual':
      default:
        manual.push(conflict)
        break
    }
  }

  return { upload, download, manual }
}

/**
 * Merge a file entry into a manifest
 */
export function updateManifestFile(
  manifest: SyncManifest,
  file: SyncFileEntry
): SyncManifest {
  return {
    ...manifest,
    lastSyncedAt: new Date().toISOString(),
    files: {
      ...manifest.files,
      [file.path]: file,
    },
  }
}

/**
 * Remove a file from a manifest
 */
export function removeManifestFile(
  manifest: SyncManifest,
  path: string
): SyncManifest {
  const { [path]: _, ...remainingFiles } = manifest.files
  return {
    ...manifest,
    lastSyncedAt: new Date().toISOString(),
    files: remainingFiles,
  }
}
