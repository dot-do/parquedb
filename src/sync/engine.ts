/**
 * SyncEngine - Push/Pull/Sync operations for ParqueDB
 *
 * Handles synchronization between local filesystem and remote R2 storage.
 */

import type { StorageBackend } from '../types/storage'
import type { Visibility } from '../types/visibility'
import {
  type SyncManifest,
  type SyncFileEntry,
  type SyncDiff,
  type SyncOptions,
  type SyncProgress,
  type SyncResult,
  type SyncError,
  createManifest,
  diffManifests,
  resolveConflicts,
} from './manifest'

// =============================================================================
// SyncEngine
// =============================================================================

/**
 * Options for creating a SyncEngine
 */
export interface SyncEngineOptions {
  /** Local storage backend (filesystem) */
  local: StorageBackend

  /** Remote storage backend (R2, S3, etc.) */
  remote: StorageBackend

  /** Database identifier */
  databaseId: string

  /** Database name */
  name: string

  /** Owner username */
  owner?: string

  /** Progress callback */
  onProgress?: (progress: SyncProgress) => void
}

/**
 * SyncEngine handles push/pull/sync operations
 */
export class SyncEngine {
  private local: StorageBackend
  private remote: StorageBackend
  private databaseId: string
  private name: string
  private owner?: string
  private onProgress?: (progress: SyncProgress) => void

  private static MANIFEST_PATH = '_meta/manifest.json'
  private static IGNORED_PATHS = [
    '_meta/manifest.json',
    '.git',
    '.DS_Store',
    'node_modules',
  ]

  constructor(options: SyncEngineOptions) {
    this.local = options.local
    this.remote = options.remote
    this.databaseId = options.databaseId
    this.name = options.name
    this.owner = options.owner
    this.onProgress = options.onProgress
  }

  // =============================================================================
  // Push - Local to Remote
  // =============================================================================

  /**
   * Push local changes to remote
   */
  async push(options: SyncOptions = {}): Promise<SyncResult> {
    const errors: SyncError[] = []
    const uploaded: string[] = []

    // Load manifests
    const localManifest = await this.loadLocalManifest()
    const remoteManifest = await this.loadRemoteManifest()

    // Build current local state
    const currentLocalManifest = await this.buildLocalManifest(
      localManifest?.visibility ?? 'private'
    )

    // Compare with remote
    const diff = diffManifests(currentLocalManifest, remoteManifest)

    if (options.dryRun) {
      return {
        success: true,
        uploaded: diff.toUpload.map(f => f.path),
        downloaded: [],
        deleted: [],
        conflictsResolved: [],
        conflictsPending: diff.conflicts,
        errors: [],
        manifest: currentLocalManifest,
      }
    }

    // Handle conflicts
    const strategy = options.conflictStrategy ?? 'local-wins'
    const { upload: conflictUploads, manual } = resolveConflicts(diff.conflicts, strategy)

    // Upload new and changed files
    const toUpload = [...diff.toUpload, ...conflictUploads]
    const total = toUpload.length
    let processed = 0
    let bytesTransferred = 0
    const bytesTotal = toUpload.reduce((sum, f) => sum + f.size, 0)

    for (const file of toUpload) {
      this.reportProgress({
        operation: 'uploading',
        currentFile: file.path,
        processed,
        total,
        bytesTransferred,
        bytesTotal,
      })

      try {
        const data = await this.local.read(file.path)
        await this.remote.write(file.path, data, {
          contentType: file.contentType,
        })
        uploaded.push(file.path)
        bytesTransferred += file.size
      } catch (error) {
        errors.push({
          path: file.path,
          operation: 'upload',
          message: error instanceof Error ? error.message : String(error),
          cause: error instanceof Error ? error : undefined,
        })
      }

      processed++
    }

    // Update remote manifest
    const updatedManifest: SyncManifest = {
      ...currentLocalManifest,
      lastSyncedAt: new Date().toISOString(),
      syncedFrom: 'local',
    }

    await this.saveRemoteManifest(updatedManifest)
    await this.saveLocalManifest(updatedManifest)

    return {
      success: errors.length === 0,
      uploaded,
      downloaded: [],
      deleted: [],
      conflictsResolved: diff.conflicts.filter(c => !manual.includes(c)),
      conflictsPending: manual,
      errors,
      manifest: updatedManifest,
    }
  }

  // =============================================================================
  // Pull - Remote to Local
  // =============================================================================

  /**
   * Pull remote changes to local
   */
  async pull(options: SyncOptions = {}): Promise<SyncResult> {
    const errors: SyncError[] = []
    const downloaded: string[] = []

    // Load manifests
    const localManifest = await this.loadLocalManifest()
    const remoteManifest = await this.loadRemoteManifest()

    if (!remoteManifest) {
      return {
        success: false,
        uploaded: [],
        downloaded: [],
        deleted: [],
        conflictsResolved: [],
        conflictsPending: [],
        errors: [{
          path: SyncEngine.MANIFEST_PATH,
          operation: 'download',
          message: 'Remote manifest not found',
        }],
        manifest: localManifest ?? createManifest(this.databaseId, this.name),
      }
    }

    // Compare with local
    const diff = diffManifests(localManifest, remoteManifest)

    if (options.dryRun) {
      return {
        success: true,
        uploaded: [],
        downloaded: diff.toDownload.map(f => f.path),
        deleted: [],
        conflictsResolved: [],
        conflictsPending: diff.conflicts,
        errors: [],
        manifest: remoteManifest,
      }
    }

    // Handle conflicts
    const strategy = options.conflictStrategy ?? 'remote-wins'
    const { download: conflictDownloads, manual } = resolveConflicts(diff.conflicts, strategy)

    // Download new and changed files
    const toDownload = [...diff.toDownload, ...conflictDownloads]
    const total = toDownload.length
    let processed = 0
    let bytesTransferred = 0
    const bytesTotal = toDownload.reduce((sum, f) => sum + f.size, 0)

    for (const file of toDownload) {
      this.reportProgress({
        operation: 'downloading',
        currentFile: file.path,
        processed,
        total,
        bytesTransferred,
        bytesTotal,
      })

      try {
        const data = await this.remote.read(file.path)
        await this.local.write(file.path, data)
        downloaded.push(file.path)
        bytesTransferred += file.size
      } catch (error) {
        errors.push({
          path: file.path,
          operation: 'download',
          message: error instanceof Error ? error.message : String(error),
          cause: error instanceof Error ? error : undefined,
        })
      }

      processed++
    }

    // Update local manifest
    const updatedManifest: SyncManifest = {
      ...remoteManifest,
      lastSyncedAt: new Date().toISOString(),
      syncedFrom: `remote:${this.remote.type}`,
    }

    await this.saveLocalManifest(updatedManifest)

    return {
      success: errors.length === 0,
      uploaded: [],
      downloaded,
      deleted: [],
      conflictsResolved: diff.conflicts.filter(c => !manual.includes(c)),
      conflictsPending: manual,
      errors,
      manifest: updatedManifest,
    }
  }

  // =============================================================================
  // Sync - Bidirectional
  // =============================================================================

  /**
   * Bidirectional sync between local and remote
   */
  async sync(options: SyncOptions = {}): Promise<SyncResult> {
    const errors: SyncError[] = []
    const uploaded: string[] = []
    const downloaded: string[] = []

    // Load manifests
    const localManifest = await this.loadLocalManifest()
    const remoteManifest = await this.loadRemoteManifest()

    // Build current local state
    const currentLocalManifest = await this.buildLocalManifest(
      localManifest?.visibility ?? remoteManifest?.visibility ?? 'private'
    )

    // Compare
    const diff = diffManifests(currentLocalManifest, remoteManifest)

    if (options.dryRun) {
      return {
        success: true,
        uploaded: diff.toUpload.map(f => f.path),
        downloaded: diff.toDownload.map(f => f.path),
        deleted: [],
        conflictsResolved: [],
        conflictsPending: diff.conflicts,
        errors: [],
        manifest: currentLocalManifest,
      }
    }

    // Handle conflicts
    const strategy = options.conflictStrategy ?? 'newest'
    const { upload: conflictUploads, download: conflictDownloads, manual } = resolveConflicts(diff.conflicts, strategy)

    // Upload
    const toUpload = [...diff.toUpload, ...conflictUploads]
    for (const file of toUpload) {
      this.reportProgress({
        operation: 'uploading',
        currentFile: file.path,
        processed: uploaded.length,
        total: toUpload.length + diff.toDownload.length + conflictDownloads.length,
        bytesTransferred: 0,
        bytesTotal: 0,
      })

      try {
        const data = await this.local.read(file.path)
        await this.remote.write(file.path, data)
        uploaded.push(file.path)
      } catch (error) {
        errors.push({
          path: file.path,
          operation: 'upload',
          message: error instanceof Error ? error.message : String(error),
          cause: error instanceof Error ? error : undefined,
        })
      }
    }

    // Download
    const toDownload = [...diff.toDownload, ...conflictDownloads]
    for (const file of toDownload) {
      this.reportProgress({
        operation: 'downloading',
        currentFile: file.path,
        processed: uploaded.length + downloaded.length,
        total: toUpload.length + toDownload.length,
        bytesTransferred: 0,
        bytesTotal: 0,
      })

      try {
        const data = await this.remote.read(file.path)
        await this.local.write(file.path, data)
        downloaded.push(file.path)
      } catch (error) {
        errors.push({
          path: file.path,
          operation: 'download',
          message: error instanceof Error ? error.message : String(error),
          cause: error instanceof Error ? error : undefined,
        })
      }
    }

    // Update manifests
    const updatedManifest = await this.buildLocalManifest(
      currentLocalManifest.visibility
    )
    updatedManifest.lastSyncedAt = new Date().toISOString()
    updatedManifest.syncedFrom = 'bidirectional'

    await this.saveRemoteManifest(updatedManifest)
    await this.saveLocalManifest(updatedManifest)

    return {
      success: errors.length === 0,
      uploaded,
      downloaded,
      deleted: [],
      conflictsResolved: diff.conflicts.filter(c => !manual.includes(c)),
      conflictsPending: manual,
      errors,
      manifest: updatedManifest,
    }
  }

  // =============================================================================
  // Status
  // =============================================================================

  /**
   * Get sync status without making changes
   */
  async status(): Promise<{
    localManifest: SyncManifest | null
    remoteManifest: SyncManifest | null
    diff: SyncDiff
    isSynced: boolean
  }> {
    const localManifest = await this.loadLocalManifest()
    const remoteManifest = await this.loadRemoteManifest()

    // Build current local state
    const currentLocalManifest = localManifest
      ? await this.buildLocalManifest(localManifest.visibility)
      : null

    const diff = diffManifests(currentLocalManifest, remoteManifest)

    const isSynced =
      diff.toUpload.length === 0 &&
      diff.toDownload.length === 0 &&
      diff.conflicts.length === 0

    return {
      localManifest: currentLocalManifest,
      remoteManifest,
      diff,
      isSynced,
    }
  }

  // =============================================================================
  // Manifest Management
  // =============================================================================

  /**
   * Load manifest from local storage
   */
  async loadLocalManifest(): Promise<SyncManifest | null> {
    try {
      const data = await this.local.read(SyncEngine.MANIFEST_PATH)
      return JSON.parse(new TextDecoder().decode(data)) as SyncManifest
    } catch {
      return null
    }
  }

  /**
   * Load manifest from remote storage
   */
  async loadRemoteManifest(): Promise<SyncManifest | null> {
    try {
      const data = await this.remote.read(SyncEngine.MANIFEST_PATH)
      return JSON.parse(new TextDecoder().decode(data)) as SyncManifest
    } catch {
      return null
    }
  }

  /**
   * Save manifest to local storage
   */
  async saveLocalManifest(manifest: SyncManifest): Promise<void> {
    const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2))
    await this.local.write(SyncEngine.MANIFEST_PATH, data)
  }

  /**
   * Save manifest to remote storage
   */
  async saveRemoteManifest(manifest: SyncManifest): Promise<void> {
    const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2))
    await this.remote.write(SyncEngine.MANIFEST_PATH, data)
  }

  /**
   * Build a manifest from current local files
   */
  async buildLocalManifest(visibility: Visibility): Promise<SyncManifest> {
    const files: Record<string, SyncFileEntry> = {}

    // List all files (files is string[] in ListResult)
    const result = await this.local.list('')

    for (const filePath of result.files) {
      // Skip ignored paths
      if (SyncEngine.IGNORED_PATHS.some(p => filePath.startsWith(p))) {
        continue
      }

      // Get file info
      const stat = await this.local.stat(filePath)
      if (!stat) continue

      // Skip directories
      if (stat.isDirectory) {
        continue
      }

      // Calculate hash
      const hash = await this.hashFile(filePath, this.local)

      files[filePath] = {
        path: filePath,
        size: stat.size,
        hash,
        hashAlgorithm: 'md5',
        modifiedAt: stat.mtime?.toISOString() ?? new Date().toISOString(),
        etag: stat.etag,
        contentType: this.guessContentType(filePath),
      }
    }

    return {
      version: 1,
      databaseId: this.databaseId,
      name: this.name,
      owner: this.owner,
      visibility,
      lastSyncedAt: new Date().toISOString(),
      files,
    }
  }

  // =============================================================================
  // Helpers
  // =============================================================================

  /**
   * Calculate file hash
   */
  private async hashFile(path: string, storage: StorageBackend): Promise<string> {
    const data = await storage.read(path)

    // Use crypto.subtle if available (Workers, modern Node.js)
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      // Create a fresh ArrayBuffer to avoid SharedArrayBuffer issues
      const buffer = new ArrayBuffer(data.length)
      const view = new Uint8Array(buffer)
      view.set(data)
      const hashBuffer = await crypto.subtle.digest('SHA-256', buffer)
      const hashArray = Array.from(new Uint8Array(hashBuffer))
      return hashArray.map(b => b.toString(16).padStart(2, '0')).join('')
    }

    // Fallback: use a simple checksum
    let hash = 0
    for (let i = 0; i < data.length; i++) {
      hash = ((hash << 5) - hash + data[i]!) | 0
    }
    return Math.abs(hash).toString(16).padStart(8, '0')
  }

  /**
   * Guess content type from file extension
   */
  private guessContentType(path: string): string {
    const ext = path.split('.').pop()?.toLowerCase()
    const types: Record<string, string> = {
      parquet: 'application/vnd.apache.parquet',
      json: 'application/json',
      jsonl: 'application/x-ndjson',
      csv: 'text/csv',
      txt: 'text/plain',
    }
    return types[ext ?? ''] ?? 'application/octet-stream'
  }

  /**
   * Report progress to callback
   */
  private reportProgress(progress: SyncProgress): void {
    if (this.onProgress) {
      this.onProgress(progress)
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a SyncEngine for push/pull operations
 */
export function createSyncEngine(options: SyncEngineOptions): SyncEngine {
  return new SyncEngine(options)
}
