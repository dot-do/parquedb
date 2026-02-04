/**
 * R2 Upload Utilities
 *
 * Provides functions for uploading files to Cloudflare R2:
 * - uploadFileToR2: Upload single parquet file
 * - uploadDirectoryToR2: Upload directory recursively
 * - verifyUpload: Verify file integrity via checksum
 * - syncToR2: Incremental sync with hash comparison
 * - computeFileHash: Compute SHA-256 hash of a file
 *
 * Features:
 * - Parallel uploads with configurable concurrency
 * - Progress tracking with callbacks
 * - Resume interrupted uploads via manifest
 * - Upload speed metrics logging
 */

import * as fs from 'node:fs'
import { promises as fsPromises } from 'node:fs'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import type { R2Bucket } from '../../src/storage/types/r2'

// =============================================================================
// Types
// =============================================================================

export interface UploadFileResult {
  success: boolean
  path: string
  size: number
  etag?: string
  error?: string
}

export interface UploadDirectoryResult {
  success: boolean
  uploaded: number
  failed: number
  files: Array<{ localPath: string; r2Path: string; size: number }>
  errors: Array<{ path: string; error: string }>
  /** Total bytes uploaded */
  totalBytes?: number
  /** Duration in milliseconds */
  durationMs?: number
  /** Upload speed in bytes/second */
  bytesPerSecond?: number
}

export interface VerifyResult {
  valid: boolean
  localHash: string
  remoteHash?: string
  method?: 'metadata' | 'content-comparison'
  error?: string
}

export interface SyncResult {
  total: number
  uploaded: number
  skipped: number
  failed: number
  deleted: number
  uploadedFiles: string[]
  skippedFiles: string[]
  deletedFiles: string[]
  errors: Array<{ path: string; error: string }>
  dryRun?: boolean
  wouldUpload?: number
  /** Total bytes uploaded */
  totalBytes?: number
  /** Duration in milliseconds */
  durationMs?: number
  /** Upload speed in bytes/second */
  bytesPerSecond?: number
}

export interface UploadOptions {
  maxRetries?: number
  retryDelay?: number
}

export interface SyncOptions {
  dryRun?: boolean
  deleteOrphans?: boolean
  /** Concurrency limit for parallel uploads (default: 5) */
  concurrency?: number
  /** Progress callback */
  onProgress?: (progress: UploadProgress) => void
  /** Resume manifest file path - if provided, will track progress for resume */
  manifestPath?: string
}

/**
 * Progress information during upload operations
 */
export interface UploadProgress {
  /** Current operation */
  operation: 'hashing' | 'uploading' | 'verifying' | 'deleting'
  /** Current file being processed */
  currentFile?: string
  /** Files processed so far */
  filesProcessed: number
  /** Total files to process */
  filesTotal: number
  /** Bytes transferred so far */
  bytesTransferred: number
  /** Total bytes to transfer */
  bytesTotal: number
  /** Current upload speed (bytes/second, rolling average) */
  bytesPerSecond: number
  /** Estimated time remaining in milliseconds */
  estimatedRemainingMs: number
}

/**
 * Upload manifest for tracking progress and enabling resume
 */
export interface UploadManifest {
  /** Version of manifest format */
  version: 1
  /** ISO timestamp when upload started */
  startedAt: string
  /** ISO timestamp of last update */
  updatedAt: string
  /** Source directory */
  sourceDir: string
  /** R2 prefix */
  r2Prefix: string
  /** Files that have been successfully uploaded */
  completedFiles: Record<string, { hash: string; size: number; uploadedAt: string }>
  /** Files that failed (for retry) */
  failedFiles: Record<string, { error: string; attempts: number }>
}

// =============================================================================
// Concurrency Limiter (p-limit implementation)
// =============================================================================

/**
 * Create a concurrency limiter (similar to p-limit)
 * Limits the number of concurrent promises
 */
function createLimiter(concurrency: number): <T>(fn: () => Promise<T>) => Promise<T> {
  const queue: Array<{
    fn: () => Promise<unknown>
    resolve: (value: unknown) => void
    reject: (error: unknown) => void
  }> = []
  let activeCount = 0

  const next = () => {
    if (activeCount < concurrency && queue.length > 0) {
      const item = queue.shift()!
      activeCount++

      item
        .fn()
        .then(value => {
          item.resolve(value)
        })
        .catch(error => {
          item.reject(error)
        })
        .finally(() => {
          activeCount--
          next()
        })
    }
  }

  return <T>(fn: () => Promise<T>): Promise<T> => {
    return new Promise<T>((resolve, reject) => {
      queue.push({
        fn: fn as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      })
      next()
    })
  }
}

// =============================================================================
// Progress Tracker
// =============================================================================

/**
 * Track upload progress and calculate metrics
 */
class ProgressTracker {
  private startTime: number
  private bytesTransferred = 0
  private filesProcessed = 0
  private readonly bytesTotal: number
  private readonly filesTotal: number
  private readonly onProgress?: (progress: UploadProgress) => void
  private readonly speedSamples: Array<{ time: number; bytes: number }> = []
  private currentOperation: UploadProgress['operation'] = 'uploading'
  private currentFile?: string

  constructor(options: {
    filesTotal: number
    bytesTotal: number
    onProgress?: (progress: UploadProgress) => void
  }) {
    this.startTime = Date.now()
    this.filesTotal = options.filesTotal
    this.bytesTotal = options.bytesTotal
    this.onProgress = options.onProgress
  }

  setOperation(operation: UploadProgress['operation']): void {
    this.currentOperation = operation
    this.emit()
  }

  setCurrentFile(file: string): void {
    this.currentFile = file
    this.emit()
  }

  addBytes(bytes: number): void {
    this.bytesTransferred += bytes
    const now = Date.now()
    this.speedSamples.push({ time: now, bytes: this.bytesTransferred })

    // Keep only last 10 seconds of samples for rolling average
    const cutoff = now - 10000
    while (this.speedSamples.length > 1 && this.speedSamples[0].time < cutoff) {
      this.speedSamples.shift()
    }

    this.emit()
  }

  incrementFiles(): void {
    this.filesProcessed++
    this.emit()
  }

  private calculateSpeed(): number {
    if (this.speedSamples.length < 2) {
      const elapsed = Date.now() - this.startTime
      return elapsed > 0 ? (this.bytesTransferred / elapsed) * 1000 : 0
    }

    const first = this.speedSamples[0]
    const last = this.speedSamples[this.speedSamples.length - 1]
    const timeDiff = last.time - first.time
    const bytesDiff = last.bytes - first.bytes

    return timeDiff > 0 ? (bytesDiff / timeDiff) * 1000 : 0
  }

  private calculateETA(): number {
    const speed = this.calculateSpeed()
    if (speed <= 0) return 0

    const remainingBytes = this.bytesTotal - this.bytesTransferred
    return (remainingBytes / speed) * 1000
  }

  private emit(): void {
    if (!this.onProgress) return

    this.onProgress({
      operation: this.currentOperation,
      currentFile: this.currentFile,
      filesProcessed: this.filesProcessed,
      filesTotal: this.filesTotal,
      bytesTransferred: this.bytesTransferred,
      bytesTotal: this.bytesTotal,
      bytesPerSecond: this.calculateSpeed(),
      estimatedRemainingMs: this.calculateETA(),
    })
  }

  getStats(): { durationMs: number; totalBytes: number; bytesPerSecond: number } {
    const durationMs = Date.now() - this.startTime
    return {
      durationMs,
      totalBytes: this.bytesTransferred,
      bytesPerSecond: durationMs > 0 ? (this.bytesTransferred / durationMs) * 1000 : 0,
    }
  }
}

// =============================================================================
// Manifest Management
// =============================================================================

/**
 * Load upload manifest from file
 */
async function loadManifest(manifestPath: string): Promise<UploadManifest | null> {
  try {
    const content = await fsPromises.readFile(manifestPath, 'utf-8')
    return JSON.parse(content) as UploadManifest
  } catch {
    return null
  }
}

/**
 * Save upload manifest to file
 */
async function saveManifest(manifestPath: string, manifest: UploadManifest): Promise<void> {
  manifest.updatedAt = new Date().toISOString()
  await fsPromises.writeFile(manifestPath, JSON.stringify(manifest, null, 2))
}

/**
 * Create a new upload manifest
 */
function createManifest(sourceDir: string, r2Prefix: string): UploadManifest {
  return {
    version: 1,
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    sourceDir,
    r2Prefix,
    completedFiles: {},
    failedFiles: {},
  }
}

// =============================================================================
// Core Functions
// =============================================================================

/**
 * Compute SHA-256 hash of a file
 */
export async function computeFileHash(localPath: string): Promise<string> {
  // Check if file exists first
  try {
    await fsPromises.access(localPath, fs.constants.R_OK)
  } catch {
    throw new Error(`ENOENT: File not found: ${localPath}`)
  }

  const content = await fsPromises.readFile(localPath)
  return crypto.createHash('sha256').update(content).digest('hex')
}

/**
 * Upload a single file to R2
 */
export async function uploadFileToR2(
  bucket: R2Bucket,
  localPath: string,
  r2Path: string,
  options?: UploadOptions
): Promise<UploadFileResult> {
  const maxRetries = options?.maxRetries ?? 0
  const retryDelay = options?.retryDelay ?? 1000

  // Validate file exists
  try {
    await fsPromises.access(localPath, fs.constants.R_OK)
  } catch {
    throw new Error(`ENOENT: File not found: ${localPath}`)
  }

  // Validate file is a parquet file
  if (!localPath.endsWith('.parquet')) {
    throw new Error(`Only parquet files can be uploaded. Got: ${localPath}`)
  }

  // Read file content
  const content = await fsPromises.readFile(localPath)
  const hash = crypto.createHash('sha256').update(content).digest('hex')

  // Retry loop
  let lastError: Error | null = null
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const result = await bucket.put(r2Path, new Uint8Array(content), {
        httpMetadata: {
          contentType: 'application/octet-stream',
        },
        customMetadata: {
          'x-parquedb-sha256': hash,
        },
      })

      if (result) {
        return {
          success: true,
          path: r2Path,
          size: content.length,
          etag: result.etag,
        }
      }

      // Null result is unexpected but handle gracefully
      return {
        success: false,
        path: r2Path,
        size: content.length,
        error: 'Upload returned null result',
      }
    } catch (error) {
      lastError = error as Error
      const errorMessage = lastError.message.toLowerCase()

      // Categorize error
      if (errorMessage.includes('network') || errorMessage.includes('connection')) {
        if (attempt < maxRetries) {
          await sleep(retryDelay * Math.pow(2, attempt))
          continue
        }
        return {
          success: false,
          path: r2Path,
          size: content.length,
          error: `Network error: ${lastError.message}`,
        }
      }

      if (errorMessage.includes('access') || errorMessage.includes('denied')) {
        return {
          success: false,
          path: r2Path,
          size: content.length,
          error: `Access denied: ${lastError.message}`,
        }
      }

      if (errorMessage.includes('quota') || errorMessage.includes('exceeded')) {
        return {
          success: false,
          path: r2Path,
          size: content.length,
          error: `Quota exceeded: ${lastError.message}`,
        }
      }

      // For temporary failures, retry
      if (errorMessage.includes('temporary') || errorMessage.includes('retry')) {
        if (attempt < maxRetries) {
          await sleep(retryDelay * Math.pow(2, attempt))
          continue
        }
      }

      // Unknown error - return gracefully
      return {
        success: false,
        path: r2Path,
        size: content.length,
        error: lastError.message,
      }
    }
  }

  // All retries exhausted
  return {
    success: true,
    path: r2Path,
    size: content.length,
    etag: '"success"', // This should only happen if retry succeeded on last attempt
  }
}

/**
 * Upload all parquet files in a directory to R2
 *
 * Supports parallel uploads with configurable concurrency.
 */
export async function uploadDirectoryToR2(
  bucket: R2Bucket,
  localDir: string,
  r2Prefix: string,
  options?: {
    concurrency?: number
    onProgress?: (progress: UploadProgress) => void
  }
): Promise<UploadDirectoryResult> {
  const concurrency = options?.concurrency ?? 5

  // Validate directory exists
  try {
    const stat = await fsPromises.stat(localDir)
    if (!stat.isDirectory()) {
      throw new Error(`Not a directory: ${localDir}`)
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new Error(`ENOENT: Directory not found: ${localDir}`)
    }
    throw error
  }

  // Find all parquet files recursively
  const parquetFiles = await findParquetFiles(localDir)

  // Calculate total bytes for progress tracking
  let totalBytes = 0
  const fileSizes = new Map<string, number>()
  for (const localPath of parquetFiles) {
    const stat = await fsPromises.stat(localPath)
    fileSizes.set(localPath, stat.size)
    totalBytes += stat.size
  }

  // Initialize progress tracker
  const tracker = new ProgressTracker({
    filesTotal: parquetFiles.length,
    bytesTotal: totalBytes,
    onProgress: options?.onProgress,
  })

  const result: UploadDirectoryResult = {
    success: true,
    uploaded: 0,
    failed: 0,
    files: [],
    errors: [],
  }

  // Create concurrency limiter
  const limit = createLimiter(concurrency)

  // Upload each file in parallel with concurrency limit
  const uploadPromises = parquetFiles.map(localPath =>
    limit(async () => {
      const relativePath = path.relative(localDir, localPath)
      const r2Path = path.join(r2Prefix, relativePath).replace(/\\/g, '/')
      const fileSize = fileSizes.get(localPath) ?? 0

      tracker.setCurrentFile(relativePath)

      try {
        const content = await fsPromises.readFile(localPath)
        const hash = crypto.createHash('sha256').update(content).digest('hex')

        await bucket.put(r2Path, new Uint8Array(content), {
          httpMetadata: {
            contentType: 'application/octet-stream',
          },
          customMetadata: {
            'x-parquedb-sha256': hash,
          },
        })

        result.uploaded++
        result.files.push({
          localPath,
          r2Path,
          size: content.length,
        })

        tracker.addBytes(fileSize)
        tracker.incrementFiles()
      } catch (error) {
        result.failed++
        result.errors.push({
          path: localPath,
          error: (error as Error).message,
        })
        tracker.incrementFiles()
      }
    })
  )

  await Promise.all(uploadPromises)

  // Mark as failed if any files failed
  if (result.failed > 0) {
    result.success = result.uploaded > 0 // Partial success
  }

  // Add metrics
  const stats = tracker.getStats()
  result.totalBytes = stats.totalBytes
  result.durationMs = stats.durationMs
  result.bytesPerSecond = stats.bytesPerSecond

  return result
}

/**
 * Verify that an uploaded file matches the local file
 */
export async function verifyUpload(
  bucket: R2Bucket,
  localPath: string,
  r2Path: string
): Promise<VerifyResult> {
  // Compute local file hash
  const localHash = await computeFileHash(localPath)

  // Get remote file
  const remoteObject = await bucket.head(r2Path)

  if (!remoteObject) {
    return {
      valid: false,
      localHash,
      error: 'Remote file not found',
    }
  }

  // Try to get hash from custom metadata
  const remoteHash = remoteObject.customMetadata?.['x-parquedb-sha256']

  if (remoteHash) {
    return {
      valid: localHash === remoteHash,
      localHash,
      remoteHash,
      method: 'metadata',
    }
  }

  // Fallback: download and compute hash
  const remoteBody = await bucket.get(r2Path)
  if (!remoteBody) {
    return {
      valid: false,
      localHash,
      error: 'Remote file not found',
    }
  }

  const remoteContent = await remoteBody.arrayBuffer()
  const computedRemoteHash = crypto
    .createHash('sha256')
    .update(new Uint8Array(remoteContent))
    .digest('hex')

  return {
    valid: localHash === computedRemoteHash,
    localHash,
    remoteHash: computedRemoteHash,
    method: 'content-comparison',
  }
}

/**
 * Sync a local directory to R2, only uploading changed files
 *
 * Features:
 * - Parallel uploads with configurable concurrency
 * - Progress tracking with callbacks
 * - Resume interrupted uploads via manifest
 * - Upload speed metrics
 *
 * Note: Unlike uploadDirectoryToR2, syncToR2 uses the relative path directly as the R2 path.
 * The r2Prefix parameter is used only for scoping the R2 list operation for orphan detection.
 */
export async function syncToR2(
  bucket: R2Bucket,
  localDir: string,
  r2Prefix: string,
  options?: SyncOptions
): Promise<SyncResult> {
  const dryRun = options?.dryRun ?? false
  const deleteOrphans = options?.deleteOrphans ?? false
  const concurrency = options?.concurrency ?? 5
  const manifestPath = options?.manifestPath

  // Load or create manifest for resume support
  let manifest: UploadManifest | null = null
  if (manifestPath) {
    manifest = await loadManifest(manifestPath)
    if (!manifest) {
      manifest = createManifest(localDir, r2Prefix)
    }
  }

  // Find all local parquet files
  const localFiles = await findParquetFiles(localDir)

  // Build set of local R2 paths (use relative path directly as R2 path)
  const localR2Paths = new Set<string>()
  for (const localPath of localFiles) {
    const relativePath = path.relative(localDir, localPath).replace(/\\/g, '/')
    localR2Paths.add(relativePath)
  }

  // Calculate total bytes for progress tracking (excluding already completed files from manifest)
  let totalBytes = 0
  const fileSizes = new Map<string, number>()
  const filesToProcess: string[] = []

  for (const localPath of localFiles) {
    const stat = await fsPromises.stat(localPath)
    const relativePath = path.relative(localDir, localPath).replace(/\\/g, '/')
    fileSizes.set(localPath, stat.size)

    // Check if already completed in manifest
    if (manifest?.completedFiles[relativePath]) {
      // Verify hash still matches
      const content = await fsPromises.readFile(localPath)
      const localHash = crypto.createHash('sha256').update(content).digest('hex')
      if (manifest.completedFiles[relativePath].hash === localHash) {
        continue // Skip already completed file
      }
    }

    totalBytes += stat.size
    filesToProcess.push(localPath)
  }

  // Initialize progress tracker
  const tracker = new ProgressTracker({
    filesTotal: localFiles.length,
    bytesTotal: totalBytes,
    onProgress: options?.onProgress,
  })

  const result: SyncResult = {
    total: localFiles.length,
    uploaded: 0,
    skipped: 0,
    failed: 0,
    deleted: 0,
    uploadedFiles: [],
    skippedFiles: [],
    deletedFiles: [],
    errors: [],
  }

  if (dryRun) {
    result.dryRun = true
    result.wouldUpload = 0
  }

  // Count files already completed from manifest
  if (manifest) {
    for (const localPath of localFiles) {
      const relativePath = path.relative(localDir, localPath).replace(/\\/g, '/')
      if (manifest.completedFiles[relativePath]) {
        const content = await fsPromises.readFile(localPath)
        const localHash = crypto.createHash('sha256').update(content).digest('hex')
        if (manifest.completedFiles[relativePath].hash === localHash) {
          result.skipped++
          result.skippedFiles.push(relativePath)
        }
      }
    }
  }

  // Create concurrency limiter
  const limit = createLimiter(concurrency)

  // Process files that need uploading
  const uploadPromises = filesToProcess.map(localPath =>
    limit(async () => {
      // Use relative path directly as R2 path (no prefix added)
      const r2Path = path.relative(localDir, localPath).replace(/\\/g, '/')
      const fileSize = fileSizes.get(localPath) ?? 0

      tracker.setCurrentFile(r2Path)

      // Compute local hash
      const localContent = await fsPromises.readFile(localPath)
      const localHash = crypto.createHash('sha256').update(localContent).digest('hex')

      // Check if file exists in R2 with same hash
      const remoteObject = await bucket.head(r2Path)
      if (remoteObject) {
        const remoteHash = remoteObject.customMetadata?.['x-parquedb-sha256']
        if (remoteHash === localHash) {
          result.skipped++
          result.skippedFiles.push(r2Path)
          tracker.incrementFiles()

          // Update manifest
          if (manifest) {
            manifest.completedFiles[r2Path] = {
              hash: localHash,
              size: fileSize,
              uploadedAt: new Date().toISOString(),
            }
          }

          return
        }
      }

      // File needs to be uploaded
      if (dryRun) {
        result.wouldUpload = (result.wouldUpload ?? 0) + 1
        tracker.incrementFiles()
        return
      }

      try {
        await bucket.put(r2Path, new Uint8Array(localContent), {
          httpMetadata: {
            contentType: 'application/octet-stream',
          },
          customMetadata: {
            'x-parquedb-sha256': localHash,
          },
        })
        result.uploaded++
        result.uploadedFiles.push(r2Path)
        tracker.addBytes(fileSize)
        tracker.incrementFiles()

        // Update manifest
        if (manifest) {
          manifest.completedFiles[r2Path] = {
            hash: localHash,
            size: fileSize,
            uploadedAt: new Date().toISOString(),
          }
          delete manifest.failedFiles[r2Path]

          // Save manifest periodically (every file for now, could optimize)
          if (manifestPath) {
            await saveManifest(manifestPath, manifest)
          }
        }
      } catch (error) {
        result.failed++
        result.errors.push({
          path: r2Path,
          error: (error as Error).message,
        })
        tracker.incrementFiles()

        // Update manifest with failure
        if (manifest) {
          const existing = manifest.failedFiles[r2Path]
          manifest.failedFiles[r2Path] = {
            error: (error as Error).message,
            attempts: (existing?.attempts ?? 0) + 1,
          }
          if (manifestPath) {
            await saveManifest(manifestPath, manifest)
          }
        }
      }
    })
  )

  await Promise.all(uploadPromises)

  // Handle orphan deletion
  if (deleteOrphans && !dryRun) {
    tracker.setOperation('deleting')

    // List all files in R2 with the prefix
    const r2Files = await listR2Files(bucket, r2Prefix)

    const deletePromises = r2Files
      .filter(r2Path => !localR2Paths.has(r2Path))
      .map(r2Path =>
        limit(async () => {
          try {
            await bucket.delete(r2Path)
            result.deleted++
            result.deletedFiles.push(r2Path)
          } catch (error) {
            result.errors.push({
              path: r2Path,
              error: `Failed to delete: ${(error as Error).message}`,
            })
          }
        })
      )

    await Promise.all(deletePromises)
  }

  // Add metrics
  const stats = tracker.getStats()
  result.totalBytes = stats.totalBytes
  result.durationMs = stats.durationMs
  result.bytesPerSecond = stats.bytesPerSecond

  // Clean up manifest on successful completion
  if (manifestPath && manifest && result.failed === 0) {
    // Optionally delete manifest or mark as complete
    manifest.updatedAt = new Date().toISOString()
    await saveManifest(manifestPath, manifest)
  }

  return result
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Recursively find all parquet files in a directory
 */
async function findParquetFiles(dir: string): Promise<string[]> {
  const files: string[] = []

  async function walk(currentDir: string): Promise<void> {
    const entries = await fsPromises.readdir(currentDir, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name)

      if (entry.isDirectory()) {
        await walk(fullPath)
      } else if (entry.isFile() && entry.name.endsWith('.parquet')) {
        files.push(fullPath)
      }
    }
  }

  await walk(dir)
  return files
}

/**
 * List all parquet files in R2 under a prefix
 */
async function listR2Files(bucket: R2Bucket, prefix: string): Promise<string[]> {
  const files: string[] = []
  let cursor: string | undefined

  do {
    const result = await bucket.list({
      prefix,
      cursor,
      include: ['customMetadata'],
    })

    for (const object of result.objects) {
      if (object.key.endsWith('.parquet')) {
        files.push(object.key)
      }
    }

    cursor = result.truncated ? result.cursor : undefined
  } while (cursor)

  return files
}

/**
 * Sleep for a specified number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
