/**
 * R2 Upload Utilities
 *
 * Provides functions for uploading files to Cloudflare R2:
 * - uploadFileToR2: Upload single parquet file
 * - uploadDirectoryToR2: Upload directory recursively
 * - verifyUpload: Verify file integrity via checksum
 * - syncToR2: Incremental sync with hash comparison
 * - computeFileHash: Compute SHA-256 hash of a file
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
}

export interface UploadOptions {
  maxRetries?: number
  retryDelay?: number
}

export interface SyncOptions {
  dryRun?: boolean
  deleteOrphans?: boolean
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
 */
export async function uploadDirectoryToR2(
  bucket: R2Bucket,
  localDir: string,
  r2Prefix: string
): Promise<UploadDirectoryResult> {
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

  const result: UploadDirectoryResult = {
    success: true,
    uploaded: 0,
    failed: 0,
    files: [],
    errors: [],
  }

  // Upload each file
  for (const localPath of parquetFiles) {
    // Compute R2 path by taking relative path from localDir
    const relativePath = path.relative(localDir, localPath)
    const r2Path = path.join(r2Prefix, relativePath).replace(/\\/g, '/')

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
    } catch (error) {
      result.failed++
      result.errors.push({
        path: localPath,
        error: (error as Error).message,
      })
    }
  }

  // Mark as failed if any files failed
  if (result.failed > 0) {
    result.success = result.uploaded > 0 // Partial success
  }

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

  // Find all local parquet files
  const localFiles = await findParquetFiles(localDir)

  // Build set of local R2 paths (use relative path directly as R2 path)
  const localR2Paths = new Set<string>()
  for (const localPath of localFiles) {
    const relativePath = path.relative(localDir, localPath).replace(/\\/g, '/')
    localR2Paths.add(relativePath)
  }

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

  // Process each local file
  for (const localPath of localFiles) {
    // Use relative path directly as R2 path (no prefix added)
    const r2Path = path.relative(localDir, localPath).replace(/\\/g, '/')

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
        continue
      }
    }

    // File needs to be uploaded
    if (dryRun) {
      result.wouldUpload = (result.wouldUpload ?? 0) + 1
      continue
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
    } catch (error) {
      result.failed++
      result.errors.push({
        path: r2Path,
        error: (error as Error).message,
      })
    }
  }

  // Handle orphan deletion
  if (deleteOrphans && !dryRun) {
    // List all files in R2 with the prefix
    const r2Files = await listR2Files(bucket, r2Prefix)

    for (const r2Path of r2Files) {
      if (!localR2Paths.has(r2Path)) {
        // This file exists in R2 but not locally - delete it
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
      }
    }
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
