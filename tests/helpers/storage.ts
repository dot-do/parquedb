/**
 * Test Storage Helpers
 *
 * Provides real storage backends for testing ParqueDB.
 * NO MOCKS - all backends use real storage implementations.
 *
 * Environment variables for R2Backend:
 * - R2_ACCESS_KEY_ID: S3-compatible access key ID
 * - R2_SECRET_ACCESS_KEY: S3-compatible secret access key
 * - R2_URL: S3-compatible endpoint URL (e.g., https://xxx.r2.cloudflarestorage.com)
 *
 * Usage:
 * ```typescript
 * import { createTestFsBackend, createTestR2Backend, cleanupTestStorage } from '../helpers/storage'
 *
 * describe('MyTest', () => {
 *   let storage: StorageBackend
 *
 *   beforeEach(async () => {
 *     storage = await createTestFsBackend()
 *   })
 *
 *   afterAll(async () => {
 *     await cleanupTestStorage()
 *   })
 * })
 * ```
 */

import { afterAll } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { R2Backend } from '../../src/storage/R2Backend'
import type { StorageBackend } from '../../src/types/storage'
import type { R2Bucket } from '../../src/storage/types/r2'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for test storage backends
 */
export interface TestStorageConfig {
  /** Optional prefix for test data isolation */
  prefix?: string
  /** Whether to automatically cleanup on afterAll (default: true) */
  autoCleanup?: boolean
}

/**
 * S3-compatible credentials for R2 backend
 */
interface S3Credentials {
  accessKeyId: string
  secretAccessKey: string
  endpoint: string
}

// =============================================================================
// Tracking for Cleanup
// =============================================================================

/** Temp directories created during tests */
const tempDirs: string[] = []

/** R2 prefixes created during tests */
const r2Prefixes: Array<{ backend: R2Backend; prefix: string }> = []

/** Generate unique test prefix */
function generateTestPrefix(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).slice(2, 8)
  return `test-${timestamp}-${random}`
}

// =============================================================================
// FsBackend Factory
// =============================================================================

/**
 * Create a FsBackend with a unique temporary directory
 *
 * The temp directory is automatically tracked for cleanup.
 * Call cleanupTestStorage() in afterAll to remove all temp directories.
 *
 * @param config - Optional configuration
 * @returns FsBackend instance with unique temp directory
 *
 * @example
 * ```typescript
 * const storage = await createTestFsBackend()
 * await storage.write('test.txt', new TextEncoder().encode('hello'))
 * ```
 */
export async function createTestFsBackend(config?: TestStorageConfig): Promise<FsBackend> {
  const prefix = config?.prefix ?? 'parquedb-test-'
  const tempDir = await mkdtemp(join(tmpdir(), prefix))
  tempDirs.push(tempDir)

  return new FsBackend(tempDir)
}

/**
 * Create a FsBackend with a specific directory path
 *
 * Use this when you need control over the directory location.
 * The directory is NOT automatically cleaned up.
 *
 * @param rootPath - Path to the root directory
 * @returns FsBackend instance
 */
export function createFsBackendWithPath(rootPath: string): FsBackend {
  return new FsBackend(rootPath)
}

// =============================================================================
// R2Backend Factory
// =============================================================================

/**
 * Load S3-compatible credentials from environment variables
 */
function loadS3Credentials(): S3Credentials | null {
  const accessKeyId = process.env.R2_ACCESS_KEY_ID
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY
  const endpoint = process.env.R2_URL

  if (!accessKeyId || !secretAccessKey || !endpoint) {
    return null
  }

  return { accessKeyId, secretAccessKey, endpoint }
}

/**
 * Helper to extract R2HTTPMetadata properties safely
 */
function extractHttpMetadata(
  metadata?: import('../../src/storage/types/r2').R2HTTPMetadata | Headers
): import('../../src/storage/types/r2').R2HTTPMetadata | undefined {
  if (!metadata) return undefined
  if (metadata instanceof Headers) {
    return {
      contentType: metadata.get('content-type') ?? undefined,
      cacheControl: metadata.get('cache-control') ?? undefined,
    }
  }
  return metadata
}

/**
 * Create an S3-compatible R2Bucket implementation using AWS SDK
 *
 * This creates a real R2Bucket-compatible interface using the @aws-sdk/client-s3
 * package to communicate with Cloudflare R2's S3-compatible API.
 *
 * Note: @aws-sdk/client-s3 is an optional dependency. Install it with:
 * npm install @aws-sdk/client-s3
 */
async function createS3CompatibleBucket(
  credentials: S3Credentials,
  bucketName: string
): Promise<R2Bucket> {
  // Dynamic import to avoid requiring aws-sdk in all environments
  // The module name is constructed to avoid TypeScript validation errors
  // when the optional dependency is not installed
  const moduleName = ['@aws-sdk', 'client-s3'].join('/')
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let S3Module: any
  try {
    S3Module = await import(/* @vite-ignore */ moduleName)
  } catch {
    throw new Error(
      '@aws-sdk/client-s3 is not installed. Run: npm install @aws-sdk/client-s3'
    )
  }

  const {
    S3Client,
    GetObjectCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
  } = S3Module

  const client = new S3Client({
    region: 'auto',
    endpoint: credentials.endpoint,
    credentials: {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
    },
  })

  // Implement R2Bucket interface using S3 client
  const bucket: R2Bucket = {
    async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<import('../../src/storage/types/r2').R2ObjectBody | null> {
      try {
        const command = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
          Range: options?.range
            ? `bytes=${options.range.offset}-${options.range.offset + options.range.length - 1}`
            : undefined,
        })

        const response = await client.send(command)
        if (!response.Body) return null

        // Convert stream to Uint8Array
        const chunks: Uint8Array[] = []
        for await (const chunk of response.Body as AsyncIterable<Uint8Array>) {
          chunks.push(chunk)
        }
        const data = new Uint8Array(Buffer.concat(chunks))

        return {
          key,
          version: response.VersionId ?? 'default',
          size: response.ContentLength ?? data.length,
          etag: response.ETag?.replace(/"/g, '') ?? '',
          httpEtag: response.ETag ?? '',
          uploaded: response.LastModified ?? new Date(),
          storageClass: 'Standard',
          checksums: {},
          httpMetadata: {
            contentType: response.ContentType,
            cacheControl: response.CacheControl,
          },
          customMetadata: response.Metadata,
          body: new ReadableStream({
            start(controller) {
              controller.enqueue(data)
              controller.close()
            },
          }),
          bodyUsed: false,
          arrayBuffer: async () => data.buffer as ArrayBuffer,
          text: async () => new TextDecoder().decode(data),
          json: async () => JSON.parse(new TextDecoder().decode(data)),
          blob: async () => new Blob([data]),
          writeHttpMetadata: () => {},
        }
      } catch (error: unknown) {
        if (error && typeof error === 'object' && 'name' in error && error.name === 'NoSuchKey') {
          return null
        }
        throw error
      }
    },

    async head(key: string): Promise<import('../../src/storage/types/r2').R2Object | null> {
      try {
        const command = new HeadObjectCommand({
          Bucket: bucketName,
          Key: key,
        })

        const response = await client.send(command)

        return {
          key,
          version: response.VersionId ?? 'default',
          size: response.ContentLength ?? 0,
          etag: response.ETag?.replace(/"/g, '') ?? '',
          httpEtag: response.ETag ?? '',
          uploaded: response.LastModified ?? new Date(),
          storageClass: 'Standard',
          checksums: {},
          httpMetadata: {
            contentType: response.ContentType,
            cacheControl: response.CacheControl,
          },
          customMetadata: response.Metadata,
          writeHttpMetadata: () => {},
        }
      } catch (error: unknown) {
        if (error && typeof error === 'object' && 'name' in error && error.name === 'NotFound') {
          return null
        }
        throw error
      }
    },

    async put(key: string, value: Uint8Array | ReadableStream, options?: import('../../src/storage/types/r2').R2PutOptions): Promise<import('../../src/storage/types/r2').R2Object> {
      // Convert ReadableStream to Uint8Array if needed
      let data: Uint8Array
      if (value instanceof ReadableStream) {
        const reader = value.getReader()
        const chunks: Uint8Array[] = []
        let done = false
        while (!done) {
          const result = await reader.read()
          done = result.done
          if (result.value) {
            chunks.push(result.value)
          }
        }
        data = new Uint8Array(Buffer.concat(chunks))
      } else {
        data = value
      }

      const httpMeta = extractHttpMetadata(options?.httpMetadata)

      const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        Body: data,
        ContentType: httpMeta?.contentType,
        CacheControl: httpMeta?.cacheControl,
        Metadata: options?.customMetadata,
      })

      const response = await client.send(command)

      return {
        key,
        version: response.VersionId ?? 'default',
        size: data.length,
        etag: response.ETag?.replace(/"/g, '') ?? '',
        httpEtag: response.ETag ?? '',
        uploaded: new Date(),
        storageClass: 'Standard',
        checksums: {},
        httpMetadata: httpMeta,
        customMetadata: options?.customMetadata,
        writeHttpMetadata: () => {},
      }
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys]

      if (keyArray.length === 1) {
        const command = new DeleteObjectCommand({
          Bucket: bucketName,
          Key: keyArray[0],
        })
        await client.send(command)
      } else if (keyArray.length > 0) {
        const command = new DeleteObjectsCommand({
          Bucket: bucketName,
          Delete: {
            Objects: keyArray.map(k => ({ Key: k })),
          },
        })
        await client.send(command)
      }
    },

    async list(options?: { prefix?: string; limit?: number; cursor?: string; delimiter?: string; include?: ('httpMetadata' | 'customMetadata')[] }): Promise<import('../../src/storage/types/r2').R2Objects> {
      const command = new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: options?.prefix,
        MaxKeys: options?.limit,
        ContinuationToken: options?.cursor,
        Delimiter: options?.delimiter,
      })

      const response = await client.send(command)

      const objects = (response.Contents ?? []).map(obj => ({
        key: obj.Key ?? '',
        version: 'default',
        size: obj.Size ?? 0,
        etag: obj.ETag?.replace(/"/g, '') ?? '',
        httpEtag: obj.ETag ?? '',
        uploaded: obj.LastModified ?? new Date(),
        storageClass: 'Standard',
        checksums: {},
        writeHttpMetadata: () => {},
      }))

      return {
        objects,
        truncated: response.IsTruncated ?? false,
        cursor: response.NextContinuationToken,
        delimitedPrefixes: response.CommonPrefixes?.map(p => p.Prefix ?? '') ?? [],
      }
    },

    async createMultipartUpload(key: string, options?: import('../../src/storage/types/r2').R2MultipartOptions): Promise<import('../../src/storage/types/r2').R2MultipartUpload> {
      const httpMeta = extractHttpMetadata(options?.httpMetadata)

      const command = new CreateMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        ContentType: httpMeta?.contentType,
        Metadata: options?.customMetadata,
      })

      const response = await client.send(command)
      const uploadId = response.UploadId!

      return {
        key,
        uploadId,

        async uploadPart(partNumber: number, data: Uint8Array): Promise<import('../../src/storage/types/r2').R2UploadedPart> {
          const partCommand = new UploadPartCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
            PartNumber: partNumber,
            Body: data,
          })

          const partResponse = await client.send(partCommand)

          return {
            partNumber,
            etag: partResponse.ETag ?? '',
          }
        },

        async complete(parts: import('../../src/storage/types/r2').R2UploadedPart[]): Promise<import('../../src/storage/types/r2').R2Object> {
          const completeCommand = new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
            MultipartUpload: {
              Parts: parts.map(p => ({
                PartNumber: p.partNumber,
                ETag: p.etag,
              })),
            },
          })

          const completeResponse = await client.send(completeCommand)

          return {
            key,
            version: completeResponse.VersionId ?? 'default',
            size: 0, // Size is unknown after multipart complete
            etag: completeResponse.ETag?.replace(/"/g, '') ?? '',
            httpEtag: completeResponse.ETag ?? '',
            uploaded: new Date(),
            storageClass: 'Standard',
            checksums: {},
            writeHttpMetadata: () => {},
          }
        },

        async abort(): Promise<void> {
          const abortCommand = new AbortMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
          })

          await client.send(abortCommand)
        },
      }
    },

    resumeMultipartUpload(_key: string, _uploadId: string): import('../../src/storage/types/r2').R2MultipartUpload {
      throw new Error('resumeMultipartUpload not implemented for S3-compatible client')
    },
  }

  return bucket
}

/**
 * Create an R2Backend with S3-compatible credentials from .env
 *
 * Requires the following environment variables:
 * - R2_ACCESS_KEY_ID
 * - R2_SECRET_ACCESS_KEY
 * - R2_URL
 *
 * Each test gets a unique prefix to isolate test data.
 * Call cleanupTestStorage() in afterAll to remove all test data.
 *
 * @param config - Optional configuration
 * @returns R2Backend instance with unique test prefix
 * @throws Error if environment variables are not set
 *
 * @example
 * ```typescript
 * const storage = await createTestR2Backend()
 * await storage.write('test.txt', new TextEncoder().encode('hello'))
 * ```
 */
export async function createTestR2Backend(config?: TestStorageConfig): Promise<R2Backend> {
  const credentials = loadS3Credentials()

  if (!credentials) {
    throw new Error(
      'R2 credentials not found. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_URL environment variables.'
    )
  }

  // Parse bucket name from endpoint URL
  // Format: https://<account_id>.r2.cloudflarestorage.com
  // We'll use a default bucket name 'parquedb-test' or extract from URL
  const bucketName = process.env.R2_BUCKET ?? 'parquedb-test'

  // Create S3-compatible bucket
  const bucket = await createS3CompatibleBucket(credentials, bucketName)

  // Generate unique prefix for test isolation
  const testPrefix = config?.prefix ?? generateTestPrefix()
  const fullPrefix = `tests/${testPrefix}/`

  const backend = new R2Backend(bucket, { prefix: fullPrefix })

  // Track for cleanup
  r2Prefixes.push({ backend, prefix: fullPrefix })

  return backend
}

/**
 * Check if R2 credentials are available
 *
 * Use this to skip R2 tests when credentials aren't configured.
 *
 * @example
 * ```typescript
 * describe.skipIf(!hasR2Credentials())('R2Backend tests', () => {
 *   // tests here
 * })
 * ```
 */
export function hasR2Credentials(): boolean {
  return loadS3Credentials() !== null
}

// =============================================================================
// Cleanup Functions
// =============================================================================

/**
 * Clean up a specific FsBackend temp directory
 */
export async function cleanupFsBackend(backend: FsBackend): Promise<void> {
  try {
    await rm(backend.rootPath, { recursive: true, force: true })
    const index = tempDirs.indexOf(backend.rootPath)
    if (index > -1) {
      tempDirs.splice(index, 1)
    }
  } catch {
    // Ignore errors during cleanup
  }
}

/**
 * Clean up a specific R2Backend prefix
 */
export async function cleanupR2Backend(backend: R2Backend): Promise<void> {
  try {
    // Delete all objects with the test prefix
    await backend.deletePrefix('')
    const index = r2Prefixes.findIndex(r => r.backend === backend)
    if (index > -1) {
      r2Prefixes.splice(index, 1)
    }
  } catch {
    // Ignore errors during cleanup
  }
}

/**
 * Clean up all test storage created during tests
 *
 * Removes all temp directories and R2 test data.
 * Should be called in afterAll hook.
 *
 * @example
 * ```typescript
 * afterAll(async () => {
 *   await cleanupTestStorage()
 * })
 * ```
 */
export async function cleanupTestStorage(): Promise<void> {
  // Cleanup temp directories
  const dirCleanupPromises = tempDirs.map(async dir => {
    try {
      await rm(dir, { recursive: true, force: true })
    } catch {
      // Ignore errors
    }
  })

  // Cleanup R2 prefixes
  const r2CleanupPromises = r2Prefixes.map(async ({ backend }) => {
    try {
      await backend.deletePrefix('')
    } catch {
      // Ignore errors
    }
  })

  await Promise.all([...dirCleanupPromises, ...r2CleanupPromises])

  // Clear tracking arrays
  tempDirs.length = 0
  r2Prefixes.length = 0
}

// =============================================================================
// Auto-cleanup Hook
// =============================================================================

/**
 * Register automatic cleanup for all test storage
 *
 * Call this once at the module level to automatically cleanup
 * after all tests complete.
 *
 * @example
 * ```typescript
 * import { registerAutoCleanup } from '../helpers/storage'
 *
 * registerAutoCleanup()
 *
 * describe('MyTests', () => {
 *   // tests here - cleanup happens automatically
 * })
 * ```
 */
export function registerAutoCleanup(): void {
  afterAll(async () => {
    await cleanupTestStorage()
  })
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a test storage backend based on environment
 *
 * Returns FsBackend if no R2 credentials are available,
 * otherwise returns R2Backend.
 *
 * @param preferR2 - If true, prefer R2Backend when available (default: false)
 * @returns StorageBackend instance
 */
export async function createTestStorageBackend(
  preferR2 = false
): Promise<StorageBackend> {
  if (preferR2 && hasR2Credentials()) {
    return createTestR2Backend()
  }
  return createTestFsBackend()
}

/**
 * Get all currently tracked temp directories
 *
 * Useful for debugging cleanup issues.
 */
export function getTrackedTempDirs(): readonly string[] {
  return [...tempDirs]
}

/**
 * Get all currently tracked R2 prefixes
 *
 * Useful for debugging cleanup issues.
 */
export function getTrackedR2Prefixes(): readonly string[] {
  return r2Prefixes.map(r => r.prefix)
}
