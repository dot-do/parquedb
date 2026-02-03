/**
 * SyncClient - Client for interacting with ParqueDB remote service
 *
 * Handles authentication, presigned URLs, and manifest operations
 * for push/pull/sync commands.
 */

import type { SyncManifest } from './manifest'
import type { Visibility } from '../types/visibility'
import { asBodyInit } from '../types/cast'
import { DEFAULT_REMOTE_TIMEOUT } from '../constants'
import {
  validateResponseFields,
  validateResponseArray,
  isRecord,
  isString,
  isArray,
} from '../utils/json-validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for SyncClient
 */
export interface SyncClientConfig {
  /** Remote service base URL */
  baseUrl: string

  /** Authentication token */
  token: string

  /** Request timeout in milliseconds */
  timeout?: number | undefined
}

/**
 * Database registration result
 */
export interface RegisterResult {
  /** Whether registration was successful */
  success: boolean

  /** Database ID if successful */
  databaseId?: string | undefined

  /** R2 bucket name */
  bucket?: string | undefined

  /** Path prefix in bucket */
  prefix?: string | undefined

  /** Error message if failed */
  error?: string | undefined
}

/**
 * Presigned URL for upload
 */
export interface PresignedUploadUrl {
  /** The path being uploaded */
  path: string

  /** Presigned URL for PUT request */
  url: string

  /** HTTP headers to include with the upload */
  headers: Record<string, string>

  /** Expiration timestamp (ISO string) */
  expiresAt: string
}

/**
 * Presigned URL for download
 */
export interface PresignedDownloadUrl {
  /** The path being downloaded */
  path: string

  /** Presigned URL for GET request */
  url: string

  /** Expiration timestamp (ISO string) */
  expiresAt: string
}

/**
 * Database lookup result
 */
export interface DatabaseInfo {
  /** Database ID */
  id: string

  /** Database name */
  name: string

  /** Owner username */
  owner: string

  /** URL slug */
  slug: string

  /** Visibility level */
  visibility: Visibility

  /** R2 bucket name */
  bucket: string

  /** Path prefix in bucket */
  prefix?: string | undefined

  /** Collection count */
  collectionCount?: number | undefined

  /** Entity count */
  entityCount?: number | undefined
}

// =============================================================================
// SyncClient Implementation
// =============================================================================

/**
 * Client for interacting with ParqueDB remote sync service
 */
export class SyncClient {
  private baseUrl: string
  private token: string
  private timeout: number

  constructor(config: SyncClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, '')
    this.token = config.token
    this.timeout = config.timeout ?? DEFAULT_REMOTE_TIMEOUT
  }

  // ===========================================================================
  // Database Registration
  // ===========================================================================

  /**
   * Register a database with the remote service
   */
  async registerDatabase(options: {
    name: string
    visibility: Visibility
    slug?: string | undefined
    owner: string
  }): Promise<RegisterResult> {
    try {
      const response = await this.fetch('/api/sync/register', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(options),
      })

      if (!response.ok) {
        const error = await response.text()
        return { success: false, error }
      }

      const data = await response.json()
      validateResponseFields(data, ['id', 'bucket'], 'registerDatabase')
      const record = data as Record<string, unknown>

      return {
        success: true,
        databaseId: String(record.id),
        bucket: String(record.bucket),
        prefix: typeof record.prefix === 'string' ? record.prefix : undefined,
      }
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Network error',
      }
    }
  }

  /**
   * Look up a database by owner/slug
   */
  async lookupDatabase(owner: string, slug: string): Promise<DatabaseInfo | null> {
    try {
      const response = await this.fetch(`/api/db/${owner}/${slug}`)

      if (!response.ok) {
        return null
      }

      const data = await response.json()
      validateResponseFields(data, ['id', 'name', 'owner', 'slug', 'visibility', 'bucket'], 'lookupDatabase')
      return data as DatabaseInfo
    } catch {
      return null
    }
  }

  // ===========================================================================
  // Presigned URLs
  // ===========================================================================

  /**
   * Get presigned URLs for uploading files
   */
  async getUploadUrls(
    databaseId: string,
    files: Array<{ path: string; size: number; contentType?: string | undefined }>
  ): Promise<PresignedUploadUrl[]> {
    const response = await this.fetch('/api/sync/upload-urls', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        databaseId,
        files,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to get upload URLs: ${error}`)
    }

    const data = await response.json()
    validateResponseFields(data, ['urls'], 'getUploadUrls')
    const record = data as Record<string, unknown>
    validateResponseArray(record.urls, 'getUploadUrls.urls')
    return record.urls as PresignedUploadUrl[]
  }

  /**
   * Get presigned URLs for downloading files
   */
  async getDownloadUrls(
    databaseId: string,
    paths: string[]
  ): Promise<PresignedDownloadUrl[]> {
    const response = await this.fetch('/api/sync/download-urls', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        databaseId,
        paths,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to get download URLs: ${error}`)
    }

    const data = await response.json()
    validateResponseFields(data, ['urls'], 'getDownloadUrls')
    const record = data as Record<string, unknown>
    validateResponseArray(record.urls, 'getDownloadUrls.urls')
    return record.urls as PresignedDownloadUrl[]
  }

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  /**
   * Get the remote manifest for a database
   */
  async getManifest(databaseId: string): Promise<SyncManifest | null> {
    try {
      const response = await this.fetch(`/api/sync/manifest/${databaseId}`)

      if (response.status === 404) {
        return null
      }

      if (!response.ok) {
        throw new Error(`Failed to get manifest: ${response.statusText}`)
      }

      const data = await response.json()
      validateResponseFields(data, ['version', 'files'], 'getManifest')
      return data as SyncManifest
    } catch (error) {
      if (error instanceof Error && error.message.includes('404')) {
        return null
      }
      throw error
    }
  }

  /**
   * Update the remote manifest
   */
  async updateManifest(databaseId: string, manifest: SyncManifest): Promise<void> {
    const response = await this.fetch(`/api/sync/manifest/${databaseId}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(manifest),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to update manifest: ${error}`)
    }
  }

  // ===========================================================================
  // File Operations
  // ===========================================================================

  /**
   * Upload a file using a presigned URL
   */
  async uploadFile(
    presignedUrl: PresignedUploadUrl,
    data: Uint8Array
  ): Promise<void> {
    const response = await fetch(presignedUrl.url, {
      method: 'PUT',
      headers: presignedUrl.headers,
      body: asBodyInit(data),
    })

    if (!response.ok) {
      throw new Error(`Upload failed for ${presignedUrl.path}: ${response.statusText}`)
    }
  }

  /**
   * Download a file using a presigned URL
   */
  async downloadFile(presignedUrl: PresignedDownloadUrl): Promise<Uint8Array> {
    const response = await fetch(presignedUrl.url)

    if (!response.ok) {
      throw new Error(`Download failed for ${presignedUrl.path}: ${response.statusText}`)
    }

    const buffer = await response.arrayBuffer()
    return new Uint8Array(buffer)
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Make an authenticated fetch request
   */
  private async fetch(path: string, init?: RequestInit): Promise<Response> {
    const url = `${this.baseUrl}${path}`

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeout)

    try {
      const response = await fetch(url, {
        ...init,
        headers: {
          ...init?.headers,
          'Authorization': `Bearer ${this.token}`,
        },
        signal: controller.signal,
      })

      return response
    } finally {
      clearTimeout(timeoutId)
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a SyncClient instance
 */
export function createSyncClient(config: SyncClientConfig): SyncClient {
  return new SyncClient(config)
}
