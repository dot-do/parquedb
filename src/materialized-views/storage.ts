/**
 * MV Storage Manager
 *
 * Handles persistence of materialized view definitions, metadata, and data.
 * Integrates with existing StorageBackend implementations.
 *
 * Storage Layout:
 *   _views/
 *     manifest.json           - Index of all MVs
 *     {viewName}/
 *       metadata.json         - View definition and state
 *       data/
 *         *.parquet           - Materialized data files
 */

import type { StorageBackend, WriteResult } from '../types/storage'
import type {
  ViewDefinition,
  ViewMetadata,
  ViewState,
  ViewStats,
} from './types'

// =============================================================================
// Storage Paths
// =============================================================================

/** Standard paths for MV storage */
export const MVStoragePaths = {
  /** Root directory for all views */
  root: '_views',

  /** Manifest file containing index of all views */
  manifest: '_views/manifest.json',

  /** Directory for a specific view */
  viewDir: (name: string) => `_views/${name}`,

  /** Metadata file for a view */
  viewMetadata: (name: string) => `_views/${name}/metadata.json`,

  /** Data directory for a view */
  viewData: (name: string) => `_views/${name}/data`,

  /** Data file for a view (single file mode) */
  viewDataFile: (name: string) => `_views/${name}/data/data.parquet`,

  /** Data shard for a view (multi-file mode) */
  viewDataShard: (name: string, shard: number) =>
    `_views/${name}/data/data.${shard.toString().padStart(4, '0')}.parquet`,

  /** Stats file for a view */
  viewStats: (name: string) => `_views/${name}/stats.json`,
} as const

// =============================================================================
// Manifest Types
// =============================================================================

/** Entry in the MV manifest */
export interface MVManifestEntry {
  /** View name */
  name: string

  /** Current state */
  state: ViewState

  /** Source collection */
  source: string

  /** When the view was created */
  createdAt: string

  /** When the view was last refreshed */
  lastRefreshedAt?: string | undefined

  /** Metadata file path */
  metadataPath: string
}

/** MV manifest (index of all views) */
export interface MVManifest {
  /** Format version */
  version: number

  /** Last updated timestamp */
  updatedAt: string

  /** All registered views */
  views: MVManifestEntry[]
}

/** Current manifest version */
export const MV_MANIFEST_VERSION = 1

// =============================================================================
// Errors
// =============================================================================

/** Base error for MV storage operations */
export class MVStorageError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly viewName?: string
  ) {
    super(message)
    this.name = 'MVStorageError'
  }
}

/** Error when a view is not found */
export class MVNotFoundError extends MVStorageError {
  constructor(name: string) {
    super(`Materialized view not found: ${name}`, 'MV_NOT_FOUND', name)
    this.name = 'MVNotFoundError'
  }
}

/** Error when a view already exists */
export class MVAlreadyExistsError extends MVStorageError {
  constructor(name: string) {
    super(`Materialized view already exists: ${name}`, 'MV_ALREADY_EXISTS', name)
    this.name = 'MVAlreadyExistsError'
  }
}

/** Error when manifest is corrupted or invalid */
export class MVManifestError extends MVStorageError {
  constructor(message: string) {
    super(message, 'MV_MANIFEST_ERROR')
    this.name = 'MVManifestError'
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/** Encode data as JSON bytes */
function encodeJson(data: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(data, null, 2))
}

/** Decode JSON bytes */
function decodeJson<T>(data: Uint8Array): T {
  return JSON.parse(new TextDecoder().decode(data)) as T
}

/** Create an empty manifest */
function createEmptyManifest(): MVManifest {
  return {
    version: MV_MANIFEST_VERSION,
    updatedAt: new Date().toISOString(),
    views: [],
  }
}

/** Create default view metadata */
function createDefaultMetadata(definition: ViewDefinition): ViewMetadata {
  return {
    definition,
    state: 'pending',
    createdAt: new Date(),
    version: 1,
  }
}

/** Create default view stats */
function createDefaultStats(): ViewStats {
  return {
    totalRefreshes: 0,
    successfulRefreshes: 0,
    failedRefreshes: 0,
    avgRefreshDurationMs: 0,
    queryCount: 0,
    cacheHitRatio: 0,
  }
}

// =============================================================================
// MV Storage Manager
// =============================================================================

/**
 * Manages storage of materialized view definitions, metadata, and data.
 *
 * This class provides a high-level API for:
 * - Creating and registering views
 * - Loading and saving view metadata
 * - Managing the view manifest (index)
 * - Storing and retrieving view data
 *
 * @example
 * ```typescript
 * const storage = new MVStorageManager(backend)
 *
 * // Create a new view
 * await storage.createView({
 *   name: viewName('active-users'),
 *   source: 'users',
 *   query: { filter: { status: 'active' } },
 *   options: { refreshMode: 'scheduled', schedule: { cron: '0 * * * *' } }
 * })
 *
 * // Get view metadata
 * const metadata = await storage.getViewMetadata('active-users')
 *
 * // List all views
 * const views = await storage.listViews()
 * ```
 */
export class MVStorageManager {
  private backend: StorageBackend
  private manifestCache: MVManifest | null = null

  constructor(backend: StorageBackend) {
    this.backend = backend
  }

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  /**
   * Load the MV manifest from storage.
   * Creates an empty manifest if it doesn't exist.
   */
  async loadManifest(): Promise<MVManifest> {
    // Return cached manifest if available
    if (this.manifestCache) {
      return this.manifestCache
    }

    try {
      const exists = await this.backend.exists(MVStoragePaths.manifest)
      if (!exists) {
        // Create new empty manifest
        const manifest = createEmptyManifest()
        await this.saveManifest(manifest)
        return manifest
      }

      const data = await this.backend.read(MVStoragePaths.manifest)
      const manifest = decodeJson<MVManifest>(data)

      // Validate manifest version
      if (manifest.version !== MV_MANIFEST_VERSION) {
        throw new MVManifestError(
          `Unsupported manifest version: ${manifest.version} (expected ${MV_MANIFEST_VERSION})`
        )
      }

      this.manifestCache = manifest
      return manifest
    } catch (error) {
      if (error instanceof MVManifestError) {
        throw error
      }
      throw new MVManifestError(`Failed to load manifest: ${error}`)
    }
  }

  /**
   * Save the MV manifest to storage.
   */
  async saveManifest(manifest: MVManifest): Promise<WriteResult> {
    manifest.updatedAt = new Date().toISOString()
    const data = encodeJson(manifest)
    const result = await this.backend.writeAtomic(MVStoragePaths.manifest, data, {
      contentType: 'application/json',
    })
    this.manifestCache = manifest
    return result
  }

  /**
   * Invalidate the manifest cache.
   * Call this after external modifications to the manifest.
   */
  invalidateManifestCache(): void {
    this.manifestCache = null
  }

  // ===========================================================================
  // View CRUD Operations
  // ===========================================================================

  /**
   * Create a new materialized view.
   *
   * @param definition - View definition including name, source, query, and options
   * @returns The created view metadata
   * @throws MVAlreadyExistsError if a view with the same name exists
   */
  async createView(definition: ViewDefinition): Promise<ViewMetadata> {
    const name = definition.name as string

    // Load manifest and check for duplicates
    const manifest = await this.loadManifest()
    const exists = manifest.views.some((v) => v.name === name)
    if (exists) {
      throw new MVAlreadyExistsError(name)
    }

    // Create metadata
    const metadata = createDefaultMetadata(definition)

    // Create view directory structure
    await this.backend.mkdir(MVStoragePaths.viewDir(name))
    await this.backend.mkdir(MVStoragePaths.viewData(name))

    // Save metadata
    await this.saveViewMetadata(name, metadata)

    // Save initial stats
    await this.saveViewStats(name, createDefaultStats())

    // Update manifest
    manifest.views.push({
      name,
      state: metadata.state,
      source: definition.source,
      createdAt: metadata.createdAt.toISOString(),
      metadataPath: MVStoragePaths.viewMetadata(name),
    })
    await this.saveManifest(manifest)

    return metadata
  }

  /**
   * Get view metadata by name.
   *
   * @param name - View name
   * @returns View metadata
   * @throws MVNotFoundError if view doesn't exist
   */
  async getViewMetadata(name: string): Promise<ViewMetadata> {
    const metadataPath = MVStoragePaths.viewMetadata(name)

    const exists = await this.backend.exists(metadataPath)
    if (!exists) {
      throw new MVNotFoundError(name)
    }

    const data = await this.backend.read(metadataPath)
    const raw = decodeJson<Record<string, unknown>>(data)

    // Convert date strings back to Date objects
    return {
      ...raw,
      definition: raw.definition as ViewDefinition,
      state: raw.state as ViewState,
      createdAt: new Date(raw.createdAt as string),
      lastRefreshedAt: raw.lastRefreshedAt ? new Date(raw.lastRefreshedAt as string) : undefined,
      nextRefreshAt: raw.nextRefreshAt ? new Date(raw.nextRefreshAt as string) : undefined,
      version: raw.version as number,
    } as ViewMetadata
  }

  /**
   * Save view metadata.
   *
   * @param name - View name
   * @param metadata - View metadata to save
   */
  async saveViewMetadata(name: string, metadata: ViewMetadata): Promise<WriteResult> {
    const metadataPath = MVStoragePaths.viewMetadata(name)
    const data = encodeJson({
      ...metadata,
      createdAt: metadata.createdAt.toISOString(),
      lastRefreshedAt: metadata.lastRefreshedAt?.toISOString(),
      nextRefreshAt: metadata.nextRefreshAt?.toISOString(),
    })
    return this.backend.writeAtomic(metadataPath, data, {
      contentType: 'application/json',
    })
  }

  /**
   * Update view state.
   *
   * @param name - View name
   * @param state - New state
   * @param error - Error message if state is 'error'
   */
  async updateViewState(name: string, state: ViewState, error?: string): Promise<void> {
    const metadata = await this.getViewMetadata(name)
    metadata.state = state
    metadata.error = error
    metadata.version += 1
    await this.saveViewMetadata(name, metadata)

    // Update manifest
    const manifest = await this.loadManifest()
    const entry = manifest.views.find((v) => v.name === name)
    if (entry) {
      entry.state = state
      await this.saveManifest(manifest)
    }
  }

  /**
   * Delete a materialized view.
   *
   * @param name - View name
   * @returns true if deleted, false if not found
   */
  async deleteView(name: string): Promise<boolean> {
    const manifest = await this.loadManifest()
    const index = manifest.views.findIndex((v) => v.name === name)

    if (index === -1) {
      return false
    }

    // Remove from manifest first
    manifest.views.splice(index, 1)
    await this.saveManifest(manifest)

    // Delete view directory and all contents
    try {
      await this.backend.rmdir(MVStoragePaths.viewDir(name), { recursive: true })
    } catch {
      // Ignore errors during cleanup - manifest is already updated
    }

    return true
  }

  /**
   * Check if a view exists.
   *
   * @param name - View name
   */
  async viewExists(name: string): Promise<boolean> {
    const manifest = await this.loadManifest()
    return manifest.views.some((v) => v.name === name)
  }

  /**
   * List all registered views.
   *
   * @returns Array of manifest entries
   */
  async listViews(): Promise<MVManifestEntry[]> {
    const manifest = await this.loadManifest()
    return [...manifest.views]
  }

  /**
   * Get all view metadata (full details for each view).
   */
  async getAllViewMetadata(): Promise<ViewMetadata[]> {
    const manifest = await this.loadManifest()
    const results: ViewMetadata[] = []

    for (const entry of manifest.views) {
      try {
        const metadata = await this.getViewMetadata(entry.name)
        results.push(metadata)
      } catch {
        // Skip views with missing/corrupt metadata
      }
    }

    return results
  }

  // ===========================================================================
  // View Stats Operations
  // ===========================================================================

  /**
   * Get view statistics.
   *
   * @param name - View name
   */
  async getViewStats(name: string): Promise<ViewStats> {
    const statsPath = MVStoragePaths.viewStats(name)

    const exists = await this.backend.exists(statsPath)
    if (!exists) {
      return createDefaultStats()
    }

    const data = await this.backend.read(statsPath)
    return decodeJson<ViewStats>(data)
  }

  /**
   * Save view statistics.
   *
   * @param name - View name
   * @param stats - View statistics
   */
  async saveViewStats(name: string, stats: ViewStats): Promise<WriteResult> {
    const statsPath = MVStoragePaths.viewStats(name)
    const data = encodeJson(stats)
    return this.backend.write(statsPath, data, {
      contentType: 'application/json',
    })
  }

  /**
   * Update view stats after a refresh.
   *
   * @param name - View name
   * @param success - Whether the refresh was successful
   * @param durationMs - Duration of the refresh in milliseconds
   */
  async recordRefresh(name: string, success: boolean, durationMs: number): Promise<void> {
    const stats = await this.getViewStats(name)

    stats.totalRefreshes += 1
    if (success) {
      stats.successfulRefreshes += 1
    } else {
      stats.failedRefreshes += 1
    }

    // Update running average of duration
    const totalDuration = stats.avgRefreshDurationMs * (stats.totalRefreshes - 1) + durationMs
    stats.avgRefreshDurationMs = totalDuration / stats.totalRefreshes

    await this.saveViewStats(name, stats)

    // Update metadata last refresh time
    if (success) {
      const metadata = await this.getViewMetadata(name)
      metadata.lastRefreshedAt = new Date()
      metadata.lastRefreshDurationMs = durationMs
      await this.saveViewMetadata(name, metadata)

      // Update manifest
      const manifest = await this.loadManifest()
      const entry = manifest.views.find((v) => v.name === name)
      if (entry) {
        entry.lastRefreshedAt = metadata.lastRefreshedAt.toISOString()
        await this.saveManifest(manifest)
      }
    }
  }

  /**
   * Record a query against a view.
   *
   * @param name - View name
   * @param cacheHit - Whether the query was served from cache
   */
  async recordQuery(name: string, cacheHit: boolean): Promise<void> {
    const stats = await this.getViewStats(name)

    stats.queryCount += 1

    // Update cache hit ratio using exponential moving average
    const alpha = 0.1 // Smoothing factor
    const hit = cacheHit ? 1 : 0
    stats.cacheHitRatio = alpha * hit + (1 - alpha) * stats.cacheHitRatio

    await this.saveViewStats(name, stats)
  }

  // ===========================================================================
  // View Data Operations
  // ===========================================================================

  /**
   * Get the data file path for a view.
   *
   * @param name - View name
   */
  getDataFilePath(name: string): string {
    return MVStoragePaths.viewDataFile(name)
  }

  /**
   * Get the data shard path for a view.
   *
   * @param name - View name
   * @param shard - Shard number
   */
  getDataShardPath(name: string, shard: number): string {
    return MVStoragePaths.viewDataShard(name, shard)
  }

  /**
   * Write view data (Parquet file).
   *
   * @param name - View name
   * @param data - Parquet file bytes
   */
  async writeViewData(name: string, data: Uint8Array): Promise<WriteResult> {
    const dataPath = MVStoragePaths.viewDataFile(name)
    return this.backend.writeAtomic(dataPath, data, {
      contentType: 'application/vnd.apache.parquet',
    })
  }

  /**
   * Write view data shard (for large views).
   *
   * @param name - View name
   * @param shard - Shard number
   * @param data - Parquet file bytes
   */
  async writeViewDataShard(name: string, shard: number, data: Uint8Array): Promise<WriteResult> {
    const dataPath = MVStoragePaths.viewDataShard(name, shard)
    return this.backend.writeAtomic(dataPath, data, {
      contentType: 'application/vnd.apache.parquet',
    })
  }

  /**
   * Read view data.
   *
   * @param name - View name
   */
  async readViewData(name: string): Promise<Uint8Array> {
    const dataPath = MVStoragePaths.viewDataFile(name)
    return this.backend.read(dataPath)
  }

  /**
   * Read a range of bytes from view data (for Parquet partial reads).
   *
   * @param name - View name
   * @param start - Start byte offset
   * @param end - End byte offset (exclusive)
   */
  async readViewDataRange(name: string, start: number, end: number): Promise<Uint8Array> {
    const dataPath = MVStoragePaths.viewDataFile(name)
    return this.backend.readRange(dataPath, start, end)
  }

  /**
   * Check if view data exists.
   *
   * @param name - View name
   */
  async viewDataExists(name: string): Promise<boolean> {
    const dataPath = MVStoragePaths.viewDataFile(name)
    return this.backend.exists(dataPath)
  }

  /**
   * Get view data file stat.
   *
   * @param name - View name
   */
  async getViewDataStat(name: string): Promise<{ size: number; mtime: Date } | null> {
    const dataPath = MVStoragePaths.viewDataFile(name)
    const stat = await this.backend.stat(dataPath)
    if (!stat) return null
    return { size: stat.size, mtime: stat.mtime }
  }

  /**
   * List all data files for a view.
   *
   * @param name - View name
   */
  async listViewDataFiles(name: string): Promise<string[]> {
    const dataPrefix = MVStoragePaths.viewData(name) + '/'
    const result = await this.backend.list(dataPrefix, { pattern: '*.parquet' })
    return result.files
  }

  /**
   * Delete all data files for a view.
   *
   * @param name - View name
   */
  async deleteViewData(name: string): Promise<number> {
    const dataPrefix = MVStoragePaths.viewData(name) + '/'
    return this.backend.deletePrefix(dataPrefix)
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Get the underlying storage backend.
   */
  getBackend(): StorageBackend {
    return this.backend
  }

  /**
   * Ensure the MV root directory exists.
   */
  async ensureRootDir(): Promise<void> {
    await this.backend.mkdir(MVStoragePaths.root)
  }

  /**
   * Get views that need refresh based on their schedule.
   *
   * @param now - Current time (for testing)
   */
  async getViewsNeedingRefresh(now: Date = new Date()): Promise<ViewMetadata[]> {
    const allMetadata = await this.getAllViewMetadata()
    return allMetadata.filter((metadata) => {
      // Skip views that are not in a refreshable state
      if (metadata.state === 'building' || metadata.state === 'disabled') {
        return false
      }

      // Manual views never auto-refresh
      if (metadata.definition.options.refreshMode === 'manual') {
        return false
      }

      // Check scheduled views
      if (metadata.definition.options.refreshMode === 'scheduled') {
        // If nextRefreshAt is set and in the past, needs refresh
        if (metadata.nextRefreshAt && metadata.nextRefreshAt <= now) {
          return true
        }
      }

      // Streaming views are handled differently (CDC-triggered)
      // For now, include stale streaming views
      if (metadata.state === 'stale') {
        return true
      }

      return false
    })
  }

  /**
   * Get views by source collection.
   *
   * @param source - Source collection name
   */
  async getViewsBySource(source: string): Promise<ViewMetadata[]> {
    const allMetadata = await this.getAllViewMetadata()
    return allMetadata.filter((metadata) => metadata.definition.source === source)
  }

  /**
   * Get streaming views for a source collection.
   * These are the views that should be updated when the source changes.
   *
   * @param source - Source collection name
   */
  async getStreamingViewsForSource(source: string): Promise<ViewMetadata[]> {
    const allMetadata = await this.getAllViewMetadata()
    return allMetadata.filter(
      (metadata) =>
        metadata.definition.source === source &&
        metadata.definition.options.refreshMode === 'streaming' &&
        metadata.state !== 'disabled'
    )
  }
}
