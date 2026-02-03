/**
 * Delta Lake Commit Utilities
 *
 * Shared utilities for committing data to Delta Lake tables.
 * Used by both DeltaBackend and the compaction workflow.
 *
 * These utilities handle the atomic commit protocol:
 * 1. Write data file
 * 2. Build commit actions
 * 3. Atomically create commit file using ifNoneMatch: '*'
 * 4. Retry with exponential backoff on conflict
 *
 * Delta Lake uses file-based OCC: commit files are immutable and
 * named by version number. Using ifNoneMatch: '*' ensures only
 * one writer can successfully create a commit at any version.
 */

import { AlreadyExistsError, isNotFoundError } from '../storage/errors'
import { logger } from '../utils/logger'
import type { StorageBackend } from '../types/storage'
import {
  formatVersion,
  serializeCommit,
  createAddAction,
  createProtocolAction,
  createMetadataAction,
  createCommitInfoAction,
  generateUUID,
  type LogAction,
} from '../delta-utils'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for a Delta commit operation
 */
export interface DeltaCommitConfig {
  /** Storage backend for file I/O */
  storage: StorageBackend

  /** Location of the Delta table (e.g., warehouse/db/table) */
  tableLocation: string

  /** Maximum OCC retries (default: 10) */
  maxRetries?: number

  /** Base backoff in ms (default: 100) */
  baseBackoffMs?: number

  /** Max backoff in ms (default: 10000) */
  maxBackoffMs?: number
}

/**
 * Information about a data file to be added to a Delta table
 */
export interface DeltaDataFileInfo {
  /** Path relative to table location */
  path: string

  /** File size in bytes */
  size: number

  /** Whether this is a data change (true) or metadata change (false) */
  dataChange?: boolean
}

/**
 * Result of a Delta commit operation
 */
export interface DeltaCommitResult {
  /** Whether the commit succeeded */
  success: boolean

  /** New version number (if successful) */
  version?: number

  /** Path to the new commit file (if successful) */
  logPath?: string

  /** Error message (if failed) */
  error?: string

  /** Number of retries performed */
  retries?: number
}

// =============================================================================
// Delta Committer Class
// =============================================================================

/**
 * Handles atomic commits to a Delta Lake table.
 *
 * This class provides methods for:
 * - Appending data files to existing tables
 * - Handling optimistic concurrency control (OCC) with retries
 *
 * OCC Strategy:
 * - Delta Lake commit files are named by version: 00000000000000000000.json
 * - Only one writer can successfully create a file at any version
 * - Uses ifNoneMatch: '*' for atomic create-only semantics
 * - On conflict (AlreadyExistsError), re-reads current version and retries
 *
 * @example
 * ```typescript
 * const committer = new DeltaCommitter({
 *   storage: r2Backend,
 *   tableLocation: 'warehouse/db/users',
 * })
 *
 * const result = await committer.commitDataFile({
 *   path: 'compacted-123.parquet',
 *   size: 1024000,
 * })
 *
 * if (result.success) {
 *   console.log(`Committed at version ${result.version}`)
 * }
 * ```
 */
export class DeltaCommitter {
  private storage: StorageBackend
  private tableLocation: string
  private maxRetries: number
  private baseBackoffMs: number
  private maxBackoffMs: number

  constructor(config: DeltaCommitConfig) {
    this.storage = config.storage
    this.tableLocation = config.tableLocation
    this.maxRetries = config.maxRetries ?? 10
    this.baseBackoffMs = config.baseBackoffMs ?? 100
    this.maxBackoffMs = config.maxBackoffMs ?? 10000
  }

  // ===========================================================================
  // Public Methods
  // ===========================================================================

  /**
   * Get the current version of the table (returns -1 if table doesn't exist)
   */
  async getCurrentVersion(): Promise<number> {
    const logDir = `${this.tableLocation}/_delta_log`

    // Try to read _last_checkpoint first for efficiency
    try {
      const checkpointData = await this.storage.read(`${logDir}/_last_checkpoint`)
      const checkpoint = JSON.parse(new TextDecoder().decode(checkpointData))
      let version = checkpoint.version as number

      // Look for commits after checkpoint
      while (true) {
        const nextCommit = `${logDir}/${formatVersion(version + 1)}.json`
        const exists = await this.storage.exists(nextCommit)
        if (!exists) break
        version++
      }

      return version
    } catch (error) {
      // No checkpoint - fall through to scan
      if (!isNotFoundError(error)) {
        // Only suppress not-found errors; re-throw others
        if (!(error instanceof SyntaxError)) {
          throw error
        }
      }
    }

    // Scan commit files to find current version
    try {
      const logFiles = await this.storage.list(`${logDir}/`)
      const commitFiles = logFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint') && !f.includes('_last_checkpoint'))
        .sort()

      if (commitFiles.length === 0) {
        return -1
      }

      const lastCommit = commitFiles[commitFiles.length - 1]!
      const versionStr = lastCommit.split('/').pop()?.replace('.json', '')
      return parseInt(versionStr ?? '-1', 10)
    } catch (error) {
      // Log directory doesn't exist - table not initialized
      if (isNotFoundError(error)) {
        return -1
      }
      throw error
    }
  }

  /**
   * Ensure the table exists, creating it if necessary
   */
  async ensureTable(): Promise<number> {
    const currentVersion = await this.getCurrentVersion()
    if (currentVersion >= 0) {
      return currentVersion
    }

    // Create new table at version 0
    const logDir = `${this.tableLocation}/_delta_log`
    await this.storage.mkdir(this.tableLocation).catch(() => {})
    await this.storage.mkdir(logDir).catch(() => {})

    const actions: LogAction[] = [
      createProtocolAction(),
      createMetadataAction(generateUUID()),
      createCommitInfoAction('CREATE TABLE', undefined, true),
    ]

    const commitPath = `${logDir}/${formatVersion(0)}.json`
    const commitContent = serializeCommit(actions)

    try {
      await this.storage.write(
        commitPath,
        new TextEncoder().encode(commitContent),
        { ifNoneMatch: '*' }
      )
      return 0
    } catch (error) {
      // Another writer created the table - that's fine
      if (error instanceof AlreadyExistsError || this.isConflictError(error)) {
        return this.getCurrentVersion()
      }
      throw error
    }
  }

  /**
   * Commit a data file to the table atomically.
   *
   * Uses optimistic concurrency control with retries.
   * On conflict, re-reads the current version and retries.
   */
  async commitDataFile(dataFile: DeltaDataFileInfo): Promise<DeltaCommitResult> {
    return this.commitDataFiles([dataFile])
  }

  /**
   * Commit one or more data files to the table atomically.
   *
   * Uses optimistic concurrency control with retries.
   * On conflict, re-reads the current version and retries.
   */
  async commitDataFiles(dataFiles: DeltaDataFileInfo[]): Promise<DeltaCommitResult> {
    if (dataFiles.length === 0) {
      return { success: true }
    }

    const logDir = `${this.tableLocation}/_delta_log`
    let retries = 0

    while (retries <= this.maxRetries) {
      try {
        // Get current version
        const currentVersion = await this.getCurrentVersion()
        const isNewTable = currentVersion < 0
        const nextVersion = isNewTable ? 0 : currentVersion + 1

        // Ensure directories exist for new tables
        if (isNewTable) {
          await this.storage.mkdir(this.tableLocation).catch(() => {})
          await this.storage.mkdir(logDir).catch(() => {})
        }

        // Build commit actions
        const actions: LogAction[] = []

        // First commit needs protocol and metadata
        if (isNewTable) {
          actions.push(createProtocolAction())
          actions.push(createMetadataAction(generateUUID()))
        }

        // Add actions for data files
        for (const dataFile of dataFiles) {
          actions.push(createAddAction(
            dataFile.path,
            dataFile.size,
            dataFile.dataChange ?? true
          ))
        }

        // Commit info
        actions.push(createCommitInfoAction(
          'COMPACTION',
          isNewTable ? undefined : currentVersion,
          true,
          { source: 'delta-committer' }
        ))

        // Try to commit atomically
        const commitPath = `${logDir}/${formatVersion(nextVersion)}.json`
        const commitContent = serializeCommit(actions)

        const committed = await this.tryCommit(commitPath, commitContent)

        if (committed) {
          return {
            success: true,
            version: nextVersion,
            logPath: commitPath,
            retries,
          }
        }

        // Conflict detected, retry
        retries++
        if (retries <= this.maxRetries) {
          logger.debug(`Delta OCC conflict on attempt ${retries}, retrying...`)
          await this.backoffDelay(retries)
        }
      } catch (error) {
        logger.error('Error during Delta commit attempt', { attempt: retries, error })
        throw error
      }
    }

    return {
      success: false,
      error: `Commit failed after ${this.maxRetries} retries due to concurrent modifications`,
      retries,
    }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Try to write commit file atomically
   * Returns true if successful, false if conflict
   */
  private async tryCommit(commitPath: string, content: string): Promise<boolean> {
    try {
      await this.storage.write(
        commitPath,
        new TextEncoder().encode(content),
        { ifNoneMatch: '*' }
      )
      return true
    } catch (error) {
      if (error instanceof AlreadyExistsError || this.isConflictError(error)) {
        // Conflict - another process claimed this version
        return false
      }
      throw error
    }
  }

  /**
   * Check if an error indicates a conflict (file already exists)
   */
  private isConflictError(error: unknown): boolean {
    if (error instanceof Error) {
      if ('code' in error && (error as { code: string }).code === 'ALREADY_EXISTS') {
        return true
      }
      if (error.message.includes('already exists') || error.message.includes('ALREADY_EXISTS')) {
        return true
      }
    }
    return false
  }

  /**
   * Apply exponential backoff delay with jitter
   */
  private async backoffDelay(retryCount: number): Promise<void> {
    const backoffMs = Math.min(
      this.baseBackoffMs * Math.pow(2, retryCount - 1) + Math.random() * this.baseBackoffMs,
      this.maxBackoffMs
    )
    await this.sleep(backoffMs)
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a Delta committer for a table
 */
export function createDeltaCommitter(config: DeltaCommitConfig): DeltaCommitter {
  return new DeltaCommitter(config)
}

/**
 * Convenience function to commit data files to a Delta table.
 * Creates a committer, ensures the table exists, and commits the files.
 *
 * @example
 * ```typescript
 * const result = await commitToDeltaTable({
 *   storage: r2Backend,
 *   tableLocation: 'warehouse/db/users',
 *   dataFiles: [
 *     { path: 'compacted-123.parquet', size: 1024000 },
 *   ],
 * })
 * ```
 */
export async function commitToDeltaTable(config: {
  storage: StorageBackend
  tableLocation: string
  dataFiles: DeltaDataFileInfo[]
  maxRetries?: number
  baseBackoffMs?: number
  maxBackoffMs?: number
}): Promise<DeltaCommitResult> {
  const committer = new DeltaCommitter({
    storage: config.storage,
    tableLocation: config.tableLocation,
    maxRetries: config.maxRetries,
    baseBackoffMs: config.baseBackoffMs,
    maxBackoffMs: config.maxBackoffMs,
  })

  // Ensure table exists
  await committer.ensureTable()

  // Commit the data files
  return committer.commitDataFiles(config.dataFiles)
}
