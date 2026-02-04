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
 *
 * Checkpoint support:
 * - Checkpoints aggregate state for faster reads
 * - Created after every N commits (configurable, default 10)
 * - Stored as Parquet files in _delta_log/
 * - _last_checkpoint file points to latest checkpoint
 */

import { AlreadyExistsError, isNotFoundError } from '../storage/errors'
import { logger } from '../utils/logger'
import type { StorageBackend } from '../types/storage'
import {
  formatVersion,
  serializeCommit,
  createAddAction,
  createRemoveAction,
  createProtocolAction,
  createMetadataAction,
  createCommitInfoAction,
  generateUUID,
  type LogAction,
  type AddAction,
  type RemoveAction,
  type ProtocolAction,
  type MetadataAction,
} from '../delta-helpers'
import { ParquetWriter } from '../parquet/writer'

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
  maxRetries?: number | undefined

  /** Base backoff in ms (default: 100) */
  baseBackoffMs?: number | undefined

  /** Max backoff in ms (default: 10000) */
  maxBackoffMs?: number | undefined

  /**
   * Create checkpoint every N commits (default: 10).
   * Set to 0 to disable automatic checkpoints.
   */
  checkpointInterval?: number | undefined
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
  dataChange?: boolean | undefined
}

/**
 * Result of a Delta commit operation
 */
export interface DeltaCommitResult {
  /** Whether the commit succeeded */
  success: boolean

  /** New version number (if successful) */
  version?: number | undefined

  /** Path to the new commit file (if successful) */
  logPath?: string | undefined

  /** Error message (if failed) */
  error?: string | undefined

  /** Number of retries performed */
  retries?: number | undefined
}

/**
 * Options for checkpoint creation
 */
export interface CheckpointOptions {
  /** Force checkpoint creation even if not at interval boundary */
  force?: boolean | undefined
}

/**
 * Result of checkpoint creation
 */
export interface CheckpointResult {
  /** Whether a checkpoint was created */
  created: boolean

  /** Version of the checkpoint (if created) */
  version: number

  /** Path to checkpoint file (if created) */
  checkpointPath?: string | undefined

  /** Number of actions in the checkpoint */
  size?: number | undefined
}

/**
 * Checkpoint metadata stored in _last_checkpoint
 */
interface LastCheckpoint {
  version: number
  size: number
  parts?: number | undefined
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
 * - Creating checkpoint files for faster reads
 *
 * OCC Strategy:
 * - Delta Lake commit files are named by version: 00000000000000000000.json
 * - Only one writer can successfully create a file at any version
 * - Uses ifNoneMatch: '*' for atomic create-only semantics
 * - On conflict (AlreadyExistsError), re-reads current version and retries
 *
 * Checkpoint Strategy:
 * - Checkpoints are created after every N commits (configurable)
 * - Checkpoint files are Parquet files with all active add actions
 * - _last_checkpoint file points to latest checkpoint
 * - Readers check _last_checkpoint first for faster version lookup
 *
 * @example
 * ```typescript
 * const committer = new DeltaCommitter({
 *   storage: r2Backend,
 *   tableLocation: 'warehouse/db/users',
 *   checkpointInterval: 10, // Create checkpoint every 10 commits
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
 *
 * // Or manually create checkpoint after compaction
 * await committer.maybeCreateCheckpoint()
 * ```
 */
export class DeltaCommitter {
  private storage: StorageBackend
  private tableLocation: string
  private maxRetries: number
  private baseBackoffMs: number
  private maxBackoffMs: number
  private checkpointInterval: number

  constructor(config: DeltaCommitConfig) {
    this.storage = config.storage
    this.tableLocation = config.tableLocation
    this.maxRetries = config.maxRetries ?? 10
    this.baseBackoffMs = config.baseBackoffMs ?? 100
    this.maxBackoffMs = config.maxBackoffMs ?? 10000
    this.checkpointInterval = config.checkpointInterval ?? 10
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
          // Check if we should create a checkpoint
          if (this.shouldCreateCheckpoint(nextVersion)) {
            await this.createCheckpoint(nextVersion)
          }

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

  /**
   * Commit remove actions for files (for compaction/cleanup)
   *
   * @param filePaths - Paths of files to remove
   * @param readVersion - Current version (for commit info)
   */
  async commitRemoveFiles(
    filePaths: string[],
    _readVersion: number
  ): Promise<DeltaCommitResult> {
    if (filePaths.length === 0) {
      return { success: true }
    }

    const logDir = `${this.tableLocation}/_delta_log`
    let retries = 0

    while (retries <= this.maxRetries) {
      try {
        const currentVersion = await this.getCurrentVersion()
        const nextVersion = currentVersion + 1

        // Build remove actions
        const actions: LogAction[] = filePaths.map(path =>
          createRemoveAction(path, false)
        )

        // Add commit info
        actions.push(createCommitInfoAction(
          'REMOVE',
          currentVersion,
          false,
          { source: 'delta-committer' }
        ))

        // Try to commit atomically
        const commitPath = `${logDir}/${formatVersion(nextVersion)}.json`
        const commitContent = serializeCommit(actions)

        const committed = await this.tryCommit(commitPath, commitContent)

        if (committed) {
          // Check if we should create a checkpoint
          if (this.shouldCreateCheckpoint(nextVersion)) {
            await this.createCheckpoint(nextVersion)
          }

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
          logger.debug(`Delta OCC conflict on remove attempt ${retries}, retrying...`)
          await this.backoffDelay(retries)
        }
      } catch (error) {
        logger.error('Error during Delta remove commit attempt', { attempt: retries, error })
        throw error
      }
    }

    return {
      success: false,
      error: `Remove commit failed after ${this.maxRetries} retries`,
      retries,
    }
  }

  /**
   * Create a checkpoint at the current version, or check if one is needed.
   *
   * Use this after compaction commits to ensure checkpoint is created
   * if the interval threshold has been reached.
   *
   * @param options - Checkpoint options
   * @returns Checkpoint result
   */
  async maybeCreateCheckpoint(options?: CheckpointOptions): Promise<CheckpointResult> {
    const currentVersion = await this.getCurrentVersion()

    if (currentVersion < 0) {
      return { created: false, version: -1 }
    }

    const shouldCreate = options?.force || this.shouldCreateCheckpoint(currentVersion)

    if (shouldCreate) {
      return this.createCheckpoint(currentVersion)
    }

    return { created: false, version: currentVersion }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Check if a checkpoint should be created at this version
   */
  private shouldCreateCheckpoint(version: number): boolean {
    if (this.checkpointInterval <= 0) {
      return false
    }
    return version > 0 && version % this.checkpointInterval === 0
  }

  /**
   * Create a checkpoint file at the specified version
   *
   * Delta Lake checkpoints are Parquet files containing all active actions
   * aggregated from version 0 to the checkpoint version. This allows readers
   * to skip reading individual log files.
   */
  private async createCheckpoint(version: number): Promise<CheckpointResult> {
    const logDir = `${this.tableLocation}/_delta_log`

    try {
      // Collect all actions from log files up to this version
      const { actions: checkpointActions, activeFiles } = await this.collectActionsForCheckpoint(version)

      if (checkpointActions.length === 0) {
        return { created: false, version }
      }

      // Convert actions to checkpoint row format
      // Delta Lake checkpoint schema: txn, add, remove, metaData, protocol, commitInfo
      // Each row has exactly one non-null column containing the action as JSON
      const checkpointRows = checkpointActions.map(action => {
        const row: Record<string, string | null> = {
          txn: null,
          add: null,
          remove: null,
          metaData: null,
          protocol: null,
          commitInfo: null,
        }

        if ('protocol' in action) {
          row.protocol = JSON.stringify(action.protocol)
        } else if ('metaData' in action) {
          row.metaData = JSON.stringify(action.metaData)
        } else if ('add' in action) {
          row.add = JSON.stringify(action.add)
        } else if ('remove' in action) {
          row.remove = JSON.stringify(action.remove)
        } else if ('commitInfo' in action) {
          row.commitInfo = JSON.stringify(action.commitInfo)
        }

        return row
      })

      // Delta Lake checkpoint Parquet schema (all optional strings containing JSON)
      const checkpointSchema = {
        txn: { type: 'STRING' as const, optional: true },
        add: { type: 'STRING' as const, optional: true },
        remove: { type: 'STRING' as const, optional: true },
        metaData: { type: 'STRING' as const, optional: true },
        protocol: { type: 'STRING' as const, optional: true },
        commitInfo: { type: 'STRING' as const, optional: true },
      }

      // Write checkpoint as Parquet file
      const checkpointPath = `${logDir}/${formatVersion(version)}.checkpoint.parquet`
      const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
      await writer.write(checkpointPath, checkpointRows, checkpointSchema)

      // Write _last_checkpoint atomically
      const lastCheckpoint: LastCheckpoint = {
        version,
        size: checkpointRows.length,
      }
      await this.storage.write(
        `${logDir}/_last_checkpoint`,
        new TextEncoder().encode(JSON.stringify(lastCheckpoint))
      )

      logger.debug('Created Delta checkpoint', {
        version,
        actionCount: checkpointRows.length,
        activeFiles: activeFiles.size,
      })

      return {
        created: true,
        version,
        checkpointPath,
        size: checkpointRows.length,
      }
    } catch (error) {
      logger.error('Failed to create checkpoint', { version, error })
      // Don't fail the commit if checkpoint creation fails
      return { created: false, version }
    }
  }

  /**
   * Collect all actions from log files up to a version for checkpoint creation
   */
  private async collectActionsForCheckpoint(version: number): Promise<{
    actions: LogAction[]
    activeFiles: Map<string, AddAction>
  }> {
    const logDir = `${this.tableLocation}/_delta_log`
    let latestProtocol: ProtocolAction | null = null
    let latestMetadata: MetadataAction | null = null
    const activeFiles = new Map<string, AddAction>()

    for (let v = 0; v <= version; v++) {
      const commitPath = `${logDir}/${formatVersion(v)}.json`

      try {
        const commitData = await this.storage.read(commitPath)
        const commitText = new TextDecoder().decode(commitData)
        const actions = this.parseCommitFile(commitText)

        for (const action of actions) {
          if ('protocol' in action) {
            latestProtocol = action as ProtocolAction
          } else if ('metaData' in action) {
            latestMetadata = action as MetadataAction
          } else if ('add' in action) {
            activeFiles.set((action as AddAction).add.path, action as AddAction)
          } else if ('remove' in action) {
            activeFiles.delete((action as RemoveAction).remove.path)
          }
          // Skip commitInfo - not needed in checkpoint
        }
      } catch (error) {
        // Skip missing commits
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    // Build final checkpoint actions list
    const checkpointActions: LogAction[] = []

    if (latestProtocol) {
      checkpointActions.push(latestProtocol)
    }
    if (latestMetadata) {
      checkpointActions.push(latestMetadata)
    }

    // Add all active files
    for (const addAction of Array.from(activeFiles.values())) {
      checkpointActions.push(addAction)
    }

    return { actions: checkpointActions, activeFiles }
  }

  /**
   * Parse commit file content into actions
   */
  private parseCommitFile(content: string): LogAction[] {
    const lines = content.trim().split('\n')
    const actions: LogAction[] = []

    for (const line of lines) {
      if (!line.trim()) continue
      try {
        actions.push(JSON.parse(line) as LogAction)
      } catch {
        // Skip invalid lines
        continue
      }
    }

    return actions
  }

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
  maxRetries?: number | undefined
  baseBackoffMs?: number | undefined
  maxBackoffMs?: number | undefined
  checkpointInterval?: number | undefined
}): Promise<DeltaCommitResult> {
  const committer = new DeltaCommitter({
    storage: config.storage,
    tableLocation: config.tableLocation,
    maxRetries: config.maxRetries,
    baseBackoffMs: config.baseBackoffMs,
    maxBackoffMs: config.maxBackoffMs,
    checkpointInterval: config.checkpointInterval,
  })

  // Ensure table exists
  await committer.ensureTable()

  // Commit the data files
  return committer.commitDataFiles(config.dataFiles)
}
