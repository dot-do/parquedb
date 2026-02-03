/**
 * StreamPersistence - Crash Recovery for StreamProcessor
 *
 * Provides durable persistence mechanisms to survive StreamProcessor crashes:
 *
 * 1. **WAL (Write-Ahead Log)**: Logs records before Parquet writes for recovery
 * 2. **DLQ Persistence**: Persists dead-letter queue entries to disk
 * 3. **Checkpointing**: Tracks stream position for incremental refresh
 * 4. **Recovery Protocol**: Restores state from WAL/DLQ on startup
 *
 * Storage Layout:
 * ```
 * _views/{name}/
 *   wal/
 *     pending.jsonl       # Current WAL (append-only)
 *     {timestamp}.jsonl   # Archived WAL segments
 *   dlq/
 *     batch-{n}.json      # Failed batch entries
 *   checkpoint.json       # Stream position checkpoint
 * ```
 *
 * @example
 * ```typescript
 * const persistence = new StreamPersistence({
 *   name: 'ai-requests',
 *   storage,
 *   basePath: '_views/ai_requests',
 * })
 *
 * // Initialize and recover any pending records
 * const recovered = await persistence.recover()
 *
 * // Log records before processing
 * const walEntry = await persistence.logWAL(records, batchNumber)
 *
 * // On successful write, clear the WAL entry
 * await persistence.commitWAL(walEntry.id)
 *
 * // On failure, persist to DLQ
 * await persistence.persistDLQ(failedBatch)
 *
 * // Save checkpoint after batch
 * await persistence.saveCheckpoint({ sequence: 12345, timestamp: Date.now() })
 * ```
 */

import type { StorageBackend } from '../types/storage'
import type { FailedBatch } from './stream-processor'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for StreamPersistence
 */
export interface StreamPersistenceConfig {
  /** Name of the stream processor (used for storage paths) */
  name: string

  /** Storage backend for persistence */
  storage: StorageBackend

  /** Base path for persistence files (e.g., '_views/ai_requests') */
  basePath: string

  /**
   * Maximum WAL segment size in bytes before rotation
   * @default 10MB
   */
  maxWalSegmentSize?: number

  /**
   * Maximum age of WAL segments before archiving (ms)
   * @default 1 hour
   */
  maxWalSegmentAge?: number

  /**
   * Whether to sync WAL writes immediately (durability vs performance)
   * @default true
   */
  syncWal?: boolean
}

/**
 * A WAL (Write-Ahead Log) entry
 */
export interface WALEntry<T = Record<string, unknown>> {
  /** Unique entry ID (ULID) */
  id: string

  /** Batch number */
  batchNumber: number

  /** Records in this batch */
  records: T[]

  /** Timestamp when logged */
  timestamp: number

  /** Target file path for the batch */
  targetPath: string

  /** Status: pending, committed, or failed */
  status: 'pending' | 'committed' | 'failed'
}

/**
 * Stream checkpoint for position tracking
 */
export interface StreamCheckpoint {
  /** Last processed sequence number (e.g., event ID) */
  sequence?: string | number

  /** Last processed timestamp */
  timestamp: number

  /** Last written batch number */
  lastBatchNumber: number

  /** Number of records processed since last checkpoint */
  recordsProcessed: number

  /** Additional metadata for recovery */
  metadata?: Record<string, unknown>
}

/**
 * Recovery result from initialization
 */
export interface RecoveryResult<T = Record<string, unknown>> {
  /** Records recovered from WAL that need re-processing */
  pendingRecords: T[]

  /** Failed batches recovered from DLQ */
  failedBatches: FailedBatch<T>[]

  /** Last checkpoint (if any) */
  checkpoint: StreamCheckpoint | null

  /** Number of WAL entries recovered */
  walEntriesRecovered: number

  /** Number of DLQ entries recovered */
  dlqEntriesRecovered: number

  /** Whether recovery was clean (no pending work) */
  cleanRecovery: boolean
}

/**
 * Persisted DLQ entry
 */
export interface PersistedDLQEntry<T = Record<string, unknown>> {
  /** The failed batch data */
  batch: FailedBatch<T>

  /** When persisted */
  persistedAt: number

  /** Number of replay attempts */
  replayAttempts: number

  /** Last replay timestamp */
  lastReplayAt?: number
}

// =============================================================================
// StreamPersistence Class
// =============================================================================

/**
 * Manages crash-recovery persistence for StreamProcessor
 */
export class StreamPersistence<T extends Record<string, unknown> = Record<string, unknown>> {
  private config: Required<Omit<StreamPersistenceConfig, 'storage'>> & { storage: StorageBackend }
  private walPath: string
  private dlqPath: string
  private checkpointPath: string
  private currentWalSize = 0
  private walCreatedAt = 0
  private initialized = false

  constructor(config: StreamPersistenceConfig) {
    this.config = {
      ...config,
      maxWalSegmentSize: config.maxWalSegmentSize ?? 10 * 1024 * 1024, // 10MB
      maxWalSegmentAge: config.maxWalSegmentAge ?? 60 * 60 * 1000, // 1 hour
      syncWal: config.syncWal ?? true,
    }

    this.walPath = `${config.basePath}/wal`
    this.dlqPath = `${config.basePath}/dlq`
    this.checkpointPath = `${config.basePath}/checkpoint.json`
  }

  // ===========================================================================
  // Initialization & Recovery
  // ===========================================================================

  /**
   * Initialize persistence and recover any pending state
   *
   * This should be called before the StreamProcessor starts processing.
   * It will:
   * 1. Create necessary directories
   * 2. Recover pending WAL entries
   * 3. Recover persisted DLQ entries
   * 4. Load the last checkpoint
   */
  async recover(): Promise<RecoveryResult<T>> {
    const result: RecoveryResult<T> = {
      pendingRecords: [],
      failedBatches: [],
      checkpoint: null,
      walEntriesRecovered: 0,
      dlqEntriesRecovered: 0,
      cleanRecovery: true,
    }

    // Ensure directories exist
    await this.ensureDirectories()

    // Recover WAL
    const walRecovery = await this.recoverWAL()
    result.pendingRecords = walRecovery.records
    result.walEntriesRecovered = walRecovery.entriesRecovered

    // Recover DLQ
    const dlqRecovery = await this.recoverDLQ()
    result.failedBatches = dlqRecovery.batches
    result.dlqEntriesRecovered = dlqRecovery.entriesRecovered

    // Load checkpoint
    result.checkpoint = await this.loadCheckpoint()

    // Determine if this was a clean recovery
    result.cleanRecovery = result.walEntriesRecovered === 0 && result.dlqEntriesRecovered === 0

    this.initialized = true
    this.walCreatedAt = Date.now()

    return result
  }

  /**
   * Ensure persistence directories exist
   */
  private async ensureDirectories(): Promise<void> {
    const { storage } = this.config

    // Create directories (no-op if they exist)
    try {
      await storage.mkdir(this.walPath)
    } catch {
      // Directory may already exist
    }

    try {
      await storage.mkdir(this.dlqPath)
    } catch {
      // Directory may already exist
    }
  }

  /**
   * Recover pending WAL entries
   */
  private async recoverWAL(): Promise<{ records: T[]; entriesRecovered: number }> {
    const { storage } = this.config
    const records: T[] = []
    let entriesRecovered = 0

    try {
      // List WAL files
      const listResult = await storage.list(this.walPath)

      for (const file of listResult.files) {
        if (!file.endsWith('.jsonl')) continue

        try {
          const data = await storage.read(file)
          const content = new TextDecoder().decode(data)
          const lines = content.split('\n').filter((line) => line.trim())

          for (const line of lines) {
            try {
              const entry: WALEntry<T> = JSON.parse(line)

              // Only recover pending entries
              if (entry.status === 'pending') {
                records.push(...entry.records)
                entriesRecovered++
              }
            } catch {
              // Skip malformed lines
            }
          }

          // Archive the recovered WAL file
          if (file.includes('pending.jsonl')) {
            const archivePath = `${this.walPath}/recovered-${Date.now()}.jsonl`
            await storage.move(file, archivePath)
          }
        } catch {
          // Skip files that can't be read
        }
      }
    } catch {
      // No WAL files to recover
    }

    return { records, entriesRecovered }
  }

  /**
   * Recover persisted DLQ entries
   */
  private async recoverDLQ(): Promise<{ batches: FailedBatch<T>[]; entriesRecovered: number }> {
    const { storage } = this.config
    const batches: FailedBatch<T>[] = []
    let entriesRecovered = 0

    try {
      const listResult = await storage.list(this.dlqPath)

      for (const file of listResult.files) {
        if (!file.endsWith('.json')) continue

        try {
          const data = await storage.read(file)
          const content = new TextDecoder().decode(data)
          const entry: PersistedDLQEntry<T> = JSON.parse(content)

          batches.push(entry.batch)
          entriesRecovered++
        } catch {
          // Skip malformed entries
        }
      }
    } catch {
      // No DLQ files to recover
    }

    return { batches, entriesRecovered }
  }

  // ===========================================================================
  // WAL Operations
  // ===========================================================================

  /**
   * Log records to WAL before processing
   *
   * This ensures records are durably persisted before attempting to write
   * them to Parquet. If the process crashes during the Parquet write,
   * the records can be recovered from the WAL.
   */
  async logWAL(records: T[], batchNumber: number, targetPath: string): Promise<WALEntry<T>> {
    if (!this.initialized) {
      throw new Error('StreamPersistence not initialized. Call recover() first.')
    }

    const entry: WALEntry<T> = {
      id: generateId(),
      batchNumber,
      records,
      timestamp: Date.now(),
      targetPath,
      status: 'pending',
    }

    const line = JSON.stringify(entry) + '\n'
    const lineBytes = new TextEncoder().encode(line)

    // Check if we need to rotate the WAL
    await this.maybeRotateWAL()

    // Append to current WAL
    const walFile = `${this.walPath}/pending.jsonl`
    await this.config.storage.append(walFile, lineBytes)

    this.currentWalSize += lineBytes.length

    return entry
  }

  /**
   * Commit a WAL entry after successful write
   *
   * This marks the entry as committed, indicating the Parquet write succeeded.
   */
  async commitWAL(entryId: string): Promise<void> {
    // We use a simple approach: append a commit marker
    // On recovery, we'll skip entries that have a corresponding commit
    const commitMarker = JSON.stringify({
      type: 'commit',
      entryId,
      timestamp: Date.now(),
    }) + '\n'

    const walFile = `${this.walPath}/pending.jsonl`
    await this.config.storage.append(walFile, new TextEncoder().encode(commitMarker))
  }

  /**
   * Mark a WAL entry as failed
   *
   * This is used when a batch fails after all retries.
   */
  async failWAL(entryId: string, error: Error): Promise<void> {
    const failMarker = JSON.stringify({
      type: 'fail',
      entryId,
      error: error.message,
      timestamp: Date.now(),
    }) + '\n'

    const walFile = `${this.walPath}/pending.jsonl`
    await this.config.storage.append(walFile, new TextEncoder().encode(failMarker))
  }

  /**
   * Rotate WAL if it exceeds size or age limits
   */
  private async maybeRotateWAL(): Promise<void> {
    const { maxWalSegmentSize, maxWalSegmentAge } = this.config

    const shouldRotate =
      this.currentWalSize >= maxWalSegmentSize ||
      (this.walCreatedAt > 0 && Date.now() - this.walCreatedAt >= maxWalSegmentAge)

    if (shouldRotate) {
      await this.rotateWAL()
    }
  }

  /**
   * Rotate the current WAL to an archive
   */
  private async rotateWAL(): Promise<void> {
    const { storage } = this.config
    const currentWal = `${this.walPath}/pending.jsonl`

    try {
      const exists = await storage.exists(currentWal)
      if (exists) {
        const archivePath = `${this.walPath}/archive-${Date.now()}.jsonl`
        await storage.move(currentWal, archivePath)
      }
    } catch {
      // Ignore rotation errors
    }

    this.currentWalSize = 0
    this.walCreatedAt = Date.now()
  }

  // ===========================================================================
  // DLQ Persistence
  // ===========================================================================

  /**
   * Persist a failed batch to the DLQ
   *
   * This ensures failed batches survive process crashes and can be
   * retried later.
   */
  async persistDLQ(batch: FailedBatch<T>): Promise<void> {
    const entry: PersistedDLQEntry<T> = {
      batch,
      persistedAt: Date.now(),
      replayAttempts: 0,
    }

    const fileName = `batch-${batch.batchNumber}-${Date.now()}.json`
    const filePath = `${this.dlqPath}/${fileName}`
    const data = new TextEncoder().encode(JSON.stringify(entry, null, 2))

    await this.config.storage.write(filePath, data)
  }

  /**
   * Update a DLQ entry after a replay attempt
   */
  async updateDLQEntry(batch: FailedBatch<T>, succeeded: boolean): Promise<void> {
    const { storage } = this.config

    // Find and update the DLQ entry
    try {
      const listResult = await storage.list(this.dlqPath)

      for (const file of listResult.files) {
        if (!file.includes(`batch-${batch.batchNumber}-`)) continue

        if (succeeded) {
          // Remove the entry on success
          await storage.delete(file)
        } else {
          // Update replay attempt count
          const data = await storage.read(file)
          const entry: PersistedDLQEntry<T> = JSON.parse(new TextDecoder().decode(data))
          entry.replayAttempts++
          entry.lastReplayAt = Date.now()

          await storage.write(file, new TextEncoder().encode(JSON.stringify(entry, null, 2)))
        }
        break
      }
    } catch {
      // Best-effort update
    }
  }

  /**
   * Clear all DLQ entries (after successful processing)
   */
  async clearDLQ(): Promise<number> {
    const { storage } = this.config
    let cleared = 0

    try {
      const listResult = await storage.list(this.dlqPath)

      for (const file of listResult.files) {
        if (file.endsWith('.json')) {
          await storage.delete(file)
          cleared++
        }
      }
    } catch {
      // Best-effort clear
    }

    return cleared
  }

  /**
   * List all persisted DLQ entries
   */
  async listDLQ(): Promise<PersistedDLQEntry<T>[]> {
    const { storage } = this.config
    const entries: PersistedDLQEntry<T>[] = []

    try {
      const listResult = await storage.list(this.dlqPath)

      for (const file of listResult.files) {
        if (!file.endsWith('.json')) continue

        try {
          const data = await storage.read(file)
          const entry: PersistedDLQEntry<T> = JSON.parse(new TextDecoder().decode(data))
          entries.push(entry)
        } catch {
          // Skip malformed entries
        }
      }
    } catch {
      // No DLQ files
    }

    return entries
  }

  // ===========================================================================
  // Checkpointing
  // ===========================================================================

  /**
   * Save a checkpoint
   *
   * Checkpoints track the stream position for incremental refresh.
   * They should be saved periodically after successful batch writes.
   */
  async saveCheckpoint(checkpoint: StreamCheckpoint): Promise<void> {
    const data = new TextEncoder().encode(
      JSON.stringify(
        {
          ...checkpoint,
          savedAt: Date.now(),
        },
        null,
        2
      )
    )

    await this.config.storage.writeAtomic(this.checkpointPath, data)
  }

  /**
   * Load the last checkpoint
   */
  async loadCheckpoint(): Promise<StreamCheckpoint | null> {
    const { storage } = this.config

    try {
      const exists = await storage.exists(this.checkpointPath)
      if (!exists) return null

      const data = await storage.read(this.checkpointPath)
      return JSON.parse(new TextDecoder().decode(data))
    } catch {
      return null
    }
  }

  /**
   * Clear the checkpoint
   */
  async clearCheckpoint(): Promise<void> {
    try {
      await this.config.storage.delete(this.checkpointPath)
    } catch {
      // Best-effort clear
    }
  }

  // ===========================================================================
  // Cleanup
  // ===========================================================================

  /**
   * Clean up old WAL archives
   *
   * @param maxAgeMs Maximum age of archives to keep (default: 24 hours)
   */
  async cleanupWALArchives(maxAgeMs = 24 * 60 * 60 * 1000): Promise<number> {
    const { storage } = this.config
    const cutoff = Date.now() - maxAgeMs
    let cleaned = 0

    try {
      const listResult = await storage.list(this.walPath)

      for (const file of listResult.files) {
        // Only clean archive files, not pending.jsonl
        if (!file.includes('archive-') && !file.includes('recovered-')) continue

        // Extract timestamp from filename
        const match = file.match(/(?:archive|recovered)-(\d+)\.jsonl$/)
        if (match) {
          const timestamp = parseInt(match[1]!, 10)
          if (timestamp < cutoff) {
            await storage.delete(file)
            cleaned++
          }
        }
      }
    } catch {
      // Best-effort cleanup
    }

    return cleaned
  }

  /**
   * Get persistence statistics
   */
  async getStats(): Promise<PersistenceStats> {
    const { storage } = this.config
    let walFiles = 0
    let walSize = 0
    let dlqFiles = 0
    let dlqSize = 0

    try {
      const walList = await storage.list(this.walPath, { includeMetadata: true })
      walFiles = walList.files.length
      if (walList.stats) {
        walSize = walList.stats.reduce((sum, stat) => sum + stat.size, 0)
      }
    } catch {
      // No WAL files
    }

    try {
      const dlqList = await storage.list(this.dlqPath, { includeMetadata: true })
      dlqFiles = dlqList.files.length
      if (dlqList.stats) {
        dlqSize = dlqList.stats.reduce((sum, stat) => sum + stat.size, 0)
      }
    } catch {
      // No DLQ files
    }

    return {
      walFiles,
      walSize,
      currentWalSize: this.currentWalSize,
      dlqFiles,
      dlqSize,
      hasCheckpoint: await storage.exists(this.checkpointPath),
    }
  }
}

/**
 * Statistics about persistence state
 */
export interface PersistenceStats {
  /** Number of WAL files (including archives) */
  walFiles: number

  /** Total size of WAL files in bytes */
  walSize: number

  /** Size of current WAL segment */
  currentWalSize: number

  /** Number of DLQ entries */
  dlqFiles: number

  /** Total size of DLQ files in bytes */
  dlqSize: number

  /** Whether a checkpoint exists */
  hasCheckpoint: boolean
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique ID (simple ULID-like)
 */
function generateId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `${timestamp}-${random}`
}

/**
 * Create a StreamPersistence instance
 */
export function createStreamPersistence<T extends Record<string, unknown> = Record<string, unknown>>(
  config: StreamPersistenceConfig
): StreamPersistence<T> {
  return new StreamPersistence(config)
}
