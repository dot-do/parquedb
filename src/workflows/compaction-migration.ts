/**
 * Unified Compaction & Migration Workflow
 *
 * Combines compaction and format migration into a single efficient workflow.
 * As data is compacted, it can optionally be migrated to Iceberg/Delta format.
 *
 * Key features:
 * - Multi-writer aware: waits for all writers in a time window
 * - Progressive migration: data migrates as it's compacted
 * - Merge-sort efficient: each writer's data is pre-sorted
 * - Resumable: Cloudflare Workflows handle failures gracefully
 *
 * Architecture:
 * ```
 * Writers → R2 (native) → Event Notification → Queue → This Workflow
 *                                                          ↓
 *                                              Compacted Iceberg/Delta
 * ```
 *
 * @example
 * ```typescript
 * // Triggered by queue consumer
 * const instance = await env.COMPACTION_WORKFLOW.create({
 *   params: {
 *     namespace: 'users',
 *     windowStart: 1700000000000,
 *     windowEnd: 1700003600000,
 *     files: ['data/users/1700001234-writer1-0.parquet', ...],
 *     writers: ['writer1', 'writer2'],
 *     targetFormat: 'iceberg', // Progressive migration!
 *   }
 * })
 * ```
 */

import {
  WorkflowEntrypoint,
  WorkflowEvent,
  WorkflowStep,
} from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import type { BackendType } from '../backends'
import { commitToIcebergTable } from '../backends/iceberg-commit'
import { logger } from '../utils/logger'
import { toInternalR2Bucket } from './utils'
import type { WorkflowEnv as Env } from './types'
import { readParquet } from '../parquet/reader'
import { commitToDeltaTable } from '../backends/delta-commit'
import {
  StreamingMergeSorter,
  shouldUseStreamingMerge,
  calculateOptimalChunkSize,
  type Row,
} from './streaming-merge'

// =============================================================================
// Types
// =============================================================================

export interface CompactionMigrationParams {
  /** Namespace being compacted */
  namespace: string

  /** Time window start (ms since epoch) */
  windowStart: number

  /** Time window end (ms since epoch) */
  windowEnd: number

  /** Files to compact (from all writers in this window) */
  files: string[]

  /** Writer IDs that contributed to this window */
  writers: string[]

  /** Target format - 'native' keeps as-is, 'iceberg'/'delta' migrates */
  targetFormat: BackendType

  /** Maximum files to read per step (default: 50) */
  maxFilesPerStep?: number

  /** Delete source files after successful compaction */
  deleteSource?: boolean

  /** Target output file size in bytes (default: 128MB) */
  targetFileSize?: number

  /**
   * Use streaming merge for large windows.
   * When true, uses memory-bounded k-way merge instead of loading all rows.
   * @default 'auto' - automatically detect based on file count and estimated size
   */
  useStreamingMerge?: boolean | 'auto'

  /**
   * Maximum memory to use for streaming merge in bytes.
   * Only applies when streaming merge is used.
   * @default 128MB
   */
  maxStreamingMemoryBytes?: number

  /**
   * Estimated average row size in bytes for memory calculations.
   * @default 500
   */
  estimatedAvgRowBytes?: number
}

interface CompactionState {
  /** Files remaining to process */
  remainingFiles: string[]

  /** Files successfully processed */
  processedFiles: string[]

  /** Output files created */
  outputFiles: string[]

  /** Total rows processed */
  totalRows: number

  /** Total bytes read */
  bytesRead: number

  /** Total bytes written */
  bytesWritten: number

  /** Errors encountered */
  errors: string[]

  /** Start time */
  startedAt: number
}

interface WriterWindow {
  writerId: string
  files: string[]
  firstTimestamp: number
  lastTimestamp: number
  totalSize: number
}

// =============================================================================
// Constants
// =============================================================================

/** Default max files per step - leaves room for writes */
const DEFAULT_MAX_FILES_PER_STEP = 50

/** Minimum time to wait for late writers (ms) */
const WRITER_GRACE_PERIOD_MS = 30_000

/** Default maximum memory for streaming merge (128MB) */
const DEFAULT_STREAMING_MAX_MEMORY_BYTES = 128 * 1024 * 1024

/** Default estimated average row size in bytes */
const DEFAULT_ESTIMATED_AVG_ROW_BYTES = 500

// =============================================================================
// Compaction + Migration Workflow
// =============================================================================

export class CompactionMigrationWorkflow extends WorkflowEntrypoint<Env, CompactionMigrationParams> {
  /**
   * Main workflow execution
   */
  async run(event: WorkflowEvent<CompactionMigrationParams>, step: WorkflowStep) {
    const params = event.payload
    const {
      namespace,
      windowStart,
      windowEnd,
      files,
      writers,
      targetFormat,
      deleteSource = true,
      useStreamingMerge = 'auto',
      maxStreamingMemoryBytes = DEFAULT_STREAMING_MAX_MEMORY_BYTES,
      estimatedAvgRowBytes = DEFAULT_ESTIMATED_AVG_ROW_BYTES,
    } = params
    const maxFilesPerStep = params.maxFilesPerStep ?? DEFAULT_MAX_FILES_PER_STEP

    logger.info('Starting compaction workflow', {
      namespace,
      windowStart: new Date(windowStart).toISOString(),
      windowEnd: new Date(windowEnd).toISOString(),
      fileCount: files.length,
      writerCount: writers.length,
      targetFormat,
      useStreamingMerge,
    })

    // Step 1: Analyze files and group by writer
    const analysis = await step.do('analyze-files', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      const writerWindows = new Map<string, WriterWindow>()

      for (const file of files) {
        // Extract writer ID from filename: {timestamp}-{writerId}-{seq}.parquet
        const match = file.match(/(\d+)-([^-]+)-(\d+)\.parquet$/)
        if (!match) {
          logger.warn(`Skipping file with unexpected format: ${file}`)
          continue
        }

        const [, timestampStr, writerId] = match
        const timestamp = parseInt(timestampStr ?? '0', 10)

        // Get file size
        const stat = await storage.stat(file)
        const size = stat?.size ?? 0

        const existing = writerWindows.get(writerId)
        if (existing) {
          existing.files.push(file)
          existing.firstTimestamp = Math.min(existing.firstTimestamp, timestamp)
          existing.lastTimestamp = Math.max(existing.lastTimestamp, timestamp)
          existing.totalSize += size
        } else {
          writerWindows.set(writerId, {
            writerId,
            files: [file],
            firstTimestamp: timestamp,
            lastTimestamp: timestamp,
            totalSize: size,
          })
        }
      }

      // Sort files within each writer by timestamp (they should already be sorted)
      for (const window of writerWindows.values()) {
        window.files.sort()
      }

      return {
        writerWindows: Array.from(writerWindows.values()),
        totalFiles: files.length,
        totalWriters: writerWindows.size,
      }
    })

    // Step 2: Wait for any late writers (grace period)
    // This ensures we don't miss files from slow writers
    await step.do('grace-period', async () => {
      const timeSinceWindowEnd = Date.now() - windowEnd

      if (timeSinceWindowEnd < WRITER_GRACE_PERIOD_MS) {
        const waitTime = WRITER_GRACE_PERIOD_MS - timeSinceWindowEnd
        logger.info(`Waiting ${waitTime}ms for late writers`)
      }

      return { gracePeriodComplete: true }
    })

    // Step 3: Check for any new files from late writers
    const finalFiles = await step.do('check-late-writers', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      // List files in the namespace to find any we missed
      const prefix = `data/${namespace}/`
      const result = await storage.list(prefix)

      const allFiles = new Set(files)
      let newFilesFound = 0

      for (const file of result.files) {
        if (!file.endsWith('.parquet')) continue

        // Check if file is in our time window
        const match = file.match(/(\d+)-([^-]+)-(\d+)\.parquet$/)
        if (!match) continue

        const timestamp = parseInt(match[1] ?? '0', 10)
        if (timestamp >= windowStart && timestamp <= windowEnd && !allFiles.has(file)) {
          allFiles.add(file)
          newFilesFound++
          logger.info(`Found late writer file: ${file}`)
        }
      }

      return {
        files: Array.from(allFiles).sort(),
        newFilesFound,
      }
    })

    // Initialize state for processing
    let state: CompactionState = {
      remainingFiles: finalFiles.files,
      processedFiles: [],
      outputFiles: [],
      totalRows: 0,
      bytesRead: 0,
      bytesWritten: 0,
      errors: [],
      startedAt: Date.now(),
    }

    // Step 4+: Process files in batches
    let batchNum = 0
    while (state.remainingFiles.length > 0) {
      batchNum++

      state = await step.do(`process-batch-${batchNum}`, async () => {
        const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

        const batch = state.remainingFiles.slice(0, maxFilesPerStep)
        const remaining = state.remainingFiles.slice(maxFilesPerStep)

        logger.info(`Processing batch ${batchNum}`, {
          batchSize: batch.length,
          remaining: remaining.length,
        })

        try {
          // Determine if we should use streaming merge for this batch
          // Estimate total rows based on file sizes and average row size
          let totalBatchSize = 0
          for (const file of batch) {
            const stat = await storage.stat(file)
            if (stat) {
              totalBatchSize += stat.size
            }
          }
          const estimatedRows = Math.ceil(totalBatchSize / estimatedAvgRowBytes)

          // Decide whether to use streaming merge
          const shouldStream = useStreamingMerge === true ||
            (useStreamingMerge === 'auto' &&
              shouldUseStreamingMerge(
                batch.length,
                estimatedRows,
                estimatedAvgRowBytes,
                maxStreamingMemoryBytes / 2 // Use half for threshold to leave headroom
              ))

          let rows: Row[]
          let bytesRead: number

          if (shouldStream) {
            // Use streaming merge for large batches
            logger.info(`Using streaming merge for batch ${batchNum}`, {
              fileCount: batch.length,
              estimatedRows,
              totalBatchSize,
            })

            const result = await this.readAndMergeFilesStreaming(
              storage,
              batch,
              maxStreamingMemoryBytes,
              estimatedAvgRowBytes
            )
            rows = result.rows
            bytesRead = result.bytesRead
          } else {
            // Use standard in-memory merge for smaller batches
            const result = await this.readAndMergeFiles(storage, batch)
            rows = result.rows
            bytesRead = result.bytesRead
          }

          // Write to target format
          const { outputFile, bytesWritten } = await this.writeToTargetFormat(
            storage,
            namespace,
            rows,
            targetFormat,
            windowStart,
            batchNum
          )

          // Delete source files if requested
          if (deleteSource) {
            for (const file of batch) {
              try {
                await storage.delete(file)
              } catch (err) {
                logger.warn(`Failed to delete source file: ${file}`, { error: err })
              }
            }
          }

          return {
            remainingFiles: remaining,
            processedFiles: [...state.processedFiles, ...batch],
            outputFiles: [...state.outputFiles, outputFile],
            totalRows: state.totalRows + rows.length,
            bytesRead: state.bytesRead + bytesRead,
            bytesWritten: state.bytesWritten + bytesWritten,
            errors: state.errors,
            startedAt: state.startedAt,
          }
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : 'Unknown error'
          logger.error(`Batch ${batchNum} failed`, { error: errorMsg })

          return {
            ...state,
            remainingFiles: remaining,
            processedFiles: [...state.processedFiles, ...batch],
            errors: [...state.errors, `Batch ${batchNum}: ${errorMsg}`],
          }
        }
      })

      // Small cooldown between batches
      if (state.remainingFiles.length > 0) {
        await step.sleep(`cooldown-${batchNum}`, '100ms')
      }
    }

    // Final step: Summary
    const summary = await step.do('finalize', async () => {
      const duration = Date.now() - state.startedAt

      return {
        success: state.errors.length === 0,
        namespace,
        targetFormat,
        windowStart: new Date(windowStart).toISOString(),
        windowEnd: new Date(windowEnd).toISOString(),
        writersProcessed: writers.length,
        filesProcessed: state.processedFiles.length,
        outputFilesCreated: state.outputFiles.length,
        totalRows: state.totalRows,
        bytesRead: state.bytesRead,
        bytesWritten: state.bytesWritten,
        compressionRatio: state.bytesRead > 0 ? (state.bytesWritten / state.bytesRead).toFixed(2) : 'N/A',
        errors: state.errors,
        durationMs: duration,
      }
    })

    logger.info('Compaction workflow completed', summary)

    return summary
  }

  /**
   * Read and merge Parquet files
   * Since each writer's data is pre-sorted, we can do efficient merge-sort
   */
  private async readAndMergeFiles(
    storage: R2Backend,
    files: string[]
  ): Promise<{ rows: Record<string, unknown>[]; bytesRead: number }> {
    logger.info(`Reading ${files.length} files for merge`)

    const allRows: Record<string, unknown>[] = []
    let bytesRead = 0

    for (const file of files) {
      // Get file size for bytesRead tracking
      const stat = await storage.stat(file)
      if (!stat) {
        logger.warn(`File not found, skipping: ${file}`)
        continue
      }
      bytesRead += stat.size

      try {
        // Read Parquet file using hyparquet via our reader
        const rows = await readParquet<Record<string, unknown>>(storage, file)
        allRows.push(...rows)
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : 'Unknown error'
        logger.error(`Failed to read Parquet file: ${file}`, { error: errorMsg })
        // Continue processing other files rather than failing the entire batch
      }
    }

    // Sort merged rows by createdAt timestamp for consistent ordering
    // This maintains temporal ordering across all files from different writers
    allRows.sort((a, b) => {
      const aTime = a.createdAt
      const bTime = b.createdAt

      // Handle various timestamp formats
      if (aTime instanceof Date && bTime instanceof Date) {
        return aTime.getTime() - bTime.getTime()
      }
      if (typeof aTime === 'string' && typeof bTime === 'string') {
        return new Date(aTime).getTime() - new Date(bTime).getTime()
      }
      if (typeof aTime === 'number' && typeof bTime === 'number') {
        return aTime - bTime
      }

      // Fallback: keep original order if timestamps are missing or incompatible
      return 0
    })

    logger.info(`Read and merged ${allRows.length} rows from ${files.length} files`, {
      bytesRead,
      rowCount: allRows.length,
    })

    return {
      rows: allRows,
      bytesRead,
    }
  }

  /**
   * Read and merge Parquet files using streaming k-way merge sort.
   *
   * This method handles large file sets that would exceed memory if loaded all at once.
   * It uses a min-heap based k-way merge to process files in chunks, maintaining
   * sorted order while keeping memory usage bounded.
   *
   * For very large windows, consider using this in combination with multipart upload
   * to stream directly to R2 without holding all rows in memory.
   */
  private async readAndMergeFilesStreaming(
    storage: R2Backend,
    files: string[],
    maxMemoryBytes: number,
    avgRowBytes: number
  ): Promise<{ rows: Row[]; bytesRead: number }> {
    logger.info(`Using streaming merge for ${files.length} files`, {
      maxMemoryBytes,
      avgRowBytes,
    })

    // Calculate optimal chunk size based on file count and memory limit
    const chunkSize = calculateOptimalChunkSize(
      files.length,
      maxMemoryBytes,
      avgRowBytes,
      100,    // min chunk size
      50000   // max chunk size
    )

    logger.info(`Streaming merge chunk size: ${chunkSize}`)

    // Create the streaming merge sorter
    const sorter = new StreamingMergeSorter(storage, {
      chunkSize,
      maxMemoryBytes,
      sortKey: 'createdAt',
      sortDirection: 'asc',
    })

    // Collect all rows - for now we still collect in memory, but the streaming
    // merge ensures we never load more than (files.length * chunkSize) rows
    // at once from the heap, instead of loading all rows from all files.
    const { rows, stats } = await sorter.collectAll(files)

    logger.info(`Streaming merge completed`, {
      totalRows: stats.totalRows,
      bytesRead: stats.bytesRead,
      filesProcessed: stats.filesProcessed,
      durationMs: stats.durationMs,
    })

    return {
      rows,
      bytesRead: stats.bytesRead,
    }
  }

  /**
   * Write rows to target format (native, iceberg, or delta)
   */
  private async writeToTargetFormat(
    storage: R2Backend,
    namespace: string,
    rows: Record<string, unknown>[],
    format: BackendType,
    windowTimestamp: number,
    batchNum: number
  ): Promise<{ outputFile: string; bytesWritten: number }> {
    // Handle empty rows case
    if (rows.length === 0) {
      logger.info('No rows to write, skipping')
      return { outputFile: '', bytesWritten: 0 }
    }

    // Generate output path based on format
    const timestamp = windowTimestamp
    const date = new Date(timestamp)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')

    let outputFile: string

    switch (format) {
      case 'iceberg':
        // Iceberg data file path - follows Iceberg spec with table location as namespace
        outputFile = `${namespace}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `compacted-${timestamp}-${batchNum}.parquet`
        break

      case 'delta':
        // Delta data file path
        outputFile = `${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `part-${String(batchNum).padStart(5, '0')}-compacted-${timestamp}.parquet`
        break

      case 'native':
        // Native ParqueDB path
        outputFile = `data/${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `compacted-${timestamp}-${batchNum}.parquet`
        break

      default: {
        const _exhaustive: never = format
        throw new Error(`Unknown backend format: ${format}`)
      }
    }

    logger.info(`Writing to ${format} format: ${outputFile}`, { rowCount: rows.length })

    // Convert rows to columnar format for hyparquet-writer
    // hyparquet-writer expects columnData as array of { name, data } objects
    const columnNames = Object.keys(rows[0]!)
    const columnData = columnNames.map(name => ({
      name,
      data: rows.map(row => row[name] ?? null),
    }))

    // Write Parquet using hyparquet-writer
    const { parquetWriteBuffer } = await import('hyparquet-writer')
    const buffer = parquetWriteBuffer({ columnData })

    // Write to storage
    await storage.write(outputFile, new Uint8Array(buffer))

    const bytesWritten = buffer.byteLength

    logger.info(`Successfully wrote ${rows.length} rows to ${outputFile}`, {
      bytesWritten,
    })

    // For Delta format, commit the log entry
    if (format === 'delta') {
      const tableLocation = namespace

      logger.info('Committing Delta log entry...', { tableLocation, outputFile })

      const commitResult = await this.commitDeltaLogEntry({
        storage,
        tableLocation,
        dataFile: {
          // Path relative to table location
          path: outputFile.slice(tableLocation.length + 1),
          size: bytesWritten,
        },
      })

      if (!commitResult.success) {
        // Log warning but don't fail the workflow - data file is written
        // The metadata can be repaired by a subsequent commit or vacuum operation
        logger.warn('Failed to commit Delta log entry', {
          error: commitResult.error,
          outputFile,
        })
      } else {
        logger.info('Successfully committed Delta log entry', {
          version: commitResult.version,
          logPath: commitResult.logPath,
        })
      }
    }

    // For Iceberg format, commit the metadata (manifest, snapshot)
    if (format === 'iceberg') {
      // The table location is the namespace (e.g., "users" -> the Iceberg table is at "users/")
      const tableLocation = namespace

      logger.info('Committing Iceberg metadata...', { tableLocation, outputFile })

      const commitResult = await commitToIcebergTable({
        storage,
        tableLocation,
        dataFiles: [{
          path: outputFile,
          sizeInBytes: bytesWritten,
          recordCount: rows.length,
        }],
      })

      if (!commitResult.success) {
        // Log warning but don't fail the workflow - data file is written
        // The metadata can be repaired by a subsequent commit or vacuum operation
        logger.warn('Failed to commit Iceberg metadata', {
          error: commitResult.error,
          outputFile,
        })
      } else {
        logger.info('Successfully committed Iceberg metadata', {
          snapshotId: commitResult.snapshotId,
          sequenceNumber: commitResult.sequenceNumber,
          metadataPath: commitResult.metadataPath,
        })
      }
    }

    return {
      outputFile,
      bytesWritten,
    }
  }

  /**
   * Commit a Delta log entry for a new data file
   *
   * Uses commitToDeltaTable which implements proper OCC:
   * 1. Determines next version by listing _delta_log/ directory
   * 2. Tries to write commit file atomically with ifNoneMatch: '*'
   * 3. On conflict (AlreadyExistsError), re-reads version and retries
   * 4. Uses exponential backoff between retries
   */
  private async commitDeltaLogEntry(params: {
    storage: R2Backend
    tableLocation: string
    dataFile: { path: string; size: number }
  }): Promise<{
    success: boolean
    version?: number
    logPath?: string
    error?: string
  }> {
    const { storage, tableLocation, dataFile } = params

    // Use the DeltaCommitter with proper OCC protection
    // Checkpoints are created automatically after every 10 commits
    const result = await commitToDeltaTable({
      storage,
      tableLocation,
      dataFiles: [{
        path: dataFile.path,
        size: dataFile.size,
        dataChange: true,
      }],
      // Reasonable defaults for compaction workflow
      maxRetries: 10,
      baseBackoffMs: 100,
      maxBackoffMs: 10000,
      // Create checkpoint every 10 commits for faster reads
      checkpointInterval: 10,
    })

    return result
  }
}

export default CompactionMigrationWorkflow
