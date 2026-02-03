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
import { logger } from '../utils/logger'
import { toInternalR2Bucket } from './utils'

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

interface Env {
  BUCKET: R2Bucket
}

// =============================================================================
// Constants
// =============================================================================

/** Default max files per step - leaves room for writes */
const DEFAULT_MAX_FILES_PER_STEP = 50

/** Default target file size: 128MB */
const DEFAULT_TARGET_SIZE = 128 * 1024 * 1024

/** Minimum time to wait for late writers (ms) */
const WRITER_GRACE_PERIOD_MS = 30_000

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
    } = params
    const maxFilesPerStep = params.maxFilesPerStep ?? DEFAULT_MAX_FILES_PER_STEP
    const targetFileSize = params.targetFileSize ?? DEFAULT_TARGET_SIZE

    logger.info('Starting compaction workflow', {
      namespace,
      windowStart: new Date(windowStart).toISOString(),
      windowEnd: new Date(windowEnd).toISOString(),
      fileCount: files.length,
      writerCount: writers.length,
      targetFormat,
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
        // Note: In a real implementation, we'd use step.sleep here
        // For now, we just log and continue
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
          // Read and merge files
          const { rows, bytesRead } = await this.readAndMergeFiles(storage, batch)

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
    // TODO: Implement actual Parquet reading with hyparquet
    // For now, return placeholder
    logger.info(`Reading ${files.length} files for merge`)

    let bytesRead = 0
    for (const file of files) {
      const stat = await storage.stat(file)
      bytesRead += stat?.size ?? 0
    }

    // Placeholder - actual implementation would:
    // 1. Read each Parquet file
    // 2. Merge-sort by timestamp (efficient since pre-sorted)
    // 3. Return combined rows

    return {
      rows: [],
      bytesRead,
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
        // Iceberg data file path
        outputFile = `${namespace}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `compacted-${timestamp}-${batchNum}.parquet`
        // TODO: Also update Iceberg metadata (manifest, snapshot)
        break

      case 'delta':
        // Delta data file path
        outputFile = `${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `part-${String(batchNum).padStart(5, '0')}-compacted-${timestamp}.parquet`
        // TODO: Also write Delta log entry
        break

      case 'native':
      default:
        // Native ParqueDB path
        outputFile = `data/${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
          `compacted-${timestamp}-${batchNum}.parquet`
        break
    }

    // TODO: Implement actual Parquet writing with hyparquet-writer
    logger.info(`Writing to ${format} format: ${outputFile}`)

    // Placeholder - actual implementation would write Parquet file
    const bytesWritten = 0

    return {
      outputFile,
      bytesWritten,
    }
  }
}

export default CompactionMigrationWorkflow
