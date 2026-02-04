/**
 * Hierarchical Compaction (LSM-tree style) - Worker Classes
 *
 * This module contains Cloudflare Worker-specific classes:
 * - LevelStateDO: Durable Object for tracking level state
 * - CompactionPromotionWorkflow: Workflow for promoting files between levels
 *
 * For types and pure functions, see hierarchical-compaction-types.ts
 *
 * @example
 * ```typescript
 * // Configure hierarchical compaction for a namespace
 * const config: HierarchicalCompactionConfig = {
 *   enabled: true,
 *   levels: {
 *     l0ToL1Threshold: 24,  // Promote when L0 has 24 files
 *     l1ToL2Threshold: 7,   // Promote when L1 has 7 files
 *   }
 * }
 * ```
 */

import {
  WorkflowEntrypoint,
  WorkflowEvent,
  WorkflowStep,
} from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import { logger } from '../utils/logger'
import { toInternalR2Bucket } from './utils'
import type { WorkflowEnv as Env } from './types'
import { readParquet } from '../parquet/reader'

// Re-export all types and pure functions
export {
  type CompactionLevel,
  type HierarchicalCompactionLevels,
  type HierarchicalCompactionConfig,
  type LevelFileMetadata,
  type LevelState,
  type NamespaceLevelState,
  type CompactionPromotionParams,
  type PromotionResult,
  DEFAULT_HIERARCHICAL_CONFIG,
  getNextLevel,
  getPromotionThreshold,
  getISOWeek,
  generateLevelPath,
  parseLevelFromPath,
  shouldPromote,
  createEmptyLevelState,
  createEmptyNamespaceLevelState,
  addFileToLevel,
  removeFilesFromLevel,
  getPromotionsNeeded,
} from './hierarchical-compaction-types'

import type {
  CompactionLevel,
  HierarchicalCompactionLevels,
  LevelFileMetadata,
  LevelState,
  CompactionPromotionParams,
  PromotionResult,
} from './hierarchical-compaction-types'

import {
  DEFAULT_HIERARCHICAL_CONFIG,
  getNextLevel,
  shouldPromote,
  generateLevelPath,
} from './hierarchical-compaction-types'

// =============================================================================
// Stored State Types (for LevelStateDO)
// =============================================================================

/**
 * Stored format for level file metadata (JSON-serializable)
 */
interface StoredLevelFileMetadata {
  path: string
  size: number
  rowCount?: number | undefined
  windowStart: number
  windowEnd: number
  createdAt: number
}

/**
 * Stored format for level state (JSON-serializable)
 */
interface StoredLevelState {
  level: CompactionLevel
  files: StoredLevelFileMetadata[]
  totalSize: number
  totalRows: number
}

/**
 * Stored format for namespace level state (JSON-serializable)
 */
interface StoredNamespaceLevelState {
  namespace: string
  levels: Record<CompactionLevel, StoredLevelState>
  updatedAt: number
}

// =============================================================================
// LevelStateDO - Durable Object for tracking level state
// =============================================================================

/**
 * Durable Object for tracking files at each compaction level.
 *
 * Each instance handles a single namespace (determined by idFromName(namespace)).
 * Tracks files at L0, L1, L2 and triggers promotion when thresholds are exceeded.
 */
export class LevelStateDO {
  private state: DurableObjectState
  private namespace: string = ''
  private levels: Record<CompactionLevel, LevelState> = {
    l0: { level: 'l0', files: [], totalSize: 0, totalRows: 0 },
    l1: { level: 'l1', files: [], totalSize: 0, totalRows: 0 },
    l2: { level: 'l2', files: [], totalSize: 0, totalRows: 0 },
  }
  private config: HierarchicalCompactionLevels = DEFAULT_HIERARCHICAL_CONFIG.levels ?? {
    l0ToL1Threshold: 24,
    l1ToL2Threshold: 7,
  }
  private initialized = false

  constructor(state: DurableObjectState) {
    this.state = state
  }

  /**
   * Load state from storage
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredNamespaceLevelState>('levelState')
    if (stored) {
      this.namespace = stored.namespace
      // Restore levels
      for (const level of ['l0', 'l1', 'l2'] as CompactionLevel[]) {
        if (stored.levels[level]) {
          this.levels[level] = stored.levels[level]
        }
      }
    }

    const storedConfig = await this.state.storage.get<HierarchicalCompactionLevels>('config')
    if (storedConfig) {
      this.config = storedConfig
    }

    this.initialized = true
  }

  /**
   * Save state to storage
   */
  private async saveState(): Promise<void> {
    const stored: StoredNamespaceLevelState = {
      namespace: this.namespace,
      levels: this.levels,
      updatedAt: Date.now(),
    }
    await this.state.storage.put('levelState', stored)
  }

  /**
   * Handle HTTP requests
   */
  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    switch (url.pathname) {
      case '/add-file':
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 })
        }
        return this.handleAddFile(request)

      case '/remove-files':
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 })
        }
        return this.handleRemoveFiles(request)

      case '/check-promotion':
        return this.handleCheckPromotion()

      case '/status':
        return this.handleStatus()

      case '/configure':
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 })
        }
        return this.handleConfigure(request)

      default:
        return new Response('Not Found', { status: 404 })
    }
  }

  /**
   * Add a file to a level
   */
  private async handleAddFile(request: Request): Promise<Response> {
    const body = await request.json() as {
      namespace: string
      level: CompactionLevel
      file: LevelFileMetadata
    }

    const { namespace, level, file } = body

    // Set namespace on first request
    if (!this.namespace) {
      this.namespace = namespace
    }

    // Add file to level
    const levelState = this.levels[level]
    levelState.files.push(file)
    levelState.totalSize += file.size
    levelState.totalRows += file.rowCount ?? 0

    await this.saveState()

    // Check if promotion is needed
    const needsPromotion = shouldPromote(level, levelState.files.length, this.config)
    const nextLevel = getNextLevel(level)

    return new Response(JSON.stringify({
      success: true,
      level,
      fileCount: levelState.files.length,
      needsPromotion,
      promoteToLevel: needsPromotion ? nextLevel : null,
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Remove files from a level (after promotion)
   */
  private async handleRemoveFiles(request: Request): Promise<Response> {
    const body = await request.json() as {
      level: CompactionLevel
      paths: string[]
    }

    const { level, paths } = body
    const pathSet = new Set(paths)

    const levelState = this.levels[level]
    const removedFiles = levelState.files.filter(f => pathSet.has(f.path))
    levelState.files = levelState.files.filter(f => !pathSet.has(f.path))

    // Recalculate totals
    let totalSize = 0
    let totalRows = 0
    for (const file of levelState.files) {
      totalSize += file.size
      totalRows += file.rowCount ?? 0
    }
    levelState.totalSize = totalSize
    levelState.totalRows = totalRows

    await this.saveState()

    return new Response(JSON.stringify({
      success: true,
      level,
      removedCount: removedFiles.length,
      remainingCount: levelState.files.length,
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Check which levels need promotion
   */
  private handleCheckPromotion(): Response {
    const promotions: Array<{
      fromLevel: CompactionLevel
      toLevel: CompactionLevel
      files: string[]
      fileCount: number
    }> = []

    for (const level of ['l0', 'l1'] as CompactionLevel[]) {
      const levelState = this.levels[level]
      if (shouldPromote(level, levelState.files.length, this.config)) {
        const nextLevel = getNextLevel(level)
        if (nextLevel) {
          promotions.push({
            fromLevel: level,
            toLevel: nextLevel,
            files: levelState.files.map(f => f.path),
            fileCount: levelState.files.length,
          })
        }
      }
    }

    return new Response(JSON.stringify({
      namespace: this.namespace,
      promotionsNeeded: promotions,
      levels: {
        l0: { fileCount: this.levels.l0.files.length, totalSize: this.levels.l0.totalSize },
        l1: { fileCount: this.levels.l1.files.length, totalSize: this.levels.l1.totalSize },
        l2: { fileCount: this.levels.l2.files.length, totalSize: this.levels.l2.totalSize },
      },
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Get current status
   */
  private handleStatus(): Response {
    return new Response(JSON.stringify({
      namespace: this.namespace,
      config: this.config,
      levels: {
        l0: {
          fileCount: this.levels.l0.files.length,
          totalSize: this.levels.l0.totalSize,
          totalRows: this.levels.l0.totalRows,
          files: this.levels.l0.files.map(f => ({
            path: f.path,
            size: f.size,
            windowStart: new Date(f.windowStart).toISOString(),
            windowEnd: new Date(f.windowEnd).toISOString(),
          })),
        },
        l1: {
          fileCount: this.levels.l1.files.length,
          totalSize: this.levels.l1.totalSize,
          totalRows: this.levels.l1.totalRows,
          files: this.levels.l1.files.map(f => ({
            path: f.path,
            size: f.size,
            windowStart: new Date(f.windowStart).toISOString(),
            windowEnd: new Date(f.windowEnd).toISOString(),
          })),
        },
        l2: {
          fileCount: this.levels.l2.files.length,
          totalSize: this.levels.l2.totalSize,
          totalRows: this.levels.l2.totalRows,
          files: this.levels.l2.files.map(f => ({
            path: f.path,
            size: f.size,
            windowStart: new Date(f.windowStart).toISOString(),
            windowEnd: new Date(f.windowEnd).toISOString(),
          })),
        },
      },
    }, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Configure thresholds
   */
  private async handleConfigure(request: Request): Promise<Response> {
    const body = await request.json() as {
      namespace: string
      config: Partial<HierarchicalCompactionLevels>
    }

    const { namespace, config } = body

    if (!this.namespace) {
      this.namespace = namespace
    }

    // Merge with existing config
    this.config = {
      ...this.config,
      ...config,
    }

    await this.state.storage.put('config', this.config)

    return new Response(JSON.stringify({
      success: true,
      config: this.config,
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

// =============================================================================
// CompactionPromotionWorkflow
// =============================================================================

/**
 * Workflow for promoting files from one level to the next.
 *
 * Uses the same merge-sort logic as CompactionMigrationWorkflow,
 * but writes to a higher-level path.
 */
/** Type for merge result from step.do */
type MergeResult = {
  rows: Record<string, unknown>[]
  bytesRead: number
  windowStart: number
  windowEnd: number
}

/** Type for write result from step.do */
type WriteResult = {
  outputFile: string
  bytesWritten: number
}

export class CompactionPromotionWorkflow extends WorkflowEntrypoint<Env, CompactionPromotionParams> {
  /**
   * Main workflow execution
   */
  override async run(event: WorkflowEvent<CompactionPromotionParams>, step: WorkflowStep): Promise<PromotionResult> {
    const params = event.payload
    const {
      namespace,
      fromLevel,
      toLevel,
      files,
      targetFormat,
      deleteSource = true,
    } = params
    const maxFilesPerStep = params.maxFilesPerStep ?? 50

    const startTime = Date.now()
    const errors: string[] = []

    logger.info('Starting promotion workflow', {
      namespace,
      fromLevel,
      toLevel,
      fileCount: files.length,
      targetFormat,
    })

    // Step 1: Read and merge all files from source level
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mergeResult = await step.do('merge-files', async (): Promise<any> => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      const allRows: Record<string, unknown>[] = []
      let bytesRead = 0
      let windowStart = Infinity
      let windowEnd = 0

      // Process files in batches
      for (let i = 0; i < files.length; i += maxFilesPerStep) {
        const batch = files.slice(i, i + maxFilesPerStep)

        for (const file of batch) {
          const stat = await storage.stat(file)
          if (!stat) {
            logger.warn(`File not found, skipping: ${file}`)
            continue
          }
          bytesRead += stat.size

          try {
            const rows = await readParquet<Record<string, unknown>>(storage, file)
            allRows.push(...rows)

            // Extract window from path
            const pathMatch = file.match(/compacted-(\d+)-\d+\.parquet$/)
            if (pathMatch) {
              const ts = parseInt(pathMatch[1]!, 10)
              windowStart = Math.min(windowStart, ts)
              windowEnd = Math.max(windowEnd, ts)
            }
          } catch (err) {
            const errorMsg = err instanceof Error ? err.message : 'Unknown error'
            logger.error(`Failed to read file: ${file}`, { error: errorMsg })
            errors.push(`Read ${file}: ${errorMsg}`)
          }
        }
      }

      // Sort by timestamp
      allRows.sort((a, b) => {
        const aTime = a.createdAt
        const bTime = b.createdAt
        if (aTime instanceof Date && bTime instanceof Date) {
          return aTime.getTime() - bTime.getTime()
        }
        if (typeof aTime === 'number' && typeof bTime === 'number') {
          return aTime - bTime
        }
        return 0
      })

      return {
        rows: allRows,
        bytesRead,
        windowStart: windowStart === Infinity ? Date.now() : windowStart,
        windowEnd: windowEnd || Date.now(),
      }
    }) as MergeResult

    // Step 2: Write to target level
    const writeResult = await step.do('write-output', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      if (mergeResult.rows.length === 0) {
        logger.info('No rows to write, skipping')
        return { outputFile: '', bytesWritten: 0 }
      }

      // Generate output path for target level
      const outputFile = generateLevelPath(
        namespace,
        toLevel,
        mergeResult.windowStart,
        0
      )

      logger.info(`Writing to ${toLevel}: ${outputFile}`, {
        rowCount: mergeResult.rows.length,
      })

      // Convert to columnar format
      const columnNames = Object.keys(mergeResult.rows[0]!)
      const columnData = columnNames.map(name => ({
        name,
        data: mergeResult.rows.map(row => row[name] ?? null),
      }))

      // Write Parquet
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const buffer = parquetWriteBuffer({ columnData })

      await storage.write(outputFile, new Uint8Array(buffer))

      return {
        outputFile,
        bytesWritten: buffer.byteLength,
      }
    }) as WriteResult

    // Step 3: Delete source files if requested
    if (deleteSource && writeResult.outputFile) {
      await step.do('delete-source', async () => {
        const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

        let deletedCount = 0
        for (const file of files) {
          try {
            await storage.delete(file)
            deletedCount++
          } catch (err) {
            const errorMsg = err instanceof Error ? err.message : 'Unknown error'
            logger.warn(`Failed to delete source file: ${file}`, { error: errorMsg })
            errors.push(`Delete ${file}: ${errorMsg}`)
          }
        }

        logger.info(`Deleted ${deletedCount}/${files.length} source files`)
        return { deletedCount }
      })
    }

    const result: PromotionResult = {
      success: errors.length === 0,
      namespace,
      fromLevel,
      toLevel,
      filesProcessed: files.length,
      outputFiles: writeResult.outputFile ? [writeResult.outputFile] : [],
      totalRows: mergeResult.rows.length,
      bytesRead: mergeResult.bytesRead,
      bytesWritten: writeResult.bytesWritten,
      durationMs: Date.now() - startTime,
      errors,
    }

    logger.info('Promotion workflow completed', result)

    return result
  }
}

// =============================================================================
// Integration with existing compaction
// =============================================================================

/**
 * After standard compaction completes, add the output file to L0 level tracking
 */
export async function registerCompactedFile(
  levelStateDO: DurableObjectStub,
  namespace: string,
  file: LevelFileMetadata
): Promise<{ needsPromotion: boolean; promoteToLevel: CompactionLevel | null }> {
  const response = await levelStateDO.fetch('http://internal/add-file', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      namespace,
      level: 'l0',
      file,
    }),
  })

  const result = await response.json() as {
    needsPromotion: boolean
    promoteToLevel: CompactionLevel | null
  }

  return result
}

/**
 * Check if any levels need promotion
 */
export async function checkPromotionNeeded(
  levelStateDO: DurableObjectStub
): Promise<Array<{
  fromLevel: CompactionLevel
  toLevel: CompactionLevel
  files: string[]
  fileCount: number
}>> {
  const response = await levelStateDO.fetch('http://internal/check-promotion')
  const result = await response.json() as {
    promotionsNeeded: Array<{
      fromLevel: CompactionLevel
      toLevel: CompactionLevel
      files: string[]
      fileCount: number
    }>
  }

  return result.promotionsNeeded
}

/**
 * Remove files from a level after successful promotion
 */
export async function removePromotedFiles(
  levelStateDO: DurableObjectStub,
  level: CompactionLevel,
  paths: string[]
): Promise<void> {
  await levelStateDO.fetch('http://internal/remove-files', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ level, paths }),
  })
}

export default {
  LevelStateDO,
  CompactionPromotionWorkflow,
}
