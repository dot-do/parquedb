/**
 * Vacuum Workflow
 *
 * Uses Cloudflare Workflows for reliable, resumable vacuum operations that
 * identify and clean up orphaned files from failed commits.
 *
 * Orphaned files can occur when:
 * - OCC commit fails and another process wins the version
 * - Workflow crashes mid-commit
 * - Network errors during metadata updates
 *
 * Key features:
 * - Scans for orphaned Iceberg files (manifests, data files not in active snapshots)
 * - Scans for orphaned Delta files (parquet files not referenced in _delta_log)
 * - Configurable retention period (default: 24 hours)
 * - Dry-run mode for analysis without deletion
 * - Detailed audit trail of all deletions
 *
 * @example
 * ```typescript
 * // Start vacuum workflow
 * const instance = await env.VACUUM_WORKFLOW.create({
 *   params: {
 *     namespace: 'users',
 *     retentionMs: 24 * 60 * 60 * 1000, // 24 hours
 *     dryRun: false,
 *   }
 * })
 *
 * // Check status
 * const status = await instance.status()
 * ```
 */

import {
  WorkflowEntrypoint,
  WorkflowEvent,
  WorkflowStep,
} from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import { toInternalR2Bucket } from './utils'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

export interface VacuumWorkflowParams {
  /** Namespace/table to vacuum */
  namespace: string

  /** Table format ('iceberg' | 'delta' | 'auto' to detect) */
  format?: 'iceberg' | 'delta' | 'auto'

  /** Retention period in milliseconds (default: 24 hours) */
  retentionMs?: number

  /** Only report what would be deleted (default: false) */
  dryRun?: boolean

  /** Warehouse/location prefix (default: '') */
  warehouse?: string

  /** Database name for Iceberg tables (default: '') */
  database?: string
}

export interface VacuumResult {
  /** Whether the vacuum succeeded */
  success: boolean

  /** Number of files scanned */
  filesScanned: number

  /** Number of orphaned files found */
  orphansFound: number

  /** Number of files deleted (0 in dry run) */
  filesDeleted: number

  /** Bytes recovered (0 in dry run) */
  bytesRecovered: number

  /** Detailed list of orphaned/deleted files */
  orphanedFiles: OrphanedFileInfo[]

  /** Errors encountered */
  errors: string[]

  /** Duration in milliseconds */
  durationMs: number

  /** Was this a dry run? */
  dryRun: boolean
}

export interface OrphanedFileInfo {
  /** Path to the orphaned file */
  path: string

  /** File size in bytes */
  size: number

  /** Last modified time */
  lastModified: Date

  /** Age in milliseconds */
  ageMs: number

  /** Why this file is considered orphaned */
  reason: 'not_in_snapshot' | 'not_in_delta_log' | 'removed_action_expired'

  /** Whether the file was deleted (false in dry run) */
  deleted: boolean
}

interface Env {
  BUCKET: R2Bucket
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Default retention period: 24 hours
 * Files must be older than this to be deleted
 */
const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000

/**
 * Maximum files to process per step to stay under subrequest limits
 */
const MAX_FILES_PER_STEP = 200

// =============================================================================
// Vacuum Workflow
// =============================================================================

export class VacuumWorkflow extends WorkflowEntrypoint<Env, VacuumWorkflowParams> {
  /**
   * Main workflow execution
   */
  override async run(event: WorkflowEvent<VacuumWorkflowParams>, step: WorkflowStep) {
    const params = event.payload
    const namespace = params.namespace
    const format = params.format ?? 'auto'
    const retentionMs = params.retentionMs ?? DEFAULT_RETENTION_MS
    const dryRun = params.dryRun ?? false
    const warehouse = params.warehouse ?? ''
    const database = params.database ?? ''

    const startTime = Date.now()
    const cutoffTime = startTime - retentionMs

    // Step 1: Detect table format
    const detectedFormat = await step.do('detect-format', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      if (format !== 'auto') {
        return format
      }

      // Build table location
      const tableLocation = this.buildTableLocation(warehouse, database, namespace)

      // Check for Iceberg metadata
      const icebergMetadataPath = `${tableLocation}/metadata/`
      const icebergFiles = await storage.list(icebergMetadataPath)
      const hasIceberg = icebergFiles.files.some(f =>
        f.endsWith('.metadata.json') || f.includes('version-hint.text')
      )

      if (hasIceberg) {
        return 'iceberg' as const
      }

      // Check for Delta log
      const deltaLogPath = `${tableLocation}/_delta_log/`
      const deltaFiles = await storage.list(deltaLogPath)
      const hasDelta = deltaFiles.files.some(f => f.endsWith('.json'))

      if (hasDelta) {
        return 'delta' as const
      }

      // Default to iceberg if no format detected
      logger.warn(`No table format detected for ${namespace}, defaulting to iceberg`)
      return 'iceberg' as const
    })

    logger.info(`Vacuum workflow starting for ${namespace}`, {
      format: detectedFormat,
      retentionMs,
      dryRun,
    })

    // Step 2: Collect referenced files based on format
    const referencedFiles = await step.do('collect-referenced-files', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))
      const tableLocation = this.buildTableLocation(warehouse, database, namespace)

      if (detectedFormat === 'iceberg') {
        return this.collectIcebergReferencedFiles(storage, tableLocation)
      } else {
        return this.collectDeltaReferencedFiles(storage, tableLocation, cutoffTime)
      }
    })

    // Step 3: List all files in storage
    const allFiles = await step.do('list-all-files', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))
      const tableLocation = this.buildTableLocation(warehouse, database, namespace)

      return this.listAllTableFiles(storage, tableLocation, detectedFormat)
    })

    // Step 4: Identify orphaned files
    const orphanedFiles = await step.do('identify-orphans', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))
      const tableLocation = this.buildTableLocation(warehouse, database, namespace)
      const referencedSet = new Set(referencedFiles)

      const orphans: OrphanedFileInfo[] = []

      for (const filePath of allFiles) {
        // Skip if file is referenced
        if (referencedSet.has(filePath)) {
          continue
        }

        // Get file stats
        const stat = await storage.stat(filePath)
        if (!stat) continue

        const fileAge = startTime - stat.mtime.getTime()

        // Skip if file is newer than retention period
        if (stat.mtime.getTime() > cutoffTime) {
          continue
        }

        const reason = this.determineOrphanReason(filePath, tableLocation, detectedFormat)

        orphans.push({
          path: filePath,
          size: stat.size,
          lastModified: stat.mtime,
          ageMs: fileAge,
          reason,
          deleted: false,
        })
      }

      return orphans
    })

    // Step 5: Delete orphaned files (in batches)
    let deletedFiles: OrphanedFileInfo[] = []
    let bytesRecovered = 0
    const errors: string[] = []

    if (!dryRun && orphanedFiles.length > 0) {
      // Process deletions in batches to stay under subrequest limits
      const batches = this.chunkArray(orphanedFiles, MAX_FILES_PER_STEP)

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i]!
        const batchResult = await step.do(`delete-batch-${i}`, async () => {
          const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))
          const results: Array<{ file: OrphanedFileInfo; deleted: boolean; error?: string }> = []

          for (const file of batch) {
            try {
              await storage.delete(file.path)
              results.push({ file: { ...file, deleted: true }, deleted: true })
              logger.info(`Vacuum deleted: ${file.path}`, {
                size: file.size,
                ageMs: file.ageMs,
                reason: file.reason,
              })
            } catch (error) {
              const errorMsg = error instanceof Error ? error.message : 'Unknown error'
              results.push({ file, deleted: false, error: errorMsg })
              logger.error(`Failed to delete ${file.path}`, { error: errorMsg })
            }
          }

          return results
        })

        for (const result of batchResult) {
          if (result.deleted) {
            deletedFiles.push(result.file)
            bytesRecovered += result.file.size
          } else if (result.error) {
            errors.push(`${result.file.path}: ${result.error}`)
          }
        }
      }
    }

    // Final step: Build result
    const result = await step.do('finalize', async () => {
      const duration = Date.now() - startTime

      const vacuumResult: VacuumResult = {
        success: errors.length === 0,
        filesScanned: allFiles.length,
        orphansFound: orphanedFiles.length,
        filesDeleted: deletedFiles.length,
        bytesRecovered,
        orphanedFiles: dryRun ? orphanedFiles : deletedFiles,
        errors,
        durationMs: duration,
        dryRun,
      }

      logger.info('Vacuum workflow completed', {
        namespace,
        format: detectedFormat,
        filesScanned: vacuumResult.filesScanned,
        orphansFound: vacuumResult.orphansFound,
        filesDeleted: vacuumResult.filesDeleted,
        bytesRecovered: vacuumResult.bytesRecovered,
        durationMs: vacuumResult.durationMs,
        dryRun,
      })

      return vacuumResult
    })

    return result
  }

  // ===========================================================================
  // Iceberg File Collection
  // ===========================================================================

  /**
   * Collect all files referenced by active Iceberg snapshots
   */
  private async collectIcebergReferencedFiles(
    storage: R2Backend,
    tableLocation: string
  ): Promise<string[]> {
    const referencedFiles: string[] = []

    // Read version-hint.text to find current metadata
    const versionHintPath = `${tableLocation}/metadata/version-hint.text`
    let currentMetadataPath: string | null = null

    try {
      const versionHintData = await storage.read(versionHintPath)
      currentMetadataPath = new TextDecoder().decode(versionHintData).trim()
      referencedFiles.push(versionHintPath)
    } catch {
      // No version hint - try to find metadata files directly
      const metadataFiles = await storage.list(`${tableLocation}/metadata/`)
      const metadataJsonFiles = metadataFiles.files
        .filter(f => f.endsWith('.metadata.json'))
        .sort()

      if (metadataJsonFiles.length > 0) {
        currentMetadataPath = metadataJsonFiles[metadataJsonFiles.length - 1]!
      }
    }

    if (!currentMetadataPath) {
      return referencedFiles
    }

    // Reference the current metadata file
    referencedFiles.push(currentMetadataPath)

    // Read metadata and collect referenced files from all snapshots
    try {
      const metadataData = await storage.read(currentMetadataPath)
      const metadata = JSON.parse(new TextDecoder().decode(metadataData)) as {
        snapshots?: Array<{ 'manifest-list'?: string }>
      }

      // Process each snapshot's manifest list
      for (const snapshot of metadata.snapshots ?? []) {
        const manifestListPath = snapshot['manifest-list']
        if (!manifestListPath) continue

        referencedFiles.push(manifestListPath)

        // Read manifest list to get manifests
        try {
          const manifestListData = await storage.read(manifestListPath)
          const manifests = this.parseManifestList(manifestListData)

          for (const manifest of manifests) {
            referencedFiles.push(manifest.manifestPath)

            // Read manifest to get data files
            try {
              const manifestData = await storage.read(manifest.manifestPath)
              const dataFiles = this.parseManifestDataFiles(manifestData)
              referencedFiles.push(...dataFiles)
            } catch {
              // Skip inaccessible manifests
            }
          }
        } catch {
          // Skip inaccessible manifest lists
        }
      }
    } catch {
      // Skip if metadata is inaccessible
    }

    return referencedFiles
  }

  /**
   * Parse manifest list (handles both Avro and JSON formats)
   */
  private parseManifestList(data: Uint8Array): Array<{ manifestPath: string }> {
    // Try JSON first (simpler format)
    try {
      const text = new TextDecoder().decode(data)
      if (text.startsWith('[') || text.startsWith('{')) {
        const parsed = JSON.parse(text) as Array<{ 'manifest-path': string }> | { manifests: Array<{ 'manifest-path': string }> }
        const manifests = Array.isArray(parsed) ? parsed : (parsed.manifests ?? [])
        return manifests.map(m => ({ manifestPath: m['manifest-path'] }))
      }
    } catch {
      // Not valid JSON
    }

    // For Avro, look for embedded file paths
    // This is a simplified parser that extracts file paths from the binary data
    let text: string
    try {
      text = new TextDecoder().decode(data)
    } catch {
      // If decoding fails, try to extract what we can
      text = ''
      for (let i = 0; i < data.length; i++) {
        const byte = data[i]!
        if (byte >= 32 && byte < 127) {
          text += String.fromCharCode(byte)
        }
      }
    }
    const paths: string[] = []
    const pathRegex = /([a-zA-Z0-9_\-/]+\.(avro|parquet))/g
    let match: RegExpExecArray | null
    while ((match = pathRegex.exec(text)) !== null) {
      paths.push(match[1]!)
    }

    return paths.map(p => ({ manifestPath: p }))
  }

  /**
   * Parse manifest to extract data file paths
   */
  private parseManifestDataFiles(data: Uint8Array): string[] {
    // Similar approach - try to extract file paths
    let text: string
    try {
      text = new TextDecoder().decode(data)
    } catch {
      // If decoding fails, try to extract what we can
      text = ''
      for (let i = 0; i < data.length; i++) {
        const byte = data[i]!
        if (byte >= 32 && byte < 127) {
          text += String.fromCharCode(byte)
        }
      }
    }
    const paths: string[] = []
    const pathRegex = /([a-zA-Z0-9_\-/]+\.parquet)/g
    let match: RegExpExecArray | null
    while ((match = pathRegex.exec(text)) !== null) {
      paths.push(match[1]!)
    }
    return paths
  }

  // ===========================================================================
  // Delta File Collection
  // ===========================================================================

  /**
   * Collect all files referenced by active Delta log entries
   */
  private async collectDeltaReferencedFiles(
    storage: R2Backend,
    tableLocation: string,
    cutoffTime: number
  ): Promise<string[]> {
    const referencedFiles: string[] = []
    const logPath = `${tableLocation}/_delta_log/`

    // Always reference log files themselves
    const logFiles = await storage.list(logPath)
    for (const file of logFiles.files) {
      referencedFiles.push(file)
    }

    // Track active files (add actions) and removed files with timestamps
    const activeFiles = new Map<string, boolean>()
    const removedFiles = new Map<string, number>() // path -> removal timestamp

    // Sort commit files by version
    const commitFiles = logFiles.files
      .filter(f => f.endsWith('.json') && !f.includes('checkpoint') && !f.includes('_last_checkpoint'))
      .sort()

    for (const commitFile of commitFiles) {
      try {
        const commitData = await storage.read(commitFile)
        const commitText = new TextDecoder().decode(commitData)
        const actions = this.parseDeltaCommit(commitText)

        for (const action of actions) {
          if (action.type === 'add') {
            const fullPath = `${tableLocation}/${action.path}`
            activeFiles.set(fullPath, true)
            removedFiles.delete(fullPath) // If re-added, remove from removed set
          } else if (action.type === 'remove') {
            const fullPath = `${tableLocation}/${action.path}`
            activeFiles.delete(fullPath)
            removedFiles.set(fullPath, action.timestamp ?? Date.now())
          }
        }
      } catch {
        // Skip inaccessible commit files
      }
    }

    // Add all active files to referenced set
    for (const path of activeFiles.keys()) {
      referencedFiles.push(path)
    }

    // Add removed files that are still within retention (for time travel)
    for (const [path, timestamp] of removedFiles.entries()) {
      if (timestamp > cutoffTime) {
        referencedFiles.push(path)
      }
    }

    return referencedFiles
  }

  /**
   * Parse Delta commit file (NDJSON format)
   */
  private parseDeltaCommit(content: string): Array<{
    type: 'add' | 'remove'
    path: string
    timestamp?: number
  }> {
    const actions: Array<{ type: 'add' | 'remove'; path: string; timestamp?: number }> = []
    const lines = content.trim().split('\n')

    for (const line of lines) {
      try {
        const action = JSON.parse(line) as Record<string, unknown>
        if ('add' in action) {
          const add = action.add as { path: string }
          actions.push({ type: 'add', path: add.path })
        } else if ('remove' in action) {
          const remove = action.remove as { path: string; deletionTimestamp?: number }
          actions.push({
            type: 'remove',
            path: remove.path,
            timestamp: remove.deletionTimestamp,
          })
        }
      } catch {
        // Skip invalid JSON lines
      }
    }

    return actions
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Build the table location path
   */
  private buildTableLocation(warehouse: string, database: string, namespace: string): string {
    const parts = [warehouse, database, namespace].filter(Boolean)
    return parts.join('/')
  }

  /**
   * List all files that could potentially be orphaned
   */
  private async listAllTableFiles(
    storage: R2Backend,
    tableLocation: string,
    format: 'iceberg' | 'delta'
  ): Promise<string[]> {
    const files: string[] = []

    if (format === 'iceberg') {
      // List metadata files (manifests, manifest lists, metadata.json)
      const metadataFiles = await storage.list(`${tableLocation}/metadata/`)
      files.push(...metadataFiles.files)

      // List data files
      const dataFiles = await storage.list(`${tableLocation}/data/`)
      files.push(...dataFiles.files)
    } else {
      // Delta: list all parquet files in table directory (excluding _delta_log)
      const allFiles = await storage.list(`${tableLocation}/`)
      for (const file of allFiles.files) {
        if (file.endsWith('.parquet') && !file.includes('/_delta_log/')) {
          files.push(file)
        }
      }

      // Also list checkpoint parquet files in _delta_log
      const logFiles = await storage.list(`${tableLocation}/_delta_log/`)
      for (const file of logFiles.files) {
        if (file.endsWith('.parquet')) {
          files.push(file)
        }
      }
    }

    return files
  }

  /**
   * Determine why a file is considered orphaned
   */
  private determineOrphanReason(
    filePath: string,
    tableLocation: string,
    format: 'iceberg' | 'delta'
  ): OrphanedFileInfo['reason'] {
    if (format === 'delta') {
      // For Delta, files not in log are either never added or removed
      return 'not_in_delta_log'
    }

    // For Iceberg
    if (filePath.includes('/metadata/')) {
      return 'not_in_snapshot'
    }
    return 'not_in_snapshot'
  }

  /**
   * Chunk an array into smaller arrays
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = []
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize))
    }
    return chunks
  }
}

export default VacuumWorkflow
