/**
 * Production Source Version Providers for Materialized View Staleness Detection
 *
 * These providers integrate with the actual storage backends to retrieve current
 * versions and change counts for staleness detection.
 *
 * - NativeSourceVersionProvider: Uses event log sequence IDs from EventManifest
 * - IcebergSourceVersionProvider: Uses Iceberg snapshot IDs from table metadata
 * - DeltaSourceVersionProvider: Uses Delta transaction log versions
 */

import type { SourceVersionProvider, SourceVersion } from './staleness'
import type { StorageBackend } from '../types/storage'
import type { EventManifest } from '../events/types'
import { isNotFoundError } from '../storage/errors'

// =============================================================================
// Native Source Version Provider
// =============================================================================

/**
 * Configuration for the NativeSourceVersionProvider
 */
export interface NativeVersionProviderConfig {
  /** Storage backend to read event manifests from */
  storage: StorageBackend
  /** Dataset name (used in path prefix) */
  dataset: string
  /** Events path prefix (default: 'events') */
  eventsPrefix?: string | undefined
}

/**
 * Source version provider that uses ParqueDB's native event log.
 *
 * Reads the event manifest to get the current sequence number and event count
 * for each source collection. The version ID is based on the manifest's nextSeq
 * which monotonically increases with each new event segment.
 *
 * @example
 * ```typescript
 * const provider = new NativeSourceVersionProvider({
 *   storage: r2Backend,
 *   dataset: 'my-app',
 * })
 *
 * const version = await provider.getCurrentVersion('orders')
 * // { versionId: 'seq-42', timestamp: 1704067200000, backend: 'native', ... }
 * ```
 */
export class NativeSourceVersionProvider implements SourceVersionProvider {
  private storage: StorageBackend
  private dataset: string
  private eventsPrefix: string

  constructor(config: NativeVersionProviderConfig) {
    this.storage = config.storage
    this.dataset = config.dataset
    this.eventsPrefix = config.eventsPrefix ?? 'events'
  }

  /**
   * Get the manifest path for a source collection.
   * The manifest tracks all event segments for the dataset.
   */
  private getManifestPath(): string {
    return `${this.dataset}/${this.eventsPrefix}/_manifest.json`
  }

  /**
   * Load the event manifest from storage
   */
  private async loadManifest(): Promise<EventManifest | null> {
    try {
      const data = await this.storage.read(this.getManifestPath())
      const json = new TextDecoder().decode(data)
      return JSON.parse(json) as EventManifest
    } catch (error) {
      // Manifest not found means no events yet
      if (isNotFoundError(error)) {
        return null
      }
      throw error
    }
  }

  /**
   * Get the current version of a source collection.
   *
   * For the native backend, version is based on:
   * - nextSeq: The next segment sequence number (monotonically increasing)
   * - totalEvents: Total event count across all segments
   * - updatedAt: When the manifest was last updated
   */
  async getCurrentVersion(source: string): Promise<SourceVersion | null> {
    const manifest = await this.loadManifest()
    if (!manifest) {
      return null
    }

    // Filter events by source namespace if we have segment-level metadata
    // For now, we use the global manifest state since events aren't partitioned by source
    const lastSegment = manifest.segments.length > 0
      ? manifest.segments[manifest.segments.length - 1]
      : undefined

    return {
      versionId: `seq-${manifest.nextSeq - 1}`,
      timestamp: manifest.updatedAt,
      lastEventId: lastSegment ? `segment-${lastSegment.seq}` : undefined,
      lastEventTs: lastSegment?.maxTs,
      backend: 'native',
    }
  }

  /**
   * Get the number of changes between two versions.
   *
   * This is estimated based on segment event counts between the two sequence numbers.
   */
  async getChangeCount(
    _source: string,
    fromVersion: SourceVersion,
    toVersion: SourceVersion
  ): Promise<number> {
    const manifest = await this.loadManifest()
    if (!manifest) {
      return 0
    }

    // Extract sequence numbers from version IDs
    const fromSeq = this.parseSequenceNumber(fromVersion.versionId)
    const toSeq = this.parseSequenceNumber(toVersion.versionId)

    if (fromSeq === null || toSeq === null) {
      return 0
    }

    // Sum event counts from segments between fromSeq and toSeq
    let changeCount = 0
    for (const segment of manifest.segments) {
      if (segment.seq > fromSeq && segment.seq <= toSeq) {
        changeCount += segment.count
      }
    }

    return changeCount
  }

  /**
   * Check if a version is still valid (segment hasn't been compacted away).
   */
  async isVersionValid(_source: string, version: SourceVersion): Promise<boolean> {
    const manifest = await this.loadManifest()
    if (!manifest) {
      return false
    }

    const seq = this.parseSequenceNumber(version.versionId)
    if (seq === null) {
      return false
    }

    // Check if the sequence number is still within the compaction window
    // A version is invalid if it's been compacted away
    if (manifest.compactedThrough !== undefined) {
      // If the version's timestamp is before the compaction watermark,
      // check if we have segments that cover it
      const segment = manifest.segments.find(s => s.seq === seq)
      if (!segment) {
        // Segment doesn't exist - may have been compacted
        return seq >= (manifest.segments[0]?.seq ?? 0)
      }
    }

    return true
  }

  /**
   * Parse sequence number from version ID (e.g., "seq-42" -> 42)
   */
  private parseSequenceNumber(versionId: string): number | null {
    const match = versionId.match(/^seq-(\d+)$/)
    if (!match) {
      return null
    }
    return parseInt(match[1]!, 10)
  }
}

// =============================================================================
// Iceberg Source Version Provider
// =============================================================================

/**
 * Configuration for the IcebergSourceVersionProvider
 */
export interface IcebergVersionProviderConfig {
  /** Storage backend to read Iceberg metadata from */
  storage: StorageBackend
  /** Warehouse path */
  warehouse: string
  /** Database name (optional, default: 'default') */
  database?: string | undefined
}

/**
 * Source version provider that uses Apache Iceberg snapshot IDs.
 *
 * Reads the Iceberg table metadata to get the current snapshot ID and
 * timestamp for each source collection. The version ID is the snapshot ID.
 *
 * @example
 * ```typescript
 * const provider = new IcebergSourceVersionProvider({
 *   storage: r2Backend,
 *   warehouse: 'warehouse',
 *   database: 'my-db',
 * })
 *
 * const version = await provider.getCurrentVersion('orders')
 * // { versionId: '1234567890', timestamp: 1704067200000, backend: 'iceberg' }
 * ```
 */
export class IcebergSourceVersionProvider implements SourceVersionProvider {
  private storage: StorageBackend
  private warehouse: string
  private database: string

  constructor(config: IcebergVersionProviderConfig) {
    this.storage = config.storage
    this.warehouse = config.warehouse
    this.database = config.database ?? 'default'
  }

  /**
   * Get the table location for a source namespace
   */
  private getTableLocation(source: string): string {
    return this.database
      ? `${this.warehouse}/${this.database}/${source}`
      : `${this.warehouse}/${source}`
  }

  /**
   * Get the version hint file path which points to the current metadata file
   */
  private getVersionHintPath(source: string): string {
    return `${this.getTableLocation(source)}/metadata/version-hint.text`
  }

  /**
   * Load the current table metadata for a source
   */
  private async loadTableMetadata(source: string): Promise<IcebergMetadataSnapshot | null> {
    try {
      // Read version hint to get current metadata file
      const versionHintPath = this.getVersionHintPath(source)
      const versionHintData = await this.storage.read(versionHintPath)
      const metadataPath = new TextDecoder().decode(versionHintData).trim()

      // Read metadata file
      const metadataData = await this.storage.read(metadataPath)
      const metadataJson = new TextDecoder().decode(metadataData)
      return JSON.parse(metadataJson) as IcebergMetadataSnapshot
    } catch (error) {
      if (isNotFoundError(error)) {
        return null
      }
      throw error
    }
  }

  /**
   * Get the current version of a source collection.
   *
   * For Iceberg, version is the current snapshot ID.
   */
  async getCurrentVersion(source: string): Promise<SourceVersion | null> {
    const metadata = await this.loadTableMetadata(source)
    if (!metadata) {
      return null
    }

    const currentSnapshotId = metadata['current-snapshot-id']
    if (currentSnapshotId === undefined || currentSnapshotId === null) {
      // Table exists but has no snapshots yet
      return {
        versionId: 'empty',
        timestamp: metadata['last-updated-ms'] ?? Date.now(),
        backend: 'iceberg',
      }
    }

    // Find the current snapshot
    const currentSnapshot = metadata.snapshots?.find(
      s => s['snapshot-id'] === currentSnapshotId
    )

    return {
      versionId: String(currentSnapshotId),
      timestamp: currentSnapshot?.['timestamp-ms'] ?? metadata['last-updated-ms'] ?? Date.now(),
      backend: 'iceberg',
    }
  }

  /**
   * Get the number of changes between two versions.
   *
   * For Iceberg, we estimate based on snapshot summaries if available.
   */
  async getChangeCount(
    source: string,
    fromVersion: SourceVersion,
    toVersion: SourceVersion
  ): Promise<number> {
    const metadata = await this.loadTableMetadata(source)
    if (!metadata || !metadata.snapshots) {
      return 0
    }

    const fromSnapshotId = this.parseSnapshotId(fromVersion.versionId)
    const toSnapshotId = this.parseSnapshotId(toVersion.versionId)

    if (fromSnapshotId === null || toSnapshotId === null) {
      return 0
    }

    // Same version means no changes
    if (fromSnapshotId === toSnapshotId) {
      return 0
    }

    // Sum added/deleted records from snapshots between fromSnapshot and toSnapshot
    let changeCount = 0
    let inRange = false

    // Snapshots are ordered oldest to newest
    for (const snapshot of metadata.snapshots) {
      if (snapshot['snapshot-id'] === fromSnapshotId) {
        inRange = true
        continue // Start counting after fromSnapshot
      }

      if (inRange) {
        const summary = snapshot.summary as Record<string, string> | undefined
        const addedRecords = parseInt(summary?.['added-records'] ?? '0', 10)
        const deletedRecords = parseInt(summary?.['deleted-records'] ?? '0', 10)
        changeCount += addedRecords + deletedRecords

        if (snapshot['snapshot-id'] === toSnapshotId) {
          break
        }
      }
    }

    return changeCount
  }

  /**
   * Check if a version is still valid (snapshot hasn't been expired).
   */
  async isVersionValid(source: string, version: SourceVersion): Promise<boolean> {
    const metadata = await this.loadTableMetadata(source)
    if (!metadata) {
      return false
    }

    const snapshotId = this.parseSnapshotId(version.versionId)
    if (snapshotId === null) {
      return version.versionId === 'empty' // Empty table is valid
    }

    // Check if snapshot still exists in metadata
    return metadata.snapshots?.some(s => s['snapshot-id'] === snapshotId) ?? false
  }

  /**
   * Parse snapshot ID from version ID
   */
  private parseSnapshotId(versionId: string): number | null {
    if (versionId === 'empty') {
      return null
    }
    const id = parseInt(versionId, 10)
    return isNaN(id) ? null : id
  }
}

/**
 * Minimal Iceberg metadata structure for version provider
 */
interface IcebergMetadataSnapshot {
  'current-snapshot-id'?: number | null | undefined
  'last-updated-ms'?: number | undefined
  'last-sequence-number'?: number | undefined
  snapshots?: Array<{
    'snapshot-id': number
    'timestamp-ms': number
    summary?: Record<string, unknown> | undefined
  }> | undefined
}

// =============================================================================
// Delta Source Version Provider
// =============================================================================

/**
 * Configuration for the DeltaSourceVersionProvider
 */
export interface DeltaVersionProviderConfig {
  /** Storage backend to read Delta log from */
  storage: StorageBackend
  /** Table location path */
  location: string
}

/**
 * Source version provider that uses Delta Lake transaction log versions.
 *
 * Reads the Delta transaction log to get the current version number for
 * each source collection. The version ID is the transaction version.
 *
 * @example
 * ```typescript
 * const provider = new DeltaSourceVersionProvider({
 *   storage: r2Backend,
 *   location: 'warehouse',
 * })
 *
 * const version = await provider.getCurrentVersion('orders')
 * // { versionId: 'txn-15', timestamp: 1704067200000, backend: 'delta' }
 * ```
 */
export class DeltaSourceVersionProvider implements SourceVersionProvider {
  private storage: StorageBackend
  private location: string

  constructor(config: DeltaVersionProviderConfig) {
    this.storage = config.storage
    this.location = config.location
  }

  /**
   * Get the table location for a source namespace
   */
  private getTableLocation(source: string): string {
    return this.location ? `${this.location}/${source}` : source
  }

  /**
   * Get the Delta log directory path
   */
  private getDeltaLogPath(source: string): string {
    return `${this.getTableLocation(source)}/_delta_log/`
  }

  /**
   * Format version number as zero-padded 20-digit string
   */
  private formatVersion(version: number): string {
    return version.toString().padStart(20, '0')
  }

  /**
   * Get the current version (highest commit number) from the transaction log
   */
  private async getCurrentVersionNumber(source: string): Promise<number> {
    const logPath = this.getDeltaLogPath(source)

    // Check for _last_checkpoint first
    try {
      const checkpointData = await this.storage.read(`${logPath}_last_checkpoint`)
      const checkpoint = JSON.parse(new TextDecoder().decode(checkpointData)) as {
        version: number
      }

      // Look for commits after checkpoint
      let version = checkpoint.version
      while (true) {
        const nextCommit = `${logPath}${this.formatVersion(version + 1)}.json`
        const exists = await this.storage.exists(nextCommit)
        if (!exists) break
        version++
      }
      return version
    } catch (error) {
      if (!isNotFoundError(error)) {
        throw error
      }
    }

    // No checkpoint, scan commit files
    try {
      const logFiles = await this.storage.list(logPath)
      const commitFiles = logFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
        .sort()

      if (commitFiles.length === 0) {
        return -1 // Table doesn't exist
      }

      const lastCommit = commitFiles[commitFiles.length - 1]!
      const versionStr = lastCommit.split('/').pop()?.replace('.json', '')
      return parseInt(versionStr ?? '-1', 10)
    } catch (error) {
      if (isNotFoundError(error)) {
        return -1
      }
      throw error
    }
  }

  /**
   * Load commit info from a specific version
   */
  private async loadCommitInfo(source: string, version: number): Promise<DeltaCommitInfo | null> {
    const logPath = this.getDeltaLogPath(source)
    const commitPath = `${logPath}${this.formatVersion(version)}.json`

    try {
      const commitData = await this.storage.read(commitPath)
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')

      for (const line of lines) {
        try {
          const action = JSON.parse(line) as { commitInfo?: DeltaCommitInfo | undefined }
          if (action.commitInfo) {
            return action.commitInfo
          }
        } catch {
          continue
        }
      }
      return null
    } catch (error) {
      if (isNotFoundError(error)) {
        return null
      }
      throw error
    }
  }

  /**
   * Get the current version of a source collection.
   *
   * For Delta, version is the current transaction version.
   */
  async getCurrentVersion(source: string): Promise<SourceVersion | null> {
    const version = await this.getCurrentVersionNumber(source)
    if (version < 0) {
      return null
    }

    const commitInfo = await this.loadCommitInfo(source, version)

    return {
      versionId: `txn-${version}`,
      timestamp: commitInfo?.timestamp ?? Date.now(),
      backend: 'delta',
    }
  }

  /**
   * Get the number of changes between two versions.
   *
   * For Delta, we sum operation metrics from commits between the two versions.
   */
  async getChangeCount(
    source: string,
    fromVersion: SourceVersion,
    toVersion: SourceVersion
  ): Promise<number> {
    const fromTxn = this.parseTransactionVersion(fromVersion.versionId)
    const toTxn = this.parseTransactionVersion(toVersion.versionId)

    if (fromTxn === null || toTxn === null) {
      return 0
    }

    let changeCount = 0

    // Read each commit between fromTxn and toTxn
    for (let v = fromTxn + 1; v <= toTxn; v++) {
      const commitInfo = await this.loadCommitInfo(source, v)
      if (commitInfo?.operationMetrics) {
        const numOutputRows = parseInt(commitInfo.operationMetrics['numOutputRows'] ?? '0', 10)
        const numAddedFiles = parseInt(commitInfo.operationMetrics['numAddedFiles'] ?? '0', 10)
        const numRemovedFiles = parseInt(commitInfo.operationMetrics['numRemovedFiles'] ?? '0', 10)
        // Use output rows if available, otherwise estimate from file counts
        changeCount += numOutputRows || (numAddedFiles + numRemovedFiles) * 1000
      } else {
        // No metrics, estimate 1 change per commit
        changeCount += 1
      }
    }

    return changeCount
  }

  /**
   * Check if a version is still valid (commit file exists and hasn't been vacuumed).
   */
  async isVersionValid(source: string, version: SourceVersion): Promise<boolean> {
    const txn = this.parseTransactionVersion(version.versionId)
    if (txn === null) {
      return false
    }

    const logPath = this.getDeltaLogPath(source)
    const commitPath = `${logPath}${this.formatVersion(txn)}.json`

    return this.storage.exists(commitPath)
  }

  /**
   * Parse transaction version from version ID (e.g., "txn-15" -> 15)
   */
  private parseTransactionVersion(versionId: string): number | null {
    const match = versionId.match(/^txn-(\d+)$/)
    if (!match) {
      return null
    }
    return parseInt(match[1]!, 10)
  }
}

/**
 * Delta commit info structure
 */
interface DeltaCommitInfo {
  timestamp: number
  operation: string
  operationMetrics?: Record<string, string> | undefined
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a NativeSourceVersionProvider instance
 */
export function createNativeVersionProvider(
  config: NativeVersionProviderConfig
): NativeSourceVersionProvider {
  return new NativeSourceVersionProvider(config)
}

/**
 * Create an IcebergSourceVersionProvider instance
 */
export function createIcebergVersionProvider(
  config: IcebergVersionProviderConfig
): IcebergSourceVersionProvider {
  return new IcebergSourceVersionProvider(config)
}

/**
 * Create a DeltaSourceVersionProvider instance
 */
export function createDeltaVersionProvider(
  config: DeltaVersionProviderConfig
): DeltaSourceVersionProvider {
  return new DeltaSourceVersionProvider(config)
}
