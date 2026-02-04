/**
 * Backend Migration Utility
 *
 * Automatically migrates data when switching between backend formats:
 * - Native Parquet → Iceberg
 * - Native Parquet → Delta Lake
 * - Iceberg → Delta Lake
 * - Delta Lake → Iceberg
 *
 * This ensures seamless backend switching without data loss.
 *
 * @example
 * ```typescript
 * // Automatic migration when creating backend
 * const backend = await createBackendWithMigration({
 *   type: 'iceberg',
 *   storage,
 *   migrateFrom: 'native', // Will auto-migrate native data to Iceberg
 * })
 *
 * // Manual migration
 * await migrateBackend({
 *   storage,
 *   from: 'native',
 *   to: 'iceberg',
 *   namespaces: ['users', 'posts'],
 * })
 * ```
 */

import { parquetQuery } from 'hyparquet'
import { compressors } from '../parquet/compressors'
import type { StorageBackend } from '../types/storage'
import type { Entity } from '../types/entity'
import type {
  BackendType,
  EntityBackend,
  IcebergBackendConfig,
  DeltaBackendConfig,
  NativeBackendConfig,
} from './types'
import { IcebergBackend } from './iceberg'
import { DeltaBackend } from './delta'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

export interface MigrationConfig {
  /** Storage backend (R2, S3, filesystem) */
  storage: StorageBackend
  /** Source format */
  from: BackendType | 'auto'
  /** Target format */
  to: BackendType
  /** Namespaces to migrate (or '*' for all) */
  namespaces?: string[] | '*' | undefined
  /** Batch size for migration */
  batchSize?: number | undefined
  /** Delete source data after successful migration */
  deleteSource?: boolean | undefined
  /** Progress callback */
  onProgress?: ((progress: MigrationProgress) => void) | undefined
}

export interface MigrationProgress {
  namespace: string
  entitiesMigrated: number
  totalEntities: number
  bytesWritten: number
  phase: 'scanning' | 'migrating' | 'verifying' | 'cleanup' | 'complete'
}

export interface MigrationResult {
  success: boolean
  namespacesProcessed: string[]
  entitiesMigrated: number
  bytesWritten: number
  durationMs: number
  errors: string[]
}

/** Migration-enabled backend configuration */
export type BackendConfigWithMigration =
  | (IcebergBackendConfig & { migrateFrom?: BackendType | 'auto' | undefined })
  | (DeltaBackendConfig & { migrateFrom?: BackendType | 'auto' | undefined })
  | (NativeBackendConfig & { migrateFrom?: BackendType | 'auto' | undefined })

// =============================================================================
// Format Detection
// =============================================================================

/**
 * Detect what format(s) exist in storage for a namespace
 */
export async function detectExistingFormat(
  storage: StorageBackend,
  namespace: string
): Promise<{ formats: BackendType[]; primary: BackendType | null }> {
  const formats: BackendType[] = []

  // Check for native Parquet (data/{ns}/data.parquet or {ns}.parquet)
  const nativePaths = [
    `data/${namespace}/data.parquet`,
    `${namespace}.parquet`,
    `${namespace}/data.parquet`,
  ]

  for (const path of nativePaths) {
    if (await storage.exists(path)) {
      formats.push('native')
      break
    }
  }

  // Check for Iceberg (metadata/v*.metadata.json)
  const icebergPaths = [
    `${namespace}/metadata/v1.metadata.json`,
    `iceberg/${namespace}/metadata/v1.metadata.json`,
  ]

  for (const path of icebergPaths) {
    if (await storage.exists(path)) {
      formats.push('iceberg')
      break
    }
  }

  // Check for Delta (_delta_log/)
  const deltaPaths = [
    `${namespace}/_delta_log/00000000000000000000.json`,
    `delta/${namespace}/_delta_log/00000000000000000000.json`,
  ]

  for (const path of deltaPaths) {
    if (await storage.exists(path)) {
      formats.push('delta')
      break
    }
  }

  // Determine primary format (newest or most complete)
  let primary: BackendType | null = null
  if (formats.length === 1) {
    primary = formats[0] ?? null
  } else if (formats.length > 1) {
    // Prefer table formats over native
    if (formats.includes('iceberg')) primary = 'iceberg'
    else if (formats.includes('delta')) primary = 'delta'
    else primary = 'native'
  }

  return { formats, primary }
}

/**
 * Discover all namespaces in storage
 */
export async function discoverNamespaces(
  storage: StorageBackend
): Promise<string[]> {
  const namespaces = new Set<string>()

  // List data/ directory for native format
  try {
    const dataResult = await storage.list('data/')
    for (const file of dataResult.files) {
      // Extract namespace from data/{ns}/data.parquet
      const match = file.match(/^data\/([^/]+)\//)
      if (match?.[1]) {
        namespaces.add(match[1])
      }
    }
  } catch {
    // Ignore if data/ doesn't exist
  }

  // List root for {ns}.parquet or {ns}/ directories
  try {
    const rootResult = await storage.list('')
    for (const file of rootResult.files) {
      // Look for metadata.json (Iceberg) or _delta_log (Delta)
      const icebergMatch = file.match(/^([^/]+)\/metadata\//)
      const deltaMatch = file.match(/^([^/]+)\/_delta_log\//)
      const nativeMatch = file.match(/^([^/]+)\.parquet$/)

      if (icebergMatch?.[1]) namespaces.add(icebergMatch[1])
      if (deltaMatch?.[1]) namespaces.add(deltaMatch[1])
      if (nativeMatch?.[1]) namespaces.add(nativeMatch[1])
    }
  } catch {
    // Ignore errors
  }

  return Array.from(namespaces).filter(ns => !ns.startsWith('_') && !ns.startsWith('.'))
}

// =============================================================================
// Native Parquet Reader
// =============================================================================

/**
 * Read all entities from native Parquet format
 */
async function readNativeEntities(
  storage: StorageBackend,
  namespace: string
): Promise<Entity[]> {
  const paths = [
    `data/${namespace}/data.parquet`,
    `${namespace}.parquet`,
    `${namespace}/data.parquet`,
  ]

  for (const path of paths) {
    if (await storage.exists(path)) {
      const data = await storage.read(path)

      // Create file-like object for hyparquet
      const file = {
        byteLength: data.byteLength,
        slice: async (start: number, end: number) => data.slice(start, end).buffer,
      }

      const rows = await parquetQuery({
        file,
        compressors,
      })

      // Convert rows to entities
      return rows.map((row: Record<string, unknown>) => {
        // If row has $data field with JSON, parse it
        if (typeof row.$data === 'string') {
          try {
            return JSON.parse(row.$data) as Entity
          } catch {
            return row as Entity
          }
        }
        return row as Entity
      })
    }
  }

  return []
}

// =============================================================================
// Source Deletion
// =============================================================================

/**
 * Delete source data after successful migration
 */
async function deleteSourceData(
  storage: StorageBackend,
  format: BackendType,
  namespace: string
): Promise<void> {
  if (format === 'native') {
    // Delete native Parquet files
    const nativePaths = [
      `data/${namespace}/data.parquet`,
      `${namespace}.parquet`,
      `${namespace}/data.parquet`,
    ]
    for (const path of nativePaths) {
      if (await storage.exists(path)) {
        await storage.delete(path)
        logger.info(`Deleted native source file: ${path}`)
      }
    }
    // Try to delete the namespace directory if empty
    try {
      const dirResult = await storage.list(`data/${namespace}/`)
      if (dirResult.files.length === 0) {
        // Directory is empty, safe to leave (storage backends typically don't have explicit directory deletion)
      }
    } catch {
      // Ignore - directory may not exist
    }
  } else if (format === 'iceberg') {
    // Delete Iceberg files (metadata + data)
    const icebergPrefixes = [
      `${namespace}/metadata/`,
      `${namespace}/data/`,
      `iceberg/${namespace}/metadata/`,
      `iceberg/${namespace}/data/`,
    ]
    for (const prefix of icebergPrefixes) {
      try {
        const result = await storage.list(prefix)
        for (const file of result.files) {
          await storage.delete(file)
        }
        if (result.files.length > 0) {
          logger.info(`Deleted ${result.files.length} Iceberg files from ${prefix}`)
        }
      } catch {
        // Ignore - prefix may not exist
      }
    }
  } else if (format === 'delta') {
    // Delete Delta Lake files (_delta_log + parquet files)
    const deltaPrefixes = [
      `${namespace}/_delta_log/`,
      `${namespace}/`,
      `delta/${namespace}/_delta_log/`,
      `delta/${namespace}/`,
    ]
    for (const prefix of deltaPrefixes) {
      try {
        const result = await storage.list(prefix)
        for (const file of result.files) {
          await storage.delete(file)
        }
        if (result.files.length > 0) {
          logger.info(`Deleted ${result.files.length} Delta files from ${prefix}`)
        }
      } catch {
        // Ignore - prefix may not exist
      }
    }
  }
}

// =============================================================================
// Migration Functions
// =============================================================================

/**
 * Migrate a single namespace from one format to another
 */
async function migrateNamespace(
  storage: StorageBackend,
  namespace: string,
  from: BackendType,
  to: BackendType,
  config: MigrationConfig
): Promise<{ entitiesMigrated: number; bytesWritten: number; error?: string | undefined }> {
  const batchSize = config.batchSize ?? 1000
  let entitiesMigrated = 0
  let bytesWritten = 0

  try {
    // Read entities from source
    config.onProgress?.({
      namespace,
      entitiesMigrated: 0,
      totalEntities: 0,
      bytesWritten: 0,
      phase: 'scanning',
    })

    let entities: Entity[] = []

    if (from === 'native') {
      entities = await readNativeEntities(storage, namespace)
    } else if (from === 'iceberg') {
      // Read from Iceberg using IcebergBackend
      const icebergBackend = new IcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: '',
      })
      await icebergBackend.initialize()
      entities = await icebergBackend.find(namespace)
      await icebergBackend.close()
    } else if (from === 'delta') {
      // Read from Delta using DeltaBackend
      const deltaBackend = new DeltaBackend({
        type: 'delta',
        storage,
      })
      await deltaBackend.initialize()
      entities = await deltaBackend.find(namespace)
      await deltaBackend.close()
    }

    const totalEntities = entities.length
    logger.info(`Migrating ${totalEntities} entities from ${from} to ${to} for namespace ${namespace}`)

    if (totalEntities === 0) {
      return { entitiesMigrated: 0, bytesWritten: 0 }
    }

    // Write to target format in batches
    config.onProgress?.({
      namespace,
      entitiesMigrated: 0,
      totalEntities,
      bytesWritten: 0,
      phase: 'migrating',
    })

    if (to === 'iceberg') {
      const icebergBackend = new IcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: '',
      })
      await icebergBackend.initialize()

      for (let i = 0; i < entities.length; i += batchSize) {
        const batch = entities.slice(i, i + batchSize)
        for (const entity of batch) {
          await icebergBackend.create(namespace, entity)
          entitiesMigrated++
        }

        config.onProgress?.({
          namespace,
          entitiesMigrated,
          totalEntities,
          bytesWritten,
          phase: 'migrating',
        })
      }

      await icebergBackend.close()
    } else if (to === 'delta') {
      const deltaBackend = new DeltaBackend({
        type: 'delta',
        storage,
      })
      await deltaBackend.initialize()

      for (let i = 0; i < entities.length; i += batchSize) {
        const batch = entities.slice(i, i + batchSize)
        for (const entity of batch) {
          await deltaBackend.create(namespace, entity)
          entitiesMigrated++
        }

        config.onProgress?.({
          namespace,
          entitiesMigrated,
          totalEntities,
          bytesWritten,
          phase: 'migrating',
        })
      }

      await deltaBackend.close()
    }

    // Verification phase
    config.onProgress?.({
      namespace,
      entitiesMigrated,
      totalEntities,
      bytesWritten,
      phase: 'verifying',
    })

    // Optional: Delete source data
    if (config.deleteSource) {
      config.onProgress?.({
        namespace,
        entitiesMigrated,
        totalEntities,
        bytesWritten,
        phase: 'cleanup',
      })
      await deleteSourceData(storage, from, namespace)
    }

    config.onProgress?.({
      namespace,
      entitiesMigrated,
      totalEntities,
      bytesWritten,
      phase: 'complete',
    })

    return { entitiesMigrated, bytesWritten }
  } catch (err) {
    const error = err instanceof Error ? err.message : 'Unknown error'
    logger.error(`Migration failed for ${namespace}`, { error })
    return { entitiesMigrated, bytesWritten, error }
  }
}

/**
 * Migrate data between backend formats
 */
export async function migrateBackend(config: MigrationConfig): Promise<MigrationResult> {
  const startTime = performance.now()
  const errors: string[] = []
  let totalEntities = 0
  let totalBytes = 0
  const processedNamespaces: string[] = []

  // Determine namespaces to migrate
  let namespacesToMigrate: string[]

  if (config.namespaces === '*' || !config.namespaces) {
    namespacesToMigrate = await discoverNamespaces(config.storage)
  } else {
    namespacesToMigrate = config.namespaces
  }

  logger.info(`Starting migration: ${config.from} → ${config.to}`, {
    namespaces: namespacesToMigrate,
  })

  // Migrate each namespace
  for (const namespace of namespacesToMigrate) {
    // Auto-detect source format if needed
    let fromFormat = config.from
    if (fromFormat === 'auto') {
      const { primary } = await detectExistingFormat(config.storage, namespace)
      if (!primary) {
        logger.warn(`No data found for namespace ${namespace}, skipping`)
        continue
      }
      fromFormat = primary
    }

    // Skip if source and target are the same
    if (fromFormat === config.to) {
      logger.info(`Namespace ${namespace} already in ${config.to} format, skipping`)
      processedNamespaces.push(namespace)
      continue
    }

    const result = await migrateNamespace(
      config.storage,
      namespace,
      fromFormat,
      config.to,
      config
    )

    totalEntities += result.entitiesMigrated
    totalBytes += result.bytesWritten
    processedNamespaces.push(namespace)

    if (result.error) {
      errors.push(`${namespace}: ${result.error}`)
    }
  }

  return {
    success: errors.length === 0,
    namespacesProcessed: processedNamespaces,
    entitiesMigrated: totalEntities,
    bytesWritten: totalBytes,
    durationMs: Math.round(performance.now() - startTime),
    errors,
  }
}

// =============================================================================
// Backend Factory with Auto-Migration
// =============================================================================

/**
 * Create a backend with automatic migration support
 *
 * If data exists in a different format, it will be automatically migrated.
 */
export async function createBackendWithMigration(
  config: BackendConfigWithMigration
): Promise<EntityBackend> {
  // Check if migration is requested
  if (config.migrateFrom) {
    // Discover namespaces
    const namespaces = await discoverNamespaces(config.storage)

    for (const namespace of namespaces) {
      const { primary } = await detectExistingFormat(config.storage, namespace)

      // If data exists in a different format, migrate it
      if (primary && primary !== config.type) {
        const shouldMigrate = config.migrateFrom === 'auto' || config.migrateFrom === primary

        if (shouldMigrate) {
          logger.info(`Auto-migrating ${namespace} from ${primary} to ${config.type}`)

          await migrateBackend({
            storage: config.storage,
            from: primary,
            to: config.type,
            namespaces: [namespace],
          })
        }
      }
    }
  }

  // Create the target backend
  let backend: EntityBackend

  switch (config.type) {
    case 'iceberg':
      backend = new IcebergBackend(config)
      break

    case 'delta':
      backend = new DeltaBackend(config)
      break

    case 'native':
      // For native, we just use the existing Parquet files directly
      // NativeBackend is intentionally not implemented - use Iceberg or Delta instead
      // See parquedb-08w1 for historical context on this decision
      throw new Error('Native backend not supported as EntityBackend - use createBackendWithMigration to auto-migrate to iceberg or delta')

    default:
      throw new Error(`Unknown backend type: ${(config as BackendConfigWithMigration).type}`)
  }

  await backend.initialize()
  return backend
}

// =============================================================================
// All public functions are exported via 'export' keyword at definition
// =============================================================================
