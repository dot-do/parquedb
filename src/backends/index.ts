/**
 * Pluggable Entity Backends for ParqueDB
 *
 * Supports three table formats for entity storage:
 * - Native: ParqueDB's simple Parquet format
 * - Iceberg: Apache Iceberg format (DuckDB, Spark, Snowflake compatible)
 * - Delta Lake: Delta Lake format
 *
 * Relationships are always stored in ParqueDB's format regardless of backend.
 *
 * @example
 * ```typescript
 * import { createIcebergBackend, createR2IcebergBackend } from 'parquedb/backends'
 *
 * // Iceberg with filesystem catalog
 * const backend = createIcebergBackend({
 *   type: 'iceberg',
 *   storage: r2Backend,
 *   warehouse: 'warehouse',
 * })
 *
 * // Iceberg with R2 Data Catalog (managed by Cloudflare)
 * const r2Backend = createR2IcebergBackend(storage, {
 *   accountId: env.CF_ACCOUNT_ID,
 *   apiToken: env.R2_DATA_CATALOG_TOKEN,
 *   warehouse: 'warehouse',
 * })
 * ```
 */

// =============================================================================
// Types
// =============================================================================

export type {
  // Core interface
  EntityBackend,
  BackendType,

  // Configuration types
  BackendConfig,
  BaseBackendConfig,
  NativeBackendConfig,
  IcebergBackendConfig,
  IcebergCatalogConfig,
  DeltaBackendConfig,

  // Schema types
  EntitySchema,
  SchemaField,
  SchemaFieldType,

  // Result types
  SnapshotInfo,
  CompactOptions,
  CompactResult,
  VacuumOptions,
  VacuumResult,
  BackendStats,

  // Factory type
  CreateBackendFn,
} from './types'

// =============================================================================
// Iceberg Backend
// =============================================================================

export {
  IcebergBackend,
  createIcebergBackend,
  createR2IcebergBackend,
} from './iceberg'

// =============================================================================
// Factory Function
// =============================================================================

import type { BackendConfig, EntityBackend } from './types'
import { IcebergBackend } from './iceberg'

/**
 * Create an entity backend from configuration
 */
export async function createBackend(config: BackendConfig): Promise<EntityBackend> {
  let backend: EntityBackend

  switch (config.type) {
    case 'iceberg':
      backend = new IcebergBackend(config)
      break

    case 'native':
      // TODO: Implement NativeBackend
      throw new Error('Native backend not yet implemented')

    case 'delta':
      // TODO: Implement DeltaLakeBackend
      throw new Error('Delta Lake backend not yet implemented')

    default:
      throw new Error(`Unknown backend type: ${(config as BackendConfig).type}`)
  }

  await backend.initialize()
  return backend
}
