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

  // Capability types
  EntityBackendCapabilities,
} from './types'

// =============================================================================
// Capability Introspection
// =============================================================================

export {
  getEntityBackendCapabilities,
  hasEntityBackendCapability,
  isCompatibleWithEngine,
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
// Delta Lake Backend
// =============================================================================

export {
  DeltaBackend,
  createDeltaBackend,
} from './delta'

// =============================================================================
// Shared Parquet Utilities
// =============================================================================

export {
  // Entity serialization
  entityToRow,
  rowToEntity,
  buildEntityParquetSchema,
  // Filter matching
  matchesFilter,
  // ID generation
  generateEntityId,
  // Data extraction
  extractDataFields,
  // Binary encoding helpers
  bytesToBase64,
  base64ToBytes,
} from './parquet-utils'

// =============================================================================
// Factory Function
// =============================================================================

import type { BackendConfig, EntityBackend } from './types'
import { IcebergBackend } from './iceberg'
import { DeltaBackend } from './delta'

/**
 * Create an entity backend from configuration
 */
export async function createBackend(config: BackendConfig): Promise<EntityBackend> {
  let backend: EntityBackend

  switch (config.type) {
    case 'iceberg':
      backend = new IcebergBackend(config)
      break

    case 'delta':
      backend = new DeltaBackend(config)
      break

    case 'native':
      // TODO: Implement NativeBackend
      throw new Error('Native backend not yet implemented')

    default:
      throw new Error(`Unknown backend type: ${(config as BackendConfig).type}`)
  }

  await backend.initialize()
  return backend
}
