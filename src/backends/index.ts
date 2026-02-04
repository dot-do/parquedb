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
// Error Classes
// =============================================================================

export {
  CommitConflictError,
  ReadOnlyError,
  BackendEntityNotFoundError,
  TableNotFoundError,
  SnapshotNotFoundError,
  InvalidNamespaceError,
  SchemaNotFoundError,
  WriteLockTimeoutError,
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
// Commit Utilities (for external use)
// =============================================================================

export {
  // Iceberg commit utilities
  IcebergCommitter,
  createIcebergCommitter,
  commitToIcebergTable,
  type IcebergCommitConfig,
  type DataFileInfo as IcebergDataFileInfo,
  type IcebergCommitResult,
} from './iceberg-commit'

export {
  // Delta commit utilities
  DeltaCommitter,
  createDeltaCommitter,
  commitToDeltaTable,
  type DeltaCommitConfig,
  type DeltaDataFileInfo,
  type DeltaCommitResult,
} from './delta-commit'

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
// Shared Entity Utilities
// =============================================================================

export {
  // Update operations
  applyUpdate,
  // Default entity creation
  createDefaultEntity,
  // Sorting utilities
  compareValues,
  sortEntities,
  sortEntitiesImmutable,
  // Pagination utilities
  applyPagination,
  applyPaginationFromOptions,
} from './entity-utils'

// =============================================================================
// Factory Function
// =============================================================================

import type { BackendConfig, EntityBackend } from './types'
import { IcebergBackend } from './iceberg'
import { DeltaBackend } from './delta'

// =============================================================================
// Backend Migration
// =============================================================================

export {
  // Migration functions
  migrateBackend,
  createBackendWithMigration,
  // Detection utilities
  detectExistingFormat,
  discoverNamespaces,
  // Types
  type MigrationConfig,
  type MigrationProgress,
  type MigrationResult,
  type BackendConfigWithMigration,
} from './migration'

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create an entity backend from configuration
 *
 * For automatic migration when switching backends, use `createBackendWithMigration` instead.
 *
 * @example
 * ```typescript
 * // Simple backend creation (no migration)
 * const backend = await createBackend({
 *   type: 'iceberg',
 *   storage,
 *   warehouse: 'warehouse',
 * })
 *
 * // With automatic migration from native
 * const backend = await createBackendWithMigration({
 *   type: 'iceberg',
 *   storage,
 *   warehouse: 'warehouse',
 *   migrateFrom: 'auto', // Auto-detect and migrate existing data
 * })
 * ```
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
      // NativeBackend is intentionally not implemented - use Iceberg or Delta instead
      throw new Error('Native backend not supported as EntityBackend - use createBackendWithMigration to auto-migrate to iceberg or delta')

    default: {
      const _exhaustive: never = config
      throw new Error(`Unknown backend type: ${(_exhaustive as { type: string }).type}`)
    }
  }

  await backend.initialize()
  return backend
}
