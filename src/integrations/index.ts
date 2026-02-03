/**
 * Integrations Module
 *
 * Provides adapters and connectors for external systems.
 *
 * ## Apache Iceberg Integration
 *
 * ParqueDB supports Apache Iceberg metadata for interoperability with
 * query engines like DuckDB, Spark, and Trino.
 *
 * ### Basic Integration (iceberg.ts)
 * - Simplified Iceberg-compatible metadata
 * - Works without @dotdo/iceberg dependency
 * - Good for basic time-travel and query engine compatibility
 *
 * ### Native Integration (iceberg-native.ts)
 * - Full Apache Iceberg specification compliance
 * - Requires @dotdo/iceberg package
 * - Atomic commits with conflict resolution
 * - Schema evolution with field ID tracking
 * - Bloom filters and column statistics
 *
 * @example
 * ```typescript
 * // Basic integration
 * import { enableIcebergMetadata } from 'parquedb/integrations'
 * const iceberg = await enableIcebergMetadata(db, 'posts', {
 *   location: './warehouse/posts',
 * })
 *
 * // Native integration (requires @dotdo/iceberg)
 * import { enableNativeIcebergMetadata } from 'parquedb/integrations'
 * const iceberg = await enableNativeIcebergMetadata(storage, 'posts', {
 *   location: './warehouse/posts',
 *   enableBloomFilters: true,
 * })
 * ```
 */

// Apache Iceberg integration (basic)
export {
  IcebergMetadataManager,
  IcebergStorageAdapter,
  createIcebergMetadataManager,
  enableIcebergMetadata,
  parqueDBTypeToIceberg,
  icebergTypeToParqueDB,
  type IcebergMetadataOptions,
  type IcebergSnapshotRef,
  type IcebergDataFile,
  type IcebergSchema,
  type IcebergField,
  type IcebergType,
  type IcebergCommitResult,
} from './iceberg'

// Apache Iceberg integration (native, using @dotdo/iceberg)
export {
  NativeIcebergMetadataManager,
  NativeIcebergStorageAdapter,
  createNativeIcebergManager,
  enableNativeIcebergMetadata,
  type NativeIcebergOptions,
  type IcebergNativeSchema,
  type PartitionSpecDefinition,
  type SortOrderDefinition,
  type NativeDataFile,
  type NativeCommitResult,
} from './iceberg-native'

// Payload CMS integration
export {
  parquedbAdapter,
  PayloadAdapter,
  translatePayloadFilter,
  translatePayloadSort,
  toPayloadDoc,
  toPayloadDocs,
  type PayloadAdapterConfig,
} from './payload'

// SQL integration (sql``, Drizzle, Prisma)
export {
  // SQL Template Tag
  createSQL,
  buildQuery,
  escapeIdentifier,
  escapeString,
  type SQLExecutor,
  type CreateSQLOptions,

  // Drizzle ORM Adapter
  createDrizzleProxy,
  getTableName,
  type DrizzleProxyOptions,

  // Prisma Driver Adapter
  PrismaParqueDBAdapter,
  createPrismaAdapter,
  type PrismaAdapterOptions,

  // Parser & Translator (advanced)
  parseSQL,
  translateSelect,
  translateInsert,
  translateUpdate,
  translateDelete,
  translateStatement,
  translateWhere,
  whereToFilter,

  // Types
  type SQLStatement,
  type SQLQueryOptions,
  type SQLQueryResult,
  type DrizzleProxyCallback,
  type PrismaDriverAdapter,
} from './sql'
