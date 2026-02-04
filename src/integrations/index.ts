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

// MCP (Model Context Protocol) integration for AI agents
export {
  createParqueDBMCPServer,
  type ParqueDBMCPOptions,
  type ToolResult,
  type CollectionInfo,
  type FindToolParams,
  type GetToolParams,
  type CreateToolParams,
  type UpdateToolParams,
  type DeleteToolParams,
  type CountToolParams,
  type AggregateToolParams,
  type SemanticSearchToolParams,
} from './mcp'

// Evalite (AI evaluation framework) integration
export {
  ParqueDBEvaliteAdapter,
  createEvaliteAdapter,
  type EvaliteAdapterConfig,
  type ResolvedEvaliteConfig,
  type RunType,
  type EvalRun,
  type CreateRunOptions,
  type GetRunsOptions,
  type SuiteStatus,
  type EvalSuite,
  type CreateSuiteOptions,
  type UpdateSuiteOptions,
  type GetSuitesOptions,
  type EvalStatus,
  type EvalResult,
  type CreateEvalOptions,
  type UpdateEvalOptions,
  type GetEvalsOptions,
  type EvalScore,
  type CreateScoreOptions,
  type GetScoresOptions,
  type EvalTrace,
  type CreateTraceOptions,
  type GetTracesOptions,
  type ScoreHistoryOptions,
  type ScorePoint,
  type RunStats,
  type RunWithResults,
  type EvalWithDetails,
} from './evalite'

// ai-database integration (DBProvider/DBProviderExtended)
export {
  ParqueDBAdapter,
  createParqueDBProvider,
  type DBProvider,
  type DBProviderExtended,
  type Transaction as AIDBTransaction,
  type DBEvent as AIDBEvent,
  type DBAction as AIDBAction,
  type DBArtifact as AIDBartifact,
  type ListOptions as AIDBListOptions,
  type SearchOptions as AIDBSearchOptions,
  type SemanticSearchOptions as AIDBSemanticSearchOptions,
  type HybridSearchOptions as AIDBHybridSearchOptions,
  type SemanticSearchResult as AIDBSemanticSearchResult,
  type HybridSearchResult as AIDBHybridSearchResult,
  type RelationMetadata as AIDBRelationMetadata,
  type CreateEventOptions as AIDBCreateEventOptions,
  type CreateActionOptions as AIDBCreateActionOptions,
  type EmbeddingsConfig as AIDBEmbeddingsConfig,
} from './ai-database'

// Vercel AI SDK middleware (caching/logging)
export {
  createParqueDBMiddleware,
  hashParams,
  isExpired,
  queryCacheEntries,
  queryLogEntries,
  clearExpiredCache,
  getCacheStats,
  type ParqueDBMiddlewareOptions,
  type CacheConfig,
  type LoggingConfig,
  type CacheEntry,
  type LogEntry,
  type LanguageModelV3Middleware,
  type LanguageModelCallOptions,
  type LanguageModelGenerateResult,
  type LanguageModelStreamResult,
  type LanguageModel,
} from './ai-sdk'

// rpc.do integration (promise pipelining and batching)
export {
  createParqueDBRPCClient,
  createBatchLoaderDB,
  type ParqueDBRPCClientOptions,
  type ParqueDBRPCClient,
  type RPCCollection,
  type BatchRelatedRequest,
  type BatchingOptions,
  type BatchedRequest,
  type BatchedResponse,
  type Transport as RPCTransport,
  type RPCBatchLoaderDB,
} from './rpc-do'

// Express integration (middleware)
export {
  createParqueDBMiddleware as createExpressMiddleware,
  createErrorMiddleware as createExpressErrorMiddleware,
  getSharedDB as getExpressSharedDB,
  resetSharedDB as resetExpressSharedDB,
  type ParqueDBMiddlewareOptions as ExpressMiddlewareOptions,
  type ParqueDBRequest as ExpressParqueDBRequest,
  type ExpressMiddleware,
  type ExpressErrorMiddleware,
} from './express'

// Fastify integration (plugin)
export {
  parquedbPlugin as fastifyPlugin,
  parquedbErrorHandler as fastifyErrorHandler,
  createParqueDBHook as createFastifyHook,
  type ParqueDBPluginOptions as FastifyPluginOptions,
  type FastifyInstance,
  type FastifyPluginAsync,
} from './fastify'
