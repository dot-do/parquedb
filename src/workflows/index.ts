/**
 * ParqueDB Workflows
 *
 * Cloudflare Workflows for long-running, resumable operations that
 * exceed the 1,000 subrequest limit of regular Workers.
 *
 * ## Available Workflows
 *
 * ### CompactionMigrationWorkflow
 * Compacts many small Parquet files into fewer large ones.
 * Optionally migrates to Iceberg/Delta format during compaction.
 *
 * ### MigrationWorkflow
 * Migrates entire namespaces between backend formats.
 *
 * ## Architecture
 *
 * ```
 * Writers → R2 (native) → Event Notification → Queue
 *                                                 ↓
 *                                    CompactionStateDO (tracks windows)
 *                                                 ↓
 *                              Window ready → CompactionMigrationWorkflow
 *                                                 ↓
 *                                    Compacted Iceberg/Delta files
 * ```
 *
 * ## Writer-Aware Compaction
 *
 * Each writer produces pre-sorted data. The workflow:
 * 1. Tracks all writers in a time window
 * 2. Waits for all writers to report (or timeout)
 * 3. Merge-sorts efficiently (data is pre-sorted per writer)
 * 4. Writes to target format
 *
 * This avoids re-sorting already-sorted data when multiple writers
 * are active simultaneously.
 *
 * @example
 * ```typescript
 * // wrangler.toml configuration
 * [[workflows]]
 * name = "compaction-migration"
 * binding = "COMPACTION_WORKFLOW"
 * class_name = "CompactionMigrationWorkflow"
 *
 * [[queues.consumers]]
 * queue = "parquedb-compaction-events"
 * max_batch_size = 100
 * max_batch_timeout = 30
 * ```
 */

// Compaction + Migration Workflow
export {
  CompactionMigrationWorkflow,
  type CompactionMigrationParams,
} from './compaction-migration'

// Queue Consumer for R2 Event Notifications
export {
  handleCompactionQueue,
  CompactionStateDO,
  type R2EventMessage,
  type CompactionConsumerConfig,
  // Time bucket sharding helpers
  type TimeBucketShardingConfig,
  calculateTimeBucket,
  getCompactionStateDOId,
  parseCompactionStateDOId,
  getRecentTimeBuckets,
  isTimeBucketExpired,
  groupUpdatesByDOId,
  shouldUseTimeBucketSharding,
  // Status aggregation
  type AggregatedCompactionStatusResponse,
  getAggregatedCompactionStatus,
} from './compaction-queue-consumer'

// Migration Workflow (standalone)
export {
  MigrationWorkflow,
  type MigrationWorkflowParams,
} from './migration-workflow'

// Vacuum Workflow (orphan cleanup)
export {
  VacuumWorkflow,
  type VacuumWorkflowParams,
  type VacuumResult,
  type OrphanedFileInfo,
} from './vacuum-workflow'

// Hierarchical Compaction (LSM-tree style) - Types and Pure Functions
export {
  type CompactionLevel,
  type HierarchicalCompactionConfig,
  type HierarchicalCompactionLevels,
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

// Hierarchical Compaction (LSM-tree style) - Worker Classes
export {
  LevelStateDO,
  CompactionPromotionWorkflow,
  registerCompactedFile,
  checkPromotionNeeded,
  removePromotedFiles,
} from './hierarchical-compaction'

// Streaming Merge-Sort (for large compaction windows)
export {
  StreamingMergeSorter,
  MinHeap,
  streamingMergeSort,
  estimateStreamingMergeMemory,
  calculateOptimalChunkSize,
  shouldUseStreamingMerge,
  type Row,
  type StreamingMergeOptions,
  type StreamingMergeResult,
} from './streaming-merge'

// Utilities and Type Guards
export {
  toInternalR2Bucket,
  isR2BucketLike,
  isInternalR2BucketLike,
  requireR2Bucket,
  toR2BucketOrUndefined,
  assertR2Bucket,
} from './utils'
