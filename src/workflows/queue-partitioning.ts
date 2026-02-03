/**
 * Queue Partitioning for Horizontal Scaling
 *
 * Provides consistent hashing for routing R2 event notifications to
 * partitioned queues. This enables horizontal scaling of compaction
 * workloads by distributing events across multiple queue consumers.
 *
 * Architecture:
 * ```
 * R2 Write → Event Notification → Partition Router → Queue N
 *                                      ↓
 *                                 hash(namespace) % numPartitions
 *                                      ↓
 *                        parquedb-compaction-events-{N}
 *                                      ↓
 *                              Queue Consumer N
 *                                      ↓
 *                    CompactionStateDO (namespace-sharded)
 * ```
 *
 * Key insight: Queues are the horizontal scaling unit for throughput,
 * while Durable Objects handle consistency. This separation allows
 * scaling queue processing independently.
 *
 * @example
 * ```typescript
 * import { getPartitionForNamespace, createPartitionedQueueName } from './queue-partitioning'
 *
 * // Get partition for a namespace
 * const partition = getPartitionForNamespace('users', 4) // Returns 0-3
 *
 * // Get queue name for routing
 * const queueName = createPartitionedQueueName('users', 4) // Returns 'parquedb-compaction-events-2'
 * ```
 */

// =============================================================================
// Configuration
// =============================================================================

/** Default number of queue partitions */
export const DEFAULT_NUM_PARTITIONS = 1

/** Base queue name for compaction events */
export const QUEUE_BASE_NAME = 'parquedb-compaction-events'

// =============================================================================
// FNV-1a Hash Implementation
// =============================================================================

/**
 * FNV-1a 32-bit hash implementation
 *
 * FNV (Fowler-Noll-Vo) is a fast, non-cryptographic hash function that
 * provides good distribution for hash tables and consistent hashing.
 *
 * Properties:
 * - Fast computation (simple XOR and multiply operations)
 * - Good distribution for short strings (like namespace names)
 * - Deterministic (same input always produces same output)
 * - Platform-independent (no endianness issues)
 *
 * @param str - Input string to hash
 * @returns 32-bit hash value
 *
 * @example
 * ```typescript
 * fnv1a32('users')    // => 1234567890
 * fnv1a32('posts')    // => 987654321
 * fnv1a32('comments') // => 1122334455
 * ```
 */
export function fnv1a32(str: string): number {
  // FNV-1a parameters (32-bit)
  const FNV_OFFSET_BASIS = 0x811c9dc5
  const FNV_PRIME = 0x01000193

  let hash = FNV_OFFSET_BASIS

  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i)
    // Multiply by prime, keep 32-bit
    hash = Math.imul(hash, FNV_PRIME) >>> 0
  }

  return hash >>> 0 // Ensure unsigned
}

// =============================================================================
// Partition Routing
// =============================================================================

/**
 * Get the queue partition index for a namespace
 *
 * Uses consistent hashing (FNV-1a) to ensure:
 * - Same namespace always routes to same partition
 * - Even distribution across partitions
 * - Fast computation with no external dependencies
 *
 * @param namespace - Namespace name (e.g., 'users', 'posts', 'app/data')
 * @param numPartitions - Total number of queue partitions (default: 1)
 * @returns Partition index (0 to numPartitions-1)
 *
 * @example
 * ```typescript
 * // Single partition (default behavior)
 * getPartitionForNamespace('users', 1) // => 0
 *
 * // Multiple partitions
 * getPartitionForNamespace('users', 4)    // => 2
 * getPartitionForNamespace('posts', 4)    // => 1
 * getPartitionForNamespace('comments', 4) // => 3
 *
 * // Consistent across calls
 * getPartitionForNamespace('users', 4) === getPartitionForNamespace('users', 4) // true
 * ```
 */
export function getPartitionForNamespace(
  namespace: string,
  numPartitions: number = DEFAULT_NUM_PARTITIONS
): number {
  if (numPartitions <= 0) {
    throw new Error(`numPartitions must be positive, got ${numPartitions}`)
  }

  if (numPartitions === 1) {
    return 0 // Optimization for single partition
  }

  const hash = fnv1a32(namespace)
  return hash % numPartitions
}

/**
 * Create the queue name for a given namespace
 *
 * Returns the full queue name for routing R2 event notifications.
 * With single partition, returns the base name. With multiple partitions,
 * returns the partitioned name (e.g., 'parquedb-compaction-events-2').
 *
 * @param namespace - Namespace name
 * @param numPartitions - Total number of queue partitions
 * @returns Full queue name for routing
 *
 * @example
 * ```typescript
 * // Single partition (backwards compatible)
 * createPartitionedQueueName('users', 1)
 * // => 'parquedb-compaction-events'
 *
 * // Multiple partitions
 * createPartitionedQueueName('users', 4)
 * // => 'parquedb-compaction-events-2' (assuming hash maps to partition 2)
 * ```
 */
export function createPartitionedQueueName(
  namespace: string,
  numPartitions: number = DEFAULT_NUM_PARTITIONS
): string {
  if (numPartitions === 1) {
    return QUEUE_BASE_NAME
  }

  const partition = getPartitionForNamespace(namespace, numPartitions)
  return `${QUEUE_BASE_NAME}-${partition}`
}

/**
 * Get all queue names for a given partition count
 *
 * Returns array of all queue names for configuring wrangler.jsonc.
 * Useful for generating queue configuration or listing all consumers.
 *
 * @param numPartitions - Total number of queue partitions
 * @returns Array of queue names
 *
 * @example
 * ```typescript
 * // Single partition
 * getAllQueueNames(1)
 * // => ['parquedb-compaction-events']
 *
 * // Four partitions
 * getAllQueueNames(4)
 * // => [
 * //   'parquedb-compaction-events-0',
 * //   'parquedb-compaction-events-1',
 * //   'parquedb-compaction-events-2',
 * //   'parquedb-compaction-events-3'
 * // ]
 * ```
 */
export function getAllQueueNames(
  numPartitions: number = DEFAULT_NUM_PARTITIONS
): string[] {
  if (numPartitions === 1) {
    return [QUEUE_BASE_NAME]
  }

  return Array.from({ length: numPartitions }, (_, i) => `${QUEUE_BASE_NAME}-${i}`)
}

// =============================================================================
// Partition Analysis
// =============================================================================

/**
 * Analyze partition distribution for a set of namespaces
 *
 * Useful for validating that namespaces are evenly distributed across
 * partitions. Returns counts per partition and distribution metrics.
 *
 * @param namespaces - Array of namespace names
 * @param numPartitions - Total number of queue partitions
 * @returns Distribution analysis
 *
 * @example
 * ```typescript
 * const analysis = analyzePartitionDistribution(
 *   ['users', 'posts', 'comments', 'likes', 'shares'],
 *   4
 * )
 * // => {
 * //   partitionCounts: { 0: 1, 1: 2, 2: 1, 3: 1 },
 * //   minCount: 1,
 * //   maxCount: 2,
 * //   namespaceToPartition: {
 * //     users: 2, posts: 1, comments: 3, likes: 0, shares: 1
 * //   }
 * // }
 * ```
 */
export function analyzePartitionDistribution(
  namespaces: string[],
  numPartitions: number = DEFAULT_NUM_PARTITIONS
): {
  partitionCounts: Record<number, number>
  minCount: number
  maxCount: number
  namespaceToPartition: Record<string, number>
} {
  const partitionCounts: Record<number, number> = {}
  const namespaceToPartition: Record<string, number> = {}

  // Initialize all partitions with 0
  for (let i = 0; i < numPartitions; i++) {
    partitionCounts[i] = 0
  }

  // Assign namespaces to partitions
  for (const namespace of namespaces) {
    const partition = getPartitionForNamespace(namespace, numPartitions)
    partitionCounts[partition] = (partitionCounts[partition] ?? 0) + 1
    namespaceToPartition[namespace] = partition
  }

  // Calculate min/max
  const counts = Object.values(partitionCounts)
  const minCount = Math.min(...counts)
  const maxCount = Math.max(...counts)

  return {
    partitionCounts,
    minCount,
    maxCount,
    namespaceToPartition,
  }
}

// =============================================================================
// Multi-Queue Consumer Support
// =============================================================================

/**
 * Configuration for partitioned queue consumer
 */
export interface PartitionedQueueConfig {
  /** Total number of partitions */
  numPartitions: number
  /** This consumer's partition index (0-based) */
  partitionIndex: number
  /** Base configuration passed to handleCompactionQueue */
  compactionConfig?: {
    windowSizeMs?: number
    minFilesToCompact?: number
    maxWaitTimeMs?: number
    targetFormat?: 'native' | 'iceberg' | 'delta'
    namespacePrefix?: string
  }
}

/**
 * Get partition config from environment variables
 *
 * Reads NUM_PARTITIONS and PARTITION_INDEX from environment.
 * Defaults to single partition for backwards compatibility.
 *
 * @param env - Worker environment
 * @returns Partition configuration
 *
 * @example
 * ```typescript
 * // In worker
 * const config = getPartitionConfigFromEnv(env)
 * console.log(config.numPartitions) // 4
 * console.log(config.partitionIndex) // 2
 * ```
 */
export function getPartitionConfigFromEnv(env: {
  NUM_PARTITIONS?: string
  PARTITION_INDEX?: string
}): {
  numPartitions: number
  partitionIndex: number
} {
  const numPartitions = parseInt(env.NUM_PARTITIONS ?? '1', 10)
  const partitionIndex = parseInt(env.PARTITION_INDEX ?? '0', 10)

  if (numPartitions <= 0) {
    throw new Error(`NUM_PARTITIONS must be positive, got ${numPartitions}`)
  }

  if (partitionIndex < 0 || partitionIndex >= numPartitions) {
    throw new Error(
      `PARTITION_INDEX must be 0-${numPartitions - 1}, got ${partitionIndex}`
    )
  }

  return { numPartitions, partitionIndex }
}

/**
 * Validate that a message belongs to this partition
 *
 * Used by queue consumers to verify messages are routed correctly.
 * Logs warning if message is misrouted (useful for debugging R2 notification config).
 *
 * @param namespace - Namespace from the message
 * @param numPartitions - Total number of partitions
 * @param expectedPartition - This consumer's partition index
 * @returns true if message belongs to this partition
 *
 * @example
 * ```typescript
 * // In queue consumer
 * for (const message of batch.messages) {
 *   const namespace = parseNamespace(message)
 *   if (!validateMessagePartition(namespace, 4, 2)) {
 *     console.warn('Misrouted message received')
 *   }
 *   // Process message...
 * }
 * ```
 */
export function validateMessagePartition(
  namespace: string,
  numPartitions: number,
  expectedPartition: number
): boolean {
  const actualPartition = getPartitionForNamespace(namespace, numPartitions)
  return actualPartition === expectedPartition
}

// =============================================================================
// R2 Event Notification Routing Helpers
// =============================================================================

/**
 * Generate R2 notification rules for partitioned queues
 *
 * Returns configuration hints for setting up R2 event notifications
 * per partition. Each namespace prefix maps to a specific queue.
 *
 * Note: R2 doesn't support hash-based routing directly, so this generates
 * prefix-based rules as a workaround. For true hash-based routing, consider
 * using a single queue that routes to partitioned consumers.
 *
 * @param namespaces - Known namespace prefixes
 * @param numPartitions - Number of partitions
 * @returns Array of notification rule suggestions
 *
 * @example
 * ```typescript
 * const rules = generateR2NotificationRules(
 *   ['users', 'posts', 'comments'],
 *   4
 * )
 * // => [
 * //   { prefix: 'data/users/', queue: 'parquedb-compaction-events-2' },
 * //   { prefix: 'data/posts/', queue: 'parquedb-compaction-events-1' },
 * //   { prefix: 'data/comments/', queue: 'parquedb-compaction-events-3' }
 * // ]
 * ```
 */
export function generateR2NotificationRules(
  namespaces: string[],
  numPartitions: number,
  dataPrefix: string = 'data/'
): Array<{
  prefix: string
  suffix: string
  queue: string
  eventTypes: string[]
}> {
  return namespaces.map(namespace => ({
    prefix: `${dataPrefix}${namespace}/`,
    suffix: '.parquet',
    queue: createPartitionedQueueName(namespace, numPartitions),
    eventTypes: ['object-create'],
  }))
}

export default {
  fnv1a32,
  getPartitionForNamespace,
  createPartitionedQueueName,
  getAllQueueNames,
  analyzePartitionDistribution,
  getPartitionConfigFromEnv,
  validateMessagePartition,
  generateR2NotificationRules,
  DEFAULT_NUM_PARTITIONS,
  QUEUE_BASE_NAME,
}
