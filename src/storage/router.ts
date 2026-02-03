/**
 * StorageRouter - Routes storage operations based on collection storage mode
 *
 * Determines whether a collection uses typed (columnar) or flexible (variant-shredded)
 * storage based on schema configuration. Also supports namespace sharding for
 * scaling large namespaces.
 *
 * @example
 * ```typescript
 * const router = new StorageRouter({
 *   User: { name: 'string!' },
 *   Post: { title: 'string!' },
 * })
 *
 * router.getStorageMode('user')   // 'typed'
 * router.getStorageMode('post')   // 'typed'
 * router.getStorageMode('events') // 'flexible' (no schema = flexible)
 *
 * // With explicit flexible mode
 * const router2 = new StorageRouter({
 *   User: { name: 'string!' },
 *   Posts: 'flexible',
 * })
 *
 * router2.getStorageMode('user')  // 'typed'
 * router2.getStorageMode('posts') // 'flexible'
 *
 * // With type-based sharding
 * const router3 = new StorageRouter({
 *   Orders: { id: 'string!', type: 'string!' },
 * }, {
 *   sharding: {
 *     orders: { strategy: 'type', typeField: 'type' }
 *   }
 * })
 *
 * router3.getShardPath('orders', { type: 'purchase' })
 * // => 'orders/_shards/type=purchase/data.parquet'
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Storage mode for a collection
 * - 'typed': Native columnar storage with schema-defined fields
 * - 'flexible': Variant-shredded storage for schema-less collections
 */
export type StorageMode = 'typed' | 'flexible'

/**
 * Sharding strategy for a namespace
 * - 'none': No sharding, single data file
 * - 'type': Shard by entity type field
 * - 'time': Shard by time period (e.g., monthly)
 * - 'hash': Shard by hash of entity ID
 */
export type ShardStrategy = 'none' | 'type' | 'time' | 'hash'

/**
 * Collection schema definition (simplified from db.ts)
 */
export type CollectionSchema = Record<string, unknown> | 'flexible'

/**
 * Schema definition for the router
 */
export interface RouterSchema {
  [collection: string]: CollectionSchema
}

/**
 * Configuration for type-based sharding
 */
export interface TypeShardConfig {
  strategy: 'type'
  /** Field name containing the entity type for sharding */
  typeField: string
}

/**
 * Configuration for time-based sharding
 */
export interface TimeShardConfig {
  strategy: 'time'
  /** Field name containing the timestamp for sharding */
  timeField: string
  /** Time bucket size: 'hour', 'day', 'week', 'month', 'year' */
  bucketSize: 'hour' | 'day' | 'week' | 'month' | 'year'
}

/**
 * Configuration for hash-based sharding
 */
export interface HashShardConfig {
  strategy: 'hash'
  /** Number of hash buckets (shards) */
  shardCount: number
}

/**
 * Union type for shard configuration
 */
export type ShardConfig = TypeShardConfig | TimeShardConfig | HashShardConfig

/**
 * Namespace sharding configuration map
 */
export interface ShardingConfig {
  [namespace: string]: ShardConfig
}

/**
 * Thresholds for automatic sharding decisions
 */
export interface ShardingThresholds {
  /** Maximum file size in bytes before considering sharding */
  maxFileSize: number
  /** Maximum entity count before considering sharding */
  maxEntityCount: number
  /** Maximum row group count before considering sharding */
  maxRowGroupCount: number
}

/**
 * Default sharding thresholds
 */
export const DEFAULT_SHARDING_THRESHOLDS: ShardingThresholds = {
  maxFileSize: 1024 * 1024 * 1024, // 1GB
  maxEntityCount: 10_000_000,
  maxRowGroupCount: 1000,
}

/**
 * Options for the StorageRouter
 */
export interface StorageRouterOptions {
  /**
   * Default storage mode for collections not in schema
   * @default 'flexible'
   */
  defaultMode?: StorageMode | undefined

  /**
   * Namespace sharding configuration
   */
  sharding?: ShardingConfig | undefined

  /**
   * Thresholds for automatic sharding decisions
   */
  shardingThresholds?: ShardingThresholds | undefined
}

/**
 * StorageRouter interface for routing storage operations
 */
export interface IStorageRouter {
  /**
   * Determine storage mode for a collection
   * @param ns - Collection namespace (case-insensitive)
   */
  getStorageMode(ns: string): StorageMode

  /**
   * Get storage path for a collection's data file
   * @param ns - Collection namespace (case-insensitive)
   * @returns Path to the data parquet file
   *
   * Typed mode: `data/{collection}.parquet`
   * Flexible mode: `data/{ns}/data.parquet`
   */
  getDataPath(ns: string): string

  /**
   * Check if collection has a typed schema
   * @param ns - Collection namespace (case-insensitive)
   */
  hasTypedSchema(ns: string): boolean

  /**
   * Get shard strategy for a namespace
   * @param ns - Collection namespace (case-insensitive)
   * @returns The shard strategy ('none' if not configured)
   */
  getShardStrategy(ns: string): ShardStrategy

  /**
   * Get shard configuration for a namespace
   * @param ns - Collection namespace (case-insensitive)
   * @returns The shard configuration or undefined if not sharded
   */
  getShardConfig(ns: string): ShardConfig | undefined

  /**
   * Get the shard path for an entity based on its data
   * @param ns - Collection namespace (case-insensitive)
   * @param entity - Entity data containing the shard key field
   * @returns Path to the shard's data parquet file
   */
  getShardPath(ns: string, entity: Record<string, unknown>): string

  /**
   * Get all possible shard paths for a namespace
   * @param ns - Collection namespace (case-insensitive)
   * @param knownShardKeys - Optional list of known shard key values
   * @returns Array of shard data paths
   */
  listShardPaths(ns: string, knownShardKeys?: string[]): string[]

  /**
   * Check if a namespace is sharded
   * @param ns - Collection namespace (case-insensitive)
   */
  isSharded(ns: string): boolean

  /**
   * Resolve all data paths for a query
   * Considers sharding configuration to return appropriate paths
   * @param ns - Collection namespace (case-insensitive)
   * @param filter - Optional filter that may include shard key
   * @returns Array of data paths to query
   */
  resolveDataPaths(ns: string, filter?: Record<string, unknown>): string[]
}

// =============================================================================
// Storage Path Helpers
// =============================================================================

/**
 * Standard file names within a namespace
 */
export const NAMESPACE_FILES = {
  DATA: 'data.parquet',
  EDGES: 'edges.parquet',
  EVENTS: 'events.parquet',
  SCHEMA: '_schema.parquet',
  META: '_meta.parquet',
  SHARDS_DIR: '_shards',
} as const

/**
 * Storage path helpers for namespace-sharded architecture
 */
export const STORAGE_PATHS = {
  /** Namespace data file */
  namespaceData: (ns: string) => `${ns}/${NAMESPACE_FILES.DATA}`,

  /** Namespace edges file */
  namespaceEdges: (ns: string) => `${ns}/${NAMESPACE_FILES.EDGES}`,

  /** Namespace events file */
  namespaceEvents: (ns: string) => `${ns}/${NAMESPACE_FILES.EVENTS}`,

  /** Namespace schema file */
  namespaceSchema: (ns: string) => `${ns}/${NAMESPACE_FILES.SCHEMA}`,

  /** Namespace metadata file */
  namespaceMeta: (ns: string) => `${ns}/${NAMESPACE_FILES.META}`,

  /** Type-based shard data file */
  typeShardData: (ns: string, entityType: string) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/type=${entityType}/data.parquet`,

  /** Time-based shard data file */
  timeShardData: (ns: string, period: string) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/period=${period}/data.parquet`,

  /** Hash-based shard data file */
  hashShardData: (ns: string, shardNum: number) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/shard=${shardNum}/data.parquet`,

  /** Shards directory prefix */
  shardsPrefix: (ns: string) => `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/`,
} as const

/**
 * Format a time period for time-based sharding
 * @param timestamp - Timestamp to format
 * @param bucketSize - Time bucket size
 * @returns Formatted period string (e.g., '2024-01' for month)
 */
export function formatTimePeriod(
  timestamp: number | Date,
  bucketSize: 'hour' | 'day' | 'week' | 'month' | 'year'
): string {
  const date = timestamp instanceof Date ? timestamp : new Date(timestamp)
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  const hour = String(date.getUTCHours()).padStart(2, '0')

  switch (bucketSize) {
    case 'hour':
      return `${year}-${month}-${day}T${hour}`
    case 'day':
      return `${year}-${month}-${day}`
    case 'week': {
      // Get ISO week number
      const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
      d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
      const weekNum = Math.ceil(((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
      return `${year}-W${String(weekNum).padStart(2, '0')}`
    }
    case 'month':
      return `${year}-${month}`
    case 'year':
      return `${year}`
  }
}

/**
 * Calculate hash shard number for an entity ID
 * @param id - Entity ID to hash
 * @param shardCount - Number of shards
 * @returns Shard number (0 to shardCount-1)
 */
export function calculateHashShard(id: string, shardCount: number): number {
  // Simple FNV-1a hash
  let hash = 2166136261
  for (let i = 0; i < id.length; i++) {
    hash ^= id.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return Math.abs(hash) % shardCount
}

// =============================================================================
// Implementation
// =============================================================================

/**
 * StorageRouter implementation
 *
 * Routes storage operations based on whether a collection has a typed schema
 * or uses flexible (variant-shredded) storage. Also supports namespace sharding
 * for scaling large namespaces.
 */
export class StorageRouter implements IStorageRouter {
  /**
   * Set of collection names with typed schemas (lowercase for case-insensitive lookup)
   */
  private readonly typedCollections: Set<string>

  /**
   * Set of collection names explicitly marked as flexible (lowercase)
   */
  private readonly flexibleCollections: Set<string>

  /**
   * Default storage mode for unknown collections
   */
  private readonly defaultMode: StorageMode

  /**
   * Namespace sharding configuration (lowercase keys)
   */
  private readonly shardingConfig: Map<string, ShardConfig>

  /**
   * Sharding thresholds for automatic decisions
   */
  private readonly thresholds: ShardingThresholds

  /**
   * Create a new StorageRouter
   *
   * @param schema - Optional schema definition
   * @param options - Router options
   */
  constructor(schema?: RouterSchema, options: StorageRouterOptions = {}) {
    this.typedCollections = new Set()
    this.flexibleCollections = new Set()
    this.defaultMode = options.defaultMode ?? 'flexible'
    this.shardingConfig = new Map()
    this.thresholds = options.shardingThresholds ?? DEFAULT_SHARDING_THRESHOLDS

    if (schema) {
      for (const [name, def] of Object.entries(schema)) {
        const normalizedName = name.toLowerCase()
        if (def === 'flexible') {
          this.flexibleCollections.add(normalizedName)
        } else if (def && typeof def === 'object') {
          // Has schema definition = typed mode
          this.typedCollections.add(normalizedName)
        }
      }
    }

    if (options.sharding) {
      for (const [ns, config] of Object.entries(options.sharding)) {
        this.shardingConfig.set(ns.toLowerCase(), config)
      }
    }
  }

  /**
   * Determine storage mode for a collection
   *
   * Priority:
   * 1. Explicitly marked as flexible -> 'flexible'
   * 2. Has typed schema -> 'typed'
   * 3. Unknown collection -> defaultMode (default: 'flexible')
   */
  getStorageMode(ns: string): StorageMode {
    const normalizedNs = ns.toLowerCase()

    // Check if explicitly marked as flexible
    if (this.flexibleCollections.has(normalizedNs)) {
      return 'flexible'
    }

    // Check if has typed schema
    if (this.typedCollections.has(normalizedNs)) {
      return 'typed'
    }

    // Unknown collection - use default mode
    return this.defaultMode
  }

  /**
   * Get storage path for a collection's data file
   *
   * Typed mode: `data/{collection}.parquet`
   * Flexible mode: `data/{ns}/data.parquet`
   */
  getDataPath(ns: string): string {
    const normalizedNs = ns.toLowerCase()
    const mode = this.getStorageMode(ns)

    if (mode === 'typed') {
      return `data/${normalizedNs}.parquet`
    }

    return `data/${normalizedNs}/data.parquet`
  }

  /**
   * Check if collection has a typed schema
   */
  hasTypedSchema(ns: string): boolean {
    return this.typedCollections.has(ns.toLowerCase())
  }

  /**
   * Get all typed collection names
   */
  getTypedCollections(): string[] {
    return Array.from(this.typedCollections)
  }

  /**
   * Get all flexible collection names
   */
  getFlexibleCollections(): string[] {
    return Array.from(this.flexibleCollections)
  }

  // ===========================================================================
  // Sharding Support
  // ===========================================================================

  /**
   * Get shard strategy for a namespace
   */
  getShardStrategy(ns: string): ShardStrategy {
    const config = this.shardingConfig.get(ns.toLowerCase())
    return config?.strategy ?? 'none'
  }

  /**
   * Get shard configuration for a namespace
   */
  getShardConfig(ns: string): ShardConfig | undefined {
    return this.shardingConfig.get(ns.toLowerCase())
  }

  /**
   * Check if a namespace is sharded
   */
  isSharded(ns: string): boolean {
    return this.shardingConfig.has(ns.toLowerCase())
  }

  /**
   * Get the shard path for an entity based on its data
   *
   * Returns the appropriate shard path based on the namespace's sharding strategy:
   * - 'type': Uses entity type field to route to type=X shard
   * - 'time': Uses timestamp field to route to period=X shard
   * - 'hash': Uses entity ID hash to route to shard=N shard
   * - 'none': Returns the base data path
   */
  getShardPath(ns: string, entity: Record<string, unknown>): string {
    const normalizedNs = ns.toLowerCase()
    const config = this.shardingConfig.get(normalizedNs)

    if (!config) {
      return this.getDataPath(ns)
    }

    switch (config.strategy) {
      case 'type': {
        const typeValue = entity[config.typeField]
        if (typeValue === undefined || typeValue === null) {
          // Fall back to unsharded path if type field is missing
          return this.getDataPath(ns)
        }
        const sanitizedType = String(typeValue).toLowerCase().replace(/[^a-z0-9_-]/g, '_')
        return STORAGE_PATHS.typeShardData(normalizedNs, sanitizedType)
      }

      case 'time': {
        const timeValue = entity[config.timeField]
        if (timeValue === undefined || timeValue === null) {
          // Fall back to unsharded path if time field is missing
          return this.getDataPath(ns)
        }
        const timestamp = typeof timeValue === 'number' ? timeValue : new Date(String(timeValue)).getTime()
        if (isNaN(timestamp)) {
          return this.getDataPath(ns)
        }
        const period = formatTimePeriod(timestamp, config.bucketSize)
        return STORAGE_PATHS.timeShardData(normalizedNs, period)
      }

      case 'hash': {
        const id = entity['id'] ?? entity['$id']
        if (id === undefined || id === null) {
          // Fall back to shard 0 if no ID
          return STORAGE_PATHS.hashShardData(normalizedNs, 0)
        }
        const shardNum = calculateHashShard(String(id), config.shardCount)
        return STORAGE_PATHS.hashShardData(normalizedNs, shardNum)
      }

      default:
        return this.getDataPath(ns)
    }
  }

  /**
   * Get all possible shard paths for a namespace
   *
   * For hash sharding, returns all shard paths.
   * For type/time sharding, requires known shard keys to be provided.
   */
  listShardPaths(ns: string, knownShardKeys?: string[]): string[] {
    const normalizedNs = ns.toLowerCase()
    const config = this.shardingConfig.get(normalizedNs)

    if (!config) {
      return [this.getDataPath(ns)]
    }

    switch (config.strategy) {
      case 'type':
      case 'time': {
        if (!knownShardKeys || knownShardKeys.length === 0) {
          // Can't enumerate without known keys - return base path
          return [this.getDataPath(ns)]
        }
        return knownShardKeys.map((key) =>
          config.strategy === 'type'
            ? STORAGE_PATHS.typeShardData(normalizedNs, key.toLowerCase().replace(/[^a-z0-9_-]/g, '_'))
            : STORAGE_PATHS.timeShardData(normalizedNs, key)
        )
      }

      case 'hash': {
        const paths: string[] = []
        for (let i = 0; i < config.shardCount; i++) {
          paths.push(STORAGE_PATHS.hashShardData(normalizedNs, i))
        }
        return paths
      }

      default:
        return [this.getDataPath(ns)]
    }
  }

  /**
   * Resolve all data paths for a query
   *
   * If the filter contains the shard key, returns only the relevant shard path(s).
   * Otherwise, returns all shard paths (or the base path if not sharded).
   */
  resolveDataPaths(ns: string, filter?: Record<string, unknown>): string[] {
    const normalizedNs = ns.toLowerCase()
    const config = this.shardingConfig.get(normalizedNs)

    if (!config) {
      return [this.getDataPath(ns)]
    }

    // Check if filter contains shard key
    if (filter) {
      switch (config.strategy) {
        case 'type': {
          const typeFilter = filter[config.typeField]
          if (typeFilter !== undefined && typeFilter !== null) {
            // Direct equality filter on type field
            if (typeof typeFilter === 'string') {
              return [STORAGE_PATHS.typeShardData(normalizedNs, typeFilter.toLowerCase().replace(/[^a-z0-9_-]/g, '_'))]
            }
            // Handle $eq operator
            if (typeof typeFilter === 'object' && '$eq' in typeFilter) {
              const eqValue = (typeFilter as { $eq: unknown }).$eq
              if (typeof eqValue === 'string') {
                return [STORAGE_PATHS.typeShardData(normalizedNs, eqValue.toLowerCase().replace(/[^a-z0-9_-]/g, '_'))]
              }
            }
            // Handle $in operator - return multiple shard paths
            if (typeof typeFilter === 'object' && '$in' in typeFilter) {
              const inValues = (typeFilter as { $in: unknown[] }).$in
              if (Array.isArray(inValues)) {
                return inValues
                  .filter((v): v is string => typeof v === 'string')
                  .map((v) => STORAGE_PATHS.typeShardData(normalizedNs, v.toLowerCase().replace(/[^a-z0-9_-]/g, '_')))
              }
            }
          }
          break
        }

        case 'time': {
          const timeFilter = filter[config.timeField]
          if (timeFilter !== undefined && timeFilter !== null) {
            // Direct equality filter on time field
            if (typeof timeFilter === 'number' || typeof timeFilter === 'string') {
              const timestamp =
                typeof timeFilter === 'number' ? timeFilter : new Date(timeFilter).getTime()
              if (!isNaN(timestamp)) {
                const period = formatTimePeriod(timestamp, config.bucketSize)
                return [STORAGE_PATHS.timeShardData(normalizedNs, period)]
              }
            }
          }
          break
        }

        case 'hash': {
          const idFilter = filter['id'] ?? filter['$id']
          if (idFilter !== undefined && idFilter !== null) {
            // Direct equality filter on ID
            if (typeof idFilter === 'string') {
              const shardNum = calculateHashShard(idFilter, config.shardCount)
              return [STORAGE_PATHS.hashShardData(normalizedNs, shardNum)]
            }
            // Handle $eq operator
            if (typeof idFilter === 'object' && '$eq' in idFilter) {
              const eqValue = (idFilter as { $eq: unknown }).$eq
              if (typeof eqValue === 'string') {
                const shardNum = calculateHashShard(eqValue, config.shardCount)
                return [STORAGE_PATHS.hashShardData(normalizedNs, shardNum)]
              }
            }
          }
          break
        }
      }
    }

    // No shard-narrowing filter - return all shard paths
    return this.listShardPaths(ns)
  }

  /**
   * Get the shards prefix for listing shards
   */
  getShardsPrefix(ns: string): string {
    return STORAGE_PATHS.shardsPrefix(ns.toLowerCase())
  }

  /**
   * Get sharding thresholds
   */
  getShardingThresholds(): ShardingThresholds {
    return this.thresholds
  }

  /**
   * Check if namespace should be sharded based on thresholds
   * @param stats - Current namespace statistics
   */
  shouldShard(stats: { fileSize: number; entityCount: number; rowGroupCount: number }): boolean {
    return (
      stats.fileSize > this.thresholds.maxFileSize ||
      stats.entityCount > this.thresholds.maxEntityCount ||
      stats.rowGroupCount > this.thresholds.maxRowGroupCount
    )
  }

  /**
   * Add or update sharding configuration for a namespace
   * @param ns - Namespace to configure
   * @param config - Shard configuration
   */
  setShardConfig(ns: string, config: ShardConfig): void {
    this.shardingConfig.set(ns.toLowerCase(), config)
  }

  /**
   * Remove sharding configuration for a namespace
   * @param ns - Namespace to remove sharding from
   */
  removeShardConfig(ns: string): void {
    this.shardingConfig.delete(ns.toLowerCase())
  }
}
