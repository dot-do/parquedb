/**
 * StorageRouter - Routes storage operations based on collection storage mode
 *
 * Determines whether a collection uses typed (columnar) or flexible (variant-shredded)
 * storage based on schema configuration.
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
 * Options for the StorageRouter
 */
export interface StorageRouterOptions {
  /**
   * Default storage mode for collections not in schema
   * @default 'flexible'
   */
  defaultMode?: StorageMode
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
}

// =============================================================================
// Implementation
// =============================================================================

/**
 * StorageRouter implementation
 *
 * Routes storage operations based on whether a collection has a typed schema
 * or uses flexible (variant-shredded) storage.
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
   * Create a new StorageRouter
   *
   * @param schema - Optional schema definition
   * @param options - Router options
   */
  constructor(schema?: RouterSchema, options: StorageRouterOptions = {}) {
    this.typedCollections = new Set()
    this.flexibleCollections = new Set()
    this.defaultMode = options.defaultMode ?? 'flexible'

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
}
