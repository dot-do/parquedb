/**
 * Dataset Configuration
 *
 * Manages per-dataset configuration for ParqueDB:
 * - events: true/false - enables event sourcing with time-travel
 * - compaction: settings for event compaction
 *
 * Configuration is stored in {dataset}/_config.json
 */

import type { DatasetConfig, CompactionConfig, TimeTravelOptions } from '../events/types'
import { DEFAULT_DATASET_CONFIG } from '../events/types'
import { safeJsonParse, isRecord, logger } from '../utils'

/**
 * Type guard for DatasetConfigFile
 *
 * Validates that a parsed value has the required structure for a config file.
 */
function isDatasetConfigFile(value: unknown): value is DatasetConfigFile {
  if (!isRecord(value)) return false
  // Required fields with loose type check (version may be parsed as number or string)
  return (
    'version' in value &&
    'dataset' in value &&
    typeof value.dataset === 'string' &&
    'events' in value &&
    typeof value.events === 'boolean' &&
    'createdAt' in value &&
    typeof value.createdAt === 'number' &&
    'updatedAt' in value &&
    typeof value.updatedAt === 'number'
  )
}

// =============================================================================
// Types
// =============================================================================

/**
 * Storage interface for configuration
 */
export interface ConfigStorage {
  /** Read config file */
  get(path: string): Promise<Uint8Array | null>
  /** Write config file */
  put(path: string, data: Uint8Array | ArrayBuffer): Promise<void>
  /** Check if config exists */
  head(path: string): Promise<boolean>
}

/**
 * Full dataset configuration with metadata
 */
export interface DatasetConfigFile {
  /** Config version */
  version: 1
  /** Dataset identifier */
  dataset: string
  /** Events configuration */
  events: boolean
  /** Compaction configuration (only used if events: true) */
  compaction?: CompactionConfig | undefined
  /** Creation timestamp */
  createdAt: number
  /** Last update timestamp */
  updatedAt: number
}

/**
 * Options for DatasetConfigManager
 */
export interface DatasetConfigManagerOptions {
  /** Dataset name */
  dataset: string
  /** Config storage */
  storage: ConfigStorage
}

// =============================================================================
// DatasetConfigManager Class
// =============================================================================

/**
 * Manages dataset configuration.
 *
 * @example
 * ```typescript
 * const config = new DatasetConfigManager({
 *   dataset: 'my-app',
 *   storage,
 * })
 *
 * // Load or create config
 * await config.load()
 *
 * // Check if events are enabled
 * if (config.isEventsEnabled()) {
 *   // Time-travel queries are supported
 * }
 *
 * // Enable events
 * await config.setEventsEnabled(true)
 * await config.save()
 * ```
 */
export class DatasetConfigManager {
  private options: DatasetConfigManagerOptions
  private config: DatasetConfigFile | null = null
  private dirty = false

  constructor(options: DatasetConfigManagerOptions) {
    this.options = options
  }

  // ===========================================================================
  // Load / Save
  // ===========================================================================

  /**
   * Get the config file path
   */
  getConfigPath(): string {
    return `${this.options.dataset}/_config.json`
  }

  /**
   * Load config from storage, or create default if it doesn't exist
   */
  async load(): Promise<DatasetConfigFile> {
    if (this.config) {
      return this.config
    }

    const path = this.getConfigPath()
    const data = await this.options.storage.get(path)

    if (data) {
      const json = new TextDecoder().decode(data)
      const result = safeJsonParse(json)
      if (result.ok && isDatasetConfigFile(result.value)) {
        this.config = result.value
      } else {
        logger.warn(`Invalid config at ${path}, using defaults`)
        this.config = this.createDefaultConfig()
      }
    } else {
      this.config = this.createDefaultConfig()
    }

    this.dirty = false
    return this.config
  }

  /**
   * Save config to storage
   */
  async save(): Promise<void> {
    if (!this.config) {
      throw new Error('No config loaded')
    }

    this.config.updatedAt = Date.now()

    const path = this.getConfigPath()
    const json = JSON.stringify(this.config, null, 2)
    const data = new TextEncoder().encode(json)

    await this.options.storage.put(path, data)
    this.dirty = false
  }

  /**
   * Save only if there are unsaved changes
   */
  async saveIfDirty(): Promise<void> {
    if (this.dirty) {
      await this.save()
    }
  }

  /**
   * Check if there are unsaved changes
   */
  isDirty(): boolean {
    return this.dirty
  }

  // ===========================================================================
  // Events Configuration
  // ===========================================================================

  /**
   * Check if events are enabled for this dataset
   */
  isEventsEnabled(): boolean {
    return this.config?.events ?? DEFAULT_DATASET_CONFIG.events ?? false
  }

  /**
   * Enable or disable events
   */
  async setEventsEnabled(enabled: boolean): Promise<void> {
    const config = await this.load()
    config.events = enabled
    this.dirty = true
  }

  /**
   * Get compaction configuration
   */
  getCompactionConfig(): CompactionConfig | undefined {
    return this.config?.compaction
  }

  /**
   * Set compaction configuration
   */
  async setCompactionConfig(compaction: CompactionConfig): Promise<void> {
    const config = await this.load()
    config.compaction = compaction
    this.dirty = true
  }

  // ===========================================================================
  // Time-Travel Support
  // ===========================================================================

  /**
   * Check if a query uses time-travel
   */
  isTimeTravelQuery(options?: TimeTravelOptions): boolean {
    return options?.at !== undefined
  }

  /**
   * Validate that time-travel is supported for this dataset
   */
  validateTimeTravelQuery(options?: TimeTravelOptions): void {
    if (this.isTimeTravelQuery(options) && !this.isEventsEnabled()) {
      throw new Error(
        `Time-travel queries require events: true in dataset configuration. ` +
        `Dataset "${this.options.dataset}" has events disabled.`
      )
    }
  }

  // ===========================================================================
  // Internal
  // ===========================================================================

  /**
   * Create default configuration
   */
  private createDefaultConfig(): DatasetConfigFile {
    const now = Date.now()
    return {
      version: 1,
      dataset: this.options.dataset,
      events: DEFAULT_DATASET_CONFIG.events ?? false,
      createdAt: now,
      updatedAt: now,
    }
  }

  /**
   * Get the loaded config (for testing)
   */
  getLoadedConfig(): DatasetConfigFile | null {
    return this.config
  }

  /**
   * Clear the cached config (for testing)
   */
  clearCache(): void {
    this.config = null
    this.dirty = false
  }

  /**
   * Get the dataset config as DatasetConfig type
   */
  toDatasetConfig(): DatasetConfig {
    return {
      events: this.config?.events ?? false,
      compaction: this.config?.compaction,
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a DatasetConfigManager instance
 */
export function createDatasetConfigManager(
  options: DatasetConfigManagerOptions
): DatasetConfigManager {
  return new DatasetConfigManager(options)
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Check if a dataset has events enabled (quick check without full load)
 */
export async function isEventsEnabled(
  storage: ConfigStorage,
  dataset: string
): Promise<boolean> {
  const path = `${dataset}/_config.json`
  const data = await storage.get(path)

  if (!data) {
    return DEFAULT_DATASET_CONFIG.events ?? false
  }

  const json = new TextDecoder().decode(data)
  const result = safeJsonParse(json)
  if (result.ok && isDatasetConfigFile(result.value)) {
    return result.value.events
  }
  return DEFAULT_DATASET_CONFIG.events ?? false
}

/**
 * Quick check if a time-travel query is valid for a dataset
 */
export async function validateTimeTravelSupport(
  storage: ConfigStorage,
  dataset: string,
  options?: TimeTravelOptions
): Promise<void> {
  if (options?.at === undefined) {
    return // Not a time-travel query
  }

  const enabled = await isEventsEnabled(storage, dataset)
  if (!enabled) {
    throw new Error(
      `Time-travel queries require events: true in dataset configuration. ` +
      `Dataset "${dataset}" has events disabled.`
    )
  }
}
