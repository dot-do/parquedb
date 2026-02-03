/**
 * Studio Types
 *
 * Type definitions for the ParqueDB Studio - a Payload CMS-based
 * admin interface for viewing and editing Parquet files.
 */

// =============================================================================
// Schema Discovery Types
// =============================================================================

/**
 * Discovered field from Parquet schema
 */
export interface DiscoveredField {
  /** Field name */
  name: string
  /** Parquet physical type */
  parquetType: string
  /** Inferred Payload field type */
  payloadType: PayloadFieldType
  /** Whether the field is optional */
  optional: boolean
  /** Whether field is a list/array */
  isArray: boolean
  /** Additional type info (precision, scale, etc.) */
  typeInfo?: Record<string, unknown>
}

/**
 * Discovered collection (namespace) from Parquet files
 */
export interface DiscoveredCollection {
  /** Collection slug (namespace name) */
  slug: string
  /** Human-readable label */
  label: string
  /** Path to the Parquet file */
  path: string
  /** Total number of rows */
  rowCount: number
  /** File size in bytes */
  fileSize: number
  /** Discovered fields/schema */
  fields: DiscoveredField[]
  /** Whether this is a ParqueDB-managed file (has $id, $type, etc.) */
  isParqueDB: boolean
  /** Last modified timestamp */
  lastModified?: Date
}

/**
 * Payload field types that we map to
 */
export type PayloadFieldType =
  | 'text'
  | 'textarea'
  | 'number'
  | 'email'
  | 'checkbox'
  | 'date'
  | 'richText'
  | 'json'
  | 'array'
  | 'relationship'
  | 'select'
  | 'code'
  | 'point'
  | 'upload'
  | 'group'

// =============================================================================
// UI Metadata Types
// =============================================================================

/**
 * UI metadata for a field
 */
export interface FieldUIMetadata {
  /** Display label override */
  label?: string
  /** Help text/description */
  description?: string
  /** Whether to hide from list view */
  hideInList?: boolean
  /** Whether to hide from form */
  hideInForm?: boolean
  /** Whether field is read-only */
  readOnly?: boolean
  /** Column width in list view */
  width?: number
  /** Sort order in form */
  order?: number
  /** Custom admin component */
  admin?: {
    position?: 'sidebar'
    width?: string
    condition?: string
  }
  /** For select fields: available options */
  options?: Array<{ label: string; value: string }>
  /** For number fields */
  min?: number
  max?: number
  step?: number
  /** For text fields */
  minLength?: number
  maxLength?: number
  /** For relationship fields */
  relationTo?: string | string[]
  hasMany?: boolean
}

/**
 * UI metadata for a collection
 */
export interface CollectionUIMetadata {
  /** Display label override */
  label?: string
  /** Singular label */
  labelSingular?: string
  /** Description */
  description?: string
  /** Admin panel configuration */
  admin?: {
    /** Use as title field */
    useAsTitle?: string
    /** Default columns in list view */
    defaultColumns?: string[]
    /** Enable preview */
    preview?: boolean
    /** Hide from nav */
    hidden?: boolean
    /** Custom group */
    group?: string
  }
  /** Field-level UI metadata */
  fields?: Record<string, FieldUIMetadata>
}

/**
 * Studio metadata file structure (.studio/metadata.json)
 */
export interface StudioMetadata {
  /** Version of the metadata format */
  version: '1.0'
  /** Collection metadata */
  collections: Record<string, CollectionUIMetadata>
  /** Global settings */
  settings?: {
    /** Theme */
    theme?: 'light' | 'dark' | 'auto'
    /** Locale */
    locale?: string
    /** Date format */
    dateFormat?: string
  }
}

// =============================================================================
// Studio Configuration Types
// =============================================================================

/**
 * Configuration for the studio command
 */
export interface StudioConfig {
  /** Port to run the server on */
  port: number
  /** Directory containing Parquet files (or .db/) */
  dataDir: string
  /** Directory for UI metadata */
  metadataDir: string
  /** Whether to auto-discover collections */
  autoDiscover: boolean
  /** Explicit collection paths (if not auto-discovering) */
  collections?: string[]
  /** Authentication mode */
  auth: 'none' | 'local' | 'env'
  /** Local admin credentials (for auth: 'local') */
  adminEmail?: string
  adminPassword?: string
  /** Whether to run in read-only mode */
  readOnly: boolean
  /** Enable verbose logging */
  debug: boolean
  /** Theme: 'light' | 'dark' | 'auto' */
  theme?: 'light' | 'dark' | 'auto'
  /** Default sidebar fields */
  defaultSidebar?: string[]
}

/**
 * Default studio configuration
 */
export const DEFAULT_STUDIO_CONFIG: StudioConfig = {
  port: 3000,
  dataDir: '.db',
  metadataDir: '.studio',
  autoDiscover: true,
  auth: 'none',
  readOnly: false,
  debug: false,
}

// =============================================================================
// Server Types
// =============================================================================

/**
 * Studio server instance
 */
export interface StudioServer {
  /** Start the server */
  start(): Promise<void>
  /** Stop the server */
  stop(): Promise<void>
  /** Get the server URL */
  getUrl(): string
  /** Refresh collections (re-discover) */
  refresh(): Promise<void>
}
