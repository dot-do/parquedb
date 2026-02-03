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
  typeInfo?: Record<string, unknown> | undefined
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
  lastModified?: Date | undefined
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
  label?: string | undefined
  /** Help text/description */
  description?: string | undefined
  /** Whether to hide from list view */
  hideInList?: boolean | undefined
  /** Whether to hide from form */
  hideInForm?: boolean | undefined
  /** Whether field is read-only */
  readOnly?: boolean | undefined
  /** Column width in list view */
  width?: number | undefined
  /** Sort order in form */
  order?: number | undefined
  /** Custom admin component */
  admin?: {
    position?: 'sidebar' | undefined
    width?: string | undefined
    condition?: string | undefined
  } | undefined
  /** For select fields: available options */
  options?: Array<{ label: string; value: string }> | undefined
  /** For number fields */
  min?: number | undefined
  max?: number | undefined
  step?: number | undefined
  /** For text fields */
  minLength?: number | undefined
  maxLength?: number | undefined
  /** For relationship fields */
  relationTo?: string | string[] | undefined
  hasMany?: boolean | undefined
}

/**
 * UI metadata for a collection
 */
export interface CollectionUIMetadata {
  /** Display label override */
  label?: string | undefined
  /** Singular label */
  labelSingular?: string | undefined
  /** Description */
  description?: string | undefined
  /** Admin panel configuration */
  admin?: {
    /** Use as title field */
    useAsTitle?: string | undefined
    /** Default columns in list view */
    defaultColumns?: string[] | undefined
    /** Enable preview */
    preview?: boolean | undefined
    /** Hide from nav */
    hidden?: boolean | undefined
    /** Custom group */
    group?: string | undefined
  } | undefined
  /** Field-level UI metadata */
  fields?: Record<string, FieldUIMetadata> | undefined
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
    theme?: 'light' | 'dark' | 'auto' | undefined
    /** Locale */
    locale?: string | undefined
    /** Date format */
    dateFormat?: string | undefined
  } | undefined
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
  collections?: string[] | undefined
  /** Authentication mode */
  auth: 'none' | 'local' | 'env' | 'oauth'
  /** Local admin credentials (for auth: 'local') */
  adminEmail?: string | undefined
  adminPassword?: string | undefined
  /** Whether to run in read-only mode */
  readOnly: boolean
  /** Enable verbose logging */
  debug: boolean
  /** Theme: 'light' | 'dark' | 'auto' */
  theme?: 'light' | 'dark' | 'auto' | undefined
  /** Default sidebar fields */
  defaultSidebar?: string[] | undefined

  // Multi-database configuration
  /** Enable multi-database mode (path-based routing) */
  multiDatabase?: {
    /** Enable multi-database mode */
    enabled: boolean
    /** Path prefix for admin routes (default: '/admin') */
    pathPrefix?: string | undefined
    /** WorkOS JWKS URI for oauth.do authentication */
    jwksUri?: string | undefined
    /** OAuth client ID */
    clientId?: string | undefined
    /** Default database ID (redirect if accessing /admin directly) */
    defaultDatabase?: string | undefined
  } | undefined
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
