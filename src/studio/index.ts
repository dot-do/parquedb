/**
 * ParqueDB Studio
 *
 * A Payload CMS-based admin interface for viewing and editing Parquet files.
 *
 * ## Features
 *
 * - **Auto-discovery**: Automatically detects Parquet files in `.db/` directory
 * - **Schema inference**: Generates Payload collections from Parquet schemas
 * - **UI customization**: Separate metadata layer (`.studio/`) for field rendering
 * - **Multiple modes**:
 *   - Schema-full: ParqueDB-managed files with relationships
 *   - Dynamic: Raw Parquet files with inferred types
 *
 * ## Usage
 *
 * ```bash
 * # Start studio with auto-discovery
 * npx parquedb studio
 *
 * # Specify data directory
 * npx parquedb studio ./my-data
 *
 * # Read-only mode
 * npx parquedb studio --read-only
 *
 * # Custom port
 * npx parquedb studio --port 8080
 * ```
 *
 * ## Programmatic Usage
 *
 * ```typescript
 * import { createStudioServer, discoverCollections } from 'parquedb/studio'
 * import { FileSystemBackend } from 'parquedb'
 *
 * const storage = new FileSystemBackend('./data')
 * const collections = await discoverCollections(storage, '.db')
 *
 * const server = await createStudioServer({
 *   port: 3000,
 *   dataDir: '.db',
 *   metadataDir: '.studio',
 *   autoDiscover: true,
 *   auth: 'none',
 *   readOnly: false,
 *   debug: false,
 * }, storage)
 *
 * await server.start()
 * ```
 *
 * @module
 */

// Types
export type {
  StudioConfig,
  StudioServer,
  StudioMetadata,
  DiscoveredCollection,
  DiscoveredField,
  CollectionUIMetadata,
  FieldUIMetadata,
  PayloadFieldType,
} from './types'

export { DEFAULT_STUDIO_CONFIG } from './types'

// Discovery
export {
  discoverCollections,
  discoverCollection,
  extractFields,
  schemaElementToField,
  slugToLabel,
  findTitleField,
  findDefaultColumns,
} from './discovery'

// Collections
export {
  generateCollection,
  generateField,
  generateCollections,
  formatFieldLabel,
  inferRelationships,
  type PayloadFieldConfig,
  type PayloadCollectionConfig,
} from './collections'

// Metadata
export {
  loadMetadata,
  saveMetadata,
  createDefaultMetadata,
  generateCollectionMetadata,
  mergeMetadata,
  updateCollectionMetadata,
  updateFieldMetadata,
  validateMetadata,
} from './metadata'

// Server
export {
  createStudioServer,
  printDiscoverySummary,
} from './server'

// Database routing (multi-database mode)
export {
  parseRoute,
  isValidDatabaseId,
  buildDatabaseUrl,
  buildPublicDatabaseUrl,
  resolveDatabase,
  databaseMiddleware,
  generateDatabaseSelectHtml,
  generateDatabaseNotFoundHtml,
  type DatabaseContext,
  type ParsedRoute,
  type DatabaseRoutingConfig,
} from './database'

// Database context management (cookie-based)
export {
  // Constants
  PAYLOAD_DATABASE_COOKIE,
  DEFAULT_COOKIE_MAX_AGE,
  // Cookie utilities
  parseCookies,
  buildSetCookie,
  buildClearCookie,
  // Context functions
  getDatabaseContext,
  getCookieDatabaseId,
  setDatabaseContext,
  clearDatabaseContext,
  // Middleware
  databaseContextMiddleware,
  requireDatabaseContext,
  autoSelectDatabase,
  // Types
  type DatabaseContextData,
  type DatabaseContextVariables,
  type DatabaseContextConfig,
  type CookieOptions,
  type HonoWithDatabaseContext,
} from './context'

// React Components (for Payload admin UI)
export {
  DatabaseCard,
  DatabaseDashboard,
  CreateDatabaseModal,
  CloneDatabaseModal,
  QuickSwitcher,
  ConfirmationDialog,
  SettingsPage,
  DatabaseDashboardView,
  DatabaseSelectView,
  SettingsView,
  type DatabaseCardProps,
  type DatabaseDashboardProps,
  type CreateDatabaseModalProps,
  type CloneDatabaseModalProps,
  type QuickSwitcherProps,
  type ConfirmationDialogProps,
  type ConfirmationVariant,
  type SettingsPageProps,
  type StudioSettings,
  type DatabaseDashboardViewProps,
  type DatabaseSelectViewProps,
  type SettingsViewProps,
  // Hooks
  useKeyboardNavigation,
  type UseKeyboardNavigationOptions,
  type UseKeyboardNavigationResult,
} from './components'

// Payload Configuration Factory
export {
  createPayloadConfig,
  createDevConfig,
  generatePayloadCollections,
  getComponentPaths,
  generateWrapperFile,
  generateAllWrapperFiles,
  type PayloadConfigOptions,
  type ComponentPaths,
} from './payload-config'

// API Routes (Hono handlers for database dashboard)
export {
  createDatabaseRoutes,
  databaseRoutes,
  requireAuthUser,
  requireActor,
  type DatabaseApiEnv,
  type DatabaseApiVariables,
  type CreateDatabaseBody,
  type UpdateDatabaseBody,
  type ListDatabasesResponse,
  type DeleteDatabaseResponse,
} from './api'
