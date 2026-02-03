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

// React Components (for Payload admin UI)
export {
  DatabaseCard,
  DatabaseDashboard,
  CreateDatabaseModal,
  DatabaseDashboardView,
  DatabaseSelectView,
  type DatabaseCardProps,
  type DatabaseDashboardProps,
  type CreateDatabaseModalProps,
  type DatabaseDashboardViewProps,
  type DatabaseSelectViewProps,
} from './components'

// Payload Configuration Factory
export {
  createPayloadConfig,
  createDevConfig,
  generatePayloadCollections,
  type PayloadConfigOptions,
} from './payload-config'
