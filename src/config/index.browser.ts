/**
 * ParqueDB Configuration (Browser Build)
 *
 * Browser-safe configuration module that excludes oauth.do dependencies.
 * Uses auth.browser.ts instead of auth.ts.
 */

// Existing exports
export * from './dataset'

// Runtime detection
export {
  detectRuntime,
  getRuntimeInfo,
  isServer,
  isWorkers,
  isBrowser,
  type Runtime,
  type RuntimeInfo,
} from './runtime'

// Environment and bindings
export {
  loadWorkersEnv,
  getWorkersEnv,
  setWorkersEnv,
  detectBindings,
  detectStoragePaths,
  ensureDataDir,
  type AvailableBindings,
  type StoragePaths,
} from './env'

// Config file loading
export {
  defineConfig,
  defineSchema,
  loadConfig,
  getConfig,
  setConfig,
  clearConfig,
  type ParqueDBConfig,
  type StudioConfig,
  type CollectionStudioConfig,
  type FieldStudioConfig,
  type LayoutConfig,
} from './loader'

// Studio configuration utilities
export {
  extractCollectionStudio,
  extractSchemaStudio,
  mergeStudioConfig,
  getSchemaFields,
  isFieldDefinition,
  normalizeOptions,
  layoutHasTabs,
  normalizeRow,
} from './studio'

// Auto-configured instances
export {
  db,
  sql,
  initializeDB,
  getDB,
  resetDB,
} from './auto.browser'

// Authentication integration (browser-safe stubs)
export {
  setActorResolver,
  getActorResolver,
  resolveActor,
  createOAuthActorResolver,
  createEnvActorResolver,
  createStaticActorResolver,
  createCombinedActorResolver,
  createAuthContext,
  type ActorResolver,
  type AuthContext,
} from './auth.browser'
