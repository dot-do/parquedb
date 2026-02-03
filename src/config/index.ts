/**
 * ParqueDB Configuration
 *
 * Runtime detection, environment bindings, and auto-configuration.
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
  loadConfig,
  getConfig,
  setConfig,
  clearConfig,
  type ParqueDBConfig,
} from './loader'

// Auto-configured instances
export {
  db,
  sql,
  initializeDB,
  getDB,
  resetDB,
} from './auto'
