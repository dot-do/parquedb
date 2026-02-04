/**
 * Test Helpers Module
 *
 * Re-exports all test helper utilities for easy import.
 *
 * @example
 * ```typescript
 * import {
 *   createTestFsBackend,
 *   createTestR2Backend,
 *   cleanupTestStorage,
 *   registerAutoCleanup,
 *   createTestContext,
 * } from '../helpers'
 * ```
 */

// Storage helpers
export {
  // Factory functions
  createTestFsBackend,
  createTestR2Backend,
  createTestStorageBackend,
  createFsBackendWithPath,

  // Environment checks
  hasR2Credentials,

  // Cleanup functions
  cleanupTestStorage,
  cleanupFsBackend,
  cleanupR2Backend,
  registerAutoCleanup,

  // Debug utilities
  getTrackedTempDirs,
  getTrackedR2Prefixes,

  // Types
  type TestStorageConfig,
} from './storage'

// Temp directory utilities (preferred for tests with ParqueDB instances)
export {
  createTestContext,
  createMultipleTestContexts,
  cleanupAllTestContexts,
  createIsolatedTempDir,
  cleanupTempDir,
  createIsolatedStorage,
  getActiveContextCount,

  // Types
  type TestContext,
  type TestContextOptions,
  type AsyncDisposable,
  type SyncDisposable,
} from './temp-dir'
