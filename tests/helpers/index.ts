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
