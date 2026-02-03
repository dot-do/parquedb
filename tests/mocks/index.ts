/**
 * Test Mocks - Factory functions for creating test doubles
 *
 * This module provides mock factories for commonly mocked objects in tests.
 * Using these factories instead of inline vi.fn() makes tests more maintainable
 * and ensures consistent mock behavior across the test suite.
 *
 * @example
 * ```typescript
 * import { createMockStorageBackend, createMockR2Bucket } from '../mocks'
 *
 * const storage = createMockStorageBackend()
 * const bucket = createMockR2Bucket()
 * ```
 */

// Re-export all mock factories
export * from './storage'
export * from './r2-bucket'
export * from './kv-namespace'
export * from './durable-object'
export * from './fetch'
export * from './execution-context'
export * from './worker'
