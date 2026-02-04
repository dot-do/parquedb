/**
 * E2E Studio Test Setup
 *
 * Provides utilities for testing ParqueDB Studio with REAL storage backends.
 * NO MOCKS - tests exercise the full code path including:
 * - File I/O (MemoryBackend or FsBackend)
 * - Parquet file reading/writing
 * - Schema discovery
 * - Database lifecycle operations
 */

import type { StorageBackend } from '../../../src/types/storage'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { FsBackend } from '../../../src/storage/FsBackend'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promises as fs } from 'node:fs'

// =============================================================================
// Storage Backend Factory
// =============================================================================

export type BackendType = 'memory' | 'fs'

let testCounter = 0

/**
 * Create a real storage backend for testing
 * NO MOCKS - these are actual implementations
 */
export async function createTestBackend(type: BackendType = 'memory'): Promise<StorageBackend> {
  if (type === 'memory') {
    return new MemoryBackend()
  }

  // Create a unique temp directory for FsBackend tests
  const testDir = join(tmpdir(), `parquedb-studio-e2e-${Date.now()}-${++testCounter}`)
  await fs.mkdir(testDir, { recursive: true })
  return new FsBackend(testDir)
}

/**
 * Clean up a storage backend after tests
 */
export async function cleanupBackend(backend: StorageBackend): Promise<void> {
  if (backend.type === 'fs') {
    const fsBackend = backend as FsBackend
    try {
      await fs.rm(fsBackend.rootPath, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  }
  // MemoryBackend doesn't need cleanup
}

// =============================================================================
// Test Data Helpers
// =============================================================================

/**
 * Sample Parquet file schema for testing
 * This is the structure of a minimal valid Parquet file
 */
export interface TestEntity {
  $id: string
  $type: string
  name: string
  createdAt: Date
  updatedAt: Date
  version: number
  [key: string]: unknown
}

/**
 * Verify an entity has expected audit fields
 */
export function assertAuditFields(entity: unknown): asserts entity is TestEntity {
  if (!entity || typeof entity !== 'object') {
    throw new Error('Entity is not an object')
  }
  const e = entity as Record<string, unknown>
  if (!e.$id || typeof e.$id !== 'string') {
    throw new Error('Entity missing $id')
  }
  if (!e.$type || typeof e.$type !== 'string') {
    throw new Error('Entity missing $type')
  }
  if (!e.name || typeof e.name !== 'string') {
    throw new Error('Entity missing name')
  }
  if (!e.createdAt || !(e.createdAt instanceof Date)) {
    throw new Error('Entity missing or invalid createdAt')
  }
  if (!e.updatedAt || !(e.updatedAt instanceof Date)) {
    throw new Error('Entity missing or invalid updatedAt')
  }
  if (e.version === undefined || typeof e.version !== 'number') {
    throw new Error('Entity missing version')
  }
}

// =============================================================================
// Mock User/Auth Context for Testing
// =============================================================================

/**
 * Test user for auth-related tests
 */
export interface TestUser {
  id: string
  email: string
  name: string
}

export const TEST_USER: TestUser = {
  id: 'test_user_001',
  email: 'test@example.com',
  name: 'Test User',
}

export const ADMIN_USER: TestUser = {
  id: 'admin_user_001',
  email: 'admin@example.com',
  name: 'Admin User',
}

// =============================================================================
// Error Simulation Helpers
// =============================================================================

/**
 * Create a corrupted Parquet file (invalid magic bytes)
 */
export async function writeCorruptedParquetFile(
  backend: StorageBackend,
  path: string
): Promise<void> {
  // Valid Parquet files start with PAR1 magic bytes
  // This creates an invalid file with wrong magic
  const corruptedData = new Uint8Array([
    0x00, 0x00, 0x00, 0x00, // Invalid magic
    0xFF, 0xFF, 0xFF, 0xFF, // Garbage data
    0x50, 0x41, 0x52, 0x31, // PAR1 at wrong position
  ])
  await backend.write(path, corruptedData)
}

/**
 * Create a truncated Parquet file (incomplete footer)
 */
export async function writeTruncatedParquetFile(
  backend: StorageBackend,
  path: string
): Promise<void> {
  // Parquet files end with PAR1 magic, footer length (4 bytes), and PAR1
  // This creates a file with valid start but truncated footer
  const truncatedData = new Uint8Array([
    0x50, 0x41, 0x52, 0x31, // PAR1 magic at start
    0x00, 0x00, 0x00, 0x00, // Some content
    // Missing footer - file ends abruptly
  ])
  await backend.write(path, truncatedData)
}

/**
 * Create an empty file (0 bytes)
 */
export async function writeEmptyFile(
  backend: StorageBackend,
  path: string
): Promise<void> {
  await backend.write(path, new Uint8Array(0))
}

// =============================================================================
// Timing Helpers
// =============================================================================

/**
 * Wait for a specified duration (for real async operations)
 */
export async function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Retry an operation until it succeeds or times out
 */
export async function retry<T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 100
): Promise<T> {
  let lastError: Error | undefined
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation()
    } catch (error) {
      lastError = error as Error
      if (i < maxRetries - 1) {
        await wait(delayMs)
      }
    }
  }
  throw lastError
}
