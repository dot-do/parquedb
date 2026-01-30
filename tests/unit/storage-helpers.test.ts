/**
 * Tests for the storage helper module
 */

import { describe, it, expect, afterAll, beforeEach } from 'vitest'
import {
  createTestFsBackend,
  cleanupTestStorage,
  hasR2Credentials,
  getTrackedTempDirs,
  registerAutoCleanup,
  createTestStorageBackend,
} from '../helpers/storage'
import type { FsBackend } from '../../src/storage/FsBackend'

describe('Storage Helpers', () => {
  // Register auto cleanup for this test suite
  registerAutoCleanup()

  describe('createTestFsBackend', () => {
    let backend: FsBackend

    beforeEach(async () => {
      backend = await createTestFsBackend()
    })

    it('should create a FsBackend instance', () => {
      expect(backend).toBeDefined()
      expect(backend.type).toBe('fs')
    })

    it('should create a unique temp directory', async () => {
      const backend2 = await createTestFsBackend()
      expect(backend.rootPath).not.toBe(backend2.rootPath)
    })

    it('should track temp directories for cleanup', async () => {
      const tracked = getTrackedTempDirs()
      expect(tracked).toContain(backend.rootPath)
    })

    it('should allow reading and writing files', async () => {
      const testData = 'Hello, World!'
      await backend.write('test.txt', new TextEncoder().encode(testData))

      const content = await backend.read('test.txt')
      expect(new TextDecoder().decode(content)).toBe(testData)
    })

    it('should allow custom prefix', async () => {
      const customBackend = await createTestFsBackend({ prefix: 'custom-test-' })
      expect(customBackend.rootPath).toContain('custom-test-')
    })
  })

  describe('hasR2Credentials', () => {
    it('should return a boolean', () => {
      const result = hasR2Credentials()
      expect(typeof result).toBe('boolean')
    })

    it('should check environment variables', () => {
      const hasCredentials = hasR2Credentials()

      // If credentials are set, hasR2Credentials should return true
      const envSet = !!(
        process.env.R2_ACCESS_KEY_ID &&
        process.env.R2_SECRET_ACCESS_KEY &&
        process.env.R2_URL
      )
      expect(hasCredentials).toBe(envSet)
    })
  })

  describe('createTestStorageBackend', () => {
    it('should create a FsBackend by default', async () => {
      const backend = await createTestStorageBackend()
      expect(backend.type).toBe('fs')
    })
  })

  describe('cleanupTestStorage', () => {
    it('should clear tracked directories', async () => {
      // Create a backend that we'll clean up
      await createTestFsBackend()
      const beforeCleanup = getTrackedTempDirs().length
      expect(beforeCleanup).toBeGreaterThan(0)

      await cleanupTestStorage()

      const afterCleanup = getTrackedTempDirs().length
      expect(afterCleanup).toBe(0)
    })
  })
})
