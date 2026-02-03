/**
 * Tests for ShredConfig in IcebergBackendConfig
 *
 * Validates that the ShredConfig interface is properly typed and can be used
 * in IcebergBackendConfig to configure variant shredding behavior.
 */

import { describe, it, expect } from 'vitest'
import type { IcebergBackendConfig, ShredConfig } from '../../../src/backends/types'
import { createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

describe('ShredConfig', () => {
  describe('type definitions', () => {
    it('should accept ShredConfig with fields array', () => {
      const config: ShredConfig = {
        fields: ['status', 'email', 'metadata.type'],
      }

      expect(config.fields).toEqual(['status', 'email', 'metadata.type'])
      expect(config.autoDetect).toBeUndefined()
      expect(config.autoDetectThreshold).toBeUndefined()
    })

    it('should accept ShredConfig with autoDetect enabled', () => {
      const config: ShredConfig = {
        autoDetect: true,
      }

      expect(config.autoDetect).toBe(true)
      expect(config.fields).toBeUndefined()
    })

    it('should accept ShredConfig with autoDetect and custom threshold', () => {
      const config: ShredConfig = {
        autoDetect: true,
        autoDetectThreshold: 25,
      }

      expect(config.autoDetect).toBe(true)
      expect(config.autoDetectThreshold).toBe(25)
    })

    it('should accept ShredConfig with all options', () => {
      const config: ShredConfig = {
        fields: ['status', 'priority'],
        autoDetect: true,
        autoDetectThreshold: 50,
      }

      expect(config.fields).toEqual(['status', 'priority'])
      expect(config.autoDetect).toBe(true)
      expect(config.autoDetectThreshold).toBe(50)
    })

    it('should accept empty ShredConfig', () => {
      const config: ShredConfig = {}

      expect(config.fields).toBeUndefined()
      expect(config.autoDetect).toBeUndefined()
      expect(config.autoDetectThreshold).toBeUndefined()
    })
  })

  describe('IcebergBackendConfig with shredding', () => {
    it('should accept IcebergBackendConfig without shredding', () => {
      const storage = new MemoryBackend()
      const config: IcebergBackendConfig = {
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
      }

      expect(config.shredding).toBeUndefined()
    })

    it('should accept IcebergBackendConfig with shredding fields', () => {
      const storage = new MemoryBackend()
      const config: IcebergBackendConfig = {
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        shredding: {
          fields: ['status', 'email'],
        },
      }

      expect(config.shredding?.fields).toEqual(['status', 'email'])
    })

    it('should accept IcebergBackendConfig with shredding autoDetect', () => {
      const storage = new MemoryBackend()
      const config: IcebergBackendConfig = {
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        shredding: {
          autoDetect: true,
          autoDetectThreshold: 15,
        },
      }

      expect(config.shredding?.autoDetect).toBe(true)
      expect(config.shredding?.autoDetectThreshold).toBe(15)
    })

    it('should accept IcebergBackendConfig with complete shredding config', () => {
      const storage = new MemoryBackend()
      const config: IcebergBackendConfig = {
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        catalog: { type: 'filesystem' },
        readOnly: false,
        shredding: {
          fields: ['status', 'category', 'user.role'],
          autoDetect: true,
          autoDetectThreshold: 20,
        },
      }

      expect(config.type).toBe('iceberg')
      expect(config.warehouse).toBe('warehouse')
      expect(config.database).toBe('testdb')
      expect(config.catalog).toEqual({ type: 'filesystem' })
      expect(config.readOnly).toBe(false)
      expect(config.shredding?.fields).toEqual(['status', 'category', 'user.role'])
      expect(config.shredding?.autoDetect).toBe(true)
      expect(config.shredding?.autoDetectThreshold).toBe(20)
    })
  })

  describe('createIcebergBackend with shredding', () => {
    it('should create backend with shredding config', async () => {
      const storage = new MemoryBackend()
      const backend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        shredding: {
          fields: ['status', 'priority'],
        },
      })

      expect(backend).toBeDefined()
      expect(backend.type).toBe('iceberg')

      // Clean up
      await backend.close()
    })

    it('should create backend with autoDetect shredding', async () => {
      const storage = new MemoryBackend()
      const backend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        shredding: {
          autoDetect: true,
          autoDetectThreshold: 5,
        },
      })

      expect(backend).toBeDefined()
      expect(backend.type).toBe('iceberg')

      // Clean up
      await backend.close()
    })
  })
})
