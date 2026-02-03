/**
 * Tests for src/studio/payload-config.ts
 *
 * Tests Payload CMS configuration factory.
 */

import { describe, it, expect, vi } from 'vitest'
import {
  createPayloadConfig,
  createDevConfig,
  getComponentPaths,
  generateWrapperFile,
  generateAllWrapperFiles,
} from '../../../src/studio/payload-config'

describe('payload-config', () => {
  describe('createPayloadConfig', () => {
    it('creates config with required secret', () => {
      const result = createPayloadConfig({
        secret: 'my-secret-key',
      })

      expect(result.secret).toBe('my-secret-key')
    })

    it('sets default admin user to users', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
      })

      expect(result.admin.user).toBe('users')
    })

    it('sets app name in title suffix', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        admin: {
          appName: 'My App',
        },
      })

      expect(result.admin.meta.titleSuffix).toBe(' - My App')
    })

    it('uses default app name when not provided', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
      })

      expect(result.admin.meta.titleSuffix).toBe(' - ParqueDB Studio')
    })

    it('configures storage when provided', () => {
      const mockStorage = {
        read: vi.fn(),
        write: vi.fn(),
        exists: vi.fn(),
      }

      const result = createPayloadConfig({
        secret: 'my-secret',
        storage: mockStorage as any,
      })

      expect(result.db).toBeDefined()
      expect(result.db.adapter).toBe('parquedb')
      expect(result.db.storage).toBe(mockStorage)
    })

    it('sets debug mode in db config', () => {
      const mockStorage = { read: vi.fn(), write: vi.fn(), exists: vi.fn() }

      const result = createPayloadConfig({
        secret: 'my-secret',
        storage: mockStorage as any,
        debug: true,
      })

      expect(result.db.debug).toBe(true)
    })

    it('configures multi-database mode with component paths', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        multiDatabase: true,
        componentPaths: {
          Dashboard: '/src/components/Dashboard',
        },
      })

      expect(result.admin.components.views).toBeDefined()
      expect(result.admin.components.views.Dashboard.Component).toBe('/src/components/Dashboard')
    })

    it('does not configure views when multiDatabase is false', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        multiDatabase: false,
        componentPaths: {
          Dashboard: '/src/components/Dashboard',
        },
      })

      expect(result.admin.components.views).toBeUndefined()
    })

    it('does not configure views when no Dashboard path', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        multiDatabase: true,
        componentPaths: {},
      })

      expect(result.admin.components.views).toBeUndefined()
    })

    it('configures graphics with Logo path', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        componentPaths: {
          Logo: '/src/components/Logo',
        },
      })

      expect(result.admin.components.graphics?.Logo?.path).toBe('/src/components/Logo')
    })

    it('configures graphics with Icon path', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        componentPaths: {
          Icon: '/src/components/Icon',
        },
      })

      expect(result.admin.components.graphics?.Icon?.path).toBe('/src/components/Icon')
    })

    it('configures graphics with logoUrl', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        admin: {
          logoUrl: 'https://example.com/logo.png',
        },
      })

      expect(result.admin.components.graphics?.Logo?.path).toBe('https://example.com/logo.png')
      expect(result.admin.components.graphics?.Icon?.path).toBe('https://example.com/logo.png')
    })

    it('prefers componentPaths over logoUrl', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        componentPaths: {
          Logo: '/src/components/Logo',
        },
        admin: {
          logoUrl: 'https://example.com/logo.png',
        },
      })

      expect(result.admin.components.graphics?.Logo?.path).toBe('/src/components/Logo')
    })

    it('configures OAuth users collection', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        oauth: {
          jwksUri: 'https://api.workos.com/sso/jwks/client_xxx',
        },
      })

      expect(result.collections).toHaveLength(1)
      expect(result.collections[0].slug).toBe('users')
      expect(result.collections[0].auth.disableLocalStrategy).toBe(true)
    })

    it('includes required fields in OAuth users collection', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        oauth: {
          jwksUri: 'https://api.workos.com/sso/jwks/client_xxx',
        },
      })

      const usersCollection = result.collections[0]
      const fieldNames = usersCollection.fields.map((f: any) => f.name)

      expect(fieldNames).toContain('email')
      expect(fieldNames).toContain('name')
      expect(fieldNames).toContain('externalId')
      expect(fieldNames).toContain('roles')
    })

    it('configures studio settings', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        studio: {
          dataDir: 'custom/.db',
          metadataDir: 'custom/.studio',
          readOnly: true,
        },
      })

      expect(result.studio.dataDir).toBe('custom/.db')
      expect(result.studio.metadataDir).toBe('custom/.studio')
      expect(result.studio.readOnly).toBe(true)
    })

    it('uses default studio directories', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
        studio: {},
      })

      expect(result.studio.dataDir).toBe('.db')
      expect(result.studio.metadataDir).toBe('.studio')
      expect(result.studio.readOnly).toBe(false)
    })

    it('returns empty collections array when no oauth', () => {
      const result = createPayloadConfig({
        secret: 'my-secret',
      })

      expect(result.collections).toEqual([])
    })
  })

  describe('createDevConfig', () => {
    it('creates config with dev secret', () => {
      const result = createDevConfig()

      expect(result.secret).toBe('dev-secret-change-in-production')
    })

    it('uses default data directory', () => {
      const result = createDevConfig()

      expect(result.db.dataDir).toBe('.db')
    })

    it('uses custom data directory', () => {
      const result = createDevConfig('./custom-data')

      expect(result.db.dataDir).toBe('./custom-data')
    })

    it('sets adapter to parquedb', () => {
      const result = createDevConfig()

      expect(result.db.adapter).toBe('parquedb')
    })

    it('sets admin user to users', () => {
      const result = createDevConfig()

      expect(result.admin.user).toBe('users')
    })

    it('returns empty collections array', () => {
      const result = createDevConfig()

      expect(result.collections).toEqual([])
    })
  })

  describe('getComponentPaths', () => {
    it('generates paths for all components', () => {
      const result = getComponentPaths('/src/components/parquedb')

      expect(result.Dashboard).toBe('/src/components/parquedb/Dashboard')
      expect(result.DatabaseSelector).toBe('/src/components/parquedb/DatabaseSelector')
      expect(result.Logo).toBe('/src/components/parquedb/Logo')
      expect(result.Icon).toBe('/src/components/parquedb/Icon')
    })

    it('handles trailing slash in base directory', () => {
      const result = getComponentPaths('/src/components/parquedb/')

      expect(result.Dashboard).toBe('/src/components/parquedb/Dashboard')
      expect(result.DatabaseSelector).toBe('/src/components/parquedb/DatabaseSelector')
    })

    it('works with nested paths', () => {
      const result = getComponentPaths('/src/app/admin/components/parquedb')

      expect(result.Dashboard).toBe('/src/app/admin/components/parquedb/Dashboard')
    })
  })

  describe('generateWrapperFile', () => {
    it('generates wrapper for DatabaseDashboardView', () => {
      const result = generateWrapperFile('DatabaseDashboardView')

      expect(result).toContain("'use client'")
      expect(result).toContain('export { DatabaseDashboardView as default }')
      expect(result).toContain("from 'parquedb/studio'")
    })

    it('generates wrapper for DatabaseSelectView', () => {
      const result = generateWrapperFile('DatabaseSelectView')

      expect(result).toContain('export { DatabaseSelectView as default }')
    })

    it('generates wrapper for DatabaseCard', () => {
      const result = generateWrapperFile('DatabaseCard')

      expect(result).toContain('export { DatabaseCard as default }')
    })

    it('generates wrapper for CreateDatabaseModal', () => {
      const result = generateWrapperFile('CreateDatabaseModal')

      expect(result).toContain('export { CreateDatabaseModal as default }')
    })

    it('includes documentation comment', () => {
      const result = generateWrapperFile('DatabaseDashboardView')

      expect(result).toContain('ParqueDB Studio Component Wrapper')
      expect(result).toContain('re-exports a parquedb component')
    })
  })

  describe('generateAllWrapperFiles', () => {
    it('generates all wrapper files', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb')

      expect(result.size).toBe(4)
    })

    it('generates Dashboard wrapper', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb')

      expect(result.has('/src/components/parquedb/Dashboard.tsx')).toBe(true)
      expect(result.get('/src/components/parquedb/Dashboard.tsx')).toContain('DatabaseDashboardView')
    })

    it('generates DatabaseSelector wrapper', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb')

      expect(result.has('/src/components/parquedb/DatabaseSelector.tsx')).toBe(true)
      expect(result.get('/src/components/parquedb/DatabaseSelector.tsx')).toContain('DatabaseSelectView')
    })

    it('generates DatabaseCard wrapper', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb')

      expect(result.has('/src/components/parquedb/DatabaseCard.tsx')).toBe(true)
      expect(result.get('/src/components/parquedb/DatabaseCard.tsx')).toContain('DatabaseCard')
    })

    it('generates CreateDatabaseModal wrapper', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb')

      expect(result.has('/src/components/parquedb/CreateDatabaseModal.tsx')).toBe(true)
      expect(result.get('/src/components/parquedb/CreateDatabaseModal.tsx')).toContain('CreateDatabaseModal')
    })

    it('handles trailing slash in base directory', () => {
      const result = generateAllWrapperFiles('/src/components/parquedb/')

      expect(result.has('/src/components/parquedb/Dashboard.tsx')).toBe(true)
    })
  })
})
