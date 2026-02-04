/**
 * E2E Tests: Studio Permissions/Auth Integration
 *
 * Tests authentication and authorization utilities with REAL storage backends (no mocks):
 * - Database routing utilities
 * - URL parsing and building
 * - Cookie utilities
 * - HTML generation for database selection
 *
 * Note: These tests verify the utilities work correctly with real inputs.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  createTestBackend,
  cleanupBackend,
  TEST_USER,
  ADMIN_USER,
  type BackendType,
} from './setup'
import type { StorageBackend } from '../../../src/types/storage'
import {
  parseRoute,
  isValidDatabaseId,
  buildDatabaseUrl,
  buildPublicDatabaseUrl,
  generateDatabaseSelectHtml,
  generateDatabaseNotFoundHtml,
} from '../../../src/studio/database'
import type { DatabaseInfo } from '../../../src/worker/DatabaseIndexDO'

describe('E2E: Studio Permissions/Auth Integration', () => {
  const backends: BackendType[] = ['memory', 'fs']

  for (const backendType of backends) {
    describe(`with ${backendType} backend`, () => {
      let storage: StorageBackend

      beforeEach(async () => {
        storage = await createTestBackend(backendType)
      })

      afterEach(async () => {
        await cleanupBackend(storage)
      })

      // =========================================================================
      // Route Parsing
      // =========================================================================

      describe('Route Parsing', () => {
        it('parses database ID from admin path', () => {
          const result = parseRoute('/admin/db_abc123/collections/posts')

          expect(result).not.toBeNull()
          expect(result!.databaseId).toBe('db_abc123')
          expect(result!.remainingPath).toBe('/collections/posts')
          expect(result!.fullPath).toBe('/admin/db_abc123/collections/posts')
        })

        it('parses database ID with root path', () => {
          const result = parseRoute('/admin/db_test/')

          expect(result).not.toBeNull()
          expect(result!.databaseId).toBe('db_test')
          expect(result!.remainingPath).toBe('/')
        })

        it('returns null for path without database ID', () => {
          expect(parseRoute('/admin/')).toBeNull()
          expect(parseRoute('/admin')).toBeNull()
        })

        it('returns null for non-admin paths', () => {
          expect(parseRoute('/api/users')).toBeNull()
          expect(parseRoute('/other/db_123')).toBeNull()
        })

        it('supports custom path prefix', () => {
          const result = parseRoute('/dashboard/db_123/settings', '/dashboard')

          expect(result).not.toBeNull()
          expect(result!.databaseId).toBe('db_123')
          expect(result!.remainingPath).toBe('/settings')
        })

        it('handles deeply nested paths', () => {
          const result = parseRoute('/admin/db_prod/api/v1/collections/users')

          expect(result).not.toBeNull()
          expect(result!.databaseId).toBe('db_prod')
          expect(result!.remainingPath).toBe('/api/v1/collections/users')
        })
      })

      // =========================================================================
      // Database ID Validation
      // =========================================================================

      describe('Database ID Validation', () => {
        it('accepts db_ prefixed IDs', () => {
          expect(isValidDatabaseId('db_123')).toBe(true)
          expect(isValidDatabaseId('db_abc')).toBe(true)
          expect(isValidDatabaseId('db_test_production')).toBe(true)
        })

        it('accepts valid slug IDs', () => {
          expect(isValidDatabaseId('my-database')).toBe(true)
          expect(isValidDatabaseId('test123')).toBe(true)
          expect(isValidDatabaseId('abc')).toBe(true)
        })

        it('accepts short alphanumeric IDs', () => {
          expect(isValidDatabaseId('a')).toBe(true)
          expect(isValidDatabaseId('ab')).toBe(true)
          expect(isValidDatabaseId('abc')).toBe(true)
        })

        it('rejects empty string', () => {
          expect(isValidDatabaseId('')).toBe(false)
        })

        it('rejects IDs starting with hyphen', () => {
          expect(isValidDatabaseId('-invalid')).toBe(false)
        })
      })

      // =========================================================================
      // URL Building
      // =========================================================================

      describe('URL Building', () => {
        it('builds URL with database ID and path', () => {
          expect(buildDatabaseUrl('db_123', '/collections/posts')).toBe(
            '/admin/db_123/collections/posts'
          )
        })

        it('builds URL with empty path', () => {
          expect(buildDatabaseUrl('db_123', '')).toBe('/admin/db_123/')
        })

        it('builds URL with default path', () => {
          expect(buildDatabaseUrl('db_123')).toBe('/admin/db_123/')
        })

        it('uses custom path prefix', () => {
          expect(buildDatabaseUrl('db_123', '/test', '/dashboard')).toBe(
            '/dashboard/db_123/test'
          )
        })

        it('builds public URL with owner and slug', () => {
          expect(buildPublicDatabaseUrl('john', 'my-project', '/settings')).toBe(
            '/admin/john/my-project/settings'
          )
        })

        it('builds public URL with empty path', () => {
          expect(buildPublicDatabaseUrl('john', 'my-project')).toBe(
            '/admin/john/my-project/'
          )
        })
      })

      // =========================================================================
      // HTML Generation
      // =========================================================================

      describe('HTML Generation', () => {
        const mockDatabases: DatabaseInfo[] = [
          {
            id: 'db_001',
            name: 'Production',
            description: 'Main production database',
            owner: TEST_USER.id,
            slug: 'prod',
            bucket: 'test-bucket',
            prefix: 'prod/',
            createdAt: new Date('2024-01-01'),
            updatedAt: new Date('2024-01-15'),
            visibility: 'private',
            entityCount: 1000,
          },
          {
            id: 'db_002',
            name: 'Development',
            description: null,
            owner: TEST_USER.id,
            slug: 'dev',
            bucket: 'test-bucket',
            prefix: 'dev/',
            createdAt: new Date('2024-01-10'),
            updatedAt: new Date('2024-01-10'),
            visibility: 'private',
            entityCount: 50,
          },
        ]

        it('generates database selection HTML', () => {
          const html = generateDatabaseSelectHtml(mockDatabases)

          expect(html).toContain('Production')
          expect(html).toContain('Development')
          expect(html).toContain('/admin/db_001')
          expect(html).toContain('/admin/db_002')
          expect(html).toContain('Select Database')
        })

        it('includes entity counts', () => {
          const html = generateDatabaseSelectHtml(mockDatabases)

          expect(html).toContain('1000 entities')
          expect(html).toContain('50 entities')
        })

        it('includes description when present', () => {
          const html = generateDatabaseSelectHtml(mockDatabases)

          expect(html).toContain('Main production database')
        })

        it('shows empty state message', () => {
          const html = generateDatabaseSelectHtml([])

          expect(html).toContain('No databases yet')
        })

        it('uses custom path prefix', () => {
          const html = generateDatabaseSelectHtml(mockDatabases, '/dashboard')

          expect(html).toContain('/dashboard/db_001')
          expect(html).toContain('/dashboard/db_002')
        })

        it('generates not found HTML', () => {
          const html = generateDatabaseNotFoundHtml('db_missing')

          expect(html).toContain('Database Not Found')
          expect(html).toContain('db_missing')
          expect(html).toContain('Back to Database List')
        })

        it('escapes HTML in database names (XSS prevention)', () => {
          const xssDatabases: DatabaseInfo[] = [
            {
              id: 'db_xss',
              name: '<script>alert("xss")</script>',
              description: '<img src="x" onerror="alert(1)">',
              owner: TEST_USER.id,
              slug: 'xss',
              bucket: 'test',
              prefix: '/',
              createdAt: new Date(),
              updatedAt: new Date(),
              visibility: 'private',
            },
          ]

          const html = generateDatabaseSelectHtml(xssDatabases)

          expect(html).not.toContain('<script>')
          expect(html).toContain('&lt;script&gt;')
          expect(html).not.toContain('<img')
          expect(html).toContain('&lt;img')
        })

        it('escapes HTML in not found page (XSS prevention)', () => {
          const html = generateDatabaseNotFoundHtml('<script>alert(1)</script>')

          expect(html).not.toContain('<script>alert(1)</script>')
          expect(html).toContain('&lt;script&gt;')
        })
      })

      // =========================================================================
      // Cookie Utilities
      // =========================================================================

      describe('Cookie Utilities', () => {
        it('parses cookies from header', async () => {
          const { parseCookies } = await import('../../../src/studio/context')

          const cookies = parseCookies('foo=bar; baz=qux; test=value')

          expect(cookies.foo).toBe('bar')
          expect(cookies.baz).toBe('qux')
          expect(cookies.test).toBe('value')
        })

        it('handles empty cookie string', async () => {
          const { parseCookies } = await import('../../../src/studio/context')

          const cookies = parseCookies('')

          expect(Object.keys(cookies).length).toBe(0)
        })

        it('handles cookies with special characters', async () => {
          const { parseCookies } = await import('../../../src/studio/context')

          // URL-encoded cookies are decoded by parseCookies
          const cookies = parseCookies('encoded=%3D%26%3B')

          // The decoded value: %3D -> '=', %26 -> '&', %3B -> ';'
          expect(cookies.encoded).toBe('=&;')
        })

        it('builds set-cookie header', async () => {
          const { buildSetCookie } = await import('../../../src/studio/context')

          const header = buildSetCookie('test', 'value', { maxAge: 3600 })

          expect(header).toContain('test=value')
          expect(header).toContain('Max-Age=3600')
        })

        it('builds clear-cookie header', async () => {
          const { buildClearCookie } = await import('../../../src/studio/context')

          const header = buildClearCookie('test')

          expect(header).toContain('test=')
          expect(header).toContain('Max-Age=0')
        })

        it('has database cookie constant', async () => {
          const { PAYLOAD_DATABASE_COOKIE } = await import('../../../src/studio/context')

          expect(PAYLOAD_DATABASE_COOKIE).toBeDefined()
          expect(typeof PAYLOAD_DATABASE_COOKIE).toBe('string')
        })
      })

      // =========================================================================
      // User Context
      // =========================================================================

      describe('User Context', () => {
        it('test user has required fields', () => {
          expect(TEST_USER.id).toBeDefined()
          expect(TEST_USER.email).toBeDefined()
          expect(TEST_USER.name).toBeDefined()
        })

        it('admin user has required fields', () => {
          expect(ADMIN_USER.id).toBeDefined()
          expect(ADMIN_USER.email).toBeDefined()
          expect(ADMIN_USER.name).toBeDefined()
        })

        it('users have distinct IDs', () => {
          expect(TEST_USER.id).not.toBe(ADMIN_USER.id)
        })
      })

      // =========================================================================
      // Storage Isolation
      // =========================================================================

      describe('Storage Isolation', () => {
        it('maintains data isolation between prefixes', async () => {
          // Write data to different "user" prefixes
          await storage.write('user1/data.json', new TextEncoder().encode('{"user": "1"}'))
          await storage.write('user2/data.json', new TextEncoder().encode('{"user": "2"}'))

          // Read back and verify
          const user1Data = await storage.read('user1/data.json')
          const user2Data = await storage.read('user2/data.json')

          expect(new TextDecoder().decode(user1Data)).toBe('{"user": "1"}')
          expect(new TextDecoder().decode(user2Data)).toBe('{"user": "2"}')
        })

        it('prefix deletion does not affect other prefixes', async () => {
          await storage.write('prefix1/file.txt', new Uint8Array([1]))
          await storage.write('prefix2/file.txt', new Uint8Array([2]))

          await storage.deletePrefix('prefix1/')

          expect(await storage.exists('prefix1/file.txt')).toBe(false)
          expect(await storage.exists('prefix2/file.txt')).toBe(true)
        })
      })
    })
  }
})
