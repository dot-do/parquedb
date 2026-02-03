/**
 * XSS Prevention Tests for Studio HTML Rendering
 *
 * Tests that user-provided content is properly escaped in HTML output
 * to prevent cross-site scripting (XSS) attacks.
 */

import { describe, it, expect } from 'vitest'
import { generateDatabaseSelectHtml, generateDatabaseNotFoundHtml } from '../../../src/studio/database'
import type { DatabaseInfo } from '../../../src/worker/DatabaseIndexDO'

describe('XSS Prevention', () => {
  describe('generateDatabaseSelectHtml', () => {
    it('escapes HTML special characters in database name', () => {
      const maliciousDatabases: DatabaseInfo[] = [{
        id: 'db_test',
        name: '<script>alert("xss")</script>',
        description: null,
        owner: 'test-user',
        slug: 'test-db',
        bucket: 'test-bucket',
        prefix: 'test/',
        createdAt: new Date(),
        updatedAt: new Date(),
        visibility: 'private',
        entityCount: 0,
      }]

      const html = generateDatabaseSelectHtml(maliciousDatabases, '/admin')

      // Should escape < and > characters
      expect(html).not.toContain('<script>alert("xss")</script>')
      expect(html).toContain('&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;')
    })

    it('escapes HTML special characters in database description', () => {
      const maliciousDatabases: DatabaseInfo[] = [{
        id: 'db_test',
        name: 'Test Database',
        description: '<img src=x onerror="alert(1)">',
        owner: 'test-user',
        slug: 'test-db',
        bucket: 'test-bucket',
        prefix: 'test/',
        createdAt: new Date(),
        updatedAt: new Date(),
        visibility: 'private',
        entityCount: 0,
      }]

      const html = generateDatabaseSelectHtml(maliciousDatabases, '/admin')

      // Should escape the malicious img tag
      expect(html).not.toContain('<img src=x onerror="alert(1)">')
      expect(html).toContain('&lt;img src=x onerror=&quot;alert(1)&quot;&gt;')
    })

    it('escapes ampersands in content', () => {
      const databases: DatabaseInfo[] = [{
        id: 'db_test',
        name: 'Test & Production',
        description: 'Development & Testing DB',
        owner: 'test-user',
        slug: 'test-db',
        bucket: 'test-bucket',
        prefix: 'test/',
        createdAt: new Date(),
        updatedAt: new Date(),
        visibility: 'private',
        entityCount: 0,
      }]

      const html = generateDatabaseSelectHtml(databases, '/admin')

      // Should escape ampersands
      expect(html).toContain('Test &amp; Production')
      expect(html).toContain('Development &amp; Testing DB')
    })

    it('escapes single quotes in content', () => {
      const databases: DatabaseInfo[] = [{
        id: 'db_test',
        name: "John's Database",
        description: "It's a test",
        owner: 'test-user',
        slug: 'test-db',
        bucket: 'test-bucket',
        prefix: 'test/',
        createdAt: new Date(),
        updatedAt: new Date(),
        visibility: 'private',
        entityCount: 0,
      }]

      const html = generateDatabaseSelectHtml(databases, '/admin')

      // Should escape single quotes
      expect(html).toContain('John&#039;s Database')
      expect(html).toContain('It&#039;s a test')
    })
  })

  describe('generateDatabaseNotFoundHtml', () => {
    it('escapes HTML special characters in database ID', () => {
      const maliciousId = '<script>alert("xss")</script>'

      const html = generateDatabaseNotFoundHtml(maliciousId, '/admin')

      // Should escape the malicious script
      expect(html).not.toContain('<script>alert("xss")</script>')
      expect(html).toContain('&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;')
    })

    it('escapes event handlers in database ID', () => {
      const maliciousId = '" onmouseover="alert(1)" data-foo="'

      const html = generateDatabaseNotFoundHtml(maliciousId, '/admin')

      // Should escape double quotes
      expect(html).not.toContain('" onmouseover="alert(1)"')
      expect(html).toContain('&quot; onmouseover=&quot;alert(1)&quot;')
    })

    it('escapes img tags with event handlers', () => {
      const maliciousId = '<img/src/onerror=alert(1)>'

      const html = generateDatabaseNotFoundHtml(maliciousId, '/admin')

      // Should escape < and > characters
      expect(html).not.toContain('<img/src/onerror=alert(1)>')
      expect(html).toContain('&lt;img/src/onerror=alert(1)&gt;')
    })
  })
})
