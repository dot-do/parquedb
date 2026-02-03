/**
 * Tests for src/studio/server.ts
 *
 * Tests the Studio server creation and utilities.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  printDiscoverySummary,
} from '../../../src/studio/server'
import type { DiscoveredCollection } from '../../../src/studio/types'

describe('server', () => {
  describe('printDiscoverySummary', () => {
    let consoleLogSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    })

    afterEach(() => {
      consoleLogSpy.mockRestore()
    })

    it('prints header', () => {
      printDiscoverySummary([])

      expect(consoleLogSpy).toHaveBeenCalledWith('\nDiscovered collections:')
    })

    it('prints collection info', () => {
      const collections: DiscoveredCollection[] = [
        {
          slug: 'users',
          label: 'Users',
          path: '.db/users/data.parquet',
          rowCount: 1000,
          fileSize: 1024 * 1024,
          fields: [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ],
          isParqueDB: true,
        },
      ]

      printDiscoverySummary(collections)

      expect(consoleLogSpy).toHaveBeenCalledWith('  users [ParqueDB]')
      expect(consoleLogSpy).toHaveBeenCalledWith('    Path: .db/users/data.parquet')
      expect(consoleLogSpy).toHaveBeenCalledWith('    Rows: 1,000')
      expect(consoleLogSpy).toHaveBeenCalledWith('    Size: 1.0 MB')
      expect(consoleLogSpy).toHaveBeenCalledWith('    Fields: 2')
    })

    it('prints non-ParqueDB collection without badge', () => {
      const collections: DiscoveredCollection[] = [
        {
          slug: 'external',
          label: 'External',
          path: 'data/external.parquet',
          rowCount: 500,
          fileSize: 512,
          fields: [
            { name: 'id', parquetType: 'INT64', payloadType: 'number', optional: false, isArray: false },
          ],
          isParqueDB: false,
        },
      ]

      printDiscoverySummary(collections)

      expect(consoleLogSpy).toHaveBeenCalledWith('  external')
      expect(consoleLogSpy).not.toHaveBeenCalledWith(expect.stringContaining('[ParqueDB]'))
    })

    it('formats file sizes correctly', () => {
      const testCases: Array<{ size: number; expected: string }> = [
        { size: 100, expected: '100 B' },
        { size: 1024, expected: '1.0 KB' },
        { size: 1536, expected: '1.5 KB' },
        { size: 1024 * 1024, expected: '1.0 MB' },
        { size: 1024 * 1024 * 1024, expected: '1.00 GB' },
        { size: 1024 * 1024 * 1024 * 2.5, expected: '2.50 GB' },
      ]

      for (const { size, expected } of testCases) {
        consoleLogSpy.mockClear()

        const collections: DiscoveredCollection[] = [
          {
            slug: 'test',
            label: 'Test',
            path: 'test.parquet',
            rowCount: 1,
            fileSize: size,
            fields: [],
            isParqueDB: false,
          },
        ]

        printDiscoverySummary(collections)

        expect(consoleLogSpy).toHaveBeenCalledWith(`    Size: ${expected}`)
      }
    })

    it('prints multiple collections', () => {
      const collections: DiscoveredCollection[] = [
        {
          slug: 'users',
          label: 'Users',
          path: '.db/users/data.parquet',
          rowCount: 100,
          fileSize: 1024,
          fields: [],
          isParqueDB: true,
        },
        {
          slug: 'posts',
          label: 'Posts',
          path: '.db/posts/data.parquet',
          rowCount: 500,
          fileSize: 2048,
          fields: [],
          isParqueDB: true,
        },
      ]

      printDiscoverySummary(collections)

      expect(consoleLogSpy).toHaveBeenCalledWith('  users [ParqueDB]')
      expect(consoleLogSpy).toHaveBeenCalledWith('  posts [ParqueDB]')
    })

    it('formats row count with locale separators', () => {
      const collections: DiscoveredCollection[] = [
        {
          slug: 'big',
          label: 'Big',
          path: 'big.parquet',
          rowCount: 1234567,
          fileSize: 1024,
          fields: [],
          isParqueDB: false,
        },
      ]

      printDiscoverySummary(collections)

      // Number formatting depends on locale, but should include separators
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringMatching(/Rows: [\d,]+/))
    })
  })

  // Note: createStudioServer is more of an integration test since it involves
  // dynamic imports and actual Payload server startup. We test the exported
  // utility functions here. Full integration tests would require mocking the
  // entire Payload CMS ecosystem.
})
