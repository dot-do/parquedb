/**
 * TDD RED Phase: Tests for StandaloneCollection naming and documentation
 *
 * These tests verify that the in-memory Collection is properly renamed and documented
 * to avoid confusion with the production storage-backed collections.
 *
 * Issue: parquedb-zsq4.1
 *
 * Current state: src/Collection.ts exists but should be renamed to StandaloneCollection.ts
 * Expected state: StandaloneCollection.ts with clear naming and import paths
 */

import { describe, it, expect } from 'vitest'
import * as fs from 'node:fs'
import * as path from 'node:path'

const SRC_DIR = path.resolve(__dirname, '../../src')
const ROOT_DIR = path.resolve(__dirname, '../..')

describe('StandaloneCollection naming and documentation', () => {
  describe('File naming', () => {
    it('should have StandaloneCollection.ts file', () => {
      const standaloneCollectionPath = path.join(SRC_DIR, 'StandaloneCollection.ts')
      expect(fs.existsSync(standaloneCollectionPath)).toBe(true)
    })

    it('should NOT have old Collection.ts file', () => {
      const oldCollectionPath = path.join(SRC_DIR, 'Collection.ts')
      expect(fs.existsSync(oldCollectionPath)).toBe(false)
    })
  })

  describe('Export naming', () => {
    it('should export StandaloneCollection class from parquedb', async () => {
      // This tests that the class is exported with a clear name
      const parquedb = await import('parquedb')
      expect(parquedb).toHaveProperty('StandaloneCollection')
      expect(typeof parquedb.StandaloneCollection).toBe('function')
    }, 60000)  // Allow 60s for initial module load

    it('should NOT export ambiguous Collection name from main entry', async () => {
      // The generic "Collection" name is confusing - users might think it's the production class
      const parquedb = await import('parquedb')
      // Collection should either not exist, or be an alias with a deprecation warning
      // For the rename, we want to ensure StandaloneCollection is the primary export
      // and Collection is either removed or clearly marked as standalone
      const hasStandalone = 'StandaloneCollection' in parquedb
      const hasGenericCollection = 'Collection' in parquedb

      // If both exist, Collection should be a deprecated alias pointing to StandaloneCollection
      // If only one exists, it should be StandaloneCollection
      if (hasGenericCollection && hasStandalone) {
        // Collection should be the same as StandaloneCollection (alias)
        expect(parquedb.Collection).toBe(parquedb.StandaloneCollection)
      } else {
        // Only StandaloneCollection should exist
        expect(hasStandalone).toBe(true)
      }
    })
  })

  describe('Package.json exports', () => {
    it('should have standalone-collection export path', async () => {
      const packageJsonPath = path.join(ROOT_DIR, 'package.json')
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

      expect(packageJson.exports).toHaveProperty('./standalone-collection')
    })

    it('should allow import from parquedb/standalone-collection path', async () => {
      // This tests the subpath export works
      // Note: This will fail until package.json is updated with the export
      const packageJsonPath = path.join(ROOT_DIR, 'package.json')
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

      // Check that the export path exists in package.json
      expect(packageJson.exports).toHaveProperty('./standalone-collection')

      // Verify the export configuration is correct
      const standaloneExport = packageJson.exports['./standalone-collection']
      expect(standaloneExport).toBeDefined()

      // Should have import and types fields
      if (typeof standaloneExport === 'object') {
        expect(standaloneExport).toHaveProperty('import')
        expect(standaloneExport).toHaveProperty('types')
      }
    })
  })

  describe('Documentation clarity', () => {
    it('should have clear JSDoc explaining when to use StandaloneCollection vs ParqueDB', async () => {
      const standaloneCollectionPath = path.join(SRC_DIR, 'StandaloneCollection.ts')

      // Skip if file doesn't exist yet (will fail in file naming test)
      if (!fs.existsSync(standaloneCollectionPath)) {
        expect.fail('StandaloneCollection.ts does not exist')
        return
      }

      const content = fs.readFileSync(standaloneCollectionPath, 'utf-8')

      // Should clearly state it's for testing/development
      expect(content).toContain('testing')
      expect(content).toContain('development')

      // Should mention production alternative
      expect(content).toContain('ParqueDB')

      // Should have clear usage examples
      expect(content).toContain('@example')

      // Class name should be StandaloneCollection
      expect(content).toMatch(/export class StandaloneCollection/)
    })

    it('should have index.ts documentation warning about standalone usage', async () => {
      const indexPath = path.join(SRC_DIR, 'index.ts')
      const content = fs.readFileSync(indexPath, 'utf-8')

      // Should export with clear documentation
      expect(content).toContain('StandaloneCollection')

      // Should have JSDoc or comments explaining the distinction
      expect(content).toMatch(/standalone.*in-memory/i)
    })
  })

  describe('No references to old Collection name in public API', () => {
    it('should not use generic "Collection" as the exported class name in index.ts', async () => {
      const indexPath = path.join(SRC_DIR, 'index.ts')
      const content = fs.readFileSync(indexPath, 'utf-8')

      // Should not have exports like: export { Collection } without renaming
      // Should either export StandaloneCollection directly or rename Collection to StandaloneCollection
      const exportLines = content.split('\n').filter(line =>
        line.includes('export') && line.includes('Collection') && !line.includes('StandaloneCollection')
      )

      // Filter out comments
      const nonCommentExports = exportLines.filter(line => !line.trim().startsWith('*') && !line.trim().startsWith('//'))

      // If Collection is exported, it should be as an alias from StandaloneCollection
      for (const line of nonCommentExports) {
        // Allow: export { StandaloneCollection as Collection }
        // Allow: export { Collection } from './StandaloneCollection' (if renamed)
        // Disallow: export { Collection } from './Collection'
        if (line.includes("from './Collection'") || line.includes('from "./Collection"')) {
          expect.fail(`Found export from old Collection.ts: ${line}`)
        }
      }
    })

    it('should export clearGlobalStorage from StandaloneCollection module', async () => {
      // clearGlobalStorage should come from StandaloneCollection, not Collection
      const indexPath = path.join(SRC_DIR, 'index.ts')
      const content = fs.readFileSync(indexPath, 'utf-8')

      // The clearGlobalStorage export should reference StandaloneCollection
      const clearStorageExport = content.split('\n').find(line =>
        line.includes('clearGlobalStorage')
      )

      expect(clearStorageExport).toBeDefined()

      if (clearStorageExport) {
        // Should be from StandaloneCollection, not Collection
        expect(clearStorageExport).not.toContain("from './Collection'")
        expect(clearStorageExport).not.toContain('from "./Collection"')
      }
    })
  })
})
