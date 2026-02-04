/**
 * Tests for JSON.parse error handling in migration utilities
 *
 * Issue: parquedb-8osi - JSON.parse calls without try-catch in migrations can crash
 * the migration process without proper context.
 *
 * These tests verify that:
 * 1. Malformed JSON in migration file throws error with line number context
 * 2. Error message includes file path and position of syntax error
 * 3. Partial import with error reporting works (don't fail entire migration)
 * 4. SyntaxError is wrapped in MigrationError with context
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { importFromJson, importFromJsonl, streamFromJson, streamFromJsonl } from '../../../src/migration/json'
import { importFromMongodb, streamFromMongodbJsonl } from '../../../src/migration/mongodb'
import { importFromCsv, streamFromCsv } from '../../../src/migration/csv'

describe('JSON.parse error handling in migrations', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-json-errors-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  describe('importFromJson - JSON array files', () => {
    it('throws MigrationError with file path for malformed JSON', async () => {
      const jsonPath = path.join(tempDir, 'malformed.json')
      // Malformed JSON - missing closing brace
      await fs.writeFile(jsonPath, '[{"name": "test"')

      const error = await importFromJson(db, 'items', jsonPath).catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('MigrationError')
      expect(error.message).toContain(jsonPath)
      expect(error.message).toContain('position')
      expect(error.cause).toBeInstanceOf(SyntaxError)
    })

    it('includes character position in error message for syntax errors', async () => {
      const jsonPath = path.join(tempDir, 'syntax-error.json')
      // Invalid JSON with syntax error at specific position
      await fs.writeFile(jsonPath, '[{"name": "test", invalid}]')

      const error = await importFromJson(db, 'items', jsonPath).catch(e => e)

      expect(error).toBeInstanceOf(Error)
      // Error should include position information from SyntaxError
      expect(error.message).toMatch(/position \d+|column \d+|offset \d+/)
    })

    it('provides context about where JSON parsing failed', async () => {
      const jsonPath = path.join(tempDir, 'context-error.json')
      // JSON with error deep in the structure
      const malformedJson = `[
        {"name": "valid1"},
        {"name": "valid2"},
        {"name": "missing-comma" "extra": "field"}
      ]`
      await fs.writeFile(jsonPath, malformedJson)

      const error = await importFromJson(db, 'items', jsonPath).catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('MigrationError')
      // Should provide helpful context about the location
      expect(error.context).toBeDefined()
      expect(error.context.filePath).toBe(jsonPath)
    })
  })

  describe('importFromJsonl - line-by-line error reporting', () => {
    it('reports line number for malformed JSON in JSONL file', async () => {
      const jsonlPath = path.join(tempDir, 'lines.jsonl')
      const lines = [
        '{"name": "good1"}',
        '{"name": "good2"}',
        '{"name": "bad" invalid}',  // Line 3 is malformed
        '{"name": "good3"}',
      ]
      await fs.writeFile(jsonlPath, lines.join('\n'))

      const result = await importFromJsonl(db, 'items', jsonlPath)

      expect(result.failed).toBe(1)
      expect(result.imported).toBe(3)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]!.index).toBe(3) // Line 3
      expect(result.errors[0]!.message).toContain('line 3')
      expect(result.errors[0]!.message).toContain('position')
    })

    it('continues processing after malformed lines (partial import)', async () => {
      const jsonlPath = path.join(tempDir, 'partial.jsonl')
      const lines = [
        '{"name": "item1"}',
        '{bad json}',
        '{"name": "item2"}',
        'not json at all',
        '{"name": "item3"}',
      ]
      await fs.writeFile(jsonlPath, lines.join('\n'))

      const result = await importFromJsonl(db, 'items', jsonlPath)

      // Should import valid lines and report errors for invalid ones
      expect(result.imported).toBe(3)
      expect(result.failed).toBe(2)
      expect(result.errors).toHaveLength(2)
      expect(result.errors.map(e => e.index)).toEqual([2, 4])
    })

    it('wraps SyntaxError in MigrationError with document content', async () => {
      const jsonlPath = path.join(tempDir, 'wrapped.jsonl')
      const malformedLine = '{"name": "test" "missing": "comma"}'
      await fs.writeFile(jsonlPath, malformedLine)

      const result = await importFromJsonl(db, 'items', jsonlPath)

      expect(result.failed).toBe(1)
      expect(result.errors[0]).toBeDefined()
      // The error should include the original document content for debugging
      expect(result.errors[0]!.document).toBe(malformedLine)
      // Error message should reference SyntaxError details
      expect(result.errors[0]!.message).toMatch(/Unexpected|position|token/)
    })
  })

  describe('importFromMongodb - Extended JSON error handling', () => {
    it('reports line number for malformed MongoDB Extended JSON', async () => {
      const mongoPath = path.join(tempDir, 'mongo.json')
      const lines = [
        '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "doc1"}',
        '{"_id": {"$oid": "507f1f77bcf86cd799439012"} "name": "doc2"}', // Missing comma
        '{"_id": {"$oid": "507f1f77bcf86cd799439013"}, "name": "doc3"}',
      ]
      await fs.writeFile(mongoPath, lines.join('\n'))

      const result = await importFromMongodb(db, 'items', mongoPath, { streaming: true })

      expect(result.failed).toBe(1)
      expect(result.imported).toBe(2)
      expect(result.errors[0]!.index).toBe(2)
      expect(result.errors[0]!.message).toContain('line 2')
    })

    it('provides file path context for JSON array format errors', async () => {
      const mongoPath = path.join(tempDir, 'mongo-array.json')
      // Malformed JSON array
      await fs.writeFile(mongoPath, '[{"name": "test"')

      const error = await importFromMongodb(db, 'items', mongoPath).catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('MigrationError')
      expect(error.message).toContain(mongoPath)
      expect(error.context?.filePath).toBe(mongoPath)
    })
  })

  describe('importFromCsv - JSON column type error handling', () => {
    it('handles malformed JSON in columns with json type', async () => {
      const csvPath = path.join(tempDir, 'with-json.csv')
      const csv = `name,config
item1,{"valid": true}
item2,{not valid json}
item3,{"also": "valid"}`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, {
        columnTypes: { config: 'json' },
      })

      // Verify that JSON parse errors are reported with context
      // At minimum, we should have an error for the malformed JSON row
      expect(result.errors.length).toBeGreaterThanOrEqual(1)
      // Find the JSON parse error (not a create error)
      const jsonError = result.errors.find(e => e.message.includes('JSON'))
      expect(jsonError).toBeDefined()
      expect(jsonError!.message).toContain('JSON')
      expect(jsonError!.message).toContain('line 3') // Line 3 has invalid JSON (header is line 1)
      expect(jsonError!.message).toContain('config')
    })
  })

  describe('Streaming functions - error reporting', () => {
    it('streamFromJsonl yields error with line number', async () => {
      const jsonlPath = path.join(tempDir, 'stream.jsonl')
      const lines = [
        '{"name": "good"}',
        '{bad',
        '{"name": "also-good"}',
      ]
      await fs.writeFile(jsonlPath, lines.join('\n'))

      const results = []
      for await (const item of streamFromJsonl(jsonlPath)) {
        results.push(item)
      }

      expect(results).toHaveLength(3)
      const errorResult = results.find(r => r.error)
      expect(errorResult).toBeDefined()
      expect(errorResult!.lineNumber).toBe(2)
      expect(errorResult!.error).toContain('position')
    })

    it('streamFromJson provides position info for parse errors', async () => {
      const jsonPath = path.join(tempDir, 'stream-array.json')
      await fs.writeFile(jsonPath, '[{"name": "test"')

      const error = await (async () => {
        const results = []
        for await (const item of streamFromJson(jsonPath)) {
          results.push(item)
        }
        return null
      })().catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('MigrationError')
      expect(error.message).toContain('position')
      expect(error.context?.filePath).toBe(jsonPath)
    })

    it('streamFromMongodbJsonl yields error with MongoDB context', async () => {
      const mongoPath = path.join(tempDir, 'mongo-stream.jsonl')
      const lines = [
        '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "doc1"}',
        '{"_id": {"$oid": invalid}, "name": "doc2"}',
      ]
      await fs.writeFile(mongoPath, lines.join('\n'))

      const results = []
      for await (const item of streamFromMongodbJsonl(mongoPath)) {
        results.push(item)
      }

      expect(results).toHaveLength(2)
      const errorResult = results.find(r => r.error)
      expect(errorResult).toBeDefined()
      expect(errorResult!.lineNumber).toBe(2)
      // Error should contain either position info or the syntax error message
      expect(errorResult!.error).toMatch(/position|Invalid JSON|Unexpected/)
    })

    it('streamFromCsv reports JSON column errors', async () => {
      const csvPath = path.join(tempDir, 'stream-csv.csv')
      const csv = `name,data
item1,{"key": "value"}
item2,{invalid json}`
      await fs.writeFile(csvPath, csv)

      const results = []
      for await (const item of streamFromCsv(csvPath, { columnTypes: { data: 'json' } })) {
        results.push(item)
      }

      // Should have 2 results: 1 success (item1) + 1 error (item2)
      expect(results.length).toBeGreaterThanOrEqual(1)
      const errorResult = results.find(r => r.error)
      expect(errorResult).toBeDefined()
      // Line 3 has invalid JSON (line 1 is header, line 2 is item1, line 3 is item2)
      expect(errorResult!.lineNumber).toBe(3)
      expect(errorResult!.error).toContain('JSON')
      expect(errorResult!.error).toContain('data') // Column name
    })
  })

  describe('MigrationError structure', () => {
    it('MigrationError includes all context for debugging', async () => {
      const jsonPath = path.join(tempDir, 'debug.json')
      await fs.writeFile(jsonPath, '{"incomplete":')

      const error = await importFromJson(db, 'items', jsonPath).catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('MigrationError')

      // Should have structured context
      expect(error.context).toBeDefined()
      expect(error.context.filePath).toBe(jsonPath)
      // Note: position may be undefined for some error types like "Unexpected end of JSON input"
      // which V8 doesn't include position info for
      expect(error.context.namespace).toBe('items')

      // Should preserve the original SyntaxError
      expect(error.cause).toBeInstanceOf(SyntaxError)
    })

    it('MigrationError is serializable for RPC', async () => {
      const jsonPath = path.join(tempDir, 'rpc.json')
      await fs.writeFile(jsonPath, '[{invalid}]')

      const error = await importFromJson(db, 'items', jsonPath).catch(e => e)

      // Should be serializable (no circular refs, etc.)
      const serialized = JSON.stringify(error)
      const parsed = JSON.parse(serialized)

      expect(parsed.name).toBe('MigrationError')
      expect(parsed.message).toBeDefined()
      expect(parsed.context).toBeDefined()
    })
  })
})
