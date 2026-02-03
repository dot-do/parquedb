/**
 * Tests for Compaction Migration Validation
 *
 * Comprehensive test suite for validating compaction workflows including:
 * - Row count verification
 * - Checksum validation
 * - Schema compatibility
 * - Data integrity checks
 * - Rollback functionality
 * - Validation reporting
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  CompactionValidator,
  createCompactionValidator,
  isValidationCheck,
  isPreValidationResult,
  isPostValidationResult,
  isMigrationValidationResult,
  type ValidationConfig,
  type PreValidationResult,
  type PostValidationResult,
  type RollbackSnapshot,
} from '@/workflows/compaction-validation'
import type { StorageBackend } from '@/types/storage'
import { parquetWriteBuffer } from 'hyparquet-writer'

// =============================================================================
// Mock Storage Implementation
// =============================================================================

class MockStorageBackend implements StorageBackend {
  private files: Map<string, Uint8Array> = new Map()
  private metadata: Map<string, { size: number; lastModified: Date }> = new Map()

  async read(path: string): Promise<Uint8Array | null> {
    return this.files.get(path) ?? null
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    this.files.set(path, data)
    this.metadata.set(path, { size: data.length, lastModified: new Date() })
  }

  async delete(path: string): Promise<void> {
    this.files.delete(path)
    this.metadata.delete(path)
  }

  async stat(path: string): Promise<{ size: number; lastModified: Date } | null> {
    return this.metadata.get(path) ?? null
  }

  async list(prefix: string): Promise<{ files: string[]; prefixes: string[] }> {
    const files = Array.from(this.files.keys()).filter(k => k.startsWith(prefix))
    return { files, prefixes: [] }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const data = this.files.get(path)
    if (!data) throw new Error(`File not found: ${path}`)
    return data.slice(start, end)
  }

  // Test helpers
  setFile(path: string, data: Uint8Array): void {
    this.files.set(path, data)
    this.metadata.set(path, { size: data.length, lastModified: new Date() })
  }

  getFile(path: string): Uint8Array | undefined {
    return this.files.get(path)
  }

  hasFile(path: string): boolean {
    return this.files.has(path)
  }

  clear(): void {
    this.files.clear()
    this.metadata.clear()
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock Parquet buffer with test data
 */
function createMockParquetBuffer(rows: Record<string, unknown>[]): Uint8Array {
  if (rows.length === 0) {
    // Create empty parquet with schema
    const columnData = [{ name: '$id', data: [] }]
    return new Uint8Array(parquetWriteBuffer({ columnData }))
  }

  const columns = Object.keys(rows[0]!)
  const columnData = columns.map(name => ({
    name,
    data: rows.map(row => row[name] ?? null),
  }))

  return new Uint8Array(parquetWriteBuffer({ columnData }))
}

/**
 * Create test data rows
 */
function createTestRows(count: number, prefix: string = 'entity'): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => ({
    $id: `${prefix}-${i + 1}`,
    name: `Test ${i + 1}`,
    value: i * 100,
    createdAt: new Date().toISOString(),
  }))
}

// =============================================================================
// Test Suite
// =============================================================================

describe('CompactionValidator', () => {
  let storage: MockStorageBackend
  let validator: CompactionValidator

  beforeEach(() => {
    storage = new MockStorageBackend()
    validator = new CompactionValidator(storage)
  })

  // ===========================================================================
  // Pre-Compaction Validation Tests
  // ===========================================================================

  describe('validatePreCompaction', () => {
    it('should pass validation for valid input files', async () => {
      // Create test parquet files
      const rows1 = createTestRows(50, 'file1')
      const rows2 = createTestRows(50, 'file2')

      storage.setFile('data/ns/file1.parquet', createMockParquetBuffer(rows1))
      storage.setFile('data/ns/file2.parquet', createMockParquetBuffer(rows2))

      const result = await validator.validatePreCompaction([
        'data/ns/file1.parquet',
        'data/ns/file2.parquet',
      ])

      expect(result.valid).toBe(true)
      expect(result.totalRowCount).toBe(100)
      expect(result.checksum).toBeTruthy()
      expect(result.schema).toContain('$id')
      expect(result.files).toHaveLength(2)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })

    it('should fail validation when files do not exist', async () => {
      const result = await validator.validatePreCompaction([
        'data/ns/missing1.parquet',
        'data/ns/missing2.parquet',
      ])

      expect(result.valid).toBe(false)
      const filesExistCheck = result.checks.find(c => c.name === 'files-exist')
      expect(filesExistCheck?.passed).toBe(false)
      expect(filesExistCheck?.error).toContain('Missing files')
    })

    it('should fail validation when row count is zero', async () => {
      // Create empty parquet file
      storage.setFile('data/ns/empty.parquet', createMockParquetBuffer([]))

      const result = await validator.validatePreCompaction(['data/ns/empty.parquet'])

      expect(result.valid).toBe(false)
      const rowCountCheck = result.checks.find(c => c.name === 'row-count-positive')
      expect(rowCountCheck?.passed).toBe(false)
    })

    it('should report missing required columns', async () => {
      // Create parquet without $id column
      const rows = [{ name: 'Test', value: 100 }]
      storage.setFile('data/ns/no-id.parquet', createMockParquetBuffer(rows))

      const result = await validator.validatePreCompaction(['data/ns/no-id.parquet'])

      const requiredColsCheck = result.checks.find(c => c.name === 'required-columns-present')
      expect(requiredColsCheck?.passed).toBe(false)
      expect(requiredColsCheck?.error).toContain('$id')
    })

    it('should compute consistent checksum for same files', async () => {
      const rows = createTestRows(10)
      storage.setFile('data/ns/test.parquet', createMockParquetBuffer(rows))

      const result1 = await validator.validatePreCompaction(['data/ns/test.parquet'])
      const result2 = await validator.validatePreCompaction(['data/ns/test.parquet'])

      expect(result1.checksum).toBe(result2.checksum)
    })

    it('should compute different checksum for different files', async () => {
      const rows1 = createTestRows(10, 'a')
      const rows2 = createTestRows(10, 'b')

      storage.setFile('data/ns/a.parquet', createMockParquetBuffer(rows1))
      storage.setFile('data/ns/b.parquet', createMockParquetBuffer(rows2))

      const result1 = await validator.validatePreCompaction(['data/ns/a.parquet'])
      const result2 = await validator.validatePreCompaction(['data/ns/b.parquet'])

      expect(result1.checksum).not.toBe(result2.checksum)
    })
  })

  // ===========================================================================
  // Post-Compaction Validation Tests
  // ===========================================================================

  describe('validatePostCompaction', () => {
    it('should pass validation for valid output files', async () => {
      const rows = createTestRows(100)
      storage.setFile('data/ns/compacted.parquet', createMockParquetBuffer(rows))

      const result = await validator.validatePostCompaction(['data/ns/compacted.parquet'])

      expect(result.valid).toBe(true)
      expect(result.totalRowCount).toBe(100)
      expect(result.checksum).toBeTruthy()
      expect(result.outputFiles).toHaveLength(1)
    })

    it('should fail validation when output files do not exist', async () => {
      const result = await validator.validatePostCompaction(['data/ns/missing.parquet'])

      expect(result.valid).toBe(false)
      const filesExistCheck = result.checks.find(c => c.name === 'files-exist')
      expect(filesExistCheck?.passed).toBe(false)
    })

    it('should check data integrity for required columns', async () => {
      // Create parquet with null $id values
      const rows = [
        { $id: null, name: 'Test 1' },
        { $id: 'valid-id', name: 'Test 2' },
      ]
      storage.setFile('data/ns/with-null.parquet', createMockParquetBuffer(rows))

      const result = await validator.validatePostCompaction(['data/ns/with-null.parquet'])

      const integrityCheck = result.checks.find(c => c.name.startsWith('data-integrity'))
      expect(integrityCheck).toBeDefined()
      // Note: The check may pass or fail depending on null handling in parquet
    })
  })

  // ===========================================================================
  // Migration Validation Tests
  // ===========================================================================

  describe('validateMigration', () => {
    it('should pass when row counts match exactly', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name', 'value'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id', 'name', 'value'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = validator.validateMigration(preValidation, postValidation)

      expect(result.valid).toBe(true)
      expect(result.shouldRollback).toBe(false)
      expect(result.recommendations).toHaveLength(0)

      const rowCountCheck = result.comparisonChecks.find(c => c.name === 'row-count-match')
      expect(rowCountCheck?.passed).toBe(true)
    })

    it('should fail when row count decreases', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 90, // 10 rows lost
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = validator.validateMigration(preValidation, postValidation)

      expect(result.valid).toBe(false)
      expect(result.shouldRollback).toBe(true)

      const rowCountCheck = result.comparisonChecks.find(c => c.name === 'row-count-match')
      expect(rowCountCheck?.passed).toBe(false)

      expect(result.recommendations.some(r => r.includes('decreased'))).toBe(true)
    })

    it('should fail when row count increases', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 110, // 10 extra rows
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = validator.validateMigration(preValidation, postValidation)

      expect(result.valid).toBe(false)
      expect(result.recommendations.some(r => r.includes('increased'))).toBe(true)
    })

    it('should pass with row count tolerance', () => {
      // Create validator with 5% tolerance
      const tolerantValidator = new CompactionValidator(storage, { rowCountTolerance: 5 })

      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 103, // 3% increase, within 5% tolerance
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = tolerantValidator.validateMigration(preValidation, postValidation)

      const rowCountCheck = result.comparisonChecks.find(c => c.name === 'row-count-match')
      expect(rowCountCheck?.passed).toBe(true)
    })

    it('should fail when schema has missing columns', () => {
      const strictValidator = new CompactionValidator(storage, { allowMissingColumns: false })

      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name', 'value', 'extra'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id', 'name', 'value'], // 'extra' column missing
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = strictValidator.validateMigration(preValidation, postValidation)

      const schemaCheck = result.comparisonChecks.find(c => c.name === 'schema-compatible')
      expect(schemaCheck?.passed).toBe(false)
      expect(schemaCheck?.error).toContain('extra')
    })

    it('should allow new columns by default', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id', 'name', 'newColumn'], // New column added
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = validator.validateMigration(preValidation, postValidation)

      const schemaCheck = result.comparisonChecks.find(c => c.name === 'schema-compatible')
      expect(schemaCheck?.passed).toBe(true)
    })

    it('should fail when pre-validation failed', () => {
      const preValidation: PreValidationResult = {
        valid: false, // Pre-validation failed
        files: ['input.parquet'],
        totalRowCount: 0,
        checksum: '',
        schema: [],
        checks: [{ name: 'files-exist', passed: false, description: 'Files exist', error: 'Missing' }],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = validator.validateMigration(preValidation, postValidation)

      expect(result.valid).toBe(false)
      expect(result.shouldRollback).toBe(true)

      const preCheck = result.comparisonChecks.find(c => c.name === 'pre-validation-passed')
      expect(preCheck?.passed).toBe(false)
    })
  })

  // ===========================================================================
  // Rollback Tests
  // ===========================================================================

  describe('rollback', () => {
    it('should delete output files during rollback', async () => {
      // Setup: Create output files
      storage.setFile('data/ns/output1.parquet', new Uint8Array([1, 2, 3]))
      storage.setFile('data/ns/output2.parquet', new Uint8Array([4, 5, 6]))

      const snapshot: RollbackSnapshot = {
        id: 'rollback-test-123',
        timestamp: Date.now(),
        inputFiles: ['data/ns/input.parquet'],
        preValidation: {
          valid: true,
          files: ['data/ns/input.parquet'],
          totalRowCount: 100,
          checksum: 'abc123',
          schema: ['$id', 'name'],
          checks: [],
          durationMs: 10,
          timestamp: Date.now(),
        },
        namespace: 'ns',
        windowStart: 1000,
        windowEnd: 2000,
      }

      const result = await validator.rollback(snapshot, [
        'data/ns/output1.parquet',
        'data/ns/output2.parquet',
      ])

      expect(result.success).toBe(true)
      expect(result.deletedFiles).toContain('data/ns/output1.parquet')
      expect(result.deletedFiles).toContain('data/ns/output2.parquet')
      expect(storage.hasFile('data/ns/output1.parquet')).toBe(false)
      expect(storage.hasFile('data/ns/output2.parquet')).toBe(false)
    })

    it('should handle missing output files gracefully', async () => {
      const snapshot: RollbackSnapshot = {
        id: 'rollback-test-456',
        timestamp: Date.now(),
        inputFiles: [],
        preValidation: {
          valid: true,
          files: [],
          totalRowCount: 0,
          checksum: '',
          schema: [],
          checks: [],
          durationMs: 0,
          timestamp: Date.now(),
        },
        namespace: 'ns',
        windowStart: 1000,
        windowEnd: 2000,
      }

      const result = await validator.rollback(snapshot, [
        'data/ns/nonexistent.parquet',
      ])

      expect(result.success).toBe(true)
      expect(result.deletedFiles).toHaveLength(0)
    })

    it('should track rollback duration', async () => {
      const snapshot: RollbackSnapshot = {
        id: 'rollback-test-789',
        timestamp: Date.now(),
        inputFiles: [],
        preValidation: {
          valid: true,
          files: [],
          totalRowCount: 0,
          checksum: '',
          schema: [],
          checks: [],
          durationMs: 0,
          timestamp: Date.now(),
        },
        namespace: 'ns',
        windowStart: 1000,
        windowEnd: 2000,
      }

      const result = await validator.rollback(snapshot, [])

      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })
  })

  // ===========================================================================
  // Rollback Snapshot Tests
  // ===========================================================================

  describe('createRollbackSnapshot', () => {
    it('should create a valid rollback snapshot', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['data/ns/input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const snapshot = validator.createRollbackSnapshot(
        ['data/ns/input.parquet'],
        preValidation,
        'test-namespace',
        1000,
        2000
      )

      expect(snapshot.id).toContain('rollback-test-namespace-1000')
      expect(snapshot.inputFiles).toEqual(['data/ns/input.parquet'])
      expect(snapshot.preValidation).toBe(preValidation)
      expect(snapshot.namespace).toBe('test-namespace')
      expect(snapshot.windowStart).toBe(1000)
      expect(snapshot.windowEnd).toBe(2000)
      expect(snapshot.timestamp).toBeLessThanOrEqual(Date.now())
    })
  })

  // ===========================================================================
  // Reporting Tests
  // ===========================================================================

  describe('generateReport', () => {
    it('should generate a complete validation report', () => {
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [
          { name: 'check1', passed: true, description: 'Check 1' },
          { name: 'check2', passed: false, description: 'Check 2', error: 'Failed' },
        ],
        durationMs: 50,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [
          { name: 'check3', passed: true, description: 'Check 3' },
        ],
        durationMs: 30,
        timestamp: Date.now(),
      }

      const migrationResult = validator.validateMigration(preValidation, postValidation)

      const report = validator.generateReport(
        migrationResult,
        'test-namespace',
        1000,
        2000,
        false
      )

      expect(report.id).toContain('report-test-namespace-1000')
      expect(report.namespace).toBe('test-namespace')
      expect(report.window).toEqual({ start: 1000, end: 2000 })
      expect(report.preValidation.rowCount).toBe(100)
      expect(report.preValidation.checksum).toBe('abc123')
      expect(report.preValidation.checksPassed).toBe(1)
      expect(report.preValidation.checksFailed).toBe(1)
      expect(report.postValidation.rowCount).toBe(100)
      expect(report.postValidation.checksPassed).toBe(1)
      expect(report.rollbackPerformed).toBe(false)
      expect(report.totalDurationMs).toBe(80)
    })

    it('should report rollback when performed', () => {
      const preValidation: PreValidationResult = {
        valid: false,
        files: [],
        totalRowCount: 0,
        checksum: '',
        schema: [],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: false,
        outputFiles: [],
        totalRowCount: 0,
        checksum: '',
        schema: [],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const migrationResult = validator.validateMigration(preValidation, postValidation)

      const report = validator.generateReport(
        migrationResult,
        'ns',
        1000,
        2000,
        true // Rollback performed
      )

      expect(report.rollbackPerformed).toBe(true)
      expect(report.overallValid).toBe(false)
    })
  })

  // ===========================================================================
  // Configuration Tests
  // ===========================================================================

  describe('configuration', () => {
    it('should use default configuration', () => {
      const defaultValidator = new CompactionValidator(storage)

      // Verify defaults are applied by checking behavior
      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 101, // 1 row difference - should fail with 0 tolerance
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = defaultValidator.validateMigration(preValidation, postValidation)

      const rowCountCheck = result.comparisonChecks.find(c => c.name === 'row-count-match')
      expect(rowCountCheck?.passed).toBe(false) // Default tolerance is 0
    })

    it('should allow custom required columns', async () => {
      const customValidator = new CompactionValidator(storage, {
        requiredColumns: ['customId', 'requiredField'],
      })

      // Create parquet without required columns
      const rows = [{ $id: 'test', name: 'Test' }]
      storage.setFile('data/ns/test.parquet', createMockParquetBuffer(rows))

      const result = await customValidator.validatePreCompaction(['data/ns/test.parquet'])

      const requiredCheck = result.checks.find(c => c.name === 'required-columns-present')
      expect(requiredCheck?.passed).toBe(false)
      expect(requiredCheck?.error).toContain('customId')
      expect(requiredCheck?.error).toContain('requiredField')
    })

    it('should disable row count validation when configured', () => {
      const noRowCountValidator = new CompactionValidator(storage, {
        validateRowCount: false,
      })

      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 50, // 50% difference
        checksum: 'def456',
        schema: ['$id', 'name'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = noRowCountValidator.validateMigration(preValidation, postValidation)

      const rowCountCheck = result.comparisonChecks.find(c => c.name === 'row-count-match')
      expect(rowCountCheck).toBeUndefined()
    })

    it('should disable schema validation when configured', () => {
      const noSchemaValidator = new CompactionValidator(storage, {
        validateSchema: false,
      })

      const preValidation: PreValidationResult = {
        valid: true,
        files: ['input.parquet'],
        totalRowCount: 100,
        checksum: 'abc123',
        schema: ['$id', 'name', 'extra'],
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const postValidation: PostValidationResult = {
        valid: true,
        outputFiles: ['output.parquet'],
        totalRowCount: 100,
        checksum: 'def456',
        schema: ['$id'], // Missing columns
        checks: [],
        durationMs: 10,
        timestamp: Date.now(),
      }

      const result = noSchemaValidator.validateMigration(preValidation, postValidation)

      const schemaCheck = result.comparisonChecks.find(c => c.name === 'schema-compatible')
      expect(schemaCheck).toBeUndefined()
    })
  })

  // ===========================================================================
  // Factory Function Tests
  // ===========================================================================

  describe('createCompactionValidator', () => {
    it('should create a validator with default config', () => {
      const validator = createCompactionValidator(storage)
      expect(validator).toBeInstanceOf(CompactionValidator)
    })

    it('should create a validator with custom config', () => {
      const config: ValidationConfig = {
        rowCountTolerance: 10,
        validateChecksum: false,
      }

      const validator = createCompactionValidator(storage, config)
      expect(validator).toBeInstanceOf(CompactionValidator)
    })
  })

  // ===========================================================================
  // Type Guard Tests
  // ===========================================================================

  describe('type guards', () => {
    describe('isValidationCheck', () => {
      it('should return true for valid ValidationCheck', () => {
        const check = {
          name: 'test',
          passed: true,
          description: 'Test check',
        }
        expect(isValidationCheck(check)).toBe(true)
      })

      it('should return false for invalid objects', () => {
        expect(isValidationCheck(null)).toBe(false)
        expect(isValidationCheck(undefined)).toBe(false)
        expect(isValidationCheck({})).toBe(false)
        expect(isValidationCheck({ name: 'test' })).toBe(false)
        expect(isValidationCheck({ name: 'test', passed: 'yes' })).toBe(false)
      })
    })

    describe('isPreValidationResult', () => {
      it('should return true for valid PreValidationResult', () => {
        const result: PreValidationResult = {
          valid: true,
          files: ['test.parquet'],
          totalRowCount: 100,
          checksum: 'abc',
          schema: ['$id'],
          checks: [],
          durationMs: 10,
          timestamp: Date.now(),
        }
        expect(isPreValidationResult(result)).toBe(true)
      })

      it('should return false for invalid objects', () => {
        expect(isPreValidationResult(null)).toBe(false)
        expect(isPreValidationResult({})).toBe(false)
        expect(isPreValidationResult({ valid: true })).toBe(false)
      })
    })

    describe('isPostValidationResult', () => {
      it('should return true for valid PostValidationResult', () => {
        const result: PostValidationResult = {
          valid: true,
          outputFiles: ['output.parquet'],
          totalRowCount: 100,
          checksum: 'def',
          schema: ['$id'],
          checks: [],
          durationMs: 10,
          timestamp: Date.now(),
        }
        expect(isPostValidationResult(result)).toBe(true)
      })

      it('should return false for invalid objects', () => {
        expect(isPostValidationResult(null)).toBe(false)
        expect(isPostValidationResult({})).toBe(false)
      })
    })

    describe('isMigrationValidationResult', () => {
      it('should return true for valid MigrationValidationResult', () => {
        const preValidation: PreValidationResult = {
          valid: true,
          files: ['input.parquet'],
          totalRowCount: 100,
          checksum: 'abc',
          schema: ['$id'],
          checks: [],
          durationMs: 10,
          timestamp: Date.now(),
        }

        const postValidation: PostValidationResult = {
          valid: true,
          outputFiles: ['output.parquet'],
          totalRowCount: 100,
          checksum: 'def',
          schema: ['$id'],
          checks: [],
          durationMs: 10,
          timestamp: Date.now(),
        }

        const result = {
          valid: true,
          preValidation,
          postValidation,
          comparisonChecks: [],
          recommendations: [],
          shouldRollback: false,
        }

        expect(isMigrationValidationResult(result)).toBe(true)
      })

      it('should return false for invalid objects', () => {
        expect(isMigrationValidationResult(null)).toBe(false)
        expect(isMigrationValidationResult({})).toBe(false)
        expect(isMigrationValidationResult({ valid: true })).toBe(false)
      })
    })
  })

  // ===========================================================================
  // Integration Test
  // ===========================================================================

  describe('full validation workflow', () => {
    it('should complete a full validation cycle', async () => {
      // Setup: Create input files
      const inputRows1 = createTestRows(50, 'batch1')
      const inputRows2 = createTestRows(50, 'batch2')
      storage.setFile('data/ns/input1.parquet', createMockParquetBuffer(inputRows1))
      storage.setFile('data/ns/input2.parquet', createMockParquetBuffer(inputRows2))

      // Step 1: Pre-validation
      const preResult = await validator.validatePreCompaction([
        'data/ns/input1.parquet',
        'data/ns/input2.parquet',
      ])
      expect(preResult.valid).toBe(true)
      expect(preResult.totalRowCount).toBe(100)

      // Step 2: Create rollback snapshot
      const snapshot = validator.createRollbackSnapshot(
        ['data/ns/input1.parquet', 'data/ns/input2.parquet'],
        preResult,
        'ns',
        1000,
        2000
      )
      expect(snapshot.id).toBeTruthy()

      // Step 3: Simulate compaction (create output file)
      const outputRows = [...inputRows1, ...inputRows2]
      storage.setFile('data/ns/compacted.parquet', createMockParquetBuffer(outputRows))

      // Step 4: Post-validation
      const postResult = await validator.validatePostCompaction(['data/ns/compacted.parquet'])
      expect(postResult.valid).toBe(true)
      expect(postResult.totalRowCount).toBe(100)

      // Step 5: Migration validation
      const migrationResult = validator.validateMigration(preResult, postResult)
      expect(migrationResult.valid).toBe(true)
      expect(migrationResult.shouldRollback).toBe(false)

      // Step 6: Generate report
      const report = validator.generateReport(migrationResult, 'ns', 1000, 2000)
      expect(report.overallValid).toBe(true)
      expect(report.preValidation.rowCount).toBe(100)
      expect(report.postValidation.rowCount).toBe(100)
    })

    it('should handle validation failure with rollback', async () => {
      // Setup: Create input files
      const inputRows = createTestRows(100)
      storage.setFile('data/ns/input.parquet', createMockParquetBuffer(inputRows))

      // Step 1: Pre-validation
      const preResult = await validator.validatePreCompaction(['data/ns/input.parquet'])
      expect(preResult.valid).toBe(true)

      // Step 2: Create rollback snapshot
      const snapshot = validator.createRollbackSnapshot(
        ['data/ns/input.parquet'],
        preResult,
        'ns',
        1000,
        2000
      )

      // Step 3: Simulate bad compaction (lose rows)
      const badOutputRows = createTestRows(80) // Lost 20 rows
      storage.setFile('data/ns/bad-output.parquet', createMockParquetBuffer(badOutputRows))

      // Step 4: Post-validation
      const postResult = await validator.validatePostCompaction(['data/ns/bad-output.parquet'])

      // Step 5: Migration validation should fail
      const migrationResult = validator.validateMigration(preResult, postResult)
      expect(migrationResult.valid).toBe(false)
      expect(migrationResult.shouldRollback).toBe(true)

      // Step 6: Perform rollback
      const rollbackResult = await validator.rollback(snapshot, ['data/ns/bad-output.parquet'])
      expect(rollbackResult.success).toBe(true)
      expect(rollbackResult.deletedFiles).toContain('data/ns/bad-output.parquet')
      expect(storage.hasFile('data/ns/bad-output.parquet')).toBe(false)

      // Step 7: Generate report with rollback flag
      const report = validator.generateReport(migrationResult, 'ns', 1000, 2000, true)
      expect(report.rollbackPerformed).toBe(true)
      expect(report.overallValid).toBe(false)
    })
  })
})
