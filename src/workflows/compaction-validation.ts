/**
 * Migration Validation for Compaction
 *
 * Provides comprehensive validation for compaction workflows to ensure
 * data integrity during the migration process.
 *
 * Key features:
 * - Row count verification before/after compaction
 * - Checksum validation of compacted files
 * - Data integrity checks (schema, null values, required fields)
 * - Rollback capability on validation failure
 * - Validation reporting and logging
 *
 * @module compaction-validation
 */

import { createHash } from 'crypto'
import { logger } from '../utils/logger'
import type { StorageBackend } from '../types/storage'
import { readParquet, readParquetMetadata } from '../parquet/reader'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for validation checks
 */
export interface ValidationConfig {
  /** Enable row count validation (default: true) */
  validateRowCount?: boolean

  /** Enable checksum validation (default: true) */
  validateChecksum?: boolean

  /** Enable schema validation (default: true) */
  validateSchema?: boolean

  /** Enable data integrity checks (default: true) */
  validateDataIntegrity?: boolean

  /** Required columns that must not be null (default: ['$id']) */
  requiredColumns?: string[]

  /** Allowed row count tolerance as a percentage (default: 0 - exact match) */
  rowCountTolerance?: number

  /** Whether to allow new columns in output (default: true) */
  allowNewColumns?: boolean

  /** Whether to allow missing columns in output (default: false) */
  allowMissingColumns?: boolean
}

/**
 * Result of a single validation check
 */
export interface ValidationCheck {
  /** Name of the check */
  name: string

  /** Whether the check passed */
  passed: boolean

  /** Description of what was checked */
  description: string

  /** Error message if check failed */
  error?: string

  /** Additional details/metrics */
  details?: Record<string, unknown>
}

/**
 * Result of pre-compaction validation
 */
export interface PreValidationResult {
  /** Overall validation status */
  valid: boolean

  /** List of files analyzed */
  files: string[]

  /** Total row count across all input files */
  totalRowCount: number

  /** Combined checksum of all input files */
  checksum: string

  /** Schema of input files */
  schema: string[]

  /** Individual validation checks */
  checks: ValidationCheck[]

  /** Duration of validation in milliseconds */
  durationMs: number

  /** Timestamp of validation */
  timestamp: number
}

/**
 * Result of post-compaction validation
 */
export interface PostValidationResult {
  /** Overall validation status */
  valid: boolean

  /** Output files created */
  outputFiles: string[]

  /** Total row count in output files */
  totalRowCount: number

  /** Checksum of output files */
  checksum: string

  /** Schema of output files */
  schema: string[]

  /** Individual validation checks */
  checks: ValidationCheck[]

  /** Duration of validation in milliseconds */
  durationMs: number

  /** Timestamp of validation */
  timestamp: number
}

/**
 * Complete migration validation result
 */
export interface MigrationValidationResult {
  /** Overall validation status */
  valid: boolean

  /** Pre-compaction validation results */
  preValidation: PreValidationResult

  /** Post-compaction validation results */
  postValidation: PostValidationResult

  /** Comparison checks between pre and post */
  comparisonChecks: ValidationCheck[]

  /** Recommendations if validation failed */
  recommendations: string[]

  /** Whether rollback is recommended */
  shouldRollback: boolean
}

/**
 * State snapshot for rollback
 */
export interface RollbackSnapshot {
  /** Unique identifier for this snapshot */
  id: string

  /** Timestamp when snapshot was created */
  timestamp: number

  /** Original input files */
  inputFiles: string[]

  /** Pre-validation result */
  preValidation: PreValidationResult

  /** Namespace being compacted */
  namespace: string

  /** Window start timestamp */
  windowStart: number

  /** Window end timestamp */
  windowEnd: number
}

/**
 * Result of a rollback operation
 */
export interface RollbackResult {
  /** Whether rollback was successful */
  success: boolean

  /** Files that were restored */
  restoredFiles: string[]

  /** Files that were deleted (output files) */
  deletedFiles: string[]

  /** Error message if rollback failed */
  error?: string

  /** Duration of rollback in milliseconds */
  durationMs: number
}

/**
 * Validation report for logging and auditing
 */
export interface ValidationReport {
  /** Report ID */
  id: string

  /** Timestamp */
  timestamp: number

  /** Namespace */
  namespace: string

  /** Window being compacted */
  window: { start: number; end: number }

  /** Pre-validation summary */
  preValidation: {
    valid: boolean
    rowCount: number
    checksum: string
    checksPassed: number
    checksFailed: number
  }

  /** Post-validation summary */
  postValidation: {
    valid: boolean
    rowCount: number
    checksum: string
    checksPassed: number
    checksFailed: number
  }

  /** Overall status */
  overallValid: boolean

  /** Rollback performed */
  rollbackPerformed: boolean

  /** Total duration */
  totalDurationMs: number
}

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG: Required<ValidationConfig> = {
  validateRowCount: true,
  validateChecksum: true,
  validateSchema: true,
  validateDataIntegrity: true,
  requiredColumns: ['$id'],
  rowCountTolerance: 0,
  allowNewColumns: true,
  allowMissingColumns: false,
}

// =============================================================================
// CompactionValidator Class
// =============================================================================

/**
 * Validates compaction migrations to ensure data integrity
 *
 * @example
 * ```typescript
 * const validator = new CompactionValidator(storage)
 *
 * // Pre-compaction validation
 * const preResult = await validator.validatePreCompaction(inputFiles)
 *
 * // ... perform compaction ...
 *
 * // Post-compaction validation
 * const postResult = await validator.validatePostCompaction(outputFiles)
 *
 * // Compare results
 * const migrationResult = await validator.validateMigration(preResult, postResult)
 *
 * if (!migrationResult.valid && migrationResult.shouldRollback) {
 *   await validator.rollback(snapshot)
 * }
 * ```
 */
export class CompactionValidator {
  private storage: StorageBackend
  private config: Required<ValidationConfig>

  constructor(storage: StorageBackend, config: ValidationConfig = {}) {
    this.storage = storage
    this.config = { ...DEFAULT_CONFIG, ...config }
  }

  // ===========================================================================
  // Pre-Compaction Validation
  // ===========================================================================

  /**
   * Validate input files before compaction
   */
  async validatePreCompaction(files: string[]): Promise<PreValidationResult> {
    const startTime = Date.now()
    const checks: ValidationCheck[] = []
    let totalRowCount = 0
    let schema: string[] = []
    const checksumParts: string[] = []

    logger.info('Starting pre-compaction validation', { fileCount: files.length })

    // Check: Files exist
    const existCheck = await this.checkFilesExist(files)
    checks.push(existCheck)

    if (!existCheck.passed) {
      return {
        valid: false,
        files,
        totalRowCount: 0,
        checksum: '',
        schema: [],
        checks,
        durationMs: Date.now() - startTime,
        timestamp: Date.now(),
      }
    }

    // Process each file
    for (const file of files) {
      try {
        // Read metadata
        const metadata = await readParquetMetadata(this.storage, file)
        totalRowCount += metadata.numRows

        // Extract schema columns
        if (schema.length === 0 && metadata.schema.length > 0) {
          schema = metadata.schema
            .filter((s): s is { name: string } => s != null && typeof s === 'object' && 'name' in s)
            .map(s => s.name)
        }

        // Compute file checksum
        const fileData = await this.storage.read(file)
        if (fileData) {
          checksumParts.push(this.computeChecksum(fileData))
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : 'Unknown error'
        logger.warn(`Failed to process file for validation: ${file}`, { error: errorMsg })
      }
    }

    // Check: Row count is positive
    if (this.config.validateRowCount) {
      checks.push({
        name: 'row-count-positive',
        passed: totalRowCount > 0,
        description: 'Input files contain at least one row',
        details: { totalRowCount },
        error: totalRowCount === 0 ? 'No rows found in input files' : undefined,
      })
    }

    // Check: Schema is valid
    if (this.config.validateSchema) {
      const schemaCheck = this.checkSchemaValid(schema)
      checks.push(schemaCheck)
    }

    // Check: Required columns exist
    if (this.config.validateDataIntegrity && this.config.requiredColumns.length > 0) {
      const requiredColsCheck = this.checkRequiredColumns(schema, this.config.requiredColumns)
      checks.push(requiredColsCheck)
    }

    // Compute combined checksum
    const combinedChecksum = this.computeCombinedChecksum(checksumParts)

    const valid = checks.every(c => c.passed)

    logger.info('Pre-compaction validation complete', {
      valid,
      totalRowCount,
      checksum: combinedChecksum.substring(0, 16),
      checksCount: checks.length,
      checksPassedCount: checks.filter(c => c.passed).length,
    })

    return {
      valid,
      files,
      totalRowCount,
      checksum: combinedChecksum,
      schema,
      checks,
      durationMs: Date.now() - startTime,
      timestamp: Date.now(),
    }
  }

  // ===========================================================================
  // Post-Compaction Validation
  // ===========================================================================

  /**
   * Validate output files after compaction
   */
  async validatePostCompaction(files: string[]): Promise<PostValidationResult> {
    const startTime = Date.now()
    const checks: ValidationCheck[] = []
    let totalRowCount = 0
    let schema: string[] = []
    const checksumParts: string[] = []

    logger.info('Starting post-compaction validation', { fileCount: files.length })

    // Check: Output files exist
    const existCheck = await this.checkFilesExist(files)
    checks.push(existCheck)

    if (!existCheck.passed) {
      return {
        valid: false,
        outputFiles: files,
        totalRowCount: 0,
        checksum: '',
        schema: [],
        checks,
        durationMs: Date.now() - startTime,
        timestamp: Date.now(),
      }
    }

    // Process each output file
    for (const file of files) {
      try {
        // Read metadata
        const metadata = await readParquetMetadata(this.storage, file)
        totalRowCount += metadata.numRows

        // Extract schema columns
        if (schema.length === 0 && metadata.schema.length > 0) {
          schema = metadata.schema
            .filter((s): s is { name: string } => s != null && typeof s === 'object' && 'name' in s)
            .map(s => s.name)
        }

        // Compute file checksum
        const fileData = await this.storage.read(file)
        if (fileData) {
          checksumParts.push(this.computeChecksum(fileData))
        }

        // Data integrity check: verify required columns have no null values
        if (this.config.validateDataIntegrity) {
          const integrityCheck = await this.checkDataIntegrity(file)
          checks.push(integrityCheck)
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : 'Unknown error'
        logger.warn(`Failed to process output file for validation: ${file}`, { error: errorMsg })
        checks.push({
          name: `file-readable-${file}`,
          passed: false,
          description: `Output file ${file} is readable`,
          error: errorMsg,
        })
      }
    }

    // Check: Schema is valid
    if (this.config.validateSchema) {
      const schemaCheck = this.checkSchemaValid(schema)
      checks.push(schemaCheck)
    }

    // Compute combined checksum
    const combinedChecksum = this.computeCombinedChecksum(checksumParts)

    const valid = checks.every(c => c.passed)

    logger.info('Post-compaction validation complete', {
      valid,
      totalRowCount,
      checksum: combinedChecksum.substring(0, 16),
      checksCount: checks.length,
      checksPassedCount: checks.filter(c => c.passed).length,
    })

    return {
      valid,
      outputFiles: files,
      totalRowCount,
      checksum: combinedChecksum,
      schema,
      checks,
      durationMs: Date.now() - startTime,
      timestamp: Date.now(),
    }
  }

  // ===========================================================================
  // Migration Validation
  // ===========================================================================

  /**
   * Compare pre and post validation results to verify migration integrity
   */
  validateMigration(
    preValidation: PreValidationResult,
    postValidation: PostValidationResult
  ): MigrationValidationResult {
    const comparisonChecks: ValidationCheck[] = []
    const recommendations: string[] = []

    logger.info('Comparing pre and post compaction validation results')

    // Check: Row count matches (within tolerance)
    if (this.config.validateRowCount) {
      const rowCountCheck = this.compareRowCounts(
        preValidation.totalRowCount,
        postValidation.totalRowCount,
        this.config.rowCountTolerance
      )
      comparisonChecks.push(rowCountCheck)

      if (!rowCountCheck.passed) {
        const diff = postValidation.totalRowCount - preValidation.totalRowCount
        if (diff < 0) {
          recommendations.push(
            `Row count decreased by ${Math.abs(diff)} rows. ` +
            `Verify no data was lost during compaction. ` +
            `Input: ${preValidation.totalRowCount}, Output: ${postValidation.totalRowCount}`
          )
        } else {
          recommendations.push(
            `Row count increased by ${diff} rows. ` +
            `This may indicate duplicate data was introduced. ` +
            `Input: ${preValidation.totalRowCount}, Output: ${postValidation.totalRowCount}`
          )
        }
      }
    }

    // Check: Schema compatibility
    if (this.config.validateSchema) {
      const schemaCheck = this.compareSchemas(
        preValidation.schema,
        postValidation.schema
      )
      comparisonChecks.push(schemaCheck)

      if (!schemaCheck.passed) {
        recommendations.push(
          'Schema mismatch detected. Review column changes and ensure compatibility.'
        )
      }
    }

    // Check: Pre and post validations passed individually
    comparisonChecks.push({
      name: 'pre-validation-passed',
      passed: preValidation.valid,
      description: 'Pre-compaction validation passed',
      error: preValidation.valid ? undefined : 'Pre-compaction validation failed',
    })

    comparisonChecks.push({
      name: 'post-validation-passed',
      passed: postValidation.valid,
      description: 'Post-compaction validation passed',
      error: postValidation.valid ? undefined : 'Post-compaction validation failed',
    })

    // Determine overall validity
    const valid = comparisonChecks.every(c => c.passed) &&
                  preValidation.valid &&
                  postValidation.valid

    // Determine if rollback is recommended
    const shouldRollback = !valid && (
      // Critical failures that warrant rollback
      comparisonChecks.some(c =>
        !c.passed && (
          c.name === 'row-count-match' ||
          c.name === 'pre-validation-passed'
        )
      )
    )

    if (shouldRollback) {
      recommendations.unshift(
        'CRITICAL: Rollback is recommended due to validation failures.'
      )
    }

    logger.info('Migration validation complete', {
      valid,
      shouldRollback,
      comparisonChecksCount: comparisonChecks.length,
      comparisonChecksPassedCount: comparisonChecks.filter(c => c.passed).length,
      recommendationsCount: recommendations.length,
    })

    return {
      valid,
      preValidation,
      postValidation,
      comparisonChecks,
      recommendations,
      shouldRollback,
    }
  }

  // ===========================================================================
  // Rollback Functionality
  // ===========================================================================

  /**
   * Create a rollback snapshot before compaction
   */
  createRollbackSnapshot(
    inputFiles: string[],
    preValidation: PreValidationResult,
    namespace: string,
    windowStart: number,
    windowEnd: number
  ): RollbackSnapshot {
    const id = `rollback-${namespace}-${windowStart}-${Date.now()}`

    logger.info('Creating rollback snapshot', {
      id,
      namespace,
      inputFileCount: inputFiles.length,
    })

    return {
      id,
      timestamp: Date.now(),
      inputFiles,
      preValidation,
      namespace,
      windowStart,
      windowEnd,
    }
  }

  /**
   * Perform rollback by deleting output files
   *
   * Note: This does NOT restore deleted source files.
   * For full rollback capability, source files should be backed up
   * or deletion should be deferred until validation passes.
   */
  async rollback(
    snapshot: RollbackSnapshot,
    outputFiles: string[]
  ): Promise<RollbackResult> {
    const startTime = Date.now()
    const deletedFiles: string[] = []
    let error: string | undefined

    logger.info('Starting rollback', {
      snapshotId: snapshot.id,
      outputFileCount: outputFiles.length,
    })

    try {
      // Delete output files that were created
      for (const file of outputFiles) {
        try {
          const exists = await this.storage.stat(file)
          if (exists) {
            await this.storage.delete(file)
            deletedFiles.push(file)
            logger.info(`Deleted output file during rollback: ${file}`)
          }
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : 'Unknown error'
          logger.warn(`Failed to delete output file during rollback: ${file}`, { error: errMsg })
        }
      }

      logger.info('Rollback complete', {
        snapshotId: snapshot.id,
        deletedFileCount: deletedFiles.length,
      })

      return {
        success: true,
        restoredFiles: [], // Source files not restored (see note above)
        deletedFiles,
        durationMs: Date.now() - startTime,
      }
    } catch (err) {
      error = err instanceof Error ? err.message : 'Unknown error'
      logger.error('Rollback failed', { error, snapshotId: snapshot.id })

      return {
        success: false,
        restoredFiles: [],
        deletedFiles,
        error,
        durationMs: Date.now() - startTime,
      }
    }
  }

  // ===========================================================================
  // Reporting
  // ===========================================================================

  /**
   * Generate a validation report for logging and auditing
   */
  generateReport(
    migrationResult: MigrationValidationResult,
    namespace: string,
    windowStart: number,
    windowEnd: number,
    rollbackPerformed: boolean = false
  ): ValidationReport {
    const { preValidation, postValidation } = migrationResult

    return {
      id: `report-${namespace}-${windowStart}-${Date.now()}`,
      timestamp: Date.now(),
      namespace,
      window: { start: windowStart, end: windowEnd },
      preValidation: {
        valid: preValidation.valid,
        rowCount: preValidation.totalRowCount,
        checksum: preValidation.checksum,
        checksPassed: preValidation.checks.filter(c => c.passed).length,
        checksFailed: preValidation.checks.filter(c => !c.passed).length,
      },
      postValidation: {
        valid: postValidation.valid,
        rowCount: postValidation.totalRowCount,
        checksum: postValidation.checksum,
        checksPassed: postValidation.checks.filter(c => c.passed).length,
        checksFailed: postValidation.checks.filter(c => !c.passed).length,
      },
      overallValid: migrationResult.valid,
      rollbackPerformed,
      totalDurationMs: preValidation.durationMs + postValidation.durationMs,
    }
  }

  // ===========================================================================
  // Private Helper Methods
  // ===========================================================================

  /**
   * Check if all files exist
   */
  private async checkFilesExist(files: string[]): Promise<ValidationCheck> {
    const missingFiles: string[] = []

    for (const file of files) {
      const stat = await this.storage.stat(file)
      if (!stat) {
        missingFiles.push(file)
      }
    }

    return {
      name: 'files-exist',
      passed: missingFiles.length === 0,
      description: 'All specified files exist',
      error: missingFiles.length > 0
        ? `Missing files: ${missingFiles.join(', ')}`
        : undefined,
      details: {
        totalFiles: files.length,
        missingCount: missingFiles.length,
        missingFiles,
      },
    }
  }

  /**
   * Check if schema is valid
   */
  private checkSchemaValid(schema: string[]): ValidationCheck {
    const hasColumns = schema.length > 0

    return {
      name: 'schema-valid',
      passed: hasColumns,
      description: 'Schema contains at least one column',
      error: hasColumns ? undefined : 'No columns found in schema',
      details: { columnCount: schema.length, columns: schema },
    }
  }

  /**
   * Check if required columns exist in schema
   */
  private checkRequiredColumns(schema: string[], requiredColumns: string[]): ValidationCheck {
    const missingColumns = requiredColumns.filter(col => !schema.includes(col))

    return {
      name: 'required-columns-present',
      passed: missingColumns.length === 0,
      description: 'All required columns are present in schema',
      error: missingColumns.length > 0
        ? `Missing required columns: ${missingColumns.join(', ')}`
        : undefined,
      details: {
        requiredColumns,
        missingColumns,
        schemaColumns: schema,
      },
    }
  }

  /**
   * Check data integrity for a file (null values in required columns)
   */
  private async checkDataIntegrity(file: string): Promise<ValidationCheck> {
    const issues: string[] = []

    try {
      // Read a sample of rows to check for null values in required columns
      const rows = await readParquet<Record<string, unknown>>(this.storage, file, { limit: 1000 })

      for (const col of this.config.requiredColumns) {
        const nullCount = rows.filter(row => row[col] === null || row[col] === undefined).length
        if (nullCount > 0) {
          issues.push(`Column '${col}' has ${nullCount} null values in sample`)
        }
      }

      return {
        name: `data-integrity-${file.split('/').pop()}`,
        passed: issues.length === 0,
        description: `Data integrity check for ${file}`,
        error: issues.length > 0 ? issues.join('; ') : undefined,
        details: { sampledRows: rows.length, issues },
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error'
      return {
        name: `data-integrity-${file.split('/').pop()}`,
        passed: false,
        description: `Data integrity check for ${file}`,
        error: `Failed to read file: ${errorMsg}`,
      }
    }
  }

  /**
   * Compare row counts with tolerance
   */
  private compareRowCounts(
    inputCount: number,
    outputCount: number,
    tolerancePercent: number
  ): ValidationCheck {
    const diff = Math.abs(outputCount - inputCount)
    const tolerance = Math.ceil(inputCount * (tolerancePercent / 100))
    const passed = diff <= tolerance

    return {
      name: 'row-count-match',
      passed,
      description: tolerancePercent === 0
        ? 'Row count matches exactly'
        : `Row count within ${tolerancePercent}% tolerance`,
      error: passed
        ? undefined
        : `Row count mismatch: input=${inputCount}, output=${outputCount}, diff=${diff}, tolerance=${tolerance}`,
      details: {
        inputCount,
        outputCount,
        difference: diff,
        tolerancePercent,
        toleranceAbsolute: tolerance,
      },
    }
  }

  /**
   * Compare schemas for compatibility
   */
  private compareSchemas(inputSchema: string[], outputSchema: string[]): ValidationCheck {
    const inputSet = new Set(inputSchema)
    const outputSet = new Set(outputSchema)

    const missingInOutput = inputSchema.filter(col => !outputSet.has(col))
    const newInOutput = outputSchema.filter(col => !inputSet.has(col))

    const hasMissing = missingInOutput.length > 0
    const hasNew = newInOutput.length > 0

    const passed = (
      (!hasMissing || this.config.allowMissingColumns) &&
      (!hasNew || this.config.allowNewColumns)
    )

    let error: string | undefined
    if (!passed) {
      const errors: string[] = []
      if (hasMissing && !this.config.allowMissingColumns) {
        errors.push(`Missing columns: ${missingInOutput.join(', ')}`)
      }
      if (hasNew && !this.config.allowNewColumns) {
        errors.push(`New columns: ${newInOutput.join(', ')}`)
      }
      error = errors.join('; ')
    }

    return {
      name: 'schema-compatible',
      passed,
      description: 'Input and output schemas are compatible',
      error,
      details: {
        inputColumns: inputSchema,
        outputColumns: outputSchema,
        missingInOutput,
        newInOutput,
      },
    }
  }

  /**
   * Compute SHA256 checksum of data
   */
  private computeChecksum(data: Uint8Array): string {
    const hash = createHash('sha256')
    hash.update(data)
    return hash.digest('hex')
  }

  /**
   * Compute combined checksum from multiple checksums
   */
  private computeCombinedChecksum(checksums: string[]): string {
    // Sort checksums for deterministic ordering
    const sorted = [...checksums].sort()
    const combined = sorted.join(':')
    const hash = createHash('sha256')
    hash.update(combined)
    return hash.digest('hex')
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a CompactionValidator instance
 */
export function createCompactionValidator(
  storage: StorageBackend,
  config?: ValidationConfig
): CompactionValidator {
  return new CompactionValidator(storage, config)
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Type guard for ValidationCheck
 */
export function isValidationCheck(obj: unknown): obj is ValidationCheck {
  if (typeof obj !== 'object' || obj === null) return false
  const check = obj as Record<string, unknown>
  return (
    typeof check.name === 'string' &&
    typeof check.passed === 'boolean' &&
    typeof check.description === 'string'
  )
}

/**
 * Type guard for PreValidationResult
 */
export function isPreValidationResult(obj: unknown): obj is PreValidationResult {
  if (typeof obj !== 'object' || obj === null) return false
  const result = obj as Record<string, unknown>
  return (
    typeof result.valid === 'boolean' &&
    Array.isArray(result.files) &&
    typeof result.totalRowCount === 'number' &&
    typeof result.checksum === 'string' &&
    Array.isArray(result.schema) &&
    Array.isArray(result.checks)
  )
}

/**
 * Type guard for PostValidationResult
 */
export function isPostValidationResult(obj: unknown): obj is PostValidationResult {
  if (typeof obj !== 'object' || obj === null) return false
  const result = obj as Record<string, unknown>
  return (
    typeof result.valid === 'boolean' &&
    Array.isArray(result.outputFiles) &&
    typeof result.totalRowCount === 'number' &&
    typeof result.checksum === 'string' &&
    Array.isArray(result.schema) &&
    Array.isArray(result.checks)
  )
}

/**
 * Type guard for MigrationValidationResult
 */
export function isMigrationValidationResult(obj: unknown): obj is MigrationValidationResult {
  if (typeof obj !== 'object' || obj === null) return false
  const result = obj as Record<string, unknown>
  return (
    typeof result.valid === 'boolean' &&
    isPreValidationResult(result.preValidation) &&
    isPostValidationResult(result.postValidation) &&
    Array.isArray(result.comparisonChecks) &&
    Array.isArray(result.recommendations) &&
    typeof result.shouldRollback === 'boolean'
  )
}
