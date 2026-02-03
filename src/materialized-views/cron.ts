/**
 * Cron Expression Validation for ParqueDB
 *
 * Provides comprehensive validation of cron expressions with detailed error messages.
 *
 * @example
 * import { validateCronExpression, isValidCronExpression } from './cron'
 *
 * // Simple boolean check
 * if (isValidCronExpression('0 * * * *')) {
 *   console.log('Valid!')
 * }
 *
 * // Detailed validation with error messages
 * const result = validateCronExpression('60 * * * *')
 * if (!result.valid) {
 *   console.error(result.error) // "Invalid value in minute: 60 is out of range (0-59)"
 * }
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Cron field definitions with valid ranges
 */
const CRON_FIELD_RANGES: Array<{ name: string; min: number; max: number }> = [
  { name: 'minute', min: 0, max: 59 },
  { name: 'hour', min: 0, max: 23 },
  { name: 'day of month', min: 1, max: 31 },
  { name: 'month', min: 1, max: 12 },
  { name: 'day of week', min: 0, max: 6 },
]

/**
 * Result of cron expression validation
 */
export interface CronValidationResult {
  /** Whether the cron expression is valid */
  valid: boolean
  /** Error message if invalid */
  error?: string | undefined
  /** Field that caused the error (if applicable) */
  field?: string | undefined
}

// =============================================================================
// Internal Validation
// =============================================================================

/**
 * Validate a single cron field
 */
function validateCronField(
  field: string,
  min: number,
  max: number,
  fieldName: string
): CronValidationResult {
  // Handle wildcard
  if (field === '*') {
    return { valid: true }
  }

  // Handle step values (e.g., */15, 0-30/5)
  if (field.includes('/')) {
    const [range, stepStr] = field.split('/')
    if (!stepStr || stepStr === '') {
      return {
        valid: false,
        error: `Invalid step value in ${fieldName}: missing step after '/'`,
        field: fieldName,
      }
    }
    const step = Number(stepStr)
    if (isNaN(step) || step <= 0 || !Number.isInteger(step)) {
      return {
        valid: false,
        error: `Invalid step value in ${fieldName}: '${stepStr}' must be a positive integer`,
        field: fieldName,
      }
    }
    // Validate the range part
    if (range !== '*') {
      const rangeResult = validateCronField(range!, min, max, fieldName)
      if (!rangeResult.valid) {
        return rangeResult
      }
    }
    return { valid: true }
  }

  // Handle lists (e.g., 1,3,5)
  if (field.includes(',')) {
    for (const part of field.split(',')) {
      const result = validateCronField(part, min, max, fieldName)
      if (!result.valid) {
        return result
      }
    }
    return { valid: true }
  }

  // Handle ranges (e.g., 1-5)
  if (field.includes('-')) {
    const [startStr, endStr] = field.split('-')
    if (!startStr || !endStr) {
      return {
        valid: false,
        error: `Invalid range in ${fieldName}: '${field}' is malformed`,
        field: fieldName,
      }
    }
    const start = Number(startStr)
    const end = Number(endStr)

    if (isNaN(start) || !Number.isInteger(start)) {
      return {
        valid: false,
        error: `Invalid range start in ${fieldName}: '${startStr}' is not a valid integer`,
        field: fieldName,
      }
    }
    if (isNaN(end) || !Number.isInteger(end)) {
      return {
        valid: false,
        error: `Invalid range end in ${fieldName}: '${endStr}' is not a valid integer`,
        field: fieldName,
      }
    }
    if (start < min || start > max) {
      return {
        valid: false,
        error: `Invalid range start in ${fieldName}: ${start} is out of range (${min}-${max})`,
        field: fieldName,
      }
    }
    if (end < min || end > max) {
      return {
        valid: false,
        error: `Invalid range end in ${fieldName}: ${end} is out of range (${min}-${max})`,
        field: fieldName,
      }
    }
    if (start > end) {
      return {
        valid: false,
        error: `Invalid range in ${fieldName}: start (${start}) is greater than end (${end})`,
        field: fieldName,
      }
    }
    return { valid: true }
  }

  // Handle single values
  const num = Number(field)
  if (isNaN(num) || !Number.isInteger(num)) {
    return {
      valid: false,
      error: `Invalid value in ${fieldName}: '${field}' is not a valid integer`,
      field: fieldName,
    }
  }
  if (num < min || num > max) {
    return {
      valid: false,
      error: `Invalid value in ${fieldName}: ${num} is out of range (${min}-${max})`,
      field: fieldName,
    }
  }

  return { valid: true }
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Validate a cron expression with detailed error messages
 *
 * Standard cron format: minute hour day-of-month month day-of-week
 *
 * @param cron - The cron expression to validate
 * @returns Validation result with error details if invalid
 *
 * @example
 * validateCronExpression('0 * * * *')
 * // { valid: true }
 *
 * @example
 * validateCronExpression('60 * * * *')
 * // { valid: false, error: 'Invalid value in minute: 60 is out of range (0-59)', field: 'minute' }
 *
 * @example
 * validateCronExpression('* * 32 * *')
 * // { valid: false, error: 'Invalid value in day of month: 32 is out of range (1-31)', field: 'day of month' }
 */
export function validateCronExpression(cron: string): CronValidationResult {
  if (!cron || typeof cron !== 'string') {
    return {
      valid: false,
      error: 'Cron expression must be a non-empty string',
    }
  }

  const trimmed = cron.trim()
  if (trimmed === '') {
    return {
      valid: false,
      error: 'Cron expression cannot be empty',
    }
  }

  const parts = trimmed.split(/\s+/)

  // Standard cron has 5 parts
  if (parts.length !== 5) {
    return {
      valid: false,
      error: `Cron expression must have exactly 5 fields (minute hour day-of-month month day-of-week), got ${parts.length}`,
    }
  }

  // Validate each field
  for (let i = 0; i < parts.length; i++) {
    const fieldDef = CRON_FIELD_RANGES[i]!
    const result = validateCronField(parts[i]!, fieldDef.min, fieldDef.max, fieldDef.name)
    if (!result.valid) {
      return result
    }
  }

  return { valid: true }
}

/**
 * Check if a cron expression is valid
 *
 * Standard format: minute hour day-of-month month day-of-week
 *
 * Supported syntax:
 * - Wildcards: * (any value)
 * - Specific values: 0, 1, 2, etc.
 * - Ranges: 1-5 (inclusive)
 * - Lists: 1,3,5
 * - Steps: *\/15, 0-30/5
 *
 * Field ranges:
 * - minute: 0-59
 * - hour: 0-23
 * - day of month: 1-31
 * - month: 1-12
 * - day of week: 0-6 (0 = Sunday)
 *
 * @example
 * ```typescript
 * isValidCronExpression('0 * * * *')      // true - every hour
 * isValidCronExpression('0 0 * * *')      // true - daily at midnight
 * isValidCronExpression('0/15 * * * *')   // true - every 15 minutes
 * isValidCronExpression('0 9-17 * * 1-5') // true - hourly 9am-5pm, Mon-Fri
 * isValidCronExpression('60 * * * *')     // false - minute out of range
 * isValidCronExpression('* * 32 * *')     // false - day out of range
 * ```
 */
export function isValidCronExpression(cron: string): boolean {
  return validateCronExpression(cron).valid
}
