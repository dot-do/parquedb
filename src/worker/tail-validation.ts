/**
 * Tail Handler Input Validation
 *
 * Runtime validation for TraceItem input received by the tail handler.
 * Validates required fields, types, and structure to ensure graceful
 * handling of malformed input from Cloudflare tail workers.
 *
 * @module worker/tail-validation
 */

import { ValidationError } from '../errors'
import {
  DEFAULT_TAIL_MAX_ITEMS,
  DEFAULT_TAIL_MAX_LOGS_PER_ITEM,
  DEFAULT_TAIL_MAX_EXCEPTIONS_PER_ITEM,
} from '../constants'

// =============================================================================
// Validation Result Types
// =============================================================================

/**
 * Result of validating a single TraceItem
 */
export interface TraceItemValidationResult {
  /** Whether the item is valid */
  valid: boolean
  /** Validation errors (empty if valid) */
  errors: ValidationError[]
  /** The validated item (with defaults applied) or null if invalid */
  item: ValidatedTraceItem | null
}

/**
 * Result of validating an array of TraceItems
 */
export interface TraceItemsValidationResult {
  /** All valid items */
  validItems: ValidatedTraceItem[]
  /** All invalid items with their errors */
  invalidItems: Array<{
    index: number
    item: unknown
    errors: ValidationError[]
  }>
  /** Total items processed */
  totalCount: number
  /** Count of valid items */
  validCount: number
  /** Count of invalid items */
  invalidCount: number
}

/**
 * A validated TraceItem with normalized fields
 */
export interface ValidatedTraceItem {
  scriptName: string | null
  outcome: string
  eventTimestamp: number | null
  event: ValidatedEventInfo | null
  logs: ValidatedLog[]
  exceptions: ValidatedException[]
  diagnosticsChannelEvents: unknown[]
}

/**
 * Validated event info structure
 */
export interface ValidatedEventInfo {
  request?: ValidatedRequest | undefined
  response?: { status: number } | undefined
  scheduledTime?: number | undefined
  cron?: string | undefined
  queue?: string | undefined
  batchSize?: number | undefined
}

/**
 * Validated request structure
 */
export interface ValidatedRequest {
  url: string
  method: string
  headers: Record<string, string>
  cf?: Record<string, unknown> | undefined
}

/**
 * Validated log entry
 */
export interface ValidatedLog {
  timestamp: number
  level: string
  message: unknown
}

/**
 * Validated exception entry
 */
export interface ValidatedException {
  name: string
  message: string
  timestamp: number
}

// =============================================================================
// Validation Configuration
// =============================================================================

/**
 * Configuration options for tail input validation
 */
export interface TailValidationConfig {
  /** Whether to throw on first error (default: false, returns all errors) */
  throwOnError?: boolean | undefined
  /** Whether to allow and skip invalid items (default: true) */
  skipInvalidItems?: boolean | undefined
  /** Maximum number of items to process (default: unlimited) */
  maxItems?: number | undefined
  /** Maximum number of logs per item (default: 1000) */
  maxLogsPerItem?: number | undefined
  /** Maximum number of exceptions per item (default: 100) */
  maxExceptionsPerItem?: number | undefined
}

/**
 * Default validation configuration
 */
type ResolvedTailValidationConfig = { [K in keyof TailValidationConfig]-?: NonNullable<TailValidationConfig[K]> }

export const DEFAULT_VALIDATION_CONFIG: ResolvedTailValidationConfig = {
  throwOnError: false,
  skipInvalidItems: true,
  maxItems: DEFAULT_TAIL_MAX_ITEMS,
  maxLogsPerItem: DEFAULT_TAIL_MAX_LOGS_PER_ITEM,
  maxExceptionsPerItem: DEFAULT_TAIL_MAX_EXCEPTIONS_PER_ITEM,
}

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Validate that a value is an object (not null, not array)
 */
function validateIsObject(value: unknown, fieldName: string): ValidationError | null {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return new ValidationError(
      `${fieldName} must be an object`,
      {
        field: fieldName,
        expectedType: 'object',
        actualType: value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value,
      }
    )
  }
  return null
}

/**
 * Validate a log entry
 */
function validateLog(log: unknown, index: number): { valid: ValidatedLog | null; error: ValidationError | null } {
  if (log === null || typeof log !== 'object' || Array.isArray(log)) {
    return {
      valid: null,
      error: new ValidationError(
        `logs[${index}] must be an object`,
        {
          field: `logs[${index}]`,
          expectedType: 'object',
          actualType: log === null ? 'null' : Array.isArray(log) ? 'array' : typeof log,
        }
      ),
    }
  }

  const logObj = log as Record<string, unknown>

  // Validate timestamp (required)
  if (typeof logObj.timestamp !== 'number') {
    return {
      valid: null,
      error: new ValidationError(
        `logs[${index}].timestamp must be a number`,
        {
          field: `logs[${index}].timestamp`,
          expectedType: 'number',
          actualType: typeof logObj.timestamp,
        }
      ),
    }
  }

  // Validate level (required)
  if (typeof logObj.level !== 'string') {
    return {
      valid: null,
      error: new ValidationError(
        `logs[${index}].level must be a string`,
        {
          field: `logs[${index}].level`,
          expectedType: 'string',
          actualType: typeof logObj.level,
        }
      ),
    }
  }

  return {
    valid: {
      timestamp: logObj.timestamp,
      level: logObj.level,
      message: logObj.message,
    },
    error: null,
  }
}

/**
 * Validate an exception entry
 */
function validateException(exception: unknown, index: number): { valid: ValidatedException | null; error: ValidationError | null } {
  if (exception === null || typeof exception !== 'object' || Array.isArray(exception)) {
    return {
      valid: null,
      error: new ValidationError(
        `exceptions[${index}] must be an object`,
        {
          field: `exceptions[${index}]`,
          expectedType: 'object',
          actualType: exception === null ? 'null' : Array.isArray(exception) ? 'array' : typeof exception,
        }
      ),
    }
  }

  const excObj = exception as Record<string, unknown>

  // Validate name (required)
  if (typeof excObj.name !== 'string') {
    return {
      valid: null,
      error: new ValidationError(
        `exceptions[${index}].name must be a string`,
        {
          field: `exceptions[${index}].name`,
          expectedType: 'string',
          actualType: typeof excObj.name,
        }
      ),
    }
  }

  // Validate message (required)
  if (typeof excObj.message !== 'string') {
    return {
      valid: null,
      error: new ValidationError(
        `exceptions[${index}].message must be a string`,
        {
          field: `exceptions[${index}].message`,
          expectedType: 'string',
          actualType: typeof excObj.message,
        }
      ),
    }
  }

  // Validate timestamp (required)
  if (typeof excObj.timestamp !== 'number') {
    return {
      valid: null,
      error: new ValidationError(
        `exceptions[${index}].timestamp must be a number`,
        {
          field: `exceptions[${index}].timestamp`,
          expectedType: 'number',
          actualType: typeof excObj.timestamp,
        }
      ),
    }
  }

  return {
    valid: {
      name: excObj.name,
      message: excObj.message,
      timestamp: excObj.timestamp,
    },
    error: null,
  }
}

/**
 * Validate request info
 */
function validateRequest(request: unknown): { valid: ValidatedRequest | null; error: ValidationError | null } {
  if (request === null || typeof request !== 'object' || Array.isArray(request)) {
    return {
      valid: null,
      error: new ValidationError(
        'event.request must be an object',
        {
          field: 'event.request',
          expectedType: 'object',
          actualType: request === null ? 'null' : Array.isArray(request) ? 'array' : typeof request,
        }
      ),
    }
  }

  const reqObj = request as Record<string, unknown>

  // Validate url (required)
  if (typeof reqObj.url !== 'string') {
    return {
      valid: null,
      error: new ValidationError(
        'event.request.url must be a string',
        {
          field: 'event.request.url',
          expectedType: 'string',
          actualType: typeof reqObj.url,
        }
      ),
    }
  }

  // Validate method (required)
  if (typeof reqObj.method !== 'string') {
    return {
      valid: null,
      error: new ValidationError(
        'event.request.method must be a string',
        {
          field: 'event.request.method',
          expectedType: 'string',
          actualType: typeof reqObj.method,
        }
      ),
    }
  }

  // Validate headers (optional, defaults to empty object)
  let headers: Record<string, string> = {}
  if (reqObj.headers !== undefined) {
    if (reqObj.headers === null || typeof reqObj.headers !== 'object' || Array.isArray(reqObj.headers)) {
      return {
        valid: null,
        error: new ValidationError(
          'event.request.headers must be an object',
          {
            field: 'event.request.headers',
            expectedType: 'object',
            actualType: reqObj.headers === null ? 'null' : Array.isArray(reqObj.headers) ? 'array' : typeof reqObj.headers,
          }
        ),
      }
    }
    headers = reqObj.headers as Record<string, string>
  }

  return {
    valid: {
      url: reqObj.url,
      method: reqObj.method,
      headers,
      cf: reqObj.cf as Record<string, unknown> | undefined,
    },
    error: null,
  }
}

/**
 * Validate event info
 */
function validateEventInfo(event: unknown): { valid: ValidatedEventInfo | null; error: ValidationError | null } {
  if (event === null) {
    return { valid: null, error: null }
  }

  if (typeof event !== 'object' || Array.isArray(event)) {
    return {
      valid: null,
      error: new ValidationError(
        'event must be an object or null',
        {
          field: 'event',
          expectedType: 'object | null',
          actualType: Array.isArray(event) ? 'array' : typeof event,
        }
      ),
    }
  }

  const eventObj = event as Record<string, unknown>
  const validatedEvent: ValidatedEventInfo = {}

  // Validate request if present
  if (eventObj.request !== undefined) {
    const requestResult = validateRequest(eventObj.request)
    if (requestResult.error) {
      return { valid: null, error: requestResult.error }
    }
    if (requestResult.valid) {
      validatedEvent.request = requestResult.valid
    }
  }

  // Validate response if present
  if (eventObj.response !== undefined) {
    if (eventObj.response !== null && typeof eventObj.response === 'object' && !Array.isArray(eventObj.response)) {
      const respObj = eventObj.response as Record<string, unknown>
      if (typeof respObj.status === 'number') {
        validatedEvent.response = { status: respObj.status }
      }
    }
  }

  // Copy optional fields
  if (typeof eventObj.scheduledTime === 'number') {
    validatedEvent.scheduledTime = eventObj.scheduledTime
  }
  if (typeof eventObj.cron === 'string') {
    validatedEvent.cron = eventObj.cron
  }
  if (typeof eventObj.queue === 'string') {
    validatedEvent.queue = eventObj.queue
  }
  if (typeof eventObj.batchSize === 'number') {
    validatedEvent.batchSize = eventObj.batchSize
  }

  return { valid: validatedEvent, error: null }
}

/**
 * Validate a single TraceItem
 *
 * @param item - The item to validate
 * @param config - Validation configuration
 * @returns Validation result with valid item or errors
 */
export function validateTraceItem(
  item: unknown,
  config: TailValidationConfig = {}
): TraceItemValidationResult {
  const cfg: ResolvedTailValidationConfig = {
    throwOnError: config.throwOnError ?? DEFAULT_VALIDATION_CONFIG.throwOnError,
    skipInvalidItems: config.skipInvalidItems ?? DEFAULT_VALIDATION_CONFIG.skipInvalidItems,
    maxItems: config.maxItems ?? DEFAULT_VALIDATION_CONFIG.maxItems,
    maxLogsPerItem: config.maxLogsPerItem ?? DEFAULT_VALIDATION_CONFIG.maxLogsPerItem,
    maxExceptionsPerItem: config.maxExceptionsPerItem ?? DEFAULT_VALIDATION_CONFIG.maxExceptionsPerItem,
  }
  const errors: ValidationError[] = []

  // Must be an object
  const objError = validateIsObject(item, 'TraceItem')
  if (objError) {
    return { valid: false, errors: [objError], item: null }
  }

  const obj = item as Record<string, unknown>

  // Validate outcome (required string)
  if (typeof obj.outcome !== 'string') {
    errors.push(new ValidationError(
      'outcome is required and must be a string',
      {
        field: 'outcome',
        expectedType: 'string',
        actualType: typeof obj.outcome,
      }
    ))
  }

  // Validate scriptName (optional, string or null)
  if (obj.scriptName !== undefined && obj.scriptName !== null && typeof obj.scriptName !== 'string') {
    errors.push(new ValidationError(
      'scriptName must be a string or null',
      {
        field: 'scriptName',
        expectedType: 'string | null',
        actualType: typeof obj.scriptName,
      }
    ))
  }

  // Validate eventTimestamp (optional, number or null)
  if (obj.eventTimestamp !== undefined && obj.eventTimestamp !== null && typeof obj.eventTimestamp !== 'number') {
    errors.push(new ValidationError(
      'eventTimestamp must be a number or null',
      {
        field: 'eventTimestamp',
        expectedType: 'number | null',
        actualType: typeof obj.eventTimestamp,
      }
    ))
  }

  // Validate event
  const eventResult = validateEventInfo(obj.event)
  if (eventResult.error) {
    errors.push(eventResult.error)
  }

  // Validate logs (must be array)
  if (!Array.isArray(obj.logs)) {
    errors.push(new ValidationError(
      'logs is required and must be an array',
      {
        field: 'logs',
        expectedType: 'array',
        actualType: typeof obj.logs,
      }
    ))
  }

  // Validate exceptions (must be array)
  if (!Array.isArray(obj.exceptions)) {
    errors.push(new ValidationError(
      'exceptions is required and must be an array',
      {
        field: 'exceptions',
        expectedType: 'array',
        actualType: typeof obj.exceptions,
      }
    ))
  }

  // If we have critical errors at this point, return early
  if (errors.length > 0 && cfg.throwOnError) {
    throw errors[0]
  }
  if (errors.length > 0) {
    return { valid: false, errors, item: null }
  }

  // Now validate logs array entries
  const validatedLogs: ValidatedLog[] = []
  const logsArray = obj.logs as unknown[]
  const logsToProcess = Math.min(logsArray.length, cfg.maxLogsPerItem)

  for (let i = 0; i < logsToProcess; i++) {
    const logResult = validateLog(logsArray[i], i)
    if (logResult.error) {
      errors.push(logResult.error)
      if (cfg.throwOnError) {
        throw logResult.error
      }
    } else if (logResult.valid) {
      validatedLogs.push(logResult.valid)
    }
  }

  // Validate exceptions array entries
  const validatedExceptions: ValidatedException[] = []
  const exceptionsArray = obj.exceptions as unknown[]
  const exceptionsToProcess = Math.min(exceptionsArray.length, cfg.maxExceptionsPerItem)

  for (let i = 0; i < exceptionsToProcess; i++) {
    const excResult = validateException(exceptionsArray[i], i)
    if (excResult.error) {
      errors.push(excResult.error)
      if (cfg.throwOnError) {
        throw excResult.error
      }
    } else if (excResult.valid) {
      validatedExceptions.push(excResult.valid)
    }
  }

  // If we had any errors in logs/exceptions, the item is still valid
  // but we exclude the invalid entries (graceful degradation)
  const validatedItem: ValidatedTraceItem = {
    scriptName: typeof obj.scriptName === 'string' ? obj.scriptName : null,
    outcome: obj.outcome as string,
    eventTimestamp: typeof obj.eventTimestamp === 'number' ? obj.eventTimestamp : null,
    event: eventResult.valid,
    logs: validatedLogs,
    exceptions: validatedExceptions,
    diagnosticsChannelEvents: Array.isArray(obj.diagnosticsChannelEvents)
      ? obj.diagnosticsChannelEvents
      : [],
  }

  return {
    valid: true,
    errors,
    item: validatedItem,
  }
}

/**
 * Validate an array of TraceItems
 *
 * @param items - The items to validate
 * @param config - Validation configuration
 * @returns Validation result with valid items and error details
 */
export function validateTraceItems(
  items: unknown,
  config: TailValidationConfig = {}
): TraceItemsValidationResult {
  const cfg: ResolvedTailValidationConfig = {
    throwOnError: config.throwOnError ?? DEFAULT_VALIDATION_CONFIG.throwOnError,
    skipInvalidItems: config.skipInvalidItems ?? DEFAULT_VALIDATION_CONFIG.skipInvalidItems,
    maxItems: config.maxItems ?? DEFAULT_VALIDATION_CONFIG.maxItems,
    maxLogsPerItem: config.maxLogsPerItem ?? DEFAULT_VALIDATION_CONFIG.maxLogsPerItem,
    maxExceptionsPerItem: config.maxExceptionsPerItem ?? DEFAULT_VALIDATION_CONFIG.maxExceptionsPerItem,
  }

  // Must be an array
  if (!Array.isArray(items)) {
    const error = new ValidationError(
      'Tail handler input must be an array of TraceItems',
      {
        field: 'events',
        expectedType: 'array',
        actualType: typeof items,
      }
    )
    if (cfg.throwOnError) {
      throw error
    }
    return {
      validItems: [],
      invalidItems: [{
        index: -1,
        item: items,
        errors: [error],
      }],
      totalCount: 0,
      validCount: 0,
      invalidCount: 1,
    }
  }

  const validItems: ValidatedTraceItem[] = []
  const invalidItems: TraceItemsValidationResult['invalidItems'] = []
  const itemsToProcess = Math.min(items.length, cfg.maxItems)

  for (let i = 0; i < itemsToProcess; i++) {
    const result = validateTraceItem(items[i], config)

    if (result.valid && result.item) {
      validItems.push(result.item)
    } else {
      invalidItems.push({
        index: i,
        item: items[i],
        errors: result.errors,
      })

      if (cfg.throwOnError && result.errors.length > 0) {
        throw result.errors[0]
      }
    }
  }

  return {
    validItems,
    invalidItems,
    totalCount: items.length,
    validCount: validItems.length,
    invalidCount: invalidItems.length,
  }
}

/**
 * Check if a value is a valid TraceItem (type guard)
 *
 * @param item - The item to check
 * @returns true if the item is a valid TraceItem
 */
export function isValidTraceItem(item: unknown): item is ValidatedTraceItem {
  const result = validateTraceItem(item)
  return result.valid
}

/**
 * Create a validation error for tail input
 */
export function createTailValidationError(
  message: string,
  context?: Record<string, unknown>
): ValidationError {
  return new ValidationError(message, {
    operation: 'tail',
    ...context,
  })
}
