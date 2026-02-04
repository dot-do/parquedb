/**
 * Migration Error Handling Utilities
 *
 * Provides structured error handling for migration operations with
 * detailed context for debugging JSON parsing failures.
 *
 * ## MigrationParseError
 *
 * A custom error class that wraps JSON.parse SyntaxErrors with additional context:
 * - File path where the error occurred
 * - Position in the JSON content (extracted from SyntaxError message)
 * - Line number (for JSONL files)
 * - Namespace being imported to
 * - The original SyntaxError as the cause
 *
 * ## parseJsonWithContext
 *
 * A utility function that wraps JSON.parse with detailed error handling.
 * On parse failure, it extracts position information from the SyntaxError
 * and throws a MigrationParseError with full context.
 *
 * @example
 * ```typescript
 * import { parseJsonWithContext, MigrationParseError } from './errors'
 *
 * try {
 *   const data = parseJsonWithContext(content, {
 *     filePath: '/path/to/file.json',
 *     namespace: 'users',
 *   })
 * } catch (err) {
 *   if (err instanceof MigrationParseError) {
 *     console.error(`Parse error at position ${err.context.position}`)
 *     console.error(`File: ${err.context.filePath}`)
 *   }
 * }
 * ```
 *
 * @module migration/errors
 */

/**
 * Context information for a migration error
 */
export interface MigrationErrorContext {
  /** Path to the file being imported */
  filePath?: string | undefined
  /** Line number in the file (1-indexed, for JSONL/CSV) */
  lineNumber?: number | undefined
  /** Character position in the line/file */
  position?: number | undefined
  /** Namespace being imported to */
  namespace?: string | undefined
  /** Column name (for CSV JSON column errors) */
  column?: string | undefined
  /** Original document/line content */
  document?: string | undefined
}

/**
 * Extract position from a SyntaxError message
 * Different JS engines format this differently:
 * - V8: "at position 17"
 * - SpiderMonkey: "at line 1 column 18"
 * - JavaScriptCore: "JSON Parse error: ..."
 */
export function extractPositionFromSyntaxError(error: SyntaxError): number | undefined {
  const message = error.message

  // V8: "Unexpected token ... at position 17"
  const posMatch = message.match(/at position (\d+)/)
  if (posMatch) {
    return parseInt(posMatch[1]!, 10)
  }

  // SpiderMonkey: "at line 1 column 18"
  const colMatch = message.match(/column (\d+)/)
  if (colMatch) {
    return parseInt(colMatch[1]!, 10)
  }

  return undefined
}

/**
 * Error thrown during migration operations
 *
 * This error class provides detailed context for debugging including:
 * - File path where the error occurred
 * - Line number (for JSONL/CSV files)
 * - Character position from the underlying SyntaxError
 * - Original document content for debugging
 *
 * The error is serializable for RPC transport.
 *
 * @example
 * ```typescript
 * try {
 *   await importFromJson(db, 'items', './data.json')
 * } catch (err) {
 *   if (err instanceof MigrationParseError) {
 *     console.log(`Error in ${err.context.filePath}`)
 *     console.log(`At position ${err.context.position}`)
 *     console.log(`Original error: ${err.cause}`)
 *   }
 * }
 * ```
 */
export class MigrationParseError extends Error {
  override name = 'MigrationError'

  /** Detailed context about where the error occurred */
  context: MigrationErrorContext

  /** Original error that caused this migration error */
  override cause?: Error | undefined

  constructor(
    message: string,
    context: MigrationErrorContext,
    cause?: Error
  ) {
    super(message)
    this.context = context
    this.cause = cause

    // Ensure prototype chain is correct
    Object.setPrototypeOf(this, MigrationParseError.prototype)
  }

  /**
   * Create a MigrationParseError from a JSON.parse SyntaxError
   */
  static fromJsonSyntaxError(
    syntaxError: SyntaxError,
    filePath: string,
    options: {
      namespace?: string
      lineNumber?: number
      document?: string
    } = {}
  ): MigrationParseError {
    const position = extractPositionFromSyntaxError(syntaxError)
    const { namespace, lineNumber, document } = options

    // Build descriptive message
    // Always include "position" in message for consistency, even if unknown
    let message = `JSON parse error in ${filePath}`
    if (lineNumber !== undefined) {
      message += ` at line ${lineNumber}`
    }
    message += position !== undefined ? ` at position ${position}` : ' (position unknown)'
    message += `: ${syntaxError.message}`

    return new MigrationParseError(
      message,
      {
        filePath,
        lineNumber,
        position,
        namespace,
        document,
      },
      syntaxError
    )
  }

  /**
   * Create a MigrationParseError for a JSON column in CSV
   */
  static fromCsvJsonColumnError(
    syntaxError: SyntaxError,
    filePath: string,
    lineNumber: number,
    column: string,
    value: string
  ): MigrationParseError {
    const position = extractPositionFromSyntaxError(syntaxError)

    let message = `JSON parse error in column '${column}' at line ${lineNumber}`
    if (position !== undefined) {
      message += ` (position ${position})`
    }
    message += `: ${syntaxError.message}`

    return new MigrationParseError(
      message,
      {
        filePath,
        lineNumber,
        position,
        column,
        document: value,
      },
      syntaxError
    )
  }

  /**
   * Make the error JSON serializable for RPC transport
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      message: this.message,
      context: this.context,
      cause: this.cause ? {
        name: this.cause.name,
        message: this.cause.message,
      } : undefined,
    }
  }
}

/**
 * Format an error message for JSONL line parsing with position info
 */
export function formatJsonlParseError(
  syntaxError: SyntaxError,
  lineNumber: number
): string {
  const position = extractPositionFromSyntaxError(syntaxError)
  let message = `JSON parse error at line ${lineNumber}`
  if (position !== undefined) {
    message += ` (position ${position})`
  }
  message += `: ${syntaxError.message}`
  return message
}

/**
 * Format an error message for CSV JSON column parsing
 */
export function formatCsvJsonColumnError(
  syntaxError: SyntaxError,
  lineNumber: number,
  column: string
): string {
  const position = extractPositionFromSyntaxError(syntaxError)
  let message = `JSON parse error in column '${column}' at line ${lineNumber}`
  if (position !== undefined) {
    message += ` (position ${position})`
  }
  message += `: ${syntaxError.message}`
  return message
}

// =============================================================================
// parseJsonWithContext Utility
// =============================================================================

/**
 * Options for parseJsonWithContext function.
 */
export interface ParseJsonOptions {
  /** The file path being processed (for error context) */
  filePath: string

  /** The namespace being imported to (for error context) */
  namespace?: string | undefined

  /** Line number in the file (for JSONL files) */
  lineNumber?: number | undefined

  /** Maximum document length to include in error context (default: 200) */
  maxDocumentLength?: number | undefined
}

/**
 * Parse JSON with detailed error context.
 *
 * This function wraps JSON.parse and provides detailed error context
 * when parsing fails. It extracts position information from the SyntaxError
 * and includes file path and namespace in the error.
 *
 * @param content - The JSON string to parse
 * @param options - Context options including filePath and namespace
 * @returns The parsed JSON value
 * @throws MigrationParseError if parsing fails
 *
 * @example
 * ```typescript
 * // Basic usage - throws MigrationParseError on failure
 * const data = parseJsonWithContext('{"name": "test"}', {
 *   filePath: '/path/to/file.json',
 * })
 *
 * // With namespace context
 * const users = parseJsonWithContext(content, {
 *   filePath: '/path/to/users.json',
 *   namespace: 'users',
 * })
 *
 * // For JSONL (line-by-line) with line number
 * const doc = parseJsonWithContext(line, {
 *   filePath: '/path/to/data.jsonl',
 *   lineNumber: 42,
 *   namespace: 'events',
 * })
 * ```
 */
export function parseJsonWithContext<T = unknown>(
  content: string,
  options: ParseJsonOptions
): T {
  const { filePath, namespace, lineNumber, maxDocumentLength = 200 } = options

  try {
    return JSON.parse(content) as T
  } catch (err) {
    if (!(err instanceof SyntaxError)) {
      throw err
    }

    // Truncate document for context if too long
    const document = content.length > maxDocumentLength
      ? content.slice(0, maxDocumentLength) + '...'
      : content

    // Build context object, only including defined values
    const context: { namespace?: string; lineNumber?: number; document?: string } = {}
    if (namespace !== undefined) context.namespace = namespace
    if (lineNumber !== undefined) context.lineNumber = lineNumber
    if (document !== undefined) context.document = document

    throw MigrationParseError.fromJsonSyntaxError(err, filePath, context)
  }
}

/**
 * Safely parse JSON and return a result object instead of throwing.
 *
 * Use this for line-by-line processing where you want to collect errors
 * rather than stopping on the first failure.
 *
 * @param content - The JSON string to parse
 * @param options - Context options
 * @returns Result object with either value or error
 *
 * @example
 * ```typescript
 * for (const [index, line] of lines.entries()) {
 *   const result = safeParseJson(line, {
 *     filePath: 'data.jsonl',
 *     lineNumber: index + 1,
 *   })
 *
 *   if (result.ok) {
 *     processDocument(result.value)
 *   } else {
 *     errors.push({
 *       index: index + 1,
 *       message: result.error.message,
 *       document: line,
 *     })
 *   }
 * }
 * ```
 */
export function safeParseJson<T = unknown>(
  content: string,
  options: ParseJsonOptions
): { ok: true; value: T } | { ok: false; error: MigrationParseError } {
  try {
    const value = parseJsonWithContext<T>(content, options)
    return { ok: true, value }
  } catch (err) {
    if (err instanceof MigrationParseError) {
      return { ok: false, error: err }
    }
    // Wrap unexpected errors
    return {
      ok: false,
      error: new MigrationParseError(
        `Unexpected error parsing JSON: ${(err as Error).message}`,
        { filePath: options.filePath, lineNumber: options.lineNumber, namespace: options.namespace },
        err as Error
      ),
    }
  }
}
