/**
 * SQL Security Utilities
 *
 * Provides utility functions for preventing SQL injection and
 * ensuring safe SQL query construction.
 */

// =============================================================================
// LIKE Pattern Escaping
// =============================================================================

/**
 * Escape SQL LIKE pattern special characters to prevent wildcard injection.
 *
 * SQL LIKE wildcards:
 * - % matches any sequence of characters
 * - _ matches any single character
 * - \ is the escape character (when ESCAPE '\' is specified)
 *
 * Without escaping, user input containing % or _ could match unintended patterns.
 *
 * @param pattern - User input to escape
 * @returns Escaped pattern safe for use in LIKE queries
 *
 * @example
 * ```typescript
 * const escaped = escapeLikePattern("50% off!")
 * // Returns: "50\\% off!"
 * // Use with: WHERE name LIKE ? ESCAPE '\'
 * ```
 */
export function escapeLikePattern(pattern: string): string {
  return pattern
    .replace(/\\/g, '\\\\')  // Escape backslashes first
    .replace(/%/g, '\\%')    // Escape % wildcard
    .replace(/_/g, '\\_')    // Escape _ wildcard
}

// =============================================================================
// Table Name Validation
// =============================================================================

/**
 * Validate that a table name is safe for SQL interpolation.
 *
 * SQLite identifiers must:
 * - Start with a letter or underscore
 * - Contain only alphanumeric characters and underscores
 * - Not be a reserved keyword (basic check)
 *
 * This prevents SQL injection via table name manipulation.
 *
 * @param name - Table name to validate
 * @returns true if the table name is safe
 */
export function isValidTableName(name: string): boolean {
  // Must start with letter or underscore, followed by alphanumeric/underscore
  // Max length 128 to prevent buffer issues
  if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,127}$/.test(name)) {
    return false
  }

  // Block common SQL keywords (case-insensitive)
  const reserved = [
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
    'ALTER', 'TABLE', 'INDEX', 'FROM', 'WHERE', 'AND', 'OR',
    'UNION', 'JOIN', 'INTO', 'VALUES', 'SET', 'NULL', 'TRUE', 'FALSE'
  ]

  return !reserved.includes(name.toUpperCase())
}

// =============================================================================
// WHERE Clause Validation
// =============================================================================

/**
 * Validate that a WHERE clause string doesn't contain SQL injection patterns.
 *
 * This is a defense-in-depth measure. The parser itself should reject invalid SQL,
 * but this provides an additional layer of protection.
 *
 * @param whereClause - The WHERE clause string to validate
 * @throws Error if dangerous patterns are detected
 */
export function validateWhereClause(whereClause: string): void {
  // Normalize for checking (case-insensitive)
  const normalized = whereClause.toUpperCase()

  // Check for multiple statement attempts (semicolon followed by SQL keyword)
  if (/;\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|UNION)/i.test(whereClause)) {
    throw new Error('SQL injection detected: multiple statements not allowed')
  }

  // Check for comment injection (could be used to bypass validation)
  if (/--/.test(whereClause) || /\/\*/.test(whereClause)) {
    throw new Error('SQL injection detected: comments not allowed in WHERE clause')
  }

  // Check for UNION injection (common attack vector)
  if (/\bUNION\b/i.test(whereClause)) {
    throw new Error('SQL injection detected: UNION not allowed in WHERE clause')
  }

  // Check for dangerous keywords that shouldn't appear in WHERE
  const dangerousKeywords = [
    'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE',
    'EXEC', 'EXECUTE', 'INTO', 'OUTFILE', 'DUMPFILE', 'LOAD_FILE'
  ]

  for (const keyword of dangerousKeywords) {
    if (new RegExp(`\\b${keyword}\\b`).test(normalized)) {
      throw new Error(`SQL injection detected: ${keyword} not allowed in WHERE clause`)
    }
  }

  // Check for excessive nesting (potential DoS via recursive parsing)
  const parenDepth = whereClause.split('(').length - whereClause.split(')').length
  if (Math.abs(parenDepth) > 0) {
    throw new Error('Invalid WHERE clause: unbalanced parentheses')
  }

  const maxParenDepth = (whereClause.match(/\(/g) || []).length
  if (maxParenDepth > 10) {
    throw new Error('Invalid WHERE clause: excessive nesting depth')
  }
}
