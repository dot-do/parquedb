/**
 * SQL Injection Security Tests
 *
 * Tests to verify protection against SQL injection vulnerabilities.
 */

import { describe, it, expect } from 'vitest'
import { whereToFilter } from '../../../src/integrations/sql/translator.js'
import { SqliteWal } from '../../../src/events/sqlite-wal.js'
import type { SqliteInterface } from '../../../src/events/sqlite-wal.js'
import { escapeLikePattern, isValidTableName, validateWhereClause, escapeIdentifier } from '../../../src/utils/sql-security.js'

// =============================================================================
// Mock SQLite for testing
// =============================================================================

class MockSqlite implements SqliteInterface {
  exec<T = unknown>(_query: string, ..._params: unknown[]): Iterable<T> {
    return []
  }
}

// =============================================================================
// whereToFilter SQL Injection Tests
// =============================================================================

describe('whereToFilter SQL Injection Protection', () => {
  describe('rejects dangerous SQL constructs', () => {
    it('rejects multiple statements with semicolon', () => {
      expect(() => whereToFilter("status = 'active'; DROP TABLE users"))
        .toThrow('SQL injection detected')
    })

    it('rejects semicolon with SELECT', () => {
      expect(() => whereToFilter("id = 1; SELECT * FROM passwords"))
        .toThrow('SQL injection detected')
    })

    it('rejects semicolon with INSERT', () => {
      expect(() => whereToFilter("id = 1; INSERT INTO admin (user) VALUES ('hacker')"))
        .toThrow('SQL injection detected')
    })

    it('rejects semicolon with UPDATE', () => {
      expect(() => whereToFilter("id = 1; UPDATE users SET role = 'admin'"))
        .toThrow('SQL injection detected')
    })

    it('rejects semicolon with DELETE', () => {
      expect(() => whereToFilter("id = 1; DELETE FROM logs"))
        .toThrow('SQL injection detected')
    })

    it('rejects UNION injection', () => {
      expect(() => whereToFilter("id = 1 UNION SELECT password FROM users"))
        .toThrow('SQL injection detected: UNION')
    })

    it('rejects single-line comments', () => {
      expect(() => whereToFilter("id = 1 --"))
        .toThrow('SQL injection detected: comments')
    })

    it('rejects single-line comments with payload', () => {
      expect(() => whereToFilter("id = 1 -- AND password = 'check'"))
        .toThrow('SQL injection detected: comments')
    })

    it('rejects multi-line comments', () => {
      expect(() => whereToFilter("id = 1 /* comment */"))
        .toThrow('SQL injection detected: comments')
    })

    it('rejects DROP keyword', () => {
      expect(() => whereToFilter("id = 1 AND DROP TABLE users"))
        .toThrow('SQL injection detected: DROP')
    })

    it('rejects TRUNCATE keyword', () => {
      expect(() => whereToFilter("id = 1 AND TRUNCATE TABLE users"))
        .toThrow('SQL injection detected: TRUNCATE')
    })

    it('rejects ALTER keyword', () => {
      expect(() => whereToFilter("id = 1 AND ALTER TABLE users"))
        .toThrow('SQL injection detected: ALTER')
    })

    it('rejects CREATE keyword', () => {
      expect(() => whereToFilter("id = 1 AND CREATE TABLE hacked"))
        .toThrow('SQL injection detected: CREATE')
    })

    it('rejects EXEC keyword', () => {
      expect(() => whereToFilter("id = 1 AND EXEC xp_cmdshell"))
        .toThrow('SQL injection detected: EXEC')
    })

    it('rejects EXECUTE keyword', () => {
      expect(() => whereToFilter("id = 1 AND EXECUTE sp_executesql"))
        .toThrow('SQL injection detected: EXECUTE')
    })

    it('rejects INTO keyword (for SELECT INTO attacks)', () => {
      expect(() => whereToFilter("id = 1 INTO outfile '/tmp/data'"))
        .toThrow('SQL injection detected: INTO')
    })

    it('rejects OUTFILE keyword', () => {
      expect(() => whereToFilter("id = 1 OUTFILE '/tmp/hack.txt'"))
        .toThrow('SQL injection detected: OUTFILE')
    })

    it('rejects LOAD_FILE keyword', () => {
      expect(() => whereToFilter("id = LOAD_FILE('/etc/passwd')"))
        .toThrow('SQL injection detected: LOAD_FILE')
    })

    it('rejects unbalanced parentheses', () => {
      expect(() => whereToFilter("id = 1 AND (status = 'active'"))
        .toThrow('unbalanced parentheses')
    })

    it('rejects excessive nesting depth', () => {
      const deeplyNested = 'a=1' + ' AND (b=2'.repeat(15) + ')'.repeat(15)
      expect(() => whereToFilter(deeplyNested))
        .toThrow('excessive nesting depth')
    })
  })

  describe('allows legitimate SQL constructs', () => {
    it('allows simple equality', () => {
      const filter = whereToFilter("status = 'active'")
      expect(filter).toEqual({ status: 'active' })
    })

    it('allows parameterized queries', () => {
      const filter = whereToFilter('status = $1 AND age > $2', ['active', 25])
      expect(filter).toEqual({ status: 'active', age: { $gt: 25 } })
    })

    it('allows comparison operators', () => {
      expect(() => whereToFilter('age > 18')).not.toThrow()
      expect(() => whereToFilter('age >= 18')).not.toThrow()
      expect(() => whereToFilter('age < 65')).not.toThrow()
      expect(() => whereToFilter('age <= 65')).not.toThrow()
      expect(() => whereToFilter("age != 0")).not.toThrow()
    })

    it('allows IN clause', () => {
      const filter = whereToFilter("status IN ('active', 'pending')")
      expect(filter).toEqual({ status: { $in: ['active', 'pending'] } })
    })

    it('allows LIKE pattern', () => {
      expect(() => whereToFilter("name LIKE '%john%'")).not.toThrow()
    })

    it('allows IS NULL', () => {
      expect(() => whereToFilter('deleted_at IS NULL')).not.toThrow()
    })

    it('allows IS NOT NULL', () => {
      expect(() => whereToFilter('deleted_at IS NOT NULL')).not.toThrow()
    })

    it('allows reasonable nesting', () => {
      expect(() => whereToFilter("(status = 'active' AND (age > 18 OR verified = true))")).not.toThrow()
    })

    it('allows AND/OR combinations', () => {
      expect(() => whereToFilter("status = 'active' AND age > 18")).not.toThrow()
      expect(() => whereToFilter("status = 'active' OR status = 'pending'")).not.toThrow()
    })
  })
})

// =============================================================================
// isValidTableName Tests
// =============================================================================

describe('isValidTableName Security', () => {
  describe('rejects invalid table names', () => {
    it('rejects names starting with numbers', () => {
      expect(isValidTableName('123table')).toBe(false)
    })

    it('rejects names with special characters', () => {
      expect(isValidTableName('table;DROP')).toBe(false)
      expect(isValidTableName('table--')).toBe(false)
      expect(isValidTableName('table/*')).toBe(false)
      expect(isValidTableName("table'")).toBe(false)
      expect(isValidTableName('table"')).toBe(false)
      expect(isValidTableName('table`')).toBe(false)
    })

    it('rejects names with spaces', () => {
      expect(isValidTableName('table name')).toBe(false)
    })

    it('rejects names with hyphens', () => {
      expect(isValidTableName('table-name')).toBe(false)
    })

    it('rejects SQL keywords', () => {
      expect(isValidTableName('SELECT')).toBe(false)
      expect(isValidTableName('select')).toBe(false)
      expect(isValidTableName('INSERT')).toBe(false)
      expect(isValidTableName('UPDATE')).toBe(false)
      expect(isValidTableName('DELETE')).toBe(false)
      expect(isValidTableName('DROP')).toBe(false)
      expect(isValidTableName('CREATE')).toBe(false)
      expect(isValidTableName('ALTER')).toBe(false)
      expect(isValidTableName('TABLE')).toBe(false)
      expect(isValidTableName('INDEX')).toBe(false)
      expect(isValidTableName('FROM')).toBe(false)
      expect(isValidTableName('WHERE')).toBe(false)
      expect(isValidTableName('UNION')).toBe(false)
    })

    it('rejects empty names', () => {
      expect(isValidTableName('')).toBe(false)
    })

    it('rejects excessively long names', () => {
      expect(isValidTableName('a'.repeat(200))).toBe(false)
    })

    it('rejects names with injection patterns', () => {
      expect(isValidTableName('users; DROP TABLE users')).toBe(false)
      expect(isValidTableName('users UNION SELECT')).toBe(false)
    })
  })

  describe('accepts valid table names', () => {
    it('accepts alphanumeric names', () => {
      expect(isValidTableName('users')).toBe(true)
      expect(isValidTableName('user_events')).toBe(true)
      expect(isValidTableName('events_wal')).toBe(true)
    })

    it('accepts names starting with underscore', () => {
      expect(isValidTableName('_internal')).toBe(true)
      expect(isValidTableName('_system_table')).toBe(true)
    })

    it('accepts names with numbers', () => {
      expect(isValidTableName('table1')).toBe(true)
      expect(isValidTableName('events_v2')).toBe(true)
    })

    it('accepts mixed case names', () => {
      expect(isValidTableName('UserEvents')).toBe(true)
      expect(isValidTableName('myTable')).toBe(true)
    })
  })
})

// =============================================================================
// SqliteWal Table Name Validation Tests
// =============================================================================

describe('SqliteWal Table Name Injection Protection', () => {
  it('throws error for malicious table name', () => {
    const sql = new MockSqlite()
    expect(() => new SqliteWal(sql, { tableName: 'users; DROP TABLE users' }))
      .toThrow('Invalid table name')
  })

  it('throws error for table name with SQL comment', () => {
    const sql = new MockSqlite()
    expect(() => new SqliteWal(sql, { tableName: 'events--' }))
      .toThrow('Invalid table name')
  })

  it('throws error for table name with special chars', () => {
    const sql = new MockSqlite()
    expect(() => new SqliteWal(sql, { tableName: "events'" }))
      .toThrow('Invalid table name')
  })

  it('throws error for SQL keyword as table name', () => {
    const sql = new MockSqlite()
    expect(() => new SqliteWal(sql, { tableName: 'SELECT' }))
      .toThrow('Invalid table name')
  })

  it('accepts valid table names', () => {
    const sql = new MockSqlite()
    expect(() => new SqliteWal(sql, { tableName: 'events_wal' })).not.toThrow()
    expect(() => new SqliteWal(sql, { tableName: 'my_custom_table' })).not.toThrow()
    expect(() => new SqliteWal(sql)).not.toThrow() // default table name
  })
})

// =============================================================================
// escapeIdentifier Tests
// =============================================================================

describe('escapeIdentifier Security', () => {
  describe('escapes identifiers correctly', () => {
    it('wraps simple identifiers in double quotes', () => {
      expect(escapeIdentifier('users')).toBe('"users"')
      expect(escapeIdentifier('my_table')).toBe('"my_table"')
    })

    it('escapes embedded double quotes', () => {
      expect(escapeIdentifier('table"name')).toBe('"table""name"')
      expect(escapeIdentifier('a"b"c')).toBe('"a""b""c"')
    })

    it('handles empty string', () => {
      expect(escapeIdentifier('')).toBe('""')
    })

    it('preserves special characters in quoted form', () => {
      // These would be invalid table names but escaping makes them safe
      expect(escapeIdentifier("table'name")).toBe('"table\'name"')
      expect(escapeIdentifier('table;name')).toBe('"table;name"')
      expect(escapeIdentifier('table--name')).toBe('"table--name"')
    })
  })

  describe('prevents identifier injection', () => {
    it('prevents SQL injection via double quotes', () => {
      // Attack: try to break out of quoted identifier
      const escaped = escapeIdentifier('users"; DROP TABLE users; --')
      expect(escaped).toBe('"users""; DROP TABLE users; --"')
      // The entire string is now treated as a single identifier
    })

    it('handles multiple injection attempts', () => {
      const escaped = escapeIdentifier('a"b"c"d')
      expect(escaped).toBe('"a""b""c""d"')
    })
  })
})

// =============================================================================
// escapeLikePattern Tests
// =============================================================================

describe('escapeLikePattern Security', () => {
  describe('escapes LIKE wildcards', () => {
    it('escapes percent sign', () => {
      expect(escapeLikePattern('50% off')).toBe('50\\% off')
    })

    it('escapes underscore', () => {
      expect(escapeLikePattern('user_name')).toBe('user\\_name')
    })

    it('escapes backslash', () => {
      expect(escapeLikePattern('path\\to\\file')).toBe('path\\\\to\\\\file')
    })

    it('escapes multiple wildcards', () => {
      expect(escapeLikePattern('100% of _all_ items'))
        .toBe('100\\% of \\_all\\_ items')
    })

    it('handles empty string', () => {
      expect(escapeLikePattern('')).toBe('')
    })

    it('preserves normal text', () => {
      expect(escapeLikePattern('normal text')).toBe('normal text')
    })

    it('escapes in correct order (backslash first)', () => {
      // Important: backslashes must be escaped first to avoid double-escaping
      expect(escapeLikePattern('\\%')).toBe('\\\\\\%')
    })
  })

  describe('prevents wildcard injection attacks', () => {
    it('prevents match-all injection', () => {
      // Without escaping, '%' would match everything
      const escaped = escapeLikePattern('%')
      expect(escaped).toBe('\\%')
      // This should now only match literal '%' character
    })

    it('prevents single-char wildcard injection', () => {
      // Without escaping, '_' would match any single character
      const escaped = escapeLikePattern('_')
      expect(escaped).toBe('\\_')
    })

    it('prevents combined wildcard injection', () => {
      // Attack: search for '%' to match everything
      const escaped = escapeLikePattern('%%%')
      expect(escaped).toBe('\\%\\%\\%')
    })
  })
})
